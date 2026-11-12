"""Packagist connector for cognee: Composer package metadata as documents.

PHP and Laravel teams track their dependencies on Packagist, and an assistant that cannot
read it cannot answer "which of our Composer packages are abandoned, and what did the
author recommend instead?". This source queries the public, zero-authentication Composer
v2 metadata API and publishes one document per package — description, latest versions,
license, authors, and the abandoned flag with its recommended replacement. See
``packagist.md``.
"""

import json
from collections.abc import Callable, Iterator
from typing import Any

from cognee.shared.logging_utils import get_logger
from cognee.tasks.ingestion import dlt_utils
from cognee.tasks.ingestion.dlt_utils import (
    DOCUMENT_SOURCE_ATTR,
    NODE_SET_COLUMN,
    PIPELINE_SCOPE_ATTR,
)

logger = get_logger("packagist_connector")

PACKAGIST_METADATA_URL = "https://repo.packagist.org/p2/{vendor}/{package}.json"
_TIMEOUT = (10, 30)


class PackagistError(Exception):
    """Base error for Packagist connector errors."""


class PackagistPackageNotFoundError(PackagistError):
    """The vendor/package pair does not exist on Packagist (HTTP 404)."""


class PackagistRateLimitError(PackagistError):
    """Packagist rejected the request for rate reasons (HTTP 429)."""


class PackagistClient:
    """HTTP client for the Composer v2 metadata API. ``session`` is injectable for tests."""

    def __init__(self, session: Any = None):
        if session is None:
            import requests

            session = requests.Session()
        self._session = session

    def get_package(self, vendor: str, package: str) -> dict[str, Any]:
        """The Composer v2 metadata payload of one ``vendor/package``."""
        url = PACKAGIST_METADATA_URL.format(vendor=vendor.strip(), package=package.strip())
        response = self._session.get(
            url,
            headers={
                "User-Agent": "cognee-packagist-connector/1.0 (https://github.com/topoteretes/cognee)",
                "Accept": "application/json",
            },
            timeout=_TIMEOUT,
        )
        if response.status_code == 404:
            raise PackagistPackageNotFoundError(
                f"Package {vendor}/{package!r} does not exist on Packagist."
            )
        if response.status_code == 429:
            raise PackagistRateLimitError("Packagist rate limit exceeded; retry later.")
        response.raise_for_status()
        try:
            payload = response.json()
        except (json.JSONDecodeError, ValueError) as error:
            raise PackagistError(
                f"Packagist returned a non-JSON payload for {vendor}/{package!r}."
            ) from error
        packages = payload.get("packages") if isinstance(payload, dict) else None
        if not isinstance(packages, dict) or not packages:
            raise PackagistError(
                f"Packagist returned a payload without package metadata for {vendor}/{package!r}."
            )
        return payload


def _split_name(qualified: str) -> tuple[str, str]:
    vendor, _, package = qualified.strip().partition("/")
    if not vendor or not package:
        raise ValueError(
            f"Package name {qualified!r} must be a vendor/package pair, like 'laravel/framework'."
        )
    return vendor, package


def format_packagist_document(payload: dict[str, Any], qualified_name: str) -> dict[str, Any]:
    """One Composer v2 payload as a document row over its package's latest versions.

    The payload's ``packages`` maps the qualified name to its version entries, newest
    first. Every distinct major-version head is rendered; the abandoned flag and its
    recommended replacement come from the newest entry, which is where Packagist records
    them.
    """
    versions = payload["packages"][qualified_name]
    entries = [entry for entry in versions if isinstance(entry, dict) and entry.get("version")]
    if not entries:
        raise PackagistError(f"Packagist returned no versions for {qualified_name!r}.")

    newest = entries[0]
    lines = [f"# {qualified_name}"]
    description = newest.get("description")
    if description:
        lines.append(f"Description: {description}")

    abandoned = newest.get("abandoned")
    if abandoned is True:
        lines.append("Abandoned: this package is abandoned and no longer maintained.")
    elif isinstance(abandoned, dict) and abandoned.get("replacement"):
        lines.append(
            f"Abandoned: use {abandoned['replacement']} instead, the author recommended it."
        )

    seen_majors = set()
    for entry in entries:
        version = str(entry.get("version"))
        major = version.lstrip("vV").split(".")[0]
        if major in seen_majors:
            continue
        seen_majors.add(major)
        lines.append(f"\n## Version {version}")
        released = entry.get("time")
        if released:
            lines.append(f"Released: {released}")
        license_list = entry.get("license")
        if isinstance(license_list, list) and license_list:
            lines.append(f"License: {', '.join(str(item) for item in license_list)}")
        authors = entry.get("authors") or []
        if isinstance(authors, list):
            names = [
                str(author.get("name"))
                for author in authors
                if isinstance(author, dict) and author.get("name")
            ]
            if names:
                lines.append(f"Authors: {', '.join(names)}")
        homepage = entry.get("homepage")
        if homepage:
            lines.append(f"Homepage: {homepage}")
        keywords = entry.get("keywords") or []
        if isinstance(keywords, list) and keywords:
            lines.append(f"Keywords: {', '.join(str(item) for item in keywords)}")
        require = entry.get("require") or {}
        if isinstance(require, dict):
            php = require.get("php")
            if php:
                lines.append(f"Requires PHP: {php}")

    row: dict[str, Any] = {
        "id": f"packagist:package:{qualified_name}",
        "title": f"Packagist: {qualified_name}",
        "content": "\n".join(lines).strip(),
        "url": newest.get("homepage") or f"https://packagist.org/packages/{qualified_name}",
        "_deleted": False,
        NODE_SET_COLUMN: "packagist:package",
    }
    return row


def _package_rows(
    client: PackagistClient,
    names: list[str],
    state: dict,
    stats: dict[str, int],
    check_active: Callable[[], None] | None = None,
) -> Iterator[dict[str, Any]]:
    """Fetch and render every package before the first row is yielded, then tombstones.

    A package that cannot be fetched stops the run with nothing published and nothing
    forgotten: a partial snapshot must not look like a deletion. A package removed from
    the selection since the last complete run is forgotten; the state is written after
    the last row, so a run that raises keeps the old one.
    """
    previous = set(state.get("members", []))
    rows: list[dict[str, Any]] = []
    present: set[str] = set()
    for qualified in names:
        if check_active is not None:
            check_active()  # outside any try: a revoked connection is not a failed fetch
        vendor, package = _split_name(qualified)
        payload = client.get_package(vendor, package)
        row = format_packagist_document(payload, qualified)
        present.add(row["id"])
        rows.append(row)
    for row in rows:
        yield row
        # After the yield: a row that check_active stopped before it was published is not counted.
        stats["synced"] += 1
    for row_id in sorted(previous - present):
        yield {"id": row_id, "_deleted": True}
        stats["deleted"] += 1
    state.update(v=1, members=sorted(present))
    logger.info("Packagist: ingested %d package(s), forgot %d.", stats["synced"], stats["deleted"])


def packagist_source(
    *,
    package_names: str | list[str],
    resource_name: str | None = None,
    session: Any = None,
    check_active: Callable[[], None] | None = None,
):
    """Return a ``dlt`` resource that yields one document per ``vendor/package``.

    Hand the result to ``cognee.remember(...)`` with ``write_disposition="merge"`` and
    ``primary_key="id"``. A package's id is its qualified name, so re-syncing after a new
    release updates the document in place. A package that does not exist raises
    ``PackagistPackageNotFoundError`` — an explicit name that misses is a caller error,
    never silently skipped. A package removed from the selection is forgotten on the next
    complete run; read ``cognee_sync_stats`` (``synced``, ``deleted``) afterwards.

    Args:
        package_names: One ``vendor/package`` pair, or a list of them (duplicates are
            ingested once).
        resource_name: Names the staging table and the sync state. Defaults to one fixed
            name, so narrowing the selection forgets the packages that left it; pass
            different names to sync different selections into one dataset.
        session: Optional HTTP session for custom connections and test mocking.
        check_active: Host authorization checkpoint, called before each request and
            before each row is published.
    """
    import dlt

    if getattr(dlt_utils, "DOCUMENT_SYNC_VERSION", 0) < 3:
        raise RuntimeError(
            "Packagist sync requires a Cognee build that reads per-row node sets and "
            "structure (DOCUMENT_SYNC_VERSION >= 3). Upgrade Cognee before syncing."
        )
    raw = [package_names] if isinstance(package_names, str) else list(package_names)
    names = []
    for name in raw:
        if not isinstance(name, str) or not name.strip():
            raise ValueError(
                "package_names must be a non-empty 'vendor/package' string or a list of them."
            )
        names.append("/".join(part.strip() for part in _split_name(name)))
    if not names:
        raise ValueError(
            "package_names must be a non-empty 'vendor/package' string or a list of them."
        )
    if resource_name is None:
        resource_name = "packagist_packages"
    elif not isinstance(resource_name, str) or not resource_name.strip():
        raise ValueError("resource_name must be a non-empty string.")
    seen: set[str] = set()
    selection = []
    for qualified in names:
        if qualified not in seen:
            seen.add(qualified)
            selection.append(qualified)
    client = PackagistClient(session=session)
    stats: dict[str, int] = {}

    @dlt.resource(
        name=resource_name,
        primary_key="id",
        write_disposition="merge",
        columns={"_deleted": {"data_type": "bool", "hard_delete": True}},
    )
    def packagist_packages():
        stats.update(synced=0, deleted=0)
        rows = _package_rows(client, selection, dlt.current.resource_state(), stats, check_active)
        yield from dlt_utils.guarded_rows(rows, check_active)

    resource = packagist_packages()
    setattr(resource, DOCUMENT_SOURCE_ATTR, "packagist")
    setattr(resource, PIPELINE_SCOPE_ATTR, resource_name)
    resource.cognee_sync_stats = stats
    return resource
