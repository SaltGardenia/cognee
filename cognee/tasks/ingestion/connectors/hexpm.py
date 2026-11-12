"""Hex.pm connector for cognee: BEAM package metadata and release notices as documents.

Elixir, Phoenix and Erlang teams track their dependencies on Hex.pm, and an assistant
that cannot read it cannot answer "have any of our Hex packages had releases retired for
security reasons, and what OTP/Elixir requirements does the latest version need?". This
source queries the public, zero-authentication Hex.pm API and publishes one document per
package — description, licenses, links, downloads, the latest release's dependency
requirements, and the retirement and security-advisory notices. See ``hexpm.md``.
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

logger = get_logger("hexpm_connector")

HEX_API_URL = "https://hex.pm/api/packages/{name}"
_TIMEOUT = (10, 30)


class HexError(Exception):
    """Base error for Hex.pm connector errors."""


class HexPackageNotFoundError(HexError):
    """The package does not exist on Hex.pm (HTTP 404)."""


class HexRateLimitError(HexError):
    """Hex.pm rate-limited the request (HTTP 429)."""


class HexClient:
    """HTTP client for the Hex.pm API. ``session`` is injectable for tests."""

    def __init__(self, session: Any = None):
        if session is None:
            import requests

            session = requests.Session()
        self._session = session

    def _get(self, url: str) -> Any:
        response = self._session.get(
            url,
            headers={"User-Agent": "cognee-hexpm-connector/1.0", "Accept": "application/json"},
            timeout=_TIMEOUT,
        )
        if response.status_code == 404:
            raise HexPackageNotFoundError(f"Nothing lives at {url}.")
        if response.status_code == 429:
            raise HexRateLimitError("Hex.pm rate limit exceeded; retry later.")
        response.raise_for_status()
        try:
            return response.json()
        except (json.JSONDecodeError, ValueError) as error:
            raise HexError(f"Hex.pm returned a non-JSON payload at {url}.") from error

    def get_package(self, name: str) -> dict[str, Any]:
        """The metadata payload of one package, releases and notices included."""
        payload = self._get(HEX_API_URL.format(name=name.strip()))
        if not isinstance(payload, dict) or not payload.get("name"):
            raise HexError(f"Hex.pm returned a payload without package metadata for {name!r}.")
        return payload

    def get_release(self, package_name: str, version: str) -> dict[str, Any]:
        """One release's detail payload: requirements, retirement, publisher."""
        url = HEX_API_URL.format(name=package_name.strip()) + f"/releases/{version.strip()}"
        payload = self._get(url)
        if not isinstance(payload, dict) or not payload.get("version"):
            raise HexError(f"Hex.pm returned no release detail for {package_name}@{version}.")
        return payload


def _requirements_lines(requirements: Any) -> list[str]:
    """One `- dep ~> 1.0` line per requirement, optional ones marked."""
    if not isinstance(requirements, dict):
        return []
    lines = []
    for dep, spec in sorted(requirements.items()):
        if not isinstance(spec, dict):
            continue
        constraint = spec.get("requirement")
        optional = " (optional)" if spec.get("optional") else ""
        lines.append(f"- {dep}{f' {constraint}' if constraint else ''}{optional}")
    return lines


def _retirement_lines(retirements: Any) -> list[str]:
    """One line per retired release: version, reason, message."""
    if not isinstance(retirements, list):
        return []
    lines = []
    for retirement in retirements:
        if not isinstance(retirement, dict):
            continue
        version = retirement.get("version")
        reason = retirement.get("reason")
        message = retirement.get("message")
        line = f"- {version}" if version else "-"
        if reason:
            line += f" retired ({reason})"
        if message:
            line += f": {message}"
        lines.append(line)
    return lines


def _advisory_lines(advisories: Any) -> list[str]:
    """One line per security advisory, with its id and title."""
    if not isinstance(advisories, list):
        return []
    lines = []
    for advisory in advisories:
        if not isinstance(advisory, dict):
            continue
        title = advisory.get("title") or advisory.get("summary")
        identifier = advisory.get("id") or advisory.get("cve_id") or ""
        if title:
            lines.append(f"- {identifier}: {title}".rstrip(": "))
    return lines


def format_hexpm_document(
    package: dict[str, Any], release_detail: dict[str, Any] | None
) -> dict[str, Any]:
    """The package's metadata, latest-release requirements and notices as one document row."""
    name = str(package["name"])
    lines = [f"# {name}"]
    description = (package.get("meta") or {}).get("description")
    if description:
        lines.append(f"Description: {description}")
    meta_licenses = (package.get("meta") or {}).get("licenses")
    if isinstance(meta_licenses, list) and meta_licenses:
        lines.append(f"License: {', '.join(str(item) for item in meta_licenses)}")
    for label, url in ((package.get("meta") or {}).get("links") or {}).items():
        if label and url:
            lines.append(f"{label}: {url}")
    downloads = package.get("downloads")
    if isinstance(downloads, dict) and downloads.get("all") is not None:
        lines.append(f"Downloads: {downloads['all']:,}")
    latest_stable = package.get("latest_stable_version") or package.get("latest_version")
    if latest_stable:
        lines.append(f"Latest stable version: {latest_stable}")
    repository = package.get("repository")
    if repository:
        lines.append(f"Organization: {repository}")

    if release_detail is not None:
        requirement_lines = _requirements_lines(release_detail.get("requirements"))
        if requirement_lines:
            lines.append("\n## Latest release requirements")
            lines.extend(requirement_lines)
        retirement_lines = _retirement_lines(release_detail.get("retirement") or [])
        # A single release's retirement is an object, not a list.
        if not retirement_lines and release_detail.get("retirement"):
            retirement = release_detail["retirement"]
            retirement_lines = _retirement_lines(
                [retirement] if isinstance(retirement, dict) else []
            )
        if retirement_lines:
            lines.append("\n## Retirements")
            lines.extend(retirement_lines)

    advisory_lines = _advisory_lines(package.get("security_advisories"))
    if advisory_lines:
        lines.append("\n## Security advisories")
        lines.extend(advisory_lines)

    return {
        "id": f"hexpm:package:{name}",
        "title": f"Hex.pm: {name}",
        "content": "\n".join(lines).strip(),
        "url": package.get("html_url") or f"https://hex.pm/packages/{name}",
        "_deleted": False,
        NODE_SET_COLUMN: "hexpm:package",
    }


def _package_rows(
    client: HexClient,
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
    for name in names:
        if check_active is not None:
            check_active()  # outside any try: a revoked connection is not a failed fetch
        package = client.get_package(name)
        latest_stable = package.get("latest_stable_version") or package.get("latest_version")
        release_detail = None
        if latest_stable:
            # The requirements and the release-level retirement live on the release.
            release_detail = client.get_release(name, str(latest_stable))
        row = format_hexpm_document(package, release_detail)
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
    logger.info("Hex.pm: ingested %d package(s), forgot %d.", stats["synced"], stats["deleted"])


def hexpm_source(
    *,
    package_names: str | list[str],
    resource_name: str | None = None,
    session: Any = None,
    check_active: Callable[[], None] | None = None,
):
    """Return a ``dlt`` resource that yields one document per package of the selection.

    Hand the result to ``cognee.remember(...)`` with ``write_disposition="merge"`` and
    ``primary_key="id"``. A package's id is its name, so re-syncing after a new release
    updates the document in place. A package that does not exist raises
    ``HexPackageNotFoundError`` — an explicit name that misses is a caller error, never
    silently skipped. A package removed from the selection is forgotten on the next
    complete run; read ``cognee_sync_stats`` (``synced``, ``deleted``) afterwards.

    Args:
        package_names: One package name, or a list of them (duplicates are ingested once).
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
            "Hex.pm sync requires a Cognee build that reads per-row node sets and "
            "structure (DOCUMENT_SYNC_VERSION >= 3). Upgrade Cognee before syncing."
        )
    names = [package_names] if isinstance(package_names, str) else list(package_names)
    names = [name.strip() for name in names if isinstance(name, str)]
    if not names or not all(names):
        raise ValueError("package_names must be a non-empty string or a list of them.")
    if resource_name is None:
        resource_name = "hexpm_packages"
    elif not isinstance(resource_name, str) or not resource_name.strip():
        raise ValueError("resource_name must be a non-empty string.")
    seen: set[str] = set()
    selection = []
    for name in names:
        if name not in seen:
            seen.add(name)
            selection.append(name)
    client = HexClient(session=session)
    stats: dict[str, int] = {}

    @dlt.resource(
        name=resource_name,
        primary_key="id",
        write_disposition="merge",
        columns={"_deleted": {"data_type": "bool", "hard_delete": True}},
    )
    def hexpm_packages():
        stats.update(synced=0, deleted=0)
        rows = _package_rows(client, selection, dlt.current.resource_state(), stats, check_active)
        yield from dlt_utils.guarded_rows(rows, check_active)

    resource = hexpm_packages()
    setattr(resource, DOCUMENT_SOURCE_ATTR, "hexpm")
    setattr(resource, PIPELINE_SCOPE_ATTR, resource_name)
    resource.cognee_sync_stats = stats
    return resource
