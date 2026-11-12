"""JSR connector for cognee: TypeScript package metadata from the modern registry as documents.

JSR (jsr.io) is the open package registry for TypeScript across Deno, Node.js, Bun and
Cloudflare Workers. This source queries the public, zero-authentication JSR API and
publishes one document per package — description, runtime compatibility, quality score,
dependency counts and its version history — so an agent can answer "which of our JSR
packages are not Node-compatible?" from memory. See ``jsr.md``.
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

logger = get_logger("jsr_connector")

JSR_PACKAGE_URL = "https://api.jsr.io/scopes/{scope}/packages/{name}"
JSR_VERSIONS_URL = "https://api.jsr.io/scopes/{scope}/packages/{name}/versions"
_TIMEOUT = (10, 30)


class JSRError(Exception):
    """Base error for JSR connector errors."""


class JSRPackageNotFoundError(JSRError):
    """The package does not exist on JSR (HTTP 404)."""


class JSRRateLimitError(JSRError):
    """JSR rate-limited the request (HTTP 429)."""


class JSRClient:
    """HTTP client for the JSR API. ``session`` is injectable for tests."""

    def __init__(self, session: Any = None):
        if session is None:
            import requests

            session = requests.Session()
        self._session = session

    def _get(self, url: str) -> Any:
        response = self._session.get(
            url,
            headers={"User-Agent": "cognee-jsr-connector/1.0", "Accept": "application/json"},
            timeout=_TIMEOUT,
        )
        if response.status_code == 404:
            raise JSRPackageNotFoundError(f"Nothing lives at {url}.")
        if response.status_code == 429:
            raise JSRRateLimitError("JSR rate limit exceeded; retry later.")
        response.raise_for_status()
        try:
            return response.json()
        except (json.JSONDecodeError, ValueError) as error:
            raise JSRError(f"JSR returned a non-JSON payload at {url}.") from error

    def get_package(self, scope: str, name: str) -> dict[str, Any]:
        """The metadata payload of one ``@scope/name`` package."""
        payload = self._get(JSR_PACKAGE_URL.format(scope=scope.strip(), name=name.strip()))
        if not isinstance(payload, dict) or not payload.get("name"):
            raise JSRError(f"JSR returned a payload without package metadata for @{scope}/{name}.")
        return payload

    def get_versions(self, scope: str, name: str) -> list[dict[str, Any]]:
        """The package's versions, newest first."""
        payload = self._get(JSR_VERSIONS_URL.format(scope=scope.strip(), name=name.strip()))
        items = payload.get("items") if isinstance(payload, dict) else None
        if not isinstance(items, list):
            raise JSRError(f"JSR returned no version list for @{scope}/{name}.")
        return [item for item in items if isinstance(item, dict)]


def _split_name(qualified: str) -> tuple[str, str]:
    text = qualified.strip()
    if not text.startswith("@") or "/" not in text:
        raise ValueError(
            f"Package name {qualified!r} must be an '@scope/name' pair, like '@std/path'."
        )
    scope, _, name = text[1:].partition("/")
    if not scope or not name:
        raise ValueError(
            f"Package name {qualified!r} must be an '@scope/name' pair, like '@std/path'."
        )
    return scope, name


def _compatibility_lines(runtime_compat: Any) -> list[str]:
    """One `yes`/`no` line per runtime the report names."""
    if not isinstance(runtime_compat, dict):
        return []
    lines = []
    for runtime, compatible in runtime_compat.items():
        if isinstance(compatible, bool):
            lines.append(f"- {runtime}: {'yes' if compatible else 'no'}")
    return lines


def format_jsr_document(
    package: dict[str, Any], versions: list[dict[str, Any]], qualified: str
) -> dict[str, Any]:
    """The package's metadata and version history as one document row."""
    lines = [f"# {qualified}"]
    description = package.get("description")
    if description:
        lines.append(f"Description: {description}")
    compatibility = _compatibility_lines(package.get("runtimeCompat"))
    if compatibility:
        lines.append("\n## Runtime compatibility")
        lines.extend(compatibility)
    score = package.get("score")
    if isinstance(score, (int, float)):
        lines.append(f"\nQuality score: {score}/100")
    counts = []
    for label, key in (
        ("versions", "versionCount"),
        ("dependencies", "dependencyCount"),
        ("dependents", "dependentCount"),
    ):
        value = package.get(key)
        if isinstance(value, (int, float)):
            counts.append(f"{int(value)} {label}")
    if counts:
        lines.append(", ".join(counts))
    if package.get("isArchived"):
        lines.append("This package is archived.")
    repository = package.get("githubRepository")
    if isinstance(repository, dict) and repository.get("owner") and repository.get("name"):
        lines.append(f"Repository: https://github.com/{repository['owner']}/{repository['name']}")

    if versions:
        lines.append("\n## Versions")
        for version in versions:
            entry = [str(version.get("version"))]
            user = version.get("user") or {}
            if isinstance(user, dict) and user.get("name"):
                entry.append(f"by {user['name']}")
            published = version.get("createdAt")
            if isinstance(published, str) and published:
                entry.append(f"on {published[:10]}")
            lines.append(f"- {' '.join(entry)}")

    latest = package.get("latestVersion")
    return {
        "id": f"jsr:package:{qualified}",
        "title": f"JSR: {qualified}",
        "content": "\n".join(lines).strip(),
        "url": f"https://jsr.io/{qualified}",
        "_deleted": False,
        NODE_SET_COLUMN: "jsr:package",
        "cognee_source_label": f"latest {latest}" if latest else None,
    }


def _package_rows(
    client: JSRClient,
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
    for scope, package, qualified in names:
        if check_active is not None:
            check_active()  # outside any try: a revoked connection is not a failed fetch
        package_payload = client.get_package(scope, package)
        versions = client.get_versions(scope, package)
        row = format_jsr_document(package_payload, versions, qualified)
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
    logger.info("JSR: ingested %d package(s), forgot %d.", stats["synced"], stats["deleted"])


def jsr_source(
    *,
    package_names: str | list[str],
    resource_name: str | None = None,
    session: Any = None,
    check_active: Callable[[], None] | None = None,
):
    """Return a ``dlt`` resource that yields one document per ``@scope/name`` package.

    Hand the result to ``cognee.remember(...)`` with ``write_disposition="merge"`` and
    ``primary_key="id"``. A package's id is its qualified name, so re-syncing after a new
    release updates the document in place. A package that does not exist raises
    ``JSRPackageNotFoundError`` — an explicit name that misses is a caller error, never
    silently skipped. A package removed from the selection is forgotten on the next
    complete run; read ``cognee_sync_stats`` (``synced``, ``deleted``) afterwards.

    Args:
        package_names: One ``@scope/name`` pair, or a list of them (duplicates are
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
            "JSR sync requires a Cognee build that reads per-row node sets and "
            "structure (DOCUMENT_SYNC_VERSION >= 3). Upgrade Cognee before syncing."
        )
    raw = [package_names] if isinstance(package_names, str) else list(package_names)
    names = []
    for name in raw:
        if not isinstance(name, str) or not name.strip():
            raise ValueError(
                "package_names must be a non-empty '@scope/name' string or a list of them."
            )
        scope, package = _split_name(name)
        names.append((scope, package, f"@{scope}/{package}"))
    if not names:
        raise ValueError(
            "package_names must be a non-empty '@scope/name' string or a list of them."
        )
    if resource_name is None:
        resource_name = "jsr_packages"
    elif not isinstance(resource_name, str) or not resource_name.strip():
        raise ValueError("resource_name must be a non-empty string.")
    seen: set[str] = set()
    selection = []
    for parsed in names:
        if parsed[2] not in seen:
            seen.add(parsed[2])
            selection.append(parsed)
    client = JSRClient(session=session)
    stats: dict[str, int] = {}

    @dlt.resource(
        name=resource_name,
        primary_key="id",
        write_disposition="merge",
        columns={"_deleted": {"data_type": "bool", "hard_delete": True}},
    )
    def jsr_packages():
        stats.update(synced=0, deleted=0)
        rows = _package_rows(client, selection, dlt.current.resource_state(), stats, check_active)
        yield from dlt_utils.guarded_rows(rows, check_active)

    resource = jsr_packages()
    setattr(resource, DOCUMENT_SOURCE_ATTR, "jsr")
    setattr(resource, PIPELINE_SCOPE_ATTR, resource_name)
    resource.cognee_sync_stats = stats
    return resource
