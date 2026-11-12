"""PyPI connector for cognee: Python package metadata and documentation as documents.

A coding assistant needs structured context about the libraries a codebase depends on —
summary, README, license, project URLs — and today a developer scrapes or copy-pastes it.
This source queries the official, zero-authentication PyPI JSON API and publishes one
document per package, so ``recall("how does fastapi handle dependencies")`` answers from
the real documentation. See ``pypi.md``.
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

logger = get_logger("pypi_connector")

PYPI_API_URL = "https://pypi.org/pypi/{package}/json"
_TIMEOUT = (10, 30)


class PyPIError(Exception):
    """Base error for PyPI connector errors."""


class PyPIPackageNotFoundError(PyPIError):
    """The package does not exist on PyPI (HTTP 404)."""


class PyPIRateLimitError(PyPIError):
    """PyPI rate-limited the request (HTTP 429)."""


class PyPIClient:
    """HTTP client for the PyPI JSON API. ``session`` is injectable for tests."""

    def __init__(self, session: Any = None):
        if session is None:
            import requests

            session = requests.Session()
        self._session = session

    def get_package(self, package_name: str) -> dict[str, Any]:
        """The full JSON payload of one package: ``info``, ``releases`` and ``urls``."""
        url = PYPI_API_URL.format(package=package_name.strip())
        response = self._session.get(
            url,
            headers={"User-Agent": "cognee-pypi-connector/1.0", "Accept": "application/json"},
            timeout=_TIMEOUT,
        )
        if response.status_code == 404:
            raise PyPIPackageNotFoundError(f"Package {package_name!r} does not exist on PyPI.")
        if response.status_code == 429:
            raise PyPIRateLimitError("PyPI rate limit exceeded; retry later.")
        response.raise_for_status()
        try:
            payload = response.json()
        except (json.JSONDecodeError, ValueError) as error:
            raise PyPIError(f"PyPI returned a non-JSON payload for {package_name!r}.") from error
        if not isinstance(payload, dict) or not isinstance(payload.get("info"), dict):
            raise PyPIError(f"PyPI returned a payload without an info object for {package_name!r}.")
        return payload


def format_pypi_document(payload: dict[str, Any]) -> dict[str, Any]:
    """One PyPI payload as a document row: metadata lines and the full README."""
    info = payload.get("info") or {}
    name = str(info.get("name") or "").strip()
    if not name:
        raise PyPIError("The payload's info carries no package name.")

    lines = [f"# {name}"]
    version = info.get("version")
    if version:
        lines.append(f"Version: {version}")
    summary = info.get("summary")
    if summary:
        lines.append(f"Summary: {summary}")
    author = info.get("author") or info.get("maintainer")
    if author:
        lines.append(f"Author: {author}")
    email = info.get("author_email") or info.get("maintainer_email")
    if email:
        lines.append(f"Email: {email}")
    license_field = info.get("license") or (
        info.get("license_expression") if isinstance(info.get("license_expression"), str) else None
    )
    if license_field:
        lines.append(f"License: {license_field}")
    requires_python = info.get("requires_python")
    if requires_python:
        lines.append(f"Requires Python: {requires_python}")
    keywords = info.get("keywords")
    if keywords:
        rendered = ", ".join(keywords) if isinstance(keywords, list) else str(keywords)
        lines.append(f"Keywords: {rendered}")
    project_urls = info.get("project_urls") or {}
    if isinstance(project_urls, dict):
        for label, url in project_urls.items():
            if label and url:
                lines.append(f"{label}: {url}")
    home_page = info.get("home_page")
    if home_page:
        lines.append(f"Home page: {home_page}")

    description = info.get("description")
    if description:
        lines.append("")
        lines.append(description)

    return {
        "id": f"pypi:package:{name}",
        "title": f"PyPI: {name}",
        "content": "\n".join(lines).strip(),
        "url": (project_urls or {}).get("Homepage")
        or home_page
        or f"https://pypi.org/project/{name}/",
        "_deleted": False,
        NODE_SET_COLUMN: "pypi:package",
    }


def _package_rows(
    client: PyPIClient,
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
        payload = client.get_package(name)
        row = format_pypi_document(payload)
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
    logger.info("PyPI: ingested %d package(s), forgot %d.", stats["synced"], stats["deleted"])


def pypi_source(
    *,
    package_name: str | list[str],
    resource_name: str | None = None,
    session: Any = None,
    check_active: Callable[[], None] | None = None,
):
    """Return a ``dlt`` resource that yields one document per package of the selection.

    Hand the result to ``cognee.remember(...)`` with ``write_disposition="merge"`` and
    ``primary_key="id"``. A package's id is its name, so re-syncing a package after a new
    release updates the document in place instead of duplicating it. A package that does
    not exist raises ``PyPIPackageNotFoundError`` — an explicit name that misses is a
    caller error, never silently skipped. A package removed from the selection is
    forgotten on the next complete run; read ``cognee_sync_stats`` (``synced``,
    ``deleted``) afterwards.

    Args:
        package_name: One package name, or a list of them (duplicates are ingested once).
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
            "PyPI sync requires a Cognee build that reads per-row node sets and "
            "structure (DOCUMENT_SYNC_VERSION >= 3). Upgrade Cognee before syncing."
        )
    names = [package_name] if isinstance(package_name, str) else list(package_name)
    names = [name.strip() for name in names if isinstance(name, str)]
    if not names or not all(names):
        raise ValueError("package_name must be a non-empty string or a list of them.")
    if resource_name is None:
        resource_name = "pypi_packages"
    elif not isinstance(resource_name, str) or not resource_name.strip():
        raise ValueError("resource_name must be a non-empty string.")
    seen: set[str] = set()
    selection = []
    for name in names:
        if name not in seen:
            seen.add(name)
            selection.append(name)
    client = PyPIClient(session=session)
    stats: dict[str, int] = {}

    @dlt.resource(
        name=resource_name,
        primary_key="id",
        write_disposition="merge",
        columns={"_deleted": {"data_type": "bool", "hard_delete": True}},
    )
    def pypi_packages():
        stats.update(synced=0, deleted=0)
        rows = _package_rows(client, selection, dlt.current.resource_state(), stats, check_active)
        yield from dlt_utils.guarded_rows(rows, check_active)

    resource = pypi_packages()
    setattr(resource, DOCUMENT_SOURCE_ATTR, "pypi")
    setattr(resource, PIPELINE_SCOPE_ATTR, resource_name)
    resource.cognee_sync_stats = stats
    return resource
