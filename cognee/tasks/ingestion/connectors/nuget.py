"""NuGet connector for cognee: .NET package metadata and notices as documents.

Teams building .NET and C# services track their dependencies on NuGet, and an assistant
that cannot read it cannot answer "which of our NuGet packages are deprecated upstream,
what replacement do their authors recommend, and do any of our versions carry known
vulnerability advisories?". This source talks to a NuGet v3 feed — the public
api.nuget.org one by default, a private one given a service index URL — and publishes one
document per package: description, authors, license, target-framework dependency groups,
deprecation notices with their recommended replacements, and the vulnerability advisories
of the listed versions. See ``nuget.md``.
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

logger = get_logger("nuget_connector")

NUGET_SERVICE_INDEX_URL = "https://api.nuget.org/v3/index.json"
_TIMEOUT = (10, 30)


class NuGetError(Exception):
    """Base error for NuGet connector errors."""


class NuGetPackageNotFoundError(NuGetError):
    """The package does not exist on the feed (HTTP 404 or an empty registration)."""


class NuGetRateLimitError(NuGetError):
    """The feed rate-limited the request (HTTP 429)."""


class NuGetFetchError(NuGetError):
    """The feed could not be reached (network failure)."""


class NuGetClient:
    """HTTP client for a NuGet v3 feed. ``session`` is injectable for tests.

    The registration base URL is resolved from the feed's service index
    (``RegistrationsBaseUrl/Versioned`` when offered, else ``RegistrationsBaseUrl``), so
    private feeds (GitHub Packages, Azure Artifacts, BaGet) work by pointing
    ``service_index_url`` at them.
    """

    def __init__(self, session: Any = None, service_index_url: str = NUGET_SERVICE_INDEX_URL):
        if session is None:
            import requests

            session = requests.Session()
        self._session = session
        self._service_index_url = service_index_url
        self._registration_base: str | None = None

    def _get_json(self, url: str) -> Any:
        try:
            response = self._session.get(
                url,
                headers={"User-Agent": "cognee-nuget-connector/1.0", "Accept": "application/json"},
                timeout=_TIMEOUT,
            )
            if response.status_code == 404:
                raise NuGetPackageNotFoundError(f"Nothing lives at {url}.")
            if response.status_code == 429:
                raise NuGetRateLimitError("NuGet rate limit exceeded; retry later.")
            if response.status_code >= 400:
                raise NuGetError(f"NuGet returned HTTP {response.status_code} for {url}.")
            return response.json()
        except NuGetError:
            raise
        except (json.JSONDecodeError, ValueError) as error:
            raise NuGetError(f"NuGet returned a non-JSON payload at {url}.") from error
        except Exception as error:
            raise NuGetFetchError(f"Fetching {url} failed: {error}") from error

    def registration_base(self) -> str:
        """The feed's registration base URL, resolved once from the service index."""
        if self._registration_base is None:
            index = self._get_json(self._service_index_url)
            resources = index.get("resources") if isinstance(index, dict) else None
            if not isinstance(resources, list):
                raise NuGetError(
                    f"The service index at {self._service_index_url} carries no resources."
                )
            base = None
            fallback = None
            for resource in resources:
                if not isinstance(resource, dict):
                    continue
                type_field = resource.get("@type") or ""
                url = resource.get("@id")
                if not url:
                    continue
                if type_field == "RegistrationsBaseUrl/Versioned":
                    base = url
                    break
                if (
                    type_field.startswith("RegistrationsBaseUrl/")
                    or type_field == "RegistrationsBaseUrl"
                ):
                    fallback = fallback or url
            base = base or fallback
            if not base:
                raise NuGetError(
                    f"The service index at {self._service_index_url} carries no "
                    "RegistrationsBaseUrl resource."
                )
            self._registration_base = base if base.endswith("/") else base + "/"
        return self._registration_base

    def get_registration(self, package_id: str) -> dict[str, Any]:
        """The registration index payload of one package: its pages and leaves."""
        url = self.registration_base() + package_id.strip().lower() + "/index.json"
        payload = self._get_json(url)
        if not isinstance(payload, dict) or not isinstance(payload.get("items"), list):
            raise NuGetPackageNotFoundError(f"NuGet returned no registration for {package_id!r}.")
        return payload

    def get_page(self, url: str) -> dict[str, Any]:
        """One externalized registration page payload."""
        payload = self._get_json(url)
        if not isinstance(payload, dict) or not isinstance(payload.get("items"), list):
            raise NuGetError(f"The registration page at {url} carries no items.")
        return payload

    def search(self, query: str) -> list[str]:
        """The package ids matching a search query, via the feed's SearchQueryService."""
        index = self._get_json(self._service_index_url)
        resources = index.get("resources") if isinstance(index, dict) else None
        search_url = None
        for resource in resources or []:
            if isinstance(resource, dict) and resource.get("@type") == "SearchQueryService":
                search_url = resource.get("@id")
                break
        if not search_url:
            raise NuGetError(
                f"The service index at {self._service_index_url} carries no SearchQueryService."
            )
        separator = "&" if "?" in search_url else "?"
        payload = self._get_json(f"{search_url}{separator}q={query.strip()}&take=20")
        data = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(data, list):
            raise NuGetError(f"The search response of {search_url} carries no data.")
        return [str(hit["id"]) for hit in data if isinstance(hit, dict) and hit.get("id")]


def _leaves_of(registration: dict[str, Any], client: NuGetClient) -> list[dict[str, Any]]:
    """The registration's leaves: inline ``items`` or, when externalized, the page payload."""
    leaves: list[dict[str, Any]] = []
    for page in registration["items"]:
        if not isinstance(page, dict):
            continue
        if isinstance(page.get("items"), list):
            leaves.extend(item for item in page["items"] if isinstance(item, dict))
            continue
        page_url = page.get("@id")
        if not isinstance(page_url, str):
            continue
        fetched = client.get_page(page_url)
        leaves.extend(item for item in fetched["items"] if isinstance(item, dict))
    return leaves


def _listed_entries(leaves: list[dict[str, Any]], package_id: str) -> list[dict[str, Any]]:
    """The catalog entries of the listed leaves, in registration (ascending) order.

    A leaf carries ``listed`` (``False`` marks an unlisted version) and its
    ``catalogEntry`` inline or as a URL; the registration order is the version order,
    so the last listed entry is the latest.
    """
    entries: list[dict[str, Any]] = []
    for leaf in leaves:
        if leaf.get("listed") is False:
            continue
        entry = leaf.get("catalogEntry")
        if isinstance(entry, str):
            raise NuGetError(
                f"A catalog leaf of {package_id} externalizes its entry to {entry}; "
                "catalog leaf URLs are not followed in this version."
            )
        if isinstance(entry, dict):
            entries.append(entry)
    return entries


def _deprecation_lines(entry: dict[str, Any]) -> list[str]:
    """The deprecation notice of a version: reasons, message, recommended replacement."""
    deprecation = entry.get("deprecation")
    if not isinstance(deprecation, dict):
        return []
    reasons = deprecation.get("reasons")
    rendered = ", ".join(str(r) for r in reasons) if isinstance(reasons, list) else None
    line = "- Deprecated" + (f" ({rendered})" if rendered else "")
    if deprecation.get("message"):
        line += f": {deprecation['message']}"
    lines = [line]
    alternate = deprecation.get("alternatePackage")
    if isinstance(alternate, dict) and alternate.get("id"):
        range_ = alternate.get("range")
        lines.append(f"  - Replacement: {alternate['id']}" + (f" ({range_})" if range_ else ""))
    return lines


def _dependency_lines(entry: dict[str, Any]) -> list[str]:
    """The target-framework dependency groups of a version."""
    groups = entry.get("dependencyGroups")
    if not isinstance(groups, list):
        return []
    lines: list[str] = []
    for group in groups:
        if not isinstance(group, dict):
            continue
        framework = group.get("targetFramework")
        header = f"  - {framework}" if framework else "  - (any framework)"
        dependencies = group.get("dependencies")
        if isinstance(dependencies, list) and dependencies:
            lines.append(header)
            for dependency in dependencies:
                if not isinstance(dependency, dict):
                    continue
                spec = f" {dependency['range']}" if dependency.get("range") else ""
                lines.append(f"    - {dependency.get('id')}{spec}")
        elif framework:
            lines.append(f"{header}: no dependencies")
    return lines


def _vulnerability_lines(leaves: list[dict[str, Any]]) -> list[str]:
    """One line per known vulnerability of the listed leaves: version, advisory, severity."""
    lines: list[str] = []
    for leaf in leaves:
        if leaf.get("listed") is False:
            continue
        entry = leaf.get("catalogEntry")
        if not isinstance(entry, dict):
            continue
        vulnerabilities = entry.get("vulnerabilities")
        if not isinstance(vulnerabilities, list):
            continue
        for vulnerability in vulnerabilities:
            if not isinstance(vulnerability, dict):
                continue
            advisory = vulnerability.get("advisoryUrl") or ""
            severity = vulnerability.get("severity")
            rendered = f" (severity {severity})" if severity is not None else ""
            lines.append(f"- {entry.get('version')}: {advisory}{rendered}".rstrip())
    return lines


def format_nuget_document(package_id: str, leaves: list[dict[str, Any]]) -> dict[str, Any]:
    """One package's metadata, latest-version details and notices as a document row."""
    entries = _listed_entries(leaves, package_id)
    if not entries:
        raise NuGetPackageNotFoundError(
            f"The registration of {package_id!r} lists no published versions."
        )
    latest = entries[-1]
    lines = [f"# {package_id}"]
    if latest.get("description"):
        lines.append(f"Description: {latest['description']}")
    authors = latest.get("authors")
    if authors:
        lines.append(f"Authors: {authors}")
    if latest.get("licenseExpression"):
        lines.append(f"License: {latest['licenseExpression']}")
    elif latest.get("licenseUrl"):
        lines.append(f"License: {latest['licenseUrl']}")
    if latest.get("version"):
        lines.append(f"Latest listed version: {latest['version']}")
    if latest.get("projectUrl"):
        lines.append(f"Project URL: {latest['projectUrl']}")

    dependency_lines = _dependency_lines(latest)
    if dependency_lines:
        lines.append("\n## Dependencies (latest listed version)")
        lines.extend(dependency_lines)
    deprecation_lines = _deprecation_lines(latest)
    if deprecation_lines:
        lines.append("\n## Deprecation (latest listed version)")
        lines.extend(deprecation_lines)
    vulnerability_lines = _vulnerability_lines(leaves)
    if vulnerability_lines:
        lines.append("\n## Vulnerabilities")
        lines.extend(vulnerability_lines)

    return {
        "id": f"nuget:package:{package_id}",
        "title": f"NuGet: {package_id}",
        "content": "\n".join(lines).strip(),
        "url": latest.get("projectUrl") or f"https://www.nuget.org/packages/{package_id}/",
        "_deleted": False,
        NODE_SET_COLUMN: "nuget:package",
    }


def _package_rows(
    client: NuGetClient,
    package_ids: list[str],
    state: dict,
    stats: dict[str, int],
    check_active: Callable[[], None] | None = None,
) -> Iterator[dict[str, Any]]:
    """Fetch and render every package before the first row is yielded, then tombstones.

    A package that cannot be fetched stops the run with nothing published and nothing
    forgotten: a partial snapshot must not look like a deletion. A package removed from
    the selection since the last complete run is forgotten; the state is written after
    the last row, so a run that raises keeps the old one. A package whose registration
    did not change renders the same row, so re-syncing costs no re-cognition.
    """
    previous = set(state.get("members", []))
    rows: list[dict[str, Any]] = []
    present: set[str] = set()
    for package_id in package_ids:
        if check_active is not None:
            check_active()  # outside any try: a revoked connection is not a failed fetch
        registration = client.get_registration(package_id)
        leaves = _leaves_of(registration, client)
        row = format_nuget_document(package_id, leaves)
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
    logger.info("NuGet: ingested %d package(s), forgot %d.", stats["synced"], stats["deleted"])


def nuget_source(
    *,
    package_ids: str | list[str],
    search_query: str | None = None,
    service_index_url: str = NUGET_SERVICE_INDEX_URL,
    resource_name: str | None = None,
    session: Any = None,
    check_active: Callable[[], None] | None = None,
):
    """Return a ``dlt`` resource that yields one document per NuGet package.

    Hand the result to ``cognee.remember(...)`` with ``write_disposition="merge"`` and
    ``primary_key="id"``. A package's id is its registry id, so re-syncing after a new
    release updates the document in place. A package that does not exist raises
    ``NuGetPackageNotFoundError`` — an explicit id that misses is a caller error, never
    silently skipped. A package removed from the selection is forgotten on the next
    complete run; read ``cognee_sync_stats`` (``synced``, ``deleted``) afterwards.

    Args:
        package_ids: One package id, or a list of them (duplicates are ingested once).
        search_query: Optional search filter; the matching package ids (via the feed's
            SearchQueryService) are ingested alongside the explicit ``package_ids``.
        service_index_url: The feed's v3 service index. Defaults to api.nuget.org; point
            it at a private feed's ``index.json`` (GitHub Packages, Azure Artifacts,
            BaGet) to sync from that feed instead.
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
            "NuGet sync requires a Cognee build that reads per-row node sets and "
            "structure (DOCUMENT_SYNC_VERSION >= 3). Upgrade Cognee before syncing."
        )
    raw = [package_ids] if isinstance(package_ids, str) else list(package_ids)
    ids = []
    for package_id in raw:
        if not isinstance(package_id, str) or not package_id.strip():
            raise ValueError("package_ids must be a non-empty string or a list of them.")
        ids.append(package_id.strip())
    if not ids:
        raise ValueError("package_ids must be a non-empty string or a list of them.")
    if search_query is not None and not isinstance(search_query, str):
        raise ValueError("search_query must be a string when given.")
    if resource_name is None:
        resource_name = "nuget_packages"
    elif not isinstance(resource_name, str) or not resource_name.strip():
        raise ValueError("resource_name must be a non-empty string.")
    # NuGet ids are case-insensitive: dedup on the lowercase id, keep the caller's casing.
    seen: set[str] = set()
    selection: list[str] = []
    for package_id in ids:
        if package_id.lower() not in seen:
            seen.add(package_id.lower())
            selection.append(package_id)
    client = NuGetClient(session=session, service_index_url=service_index_url)
    if search_query is not None and search_query.strip():
        for package_id in client.search(search_query):
            if package_id.lower() not in seen:
                seen.add(package_id.lower())
                selection.append(package_id)
    stats: dict[str, int] = {}

    @dlt.resource(
        name=resource_name,
        primary_key="id",
        write_disposition="merge",
        columns={"_deleted": {"data_type": "bool", "hard_delete": True}},
    )
    def nuget_packages():
        stats.update(synced=0, deleted=0)
        rows = _package_rows(client, selection, dlt.current.resource_state(), stats, check_active)
        yield from dlt_utils.guarded_rows(rows, check_active)

    resource = nuget_packages()
    setattr(resource, DOCUMENT_SOURCE_ATTR, "nuget")
    setattr(resource, PIPELINE_SCOPE_ATTR, resource_name)
    resource.cognee_sync_stats = stats
    return resource
