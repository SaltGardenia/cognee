"""The JSR source queries the public API and renders one document per package.

No network: a fake session returns recorded JSR payloads.
"""

import pytest

from cognee.tasks.ingestion.connectors import jsr as jsr_module
from cognee.tasks.ingestion.connectors.jsr import (
    JSRError,
    JSRPackageNotFoundError,
    JSRRateLimitError,
    jsr_source,
)
from cognee.tasks.ingestion.dlt_utils import NODE_SET_COLUMN

PACKAGE = {
    "scope": "std",
    "name": "path",
    "description": "Utilities for working with file system paths",
    "runtimeCompat": {"browser": True, "deno": True, "node": True, "workerd": True, "bun": False},
    "score": 100,
    "versionCount": 58,
    "dependencyCount": 1,
    "dependentCount": 1904,
    "latestVersion": "1.1.6",
    "isArchived": False,
    "githubRepository": {"owner": "denoland", "name": "std"},
}
VERSIONS = {
    "items": [
        {
            "version": "1.1.6",
            "user": {"name": "Bartek Iwańczuk"},
            "createdAt": "2024-02-02T20:14:53.995437Z",
        },
        {"version": "1.1.5", "user": {"name": "Someone Else"}, "createdAt": "2024-01-01T00:00:00Z"},
    ]
}


class _FakeResponse:
    def __init__(self, status_code=200, payload=None):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise JSRError(f"HTTP {self.status_code}")


class _FakeSession:
    """Answers GETs by URL substring: 'packages/@' style lookups route to their payload."""

    def __init__(self, routes: dict[str, object]):
        self.routes = routes
        self.requested = []

    def get(self, url, **kwargs):
        self.requested.append(url)
        for fragment, payload in self.routes.items():
            if fragment in url:
                return (
                    _FakeResponse(payload=payload)
                    if not isinstance(payload, _FakeResponse)
                    else payload
                )
        raise AssertionError(f"unexpected URL: {url}")


def _session():
    return _FakeSession(
        {
            "/scopes/std/packages/path/versions": VERSIONS,
            "/scopes/std/packages/path": PACKAGE,
        }
    )


# --- formatting -----------------------------------------------------------------


def test_the_document_carries_metadata_compatibility_and_versions():
    row = jsr_module.format_jsr_document(PACKAGE, VERSIONS["items"], "@std/path")
    assert row["id"] == "jsr:package:@std/path"
    assert row["title"] == "JSR: @std/path"
    assert row["content"].startswith("# @std/path")
    assert "Description: Utilities for working with file system paths" in row["content"]
    assert "- node: yes" in row["content"] and "- bun: no" in row["content"]
    assert "Quality score: 100/100" in row["content"]
    assert "58 versions, 1 dependencies, 1904 dependents" in row["content"]
    assert "Repository: https://github.com/denoland/std" in row["content"]
    assert "- 1.1.6 by Bartek Iwańczuk on 2024-02-02" in row["content"]
    assert "- 1.1.5 by Someone Else on 2024-01-01" in row["content"]
    assert row["url"] == "https://jsr.io/@std/path"
    assert row[NODE_SET_COLUMN] == "jsr:package"


def test_an_archived_package_says_so():
    row = jsr_module.format_jsr_document(
        dict(PACKAGE, isArchived=True), VERSIONS["items"], "@std/path"
    )
    assert "This package is archived." in row["content"]


def test_a_package_without_runtime_compat_renders_no_section():
    row = jsr_module.format_jsr_document(dict(PACKAGE, runtimeCompat=None), [], "@std/path")
    assert "Runtime compatibility" not in row["content"]
    assert row["content"].startswith("# @std/path")


# --- the client -------------------------------------------------------------------


def test_a_404_raises_package_not_found():
    client = jsr_module.JSRClient(
        session=_FakeSession({"/scopes/x/packages/y": _FakeResponse(status_code=404)})
    )
    with pytest.raises(JSRPackageNotFoundError):
        client.get_package("x", "y")


def test_a_429_raises_rate_limit():
    client = jsr_module.JSRClient(session=_FakeSession({"/": _FakeResponse(status_code=429)}))
    with pytest.raises(JSRRateLimitError):
        client.get_package("x", "y")


def test_a_payload_without_metadata_raises_jsr_error():
    client = jsr_module.JSRClient(session=_FakeSession({"/scopes/x/packages/y": {}}))
    with pytest.raises(JSRError, match="without package metadata"):
        client.get_package("x", "y")


def test_the_api_urls_are_the_official_ones():
    session = _session()
    list(jsr_source(package_names="@std/path", session=session))
    assert session.requested == [
        "https://api.jsr.io/scopes/std/packages/path",
        "https://api.jsr.io/scopes/std/packages/path/versions",
    ]


# --- the source -------------------------------------------------------------------


def test_a_batch_yields_one_document_per_package_and_deduplicates():
    other_package = dict(PACKAGE, scope="std", name="csv", latestVersion="1.0.0")
    other_versions = {"items": [{"version": "1.0.0"}]}
    session = _FakeSession(
        {
            "/scopes/std/packages/path/versions": VERSIONS,
            "/scopes/std/packages/path": PACKAGE,
            "/scopes/std/packages/csv/versions": other_versions,
            "/scopes/std/packages/csv": other_package,
        }
    )
    rows = list(jsr_source(package_names=["@std/path", "@std/csv", " @std/path "], session=session))
    assert [row["id"] for row in rows] == ["jsr:package:@std/path", "jsr:package:@std/csv"]


def test_a_missing_package_in_a_batch_fails_the_run_before_anything_is_published():
    """A partial snapshot must not look like a deletion: the run raises with nothing published."""
    session = _FakeSession(
        {
            "/scopes/std/packages/path/versions": VERSIONS,
            "/scopes/std/packages/path": PACKAGE,
            "/scopes/x/packages/missing/versions": _FakeResponse(status_code=404),
            "/scopes/x/packages/missing": _FakeResponse(status_code=404),
        }
    )
    source = jsr_source(package_names=["@std/path", "@x/missing"], session=session)
    with pytest.raises(Exception) as excinfo:
        list(source)
    # dlt wraps generator failures; the connector's error is what caused the stop.
    assert isinstance(excinfo.value.__cause__, JSRPackageNotFoundError)
    assert source.cognee_sync_stats == {"synced": 0, "deleted": 0}


def test_a_name_without_an_at_sign_is_refused():
    with pytest.raises(ValueError, match="@scope/name"):
        jsr_source(package_names="std/path")


def test_an_empty_name_list_is_refused():
    with pytest.raises(ValueError):
        jsr_source(package_names=[])


def test_an_empty_resource_name_is_refused():
    with pytest.raises(ValueError):
        jsr_source(package_names="@std/path", resource_name="  ")


# --- through a real dlt pipeline ---------------------------------------------------


def test_sync_update_and_forget_through_a_real_dlt_pipeline(tmp_path):
    import dlt

    from cognee.tasks.ingestion.connectors import jsr_source as exported_source
    from cognee.tasks.ingestion.dlt_utils import DOCUMENT_COLUMN_HINTS

    other_package = dict(PACKAGE, scope="std", name="csv", latestVersion="1.0.0")
    other_versions = {"items": [{"version": "1.0.0"}]}
    routes = {
        "/scopes/std/packages/path/versions": VERSIONS,
        "/scopes/std/packages/path": PACKAGE,
        "/scopes/std/packages/csv/versions": other_versions,
        "/scopes/std/packages/csv": other_package,
    }
    table = "jsr_test"

    def pipeline():
        return dlt.pipeline(
            pipeline_name="jsr_test",
            destination=dlt.destinations.sqlalchemy(f"sqlite:///{(tmp_path / 'a.db').as_posix()}"),
            dataset_name="jsr",
            pipelines_dir=str(tmp_path / "pipelines"),
        )

    def run(names) -> list[str]:
        source = exported_source(
            package_names=names, session=_FakeSession(routes), resource_name=table
        )
        pipeline().run(source, columns=DOCUMENT_COLUMN_HINTS)
        with (
            pipeline().sql_client() as sql,
            sql.execute_query(f'SELECT id FROM "{table}" ORDER BY id') as cursor,
        ):
            return [row[0] for row in cursor.fetchall()]

    stored = run(["@std/path", "@std/csv"])
    assert stored == ["jsr:package:@std/csv", "jsr:package:@std/path"]

    # A re-sync of the same selection publishes the same ids and forgets nothing.
    assert run(["@std/path", "@std/csv"]) == ["jsr:package:@std/csv", "jsr:package:@std/path"]

    # Narrowing the selection forgets the package that left it.
    assert run(["@std/path"]) == ["jsr:package:@std/path"]
