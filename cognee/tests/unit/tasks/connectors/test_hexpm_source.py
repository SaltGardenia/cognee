"""The Hex.pm source queries the public API and renders one document per package.

No network: a fake session returns recorded Hex.pm payloads.
"""

import pytest

from cognee.tasks.ingestion.connectors import hexpm as hexpm_module
from cognee.tasks.ingestion.connectors.hexpm import (
    HexError,
    HexPackageNotFoundError,
    HexRateLimitError,
    hexpm_source,
)
from cognee.tasks.ingestion.dlt_utils import NODE_SET_COLUMN

PACKAGE = {
    "name": "phoenix",
    "meta": {
        "description": "Peace of mind from prototype to production",
        "licenses": ["MIT"],
        "links": {
            "Changelog": "https://phoenix.hexdocs.pm/changelog.html",
            "GitHub": "https://github.com/phoenixframework/phoenix",
        },
        "maintainers": [],
    },
    "downloads": {"all": 42_000_000, "week": 100_000},
    "latest_stable_version": "1.8.16",
    "latest_version": "1.8.16",
    "html_url": "https://hex.pm/packages/phoenix",
    "repository": None,
    "security_advisories": [{"id": "GHSA-xxxx", "title": "Directory traversal in plug_static"}],
    "releases": [{"version": "1.8.16", "has_docs": True}],
}
RELEASE = {
    "version": "1.8.16",
    "requirements": {
        "plug": {"app": "plug", "optional": False, "requirement": "~> 1.14"},
        "bandit": {"app": "bandit", "optional": True, "requirement": "~> 1.0"},
    },
    "retirement": None,
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
            raise HexError(f"HTTP {self.status_code}")


class _FakeSession:
    """Answers GETs by URL substring."""

    def __init__(self, routes: dict[str, object]):
        self.routes = routes
        self.requested = []

    def get(self, url, **kwargs):
        self.requested.append(url)
        for fragment, payload in self.routes.items():
            if fragment in url:
                return payload
        raise AssertionError(f"unexpected URL: {url}")


def _session():
    return _FakeSession(
        {
            "/api/packages/phoenix/releases": _FakeResponse(payload=RELEASE),
            "/api/packages/phoenix": _FakeResponse(payload=PACKAGE),
        }
    )


# --- formatting -----------------------------------------------------------------


def test_the_document_carries_metadata_requirements_and_advisories():
    row = hexpm_module.format_hexpm_document(PACKAGE, RELEASE)
    assert row["id"] == "hexpm:package:phoenix"
    assert row["title"] == "Hex.pm: phoenix"
    assert row["content"].startswith("# phoenix")
    assert "Description: Peace of mind from prototype to production" in row["content"]
    assert "License: MIT" in row["content"]
    assert "Changelog: https://phoenix.hexdocs.pm/changelog.html" in row["content"]
    assert "Downloads: 42,000,000" in row["content"]
    assert "Latest stable version: 1.8.16" in row["content"]
    assert "## Latest release requirements" in row["content"]
    assert "- plug ~> 1.14" in row["content"]
    assert "- bandit ~> 1.0 (optional)" in row["content"]
    assert "## Security advisories" in row["content"]
    assert "- GHSA-xxxx: Directory traversal in plug_static" in row["content"]
    assert row["url"] == "https://hex.pm/packages/phoenix"
    assert row[NODE_SET_COLUMN] == "hexpm:package"


def test_retirements_render_with_reason_and_message():
    release = dict(
        RELEASE, retirement={"version": "1.0.0", "reason": "security", "message": "do not use"}
    )
    row = hexpm_module.format_hexpm_document(PACKAGE, release)
    assert "- 1.0.0 retired (security): do not use" in row["content"]


def test_a_package_without_advisories_renders_no_section():
    row = hexpm_module.format_hexpm_document(dict(PACKAGE, security_advisories=[]), RELEASE)
    assert "Security advisories" not in row["content"]


def test_a_package_without_a_release_detail_skips_the_requirements_section():
    row = hexpm_module.format_hexpm_document(PACKAGE, None)
    assert "Latest release requirements" not in row["content"]
    assert row["content"].startswith("# phoenix")


# --- the client -------------------------------------------------------------------


def test_a_404_raises_package_not_found():
    client = hexpm_module.HexClient(
        session=_FakeSession({"/api/packages/x": _FakeResponse(status_code=404)})
    )
    with pytest.raises(HexPackageNotFoundError):
        client.get_package("x")


def test_a_429_raises_rate_limit():
    client = hexpm_module.HexClient(session=_FakeSession({"/": _FakeResponse(status_code=429)}))
    with pytest.raises(HexRateLimitError):
        client.get_package("x")


def test_a_payload_without_metadata_raises_hex_error():
    client = hexpm_module.HexClient(
        session=_FakeSession({"/api/packages/x": _FakeResponse(payload={})})
    )
    with pytest.raises(HexError, match="without package metadata"):
        client.get_package("x")


def test_the_api_urls_are_the_official_ones():
    session = _session()
    list(hexpm_source(package_names="phoenix", session=session))
    assert session.requested == [
        "https://hex.pm/api/packages/phoenix",
        "https://hex.pm/api/packages/phoenix/releases/1.8.16",
    ]


# --- the source -------------------------------------------------------------------


def test_a_release_detail_404_fails_the_run_before_anything_is_published():
    """The package exists but its latest release's detail is gone: a hard error, not a skip."""
    second = {
        "name": "ecto",
        "meta": {"description": "A database layer"},
        "latest_stable_version": "3.12.0",
    }
    session = _FakeSession(
        {
            "/api/packages/phoenix/releases": _FakeResponse(payload=RELEASE),
            "/api/packages/phoenix": _FakeResponse(payload=PACKAGE),
            "/api/packages/ecto/releases": _FakeResponse(status_code=404),
            "/api/packages/ecto": _FakeResponse(payload=second),
        }
    )
    source = hexpm_source(package_names=["phoenix", "ecto"], session=session)
    with pytest.raises(Exception) as excinfo:
        list(source)
    # dlt wraps generator failures; the connector's error is what caused the stop.
    assert isinstance(excinfo.value.__cause__, HexPackageNotFoundError)
    # A partial snapshot must not look like a deletion: nothing was fetched for ecto,
    # so the run raised before the first row was published and the state kept its members.
    assert source.cognee_sync_stats == {"synced": 0, "deleted": 0}


def test_a_batch_that_reads_all_packages_succeeds():
    second = dict(PACKAGE, name="ecto", latest_stable_version=None, latest_version=None)
    session = _FakeSession(
        {
            "/api/packages/phoenix/releases": _FakeResponse(payload=RELEASE),
            "/api/packages/phoenix": _FakeResponse(payload=PACKAGE),
            "/api/packages/ecto": _FakeResponse(payload=second),
        }
    )
    rows = list(hexpm_source(package_names=["phoenix", "ecto"], session=session))
    assert [row["id"] for row in rows] == ["hexpm:package:phoenix", "hexpm:package:ecto"]
    assert all(row["_deleted"] is False for row in rows)


def test_an_empty_name_list_is_refused():
    with pytest.raises(ValueError):
        hexpm_source(package_names=[])


def test_an_empty_string_name_is_refused():
    with pytest.raises(ValueError):
        hexpm_source(package_names="  ")


def test_an_empty_resource_name_is_refused():
    with pytest.raises(ValueError):
        hexpm_source(package_names="phoenix", resource_name="  ")


def test_duplicates_are_ingested_once():
    session = _session()
    rows = list(hexpm_source(package_names=["phoenix", " phoenix "], session=session))
    assert [row["id"] for row in rows] == ["hexpm:package:phoenix"]
    assert len(session.requested) == 2  # metadata + release detail, fetched once


# --- through a real dlt pipeline ---------------------------------------------------


def test_sync_update_and_forget_through_a_real_dlt_pipeline(tmp_path):
    import dlt

    from cognee.tasks.ingestion.connectors import hexpm_source as exported_source
    from cognee.tasks.ingestion.dlt_utils import DOCUMENT_COLUMN_HINTS

    second = dict(PACKAGE, name="ecto", latest_stable_version=None, latest_version=None)
    table = "hexpm_test"
    routes = {
        "/api/packages/phoenix/releases": _FakeResponse(payload=RELEASE),
        "/api/packages/phoenix": _FakeResponse(payload=PACKAGE),
        "/api/packages/ecto": _FakeResponse(payload=second),
    }

    def pipeline():
        return dlt.pipeline(
            pipeline_name="hexpm_test",
            destination=dlt.destinations.sqlalchemy(f"sqlite:///{(tmp_path / 'a.db').as_posix()}"),
            dataset_name="hexpm",
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

    stored = run(["phoenix", "ecto"])
    assert stored == ["hexpm:package:ecto", "hexpm:package:phoenix"]

    # A re-sync of the same selection publishes the same ids and forgets nothing.
    assert run(["phoenix", "ecto"]) == ["hexpm:package:ecto", "hexpm:package:phoenix"]

    # Narrowing the selection forgets the package that left it.
    assert run(["phoenix"]) == ["hexpm:package:phoenix"]
