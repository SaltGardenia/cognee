"""The NuGet source resolves the v3 service index and renders one document per package.

No network: a fake session returns recorded NuGet v3 payloads; a real dlt pipeline
backs the sync-and-forget test.
"""

import json

import pytest

from cognee.tasks.ingestion.connectors import nuget as nuget_module
from cognee.tasks.ingestion.connectors.nuget import (
    NuGetError,
    NuGetPackageNotFoundError,
    NuGetRateLimitError,
    nuget_source,
)
from cognee.tasks.ingestion.dlt_utils import NODE_SET_COLUMN

SERVICE_INDEX = {
    "resources": [
        {"@id": "https://azuresearch-usnc.nuget.org/query", "@type": "SearchQueryService"},
        {
            "@id": "https://api.nuget.org/v3/registration5-gz-semver2/",
            "@type": "RegistrationsBaseUrl/Versioned",
        },
    ]
}

ENTRY_LATEST = {
    "version": "13.0.3",
    "description": "Popular high-performance JSON framework for .NET",
    "authors": "James Newton-King",
    "licenseExpression": "MIT",
    "projectUrl": "https://www.newtonsoft.com/json",
    "dependencyGroups": [
        {
            "targetFramework": "net6.0",
            "dependencies": [{"id": "NETStandard.Library", "range": ">= 2.0.0"}],
        },
        {"targetFramework": "net45", "dependencies": []},
    ],
}
ENTRY_OLD = {
    "version": "12.0.2",
    "description": "Popular high-performance JSON framework for .NET",
    "vulnerabilities": [
        {"advisoryUrl": "https://github.com/advisories/GHSA-5crp-9r3c-p9vr", "severity": "2"}
    ],
}
REGISTRATION = {
    "items": [
        {
            "items": [
                {"catalogEntry": ENTRY_OLD},
                {"catalogEntry": ENTRY_LATEST},
            ]
        }
    ]
}
REGISTRATION_UNLISTED = {
    "items": [
        {
            "items": [
                {"catalogEntry": ENTRY_OLD},
                {"listed": False, "catalogEntry": ENTRY_LATEST},
            ]
        }
    ]
}
DEPRECATION_ENTRY = {
    "version": "1.0.0",
    "description": "Legacy Azure Storage client",
    "deprecation": {
        "reasons": ["Legacy"],
        "message": "WindowsAzure.Storage is deprecated, use Azure.Storage.Common.",
        "alternatePackage": {"id": "Azure.Storage.Common", "range": "*"},
    },
}
DEPRECATION_REGISTRATION = {"items": [{"items": [{"catalogEntry": DEPRECATION_ENTRY}]}]}


class _FakeResponse:
    def __init__(self, status_code=200, payload=None):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


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


def _session(registration=None):
    return _FakeSession(
        {
            "/v3/index.json": _FakeResponse(payload=SERVICE_INDEX),
            "/newtonsoft.json/index.json": _FakeResponse(payload=registration or REGISTRATION),
        }
    )


# --- formatting -----------------------------------------------------------------


def test_the_document_carries_metadata_dependencies_and_the_latest_version():
    row = nuget_module.format_nuget_document("Newtonsoft.Json", REGISTRATION["items"][0]["items"])
    assert row["id"] == "nuget:package:Newtonsoft.Json"
    assert row["title"] == "NuGet: Newtonsoft.Json"
    assert row["content"].startswith("# Newtonsoft.Json")
    assert "Description: Popular high-performance JSON framework for .NET" in row["content"]
    assert "Authors: James Newton-King" in row["content"]
    assert "License: MIT" in row["content"]
    assert "Latest listed version: 13.0.3" in row["content"]
    assert "Project URL: https://www.newtonsoft.com/json" in row["content"]
    assert "## Dependencies (latest listed version)" in row["content"]
    assert "  - net6.0" in row["content"]
    assert "    - NETStandard.Library >= 2.0.0" in row["content"]
    assert "  - net45: no dependencies" in row["content"]
    assert row["url"] == "https://www.newtonsoft.com/json"
    assert row[NODE_SET_COLUMN] == "nuget:package"


def test_a_deprecation_notice_renders_its_replacement():
    row = nuget_module.format_nuget_document(
        "WindowsAzure.Storage", DEPRECATION_REGISTRATION["items"][0]["items"]
    )
    assert (
        "- Deprecated (Legacy): WindowsAzure.Storage is deprecated, use Azure.Storage.Common."
        in row["content"]
    )
    assert "  - Replacement: Azure.Storage.Common (*)" in row["content"]


def test_vulnerabilities_of_listed_versions_render_with_severity():
    row = nuget_module.format_nuget_document("Newtonsoft.Json", REGISTRATION["items"][0]["items"])
    assert "## Vulnerabilities" in row["content"]
    assert (
        "- 12.0.2: https://github.com/advisories/GHSA-5crp-9r3c-p9vr (severity 2)" in row["content"]
    )


def test_unlisted_versions_stop_contributing_to_the_document():
    leaves = [
        {"catalogEntry": ENTRY_OLD},
        {
            "listed": False,
            "catalogEntry": dict(
                ENTRY_LATEST,
                vulnerabilities=[
                    {"advisoryUrl": "https://github.com/advisories/GHSA-x", "severity": "3"}
                ],
            ),
        },
    ]
    row = nuget_module.format_nuget_document("Newtonsoft.Json", leaves)
    assert "Latest listed version: 12.0.2" in row["content"]  # 13.0.3 is unlisted
    assert "13.0.3" not in row["content"]
    assert "GHSA-x" not in row["content"]


def test_a_registration_without_listed_versions_is_refused():
    leaves = [{"listed": False, "catalogEntry": ENTRY_LATEST}]
    with pytest.raises(NuGetPackageNotFoundError):
        nuget_module.format_nuget_document("Newtonsoft.Json", leaves)


# --- the client -------------------------------------------------------------------


def test_the_registration_base_is_resolved_from_the_service_index():
    session = _session()
    nuget_module.NuGetClient(session=session).registration_base()
    assert session.requested == ["https://api.nuget.org/v3/index.json"]


def test_a_service_index_without_a_registration_resource_raises():
    session = _FakeSession(
        {"/v3/index.json": _FakeResponse(payload={"resources": [{"@type": "SearchQueryService"}]})}
    )
    with pytest.raises(NuGetError, match="RegistrationsBaseUrl"):
        nuget_module.NuGetClient(session=session).registration_base()


def test_a_fallback_registration_base_is_used_without_the_versioned_one():
    index = {"resources": [{"@id": "https://feed.example/reg/", "@type": "RegistrationsBaseUrl"}]}
    session = _FakeSession({"/v3/index.json": _FakeResponse(payload=index)})
    assert (
        nuget_module.NuGetClient(session=session).registration_base() == "https://feed.example/reg/"
    )


def test_a_404_raises_package_not_found():
    client = nuget_module.NuGetClient(
        session=_FakeSession(
            {
                "/v3/index.json": _FakeResponse(payload=SERVICE_INDEX),
                "/no-such/": _FakeResponse(status_code=404),
            }
        )
    )
    with pytest.raises(NuGetPackageNotFoundError):
        client.get_registration("no-such")


def test_a_429_raises_rate_limit():
    client = nuget_module.NuGetClient(
        session=_FakeSession(
            {
                "/v3/index.json": _FakeResponse(payload=SERVICE_INDEX),
                "/": _FakeResponse(status_code=429),
            }
        )
    )
    with pytest.raises(NuGetRateLimitError):
        client.get_registration("anything")


def test_a_non_json_payload_raises_nuget_error():
    client = nuget_module.NuGetClient(
        session=_FakeSession(
            {
                "/v3/index.json": _FakeResponse(payload=SERVICE_INDEX),
                "/": _FakeResponse(payload=ValueError("bad")),
            }
        )
    )
    with pytest.raises(NuGetError, match="non-JSON"):
        client.get_registration("anything")


def test_search_uses_the_indexed_search_service_and_returns_ids():
    query_result = {"data": [{"id": "Newtonsoft.Json"}, {"id": "newtonsoft.json.bson"}]}
    session = _FakeSession(
        {
            "/v3/index.json": _FakeResponse(payload=SERVICE_INDEX),
            "/query": _FakeResponse(payload=query_result),
        }
    )
    assert nuget_module.NuGetClient(session=session).search("newtonsoft") == [
        "Newtonsoft.Json",
        "newtonsoft.json.bson",
    ]
    assert session.requested[-1] == "https://azuresearch-usnc.nuget.org/query?q=newtonsoft&take=20"


def test_externalized_registration_pages_are_followed():
    externalized = {
        "items": [
            {"@id": "https://api.nuget.org/v3/registration5-gz-semver2/newtonsoft.json/page_0.json"}
        ]
    }
    page = {"items": [{"catalogEntry": ENTRY_LATEST}]}
    session = _FakeSession(
        {
            "/v3/index.json": _FakeResponse(payload=SERVICE_INDEX),
            "/newtonsoft.json/index.json": _FakeResponse(payload=externalized),
            "/page_0.json": _FakeResponse(payload=page),
        }
    )
    rows = list(nuget_source(package_ids="Newtonsoft.Json", session=session))
    assert rows[0]["id"] == "nuget:package:Newtonsoft.Json"
    assert (
        session.requested[-1]
        == "https://api.nuget.org/v3/registration5-gz-semver2/newtonsoft.json/page_0.json"
    )


def test_an_externalized_catalog_leaf_raises_rather_than_rendering_less():
    leaves = [{"catalogEntry": "https://api.nuget.org/v3/catalog/leaf.json"}]
    with pytest.raises(NuGetError, match="externalizes"):
        nuget_module.format_nuget_document("Newtonsoft.Json", leaves)


# --- the source -------------------------------------------------------------------


def test_the_official_urls_are_used_in_order():
    session = _session()
    list(nuget_source(package_ids="Newtonsoft.Json", session=session))
    assert session.requested == [
        "https://api.nuget.org/v3/index.json",
        "https://api.nuget.org/v3/registration5-gz-semver2/newtonsoft.json/index.json",
    ]


def test_a_search_query_extends_the_selection():
    query_result = {"data": [{"id": "Newtonsoft.Json"}]}
    session = _FakeSession(
        {
            "/v3/index.json": _FakeResponse(payload=SERVICE_INDEX),
            "/newtonsoft.json/index.json": _FakeResponse(payload=REGISTRATION),
            "/query": _FakeResponse(payload=query_result),
        }
    )
    rows = list(
        nuget_source(package_ids=["Newtonsoft.Json"], search_query="newtonsoft", session=session)
    )
    # The search hit is the same package: ingested once, not twice.
    assert [row["id"] for row in rows] == ["nuget:package:Newtonsoft.Json"]


def test_a_missing_package_fails_the_run_before_anything_is_published():
    session = _FakeSession(
        {
            "/v3/index.json": _FakeResponse(payload=SERVICE_INDEX),
            "/newtonsoft.json/index.json": _FakeResponse(payload=REGISTRATION),
            "/no-such/": _FakeResponse(status_code=404),
        }
    )
    source = nuget_source(package_ids=["Newtonsoft.Json", "no-such"], session=session)
    with pytest.raises(Exception) as excinfo:
        list(source)
    # dlt wraps generator failures; the connector's error is what caused the stop.
    assert isinstance(excinfo.value.__cause__, NuGetPackageNotFoundError)
    assert source.cognee_sync_stats == {"synced": 0, "deleted": 0}


def test_an_empty_or_malformed_id_is_refused():
    with pytest.raises(ValueError):
        nuget_source(package_ids="")
    with pytest.raises(ValueError):
        nuget_source(package_ids=[])
    with pytest.raises(ValueError):
        nuget_source(package_ids=["Newtonsoft.Json", "  "])


def test_an_empty_resource_name_is_refused():
    with pytest.raises(ValueError):
        nuget_source(package_ids="Newtonsoft.Json", resource_name="  ")


def test_duplicate_ids_are_ingested_once():
    session = _session()
    rows = list(nuget_source(package_ids=["Newtonsoft.Json", " newtonsoft.json "], session=session))
    # NuGet ids are case-insensitive: one document, the caller's casing kept.
    assert [row["id"] for row in rows] == ["nuget:package:Newtonsoft.Json"]
    assert (
        session.requested.count(
            "https://api.nuget.org/v3/registration5-gz-semver2/newtonsoft.json/index.json"
        )
        == 1
    )


# --- through a real dlt pipeline ---------------------------------------------------


def test_sync_update_and_forget_through_a_real_dlt_pipeline(tmp_path):
    import dlt

    from cognee.tasks.ingestion.connectors import nuget_source as exported_source
    from cognee.tasks.ingestion.dlt_utils import DOCUMENT_COLUMN_HINTS

    table = "nuget_test"
    other_registration = {"items": [{"items": [{"catalogEntry": DEPRECATION_ENTRY}]}]}
    routes = {
        "/v3/index.json": _FakeResponse(payload=SERVICE_INDEX),
        "/newtonsoft.json/index.json": _FakeResponse(payload=REGISTRATION),
        "/windowsazure.storage/index.json": _FakeResponse(payload=other_registration),
    }

    def pipeline():
        return dlt.pipeline(
            pipeline_name="nuget_test",
            destination=dlt.destinations.sqlalchemy(f"sqlite:///{(tmp_path / 'a.db').as_posix()}"),
            dataset_name="nuget",
            pipelines_dir=str(tmp_path / "pipelines"),
        )

    def run(ids) -> list[str]:
        source = exported_source(package_ids=ids, session=_FakeSession(routes), resource_name=table)
        pipeline().run(source, columns=DOCUMENT_COLUMN_HINTS)
        with (
            pipeline().sql_client() as sql,
            sql.execute_query(f'SELECT id FROM "{table}" ORDER BY id') as cursor,
        ):
            return [row[0] for row in cursor.fetchall()]

    stored = run(["Newtonsoft.Json", "WindowsAzure.Storage"])
    assert stored == [
        "nuget:package:Newtonsoft.Json",
        "nuget:package:WindowsAzure.Storage",
    ]

    # A re-sync of the same selection publishes the same ids and forgets nothing.
    assert run(["Newtonsoft.Json", "WindowsAzure.Storage"]) == stored

    # Narrowing the selection forgets the package that left it.
    assert run(["Newtonsoft.Json"]) == ["nuget:package:Newtonsoft.Json"]
