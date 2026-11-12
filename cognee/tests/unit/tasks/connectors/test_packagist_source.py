"""The Packagist source queries the Composer v2 API and renders one document per package.

No network: a fake session returns recorded Composer v2 payloads.
"""

import pytest

from cognee.tasks.ingestion.connectors import packagist as packagist_module
from cognee.tasks.ingestion.connectors.packagist import (
    PackagistError,
    PackagistPackageNotFoundError,
    PackagistRateLimitError,
    packagist_source,
)
from cognee.tasks.ingestion.dlt_utils import NODE_SET_COLUMN

PAYLOAD = {
    "packages": {
        "laravel/framework": [
            {
                "name": "laravel/framework",
                "description": "The Laravel Framework.",
                "version": "v11.22.0",
                "time": "2026-10-01T10:00:00+00:00",
                "homepage": "https://laravel.com",
                "license": ["MIT"],
                "authors": [{"name": "Taylor Otwell", "email": "taylor@laravel.com"}],
                "keywords": ["framework", "laravel"],
                "require": {"php": "^8.2"},
                "abandoned": {"replacement": "laravel/framework-next"},
            },
            {
                "name": "laravel/framework",
                "description": "The Laravel Framework.",
                "version": "v10.48.0",
                "time": "2025-09-01T10:00:00+00:00",
                "license": ["MIT"],
            },
        ]
    }
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
            raise PackagistError(f"HTTP {self.status_code}")


class _FakeSession:
    def __init__(self, response):
        self.response = response
        self.requested = []

    def get(self, url, **kwargs):
        self.requested.append(url)
        return self.response if not isinstance(self.response, list) else self.response.pop(0)


# --- formatting -----------------------------------------------------------------


def test_the_document_carries_description_versions_and_the_replacement():
    row = packagist_module.format_packagist_document(PAYLOAD, "laravel/framework")
    assert row["id"] == "packagist:package:laravel/framework"
    assert row["title"] == "Packagist: laravel/framework"
    assert row["content"].startswith("# laravel/framework")
    assert "Description: The Laravel Framework." in row["content"]
    assert "Abandoned: use laravel/framework-next instead" in row["content"]
    # One section per major-version head: v11 and v10, not every patch.
    assert "## Version v11.22.0" in row["content"]
    assert "## Version v10.48.0" in row["content"]
    assert "License: MIT" in row["content"]
    assert "Authors: Taylor Otwell" in row["content"]
    assert "Requires PHP: ^8.2" in row["content"]
    assert row["url"] == "https://laravel.com"
    assert row[NODE_SET_COLUMN] == "packagist:package"


def test_a_plain_abandoned_flag_renders_without_a_replacement():
    payload = {
        "packages": {
            "old/pkg": [
                {"name": "old/pkg", "description": "d", "version": "1.0", "abandoned": True}
            ]
        }
    }
    row = packagist_module.format_packagist_document(payload, "old/pkg")
    assert "no longer maintained" in row["content"]


def test_a_live_package_renders_no_abandonment_line():
    payload = {
        "packages": {"live/pkg": [{"name": "live/pkg", "description": "d", "version": "2.0"}]}
    }
    row = packagist_module.format_packagist_document(payload, "live/pkg")
    assert "Abandoned" not in row["content"]


def test_a_payload_without_versions_is_refused():
    with pytest.raises(PackagistError):
        packagist_module.format_packagist_document({"packages": {"a/b": []}}, "a/b")


# --- the client -------------------------------------------------------------------


def test_a_404_raises_package_not_found():
    client = packagist_module.PackagistClient(session=_FakeSession(_FakeResponse(status_code=404)))
    with pytest.raises(PackagistPackageNotFoundError):
        client.get_package("vendor", "no-such")


def test_a_429_raises_rate_limit():
    client = packagist_module.PackagistClient(session=_FakeSession(_FakeResponse(status_code=429)))
    with pytest.raises(PackagistRateLimitError):
        client.get_package("vendor", "anything")


def test_a_non_json_payload_raises_packagist_error():
    client = packagist_module.PackagistClient(
        session=_FakeSession(_FakeResponse(payload=ValueError("bad")))
    )
    with pytest.raises(PackagistError):
        client.get_package("vendor", "anything")


def test_the_api_url_is_the_composer_v2_metadata_one():
    session = _FakeSession(_FakeResponse(payload=PAYLOAD))
    packagist_module.PackagistClient(session=session).get_package("laravel", "framework")
    assert session.requested == ["https://repo.packagist.org/p2/laravel/framework.json"]


# --- the source -------------------------------------------------------------------


def test_a_batch_yields_one_document_per_package_and_deduplicates():
    second = {"packages": {"symfony/console": [{"name": "symfony/console", "version": "v7.0.0"}]}}
    session = _FakeSession([_FakeResponse(payload=PAYLOAD), _FakeResponse(payload=second)])
    rows = list(
        packagist_source(
            package_names=["laravel/framework", "symfony/console", " laravel/framework "],
            session=session,
        )
    )
    assert [row["id"] for row in rows] == [
        "packagist:package:laravel/framework",
        "packagist:package:symfony/console",
    ]
    assert session.requested == [
        "https://repo.packagist.org/p2/laravel/framework.json",
        "https://repo.packagist.org/p2/symfony/console.json",
    ]


def test_a_missing_package_in_a_batch_fails_the_run_before_anything_is_published():
    """A partial snapshot must not look like a deletion: the run raises with nothing published."""
    session = _FakeSession([_FakeResponse(payload=PAYLOAD), _FakeResponse(status_code=404)])
    source = packagist_source(
        package_names=["laravel/framework", "vendor/no-such"], session=session
    )
    with pytest.raises(Exception) as excinfo:
        list(source)
    # dlt wraps generator failures; the connector's error is what caused the stop.
    assert isinstance(excinfo.value.__cause__, PackagistPackageNotFoundError)
    assert source.cognee_sync_stats == {"synced": 0, "deleted": 0}


def test_a_name_without_a_vendor_is_refused():
    with pytest.raises(ValueError, match="vendor/package"):
        packagist_source(package_names="framework")


def test_an_empty_name_list_is_refused():
    with pytest.raises(ValueError):
        packagist_source(package_names=[])


def test_an_empty_resource_name_is_refused():
    with pytest.raises(ValueError):
        packagist_source(package_names="laravel/framework", resource_name="  ")


# --- through a real dlt pipeline ---------------------------------------------------


def test_sync_update_and_forget_through_a_real_dlt_pipeline(tmp_path):
    import dlt

    from cognee.tasks.ingestion.connectors import packagist_source as exported_source
    from cognee.tasks.ingestion.dlt_utils import DOCUMENT_COLUMN_HINTS

    second = {"packages": {"symfony/console": [{"name": "symfony/console", "version": "v7.0.0"}]}}
    table = "packagist_test"

    def session():
        return _FakeSession([_FakeResponse(payload=PAYLOAD), _FakeResponse(payload=second)])

    def pipeline():
        return dlt.pipeline(
            pipeline_name="packagist_test",
            destination=dlt.destinations.sqlalchemy(f"sqlite:///{(tmp_path / 'a.db').as_posix()}"),
            dataset_name="packagist",
            pipelines_dir=str(tmp_path / "pipelines"),
        )

    def run(names) -> list[str]:
        source = exported_source(package_names=names, session=session(), resource_name=table)
        pipeline().run(source, columns=DOCUMENT_COLUMN_HINTS)
        with (
            pipeline().sql_client() as sql,
            sql.execute_query(f'SELECT id FROM "{table}" ORDER BY id') as cursor,
        ):
            return [row[0] for row in cursor.fetchall()]

    stored = run(["laravel/framework", "symfony/console"])
    assert stored == [
        "packagist:package:laravel/framework",
        "packagist:package:symfony/console",
    ]

    # A re-sync of the same selection publishes the same ids and forgets nothing.
    assert run(["laravel/framework", "symfony/console"]) == [
        "packagist:package:laravel/framework",
        "packagist:package:symfony/console",
    ]

    # Narrowing the selection forgets the package that left it.
    assert run(["laravel/framework"]) == ["packagist:package:laravel/framework"]
