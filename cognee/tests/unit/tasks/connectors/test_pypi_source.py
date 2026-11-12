"""The PyPI source queries the official JSON API and renders one document per package.

No network: a fake session returns recorded PyPI payloads, like the Apollo/Gmail
connector tests.
"""

import pytest

from cognee.tasks.ingestion.connectors import pypi as pypi_module
from cognee.tasks.ingestion.connectors.pypi import (
    PyPIError,
    PyPIPackageNotFoundError,
    PyPIRateLimitError,
    format_pypi_document,
    pypi_source,
)
from cognee.tasks.ingestion.dlt_utils import NODE_SET_COLUMN

PAYLOAD = {
    "info": {
        "name": "fastapi",
        "version": "0.115.0",
        "summary": "FastAPI framework, high performance, easy to learn",
        "author": "Sebastián Ramírez",
        "author_email": "tiangolo@example.com",
        "license": "MIT",
        "requires_python": ">=3.8",
        "keywords": ["web", "framework", "async"],
        "project_urls": {
            "Homepage": "https://github.com/tiangolo/fastapi",
            "Documentation": "https://fastapi.tiangolo.com/",
        },
        "home_page": None,
        "description": "# FastAPI\n\nFastAPI is a modern web framework.\n\n- Type hints\n- Async support\n",
    },
    "releases": {"0.115.0": [], "0.114.0": []},
    "urls": [],
}


class _FakeResponse:
    def __init__(self, status_code=200, payload=None):
        self.status_code = status_code
        self._payload = payload
        self.text = ""

    def json(self):
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise PyPIError(f"HTTP {self.status_code}")


class _FakeSession:
    """A session that answers every GET with the given response or exception."""

    def __init__(self, response):
        self.response = response
        self.requested = []

    def get(self, url, **kwargs):
        self.requested.append(url)
        return self.response if not isinstance(self.response, list) else self.response.pop(0)


# --- formatting -----------------------------------------------------------------


def test_the_document_carries_the_metadata_and_the_full_readme():
    row = format_pypi_document(PAYLOAD)
    assert row["id"] == "pypi:package:fastapi"
    assert row["title"] == "PyPI: fastapi"
    assert row["content"].startswith("# fastapi")
    assert "Version: 0.115.0" in row["content"]
    assert "Summary: FastAPI framework" in row["content"]
    assert "Author: Sebastián Ramírez" in row["content"]
    assert "License: MIT" in row["content"]
    assert "Requires Python: >=3.8" in row["content"]
    assert "Keywords: web, framework, async" in row["content"]
    assert "Documentation: https://fastapi.tiangolo.com/" in row["content"]
    assert "# FastAPI\n\nFastAPI is a modern web framework." in row["content"]
    assert row["url"] == "https://github.com/tiangolo/fastapi"
    assert row[NODE_SET_COLUMN] == "pypi:package"
    assert row["_deleted"] is False


def test_a_package_without_project_urls_falls_back_to_the_pypi_page():
    payload = {"info": {"name": "solo", "version": "1.0", "summary": "s", "description": "d"}}
    row = format_pypi_document(payload)
    assert row["url"] == "https://pypi.org/project/solo/"


def test_a_payload_without_a_name_is_refused():
    with pytest.raises(PyPIError):
        format_pypi_document({"info": {}})


def test_a_list_valued_keywords_field_renders_comma_separated():
    row = format_pypi_document({"info": {"name": "x", "keywords": ["a", "b"]}})
    assert "Keywords: a, b" in row["content"]


# --- the client -------------------------------------------------------------------


def test_a_404_raises_package_not_found():
    client = pypi_module.PyPIClient(session=_FakeSession(_FakeResponse(status_code=404)))
    with pytest.raises(PyPIPackageNotFoundError):
        client.get_package("no-such-package")


def test_a_429_raises_rate_limit():
    client = pypi_module.PyPIClient(session=_FakeSession(_FakeResponse(status_code=429)))
    with pytest.raises(PyPIRateLimitError):
        client.get_package("anything")


def test_a_non_json_payload_raises_pypi_error():
    client = pypi_module.PyPIClient(session=_FakeSession(_FakeResponse(payload=ValueError("bad"))))
    with pytest.raises(PyPIError):
        client.get_package("anything")


def test_a_payload_without_info_raises_pypi_error():
    client = pypi_module.PyPIClient(session=_FakeSession(_FakeResponse(payload={"releases": {}})))
    with pytest.raises(PyPIError, match="without an info object"):
        client.get_package("anything")


def test_the_api_url_is_the_official_one():
    session = _FakeSession(_FakeResponse(payload=PAYLOAD))
    pypi_module.PyPIClient(session=session).get_package("fastapi")
    assert session.requested == ["https://pypi.org/pypi/fastapi/json"]


# --- the source -------------------------------------------------------------------


def test_a_single_package_yields_one_document():
    session = _FakeSession(_FakeResponse(payload=PAYLOAD))
    rows = list(pypi_source("fastapi", session=session))
    assert [row["id"] for row in rows] == ["pypi:package:fastapi"]


def test_a_batch_yields_one_document_per_package_in_order():
    second = {"info": {"name": "pydantic", "version": "2.9.0", "summary": "Data validation"}}
    session = _FakeSession([_FakeResponse(payload=PAYLOAD), _FakeResponse(payload=second)])
    rows = list(pypi_source(["fastapi", "pydantic"], session=session))
    assert [row["id"] for row in rows] == ["pypi:package:fastapi", "pypi:package:pydantic"]


def test_duplicates_in_a_batch_are_ingested_once():
    session = _FakeSession([_FakeResponse(payload=PAYLOAD), _FakeResponse(payload=PAYLOAD)])
    rows = list(pypi_source(["fastapi", "fastapi"], session=session))
    assert len(rows) == 1


def test_a_missing_package_in_a_batch_fails_the_run():
    session = _FakeSession([_FakeResponse(payload=PAYLOAD), _FakeResponse(status_code=404)])
    with pytest.raises(PyPIPackageNotFoundError):
        list(pypi_source(["fastapi", "no-such-package"], session=session))


def test_an_empty_or_malformed_name_is_refused():
    with pytest.raises(ValueError):
        list(pypi_source(""))
    with pytest.raises(ValueError):
        list(pypi_source([]))
    with pytest.raises(ValueError):
        list(pypi_source(["fastapi", "  "]))
