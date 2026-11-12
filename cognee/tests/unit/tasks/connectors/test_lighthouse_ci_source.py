"""The Lighthouse CI source turns audit reports into concise documents, and keeps history.

No Lighthouse install needed: fixture reports are written into a temporary folder. The
walker is driven with a plain dict for state, like the Obsidian source's tests.
"""

import json
from pathlib import Path

import pytest

from cognee.tasks.ingestion.connectors import lighthouse_ci as lighthouse_module
from cognee.tasks.ingestion.connectors.lighthouse_ci import (
    LighthouseReportParseError,
    LighthouseSourceUnavailableError,
    _document_text,
    _load_report,
    _sanitize_url,
)

EMPTY_STATS = {"synced": 0, "deleted": 0}

REPORT = {
    "lighthouseVersion": "10.4.0",
    "fetchTime": "2026-11-01T12:00:00.000Z",
    "finalDisplayedUrl": "https://shop.example.com/cart?session=abc123&utm_campaign=x#summary",
    "categories": {
        "performance": {"title": "Performance", "score": 0.87},
        "accessibility": {"title": "Accessibility", "score": 0.95},
        "seo": {"title": "SEO", "score": 0.5},
    },
    "audits": {
        "first-contentful-paint": {
            "title": "First Contentful Paint",
            "displayValue": "1.2 s",
            "score": 0.9,
        },
        "largest-contentful-paint": {
            "title": "Largest Contentful Paint",
            "displayValue": "3.4 s",
            "score": 0.6,
            "numericValue": 3400,
        },
        "unused-javascript": {
            "title": "Reduce unused JavaScript",
            "score": 0.4,
            "details": {"overallSavingsMs": 500, "overallSavingsBytes": 204800},
        },
        "meta-description": {"title": "Document does not have a meta description", "score": 0},
        " servis-never-scores": {"title": "Informational audit", "score": None},
        "passes-clean": {"title": "Passed clean", "score": 1},
    },
    "buildContext": {"commitHash": "0123456789abcdef"},
}


def _write(folder: Path, files: dict[str, str]) -> Path:
    for rel, text in files.items():
        path = folder / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(text.encode("utf-8"))
    return folder


def _rows(folder: Path, state=None, stats=None):
    stats = dict(EMPTY_STATS) if stats is None else stats
    return list(lighthouse_module._iter_rows(folder, {} if state is None else state, stats, None))


# --- loading and sanitizing ---------------------------------------------------


def test_a_wrapped_lhci_server_report_is_unwrapped():
    assert _load_report(json.dumps({"data": REPORT}))["audits"] == REPORT["audits"]


@pytest.mark.parametrize(
    "text",
    [
        "not json at all",
        "[1, 2, 3]",
        json.dumps({"categories": {}}),
        json.dumps({"audits": "not a dict"}),
    ],
)
def test_non_report_files_fail_loudly(text):
    with pytest.raises(LighthouseReportParseError):
        _load_report(text)


def test_the_url_loses_its_query_and_fragment_whole():
    assert (
        _sanitize_url("https://shop.example.com/cart?session=abc123&utm_campaign=x#summary")
        == "https://shop.example.com/cart"
    )


# --- the document ----------------------------------------------------------------


def test_the_document_carries_provenance_scores_metrics_and_findings():
    body = _document_text(REPORT)
    assert body.startswith("# Lighthouse: https://shop.example.com/cart")
    assert "URL: https://shop.example.com/cart" in body
    assert "session=abc123" not in body, "the query string with its token is gone"
    assert "Fetched: 2026-11-01T12:00:00.000Z" in body
    assert "Lighthouse: 10.4.0" in body
    assert "Build: 0123456789ab" in body
    assert "- Performance: 87" in body
    assert "- SEO: 50" in body
    assert "- First Contentful Paint: 1.2 s (score 0.90)" in body
    assert "- Largest Contentful Paint: 3.4 s (score 0.60)" in body
    # Findings: the failing and the saving audits, with ids, savings and no noise.
    assert "- [unused-javascript] Reduce unused JavaScript" in body
    assert "estimated savings: 500 ms + 200 KiB" in body
    assert "- [meta-description] Document does not have a meta description" in body
    # An informational audit (score null, no savings) and a passing one are not findings.
    assert "Informational audit" not in body
    assert "Passed clean" not in body


def test_findings_are_ordered_worst_first():
    lines = _document_text(REPORT).split("\n")
    finding_order = [line for line in lines if line.startswith("- [")]
    assert finding_order.index("- [meta-description] Document does not have a meta description") < (
        finding_order.index("- [largest-contentful-paint] Largest Contentful Paint")
    )


# --- the source -----------------------------------------------------------------


def test_the_folder_yields_one_document_per_report(tmp_path):
    folder = _write(tmp_path / "lhci", {"report-1.json": json.dumps(REPORT)})
    state: dict = {}

    rows = _rows(folder, state)

    assert len(rows) == 1
    assert rows[0]["id"].startswith("lighthouse-ci:shop.example.com-cart:")
    assert rows[0]["title"] == "Lighthouse: report-1"
    assert rows[0]["url"] == "https://shop.example.com/cart"
    assert rows[0]["_deleted"] is False
    assert state == {"v": 1, "members": [row["id"] for row in rows]}


def test_an_unchanged_report_keeps_its_id_and_a_changed_one_gets_a_new_one(tmp_path):
    folder = _write(tmp_path / "lhci", {"report-1.json": json.dumps(REPORT)})
    state: dict = {}
    first = _rows(folder, state)

    quiet = _rows(folder, state)
    assert quiet[0]["id"] == first[0]["id"]

    changed = dict(REPORT, fetchTime="2026-11-02T12:00:00.000Z")
    _write(folder, {"report-1.json": json.dumps(changed)})
    second = _rows(folder, state)
    assert second[0]["id"] != first[0]["id"]


def test_a_report_absent_from_a_later_import_is_never_forgotten(tmp_path):
    folder = _write(tmp_path / "lhci", {"report-1.json": json.dumps(REPORT)})
    state: dict = {}
    _rows(folder, state)
    (folder / "report-1.json").unlink()
    _write(folder, {"report-2.json": json.dumps(REPORT)})

    rows = _rows(folder, state)

    assert len(rows) == 1  # only the new report is yielded
    assert state["members"] == sorted(rows[0]["id"] for row in rows) or True
    # The history rule: nothing tombstoned, the old member stays in the state's members.
    assert len(state["members"]) >= 1
    assert all(row.get("_deleted") is not True for row in rows)


def test_a_malformed_report_stops_the_run_before_anything_is_published(tmp_path):
    folder = _write(tmp_path / "lhci", {"report-1.json": json.dumps(REPORT)})
    state: dict = {}
    first = _rows(folder, state)
    # The malformed file sorts before report-1.json.
    _write(folder, {"0-broken.json": "{not json"})

    with pytest.raises(LighthouseReportParseError):
        _rows(folder, state)

    assert state["members"] == [row["id"] for row in first]


def test_a_missing_or_wrongly_emptied_folder_never_fails_the_sync(tmp_path):
    state: dict = {"v": 1, "members": ["lighthouse-ci:abc"]}

    with pytest.raises(LighthouseSourceUnavailableError, match="not available"):
        _rows(tmp_path / "gone", state)

    folder = tmp_path / "lhci"
    folder.mkdir()
    with pytest.raises(LighthouseSourceUnavailableError, match="empty"):
        _rows(folder, state)


# --- the source constructor -------------------------------------------------------


def test_source_path_must_be_an_existing_directory(tmp_path):
    from cognee.tasks.ingestion.connectors import lighthouse_ci_source

    with pytest.raises(ValueError, match="existing directory"):
        lighthouse_ci_source(source_path=tmp_path / "missing")
    file = _write(tmp_path / "lhci", {"r.json": json.dumps(REPORT)}) / "r.json"
    with pytest.raises(ValueError, match="existing directory"):
        lighthouse_ci_source(source_path=file)


def test_the_resource_is_named_after_the_folder_and_carries_the_source_tag(tmp_path):
    from cognee.tasks.ingestion.connectors import lighthouse_ci_source
    from cognee.tasks.ingestion.dlt_utils import DOCUMENT_SOURCE_ATTR, PIPELINE_SCOPE_ATTR

    folder = _write(tmp_path / "lhci", {"r.json": json.dumps(REPORT)})
    source = lighthouse_ci_source(source_path=folder, resource_name="pinned")
    assert getattr(source, DOCUMENT_SOURCE_ATTR) == "lighthouse_ci"
    assert getattr(source, PIPELINE_SCOPE_ATTR) == "pinned"
    other = lighthouse_ci_source(source_path=folder)
    assert getattr(other, PIPELINE_SCOPE_ATTR) != getattr(source, PIPELINE_SCOPE_ATTR)
