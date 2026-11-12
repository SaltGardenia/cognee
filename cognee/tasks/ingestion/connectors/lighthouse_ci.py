"""Lighthouse CI connector for cognee: performance audit reports as searchable documents.

Lighthouse CI writes one JSON report per audited URL per run, and the findings sit apart
from the project knowledge an agent can retrieve. This source walks a folder of reports
(``lhci collect`` output and Lighthouse's filesystem export), validates each one and
publishes one concise document per report — category scores, the performance metrics and
the actionable findings with their savings — with a content-hash id, so an unchanged report
is not re-cognified. Raw traces, screenshots and other diagnostic payloads are never
carried, and query parameters are stripped from URLs: they carry session tokens more often
than not. Reports are historical records: a report absent from a later import is never
forgotten, so audit runs stay comparable. See ``lighthouse_ci.md``.
"""

import hashlib
import json
import os
import re
from collections.abc import Callable, Iterator
from pathlib import Path, PurePosixPath
from typing import Any
from urllib.parse import urldefrag, urlparse, urlunparse

from cognee.shared.logging_utils import get_logger
from cognee.tasks.ingestion import dlt_utils
from cognee.tasks.ingestion.dlt_utils import (
    DOCUMENT_SOURCE_ATTR,
    PIPELINE_SCOPE_ATTR,
)

logger = get_logger("lighthouse_ci_connector")

# The metrics a regression hunt cares about, in the order they render.
_PERFORMANCE_METRICS = (
    "first-contentful-paint",
    "largest-contentful-paint",
    "total-blocking-time",
    "cumulative-layout-shift",
    "speed-index",
    "interactive",
)
# A finding is an audit that scored below this (0..1) or that reports savings.
_FINDING_SCORE_BELOW = 0.9

_QUERY_PARAM_REMOVED = re.compile(r"\?.*\Z")


class LighthouseSourceError(RuntimeError):
    """Base error. Messages carry folder-relative paths only, never report text."""


class LighthouseSourceUnavailableError(LighthouseSourceError):
    """The report folder is gone at run time, or it is empty though reports were synced."""


class LighthouseReportParseError(LighthouseSourceError):
    """A file is not a Lighthouse report; the run stops before anything is published."""


def _sanitize_url(url: str) -> str:
    """The audited URL without its query string and fragment.

    Lighthouse URLs are the pages as audited, and CI often appends cache busters, session
    tokens and preview parameters. The identity of the page is scheme, host and path; the
    query is dropped whole rather than filtered key by key, because a new key is one
    deploy away. The fragment is display-only and goes too.
    """
    without_fragment = urldefrag(url)[0]
    parsed = urlparse(without_fragment)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", "", ""))


def _load_report(text: str) -> dict[str, Any]:
    """The Lighthouse report of one file, as a dict.

    Accepts a plain Lighthouse report and the ``{"data": {...}}`` wrapper the LHCI server
    API writes. Anything without a report shape is malformed.
    """
    try:
        payload = json.loads(text)
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise LighthouseReportParseError("not valid JSON") from error
    if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
        payload = payload["data"]
    if not isinstance(payload, dict) or not isinstance(payload.get("audits"), dict):
        raise LighthouseReportParseError("no audits object: not a Lighthouse report")
    return payload


def _score_line(audit: dict[str, Any]) -> str | None:
    """``1.2 s (score 0.90)`` from an audit's display and numeric value."""
    parts = []
    display = audit.get("displayValue")
    if isinstance(display, str) and display.strip():
        parts.append(display.strip())
    score = audit.get("score")
    if isinstance(score, (int, float)) and 0 <= score <= 1:
        parts.append(f"(score {score:.2f})")
    return " ".join(parts) or None


def _category_lines(report: dict[str, Any]) -> list[str]:
    """One ``<title>: <score out of 100>`` line per scored category, file order."""
    lines = []
    categories = report.get("categories") or {}
    for key, category in categories.items():
        if not isinstance(category, dict):
            continue
        score = category.get("score")
        if not isinstance(score, (int, float)):
            continue
        title = category.get("title") or key.replace("-", " ").title()
        lines.append(f"- {title}: {round(score * 100)}")
    return lines


def _metric_lines(report: dict[str, Any]) -> list[str]:
    """The performance metrics the regression hunt cares about, where the report has them."""
    audits = report["audits"]
    lines = []
    for metric in _PERFORMANCE_METRICS:
        audit = audits.get(metric)
        if not isinstance(audit, dict):
            continue
        rendered = _score_line(audit)
        title = audit.get("title") or metric.replace("-", " ").title()
        lines.append(f"- {title}: {rendered}" if rendered else f"- {title}")
    return lines


def _finding_lines(report: dict[str, Any], max_findings: int | None) -> list[str]:
    """The actionable findings: audits that failed or that report savings.

    Ordered by score ascending, so the worst finding comes first; a cap bounds the
    document size, and 0 means unbounded. Audits that are informational (score null,
    no savings) are not findings.
    """
    findings = []
    for key, audit in report["audits"].items():
        if not isinstance(audit, dict):
            continue
        score = audit.get("score")
        details = audit.get("details") or {}
        savings_ms = details.get("overallSavingsMs")
        savings_bytes = details.get("overallSavingsBytes")
        scored_low = isinstance(score, (int, float)) and score < _FINDING_SCORE_BELOW
        has_savings = (
            isinstance(savings_ms, (int, float))
            and savings_ms > 0
            or (isinstance(savings_bytes, (int, float)) and savings_bytes > 0)
        )
        if not (scored_low or has_savings):
            continue
        title = audit.get("title") or key
        lines = [f"- [{key}] {title}"]
        rendered = _score_line(audit)
        if rendered:
            lines.append(f"  {rendered}")
        savings = []
        if isinstance(savings_ms, (int, float)) and savings_ms > 0:
            savings.append(f"{int(savings_ms)} ms")
        if isinstance(savings_bytes, (int, float)) and savings_bytes > 0:
            savings.append(f"{int(savings_bytes) / 1024:.0f} KiB")
        if savings:
            lines.append(f"  estimated savings: {' + '.join(savings)}")
        findings.append((score if isinstance(score, (int, float)) else 1.0, "\n".join(lines)))

    findings.sort(key=lambda finding: (finding[0], finding[1]))
    if max_findings is not None and max_findings > 0:
        findings = findings[:max_findings]
    return [finding[1] for finding in findings]


def _document_text(report: dict[str, Any]) -> str:
    """One report's markdown body: provenance, scores, metrics and findings."""
    url = _sanitize_url(
        report.get("finalDisplayedUrl")
        or report.get("finalUrl")
        or report.get("requestedUrl")
        or ""
    )
    lines = [f"# Lighthouse: {url or 'unknown page'}"]
    if url:
        lines.append(f"URL: {url}")
    fetched = report.get("fetchTime")
    if isinstance(fetched, str) and fetched:
        lines.append(f"Fetched: {fetched}")
    version = report.get("lighthouseVersion")
    if isinstance(version, str) and version:
        lines.append(f"Lighthouse: {version}")
    build = report.get("buildContext") or {}
    if isinstance(build, dict) and build.get("commitHash"):
        commit = str(build.get("commitHash"))
        lines.append(f"Build: {commit[:12]}")

    scores = _category_lines(report)
    if scores:
        lines.append("\n## Scores")
        lines.extend(scores)
    metrics = _metric_lines(report)
    if metrics:
        lines.append("\n## Metrics")
        lines.extend(metrics)
    findings = _finding_lines(report, max_findings=None)
    if findings:
        lines.append("\n## Findings")
        lines.extend(findings)
    return "\n".join(lines)


def _report_id(url: str, body: str) -> str:
    host_path = urlparse(url).netloc + urlparse(url).path if url else "unknown"
    slug = _QUERY_PARAM_REMOVED.sub("", host_path).strip("/").replace("/", "-") or "unknown"
    return f"lighthouse-ci:{slug}:{hashlib.sha256(body.encode()).hexdigest()[:12]}"


def _list_files(root: Path) -> list[str]:
    """Root-relative posix paths of the ``*.json`` files to ingest, sorted.

    The same listing rules as the Obsidian source: regular files only, nothing hidden, no
    symlinks followed. A folder that cannot be listed raises, so an unreadable root never
    looks like a deleted set of reports.
    """

    def refuse(error: OSError) -> None:
        rel = os.path.relpath(error.filename, root) if error.filename else error.strerror
        raise LighthouseSourceUnavailableError(
            f"Could not list the folder {rel!r} ({type(error).__name__}); "
            "nothing was synced and nothing was forgotten."
        ) from None

    files = []
    for directory, subdirs, names in os.walk(root, onerror=refuse):
        subdirs[:] = [name for name in subdirs if not name.startswith(".")]
        for name in names:
            path = os.path.join(directory, name)
            if (
                name.lower().endswith(".json")
                and not name.startswith(".")
                and not os.path.islink(path)
                and os.path.isfile(path)
            ):
                files.append(Path(path).relative_to(root).as_posix())
    return sorted(files)


def _iter_rows(
    root: Path,
    state: dict,
    stats: dict[str, int],
    check_active: Callable[[], None] | None = None,
) -> Iterator[dict[str, Any]]:
    """Yield one document per valid report. Historical records: no tombstones, ever.

    Every file is validated before the first row is yielded, so a malformed report stops
    the run with nothing published. A report absent from a later import keeps its
    document -- audit history is what makes regression hunts possible -- and an unchanged
    report file makes the same id, so nothing is re-cognified.
    """
    if not root.is_dir():
        raise LighthouseSourceUnavailableError(
            "The report folder is not available; nothing was synced and nothing was "
            "forgotten. Check that the drive is mounted or point source_path at the new "
            "location."
        )
    listed = _list_files(root)
    if state.get("members") and not os.listdir(root):
        raise LighthouseSourceUnavailableError(
            "The report folder is empty, but earlier runs synced reports from it; nothing "
            "was synced and nothing was forgotten. A drive that did not mount looks exactly "
            "like this. Check the mount."
        )

    rows: list[dict[str, Any]] = []
    present: set[str] = set()
    for rel_path in listed:
        if check_active is not None:
            check_active()  # outside any try: a revoked connection is not a failed file
        raw = (root / rel_path).read_bytes()
        try:
            text = raw.decode("utf-8-sig")
        except UnicodeDecodeError as error:
            raise LighthouseReportParseError("not UTF-8 text") from error
        report = _load_report(text)
        body = _document_text(report)
        url = _sanitize_url(
            report.get("finalDisplayedUrl")
            or report.get("finalUrl")
            or report.get("requestedUrl")
            or ""
        )
        rows.append(
            {
                "id": _report_id(url, body),
                "title": f"Lighthouse: {PurePosixPath(rel_path).stem}",
                "content": body,
                "url": url,
                "_deleted": False,
            }
        )
        present.add(rows[-1]["id"])

    for row in rows:
        yield row
        # After the yield: a row that check_active stopped before it was published is not counted.
        stats["synced"] += 1
    # Historical records: the reports an earlier run synced are never tombstoned. The
    # state holds the ids for bookkeeping only.
    state.update(v=1, members=sorted(present | set(state.get("members", []))))
    logger.info(
        "Lighthouse CI: %d report(s) synced, %d historical record(s) kept.",
        stats["synced"],
        len(state["members"]) - len(present),
    )


def lighthouse_ci_source(
    *,
    source_path: str | os.PathLike[str],
    resource_name: str | None = None,
    check_active: Callable[[], None] | None = None,
):
    """Return a ``dlt`` resource that yields one document per Lighthouse report of a folder.

    Hand the result to ``cognee.remember(...)`` with ``write_disposition="merge"`` and
    ``primary_key="id"``. A run walks the whole folder; a report whose content did not
    change makes the same row id, so nothing is re-cognified. Reports are historical
    records: a report absent from a later import is never forgotten.

    Args:
        source_path: The folder walked recursively, holding the JSON reports of ``lhci
            collect`` or Lighthouse's filesystem export. A ``ValueError`` if it does not
            exist. A run raises if it has disappeared by then.
        resource_name: Names the staging table and the sync state. The default is derived
            from the resolved path, so moving the folder changes it and ingests everything
            again; pass a name to pin it. Two folders in one dataset need different names.
        check_active: Host authorization checkpoint, called before each file is read and
            before each row is published.
    """
    import dlt

    if getattr(dlt_utils, "DOCUMENT_SYNC_VERSION", 0) < 3:
        raise RuntimeError(
            "Lighthouse CI sync requires a Cognee build that reads per-row node sets and "
            "structure (DOCUMENT_SYNC_VERSION >= 3). Upgrade Cognee before syncing."
        )
    root = Path(source_path).expanduser().resolve()
    if not root.is_dir():
        raise ValueError("source_path must be an existing directory of Lighthouse JSON reports.")
    if resource_name is None:
        resource_name = f"lighthouse_ci_{hashlib.sha256(str(root).encode()).hexdigest()[:12]}"
    elif not isinstance(resource_name, str) or not resource_name.strip():
        raise ValueError("resource_name must be a non-empty string.")
    stats: dict[str, int] = {}

    @dlt.resource(
        name=resource_name,
        primary_key="id",
        write_disposition="merge",
        columns={"_deleted": {"data_type": "bool", "hard_delete": True}},
    )
    def lighthouse_reports():
        stats.update(synced=0, deleted=0)
        rows = _iter_rows(root, dlt.current.resource_state(), stats, check_active)
        yield from dlt_utils.guarded_rows(rows, check_active)

    resource = lighthouse_reports()
    setattr(resource, DOCUMENT_SOURCE_ATTR, "lighthouse_ci")
    setattr(resource, PIPELINE_SCOPE_ATTR, resource_name)
    resource.cognee_sync_stats = stats
    return resource
