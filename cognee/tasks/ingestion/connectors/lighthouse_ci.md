# Lighthouse CI SDK connector

A Lighthouse CI connector for [cognee](https://github.com/topoteretes/cognee): sync the
JSON audit reports of a folder into memory as concise, searchable documents, with
forget-off (reports are historical records), content-hash dedup and no credentials.

It exposes a `dlt` source you hand to `cognee.remember(...)`, reusing cognee's existing DLT
ingestion path. Each report becomes one document — category scores, the performance
metrics, and the actionable findings with their estimated savings — so a developer hunting
a regression can ask "which runs scored low on accessibility?" or "what keeps costing us
JavaScript bytes?" without opening JSON files.

## Requirements

Bundled with the Cognee SDK, no extra install. No account, no token, no network: the
connector reads report files from the machine that runs it — the `.lighthouseci` folder
`lhci collect` writes, or Lighthouse's filesystem export (`lighthouse --output=json
--output-path=./report.json`).

**SDK only.** The connector has no entry in the integration provider table
(`cognee/modules/integrations/ingestion.py`): nothing hosted can reach a folder on your
disk. The hosted LHCI server API is out of scope for this version.

## Usage

```python
import cognee
from cognee.tasks.ingestion.connectors import lighthouse_ci_source

source = lighthouse_ci_source(source_path=".lighthouseci")
await cognee.remember(
    source,
    dataset_name="lighthouse_reports",
    primary_key="id",
    write_disposition="merge",  # REQUIRED, the add pipeline defaults to "replace"
    max_rows_per_table=0,
    self_improvement=False,
)

# Counts only: reports published by this run. Nothing is ever forgotten by a sync.
assert source.cognee_sync_stats["deleted"] == 0
```

Run the same call again whenever new reports land. An unchanged report file makes the same
row id and is not re-cognified.

| Argument | Meaning | Default |
|---|---|---|
| `source_path` | The folder walked recursively, holding the JSON reports. `ValueError` unless it is an existing directory | required |
| `resource_name` | Names this folder: it keys the staging table and the sync state | `lighthouse_ci_<12 hex of the sha256 of the resolved folder>` |
| `check_active` | Host checkpoint, called before each file is read and before each row is published | none |

All arguments are keyword-only. After a run, `source.cognee_sync_stats` holds `synced`
(reports published); `deleted` stays 0 — see the historical-records rule below.

## What is read

Every `*.json` file under the folder, case-insensitive on the suffix. A plain Lighthouse
report and the `{"data": {...}}` wrapper the LHCI server API writes are both accepted.
Anything without a report shape (no `audits` object) is malformed and stops the run.

One report becomes one document:

| Field | Value |
|---|---|
| id | `lighthouse-ci:<host-path-slug>:<12 hex of the sha256 of the rendered body>` (the dlt primary key) |
| title | `Lighthouse: <report file name>` |
| content | provenance, scores, metrics and findings, as below |
| url | the audited URL with its query string and fragment removed |

The document body carries, when the report has them:

- **Provenance**: the audited URL, `fetchTime`, `lighthouseVersion`, and the first 12
  characters of `buildContext.commitHash` when build metadata was supplied.
- **Scores**: one `<title>: <score out of 100>` line per scored category.
- **Metrics**: First Contentful Paint, Largest Contentful Paint, Total Blocking Time,
  Cumulative Layout Shift, Speed Index, Time to Interactive — display value and score,
  in that order.
- **Findings**: every audit that scored below 0.9 or that reports estimated savings
  (`overallSavingsMs` / `overallSavingsBytes`), worst first, each with its audit
  identifier, title, score and savings.

**What is excluded**: the raw trace, screenshots, stacks and per-item diagnostic details
never leave the report file — only the identifiers, titles, scores and savings above are
carried, so a document stays concise and holds nothing sensitive beyond the scores.

**URL sanitization.** Query string and fragment are removed from the audited URL before
anything is stored: CI URLs carry cache busters, session tokens and preview parameters,
and the page's identity is scheme, host and path. The query is dropped whole rather than
filtered key by key, because a new key is one deploy away.

**Historical records.** A report absent from a later import is never tombstoned, and an
earlier import is never rolled back: audit runs exist to be compared, and the issue is
explicit that deleting history because a file disappeared would destroy it. Remove a
report from memory with `cognee.forget(data_id=...)` or a scoped `forget(dataset=...)`.

## Incremental model

There is no cursor. Every run walks the whole folder, validates every file and publishes a
row per report. The id carries the hash of the rendered body, so an unchanged report is
recognized and nothing is extracted again — a quiet sync costs the reads and no LLM calls,
even though every report is re-yielded. File times are never used.

The sync state is kept in `dlt.current.resource_state()` and holds the ids ever published
(`members`, sorted) and a format version (`v`); it is written after the last row, so a run
that raises keeps the old state. It is bookkeeping for the kept-history count in the log.

## Failure model

Every file is validated before the first row is published, so a malformed report (not
JSON, not UTF-8, no `audits` object) raises `LighthouseReportParseError` with nothing
published. If the folder is not a directory at run time, or is empty while earlier runs
synced from it (a drive that did not mount), the run raises
`LighthouseSourceUnavailableError` the same way. A folder that cannot be listed raises.
None of these forget anything.

## Privacy

Nothing is read until you construct a source and call `remember`. The rendered findings
become cognee documents and, during cognify, are sent to your configured LLM and embedding
providers like any document — check that you may send them there, or use local models.
Raw traces and screenshots stay in the report files; nothing reads them. Logs, errors and
`cognee_sync_stats` carry counts, classes and folder-relative paths, never an absolute
path or report content.

## Graph shape

A report is one document whose text carries the scores, metrics and findings; the audited
host and path appear in the document title line and URL column. Runs of one URL are
separate documents with separate timestamps, so an agent can compare them by asking for
the page and reading the `Fetched:` lines.

## Testing

```bash
uv run pytest cognee/tests/unit/tasks/connectors/test_lighthouse_ci_source.py
```

No Lighthouse install needed: the tests build report JSON files in a temporary folder.
