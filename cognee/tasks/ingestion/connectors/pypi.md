# PyPI SDK connector

A PyPI data-source connector for [cognee](https://github.com/topoteretes/cognee): index
Python packages — their metadata and full README documentation — into memory through the
official, zero-authentication PyPI JSON API, so a coding assistant answers questions about
its dependencies from the real documentation.

It exposes a `dlt` source you hand to `cognee.remember(...)`, reusing cognee's existing
DLT ingestion path. Each package becomes one document that goes through normal chunking
and LLM graph extraction.

## Requirements

Bundled with the Cognee SDK. Needs network access to `pypi.org` and `requests` (already a
cognee dependency). No account, no API key: the PyPI JSON API is public.

**SDK only.** The connector has no entry in the integration provider table
(`cognee/modules/integrations/ingestion.py`): it reads a public registry, not a
credential-holding account.

## Usage

```python
import cognee
from cognee.tasks.ingestion.connectors import pypi_source

source = pypi_source(package_name=["fastapi", "pydantic", "cognee"])
await cognee.remember(
    source,
    dataset_name="python_dependencies",
    primary_key="id",
    write_disposition="merge",  # REQUIRED, the add pipeline defaults to "replace"
    max_rows_per_table=0,
    self_improvement=False,
)
```

Or a single package: `pypi_source(package_name="fastapi")`.

| Argument | Meaning | Default |
|---|---|---|
| `package_name` | One package name, or a list of them; duplicates are ingested once | required |
| `resource_name` | Names the staging table and the sync state; pass different names to sync different selections into one dataset | `pypi_packages` |
| `session` | Optional HTTP session for custom connections and test mocking | none |
| `check_active` | Host authorization checkpoint, called before each request and before each row is published | none |

A package's id is its name (`pypi:package:<name>`), so re-syncing after a new release
updates the document in place instead of duplicating it. Every package is fetched before
the first row is published, so a run that fails mid-list publishes nothing and forgets
nothing. A package removed from the selection is forgotten on the next complete run —
read `source.cognee_sync_stats` (`synced`, `deleted`) afterwards. A package that does not
exist raises `PyPIPackageNotFoundError` — an explicit name that misses is a caller error,
never silently skipped. HTTP 429 raises `PyPIRateLimitError`; the API is rate-limited per
IP, so very large batches should pace themselves.

## What is read

One document per package, from the `/pypi/{package}/json` payload:

| Field | Value |
|---|---|
| id | `pypi:package:<name>` (the dlt primary key) |
| title | `PyPI: <name>` |
| content | `# <name>`, then Version, Summary, Author, Email, License, Requires Python, Keywords, the project's URLs, and the full `description` (the README, Markdown or reST) |
| url | the project's `Homepage` project URL, else `home_page`, else the PyPI project page |
| cognee_node_set | `pypi:package` |

Version history (`releases`) is not rendered per version — the document describes the
latest release, which is what a code assistant needs; the payload's `releases` and `urls`
fields are ignored.

## Error model

- `PyPIPackageNotFoundError` — the package does not exist (HTTP 404).
- `PyPIRateLimitError` — HTTP 429; retry later.
- `PyPIError` — network failure, a non-JSON payload, or a payload without an `info`
  object.

Any of them propagates out of the run: a package that cannot be fetched is never silently
skipped, because a batch that silently drops packages looks like it succeeded.

## Privacy

Package documentation is public data. It is sent to your configured LLM and embedding
providers during cognify, like any document. The connector itself sends only the package
name to `pypi.org` and logs the name and version, nothing else.

## Graph shape

A package is one document: the summary, license and keywords become text on it, project
URLs are text lines, and the README's code examples and API descriptions drive entity
extraction. Packages named in other documents (a requirements file, a doc page) merge into
one node per package name.

## Testing

```bash
uv run pytest cognee/tests/unit/tasks/connectors/test_pypi_source.py
```

No network: the unit tests mock the HTTP session with recorded PyPI payloads.
