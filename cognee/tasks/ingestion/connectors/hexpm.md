# Hex.pm SDK connector

A Hex.pm connector for [cognee](https://github.com/topoteretes/cognee): index BEAM
packages — description, licenses, links, downloads, the latest release's dependency
requirements, and retirement and security-advisory notices — into memory through the
public, zero-authentication Hex.pm API.

It exposes a `dlt` source you hand to `cognee.remember(...)`, reusing cognee's existing
DLT ingestion path. Each package becomes one document that goes through normal chunking
and LLM graph extraction, so "have any of our Hex packages had releases retired for
security reasons, and what OTP/Elixir requirements does the latest version need?" is one
search.

## Requirements

Bundled with the Cognee SDK. Needs network access to `hex.pm` and `requests` (already a
cognee dependency). No account, no API key for public packages; private Hex organizations
are out of scope for this version.

**SDK only.** The connector has no entry in the integration provider table
(`cognee/modules/integrations/ingestion.py`): it reads a public registry, not a
credential-holding account.

## Usage

```python
import cognee
from cognee.tasks.ingestion.connectors import hexpm_source

source = hexpm_source(package_names=["phoenix", "ecto", "plug"])
await cognee.remember(
    source,
    dataset_name="hex_packages",
    primary_key="id",
    write_disposition="merge",  # REQUIRED, the add pipeline defaults to "replace"
    max_rows_per_table=0,
    self_improvement=False,
)
```

Or a single package: `hexpm_source(package_names="phoenix")`.

| Argument | Meaning | Default |
|---|---|---|
| `package_names` | One package name, or a list of them; duplicates are ingested once | required |
| `resource_name` | Names the staging table and the sync state; pass different names to sync different selections into one dataset | `hexpm_packages` |
| `session` | Optional HTTP session for custom connections and test mocking | none |
| `check_active` | Host authorization checkpoint, called before each request and before each row is published | none |

A package's id is its name (`hexpm:package:<name>`), so re-syncing after a new release
updates the document in place. Every package is fetched before the first row is
published, so a run that fails mid-list publishes nothing and forgets nothing. A package
removed from the selection is forgotten on the next complete run — read
`source.cognee_sync_stats` (`synced`, `deleted`) afterwards. A package that does not
exist raises `HexPackageNotFoundError` — an explicit name that misses is a caller error,
never silently skipped. HTTP 429 raises `HexRateLimitError`.

## What is read

Two GETs per package with a stable latest version — `https://hex.pm/api/packages/{name}`
and the latest stable release's detail — rendered as one document:

| Field | Value |
|---|---|
| id | `hexpm:package:<name>` (the dlt primary key) |
| title | `Hex.pm: <name>` |
| content | `# <name>`, Description, License, the meta links, Downloads, Latest stable version, the organization when the package is private, the latest release's dependency requirements (`- plug ~> 1.14`, optional ones marked), retirement notices, and the security advisories with their identifiers and titles |
| url | the package's hex.pm page |
| cognee_node_set | `hexpm:package` |

A package with no stable version renders without the requirements section. Per-version
details of every historical release are not rendered — the document describes the latest
stable release, which is what the dependency question needs; the payload's full `releases`
list is used only to reach the latest one.

## Error model

- `HexPackageNotFoundError` — nothing lives at that URL (HTTP 404).
- `HexRateLimitError` — HTTP 429; retry later.
- `HexError` — network failure, a non-JSON payload, or a payload without package metadata
  or release detail.

Any of them propagates out of the run: a package that cannot be fetched is never silently
skipped, because a batch that silently drops packages looks like it succeeded.

## Privacy

Package metadata is public data. It is sent to your configured LLM and embedding
providers during cognify, like any document. The connector itself sends only the package
name to `hex.pm` and logs the name and latest stable version, nothing else.

## Graph shape

A package is one document: the description and licenses become text on it, dependency
requirements drive extraction around the dependency names, and retirement/advisory lines
carry the security context. Packages named in other documents (a `mix.exs`, a doc page)
merge into one node per package name.

## Testing

```bash
uv run pytest cognee/tests/unit/tasks/connectors/test_hexpm_source.py
```

No network: the unit tests mock the HTTP session with recorded Hex.pm payloads.
