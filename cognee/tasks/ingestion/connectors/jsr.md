# JSR SDK connector

A JSR connector for [cognee](https://github.com/topoteretes/cognee): index TypeScript
packages — description, runtime compatibility, quality score, dependency counts and
version history — into memory through the public, zero-authentication JSR API
(api.jsr.io).

It exposes a `dlt` source you hand to `cognee.remember(...)`, reusing cognee's existing
DLT ingestion path. Each package becomes one document that goes through normal chunking
and LLM graph extraction, so an agent building TypeScript across Deno, Node.js, Bun and
Cloudflare Workers can ask "which of our JSR packages are not Node-compatible?" from
memory.

## Requirements

Bundled with the Cognee SDK. Needs network access to `api.jsr.io` and `requests` (already
a cognee dependency). No account, no API token for public packages.

**SDK only.** The connector has no entry in the integration provider table
(`cognee/modules/integrations/ingestion.py`): it reads a public registry, not a
credential-holding account. Full-scope listing and keyword search are out of scope for
this version; name the packages explicitly.

## Usage

```python
import cognee
from cognee.tasks.ingestion.connectors import jsr_source

source = jsr_source(package_names=["@std/path", "@std/csv"])
await cognee.remember(
    source,
    dataset_name="jsr_packages",
    primary_key="id",
    write_disposition="merge",  # REQUIRED, the add pipeline defaults to "replace"
    max_rows_per_table=0,
    self_improvement=False,
)
```

Or a single package: `jsr_source(package_names="@std/path")`.

| Argument | Meaning | Default |
|---|---|---|
| `package_names` | One `@scope/name` pair, or a list of them; duplicates are ingested once | required |
| `resource_name` | Names the staging table and the sync state; pass different names to sync different selections into one dataset | `jsr_packages` |
| `session` | Optional HTTP session for custom connections and test mocking | none |
| `check_active` | Host authorization checkpoint, called before each request and before each row is published | none |

A package's id is its qualified name (`jsr:package:@scope/name`), so re-syncing after a
new release updates the document in place. Every package is fetched before the first row
is published, so a run that fails mid-list publishes nothing and forgets nothing. A
package removed from the selection is forgotten on the next complete run — read
`source.cognee_sync_stats` (`synced`, `deleted`) afterwards. A package that does not
exist raises `JSRPackageNotFoundError` — an explicit name that misses is a caller error,
never silently skipped. HTTP 429 raises `JSRRateLimitError`.

## What is read

Two GETs per package — `https://api.jsr.io/scopes/{scope}/packages/{name}` and its
`/versions` — rendered as one document:

| Field | Value |
|---|---|
| id | `jsr:package:@scope/name` (the dlt primary key) |
| title | `JSR: @scope/name` |
| content | `# @scope/name`, Description, the runtime-compatibility lines (browser, Deno, Node.js, workerd, Bun — yes/no each), quality score, version/dependency/dependent counts, the archived flag, the GitHub repository, and the version history (version, author, date) |
| url | the package's jsr.io page |
| cognee_node_set | `jsr:package` |

## Error model

- `JSRPackageNotFoundError` — nothing lives at that URL (HTTP 404).
- `JSRRateLimitError` — HTTP 429; retry later.
- `JSRError` — network failure, a non-JSON payload, or a payload without package
  metadata / a version list.

Any of them propagates out of the run: a package that cannot be fetched is never silently
skipped, because a batch that silently drops packages looks like it succeeded.

## Privacy

Package metadata is public data. It is sent to your configured LLM and embedding
providers during cognify, like any document. The connector itself sends only the package
name to `api.jsr.io` and logs the name and latest version, nothing else.

## Graph shape

A package is one document: the description and compatibility matrix become text on it,
and the version history drives extraction around versions and authors. Packages named in
other documents merge into one node per qualified name.

## Testing

```bash
uv run pytest cognee/tests/unit/tasks/connectors/test_jsr_source.py
```

No network: the unit tests mock the HTTP session with recorded JSR payloads.
