# Packagist SDK connector

A Packagist connector for [cognee](https://github.com/topoteretes/cognee): index Composer
packages — description, latest versions per major line, license, authors, and the
abandoned flag with its recommended replacement — into memory through the public,
zero-authentication Composer v2 metadata API.

It exposes a `dlt` source you hand to `cognee.remember(...)`, reusing cognee's existing
DLT ingestion path. Each package becomes one document that goes through normal chunking
and LLM graph extraction, so "which of our Composer packages are abandoned, and what did
the author recommend instead?" is one search.

## Requirements

Bundled with the Cognee SDK. Needs network access to `repo.packagist.org` and `requests`
(already a cognee dependency). No account, no API key. Packagist's API policy asks for a
descriptive User-Agent, which the client sends.

**SDK only.** The connector has no entry in the integration provider table
(`cognee/modules/integrations/ingestion.py`): it reads a public registry, not a
credential-holding account. Private Packagist is out of scope for this version.

## Usage

```python
import cognee
from cognee.tasks.ingestion.connectors import packagist_source

source = packagist_source(package_names=["laravel/framework", "symfony/console"])
await cognee.remember(
    source,
    dataset_name="composer_dependencies",
    primary_key="id",
    write_disposition="merge",  # REQUIRED, the add pipeline defaults to "replace"
    max_rows_per_table=0,
    self_improvement=False,
)
```

Or a single package: `packagist_source(package_names="laravel/framework")`.

| Argument | Meaning | Default |
|---|---|---|
| `package_names` | One `vendor/package` pair, or a list of them; duplicates are ingested once | required |
| `resource_name` | Names the staging table and the sync state; pass different names to sync different selections into one dataset | `packagist_packages` |
| `session` | Optional HTTP session for custom connections and test mocking | none |
| `check_active` | Host authorization checkpoint, called before each request and before each row is published | none |

A package's id is its qualified name (`packagist:package:<vendor>/<name>`), so re-syncing
after a new release updates the document in place. Every package is fetched before the
first row is published, so a run that fails mid-list publishes nothing and forgets
nothing. A package removed from the selection is forgotten on the next complete run —
read `source.cognee_sync_stats` (`synced`, `deleted`) afterwards. A package that does not
exist raises `PackagistPackageNotFoundError` — an explicit name that misses is a caller
error, never silently skipped. HTTP 429 raises `PackagistRateLimitError`.

## What is read

One document per package, from `https://repo.packagist.org/p2/{vendor}/{package}.json`:

| Field | Value |
|---|---|
| id | `packagist:package:<vendor>/<name>` (the dlt primary key) |
| title | `Packagist: <vendor>/<name>` |
| content | `# <name>`, Description, the abandonment line when set, then one `## Version <v>` section per major-version head (Released, License, Authors, Homepage, Keywords, Requires PHP) |
| url | the package's homepage, else the Packagist project page |
| cognee_node_set | `packagist:package` |

**Abandonment.** Packagist records `abandoned: true` or `abandoned: {"replacement":
"..."}` on the newest version entry. The document renders either the bare warning or the
recommended replacement — the question the connector exists to answer.

**Versions.** The Composer v2 payload lists the head of every version constraint line,
newest first. The document renders one section per distinct major-version head, so a
project pinned to an older major still finds its line's metadata; patch releases of one
major collapse into their head.

## Error model

- `PackagistPackageNotFoundError` — the vendor/package pair does not exist (HTTP 404).
- `PackagistRateLimitError` — HTTP 429; retry later.
- `PackagistError` — network failure, a non-JSON payload, or a payload without package
  metadata.

Any of them propagates out of the run: a package that cannot be fetched is never silently
skipped, because a batch that silently drops packages looks like it succeeded.

## Privacy

Package metadata is public data. It is sent to your configured LLM and embedding
providers during cognify, like any document. The connector itself sends only the package
name to `repo.packagist.org` and logs the name, nothing else.

## Graph shape

A package is one document: description, license and keywords become text on it, and the
abandonment line drives entity extraction around the replacement package. Packages named
in other documents (a `composer.json`, a doc page) merge into one node per qualified
name.

## Testing

```bash
uv run pytest cognee/tests/unit/tasks/connectors/test_packagist_source.py
```

No network: the unit tests mock the HTTP session with recorded Composer v2 payloads.
