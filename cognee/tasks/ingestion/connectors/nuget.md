# NuGet SDK connector

A NuGet data-source connector for [cognee](https://github.com/topoteretes/cognee): index
.NET packages — description, authors, license, target-framework dependency groups,
deprecation notices with their recommended replacements, and known vulnerability
advisories — into memory through a NuGet v3 feed, so a .NET platform team asks "which of
our NuGet dependencies are deprecated upstream, what replacements do their authors
recommend, and do any of our versions carry known advisories?" and gets the real registry
data.

It exposes a `dlt` source you hand to `cognee.remember(...)`, reusing cognee's existing
DLT ingestion path. Each package becomes one document that goes through normal chunking
and LLM graph extraction.

## Requirements

Bundled with the Cognee SDK. Needs network access to the feed host and `requests` (already
a cognee dependency). No account and no API key for api.nuget.org; private feeds that need
a token are out of scope for this version (point `service_index_url` at an anonymous
private feed such as a BaGet instance).

**SDK only.** The connector has no entry in the integration provider table
(`cognee/modules/integrations/ingestion.py`): it reads a public registry, not a
credential-holding account.

## Usage

```python
import cognee
from cognee.tasks.ingestion.connectors import nuget_source

source = nuget_source(package_ids=["Newtonsoft.Json", "WindowsAzure.Storage"])
await cognee.remember(
    source,
    dataset_name="dotnet_dependencies",
    primary_key="id",
    write_disposition="merge",  # REQUIRED, the add pipeline defaults to "replace"
    max_rows_per_table=0,
    self_improvement=False,
)
```

Or a single package: `nuget_source(package_ids="Newtonsoft.Json")`.

| Argument | Meaning | Default |
|---|---|---|
| `package_ids` | One package id, or a list of them; duplicates are ingested once | required |
| `search_query` | Optional search filter; matching ids (via the feed's SearchQueryService) are ingested alongside the explicit ids | none |
| `service_index_url` | The feed's v3 service index; point it at a private feed's `index.json` to sync from that feed | `https://api.nuget.org/v3/index.json` |
| `resource_name` | Names the staging table and the sync state; pass different names to sync different selections into one dataset | `nuget_packages` |
| `session` | Optional HTTP session for custom connections and test mocking | none |
| `check_active` | Host authorization checkpoint, called before each request and before each row is published | none |

## What is read

The registration base URL is resolved from the feed's service index
(`RegistrationsBaseUrl/Versioned` when offered, else `RegistrationsBaseUrl`) — no
hardcoded CDN hostnames. The registration index of each package is read; registration
pages with externalized leaves (large packages) are followed through their `@id`. One
document per package:

| Field | Value |
|---|---|
| id | `nuget:package:<id>` (the dlt primary key) |
| title | `NuGet: <id>` |
| content | `# <id>`, Description, Authors, License, Latest listed version, Project URL, the latest listed version's target-framework dependency groups, its deprecation notice (reasons, message, `Replacement: <id> (<range>)`), and the vulnerability advisories of every listed version with their severity |
| url | the package's `projectUrl`, else its nuget.org page |
| cognee_node_set | `nuget:package` |

Versions marked `listed: false` (unlisted) are skipped and stop contributing to the
document. Per-version catalog entries externalized behind a leaf URL are not followed in
this version — an externalized entry raises `NuGetError` rather than rendering an
incomplete document.

## Incremental sync and deletion

A package's id is its registry id, so re-syncing after a new release updates the document
in place; a package whose registration did not change renders the same row and costs no
re-cognition. Every package is fetched and rendered before the first row is published, so
a run that fails mid-list publishes nothing and forgets nothing. A package removed from
the selection is forgotten on the next complete run — read `source.cognee_sync_stats`
(`synced`, `deleted`) afterwards. A `commitTimeStamp`-based fetch cache (skipping the
page reads of an unchanged registration) is a natural follow-up.

## Error model

- `NuGetPackageNotFoundError` — nothing lives at that registration (HTTP 404), the
  registration carries no pages, or no listed version remains.
- `NuGetRateLimitError` — HTTP 429; retry later.
- `NuGetFetchError` — network failure.
- `NuGetError` — a non-JSON payload, a service index without a registration resource,
  an externalized catalog leaf, or a malformed page.

Any of them propagates out of the run: a package that cannot be fetched is never silently
skipped, because a batch that silently drops packages looks like it succeeded.

## Privacy

Package metadata is public data. It is sent to your configured LLM and embedding
providers during cognify, like any document. The connector itself sends only the package
ids (and the optional search query) to the feed and logs counts, nothing else.

## Graph shape

A package is one document: the description and license become text on it, dependency
names drive extraction around the dependency graph, and deprecation lines tie a package
to its recommended replacement. Packages named in other documents (a `.csproj`, a
README) merge into one node per package id.

## Testing

```bash
uv run pytest cognee/tests/unit/tasks/connectors/test_nuget_source.py
```

No network: the unit tests mock the HTTP session with recorded NuGet v3 payloads, and the
sync-and-forget test runs a real dlt pipeline over a temporary SQLite destination.
