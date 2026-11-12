# JSON Feed SDK connector

A JSON Feed data-source connector for [cognee](https://github.com/topoteretes/cognee):
index JSON Feed (https://jsonfeed.org/) feeds — blogs, changelogs, newsletters — into
memory through plain HTTP, no account and no API key, so an agent answers "what did we
publish about the new release last week?" from memory.

It exposes a `dlt` source you hand to `cognee.remember(...)`, reusing cognee's existing
DLT ingestion path. Each feed item becomes one document that goes through normal chunking
and LLM graph extraction.

## Requirements

Bundled with the Cognee SDK. Needs network access to the feed host and `requests` (already
a cognee dependency). No account, no API key: JSON Feed is a public document format
(`application/feed+json`). Authenticated feeds are out of scope for this version.

**SDK only.** The connector has no entry in the integration provider table
(`cognee/modules/integrations/ingestion.py`): it reads public feeds, not a
credential-holding account.

## Usage

```python
import cognee
from cognee.tasks.ingestion.connectors import jsonfeed_source

source = jsonfeed_source(feed_urls=["https://example.com/feed.json"])
await cognee.remember(
    source,
    dataset_name="blog_posts",
    primary_key="id",
    write_disposition="merge",  # REQUIRED, the add pipeline defaults to "replace"
    max_rows_per_table=0,
    self_improvement=False,
)
```

Or a single feed: `jsonfeed_source(feed_urls="https://example.com/feed.json")`.

| Argument | Meaning | Default |
|---|---|---|
| `feed_urls` | One http(s) feed URL, or a list of them; duplicates are fetched once | required |
| `resource_name` | Names the staging table and the sync state; two feed selections in one dataset need different names | derived from the feed URLs |
| `session` | Optional HTTP session for custom connections and test mocking | none |
| `check_active` | Host authorization checkpoint, called before each request and before each row is published | none |

## What is read

Every `items[]` entry of every feed becomes one document:

| Field | Value |
|---|---|
| id | `jsonfeed:item:<feed digest>:<item id>` — the feed's own `id` prefixed with the feed URL's digest so two feeds never collide; a feed item without an id gets a hash of its url and title |
| title | `JSON Feed: <item title>` (falling back to the item's url) |
| content | `Feed: <feed title> (<url>)` header, then `# <title>`, Authors, Tags, Published, Modified, and the item's `content_text` or its `content_html` stripped to text |
| url | the item's `url`, else its `external_url` |
| cognee_node_set | `jsonfeed:item` |

A 1.1 feed's `next_url` chain is followed (with loop detection), so paginated feeds are
read whole. Items missing both `content_text` and `content_html` are stored with their
metadata only. `attachments` are not downloaded.

## Incremental sync and deletion

A run walks every feed whole; an item whose content did not change makes the same row id,
so nothing is re-cognified. Feeds are sliding windows: an item absent from a later
complete run is forgotten (its document is removed), which matches the merge +
`_deleted` hard-delete contract the other document connectors follow. Every feed is
fetched and parsed before the first row is published, so a run that fails mid-way
publishes nothing and forgets nothing — a partial snapshot must not look like a
deletion. Read `source.cognee_sync_stats` (`synced`, `deleted`) afterwards.

## Error model

- `JSONFeedFetchError` — network failure or an HTTP error on a feed URL.
- `JSONFeedParseError` — the payload is not JSON, is not a JSON Feed 1.x document (no
  `version` starting with `1` and no `items` list), or an item object is malformed.
- `JSONFeedError` — base class.

Any of them propagates out of the run: a feed that cannot be read is never silently
skipped, because a batch that silently drops feeds looks like it succeeded.

## Privacy

Feed content is public data. It is sent to your configured LLM and embedding providers
during cognify, like any document. The connector itself sends only the feed URLs to the
feed hosts and logs counts, nothing else.

## Graph shape

An item is one document: the title and tags anchor the topic, author names become person
nodes, and the content's prose drives entity extraction. Items of one feed carry the
same `Feed:` header, so a feed's posts join into one neighborhood.

## Testing

```bash
uv run pytest cognee/tests/unit/tasks/connectors/test_jsonfeed_source.py
```

No network: the unit tests mock the HTTP session with recorded JSON Feed payloads and
run a real dlt pipeline over a temporary SQLite destination.
