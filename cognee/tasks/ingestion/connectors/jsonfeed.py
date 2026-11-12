"""JSON Feed connector for cognee: JSON Feed (https://jsonfeed.org/) entries as documents.

Blogs, changelogs and newsletters increasingly publish JSON Feed alongside or instead of
RSS. This source fetches one or more feed URLs, renders the feed's items as documents —
title, authors, tags, publish dates, url and the item's ``content_text`` or, failing that,
its ``content_html`` stripped to text — and follows a 1.1 feed's ``next_url`` chain. See
``jsonfeed.md``.
"""

import hashlib
import html
import json
import re
from collections.abc import Callable, Iterator
from typing import Any
from urllib.parse import urlparse, urlunparse

from cognee.shared.logging_utils import get_logger
from cognee.tasks.ingestion import dlt_utils
from cognee.tasks.ingestion.dlt_utils import (
    DOCUMENT_SOURCE_ATTR,
    NODE_SET_COLUMN,
    PIPELINE_SCOPE_ATTR,
)

logger = get_logger("jsonfeed_connector")

_TIMEOUT = (10, 30)
# Block-level closings and <br> end a paragraph; the rest is inline markup to drop.
_BLOCK_TAGS = re.compile(r"(?i)</(?:p|div|li|h[1-6]|tr|blockquote)>|<br\s*/?>")
_TAG = re.compile(r"<[^>]+>")
_WHITESPACE = re.compile(r"[ \t\f\v\u00a0]+")
_BLANK_LINES = re.compile(r"(?:\r?\n){3,}")
# "1" for 1.0, or the canonical URL the spec prescribes for 1.1.
_VERSION = re.compile(r"^(?:1(?:\.\d+)?|https://jsonfeed\.org/version/1(?:\.\d+)?/?)$")


class JSONFeedError(Exception):
    """Base error for JSON Feed connector errors."""


class JSONFeedFetchError(JSONFeedError):
    """A feed URL could not be fetched (network failure or HTTP error)."""


class JSONFeedParseError(JSONFeedError):
    """A payload is not JSON or is not a JSON Feed (no ``version`` and ``items``)."""


class JSONFeedClient:
    """HTTP client for feed URLs. ``session`` is injectable for tests."""

    def __init__(self, session: Any = None):
        if session is None:
            import requests

            session = requests.Session()
        self._session = session

    def get(self, url: str) -> str:
        """The response body of a feed URL; transport and HTTP failures raise ``JSONFeedFetchError``."""
        try:
            response = self._session.get(
                url,
                headers={
                    "User-Agent": "cognee-jsonfeed-connector/1.0",
                    "Accept": "application/feed+json, application/json",
                },
                timeout=_TIMEOUT,
            )
            if response.status_code >= 400:
                raise JSONFeedFetchError(f"Fetching {url} returned HTTP {response.status_code}.")
            return response.text
        except JSONFeedError:
            raise
        except Exception as error:
            raise JSONFeedFetchError(f"Fetching {url} failed: {error}") from error


def _strip_html(markup: str) -> str:
    """The text of an HTML fragment: block tags end lines, entities decoded, spacing tidied."""
    text = _BLOCK_TAGS.sub("\n\n", markup)
    text = _TAG.sub(" ", text)
    text = html.unescape(text)
    text = _WHITESPACE.sub(" ", text)
    text = re.sub(r" ?\r?\n ?", "\n", text)
    return _BLANK_LINES.sub("\n\n", text).strip()


def _item_content(item: dict[str, Any]) -> str:
    """The item's ``content_text``, or its ``content_html`` stripped to text."""
    text = item.get("content_text")
    if isinstance(text, str) and text.strip():
        return text.strip()
    markup = item.get("content_html")
    if isinstance(markup, str) and markup.strip():
        return _strip_html(markup)
    return ""


def _authors_of(item: dict[str, Any], feed: dict[str, Any]) -> list[str]:
    """The item's author names, falling back to the feed's top-level authors."""
    for holder in (item, feed):
        authors = holder.get("authors")
        if isinstance(authors, list):
            names = [
                author.get("name")
                for author in authors
                if isinstance(author, dict) and author.get("name")
            ]
            if names:
                return [str(name) for name in names]
        author = holder.get("author")
        if isinstance(author, dict) and author.get("name"):
            return [str(author["name"])]
        if isinstance(author, str) and author.strip():
            return [author.strip()]
    return []


def _tag_list(item: dict[str, Any]) -> list[str]:
    tags = item.get("tags")
    if isinstance(tags, list):
        return [str(tag) for tag in tags if isinstance(tag, str) and tag.strip()]
    return []


def _feed_digest(feed_url: str) -> str:
    return hashlib.sha256(feed_url.strip().encode()).hexdigest()[:8]


def _item_row_id(item: dict[str, Any], feed_url: str) -> str:
    """A stable id for the item: its own id when it has one, else a hash of url and title.

    A feed's item ids are unique within the feed, so they are prefixed with the feed
    URL's digest to keep two feeds' items apart.
    """
    item_id = item.get("id")
    if isinstance(item_id, (str, int)) and str(item_id).strip():
        local = str(item_id).strip()
    else:
        seed = f"{item.get('url') or ''}|{item.get('title') or ''}"
        if not seed.strip("|"):
            raise JSONFeedParseError(
                f"An item of {feed_url} has no id, url or title to identify it with."
            )
        local = hashlib.sha256(seed.encode()).hexdigest()[:12]
    return f"jsonfeed:item:{_feed_digest(feed_url)}:{local}"


def _load_feed(text: str, feed_url: str) -> dict[str, Any]:
    """The parsed feed payload; anything without ``version`` and ``items`` is not a feed."""
    try:
        payload = json.loads(text)
    except (json.JSONDecodeError, ValueError) as error:
        raise JSONFeedParseError(f"The payload at {feed_url} is not JSON.") from error
    if not isinstance(payload, dict) or not isinstance(payload.get("items"), list):
        raise JSONFeedParseError(f"The payload at {feed_url} is not a JSON Feed.")
    version = payload.get("version")
    if not isinstance(version, str) or not _VERSION.match(version.strip()):
        raise JSONFeedParseError(
            f"The payload at {feed_url} is not a JSON Feed 1.x document (version {version!r})."
        )
    return payload


def _feed_pages(client: JSONFeedClient, feed_url: str) -> Iterator[dict[str, Any]]:
    """The feed's payloads, following a 1.1 feed's ``next_url`` chain."""
    url: str | None = feed_url.strip()
    seen: set[str] = set()
    while url:
        if url in seen:
            raise JSONFeedParseError(f"The next_url chain of {feed_url} loops at {url}.")
        seen.add(url)
        payload = _load_feed(client.get(url), url)
        yield payload
        following = payload.get("next_url")
        url = following if isinstance(following, str) and following.strip() else None


def format_jsonfeed_item(
    feed: dict[str, Any], item: dict[str, Any], feed_url: str
) -> dict[str, Any]:
    """One feed item as a document row."""
    title = item.get("title")
    if not isinstance(title, str) or not title.strip():
        title = item.get("url") or f"Item {item.get('id') or ''}".strip() or "Untitled"
    content = _item_content(item)
    lines = [f"# {title.strip()}"]
    authors = _authors_of(item, feed)
    if authors:
        lines.append(f"Authors: {', '.join(authors)}")
    tags = _tag_list(item)
    if tags:
        lines.append(f"Tags: {', '.join(tags)}")
    published = item.get("date_published")
    if isinstance(published, str) and published.strip():
        lines.append(f"Published: {published.strip()}")
    modified = item.get("date_modified")
    if isinstance(modified, str) and modified.strip():
        lines.append(f"Modified: {modified.strip()}")
    if content:
        lines.append("")
        lines.append(content)
    item_url = item.get("url") or item.get("external_url") or ""
    return {
        "id": _item_row_id(item, feed_url),
        "title": f"JSON Feed: {str(title).strip()}",
        "content": "\n".join(lines).strip(),
        "url": item_url,
        "_deleted": False,
        NODE_SET_COLUMN: "jsonfeed:item",
    }


def _feed_title_line(feed: dict[str, Any], feed_url: str) -> str:
    feed_title = feed.get("title")
    if isinstance(feed_title, str) and feed_title.strip():
        return f"Feed: {feed_title.strip()} ({feed_url})"
    return f"Feed: {feed_url}"


def _feed_rows(
    client: JSONFeedClient,
    feed_urls: list[str],
    state: dict,
    stats: dict[str, int],
    check_active: Callable[[], None] | None = None,
) -> Iterator[dict[str, Any]]:
    """Fetch, parse and render every feed's items before the first row is yielded.

    A feed that cannot be fetched or parsed stops the run with nothing published and
    nothing forgotten: a partial snapshot must not look like a deletion. An item that
    left its feed since the last complete run is forgotten — feeds are sliding windows,
    so an item absent from a later fetch is gone. The state is written after the last
    row, so a run that raises keeps the old one.
    """
    previous = set(state.get("members", []))
    rows: list[dict[str, Any]] = []
    present: set[str] = set()
    for feed_url in feed_urls:
        if check_active is not None:
            check_active()  # outside any try: a revoked connection is not a failed fetch
        feed_header = _feed_title_line({}, feed_url)
        for page in _feed_pages(client, feed_url):
            feed_header = _feed_title_line(page, feed_url)
            for item in page["items"]:
                if not isinstance(item, dict):
                    raise JSONFeedParseError(f"An item of {feed_url} is not an object.")
                row = format_jsonfeed_item(page, item, feed_url)
                row["content"] = f"{feed_header}\n\n{row['content']}"
                present.add(row["id"])
                rows.append(row)
    for row in rows:
        yield row
        # After the yield: a row that check_active stopped before it was published is not counted.
        stats["synced"] += 1
    for row_id in sorted(previous - present):
        yield {"id": row_id, "_deleted": True}
        stats["deleted"] += 1
    state.update(v=1, members=sorted(present))
    logger.info(
        "JSON Feed: ingested %d item(s) from %d feed(s), forgot %d.",
        stats["synced"],
        len(feed_urls),
        stats["deleted"],
    )


def _normalize_url(feed_url: str, field: str) -> str:
    text = feed_url.strip()
    parsed = urlparse(text)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise ValueError(f"{field} must be an http(s) URL, not {feed_url!r}.")
    return urlunparse(parsed)


def jsonfeed_source(
    *,
    feed_urls: str | list[str],
    resource_name: str | None = None,
    session: Any = None,
    check_active: Callable[[], None] | None = None,
):
    """Return a ``dlt`` resource that yields one document per item of the feeds.

    Hand the result to ``cognee.remember(...)`` with ``write_disposition="merge"`` and
    ``primary_key="id"``. An item's id is its feed id (or a hash of its url and title
    when the feed omits one), prefixed with the feed URL's digest, so re-syncing
    publishes the same rows for unchanged items and nothing is re-cognified. A feed that
    cannot be fetched or parsed raises out of the run, never silently skipped. An item
    that left its feed is forgotten on the next complete run; read ``cognee_sync_stats``
    (``synced``, ``deleted``) afterwards.

    Args:
        feed_urls: One feed URL, or a list of them (duplicates are fetched once).
        resource_name: Names the staging table and the sync state. The default is derived
            from the feed URLs, so changing the selection ingests the new feeds alongside
            the old; pass a name to pin it. Two feed selections in one dataset need
            different names.
        session: Optional HTTP session for custom connections and test mocking.
        check_active: Host authorization checkpoint, called before each request and
            before each row is published.
    """
    import dlt

    if getattr(dlt_utils, "DOCUMENT_SYNC_VERSION", 0) < 3:
        raise RuntimeError(
            "JSON Feed sync requires a Cognee build that reads per-row node sets and "
            "structure (DOCUMENT_SYNC_VERSION >= 3). Upgrade Cognee before syncing."
        )
    raw = [feed_urls] if isinstance(feed_urls, str) else list(feed_urls)
    urls = [_normalize_url(url, "feed_urls") for url in raw if isinstance(url, str)]
    if not urls or len(urls) != len(raw):
        raise ValueError("feed_urls must be a non-empty http(s) URL string or a list of them.")
    seen: set[str] = set()
    selection = []
    for url in urls:
        if url not in seen:
            seen.add(url)
            selection.append(url)
    if resource_name is None:
        resource_name = (
            f"jsonfeed_{hashlib.sha256('|'.join(sorted(selection)).encode()).hexdigest()[:12]}"
        )
    elif not isinstance(resource_name, str) or not resource_name.strip():
        raise ValueError("resource_name must be a non-empty string.")
    client = JSONFeedClient(session=session)
    stats: dict[str, int] = {}

    @dlt.resource(
        name=resource_name,
        primary_key="id",
        write_disposition="merge",
        columns={"_deleted": {"data_type": "bool", "hard_delete": True}},
    )
    def jsonfeed_items():
        stats.update(synced=0, deleted=0)
        rows = _feed_rows(client, selection, dlt.current.resource_state(), stats, check_active)
        yield from dlt_utils.guarded_rows(rows, check_active)

    resource = jsonfeed_items()
    setattr(resource, DOCUMENT_SOURCE_ATTR, "jsonfeed")
    setattr(resource, PIPELINE_SCOPE_ATTR, resource_name)
    resource.cognee_sync_stats = stats
    return resource
