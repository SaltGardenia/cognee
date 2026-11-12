"""The JSON Feed source fetches feed URLs and renders one document per item.

No network: a fake session returns recorded JSON Feed payloads; a real dlt pipeline
backs the sync-and-forget test.
"""

import json

import pytest

from cognee.tasks.ingestion.connectors import jsonfeed as jsonfeed_module
from cognee.tasks.ingestion.connectors.jsonfeed import (
    JSONFeedError,
    JSONFeedFetchError,
    JSONFeedParseError,
    jsonfeed_source,
)
from cognee.tasks.ingestion.dlt_utils import NODE_SET_COLUMN

FEED_URL = "https://example.com/feed.json"
FEED = {
    "version": "https://jsonfeed.org/version/1.1",
    "title": "Example Blog",
    "items": [
        {
            "id": "https://example.com/posts/1",
            "url": "https://example.com/posts/1",
            "title": "First post",
            "content_text": "Hello world.",
            "date_published": "2026-01-01T00:00:00Z",
            "authors": [{"name": "Ada"}],
            "tags": ["intro", "release"],
        },
        {
            "id": "2",
            "title": "HTML post",
            "content_html": "<p>Hello <b>world</b>&nbsp;— truly</p><p>Second para</p>",
        },
        {
            "url": "https://example.com/posts/3",
            "title": "Id-less post",
            "content_text": "No id here.",
        },
    ],
}


class _FakeResponse:
    def __init__(self, status_code=200, text=None):
        self.status_code = status_code
        self._text = text if text is not None else ""

    @property
    def text(self):
        if isinstance(self._text, Exception):
            raise self._text
        return self._text

    def raise_for_status(self):
        if self.status_code >= 400:
            raise JSONFeedFetchError(f"HTTP {self.status_code}")


class _FakeSession:
    """Answers GETs by URL substring, one recorded text per fragment."""

    def __init__(self, routes: dict[str, object]):
        self.routes = routes
        self.requested = []

    def get(self, url, **kwargs):
        self.requested.append(url)
        for fragment, response in self.routes.items():
            if fragment in url:
                return response
        raise AssertionError(f"unexpected URL: {url}")


def _session(feed=None):
    return _FakeSession({"/feed.json": _FakeResponse(text=json.dumps(feed or FEED))})


# --- formatting -----------------------------------------------------------------


def test_the_document_carries_the_item_metadata_and_content():
    rows = list(jsonfeed_source(feed_urls=FEED_URL, session=_session()))
    first = rows[0]
    assert (
        first["id"]
        == "jsonfeed:item:"
        + jsonfeed_module._feed_digest(FEED_URL)
        + ":https://example.com/posts/1"
    )
    assert first["title"] == "JSON Feed: First post"
    assert first["content"].startswith("Feed: Example Blog (https://example.com/feed.json)")
    assert "# First post" in first["content"]
    assert "Authors: Ada" in first["content"]
    assert "Tags: intro, release" in first["content"]
    assert "Published: 2026-01-01T00:00:00Z" in first["content"]
    assert "Hello world." in first["content"]
    assert first["url"] == "https://example.com/posts/1"
    assert first[NODE_SET_COLUMN] == "jsonfeed:item"
    assert first["_deleted"] is False


def test_html_content_is_stripped_to_text():
    rows = list(jsonfeed_source(feed_urls=FEED_URL, session=_session()))
    html_row = rows[1]
    assert "Hello world — truly" in html_row["content"]
    assert "Second para" in html_row["content"]
    assert "<p>" not in html_row["content"]


def test_an_item_without_an_id_gets_a_stable_hashed_row_id():
    rows = list(jsonfeed_source(feed_urls=FEED_URL, session=_session()))
    idless = rows[2]
    assert idless["id"].startswith("jsonfeed:item:" + jsonfeed_module._feed_digest(FEED_URL) + ":")
    rows_again = list(jsonfeed_source(feed_urls=FEED_URL, session=_session()))
    assert rows_again[2]["id"] == idless["id"]


def test_an_item_with_neither_id_url_nor_title_is_refused():
    item = {"content_text": "ghost"}
    with pytest.raises(JSONFeedParseError):
        jsonfeed_module.format_jsonfeed_item(FEED, item, FEED_URL)


def test_an_idless_item_keeps_a_stable_row_id_when_its_content_changes():
    item = {"url": "https://example.com/posts/3", "title": "Id-less post", "content_text": "v1"}
    first = jsonfeed_module.format_jsonfeed_item(FEED, item, FEED_URL)
    second = jsonfeed_module.format_jsonfeed_item(FEED, dict(item, content_text="v2"), FEED_URL)
    assert first["id"] == second["id"]


def test_the_feed_authors_backfill_an_item_without_authors():
    feed = {
        "version": "1.1",
        "authors": [{"name": "Blog"}],
        "items": [{"id": "1", "content_text": "t"}],
    }
    row = jsonfeed_module.format_jsonfeed_item(feed, feed["items"][0], FEED_URL)
    assert "Authors: Blog" in row["content"]


def test_an_item_without_a_title_falls_back_to_its_url():
    item = {"id": "1", "url": "https://example.com/x", "content_text": "t"}
    row = jsonfeed_module.format_jsonfeed_item(FEED, item, FEED_URL)
    assert row["title"] == "JSON Feed: https://example.com/x"


def test_an_item_without_content_still_publishes_its_metadata():
    item = {"id": "1", "title": "Bare", "url": "https://example.com/bare"}
    row = jsonfeed_module.format_jsonfeed_item(FEED, item, FEED_URL)
    assert "# Bare" in row["content"]
    assert row["url"] == "https://example.com/bare"


def test_strip_html_decodes_entities_and_keeps_paragraphs():
    assert jsonfeed_module._strip_html("<p>A &amp; B</p><p>C</p>") == "A & B\n\nC"
    assert jsonfeed_module._strip_html("a<br/>b") == "a\n\nb"
    assert jsonfeed_module._strip_html("plain text") == "plain text"


# --- the client and the feed parser ------------------------------------------------


def test_an_http_error_raises_fetch_error():
    client = jsonfeed_module.JSONFeedClient(
        session=_FakeSession({"/feed.json": _FakeResponse(status_code=404)})
    )
    with pytest.raises(JSONFeedFetchError):
        client.get(FEED_URL)


def test_a_network_failure_raises_fetch_error():
    session = _FakeSession({"/feed.json": _FakeResponse(text=RuntimeError("boom"))})
    client = jsonfeed_module.JSONFeedClient(session=session)
    with pytest.raises(JSONFeedFetchError):
        client.get(FEED_URL)


def _cause_of(excinfo):
    # dlt wraps generator failures; the connector's error is what caused the stop.
    return excinfo.value.__cause__


def test_a_non_json_payload_raises_parse_error():
    session = _FakeSession({"/feed.json": _FakeResponse(text="<html>not json</html>")})
    with pytest.raises(Exception, match="not JSON") as excinfo:
        list(jsonfeed_source(feed_urls=FEED_URL, session=session))
    assert isinstance(_cause_of(excinfo), JSONFeedParseError)


def test_a_payload_without_items_raises_parse_error():
    session = _FakeSession({"/feed.json": _FakeResponse(text=json.dumps({"version": "1.1"}))})
    with pytest.raises(Exception, match="not a JSON Feed") as excinfo:
        list(jsonfeed_source(feed_urls=FEED_URL, session=session))
    assert isinstance(_cause_of(excinfo), JSONFeedParseError)


def test_a_payload_that_is_not_a_1x_feed_raises_parse_error():
    session = _FakeSession(
        {"feed.json": _FakeResponse(text=json.dumps({"version": "2.0", "items": []}))}
    )
    with pytest.raises(Exception, match="JSON Feed 1.x") as excinfo:
        list(jsonfeed_source(feed_urls=FEED_URL, session=session))
    assert isinstance(_cause_of(excinfo), JSONFeedParseError)


def test_a_next_url_chain_is_followed():
    page_one = dict(FEED, items=[FEED["items"][0]], next_url="https://example.com/feed-2.json")
    page_two = dict(FEED, items=[FEED["items"][1]])
    session = _FakeSession(
        {
            "/feed.json": _FakeResponse(text=json.dumps(page_one)),
            "/feed-2.json": _FakeResponse(text=json.dumps(page_two)),
        }
    )
    rows = list(jsonfeed_source(feed_urls=FEED_URL, session=session))
    assert [row["title"] for row in rows] == ["JSON Feed: First post", "JSON Feed: HTML post"]
    assert session.requested == [FEED_URL, "https://example.com/feed-2.json"]


def test_a_looping_next_url_chain_raises():
    page = dict(FEED, next_url=FEED_URL)
    session = _FakeSession({"/feed.json": _FakeResponse(text=json.dumps(page))})
    with pytest.raises(Exception, match="loops") as excinfo:
        list(jsonfeed_source(feed_urls=FEED_URL, session=session))
    assert isinstance(_cause_of(excinfo), JSONFeedParseError)


# --- the source -------------------------------------------------------------------


def test_a_batch_yields_one_document_per_item_of_each_feed():
    other = dict(
        FEED, title="Other Blog", items=[{"id": "x1", "title": "Other", "content_text": "o"}]
    )
    session = _FakeSession(
        {
            "/feed.json": _FakeResponse(text=json.dumps(FEED)),
            "/other.json": _FakeResponse(text=json.dumps(other)),
        }
    )
    rows = list(
        jsonfeed_source(feed_urls=[FEED_URL, "https://example.com/other.json"], session=session)
    )
    assert [row["title"] for row in rows] == [
        "JSON Feed: First post",
        "JSON Feed: HTML post",
        "JSON Feed: Id-less post",
        "JSON Feed: Other",
    ]
    # The two feeds' item ids never collide, even with the same local id.
    assert len({row["id"] for row in rows}) == len(rows)


def test_duplicate_feed_urls_are_fetched_once():
    session = _session()
    rows = list(
        jsonfeed_source(feed_urls=[FEED_URL, " https://example.com/feed.json "], session=session)
    )
    assert len(rows) == 3
    assert session.requested == [FEED_URL]


def test_a_non_http_url_is_refused():
    with pytest.raises(ValueError):
        jsonfeed_source(feed_urls="ftp://example.com/feed.json")
    with pytest.raises(ValueError):
        jsonfeed_source(feed_urls="not-a-url")


def test_an_empty_feed_url_list_is_refused():
    with pytest.raises(ValueError):
        jsonfeed_source(feed_urls=[])


def test_an_empty_resource_name_is_refused():
    with pytest.raises(ValueError):
        jsonfeed_source(feed_urls=FEED_URL, resource_name="  ")


def test_an_unreadable_feed_fails_the_run_before_anything_is_published():
    """A partial snapshot must not look like a deletion: the run raises with nothing published."""
    session = _FakeSession(
        {
            "/feed.json": _FakeResponse(text=json.dumps(FEED)),
            "/broken.json": _FakeResponse(status_code=500),
        }
    )
    source = jsonfeed_source(
        feed_urls=[FEED_URL, "https://example.com/broken.json"], session=session
    )
    with pytest.raises(Exception) as excinfo:
        list(source)
    assert isinstance(_cause_of(excinfo), JSONFeedFetchError)
    assert source.cognee_sync_stats == {"synced": 0, "deleted": 0}


# --- through a real dlt pipeline ---------------------------------------------------


def test_sync_update_and_forget_through_a_real_dlt_pipeline(tmp_path):
    import dlt

    from cognee.tasks.ingestion.connectors import jsonfeed_source as exported_source
    from cognee.tasks.ingestion.dlt_utils import DOCUMENT_COLUMN_HINTS

    table = "jsonfeed_test"
    shortened = dict(FEED, items=FEED["items"][1:])  # the first item left the feed

    def pipeline():
        return dlt.pipeline(
            pipeline_name="jsonfeed_test",
            destination=dlt.destinations.sqlalchemy(f"sqlite:///{(tmp_path / 'a.db').as_posix()}"),
            dataset_name="jsonfeed",
            pipelines_dir=str(tmp_path / "pipelines"),
        )

    def run(feed) -> list[str]:
        source = exported_source(feed_urls=FEED_URL, session=_session(feed), resource_name=table)
        pipeline().run(source, columns=DOCUMENT_COLUMN_HINTS)
        with (
            pipeline().sql_client() as sql,
            sql.execute_query(f'SELECT id FROM "{table}" ORDER BY id') as cursor,
        ):
            return [row[0] for row in cursor.fetchall()]

    stored = run(FEED)
    assert len(stored) == 3
    first_item_id = (
        f"jsonfeed:item:{jsonfeed_module._feed_digest(FEED_URL)}:https://example.com/posts/1"
    )

    # A re-sync of the same feed publishes the same ids and forgets nothing.
    assert run(FEED) == stored

    # An item that left the feed is forgotten.
    after = run(shortened)
    assert len(after) == 2
    assert set(after) == set(stored) - {first_item_id}
