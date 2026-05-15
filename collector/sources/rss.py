from datetime import datetime, timezone
import feedparser
import sys, os; sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from shared_models import RawPost


def _parse_date(entry) -> datetime:
    if hasattr(entry, "published_parsed") and entry.published_parsed:
        return datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
    return datetime.now(tz=timezone.utc)


def fetch_feed(url: str) -> list[RawPost]:
    feed = feedparser.parse(url)
    posts: list[RawPost] = []
    for entry in feed.entries:
        body = entry.get("summary") or entry.get("description") or entry.get("title") or ""
        if not body:
            continue
        posts.append(RawPost(
            platform="rss",
            external_id=entry.get("id") or entry.get("link") or body[:64],
            author=entry.get("author"),
            title=entry.get("title"),
            body=body,
            url=entry.get("link"),
            timestamp=_parse_date(entry),
        ))
    return posts


async def fetch_all_feeds(feed_urls: list[str]) -> list[RawPost]:
    posts: list[RawPost] = []
    for url in feed_urls:
        try:
            posts.extend(fetch_feed(url))
        except Exception:
            pass
    return posts
