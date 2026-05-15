import sys, os; sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import re
import logging
from datetime import datetime, timezone
from atproto import AsyncClient
from shared_models import RawPost

log = logging.getLogger(__name__)

SEARCH_QUERIES = [
    "politics news", "economy inflation", "technology AI",
    "health science", "climate environment", "entertainment sports",
    "crime safety", "world news", "wall street markets",
]


def _strip_facets(text: str) -> str:
    return re.sub(r"<[^>]+>", " ", text).strip()


async def fetch_bluesky(
    handle: str,
    app_password: str,
    limit_per_query: int = 10,
) -> list[RawPost]:
    if not handle or not app_password:
        log.info("Bluesky: skipped (set BLUESKY_HANDLE + BLUESKY_APP_PASSWORD)")
        return []

    client = AsyncClient()
    try:
        await client.login(handle, app_password)
    except Exception as e:
        log.warning("Bluesky login failed: %s", e)
        return []

    posts: list[RawPost] = []
    seen:  set[str]      = set()

    for query in SEARCH_QUERIES:
        try:
            result = await client.app.bsky.feed.search_posts(
                params={"q": query, "limit": limit_per_query}
            )
            for item in result.posts:
                uri = item.uri
                if uri in seen:
                    continue
                seen.add(uri)
                body = _strip_facets(item.record.text or "")
                if not body:
                    continue
                handle_str = item.author.handle or ""
                try:
                    ts = datetime.fromisoformat(
                        item.record.created_at.replace("Z", "+00:00")
                    )
                except Exception:
                    ts = datetime.now(tz=timezone.utc)
                likes    = getattr(item, "like_count", 0) or 0
                reposts  = getattr(item, "repost_count", 0) or 0
                posts.append(RawPost(
                    platform="bluesky",
                    external_id=uri.split("/")[-1],
                    author=handle_str,
                    body=body,
                    url=f"https://bsky.app/profile/{handle_str}/post/{uri.split('/')[-1]}",
                    raw_score=likes + reposts,
                    timestamp=ts,
                ))
        except Exception as e:
            log.debug("Bluesky query '%s' failed: %s", query, e)

    return posts
