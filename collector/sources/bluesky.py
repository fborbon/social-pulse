import sys, os; sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import re
from datetime import datetime, timezone
import aiohttp
from shared_models import RawPost

# Public AppView endpoint — no auth required for reading public content
BSKY_PUBLIC = "https://public.api.bsky.app/xrpc"

# Representative search terms covering several topics
SEARCH_QUERIES = [
    "news politics", "economy inflation", "technology AI",
    "health science", "climate environment", "entertainment sports",
]


def _strip_html(text: str) -> str:
    return re.sub(r"<[^>]+>", " ", text).strip()


async def fetch_bluesky(limit_per_query: int = 10) -> list[RawPost]:
    posts: list[RawPost] = []
    seen: set[str] = set()

    async with aiohttp.ClientSession() as s:
        for query in SEARCH_QUERIES:
            try:
                url = f"{BSKY_PUBLIC}/app.bsky.feed.searchPosts"
                async with s.get(
                    url,
                    params={"q": query, "limit": limit_per_query},
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as r:
                    if r.status != 200:
                        continue
                    data = await r.json()
            except Exception:
                continue

            for item in data.get("posts", []):
                uri = item.get("uri", "")
                if uri in seen:
                    continue
                seen.add(uri)

                record  = item.get("record", {})
                body    = _strip_html(record.get("text", ""))
                if not body:
                    continue
                author  = item.get("author", {})
                counts  = item.get("likeCount", 0) + item.get("repostCount", 0)
                try:
                    ts = datetime.fromisoformat(
                        record.get("createdAt", "").replace("Z", "+00:00")
                    )
                except Exception:
                    ts = datetime.now(tz=timezone.utc)

                handle = author.get("handle", "")
                posts.append(RawPost(
                    platform="bluesky",
                    external_id=uri.split("/")[-1],
                    author=handle,
                    body=body,
                    url=f"https://bsky.app/profile/{handle}/post/{uri.split('/')[-1]}",
                    raw_score=counts,
                    timestamp=ts,
                ))
    return posts
