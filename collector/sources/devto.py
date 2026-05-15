import sys, os; sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from datetime import datetime, timezone
import aiohttp
from shared_models import RawPost


async def fetch_devto(limit: int = 25) -> list[RawPost]:
    url = f"https://dev.to/api/articles?per_page={limit}&top=1"
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                items = await r.json()
    except Exception:
        return []

    posts = []
    for item in items:
        body = item.get("description") or item.get("title") or ""
        try:
            ts = datetime.fromisoformat(item["published_at"].replace("Z", "+00:00"))
        except Exception:
            ts = datetime.now(tz=timezone.utc)
        posts.append(RawPost(
            platform="devto",
            external_id=str(item["id"]),
            author=item.get("user", {}).get("username"),
            title=item.get("title"),
            body=body,
            url=item.get("url"),
            raw_score=item.get("positive_reactions_count", 0) + item.get("comments_count", 0),
            timestamp=ts,
        ))
    return posts
