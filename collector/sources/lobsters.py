import sys, os; sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from datetime import datetime, timezone
import aiohttp
from shared_models import RawPost


async def fetch_lobsters(limit: int = 25) -> list[RawPost]:
    url = "https://lobste.rs/hottest.json"
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                items = await r.json()
    except Exception:
        return []

    posts = []
    for item in items[:limit]:
        body = item.get("description_plain") or item.get("description") or item.get("title") or ""
        if not body:
            continue
        su = item.get("submitter_user")
        author = su.get("username") if isinstance(su, dict) else None
        try:
            ts = datetime.fromisoformat(item["created_at"])
        except Exception:
            ts = datetime.now(tz=timezone.utc)
        posts.append(RawPost(
            platform="lobsters",
            external_id=item["short_id"],
            author=author,
            title=item.get("title"),
            body=body,
            url=item.get("url") or f"https://lobste.rs/s/{item['short_id']}",
            raw_score=item.get("score", 0),
            timestamp=ts,
        ))
    return posts
