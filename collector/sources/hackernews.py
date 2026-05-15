import asyncio
from datetime import datetime, timezone
import aiohttp
import sys, os; sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from shared_models import RawPost


HN_API = "https://hacker-news.firebaseio.com/v0"


async def _fetch(session: aiohttp.ClientSession, url: str) -> dict:
    async with session.get(url) as r:
        return await r.json()


async def fetch_top_posts(limit: int = 30) -> list[RawPost]:
    async with aiohttp.ClientSession() as session:
        top_ids: list[int] = await _fetch(session, f"{HN_API}/topstories.json")
        tasks = [_fetch(session, f"{HN_API}/item/{item_id}.json") for item_id in top_ids[:limit]]
        items = await asyncio.gather(*tasks, return_exceptions=True)

    posts: list[RawPost] = []
    for item in items:
        if isinstance(item, Exception) or not isinstance(item, dict):
            continue
        if item.get("type") not in ("story", "ask"):
            continue
        body = item.get("text") or item.get("title") or ""
        if not body:
            continue
        posts.append(RawPost(
            platform="hackernews",
            external_id=str(item["id"]),
            author=item.get("by"),
            title=item.get("title"),
            body=body,
            url=item.get("url"),
            raw_score=item.get("score", 0),
            timestamp=datetime.fromtimestamp(item.get("time", 0), tz=timezone.utc),
        ))
    return posts
