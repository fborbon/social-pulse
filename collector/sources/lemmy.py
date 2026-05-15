import sys, os; sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from datetime import datetime, timezone
import aiohttp
from shared_models import RawPost

LEMMY_INSTANCE = "https://lemmy.world"


async def fetch_lemmy(limit: int = 25) -> list[RawPost]:
    url = f"{LEMMY_INSTANCE}/api/v3/post/list?type_=All&sort=Hot&limit={limit}&page=1"
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                data = await r.json()
    except Exception:
        return []

    posts = []
    for item in data.get("posts", []):
        p = item.get("post", {})
        body = p.get("body") or p.get("name") or ""
        if not body:
            continue
        try:
            ts = datetime.fromisoformat(p["published"].replace("Z", "+00:00"))
        except Exception:
            ts = datetime.now(tz=timezone.utc)
        counts = item.get("counts", {})
        posts.append(RawPost(
            platform="lemmy",
            external_id=str(p["id"]),
            author=item.get("creator", {}).get("name"),
            title=p.get("name"),
            body=body,
            url=p.get("url") or f"{LEMMY_INSTANCE}/post/{p['id']}",
            raw_score=counts.get("score", 0),
            timestamp=ts,
        ))
    return posts
