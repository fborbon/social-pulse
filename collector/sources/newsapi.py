import sys, os; sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from datetime import datetime, timezone
import aiohttp
from shared_models import RawPost

NEWSAPI_URL = "https://newsapi.org/v2/top-headlines"


async def fetch_newsapi(api_key: str, limit: int = 40) -> list[RawPost]:
    if not api_key:
        return []
    params = {
        "apiKey":   api_key,
        "language": "en",
        "pageSize": limit,
    }
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                NEWSAPI_URL, params=params, timeout=aiohttp.ClientTimeout(total=10)
            ) as r:
                if r.status != 200:
                    return []
                data = await r.json()
    except Exception:
        return []

    posts = []
    for art in data.get("articles", []):
        url = art.get("url", "")
        if not url or url == "https://removed.com":
            continue
        body = art.get("description") or art.get("title") or ""
        if not body:
            continue
        try:
            ts = datetime.fromisoformat(art["publishedAt"].replace("Z", "+00:00"))
        except Exception:
            ts = datetime.now(tz=timezone.utc)
        source = art.get("source", {}).get("name", "")
        posts.append(RawPost(
            platform="newsapi",
            external_id=url[-80:],
            author=art.get("author") or source or None,
            title=art.get("title"),
            body=body,
            url=url,
            raw_score=0,
            timestamp=ts,
        ))
    return posts
