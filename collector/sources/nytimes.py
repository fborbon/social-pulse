import sys, os; sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from datetime import datetime, timezone
import aiohttp
from shared_models import RawPost

NYT_API = "https://api.nytimes.com/svc/topstories/v2"
SECTIONS = ["home", "world", "politics", "technology", "science", "health",
            "business", "sports", "arts", "climate"]


async def fetch_nytimes(api_key: str, limit_per_section: int = 5) -> list[RawPost]:
    if not api_key:
        return []
    posts: list[RawPost] = []
    seen: set[str] = set()

    async with aiohttp.ClientSession() as s:
        for section in SECTIONS:
            try:
                async with s.get(
                    f"{NYT_API}/{section}.json",
                    params={"api-key": api_key},
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as r:
                    if r.status != 200:
                        continue
                    data = await r.json()
            except Exception:
                continue

            for art in data.get("results", [])[:limit_per_section]:
                url = art.get("url", "")
                if url in seen:
                    continue
                seen.add(url)
                body = art.get("abstract") or art.get("title") or ""
                if not body:
                    continue
                try:
                    ts = datetime.fromisoformat(
                        art.get("published_date", "").replace("Z", "+00:00")
                    )
                except Exception:
                    ts = datetime.now(tz=timezone.utc)
                byline = art.get("byline", "").replace("By ", "")
                posts.append(RawPost(
                    platform="nytimes",
                    external_id=url.split("/")[-1][:80],
                    author=byline or None,
                    title=art.get("title"),
                    body=body,
                    url=url,
                    raw_score=0,
                    timestamp=ts,
                ))
    return posts
