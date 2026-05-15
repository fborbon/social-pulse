import sys, os; sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from datetime import datetime, timezone
import aiohttp
from shared_models import RawPost

GDELT_API = "https://api.gdeltproject.org/api/v2/doc/doc"
QUERIES = [
    "politics government", "economy inflation", "technology",
    "health pandemic", "climate environment", "crime police",
    "war conflict", "election vote",
]


async def fetch_gdelt(limit_per_query: int = 5) -> list[RawPost]:
    posts: list[RawPost] = []
    seen: set[str] = set()

    async with aiohttp.ClientSession() as s:
        for query in QUERIES:
            params = {
                "query":      f"{query} sourcelang:english",
                "mode":       "artlist",
                "maxrecords": str(limit_per_query),
                "format":     "json",
            }
            try:
                async with s.get(
                    GDELT_API, params=params, timeout=aiohttp.ClientTimeout(total=15)
                ) as r:
                    if r.status != 200:
                        continue
                    data = await r.json(content_type=None)
            except Exception:
                continue

            for art in data.get("articles", []):
                url = art.get("url", "")
                if url in seen:
                    continue
                seen.add(url)
                title  = art.get("title", "")
                body   = art.get("seendate", title)   # seendate as fallback
                body   = title                         # title is most useful
                if not title:
                    continue
                try:
                    raw_date = art.get("seendate", "")
                    ts = datetime.strptime(raw_date, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
                except Exception:
                    ts = datetime.now(tz=timezone.utc)
                posts.append(RawPost(
                    platform="gdelt",
                    external_id=url[-80:],
                    title=title,
                    body=title,
                    url=url,
                    raw_score=0,
                    timestamp=ts,
                ))
    return posts
