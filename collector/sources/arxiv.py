import sys, os; sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from datetime import datetime, timezone
import aiohttp
import feedparser
from shared_models import RawPost

SEARCH_TERMS = [
    "artificial intelligence", "climate change", "economics",
    "machine learning", "public health", "quantum computing",
]


async def fetch_arxiv(limit_per_term: int = 5) -> list[RawPost]:
    posts: list[RawPost] = []
    seen: set[str] = set()

    async with aiohttp.ClientSession() as s:
        for term in SEARCH_TERMS:
            url = (
                "https://export.arxiv.org/api/query"
                f"?search_query=all:{term.replace(' ', '+')}"
                f"&start=0&max_results={limit_per_term}"
                "&sortBy=submittedDate&sortOrder=descending"
            )
            try:
                async with s.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
                    content = await r.text()
            except Exception:
                continue

            feed = feedparser.parse(content)
            for entry in feed.entries:
                eid = entry.get("id", "")
                if eid in seen:
                    continue
                seen.add(eid)
                body = entry.get("summary", "") or entry.get("title", "")
                if not body:
                    continue
                try:
                    ts = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
                except Exception:
                    ts = datetime.now(tz=timezone.utc)
                posts.append(RawPost(
                    platform="arxiv",
                    external_id=eid.split("/abs/")[-1].replace("/", "_"),
                    author=", ".join(a.get("name", "") for a in entry.get("authors", [])[:3]),
                    title=entry.get("title", "").replace("\n", " "),
                    body=body[:500],
                    url=eid,
                    raw_score=0,
                    timestamp=ts,
                ))
    return posts
