import sys, os; sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from datetime import datetime, timezone
import aiohttp
from shared_models import RawPost

FR_API = "https://www.federalregister.gov/api/v1/articles.json"
TERMS  = ["technology", "health", "environment", "economy", "public safety"]


async def fetch_federal_register(limit_per_term: int = 5) -> list[RawPost]:
    posts: list[RawPost] = []
    seen: set[str] = set()

    async with aiohttp.ClientSession() as s:
        for term in TERMS:
            params = {
                "per_page":           limit_per_term,
                "order":              "newest",
                "fields[]":           ["abstract", "title", "agency_names", "publication_date", "html_url", "document_number"],
                "conditions[term]":   term,
            }
            try:
                async with s.get(
                    FR_API, params=params, timeout=aiohttp.ClientTimeout(total=10)
                ) as r:
                    if r.status != 200:
                        continue
                    data = await r.json(content_type=None)
            except Exception:
                continue

            for art in data.get("results", []):
                doc_num = art.get("document_number", "")
                if doc_num in seen:
                    continue
                seen.add(doc_num)
                body = art.get("abstract") or art.get("title") or ""
                if not body:
                    continue
                agencies = ", ".join(art.get("agency_names", [])[:2])
                try:
                    ts = datetime.strptime(art["publication_date"], "%Y-%m-%d").replace(tzinfo=timezone.utc)
                except Exception:
                    ts = datetime.now(tz=timezone.utc)
                posts.append(RawPost(
                    platform="federal_register",
                    external_id=doc_num,
                    author=agencies or None,
                    title=art.get("title"),
                    body=body[:400],
                    url=art.get("html_url"),
                    raw_score=0,
                    timestamp=ts,
                ))
    return posts
