import sys, os; sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from datetime import datetime, timezone, timedelta
import aiohttp
from shared_models import RawPost

EDGAR_SEARCH = "https://efts.sec.gov/LATEST/search-index"
QUERIES = ["earnings revenue", "merger acquisition", "layoffs workforce", "AI technology investment"]


async def fetch_sec_edgar(limit_per_query: int = 5) -> list[RawPost]:
    posts: list[RawPost] = []
    seen: set[str] = set()
    since = (datetime.now(timezone.utc) - timedelta(days=3)).strftime("%Y-%m-%d")

    # SEC requires a descriptive User-Agent per their fair-access policy
    headers = {"User-Agent": "social-pulse research@socialpulse.example.com"}
    async with aiohttp.ClientSession(headers=headers) as s:
        for query in QUERIES:
            params = {
                "q":         query,
                "forms":     "8-K",
                "dateRange": "custom",
                "startdt":   since,
            }
            try:
                async with s.get(
                    EDGAR_SEARCH, params=params, timeout=aiohttp.ClientTimeout(total=10)
                ) as r:
                    if r.status != 200:
                        continue
                    data = await r.json(content_type=None)
            except Exception:
                continue

            for hit in data.get("hits", {}).get("hits", [])[:limit_per_query]:
                src  = hit.get("_source", {})
                eid  = hit.get("_id", "")
                if eid in seen:
                    continue
                seen.add(eid)
                entity  = src.get("entity_name", "Unknown")
                form    = src.get("form_type", "8-K")
                title   = f"{entity} filed {form}"
                body    = f"{entity} — {form} filing. Period: {src.get('period_of_report', 'N/A')}."
                url     = src.get("file_url_htm") or f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&company={entity}"
                try:
                    ts = datetime.strptime(src["file_date"], "%Y-%m-%d").replace(tzinfo=timezone.utc)
                except Exception:
                    ts = datetime.now(tz=timezone.utc)
                posts.append(RawPost(
                    platform="sec_edgar",
                    external_id=eid,
                    title=title,
                    body=body,
                    url=url,
                    raw_score=0,
                    timestamp=ts,
                ))
    return posts
