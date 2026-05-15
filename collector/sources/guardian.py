import sys, os; sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from datetime import datetime, timezone
import aiohttp
from shared_models import RawPost

GUARDIAN_API = "https://content.guardianapis.com/search"
SECTIONS = ["politics", "world", "business", "technology", "science", "environment",
            "sport", "culture", "society", "film", "music"]


async def fetch_guardian(api_key: str, limit_per_section: int = 5) -> list[RawPost]:
    if not api_key:
        return []
    posts: list[RawPost] = []
    seen: set[str] = set()

    async with aiohttp.ClientSession() as s:
        for section in SECTIONS:
            params = {
                "api-key":      api_key,
                "section":      section,
                "page-size":    limit_per_section,
                "show-fields":  "headline,bodyText,trailText",
                "order-by":     "newest",
            }
            try:
                async with s.get(
                    GUARDIAN_API, params=params, timeout=aiohttp.ClientTimeout(total=10)
                ) as r:
                    if r.status != 200:
                        continue
                    data = await r.json()
            except Exception:
                continue

            for art in data.get("response", {}).get("results", []):
                uid = art.get("id", "")
                if uid in seen:
                    continue
                seen.add(uid)
                fields = art.get("fields", {})
                body   = fields.get("trailText") or fields.get("bodyText", "")[:400] or art.get("webTitle", "")
                try:
                    ts = datetime.fromisoformat(art["webPublicationDate"].replace("Z", "+00:00"))
                except Exception:
                    ts = datetime.now(tz=timezone.utc)
                posts.append(RawPost(
                    platform="guardian",
                    external_id=uid,
                    title=art.get("webTitle"),
                    body=body,
                    url=art.get("webUrl"),
                    raw_score=0,
                    timestamp=ts,
                ))
    return posts
