import re
from datetime import datetime, timezone
import sys, os; sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from mastodon import Mastodon
from shared_models import RawPost


def _strip_html(text: str) -> str:
    return re.sub(r"<[^>]+>", "", text).strip()


def fetch_timeline(base_url: str, access_token: str, limit: int = 40) -> list[RawPost]:
    mastodon = Mastodon(api_base_url=base_url, access_token=access_token)
    timeline = mastodon.timeline_public(limit=limit)
    posts: list[RawPost] = []
    for status in timeline:
        body = _strip_html(status.get("content", ""))
        if not body:
            continue
        posts.append(RawPost(
            platform="mastodon",
            external_id=str(status["id"]),
            author=status.get("account", {}).get("acct"),
            body=body,
            url=status.get("url"),
            raw_score=status.get("reblogs_count", 0) + status.get("favourites_count", 0),
            timestamp=status["created_at"].replace(tzinfo=timezone.utc)
            if status["created_at"].tzinfo is None
            else status["created_at"],
        ))
    return posts
