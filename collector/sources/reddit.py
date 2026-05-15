from datetime import datetime, timezone
import asyncpraw
import sys, os; sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from shared_models import RawPost


async def fetch_subreddit_posts(
    client_id: str,
    client_secret: str,
    user_agent: str,
    subreddit_name: str,
    limit: int = 25,
) -> list[RawPost]:
    async with asyncpraw.Reddit(
        client_id=client_id,
        client_secret=client_secret,
        user_agent=user_agent,
    ) as reddit:
        subreddit = await reddit.subreddit(subreddit_name)
        posts: list[RawPost] = []
        async for submission in subreddit.hot(limit=limit):
            body = submission.selftext or submission.title or ""
            if not body:
                continue
            posts.append(RawPost(
                platform="reddit",
                external_id=submission.id,
                author=str(submission.author) if submission.author else None,
                title=submission.title,
                body=body,
                url=f"https://reddit.com{submission.permalink}",
                raw_score=submission.score,
                timestamp=datetime.fromtimestamp(submission.created_utc, tz=timezone.utc),
            ))
        return posts


TOPIC_SUBREDDITS: dict[str, list[str]] = {
    "artificial intelligence": ["MachineLearning", "artificial", "ChatGPT"],
    "climate change": ["climate", "environment", "ClimateChange"],
    "cryptocurrency": ["CryptoCurrency", "Bitcoin", "ethereum"],
    "elections": ["politics", "PoliticalDiscussion"],
}
