import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel

from config import settings
from kafka_producer import publish, close_producer
from sources.hackernews import fetch_top_posts as hn_posts
from sources.rss import fetch_all_feeds
from sources.reddit import fetch_subreddit_posts, TOPIC_SUBREDDITS
from sources.mastodon import fetch_timeline
from sources.lobsters import fetch_lobsters
from sources.devto import fetch_devto
from sources.lemmy import fetch_lemmy
from sources.bluesky import fetch_bluesky
from sources.arxiv import fetch_arxiv
from sources.gdelt import fetch_gdelt
from sources.sec_edgar import fetch_sec_edgar
from sources.federal_register import fetch_federal_register
from sources.guardian import fetch_guardian
from sources.newsapi import fetch_newsapi
from sources.nytimes import fetch_nytimes
from shared_models import RawPost

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

TOPIC = "posts.raw"

collection_status: dict[str, str] = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(periodic_collection())
    yield
    await close_producer()


app = FastAPI(title="Social Pulse Collector", lifespan=lifespan)


async def _publish_posts(posts: list[RawPost], source: str) -> int:
    count = 0
    for post in posts:
        await publish(TOPIC, post.model_dump(mode="json"), key=post.kafka_key())
        count += 1
    log.info("Published %d posts from %s", count, source)
    return count


async def collect_hackernews() -> int:
    posts = await hn_posts(limit=30)
    return await _publish_posts(posts, "hackernews")


async def collect_rss() -> int:
    if not settings.rss_feed_list:
        return 0
    posts = await fetch_all_feeds(settings.rss_feed_list)
    return await _publish_posts(posts, "rss")


async def collect_reddit() -> int:
    if not settings.reddit_client_id:
        return 0
    total = 0
    for topic in settings.topics:
        subreddits = TOPIC_SUBREDDITS.get(topic, [])
        for sub in subreddits[:2]:
            try:
                posts = await fetch_subreddit_posts(
                    settings.reddit_client_id,
                    settings.reddit_client_secret,
                    settings.reddit_user_agent,
                    sub,
                    limit=20,
                )
                total += await _publish_posts(posts, f"reddit/{sub}")
            except Exception as e:
                log.warning("Reddit %s failed: %s", sub, e)
    return total


async def collect_mastodon() -> int:
    if not settings.mastodon_access_token:
        return 0
    try:
        posts = fetch_timeline(settings.mastodon_base_url, settings.mastodon_access_token)
        return await _publish_posts(posts, "mastodon")
    except Exception as e:
        log.warning("Mastodon failed: %s", e)
        return 0


async def _collect(name: str, coro) -> int:
    try:
        posts = await coro
        return await _publish_posts(posts, name)
    except Exception as e:
        log.warning("%s failed: %s", name, e)
        return 0


async def run_all_collectors() -> dict[str, int]:
    named = [
        ("hackernews",       collect_hackernews()),
        ("rss",              collect_rss()),
        ("reddit",           collect_reddit()),
        ("mastodon",         collect_mastodon()),
        ("lobsters",         _collect("lobsters",         fetch_lobsters())),
        ("devto",            _collect("devto",            fetch_devto())),
        ("lemmy",            _collect("lemmy",            fetch_lemmy())),
        ("bluesky",          _collect("bluesky",          fetch_bluesky())),
        ("arxiv",            _collect("arxiv",            fetch_arxiv())),
        ("gdelt",            _collect("gdelt",            fetch_gdelt())),
        ("sec_edgar",        _collect("sec_edgar",        fetch_sec_edgar())),
        ("federal_register", _collect("federal_register", fetch_federal_register())),
        ("guardian",         _collect("guardian",         fetch_guardian(settings.guardian_api_key))),
        ("newsapi",          _collect("newsapi",          fetch_newsapi(settings.newsapi_key))),
        ("nytimes",          _collect("nytimes",          fetch_nytimes(settings.nytimes_api_key))),
    ]
    sources, coros = zip(*named)
    results = await asyncio.gather(*coros, return_exceptions=True)
    return {src: (r if isinstance(r, int) else 0) for src, r in zip(sources, results)}


async def periodic_collection():
    """Runs all collectors every 15 minutes."""
    while True:
        try:
            log.info("Starting periodic collection at %s", datetime.now(timezone.utc))
            counts = await run_all_collectors()
            log.info("Collection complete: %s", counts)
        except Exception as e:
            log.error("Periodic collection error: %s", e)
        await asyncio.sleep(900)  # 15 minutes


# ---------- API ----------

class CollectResponse(BaseModel):
    source: str
    posts_published: int
    timestamp: str


@app.get("/health")
async def health():
    return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}


@app.post("/collect/all", response_model=dict[str, int])
async def trigger_all():
    return await run_all_collectors()


@app.post("/collect/{source}", response_model=CollectResponse)
async def trigger_source(source: str):
    collectors = {
        "hackernews": collect_hackernews,
        "rss": collect_rss,
        "reddit": collect_reddit,
        "mastodon": collect_mastodon,
    }
    if source not in collectors:
        raise HTTPException(404, f"Unknown source '{source}'. Valid: {list(collectors)}")
    count = await collectors[source]()
    return CollectResponse(
        source=source,
        posts_published=count,
        timestamp=datetime.now(timezone.utc).isoformat(),
    )


@app.get("/topics")
async def get_topics():
    return {"topics": settings.topics}
