"""
Summary Worker
Consumes: posts.enriched
At midnight UTC (or on demand via SIGINT) it:
  - groups posts by topic + date
  - computes sentiment breakdown
  - extracts trending words
  - writes a daily_summaries row per topic
"""
import asyncio
import json
import logging
import os
import re
import signal
from collections import Counter, defaultdict
from datetime import date, datetime, timezone

import boto3
import asyncpg
from aiokafka import AIOKafkaConsumer

_bedrock = boto3.client("bedrock-runtime", region_name=os.getenv("AWS_REGION", "eu-west-1"))

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("summary-worker")

KAFKA = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
DB_URL = os.getenv("DATABASE_URL", "postgresql://pulse:pulse@localhost:5432/social_pulse")
DB_DSN = DB_URL.replace("postgresql+asyncpg://", "postgresql://")

IN_TOPIC = "posts.enriched"

# {date_str: {topic: [post_dict, ...]}}
_buffer: dict[str, dict[str, list[dict]]] = defaultdict(lambda: defaultdict(list))

STOP_WORDS = {"the", "a", "an", "and", "or", "but", "is", "are", "was", "were",
              "in", "on", "at", "to", "for", "of", "with", "by", "from", "that",
              "this", "it", "he", "she", "they", "we", "i", "you"}


def _trending_words(posts: list[dict], top_n: int = 10) -> list[str]:
    words: list[str] = []
    for p in posts:
        text = " ".join(filter(None, [p.get("title"), p.get("body", "")]))
        words.extend(re.findall(r"\b[a-zA-Z]{4,}\b", text.lower()))
    filtered = [w for w in words if w not in STOP_WORDS]
    return [w for w, _ in Counter(filtered).most_common(top_n)]


def _build_summary_text(topic: str, posts: list[dict], sentiments: dict) -> str:
    import json as _json
    total   = len(posts)
    pos_pct = round(sentiments["positive"] / total * 100)
    neg_pct = round(sentiments["negative"] / total * 100)
    neu_pct = 100 - pos_pct - neg_pct

    titles = "\n".join(
        f"- {p.get('title') or p.get('body','')[:80]} "
        f"(score: {p.get('raw_score', 0)}, sentiment: {p.get('sentiment','neutral')})"
        for p in sorted(posts, key=lambda x: x.get("raw_score", 0), reverse=True)[:20]
    )

    try:
        body = _json.dumps({
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 300,
            "system": (
                "You are a social media analyst writing concise daily briefings. "
                "Be factual, neutral, and highlight the most significant stories. "
                "Write 2-3 sentences maximum."
            ),
            "messages": [{"role": "user", "content": (
                f"Write a daily briefing for the topic '{topic}' based on these "
                f"{total} posts from today ({pos_pct}% positive, {neu_pct}% neutral, "
                f"{neg_pct}% negative):\n\n{titles}"
            )}],
        })
        resp = _bedrock.invoke_model(
            modelId="anthropic.claude-3-haiku-20240307-v1:0",
            body=body,
            contentType="application/json",
            accept="application/json",
        )
        return _json.loads(resp["body"].read())["content"][0]["text"]
    except Exception as e:
        log.warning("Bedrock summary failed: %s", e)
        trending = _trending_words(posts, 5)
        return (
            f"Topic '{topic}': {total} posts today. "
            f"Sentiment: {pos_pct}% positive, {neu_pct}% neutral, {neg_pct}% negative. "
            f"Trending: {', '.join(trending)}."
        )


async def flush_summaries(pool: asyncpg.Pool, target_date: str):
    if target_date not in _buffer:
        return
    async with pool.acquire() as conn:
        for topic, posts in _buffer[target_date].items():
            sentiments = Counter(p.get("sentiment", "neutral") for p in posts)
            total = len(posts)
            top_ids = [p["db_id"] for p in posts if "db_id" in p][:5]
            trending = _trending_words(posts)
            summary_text = _build_summary_text(topic, posts, sentiments)
            try:
                await conn.execute(
                    """
                    INSERT INTO daily_summaries
                        (summary_date, topic, summary_text, post_count,
                         positive_pct, neutral_pct, negative_pct,
                         trending_words, top_post_ids)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::uuid[])
                    ON CONFLICT (summary_date, topic) DO UPDATE SET
                        summary_text=EXCLUDED.summary_text,
                        post_count=EXCLUDED.post_count,
                        positive_pct=EXCLUDED.positive_pct,
                        neutral_pct=EXCLUDED.neutral_pct,
                        negative_pct=EXCLUDED.negative_pct,
                        trending_words=EXCLUDED.trending_words,
                        top_post_ids=EXCLUDED.top_post_ids
                    """,
                    date.fromisoformat(target_date),
                    topic,
                    summary_text,
                    total,
                    sentiments["positive"] / total,
                    sentiments["neutral"] / total,
                    sentiments["negative"] / total,
                    trending,
                    top_ids or [],
                )
                log.info("Saved summary: %s / %s (%d posts)", target_date, topic, total)
            except Exception as e:
                log.error("Failed to save summary %s/%s: %s", target_date, topic, e)
    del _buffer[target_date]


async def midnight_flusher(pool: asyncpg.Pool):
    """Flushes the previous day's buffer at midnight UTC."""
    while True:
        now = datetime.now(timezone.utc)
        seconds_until_midnight = (
            86400 - (now.hour * 3600 + now.minute * 60 + now.second)
        )
        await asyncio.sleep(seconds_until_midnight)
        yesterday = date.today().isoformat()
        log.info("Midnight flush triggered for %s", yesterday)
        await flush_summaries(pool, yesterday)


async def run():
    pool = await asyncpg.create_pool(DB_DSN, min_size=2, max_size=5)
    consumer = AIOKafkaConsumer(
        IN_TOPIC,
        bootstrap_servers=KAFKA,
        group_id="summary-worker",
        value_deserializer=lambda v: json.loads(v.decode()),
        auto_offset_reset="earliest",
    )
    await consumer.start()
    log.info("Summary worker started")

    asyncio.create_task(midnight_flusher(pool))

    def handle_signal(*_):
        log.info("Signal received — flushing today's buffer")
        today = date.today().isoformat()
        asyncio.create_task(flush_summaries(pool, today))

    signal.signal(signal.SIGUSR1, handle_signal)

    try:
        async for msg in consumer:
            post: dict = msg.value
            ts = post.get("timestamp", "")
            day = ts[:10] if ts else date.today().isoformat()
            for tag in post.get("topic_tags", ["general"]):
                _buffer[day][tag].append(post)
    finally:
        await consumer.stop()
        await pool.close()


if __name__ == "__main__":
    asyncio.run(run())
