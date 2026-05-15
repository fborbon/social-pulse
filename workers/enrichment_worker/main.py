"""
Enrichment Worker
Consumes: posts.filtered
Produces: posts.enriched
Persists: posts + enriched_posts tables
"""
import asyncio
import json
import logging
import os

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import asyncpg

from enricher import enrich

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("enrichment-worker")

KAFKA = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
DB_URL = os.getenv("DATABASE_URL", "postgresql://pulse:pulse@localhost:5432/social_pulse")
# asyncpg uses plain postgres:// URL
DB_DSN = DB_URL.replace("postgresql+asyncpg://", "postgresql://")

IN_TOPIC = "posts.filtered"
OUT_TOPIC = "posts.enriched"


async def upsert_post(conn: asyncpg.Connection, post: dict) -> str | None:
    try:
        row = await conn.fetchrow(
            """
            INSERT INTO posts (platform, external_id, author, title, body, url,
                               topic_tags, raw_score, timestamp)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
            ON CONFLICT (platform, external_id) DO NOTHING
            RETURNING id
            """,
            post["platform"], post["external_id"], post.get("author"),
            post.get("title"), post["body"], post.get("url"),
            post.get("topic_tags", []), post.get("raw_score", 0),
            post["timestamp"],
        )
        return str(row["id"]) if row else None
    except Exception as e:
        log.warning("upsert_post failed: %s", e)
        return None


async def upsert_enrichment(conn: asyncpg.Connection, post_id: str, post: dict):
    try:
        await conn.execute(
            """
            INSERT INTO enriched_posts (id, sentiment, sentiment_score, category, entities, summary)
            VALUES ($1,$2,$3,$4,$5,$6)
            ON CONFLICT (id) DO UPDATE SET
                sentiment=EXCLUDED.sentiment,
                sentiment_score=EXCLUDED.sentiment_score,
                category=EXCLUDED.category,
                entities=EXCLUDED.entities,
                summary=EXCLUDED.summary
            """,
            post_id, post["sentiment"], post["sentiment_score"],
            post["category"], post["entities"], post["summary"],
        )
    except Exception as e:
        log.warning("upsert_enrichment failed: %s", e)


async def run():
    pool = await asyncpg.create_pool(DB_DSN, min_size=2, max_size=5)
    consumer = AIOKafkaConsumer(
        IN_TOPIC,
        bootstrap_servers=KAFKA,
        group_id="enrichment-worker",
        value_deserializer=lambda v: json.loads(v.decode()),
        auto_offset_reset="earliest",
    )
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA,
        value_serializer=lambda v: json.dumps(v).encode(),
        key_serializer=lambda k: k.encode() if k else None,
    )
    await consumer.start()
    await producer.start()
    log.info("Enrichment worker started")

    try:
        async for msg in consumer:
            post: dict = msg.value
            post = enrich(post)

            async with pool.acquire() as conn:
                post_id = await upsert_post(conn, post)
                if post_id:
                    post["db_id"] = post_id
                    await upsert_enrichment(conn, post_id, post)

            key = f"{post['platform']}:{post['external_id']}"
            await producer.send_and_wait(OUT_TOPIC, value=post, key=key)
            log.debug("Enriched %s → sentiment=%s", post["external_id"], post["sentiment"])
    finally:
        await consumer.stop()
        await producer.stop()
        await pool.close()


if __name__ == "__main__":
    asyncio.run(run())
