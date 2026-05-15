"""
Filter Worker
Consumes: posts.raw
Produces: posts.filtered

Checks if the post text/title contains any tracked topic keyword.
Adds topic_tags list to the message and forwards it.
"""
import asyncio
import json
import logging
import os
import sys

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("filter-worker")

KAFKA = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
TOPICS_RAW = "posts.raw"
TOPICS_FILTERED = "posts.filtered"
TRACK_TOPICS_ENV = os.getenv(
    "TRACK_TOPICS",
    "artificial intelligence,climate change,cryptocurrency,elections",
)
TRACK_TOPICS: list[str] = [t.strip().lower() for t in TRACK_TOPICS_ENV.split(",") if t.strip()]


def extract_tags(text: str) -> list[str]:
    lower = text.lower()
    return [topic for topic in TRACK_TOPICS if topic in lower]


async def run():
    consumer = AIOKafkaConsumer(
        TOPICS_RAW,
        bootstrap_servers=KAFKA,
        group_id="filter-worker",
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
    log.info("Filter worker started. Tracking: %s", TRACK_TOPICS)

    try:
        async for msg in consumer:
            post: dict = msg.value
            searchable = " ".join(filter(None, [post.get("title"), post.get("body")])).lower()
            tags = extract_tags(searchable)

            if not tags:
                continue  # drop posts that don't match any topic

            post["topic_tags"] = tags
            key = f"{post['platform']}:{post['external_id']}"
            await producer.send_and_wait(TOPICS_FILTERED, value=post, key=key)
            log.debug("Forwarded post %s with tags %s", post["external_id"], tags)
    finally:
        await consumer.stop()
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(run())
