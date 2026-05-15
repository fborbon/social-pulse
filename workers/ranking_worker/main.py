"""
Ranking Worker
Consumes: posts.enriched
Produces: posts.ranked

Applies time-decay scoring and deduplicates near-identical posts
(including cross-platform aggregation) before forwarding to the
summary worker and API.
"""
import asyncio
import json
import logging
import os
import re
from collections import defaultdict
from datetime import datetime, timezone

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("ranking-worker")

KAFKA          = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
IN_TOPIC       = "posts.enriched"
OUT_TOPIC      = "posts.ranked"
DEDUP_THRESHOLD = float(os.getenv("DEDUP_THRESHOLD", "0.55"))

# Buffer accumulates posts in a rolling 1-hour window before ranking + dedup.
# Key: date string. Value: list of posts.
_buffer: dict[str, list[dict]] = defaultdict(list)


def time_decay_score(post: dict) -> float:
    now = datetime.now(timezone.utc)
    try:
        ts        = datetime.fromisoformat(post["timestamp"])
        age_hours = max((now - ts).total_seconds() / 3600, 0.1)
    except Exception:
        age_hours = 24.0
    return post.get("raw_score", 0) / (age_hours + 2) ** 1.5


def deduplicate(posts: list[dict]) -> list[dict]:
    if len(posts) < 2:
        for i, p in enumerate(posts):
            p.update({"cluster_id": i, "cluster_size": 1,
                      "platforms": [p["platform"]], "cross_platform": False})
        return posts

    texts      = [" ".join(filter(None, [p.get("title"), p.get("body", "")])) for p in posts]
    vectorizer = TfidfVectorizer(stop_words="english")
    tfidf      = vectorizer.fit_transform(texts)
    sim        = cosine_similarity(tfidf)

    assigned: set[int]       = set()
    clusters: list[list[int]] = []

    for i in range(len(posts)):
        if i in assigned:
            continue
        cluster = [i]
        assigned.add(i)
        for j in range(i + 1, len(posts)):
            if j not in assigned and sim[i, j] >= DEDUP_THRESHOLD:
                cluster.append(j)
                assigned.add(j)
        clusters.append(cluster)

    result = []
    for cid, cluster in enumerate(clusters):
        rep_idx   = max(cluster, key=lambda k: posts[k].get("ranked_score", 0))
        rep       = dict(posts[rep_idx])
        platforms = list({posts[k]["platform"] for k in cluster})
        rep.update({
            "cluster_id":    cid,
            "cluster_size":  len(cluster),
            "platforms":     platforms,
            "cross_platform": len(platforms) > 1,
        })
        result.append(rep)
    return result


async def run():
    consumer = AIOKafkaConsumer(
        IN_TOPIC,
        bootstrap_servers=KAFKA,
        group_id="ranking-worker",
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
    log.info("Ranking worker started")

    try:
        async for msg in consumer:
            post: dict = msg.value
            post["ranked_score"] = round(time_decay_score(post), 2)

            day = (post.get("timestamp") or "")[:10]
            _buffer[day].append(post)

            # Publish ranked + deduped view of today's buffer on every new post.
            # The summary worker will see only cluster representatives.
            today_posts = _buffer[day]
            ranked      = sorted(today_posts, key=lambda p: p["ranked_score"], reverse=True)
            deduped     = deduplicate(ranked)

            for p in deduped:
                key = f"{p['platform']}:{p['external_id']}"
                await producer.send_and_wait(OUT_TOPIC, value=p, key=key)

            log.debug(
                "Day %s: %d raw → %d clusters (cross-platform: %d)",
                day, len(today_posts), len(deduped),
                sum(1 for p in deduped if p.get("cross_platform")),
            )
    finally:
        await consumer.stop()
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(run())
