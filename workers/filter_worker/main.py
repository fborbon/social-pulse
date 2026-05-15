"""
Filter Worker
Consumes: posts.raw
Produces: posts.filtered

Uses TF-IDF cosine similarity against topic descriptions instead of
exact keyword matching, catching synonyms and related terms.
"""
import asyncio
import json
import logging
import os

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("filter-worker")

KAFKA     = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
IN_TOPIC  = "posts.raw"
OUT_TOPIC = "posts.filtered"
THRESHOLD = float(os.getenv("SEMANTIC_THRESHOLD", "0.12"))

TOPIC_DESCRIPTIONS = {
    "politics":                    "politics government congress senate parliament election candidate legislation policy democrat republican president prime minister vote bill law",
    "world news":                  "international global foreign affairs diplomacy war conflict geopolitics UN NATO summit treaty relations ambassador sanctions",
    "business & economy":          "economy GDP inflation recession stock market earnings revenue profit trade tariff fiscal monetary federal reserve interest rate company merger acquisition",
    "technology":                  "technology software hardware innovation cloud computing mobile internet digital product launch developer platform startup app gadget",
    "health":                      "health medical medicine hospital patient disease treatment drug pharmaceutical FDA CDC vaccine pandemic surgery clinical trial wellness",
    "science & environment":       "science research study university lab discovery experiment physics chemistry biology nature environment ecology species conservation wildlife",
    "crime & public safety":       "crime police arrest criminal murder shooting robbery fraud court trial law enforcement prosecution safety investigation suspect victim",
    "entertainment & culture":     "entertainment movie film television show celebrity actor actress award streaming box office culture arts exhibition theater comedy drama",
    "sports":                      "sports game match team player score championship league tournament athlete football basketball baseball soccer tennis olympics coach",
    "lifestyle & human interest":  "lifestyle travel food restaurant family parenting relationship wellness fitness mental health community volunteer charity personal story",
    "artificial intelligence":     "artificial intelligence machine learning neural network deep learning LLM GPT model training inference AI chatbot automation robotics generative",
    "wall street":                 "wall street stock market NYSE NASDAQ S&P trading investor hedge fund IPO earnings bond interest rate Federal Reserve bulls bears portfolio",
    "silicon valley":              "silicon valley tech startup venture capital unicorn founder YC Sequoia Andreessen engineer valuation funding seed series runway pivot",
    "social networks":             "social media twitter facebook instagram tiktok youtube reddit linkedin platform users followers engagement viral post content creator influencer",
    "global warming":              "global warming climate change temperature greenhouse gas carbon emissions sea level arctic glacier fossil fuels renewable energy solar wind drought flood",
    "cost of living":              "cost of living inflation housing rent mortgage grocery prices wages salary affordability poverty economic hardship budget consumer spending",
    "employment & work balance":   "employment jobs work remote workplace burnout productivity career layoffs hiring salary work life balance flexible hours labor union strike",
    "gender equity":               "gender equity women equality feminism discrimination workplace diversity inclusion pay gap rights harassment maternity paternity LGBTQ",
    "pets & animal kingdom":       "pets dogs cats animals wildlife veterinary adoption shelter breeding species conservation endangered habitat zoo farm livestock nature",
    "music & movies":              "music artist album song concert tour Grammy Oscar award film director box office streaming Spotify Netflix release debut performance",
}

_topic_names = list(TOPIC_DESCRIPTIONS.keys())
_topic_texts = list(TOPIC_DESCRIPTIONS.values())

# Fit vectorizer once on startup with topic descriptions; posts are transformed
# into the same space using the vocabulary learned from topic texts.
_vectorizer  = TfidfVectorizer(stop_words="english", ngram_range=(1, 2))
_topic_vecs  = _vectorizer.fit_transform(_topic_texts)
log.info("Semantic vectorizer ready. Topics: %s", _topic_names)


def tag_post(text: str) -> dict[str, float]:
    post_vec = _vectorizer.transform([text])
    sims     = cosine_similarity(post_vec, _topic_vecs)[0]
    return {
        _topic_names[i]: round(float(sims[i]), 3)
        for i in range(len(_topic_names))
        if sims[i] >= THRESHOLD
    }


async def run():
    consumer = AIOKafkaConsumer(
        IN_TOPIC,
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
    log.info("Filter worker started (semantic threshold=%.2f)", THRESHOLD)

    try:
        async for msg in consumer:
            post: dict = msg.value
            text   = " ".join(filter(None, [post.get("title"), post.get("body", "")])).strip()
            scores = tag_post(text)
            if not scores:
                continue
            post["topic_tags"]   = sorted(scores, key=scores.get, reverse=True)
            post["topic_scores"] = scores
            key = f"{post['platform']}:{post['external_id']}"
            await producer.send_and_wait(OUT_TOPIC, value=post, key=key)
            log.debug("Tagged %s → %s", post["external_id"], post["topic_tags"])
    finally:
        await consumer.stop()
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(run())
