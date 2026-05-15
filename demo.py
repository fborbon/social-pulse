"""
Standalone demo — no Kafka or Postgres required.
Fetches real HackerNews top stories, runs them through the full pipeline
(filter → enrich → summarize) in memory, then starts a GraphQL server.
"""
import os
from pathlib import Path
from dotenv import load_dotenv
load_dotenv(Path(__file__).parent / ".env")

import asyncio
import re
import sys
from collections import Counter
from datetime import datetime, timezone, date
from typing import Optional

import anthropic
import aiohttp
import uvicorn
import strawberry
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from strawberry.fastapi import GraphQLRouter


# ── 1. Fetch from HackerNews ─────────────────────────────────────────────────

HN_API = "https://hacker-news.firebaseio.com/v0"
TRACK_TOPICS = [
    "artificial intelligence", "ai", "llm", "machine learning",
    "climate change", "climate", "energy",
    "cryptocurrency", "bitcoin", "crypto",
    "elections", "politics",
    "open source", "github",
    "security", "privacy", "vulnerability",
    "programming", "python", "rust", "software",
    "startup", "funding",
    "google", "apple", "microsoft", "meta", "amazon",
]
STOP_WORDS = {
    "the","a","an","and","or","but","is","are","was","were","in","on","at",
    "to","for","of","with","by","from","that","this","it","he","she","they",
    "we","i","you","have","has","had","will","be","been","being","not","its",
}


async def _fetch(session, url):
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
        return await r.json()


async def fetch_hn(limit=40):
    async with aiohttp.ClientSession() as s:
        ids = await _fetch(s, f"{HN_API}/topstories.json")
        tasks = [_fetch(s, f"{HN_API}/item/{i}.json") for i in ids[:limit]]
        items = await asyncio.gather(*tasks, return_exceptions=True)
    posts = []
    for item in items:
        if isinstance(item, Exception) or not isinstance(item, dict):
            continue
        body = item.get("text") or item.get("title") or ""
        if not body:
            continue
        posts.append({
            "platform": "hackernews",
            "external_id": str(item["id"]),
            "author": item.get("by"),
            "title": item.get("title"),
            "body": body,
            "url": item.get("url"),
            "raw_score": item.get("score", 0),
            "timestamp": datetime.fromtimestamp(item.get("time", 0), tz=timezone.utc).isoformat(),
        })
    return posts


# ── 2. Filter ────────────────────────────────────────────────────────────────

def filter_posts(posts):
    out = []
    for p in posts:
        text = " ".join(filter(None, [p.get("title"), p.get("body")])).lower()
        tags = [t for t in TRACK_TOPICS if t in text]
        if tags:
            p["topic_tags"] = tags
            out.append(p)
    return out


# ── 3. Enrich (stub) ─────────────────────────────────────────────────────────

_POS = {"good","great","breakthrough","progress","success","win","hope",
        "innovative","exciting","positive","benefit","improve","launch","new"}
_NEG = {"bad","crash","fail","crisis","danger","threat","corrupt","scandal",
        "loss","worse","terrible","disaster","breach","attack","bug","broken"}


def _sentiment(text):
    words = set(re.findall(r"\b\w+\b", text.lower()))
    pos, neg = len(words & _POS), len(words & _NEG)
    score = (pos - neg) / max(pos + neg, 1)
    if score > 0.1:  return "positive", round(score, 2)
    if score < -0.1: return "negative", round(score, 2)
    return "neutral", 0.0


def _entities(text):
    words = re.findall(r"\b[A-Z][a-z]{2,}\b", text)
    return [w for w, _ in Counter(words).most_common(4)]


def enrich(p):
    text = " ".join(filter(None, [p.get("title"), p.get("body")]))
    p["sentiment"], p["sentiment_score"] = _sentiment(text)
    p["entities"] = _entities(text)
    sentences = re.split(r"(?<=[.!?])\s+", text.strip())
    p["summary"] = sentences[0][:180] if sentences else text[:180]
    return p


def enrich_posts(posts):
    return [enrich(p) for p in posts]


# ── 4. Summarize ─────────────────────────────────────────────────────────────

_ai_client = anthropic.Anthropic()


def _ai_summary(topic: str, posts: list[dict], pos_pct: int, neu_pct: int, neg_pct: int) -> str:
    titles = "\n".join(
        f"- {p.get('title') or p.get('body','')[:80]} (score: {p.get('raw_score',0)}, sentiment: {p.get('sentiment','neutral')})"
        for p in sorted(posts, key=lambda x: x.get("raw_score", 0), reverse=True)[:20]
    )
    response = _ai_client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=300,
        system=(
            "You are a social media analyst writing concise daily briefings. "
            "Be factual, neutral, and highlight the most significant stories. "
            "Write 2-3 sentences maximum."
        ),
        messages=[{
            "role": "user",
            "content": (
                f"Write a daily briefing for the topic '{topic}' based on these {len(posts)} posts from today "
                f"({pos_pct}% positive sentiment, {neu_pct}% neutral, {neg_pct}% negative):\n\n{titles}"
            ),
        }],
    )
    return response.content[0].text


def build_summaries(posts):
    by_topic: dict[str, list] = {}
    for p in posts:
        for tag in p.get("topic_tags", []):
            by_topic.setdefault(tag, []).append(p)

    summaries = {}
    for topic, tposts in by_topic.items():
        total = len(tposts)
        sents = Counter(p["sentiment"] for p in tposts)
        words = []
        for p in tposts:
            text = " ".join(filter(None, [p.get("title"), p.get("body","")]))
            words += re.findall(r"\b[a-zA-Z]{4,}\b", text.lower())
        trending = [w for w, _ in Counter(w for w in words if w not in STOP_WORDS).most_common(8)]
        pos_pct = round(sents["positive"] / total * 100)
        neg_pct = round(sents["negative"] / total * 100)
        neu_pct = 100 - pos_pct - neg_pct
        summaries[topic] = {
            "topic": topic,
            "summary_date": date.today().isoformat(),
            "post_count": total,
            "positive_pct": pos_pct / 100,
            "neutral_pct": neu_pct / 100,
            "negative_pct": neg_pct / 100,
            "trending_words": trending,
            "summary_text": _ai_summary(topic, tposts, pos_pct, neu_pct, neg_pct),
        }
    return summaries


# ── 5. In-memory store ───────────────────────────────────────────────────────

DB_POSTS: list[dict] = []
DB_SUMMARIES: dict[str, dict] = {}


# ── 6. GraphQL schema ────────────────────────────────────────────────────────

@strawberry.type
class Post:
    id: str
    platform: str
    author: Optional[str]
    title: Optional[str]
    body: str
    url: Optional[str]
    topic_tags: list[str]
    raw_score: int
    timestamp: str
    sentiment: Optional[str]
    sentiment_score: Optional[float]
    entities: list[str]
    summary: Optional[str]


@strawberry.type
class SentimentBreakdown:
    positive: float
    neutral: float
    negative: float


@strawberry.type
class DailySummary:
    summary_date: str
    topic: str
    summary_text: str
    post_count: int
    sentiment: SentimentBreakdown
    trending_words: list[str]


@strawberry.type
class TopicStats:
    topic: str
    post_count: int
    positive_pct: float
    neutral_pct: float
    negative_pct: float


def _to_post(p, idx) -> Post:
    return Post(
        id=str(idx),
        platform=p["platform"],
        author=p.get("author"),
        title=p.get("title"),
        body=p.get("body", ""),
        url=p.get("url"),
        topic_tags=p.get("topic_tags", []),
        raw_score=p.get("raw_score", 0),
        timestamp=p.get("timestamp", ""),
        sentiment=p.get("sentiment"),
        sentiment_score=p.get("sentiment_score"),
        entities=p.get("entities", []),
        summary=p.get("summary"),
    )


@strawberry.type
class Query:
    @strawberry.field
    def posts(
        self,
        topic: Optional[str] = None,
        platform: Optional[str] = None,
        sentiment: Optional[str] = None,
        limit: int = 20,
        offset: int = 0,
    ) -> list[Post]:
        results = DB_POSTS
        if topic:
            results = [p for p in results if topic.lower() in p.get("topic_tags", [])]
        if platform:
            results = [p for p in results if p["platform"] == platform]
        if sentiment:
            results = [p for p in results if p.get("sentiment") == sentiment]
        results = sorted(results, key=lambda p: p.get("raw_score", 0), reverse=True)
        return [_to_post(p, i) for i, p in enumerate(results[offset:offset+limit])]

    @strawberry.field
    def daily_summary(self, topic: str) -> Optional[DailySummary]:
        s = DB_SUMMARIES.get(topic.lower())
        if not s:
            return None
        return DailySummary(
            summary_date=s["summary_date"],
            topic=s["topic"],
            summary_text=s["summary_text"],
            post_count=s["post_count"],
            sentiment=SentimentBreakdown(
                positive=s["positive_pct"],
                neutral=s["neutral_pct"],
                negative=s["negative_pct"],
            ),
            trending_words=s["trending_words"],
        )

    @strawberry.field
    def topic_stats(self) -> list[TopicStats]:
        stats = []
        for topic, s in DB_SUMMARIES.items():
            stats.append(TopicStats(
                topic=topic,
                post_count=s["post_count"],
                positive_pct=round(s["positive_pct"] * 100, 1),
                neutral_pct=round(s["neutral_pct"] * 100, 1),
                negative_pct=round(s["negative_pct"] * 100, 1),
            ))
        return sorted(stats, key=lambda x: x.post_count, reverse=True)

    @strawberry.field
    def platforms(self) -> list[str]:
        return list({p["platform"] for p in DB_POSTS})


# ── 7. Bootstrap ─────────────────────────────────────────────────────────────

async def load_data():
    print("Fetching HackerNews top stories...", flush=True)
    raw = await fetch_hn(limit=40)
    print(f"  Fetched {len(raw)} posts", flush=True)

    filtered = filter_posts(raw)
    print(f"  Filtered to {len(filtered)} on-topic posts", flush=True)

    enriched = enrich_posts(filtered)
    print(f"  Enriched {len(enriched)} posts", flush=True)

    summaries = build_summaries(enriched)
    print(f"  Built summaries for {len(summaries)} topics: {list(summaries.keys())}", flush=True)

    DB_POSTS.extend(enriched)
    DB_SUMMARIES.update(summaries)
    print("\nReady! Open http://localhost:8000/graphql\n", flush=True)


from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(_app):
    await load_data()
    yield

schema = strawberry.Schema(query=Query)
graphql_app = GraphQLRouter(schema, graphql_ide="graphiql")

app = FastAPI(title="Social Pulse Demo", lifespan=lifespan)
app.include_router(graphql_app, prefix="/graphql")
app.mount("/static", StaticFiles(directory=Path(__file__).parent / "frontend"), name="static")


@app.get("/health")
def health():
    return {"status": "ok", "posts": len(DB_POSTS), "topics": list(DB_SUMMARIES.keys())}


@app.get("/")
def dashboard():
    return FileResponse(Path(__file__).parent / "frontend" / "index.html")


if __name__ == "__main__":
    uvicorn.run("demo:app", host="0.0.0.0", port=8000, reload=False)
