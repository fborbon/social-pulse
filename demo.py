"""
Standalone demo — no Kafka or Postgres required.
Pipeline: fetch → semantic_filter → time_decay_rank → enrich → deduplicate → summarize
"""
import os
from pathlib import Path
from dotenv import load_dotenv
load_dotenv(Path(__file__).parent / ".env")

import asyncio
import re
from collections import Counter, defaultdict
from contextlib import asynccontextmanager
from datetime import datetime, timezone, date
from typing import Optional

import anthropic
import aiohttp
import numpy as np
import uvicorn
import strawberry
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from strawberry.fastapi import GraphQLRouter
from pydantic import BaseModel
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity


# ── 1. Fetch ──────────────────────────────────────────────────────────────────

HN_API = "https://hacker-news.firebaseio.com/v0"

STOP_WORDS = {
    "the","a","an","and","or","but","is","are","was","were","in","on","at",
    "to","for","of","with","by","from","that","this","it","he","she","they",
    "we","i","you","have","has","had","will","be","been","being","not","its",
}

# Each topic maps to a rich description used for semantic matching
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
            "platform":    "hackernews",
            "external_id": str(item["id"]),
            "author":      item.get("by"),
            "title":       item.get("title"),
            "body":        body,
            "url":         item.get("url"),
            "raw_score":   item.get("score", 0),
            "timestamp":   datetime.fromtimestamp(item.get("time", 0), tz=timezone.utc).isoformat(),
        })
    return posts


# ── 2. Semantic filter ────────────────────────────────────────────────────────
# Replaces keyword matching with TF-IDF cosine similarity.
# A post passes if its text is semantically close enough to at least one topic.

SEMANTIC_THRESHOLD = 0.07   # min cosine similarity to be tagged with a topic


def semantic_filter(posts: list[dict]) -> list[dict]:
    topic_names  = list(TOPIC_DESCRIPTIONS.keys())
    topic_texts  = list(TOPIC_DESCRIPTIONS.values())
    post_texts   = [
        " ".join(filter(None, [p.get("title"), p.get("body", "")]))
        for p in posts
    ]

    # Fit on topic descriptions only so the vocabulary is topic-centric.
    # Posts are then projected into that vocabulary space.
    vectorizer  = TfidfVectorizer(stop_words="english")
    topic_vecs  = vectorizer.fit_transform(topic_texts)
    post_vecs   = vectorizer.transform(post_texts)
    sims        = cosine_similarity(post_vecs, topic_vecs)   # (n_posts, n_topics)

    result = []
    for i, post in enumerate(posts):
        row          = sims[i]
        matched      = [(topic_names[j], float(row[j])) for j in range(len(topic_names)) if row[j] >= SEMANTIC_THRESHOLD]
        if not matched:
            continue
        matched.sort(key=lambda x: x[1], reverse=True)
        post["topic_tags"]    = [t for t, _ in matched]
        post["topic_scores"]  = {t: round(s, 3) for t, s in matched}
        result.append(post)

    return result


# ── 3. Time-decay ranking ─────────────────────────────────────────────────────
# ranked_score = raw_score / (age_hours + 2)^1.5
# Prevents old high-score posts from dominating; recent posts with moderate
# scores rank above stale viral ones.

def time_decay_rank(posts: list[dict]) -> list[dict]:
    now = datetime.now(timezone.utc)
    for post in posts:
        try:
            ts        = datetime.fromisoformat(post["timestamp"])
            age_hours = max((now - ts).total_seconds() / 3600, 0.1)
        except Exception:
            age_hours = 24.0
        post["ranked_score"] = round(post.get("raw_score", 0) / (age_hours + 2) ** 1.5, 2)
    return sorted(posts, key=lambda p: p["ranked_score"], reverse=True)


# ── 4. Enrich ─────────────────────────────────────────────────────────────────

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
    return [w for w, _ in Counter(re.findall(r"\b[A-Z][a-z]{2,}\b", text)).most_common(4)]


def enrich(p):
    text = " ".join(filter(None, [p.get("title"), p.get("body")]))
    p["sentiment"], p["sentiment_score"] = _sentiment(text)
    p["entities"]  = _entities(text)
    sentences      = re.split(r"(?<=[.!?])\s+", text.strip())
    p["summary"]   = sentences[0][:180] if sentences else text[:180]
    return p


def enrich_posts(posts):
    return [enrich(p) for p in posts]


# ── 5. Deduplication + cross-platform aggregation ─────────────────────────────
# Builds a TF-IDF similarity matrix across all post texts.
# Posts with cosine similarity ≥ DEDUP_THRESHOLD are grouped into a cluster.
# The cluster representative is the post with the highest ranked_score.
# If a cluster contains posts from multiple platforms, cross_platform=True.

DEDUP_THRESHOLD = 0.55


def deduplicate(posts: list[dict]) -> list[dict]:
    if len(posts) < 2:
        for i, p in enumerate(posts):
            p.update({"cluster_id": i, "cluster_size": 1, "platforms": [p["platform"]], "cross_platform": False})
        return posts

    texts      = [" ".join(filter(None, [p.get("title"), p.get("body", "")])) for p in posts]
    vectorizer = TfidfVectorizer(stop_words="english")
    tfidf      = vectorizer.fit_transform(texts)
    sim        = cosine_similarity(tfidf)

    assigned:  set[int]       = set()
    clusters:  list[list[int]] = []

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


# ── 6. User interest profile ──────────────────────────────────────────────────
# In-memory profile: records how often the user engages with each topic/platform.
# Engagement boosts the ranked_score via a multiplicative factor.
# topic weight  ∈ [0, 2.0], increments by 0.15 per click
# platform weight ∈ [0, 1.0], increments by 0.05 per click

_profile: dict[str, float] = defaultdict(float)


def personalized_score(post: dict) -> float:
    score = post.get("ranked_score", 0.0)
    for tag in post.get("topic_tags", []):
        score *= 1 + _profile.get(f"topic:{tag}", 0) * 0.5
    score *= 1 + _profile.get(f"platform:{post['platform']}", 0) * 0.3
    return round(score, 3)


# ── 7. AI summaries ───────────────────────────────────────────────────────────

_ai_client = anthropic.Anthropic()


def _ai_summary(topic: str, posts: list[dict], pos_pct: int, neu_pct: int, neg_pct: int) -> str:
    titles = "\n".join(
        f"- {p.get('title') or p.get('body','')[:80]} "
        f"(score: {p.get('raw_score',0)}, sentiment: {p.get('sentiment','neutral')})"
        for p in sorted(posts, key=lambda x: x.get("ranked_score", 0), reverse=True)[:20]
    )
    resp = _ai_client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=300,
        system=(
            "You are a social media analyst writing concise daily briefings. "
            "Be factual, neutral, and highlight the most significant stories. "
            "Write 2-3 sentences maximum."
        ),
        messages=[{"role": "user", "content": (
            f"Write a daily briefing for the topic '{topic}' based on these "
            f"{len(posts)} posts from today "
            f"({pos_pct}% positive, {neu_pct}% neutral, {neg_pct}% negative):\n\n{titles}"
        )}],
    )
    return resp.content[0].text


def build_summaries(posts: list[dict]) -> dict:
    by_topic: dict[str, list] = {}
    for p in posts:
        for tag in p.get("topic_tags", []):
            by_topic.setdefault(tag, []).append(p)

    summaries: dict[str, dict] = {}
    for topic, tposts in by_topic.items():
        total   = len(tposts)
        sents   = Counter(p["sentiment"] for p in tposts)
        words   = []
        for p in tposts:
            text   = " ".join(filter(None, [p.get("title"), p.get("body", "")]))
            words += re.findall(r"\b[a-zA-Z]{4,}\b", text.lower())
        trending = [w for w, _ in Counter(w for w in words if w not in STOP_WORDS).most_common(8)]
        pos_pct  = round(sents["positive"] / total * 100)
        neg_pct  = round(sents["negative"] / total * 100)
        neu_pct  = 100 - pos_pct - neg_pct
        summaries[topic] = {
            "topic":        topic,
            "summary_date": date.today().isoformat(),
            "post_count":   total,
            "positive_pct": pos_pct / 100,
            "neutral_pct":  neu_pct / 100,
            "negative_pct": neg_pct / 100,
            "trending_words": trending,
            "summary_text": _ai_summary(topic, tposts, pos_pct, neu_pct, neg_pct),
        }
    return summaries


# ── 8. In-memory store ────────────────────────────────────────────────────────

DB_POSTS:     list[dict]       = []
DB_SUMMARIES: dict[str, dict]  = {}


# ── 9. GraphQL schema ─────────────────────────────────────────────────────────

@strawberry.type
class Post:
    id:              str
    platform:        str
    author:          Optional[str]
    title:           Optional[str]
    body:            str
    url:             Optional[str]
    topic_tags:      list[str]
    raw_score:       int
    ranked_score:    float
    timestamp:       str
    sentiment:       Optional[str]
    sentiment_score: Optional[float]
    entities:        list[str]
    summary:         Optional[str]
    cluster_id:      int
    cluster_size:    int
    platforms:       list[str]
    cross_platform:  bool
    topic_scores:    strawberry.scalars.JSON


@strawberry.type
class SentimentBreakdown:
    positive: float
    neutral:  float
    negative: float


@strawberry.type
class DailySummary:
    summary_date:   str
    topic:          str
    summary_text:   str
    post_count:     int
    sentiment:      SentimentBreakdown
    trending_words: list[str]


@strawberry.type
class TopicStats:
    topic:         str
    post_count:    int
    positive_pct:  float
    neutral_pct:   float
    negative_pct:  float


@strawberry.type
class ProfileEntry:
    key:    str
    weight: float


def _to_post(p: dict, idx: int) -> Post:
    return Post(
        id=str(idx),
        platform=p["platform"],
        author=p.get("author"),
        title=p.get("title"),
        body=p.get("body", ""),
        url=p.get("url"),
        topic_tags=p.get("topic_tags", []),
        raw_score=p.get("raw_score", 0),
        ranked_score=p.get("ranked_score", 0.0),
        timestamp=p.get("timestamp", ""),
        sentiment=p.get("sentiment"),
        sentiment_score=p.get("sentiment_score"),
        entities=p.get("entities", []),
        summary=p.get("summary"),
        cluster_id=p.get("cluster_id", idx),
        cluster_size=p.get("cluster_size", 1),
        platforms=p.get("platforms", [p["platform"]]),
        cross_platform=p.get("cross_platform", False),
        topic_scores=p.get("topic_scores", {}),
    )


@strawberry.type
class Query:
    @strawberry.field
    def posts(
        self,
        topic:         Optional[str]  = None,
        platform:      Optional[str]  = None,
        sentiment:     Optional[str]  = None,
        personalized:  bool           = False,
        limit:         int            = 20,
        offset:        int            = 0,
    ) -> list[Post]:
        results = list(DB_POSTS)
        if topic:
            results = [p for p in results if topic.lower() in p.get("topic_tags", [])]
        if platform:
            results = [p for p in results if p["platform"] == platform]
        if sentiment:
            results = [p for p in results if p.get("sentiment") == sentiment]

        sort_key = personalized_score if personalized else lambda p: p.get("ranked_score", 0)
        results  = sorted(results, key=sort_key, reverse=True)
        return [_to_post(p, i) for i, p in enumerate(results[offset : offset + limit])]

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
        return sorted(
            [TopicStats(
                topic=topic,
                post_count=s["post_count"],
                positive_pct=round(s["positive_pct"] * 100, 1),
                neutral_pct=round(s["neutral_pct"] * 100, 1),
                negative_pct=round(s["negative_pct"] * 100, 1),
            ) for topic, s in DB_SUMMARIES.items()],
            key=lambda x: x.post_count, reverse=True,
        )

    @strawberry.field
    def platforms(self) -> list[str]:
        return sorted({p["platform"] for p in DB_POSTS})

    @strawberry.field
    def interest_profile(self) -> list[ProfileEntry]:
        return sorted(
            [ProfileEntry(key=k, weight=round(v, 3)) for k, v in _profile.items() if v > 0],
            key=lambda e: e.weight, reverse=True,
        )


# ── 10. Bootstrap ─────────────────────────────────────────────────────────────

async def load_data():
    print("Fetching HackerNews top stories...", flush=True)
    raw      = await fetch_hn(limit=40)
    print(f"  Fetched {len(raw)} posts", flush=True)

    filtered = semantic_filter(raw)
    print(f"  Semantic filter: {len(filtered)} on-topic posts", flush=True)

    ranked   = time_decay_rank(filtered)
    print(f"  Time-decay ranked", flush=True)

    enriched = enrich_posts(ranked)
    print(f"  Enriched {len(enriched)} posts", flush=True)

    deduped  = deduplicate(enriched)
    print(f"  Deduplicated to {len(deduped)} clusters "
          f"({sum(1 for p in deduped if p['cross_platform'])} cross-platform)", flush=True)

    summaries = build_summaries(deduped)
    print(f"  Built summaries for {len(summaries)} topics: {list(summaries.keys())}", flush=True)

    DB_POSTS.extend(deduped)
    DB_SUMMARIES.update(summaries)
    print("\nReady!  http://localhost:8000\n", flush=True)


@asynccontextmanager
async def lifespan(_app):
    await load_data()
    yield


schema      = strawberry.Schema(query=Query)
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


class EngageRequest(BaseModel):
    platform: str = ""
    topics: list[str] = []


@app.post("/profile/engage")
def record_engagement(body: EngageRequest):
    """Record a user interaction to build the interest profile."""
    for t in body.topics:
        _profile[f"topic:{t}"] = min(_profile[f"topic:{t}"] + 0.15, 2.0)
    if body.platform:
        _profile[f"platform:{body.platform}"] = min(_profile[f"platform:{body.platform}"] + 0.05, 1.0)
    return {"profile": dict(_profile)}


@app.get("/profile")
def get_profile():
    return {"profile": dict(_profile)}


if __name__ == "__main__":
    uvicorn.run("demo:app", host="0.0.0.0", port=8000, reload=False)
