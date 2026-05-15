from __future__ import annotations
import strawberry
from datetime import date
from typing import Optional
from db import get_pool


# ---------- Types ----------

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
    sentiment: Optional[str] = None
    sentiment_score: Optional[float] = None
    category: Optional[str] = None
    entities: list[str] = strawberry.field(default_factory=list)
    summary: Optional[str] = None


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
    avg_sentiment_score: float
    positive_pct: float
    neutral_pct: float
    negative_pct: float


# ---------- Helpers ----------

def _row_to_post(row) -> Post:
    return Post(
        id=str(row["id"]),
        platform=row["platform"],
        author=row.get("author"),
        title=row.get("title"),
        body=row["body"],
        url=row.get("url"),
        topic_tags=list(row.get("topic_tags") or []),
        raw_score=row.get("raw_score", 0),
        timestamp=row["timestamp"].isoformat(),
        sentiment=row.get("sentiment"),
        sentiment_score=row.get("sentiment_score"),
        category=row.get("category"),
        entities=list(row.get("entities") or []),
        summary=row.get("summary"),
    )


# ---------- Queries ----------

@strawberry.type
class Query:
    @strawberry.field
    async def posts(
        self,
        topic: Optional[str] = None,
        platform: Optional[str] = None,
        sentiment: Optional[str] = None,
        limit: int = 20,
        offset: int = 0,
    ) -> list[Post]:
        pool = await get_pool()
        conditions = ["TRUE"]
        params: list = []

        if topic:
            params.append(topic.lower())
            conditions.append(f"${len(params)} = ANY(p.topic_tags)")
        if platform:
            params.append(platform)
            conditions.append(f"p.platform = ${len(params)}")
        if sentiment:
            params.append(sentiment)
            conditions.append(f"e.sentiment = ${len(params)}")

        params.extend([limit, offset])
        where = " AND ".join(conditions)

        rows = await pool.fetch(
            f"""
            SELECT p.*, e.sentiment, e.sentiment_score, e.category, e.entities, e.summary
            FROM posts p
            LEFT JOIN enriched_posts e ON e.id = p.id
            WHERE {where}
            ORDER BY p.timestamp DESC
            LIMIT ${len(params) - 1} OFFSET ${len(params)}
            """,
            *params,
        )
        return [_row_to_post(r) for r in rows]

    @strawberry.field
    async def post(self, id: str) -> Optional[Post]:
        pool = await get_pool()
        row = await pool.fetchrow(
            """
            SELECT p.*, e.sentiment, e.sentiment_score, e.category, e.entities, e.summary
            FROM posts p
            LEFT JOIN enriched_posts e ON e.id = p.id
            WHERE p.id = $1
            """,
            id,
        )
        return _row_to_post(row) if row else None

    @strawberry.field
    async def daily_summary(
        self,
        topic: str,
        summary_date: Optional[str] = None,
    ) -> Optional[DailySummary]:
        pool = await get_pool()
        target = summary_date or date.today().isoformat()
        row = await pool.fetchrow(
            "SELECT * FROM daily_summaries WHERE topic = $1 AND summary_date = $2",
            topic, date.fromisoformat(target),
        )
        if not row:
            return None
        return DailySummary(
            summary_date=str(row["summary_date"]),
            topic=row["topic"],
            summary_text=row["summary_text"],
            post_count=row["post_count"],
            sentiment=SentimentBreakdown(
                positive=row["positive_pct"],
                neutral=row["neutral_pct"],
                negative=row["negative_pct"],
            ),
            trending_words=list(row["trending_words"] or []),
        )

    @strawberry.field
    async def topic_stats(self, days: int = 7) -> list[TopicStats]:
        pool = await get_pool()
        rows = await pool.fetch(
            """
            SELECT
                unnest(p.topic_tags) AS topic,
                COUNT(*)             AS post_count,
                AVG(e.sentiment_score) AS avg_score,
                AVG(CASE WHEN e.sentiment='positive' THEN 1.0 ELSE 0 END) AS pos_pct,
                AVG(CASE WHEN e.sentiment='neutral'  THEN 1.0 ELSE 0 END) AS neu_pct,
                AVG(CASE WHEN e.sentiment='negative' THEN 1.0 ELSE 0 END) AS neg_pct
            FROM posts p
            JOIN enriched_posts e ON e.id = p.id
            WHERE p.timestamp > NOW() - ($1 || ' days')::INTERVAL
            GROUP BY topic
            ORDER BY post_count DESC
            """,
            str(days),
        )
        return [
            TopicStats(
                topic=r["topic"],
                post_count=r["post_count"],
                avg_sentiment_score=round(r["avg_score"] or 0, 3),
                positive_pct=round((r["pos_pct"] or 0) * 100, 1),
                neutral_pct=round((r["neu_pct"] or 0) * 100, 1),
                negative_pct=round((r["neg_pct"] or 0) * 100, 1),
            )
            for r in rows
        ]

    @strawberry.field
    async def platforms(self) -> list[str]:
        pool = await get_pool()
        rows = await pool.fetch("SELECT DISTINCT platform FROM posts ORDER BY platform")
        return [r["platform"] for r in rows]
