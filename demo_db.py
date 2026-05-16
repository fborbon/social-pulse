"""
SQLite persistence layer for the Social Pulse demo.
Stores posts, enriched data, daily summaries, and spike history
so data survives restarts and historical charts work.
"""
import json
import logging
from datetime import date, datetime, timezone, timedelta
from pathlib import Path

import aiosqlite
import numpy as np

log = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent / "social_pulse.db"


# ── Schema ────────────────────────────────────────────────────────────────────

SCHEMA = """
CREATE TABLE IF NOT EXISTS posts (
    id            TEXT PRIMARY KEY,
    platform      TEXT NOT NULL,
    external_id   TEXT NOT NULL,
    author        TEXT,
    title         TEXT,
    body          TEXT NOT NULL,
    url           TEXT,
    topic_tags    TEXT DEFAULT '[]',
    raw_score     INTEGER DEFAULT 0,
    ranked_score  REAL DEFAULT 0,
    timestamp     TEXT,
    sentiment     TEXT DEFAULT 'neutral',
    sentiment_score REAL DEFAULT 0,
    entities      TEXT DEFAULT '[]',
    summary       TEXT,
    cluster_id    INTEGER DEFAULT 0,
    cluster_size  INTEGER DEFAULT 1,
    fetched_date  TEXT NOT NULL,
    UNIQUE(platform, external_id)
);

CREATE TABLE IF NOT EXISTS daily_summaries (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    summary_date    TEXT NOT NULL,
    topic           TEXT NOT NULL,
    summary_text    TEXT NOT NULL,
    post_count      INTEGER DEFAULT 0,
    positive_pct    REAL DEFAULT 0,
    neutral_pct     REAL DEFAULT 0,
    negative_pct    REAL DEFAULT 0,
    trending_words  TEXT DEFAULT '[]',
    UNIQUE(summary_date, topic)
);

CREATE INDEX IF NOT EXISTS idx_posts_date    ON posts(fetched_date);
CREATE INDEX IF NOT EXISTS idx_posts_topic   ON posts(topic_tags);
CREATE INDEX IF NOT EXISTS idx_summary_date  ON daily_summaries(summary_date);
CREATE INDEX IF NOT EXISTS idx_summary_topic ON daily_summaries(topic);
"""


async def init_db() -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(SCHEMA)
        await db.commit()
    log.info("SQLite DB ready at %s", DB_PATH)


# ── Write ─────────────────────────────────────────────────────────────────────

async def save_posts(posts: list[dict]) -> int:
    today = date.today().isoformat()
    saved = 0
    async with aiosqlite.connect(DB_PATH) as db:
        for p in posts:
            pid = f"{p['platform']}:{p['external_id']}"
            try:
                await db.execute(
                    """INSERT OR IGNORE INTO posts
                       (id,platform,external_id,author,title,body,url,topic_tags,
                        raw_score,ranked_score,timestamp,sentiment,sentiment_score,
                        entities,summary,cluster_id,cluster_size,fetched_date)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (pid, p.get("platform"), p.get("external_id"),
                     p.get("author"), p.get("title"), p.get("body",""),
                     p.get("url"), json.dumps(p.get("topic_tags",[])),
                     p.get("raw_score",0), p.get("ranked_score",0),
                     p.get("timestamp",""), p.get("sentiment","neutral"),
                     p.get("sentiment_score",0), json.dumps(p.get("entities",[])),
                     p.get("summary",""), p.get("cluster_id",0),
                     p.get("cluster_size",1), today),
                )
                saved += 1
            except Exception as e:
                log.debug("save_posts skip %s: %s", pid, e)
        await db.commit()
    return saved


async def save_summaries(summaries: dict) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        for topic, s in summaries.items():
            await db.execute(
                """INSERT OR REPLACE INTO daily_summaries
                   (summary_date,topic,summary_text,post_count,
                    positive_pct,neutral_pct,negative_pct,trending_words)
                   VALUES (?,?,?,?,?,?,?,?)""",
                (s["summary_date"], topic, s["summary_text"], s["post_count"],
                 s["positive_pct"], s["neutral_pct"], s["negative_pct"],
                 json.dumps(s["trending_words"])),
            )
        await db.commit()
    log.info("Saved %d summaries to DB", len(summaries))


# ── Read ──────────────────────────────────────────────────────────────────────

async def load_today_posts() -> list[dict]:
    today = date.today().isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM posts WHERE fetched_date = ?", (today,)
        ) as cur:
            rows = await cur.fetchall()
    posts = []
    for r in rows:
        p = dict(r)
        p["topic_tags"] = json.loads(p.get("topic_tags") or "[]")
        p["entities"]   = json.loads(p.get("entities")   or "[]")
        posts.append(p)
    return posts


async def load_today_summaries() -> dict:
    today = date.today().isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM daily_summaries WHERE summary_date = ?", (today,)
        ) as cur:
            rows = await cur.fetchall()
    out = {}
    for r in rows:
        d = dict(r)
        d["trending_words"] = json.loads(d.get("trending_words") or "[]")
        out[d["topic"]] = d
    return out


async def get_history(topic: str, days: int = 30) -> list[dict]:
    since = (date.today() - timedelta(days=days)).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            """SELECT summary_date, post_count, positive_pct, neutral_pct, negative_pct
               FROM daily_summaries
               WHERE topic = ? AND summary_date >= ?
               ORDER BY summary_date""",
            (topic, since),
        ) as cur:
            rows = await cur.fetchall()
    return [
        {"date": r[0], "post_count": r[1],
         "positive_pct": r[2], "neutral_pct": r[3], "negative_pct": r[4]}
        for r in rows
    ]


async def get_all_topics_history(days: int = 30) -> dict[str, list]:
    since = (date.today() - timedelta(days=days)).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            """SELECT topic, summary_date, post_count
               FROM daily_summaries WHERE summary_date >= ?
               ORDER BY topic, summary_date""",
            (since,),
        ) as cur:
            rows = await cur.fetchall()
    result: dict[str, list] = {}
    for topic, day, count in rows:
        result.setdefault(topic, []).append({"date": day, "post_count": count})
    return result


# ── Spike detection ───────────────────────────────────────────────────────────

async def detect_spikes(threshold: float = 1.5) -> dict[str, dict]:
    """
    Compare today's post count per topic against the 7-day rolling average.
    Returns topics where today > threshold * mean (default: 50% above baseline).
    """
    today = date.today().isoformat()
    since = (date.today() - timedelta(days=8)).isoformat()

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            """SELECT topic, summary_date, post_count FROM daily_summaries
               WHERE summary_date >= ? ORDER BY summary_date""",
            (since,),
        ) as cur:
            rows = await cur.fetchall()

    by_topic: dict[str, dict] = {}
    for topic, day, count in rows:
        by_topic.setdefault(topic, {"today": 0, "history": []})
        if day == today:
            by_topic[topic]["today"] = count
        else:
            by_topic[topic]["history"].append(count)

    spikes = {}
    for topic, data in by_topic.items():
        hist = data["history"]
        today_count = data["today"]
        if not hist or today_count == 0:
            continue
        mean = float(np.mean(hist))
        std  = float(np.std(hist))
        if mean == 0:
            continue
        ratio = today_count / mean
        if ratio >= threshold:
            spikes[topic] = {
                "ratio":       round(ratio, 2),
                "today":       today_count,
                "baseline":    round(mean, 1),
                "std":         round(std, 1),
                "severity":    "high" if ratio >= 2.5 else "medium" if ratio >= 1.8 else "low",
            }
    return spikes
