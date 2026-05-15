CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS posts (
    id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    platform    VARCHAR(50) NOT NULL,
    external_id VARCHAR(255) NOT NULL,
    author      VARCHAR(255),
    title       TEXT,
    body        TEXT NOT NULL,
    url         TEXT,
    topic_tags  TEXT[] DEFAULT '{}',
    raw_score   INTEGER DEFAULT 0,
    timestamp   TIMESTAMPTZ NOT NULL,
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (platform, external_id)
);

CREATE TABLE IF NOT EXISTS enriched_posts (
    id          UUID PRIMARY KEY REFERENCES posts(id) ON DELETE CASCADE,
    sentiment   VARCHAR(20),   -- positive / neutral / negative
    sentiment_score FLOAT,
    category    VARCHAR(100),
    entities    TEXT[] DEFAULT '{}',
    summary     TEXT,
    enriched_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS daily_summaries (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    summary_date        DATE NOT NULL,
    topic               VARCHAR(255) NOT NULL,
    summary_text        TEXT NOT NULL,
    post_count          INTEGER DEFAULT 0,
    positive_pct        FLOAT DEFAULT 0,
    neutral_pct         FLOAT DEFAULT 0,
    negative_pct        FLOAT DEFAULT 0,
    trending_words      TEXT[] DEFAULT '{}',
    top_post_ids        UUID[] DEFAULT '{}',
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (summary_date, topic)
);

CREATE INDEX IF NOT EXISTS idx_posts_platform   ON posts(platform);
CREATE INDEX IF NOT EXISTS idx_posts_timestamp  ON posts(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_posts_topic_tags ON posts USING GIN(topic_tags);
CREATE INDEX IF NOT EXISTS idx_summaries_date   ON daily_summaries(summary_date DESC);
