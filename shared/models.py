from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field
import uuid


class RawPost(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    platform: str
    external_id: str
    author: Optional[str] = None
    title: Optional[str] = None
    body: str
    url: Optional[str] = None
    raw_score: int = 0
    timestamp: datetime

    def kafka_key(self) -> str:
        return f"{self.platform}:{self.external_id}"


class FilteredPost(RawPost):
    topic_tags: list[str] = []


class EnrichedPost(FilteredPost):
    sentiment: str = "neutral"        # positive / neutral / negative
    sentiment_score: float = 0.0      # -1.0 to 1.0
    category: str = "general"
    entities: list[str] = []
    summary: str = ""


class DailySummary(BaseModel):
    summary_date: str                 # YYYY-MM-DD
    topic: str
    summary_text: str
    post_count: int
    positive_pct: float
    neutral_pct: float
    negative_pct: float
    trending_words: list[str]
    top_post_ids: list[str]
