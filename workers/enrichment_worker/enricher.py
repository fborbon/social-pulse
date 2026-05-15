"""
Stub AI enrichment. Returns deterministic mock values.
Swap the body of `enrich()` with real API calls when ready.
"""
import hashlib
import re
from collections import Counter


_POSITIVE = {"good", "great", "breakthrough", "progress", "success", "win",
             "hope", "innovative", "exciting", "positive", "benefit"}
_NEGATIVE = {"bad", "crash", "fail", "crisis", "danger", "threat", "corrupt",
             "scandal", "loss", "worse", "terrible", "disaster"}

_CATEGORIES = {
    "artificial intelligence": ["ai", "gpt", "llm", "neural", "model", "openai", "machine learning"],
    "climate change": ["climate", "carbon", "emissions", "temperature", "fossil", "renewable"],
    "cryptocurrency": ["bitcoin", "crypto", "blockchain", "ethereum", "token", "defi"],
    "elections": ["election", "vote", "ballot", "candidate", "poll", "democracy"],
}


def _sentiment(text: str) -> tuple[str, float]:
    words = set(re.findall(r"\b\w+\b", text.lower()))
    pos = len(words & _POSITIVE)
    neg = len(words & _NEGATIVE)
    score = (pos - neg) / max(pos + neg, 1)
    if score > 0.1:
        return "positive", round(score, 2)
    if score < -0.1:
        return "negative", round(score, 2)
    return "neutral", round(score, 2)


def _category(text: str, tags: list[str]) -> str:
    lower = text.lower()
    for tag in tags:
        if tag in _CATEGORIES:
            return tag
    for cat, keywords in _CATEGORIES.items():
        if any(kw in lower for kw in keywords):
            return cat
    return "general"


def _entities(text: str) -> list[str]:
    # Stub: return capitalized words as stand-in for named entities
    words = re.findall(r"\b[A-Z][a-z]{2,}\b", text)
    top = [w for w, _ in Counter(words).most_common(5)]
    return top


def _summarize(title: str | None, body: str) -> str:
    src = title or body
    sentences = re.split(r"(?<=[.!?])\s+", src.strip())
    return sentences[0][:200] if sentences else src[:200]


def enrich(post: dict) -> dict:
    text = " ".join(filter(None, [post.get("title"), post.get("body")]))
    sentiment, score = _sentiment(text)
    post["sentiment"] = sentiment
    post["sentiment_score"] = score
    post["category"] = _category(text, post.get("topic_tags", []))
    post["entities"] = _entities(text)
    post["summary"] = _summarize(post.get("title"), post.get("body", ""))
    return post
