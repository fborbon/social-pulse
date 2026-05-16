"""
RAG (Retrieval-Augmented Generation) and Entity Graph for Social Pulse demo.

RAG uses TF-IDF for retrieval and AWS Bedrock Claude 3 Haiku for synthesis.
Entity graph builds a co-occurrence network of named entities extracted
from enriched posts, ready for D3.js force-directed rendering.
"""
import logging
import re
from collections import Counter

import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer

import bedrock_client

log = logging.getLogger(__name__)

_posts_ref:    list[dict]          = []
_vectorizer:   TfidfVectorizer | None = None
_index_matrix = None


def init_rag(posts: list[dict]) -> None:
    """Call once after posts are loaded. Builds the TF-IDF index."""
    global _posts_ref, _vectorizer, _index_matrix
    _posts_ref = posts
    _rebuild_index(posts)


def _rebuild_index(posts: list[dict]) -> None:
    global _vectorizer, _index_matrix
    if not posts:
        return
    texts = [
        " ".join(filter(None, [p.get("title"), p.get("body", "")]))
        for p in posts
    ]
    _vectorizer   = TfidfVectorizer(stop_words="english", max_features=15000)
    _index_matrix = _vectorizer.fit_transform(texts)
    log.info("RAG index built: %d posts, %d features", len(posts), _index_matrix.shape[1])


def search_posts(query: str, top_k: int = 8) -> list[dict]:
    """Return the top_k most relevant posts for the query."""
    if _vectorizer is None or _index_matrix is None or not _posts_ref:
        return []
    from sklearn.metrics.pairwise import cosine_similarity
    q_vec = _vectorizer.transform([query])
    sims  = cosine_similarity(q_vec, _index_matrix)[0]
    top   = sims.argsort()[-top_k:][::-1]
    return [
        {**_posts_ref[i], "relevance": round(float(sims[i]), 3)}
        for i in top if sims[i] > 0.03
    ]


def rag_answer(query: str, top_k: int = 8) -> dict:
    """Retrieve relevant posts and synthesize an answer via Bedrock Claude 3 Haiku."""
    sources = search_posts(query, top_k)
    if not sources:
        return {"query": query, "answer": "No relevant posts found.", "sources": []}

    context = "\n\n".join([
        f"[{p['platform'].upper()}] {p.get('title') or p.get('body','')[:200]}"
        for p in sources
    ])

    answer = bedrock_client.invoke(
        system=(
            "You are a news analyst. Answer the user's question using only "
            "the provided news context. Be concise (2-3 sentences). "
            "If the context doesn't cover the question, say so."
        ),
        user=f"Question: {query}\n\nNews context:\n{context}",
        max_tokens=350,
    )

    if not answer:
        answer = "Based on " + str(len(sources)) + " retrieved posts: " + "; ".join(
            p.get("title") or p.get("body", "")[:60] for p in sources[:3]
        ) + "."

    return {
        "query":   query,
        "answer":  answer,
        "sources": [
            {"title": p.get("title"), "url": p.get("url"),
             "platform": p.get("platform"), "relevance": p.get("relevance")}
            for p in sources[:5]
        ],
    }


# ── Entity co-occurrence graph ─────────────────────────────────────────────────

_STOPWORDS = {
    "The","A","An","This","That","These","Those","It","He","She","They",
    "Mr","Ms","Dr","Inc","Ltd","Corp","Co","Us","New",
}


def compute_entity_graph(posts: list[dict], min_cooccurrence: int = 2) -> dict:
    """
    Build a co-occurrence graph of named entities from enriched posts.
    Nodes = entities (sized by frequency), edges = co-appearance in same post.
    """
    entity_count: Counter = Counter()
    pair_count:   Counter = Counter()

    for p in posts:
        raw = p.get("entities", [])
        if not raw:
            continue
        entities = [
            e for e in dict.fromkeys(raw)
            if len(e) > 2 and e not in _STOPWORDS
        ][:8]

        for e in entities:
            entity_count[e] += 1

        for i, e1 in enumerate(entities):
            for e2 in entities[i + 1:]:
                pair_count[tuple(sorted([e1, e2]))] += 1

    nodes = [
        {"id": e, "count": c, "group": _entity_group(e, posts)}
        for e, c in entity_count.most_common(60)
        if c >= 2
    ]
    node_ids = {n["id"] for n in nodes}

    links = [
        {"source": p[0], "target": p[1], "weight": c}
        for p, c in pair_count.most_common(120)
        if c >= min_cooccurrence and p[0] in node_ids and p[1] in node_ids
    ]

    return {"nodes": nodes, "links": links}


def _entity_group(entity: str, posts: list[dict]) -> str:
    topics: Counter = Counter()
    for p in posts:
        if entity in (p.get("entities") or []):
            for tag in p.get("topic_tags", []):
                topics[tag] += 1
    top = topics.most_common(1)
    return top[0][0] if top else "general"
