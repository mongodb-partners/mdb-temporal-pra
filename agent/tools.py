# ABOUTME: Agent tools as Temporal activities — the pipeline's retrieval stages
# (vector search, Voyage rerank) exposed to the durable research agent via activity_as_tool.

from __future__ import annotations

from typing import Any

from temporalio import activity

from pipeline.clients import knowledge_collection, voyage_client
from pipeline.config import settings
from pipeline.config_store import get_active
from pipeline.retrieval import vector_search


@activity.defn
def vector_search_tool(query: str, k: int = 10) -> list[dict[str, Any]]:
    """Semantic vector search over the active knowledge base.

    Use this first to find material relevant to the question. Returns up to `k`
    candidate chunks, each with `chunk_id`, `source_uri`, `text`, and a similarity
    `score`.
    """
    return [
        {
            "chunk_id": h["chunk_id"],
            "source_uri": h["source_uri"],
            "text": h["text"],
            "score": round(float(h.get("score", 0.0)), 4),
        }
        for h in vector_search(query, k=k)
    ]


@activity.defn
def rerank_tool(query: str, chunk_ids: list[str], top_k: int = 5) -> list[dict[str, Any]]:
    """Rerank candidate chunks for relevance to the query and keep the best `top_k`.

    Pass the `chunk_id`s returned by `vector_search_tool`; the chunk text is reloaded
    server-side (you do not pass it back). Returns the top_k chunks in relevance order,
    each with `chunk_id`, `source_uri`, and `rerank_score`.
    """
    if not chunk_ids:
        return []

    active = get_active()
    coll = knowledge_collection(active["active_collection"])
    docs = list(
        coll.find(
            {"chunk_id": {"$in": chunk_ids}},
            {"_id": 0, "chunk_id": 1, "source_uri": 1, "text": 1},
        )
    )
    if not docs:
        return []

    res = voyage_client().rerank(
        query, [d["text"] for d in docs], model=settings.voyage_rerank_model, top_k=top_k
    )
    return [
        {
            "chunk_id": docs[r.index]["chunk_id"],
            "source_uri": docs[r.index]["source_uri"],
            "rerank_score": round(float(r.relevance_score), 4),
        }
        for r in res.results
    ]
