# ABOUTME: Agent tools as Temporal activities: zero-copy search (pointers, no text), a
# verified span read, and a Voyage rerank over verified spans, exposed via activity_as_tool.

from __future__ import annotations

from typing import Any

from temporalio import activity

from pipeline.clients import voyage_client
from pipeline.config import settings
from pipeline.retrieval import vector_search
from pipeline.spanio import read_span


@activity.defn
def vector_search_tool(query: str, k: int = 10) -> list[dict[str, Any]]:
    """Semantic vector search over the active knowledge base. Returns pointers, not text.

    Use this first to find material relevant to the question. Returns up to `k`
    candidate chunks, each with `chunk_id`, `source_uri`, `ordinal`, `span` and a
    similarity `score`. Pass the chunk_ids to `rerank_tool` or `read_span_tool` to
    get the text.
    """
    return [
        {
            "chunk_id": h["chunk_id"],
            "source_uri": h["source_uri"],
            "ordinal": h.get("ordinal"),
            "span": h.get("span"),
            "score": round(float(h.get("score", 0.0)), 4),
        }
        for h in vector_search(query, k=k)
    ]


@activity.defn
def read_span_tool(chunk_id: str) -> dict[str, Any]:
    """Read the exact source text behind one chunk_id, verified against its index-time hash.

    Takes only a chunk_id returned by `vector_search_tool`. The result carries `text`
    only when `status` is "ok"; "stale", "missing" or "unknown" means the source is
    unavailable, so say so rather than answering from the search result alone.
    """
    return read_span(chunk_id)


@activity.defn
def rerank_tool(query: str, chunk_ids: list[str], top_k: int = 5) -> list[dict[str, Any]]:
    """Rerank candidate chunks for relevance to the query and keep the best `top_k`.

    Pass the `chunk_id`s returned by `vector_search_tool`. The text is read from the
    source object and verified server-side (the index holds none); chunks that do not
    verify are dropped. Returns the top_k chunks in relevance order, each with
    `chunk_id`, `source_uri`, `rerank_score` and the verified `text`.
    """
    spans = [s for s in (read_span(c) for c in dict.fromkeys(chunk_ids)) if s["status"] == "ok"]
    if not spans:
        return []

    res = voyage_client().rerank(
        query, [s["text"] for s in spans], model=settings.voyage_rerank_model, top_k=top_k
    )
    return [
        {
            "chunk_id": spans[r.index]["chunk_id"],
            "source_uri": spans[r.index]["source_uri"],
            "rerank_score": round(float(r.relevance_score), 4),
            "text": spans[r.index]["text"],
        }
        for r in res.results
    ]
