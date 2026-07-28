"""Deep-agent retrieval: vector search -> Voyage rerank -> Claude answer + memory write.

Reuses the pipeline's active-collection vector search (`pipeline.retrieval`). Reranking and
answer synthesis degrade gracefully: if the Voyage rerank model or the Anthropic key is
unavailable, the endpoint still returns ranked source chunks.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from pipeline.clients import mongo_client, voyage_client
from pipeline.config import settings
from pipeline.config_store import get_active
from pipeline.retrieval import vector_search

_SYSTEM = (
    "You are a precise assistant answering from a MongoDB Atlas knowledge base built by a "
    "Temporal + Voyage ingestion pipeline. Answer ONLY from the provided sources. Cite sources "
    "inline as [n]. If the sources don't contain the answer, say so plainly."
)

_LOG = logging.getLogger(__name__)


def _rerank(query: str, docs: list[dict[str, Any]], top_k: int) -> list[dict[str, Any]]:
    if not docs:
        return docs
    try:
        res = voyage_client().rerank(
            query, [d["text"] for d in docs], model=settings.voyage_rerank_model, top_k=top_k
        )
        ordered = []
        for r in res.results:
            d = dict(docs[r.index])
            d["rerank_score"] = r.relevance_score
            ordered.append(d)
        return ordered
    except Exception:
        return docs[:top_k]  # rerank unavailable -> keep vector order


def _synthesize(query: str, docs: list[dict[str, Any]]) -> str | None:
    if not settings.anthropic_api_key or settings.anthropic_api_key.startswith("<"):
        return None
    import anthropic

    context = "\n\n".join(
        f"[{i + 1}] (source: {d['source_uri']})\n{d['text']}" for i, d in enumerate(docs)
    )
    client_kwargs: dict[str, Any] = {"api_key": settings.anthropic_api_key}
    if settings.anthropic_base_url:
        client_kwargs["base_url"] = settings.anthropic_base_url
    subscription_key = settings.anthropic_subscription_key
    if not subscription_key and settings.anthropic_base_url and "azure-api.net" in settings.anthropic_base_url:
        subscription_key = settings.anthropic_api_key
    if subscription_key:
        client_kwargs["default_headers"] = {
            "Ocp-Apim-Subscription-Key": subscription_key,
            "api-key": subscription_key,
        }
        client_kwargs["default_query"] = {"subscription-key": subscription_key}
    client = anthropic.Anthropic(**client_kwargs)
    try:
        resp = client.messages.create(
            model=settings.answer_model,
            max_tokens=1024,
            system=_SYSTEM,
            messages=[{"role": "user", "content": f"Question: {query}\n\nSources:\n{context}"}],
        )
        return "".join(
            block.text for block in resp.content if getattr(block, "type", None) == "text"
        )
    except anthropic.AuthenticationError as exc:
        _LOG.warning("Anthropic authentication failed; returning sources only: %s", exc)
        return None
    except Exception as exc:
        _LOG.warning("Anthropic synthesis failed; returning sources only: %s", exc)
        return None


def _remember(query: str, answer: str | None, docs: list[dict[str, Any]]) -> None:
    try:
        mongo_client()[settings.mongodb_db][settings.memory_collection].insert_one({
            "query": query,
            "answer": answer,
            "sources": [d["chunk_id"] for d in docs],
            "model": settings.answer_model,
            "ts": datetime.now(timezone.utc),
        })
    except Exception:
        pass  # memory write is best-effort


def ask(query: str, k: int = 10, top_k: int = 5) -> dict[str, Any]:
    """Run the deep-agent retrieval + synthesis for one query."""
    active = get_active()
    hits = vector_search(query, k=k)
    ranked = _rerank(query, hits, top_k)
    answer = _synthesize(query, ranked)
    _remember(query, answer, ranked)

    return {
        "query": query,
        "answer": answer,
        "answer_available": answer is not None,
        "active_collection": active["active_collection"],
        "model": active["model"],
        "sources": [
            {
                "n": i + 1,
                "s3_uri": d["source_uri"],
                "chunk_id": d["chunk_id"],
                "score": round(float(d.get("rerank_score", d.get("score", 0.0))), 4),
                "vector_score": round(float(d.get("score", 0.0)), 4),
                "text": d["text"][:600],
            }
            for i, d in enumerate(ranked)
        ],
    }
