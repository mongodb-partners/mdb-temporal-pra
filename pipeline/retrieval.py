"""Shared vector-search over the knowledge_auto_embedding collection."""

from __future__ import annotations

from typing import Any

from .clients import knowledge_collection
from .config import settings


def vector_search(query: str, k: int = 5) -> list[dict[str, Any]]:
    """Run $vectorSearch using Atlas auto-embedding — no manual embedding step needed."""
    coll = knowledge_collection()
    results = coll.aggregate([
        {
            "$vectorSearch": {
                "index": settings.auto_embedding_index_name,
                "path": "text",
                "query": query,
                "numCandidates": max(100, k * 20),
                "limit": k,
            }
        },
        {
            "$project": {
                "_id": 0,
                "source_uri": 1,
                "chunk_id": 1,
                "ordinal": 1,
                "text": 1,
                "score": {"$meta": "vectorSearchScore"},
            }
        },
    ])
    return list(results)
