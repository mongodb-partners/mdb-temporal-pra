"""Vector search over the active collection. Returns pointers; text comes from spanio."""

from __future__ import annotations

from typing import Any

from .clients import knowledge_collection, voyage_client
from .config_store import get_active


def vector_search(query: str, k: int = 5) -> list[dict[str, Any]]:
    """Embed the query with the active model and $vectorSearch the active collection."""
    active = get_active()
    qv = voyage_client().embed([query], model=active["model"], input_type="query").embeddings[0]

    coll = knowledge_collection(active["active_collection"])
    results = coll.aggregate([
        {
            "$vectorSearch": {
                "index": active["active_index"],
                "path": "embedding",
                "queryVector": list(qv),
                "numCandidates": max(100, k * 20),
                "limit": k,
            }
        },
        {
            # No text. The searchable collection has none to project, and asking for
            # it would silently return nothing rather than fail loudly.
            "$project": {
                "_id": 0,
                "source_uri": 1,
                "chunk_id": 1,
                "ordinal": 1,
                "span": 1,
                "metadata": 1,
                "score": {"$meta": "vectorSearchScore"},
            }
        },
    ])
    return list(results)
