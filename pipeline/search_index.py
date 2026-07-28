"""Idempotent Atlas Vector Search index management (shared by activities + infra)."""

from __future__ import annotations

from typing import Any

from .config import settings


def vector_index_definition(dim: int) -> dict[str, Any]:
    return {
        "fields": [
            {"type": "vector", "path": "embedding", "numDimensions": dim, "similarity": "cosine"},
            {"type": "filter", "path": "doc_id"},
            {"type": "filter", "path": "source_uri"},
        ]
    }


def ensure_vector_index(coll, name: str | None = None, dim: int = 1024) -> bool:
    """Create the vector search index if absent. Returns True if a creation was issued."""
    from pymongo.operations import SearchIndexModel

    name = name or settings.vector_search_index_name

    try:
        existing = {ix["name"] for ix in coll.list_search_indexes()}
    except Exception:
        existing = set()
    if name in existing:
        return False

    coll.create_search_index(
        SearchIndexModel(definition=vector_index_definition(dim), name=name, type="vectorSearch")
    )
    return True
