"""Idempotent Atlas Vector Search index management for auto-embedding."""

from __future__ import annotations

from typing import Any

from .config import settings


def auto_embedding_index_definition(model: str) -> dict[str, Any]:
    return {
        "fields": [
            {
                "type": "autoEmbed",
                "modality": "text",
                "path": "text",
                "model": model,
            },
            {"type": "filter", "path": "doc_id"},
            {"type": "filter", "path": "source_uri"},
        ]
    }


def ensure_auto_embedding_index(coll, name: str | None = None, model: str | None = None) -> bool:
    """Create the auto-embedding vector search index if absent. Returns True if created."""
    from pymongo.operations import SearchIndexModel

    name = name or settings.auto_embedding_index_name
    model = model or settings.auto_embedding_model

    try:
        existing = {ix["name"] for ix in coll.list_search_indexes()}
    except Exception:
        existing = set()
    if name in existing:
        return False

    coll.create_search_index(
        SearchIndexModel(
            definition=auto_embedding_index_definition(model),
            name=name,
            type="vectorSearch",
        )
    )
    return True
