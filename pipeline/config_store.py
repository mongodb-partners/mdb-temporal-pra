"""Cutover pointer: which collection + index + model retrieval should read.

A single doc `{_id:'active', active_collection, active_index, model, dim}` in `temporal_config`.
Backfill writes a new (green) collection; `cutover.py` flips this pointer atomically for
readers, giving a blue/green swap on embedding-model changes.
"""

from __future__ import annotations

from .clients import mongo_client
from .config import settings

_ACTIVE_ID = "active"


def _coll():
    return mongo_client()[settings.mongodb_db][settings.config_collection]


def default_active() -> dict:
    return {
        "_id": _ACTIVE_ID,
        "active_collection": settings.knowledge_collection,
        "active_index": settings.vector_search_index_name,
        "model": settings.voyage_model,
        "dim": settings.embed_dim,
    }


def get_active() -> dict:
    """Return the active pointer, falling back to defaults if unset."""
    return _coll().find_one({"_id": _ACTIVE_ID}) or default_active()


def set_active(collection: str, model: str, dim: int, index: str | None = None) -> dict:
    index = index or settings.vector_search_index_name
    doc = {"_id": _ACTIVE_ID, "active_collection": collection, "active_index": index,
           "model": model, "dim": dim}
    _coll().replace_one({"_id": _ACTIVE_ID}, doc, upsert=True)
    return doc
