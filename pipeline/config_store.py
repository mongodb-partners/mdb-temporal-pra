"""Active collection pointer stored in temporal_config.

A single doc ``{_id:'active', active_collection, active_index}`` in ``temporal_config``
tells all readers which collection and index to query. With Atlas auto-embedding there is
no longer a model/dim concept to track here.
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
        "active_collection": settings.knowledge_auto_embedding_collection,
        "active_index": settings.auto_embedding_index_name,
    }


def get_active() -> dict:
    """Return the active pointer, falling back to defaults if unset."""
    return _coll().find_one({"_id": _ACTIVE_ID}) or default_active()
