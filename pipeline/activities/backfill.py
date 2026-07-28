"""Backfill activities: re-embed existing docs into a new (green) collection.

Triggered on an embedding-model change (e.g. dim change). Reads the current active
collection in pages and rewrites each chunk with a fresh embedding into the target
collection, which gets its own vector index at the new dimension.
"""

from __future__ import annotations

from typing import Any

from temporalio import activity

from ..clients import knowledge_collection, voyage_client
from ..config import settings
from ..search_index import ensure_vector_index


@activity.defn
def read_source_batch(after_id: str | None, limit: int, source_collection: str) -> list[dict[str, Any]]:
    """Read a page of chunks from the source collection, ascending by _id."""
    from bson import ObjectId

    coll = knowledge_collection(source_collection)
    query: dict[str, Any] = {}
    if after_id:
        query["_id"] = {"$gt": ObjectId(after_id)}
    cursor = coll.find(query).sort("_id", 1).limit(limit)
    out = []
    for d in cursor:
        d["_id"] = str(d["_id"])
        out.append(d)
    return out


@activity.defn
def reembed_and_write(doc: dict[str, Any], model: str, target_collection: str) -> str:
    """Re-embed one chunk with the new model and upsert into the target collection."""
    activity.heartbeat(doc.get("chunk_id", ""))
    vector = voyage_client().embed([doc["text"]], model=model, input_type="document").embeddings[0]
    target = knowledge_collection(target_collection)
    target.update_one(
        {"chunk_id": doc["chunk_id"]},
        {"$set": {
            "doc_id": doc["doc_id"],
            "chunk_id": doc["chunk_id"],
            "ordinal": doc.get("ordinal", 0),
            "text": doc["text"],
            "content_hash": doc.get("content_hash", ""),
            "doc_content_hash": doc.get("doc_content_hash", ""),
            "embedding": list(vector),
            "model": model,
            "dim": len(vector),
            "source_uri": doc.get("source_uri", ""),
            "metadata": doc.get("metadata", {}),
        }},
        upsert=True,
    )
    return doc["chunk_id"]


@activity.defn
def ensure_target_index(target_collection: str, dim: int) -> bool:
    """Ensure the vector index exists on the target (green) collection at the new dim."""
    return ensure_vector_index(knowledge_collection(target_collection), settings.vector_search_index_name, dim)
