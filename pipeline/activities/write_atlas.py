"""Atlas write/read activities: upsert embedded chunks and read batches for backfill."""

from __future__ import annotations

from typing import Any

from temporalio import activity

from ..clients import knowledge_collection
from ..models import EmbeddedChunk


@activity.defn
def upsert_embedded_chunk(embedded: EmbeddedChunk, collection: str | None = None) -> str:
    """Idempotent upsert of one embedded chunk, keyed by (doc_id, chunk_id).

    Re-running the same chunk overwrites in place, so a resumed workflow that replays
    a completed step never creates duplicates.
    """
    coll = knowledge_collection(collection)
    doc = {
        "doc_id": embedded.doc_id,
        "chunk_id": embedded.chunk_id,
        "ordinal": embedded.ordinal,
        "text": embedded.text,
        "content_hash": embedded.content_hash,
        "doc_content_hash": embedded.doc_content_hash,
        "embedding": embedded.embedding,
        "model": embedded.model,
        "dim": embedded.dim,
        "source_uri": embedded.source_uri,
        "metadata": embedded.metadata,
    }
    coll.update_one({"chunk_id": embedded.chunk_id}, {"$set": doc}, upsert=True)
    return embedded.chunk_id


@activity.defn
def read_knowledge_batch(after_id: str | None, limit: int, collection: str | None = None) -> list[dict[str, Any]]:
    """Read a page of existing knowledge docs (for BackfillWorkflow).

    Pages by ascending ``_id`` using ``after_id`` as an exclusive cursor. Returns the
    minimal fields needed to re-embed and rewrite.
    """
    from bson import ObjectId

    coll = knowledge_collection(collection)
    query: dict[str, Any] = {}
    if after_id:
        query["_id"] = {"$gt": ObjectId(after_id)}

    cursor = coll.find(
        query,
        projection={
            "doc_id": 1,
            "chunk_id": 1,
            "ordinal": 1,
            "text": 1,
            "content_hash": 1,
            "doc_content_hash": 1,
            "source_uri": 1,
            "metadata": 1,
        },
    ).sort("_id", 1).limit(limit)

    out: list[dict[str, Any]] = []
    for d in cursor:
        d["_id"] = str(d["_id"])
        out.append(d)
    return out
