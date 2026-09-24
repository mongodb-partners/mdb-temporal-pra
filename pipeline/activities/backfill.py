"""Backfill activities: re-embed existing docs into a new (green) collection.

Triggered on an embedding-model change (e.g. dim change). Reads the current active
collection in pages and rewrites each chunk with a fresh embedding into the target
collection, which gets its own vector index at the new dimension.
"""

from __future__ import annotations

from typing import Any

from temporalio import activity
from temporalio.exceptions import ApplicationError

from ..clients import knowledge_collection
from ..config import settings
from ..search_index import ensure_vector_index


@activity.defn
def read_source_batch(after_id: str | None, limit: int, source_collection: str) -> list[dict[str, Any]]:
    """Read a page of chunks from the source collection, ascending by _id.

    The vector stays behind: this result is recorded in workflow history, and fifty
    1024-dim embeddings exceed Temporal's payload warning limit on their own.
    """
    from bson import ObjectId

    coll = knowledge_collection(source_collection)
    query: dict[str, Any] = {}
    if after_id:
        query["_id"] = {"$gt": ObjectId(after_id)}
    cursor = coll.find(query, {"embedding": 0}).sort("_id", 1).limit(limit)
    out = []
    for d in cursor:
        d["_id"] = str(d["_id"])
        out.append(d)
    return out


@activity.defn
def reembed_and_write(doc: dict[str, Any], model: str, target_collection: str) -> str:
    """Deferred: the searchable collection no longer stores text to re-embed from.

    Non-retryable on purpose. A deferred feature is not implemented any more on the
    sixth attempt than on the first, so the default retry policy only delays the
    message and buries it under identical failures.
    """
    raise ApplicationError(
        "backfill is deferred; see 'Backfill + model cutover' in docs/RUNBOOK.md. "
        "Re-embedding would need "
        "the chunk text, which knowledge_zc deliberately does not store. An embedding "
        "model change is handled by re-ingesting from S3 instead.",
        type="BackfillDeferred",
        non_retryable=True,
    )


@activity.defn
def ensure_target_index(target_collection: str, dim: int) -> bool:
    """Ensure the vector index exists on the target (green) collection at the new dim."""
    return ensure_vector_index(knowledge_collection(target_collection), settings.vector_search_index_name, dim)
