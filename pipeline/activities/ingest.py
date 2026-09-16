"""Ingest activities for IngestWorkflow (sync — run in the worker thread pool).

Stages (each a distinct activity, so the workflow is resumable):
  1. fetch_and_stage_chunks  — download S3 object, factory-extract+chunk, stage in MDB
  2. index_document          — upsert staged chunks into knowledge_auto_embedding;
                               Atlas auto-embedding generates vectors asynchronously
"""

from __future__ import annotations

from temporalio import activity

from ..clients import knowledge_collection, mongo_client, s3_client
from ..config import settings
from ..extractors import get_extractor
from ..models import S3Ref, doc_id_for_uri, sha256_hex
from ..search_index import ensure_auto_embedding_index


def _staging():
    return mongo_client()[settings.mongodb_db][settings.chunks_collection]


@activity.defn
def fetch_and_stage_chunks(ref: S3Ref) -> dict:
    """Stage 1: download, extract+chunk by file type, persist chunks to MDB (batched)."""
    obj = s3_client().get_object(Bucket=ref.bucket, Key=ref.key)
    body: bytes = obj["Body"].read()
    content_type = obj.get("ContentType", ref.content_type or "")

    doc_id = doc_id_for_uri(ref.s3_uri)
    doc_hash = sha256_hex(body)

    # Short-circuit: this exact version is already indexed.
    if knowledge_collection().find_one({"doc_id": doc_id, "doc_content_hash": doc_hash}, {"_id": 1}):
        return {"doc_id": doc_id, "doc_hash": doc_hash, "n": 0, "status": "unchanged"}

    extractor = get_extractor(ref.key, content_type)
    raws = extractor.chunk(body)

    staging = _staging()
    staging.delete_many({"doc_id": doc_id})  # clear any stale staging for this doc
    if raws:
        staging.insert_many(
            [
                {
                    "doc_id": doc_id,
                    "chunk_id": f"{doc_id}:{r.ordinal}",
                    "ordinal": r.ordinal,
                    "text": r.text,
                    "content_hash": sha256_hex(r.text),
                    "doc_content_hash": doc_hash,
                    "source_uri": ref.s3_uri,
                    "metadata": r.meta,
                    "extractor": extractor.name,
                    "status": "pending",
                }
                for r in raws
            ]
        )
    activity.logger.info("staged %d chunk(s) for %s via %s", len(raws), ref.s3_uri, extractor.name)
    return {"doc_id": doc_id, "doc_hash": doc_hash, "n": len(raws), "status": "staged", "extractor": extractor.name}


@activity.defn
def index_document(doc_id: str, doc_hash: str) -> dict:
    """Stage 2: upsert staged chunks into knowledge_auto_embedding (no embedding needed).

    Atlas auto-embedding generates vectors asynchronously once documents land in the
    collection. The ``text`` field is indexed by the autoEmbed Atlas Search index.
    """
    know = knowledge_collection()
    staging = _staging()

    chunks = list(staging.find({"doc_id": doc_id, "status": "pending"}).sort("ordinal", 1))
    n = len(chunks)

    for c in chunks:
        know.update_one(
            {"chunk_id": c["chunk_id"]},
            {"$set": {
                "doc_id": doc_id,
                "chunk_id": c["chunk_id"],
                "ordinal": c["ordinal"],
                "text": c["text"],
                "content_hash": c["content_hash"],
                "doc_content_hash": doc_hash,
                "source_uri": c["source_uri"],
                "metadata": c.get("metadata", {}),
            }},
            upsert=True,
        )

    # Update-in-place: drop chunks from a previous, longer version of this doc.
    know.delete_many({"doc_id": doc_id, "ordinal": {"$gte": n}})

    ensure_auto_embedding_index(know, settings.auto_embedding_index_name, settings.auto_embedding_model)
    staging.delete_many({"doc_id": doc_id})  # staging is transient

    activity.logger.info("indexed %d chunk(s) into %s", n, settings.knowledge_auto_embedding_collection)
    return {"doc_id": doc_id, "indexed": n, "collection": settings.knowledge_auto_embedding_collection}
