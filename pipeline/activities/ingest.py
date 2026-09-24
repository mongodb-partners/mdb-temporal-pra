"""Ingest activities for IngestWorkflow (sync, run in the worker thread pool).

Stages (each a distinct activity, so the workflow is resumable):
  1. fetch_and_stage_chunks  : download S3 object, factory-extract+chunk, stage pointers in MDB
  2. embed_staged_batch      : re-read one contiguous byte range per batch, verify, embed
  3. index_document          : upsert pointers + vectors into the searchable collection,
                               prune stale chunks (update-in-place), ensure the index
  clear_document             : remove a document's chunks when it yields none (empty,
                               unsupported, or undecodable at the source)
"""

from __future__ import annotations

from pymongo import UpdateOne
from temporalio import activity

from ..clients import knowledge_collection, mongo_client, s3_client, voyage_client
from ..config import settings
from ..extractors import get_extractor
from ..extractors.base import UnsupportedSource
from ..models import S3Ref, doc_id_for_uri, sha256_hex
from ..search_index import ensure_vector_index
from ..spanio import fetch_range


def _staging():
    return mongo_client()[settings.mongodb_db][settings.chunks_collection]


@activity.defn
def fetch_and_stage_chunks(ref: S3Ref, target_collection: str | None = None) -> dict:
    """Stage 1: download, extract to byte spans, stage pointers. Never stages text."""
    obj = s3_client().get_object(Bucket=ref.bucket, Key=ref.key)
    body: bytes = obj["Body"].read()
    content_type = obj.get("ContentType", ref.content_type or "")
    # The ETag from the GET is authoritative. The event's copy may be absent on the
    # poll path, and it is what every later ranged read is conditioned on.
    etag = str(obj.get("ETag", ref.etag)).strip('"')

    doc_id = doc_id_for_uri(ref.s3_uri)
    doc_hash = sha256_hex(body)

    # Short-circuit: this exact version is already indexed. The question is only ever
    # about the collection this run will write to, so it has to be asked of that
    # collection: checking the default one would report `unchanged` for a document the
    # target does not hold, and the target would never receive it.
    know = knowledge_collection(target_collection or settings.knowledge_collection)
    if know.find_one({"doc_id": doc_id, "doc_content_hash": doc_hash}, {"_id": 1}):
        return {"doc_id": doc_id, "doc_hash": doc_hash, "n": 0, "status": "unchanged"}

    staging = _staging()
    staging.delete_many({"doc_id": doc_id})  # clear any stale staging for this doc

    try:
        extractor = get_extractor(ref.key, content_type)
        raws = extractor.chunk(body)
    except UnsupportedSource as exc:
        # Not an error: it is a lifecycle transition. Stage 3 clears the document.
        activity.logger.info("unsupported source %s: %s", ref.s3_uri, exc)
        return {"doc_id": doc_id, "doc_hash": doc_hash, "n": 0,
                "status": "unsupported", "reason": str(exc)}

    if not raws:
        return {"doc_id": doc_id, "doc_hash": doc_hash, "n": 0,
                "status": "empty", "reason": "document yielded no chunks",
                "extractor": extractor.name}

    staging.insert_many(
        [
            {
                "doc_id": doc_id,
                "chunk_id": f"{doc_id}:{r.ordinal}",
                "ordinal": r.ordinal,
                "span": {"kind": r.span.kind, "start": r.span.start, "end": r.span.end},
                "content_hash": sha256_hex(r.text),
                "doc_content_hash": doc_hash,
                "source_uri": ref.s3_uri,
                "bucket": ref.bucket,
                "key": ref.key,
                "etag": etag,
                "metadata": r.meta,
                "extractor": extractor.name,
                "status": "pending",
                "embedding": None,
            }
            for r in raws
        ]
    )
    activity.logger.info("staged %d span(s) for %s via %s", len(raws), ref.s3_uri, extractor.name)
    return {"doc_id": doc_id, "doc_hash": doc_hash, "n": len(raws),
            "status": "staged", "extractor": extractor.name}


@activity.defn
def embed_staged_batch(doc_id: str, ordinals: list[int], model: str | None = None) -> int:
    """Stage 2: re-read one contiguous byte range, verify it, embed the batch in one call.

    Spans are monotonic in ordinal, so a run of ordinals covers one contiguous range.
    Per-chunk ranged GETs would mean one request per chunk; a 421 KB file at 1200/150
    is roughly 400 of them.
    """
    use_model = model or settings.voyage_model
    staging = _staging()
    docs = list(staging.find({"doc_id": doc_id, "ordinal": {"$in": list(ordinals)}}).sort("ordinal", 1))
    if not docs:
        raise ValueError(f"no staged chunks for {doc_id} ordinals {ordinals}")

    todo = [d for d in docs if not (d.get("status") == "embedded" and d.get("model") == use_model)]
    if not todo:
        return 0  # resume path

    # Heartbeat after each slow step, not once before all of them. The ranged GET and
    # the embed call are the two that can outlast the heartbeat timeout, and a heartbeat
    # that fires before them reports progress that has not happened yet.
    lo = min(d["span"]["start"] for d in todo)
    hi = max(d["span"]["end"] for d in todo)
    blob = fetch_range(todo[0]["bucket"], todo[0]["key"], todo[0]["etag"], lo, hi)
    activity.heartbeat(f"{doc_id}:fetched:{lo}-{hi}")

    texts: list[str] = []
    for d in todo:
        segment = blob[d["span"]["start"] - lo : d["span"]["end"] - lo].decode("utf-8")
        if sha256_hex(segment) != d["content_hash"]:
            raise RuntimeError(f"span verification failed for {d['chunk_id']}")
        texts.append(segment)

    activity.heartbeat(f"{doc_id}:verified:{len(texts)}")

    vectors = list(voyage_client().embed(texts, model=use_model, input_type="document").embeddings)
    if len(vectors) != len(texts):
        # zip() would pair what it can and drop the rest, leaving the tail of the batch
        # at status == "pending" while this activity still reported len(todo) embedded.
        # The workflow would then index a document with a gap in its ordinals. A short
        # response is a broken response: write nothing.
        raise RuntimeError(
            f"embedding provider returned {len(vectors)} vector(s) for {len(texts)} text(s) "
            f"in {doc_id} ordinals {[d['ordinal'] for d in todo]}; refusing a partial write"
        )
    activity.heartbeat(f"{doc_id}:embedded:{len(vectors)}")

    staging.bulk_write(
        [
            UpdateOne(
                {"chunk_id": d["chunk_id"]},
                {"$set": {"embedding": list(v), "model": use_model,
                          "dim": len(v), "status": "embedded"}},
            )
            for d, v in zip(todo, vectors)
        ],
        ordered=False,
    )
    return len(todo)


@activity.defn
def index_document(doc_id: str, doc_hash: str, target_collection: str | None = None) -> dict:
    """Stage 3: upsert pointers and vectors into the searchable collection.

    This is the activity that makes the claim true: the document written here has no
    text field, and the only way back to text is a ranged read through spanio.
    """
    coll_name = target_collection or settings.knowledge_collection
    know = knowledge_collection(coll_name)
    staging = _staging()

    chunks = list(staging.find({"doc_id": doc_id, "status": "embedded"}).sort("ordinal", 1))
    if not chunks:
        # Empty staging means one of two very different things, and they must not be
        # treated alike.
        #
        # (a) This activity already ran to completion: its last side effect is
        #     staging.delete_many, so an at-least-once retry after the writes landed but
        #     before Temporal recorded the result re-enters here with staging empty on a
        #     document that is already indexed correctly. Raising would burn the retry
        #     budget and fail a workflow that had in fact succeeded.
        # (b) A caller genuinely reached index_document with nothing staged and nothing
        #     indexed. The workflow routes n == 0 to clear_document instead, which
        #     records a reason and an accurate removed count; falling through here would
        #     prune the whole document with no bookkeeping. Refuse, as before.
        #
        # The knowledge collection tells them apart, but only if it is asked about
        # THIS version. Counting by doc_id alone answers "is some version of this
        # document indexed", which is the wrong question: a terminated predecessor
        # workflow can finish its own stage 3 after its successor started (Temporal
        # cannot interrupt a sync activity mid-call), emptying staging out from under
        # the successor while the collection still holds the PREVIOUS version. Keyed
        # on doc_id alone that interleaving returns already_indexed and the workflow
        # reports success for a version that was never indexed. Keyed on the hash it
        # falls through to the raise below, which is the honest answer.
        already = know.count_documents({"doc_id": doc_id, "doc_content_hash": doc_hash})
        if already:
            activity.logger.info(
                "index_document re-entered for %s with staging already empty; %d chunk(s) "
                "already present in %s. Treating as a completed attempt.", doc_id, already, coll_name
            )
            return {"doc_id": doc_id, "indexed": already, "collection": coll_name,
                    "index_created": False, "status": "already_indexed"}
        raise ValueError(
            f"index_document called with no embedded chunks staged for doc_id={doc_id!r} "
            f"and no chunks for doc_content_hash={doc_hash!r} indexed in {coll_name!r}; "
            "clearing a document is clear_document's job, not index_document's."
        )
    n = len(chunks)
    dim = chunks[0]["dim"]

    for c in chunks:
        know.update_one(
            {"chunk_id": c["chunk_id"]},
            {"$set": {
                "doc_id": doc_id,
                "chunk_id": c["chunk_id"],
                "ordinal": c["ordinal"],
                "span": c["span"],
                "content_hash": c["content_hash"],
                "doc_content_hash": doc_hash,
                "embedding": c["embedding"],
                "model": c["model"],
                "dim": c["dim"],
                "source_uri": c["source_uri"],
                "bucket": c["bucket"],
                "key": c["key"],
                "etag": c["etag"],
                "metadata": c.get("metadata", {}),
            }},
            upsert=True,
        )

    # Update-in-place: drop chunks from a previous version of this doc. Prune by the
    # ordinals actually written, not by how many were written: len(chunks) is only a
    # valid ordinal bound while the staged embedded ordinals are exactly 0..n-1, and a
    # gap in that set would make this delete the chunks just upserted above it.
    know.delete_many({"doc_id": doc_id, "ordinal": {"$nin": [c["ordinal"] for c in chunks]}})

    created = ensure_vector_index(know, settings.vector_search_index_name, dim)
    staging.delete_many({"doc_id": doc_id})  # staging is transient

    activity.logger.info("indexed %d pointer(s) into %s (index_created=%s)", n, coll_name, created)
    return {"doc_id": doc_id, "indexed": n, "collection": coll_name,
            "index_created": created, "status": "indexed"}


@activity.defn
def clear_document(doc_id: str, reason: str, target_collection: str | None = None) -> dict:
    """Remove every chunk of a document that no longer yields any.

    Empty, unsupported and undecodable are the same transition. Leaving the old chunks
    searchable is worse than losing the document: the agent cites text that is gone.
    """
    coll_name = target_collection or settings.knowledge_collection
    know = knowledge_collection(coll_name)
    removed = know.delete_many({"doc_id": doc_id}).deleted_count
    _staging().delete_many({"doc_id": doc_id})
    activity.logger.info("cleared %d chunk(s) for %s from %s (%s)", removed, doc_id, coll_name, reason)
    return {"doc_id": doc_id, "removed": removed, "collection": coll_name, "reason": reason}
