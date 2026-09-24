"""IngestWorkflow: the drawing's Temporal workflow.

  (1) fetch S3 object + chunk (factory by file type) → persist chunks in MDB (batched)
  (2) embed staged spans in batches of 32 (one ranged GET and one Voyage call per batch);
      a crash resumes without re-embedding finished chunks
  (3) create / UPDATE the Atlas Search index (re-upload updates in place)
"""

from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from ..activities.ingest import (
        clear_document,
        embed_staged_batch,
        fetch_and_stage_chunks,
        index_document,
    )
    from ..models import S3Ref

# Embedding is retried generously, because its transient failures (rate limits, a
# throttled or briefly unavailable provider) do succeed on a later attempt. Its
# permanent failures do not, and must not be retried at all:
#
#   SpanStale / SpanMissing : the object was replaced or deleted, so the IfMatch etag
#                             captured at staging time can never match again.
#   RuntimeError            : span verification, or a short embedding response, over
#                             bytes already fetched under a fixed etag.
#   ValueError              : nothing staged for the requested ordinals.
#
# Retrying any of those forever gives a workflow that neither completes nor fails: it
# holds a worker slot indefinitely and appears in no failed-workflow view. The finite
# maximum_attempts is the backstop for anything not named here.
_EMBED_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=2),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=30),
    maximum_attempts=20,
    non_retryable_error_types=["SpanStale", "SpanMissing", "RuntimeError", "ValueError"],
)

# Embed this many contiguous chunks per activity call (one ranged GET + one Voyage call).
_EMBED_BATCH = 32


@workflow.defn
class IngestWorkflow:
    @workflow.run
    async def run(self, ref: S3Ref, target_collection: str | None = None) -> dict:
        # Stage 1: fetch + factory chunk + stage in MDB.
        staged = await workflow.execute_activity(
            fetch_and_stage_chunks,
            args=[ref, target_collection],
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=RetryPolicy(maximum_attempts=5),
        )
        doc_id, doc_hash, n = staged["doc_id"], staged["doc_hash"], staged["n"]

        if staged["status"] == "unchanged":
            return {"doc_id": doc_id, "status": "unchanged", "indexed": 0}

        if n == 0:
            # Empty, unsupported or undecodable. Clear rather than return, or the
            # previous version stays searchable and citable.
            reason = staged.get("reason", staged["status"])
            cleared = await workflow.execute_activity(
                clear_document,
                args=[doc_id, reason, target_collection],
                start_to_close_timeout=timedelta(minutes=2),
                retry_policy=RetryPolicy(maximum_attempts=6),
            )
            return {"doc_id": doc_id, "status": staged["status"], "indexed": 0,
                    "removed": cleared["removed"], "reason": reason}

        # Stage 2: embed in contiguous batches. Each batch re-reads exactly its own
        # byte range and stays independently retryable.
        for start in range(0, n, _EMBED_BATCH):
            await workflow.execute_activity(
                embed_staged_batch,
                args=[doc_id, list(range(start, min(start + _EMBED_BATCH, n))), None],
                start_to_close_timeout=timedelta(minutes=5),
                heartbeat_timeout=timedelta(seconds=60),
                retry_policy=_EMBED_RETRY,
            )

        # Stage 3: upsert into the searchable collection + ensure index (update in place).
        result = await workflow.execute_activity(
            index_document,
            args=[doc_id, doc_hash, target_collection],
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=RetryPolicy(maximum_attempts=6),
        )
        return {"doc_id": doc_id, "status": "indexed", "indexed": result["indexed"],
                "collection": result["collection"], "extractor": staged.get("extractor")}
