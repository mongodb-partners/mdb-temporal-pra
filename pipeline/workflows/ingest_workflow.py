"""IngestWorkflow — the drawing's Temporal workflow.

  (1) fetch S3 object + chunk (factory by file type) → persist chunks in MDB (batched)
  (2) embed each chunk (Voyage)  — one activity per chunk, so a crash resumes without re-embed
  (3) create / UPDATE the Atlas Search index (re-upload updates in place)
"""

from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from ..activities.ingest import embed_staged_chunk, fetch_and_stage_chunks, index_document
    from ..models import S3Ref

_EMBED_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=2),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=30),
    maximum_attempts=6,
)


@workflow.defn
class IngestWorkflow:
    @workflow.run
    async def run(self, ref: S3Ref, target_collection: str | None = None) -> dict:
        # Stage 1 — fetch + factory chunk + stage in MDB.
        staged = await workflow.execute_activity(
            fetch_and_stage_chunks,
            ref,
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=RetryPolicy(maximum_attempts=5),
        )
        doc_id, doc_hash, n = staged["doc_id"], staged["doc_hash"], staged["n"]

        if staged["status"] == "unchanged":
            return {"doc_id": doc_id, "status": "unchanged", "indexed": 0}
        if n == 0:
            return {"doc_id": doc_id, "status": "empty", "indexed": 0}

        # Stage 2 — embed each staged chunk (deterministic chunk ids from stage 1).
        for i in range(n):
            await workflow.execute_activity(
                embed_staged_chunk,
                args=[f"{doc_id}:{i}", None],
                start_to_close_timeout=timedelta(minutes=2),
                heartbeat_timeout=timedelta(seconds=30),
                retry_policy=_EMBED_RETRY,
            )

        # Stage 3 — upsert into the searchable collection + ensure index (update in place).
        result = await workflow.execute_activity(
            index_document,
            args=[doc_id, doc_hash, target_collection],
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=RetryPolicy(maximum_attempts=6),
        )
        return {"doc_id": doc_id, "status": "indexed", "indexed": result["indexed"],
                "collection": result["collection"], "extractor": staged.get("extractor")}
