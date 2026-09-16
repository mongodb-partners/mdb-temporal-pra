"""IngestWorkflow — durable document ingestion pipeline.

  (1) fetch S3 object + chunk (factory by file type) → persist chunks in MDB (batched)
  (2) move staged chunks into knowledge_auto_embedding — Atlas auto-embedding generates
      vectors asynchronously; a crash before this step replays from the staged chunks
"""

from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from ..activities.ingest import fetch_and_stage_chunks, index_document
    from ..models import S3Ref


@workflow.defn
class IngestWorkflow:
    @workflow.run
    async def run(self, ref: S3Ref) -> dict:
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

        # Stage 2 — upsert staged chunks into knowledge_auto_embedding.
        # Atlas generates embeddings automatically; no Voyage API calls needed.
        result = await workflow.execute_activity(
            index_document,
            args=[doc_id, doc_hash],
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=RetryPolicy(maximum_attempts=6),
        )
        return {"doc_id": doc_id, "status": "indexed", "indexed": result["indexed"],
                "collection": result["collection"], "extractor": staged.get("extractor")}
