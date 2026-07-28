"""ChunkWorkflow — consume a raw record, dedupe, chunk, hand off to the chunks topic."""

from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from ..activities.chunk import chunk_document, is_duplicate
    from ..activities.produce_chunks import produce_chunks_activity
    from ..models import ChunkResult, RawRecord


@workflow.defn
class ChunkWorkflow:
    @workflow.run
    async def run(self, record: RawRecord) -> dict:
        # 1. Download + extract + chunk (cheap relative to embedding).
        result: ChunkResult = await workflow.execute_activity(
            chunk_document,
            record,
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=RetryPolicy(maximum_attempts=5),
        )

        if not result.chunks:
            return {"doc_id": record.doc_id, "status": "empty", "chunks": 0}

        # 2. Guarantee #2: skip if this exact object version was already embedded.
        duplicate = await workflow.execute_activity(
            is_duplicate,
            args=[result.doc_id, result.doc_content_hash],
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(maximum_attempts=5),
        )
        if duplicate:
            return {"doc_id": record.doc_id, "status": "duplicate", "chunks": 0}

        # 3. Durable hand-off: write chunks back to Kafka for the embed stage.
        count = await workflow.execute_activity(
            produce_chunks_activity,
            args=[result.doc_id, result.chunks],
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=RetryPolicy(maximum_attempts=5),
        )
        return {"doc_id": record.doc_id, "status": "chunked", "chunks": count}
