"""BackfillWorkflow — re-embed all docs into a green collection on a model change.

Reads the source (currently-active) collection in pages, re-embeds each chunk with the
new model, and writes to the target collection (which gets its own vector index at the new
dimension). History is bounded with continue-as-new. Cutover (flipping the active pointer)
is a separate operator step — see pipeline/cutover.py.
"""

from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from ..activities.backfill import ensure_target_index, reembed_and_write, read_source_batch

_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=2),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=30),
    maximum_attempts=6,
)


@workflow.defn
class BackfillWorkflow:
    @workflow.run
    async def run(
        self,
        new_model: str,
        source_collection: str,
        target_collection: str,
        new_dim: int,
        batch_size: int = 50,
        after_id: str | None = None,
        processed: int = 0,
    ) -> dict:
        # Ensure the green index exists before the first writes.
        if after_id is None:
            await workflow.execute_activity(
                ensure_target_index,
                args=[target_collection, new_dim],
                start_to_close_timeout=timedelta(seconds=60),
                retry_policy=_RETRY,
            )

        batch = await workflow.execute_activity(
            read_source_batch,
            args=[after_id, batch_size, source_collection],
            start_to_close_timeout=timedelta(seconds=60),
            retry_policy=_RETRY,
        )

        if not batch:
            workflow.logger.info("backfill complete: %d chunks -> %s (%s)", processed, target_collection, new_model)
            return {"status": "complete", "processed": processed,
                    "model": new_model, "target": target_collection}

        for doc in batch:
            await workflow.execute_activity(
                reembed_and_write,
                args=[doc, new_model, target_collection],
                start_to_close_timeout=timedelta(minutes=2),
                heartbeat_timeout=timedelta(seconds=30),
                retry_policy=_RETRY,
            )

        processed += len(batch)
        last_id = batch[-1]["_id"]
        workflow.continue_as_new(
            args=[new_model, source_collection, target_collection, new_dim, batch_size, last_id, processed]
        )
