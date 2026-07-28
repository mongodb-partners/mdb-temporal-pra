"""EmbedWriteWorkflow — embed each chunk and upsert it into Atlas.

One embed activity per chunk: every completed embed is checkpointed in workflow
history, so a crash resumes *without re-embedding* the chunks already done — the
metered, expensive step is never repeated (Guarantee #3).
"""

from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from ..activities.embed_voyage import embed_chunk
    from ..activities.write_atlas import upsert_embedded_chunk
    from ..models import Chunk

_EMBED_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=2),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=30),
    maximum_attempts=6,
)


@workflow.defn
class EmbedWriteWorkflow:
    @workflow.run
    async def run(self, doc_id: str, chunks: list[Chunk], target_collection: str | None = None) -> dict:
        written = 0
        for chunk in chunks:
            embedded = await workflow.execute_activity(
                embed_chunk,
                args=[chunk, None],
                start_to_close_timeout=timedelta(minutes=2),
                heartbeat_timeout=timedelta(seconds=30),
                retry_policy=_EMBED_RETRY,
            )
            await workflow.execute_activity(
                upsert_embedded_chunk,
                args=[embedded, target_collection],
                start_to_close_timeout=timedelta(seconds=60),
                retry_policy=RetryPolicy(maximum_attempts=6),
            )
            written += 1

        return {"doc_id": doc_id, "status": "embedded", "written": written}
