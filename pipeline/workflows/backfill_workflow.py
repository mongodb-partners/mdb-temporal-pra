"""BackfillWorkflow — re-embed existing Atlas data with an upgraded model.

Triggered on a model-version change (e.g. voyage-3 -> voyage-3-large, which changes
vector dimensions). It reads the already-embedded documents from Atlas, re-embeds with
the new model, and writes them to the blue/green target collection (knowledge_v2) — no
re-ingestion from source, no lost progress (Guarantee #4).

History is bounded with continue-as-new: each run processes one page, then continues
from the last _id cursor.
"""

from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from ..activities.embed_voyage import embed_chunk
    from ..activities.write_atlas import read_knowledge_batch, upsert_embedded_chunk
    from ..config import settings
    from ..models import Chunk

_EMBED_RETRY = RetryPolicy(
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
        batch_size: int = 50,
        after_id: str | None = None,
        processed: int = 0,
    ) -> dict:
        target = settings.knowledge_v2_collection
        source = settings.knowledge_collection

        batch = await workflow.execute_activity(
            read_knowledge_batch,
            args=[after_id, batch_size, source],
            start_to_close_timeout=timedelta(seconds=60),
            retry_policy=RetryPolicy(maximum_attempts=5),
        )

        if not batch:
            workflow.logger.info("backfill complete: %d docs re-embedded into %s", processed, target)
            return {"status": "complete", "processed": processed, "model": new_model, "target": target}

        for doc in batch:
            chunk = Chunk(
                doc_id=doc["doc_id"],
                chunk_id=doc["chunk_id"],
                ordinal=doc.get("ordinal", 0),
                text=doc["text"],
                content_hash=doc.get("content_hash", ""),
                doc_content_hash=doc.get("doc_content_hash", ""),
                source_uri=doc.get("source_uri", ""),
                metadata=doc.get("metadata", {}),
            )
            embedded = await workflow.execute_activity(
                embed_chunk,
                args=[chunk, new_model],
                start_to_close_timeout=timedelta(minutes=2),
                heartbeat_timeout=timedelta(seconds=30),
                retry_policy=_EMBED_RETRY,
            )
            await workflow.execute_activity(
                upsert_embedded_chunk,
                args=[embedded, target],
                start_to_close_timeout=timedelta(seconds=60),
                retry_policy=RetryPolicy(maximum_attempts=6),
            )

        processed += len(batch)
        last_id = batch[-1]["_id"]

        # Bound history: continue from the last cursor as a fresh run.
        workflow.continue_as_new(args=[new_model, batch_size, last_id, processed])
