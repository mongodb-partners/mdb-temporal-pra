"""Start a BackfillWorkflow to re-embed existing Atlas data with a new model.

Run:  uv run python -m pipeline.trigger_backfill --model voyage-3-large
"""

from __future__ import annotations

import argparse
import asyncio

from temporalio.client import Client

from .config import settings
from .workflows.backfill_workflow import BackfillWorkflow


async def main() -> None:
    parser = argparse.ArgumentParser(description="Trigger a re-embedding backfill into knowledge_v2.")
    parser.add_argument("--model", required=True, help="New Voyage model, e.g. voyage-3-large.")
    parser.add_argument("--batch-size", type=int, default=50, help="Docs re-embedded per page.")
    args = parser.parse_args()

    client = await Client.connect(settings.temporal_address, namespace=settings.temporal_namespace)

    handle = await client.start_workflow(
        BackfillWorkflow.run,
        args=[args.model, args.batch_size],
        id=f"backfill-{args.model}",
        task_queue=settings.temporal_task_queue,
    )
    print(f"started {handle.id}; re-embedding {settings.knowledge_collection} -> {settings.knowledge_v2_collection}")
    print("watch progress in the Temporal Web UI at http://localhost:8233")


if __name__ == "__main__":
    asyncio.run(main())
