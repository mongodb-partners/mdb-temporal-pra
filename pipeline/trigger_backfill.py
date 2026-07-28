"""Start a BackfillWorkflow to re-embed the active collection into the green collection.

Run:  uv run python -m pipeline.trigger_backfill --model voyage-3-large --dim 1024
"""

from __future__ import annotations

import argparse
import asyncio

from temporalio.client import Client

from .config import settings
from .config_store import get_active
from .workflows.backfill_workflow import BackfillWorkflow


async def main() -> None:
    parser = argparse.ArgumentParser(description="Re-embed the active collection into the green collection.")
    parser.add_argument("--model", required=True, help="New Voyage model, e.g. voyage-3-large.")
    parser.add_argument("--dim", type=int, default=settings.embed_dim, help="New embedding dimension.")
    parser.add_argument("--batch-size", type=int, default=50)
    args = parser.parse_args()

    active = get_active()
    source = active["active_collection"]
    # Target is the other of the blue/green pair.
    target = (settings.knowledge_v2_collection
              if source == settings.knowledge_collection
              else settings.knowledge_collection)

    client = await Client.connect(settings.temporal_address, namespace=settings.temporal_namespace)
    handle = await client.start_workflow(
        BackfillWorkflow.run,
        args=[args.model, source, target, args.dim, args.batch_size],
        id=f"backfill-{target}-{args.model}",
        task_queue=settings.temporal_task_queue,
    )
    print(f"started {handle.id}: re-embed {source} -> {target} with {args.model} (dim={args.dim})")
    print(f"when done, cut over:  uv run python -m pipeline.cutover --to {target} --model {args.model} --dim {args.dim}")


if __name__ == "__main__":
    asyncio.run(main())
