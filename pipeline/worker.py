"""Temporal worker: hosts the Part 1 workflows and activities.

Run:  uv run python -m pipeline.worker
"""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor

from temporalio.client import Client
from temporalio.worker import Worker

from .activities import ALL_ACTIVITIES
from .config import settings
from .workflows import ALL_WORKFLOWS


async def main() -> None:
    client = await Client.connect(
        settings.temporal_address,
        namespace=settings.temporal_namespace,
    )

    # Sync activities (pymongo / voyage / boto3) run in this thread pool; async
    # activities (Kafka produce) run on the worker event loop.
    with ThreadPoolExecutor(max_workers=16) as executor:
        worker = Worker(
            client,
            task_queue=settings.temporal_task_queue,
            workflows=ALL_WORKFLOWS,
            activities=ALL_ACTIVITIES,
            activity_executor=executor,
        )
        print(
            f"[worker] connected to {settings.temporal_address} "
            f"(ns={settings.temporal_namespace}) on task queue '{settings.temporal_task_queue}'"
        )
        await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
