"""Temporal worker: hosts the Part 1 workflows and activities.

Run:  uv run python -m pipeline.worker
"""

from __future__ import annotations

import asyncio
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

from temporalio.client import Client
from temporalio.contrib.openai_agents import ModelActivityParameters, OpenAIAgentsPlugin
from temporalio.worker import Worker

from agent.agent_workflow import DeepResearchAgent
from agent.tools import rerank_tool, vector_search_tool

from .activities import ALL_ACTIVITIES
from .config import settings
from .workflows import ALL_WORKFLOWS


async def main() -> None:
    # The durable research agent (OpenAI Agents SDK) is opt-in: it loads only when
    # OPENAI_API_KEY is set, because the plugin builds an OpenAI client at worker startup.
    # Without a key the worker still runs ingestion/backfill exactly as before.
    plugins: list = []
    agent_workflows: list = []
    agent_activities: list = []
    if settings.openai_api_key:
        os.environ.setdefault("OPENAI_API_KEY", settings.openai_api_key)
        plugins.append(
            OpenAIAgentsPlugin(
                model_params=ModelActivityParameters(
                    start_to_close_timeout=timedelta(seconds=60)
                )
            )
        )
        agent_workflows = [DeepResearchAgent]
        agent_activities = [vector_search_tool, rerank_tool]
    else:
        print("[worker] OPENAI_API_KEY not set — durable research agent disabled")

    client = await Client.connect(
        settings.temporal_address,
        namespace=settings.temporal_namespace,
        plugins=plugins,
    )

    # Sync activities (pymongo / voyage / boto3) run in this thread pool; async
    # activities run on the worker event loop.
    with ThreadPoolExecutor(max_workers=16) as executor:
        worker = Worker(
            client,
            task_queue=settings.temporal_task_queue,
            workflows=[*ALL_WORKFLOWS, *agent_workflows],
            activities=[*ALL_ACTIVITIES, *agent_activities],
            activity_executor=executor,
        )
        print(
            f"[worker] connected to {settings.temporal_address} "
            f"(ns={settings.temporal_namespace}) on task queue '{settings.temporal_task_queue}'"
        )
        await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
