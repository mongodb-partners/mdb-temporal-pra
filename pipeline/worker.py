"""Temporal worker: hosts the Part 1 workflows and activities.

Run:  uv run python -m pipeline.worker
"""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor

from temporalio.client import Client
from temporalio.worker import Worker

from agent.agent_workflow import DeepResearchAgent
from agent.provider import build_openai_agents_plugin
from agent.tools import read_span_tool, rerank_tool, vector_search_tool

from .activities import ALL_ACTIVITIES
from .config import settings
from .workflows import ALL_WORKFLOWS

# Sync activities (pymongo, voyage, boto3) run in a thread pool of this size.
ACTIVITY_THREADS = 16


def build_worker(
    client: Client,
    executor: ThreadPoolExecutor,
    agent_workflows: list | None = None,
    agent_activities: list | None = None,
) -> Worker:
    # Accept no more activities than there are threads. An activity's timeout starts
    # when the worker accepts it, so any excess would sit in the executor queue with
    # its start_to_close timer already running.
    return Worker(
        client,
        task_queue=settings.temporal_task_queue,
        workflows=[*ALL_WORKFLOWS, *(agent_workflows or [])],
        activities=[*ALL_ACTIVITIES, *(agent_activities or [])],
        activity_executor=executor,
        max_concurrent_activities=ACTIVITY_THREADS,
    )


async def main() -> None:
    # The durable research agent (OpenAI Agents SDK) is opt-in: it loads only when
    # OPENAI_API_KEY is set, because the plugin builds an OpenAI client at worker startup.
    # Without a key the worker still runs ingestion/backfill exactly as before.
    plugins: list = []
    agent_workflows: list = []
    agent_activities: list = []
    if settings.openai_api_key:
        plugins.append(build_openai_agents_plugin())
        agent_workflows = [DeepResearchAgent]
        agent_activities = [vector_search_tool, rerank_tool, read_span_tool]
    else:
        print("[worker] OPENAI_API_KEY not set — durable research agent disabled")

    client = await Client.connect(
        settings.temporal_address,
        namespace=settings.temporal_namespace,
        plugins=plugins,
    )

    with ThreadPoolExecutor(max_workers=ACTIVITY_THREADS) as executor:
        worker = build_worker(client, executor, agent_workflows, agent_activities)
        print(
            f"[worker] connected to {settings.temporal_address} "
            f"(ns={settings.temporal_namespace}) on task queue '{settings.temporal_task_queue}'"
        )
        await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
