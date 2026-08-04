# ABOUTME: AWS Lambda entrypoint — S3 ObjectCreated event -> Temporal IngestWorkflow.
# Thin adapter over the shared handle_s3_event core; the same logic the MinIO webhook runs.

from __future__ import annotations

import asyncio

from temporalio.client import Client

from .config import settings
from .trigger import handle_s3_event


async def _run(event: dict) -> list[str]:
    # A fresh client per invocation is correct here: Lambda runs each async invocation on
    # its own event loop. Warm-start client reuse and Temporal Cloud mTLS are deployment
    # concerns documented in the RUNBOOK, not wired here.
    client = await Client.connect(
        settings.temporal_address, namespace=settings.temporal_namespace
    )
    return await handle_s3_event(client, event)


def lambda_handler(event: dict, context) -> dict:
    """S3-triggered Lambda handler. Wire to an S3 ObjectCreated event notification."""
    return {"started": asyncio.run(_run(event))}
