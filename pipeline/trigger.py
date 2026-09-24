"""Shared trigger logic: start an IngestWorkflow for an S3 object.

Used by the HTTP endpoint (`trigger_api`), the AWS Lambda (`lambda_handler`) and the
seed scripts, which upload and then start the ingest themselves. Re-uploads terminate any in-flight ingest for the same doc and start fresh, so the search
index is updated in place rather than duplicated.
"""

from __future__ import annotations

import asyncio

from temporalio.client import Client
from temporalio.common import WorkflowIDConflictPolicy

from .config import settings
from .models import S3Ref, doc_id_for_uri
from .s3util import refs_from_s3_event

INGEST_WORKFLOW = "IngestWorkflow"  # referenced by name to avoid importing workflow deps


async def get_client() -> Client:
    return await Client.connect(settings.temporal_address, namespace=settings.temporal_namespace)


async def handle_s3_event(client: Client, event: dict | str | bytes) -> list[str]:
    """Parse an S3 ObjectCreated event and start an IngestWorkflow per object.

    The single entrypoint shared by the event adapters: `trigger_api`'s /ingest-event and
    the AWS Lambda (`lambda_handler`). Returns the started workflow ids (empty for a
    non-object event such as S3's `s3:TestEvent`).
    """
    return [await start_ingest(client, ref) for ref in refs_from_s3_event(event)]


async def start_ingest(client: Client, ref: S3Ref) -> str:
    """Start (or restart) the IngestWorkflow for one S3 object. Returns the workflow id."""
    doc_id = doc_id_for_uri(ref.s3_uri)
    handle = await client.start_workflow(
        INGEST_WORKFLOW,
        ref,
        id=f"ingest-{doc_id}",
        task_queue=settings.temporal_task_queue,
        # Re-upload of the same key replaces any running ingest -> update in place.
        id_conflict_policy=WorkflowIDConflictPolicy.TERMINATE_EXISTING,
    )
    return handle.id


def start_ingests(refs: list[S3Ref]) -> list[str]:
    """Start an IngestWorkflow per ref from synchronous code. Returns the workflow ids.

    The seed scripts use this after uploading. Nothing emits an object-created event
    locally, so without it an upload would sit in the bucket unindexed. If an S3 event
    trigger is also wired to the bucket, both starts share one workflow id: the later one
    replaces the earlier, and the index is updated in place rather than duplicated.
    """

    async def _run() -> list[str]:
        client = await get_client()
        return [await start_ingest(client, ref) for ref in refs]

    return asyncio.run(_run())
