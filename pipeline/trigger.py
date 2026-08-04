"""Shared trigger logic: start an IngestWorkflow for an S3 object.

Used by both the ASP HTTP endpoint (`trigger_api`) and the local change-stream shim
(`trigger_listener`). Re-uploads terminate any in-flight ingest for the same doc and
start fresh, so the search index is updated in place rather than duplicated.
"""

from __future__ import annotations

from temporalio.client import Client
from temporalio.common import WorkflowIDConflictPolicy

from .config import settings
from .models import S3Ref, doc_id_for_uri
from .s3util import refs_from_s3_event

INGEST_WORKFLOW = "IngestWorkflow"  # referenced by name to avoid importing workflow deps


async def get_client() -> Client:
    return await Client.connect(settings.temporal_address, namespace=settings.temporal_namespace)


async def handle_s3_event(client: Client, event: dict | str | bytes) -> list[str]:
    """Parse an S3/MinIO ObjectCreated event and start an IngestWorkflow per object.

    The single source-agnostic entrypoint shared by every trigger adapter — the local
    webhook (`trigger_api`), the AWS Lambda (`lambda_handler`), and the change-stream
    shim (`trigger_listener`). Returns the started workflow ids (empty for a non-object
    event such as MinIO's `s3:TestEvent`).
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
