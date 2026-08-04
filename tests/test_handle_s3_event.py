# ABOUTME: Tests for the shared handle_s3_event trigger core.
# Verifies MinIO and AWS S3 ObjectCreated events start the IngestWorkflow identically.

from __future__ import annotations

from temporalio.common import WorkflowIDConflictPolicy

from pipeline.config import settings
from pipeline.models import S3Ref, doc_id_for_uri
from pipeline.trigger import handle_s3_event


class FakeHandle:
    def __init__(self, wf_id: str) -> None:
        self.id = wf_id


class FakeClient:
    """Records start_workflow calls instead of contacting Temporal."""

    def __init__(self) -> None:
        self.calls: list[dict] = []

    async def start_workflow(self, workflow, arg, *, id, task_queue, id_conflict_policy, **_):
        self.calls.append(
            {
                "workflow": workflow,
                "arg": arg,
                "id": id,
                "task_queue": task_queue,
                "id_conflict_policy": id_conflict_policy,
            }
        )
        return FakeHandle(id)


def _event(event_name: str, key: str = "docs/a.md") -> dict:
    return {
        "Records": [
            {
                "eventName": event_name,
                "s3": {
                    "bucket": {"name": "temporal-datasources"},
                    "object": {"key": key, "eTag": "abc123", "size": 42},
                },
            }
        ]
    }


MINIO_EVENT = _event("s3:ObjectCreated:Put")   # MinIO prefixes with "s3:"
AWS_EVENT = _event("ObjectCreated:Put")         # AWS S3 does not


async def test_minio_event_starts_one_workflow():
    client = FakeClient()
    started = await handle_s3_event(client, MINIO_EVENT)

    assert len(client.calls) == 1
    call = client.calls[0]
    ref = call["arg"]
    assert isinstance(ref, S3Ref)
    assert ref.bucket == "temporal-datasources"
    assert ref.key == "docs/a.md"
    assert ref.s3_uri == "s3://temporal-datasources/docs/a.md"

    expected_id = f"ingest-{doc_id_for_uri('s3://temporal-datasources/docs/a.md')}"
    assert call["id"] == expected_id
    assert call["task_queue"] == settings.temporal_task_queue
    assert call["id_conflict_policy"] == WorkflowIDConflictPolicy.TERMINATE_EXISTING
    assert started == [expected_id]


async def test_minio_and_aws_events_are_handled_identically():
    """The same handler code must produce the same workflow start for both sources."""
    minio_client = FakeClient()
    aws_client = FakeClient()

    minio_started = await handle_s3_event(minio_client, MINIO_EVENT)
    aws_started = await handle_s3_event(aws_client, AWS_EVENT)

    assert minio_started == aws_started
    m, a = minio_client.calls[0], aws_client.calls[0]
    assert (m["arg"].bucket, m["arg"].key) == (a["arg"].bucket, a["arg"].key)
    assert m["id"] == a["id"]


async def test_test_event_starts_nothing():
    client = FakeClient()
    started = await handle_s3_event(client, {"Event": "s3:TestEvent"})
    assert started == []
    assert client.calls == []


async def test_multi_record_event_starts_one_per_object():
    client = FakeClient()
    event = {"Records": _event("s3:ObjectCreated:Put", "docs/a.md")["Records"]
             + _event("s3:ObjectCreated:Put", "docs/b.md")["Records"]}

    started = await handle_s3_event(client, event)

    assert len(started) == 2
    keys = {c["arg"].key for c in client.calls}
    assert keys == {"docs/a.md", "docs/b.md"}
