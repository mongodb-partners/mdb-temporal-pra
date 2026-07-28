"""Raw topic: producer helper + dispatcher that starts a ChunkWorkflow per record.

The dispatcher is the bridge from Kafka into Temporal. It never runs workflow logic
itself — it just consumes raw records and starts a durable workflow, using a
deterministic workflow id so duplicate Kafka deliveries collapse to one run.
"""

from __future__ import annotations

import json

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from temporalio.client import Client
from temporalio.service import RPCError

from ..config import settings
from ..models import RawRecord, S3Ref, to_json


async def produce_raw(producer: AIOKafkaProducer, record: RawRecord) -> None:
    """Publish a source record onto the raw topic, keyed by doc_id."""
    await producer.send_and_wait(
        settings.raw_topic,
        key=record.doc_id.encode("utf-8"),
        value=to_json(record),
    )


def _record_from_json(data: bytes) -> RawRecord:
    obj = json.loads(data)
    ref = obj.get("ref")
    return RawRecord(
        source=obj.get("source", "s3"),
        doc_id=obj["doc_id"],
        ref=S3Ref(**ref) if ref else None,
        payload=obj.get("payload"),
        metadata=obj.get("metadata", {}),
    )


async def run_raw_dispatcher(client: Client) -> None:
    """Consume the raw topic and start a ChunkWorkflow for each record."""
    # Imported here so the workflow sandbox never transitively imports this module.
    from ..workflows.chunk_workflow import ChunkWorkflow

    consumer = AIOKafkaConsumer(
        settings.raw_topic,
        bootstrap_servers=settings.kafka_bootstrap,
        group_id="raw-dispatcher",
        enable_auto_commit=True,
        auto_offset_reset="earliest",
    )
    await consumer.start()
    print(f"[raw-dispatcher] consuming '{settings.raw_topic}' -> ChunkWorkflow")
    try:
        async for msg in consumer:
            record = _record_from_json(msg.value)
            try:
                await client.start_workflow(
                    ChunkWorkflow.run,
                    record,
                    id=f"chunk-{record.doc_id}",
                    task_queue=settings.temporal_task_queue,
                )
                print(f"[raw-dispatcher] started ChunkWorkflow chunk-{record.doc_id}")
            except RPCError as exc:
                # WorkflowExecutionAlreadyStarted → already handled this record; skip.
                print(f"[raw-dispatcher] skip {record.doc_id}: {exc.message}")
    finally:
        await consumer.stop()
