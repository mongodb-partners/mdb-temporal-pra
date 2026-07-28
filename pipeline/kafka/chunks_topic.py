"""Chunks topic: durable hand-off between chunking and embedding.

The chunk workflow writes each document's chunks back to Kafka (one message per
document). This decouples the cheap chunking step from the expensive embedding step
and guarantees every chunk is processed even across restarts (Guarantee #3).
"""

from __future__ import annotations

import json

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from temporalio.client import Client
from temporalio.service import RPCError

from ..config import settings
from ..models import Chunk


def _chunks_message(doc_id: str, chunks: list[Chunk]) -> bytes:
    from dataclasses import asdict

    payload = {"doc_id": doc_id, "chunks": [asdict(c) for c in chunks]}
    return json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


async def produce_chunks(producer: AIOKafkaProducer, doc_id: str, chunks: list[Chunk]) -> None:
    """Publish one document's chunk list onto the chunks topic, keyed by doc_id."""
    await producer.send_and_wait(
        settings.chunks_topic,
        key=doc_id.encode("utf-8"),
        value=_chunks_message(doc_id, chunks),
    )


def _chunks_from_json(data: bytes) -> tuple[str, list[Chunk]]:
    obj = json.loads(data)
    chunks = [Chunk(**c) for c in obj["chunks"]]
    return obj["doc_id"], chunks


async def run_chunks_dispatcher(client: Client) -> None:
    """Consume the chunks topic and start an EmbedWriteWorkflow per document."""
    from ..workflows.embed_write_workflow import EmbedWriteWorkflow

    consumer = AIOKafkaConsumer(
        settings.chunks_topic,
        bootstrap_servers=settings.kafka_bootstrap,
        group_id="chunks-dispatcher",
        enable_auto_commit=True,
        auto_offset_reset="earliest",
    )
    await consumer.start()
    print(f"[chunks-dispatcher] consuming '{settings.chunks_topic}' -> EmbedWriteWorkflow")
    try:
        async for msg in consumer:
            doc_id, chunks = _chunks_from_json(msg.value)
            try:
                await client.start_workflow(
                    EmbedWriteWorkflow.run,
                    args=[doc_id, chunks],
                    id=f"embed-{doc_id}",
                    task_queue=settings.temporal_task_queue,
                )
                print(f"[chunks-dispatcher] started EmbedWriteWorkflow embed-{doc_id} ({len(chunks)} chunks)")
            except RPCError as exc:
                print(f"[chunks-dispatcher] skip {doc_id}: {exc.message}")
    finally:
        await consumer.stop()
