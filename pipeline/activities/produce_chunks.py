"""Async activity that writes a document's chunks back to the Kafka chunks topic.

This is the durable hand-off inside ChunkWorkflow. It's async (aiokafka) and runs on
the worker's event loop; the producer is started lazily on first use and reused.
"""

from __future__ import annotations

from aiokafka import AIOKafkaProducer
from temporalio import activity

from ..config import settings
from ..kafka.chunks_topic import produce_chunks
from ..models import Chunk

_producer: AIOKafkaProducer | None = None


async def _get_producer() -> AIOKafkaProducer:
    global _producer
    if _producer is None:
        _producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap)
        await _producer.start()
    return _producer


@activity.defn
async def produce_chunks_activity(doc_id: str, chunks: list[Chunk]) -> int:
    """Publish the document's chunks to the chunks topic; returns the chunk count."""
    producer = await _get_producer()
    await produce_chunks(producer, doc_id, chunks)
    activity.logger.info("produced %d chunk(s) for %s to '%s'", len(chunks), doc_id, settings.chunks_topic)
    return len(chunks)
