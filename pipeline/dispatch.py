"""Kafka dispatchers + S3 source producer.

Bridges S3 -> Kafka and Kafka -> Temporal:

  * S3 source producer:  S3/SQS (or bucket poll) -> raw topic
  * RawDispatcher:       raw topic -> ChunkWorkflow
  * ChunksDispatcher:    chunks topic -> EmbedWriteWorkflow

Run:  uv run python -m pipeline.dispatch
"""

from __future__ import annotations

import asyncio

from aiokafka import AIOKafkaProducer
from temporalio.client import Client

from .config import settings
from .kafka.chunks_topic import run_chunks_dispatcher
from .kafka.producers import (
    run_minio_kafka_bridge,
    run_s3_poll_producer,
    run_s3_sqs_bridge,
)
from .kafka.raw_topic import run_raw_dispatcher


async def main() -> None:
    client = await Client.connect(
        settings.temporal_address,
        namespace=settings.temporal_namespace,
    )

    producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap)
    await producer.start()

    mode = settings.resolved_source()
    if mode == "minio":
        source = run_minio_kafka_bridge(producer)
        print("[dispatch] S3 source: MinIO -> Kafka native events")
    elif mode == "sqs":
        source = run_s3_sqs_bridge(producer)
        print("[dispatch] S3 source: AWS S3 -> SQS event bridge")
    else:
        source = run_s3_poll_producer(producer)
        print("[dispatch] S3 source: bucket poll")

    try:
        await asyncio.gather(
            source,
            run_raw_dispatcher(client),
            run_chunks_dispatcher(client),
        )
    finally:
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
