"""Create the raw + chunks Kafka topics on the local Redpanda broker.

Run:  uv run python -m infra.create_topics
"""

from __future__ import annotations

import asyncio

from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError

from pipeline.config import settings


async def main() -> None:
    admin = AIOKafkaAdminClient(bootstrap_servers=settings.kafka_bootstrap)
    await admin.start()
    try:
        topics = [
            NewTopic(settings.raw_topic, num_partitions=3, replication_factor=1),
            NewTopic(settings.chunks_topic, num_partitions=3, replication_factor=1),
            # MinIO publishes native S3 events here (unused on the AWS/SQS path).
            NewTopic(settings.s3_events_topic, num_partitions=3, replication_factor=1),
        ]
        for topic in topics:
            try:
                await admin.create_topics([topic])
                print(f"created topic '{topic.name}'")
            except TopicAlreadyExistsError:
                print(f"topic '{topic.name}' already exists")
    finally:
        await admin.close()


if __name__ == "__main__":
    asyncio.run(main())
