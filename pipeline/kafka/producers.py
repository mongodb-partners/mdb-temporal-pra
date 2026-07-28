"""S3 source producers → Kafka raw topic.

Three ways to get "on upload" events onto the raw topic — all normalize to RawRecord:

* ``run_minio_kafka_bridge`` — MinIO publishes native S3 events straight to a Kafka
  topic (no SQS). This bridge consumes that topic and re-emits RawRecords onto ``raw``.
  The self-contained demo path.
* ``run_s3_sqs_bridge`` — real AWS S3. Event Notifications (s3:ObjectCreated:*) land in
  an SQS queue; this bridge long-polls the queue and produces a RawRecord per new object.
* ``run_s3_poll_producer`` — fallback for environments without event wiring. Lists the
  bucket on an interval and produces objects it hasn't seen before.

MinIO and SQS both deliver the *same* S3 event JSON, so ``_refs_from_s3_event`` parses
both. boto3 is synchronous; its calls are offloaded to threads so they don't block the loop.
"""

from __future__ import annotations

import asyncio
import json
from urllib.parse import unquote_plus

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from ..clients import s3_client, sqs_client
from ..config import settings
from ..models import RawRecord, S3Ref
from .raw_topic import produce_raw


def _refs_from_s3_event(body: str) -> list[S3Ref]:
    """Parse an S3 event notification (from SQS or MinIO/Kafka) into S3Refs.

    Handles raw S3 events, SNS-wrapped events, and MinIO's envelope — all share the
    ``Records[].s3`` shape and ``s3:ObjectCreated:*`` event names.
    """
    try:
        msg = json.loads(body)
    except json.JSONDecodeError:
        return []

    # SNS-wrapped notifications nest the S3 event as a JSON string under "Message".
    if "Message" in msg and "Records" not in msg:
        try:
            msg = json.loads(msg["Message"])
        except (json.JSONDecodeError, TypeError):
            return []

    if msg.get("Event") == "s3:TestEvent" or "Records" not in msg:
        return []

    refs: list[S3Ref] = []
    for rec in msg["Records"]:
        # AWS emits "ObjectCreated:Put"; MinIO emits "s3:ObjectCreated:Put".
        if "ObjectCreated" not in rec.get("eventName", ""):
            continue
        s3 = rec.get("s3", {})
        bucket = s3.get("bucket", {}).get("name", "")
        obj = s3.get("object", {})
        key = unquote_plus(obj.get("key", ""))
        if not bucket or not key:
            continue
        refs.append(
            S3Ref.make(
                bucket=bucket,
                key=key,
                etag=obj.get("eTag", ""),
                size=int(obj.get("size", 0) or 0),
            )
        )
    return refs


async def run_s3_sqs_bridge(producer: AIOKafkaProducer) -> None:
    """Long-poll SQS for S3 ObjectCreated events and produce them to the raw topic."""
    if not settings.sqs_queue_url:
        raise RuntimeError("SQS_QUEUE_URL is empty — use run_s3_poll_producer instead.")

    sqs = sqs_client()
    print(f"[s3-sqs-bridge] polling {settings.sqs_queue_url} -> '{settings.raw_topic}'")
    while True:
        resp = await asyncio.to_thread(
            sqs.receive_message,
            QueueUrl=settings.sqs_queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=20,
        )
        for message in resp.get("Messages", []):
            for ref in _refs_from_s3_event(message.get("Body", "")):
                await produce_raw(producer, RawRecord.from_s3(ref))
                print(f"[s3-sqs-bridge] produced {ref.s3_uri}")
            await asyncio.to_thread(
                sqs.delete_message,
                QueueUrl=settings.sqs_queue_url,
                ReceiptHandle=message["ReceiptHandle"],
            )


async def run_s3_poll_producer(producer: AIOKafkaProducer) -> None:
    """Fallback: list the bucket on an interval and produce newly-seen objects."""
    s3 = s3_client()
    seen: dict[str, str] = {}  # key -> etag
    print(
        f"[s3-poll] every {settings.s3_poll_interval_seconds}s: "
        f"s3://{settings.s3_bucket}/{settings.s3_poll_prefix} -> '{settings.raw_topic}'"
    )
    paginator = s3.get_paginator("list_objects_v2")
    while True:
        pages = await asyncio.to_thread(
            lambda: list(paginator.paginate(Bucket=settings.s3_bucket, Prefix=settings.s3_poll_prefix))
        )
        for page in pages:
            for obj in page.get("Contents", []):
                key, etag = obj["Key"], obj.get("ETag", "").strip('"')
                if key.endswith("/"):
                    continue  # folder placeholder
                if seen.get(key) == etag:
                    continue  # unchanged since last poll
                seen[key] = etag
                ref = S3Ref.make(settings.s3_bucket, key, etag=etag, size=int(obj.get("Size", 0)))
                await produce_raw(producer, RawRecord.from_s3(ref))
                print(f"[s3-poll] produced {ref.s3_uri}")
        await asyncio.sleep(settings.s3_poll_interval_seconds)


async def run_minio_kafka_bridge(producer: AIOKafkaProducer) -> None:
    """Consume MinIO's native S3 events from Kafka and re-emit RawRecords onto raw.

    MinIO publishes bucket notifications directly to a Kafka topic (no SQS). We keep the
    raw-topic contract intact by normalizing those events into RawRecord here.
    """
    consumer = AIOKafkaConsumer(
        settings.s3_events_topic,
        bootstrap_servers=settings.kafka_bootstrap,
        group_id="minio-bridge",
        enable_auto_commit=True,
        auto_offset_reset="earliest",
    )
    await consumer.start()
    print(f"[minio-bridge] consuming '{settings.s3_events_topic}' -> '{settings.raw_topic}'")
    try:
        async for msg in consumer:
            for ref in _refs_from_s3_event(msg.value.decode("utf-8")):
                await produce_raw(producer, RawRecord.from_s3(ref))
                print(f"[minio-bridge] produced {ref.s3_uri}")
    finally:
        await consumer.stop()
