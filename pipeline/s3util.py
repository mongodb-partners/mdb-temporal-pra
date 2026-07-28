"""Parse S3 event notifications (from SQS, MinIO, or a sources doc) into S3Refs.

AWS emits ``eventName`` like ``ObjectCreated:Put``; MinIO emits ``s3:ObjectCreated:Put``.
Both share the ``Records[].s3`` shape, so one parser handles all sources.
"""

from __future__ import annotations

import json
from typing import Any
from urllib.parse import unquote_plus

from .models import S3Ref


def refs_from_s3_event(event: dict[str, Any] | str | bytes) -> list[S3Ref]:
    if isinstance(event, (str, bytes)):
        try:
            event = json.loads(event)
        except (json.JSONDecodeError, ValueError):
            return []
    if not isinstance(event, dict):
        return []

    # SNS-wrapped notifications nest the S3 event JSON under "Message".
    if "Message" in event and "Records" not in event:
        try:
            event = json.loads(event["Message"])
        except (json.JSONDecodeError, TypeError, ValueError):
            return []

    if event.get("Event") == "s3:TestEvent" or "Records" not in event:
        return []

    refs: list[S3Ref] = []
    for rec in event["Records"]:
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
