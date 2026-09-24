# The only module that issues ranged reads against the object store.
# fetch_range/fetch_span are trusted internals; read_span is the model-facing resolver.

from __future__ import annotations

from typing import Any

from botocore.exceptions import ClientError

from .clients import knowledge_collection, s3_client
from .config_store import get_active
from .models import sha256_hex

_STALE_CODES = {"PreconditionFailed", "412"}
_MISSING_CODES = {"NoSuchKey", "NoSuchBucket", "404"}

_REQUIRED_POINTER_FIELDS = ("span", "bucket", "key", "etag", "content_hash")
_REQUIRED_SPAN_FIELDS = ("start", "end")

_POINTER_PROJECTION = {
    "_id": 0,
    "chunk_id": 1,
    "ordinal": 1,
    "source_uri": 1,
    "bucket": 1,
    "key": 1,
    "etag": 1,
    "span": 1,
    "content_hash": 1,
}


class SpanStale(Exception):
    """The object changed since indexing, or its bytes no longer hash to what was stored."""


class SpanMissing(Exception):
    """The object is gone."""


def fetch_range(
    bucket: str, key: str, etag: str, start: int, end: int, *, client: Any = None
) -> bytes:
    """Ranged GET with the IfMatch guard. ``end`` is exclusive; HTTP Range is inclusive."""
    c = client or s3_client()
    try:
        obj = c.get_object(
            Bucket=bucket, Key=key, IfMatch=etag, Range=f"bytes={start}-{end - 1}"
        )
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        if code in _STALE_CODES:
            raise SpanStale(f"{bucket}/{key} changed since indexing") from exc
        if code in _MISSING_CODES:
            raise SpanMissing(f"{bucket}/{key} is gone") from exc
        raise
    return obj["Body"].read()


def fetch_span(
    bucket: str, key: str, etag: str, start: int, end: int, *, client: Any = None
) -> str:
    """Ranged GET decoded strictly. Span bounds are character boundaries by construction."""
    raw = fetch_range(bucket, key, etag, start, end, client=client)
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise SpanStale(f"{bucket}/{key} range does not decode as utf-8") from exc


def _pointer_is_usable(doc: dict) -> bool:
    """True only when every pointer field is present AND the right shape.

    Presence alone is not enough. These values are read back out of the
    database, so a half-written or hand-edited document can carry a null etag
    or a string span bound, and those reach boto3 as a TypeError or a
    parameter-validation error rather than as one of read_span's four
    statuses. This is the model-facing tool: it returns a status or it returns
    text, never a traceback.
    """
    for field in _REQUIRED_POINTER_FIELDS:
        if field == "span":
            continue
        value = doc.get(field)
        if not isinstance(value, str) or not value:
            return False

    span = doc.get("span")
    if not isinstance(span, dict):
        return False
    bounds = []
    for field in _REQUIRED_SPAN_FIELDS:
        bound = span.get(field)
        # bool is an int subclass, and True as a byte offset is nonsense.
        if not isinstance(bound, int) or isinstance(bound, bool):
            return False
        bounds.append(bound)
    start, end = bounds
    return 0 <= start < end


def read_span(chunk_id: str, *, collection: Any = None, client: Any = None) -> dict:
    """Resolve a chunk id to its pointer server-side, then read and verify it.

    Takes no pointer arguments on purpose. A model that could supply a bucket, key
    and range could read any object this tool's credential reaches, and a
    caller-supplied hash would certify whatever it found.
    """
    if not isinstance(chunk_id, str):
        return {"chunk_id": None, "status": "unknown"}

    coll = collection if collection is not None else knowledge_collection(
        get_active()["active_collection"]
    )
    doc = coll.find_one({"chunk_id": chunk_id}, _POINTER_PROJECTION)
    if doc is None:
        return {"chunk_id": chunk_id, "status": "unknown"}

    base = {
        "chunk_id": doc["chunk_id"],
        "ordinal": doc.get("ordinal"),
        "source_uri": doc.get("source_uri"),
    }

    if not _pointer_is_usable(doc):
        # Exists under this chunk_id but its pointer is absent or malformed: a
        # legacy pre-branch document, a partially written collection, a pointer
        # aimed at the wrong place, or a field of the wrong type. Degrade to a
        # status. Never fall through to any `text` this doc might itself carry;
        # only "ok" carries text. And never spend the GetObject: the pointer is
        # already known to be unusable before a ranged read would run.
        return {**base, "status": "unknown"}

    span = doc["span"]

    try:
        text = fetch_span(
            doc["bucket"], doc["key"], doc["etag"], span["start"], span["end"], client=client
        )
    except SpanMissing:
        return {**base, "status": "missing"}
    except SpanStale:
        return {**base, "status": "stale"}

    if sha256_hex(text) != doc["content_hash"]:
        return {**base, "status": "stale"}
    return {**base, "status": "ok", "text": text}
