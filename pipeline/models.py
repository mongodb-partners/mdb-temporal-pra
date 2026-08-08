"""Data contracts passed across Temporal activity/workflow boundaries.

These are plain dataclasses so they serialize cleanly through Temporal's default
(JSON) data converter. Keep them free of any client objects or non-serializable state.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Any


@dataclass
class S3Ref:
    """A reference to a single object in S3 (the unit passed to the IngestWorkflow)."""

    bucket: str
    key: str
    s3_uri: str
    etag: str = ""
    size: int = 0
    content_type: str = ""

    @staticmethod
    def make(bucket: str, key: str, etag: str = "", size: int = 0, content_type: str = "") -> "S3Ref":
        return S3Ref(
            bucket=bucket,
            key=key,
            s3_uri=f"s3://{bucket}/{key}",
            etag=etag.strip('"'),
            size=size,
            content_type=content_type,
        )


@dataclass
class Chunk:
    """One chunk of a document, ready to be embedded."""

    doc_id: str
    chunk_id: str
    ordinal: int
    text: str
    content_hash: str  # sha256 of this chunk's text
    doc_content_hash: str = ""  # sha256 of the whole source object (for dedupe)
    source_uri: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ChunkResult:
    """Return value of the chunk_document activity."""

    doc_id: str
    doc_content_hash: str
    chunks: list[Chunk] = field(default_factory=list)


@dataclass
class EmbeddedChunk:
    """A chunk plus its Voyage embedding, ready to upsert into Atlas."""

    doc_id: str
    chunk_id: str
    ordinal: int
    text: str
    content_hash: str
    embedding: list[float]
    model: str
    dim: int
    doc_content_hash: str = ""
    source_uri: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


def doc_id_for_uri(uri: str) -> str:
    """Stable, filesystem/workflow-id-safe document id derived from a source URI."""
    return hashlib.sha1(uri.encode("utf-8")).hexdigest()[:16]


def sha256_hex(data: bytes | str) -> str:
    if isinstance(data, str):
        data = data.encode("utf-8")
    return hashlib.sha256(data).hexdigest()
