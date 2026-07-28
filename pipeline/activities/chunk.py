"""Chunking activities: download an S3 object, extract text, split, and dedupe.

These are synchronous activities — Temporal runs them in the worker's thread pool,
so blocking boto3/pymongo calls are fine here.
"""

from __future__ import annotations

import json

from temporalio import activity

from ..clients import knowledge_collection, s3_client
from ..config import settings
from ..models import Chunk, ChunkResult, RawRecord, sha256_hex

# Extensions we can turn into text without extra dependencies.
_TEXT_EXTS = (".txt", ".md", ".markdown", ".json", ".csv", ".log", ".html", ".xml", ".yaml", ".yml")


def _extract_text(key: str, body: bytes, content_type: str) -> str:
    """Best-effort text extraction from raw object bytes."""
    lower = key.lower()

    if lower.endswith(".pdf") or content_type == "application/pdf":
        try:
            import io

            from pypdf import PdfReader  # optional extra
        except ImportError as exc:  # pragma: no cover - depends on extras
            raise RuntimeError(
                "PDF object received but 'pypdf' is not installed. Install with: uv sync --extra pdf"
            ) from exc
        reader = PdfReader(io.BytesIO(body))
        return "\n\n".join((page.extract_text() or "") for page in reader.pages)

    if lower.endswith(".json"):
        try:
            return json.dumps(json.loads(body), indent=2, ensure_ascii=False)
        except (json.JSONDecodeError, UnicodeDecodeError):
            pass  # fall through to plain decode

    if lower.endswith(_TEXT_EXTS) or content_type.startswith("text/"):
        return body.decode("utf-8", errors="replace")

    # Unknown binary type: attempt a lenient decode so the pipeline still flows.
    return body.decode("utf-8", errors="replace")


def _split(text: str, size: int, overlap: int) -> list[str]:
    """Character-window splitter with overlap. Simple and deterministic."""
    text = text.strip()
    if not text:
        return []
    if size <= 0:
        return [text]
    step = max(1, size - overlap)
    return [text[i : i + size] for i in range(0, len(text), step) if text[i : i + size].strip()]


@activity.defn
def chunk_document(record: RawRecord) -> ChunkResult:
    """Download the S3 object referenced by ``record``, extract text, and chunk it."""
    if record.ref is None:
        raise ValueError(f"RawRecord {record.doc_id} has no S3 ref")

    ref = record.ref
    obj = s3_client().get_object(Bucket=ref.bucket, Key=ref.key)
    body: bytes = obj["Body"].read()
    content_type = obj.get("ContentType", ref.content_type or "")

    doc_hash = sha256_hex(body)
    text = _extract_text(ref.key, body, content_type)

    pieces = _split(text, settings.chunk_size, settings.chunk_overlap)
    activity.logger.info("chunked %s into %d chunk(s)", ref.s3_uri, len(pieces))

    chunks = [
        Chunk(
            doc_id=record.doc_id,
            chunk_id=f"{record.doc_id}:{i}",
            ordinal=i,
            text=piece,
            content_hash=sha256_hex(piece),
            doc_content_hash=doc_hash,
            source_uri=ref.s3_uri,
            metadata={**record.metadata, "content_type": content_type, "key": ref.key},
        )
        for i, piece in enumerate(pieces)
    ]
    return ChunkResult(doc_id=record.doc_id, doc_content_hash=doc_hash, chunks=chunks)


@activity.defn
def is_duplicate(doc_id: str, doc_content_hash: str) -> bool:
    """Guarantee #2: skip re-processing an object whose content is unchanged.

    Returns True if the knowledge collection already holds this doc_id at the same
    content hash (i.e. this exact object version was already embedded).
    """
    coll = knowledge_collection()
    existing = coll.find_one(
        {"doc_id": doc_id, "doc_content_hash": doc_content_hash},
        projection={"_id": 1},
    )
    return existing is not None
