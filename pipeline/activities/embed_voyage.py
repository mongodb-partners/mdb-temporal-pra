"""Embedding activity — one chunk per activity call (the metered, expensive step).

Keeping embedding at single-chunk granularity is deliberate: each successful call is
recorded in Temporal workflow history, so a crash mid-document resumes *without
re-embedding* the chunks already done (Guarantee #3).
"""

from __future__ import annotations

from temporalio import activity

from ..clients import voyage_client
from ..config import settings
from ..models import Chunk, EmbeddedChunk


@activity.defn
def embed_chunk(chunk: Chunk, model: str | None = None) -> EmbeddedChunk:
    """Embed a single chunk with Voyage and return it ready for Atlas upsert.

    ``model`` overrides the configured model — used by BackfillWorkflow to re-embed
    existing documents with an upgraded model (e.g. voyage-3 -> voyage-3-large).
    """
    use_model = model or settings.voyage_model
    activity.heartbeat(f"embedding {chunk.chunk_id}")

    result = voyage_client().embed(
        [chunk.text],
        model=use_model,
        input_type="document",
    )
    vector = result.embeddings[0]

    return EmbeddedChunk(
        doc_id=chunk.doc_id,
        chunk_id=chunk.chunk_id,
        ordinal=chunk.ordinal,
        text=chunk.text,
        content_hash=chunk.content_hash,
        doc_content_hash=chunk.doc_content_hash,
        embedding=list(vector),
        model=use_model,
        dim=len(vector),
        source_uri=chunk.source_uri,
        metadata=chunk.metadata,
    )
