"""Temporal activities for the ingest pipeline."""

from .backfill import ensure_target_index, reembed_and_write, read_source_batch
from .ingest import embed_staged_chunk, fetch_and_stage_chunks, index_document

ALL_ACTIVITIES = [
    fetch_and_stage_chunks,
    embed_staged_chunk,
    index_document,
    read_source_batch,
    reembed_and_write,
    ensure_target_index,
]
