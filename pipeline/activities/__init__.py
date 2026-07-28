"""Temporal activities for the Part 1 pipeline."""

from .chunk import chunk_document, is_duplicate
from .embed_voyage import embed_chunk
from .produce_chunks import produce_chunks_activity
from .write_atlas import read_knowledge_batch, upsert_embedded_chunk

ALL_ACTIVITIES = [
    chunk_document,
    is_duplicate,
    embed_chunk,
    produce_chunks_activity,
    upsert_embedded_chunk,
    read_knowledge_batch,
]
