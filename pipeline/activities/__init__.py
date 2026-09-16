"""Temporal activities for the ingest pipeline."""

from .ingest import fetch_and_stage_chunks, index_document

ALL_ACTIVITIES = [
    fetch_and_stage_chunks,
    index_document,
]
