"""Temporal workflows for the ingest pipeline."""

from .backfill_workflow import BackfillWorkflow
from .ingest_workflow import IngestWorkflow

ALL_WORKFLOWS = [IngestWorkflow, BackfillWorkflow]
