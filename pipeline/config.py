"""Environment-driven configuration for the Part 1 pipeline.

All settings come from environment variables (loaded from a local ``.env`` in dev).
Nothing here reaches out to a network; construct ``settings`` once and pass the
values into activities/clients.
"""

from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # ---- Temporal ----
    temporal_address: str = "localhost:7233"
    temporal_namespace: str = "default"
    temporal_task_queue: str = "pra-pipeline"

    # ---- Kafka ----
    kafka_bootstrap: str = "localhost:19092"
    raw_topic: str = "raw"
    chunks_topic: str = "chunks"
    s3_events_topic: str = "s3-events"  # MinIO publishes native S3 events here

    # ---- MongoDB Atlas ----
    mongodb_uri: str = ""
    mongodb_db: str = "pra"
    knowledge_collection: str = "knowledge"
    knowledge_v2_collection: str = "knowledge_v2"

    # ---- Voyage AI ----
    voyage_api_key: str = ""
    voyage_model: str = "voyage-3"
    embed_dim: int = 1024

    # ---- AWS / S3 ----
    aws_region: str = "us-east-1"
    s3_bucket: str = ""
    sqs_queue_url: str = ""
    # Set to a MinIO endpoint (e.g. http://localhost:9000) to use MinIO instead of AWS S3.
    s3_endpoint_url: str = ""
    # Source selection: "auto" picks minio (if s3_endpoint_url) -> sqs (if queue) -> poll.
    s3_source: str = "auto"  # auto | minio | sqs | poll

    def resolved_source(self) -> str:
        if self.s3_source != "auto":
            return self.s3_source
        if self.s3_endpoint_url:
            return "minio"
        if self.sqs_queue_url:
            return "sqs"
        return "poll"

    # ---- Chunking ----
    chunk_size: int = 1200
    chunk_overlap: int = 150

    # ---- S3 poll fallback ----
    s3_poll_prefix: str = ""
    s3_poll_interval_seconds: int = 15


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return a process-wide cached Settings instance."""
    return Settings()


# Convenience singleton for import sites that just want values.
settings = get_settings()
