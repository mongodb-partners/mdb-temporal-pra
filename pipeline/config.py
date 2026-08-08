"""Environment-driven configuration for the Part 1 pipeline.

All settings come from environment variables (loaded from a local ``.env`` in dev).
Nothing here reaches out to a network; construct ``settings`` once and pass the
values into activities/clients.
"""

from __future__ import annotations

from functools import lru_cache

from pydantic_settings import (
    BaseSettings,
    DotEnvSettingsSource,
    EnvSettingsSource,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        # In local development, prefer values from .env over stale exported shell
        # variables so repo config changes take effect consistently across restarts.
        return (
            init_settings,
            DotEnvSettingsSource(settings_cls),
            EnvSettingsSource(settings_cls),
            file_secret_settings,
        )

    # ---- Temporal ----
    temporal_address: str = "localhost:7233"
    temporal_namespace: str = "default"
    temporal_task_queue: str = "temporal-pipeline"

    # ---- MongoDB Atlas ----
    mongodb_uri: str = ""
    mongodb_db: str = "temporal"
    chunks_collection: str = "chunks_staging"     # staged chunks between workflow stages
    knowledge_collection: str = "knowledge"       # searchable embedded chunks (blue)
    knowledge_v2_collection: str = "knowledge_v2" # blue/green backfill target (green)
    config_collection: str = "temporal_config"         # cutover active-pointer doc
    memory_collection: str = "agent_memory"       # deep-agent write-back
    vector_search_index_name: str = "temporalai_search_index"

    # ---- Voyage AI ----
    voyage_api_key: str = ""
    voyage_model: str = "voyage-3.5"
    voyage_rerank_model: str = "rerank-2.5"
    embed_dim: int = 1024

    # ---- OpenAI (durable research agent — OpenAI Agents SDK on Temporal) ----
    openai_api_key: str = ""
    agent_model: str = "gpt-4.1"  # OpenAI model for the agent loop; adjust to a current one
    agent_max_turns: int = 8  # guardrail on the tool-use loop

    # ---- Service ports ----
    trigger_api_port: int = 8088
    agent_api_port: int = 8090

    # ---- AWS / S3 ----
    aws_region: str = "us-east-1"
    # Explicit creds (also matches MinIO's minioadmin/minioadmin). Leave blank on real
    # AWS to fall back to the standard credential chain (profile / role / env).
    aws_access_key_id: str = ""
    aws_secret_access_key: str = ""
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
