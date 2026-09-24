"""Environment-driven configuration for the Part 1 pipeline.

All settings come from environment variables (loaded from a local ``.env`` in dev).
Nothing here reaches out to a network; construct ``settings`` once and pass the
values into activities/clients.
"""

from __future__ import annotations

import os
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
        # ENV_FILE picks the dotenv file (e.g. .env.minio for local MinIO); default .env.
        env_file=os.environ.get("ENV_FILE", ".env"),
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
    knowledge_collection: str = "knowledge_zc"    # searchable pointers + vectors (no text)
    knowledge_v2_collection: str = "knowledge_v2" # blue/green backfill target (green)
    config_collection: str = "temporal_config"         # cutover active-pointer doc
    memory_collection: str = "agent_memory"       # deep-agent write-back
    vector_search_index_name: str = "temporalai_search_index"

    # ---- Voyage AI ----
    voyage_api_key: str = ""
    voyage_model: str = "voyage-3.5"
    # MongoDB's hosted Voyage endpoint. The voyageai SDK would infer this from
    # the key prefix (al- routes here, anything else to api.voyageai.com); set
    # it explicitly so the endpoint is predictable (and allow-listable)
    # instead of depending on the shape of a credential.
    voyage_base_url: str = "https://ai.mongodb.com/v1"
    embed_dim: int = 1024
    voyage_rerank_model: str = "rerank-2.5"

    # ---- Durable research agent (OpenAI Agents SDK) ----
    openai_api_key: str = ""
    # Optional OpenAI-compatible endpoint (a gateway or proxy). Blank means api.openai.com.
    openai_base_url: str = ""
    # Also send the key in this header, for gateways that do not read the Bearer token
    # (e.g. "api-key" on Azure API Management). Blank means Bearer only.
    openai_api_key_header: str = ""
    # False selects Chat Completions, for endpoints without the Responses API. The hosted
    # web-search tool needs the Responses API, so the agent drops it when this is false.
    openai_use_responses: bool = True
    agent_model: str = "gpt-4.1"  # OpenAI model for the agent loop; adjust to a current one
    agent_max_turns: int = 12  # guardrail on the tool-use loop

    # ---- Service ports ----
    trigger_api_port: int = 8088
    agent_api_port: int = 8090

    # ---- AWS / S3 ----
    aws_region: str = "us-east-1"
    # Explicit creds. Leave blank to fall back to boto3's standard credential chain
    # (profile / role / env).
    aws_access_key_id: str = ""
    aws_secret_access_key: str = ""
    s3_bucket: str = ""
    # S3-compatible endpoint, e.g. http://localhost:9000 for local MinIO. Blank means AWS S3.
    s3_endpoint_url: str = ""

    # ---- Chunking ----
    chunk_size: int = 1200
    chunk_overlap: int = 150


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return a process-wide cached Settings instance."""
    return Settings()


# Convenience singleton for import sites that just want values.
settings = get_settings()
