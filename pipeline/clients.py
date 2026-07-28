"""Lazily-constructed external clients (Mongo, Voyage, S3, SQS).

Clients are built on first use and cached per-process, so importing an activity
module never opens a socket. Activities run in the worker process (outside the
Temporal workflow sandbox), so real I/O clients are safe here.
"""

from __future__ import annotations

from functools import lru_cache
from typing import TYPE_CHECKING

from .config import settings

if TYPE_CHECKING:  # avoid importing heavy deps at module load
    import boto3
    import voyageai
    from pymongo import MongoClient


@lru_cache(maxsize=1)
def mongo_client() -> "MongoClient":
    from pymongo import MongoClient

    if not settings.mongodb_uri:
        raise RuntimeError("MONGODB_URI is not set — populate .env before running.")
    return MongoClient(settings.mongodb_uri, appname="teamporal-app")


def knowledge_collection(name: str | None = None):
    db = mongo_client()[settings.mongodb_db]
    return db[name or settings.knowledge_collection]


@lru_cache(maxsize=1)
def voyage_client() -> "voyageai.Client":
    import voyageai

    if not settings.voyage_api_key:
        raise RuntimeError("VOYAGE_API_KEY is not set — populate .env before running.")
    return voyageai.Client(api_key=settings.voyage_api_key)


def _aws_kwargs() -> dict:
    """Common boto3 kwargs: region + explicit creds when provided in settings.

    boto3 does not read our .env, so any creds set there (incl. MinIO's
    minioadmin/minioadmin) must be passed explicitly. When blank, boto3 falls back
    to its standard credential chain (profile / role / actual env vars).
    """
    kwargs: dict = {"region_name": settings.aws_region}
    if settings.aws_access_key_id and settings.aws_secret_access_key:
        kwargs["aws_access_key_id"] = settings.aws_access_key_id
        kwargs["aws_secret_access_key"] = settings.aws_secret_access_key
    return kwargs


@lru_cache(maxsize=1)
def s3_client():
    import boto3
    from botocore.config import Config

    kwargs = _aws_kwargs()
    if settings.s3_endpoint_url:
        # MinIO (or any S3-compatible endpoint) needs an explicit endpoint and
        # path-style addressing (no virtual-host buckets).
        kwargs["endpoint_url"] = settings.s3_endpoint_url
        kwargs["config"] = Config(s3={"addressing_style": "path"})
    return boto3.client("s3", **kwargs)


@lru_cache(maxsize=1)
def sqs_client():
    import boto3

    return boto3.client("sqs", **_aws_kwargs())
