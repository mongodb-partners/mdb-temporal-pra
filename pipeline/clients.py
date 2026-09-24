"""Lazily-constructed external clients (Mongo, Voyage, S3).

Clients are built on first use and cached per-process, so importing an activity
module never opens a socket. Activities run in the worker process (outside the
Temporal workflow sandbox), so real I/O clients are safe here.

Credentials are resolved on every call rather than once at import. Under Temporal
that changes nothing: the worker's environment is fixed before the process
starts. It matters in a sandboxed tool runtime, where ``pipeline.config``'s
``settings`` singleton can be built when the tool module is imported, before
the sandbox's secrets are in the environment. Resolving late, and keying
each cache on the resolved value, means a client built from a blank credential is
never the one handed to a later caller.
"""

from __future__ import annotations

import os
from functools import lru_cache
from typing import TYPE_CHECKING

from .config import settings

if TYPE_CHECKING:  # avoid importing heavy deps at module load
    import voyageai
    from pymongo import MongoClient

# Caches are keyed on credentials, so they hold a handful of entries rather than
# one. Bounded, because the key space is attacker-influenced only in the sense
# that a misconfigured deploy could flap between values.
_CACHE_SIZE = 4


def _resolve(setting_value: str, env_name: str) -> str:
    """Prefer the configured value, fall back to the live environment.

    ``pipeline.config`` deliberately lets ``.env`` win over exported shell
    variables, and that ordering is preserved here: the environment is consulted
    only when the setting is blank, which is the deployed-agent case and never
    the local-dev or test case.
    """
    return setting_value or os.environ.get(env_name, "")


@lru_cache(maxsize=_CACHE_SIZE)
def _mongo_client_for(uri: str) -> "MongoClient":
    from pymongo import MongoClient

    return MongoClient(uri, appname="temporal-app")


def mongo_client() -> "MongoClient":
    uri = _resolve(settings.mongodb_uri, "MONGODB_URI")
    if not uri:
        raise RuntimeError("MONGODB_URI is not set. Populate .env or grant the secret.")
    return _mongo_client_for(uri)


def knowledge_collection(name: str | None = None):
    db = mongo_client()[settings.mongodb_db]
    return db[name or settings.knowledge_collection]


@lru_cache(maxsize=_CACHE_SIZE)
def _voyage_client_for(api_key: str, base_url: str) -> "voyageai.Client":
    import voyageai

    return voyageai.Client(api_key=api_key, base_url=base_url)


def voyage_client() -> "voyageai.Client":
    api_key = _resolve(settings.voyage_api_key, "VOYAGE_API_KEY")
    if not api_key:
        raise RuntimeError("VOYAGE_API_KEY is not set. Populate .env or grant the secret.")
    base_url = _resolve(settings.voyage_base_url, "VOYAGE_BASE_URL")
    return _voyage_client_for(api_key, base_url)


def _aws_creds() -> tuple[str, str, str]:
    """Region, access key id, secret access key, as they are right now.

    Blank credentials are not an error: boto3 falls back to its standard chain
    (profile / instance role / its own environment reading), which is how a
    deployment using an IAM role rather than static keys is meant to work.
    """
    return (
        _resolve(settings.aws_region, "AWS_REGION") or "us-east-1",
        _resolve(settings.aws_access_key_id, "AWS_ACCESS_KEY_ID"),
        _resolve(settings.aws_secret_access_key, "AWS_SECRET_ACCESS_KEY"),
    )


def _aws_kwargs(region: str, access_key: str, secret_key: str) -> dict:
    """Common boto3 kwargs: region + explicit creds when both are provided.

    boto3 does not read our .env, so any creds set there must be passed explicitly.
    """
    kwargs: dict = {"region_name": region}
    if access_key and secret_key:
        kwargs["aws_access_key_id"] = access_key
        kwargs["aws_secret_access_key"] = secret_key
    return kwargs


@lru_cache(maxsize=_CACHE_SIZE)
def _s3_client_for(region: str, access_key: str, secret_key: str, endpoint_url: str = ""):
    import boto3
    from botocore.config import Config

    # Pin us-east-1 to its regional endpoint. botocore still defaults that one
    # region to the global s3.amazonaws.com, so a bucket there is addressed as
    # <bucket>.s3.amazonaws.com. An egress allow-list that names only the
    # regional hosts then blocks the global host, and every read_span fails. Every other region is already
    # regional, which makes this a no-op outside us-east-1. Widening the
    # allow-list instead would admit the global endpoint, which fronts buckets in
    # every region.
    if endpoint_url:
        # MinIO (or any S3-compatible endpoint) needs an explicit endpoint and
        # path-style addressing (no virtual-host buckets).
        return boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            config=Config(s3={"addressing_style": "path"}),
            **_aws_kwargs(region, access_key, secret_key),
        )
    return boto3.client(
        "s3",
        config=Config(s3={"us_east_1_regional_endpoint": "regional"}),
        **_aws_kwargs(region, access_key, secret_key),
    )


def s3_client():
    return _s3_client_for(*_aws_creds(), _resolve(settings.s3_endpoint_url, "S3_ENDPOINT_URL"))
