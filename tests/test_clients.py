# Covers how pipeline/clients.py builds the Voyage client: which endpoint it
# targets, and that credentials are read at call time rather than frozen at import.

from __future__ import annotations

import pytest
import voyageai

from pipeline import clients


@pytest.fixture
def built(monkeypatch):
    """Record the kwargs voyage_client() constructs its client with."""
    clients._voyage_client_for.cache_clear()
    calls: list[dict] = []

    class _FakeClient:
        def __init__(self, **kwargs):
            calls.append(kwargs)

    monkeypatch.setattr(voyageai, "Client", _FakeClient)
    yield calls
    clients._voyage_client_for.cache_clear()


def test_embeddings_go_to_the_mongodb_hosted_endpoint(built, monkeypatch):
    monkeypatch.setattr(clients.settings, "voyage_api_key", "al-test-key")
    monkeypatch.setattr(clients.settings, "voyage_base_url", "https://ai.mongodb.com/v1")
    clients.voyage_client()
    assert built[0]["base_url"] == "https://ai.mongodb.com/v1"
    assert built[0]["api_key"] == "al-test-key"


def test_the_endpoint_is_explicit_not_inferred_from_the_key_prefix(built, monkeypatch):
    """The SDK would pick api.voyageai.com for a non ``al-`` key. We do not let it."""
    monkeypatch.setattr(clients.settings, "voyage_api_key", "pa-not-a-mongodb-key")
    monkeypatch.setattr(clients.settings, "voyage_base_url", "https://ai.mongodb.com/v1")
    clients.voyage_client()
    assert built[0]["base_url"] == "https://ai.mongodb.com/v1"


def test_a_missing_key_is_an_error_not_an_anonymous_call(built, monkeypatch):
    monkeypatch.setattr(clients.settings, "voyage_api_key", "")
    monkeypatch.delenv("VOYAGE_API_KEY", raising=False)
    with pytest.raises(RuntimeError, match="VOYAGE_API_KEY"):
        clients.voyage_client()
    assert built == []


def test_the_key_is_read_from_the_environment_when_settings_are_blank(built, monkeypatch):
    """The sandboxed-runtime case: secrets can arrive after pipeline.config is imported."""
    monkeypatch.setattr(clients.settings, "voyage_api_key", "")
    monkeypatch.setattr(clients.settings, "voyage_base_url", "https://ai.mongodb.com/v1")
    monkeypatch.setenv("VOYAGE_API_KEY", "al-late-key")
    clients.voyage_client()
    assert built[0]["api_key"] == "al-late-key"


def test_two_keys_do_not_share_one_cached_client(built, monkeypatch):
    monkeypatch.setattr(clients.settings, "voyage_base_url", "https://ai.mongodb.com/v1")
    monkeypatch.setattr(clients.settings, "voyage_api_key", "al-first")
    clients.voyage_client()
    monkeypatch.setattr(clients.settings, "voyage_api_key", "al-second")
    clients.voyage_client()
    assert [call["api_key"] for call in built] == ["al-first", "al-second"]


def test_an_s3_endpoint_selects_path_style_addressing(monkeypatch):
    """Local MinIO: S3_ENDPOINT_URL routes boto3 there with path-style bucket URLs."""
    monkeypatch.setattr(clients.settings, "s3_endpoint_url", "http://localhost:9000")
    clients._s3_client_for.cache_clear()
    s3 = clients.s3_client()
    assert s3.meta.endpoint_url == "http://localhost:9000"
    assert s3.meta.config.s3["addressing_style"] == "path"


def test_no_s3_endpoint_keeps_aws(monkeypatch):
    monkeypatch.setattr(clients.settings, "s3_endpoint_url", "")
    monkeypatch.delenv("S3_ENDPOINT_URL", raising=False)
    clients._s3_client_for.cache_clear()
    assert "amazonaws.com" in clients.s3_client().meta.endpoint_url
