# ABOUTME: The OpenAI Agents plugin targets api.openai.com by default and an
# OpenAI-compatible gateway when OPENAI_BASE_URL is set; web search needs the Responses API.

from __future__ import annotations

from agent import provider


def test_default_provider_uses_the_sdk_default(monkeypatch):
    monkeypatch.setattr(provider.settings, "openai_base_url", "")
    monkeypatch.setattr(provider.settings, "openai_api_key", "sk-test")
    plugin = provider.build_openai_agents_plugin()
    assert plugin is not None


def test_gateway_provider_sends_the_key_header_and_uses_chat_completions(monkeypatch):
    captured = {}

    import agents

    class _FakeProvider:
        def __init__(self, *, openai_client, use_responses):
            captured["client"] = openai_client
            captured["use_responses"] = use_responses

    monkeypatch.setattr(agents, "OpenAIProvider", _FakeProvider)
    monkeypatch.setattr(agents, "set_tracing_disabled", lambda disabled: captured.setdefault("tracing_off", disabled))
    monkeypatch.setattr(provider, "OpenAIAgentsPlugin", lambda **kw: kw)
    monkeypatch.setattr(provider.settings, "openai_base_url", "https://gateway.example/openai/v1")
    monkeypatch.setattr(provider.settings, "openai_api_key", "gw-key")
    monkeypatch.setattr(provider.settings, "openai_api_key_header", "api-key")
    monkeypatch.setattr(provider.settings, "openai_use_responses", False)

    kw = provider.build_openai_agents_plugin()

    client = captured["client"]
    assert str(client.base_url).rstrip("/") == "https://gateway.example/openai/v1"
    assert client.default_headers["api-key"] == "gw-key"
    assert captured["use_responses"] is False
    assert captured["tracing_off"] is True
    assert isinstance(kw["model_provider"], _FakeProvider)


def test_without_responses_the_agent_is_told_there_is_no_web_search(monkeypatch):
    from agent import agent_workflow

    monkeypatch.setattr(agent_workflow.settings, "openai_use_responses", False)
    assert "Web search is unavailable" in agent_workflow._instructions()
    monkeypatch.setattr(agent_workflow.settings, "openai_use_responses", True)
    assert "Web search is unavailable" not in agent_workflow._instructions()


def test_env_file_selects_the_dotenv_source(monkeypatch, tmp_path):
    env = tmp_path / ".env.alt"
    env.write_text("S3_BUCKET=from-alt-file\n")
    monkeypatch.setenv("ENV_FILE", str(env))
    monkeypatch.delenv("S3_BUCKET", raising=False)
    import importlib

    import pipeline.config as config

    fresh = importlib.reload(config)
    try:
        assert fresh.Settings().s3_bucket == "from-alt-file"
    finally:
        monkeypatch.delenv("ENV_FILE")
        importlib.reload(config)
