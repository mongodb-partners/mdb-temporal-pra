# ABOUTME: Builds the OpenAI Agents plugin shared by the worker and the agent API, so both use
# the same model provider (OpenAI directly, or any OpenAI-compatible gateway via OPENAI_BASE_URL).

from __future__ import annotations

import os
from datetime import timedelta

from temporalio.contrib.openai_agents import ModelActivityParameters, OpenAIAgentsPlugin

from pipeline.config import settings

# Model calls on larger reasoning models regularly exceed a minute.
MODEL_CALL_TIMEOUT = timedelta(seconds=120)


def build_openai_agents_plugin() -> OpenAIAgentsPlugin:
    """The plugin with a model provider matching the configured OpenAI endpoint.

    With no OPENAI_BASE_URL this is the SDK default (api.openai.com, Responses API).
    With one, the client points there; OPENAI_API_KEY_HEADER additionally sends the key
    in that header (e.g. ``api-key`` for Azure API Management style gateways), and
    OPENAI_USE_RESPONSES=false switches to Chat Completions.
    """
    os.environ.setdefault("OPENAI_API_KEY", settings.openai_api_key)
    params = ModelActivityParameters(start_to_close_timeout=MODEL_CALL_TIMEOUT)
    if not settings.openai_base_url:
        return OpenAIAgentsPlugin(model_params=params)

    from agents import OpenAIProvider, set_tracing_disabled
    from openai import AsyncOpenAI

    headers = (
        {settings.openai_api_key_header: settings.openai_api_key}
        if settings.openai_api_key_header
        else None
    )
    client = AsyncOpenAI(
        api_key=settings.openai_api_key,
        base_url=settings.openai_base_url,
        default_headers=headers,
    )
    # Traces export to api.openai.com with the same key, which a gateway key cannot do.
    set_tracing_disabled(True)
    return OpenAIAgentsPlugin(
        model_params=params,
        model_provider=OpenAIProvider(
            openai_client=client, use_responses=settings.openai_use_responses
        ),
    )
