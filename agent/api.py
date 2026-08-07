"""Deep-agent FastAPI backend.

  POST /query    {query, k?, top_k?}  -> {answer, sources[], ...}   (fixed RAG pipeline)
  POST /research {query}              -> {answer, tool_calls[], ...} (durable agent workflow)
  GET  /health

Run:  uv run python -m agent.api
"""

from __future__ import annotations

import logging
import os
import uuid
from datetime import timedelta

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from temporalio.client import Client
from temporalio.contrib.openai_agents import ModelActivityParameters, OpenAIAgentsPlugin

from infra.create_atlas_index import ensure_atlas_indexes, ensure_collections_and_indexes
from pipeline.config import settings

from .retrieval import ask

app = FastAPI(title="Temporal deep agent")
logger = logging.getLogger(__name__)

# Allow the Vite dev server (and any localhost origin) to call the API.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


class QueryRequest(BaseModel):
    query: str
    k: int = 10
    top_k: int = 5


@app.on_event("startup")
async def ensure_index_on_startup() -> None:
    try:
        boot = ensure_collections_and_indexes()
        if boot["collections"]:
            logger.info("Created MongoDB collections at startup: %s", ", ".join(boot["collections"]))
        if boot["indexes"]:
            logger.info("Ensured MongoDB indexes at startup: %s", ", ".join(boot["indexes"]))

        created = ensure_atlas_indexes(collection=settings.knowledge_collection)
        if created:
            logger.info("Created Atlas Search index at startup: %s", ", ".join(created))
        else:
            logger.info("Atlas Search index already present for '%s'", settings.knowledge_collection)
    except Exception:
        logger.exception("Failed to bootstrap MongoDB collections/indexes on startup")


@app.get("/health")
async def health() -> dict:
    return {"ok": True, "answer_model": settings.answer_model}


@app.post("/query")
async def query(req: QueryRequest) -> dict:
    return ask(req.query, k=req.k, top_k=req.top_k)


_agent_client: Client | None = None


async def _get_agent_client() -> Client:
    """Temporal client configured with the OpenAI Agents plugin — matches the worker's
    data converter so agent-workflow results decode correctly."""
    global _agent_client
    if _agent_client is None:
        os.environ.setdefault("OPENAI_API_KEY", settings.openai_api_key)
        _agent_client = await Client.connect(
            settings.temporal_address,
            namespace=settings.temporal_namespace,
            plugins=[
                OpenAIAgentsPlugin(
                    model_params=ModelActivityParameters(
                        start_to_close_timeout=timedelta(seconds=60)
                    )
                )
            ],
        )
    return _agent_client


class ResearchRequest(BaseModel):
    query: str


@app.post("/research")
async def research(req: ResearchRequest) -> dict:
    """Durable research agent: starts the DeepResearchAgent workflow (OpenAI Agents SDK on
    Temporal) and returns its cited answer. Watch the reasoning trajectory in the Temporal UI."""
    if not settings.openai_api_key:
        raise HTTPException(
            status_code=503,
            detail="Research agent unavailable: set OPENAI_API_KEY in .env and restart the worker.",
        )
    client = await _get_agent_client()
    wf_id = f"agent-{uuid.uuid4().hex[:16]}"
    result = await client.execute_workflow(
        "DeepResearchAgent",
        req.query,
        id=wf_id,
        task_queue=settings.temporal_task_queue,
    )
    return {"workflow_id": wf_id, **result}


def main() -> None:
    uvicorn.run(app, host="0.0.0.0", port=settings.agent_api_port)


if __name__ == "__main__":
    main()
