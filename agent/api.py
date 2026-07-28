"""Deep-agent FastAPI backend.

  POST /query {query, k?, top_k?}  -> {answer, sources[], ...}
  GET  /health

Run:  uv run python -m agent.api
"""

from __future__ import annotations

import logging

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

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


def main() -> None:
    uvicorn.run(app, host="0.0.0.0", port=settings.agent_api_port)


if __name__ == "__main__":
    main()
