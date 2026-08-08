"""HTTP trigger endpoint for direct-from-S3 ingestion.

Two entrypoints, both starting the IngestWorkflow:
  - POST /ingest-event   raw S3/MinIO ObjectCreated event envelope. Used locally by the
    MinIO webhook target and — as the *same* handler code — by the AWS Lambda.
  - POST /ingest-trigger flat {bucket, key} body. A convenience for manual/scripted triggering.

Run:  uv run python -m pipeline.trigger_api
"""

from __future__ import annotations

from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, Request
from pydantic import BaseModel

from .config import settings
from .models import S3Ref
from .trigger import get_client, handle_s3_event, start_ingest


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Connect once at startup and reuse the client for every request.
    app.state.temporal = await get_client()
    yield


app = FastAPI(title="PRA ingest trigger", lifespan=lifespan)


class TriggerRequest(BaseModel):
    bucket: str
    key: str


@app.get("/health")
async def health() -> dict:
    return {"ok": True}


@app.post("/ingest-trigger")
async def ingest_trigger(req: TriggerRequest, request: Request) -> dict:
    # Flat {bucket, key} body — a convenience for manual/scripted triggering.
    ref = S3Ref.make(bucket=req.bucket, key=req.key)
    wf_id = await start_ingest(request.app.state.temporal, ref)
    return {"started": wf_id, "s3_uri": ref.s3_uri}


@app.post("/ingest-event")
async def ingest_event(request: Request) -> dict:
    # Raw S3/MinIO ObjectCreated event envelope — used by the MinIO webhook target and,
    # as the same code, by the AWS Lambda. A TestEvent yields no starts.
    event = await request.json()
    started = await handle_s3_event(request.app.state.temporal, event)
    return {"started": started}


def main() -> None:
    uvicorn.run(app, host="0.0.0.0", port=settings.trigger_api_port)


if __name__ == "__main__":
    main()
