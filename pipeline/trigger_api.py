"""HTTP trigger endpoint that Atlas Stream Processing ($https) calls to start ingestion.

ASP watches sources and POSTs the new object's {bucket, key} here; this starts the
IngestWorkflow. Deploy this reachable by Atlas (public URL / tunnel). Locally, prefer
`trigger_listener.py` (change stream), since ASP cannot reach localhost.

Run:  uv run python -m pipeline.trigger_api
"""

from __future__ import annotations

import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel

from .config import settings
from .models import S3Ref
from .trigger import get_client, start_ingest

app = FastAPI(title="PRA ingest trigger")


class TriggerRequest(BaseModel):
    bucket: str
    key: str


@app.get("/health")
async def health() -> dict:
    return {"ok": True}


@app.post("/ingest-trigger")
async def ingest_trigger(req: TriggerRequest) -> dict:
    ref = S3Ref.make(bucket=req.bucket, key=req.key)
    client = await get_client()
    wf_id = await start_ingest(client, ref)
    return {"started": wf_id, "s3_uri": ref.s3_uri}


def main() -> None:
    uvicorn.run(app, host="0.0.0.0", port=settings.trigger_api_port)


if __name__ == "__main__":
    main()
