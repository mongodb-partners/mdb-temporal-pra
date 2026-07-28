"""Local dev shim for Atlas Stream Processing.

Watches the `sources` change stream (inserts/replaces written by the MongoDB sink
connector) and starts an IngestWorkflow per object. In production this is replaced by an
Atlas Stream Processing processor that `$https`-POSTs to `pipeline/trigger_api.py`
(ASP cannot reach a local Temporal). See infra/asp/.

Run:  uv run python -m pipeline.trigger_listener
"""

from __future__ import annotations

import asyncio

from .clients import mongo_client
from .config import settings
from .s3util import refs_from_s3_event
from .trigger import get_client, start_ingest


async def main() -> None:
    temporal = await get_client()
    coll = mongo_client()[settings.mongodb_db][settings.src_collection]

    print(f"[trigger-listener] watching {settings.mongodb_db}.{settings.src_collection} "
          f"-> start {settings.temporal_task_queue}/IngestWorkflow")

    # Watch inserts + replaces (sink upserts replace on re-upload). full_document to read the doc.
    pipeline = [{"$match": {"operationType": {"$in": ["insert", "replace", "update"]}}}]
    with coll.watch(pipeline, full_document="updateLookup") as stream:
        while True:
            # stream.next() blocks until the next change; run it off the event loop.
            change = await asyncio.to_thread(stream.next)
            doc = change.get("fullDocument") or {}
            for ref in refs_from_s3_event(doc):
                wf_id = await start_ingest(temporal, ref)
                print(f"[trigger-listener] {ref.s3_uri} -> started {wf_id}")


if __name__ == "__main__":
    asyncio.run(main())
