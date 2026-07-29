# Low-Level Design

**MongoDB × Temporal Partner Reference Architecture**
Version: 1.0 · Branch: `pipeline-impl` · Date: 2026-07-29

---

## Table of contents

- [1. Overview](#1-overview)
- [2. Component inventory](#2-component-inventory)
- [3. Data contracts](#3-data-contracts)
- [4. End-to-end data flow](#4-end-to-end-data-flow)
- [5. Workflow design](#5-workflow-design)
  - [5.1 IngestWorkflow](#51-ingestworkflow)
  - [5.2 BackfillWorkflow](#52-backfillworkflow)
- [6. Activity design](#6-activity-design)
- [7. Extractor system](#7-extractor-system)
- [8. MongoDB data model](#8-mongodb-data-model)
- [9. Atlas Vector Search index](#9-atlas-vector-search-index)
- [10. Trigger layer](#10-trigger-layer)
- [11. Deep agent](#11-deep-agent)
- [12. Configuration reference](#12-configuration-reference)
- [13. Scaling to multiple data sources](#13-scaling-to-multiple-data-sources)
- [14. Scaling to multiple data types](#14-scaling-to-multiple-data-types)
- [15. Extension patterns and future sources](#15-extension-patterns-and-future-sources)

---

## 1. Overview

This implementation is a **durable, change-driven RAG ingestion pipeline** built on three services:

| Layer               | Service                        | Role                                                                          |
| ------------------- | ------------------------------ | ----------------------------------------------------------------------------- |
| Streaming ingestion | Kafka + MongoDB Sink Connector | Decouple sources from processing; land raw events into Atlas                  |
| Orchestration       | Temporal                       | Chunk → embed → index, with per-step resumability and automatic retry         |
| Storage + retrieval | MongoDB Atlas                  | Single store for raw records, staged chunks, embedded knowledge, agent memory |

The implementation is intentionally **source-agnostic**. The `S3Ref` / `RawRecord` data contracts
and the extractor factory are the only points that need extension when adding a new source or file
type — the workflow, activities, and Atlas storage layer are unchanged.

---

## 2. Component inventory

```
pipeline/
├── worker.py               ← Temporal worker (hosts all workflows + activities)
├── config.py               ← Pydantic settings (env-driven, lru_cache singleton)
├── models.py               ← Data contracts: S3Ref, RawRecord, Chunk, EmbeddedChunk
├── clients.py              ← Lazy singletons: Temporal, MongoDB, Voyage, S3
├── trigger_listener.py     ← Local dev: Atlas change-stream → start IngestWorkflow
├── trigger_api.py          ← Production: ASP $https POST → start IngestWorkflow
├── trigger.py              ← Shared trigger logic (client connect + workflow start)
├── search_index.py         ← Idempotent Atlas Vector Search index management
├── config_store.py         ← Active collection/index pointer (cutover)
├── s3util.py               ← Parse MinIO/S3 event → S3Ref list
├── seed.py                 ← Dev utility: upload a local file to MinIO
├── seed_repo.py            ← Dev utility: bulk-upload a markdown docs repo
├── cutover.py              ← Flip active collection pointer in temporal_config
├── retrieval.py            ← Shared Atlas vector search (used by agent)
├── workflows/
│   ├── ingest_workflow.py  ← IngestWorkflow: 3-stage, per-chunk checkpointing
│   └── backfill_workflow.py← BackfillWorkflow: paginated re-embed with continue-as-new
├── activities/
│   ├── ingest.py           ← fetch_and_stage_chunks, embed_staged_chunk, index_document
│   └── backfill.py         ← read_source_batch, reembed_and_write, ensure_target_index
└── extractors/
    ├── base.py             ← Extractor ABC + window() splitter + RawChunk
    ├── factory.py          ← get_extractor(): extension → MIME → TextExtractor
    ├── markdown.py         ← Section-aware markdown extractor
    ├── pdf.py              ← PyPDF page extractor
    ├── csv_ext.py          ← Row-group extractor
    └── text.py             ← Plain text window splitter (fallback)
```

---

## 3. Data contracts

Defined in `pipeline/models.py`. All contracts are plain Python `@dataclass`s — they serialize
cleanly through Temporal's default JSON data converter and as Kafka message payloads.

### S3Ref

The atomic unit flowing into `IngestWorkflow`. Identifies a single object in S3 (or MinIO).

```python
@dataclass
class S3Ref:
    bucket: str          # S3 bucket name
    key: str             # object key (path within bucket)
    s3_uri: str          # "s3://{bucket}/{key}" — canonical identifier
    etag: str            # object ETag for version tracking
    size: int            # object size in bytes
    content_type: str    # MIME type hint (used by extractor factory)
```

### RawRecord

Source-agnostic envelope for the raw Kafka topic. `source` identifies the producer type; other
sources (RDBMS row, IoT payload, webhook) set `payload` instead of `ref`.

```python
@dataclass
class RawRecord:
    source: str          # "s3" | "rdbms" | "iot" | "webhook" | ...
    doc_id: str          # stable sha1-derived identifier for this document
    ref: S3Ref | None    # populated for S3/MinIO sources
    payload: str | None  # populated for inline-text sources
    metadata: dict       # source-specific metadata (table name, topic, tags, ...)
```

### Chunk / EmbeddedChunk

```python
@dataclass
class Chunk:
    doc_id: str          # parent document identifier
    chunk_id: str        # "{doc_id}:{ordinal}" — deterministic, workflow-resumable
    ordinal: int         # position within the document
    text: str            # chunk text (1200 chars default, configurable)
    content_hash: str    # sha256 of this chunk's text (dedupe within a document)
    doc_content_hash: str# sha256 of the whole source object (version dedupe)
    source_uri: str      # "s3://" URI for citation
    metadata: dict       # extractor-specific (heading, page, row range, ...)

@dataclass
class EmbeddedChunk(Chunk):
    embedding: list[float]  # 1024-dim Voyage vector
    model: str              # model name used for embedding
    dim: int                # embedding dimensionality
```

### Document identity and deduplication

```python
def doc_id_for_uri(uri: str) -> str:
    # SHA-1 of the URI → 16-char hex. Stable across re-uploads of the same key.
    return hashlib.sha1(uri.encode()).hexdigest()[:16]

def sha256_hex(data: bytes | str) -> str:
    # Used for content-hash dedupe: same bytes → skip re-embed.
    return hashlib.sha256(data).hexdigest()
```

`fetch_and_stage_chunks` short-circuits if `knowledge` already contains a doc with the same
`doc_id` and `doc_content_hash` — identical bytes at the same key are never re-embedded.

---

## 4. End-to-end data flow

```
[Source]
   │  upload / write
   ▼
[MinIO / S3]  ──── native S3 event ───▶  [Kafka: s3-events topic]
                                                │
                                         Kafka Connect
                                     (mongo-sink connector)
                                                │ upsert by key
                                                ▼
                                    [Atlas: temporal.sources]
                                                │
                                         change stream
                                                │
                              ┌─────────────────┴─────────────────┐
                              │ Dev: trigger_listener.py           │
                              │ Prod: ASP $https → trigger_api.py  │
                              └─────────────────┬─────────────────┘
                                                │ start IngestWorkflow(S3Ref)
                                                ▼
                          ┌─────────────────────────────────────┐
                          │  IngestWorkflow (Temporal)          │
                          │                                     │
                          │  Activity 1: fetch_and_stage_chunks │
                          │    • GET s3://{bucket}/{key}        │
                          │    • sha256 dedupe check            │
                          │    • factory extractor → chunks     │
                          │    • INSERT → temporal.chunks_staging│
                          │                                     │
                          │  Activity 2..N: embed_staged_chunk  │
                          │    • one activity per chunk         │
                          │    • idempotent (skip if embedded)  │
                          │    • Voyage embed(text) → vector    │
                          │    • UPDATE chunks_staging          │
                          │                                     │
                          │  Activity N+1: index_document       │
                          │    • UPSERT chunks → knowledge      │
                          │    • DELETE stale ordinals          │
                          │    • ensure_vector_index (idempotent│
                          │    • DELETE chunks_staging (cleanup)│
                          └─────────────────────────────────────┘
                                                │
                                                ▼
                                  [Atlas: temporal.knowledge]
                                  [Atlas Vector Search Index]
                                                │
                                     agent vector search
                                                ▼
                                   [FastAPI /query → React UI]
```

---

## 5. Workflow design

### 5.1 IngestWorkflow

**File:** `pipeline/workflows/ingest_workflow.py`

**Input:** `S3Ref`, optional `target_collection`

**Stages:**

| #   | Activity                             | Timeout | Retries                  | Idempotent                                              |
| --- | ------------------------------------ | ------- | ------------------------ | ------------------------------------------------------- |
| 1   | `fetch_and_stage_chunks`             | 5 min   | 5                        | Yes — short-circuits on matching `doc_content_hash`     |
| 2…N | `embed_staged_chunk` (one per chunk) | 2 min   | 6 (exp backoff, max 30s) | Yes — skips if `status == "embedded"` and model matches |
| N+1 | `index_document`                     | 2 min   | 6                        | Yes — upsert by `chunk_id`, prune stale ordinals        |

**Resumability guarantee:** Each chunk is a separate activity. If the worker crashes between
chunk `i` and chunk `i+1`, Temporal replays the workflow history and skips all already-embedded
chunks (the `status == "embedded"` check in `embed_staged_chunk`). Only the in-flight chunk is
retried — no re-embedding of completed work.

**Update-in-place:** Re-uploading the same S3 key with different content produces a new
`doc_content_hash`. `fetch_and_stage_chunks` clears stale staging rows, the workflow re-embeds
all new chunks, and `index_document` upserts them into `knowledge` and prunes chunks whose
`ordinal >= new_n` (stale chunks from a previously longer version).

**Workflow ID:** derived from `S3Ref.s3_uri` — duplicate triggers for the same key within the
workflow's run window are deduplicated by Temporal.

### 5.2 BackfillWorkflow

**File:** `pipeline/workflows/backfill_workflow.py`

**Trigger:** model upgrade requiring a dimension change (e.g. `voyage-3.5` 1024-dim → new model).

**Design pattern:** `continue_as_new` — the workflow re-starts itself with a cursor (`after_id`)
after each batch. This keeps Temporal workflow history bounded regardless of collection size.

**Stages per batch:**

| Activity                      | Description                                                                                     |
| ----------------------------- | ----------------------------------------------------------------------------------------------- |
| `ensure_target_index`         | Create the vector index on the green collection at the new dimension (once, before first batch) |
| `read_source_batch`           | Paginated read of the active collection, 50 chunks at a time, sorted by `_id`                   |
| `reembed_and_write` (per doc) | Re-embed with new model, upsert into target collection                                          |

**Blue/green cutover:** `BackfillWorkflow` writes exclusively to `knowledge_v2` (green). The
active collection pointer in `temporal_config` stays on `knowledge` (blue) until the operator runs
`make cutover TO=knowledge_v2`. The agent reads the active pointer on every query — no restart
needed.

```
knowledge (blue)  ──read──▶  BackfillWorkflow  ──write──▶  knowledge_v2 (green)
                                                                │
                                                          make cutover
                                                                │
                                                    temporal_config.active = "knowledge_v2"
```

---

## 6. Activity design

All activities follow three design rules:

1. **Idempotent** — safe to retry at any point; re-running a completed activity produces the same
   result and no duplicate writes.
2. **Heartbeating** — long-running activities (`embed_staged_chunk`, `reembed_and_write`) call
   `activity.heartbeat()` so Temporal can detect stalled activities within the `heartbeat_timeout`
   (30s).
3. **Sync execution** — all activities use standard `pymongo`, `voyageai`, and `boto3` clients
   (synchronous). The worker runs them in a `ThreadPoolExecutor(max_workers=16)`, keeping the
   Temporal event loop free.

### Activity retry policy (embed activities)

```python
RetryPolicy(
    initial_interval=timedelta(seconds=2),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=30),
    maximum_attempts=6,
)
```

Voyage AI rate-limit errors (429) and transient network errors are handled transparently.

---

## 7. Extractor system

**File:** `pipeline/extractors/`

The extractor factory decouples file-type handling from the workflow. Adding a new format requires
only a new extractor class — no workflow or activity changes.

### Base class

```python
class Extractor(ABC):
    name: str                                        # identifies extractor in metadata
    chunk_size: int                                  # configurable via settings
    chunk_overlap: int                               # configurable via settings

    @abstractmethod
    def pieces(self, body: bytes) -> list[tuple[str, dict]]:
        """Return (text, meta) pairs; ordinal assignment handled by base."""

    def chunk(self, body: bytes) -> list[RawChunk]:
        """Call pieces(), assign ordinals, drop blanks."""
```

### Chunking strategy

The base provides a `window()` splitter — a character-window with overlap:

```python
def window(text: str, size: int, overlap: int) -> list[str]:
    step = max(1, size - overlap)
    return [text[i : i + size] for i in range(0, len(text), step) if text[i : i + size].strip()]
```

Default: `chunk_size=1200`, `chunk_overlap=150` (configurable in `.env`).

### Registered extractors

| Extension / MIME                       | Extractor           | Chunking strategy                                                       |
| -------------------------------------- | ------------------- | ----------------------------------------------------------------------- |
| `.md`, `.markdown` / `text/markdown`   | `MarkdownExtractor` | Section-aware: splits on `#` headings, then window-splits long sections |
| `.pdf` / `application/pdf`             | `PdfExtractor`      | PyPDF page-by-page, window-splits long pages                            |
| `.csv` / `text/csv`, `application/csv` | `CsvExtractor`      | Row-group batching                                                      |
| anything else                          | `TextExtractor`     | Plain window split (fallback)                                           |

### Factory resolution

```python
def get_extractor(key: str, content_type: str = "") -> Extractor:
    ext = key.rsplit(".", 1)[-1].lower()
    cls = _BY_EXT.get(ext) \
       or _BY_MIME.get(content_type.split(";")[0].strip()) \
       or TextExtractor
    return cls(chunk_size=settings.chunk_size, chunk_overlap=settings.chunk_overlap)
```

Resolution order: **file extension → MIME type → TextExtractor fallback**.

---

## 8. MongoDB data model

Database: `temporal` (configurable via `MONGODB_DB`)

### `sources` collection

Written by the Kafka Sink Connector. One document per S3 object key (upsert by key).

```json
{
  "_id": "ObjectId",
  "Records": [
    {
      "s3": {
        "bucket": { "name": "temporal-datasources" },
        "object": {
          "key": "docs%2Fmy-doc.pdf",
          "size": 45123,
          "eTag": "abc123"
        }
      }
    }
  ]
}
```

The trigger layer (`s3util.py`) parses `Records[*].s3` → `S3Ref`, URL-decoding the key.

### `chunks_staging` collection

Transient. Created by `fetch_and_stage_chunks`, updated by `embed_staged_chunk`, deleted by
`index_document`. Survives worker crashes — the workflow resumes from the correct chunk.

```json
{
  "doc_id": "a3f9c1d2e4b5f678",
  "chunk_id": "a3f9c1d2e4b5f678:0",
  "ordinal": 0,
  "text": "...",
  "content_hash": "sha256hex",
  "doc_content_hash": "sha256hex",
  "source_uri": "s3://temporal-datasources/docs/my-doc.pdf",
  "metadata": { "extractor": "pdf", "page": 1 },
  "status": "pending | embedded",
  "embedding": [0.123, ...],
  "model": "voyage-3.5",
  "dim": 1024
}
```

Indexes needed: `{ doc_id: 1 }`, `{ chunk_id: 1 }` (unique), `{ doc_id: 1, status: 1 }`.

### `knowledge` collection (active, blue)

The searchable store. Upserted by `index_document` using `chunk_id` as the upsert key.

```json
{
  "_id": "ObjectId",
  "doc_id": "a3f9c1d2e4b5f678",
  "chunk_id": "a3f9c1d2e4b5f678:0",
  "ordinal": 0,
  "text": "...",
  "content_hash": "sha256hex",
  "doc_content_hash": "sha256hex",
  "embedding": [0.123, ...],    ← 1024-dim Voyage vector
  "model": "voyage-3.5",
  "dim": 1024,
  "source_uri": "s3://temporal-datasources/docs/my-doc.pdf",
  "metadata": { "extractor": "pdf", "page": 1 }
}
```

### `knowledge_v2` collection (green, backfill target)

Same schema as `knowledge`. Populated by `BackfillWorkflow`. Becomes active after `make cutover`.

### `temporal_config` collection

Single document: the active collection/index pointer. Read by the agent on every query.

```json
{
  "_id": "active",
  "collection": "knowledge",
  "index": "temporalai_search_index"
}
```

### `agent_memory` collection

Written by the deep agent after every query. Stores conversation history, citations, and
synthesized answers.

```json
{
  "_id": "ObjectId",
  "query": "what does Temporal own in this architecture?",
  "answer": "...",
  "sources": [{ "chunk_id": "...", "text": "...", "score": 0.92 }],
  "model": "voyage-3.5",
  "timestamp": "ISODate"
}
```

---

## 9. Atlas Vector Search index

**File:** `pipeline/search_index.py`

Index definition (created idempotently by `ensure_vector_index` at the end of every
`index_document` activity):

```json
{
  "fields": [
    {
      "type": "vector",
      "path": "embedding",
      "numDimensions": 1024,
      "similarity": "cosine"
    },
    { "type": "filter", "path": "doc_id" },
    { "type": "filter", "path": "source_uri" }
  ]
}
```

The `filter` fields allow the agent to scope vector search to a specific document or source URI
without a full collection scan.

`ensure_vector_index` is idempotent — it lists existing indexes and skips creation if the index
already exists. The `BackfillWorkflow` calls `ensure_target_index` once before writing to the green
collection so the new-dimension index is ready before any chunk is written.

---

## 10. Trigger layer

Two implementations of the same contract: watch `sources` → parse `S3Ref` → start
`IngestWorkflow`.

### Local dev — `trigger_listener.py`

Uses PyMongo's `collection.watch()` (change stream) to watch `temporal.sources` for inserts,
replaces, and updates. Calls `start_ingest(temporal_client, ref)` directly.

```python
pipeline = [{"$match": {"operationType": {"$in": ["insert", "replace", "update"]}}}]
with coll.watch(pipeline, full_document="updateLookup") as stream:
    change = await asyncio.to_thread(stream.next)
    for ref in refs_from_s3_event(change["fullDocument"]):
        await start_ingest(temporal, ref)
```

### Production — ASP + `trigger_api.py`

Atlas Stream Processing watches `temporal.sources` and `$https`-POSTs each new record to
`POST /ingest-trigger` on `trigger_api.py`. The endpoint parses the body into an `S3Ref` and
calls `start_ingest`.

```python
@app.post("/ingest-trigger")
async def ingest_trigger(body: dict):
    ref = S3Ref.make(bucket=body["bucket"], key=urllib.parse.unquote_plus(body["key"]))
    wf_id = await start_ingest(await get_client(), ref)
    return {"workflow_id": wf_id}
```

**Serverless option:** the `$https` POST can target an AWS Lambda function running a
[Temporal Serverless Worker](https://temporal.io/blog/introducing-temporal-serverless-workers-deploy-temporal-workers-to-aws-lambda).
ASP fires the trigger → Lambda cold-starts a worker → `IngestWorkflow` runs to completion with
full Temporal durability. No always-on worker process required.

---

## 11. Deep agent

**Files:** `agent/api.py`, `agent/retrieval.py`, `agent/ui/`

### Query flow

```
React UI  ──POST /query──▶  FastAPI  ──▶  read temporal_config (active collection)
                                     ──▶  Voyage embed(query)
                                     ──▶  Atlas $vectorSearch (top-k chunks)
                                     ──▶  Voyage rerank(query, chunks)
                                     ──▶  Anthropic Claude (RAG synthesis)
                                     ──▶  INSERT agent_memory
                                     ──▶  streaming SSE response to UI
```

### Vector search query

```python
pipeline = [
    {
        "$vectorSearch": {
            "index": active_index,
            "path": "embedding",
            "queryVector": query_embedding,
            "numCandidates": 150,
            "limit": 10,
        }
    },
    { "$project": { "text": 1, "source_uri": 1, "metadata": 1, "score": { "$meta": "vectorSearchScore" } } }
]
```

### Reranking

The top-10 vector search results are passed to Voyage `rerank-2.5` with the original query.
The reranked top-5 are used as context for Claude synthesis.

---

## 12. Configuration reference

All settings live in `.env` (loaded by `pipeline/config.py` via Pydantic Settings).

| Variable                  | Default                 | Description                                 |
| ------------------------- | ----------------------- | ------------------------------------------- |
| `MONGODB_URI`             | —                       | Atlas connection string (required)          |
| `MONGODB_DB`              | `temporal`              | Database name                               |
| `SRC_COLLECTION`          | `sources`               | Kafka sink landing collection               |
| `CHUNKS_COLLECTION`       | `chunks_staging`        | Transient staging between workflow stages   |
| `KNOWLEDGE_COLLECTION`    | `knowledge`             | Active (blue) embedded knowledge store      |
| `KNOWLEDGE_V2_COLLECTION` | `knowledge_v2`          | Green backfill target                       |
| `CONFIG_COLLECTION`       | `temporal_config`       | Active pointer document                     |
| `MEMORY_COLLECTION`       | `agent_memory`          | Agent write-back                            |
| `VOYAGE_MODEL`            | `voyage-3.5`            | Embedding model (1024-dim)                  |
| `VOYAGE_RERANK_MODEL`     | `rerank-2.5`            | Reranking model                             |
| `EMBED_DIM`               | `1024`                  | Embedding dimensionality (must match model) |
| `ANSWER_MODEL`            | `claude-sonnet-4-5`     | Claude model for synthesis                  |
| `CHUNK_SIZE`              | `1200`                  | Maximum characters per chunk                |
| `CHUNK_OVERLAP`           | `150`                   | Overlap characters between adjacent chunks  |
| `TEMPORAL_ADDRESS`        | `localhost:7233`        | Temporal server address                     |
| `TEMPORAL_TASK_QUEUE`     | `temporal-pipeline`     | Worker task queue                           |
| `KAFKA_BOOTSTRAP`         | `localhost:29092`       | Kafka broker bootstrap servers              |
| `S3_ENDPOINT_URL`         | `http://localhost:9000` | MinIO endpoint (blank = real AWS S3)        |
| `S3_BUCKET`               | `temporal-datasources`  | Source bucket                               |

---

## 13. Scaling to multiple data sources

The architecture is designed with a **source-agnostic Kafka boundary**. Any system that can
produce records to a Kafka topic can feed the pipeline without modifying workflows or activities.

### Current source: MinIO / S3

```
S3 upload → native S3 event notification → Kafka (s3-events topic)
         → MongoDB Sink Connector → sources → change stream → IngestWorkflow(S3Ref)
```

### Adding a new source: general pattern

Every new source follows the same three-step pattern:

```
Step 1:  Produce an event to Kafka (using a connector or producer)
Step 2:  MongoDB Sink Connector (or a custom consumer) lands it in `temporal.sources`
Step 3:  trigger_listener / ASP detects the change → start IngestWorkflow
```

The `RawRecord.source` field and `S3Ref`/`payload` union in `models.py` already accommodate
non-S3 sources with inline payloads. Extend `s3util.py` (or add a parallel parser) to extract
the relevant fields for the new source type.

### Source extension examples

#### RDBMS (MySQL / Postgres via Debezium)

```
MySQL binlog → Debezium Kafka connector → Kafka (cdc-events topic)
           → Custom consumer OR Kafka Connect JDBC sink → temporal.sources
           → trigger_listener → IngestWorkflow(RawRecord{source="rdbms", payload=row_json})
```

The `fetch_and_stage_chunks` activity inspects `ref.source` to decide how to retrieve the
object body. For RDBMS, the row JSON is inlined in `payload` — no S3 fetch required. Add a
branch in `fetch_and_stage_chunks`:

```python
if ref.source == "rdbms":
    body = ref.payload.encode()
    content_type = "application/json"
else:
    obj = s3_client().get_object(Bucket=ref.bucket, Key=ref.key)
    body = obj["Body"].read()
```

#### IoT / time-series (MQTT / Kafka native)

```
IoT device → MQTT broker → Kafka MQTT connector → Kafka (iot-events topic)
           → MongoDB Sink Connector → temporal.sources
           → trigger_listener → IngestWorkflow(RawRecord{source="iot", payload=json})
```

IoT payloads are typically small JSON blobs. A `JsonExtractor` (see §14) handles them directly.
Batch multiple readings into a single `RawRecord` using a tumbling window in ASP before writing
to `sources` to reduce workflow starts.

#### MongoDB change stream (existing Atlas data)

```
Atlas collection (operational) → Atlas Stream Processing change stream
                               → $https POST to trigger_api.py / ASP processor
                               → IngestWorkflow(RawRecord{source="mongodb", payload=doc_json})
```

This path uses Atlas Stream Processing natively without Kafka. The `$project` stage in the ASP
pipeline extracts the relevant fields and posts them directly to the trigger endpoint.

#### Webhook / HTTP API

```
External system (Notion, Confluence, GitHub) → webhook POST → trigger_api.py /ingest-trigger
                                             → IngestWorkflow(S3Ref or RawRecord)
```

For large files: the webhook handler uploads the content to S3 first, then constructs an
`S3Ref`. For small content (< 1 MB): inline as `RawRecord.payload`.

#### AWS SQS (production S3 events)

The config already supports SQS-driven S3 events:

```python
# config.py
sqs_queue_url: str = ""
s3_source: str = "auto"  # auto | minio | sqs | poll

def resolved_source(self) -> str:
    if self.s3_endpoint_url: return "minio"
    if self.sqs_queue_url: return "sqs"
    return "poll"
```

Set `SQS_QUEUE_URL` in `.env` to switch from MinIO to real SQS-driven S3 events.

### Multi-source worker scaling

The Temporal worker is stateless. Scale horizontally by running additional worker processes
pointing at the same task queue:

```bash
# Start N additional workers (each handles IngestWorkflow + BackfillWorkflow)
uv run python -m pipeline.worker &   # worker 1
uv run python -m pipeline.worker &   # worker 2
uv run python -m pipeline.worker &   # worker N
```

Temporal distributes workflow tasks across all available workers automatically. Per-worker
concurrency is set by `ThreadPoolExecutor(max_workers=16)` in `worker.py` — tune to the
embedding API rate limit.

---

## 14. Scaling to multiple data types

The extractor factory (`pipeline/extractors/factory.py`) is the sole extension point for new
file or data types. The workflow and all activities are type-agnostic — they receive `bytes` and
call `get_extractor(key, content_type).chunk(body)`.

### Adding a new extractor

1. Create `pipeline/extractors/my_format.py`:

```python
from .base import Extractor, RawChunk, window

class MyFormatExtractor(Extractor):
    name = "my_format"

    def pieces(self, body: bytes) -> list[tuple[str, dict]]:
        # Parse body, return list of (text_segment, metadata_dict)
        results = []
        for section in parse_my_format(body):
            for window_text in window(section.text, self.chunk_size, self.chunk_overlap):
                results.append((window_text, {"section": section.title}))
        return results
```

2. Register in `pipeline/extractors/factory.py`:

```python
from .my_format import MyFormatExtractor

_BY_EXT["myext"] = MyFormatExtractor
_BY_MIME["application/x-my-format"] = MyFormatExtractor
```

3. No changes required to workflows, activities, or the Atlas data model.

### Current extractors and their chunking strategies

| Extractor           | `pieces()` strategy                                          | Metadata emitted       |
| ------------------- | ------------------------------------------------------------ | ---------------------- |
| `MarkdownExtractor` | Split on `#`/`##`/`###` headings; window-split long sections | `heading`, `level`     |
| `PdfExtractor`      | PyPDF page iteration; window-split long pages                | `page`                 |
| `CsvExtractor`      | Group rows into batches; emit each batch as one chunk        | `row_start`, `row_end` |
| `TextExtractor`     | Single `window()` pass over the entire body                  | —                      |

### Planned extractor extensions

| Format                     | Notes                                                                  |
| -------------------------- | ---------------------------------------------------------------------- |
| `JsonExtractor`            | Flatten nested JSON; embed per top-level object or configurable depth  |
| `HtmlExtractor`            | Strip tags, split on semantic blocks (`<article>`, `<section>`, `<p>`) |
| `DocxExtractor`            | python-docx paragraph extraction                                       |
| `XlsxExtractor`            | openpyxl sheet-to-row-group batching                                   |
| `AudioTranscriptExtractor` | Accept a transcript JSON (from Whisper/AWS Transcribe); treat as text  |
| `SqlResultExtractor`       | Accept a JSON array of rows from an RDBMS query; one chunk per N rows  |

### Chunking parameter tuning

| Use case                             | Recommended `CHUNK_SIZE` | Recommended `CHUNK_OVERLAP`     |
| ------------------------------------ | ------------------------ | ------------------------------- |
| Long narrative docs (PDF, markdown)  | 1200                     | 150                             |
| Short structured records (CSV, JSON) | 512                      | 64                              |
| Code files                           | 800                      | 200 (preserve function context) |
| IoT telemetry batches                | 400                      | 0 (batches are atomic)          |

---

## 15. Extension patterns and future sources

### Multi-tenant / multi-database

Route different sources to different Atlas databases or collections by parameterizing
`IngestWorkflow(ref, target_collection="customer_a_knowledge")`. The `target_collection`
argument threads through all three activities. The agent reads the active collection from
`temporal_config` — point it at the tenant-specific collection.

### Parallel ingestion

Fan out multiple `IngestWorkflow` starts in a parent workflow or from the trigger layer:

```python
# Start all refs from a batch event concurrently
await asyncio.gather(*[start_ingest(client, ref) for ref in refs])
```

Each `IngestWorkflow` instance is independent — they share the same `chunks_staging` and
`knowledge` collections but operate on disjoint `doc_id` namespaces.

### Incremental sync (change-driven dedupe)

The content-hash check in `fetch_and_stage_chunks` provides built-in incremental sync:

- Same key, same content → `status: unchanged`, workflow returns immediately, no embedding call.
- Same key, new content → full re-embed, `index_document` updates in place and prunes stale chunks.
- New key → full ingest.

For RDBMS sources, hash the serialized row JSON as the `doc_content_hash` to get the same
behaviour for database row updates.

### Scheduled full re-sync

Use a Temporal scheduled workflow (cron) to poll a source and submit `IngestWorkflow` for each
object, relying on the content-hash dedupe to skip unchanged documents:

```python
@workflow.defn
class FullSyncWorkflow:
    @workflow.run
    async def run(self, prefix: str) -> dict:
        keys = await workflow.execute_activity(list_s3_keys, args=[prefix], ...)
        for key in keys:
            ref = S3Ref.make(bucket=settings.s3_bucket, key=key)
            await workflow.execute_child_workflow(IngestWorkflow, args=[ref], ...)
        return {"synced": len(keys)}
```

### Model A/B testing

Run `BackfillWorkflow` into a third collection (`knowledge_v3`) with a different model. Point
a shadow agent at `knowledge_v3` without cutting over production, compare retrieval quality,
then cut over when satisfied.

### Observability hooks

Each activity emits structured log lines via `activity.logger`. Add a MongoDB sink to
Temporal's visibility store (or use Temporal Cloud's built-in search) to query workflow status
by `source_uri`, `doc_id`, or `extractor`.
