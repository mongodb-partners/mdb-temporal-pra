# Low-Level Design

**MongoDB × Temporal Partner Reference Architecture**
Version: 2.0 · Branch: `straight-to-temporal-ingest` · Date: 2026-08-07

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
- [11. Agent](#11-agent)
- [12. Configuration reference](#12-configuration-reference)
- [13. Scaling to multiple data sources](#13-scaling-to-multiple-data-sources)
- [14. Scaling to multiple data types](#14-scaling-to-multiple-data-types)
- [15. Extension patterns and future sources](#15-extension-patterns-and-future-sources)

---

## 1. Overview

This implementation is a **durable, event-driven RAG ingestion pipeline** plus a **durable
research agent**, built on two services:

| Layer               | Service       | Role                                                                            |
| ------------------- | ------------- | ------------------------------------------------------------------------------- |
| Orchestration       | Temporal      | Ingestion (chunk → embed → index) and the agent loop — durable, resumable, observable |
| Storage + retrieval | MongoDB Atlas | Single store for staged chunks, embedded knowledge, agent memory + vector search |

Embeddings and reranking are provided by **Voyage AI (MongoDB AI)**.

Ingestion is triggered **directly** from an object-created event — **no Kafka or message
broker**. The moment the object lands, an AWS Lambda (real S3) or a MinIO webhook (local) starts
an `IngestWorkflow`; once started, Temporal guarantees it runs to completion through failures.
This removes the previous Kafka → Sink Connector → `sources` → Atlas Stream Processing chain and
its operational overhead (see ADR `docs/decisions/0001-trigger-ingestion-directly-from-s3.md`).

The design is **source-agnostic** at two seams: the `S3Ref` data contract + the `handle_s3_event`
trigger core (adding a source means calling `start_ingest` from a new adapter), and the extractor
factory (adding a file type means one new extractor class). Workflows, activities, and the Atlas
storage layer are unchanged in both cases.

---

## 2. Component inventory

```
pipeline/
├── worker.py               ← Temporal worker (hosts ingestion + agent workflows/activities)
├── config.py               ← Pydantic settings (env-driven, lru_cache singleton)
├── models.py               ← Data contracts: S3Ref, Chunk, EmbeddedChunk
├── clients.py              ← Lazy singletons: Temporal, MongoDB, Voyage, S3
├── trigger.py              ← Shared trigger core: handle_s3_event + start_ingest
├── trigger_api.py          ← Webhook endpoint: POST /ingest-event (+ manual /ingest-trigger)
├── lambda_handler.py       ← AWS Lambda entrypoint for real S3 (same handle_s3_event core)
├── s3util.py               ← Parse an S3 / MinIO ObjectCreated event → list[S3Ref]
├── search_index.py         ← Idempotent Atlas Vector Search index management
├── config_store.py         ← Active collection/index pointer (cutover)
├── retrieval.py            ← Shared Atlas vector search (used by the agents)
├── seed.py                 ← Dev utility: upload a local file to MinIO
├── seed_repo.py            ← Dev utility: bulk-upload a markdown docs repo
├── cutover.py              ← Flip active collection pointer in temporal_config
├── workflows/
│   ├── ingest_workflow.py  ← IngestWorkflow: 3 stages, per-chunk checkpointing, parallel embed
│   └── backfill_workflow.py← BackfillWorkflow: paginated re-embed with continue-as-new
├── activities/
│   ├── ingest.py           ← fetch_and_stage_chunks, embed_staged_chunk, index_document
│   └── backfill.py         ← read_source_batch, reembed_and_write, ensure_target_index
└── extractors/
    ├── base.py             ← Extractor ABC + window() splitter + RawChunk
    ├── factory.py          ← get_extractor(): extension → MIME → TextExtractor
    ├── markdown.py / pdf.py / csv_ext.py / text.py

agent/
├── api.py                  ← FastAPI: /research (start), /research/{id} (poll)
├── tools.py                ← Agent tools as activities: vector_search_tool, rerank_tool
├── agent_workflow.py       ← DeepResearchAgent: OpenAI Agents SDK loop as a Temporal workflow
└── ui/                     ← React/Vite chat UI (polls /research for live progress)
```

---

## 3. Data contracts

Defined in `pipeline/models.py`. All contracts are plain Python `@dataclass`es — they serialize
cleanly through Temporal's default JSON data converter.

### S3Ref

The atomic unit passed into `IngestWorkflow`. Identifies a single object in S3 (or MinIO).

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

`S3Ref.make(bucket, key, ...)` derives `s3_uri` and strips quotes from the ETag.

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

> The ingest activities stage chunks as plain documents in MongoDB (see §8); `Chunk` /
> `EmbeddedChunk` are the conceptual schema for those documents.

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
[MinIO / S3]
   │  native ObjectCreated event
   ▼
┌─────────────────────────────────────┐
│ Trigger adapter                     │
│   • real S3:  AWS Lambda            │  ── both call ──▶  handle_s3_event(event)
│   • local:    MinIO webhook         │                      → refs_from_s3_event → S3Ref[]
│               POST /ingest-event    │                      → start_ingest(S3Ref) per object
└─────────────────────────────────────┘
                   │  start_workflow("IngestWorkflow", S3Ref)
                   │  id = ingest-<sha1(s3_uri)>, conflict = TERMINATE_EXISTING
                   ▼
┌─────────────────────────────────────┐
│  IngestWorkflow (Temporal)          │
│                                     │
│  Stage 1: fetch_and_stage_chunks    │
│    • GET s3://{bucket}/{key}        │
│    • sha256 dedupe check            │
│    • factory extractor → chunks     │
│    • INSERT → temporal.chunks_staging│
│                                     │
│  Stage 2: embed_staged_chunk        │
│    • one activity per chunk         │
│    • run in parallel waves (10)     │
│    • idempotent (skip if embedded)  │
│    • Voyage embed(text) → vector    │
│                                     │
│  Stage 3: index_document            │
│    • UPSERT chunks → knowledge      │
│    • DELETE stale ordinals          │
│    • ensure_vector_index (idempotent)│
│    • DELETE chunks_staging (cleanup)│
└─────────────────────────────────────┘
                   │
                   ▼
     [Atlas: temporal.knowledge + Vector Search index]
                   │
        agent vector search (tool)
                   ▼
   [DeepResearchAgent workflow / FastAPI / React UI]
```

No `sources` collection and no broker sit between the event and the workflow: the trigger adapter
calls `start_ingest` directly, and durability begins the instant `start_workflow` returns.

---

## 5. Workflow design

### 5.1 IngestWorkflow

**File:** `pipeline/workflows/ingest_workflow.py`

**Input:** `S3Ref`, optional `target_collection`

**Stages:**

| #     | Activity                             | Timeout | Retries                  | Idempotent                                              |
| ----- | ------------------------------------ | ------- | ------------------------ | ------------------------------------------------------- |
| 1     | `fetch_and_stage_chunks`             | 5 min   | 5                        | Yes — short-circuits on matching `doc_content_hash`     |
| 2     | `embed_staged_chunk` (one per chunk) | 2 min   | 6 (exp backoff, max 30s) | Yes — skips if `status == "embedded"` and model matches |
| 3     | `index_document`                     | 2 min   | 6                        | Yes — upsert by `chunk_id`, prune stale ordinals        |

**Parallel embedding:** Stage 2 fans the per-chunk embed activities out in **waves of
`_EMBED_BATCH` (10)** — up to 10 embeddings run concurrently, then the next wave — rather than
one at a time:

```python
for start in range(0, n, _EMBED_BATCH):
    await asyncio.gather(*(
        workflow.execute_activity(embed_staged_chunk, args=[f"{doc_id}:{i}", None], ...)
        for i in range(start, min(start + _EMBED_BATCH, n))
    ))
```

The worker's `ThreadPoolExecutor(max_workers=16)` runs these sync activities concurrently, so the
wave genuinely parallelizes; bounding to 10 stays within the pool and Voyage rate limits.

**Resumability guarantee:** Each chunk is a separate activity, and `embed_staged_chunk` skips a
chunk already embedded with the active model. If the worker crashes mid-run, Temporal resumes and
re-runs only the unfinished chunks — no re-embedding of completed work.

**Update-in-place:** Re-uploading the same key with new content yields a new `doc_content_hash`.
Stage 1 clears stale staging rows, Stage 2 re-embeds, and Stage 3 upserts into `knowledge` and
prunes chunks whose `ordinal >= new_n` (leftovers from a previously longer version).

**Workflow ID / dedupe:** the id is `ingest-<sha1(s3_uri)>` (stable per object key) with
`WorkflowIDConflictPolicy.TERMINATE_EXISTING` — a re-upload while an ingest is still running
terminates the in-flight run and starts fresh, so there is never a duplicate or a race.

### 5.2 BackfillWorkflow

**File:** `pipeline/workflows/backfill_workflow.py`

**Trigger:** model upgrade requiring a dimension change (e.g. `voyage-3.5` 1024-dim → new model).

**Design pattern:** `continue_as_new` — the workflow re-starts itself with a cursor (`after_id`)
after each batch, keeping Temporal history bounded regardless of collection size.

**Stages per batch:**

| Activity                      | Description                                                                                     |
| ----------------------------- | ----------------------------------------------------------------------------------------------- |
| `ensure_target_index`         | Create the vector index on the green collection at the new dimension (once, before first batch) |
| `read_source_batch`           | Paginated read of the active collection, 50 chunks at a time, sorted by `_id`                   |
| `reembed_and_write` (per doc) | Re-embed with new model, upsert into target collection                                          |

**Blue/green cutover:** `BackfillWorkflow` writes only to `knowledge_v2` (green). The active
pointer in `temporal_config` stays on `knowledge` (blue) until the operator runs
`make cutover TO=knowledge_v2`. The agent reads the active pointer on every query — no restart.

```
knowledge (blue)  ──read──▶  BackfillWorkflow  ──write──▶  knowledge_v2 (green)
                                                                │
                                                          make cutover
                                                                │
                                                    temporal_config.active = "knowledge_v2"
```

---

## 6. Activity design

All activities follow three rules:

1. **Idempotent** — safe to retry; re-running a completed activity produces no duplicate writes.
2. **Heartbeating** — long-running activities (`embed_staged_chunk`, `reembed_and_write`) call
   `activity.heartbeat()` so Temporal detects stalls within the `heartbeat_timeout` (30s).
3. **Sync execution** — activities use standard `pymongo`, `voyageai`, `boto3` clients and run in
   the worker's `ThreadPoolExecutor(max_workers=16)`, keeping the Temporal event loop free.

### Activity retry policy (embed activities)

```python
RetryPolicy(
    initial_interval=timedelta(seconds=2),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=30),
    maximum_attempts=6,
)
```

Voyage AI rate-limit (429) and transient network errors are handled transparently.

---

## 7. Extractor system

**File:** `pipeline/extractors/`

The extractor factory decouples file-type handling from the workflow. Adding a format requires
only a new extractor class — no workflow or activity changes.

### Base class

```python
class Extractor(ABC):
    name: str
    chunk_size: int
    chunk_overlap: int

    @abstractmethod
    def pieces(self, body: bytes) -> list[tuple[str, dict]]:
        """Return (text, meta) pairs; ordinal assignment handled by base."""

    def chunk(self, body: bytes) -> list[RawChunk]:
        """Call pieces(), assign ordinals, drop blanks."""
```

### Chunking strategy

The base provides a `window()` splitter — a character-window with overlap. Default:
`chunk_size=1200`, `chunk_overlap=150` (configurable in `.env`).

### Registered extractors

| Extension / MIME                       | Extractor           | Chunking strategy                                                       |
| -------------------------------------- | ------------------- | ----------------------------------------------------------------------- |
| `.md`, `.markdown` / `text/markdown`   | `MarkdownExtractor` | Section-aware: splits on `#` headings, then window-splits long sections |
| `.pdf` / `application/pdf`             | `PdfExtractor`      | PyPDF page-by-page, window-splits long pages                            |
| `.csv` / `text/csv`, `application/csv` | `CsvExtractor`      | Row-group batching                                                      |
| anything else                          | `TextExtractor`     | Plain window split (fallback)                                           |

Resolution order: **file extension → MIME type → TextExtractor fallback**.

---

## 8. MongoDB data model

Database: `temporal` (configurable via `MONGODB_DB`)

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

Indexes: `{ doc_id: 1 }`, `{ chunk_id: 1 }` (unique), `{ doc_id: 1, status: 1 }`.

### `knowledge` collection (active, blue)

The searchable store. Upserted by `index_document` using `chunk_id` as the upsert key.

```json
{
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
{ "_id": "active", "collection": "knowledge", "index": "temporalai_search_index" }
```

### `agent_memory` collection

Reserved for agent memory. **Not currently written** by any code path — the durable research
agent does not yet persist memory. Atlas remains the intended store for it (same database as
retrieval); wiring it into the agent loop is a follow-up.

---

## 9. Atlas Vector Search index

**File:** `pipeline/search_index.py`

Created idempotently by `ensure_vector_index` at the end of every `index_document`:

```json
{
  "fields": [
    { "type": "vector", "path": "embedding", "numDimensions": 1024, "similarity": "cosine" },
    { "type": "filter", "path": "doc_id" },
    { "type": "filter", "path": "source_uri" }
  ]
}
```

The `filter` fields let retrieval scope to a document or source URI without a full scan.
`ensure_vector_index` lists existing indexes and skips creation if present. `BackfillWorkflow`
calls `ensure_target_index` once before writing to the green collection.

---

## 10. Trigger layer

The trigger turns an S3 **ObjectCreated** event into the start of an `IngestWorkflow`. All
adapters funnel through one shared, source-agnostic core in `pipeline/trigger.py`:

```python
async def handle_s3_event(client, event) -> list[str]:
    # parse the event envelope → S3Ref[]; start one IngestWorkflow per object
    return [await start_ingest(client, ref) for ref in refs_from_s3_event(event)]

async def start_ingest(client, ref: S3Ref) -> str:
    handle = await client.start_workflow(
        "IngestWorkflow", ref,
        id=f"ingest-{doc_id_for_uri(ref.s3_uri)}",
        task_queue=settings.temporal_task_queue,
        id_conflict_policy=WorkflowIDConflictPolicy.TERMINATE_EXISTING,
    )
    return handle.id
```

`refs_from_s3_event` (`s3util.py`) parses the standard `Records[*].s3` envelope (AWS and MinIO
share the shape; SNS-wrapped and `s3:TestEvent` bodies are handled), URL-decoding the key.

### Local dev — MinIO webhook → `trigger_api.py`

MinIO's native **webhook** notification POSTs each ObjectCreated event to `POST /ingest-event`.
The endpoint reuses one cached Temporal client (FastAPI lifespan) and calls the shared core:

```python
@app.post("/ingest-event")
async def ingest_event(request: Request) -> dict:
    event = await request.json()
    return {"started": await handle_s3_event(request.app.state.temporal, event)}
```

MinIO uses a `queue_dir`, so events that arrive before the host `trigger_api` is up are buffered
and replayed (at-least-once locally). A manual `POST /ingest-trigger {bucket, key}` endpoint is
also provided for scripted/testing triggers.

### Production — AWS Lambda → `lambda_handler.py`

In production, an AWS Lambda subscribed to the bucket's S3 event notifications runs the **same**
core:

```python
def lambda_handler(event, context) -> dict:
    return {"started": asyncio.run(_run(event))}   # _run connects a client, calls handle_s3_event
```

Same parsing, same `start_ingest`, same durability guarantee — the local webhook is simply the
stand-in for this Lambda. Deploy notes are in `docs/RUNBOOK.md` §10.

---

## 11. Agent

The durable research agent runs over the same Atlas knowledge base the ingestion pipeline wrote
(and the same Voyage embedding space).

### DeepResearchAgent — `agent/agent_workflow.py`

The agent's reasoning loop runs **as a Temporal workflow** (`DeepResearchAgent`) via the OpenAI
Agents SDK ↔ Temporal integration (`temporalio.contrib.openai_agents`). Model calls run as
activities; the agent decides which tools to call and how often. The whole trajectory is durable,
resumable, and inspectable in the Temporal UI.

- **Tools:** `vector_search_tool` and `rerank_tool` (`agent/tools.py`) are Temporal activities
  wrapped via `activity_as_tool`. `rerank_tool` takes `chunk_id`s and reloads chunk text
  server-side, so the model never shuttles chunk text through tool arguments. A hosted
  `WebSearchTool` supplements the corpus (it runs inside the model-call activity).
- **Instructions:** decompose multi-part questions and search each sub-topic, rerank, prefer the
  ingested docs over the open web, answer only from gathered sources with inline `[n]` citations.
- **Live progress:** run hooks append human-readable steps ("Searching the docs…", "Reranking…",
  "Reasoning…") to workflow state exposed via a `progress` **query**. `POST /research` starts the
  workflow and returns a `workflow_id`; the UI polls `GET /research/{id}` for steps + the final
  answer (step-level, not token streaming).
- **Opt-in:** the agent + `OpenAIAgentsPlugin` load only when `OPENAI_API_KEY` is set; without it
  the worker runs ingestion exactly as before.

See `docs/agent-retrieval.md` for the full agent design.

### Vector search query (used by the `vector_search` tool, `pipeline/retrieval.py`)

```python
pipeline = [
    { "$vectorSearch": {
        "index": active_index, "path": "embedding", "queryVector": query_embedding,
        "numCandidates": max(100, k * 20), "limit": k,
    }},
    { "$project": { "_id": 0, "source_uri": 1, "chunk_id": 1, "text": 1,
                    "score": { "$meta": "vectorSearchScore" } } },
]
```

The query is embedded with the **active** model from `temporal_config` (so it matches the
collection's vector space after a cutover).

---

## 12. Configuration reference

All settings live in `.env` (loaded by `pipeline/config.py` via Pydantic Settings).

| Variable                  | Default                 | Description                                 |
| ------------------------- | ----------------------- | ------------------------------------------- |
| `MONGODB_URI`             | —                       | Atlas connection string (required)          |
| `MONGODB_DB`              | `temporal`              | Database name                               |
| `CHUNKS_COLLECTION`       | `chunks_staging`        | Transient staging between workflow stages   |
| `KNOWLEDGE_COLLECTION`    | `knowledge`             | Active (blue) embedded knowledge store      |
| `KNOWLEDGE_V2_COLLECTION` | `knowledge_v2`          | Green backfill target                       |
| `CONFIG_COLLECTION`       | `temporal_config`       | Active pointer document                     |
| `MEMORY_COLLECTION`       | `agent_memory`          | Reserved for agent memory (not yet written) |
| `VOYAGE_API_KEY`          | —                       | Voyage AI key (embeddings + rerank)         |
| `VOYAGE_MODEL`            | `voyage-3.5`            | Embedding model (1024-dim)                  |
| `VOYAGE_RERANK_MODEL`     | `rerank-2.5`            | Reranking model                             |
| `EMBED_DIM`               | `1024`                  | Embedding dimensionality (must match model) |
| `OPENAI_API_KEY`          | —                       | Enables the durable research agent          |
| `AGENT_MODEL`             | `gpt-4.1`               | OpenAI model for the agent loop             |
| `AGENT_MAX_TURNS`         | `8`                     | Guardrail on the agent tool-use loop        |
| `CHUNK_SIZE`              | `1200`                  | Maximum characters per chunk                |
| `CHUNK_OVERLAP`           | `150`                   | Overlap characters between adjacent chunks  |
| `TEMPORAL_ADDRESS`        | `localhost:7233`        | Temporal server address                     |
| `TEMPORAL_TASK_QUEUE`     | `temporal-pipeline`     | Worker task queue                           |
| `TRIGGER_API_PORT`        | `8088`                  | Webhook trigger endpoint port               |
| `S3_ENDPOINT_URL`         | `http://localhost:9000` | MinIO endpoint (blank = real AWS S3)        |
| `S3_BUCKET`               | `temporal-datasources`  | Source bucket                               |
| `SQS_QUEUE_URL`           | —                       | Set to use SQS-driven S3 events in prod     |

---

## 13. Scaling to multiple data sources

The design is **source-agnostic at the trigger seam**: any adapter that can build an `S3Ref` (or
object pointer) and call `start_ingest` feeds the pipeline — no broker required. The workflow,
activities, and storage layer are unchanged per source; only the trigger adapter differs.

### Current source: S3 / MinIO

```
S3 upload → ObjectCreated event → AWS Lambda (prod) / MinIO webhook (local)
         → handle_s3_event → start_ingest(S3Ref) → IngestWorkflow
```

### Adding a new source: general pattern

```
Step 1:  A source event fires (object store, queue, CDC, webhook).
Step 2:  A thin adapter (Lambda / small consumer / HTTP handler) turns it into an S3Ref
         (or, for inline content, uploads to S3 first) and calls start_ingest / handle_s3_event.
```

There is no `sources` collection, no sink connector, and no change-stream watcher to operate.

### Source examples

- **Other object stores / SQS-driven S3:** config already supports `SQS_QUEUE_URL` and
  `s3_source = auto | minio | sqs | poll` (`resolved_source()`); a queue consumer calls
  `start_ingest` per message.
- **RDBMS / CDC (Debezium, etc.):** a small consumer receives change events and calls
  `start_ingest`. For inline row content (no object to fetch), extend the contract with a
  `payload` and add a branch in `fetch_and_stage_chunks` to use it instead of an S3 GET.
- **Existing Atlas data (change stream):** an Atlas trigger or a watcher process calls the
  `/ingest-event` endpoint (or `start_ingest`) per change.
- **Webhook / HTTP (Notion, GitHub, …):** POST to `trigger_api`; large payloads upload to S3
  first and construct an `S3Ref`, small ones inline.

### Multi-source worker scaling

The Temporal worker is stateless. Scale horizontally by running more worker processes on the same
task queue; Temporal distributes workflow and activity tasks across them automatically. Per-worker
concurrency is `ThreadPoolExecutor(max_workers=16)` — tune to the embedding API rate limit.

```bash
uv run python -m pipeline.worker &   # worker 1
uv run python -m pipeline.worker &   # worker N
```

---

## 14. Scaling to multiple data types

The extractor factory (`pipeline/extractors/factory.py`) is the sole extension point for new file
or data types. Workflows and activities are type-agnostic — they receive `bytes` and call
`get_extractor(key, content_type).chunk(body)`.

### Adding a new extractor

1. Create `pipeline/extractors/my_format.py` subclassing `Extractor`, implementing `pieces()`.
2. Register it in `factory.py` (`_BY_EXT["myext"] = MyFormatExtractor`, and/or `_BY_MIME[...]`).
3. No changes to workflows, activities, or the Atlas data model.

### Current extractors

| Extractor           | `pieces()` strategy                                          | Metadata emitted       |
| ------------------- | ------------------------------------------------------------ | ---------------------- |
| `MarkdownExtractor` | Split on `#`/`##`/`###` headings; window-split long sections | `heading`, `level`     |
| `PdfExtractor`      | PyPDF page iteration; window-split long pages                | `page`                 |
| `CsvExtractor`      | Group rows into batches; emit each batch as one chunk        | `row_start`, `row_end` |
| `TextExtractor`     | Single `window()` pass over the entire body                  | —                      |

### Planned extractor extensions

`JsonExtractor`, `HtmlExtractor`, `DocxExtractor`, `XlsxExtractor`, `AudioTranscriptExtractor`,
`SqlResultExtractor` — each is one new class, no pipeline changes.

### Chunking parameter tuning

| Use case                             | `CHUNK_SIZE` | `CHUNK_OVERLAP`                 |
| ------------------------------------ | ------------ | ------------------------------- |
| Long narrative docs (PDF, markdown)  | 1200         | 150                             |
| Short structured records (CSV, JSON) | 512          | 64                              |
| Code files                           | 800          | 200 (preserve function context) |
| IoT telemetry batches                | 400          | 0 (batches are atomic)          |

---

## 15. Extension patterns and future sources

### Multi-tenant / multi-database

Parameterize `IngestWorkflow(ref, target_collection="customer_a_knowledge")`; the argument
threads through all three activities. Point the agent's active pointer at the tenant collection.

### Parallel ingestion

Fan out multiple `IngestWorkflow` starts (each is independent, operating on disjoint `doc_id`s):

```python
await asyncio.gather(*[start_ingest(client, ref) for ref in refs])
```

### Incremental sync (change-driven dedupe)

The content-hash check in `fetch_and_stage_chunks` gives built-in incremental sync:

- Same key, same content → `status: unchanged`, returns immediately, no embedding call.
- Same key, new content → re-embed; `index_document` updates in place and prunes stale chunks.
- New key → full ingest.

### Scheduled reconciliation / full re-sync

Because the object store (S3) is the source of truth, a **Temporal Scheduled Workflow** can list
the bucket and start `IngestWorkflow` for each key, relying on content-hash dedupe to skip
unchanged docs. This is also the backstop for a dropped source-event notification (the one
best-effort hop, on par with any broker-based design — see ADR 0001 "Delivery semantics"):

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

Run `BackfillWorkflow` into a third collection with a different model; point a shadow agent at it
and compare retrieval quality before cutting over.

### Observability hooks

Each activity emits structured logs via `activity.logger`; the durable agent's every tool/model
call is workflow history. Query Temporal visibility (or Temporal Cloud search) by `source_uri`,
`doc_id`, or workflow id.
