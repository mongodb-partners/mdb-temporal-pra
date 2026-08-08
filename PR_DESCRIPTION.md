# Direct-to-Temporal ingestion (no Kafka) + durable OpenAI research agent

## What this PR does

Reworks the MongoDB × Temporal reference architecture into a **minimal, two-path design** — a
direct-triggered ingestion pipeline and a durable agent — removing the Kafka/connector stack and
the legacy Claude RAG endpoint entirely.

1. **Ingestion triggers Temporal directly from S3 events.** An S3 `ObjectCreated` event starts the
   `IngestWorkflow` via an **AWS Lambda** (prod) or a **MinIO webhook** (local) — both calling the
   same `handle_s3_event` core. Temporal provides the durability that a broker would (no lost work
   once the workflow starts), so **Kafka, the MongoDB Sink Connector, the `sources` collection, and
   Atlas Stream Processing are gone**. Rationale in ADR
   `docs/decisions/0001-trigger-ingestion-directly-from-s3.md`.

2. **A durable research agent (OpenAI Agents SDK on Temporal).** The agent's reasoning loop runs as
   a Temporal workflow (`DeepResearchAgent`); model + tool calls are activities, so the trajectory
   is durable, resumable, and auditable. Tools: `vector_search` + `rerank` (as activities) + hosted
   web search. Live step-progress streams to the UI via a workflow query + polling.

3. **Removed the legacy `/query` + Anthropic path.** The agent stack is now single-vendor for
   reasoning (OpenAI); Voyage still does embeddings/rerank, Atlas is the store.

## High-level changes

**Ingestion**
- Shared `handle_s3_event` (`pipeline/trigger.py`); `POST /ingest-event` webhook (`trigger_api.py`);
  `pipeline/lambda_handler.py` (Lambda entrypoint, same core).
- `IngestWorkflow` embeds chunks in **parallel waves of 10** instead of serially.
- `docker-compose.yml` is **MinIO-only**; `make start` runs `trigger-api`.

**Agent**
- `agent/agent_workflow.py` (`DeepResearchAgent` + `WebSearchTool` + `progress` query + `RunHooks`),
  `agent/tools.py` (`vector_search_tool`, `rerank_tool`; rerank reloads text server-side).
- `worker.py` loads `OpenAIAgentsPlugin` + the agent **only when `OPENAI_API_KEY` is set**
  (ingestion unaffected without it).
- `agent/api.py`: `POST /research` (start) + `GET /research/{id}` (poll progress). UI polls and
  renders the live step feed.

**Removals**
- Kafka: `kafka`/`connect`/`minio-setup-kafka` services, `infra/register_connector.py`,
  `infra/connectors/mongo-sink.json`.
- Change-stream path: `pipeline/trigger_listener.py`, `infra/asp/`, the `sources` collection (code
  refs), `make kafka-up`/`connector-status`/`trigger-listen`.
- Legacy agent: `agent/retrieval.py`, the `/query` endpoint, the `anthropic` dependency, and dead
  Kafka helpers (`RawRecord`/`to_json` in `models.py`).

**Deps / config**
- `pyproject.toml`: **+`openai-agents`, +`temporalio[opentelemetry]`, −`anthropic`**,
  +`pytest`/`pytest-asyncio` dev group.
- `config.py`: **+`openai_api_key`/`agent_model`/`agent_max_turns`**;
  **−`kafka_*`/`s3_events_topic`/`src_collection`/`anthropic_*`/`answer_model`**.

**Tests / docs / fix**
- New pytest suite — `tests/test_handle_s3_event.py`, `tests/test_s3util.py`,
  `tests/test_agent_tools.py` (**13 tests**; one asserts a MinIO event and an AWS event produce the
  identical `start_workflow` call).
- Docs updated end-to-end: **README** (Mermaid diagrams, direct-trigger + OpenAI agent), **RUNBOOK**
  (webhook default, §3 OpenAI key, §10 Lambda prod trigger), **ADR 0001** (Accepted; Kafka removed;
  delivery semantics), **LLD** (full rewrite), **`docs/agent-retrieval.md`** (durable agent).
- Fix: `agent/ui/package-lock.json` now resolves from public npm (was pinned to an internal
  Artifactory host, breaking `npm install` externally).

## Behavior / breaking changes
- **Ingestion trigger** is webhook (local) / Lambda (prod) — no broker. `make start` runs
  `trigger-api`; `kafka-up`/`connector-status`/`trigger-listen` targets removed.
- **Removed endpoint `/query`**; the UI uses `/research`.
- **Env added:** `OPENAI_API_KEY` (agent opt-in), `AGENT_MODEL`, `AGENT_MAX_TURNS`.
  **Env removed:** `KAFKA_BOOTSTRAP`, `KAFKA_CONNECT_URL`, `S3_EVENTS_TOPIC`, `SRC_COLLECTION`,
  `ANTHROPIC_API_KEY`, `ANSWER_MODEL`.
- **Deps:** removed `anthropic`; added `openai-agents`, `temporalio[opentelemetry]` (the Agents
  plugin needs opentelemetry).
- **MongoDB:** `temporal.sources` is no longer used or created; a leftover from prior runs is stale
  and can be dropped.

## Testing
- **13 unit tests pass** (`uv run pytest`); worker constructs and Temporal's sandbox validates the
  agent workflow both with and without an OpenAI key; the UI typechecks/builds; `docker compose
  config` is MinIO-only and valid; grep confirms no stale Kafka/Anthropic/`/query` references remain.
- **Not exercised live:** a full agent run against a real `OPENAI_API_KEY` + web-search-capable model.

## Known gaps / follow-ups
- **Agent memory** isn't read or written yet (Atlas is the intended store).
- **No reconciliation sweep** for a dropped S3→trigger notification — this gap is in the S3 eventing
  layer, **on par with a Kafka-based approach** (which ingests S3 events through the same
  best-effort hop); the fix is a scheduled S3-vs-`knowledge` reconciliation, independent of Kafka.
- Embeddings are **parallel but not batched**; **no token streaming** (step-level progress);
  **web search is hosted** (runs inside the model activity; needs a web-search-capable OpenAI model).

## Reviewer / committer notes
- **`.env.example`** was not editable in the authoring environment — confirm it has **no**
  `KAFKA_*`/`SRC_COLLECTION`/`ANTHROPIC_API_KEY`/`ANSWER_MODEL` and **includes** `OPENAI_API_KEY` +
  `AGENT_MODEL`.
- `AGENT_MODEL` defaults to `gpt-4.1` — set to a current web-search-capable OpenAI model.
- A stale **`temporal.sources`** collection may exist in Atlas from earlier runs — drop it manually
  (`db.sources.drop()`), or `dropDatabase()` for a full reset.
- `docs/blog-temporal-mongodb-outline.md` is untracked (marketing draft) — keep it out of this PR.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
