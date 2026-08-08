# Runbook — Developer Setup Guide

Everything a new developer needs to spin up this repo locally and understand the production
cloud setup.

---

## Table of contents

- [Prerequisites](#prerequisites)
- [1. MongoDB Atlas setup](#1-mongodb-atlas-setup)
- [2. Voyage AI API key](#2-voyage-ai-api-key)
- [3. OpenAI API key (for the research agent)](#3-openai-api-key-for-the-research-agent)
- [4. Environment configuration](#4-environment-configuration)
- [5. Local setup (make setup)](#5-local-setup-make-setup)
- [6. Local Docker infra — MinIO](#6-local-docker-infra--minio)
- [7. Run the full stack](#7-run-the-full-stack)
- [8. Verify ingestion](#8-verify-ingestion)
- [9. Backfill + model cutover](#9-backfill--model-cutover)
- [10. Production trigger (AWS Lambda)](#10-production-trigger-aws-lambda)
- [Cloud infra references](#cloud-infra-references)

---

## Prerequisites

Install these tools before running anything:

| Tool               | Version | Install                                                                                                              |
| ------------------ | ------- | -------------------------------------------------------------------------------------------------------------------- |
| **uv**             | latest  | `curl -LsSf https://astral.sh/uv/install.sh \| sh` — [docs](https://docs.astral.sh/uv/getting-started/installation/) |
| **Docker Desktop** | ≥ 4.x   | [docker.com/products/docker-desktop](https://www.docker.com/products/docker-desktop/)                                |
| **Temporal CLI**   | latest  | `brew install temporal` or [docs.temporal.io/cli](https://docs.temporal.io/cli)                                      |
| **Node.js**        | ≥ 20    | `brew install node` or [nodejs.org](https://nodejs.org/)                                                             |
| **mongosh**        | latest  | `brew install mongosh` — optional, handy for inspecting Atlas collections                                           |

Verify:

```bash
uv --version
docker --version
temporal --version
node --version
```

---

## 1. MongoDB Atlas setup

### Create a free cluster

1. Sign up or log in at [cloud.mongodb.com](https://cloud.mongodb.com).
2. Create a new **M0 free cluster** (or any tier) in a region close to you.
3. Docs: [Create a Cluster](https://www.mongodb.com/docs/atlas/tutorial/create-new-cluster/)

### Create a database user

1. In the Atlas UI: **Security → Database Access → Add New Database User**.
2. Choose **Password** auth. Note the username and password.
3. Grant **Atlas Admin** role (or at minimum `readWriteAnyDatabase`).
4. Docs: [Configure Database Users](https://www.mongodb.com/docs/atlas/security-add-mongodb-users/)

### Allow network access

Your local machine (the Python worker/trigger processes) and the MinIO container reach Atlas from your machine's public IP. You need to allow:

- Your **local machine IP** (for Python scripts).
- **0.0.0.0/0** temporarily while testing, or your NAT public IP for the Docker network.

Steps: **Security → Network Access → Add IP Address**.

Docs: [Configure IP Access List](https://www.mongodb.com/docs/atlas/security/ip-access-list/)

### Get the connection string

1. **Database → Connect → Drivers** → copy the `mongodb+srv://` URI.
2. Replace `<username>` and `<password>` with the credentials you created above.

The URI will look like:

```
mongodb+srv://myuser:mypass@mycluster.abc12.mongodb.net/?retryWrites=true&w=majority
```

---

## 2. Voyage AI API key

Voyage AI is available directly through **MongoDB Atlas Models** — no separate Voyage AI account
required.

1. In the Atlas UI go to **Services → Atlas Models** (or search "Models" in the left nav).
2. Select **Voyage AI** from the provider list and click **Generate API Key**.
3. Copy the key — it will only be shown once.
4. The default model used is `voyage-3.5` (1024 dimensions).
5. Docs: [Atlas Models — Voyage AI](https://www.mongodb.com/docs/atlas/ai-integrations/)

---

## 3. OpenAI API key (for the research agent)

The durable research agent is built with the OpenAI Agents SDK, so it needs an OpenAI key.

1. Sign up at [platform.openai.com](https://platform.openai.com/) and create an **API key**.
2. Set `OPENAI_API_KEY` in `.env`. Optionally set `AGENT_MODEL` to a **web-search-capable**
   OpenAI model (the agent's `WebSearchTool` requires one).
3. The agent is **optional**: ingestion runs without it. Without the key the worker skips the
   agent and `POST /research` returns a clear 503.
4. Docs: [OpenAI API keys](https://platform.openai.com/docs/api-reference/authentication)

---

## 4. Environment configuration

```bash
cp .env.example .env
```

Open `.env` and fill in the required values:

```bash
# REQUIRED — fill these in
MONGODB_URI=mongodb+srv://<user>:<pass>@<cluster>.mongodb.net/?retryWrites=true&w=majority
VOYAGE_API_KEY=<your-voyage-api-key>
# For the research agent (optional — ingestion works without it):
OPENAI_API_KEY=<your-openai-api-key>
AGENT_MODEL=gpt-4.1
```

The remaining defaults work as-is for local development (MinIO on `localhost:9000`, Temporal on
`localhost:7233`, trigger API on `localhost:8088`).

For real AWS S3 instead of local MinIO:

```bash
# Comment out S3_ENDPOINT_URL and set real values:
# S3_ENDPOINT_URL=http://localhost:9000   ← remove this line
S3_BUCKET=your-real-s3-bucket
AWS_ACCESS_KEY_ID=<real-key>
AWS_SECRET_ACCESS_KEY=<real-secret>
AWS_REGION=us-east-1
```

---

## 5. Local setup (make setup)

Install Python and UI dependencies in one command:

```bash
make setup
```

This runs:

- `uv sync` — installs Python 3.12 + all dependencies from `pyproject.toml`
- `npm install` in `agent/ui/` — installs the React/Vite frontend

To install separately:

```bash
make install        # Python only
cd agent/ui && npm install   # UI only
```

---

## 6. Local Docker infra — MinIO

The local stack triggers ingestion **directly**: MinIO's native **webhook** notification POSTs
each new object to the trigger API, which starts the `IngestWorkflow` (see ADR
`docs/decisions/0001-trigger-ingestion-directly-from-s3.md`). No Kafka or message broker is
involved.

| Production                   | Local substitute               | Purpose                                     |
| ---------------------------- | ------------------------------ | ------------------------------------------- |
| AWS S3                       | **MinIO** (Docker)             | Object storage + native event notifications |
| AWS Lambda (S3 event target) | **trigger API** (host process) | Receives the event and starts the workflow  |

### Start infra (default)

```bash
make infra-up
```

Starts **MinIO** only, creates the `temporal-datasources` bucket, and subscribes its
ObjectCreated events to the MinIO **webhook** target. The webhook endpoint is
`http://host.docker.internal:8088/ingest-event` — the `trigger_api` host process started by
`make start`. MinIO uses a `queue_dir`, so events that land before the trigger API is up are
buffered and replayed (at-least-once).

### Default trigger: MinIO webhook → trigger API

MinIO POSTs the raw S3 event JSON to `POST /ingest-event`, which parses it with the shared
`refs_from_s3_event` and starts one `IngestWorkflow` per object — the **same code path** an
AWS Lambda runs against real S3 (`pipeline/lambda_handler.py`). No Kafka, Sink Connector,
`sources` collection, or Stream Processing is involved.

### MinIO (local S3)

- Console: [http://localhost:9001](http://localhost:9001) (user: `minioadmin` / pass: `minioadmin`)
- API: `http://localhost:9000`
- Bucket: `temporal-datasources` (auto-created)

### Stop / clean

```bash
make infra-down     # stop containers (keep volumes)
make infra-clean    # stop + delete volumes (wipes MinIO data)
```

---

## 7. Run the full stack

`make start` starts all services in the background:

```bash
make start
```

Starts (in order):

1. Infra (MinIO) via `make infra-up`
2. Temporal dev server (`:7233`, Web UI `:8233`)
3. Temporal worker (`pipeline/worker.py`)
4. Trigger API (`pipeline/trigger_api.py`, `:8088`) — receives MinIO webhook POSTs at `/ingest-event`
5. Agent API (`agent/api.py`, `:8090`)
6. Agent UI (`agent/ui`, `:5173`)

Logs go to `.local/*.log`. Tail them:

```bash
make app-logs
```

### Create the Atlas Vector Search index (one-time)

```bash
make index
```

This creates the `temporalai_search_index` vector search index on `temporal.knowledge` in Atlas.
The workflow also creates it automatically on first ingest, but running `make index` upfront avoids
a delay on the first document.

Docs: [Atlas Vector Search](https://www.mongodb.com/docs/atlas/atlas-vector-search/vector-search-overview/)

### Seed a document

```bash
make seed                                         # uploads seed/awesome-temporal.md
make seed FILE=./my-doc.pdf KEY=docs/my-doc.pdf   # upload your own file
```

Supported formats: `.md`, `.pdf`, `.csv`, `.txt`.

Watch the workflow run in the Temporal Web UI at [http://localhost:8233](http://localhost:8233).

### Use the agent UI

Open [http://localhost:5173](http://localhost:5173) and ask questions about the ingested docs.

### Stop everything

```bash
make stop           # stops app + Temporal + infra
make stop-app       # stops app processes only (leaves infra + Temporal running)
make restart-app    # restart app processes after .env changes
```

---

## 8. Verify ingestion

```bash
make seed FILE=./my-doc.pdf KEY=docs/my-doc.pdf
# watch Temporal UI: IngestWorkflow → Completed
make query Q="something in that file"                   # vector search result
```

In Atlas, confirm:

- `temporal.knowledge` has embedded chunk documents.

---

## 9. Backfill + model cutover

Use this when upgrading the embedding model (e.g. `voyage-3.5` → a newer model with different
dimensions):

```bash
# Re-embed the active collection into knowledge_v2
make backfill MODEL=voyage-3-large

# Wait for BackfillWorkflow to complete in the Temporal UI
# Wait for the knowledge_v2 Atlas Vector Search index to reach READY state

# Flip the active pointer to knowledge_v2
make cutover TO=knowledge_v2

# Verify queries now hit knowledge_v2
make query Q="test question"
```

The agent reads the `temporal_config` collection to know which collection is active. No restart
needed.

---

## 10. Production trigger (AWS Lambda)

In production the trigger is an **AWS Lambda** subscribed to the S3 bucket's **ObjectCreated**
event notifications. The Lambda calls `pipeline.lambda_handler`, which runs the **same**
`handle_s3_event` code the local MinIO webhook uses — parse the event, start one
`IngestWorkflow` per object. No Kafka, `sources` collection, or Stream Processing is involved;
the `queue_dir`-backed MinIO webhook is simply the local stand-in for this Lambda.

**Deploy notes:**

- Handler entrypoint: `pipeline.lambda_handler.lambda_handler`; package `pipeline/` with the
  `temporalio` dependency.
- Give the Lambda network egress to your Temporal service and set `TEMPORAL_ADDRESS` /
  `TEMPORAL_NAMESPACE` (plus mTLS cert paths for Temporal Cloud) via environment.
- Wire the bucket's `s3:ObjectCreated:*` notifications to the Lambda (S3 console or IaC).

---

## Cloud infra references

When moving from local dev to real cloud infrastructure, use these references:

### MongoDB Atlas

| Topic                   | Documentation                                                                                                                |
| ----------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| Create a cluster        | [mongodb.com/docs/atlas/tutorial/create-new-cluster](https://www.mongodb.com/docs/atlas/tutorial/create-new-cluster/)        |
| Database users          | [mongodb.com/docs/atlas/security-add-mongodb-users](https://www.mongodb.com/docs/atlas/security-add-mongodb-users/)          |
| Network access          | [mongodb.com/docs/atlas/security/ip-access-list](https://www.mongodb.com/docs/atlas/security/ip-access-list/)                |
| Atlas Vector Search     | [mongodb.com/docs/atlas/atlas-vector-search](https://www.mongodb.com/docs/atlas/atlas-vector-search/vector-search-overview/) |

### AWS S3 (production)

Replace MinIO with real S3:

```bash
# In .env — comment out S3_ENDPOINT_URL, set real values
S3_BUCKET=your-production-bucket
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=<real-key>
AWS_SECRET_ACCESS_KEY=<real-secret>
```

| Topic                                   | Documentation                                                                                                               |
| --------------------------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| S3 Getting Started                      | [docs.aws.amazon.com/s3/getting-started](https://docs.aws.amazon.com/AmazonS3/latest/userguide/GetStartedWithS3.html)       |
| S3 Event Notifications (→ Lambda)       | [docs.aws.amazon.com/s3/event-notifications](https://docs.aws.amazon.com/AmazonS3/latest/userguide/EventNotifications.html) |

### Temporal (production)

| Option                | Documentation                                                                    |
| --------------------- | -------------------------------------------------------------------------------- |
| Temporal Cloud        | [temporal.io/cloud](https://temporal.io/cloud)                                   |
| Self-hosted with Helm | [docs.temporal.io/self-hosted-guide](https://docs.temporal.io/self-hosted-guide) |

Update `TEMPORAL_ADDRESS` in `.env` to the Temporal Cloud endpoint. Add mTLS cert paths for
Temporal Cloud connections.

### Voyage AI (via MongoDB Atlas Models)

| Topic                         | Documentation                                                                                                                                             |
| ----------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Atlas Models overview         | [mongodb.com/docs/atlas/ai-integrations](https://www.mongodb.com/docs/atlas/ai-integrations/)                                                             |
| Voyage AI embeddings on Atlas | [mongodb.com/docs/atlas/atlas-vector-search/ai-integrations/voyage-ai](https://www.mongodb.com/docs/atlas/atlas-vector-search/ai-integrations/voyage-ai/) |
| Available embedding models    | [mongodb.com/docs/atlas/ai-integrations/voyage-ai/models](https://www.mongodb.com/docs/atlas/ai-integrations/)                                            |

### OpenAI (research agent)

| Topic             | Documentation                                                                                     |
| ----------------- | ------------------------------------------------------------------------------------------------- |
| Agents SDK        | [openai.github.io/openai-agents-python](https://openai.github.io/openai-agents-python/)           |
| Temporal ↔ Agents | [docs.temporal.io/ai-cookbook/openai-agents-sdk-python](https://docs.temporal.io/ai-cookbook/openai-agents-sdk-python) |
