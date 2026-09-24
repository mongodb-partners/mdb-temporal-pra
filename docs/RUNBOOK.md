# Runbook: Developer Setup Guide

Everything a new developer needs to run this repo locally, against AWS S3 or a local MinIO, and
the references for moving to production cloud services.

---

## Table of contents

- [Prerequisites](#prerequisites)
- [1. MongoDB Atlas setup](#1-mongodb-atlas-setup)
- [2. Voyage AI API key](#2-voyage-ai-api-key)
- [3. OpenAI API key (for the research agent)](#3-openai-api-key-for-the-research-agent)
- [4. Environment configuration](#4-environment-configuration)
- [5. Local setup (make setup)](#5-local-setup-make-setup)
- [6. Storage: AWS S3 or local MinIO](#6-storage-aws-s3-or-local-minio)
- [7. Run the full stack](#7-run-the-full-stack)
- [8. Verify ingestion](#8-verify-ingestion)
- [9. Backfill + model cutover](#9-backfill--model-cutover)
- [10. Production trigger (AWS Lambda)](#10-production-trigger-aws-lambda)
- [Cloud infra references](#cloud-infra-references)

---

## Prerequisites

Install these tools before running anything:

| Tool               | Version | Install                                                                                                            |
| ------------------ | ------- | ------------------------------------------------------------------------------------------------------------------ |
| **uv**             | latest  | `curl -LsSf https://astral.sh/uv/install.sh \| sh` ([docs](https://docs.astral.sh/uv/getting-started/installation/)) |
| **Docker Desktop** | ≥ 4.x   | [docker.com/products/docker-desktop](https://www.docker.com/products/docker-desktop/). Needed for MinIO mode only  |
| **Temporal CLI**   | latest  | `brew install temporal` or [docs.temporal.io/cli](https://docs.temporal.io/cli)                                    |
| **Node.js**        | ≥ 20    | `brew install node` or [nodejs.org](https://nodejs.org/)                                                           |
| **mongosh**        | latest  | `brew install mongosh`. Optional, handy for inspecting Atlas collections                                           |

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

The worker, trigger API and agent API run as processes on your machine and connect to Atlas from
its public IP. Add that IP under **Security → Network Access → Add IP Address**, or `0.0.0.0/0`
temporarily while testing. MinIO never talks to Atlas.

Docs: [Configure IP Access List](https://www.mongodb.com/docs/atlas/security/ip-access-list/)

### Get the connection string

1. **Database → Connect → Drivers**, then copy the `mongodb+srv://` URI.
2. Replace `<username>` and `<password>` with the credentials you created above.

The URI will look like:

```
mongodb+srv://myuser:mypass@mycluster.abc12.mongodb.net/?retryWrites=true&w=majority
```

---

## 2. Voyage AI API key

Voyage AI is available directly through **MongoDB Atlas Models**, with no separate Voyage AI
account.

1. In the Atlas UI go to **Services → Atlas Models** (or search "Models" in the left nav).
2. Select **Voyage AI** from the provider list and click **Generate API Key**.
3. Copy the key. It is shown only once.
4. The default model is `voyage-3.5` (1024 dimensions). Reranking uses `rerank-2.5`.
5. A key from voyageai.com instead of Atlas also works: set
   `VOYAGE_BASE_URL=https://api.voyageai.com/v1`.
6. Docs: [Atlas Models: Voyage AI](https://www.mongodb.com/docs/atlas/ai-integrations/)

---

## 3. OpenAI API key (for the research agent)

The research agent is built on the OpenAI Agents SDK. It talks to api.openai.com by default, or to
any OpenAI-compatible gateway.

1. Sign up at [platform.openai.com](https://platform.openai.com/) and create an **API key**, or
   use the key your gateway issues.
2. Set `OPENAI_API_KEY` in `.env`, and set `AGENT_MODEL` to a model your endpoint serves (the
   code default is `gpt-4.1`).
3. **Gateway only:** set `OPENAI_BASE_URL` to the gateway's OpenAI-compatible base URL. If the
   gateway reads the key from a header other than `Authorization: Bearer`, name it in
   `OPENAI_API_KEY_HEADER` (for example `api-key`). If it offers Chat Completions but not the
   Responses API, set `OPENAI_USE_RESPONSES=false`. With a base URL set, the SDK's trace export
   to api.openai.com is turned off.
4. **Web search** needs the Responses API. With `OPENAI_USE_RESPONSES=false` the agent runs
   without the hosted `WebSearchTool` and answers from the knowledge base alone.
5. The agent is **optional**: ingestion runs without it. Without a key the worker skips the
   agent and `POST /research` returns a 503.
6. Docs: [OpenAI API keys](https://platform.openai.com/docs/api-reference/authentication)

---

## 4. Environment configuration

```bash
cp .env.example .env
```

The app reads exactly one dotenv file: `.env`, or whichever file `ENV_FILE` names. Values in that
file win over variables exported in your shell, so switch configurations with `ENV_FILE`, not
with `export`.

Fill in the required values:

```bash
# REQUIRED
MONGODB_URI=mongodb+srv://<user>:<pass>@<cluster>.mongodb.net/?retryWrites=true&w=majority
VOYAGE_API_KEY=<your-voyage-api-key>
S3_BUCKET=<your-bucket>
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=<your-aws-access-key-id>        # blank both to use the boto3 credential chain
AWS_SECRET_ACCESS_KEY=<your-aws-secret-access-key>
S3_ENDPOINT_URL=                                  # blank means AWS S3

# Research agent (optional; ingestion works without it)
OPENAI_API_KEY=<your-openai-api-key>
AGENT_MODEL=gpt-4.1
# Gateway only (see section 3)
OPENAI_BASE_URL=
OPENAI_API_KEY_HEADER=
OPENAI_USE_RESPONSES=true
```

The IAM principal needs `s3:PutObject` (for `make seed`) and `s3:GetObject` (for ingestion and
for every `read_span`) on the bucket. The remaining defaults work as-is: Temporal on
`localhost:7233`, trigger API on `localhost:8088`, agent API on `localhost:8090`.

For local MinIO, see [section 6](#6-storage-aws-s3-or-local-minio).

---

## 5. Local setup (make setup)

Install Python and UI dependencies in one command:

```bash
make setup
```

This runs:

- `uv sync`: installs Python 3.12 and all dependencies from `pyproject.toml`
- `npm install` in `agent/ui/`: installs the React/Vite frontend

To install separately:

```bash
make install        # Python only
cd agent/ui && npm install   # UI only
```

---

## 6. Storage: AWS S3 or local MinIO

The bucket holds the only copy of every document's text. Ingestion reads it to build pointers,
and the agent reads it again, by byte range, whenever it needs a chunk. Both modes trigger
ingestion **directly**, with no Kafka or other broker (see ADR
`docs/decisions/0001-trigger-ingestion-directly-from-s3.md`).

| Production                   | Local substitute               | Purpose                                     |
| ---------------------------- | ------------------------------ | ------------------------------------------- |
| AWS S3                       | **MinIO** (Docker)             | Object storage + native event notifications |
| AWS Lambda (S3 event target) | **trigger API** (host process) | Receives the event and starts the workflow  |

### AWS S3 (`.env`)

Leave `S3_ENDPOINT_URL` blank and set `S3_BUCKET` and credentials as in section 4. Locally there
is no Lambda, so `make seed` uploads the file and then starts its `IngestWorkflow` itself. With a
Lambda wired to the bucket (section 10), upload with `--no-trigger` and let the event start it.

### Local MinIO (`.env.minio`)

Copy your `.env` to `.env.minio` and change the storage lines:

```bash
S3_ENDPOINT_URL=http://localhost:9000
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
S3_BUCKET=temporal-datasources
MONGODB_DB=temporal-local-minio      # any name other than the one your AWS setup uses
```

Give MinIO its own `MONGODB_DB`. Every pointer records the bucket and key it came from, and
pointers into MinIO are unreadable from the AWS configuration (and the other way round). Sharing a
database would mix the two and leave the agent with chunks it cannot read.

Then pass the file to every `make` command:

```bash
make start ENV_FILE=.env.minio
make index ENV_FILE=.env.minio
make seed  ENV_FILE=.env.minio
```

When the file sets `S3_ENDPOINT_URL`, `make start` first runs `make infra-up`, which:

- starts MinIO (`quay.io/minio/minio`, pinned) on `:9000`, console on `:9001`
- creates `S3_BUCKET` and subscribes its ObjectCreated events to MinIO's **webhook** target
- points the webhook at `http://host.docker.internal:8088/ingest-event`, the trigger API that
  `make start` runs on the host

MinIO buffers events in a `queue_dir`, so events that arrive before the trigger API is up are
replayed once it is (at-least-once). `make seed` also starts the workflow after uploading. Both
starts use the same workflow ID with a terminate-existing policy, so one run survives and the
other is terminated. That is expected.

The S3 client uses path-style addressing whenever an endpoint is set, which MinIO requires.

- Console: [http://localhost:9001](http://localhost:9001) (user `minioadmin`, password `minioadmin`)
- API: `http://localhost:9000`

### Stop / clean

```bash
make infra-down  ENV_FILE=.env.minio   # stop MinIO (keeps its data volume)
make infra-clean ENV_FILE=.env.minio   # stop and delete the volume (wipes MinIO data)
make infra-logs  ENV_FILE=.env.minio   # tail MinIO logs
```

---

## 7. Run the full stack

`make start` starts all services in the background:

```bash
make start                      # AWS S3, reads .env
make start ENV_FILE=.env.minio  # local MinIO, reads .env.minio
```

Starts, in order:

1. MinIO via `make infra-up`, only when the env file sets `S3_ENDPOINT_URL`
2. Temporal dev server (`:7233`, Web UI `:8233`), or reuses one already on `:7233`
3. Temporal worker (`pipeline/worker.py`). `NO_WORKER=1` skips it, so you can run `make worker`
   in the foreground and kill it mid-ingest to show a resume
4. Trigger API (`pipeline/trigger_api.py`, `:8088`), which receives MinIO webhook POSTs at
   `/ingest-event`
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

This creates the `temporalai_search_index` vector index on `KNOWLEDGE_COLLECTION` (`knowledge_zc`
by default) in `MONGODB_DB`, sized to `EMBED_DIM`. The workflow also creates it on first ingest, but running
`make index` first avoids waiting on the first document.

Docs: [Atlas Vector Search](https://www.mongodb.com/docs/atlas/atlas-vector-search/vector-search-overview/)

### Seed a document

```bash
make seed                                       # uploads seed/ethical_governance.md
make seed FILE=./my-doc.md KEY=docs/my-doc.md   # upload your own file
```

Supported format: **Markdown only** (`.md`, `.markdown`, or content type `text/markdown`). Any
other object is refused: the workflow completes with status `unsupported` and removes any pointers
an earlier version of that key left behind.

Watch the workflow run in the Temporal Web UI at [http://localhost:8233](http://localhost:8233).

### Use the agent UI

Open [http://localhost:5173](http://localhost:5173) and ask about the ingested documents. For the
default seed, try "what are the implementation stages for constitutional AI?". The agent answers
only from the knowledge base (plus web search when enabled) and says so when the documents do not
cover a question.

### Stop everything

```bash
make stop           # stops app + Temporal (+ MinIO in MinIO mode)
make stop-app       # stops app processes only (leaves Temporal and MinIO running)
make restart-app    # restart app processes after editing the env file
```

Pass the same `ENV_FILE` you started with.

---

## 8. Verify ingestion

```bash
make seed FILE=./my-doc.md KEY=docs/my-doc.md
# watch Temporal UI: IngestWorkflow → Completed
make query Q="something in that file"
```

`make query` prints the active collection, index and model, then each hit with its score, source
URI, chunk ID and byte span. Under each hit is the text `read_span` fetched from the bucket, or its
status (`stale`, `missing` or `unknown`) when the bytes cannot be read or no longer match.

In Atlas, confirm:

- `knowledge_zc` in your `MONGODB_DB` has one document per chunk, each with `span`,
  `content_hash`, `embedding`, `bucket`, `key` and `etag`.
- No document has a `text` field. In mongosh:
  `db.knowledge_zc.countDocuments({text: {$exists: true}})` returns `0`.
- `chunks_staging` holds nothing for the document once the workflow completes.

---

## 9. Backfill + model cutover

**Backfill is deferred on this branch.** Re-embedding needs chunk text, and `knowledge_zc`
deliberately stores none. `make backfill` starts `BackfillWorkflow`, whose `reembed_and_write`
activity fails straight away with a non-retryable `BackfillDeferred` error.

To move to a new embedding model (for example `voyage-3.5` to a model with different dimensions),
re-ingest from S3 into a second collection and cut over when it is full:

```bash
# 1. Pin retrieval to the current collection so it stays live during the rebuild
make cutover TO=knowledge_zc

# 2. In the env file: KNOWLEDGE_COLLECTION=knowledge_v2, VOYAGE_MODEL=<new>, EMBED_DIM=<new dim>
make restart-app
make index

# 3. Re-seed every object (make seed per file, or make seed-docs for a Markdown repo)

# 4. Once the knowledge_v2 index is READY, flip retrieval to it
make cutover TO=knowledge_v2
make query Q="test question"
```

`cutover` reads the model and dimension from a document in the target collection. The agent reads
the `active` document in `temporal_config` on every query, so no restart is needed after step 4.
The old collection stays in place for rollback.

---

## 10. Production trigger (AWS Lambda)

In production the trigger is an **AWS Lambda** subscribed to the S3 bucket's **ObjectCreated**
event notifications. The Lambda calls `pipeline.lambda_handler`, which runs the **same**
`handle_s3_event` code the local MinIO webhook uses: parse the event, start one `IngestWorkflow`
per object. No Kafka, `sources` collection or Stream Processing is involved. The
`queue_dir`-backed MinIO webhook is the local stand-in for this Lambda.

**Deploy notes:**

- Handler entrypoint: `pipeline.lambda_handler.lambda_handler`; package `pipeline/` with the
  `temporalio` dependency.
- Give the Lambda network egress to your Temporal service and set `TEMPORAL_ADDRESS` and
  `TEMPORAL_NAMESPACE` (plus mTLS cert paths for Temporal Cloud) in its environment.
- Wire the bucket's `s3:ObjectCreated:*` notifications to the Lambda (S3 console or IaC).
- The worker and the agent both need `s3:GetObject` on the bucket: the agent reads chunk text
  from it at query time.

---

## Cloud infra references

When moving from local dev to real cloud infrastructure, use these references:

### MongoDB Atlas

| Topic               | Documentation                                                                                                                |
| ------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| Create a cluster    | [mongodb.com/docs/atlas/tutorial/create-new-cluster](https://www.mongodb.com/docs/atlas/tutorial/create-new-cluster/)        |
| Database users      | [mongodb.com/docs/atlas/security-add-mongodb-users](https://www.mongodb.com/docs/atlas/security-add-mongodb-users/)          |
| Network access      | [mongodb.com/docs/atlas/security/ip-access-list](https://www.mongodb.com/docs/atlas/security/ip-access-list/)                |
| Atlas Vector Search | [mongodb.com/docs/atlas/atlas-vector-search](https://www.mongodb.com/docs/atlas/atlas-vector-search/vector-search-overview/) |

### AWS S3 (production)

Use the AWS S3 settings from section 4: `S3_ENDPOINT_URL` blank, real bucket and credentials.

| Topic                            | Documentation                                                                                                               |
| -------------------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| S3 Getting Started               | [docs.aws.amazon.com/s3/getting-started](https://docs.aws.amazon.com/AmazonS3/latest/userguide/GetStartedWithS3.html)       |
| S3 Event Notifications (→ Lambda) | [docs.aws.amazon.com/s3/event-notifications](https://docs.aws.amazon.com/AmazonS3/latest/userguide/EventNotifications.html) |

### Temporal (production)

| Option                | Documentation                                                                    |
| --------------------- | -------------------------------------------------------------------------------- |
| Temporal Cloud        | [temporal.io/cloud](https://temporal.io/cloud)                                   |
| Self-hosted with Helm | [docs.temporal.io/self-hosted-guide](https://docs.temporal.io/self-hosted-guide) |

Update `TEMPORAL_ADDRESS` in the env file to the Temporal Cloud endpoint, and add mTLS cert paths
for Temporal Cloud connections.

### Voyage AI (via MongoDB Atlas Models)

| Topic                         | Documentation                                                                                                                                             |
| ----------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Atlas Models overview         | [mongodb.com/docs/atlas/ai-integrations](https://www.mongodb.com/docs/atlas/ai-integrations/)                                                             |
| Voyage AI embeddings on Atlas | [mongodb.com/docs/atlas/atlas-vector-search/ai-integrations/voyage-ai](https://www.mongodb.com/docs/atlas/atlas-vector-search/ai-integrations/voyage-ai/) |
| Available embedding models    | [mongodb.com/docs/atlas/ai-integrations/voyage-ai/models](https://www.mongodb.com/docs/atlas/ai-integrations/)                                            |

### OpenAI (research agent)

| Topic            | Documentation                                                                                                          |
| ---------------- | ---------------------------------------------------------------------------------------------------------------------- |
| Agents SDK       | [openai.github.io/openai-agents-python](https://openai.github.io/openai-agents-python/)                                |
| Temporal ↔ Agents | [docs.temporal.io/ai-cookbook/openai-agents-sdk-python](https://docs.temporal.io/ai-cookbook/openai-agents-sdk-python) |
