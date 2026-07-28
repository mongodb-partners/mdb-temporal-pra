# Runbook — Part 1: S3 → Kafka → Temporal → Voyage → Atlas

End-to-end flow:

> **Upload a file to S3 → S3 event → Kafka `raw` → Temporal (chunk → Voyage embed) → MongoDB Atlas.**

## 0. Prerequisites

- Python via `uv` (repo pins CPython 3.12).
- Docker (for Redpanda + MinIO) and the `temporal` CLI (`temporal server start-dev`).
- A MongoDB Atlas cluster and a Voyage API key.
- For the **real-AWS path only**: an S3 bucket + AWS credentials.

There are two source modes. Pick one:

- **MinIO (default, self-contained)** — no AWS account, no SQS. MinIO runs in Docker and
  publishes bucket events *natively* to Kafka. Use the `.env.example` defaults as-is.
- **Real AWS S3** — comment out `S3_ENDPOINT_URL` in `.env`, set a real bucket + creds,
  and wire S3 → SQS (§2b) or use the poll fallback.

## 1. Configure

```bash
cp .env.example .env
# MinIO demo: only MONGODB_URI and VOYAGE_API_KEY need filling — S3 defaults point at MinIO.
# Real AWS:   also set S3_BUCKET, AWS creds/region, and (optionally) SQS_QUEUE_URL.
uv sync            # add --extra pdf if you'll ingest PDFs
```

AWS credentials use the standard chain (env vars, `AWS_PROFILE`, or an instance role).
For MinIO, the `.env.example` sets `AWS_ACCESS_KEY_ID=minioadmin` / `...=minioadmin`.

## 2a. MinIO path (default) — nothing to wire

`docker compose up` (below) starts MinIO, creates the `pra-bucket` bucket, and subscribes
its `ObjectCreated` events to the Kafka `s3-events` topic automatically (see the
`minio-setup` service). Skip to §3.

## 2b. Real AWS S3 → SQS notifications (event-driven path)

Only for the real-AWS path. Skip to use the **poll fallback** instead (leave
`SQS_QUEUE_URL` empty; `dispatch` lists the bucket on an interval).

```bash
REGION=us-east-1 ; BUCKET=<your-bucket> ; QUEUE=pra-s3-events
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)

# 1) Create the queue
QURL=$(aws sqs create-queue --queue-name "$QUEUE" --region "$REGION" --query QueueUrl --output text)
QARN=$(aws sqs get-queue-attributes --queue-url "$QURL" --attribute-names QueueArn \
        --region "$REGION" --query Attributes.QueueArn --output text)

# 2) Allow S3 to send to the queue
aws sqs set-queue-attributes --queue-url "$QURL" --region "$REGION" --attributes '{
  "Policy": "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"Service\":\"s3.amazonaws.com\"},\"Action\":\"sqs:SendMessage\",\"Resource\":\"'$QARN'\",\"Condition\":{\"ArnLike\":{\"aws:SourceArn\":\"arn:aws:s3:::'$BUCKET'\"}}}]}"
}'

# 3) Point bucket ObjectCreated events at the queue
aws s3api put-bucket-notification-configuration --bucket "$BUCKET" --region "$REGION" \
  --notification-configuration '{"QueueConfigurations":[{"QueueArn":"'$QARN'","Events":["s3:ObjectCreated:*"]}]}'

echo "SQS_QUEUE_URL=$QURL"   # paste into .env
```

## 3. Start infrastructure

```bash
temporal server start-dev                                  # Web UI: http://localhost:8233
docker compose -f infra/docker-compose.yml up -d           # Redpanda + Console + MinIO (auto-wired)
# Topics (raw, chunks, s3-events) are created by the redpanda-setup container. To (re)create
# them from the host instead: uv run python -m infra.create_topics
uv run python -m infra.create_atlas_index                  # vector indexes on Atlas (dim = EMBED_DIM)
```

MinIO Console: http://localhost:9001 (minioadmin / minioadmin). Confirm the bucket event is
wired: `docker logs pra-minio-setup` should list an `arn:minio:sqs::PRIMARY:kafka` entry.

## 4. Run the pipeline

```bash
# Terminal A — Temporal worker (workflows + activities)
uv run python -m pipeline.worker

# Terminal B — S3 source producer + Kafka dispatchers
uv run python -m pipeline.dispatch
```

## 5. Trigger + verify

```bash
# Upload a file -> S3 event -> pipeline runs
uv run python -m pipeline.seed --file ./somefile.md

# Watch ChunkWorkflow -> EmbedWriteWorkflow complete in the Temporal Web UI (http://localhost:8233)

# Confirm retrieval works against the freshly-embedded data
uv run python -m infra.query_atlas "what does Temporal own in this architecture?"
```

## 6. Prove the durability guarantees

- **#2 Incremental / dedupe:** re-upload the *same* file — `ChunkWorkflow` returns
  `status=duplicate` (content-hash match), no re-embedding.
- **#3 Resume without re-embed:** upload a larger file; while `EmbedWriteWorkflow` is
  mid-flight, kill the worker (Ctrl-C in Terminal A) and restart it. In the UI, already
  completed `embed_chunk` activities are **not** re-run.
- **#4 Backfill:** `uv run python -m pipeline.trigger_backfill --model voyage-3-large`
  reads `knowledge`, re-embeds, and writes `knowledge_v2`. (Create its index at the new
  dimension first: `uv run python -m infra.create_atlas_index --collection knowledge_v2 --dim <n>`.)

## Component map

| Component | File | Role |
| --- | --- | --- |
| S3 → raw | `pipeline/kafka/producers.py` | SQS bridge (events) or bucket poll |
| raw → ChunkWorkflow | `pipeline/kafka/raw_topic.py` | dispatcher |
| ChunkWorkflow | `pipeline/workflows/chunk_workflow.py` | download, dedupe, chunk, hand off |
| chunks → EmbedWriteWorkflow | `pipeline/kafka/chunks_topic.py` | dispatcher |
| EmbedWriteWorkflow | `pipeline/workflows/embed_write_workflow.py` | embed per chunk, upsert Atlas |
| BackfillWorkflow | `pipeline/workflows/backfill_workflow.py` | re-embed → knowledge_v2 |
| activities | `pipeline/activities/` | S3/chunk, Voyage embed, Atlas write |
| worker / dispatch | `pipeline/worker.py`, `pipeline/dispatch.py` | processes to run |
