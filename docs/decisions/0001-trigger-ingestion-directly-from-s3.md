<!-- ABOUTME: ADR on triggering ingestion directly from S3 events instead of the
     Kafka -> Sink Connector -> sources -> Atlas Stream Processing chain. -->

# 1. Trigger ingestion directly from S3 events

- Status: Accepted
- Date: 2026-07-30
- Deciders: Cornelia Davis (Temporal)

## Context

The reference architecture triggers ingestion through a five-hop chain:

    S3/MinIO drop -> Kafka (s3-events) -> MongoDB Sink Connector
      -> temporal.sources -> Atlas Stream Processing (change stream)
      -> POST /ingest-trigger -> IngestWorkflow

Code investigation shows the chain exists only to deliver a `bucket`+`key` to
`start_workflow`:

- `IngestWorkflow` takes a single `S3Ref` and re-fetches the object bytes from S3
  in its first activity (`pipeline/activities/ingest.py`). It never reads
  `temporal.sources`.
- `temporal.sources` is upsert-by-key (not an audit log); its only consumer is the
  trigger change stream (`pipeline/trigger_listener.py`).
- Re-upload dedup / replace is already guaranteed by Temporal via workflow id
  `ingest-<sha1(s3_uri)>` + `WorkflowIDConflictPolicy.TERMINATE_EXISTING`
  (`pipeline/trigger.py`), independent of the sink's upsert-by-key.
- The repo already supports direct triggering: a webhook source POSTs to
  `/ingest-trigger` (`pipeline/trigger_api.py`), bypassing Kafka, `sources`, and
  ASP entirely (`docs/LLD.md` section 13).

## Decision

For S3-sourced ingestion, prefer triggering Temporal directly from the S3 event:

    S3 event -> start_ingest(S3Ref(bucket, key)) -> IngestWorkflow

reusing the existing `start_ingest` seam (`pipeline/trigger.py`). Concretely:

- Local (MinIO): a small consumer on the existing `s3-events` topic — or a MinIO
  bucket webhook — that calls `start_ingest`. Drops the Sink Connector, `sources`,
  and the change-stream listener.
- Production (AWS S3): S3 Event Notification -> Lambda/EventBridge ->
  `start_workflow("IngestWorkflow", S3Ref(bucket, key))`.

Retain the Kafka/`sources`/ASP chain only when its distinct value is actually needed:
a genuine multi-source fan-in, or an explicit goal of demonstrating the MongoDB
Sink Connector + Change Streams + Stream Processing products.

## Delivery semantics

At-least-once is a *delivery* guarantee (nothing lost in transit), not a
*successful-processing* guarantee. Knowing where it comes from clarifies what direct
triggering does and does not change.

Getting an S3 event to `start_workflow`:

- AWS S3 has exactly four native notification destinations -- SQS, SNS, Lambda,
  EventBridge (one per config; all at-least-once). There is NO native S3 -> Kafka
  destination. MinIO can emit to Kafka natively (which is why the local demo does
  S3 -> Kafka); real S3 cannot, so a production Kafka path still needs
  S3 -> SQS/EventBridge/Lambda -> Kafka -- i.e. Kafka sits downstream of a queue that
  already provides at-least-once.
- The durable at-least-once buffer is the queue (Kafka today; SQS/EventBridge in the
  direct design). Temporal provides idempotency, not delivery: the deterministic
  workflow id `ingest-<sha1(uri)>` collapses duplicate deliveries onto one workflow.
  So: at-least-once transport + idempotent workflow id = effectively-once processing.

Failure handling:

- A DLQ is not a hole in at-least-once; it is the anti-loss backstop. After retries
  exhaust (Lambda async: 2 retries by default) the event is captured in the DLQ
  (durably retained, redrivable) instead of discarded. Here the consumer's only job
  is to call `start_workflow`, so the DLQ only ever holds events that cannot become a
  workflow (malformed event, glue bug) -- genuine poison messages worth a human's
  attention. Once the workflow starts, Temporal owns retries and durability.
- The real weak point is the S3 -> destination notification hop itself, and it is the
  same for all four destinations. S3 notifications are "designed to be delivered at
  least once" but AWS does NOT guarantee delivery, and this hop has no DLQ for any
  destination: if the destination is unavailable beyond S3's short internal retry, the
  notification is lost. (Lambda is partly cushioned -- throttles and concurrency
  limits are retried by Lambda's async queue for up to 6h once the event is accepted,
  so only a true Lambda service outage reduces to short-retry-then-drop.) This
  weakness is upstream of Kafka and Kafka does not fix it; the current local
  MinIO -> Kafka path has it unmitigated (no `queue_dir`).

Mitigations (architecture-independent -- both designs need them):

- Destination choice only tunes the POST-acceptance window; none of the four protect
  the S3 -> destination handoff:
    - SQS         holds the message durably up to 14 days
    - Lambda      retries throttle/system errors up to 6h (then DLQ / discard)
    - EventBridge 24h / ~185 retries to targets, plus archive & replay + target DLQ
    - SNS         immediate fan-out; per-subscriber retry
  Pick per the consumer's needs (EventBridge is a reasonable default once events are
  on the bus), but this choice does NOT close the lost-notification risk.
- Reconcile against S3 as source of truth -- the load-bearing mitigation. The object
  is durably in S3 regardless of notification fate, and a scheduled sweep (S3 Inventory
  or ListObjects) diffed against processed keys in `knowledge`, starting ingest for the
  gaps, catches anything the notification missed -- independent of which destination
  was chosen. This is naturally a Temporal Scheduled Workflow, made safe by the
  idempotent workflow id (re-driving a processed object is a no-op).

Implication: delivery guarantees do not favor the Kafka chain. SQS already provides
at-least-once the moment S3 events land in it, and the one genuine risk (a lost S3
notification) sits upstream of Kafka and is closed by reconciliation, not by Kafka.

## Consequences

Positive:
- Removes four moving parts (Kafka, Sink Connector, `sources` collection, ASP) from
  the S3 path; fewer failure modes and less local infra.
- No loss of at-least-once delivery (provided by the queue) or dedup (Temporal's
  deterministic workflow id) — see Delivery semantics.
- No loss of the Atlas retrieval showcase: Voyage embeddings, the `knowledge`
  vector store, Atlas Vector Search, and agent memory are downstream of the trigger
  and unchanged.

Negative / trade-offs:
- Removing the connector chain would stop demonstrating three MongoDB *integration*
  features (Sink Connector, Change Streams, Stream Processing). Because that demo is a
  goal of this partner artifact, the implementation *retains* the Kafka path as an
  opt-in (`make kafka-up`) rather than deleting it — see Implementation.
- Loses the single source-agnostic fan-in boundary; each new source type would wire
  its own trigger (which the LLD already does for webhooks/CDC).

## Alternatives considered

1. Keep the chain as-is — justified only by multi-source fan-in (premature for one
   source) or the partner-demo goal.
2. Direct trigger, drop Atlas from the trigger path (this ADR) — Atlas remains the
   vector store + embeddings provider on the retrieval side.
3. Direct trigger but still record a source doc in Atlas from inside the workflow —
   keeps an Atlas "landing record" without Kafka/Sink/ASP, if that record is wanted.

## Implementation

Implemented with the webhook path as the **default** local trigger and the Kafka chain
**retained opt-in**, so the MongoDB-connector showcase still runs:

- Shared core `handle_s3_event(client, event)` in `pipeline/trigger.py` wraps the existing
  `refs_from_s3_event` + `start_ingest`. Two thin adapters call it: `POST /ingest-event`
  (`pipeline/trigger_api.py`) for the MinIO webhook, and `pipeline/lambda_handler.py` for
  real S3 via AWS Lambda — the same handler in both.
- Local default: MinIO's `webhook` notify target (with `queue_dir` for at-least-once) POSTs
  each ObjectCreated event to `trigger_api` at `host.docker.internal:8088/ingest-event`.
  `make start` runs `trigger_api`; `make infra-up` starts only MinIO.
- Opt-in showcase: `make kafka-up` (compose `kafka` profile) starts Kafka + Connect, adds the
  Kafka bucket subscription, and registers the sink connector; the `sources` change-stream
  listener (`make trigger-listen`) still works. The deterministic workflow id makes a
  concurrent webhook + Kafka trigger a harmless no-op.
- Tests: `tests/test_handle_s3_event.py` asserts a MinIO event and an AWS event produce the
  identical `start_workflow` call (the "same code both places" guarantee).
- Follow-ups: deploying the Lambda (packaging, Temporal Cloud mTLS, VPC egress) and the
  reconciliation sweep from Delivery semantics.
