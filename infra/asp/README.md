# Atlas Stream Processing → Temporal trigger

In production, Atlas Stream Processing (ASP) watches `temporal.sources` and **directly
triggers Temporal** by `$https`-POSTing each new object to the trigger service
(`pipeline/trigger_api.py`, `POST /ingest-trigger`). ASP runs in Atlas and cannot reach a
local Temporal, so for local dev use the change-stream shim instead:

```bash
make trigger-listen        # pipeline/trigger_listener.py — same behavior, runs locally
```

## One-time ASP setup (Atlas)

Prereqs: an Atlas **Stream Processing Instance (SPI)**, a connection to your cluster
registered in the SPI's connection registry (named `atlasCluster` below), and the trigger
service deployed at a URL Atlas can reach (`$TRIGGER_URL`, e.g. `https://<host>/ingest-trigger`).

Connect to the SPI with `mongosh` and create the processor:

```javascript
sp.createStreamProcessor("src_to_temporal", [
  {
    $source: {
      connectionName: "atlasCluster",
      db: "temporal",
      coll: "sources",
      // React to the sink connector's inserts and re-upload replaces.
      config: { fullDocument: "updateLookup" },
    },
  },
  { $match: { operationType: { $in: ["insert", "replace", "update"] } } },
  {
    // MinIO/S3 event shape: Records[0].s3.{bucket.name, object.key}
    $project: {
      bucket: { $arrayElemAt: ["$fullDocument.Records.s3.bucket.name", 0] },
      key: { $arrayElemAt: ["$fullDocument.Records.s3.object.key", 0] },
    },
  },
  {
    $https: {
      connectionName: "temporalTrigger", // registered HTTPS connection -> $TRIGGER_URL base
      path: "/ingest-trigger",
      method: "POST",
      as: "response",
      payload: [
        { $replaceRoot: { newRoot: { bucket: "$bucket", key: "$key" } } },
      ],
    },
  },
]);

sp.src_to_temporal.start();
sp.src_to_temporal.stats();
```

Notes:

- The `$https` operator name/shape follows the Atlas Stream Processing external-function
  syntax; confirm against your Atlas version's docs. If your version lacks `$https`, use
  `$emit` to a Kafka topic and run a tiny consumer that calls `/ingest-trigger`.
- The object `key` from MinIO is URL-encoded (`%2F`); the trigger service / workflow decode it.
- Docs: <https://www.mongodb.com/docs/atlas/atlas-stream-processing/>
