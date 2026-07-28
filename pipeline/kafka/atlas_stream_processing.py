"""Atlas Stream Processing (ASP) — managed alternative to the S3/SQS bridge.

Not used by the S3 demo path, but kept per the PRA: ASP reads change streams (from
Atlas and other sources) and emits records *onto Kafka topics*, replacing an external
Debezium connector. Below is a reference stream-processor definition plus the mongosh
commands to create it. Requires an Atlas Stream Processing instance and a Kafka
connection registered in the Atlas project.

Docs: https://www.mongodb.com/docs/atlas/atlas-stream-processing/
"""

from __future__ import annotations

from .. import config  # noqa: F401  (kept so the module participates in config discovery)

# A processor that tails a source collection's change stream and writes each change
# document to the raw Kafka topic, shaped like our RawRecord contract.
CHANGE_STREAM_TO_KAFKA_PIPELINE = [
    {
        "$source": {
            "connectionName": "atlasClusterConnection",
            "db": "pra",
            "coll": "source_documents",
        }
    },
    {
        "$project": {
            "source": "mongodb",
            "doc_id": {"$toString": "$documentKey._id"},
            "payload": {"$toString": "$fullDocument"},
            "metadata": {"operationType": "$operationType", "ns": "$ns"},
        }
    },
    {
        "$emit": {
            "connectionName": "kafkaConnection",
            "topic": "raw",
        }
    },
]

# mongosh (against the Atlas Stream Processing instance connection string):
#
#   sp.createStreamProcessor("source_to_raw", CHANGE_STREAM_TO_KAFKA_PIPELINE)
#   sp.source_to_raw.start()
#   sp.source_to_raw.stats()
#
# where CHANGE_STREAM_TO_KAFKA_PIPELINE is the array above.
