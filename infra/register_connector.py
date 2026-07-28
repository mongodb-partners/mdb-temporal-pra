"""Register (or update) the MongoDB sink connector on Kafka Connect.

Reads infra/connectors/mongo-sink.json, injects MONGODB_URI from settings, and PUTs it
to the Connect REST API (idempotent create-or-update).

Run:  uv run python -m infra.register_connector
"""

from __future__ import annotations

import json
import os
import urllib.request

from pipeline.config import settings

_DEF = os.path.join(os.path.dirname(__file__), "connectors", "mongo-sink.json")
_NAME = "mongo-sink"


def main() -> None:
    if not settings.mongodb_uri:
        raise SystemExit("MONGODB_URI is not set — populate .env first.")

    with open(_DEF) as fh:
        config = json.load(fh)
    config = {k: v for k, v in config.items() if not k.startswith("_")}
    config["connection.uri"] = settings.mongodb_uri

    url = f"{settings.kafka_connect_url}/connectors/{_NAME}/config"
    req = urllib.request.Request(
        url,
        data=json.dumps(config).encode(),
        method="PUT",
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        body = json.loads(resp.read())
    print(f"registered connector '{_NAME}' -> {body.get('config', {}).get('collection')} "
          f"(tasks.max={body.get('config', {}).get('tasks.max')})")

    # Report task state (status lags creation by a moment — poll briefly).
    import time

    status_url = f"{settings.kafka_connect_url}/connectors/{_NAME}/status"
    conn_state, task_states = None, []
    for _ in range(10):
        try:
            with urllib.request.urlopen(status_url, timeout=30) as resp:
                status = json.loads(resp.read())
            conn_state = status.get("connector", {}).get("state")
            task_states = [t.get("state") for t in status.get("tasks", [])]
            if conn_state and task_states:
                break
        except urllib.error.HTTPError as exc:
            if exc.code != 404:
                raise
        time.sleep(1)
    print(f"connector state: {conn_state} | tasks: {task_states or '[pending]'}")


if __name__ == "__main__":
    main()
