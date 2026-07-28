"""Create Atlas Vector Search indexes from infra/atlas_indexes.json.

numDimensions is taken from EMBED_DIM (.env) unless --dim is passed, so the index
matches the Voyage model you're using.

Run:
  uv run python -m infra.create_atlas_index
  uv run python -m infra.create_atlas_index --collection knowledge_v2 --dim 2048
"""

from __future__ import annotations

import argparse
import json
import os

from pymongo.operations import SearchIndexModel

from pipeline.clients import mongo_client
from pipeline.config import settings

_DEFS_PATH = os.path.join(os.path.dirname(__file__), "atlas_indexes.json")


def _existing_index_names(coll) -> set[str]:
    try:
        return {ix["name"] for ix in coll.list_search_indexes()}
    except Exception:  # collection may not exist yet
        return set()


def main() -> None:
    parser = argparse.ArgumentParser(description="Create Atlas Vector Search indexes.")
    parser.add_argument("--collection", help="Only create the index for this collection.")
    parser.add_argument("--dim", type=int, help="Override numDimensions (default: EMBED_DIM).")
    args = parser.parse_args()

    with open(_DEFS_PATH) as fh:
        defs = json.load(fh)["collections"]

    db = mongo_client()[settings.mongodb_db]
    dim = args.dim or settings.embed_dim

    for coll_name, spec in defs.items():
        if args.collection and coll_name != args.collection:
            continue

        # Ensure the collection exists so the search index can attach.
        if coll_name not in db.list_collection_names():
            db.create_collection(coll_name)

        # Apply the dimension override to every vector field.
        for field in spec["definition"]["fields"]:
            if field.get("type") == "vector":
                field["numDimensions"] = dim

        coll = db[coll_name]
        if spec["name"] in _existing_index_names(coll):
            print(f"[{coll_name}] index '{spec['name']}' already exists — skipping")
            continue

        model = SearchIndexModel(definition=spec["definition"], name=spec["name"], type=spec["type"])
        coll.create_search_index(model)
        print(f"[{coll_name}] creating '{spec['name']}' (dim={dim}, cosine) — building in the background")

    print("done. Index builds may take a minute; check status in the Atlas UI or list_search_indexes().")


if __name__ == "__main__":
    main()
