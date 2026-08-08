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

from pymongo import IndexModel
from pymongo.operations import SearchIndexModel

from pipeline.clients import mongo_client
from pipeline.config import settings

_DEFS_PATH = os.path.join(os.path.dirname(__file__), "atlas_indexes.json")


def _existing_index_names(coll) -> set[str]:
    try:
        return {ix["name"] for ix in coll.list_search_indexes()}
    except Exception:  # collection may not exist yet
        return set()


def ensure_collections_and_indexes() -> dict[str, list[str]]:
    """Ensure core MongoDB collections and non-search indexes exist."""
    db = mongo_client()[settings.mongodb_db]
    existing = set(db.list_collection_names())
    created_collections: list[str] = []
    created_indexes: list[str] = []

    required_collections = [
        settings.chunks_collection,
        settings.knowledge_collection,
        settings.knowledge_v2_collection,
        settings.memory_collection,
        settings.config_collection,
    ]

    for name in required_collections:
        if name in existing:
            continue
        db.create_collection(name)
        created_collections.append(name)

    index_specs = {
        settings.chunks_collection: [
            IndexModel([("chunk_id", 1)], name="chunk_id_unique", unique=True),
            IndexModel([("doc_id", 1), ("status", 1), ("ordinal", 1)], name="doc_status_ordinal"),
        ],
        settings.knowledge_collection: [
            IndexModel([("chunk_id", 1)], name="chunk_id_unique", unique=True),
            IndexModel([("doc_id", 1), ("doc_content_hash", 1)], name="doc_hash_lookup"),
            IndexModel([("doc_id", 1), ("ordinal", 1)], name="doc_ordinal"),
        ],
        settings.knowledge_v2_collection: [
            IndexModel([("chunk_id", 1)], name="chunk_id_unique", unique=True),
            IndexModel([("doc_id", 1), ("doc_content_hash", 1)], name="doc_hash_lookup"),
            IndexModel([("doc_id", 1), ("ordinal", 1)], name="doc_ordinal"),
        ],
        settings.memory_collection: [
            IndexModel([("ts", -1)], name="ts_desc"),
        ],
    }

    for coll_name, specs in index_specs.items():
        if not specs:
            continue
        created = db[coll_name].create_indexes(specs)
        created_indexes.extend([f"{coll_name}:{name}" for name in created])

    return {"collections": created_collections, "indexes": created_indexes}


def ensure_atlas_indexes(collection: str | None = None, dim: int | None = None) -> list[str]:
    """Ensure configured Atlas Search indexes exist and return created index names."""
    with open(_DEFS_PATH) as fh:
        defs = json.load(fh)["collections"]

    db = mongo_client()[settings.mongodb_db]
    resolved_dim = dim or settings.embed_dim
    created: list[str] = []

    for coll_name, spec in defs.items():
        if collection and coll_name != collection:
            continue

        # Ensure the collection exists so the search index can attach.
        if coll_name not in db.list_collection_names():
            db.create_collection(coll_name)

        # Apply the dimension override to every vector field.
        for field in spec["definition"]["fields"]:
            if field.get("type") == "vector":
                field["numDimensions"] = resolved_dim

        coll = db[coll_name]
        if spec["name"] in _existing_index_names(coll):
            print(f"[{coll_name}] index '{spec['name']}' already exists — skipping")
            continue

        model = SearchIndexModel(definition=spec["definition"], name=spec["name"], type=spec["type"])
        coll.create_search_index(model)
        created.append(f"{coll_name}:{spec['name']}")
        print(
            f"[{coll_name}] creating '{spec['name']}' "
            f"(dim={resolved_dim}, cosine) — building in the background"
        )

    return created


def main() -> None:
    parser = argparse.ArgumentParser(description="Create Atlas Vector Search indexes.")
    parser.add_argument(
        "--collection",
        default=settings.knowledge_collection,
        help=(
            "Collection to index. Defaults to settings.knowledge_collection "
            f"('{settings.knowledge_collection}')."
        ),
    )
    parser.add_argument("--dim", type=int, help="Override numDimensions (default: EMBED_DIM).")
    args = parser.parse_args()

    ensure_atlas_indexes(collection=args.collection, dim=args.dim)

    print("done. Index builds may take a minute; check status in the Atlas UI or list_search_indexes().")


if __name__ == "__main__":
    main()
