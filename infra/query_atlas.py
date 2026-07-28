"""Verification helper: run an Atlas $vectorSearch over the knowledge collection.

Embeds the query with Voyage (input_type=query) and prints the top matches.

Run:  uv run python -m infra.query_atlas "how does resume without re-embed work?"
"""

from __future__ import annotations

import argparse

from pipeline.clients import knowledge_collection, voyage_client
from pipeline.config import settings


def main() -> None:
    parser = argparse.ArgumentParser(description="Vector-search the knowledge collection.")
    parser.add_argument("query", help="Natural-language query.")
    parser.add_argument("--k", type=int, default=5, help="Number of results.")
    parser.add_argument("--collection", default=settings.knowledge_collection)
    args = parser.parse_args()

    qv = voyage_client().embed([args.query], model=settings.voyage_model, input_type="query").embeddings[0]

    coll = knowledge_collection(args.collection)
    pipeline = [
        {
            "$vectorSearch": {
                "index": "vector_index",
                "path": "embedding",
                "queryVector": list(qv),
                "numCandidates": max(100, args.k * 20),
                "limit": args.k,
            }
        },
        {
            "$project": {
                "_id": 0,
                "source_uri": 1,
                "chunk_id": 1,
                "text": 1,
                "score": {"$meta": "vectorSearchScore"},
            }
        },
    ]

    results = list(coll.aggregate(pipeline))
    if not results:
        print("no results — is data ingested and the index built?")
        return
    for r in results:
        snippet = r["text"][:160].replace("\n", " ")
        print(f"[{r['score']:.4f}] {r['source_uri']} #{r['chunk_id']}\n    {snippet}...\n")


if __name__ == "__main__":
    main()
