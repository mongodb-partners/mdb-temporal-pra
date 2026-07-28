"""Verification helper: $vectorSearch over the active knowledge collection.

Run:  uv run python -m infra.query_atlas "how does resume without re-embed work?"
"""

from __future__ import annotations

import argparse

from pipeline.config_store import get_active
from pipeline.retrieval import vector_search


def main() -> None:
    parser = argparse.ArgumentParser(description="Vector-search the active knowledge collection.")
    parser.add_argument("query", help="Natural-language query.")
    parser.add_argument("--k", type=int, default=5, help="Number of results.")
    args = parser.parse_args()

    active = get_active()
    print(f"(active: {active['active_collection']} / {active['active_index']} / {active['model']})")

    results = vector_search(args.query, k=args.k)
    if not results:
        print("no results — is data ingested and the index built?")
        return
    for r in results:
        snippet = r["text"][:160].replace("\n", " ")
        print(f"[{r['score']:.4f}] {r['source_uri']} #{r['chunk_id']}\n    {snippet}...\n")


if __name__ == "__main__":
    main()
