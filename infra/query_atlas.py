"""Verification helper: $vectorSearch over the active knowledge collection.

Run:  uv run python -m infra.query_atlas "how does resume without re-embed work?"
"""

from __future__ import annotations

import argparse

from pipeline.config_store import get_active
from pipeline.retrieval import vector_search
from pipeline.spanio import read_span


def format_hit(hit: dict, resolved: dict) -> str:
    """Render one search hit plus its resolved read_span outcome as printable lines.

    The hit carries score, source_uri and span (all vector_search projects). The
    resolved dict is what read_span(hit["chunk_id"]) returned: only status "ok"
    carries a "text" key, so every other status is rendered as its status, not
    treated as an error.
    """
    header = f"[{hit['score']:.4f}] {hit['source_uri']} #{hit['chunk_id']} {hit['span']}"
    if resolved["status"] == "ok":
        snippet = resolved["text"][:160].replace("\n", " ")
        return f"{header}\n    {snippet}...\n"
    return f"{header}\n    <{resolved['status']}: text not available>\n"


def main() -> None:
    parser = argparse.ArgumentParser(description="Vector-search the active knowledge collection.")
    parser.add_argument("query", help="Natural-language query.")
    parser.add_argument("--k", type=int, default=5, help="Number of results.")
    args = parser.parse_args()

    active = get_active()
    print(f"(active: {active['active_collection']} / {active['active_index']} / {active['model']})")

    results = vector_search(args.query, k=args.k)
    if not results:
        print("no results. Is data ingested and the index built?")
        return
    for r in results:
        resolved = read_span(r["chunk_id"])
        print(format_hit(r, resolved))


if __name__ == "__main__":
    main()
