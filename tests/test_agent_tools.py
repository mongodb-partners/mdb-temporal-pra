# ABOUTME: Tests for the research agent's tool activities (vector_search_tool, read_span_tool,
# rerank_tool). Search returns pointers only; text arrives only through a verified span read.

from __future__ import annotations

import types

import agent.tools as tools


def test_vector_search_tool_returns_pointers_without_text(monkeypatch):
    span = {"kind": "byte", "start": 0, "end": 5}
    fake_hits = [
        {"chunk_id": "d:0", "source_uri": "s3://b/a.md", "ordinal": 0, "span": span, "score": 0.9},
        {"chunk_id": "d:1", "source_uri": "s3://b/a.md", "ordinal": 1, "span": span, "score": 0.8},
    ]
    monkeypatch.setattr(tools, "vector_search", lambda query, k=10: fake_hits)

    out = tools.vector_search_tool("q", k=5)

    assert [d["chunk_id"] for d in out] == ["d:0", "d:1"]
    assert out[0] == {
        "chunk_id": "d:0", "source_uri": "s3://b/a.md", "ordinal": 0, "span": span, "score": 0.9,
    }
    assert all("text" not in d for d in out)


def test_read_span_tool_forwards_only_the_chunk_id(monkeypatch):
    calls = []
    monkeypatch.setattr(
        tools, "read_span", lambda chunk_id: calls.append(chunk_id) or {"status": "ok"}
    )

    assert tools.read_span_tool("d:0") == {"status": "ok"}
    assert calls == ["d:0"]


class _FakeVoyage:
    def __init__(self, ordered):
        self._ordered = ordered  # list of (index, relevance_score)
        self.calls = []

    def rerank(self, query, docs, model=None, top_k=None):
        self.calls.append({"query": query, "docs": docs, "top_k": top_k})
        results = [types.SimpleNamespace(index=i, relevance_score=s) for i, s in self._ordered]
        return types.SimpleNamespace(results=results)


def test_rerank_tool_reranks_verified_spans_and_drops_the_rest(monkeypatch):
    spans = {
        "d:0": {"chunk_id": "d:0", "source_uri": "s3://b/a.md", "status": "ok", "text": "alpha"},
        "d:1": {"chunk_id": "d:1", "source_uri": "s3://b/a.md", "status": "ok", "text": "beta"},
        "d:2": {"chunk_id": "d:2", "source_uri": "s3://b/a.md", "status": "stale"},
    }
    monkeypatch.setattr(tools, "read_span", lambda chunk_id: spans[chunk_id])
    fake_voyage = _FakeVoyage(ordered=[(1, 0.95), (0, 0.40)])  # reranks d:1 above d:0
    monkeypatch.setattr(tools, "voyage_client", lambda: fake_voyage)

    out = tools.rerank_tool("q", ["d:0", "d:1", "d:2", "d:0"], top_k=2)

    assert [d["chunk_id"] for d in out] == ["d:1", "d:0"]
    assert out[0] == {
        "chunk_id": "d:1", "source_uri": "s3://b/a.md", "rerank_score": 0.95, "text": "beta",
    }
    # Only verified text reaches the reranker, once per chunk; the stale chunk is dropped.
    assert fake_voyage.calls[0]["docs"] == ["alpha", "beta"]


def test_rerank_tool_empty_ids_returns_empty():
    assert tools.rerank_tool("q", [], top_k=5) == []
