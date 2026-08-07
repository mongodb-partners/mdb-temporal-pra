# ABOUTME: Tests for the research agent's tool activities (vector_search_tool, rerank_tool).
# Verifies result shapes and that rerank loads chunk text server-side (ids in, ordered ids out).

from __future__ import annotations

import types

import agent.tools as tools


def test_vector_search_tool_shapes_results(monkeypatch):
    fake_hits = [
        {"chunk_id": "d:0", "source_uri": "s3://b/a.md", "ordinal": 0, "text": "alpha", "score": 0.9},
        {"chunk_id": "d:1", "source_uri": "s3://b/a.md", "ordinal": 1, "text": "beta", "score": 0.8},
    ]
    monkeypatch.setattr(tools, "vector_search", lambda query, k=10: fake_hits)

    out = tools.vector_search_tool("q", k=5)

    assert [d["chunk_id"] for d in out] == ["d:0", "d:1"]
    assert out[0] == {"chunk_id": "d:0", "source_uri": "s3://b/a.md", "text": "alpha", "score": 0.9}
    assert set(out[0]) == {"chunk_id", "source_uri", "text", "score"}  # ordinal not leaked


class _FakeColl:
    def __init__(self, docs):
        self._docs = docs

    def find(self, filt, proj=None):
        ids = set(filt["chunk_id"]["$in"])
        return [d for d in self._docs if d["chunk_id"] in ids]


class _FakeVoyage:
    def __init__(self, ordered):
        self._ordered = ordered  # list of (index, relevance_score)
        self.calls = []

    def rerank(self, query, docs, model=None, top_k=None):
        self.calls.append({"query": query, "docs": docs, "top_k": top_k})
        results = [types.SimpleNamespace(index=i, relevance_score=s) for i, s in self._ordered]
        return types.SimpleNamespace(results=results)


def test_rerank_tool_loads_text_and_orders(monkeypatch):
    docs = [
        {"chunk_id": "d:0", "source_uri": "s3://b/a.md", "text": "alpha"},
        {"chunk_id": "d:1", "source_uri": "s3://b/a.md", "text": "beta"},
    ]
    monkeypatch.setattr(tools, "get_active", lambda: {"active_collection": "knowledge"})
    monkeypatch.setattr(tools, "knowledge_collection", lambda name=None: _FakeColl(docs))
    fake_voyage = _FakeVoyage(ordered=[(1, 0.95), (0, 0.40)])  # reranks d:1 above d:0
    monkeypatch.setattr(tools, "voyage_client", lambda: fake_voyage)

    out = tools.rerank_tool("q", ["d:0", "d:1"], top_k=2)

    assert [d["chunk_id"] for d in out] == ["d:1", "d:0"]
    assert out[0] == {"chunk_id": "d:1", "source_uri": "s3://b/a.md", "rerank_score": 0.95}
    assert set(out[0]) == {"chunk_id", "source_uri", "rerank_score"}
    # text was loaded server-side from the collection, not shuttled through tool args:
    assert fake_voyage.calls[0]["docs"] == ["alpha", "beta"]


def test_rerank_tool_empty_ids_returns_empty():
    assert tools.rerank_tool("q", [], top_k=5) == []
