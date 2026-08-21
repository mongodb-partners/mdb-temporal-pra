# ABOUTME: Tests for ingest resumability after a partially-completed index_document.
# Verifies a crash mid-upsert does not make the next attempt believe the doc is unchanged.

from __future__ import annotations

import pytest

from pipeline.activities import ingest
from pipeline.extractors.base import RawChunk
from pipeline.models import S3Ref, doc_id_for_uri, sha256_hex

BODY = b"# Title\n\npara one\n\npara two\n\npara three\n\npara four\n\npara five\n"
N_CHUNKS = 5


# --- fakes (no live Mongo/S3/Voyage; mirrors the FakeClient style in test_handle_s3_event) ---


def _matches(doc: dict, query: dict) -> bool:
    for key, cond in query.items():
        if isinstance(cond, dict):
            if "$gte" in cond and not doc.get(key, 0) >= cond["$gte"]:
                return False
        elif doc.get(key) != cond:
            return False
    return True


class FakeCursor:
    def __init__(self, docs: list[dict]) -> None:
        self._docs = docs

    def sort(self, key: str, direction: int = 1) -> "FakeCursor":
        self._docs.sort(key=lambda d: d.get(key, 0), reverse=direction < 0)
        return self

    def __iter__(self):
        return iter(self._docs)


class FakeCollection:
    """Minimal in-memory stand-in for a pymongo Collection."""

    def __init__(self, docs: list[dict] | None = None) -> None:
        self.docs: list[dict] = list(docs or [])

    def find_one(self, query: dict, projection: dict | None = None) -> dict | None:
        return next((d for d in self.docs if _matches(d, query)), None)

    def find(self, query: dict | None = None) -> FakeCursor:
        return FakeCursor([d for d in self.docs if _matches(d, query or {})])

    def count_documents(self, query: dict) -> int:
        return sum(1 for d in self.docs if _matches(d, query))

    def insert_many(self, docs: list[dict]) -> None:
        self.docs.extend(dict(d) for d in docs)

    def insert_one(self, doc: dict) -> None:
        self.docs.append(dict(doc))

    def delete_many(self, query: dict) -> None:
        self.docs = [d for d in self.docs if not _matches(d, query)]

    def update_one(self, query: dict, update: dict, upsert: bool = False) -> None:
        existing = self.find_one(query)
        if existing is not None:
            existing.update(update["$set"])
        elif upsert:
            self.docs.append({**query, **update["$set"]})


class FlakyCollection(FakeCollection):
    """Fails on the Nth update_one — simulates a worker/Mongo death mid-loop."""

    def __init__(self, fail_on_call: int) -> None:
        super().__init__()
        self.fail_on_call = fail_on_call
        self.calls = 0

    def update_one(self, query: dict, update: dict, upsert: bool = False) -> None:
        self.calls += 1
        if self.calls == self.fail_on_call:
            raise ConnectionError("simulated Mongo failure mid-index_document")
        super().update_one(query, update, upsert)


class FakeExtractor:
    name = "fake"

    def chunk(self, body: bytes) -> list[RawChunk]:
        return [RawChunk(ordinal=i, text=f"chunk text {i}", meta={}) for i in range(N_CHUNKS)]


@pytest.fixture
def ref() -> S3Ref:
    return S3Ref.make("bucket", "docs/a.md", content_type="text/markdown")


@pytest.fixture
def wiring(monkeypatch, ref):
    """Patch the S3/Mongo/extractor/index seams in pipeline.activities.ingest."""
    know = FakeCollection()
    staging = FakeCollection()

    class FakeBody:
        def read(self) -> bytes:
            return BODY

    monkeypatch.setattr(
        ingest, "s3_client", lambda: type("S3", (), {"get_object": staticmethod(lambda **_: {"Body": FakeBody()})})()
    )
    monkeypatch.setattr(ingest, "knowledge_collection", lambda name=None: know)
    monkeypatch.setattr(ingest, "_staging", lambda: staging)
    monkeypatch.setattr(ingest, "get_extractor", lambda key, ct: FakeExtractor())
    monkeypatch.setattr(ingest, "ensure_vector_index", lambda *a, **k: False)
    return know, staging


def _stage_embedded(staging: FakeCollection, doc_id: str, doc_hash: str) -> None:
    staging.insert_many([
        {
            "doc_id": doc_id,
            "chunk_id": f"{doc_id}:{i}",
            "ordinal": i,
            "text": f"chunk text {i}",
            "content_hash": sha256_hex(f"chunk text {i}"),
            "doc_content_hash": doc_hash,
            "source_uri": "s3://bucket/docs/a.md",
            "metadata": {},
            "status": "embedded",
            "embedding": [0.1] * 3,
            "model": "voyage-3",
            "dim": 3,
        }
        for i in range(N_CHUNKS)
    ])


# --- the tests ---


def test_partial_index_leaves_new_hash_on_a_subset(monkeypatch, wiring, ref):
    """Precondition: a stage-3 crash leaves *some* chunks stamped with the new doc hash.

    This documents current behaviour (it passes today) — it is what makes the next test's
    bug reachable rather than hypothetical.
    """
    know, staging = wiring
    doc_id, doc_hash = doc_id_for_uri(ref.s3_uri), sha256_hex(BODY)
    _stage_embedded(staging, doc_id, doc_hash)

    flaky = FlakyCollection(fail_on_call=3)
    monkeypatch.setattr(ingest, "knowledge_collection", lambda name=None: flaky)

    with pytest.raises(ConnectionError):
        ingest.index_document(doc_id, doc_hash)

    assert len(flaky.docs) == 2, "expected a partial write: 2 of 5 chunks landed"
    assert all(d["doc_content_hash"] == doc_hash for d in flaky.docs)
    assert staging.docs, "staging must survive so a retry can resume"


def test_reingest_after_partial_index_restages_all_chunks(wiring, ref):
    """A half-indexed doc must be re-staged, not reported unchanged.

    Reproduces the bug: one orphan chunk carrying the new hash satisfies the existence
    check in fetch_and_stage_chunks, so the workflow short-circuits and the document
    stays permanently half-indexed.
    """
    know, staging = wiring
    doc_id, doc_hash = doc_id_for_uri(ref.s3_uri), sha256_hex(BODY)

    # Wreckage from the crashed attempt above: 2 of 5 chunks, both with the NEW hash.
    for i in range(2):
        know.insert_one({
            "doc_id": doc_id,
            "chunk_id": f"{doc_id}:{i}",
            "ordinal": i,
            "doc_content_hash": doc_hash,
        })

    result = ingest.fetch_and_stage_chunks(ref)

    assert result["status"] == "staged", (
        f"half-indexed doc reported as {result['status']!r} — the remaining "
        f"{N_CHUNKS - 2} chunks will never be indexed"
    )
    assert result["n"] == N_CHUNKS


def test_fully_indexed_doc_is_still_reported_unchanged(wiring, ref):
    """The completeness check must not cost us the short-circuit it replaces.

    Guards against "fix" the bug by never short-circuiting: a document that really did
    finish indexing must still skip re-staging and re-embedding.
    """
    know, staging = wiring
    doc_id, doc_hash = doc_id_for_uri(ref.s3_uri), sha256_hex(BODY)
    _stage_embedded(staging, doc_id, doc_hash)
    ingest.index_document(doc_id, doc_hash)  # completes; staging is cleared

    result = ingest.fetch_and_stage_chunks(ref)

    assert result["status"] == "unchanged"
    assert result["n"] == 0
    assert not staging.docs, "an unchanged doc must not be re-staged"


def test_reingest_restages_when_stale_chunks_survived_the_prune(wiring, ref):
    """A crash between the last upsert and the stale-chunk prune is also incomplete.

    All N_CHUNKS chunks carry the new hash, but leftovers from a longer previous version
    were never pruned, so the doc is not in the state a finished index_document produces.
    """
    know, staging = wiring
    doc_id, doc_hash = doc_id_for_uri(ref.s3_uri), sha256_hex(BODY)

    for i in range(N_CHUNKS):
        know.insert_one({
            "doc_id": doc_id,
            "chunk_id": f"{doc_id}:{i}",
            "ordinal": i,
            "doc_content_hash": doc_hash,
            "doc_chunk_count": N_CHUNKS,
        })
    for i in (N_CHUNKS, N_CHUNKS + 1):  # tail of the older, longer version
        know.insert_one({
            "doc_id": doc_id,
            "chunk_id": f"{doc_id}:{i}",
            "ordinal": i,
            "doc_content_hash": "older-hash",
            "doc_chunk_count": N_CHUNKS + 2,
        })

    result = ingest.fetch_and_stage_chunks(ref)

    assert result["status"] == "staged", "stale chunks left searchable — the prune never ran"
    assert result["n"] == N_CHUNKS
