# Tests the claim at the storage layer: indexed documents carry pointers and
# no text, an emptied document is cleared, and retrieval never projects text.

from __future__ import annotations

import pytest
from temporalio.testing import ActivityEnvironment

from pipeline import retrieval
from pipeline.activities import ingest as ing
from pipeline.config import settings

POINTER_KEYS = {
    "doc_id", "chunk_id", "ordinal", "span", "content_hash", "doc_content_hash",
    "embedding", "model", "dim", "source_uri", "bucket", "key", "etag", "metadata",
}

# `metadata` is the one indexed field whose value is a nested dict, so a top-level
# key-set check says nothing about what is inside it. It is also projected verbatim by
# retrieval.vector_search and handed verbatim to the model, so anything that lands in it
# is both persisted in the searchable collection and rendered. Bound it the same way the
# extractor layer is bounded by test_markdown_spans.test_metadata_carries_no_heading_text.
METADATA_KEYS = {"extractor", "section", "heading_span"}
EXTRACTOR_NAMES = {"markdown", "text", "csv", "pdf"}


def _assert_metadata_cannot_carry_text(meta, where):
    """Every metadata leaf must be an int, a bounded span of ints, or an extractor name.

    No leaf may be free-form text, so no slice of the document can hide in here.
    """
    assert isinstance(meta, dict), f"{where}: metadata must be a dict, got {type(meta)}"
    unexpected = set(meta) - METADATA_KEYS
    assert not unexpected, f"{where}: metadata carries unexpected key(s) {sorted(unexpected)}"
    for key, value in meta.items():
        if key == "extractor":
            assert value in EXTRACTOR_NAMES, f"{where}: metadata.extractor is not a known extractor"
        elif key == "heading_span":
            assert set(value) == {"start", "end"}, f"{where}: metadata.heading_span is not a span"
            assert all(isinstance(v, int) and not isinstance(v, bool) for v in value.values()), (
                f"{where}: metadata.heading_span bounds must be ints (offsets, never text)"
            )
        else:
            assert isinstance(value, int) and not isinstance(value, bool), (
                f"{where}: metadata.{key} must be an int, got {type(value)}"
            )


def _matches(doc, filt):
    """The subset of Mongo filter semantics these fakes need: equality plus $gte/$in/$nin.

    Unknown operators raise rather than silently matching everything, so a change to the
    prune filter cannot quietly turn these fakes into no-ops.
    """
    for key, want in filt.items():
        got = doc.get(key)
        if isinstance(want, dict):
            for op, operand in want.items():
                if op == "$gte":
                    if got is None or not got >= operand:
                        return False
                elif op == "$in":
                    if got not in operand:
                        return False
                elif op == "$nin":
                    if got in operand:
                        return False
                else:
                    raise AssertionError(f"fake collection got unsupported operator {op!r}")
        elif got != want:
            return False
    return True


class _FakeKnow:
    """A knowledge collection that actually stores what it is told to store.

    It has to: index_document's idempotent-retry path counts existing documents, its
    prune must be judged by which documents survive rather than by the filter literal,
    and clear_document's `removed` has to be a number this fake computed from a real
    deletion instead of a constant the test could have asserted against itself.
    """

    def __init__(self, docs=None):
        self.docs = [dict(d) for d in (docs or [])]
        self.upserts = []
        self.deletes = []

    def update_one(self, filt, update, upsert=False):
        doc = update["$set"]
        self.upserts.append(doc)
        for i, existing in enumerate(self.docs):
            if _matches(existing, filt):
                self.docs[i] = {**existing, **doc}
                return
        if upsert:
            self.docs.append(dict(doc))

    def delete_many(self, filt):
        self.deletes.append(filt)
        keep = [d for d in self.docs if not _matches(d, filt)]
        removed = len(self.docs) - len(keep)
        self.docs = keep
        return type("R", (), {"deleted_count": removed})()

    def count_documents(self, filt):
        return sum(1 for d in self.docs if _matches(d, filt))


class _Cursor(list):
    def sort(self, key, direction=1):
        return _Cursor(sorted(self, key=lambda d: d[key], reverse=direction < 0))


class _FakeStaging:
    def __init__(self, docs):
        self.docs = [dict(d) for d in docs]
        self.deletes = []

    def find(self, filt, proj=None):
        return _Cursor([d for d in self.docs if _matches(d, filt)])

    def delete_many(self, filt):
        """Honour the delete. index_document's last side effect is exactly this, and a
        retry that re-enters against staging it did not really empty is not a retry."""
        self.deletes.append(filt)
        keep = [d for d in self.docs if not _matches(d, filt)]
        removed = len(self.docs) - len(keep)
        self.docs = keep
        return type("R", (), {"deleted_count": removed})()


def _staged(i):
    return {
        "doc_id": "d", "chunk_id": f"d:{i}", "ordinal": i,
        "span": {"kind": "byte", "start": 100 * i, "end": 100 * i + 80},
        "content_hash": f"h{i}", "doc_content_hash": "dh",
        "source_uri": "s3://b/k.md", "bucket": "b", "key": "k.md", "etag": "e1",
        "metadata": {"extractor": "markdown", "section": 0},
        "extractor": "markdown", "status": "embedded",
        "embedding": [0.1, 0.2], "model": "voyage-3.5", "dim": 2,
    }


def _indexed(i, doc_id="d"):
    """A document already sitting in the knowledge collection, as index_document writes it."""
    return {"doc_id": doc_id, "chunk_id": f"{doc_id}:{i}", "ordinal": i,
            "doc_content_hash": "dh", "metadata": {"extractor": "markdown", "section": 0}}


@pytest.fixture
def wired(monkeypatch):
    know, staging = _FakeKnow(), _FakeStaging([_staged(0), _staged(1)])
    monkeypatch.setattr(ing, "knowledge_collection", lambda name=None: know)
    monkeypatch.setattr(ing, "_staging", lambda: staging)
    monkeypatch.setattr(ing, "ensure_vector_index", lambda *a, **k: False)
    return know, staging


def test_indexed_documents_carry_pointers_and_no_text(wired):
    know, _ = wired
    out = ing.index_document("d", "dh")
    assert out["indexed"] == 2
    for doc in know.upserts:
        assert "text" not in doc
        assert set(doc) == POINTER_KEYS
        assert set(doc["span"]) == {"kind", "start", "end"}
        # The top-level key set above admits `metadata` as a whole. Nothing there bounds
        # what is inside it, and retrieval projects it straight through to the model, so
        # the contents need their own bound or a text preview tucked into metadata would
        # be both persisted in the searchable collection and rendered.
        _assert_metadata_cannot_carry_text(doc["metadata"], doc["chunk_id"])


def test_indexed_metadata_is_a_closed_set_of_non_text_leaves(wired):
    """Pins the contents of `metadata`, not just its presence as a key.

    The extractor layer is already bounded (test_markdown_spans.py's
    test_metadata_carries_no_heading_text asserts set(c.meta) <= a closed set). That
    bound lives one layer below index_document and is bypassed by anything the index
    layer adds, so assert the same closed set on what is actually written, and assert
    that every leaf is an offset or an extractor name rather than free-form text.
    """
    know, staging = wired
    for d in staging.docs:
        d["metadata"] = {"extractor": "markdown", "section": 1,
                         "heading_span": {"start": 2, "end": 7}}
    ing.index_document("d", "dh")
    assert know.docs, "nothing was written, so nothing was actually checked"
    for doc in know.docs:
        _assert_metadata_cannot_carry_text(doc["metadata"], doc["chunk_id"])


def test_indexing_prunes_a_shortened_document(wired):
    """A previously longer version of the doc leaves ordinals 2 and 3 behind; indexing
    a two-chunk version must remove them. Asserted on what survives, not on the filter
    literal, so the assertion stays honest whatever shape the prune filter takes.
    """
    know, _ = wired
    know.docs.extend([_indexed(i) for i in range(4)])
    ing.index_document("d", "dh")
    assert {doc["chunk_id"] for doc in know.docs} == {"d:0", "d:1"}


def test_indexing_prunes_by_the_ordinals_written_not_by_their_count(monkeypatch):
    """The prune bound must be the set of ordinals actually written, not len(chunks).

    A count is only a valid ordinal bound while the staged embedded ordinals are exactly
    0..n-1. With a gap (0, 1, 5) a count-based prune of {"ordinal": {"$gte": 3}} deletes
    d:5 in the same activity that just upserted it, and still returns indexed=3.
    """
    know = _FakeKnow()
    staging = _FakeStaging([_staged(0), _staged(1), _staged(5)])
    monkeypatch.setattr(ing, "knowledge_collection", lambda name=None: know)
    monkeypatch.setattr(ing, "_staging", lambda: staging)
    monkeypatch.setattr(ing, "ensure_vector_index", lambda *a, **k: False)

    out = ing.index_document("d", "dh")

    assert out["indexed"] == 3
    assert {doc["chunk_id"] for doc in know.docs} == {"d:0", "d:1", "d:5"}, (
        "a chunk this activity wrote was deleted by this activity's own prune"
    )


def test_index_document_is_idempotent_across_a_retry(wired):
    """An at-least-once retry after the writes landed must succeed, not raise.

    index_document's last side effect empties staging, so a lost completion response
    (the ordinary case a retry policy exists for) re-enters with staging empty on a
    document that was already indexed correctly. That must not burn the retry budget
    and fail the workflow, and the second result must be distinguishable from the first
    so a caller can tell a retry from a fresh index.
    """
    know, staging = wired
    first = ing.index_document("d", "dh")
    assert staging.docs == [], "the activity must clear staging, or this is not a retry"

    second = ActivityEnvironment().run(ing.index_document, "d", "dh")

    assert second["status"] == "already_indexed"
    assert first["status"] != second["status"]
    assert second["indexed"] == 2
    assert second["collection"] == first["collection"]
    assert {doc["chunk_id"] for doc in know.docs} == {"d:0", "d:1"}, (
        "the retry must not disturb the documents the first pass wrote"
    )


def test_index_document_refuses_to_wipe_when_nothing_staged(wired):
    """index_document must never be the thing that empties a document. The workflow
    routes n == 0 to clear_document; if a caller reaches index_document anyway with
    no embedded chunks staged, {"ordinal": {"$gte": 0}} would prune every existing
    chunk with no reason recorded anywhere. Refuse instead, and prove nothing was
    wiped by inspecting the fake's recorded delete_many calls.
    """
    know, _ = wired
    with pytest.raises(ValueError, match="d-missing"):
        ActivityEnvironment().run(ing.index_document, "d-missing", "dh")
    assert know.deletes == []


def test_clear_document_removes_every_chunk(wired):
    """`removed` must be the count the activity got back from the deletion, and
    `collection` must be the collection it actually resolved. Both are asserted against
    independently-known values here: the number of `d` documents seeded into the fake,
    and settings.knowledge_collection. Comparing either against the activity's own
    output would hold whatever the activity returned.
    """
    know, staging = wired
    know.docs.extend([_indexed(i) for i in range(3)] + [_indexed(0, doc_id="other")])

    out = ing.clear_document("d", "unsupported")

    assert out == {"doc_id": "d", "removed": 3, "collection": settings.knowledge_collection,
                   "reason": "unsupported"}
    assert {"doc_id": "d"} in know.deletes
    assert {"doc_id": "d"} in staging.deletes
    assert [doc["chunk_id"] for doc in know.docs] == ["other:0"], "cleared the wrong document"


def test_retrieval_projection_has_no_text(monkeypatch):
    captured = {}

    class _Coll:
        def aggregate(self, pipeline):
            captured["pipeline"] = pipeline
            return iter([{"chunk_id": "d:0", "ordinal": 0, "source_uri": "s3://b/k.md",
                          "span": {"kind": "byte", "start": 0, "end": 80},
                          "metadata": {}, "score": 0.9}])

    class _Voy:
        def embed(self, *a, **k):
            return type("R", (), {"embeddings": [[0.1, 0.2]]})()

    monkeypatch.setattr(retrieval, "knowledge_collection", lambda name=None: _Coll())
    monkeypatch.setattr(retrieval, "voyage_client", _Voy)
    monkeypatch.setattr(retrieval, "get_active", lambda: {
        "active_collection": "knowledge_zc", "active_index": "ix",
        "model": "voyage-3.5", "dim": 1024})

    out = retrieval.vector_search("q", k=3)
    projection = captured["pipeline"][1]["$project"]
    assert "text" not in projection
    assert set(projection) == {"_id", "source_uri", "chunk_id", "ordinal", "span", "metadata", "score"}
    assert set(out[0]) == {"chunk_id", "ordinal", "source_uri", "span", "metadata", "score"}


def _fake_workflow(calls, staged, cleared=None):
    """A workflow.execute_activity stand-in that records each activity's name (in
    call order) into `calls`, returns `staged` for fetch_and_stage_chunks, and
    returns `cleared` for any subsequent activity call (clear_document, in practice).
    Shared by every IngestWorkflow branch test below so the call-recording behavior
    stays identical across them.
    """

    class _FakeWorkflow:
        @staticmethod
        async def execute_activity(fn, *args, **kwargs):
            calls.append(getattr(fn, "__name__", str(fn)))
            if calls[-1] == "fetch_and_stage_chunks":
                return staged
            return cleared

    return _FakeWorkflow


@pytest.mark.parametrize("status,reason", [("empty", "document yielded no chunks"),
                                           ("unsupported", "pdf is deferred")])
async def test_workflow_clears_instead_of_returning_early(monkeypatch, status, reason):
    from pipeline.workflows import ingest_workflow as wf
    from pipeline.models import S3Ref

    calls = []
    staged = {"doc_id": "d", "doc_hash": "dh", "n": 0, "status": status, "reason": reason}
    cleared = {"doc_id": "d", "removed": 4, "collection": "knowledge_zc", "reason": reason}
    monkeypatch.setattr(wf, "workflow", _fake_workflow(calls, staged, cleared))

    out = await wf.IngestWorkflow().run(S3Ref.make("b", "k.md"))
    assert calls == ["fetch_and_stage_chunks", "clear_document"]
    assert out["status"] == status
    assert out["removed"] == 4


async def test_workflow_unchanged_does_not_clear(monkeypatch):
    """Pins the `unchanged` early-return: it must call only fetch_and_stage_chunks
    and never clear_document, and the returned dict must carry neither `removed`
    nor `reason`. Those keys are what distinguish a clear from a no-op to callers.
    A future edit that folds `unchanged` into the n == 0 clear branch would wipe a
    healthy document's index on every no-op re-ingest, and asserting on the return
    value alone would not catch it, since both outcomes share the shape
    {"doc_id": ..., "status": ..., "indexed": 0} unless it's the clear branch
    (which also stamps `removed`/`reason`).
    """
    from pipeline.workflows import ingest_workflow as wf
    from pipeline.models import S3Ref

    calls = []
    staged = {"doc_id": "d", "doc_hash": "dh", "n": 0, "status": "unchanged"}
    monkeypatch.setattr(wf, "workflow", _fake_workflow(calls, staged))

    out = await wf.IngestWorkflow().run(S3Ref.make("b", "k.md"))
    assert calls == ["fetch_and_stage_chunks"]
    assert out["status"] == "unchanged"
    assert "removed" not in out
    assert "reason" not in out


def _recording_workflow(seen, staged, indexed):
    """A workflow.execute_activity stand-in that records the args and kwargs each
    activity was invoked with, so the workflow's own wiring (what it threads through,
    and which retry policy it applies where) can be asserted rather than assumed."""

    class _FakeWorkflow:
        @staticmethod
        async def execute_activity(fn, *args, **kwargs):
            name = getattr(fn, "__name__", str(fn))
            seen[name] = {"args": kwargs.get("args", list(args)), "kwargs": kwargs}
            if name == "fetch_and_stage_chunks":
                return staged
            if name == "index_document":
                return indexed
            return 1

    return _FakeWorkflow


async def test_workflow_threads_the_target_collection_through_every_stage(monkeypatch):
    """`target_collection` is a workflow parameter, so every activity that resolves a
    collection has to receive it. Stage 1's "already indexed" check is one of them: if
    it is not given the target, it answers about the wrong collection.
    """
    from pipeline.models import S3Ref
    from pipeline.workflows import ingest_workflow as wf

    seen = {}
    staged = {"doc_id": "d", "doc_hash": "dh", "n": 1, "status": "staged", "extractor": "markdown"}
    indexed = {"doc_id": "d", "indexed": 1, "collection": "knowledge_green", "status": "indexed"}
    monkeypatch.setattr(wf, "workflow", _recording_workflow(seen, staged, indexed))

    await wf.IngestWorkflow().run(S3Ref.make("b", "k.md"), "knowledge_green")

    assert seen["fetch_and_stage_chunks"]["args"][-1] == "knowledge_green"
    assert seen["index_document"]["args"][-1] == "knowledge_green"


async def test_permanent_embed_failures_are_not_retried_forever(monkeypatch):
    """The embed retry policy is unbounded, which is right for rate limits and wrong for
    the two failures that can never succeed on a later attempt:

      - SpanStale: the object was replaced, so the IfMatch etag captured at staging time
        can never match again.
      - RuntimeError: span verification, or a short embedding response, over bytes
        already fetched under a fixed etag.

    Retrying either forever produces a workflow that neither completes nor fails and
    holds a worker slot indefinitely, invisible in any failed-workflow view. Assert the
    policy stops, and assert it is the policy actually applied to the embed activity.
    """
    from pipeline.models import S3Ref
    from pipeline.spanio import SpanStale
    from pipeline.workflows import ingest_workflow as wf

    non_retryable = set(wf._EMBED_RETRY.non_retryable_error_types or ())
    assert SpanStale.__name__ in non_retryable
    assert RuntimeError.__name__ in non_retryable
    assert wf._EMBED_RETRY.maximum_attempts not in (0, None), (
        "an unbounded policy with no non-retryable types retries a permanent failure forever"
    )

    seen = {}
    staged = {"doc_id": "d", "doc_hash": "dh", "n": 1, "status": "staged", "extractor": "markdown"}
    indexed = {"doc_id": "d", "indexed": 1, "collection": "knowledge_zc", "status": "indexed"}
    monkeypatch.setattr(wf, "workflow", _recording_workflow(seen, staged, indexed))
    await wf.IngestWorkflow().run(S3Ref.make("b", "k.md"))

    assert seen["embed_staged_batch"]["kwargs"]["retry_policy"] is wf._EMBED_RETRY


def test_retry_guard_does_not_claim_success_for_a_different_version(wired):
    """The retry branch must answer "is THIS version indexed", not "is anything".

    A terminated predecessor workflow can still finish its own stage 3 after its
    successor has started, because Temporal cannot interrupt a sync activity
    mid-call. Its staging.delete_many then empties staging out from under the
    successor while the collection still holds the PREVIOUS version. Counting by
    doc_id alone, the successor's stage 3 sees a non-zero count, returns
    already_indexed, and the workflow reports success for a version that was
    never written. The count has to be keyed on doc_content_hash so this
    interleaving raises instead.

    Seeded with the OLD hash and called with the NEW one, so no assertion here
    holds against a literal this test wrote into the result.
    """
    know, staging = wired
    staging.docs.clear()
    know.docs.extend([_indexed(i) for i in range(2)])  # doc_content_hash "dh"
    assert all(d["doc_content_hash"] == "dh" for d in know.docs)

    with pytest.raises(ValueError) as exc:
        ing.index_document("d", "dh2")

    assert "dh2" in str(exc.value), "the raise must name the version it could not find"
    assert len(know.docs) == 2, "refusing must not delete the version already indexed"
    assert know.deletes == [], "nothing may be pruned on the refusal path"


def test_retry_guard_still_accepts_a_genuine_retry_of_the_same_version(wired):
    """The N1 fix must not break C1: same hash, staging already consumed, still a retry.

    This is the other half of the pair. Without it the doc_content_hash key could
    be tightened until every re-entry raises, which would fix the false success by
    reintroducing the failure C1 removed.
    """
    know, staging = wired
    staging.docs.clear()
    know.docs.extend([_indexed(i) for i in range(2)])

    out = ActivityEnvironment().run(ing.index_document, "d", "dh")

    assert out["status"] == "already_indexed"
    assert out["indexed"] == 2


def test_activities_write_to_the_collection_they_were_given(monkeypatch):
    """Asserting the returned `collection` name proves nothing about where the
    writes landed: it is a label the activity copied from its own argument.

    The `wired` fixture hands back one fake for every name, so an activity that
    ignored `target_collection` entirely and resolved the default would still
    satisfy every other test in this file. Resolve through a registry keyed by
    name instead, and assert on which collection holds the documents afterwards.
    The default collection must stay untouched: during a blue/green backfill it
    is the one still serving live queries.
    """
    registry: dict[str, _FakeKnow] = {}
    staging = _FakeStaging([_staged(0), _staged(1)])
    monkeypatch.setattr(ing, "knowledge_collection", lambda name=None: registry.setdefault(name, _FakeKnow()))
    monkeypatch.setattr(ing, "_staging", lambda: staging)
    monkeypatch.setattr(ing, "ensure_vector_index", lambda *a, **k: False)

    out = ing.index_document("d", "dh", "knowledge_green")

    assert out["collection"] == "knowledge_green"
    assert [d["chunk_id"] for d in registry["knowledge_green"].docs] == ["d:0", "d:1"]
    assert settings.knowledge_collection not in registry, (
        "the live default collection must not be touched by a backfill into another target"
    )


def test_clear_document_clears_the_collection_it_was_given(monkeypatch):
    """Same gap on the destructive path, where resolving the wrong collection
    would delete a live document instead of the backfill target's copy."""
    registry: dict[str, _FakeKnow] = {}
    registry["knowledge_green"] = _FakeKnow([_indexed(i) for i in range(3)])
    registry[settings.knowledge_collection] = _FakeKnow([_indexed(i) for i in range(3)])
    monkeypatch.setattr(ing, "knowledge_collection", lambda name=None: registry.setdefault(name, _FakeKnow()))
    monkeypatch.setattr(ing, "_staging", lambda: _FakeStaging([]))

    out = ing.clear_document("d", "unsupported", "knowledge_green")

    assert out["removed"] == 3
    assert registry["knowledge_green"].docs == []
    assert len(registry[settings.knowledge_collection].docs) == 3, (
        "clearing a backfill target must not clear the live collection"
    )
