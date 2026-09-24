# Tests that staging persists pointers rather than text, and that the batched
# embed activity re-reads its own byte range, verifies it, and embeds in one call.

from __future__ import annotations

import hashlib

import pytest
from temporalio.testing import ActivityEnvironment

from pipeline import spanio
from pipeline.activities import ingest as ing
from pipeline.config import settings
from pipeline.extractors.base import UnsupportedSource
from pipeline.models import S3Ref, doc_id_for_uri

BODY = ("# Alpha\n\n" + "a" * 300 + "\n\n## Beta\n\n" + "b" * 300 + "\n").encode("utf-8")

# A synthetic multi-chunk source used to exercise batches whose lo != 0, i.e. batches
# that don't start at the front of the object. BODY's two spans start at {0, 311}, so
# every existing test below (which always requests ordinals starting at 0) computes
# lo == 0 and can't tell a correct `- lo` rebase from a dropped one. BIG has six equal
# 50-byte chunks so a batch can be requested that starts mid-object.
BIG = ("A" * 50 + "B" * 50 + "C" * 50 + "D" * 50 + "E" * 50 + "F" * 50).encode("utf-8")
_CHUNK = 50


def _run(fn, *args):
    """Run a sync @activity.defn function the way the worker does: inside an
    ActivityEnvironment. Calling the plain function directly raises
    ``RuntimeError: Not in activity context`` in temporalio 1.33; see
    embed_staged_batch's activity.heartbeat call.
    """
    return ActivityEnvironment().run(fn, *args)


def _stage_big(staging, doc_id, ordinals, *, bucket="b", key="big.md", etag="etag1"):
    """Insert staged pointer docs for BIG directly, bypassing fetch_and_stage_chunks,
    so tests can control exactly which ordinals exist and their status."""
    doc_hash = hashlib.sha256(BIG).hexdigest()
    docs = []
    for i in ordinals:
        start, end = i * _CHUNK, (i + 1) * _CHUNK
        seg = BIG[start:end]
        docs.append(
            {
                "doc_id": doc_id,
                "chunk_id": f"{doc_id}:{i}",
                "ordinal": i,
                "span": {"kind": "byte", "start": start, "end": end},
                "content_hash": hashlib.sha256(seg).hexdigest(),
                "doc_content_hash": doc_hash,
                "source_uri": f"s3://{bucket}/{key}",
                "bucket": bucket,
                "key": key,
                "etag": etag,
                "metadata": {},
                "extractor": "markdown",
                "status": "pending",
                "embedding": None,
            }
        )
    staging.docs.extend(docs)
    return docs


class _Body:
    def __init__(self, data):
        self._data = data

    def read(self):
        return self._data


class _FakeS3:
    def __init__(self, body=BODY):
        self.body = body
        self.calls = []

    def get_object(self, **kw):
        self.calls.append(kw)
        if "Range" not in kw:
            return {"Body": _Body(self.body), "ContentType": "text/markdown", "ETag": '"etag1"'}
        lo, hi = kw["Range"].removeprefix("bytes=").split("-")
        return {"Body": _Body(self.body[int(lo) : int(hi) + 1])}


class _FakeColl:
    def __init__(self, docs=None):
        self.docs = list(docs or [])
        self.deleted = []
        self.bulk = []

    def find_one(self, filt, proj=None):
        for d in self.docs:
            if all(d.get(k) == v for k, v in filt.items()):
                return d
        return None

    def find(self, filt, proj=None):
        def matches(d):
            for k, v in filt.items():
                if isinstance(v, dict) and "$in" in v:
                    if d.get(k) not in v["$in"]:
                        return False
                elif d.get(k) != v:
                    return False
            return True

        return _Cursor([d for d in self.docs if matches(d)])

    def insert_many(self, docs):
        self.docs.extend(docs)

    def delete_many(self, filt):
        self.deleted.append(filt)

    def bulk_write(self, ops, ordered=True):
        self.bulk.extend(ops)


class _Cursor(list):
    def sort(self, key, direction=1):
        return _Cursor(sorted(self, key=lambda d: d[key], reverse=direction < 0))


class _FakeVoyage:
    def __init__(self):
        self.batches = []

    def embed(self, texts, model=None, input_type=None):
        self.batches.append(list(texts))
        return type("R", (), {"embeddings": [[0.1] * 4 for _ in texts]})()


@pytest.fixture
def wired(monkeypatch):
    staging, know, s3, voy = _FakeColl(), _FakeColl(), _FakeS3(), _FakeVoyage()
    monkeypatch.setattr(ing, "_staging", lambda: staging)
    monkeypatch.setattr(ing, "knowledge_collection", lambda name=None: know)
    monkeypatch.setattr(ing, "s3_client", lambda: s3)
    monkeypatch.setattr(ing, "voyage_client", lambda: voy)
    monkeypatch.setattr(spanio, "s3_client", lambda: s3)
    return staging, know, s3, voy


REF = S3Ref.make("b", "doc.md", content_type="text/markdown")


def test_staged_documents_carry_no_text(wired):
    staging, *_ = wired
    out = ing.fetch_and_stage_chunks(REF)
    assert out["status"] == "staged" and out["n"] > 0
    for d in staging.docs:
        assert "text" not in d
        assert set(d) == {
            "doc_id", "chunk_id", "ordinal", "span", "content_hash", "doc_content_hash",
            "source_uri", "bucket", "key", "etag", "metadata", "extractor", "status", "embedding",
        }


def test_staged_spans_reproduce_the_source(wired):
    staging, *_ = wired
    ing.fetch_and_stage_chunks(REF)
    for d in staging.docs:
        seg = BODY[d["span"]["start"] : d["span"]["end"]].decode("utf-8")
        assert hashlib.sha256(seg.encode("utf-8")).hexdigest() == d["content_hash"]


def test_etag_comes_from_the_get_response_not_the_event(wired):
    staging, *_ = wired
    ing.fetch_and_stage_chunks(S3Ref.make("b", "doc.md", etag="stale-from-event"))
    assert {d["etag"] for d in staging.docs} == {"etag1"}


def test_unsupported_type_stages_nothing_and_reports_why(wired, monkeypatch):
    staging, *_ = wired
    monkeypatch.setattr(ing, "get_extractor",
                        lambda *a, **k: (_ for _ in ()).throw(UnsupportedSource("pdf is deferred")))
    out = ing.fetch_and_stage_chunks(S3Ref.make("b", "doc.pdf"))
    assert out["n"] == 0
    assert out["status"] == "unsupported"
    assert "pdf" in out["reason"]
    assert staging.docs == []


def test_embed_batch_makes_one_ranged_get_and_one_embed_call(wired):
    staging, know, s3, voy = wired
    n = ing.fetch_and_stage_chunks(REF)["n"]
    ranged_before = len([c for c in s3.calls if "Range" in c])

    embedded = _run(ing.embed_staged_batch, staging.docs[0]["doc_id"], list(range(n)), None)

    ranged = [c for c in s3.calls if "Range" in c][ranged_before:]
    assert embedded == n
    assert len(ranged) == 1
    assert ranged[0]["IfMatch"] == "etag1"
    assert len(voy.batches) == 1 and len(voy.batches[0]) == n


def test_embed_batch_texts_match_the_staged_hashes(wired):
    staging, *_, voy = wired
    n = ing.fetch_and_stage_chunks(REF)["n"]
    doc_id = staging.docs[0]["doc_id"]
    _run(ing.embed_staged_batch, doc_id, list(range(n)), None)
    sent = voy.batches[0]
    for d, text in zip(sorted(staging.docs, key=lambda x: x["ordinal"]), sent):
        assert hashlib.sha256(text.encode("utf-8")).hexdigest() == d["content_hash"]


def test_embed_batch_refuses_a_span_that_does_not_verify(wired):
    staging, *_ = wired
    n = ing.fetch_and_stage_chunks(REF)["n"]
    staging.docs[0]["content_hash"] = "deadbeef"
    with pytest.raises(RuntimeError, match="verification"):
        _run(ing.embed_staged_batch, staging.docs[0]["doc_id"], list(range(n)), None)


def test_embed_batch_is_idempotent(wired):
    staging, _, s3, voy = wired
    n = ing.fetch_and_stage_chunks(REF)["n"]
    doc_id = staging.docs[0]["doc_id"]
    _run(ing.embed_staged_batch, doc_id, list(range(n)), None)
    for d in staging.docs:
        d["status"] = "embedded"
        d["model"] = "voyage-3.5"
    before = len(s3.calls)
    assert _run(ing.embed_staged_batch, doc_id, list(range(n)), None) == 0
    assert len(s3.calls) == before
    assert len(voy.batches) == 1


def test_per_chunk_embed_activity_is_gone():
    assert not hasattr(ing, "embed_staged_chunk")


def test_embed_batch_rebases_a_nonzero_lo_before_slicing(wired, monkeypatch):
    """Pins the `- lo` rebase: `blob = fetch_range(..., lo, hi)` returns a blob that
    starts at absolute byte `lo`, not at byte 0, so every slice into it must be taken
    relative to `lo` (`blob[start - lo : end - lo]`), not at the span's absolute
    offsets (`blob[start:end]`). BODY-based tests always request ordinal 0, so lo == 0
    there and the two slicing expressions are indistinguishable. This test requests
    ordinals that exclude 0, forcing lo > 0, and asserts on it explicitly.
    """
    staging, _, _, voy = wired
    big_s3 = _FakeS3(body=BIG)
    monkeypatch.setattr(ing, "s3_client", lambda: big_s3)
    monkeypatch.setattr(spanio, "s3_client", lambda: big_s3)

    doc_id = "doc-nonzero-lo"
    docs = _stage_big(staging, doc_id, ordinals=[2, 3, 4])  # bytes 100-250, lo == 100

    embedded = _run(ing.embed_staged_batch, doc_id, [2, 3, 4], None)

    ranged = [c for c in big_s3.calls if "Range" in c]
    assert embedded == 3
    assert len(ranged) == 1
    lo, hi_incl = (int(x) for x in ranged[0]["Range"].removeprefix("bytes=").split("-"))
    assert lo != 0, "lo must be strictly positive for this to test the rebase at all"
    assert lo == 100 and hi_incl + 1 == 250

    sent = voy.batches[0]
    assert len(sent) == 3
    for d, text in zip(sorted(docs, key=lambda x: x["ordinal"]), sent):
        expected = BIG[d["span"]["start"] : d["span"]["end"]].decode("utf-8")
        assert text == expected
        assert hashlib.sha256(text.encode("utf-8")).hexdigest() == d["content_hash"]


def test_embed_batch_mixed_batch_narrows_range_to_the_pending_subset(wired, monkeypatch):
    """When some requested ordinals are already embedded with the current model, `todo`
    drops them, so lo/hi (and the ranged GET) must be computed over the surviving
    pending subset, not the full requested batch. Ordinals 0 and 5 (the outer edges of
    BIG) are pre-marked embedded; only 1..4 are pending, so the fetched range must be
    the pending subset's byte span, strictly narrower than the full batch's.
    """
    staging, _, _, voy = wired
    big_s3 = _FakeS3(body=BIG)
    monkeypatch.setattr(ing, "s3_client", lambda: big_s3)
    monkeypatch.setattr(spanio, "s3_client", lambda: big_s3)

    doc_id = "doc-mixed-batch"
    docs = _stage_big(staging, doc_id, ordinals=[0, 1, 2, 3, 4, 5])
    by_ordinal = {d["ordinal"]: d for d in docs}
    for ordinal in (0, 5):
        by_ordinal[ordinal]["status"] = "embedded"
        by_ordinal[ordinal]["model"] = settings.voyage_model

    embedded = _run(ing.embed_staged_batch, doc_id, [0, 1, 2, 3, 4, 5], None)

    ranged = [c for c in big_s3.calls if "Range" in c]
    assert embedded == 4  # ordinals 1,2,3,4 only
    assert len(ranged) == 1
    lo, hi_incl = (int(x) for x in ranged[0]["Range"].removeprefix("bytes=").split("-"))
    hi = hi_incl + 1

    pending_lo, pending_hi = 1 * _CHUNK, 5 * _CHUNK  # ordinals 1..4: bytes 50-250
    full_lo, full_hi = 0, 6 * _CHUNK  # the full requested batch: bytes 0-300
    assert (lo, hi) == (pending_lo, pending_hi)
    assert lo != 0
    assert (hi - lo) < (full_hi - full_lo)

    sent = voy.batches[0]
    pending_docs = sorted((by_ordinal[i] for i in (1, 2, 3, 4)), key=lambda x: x["ordinal"])
    assert len(sent) == len(pending_docs)
    for d, text in zip(pending_docs, sent):
        expected = BIG[d["span"]["start"] : d["span"]["end"]].decode("utf-8")
        assert text == expected
        assert hashlib.sha256(text.encode("utf-8")).hexdigest() == d["content_hash"]


def _doc_hash(body=BODY):
    return hashlib.sha256(body).hexdigest()


def test_an_unchanged_document_is_not_restaged(wired, monkeypatch):
    """Pins the `unchanged` short-circuit in fetch_and_stage_chunks.

    Without it, every re-ingest of a byte-identical document re-downloads, re-extracts,
    re-stages and (via the workflow) fully re-embeds it. That is silent, unbounded
    embedding spend on work whose result is already in the index, and it is invisible
    from the outside because the end state is correct either way. The workflow-level
    test only pins how the workflow handles an `unchanged` dict a fake hands it; this
    pins the activity that decides to produce one.
    """
    staging, know, _, voy = wired
    doc_id = doc_id_for_uri(REF.s3_uri)
    know.docs.append({"doc_id": doc_id, "chunk_id": f"{doc_id}:0",
                      "doc_content_hash": _doc_hash()})

    extracted = []
    real_get_extractor = ing.get_extractor
    monkeypatch.setattr(ing, "get_extractor",
                        lambda *a, **k: extracted.append(a) or real_get_extractor(*a, **k))

    out = ing.fetch_and_stage_chunks(REF)

    assert out == {"doc_id": doc_id, "doc_hash": _doc_hash(), "n": 0, "status": "unchanged"}
    assert extracted == [], "an unchanged document must not be re-extracted"
    assert staging.docs == [], "an unchanged document must not be re-staged"
    assert staging.deleted == [], "an unchanged document must not disturb staging at all"
    assert voy.batches == []


def test_the_unchanged_check_consults_the_target_collection(wired, monkeypatch):
    """The "already indexed" short-circuit must ask the collection being written to.

    When a target collection is threaded through, checking the default collection
    instead reports `unchanged` for a document the target does not contain, so the
    target never receives it. Here the default (blue) collection holds this exact
    version and the target (green) does not, so the only correct answer is to stage.
    """
    staging, blue, _, _ = wired
    green = _FakeColl()
    asked = []

    def _know(name=None):
        asked.append(name)
        return {"knowledge_green": green}.get(name, blue)

    monkeypatch.setattr(ing, "knowledge_collection", _know)
    doc_id = doc_id_for_uri(REF.s3_uri)
    blue.docs.append({"doc_id": doc_id, "chunk_id": f"{doc_id}:0",
                      "doc_content_hash": _doc_hash()})

    out = ing.fetch_and_stage_chunks(REF, "knowledge_green")

    assert asked == ["knowledge_green"], f"consulted the wrong collection: {asked}"
    assert out["status"] == "staged"
    assert out["n"] > 0
    assert len(staging.docs) == out["n"]


def test_embed_batch_heartbeats_after_the_slow_calls(wired):
    """A heartbeat only buys time for work that comes after it.

    embed_staged_batch's two slow calls are the ranged GET and the embed of up to 32
    documents, and the activity runs under a heartbeat timeout. A single heartbeat
    before both of them reports progress that has not happened yet and leaves the whole
    of both calls unheartbeated. Record how far the activity had got at each heartbeat
    and require at least one after each slow call.
    """
    staging, _, s3, voy = wired
    n = ing.fetch_and_stage_chunks(REF)["n"]
    doc_id = staging.docs[0]["doc_id"]

    progress = []
    env = ActivityEnvironment()
    env.on_heartbeat = lambda *a: progress.append(
        (len([c for c in s3.calls if "Range" in c]), len(voy.batches))
    )

    env.run(ing.embed_staged_batch, doc_id, list(range(n)), None)

    assert progress, "embed_staged_batch never heartbeated"
    assert any(ranged >= 1 for ranged, _ in progress), "no heartbeat after the ranged GET"
    assert any(embeds >= 1 for _, embeds in progress), "no heartbeat after the embed call"


def test_embed_batch_refuses_a_short_embedding_response(wired, monkeypatch):
    """Pairing texts with vectors by zip() silently truncates.

    If the provider returns fewer vectors than texts, the tail of the batch keeps
    status == "pending" while the activity still reports len(todo) embedded and the
    workflow moves on. That is how a non-dense staged ordinal set is reached. Fail
    loudly instead, and write nothing at all.
    """
    staging, *_ = wired
    n = ing.fetch_and_stage_chunks(REF)["n"]
    assert n >= 2, "this test needs a batch it can return a short response for"

    class _ShortVoyage:
        def embed(self, texts, model=None, input_type=None):
            return type("R", (), {"embeddings": [[0.1] * 4 for _ in list(texts)[:-1]]})()

    monkeypatch.setattr(ing, "voyage_client", lambda: _ShortVoyage())

    with pytest.raises(RuntimeError, match="embedding"):
        _run(ing.embed_staged_batch, staging.docs[0]["doc_id"], list(range(n)), None)

    assert staging.bulk == [], "a short response must not leave a partial write behind"
    assert [d["status"] for d in staging.docs] == ["pending"] * n
