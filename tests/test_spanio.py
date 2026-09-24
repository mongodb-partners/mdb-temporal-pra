# Tests for the ranged-read path: IfMatch and hash guards, failure statuses,
# and that read_span resolves a chunk_id server-side rather than trusting caller pointers.

from __future__ import annotations

import hashlib

import pytest
from botocore.exceptions import ClientError

from pipeline import spanio

BODY = "# Title\n\nAlpha beta gamma delta.\n".encode("utf-8")
START, END = 9, 20  # "Alpha beta "
SEGMENT = BODY[START:END].decode("utf-8")
SEGMENT_HASH = hashlib.sha256(SEGMENT.encode("utf-8")).hexdigest()


class _Body:
    def __init__(self, data: bytes):
        self._data = data

    def read(self) -> bytes:
        return self._data


class _FakeS3:
    def __init__(self, body: bytes = BODY, error: str | None = None):
        self._body = body
        self._error = error
        self.calls: list[dict] = []

    def get_object(self, **kwargs):
        self.calls.append(kwargs)
        if self._error:
            raise ClientError({"Error": {"Code": self._error}}, "GetObject")
        lo, hi = kwargs["Range"].removeprefix("bytes=").split("-")
        return {"Body": _Body(self._body[int(lo) : int(hi) + 1])}


def test_fetch_range_sends_ifmatch_and_inclusive_range():
    s3 = _FakeS3()
    out = spanio.fetch_range("b", "k.md", "etag1", START, END, client=s3)
    assert out == BODY[START:END]
    assert s3.calls[0]["IfMatch"] == "etag1"
    assert s3.calls[0]["Range"] == f"bytes={START}-{END - 1}"


def test_fetch_span_decodes_strict():
    assert spanio.fetch_span("b", "k.md", "e", START, END, client=_FakeS3()) == SEGMENT


def test_precondition_failed_raises_stale():
    with pytest.raises(spanio.SpanStale):
        spanio.fetch_span("b", "k.md", "e", START, END, client=_FakeS3(error="PreconditionFailed"))


def test_missing_key_raises_missing():
    with pytest.raises(spanio.SpanMissing):
        spanio.fetch_span("b", "k.md", "e", START, END, client=_FakeS3(error="NoSuchKey"))


def test_undecodable_range_raises_stale():
    with pytest.raises(spanio.SpanStale):
        spanio.fetch_span("b", "k.md", "e", 0, 2, client=_FakeS3(body=b"\xff\xfe"))


def test_unexpected_client_error_propagates():
    with pytest.raises(ClientError):
        spanio.fetch_span("b", "k.md", "e", START, END, client=_FakeS3(error="AccessDenied"))


class _FakeColl:
    def __init__(self, doc):
        self._doc = doc
        self.filters: list[dict] = []

    def find_one(self, filt, proj=None):
        self.filters.append(filt)
        return self._doc if self._doc and filt.get("chunk_id") == self._doc["chunk_id"] else None


def _doc(content_hash=SEGMENT_HASH):
    return {
        "chunk_id": "d:0",
        "ordinal": 0,
        "source_uri": "s3://b/k.md",
        "bucket": "b",
        "key": "k.md",
        "etag": "etag1",
        "span": {"kind": "byte", "start": START, "end": END},
        "content_hash": content_hash,
    }


def test_read_span_returns_text_on_match():
    out = spanio.read_span("d:0", collection=_FakeColl(_doc()), client=_FakeS3())
    assert out["status"] == "ok"
    assert out["text"] == SEGMENT
    assert out["source_uri"] == "s3://b/k.md"


def test_read_span_hash_mismatch_is_stale_and_returns_no_text():
    out = spanio.read_span("d:0", collection=_FakeColl(_doc(content_hash="deadbeef")), client=_FakeS3())
    assert out["status"] == "stale"
    assert "text" not in out


def test_read_span_unknown_chunk_id_is_unknown():
    out = spanio.read_span("nope", collection=_FakeColl(_doc()), client=_FakeS3())
    assert out["status"] == "unknown"
    assert "text" not in out


def test_read_span_missing_object():
    out = spanio.read_span("d:0", collection=_FakeColl(_doc()), client=_FakeS3(error="NoSuchKey"))
    assert out["status"] == "missing"
    assert "text" not in out


def test_read_span_takes_no_pointer_arguments():
    import inspect

    params = set(inspect.signature(spanio.read_span).parameters)
    assert params == {"chunk_id", "collection", "client"}
    assert not params & {"bucket", "key", "start", "end", "etag", "span"}


def test_read_span_missing_pointer_fields_degrades_to_status_not_crash():
    """A document that exists under chunk_id but lacks span/bucket/key/etag
    (a legacy pre-branch document, a partially written collection, or a
    pointer aimed at the wrong place) must degrade to a status, not raise
    KeyError out of the one tool a model can call. The legacy doc here
    carries its own `text` field, which must never be passed through:
    only status "ok" is allowed to carry text."""
    legacy_doc = {
        "chunk_id": "d:0",
        "ordinal": 0,
        "source_uri": "s3://b/k.md",
        "text": "legacy inline text that must never reach the caller",
    }
    out = spanio.read_span("d:0", collection=_FakeColl(legacy_doc), client=_FakeS3())
    assert out["status"] != "ok"
    assert "text" not in out


def test_read_span_missing_content_hash_degrades_to_status_not_crash():
    """A doc with a complete bucket/key/etag/span pointer but no content_hash
    is the verification half of a partially written document, and must
    degrade to a status instead of raising KeyError at the final hash check.
    Validate before spending the GetObject: the S3 client must never be
    called for a pointer already known to be unusable."""
    doc = _doc()
    del doc["content_hash"]
    s3 = _FakeS3()
    out = spanio.read_span("d:0", collection=_FakeColl(doc), client=s3)
    assert out["status"] != "ok"
    assert "text" not in out
    assert s3.calls == []


def test_read_span_malformed_span_degrades_to_status_not_crash():
    """A span subdocument present but missing start/end is the partial-write
    case by definition and must degrade to a status instead of raising
    KeyError, again without ever calling S3."""
    doc = _doc()
    doc["span"] = {}
    s3 = _FakeS3()
    out = spanio.read_span("d:0", collection=_FakeColl(doc), client=s3)
    assert out["status"] != "ok"
    assert "text" not in out
    assert s3.calls == []


def test_read_span_rejects_non_string_chunk_id_without_querying():
    """A non-string chunk_id (e.g. a dict) must never reach find_one as a
    Mongo query operator. Assert the query was never issued at all, not
    just that the outcome looks like a miss."""
    coll = _FakeColl(_doc())
    out = spanio.read_span({"$gt": ""}, collection=coll, client=_FakeS3())
    assert out["status"] == "unknown"
    assert "text" not in out
    assert coll.filters == []


def test_default_wiring_reads_the_collection_the_cutover_pointer_names(monkeypatch):
    """Every other test here injects `collection=`, so the default branch that
    resolves the collection itself is the one path no test exercises.

    That branch is load-bearing. `pipeline/retrieval.py` picks its collection from
    the cutover pointer (`get_active()["active_collection"]`), and this resolver
    must pick the SAME one. If it ever resolved from `settings.knowledge_collection`
    instead, the two would agree right up until a blue/green cutover pointed
    retrieval at a green collection, and then every chunk_id search just handed the
    model would come back `unknown` here. The suite would stay green throughout.
    So point the pointer at a sentinel and demand the resolver follows it.
    """
    seen: list = []

    def _fake_knowledge_collection(name=None):
        seen.append(name)
        return _FakeColl(None)

    monkeypatch.setattr(spanio, "get_active", lambda: {"active_collection": "green_zc"})
    monkeypatch.setattr(spanio, "knowledge_collection", _fake_knowledge_collection)

    out = spanio.read_span("doc:0")

    assert seen == ["green_zc"], seen
    assert out == {"chunk_id": "doc:0", "status": "unknown"}


@pytest.mark.parametrize(
    "mutate, why",
    [
        ({"etag": None}, "a null etag reaches boto3 as a parameter-validation error"),
        ({"bucket": None}, "a null bucket reaches boto3 as a parameter-validation error"),
        ({"key": ""}, "an empty key addresses no object"),
        ({"span": {"kind": "byte", "start": "0", "end": "80"}}, "string bounds break range arithmetic"),
        ({"span": {"kind": "byte", "start": None, "end": 80}}, "a null bound breaks range arithmetic"),
        ({"span": {"kind": "byte", "start": 80, "end": 10}}, "an inverted span addresses no bytes"),
        ({"span": {"kind": "byte", "start": -5, "end": 80}}, "a negative offset is not a byte position"),
    ],
)
def test_read_span_degrades_on_a_wrongly_typed_pointer(mutate, why):
    """Presence checks are not enough: these fields come back out of the database.

    A half-written, migrated or hand-edited document can carry a null etag or a
    string byte offset. Those are all present, so a `field in doc` check passes
    them straight to boto3, which raises TypeError or ParamValidationError out of
    the one tool a model can call. read_span returns a status or it returns text,
    never a traceback. The S3 assertion matters as much as the status one: an
    unusable pointer must not cost a ranged GET before it is rejected.
    """
    doc = _doc()
    doc.update(mutate)
    s3 = _FakeS3()

    out = spanio.read_span("d:0", collection=_FakeColl(doc), client=s3)

    assert out["status"] != "ok", why
    assert "text" not in out, why
    assert s3.calls == [], f"{why}: rejected pointers must not spend a GetObject"
