# BackfillWorkflow is deferred (docs/RUNBOOK.md, "Backfill + model cutover"). These tests pin how it
# fails, because the default failure mode of a deferred activity is six identical retries.

from __future__ import annotations

import pytest
from temporalio.exceptions import ApplicationError
from temporalio.testing import ActivityEnvironment

from pipeline.activities import backfill

DOC = {"chunk_id": "d:0", "doc_id": "d", "ordinal": 0}


def _run():
    return ActivityEnvironment().run(backfill.reembed_and_write, DOC, "voyage-3.5", "knowledge_green")


def test_reembed_and_write_is_deferred():
    with pytest.raises(ApplicationError, match="backfill is deferred"):
        _run()


def test_the_deferral_fails_on_the_first_attempt():
    """A plain NotImplementedError is retryable, so Temporal retried this deferral six
    times over roughly a minute before surfacing it. Nothing about a deferred feature
    becomes implemented between attempt one and attempt six, so the retry budget buys
    nothing and buries the reason under identical failures.
    """
    with pytest.raises(ApplicationError) as excinfo:
        _run()

    assert excinfo.value.non_retryable
    assert excinfo.value.type == "BackfillDeferred"


def test_the_deferral_says_where_the_decision_is_recorded():
    """Whoever runs `make backfill` needs the reason, not just the refusal."""
    with pytest.raises(ApplicationError) as excinfo:
        _run()

    assert "docs/RUNBOOK.md" in str(excinfo.value)


class _Cursor:
    def __init__(self, docs):
        self._docs = docs

    def sort(self, *a):
        return self

    def limit(self, n):
        return self

    def __iter__(self):
        return iter(self._docs)


class _Collection:
    def __init__(self):
        self.projection = None

    def find(self, query, projection=None):
        self.projection = projection
        return _Cursor([{"_id": "a" * 24, "chunk_id": "d:0"}])


def test_a_source_batch_leaves_the_vectors_in_mongo(monkeypatch):
    """Activity results are written into workflow history. Fifty chunks with their
    1024-dim vectors came to 1.1 MB, past Temporal's 512 KB payload warning and
    heading for its 2 MB hard limit, and nothing downstream reads the old vector."""
    coll = _Collection()
    monkeypatch.setattr(backfill, "knowledge_collection", lambda name: coll)

    out = ActivityEnvironment().run(backfill.read_source_batch, None, 50, "knowledge_zc")

    assert coll.projection == {"embedding": 0}
    assert out == [{"_id": "a" * 24, "chunk_id": "d:0"}]
