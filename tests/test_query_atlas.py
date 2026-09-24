# Tests for the pure per-hit formatter in infra/query_atlas.py, covering the
# ok snippet path and the missing, stale and unknown read_span statuses.

from __future__ import annotations

import pytest

from infra.query_atlas import format_hit

HIT = {
    "chunk_id": "c1",
    "ordinal": 0,
    "source_uri": "s3://bucket/doc.md",
    "span": {"kind": "char", "start": 9, "end": 400},
    "metadata": {},
    "score": 0.8123,
}


def test_format_hit_ok_collapses_newlines_and_truncates():
    text = ("alpha beta\ngamma delta " * 20).strip()
    resolved = {"chunk_id": "c1", "ordinal": 0, "source_uri": "s3://bucket/doc.md", "status": "ok", "text": text}
    line = format_hit(HIT, resolved)
    snippet = text[:160].replace("\n", " ")
    assert snippet in line
    assert text[161:] not in line


UNVERIFIED = "SECRET-PAYLOAD-THAT-FAILED-VERIFICATION"


@pytest.mark.parametrize("status", ["missing", "stale", "unknown"])
def test_format_hit_renders_the_status_for_every_non_ok_outcome(status):
    resolved = {"chunk_id": "c1", "source_uri": "s3://bucket/doc.md", "status": status}
    line = format_hit(HIT, resolved)
    assert status in line
    assert not line.endswith(UNVERIFIED)


@pytest.mark.parametrize("status", ["missing", "stale", "unknown"])
def test_format_hit_never_renders_text_that_failed_verification(status):
    """Gate on status, not on whether a "text" key happens to be present.

    read_span returns "stale" when the etag or the sha256 no longer matches, which
    means whatever bytes came back are unverified: they are not provably the chunk
    that was indexed. A formatter that prints a snippet whenever one is available
    would surface exactly that content. The earlier version of this test asserted
    `"text" not in resolved` against its own literal, which is true by construction
    and stays true no matter what the formatter does, so it could not catch this.
    Hand the formatter text alongside a failing status and demand it stays hidden.
    """
    resolved = {"chunk_id": "c1", "source_uri": "s3://bucket/doc.md",
                "status": status, "text": UNVERIFIED}
    line = format_hit(HIT, resolved)
    assert UNVERIFIED not in line
    assert status in line


def test_format_hit_does_not_raise_when_a_non_ok_result_omits_text():
    for status in ("missing", "stale", "unknown"):
        format_hit(HIT, {"chunk_id": "c1", "status": status})
