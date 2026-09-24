# Round-trip tests for the markdown extractor: every chunk's byte span must
# reproduce that chunk's text exactly, across ASCII, CJK, emoji, combining marks, BOM and CRLF.

from __future__ import annotations

import hashlib

import pytest

from pipeline.extractors.base import UnsupportedSource
from pipeline.extractors.factory import get_extractor
from pipeline.extractors.markdown import MarkdownExtractor

FIXTURES = {
    "ascii": "# Title\n\nAlpha beta gamma.\n\n## Second\n\nDelta epsilon.\n",
    "cjk": "# 漢字の見出し\n\nこれはテストです。\n\n## 第二章\n\nさらにテスト。\n",
    "emoji": "# Hi \U0001f600\n\nText with \U0001f680 and \U0001f1ef\U0001f1f5 flags.\n",
    "combining": "# Café notes\n\nNaïve résumé text.\n",
    "bom": "﻿# BOM leads\n\nBody after a byte order mark.\n",
    "crlf": "# CRLF\r\n\r\nLine one.\r\nLine two.\r\n",
    "no_heading": "Just a paragraph with no heading at all.\n",
}


def _extractor() -> MarkdownExtractor:
    return MarkdownExtractor(chunk_size=40, chunk_overlap=10)


@pytest.mark.parametrize("name", sorted(FIXTURES))
def test_span_reproduces_chunk_text(name):
    body = FIXTURES[name].encode("utf-8")
    chunks = _extractor().chunk(body)
    assert chunks, f"{name} produced no chunks"
    for c in chunks:
        assert body[c.span.start : c.span.end].decode("utf-8") == c.text


@pytest.mark.parametrize("name", sorted(FIXTURES))
def test_chunk_text_has_no_outer_whitespace(name):
    for c in _extractor().chunk(FIXTURES[name].encode("utf-8")):
        assert c.text == c.text.strip()


def test_ordinals_are_dense_and_spans_are_monotonic():
    chunks = _extractor().chunk(FIXTURES["ascii"].encode("utf-8"))
    assert [c.ordinal for c in chunks] == list(range(len(chunks)))
    starts = [c.span.start for c in chunks]
    assert starts == sorted(starts)


def test_bom_stays_inside_the_first_span():
    body = FIXTURES["bom"].encode("utf-8")
    first = _extractor().chunk(body)[0]
    assert first.span.start == 0
    assert first.text.startswith("﻿")


def test_metadata_carries_no_heading_text():
    for c in _extractor().chunk(FIXTURES["ascii"].encode("utf-8")):
        assert "heading" not in c.meta
        assert "extractor" in c.meta
        assert set(c.meta) <= {"extractor", "section", "heading_span"}
        if "heading_span" in c.meta:
            assert set(c.meta["heading_span"]) == {"start", "end"}


def test_heading_span_points_at_the_heading_text():
    body = FIXTURES["ascii"].encode("utf-8")
    chunks = _extractor().chunk(body)
    spans = {c.meta["heading_span"]["start"]: c.meta["heading_span"]["end"]
             for c in chunks if "heading_span" in c.meta}
    titles = {body[s:e].decode("utf-8") for s, e in spans.items()}
    assert titles == {"Title", "Second"}


def test_preamble_before_first_heading_has_no_heading_span():
    body = "Preamble line.\n\n# Later\n\nBody.\n".encode("utf-8")
    chunks = _extractor().chunk(body)
    assert "heading_span" not in chunks[0].meta


def test_content_hash_matches_the_span_bytes():
    body = FIXTURES["cjk"].encode("utf-8")
    for c in _extractor().chunk(body):
        digest = hashlib.sha256(c.text.encode("utf-8")).hexdigest()
        assert hashlib.sha256(body[c.span.start : c.span.end]).hexdigest() == digest


def test_invalid_utf8_is_unsupported():
    with pytest.raises(UnsupportedSource, match="utf-8"):
        _extractor().chunk(b"# ok\n\n\xff\xfe not utf-8\n")


def test_factory_accepts_markdown():
    assert get_extractor("a.md").name == "markdown"
    assert get_extractor("a.markdown").name == "markdown"
    assert get_extractor("a", "text/markdown; charset=utf-8").name == "markdown"


@pytest.mark.parametrize("key", ["a.pdf", "a.csv", "a.txt", "a.json", "noext"])
def test_factory_refuses_everything_else(key):
    with pytest.raises(UnsupportedSource):
        get_extractor(key)
