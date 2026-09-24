# Tests for the span primitives: char-to-byte mapping, whitespace trimming,
# and index-returning windowing. These underpin every pointer the pipeline stores.

from __future__ import annotations

from pipeline.extractors.base import (
    char_to_byte_map,
    trim_bounds,
    window_indices,
)


def test_char_to_byte_map_is_identity_for_ascii():
    text = "hello"
    assert char_to_byte_map(text) == [0, 1, 2, 3, 4, 5]


def test_char_to_byte_map_counts_multibyte():
    text = "aé中\U0001f600b"  # 1 + 2 + 3 + 4 + 1 bytes
    c2b = char_to_byte_map(text)
    assert c2b == [0, 1, 3, 6, 10, 11]
    assert c2b[len(text)] == len(text.encode("utf-8"))


def test_char_to_byte_map_matches_prefix_encoding():
    text = "﻿# 漢字\n\ncáfé \U0001f600\r\nend"
    c2b = char_to_byte_map(text)
    for i in range(len(text) + 1):
        assert c2b[i] == len(text[:i].encode("utf-8")), i


def test_trim_bounds_strips_both_edges():
    text = "  ab  "
    assert trim_bounds(text, 0, len(text)) == (2, 4)


def test_trim_bounds_on_all_whitespace_collapses():
    text = "    "
    start, end = trim_bounds(text, 0, len(text))
    assert start == end


def test_window_indices_reproduce_substrings():
    text = "x" * 3000
    out = window_indices(text, 0, len(text), size=1200, overlap=150)
    assert out[0] == (0, 1200)
    assert out[1] == (1050, 2250)
    assert out[-1][1] == 3000


def test_window_indices_skip_blank_windows():
    text = "ab" + " " * 50 + "cd"
    out = window_indices(text, 0, len(text), size=2, overlap=0)
    assert [text[a:b] for a, b in out] == ["ab", "cd"]


def test_window_indices_empty_range_returns_nothing():
    assert window_indices("   ", 0, 3, size=10, overlap=0) == []


def test_window_indices_nonpositive_size_returns_trimmed_range():
    text = "  ab  "
    assert window_indices(text, 0, len(text), size=0, overlap=0) == [(2, 4)]
    assert window_indices(text, 0, len(text), size=-5, overlap=0) == [(2, 4)]


def test_window_indices_large_overlap_still_advances():
    text = "abcde"
    out = window_indices(text, 0, len(text), size=2, overlap=3)
    # step = max(1, 2 - 3) = 1, so advances by 1 each iteration
    assert out == [(0, 2), (1, 3), (2, 4), (3, 5), (4, 5)]
    # Verify start indices are strictly increasing
    starts = [s for s, e in out]
    assert starts == sorted(set(starts))
