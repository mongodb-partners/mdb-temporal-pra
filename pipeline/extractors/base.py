"""Extractor base: turn raw object bytes into ordered chunks plus byte spans.

Subclasses implement ``ranges()``, which returns character ranges over the decoded
text. The base converts those to byte spans through one prefix array per document,
so the invariant ``body[span.start:span.end].decode() == chunk.text`` is enforced
in exactly one place.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

from ..models import Span


class UnsupportedSource(Exception):
    """The object cannot be indexed zero-copy. Carries a human-readable reason."""


@dataclass
class RawChunk:
    ordinal: int
    text: str  # in-memory only; never persisted
    span: Span
    meta: dict[str, Any] = field(default_factory=dict)


def char_to_byte_map(text: str) -> list[int]:
    """Prefix array where ``out[i]`` is the UTF-8 byte offset of character ``i``.

    Built once per document. Re-encoding a prefix per chunk would be quadratic on a
    400-chunk file.
    """
    out = [0] * (len(text) + 1)
    total = 0
    for i, ch in enumerate(text):
        out[i] = total
        total += len(ch.encode("utf-8"))
    out[len(text)] = total
    return out


def trim_bounds(text: str, start: int, end: int) -> tuple[int, int]:
    """Move both edges inward past whitespace. Returns ``(start, start)`` if all blank."""
    while start < end and text[start].isspace():
        start += 1
    while end > start and text[end - 1].isspace():
        end -= 1
    return start, end


def window_indices(
    text: str, start: int, end: int, size: int, overlap: int
) -> list[tuple[int, int]]:
    """Character-window a range with overlap, returning index pairs rather than strings."""
    start, end = trim_bounds(text, start, end)
    if start >= end:
        return []
    if size <= 0:
        return [(start, end)]
    step = max(1, size - overlap)
    out: list[tuple[int, int]] = []
    for i in range(start, end, step):
        w0, w1 = trim_bounds(text, i, min(i + size, end))
        if w0 < w1:
            out.append((w0, w1))
    return out


class Extractor(ABC):
    """Base extractor. Subclasses set ``name`` and implement ``ranges``."""

    name: str = "base"

    def __init__(self, chunk_size: int, chunk_overlap: int) -> None:
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap

    @abstractmethod
    def ranges(self, text: str, c2b: Sequence[int]) -> list[tuple[int, int, dict[str, Any]]]:
        """Return ordered ``(char_start, char_end, meta)`` triples over ``text``.

        Any offsets placed in ``meta`` must already be byte offsets, converted
        through ``c2b`` by the subclass.
        """

    def chunk(self, body: bytes) -> list[RawChunk]:
        try:
            text = body.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise UnsupportedSource(f"not valid utf-8: {exc}") from exc

        c2b = char_to_byte_map(text)
        if c2b[len(text)] != len(body):
            raise UnsupportedSource("byte map does not cover the object")

        out: list[RawChunk] = []
        for cs, ce, meta in self.ranges(text, c2b):
            out.append(
                RawChunk(
                    ordinal=len(out),
                    text=text[cs:ce],
                    span=Span(start=c2b[cs], end=c2b[ce]),
                    meta={**meta, "extractor": self.name},
                )
            )
        return out
