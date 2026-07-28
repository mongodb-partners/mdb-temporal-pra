"""Extractor base: turn raw object bytes into ordered chunks.

Each concrete extractor implements ``pieces()`` (type-specific segmentation); the base
assigns global ordinals and drops empties. A shared character-window splitter with overlap
is provided for extractors that need it.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any


@dataclass
class RawChunk:
    ordinal: int
    text: str
    meta: dict[str, Any] = field(default_factory=dict)


def window(text: str, size: int, overlap: int) -> list[str]:
    """Character-window split with overlap. Deterministic; drops blank windows."""
    text = text.strip()
    if not text:
        return []
    if size <= 0:
        return [text]
    step = max(1, size - overlap)
    return [text[i : i + size] for i in range(0, len(text), step) if text[i : i + size].strip()]


class Extractor(ABC):
    """Base extractor. Subclasses set ``name`` and implement ``pieces``."""

    name: str = "base"

    def __init__(self, chunk_size: int, chunk_overlap: int) -> None:
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap

    @abstractmethod
    def pieces(self, body: bytes) -> list[tuple[str, dict[str, Any]]]:
        """Return ordered (text, meta) pairs before ordinal assignment."""

    def chunk(self, body: bytes) -> list[RawChunk]:
        out: list[RawChunk] = []
        for text, meta in self.pieces(body):
            if text and text.strip():
                out.append(RawChunk(ordinal=len(out), text=text, meta={**meta, "extractor": self.name}))
        return out
