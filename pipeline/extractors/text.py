"""Plain text / JSON extractor: deferred. Only Markdown is chunked into byte spans."""

from __future__ import annotations

from .base import Extractor, UnsupportedSource


class TextExtractor(Extractor):
    name = "text"

    def ranges(self, text, c2b):
        raise UnsupportedSource("plain text and json are deferred: only Markdown is supported")
