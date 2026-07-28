"""Markdown extractor: segment by headings, then character-window each section."""

from __future__ import annotations

import re
from typing import Any

from .base import Extractor, window

_HEADING = re.compile(r"^(#{1,6})\s+(.*)$", re.MULTILINE)


class MarkdownExtractor(Extractor):
    name = "markdown"

    def pieces(self, body: bytes) -> list[tuple[str, dict[str, Any]]]:
        text = body.decode("utf-8", errors="replace")

        # Split into (heading, section-body) spans on the heading boundaries.
        matches = list(_HEADING.finditer(text))
        spans: list[tuple[str, str]] = []
        if not matches:
            spans = [("", text)]
        else:
            if matches[0].start() > 0:
                spans.append(("", text[: matches[0].start()]))
            for i, m in enumerate(matches):
                end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
                spans.append((m.group(2).strip(), text[m.start() : end]))

        out: list[tuple[str, dict[str, Any]]] = []
        for title, section in spans:
            for w in window(section, self.chunk_size, self.chunk_overlap):
                out.append((w, {"heading": title} if title else {}))
        return out
