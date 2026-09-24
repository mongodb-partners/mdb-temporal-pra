"""Markdown extractor: segment by headings, then character-window each section.

Emits character ranges rather than strings so the base class can convert them to
byte spans. The heading is carried as a byte range, never as text.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from typing import Any

from .base import Extractor, trim_bounds, window_indices

_HEADING = re.compile(r"^(#{1,6})\s+(.*)$", re.MULTILINE)


class MarkdownExtractor(Extractor):
    name = "markdown"

    def ranges(self, text: str, c2b: Sequence[int]) -> list[tuple[int, int, dict[str, Any]]]:
        matches = list(_HEADING.finditer(text))

        # (section_start, section_end, title_start, title_end); -1 titles mean "no heading".
        sections: list[tuple[int, int, int, int]] = []
        if not matches:
            sections.append((0, len(text), -1, -1))
        else:
            if matches[0].start() > 0:
                sections.append((0, matches[0].start(), -1, -1))
            for i, m in enumerate(matches):
                end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
                t0, t1 = trim_bounds(text, m.start(2), m.end(2))
                sections.append((m.start(), end, t0, t1))

        out: list[tuple[int, int, dict[str, Any]]] = []
        for index, (s, e, t0, t1) in enumerate(sections):
            meta: dict[str, Any] = {"section": index}
            if t0 >= 0 and t1 > t0:
                meta["heading_span"] = {"start": c2b[t0], "end": c2b[t1]}
            for w0, w1 in window_indices(text, s, e, self.chunk_size, self.chunk_overlap):
                out.append((w0, w1, dict(meta)))
        return out
