"""Fallback text extractor: decode and character-window the whole document."""

from __future__ import annotations

import json
from typing import Any

from .base import Extractor, window


class TextExtractor(Extractor):
    name = "text"

    def pieces(self, body: bytes) -> list[tuple[str, dict[str, Any]]]:
        text = body.decode("utf-8", errors="replace")
        # Pretty-print JSON so structure survives windowing.
        stripped = text.lstrip()
        if stripped[:1] in "{[":
            try:
                text = json.dumps(json.loads(text), indent=2, ensure_ascii=False)
            except (json.JSONDecodeError, ValueError):
                pass
        return [(w, {}) for w in window(text, self.chunk_size, self.chunk_overlap)]
