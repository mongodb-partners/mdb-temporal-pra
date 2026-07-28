"""PDF extractor: per-page text (pypdf), then character-window each page."""

from __future__ import annotations

import io
from typing import Any

from .base import Extractor, window


class PdfExtractor(Extractor):
    name = "pdf"

    def pieces(self, body: bytes) -> list[tuple[str, dict[str, Any]]]:
        from pypdf import PdfReader

        reader = PdfReader(io.BytesIO(body))
        out: list[tuple[str, dict[str, Any]]] = []
        for page_no, page in enumerate(reader.pages, start=1):
            text = page.extract_text() or ""
            for w in window(text, self.chunk_size, self.chunk_overlap):
                out.append((w, {"page": page_no}))
        return out
