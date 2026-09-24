"""PDF extractor: deferred. PDF text is extracted, not sliced, so a byte range
cannot reproduce it exactly.
"""

from __future__ import annotations

from .base import Extractor, UnsupportedSource


class PdfExtractor(Extractor):
    name = "pdf"

    def ranges(self, text, c2b):
        raise UnsupportedSource("pdf is deferred: extracted text has no byte range in the file")
