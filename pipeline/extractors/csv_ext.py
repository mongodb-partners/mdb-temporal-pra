"""CSV extractor: deferred. Chunks are rendered as ``header: value`` records that
never appear verbatim in the file, so a byte range cannot reproduce them.
"""

from __future__ import annotations

from .base import Extractor, UnsupportedSource


class CsvExtractor(Extractor):
    name = "csv"

    def ranges(self, text, c2b):
        raise UnsupportedSource("csv is deferred: its chunks are rendered records, not byte ranges")
