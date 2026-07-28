"""Factory: pick an extractor by file extension / content type."""

from __future__ import annotations

from ..config import settings
from .base import Extractor
from .csv_ext import CsvExtractor
from .markdown import MarkdownExtractor
from .pdf import PdfExtractor
from .text import TextExtractor

_BY_EXT: dict[str, type[Extractor]] = {
    "md": MarkdownExtractor,
    "markdown": MarkdownExtractor,
    "pdf": PdfExtractor,
    "csv": CsvExtractor,
}

_BY_MIME: dict[str, type[Extractor]] = {
    "text/markdown": MarkdownExtractor,
    "application/pdf": PdfExtractor,
    "text/csv": CsvExtractor,
    "application/csv": CsvExtractor,
}


def get_extractor(key: str, content_type: str = "") -> Extractor:
    """Return the extractor for an object, defaulting to plain text."""
    ext = key.rsplit(".", 1)[-1].lower() if "." in key else ""
    cls = _BY_EXT.get(ext) or _BY_MIME.get((content_type or "").split(";")[0].strip()) or TextExtractor
    return cls(chunk_size=settings.chunk_size, chunk_overlap=settings.chunk_overlap)
