"""Factory: pick an extractor by file extension / content type.

Markdown only. Everything else is refused rather than silently routed to a
text-storing path, because a partially zero-copy collection is worse than an
honest gap.
"""

from __future__ import annotations

from ..config import settings
from .base import Extractor, UnsupportedSource
from .markdown import MarkdownExtractor

_BY_EXT: dict[str, type[Extractor]] = {
    "md": MarkdownExtractor,
    "markdown": MarkdownExtractor,
}

_BY_MIME: dict[str, type[Extractor]] = {
    "text/markdown": MarkdownExtractor,
    "text/x-markdown": MarkdownExtractor,
}


def get_extractor(key: str, content_type: str = "") -> Extractor:
    """Return the extractor for an object, or raise UnsupportedSource."""
    ext = key.rsplit(".", 1)[-1].lower() if "." in key else ""
    mime = (content_type or "").split(";")[0].strip().lower()
    cls = _BY_EXT.get(ext) or _BY_MIME.get(mime)
    if cls is None:
        raise UnsupportedSource(f"unsupported source type: key={key!r} content_type={mime!r}")
    return cls(chunk_size=settings.chunk_size, chunk_overlap=settings.chunk_overlap)
