"""File-type extractor factory. Markdown only, because only Markdown
is served back as verbatim byte spans. Other formats are refused, not approximated."""

from .base import Extractor, RawChunk, UnsupportedSource
from .factory import get_extractor

__all__ = ["Extractor", "RawChunk", "UnsupportedSource", "get_extractor"]
