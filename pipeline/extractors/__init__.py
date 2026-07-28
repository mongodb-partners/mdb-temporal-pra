"""File-type extractor factory (markdown / pdf / csv / text)."""

from .base import Extractor, RawChunk
from .factory import get_extractor

__all__ = ["Extractor", "RawChunk", "get_extractor"]
