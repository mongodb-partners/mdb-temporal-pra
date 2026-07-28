"""CSV extractor: group N rows per chunk, rendered as header + row records.

Row-oriented rather than character-windowed so each chunk is a coherent set of records
the embedding can represent (and a query can match) meaningfully.
"""

from __future__ import annotations

import csv
import io
from typing import Any

from .base import Extractor

ROWS_PER_CHUNK = 50


class CsvExtractor(Extractor):
    name = "csv"

    def pieces(self, body: bytes) -> list[tuple[str, dict[str, Any]]]:
        text = body.decode("utf-8", errors="replace")
        reader = csv.reader(io.StringIO(text))
        rows = list(reader)
        if not rows:
            return []

        header = rows[0]
        data = rows[1:] if len(rows) > 1 else []
        if not data:  # header-only or single-line file
            return [(", ".join(header), {"rows": "header"})]

        out: list[tuple[str, dict[str, Any]]] = []
        for start in range(0, len(data), ROWS_PER_CHUNK):
            batch = data[start : start + ROWS_PER_CHUNK]
            lines = []
            for row in batch:
                pairs = [f"{header[i] if i < len(header) else f'col{i}'}: {val}" for i, val in enumerate(row)]
                lines.append("; ".join(pairs))
            end = start + len(batch)
            out.append(("\n".join(lines), {"rows": f"{start + 1}-{end}", "columns": header}))
        return out
