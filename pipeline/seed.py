"""Trigger the pipeline by uploading a file to S3 (the real 'file upload' event).

Run:
  uv run python -m pipeline.seed                      # uploads a built-in sample.md
  uv run python -m pipeline.seed --file ./mydoc.pdf   # uploads your own file
  uv run python -m pipeline.seed --key docs/note.md   # choose the S3 key
"""

from __future__ import annotations

import argparse
import mimetypes
import os

from .clients import s3_client
from .config import settings

_SAMPLE = """# Temporal x MongoDB PRA — sample document

This file was uploaded to S3 to trigger the Part 1 pipeline end to end:

    S3 upload -> event -> Kafka raw -> Temporal ChunkWorkflow -> chunks topic
              -> EmbedWriteWorkflow -> Voyage embeddings -> MongoDB Atlas.

Temporal owns orchestration, retries, checkpointing and resumability. MongoDB Atlas is
the single source of truth for the knowledge base, the vector index, and agent memory.
Voyage AI produces the embeddings. A crash mid-embedding resumes without re-embedding
the chunks already completed.
"""


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload a file to S3 to trigger the pipeline.")
    parser.add_argument("--file", help="Path to a local file to upload. Omit to upload a sample.")
    parser.add_argument("--key", help="S3 key to write to. Defaults to the file name (or sample.md).")
    parser.add_argument("--bucket", default=settings.s3_bucket, help="Override the target bucket.")
    args = parser.parse_args()

    if not args.bucket:
        raise SystemExit("S3_BUCKET is not set — populate .env or pass --bucket.")

    if args.file:
        with open(args.file, "rb") as fh:
            body = fh.read()
        key = args.key or os.path.basename(args.file)
        content_type = mimetypes.guess_type(args.file)[0] or "application/octet-stream"
    else:
        body = _SAMPLE.encode("utf-8")
        key = args.key or "sample.md"
        content_type = "text/markdown"

    s3_client().put_object(Bucket=args.bucket, Key=key, Body=body, ContentType=content_type)
    print(f"uploaded s3://{args.bucket}/{key} ({len(body)} bytes, {content_type})")
    print("watch the Temporal Web UI at http://localhost:8233 for ChunkWorkflow -> EmbedWriteWorkflow")


if __name__ == "__main__":
    main()
