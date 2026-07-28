"""Upload only Markdown/MDX files from a local repo tree to S3/MinIO.

Run:
    uv run python -m pipeline.seed_repo /path/to/repo
    uv run python -m pipeline.seed_repo /path/to/repo --prefix temporal-docs
    uv run python -m pipeline.seed_repo --repo-url https://github.com/temporalio/documentation.git \
            --checkout-dir .local/imports/temporal-documentation --delay-ms 250

This is a bulk-ingest helper for documentation repositories where only `.md` and
`.mdx` files should trigger the indexing pipeline.
"""

from __future__ import annotations

import argparse
import mimetypes
import subprocess
import time
from pathlib import Path

from .clients import s3_client
from .config import settings

_ALLOWED_SUFFIXES = {".md", ".mdx"}
_SKIP_DIRS = {".git"}


def _iter_markdown_files(root: Path) -> list[Path]:
    files: list[Path] = []
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        if any(part in _SKIP_DIRS for part in path.parts):
            continue
        if path.suffix.lower() not in _ALLOWED_SUFFIXES:
            continue
        files.append(path)
    return sorted(files)


def _key_for(path: Path, root: Path, prefix: str) -> str:
    rel = path.relative_to(root).as_posix()
    clean_prefix = prefix.strip("/")
    if not clean_prefix:
        return rel
    return f"{clean_prefix}/{rel}"


def _checkout_repo(repo_url: str, checkout_dir: Path, ref: str) -> Path:
    checkout_dir.parent.mkdir(parents=True, exist_ok=True)
    if (checkout_dir / ".git").is_dir():
        subprocess.run(
            ["git", "-C", str(checkout_dir), "fetch", "--depth", "1", "origin", ref],
            check=True,
        )
        subprocess.run(
            ["git", "-C", str(checkout_dir), "reset", "--hard", f"origin/{ref}"],
            check=True,
        )
    else:
        subprocess.run(
            ["git", "clone", "--depth", "1", "--branch", ref, repo_url, str(checkout_dir)],
            check=True,
        )
    return checkout_dir.resolve()


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload only .md/.mdx files from a local repo tree.")
    parser.add_argument("source_dir", nargs="?", help="Path to a checked-out local repository or docs folder.")
    parser.add_argument("--repo-url", help="Git repository URL to clone or refresh before uploading.")
    parser.add_argument("--checkout-dir", help="Local checkout path used with --repo-url.")
    parser.add_argument("--ref", default="main", help="Git branch or ref to clone/reset to. Default: main.")
    parser.add_argument(
        "--prefix",
        help="S3 key prefix to write under. Defaults to the source directory name.",
    )
    parser.add_argument("--bucket", default=settings.s3_bucket, help="Override the target bucket.")
    parser.add_argument("--dry-run", action="store_true", help="Print matching files without uploading.")
    parser.add_argument("--delay-ms", type=int, default=0, help="Delay between uploads in milliseconds.")
    args = parser.parse_args()

    if args.repo_url:
        if not args.checkout_dir:
            raise SystemExit("--checkout-dir is required when --repo-url is set.")
        root = _checkout_repo(args.repo_url, Path(args.checkout_dir).expanduser(), args.ref)
    elif args.source_dir:
        root = Path(args.source_dir).expanduser().resolve()
    else:
        raise SystemExit("Provide source_dir or use --repo-url with --checkout-dir.")

    if not root.exists() or not root.is_dir():
        raise SystemExit(f"source_dir is not a directory: {root}")
    if not args.bucket:
        raise SystemExit("S3_BUCKET is not set — populate .env or pass --bucket.")
    if args.delay_ms < 0:
        raise SystemExit("--delay-ms must be >= 0")

    prefix = args.prefix or root.name
    files = _iter_markdown_files(root)
    if not files:
        print(f"no .md/.mdx files found under {root}")
        return

    if args.dry_run:
        for path in files:
            print(_key_for(path, root, prefix))
        print(f"dry run: {len(files)} markdown files matched")
        return

    client = s3_client()
    uploaded = 0
    delay_seconds = args.delay_ms / 1000.0
    for path in files:
        key = _key_for(path, root, prefix)
        content_type = mimetypes.guess_type(path.name)[0] or "text/markdown"
        with path.open("rb") as fh:
            client.put_object(Bucket=args.bucket, Key=key, Body=fh.read(), ContentType=content_type)
        uploaded += 1
        print(f"uploaded s3://{args.bucket}/{key}")
        if delay_seconds > 0:
            time.sleep(delay_seconds)

    print(f"uploaded {uploaded} markdown files from {root} to s3://{args.bucket}/{prefix}/")


if __name__ == "__main__":
    main()