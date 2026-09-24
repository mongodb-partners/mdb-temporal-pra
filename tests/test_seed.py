# Tests for the seed uploaders: content-type resolution, and starting the ingest
# after upload. Pins that an extension mimetypes cannot guess (e.g. .mdx) still
# uploads as markdown, matching pipeline/seed_repo.py's bulk path rather than
# getting rejected as application/octet-stream by the extractor factory.

from __future__ import annotations

import sys

import pytest

from pipeline import seed, seed_repo


class _FakeS3:
    def __init__(self):
        self.calls: list[dict] = []

    def put_object(self, **kwargs):
        self.calls.append(kwargs)
        return {"ETag": '"etag-1"'}


@pytest.fixture
def started(monkeypatch):
    """Record start_ingests calls instead of contacting Temporal."""
    refs: list = []

    def _fake(batch):
        refs.extend(batch)
        return [f"ingest-{i}" for i, _ in enumerate(batch)]

    monkeypatch.setattr(seed, "start_ingests", _fake)
    monkeypatch.setattr(seed_repo, "start_ingests", _fake)
    return refs


def test_seed_unknown_extension_defaults_to_text_markdown(tmp_path, monkeypatch, started):
    path = tmp_path / "note.mdx"
    path.write_text("# heading\n\nbody text\n")

    fake = _FakeS3()
    monkeypatch.setattr(seed, "s3_client", lambda: fake)
    monkeypatch.setattr(
        sys, "argv", ["seed", "--file", str(path), "--bucket", "test-bucket"]
    )

    seed.main()

    assert len(fake.calls) == 1
    assert fake.calls[0]["ContentType"] == "text/markdown"


def test_seed_known_extension_still_uses_guessed_type(tmp_path, monkeypatch, started):
    path = tmp_path / "note.html"
    path.write_text("<p>hi</p>")

    fake = _FakeS3()
    monkeypatch.setattr(seed, "s3_client", lambda: fake)
    monkeypatch.setattr(
        sys, "argv", ["seed", "--file", str(path), "--bucket", "test-bucket"]
    )

    seed.main()

    assert fake.calls[0]["ContentType"] == "text/html"


def test_seed_starts_the_ingest_for_what_it_uploaded(tmp_path, monkeypatch, started):
    """Nothing emits an object-created event locally, so the seed has to start it."""
    path = tmp_path / "note.md"
    path.write_text("# heading\n")

    monkeypatch.setattr(seed, "s3_client", lambda: _FakeS3())
    monkeypatch.setattr(
        sys, "argv", ["seed", "--file", str(path), "--bucket", "test-bucket", "--key", "docs/n.md"]
    )

    seed.main()

    assert len(started) == 1
    ref = started[0]
    assert (ref.bucket, ref.key, ref.etag) == ("test-bucket", "docs/n.md", "etag-1")
    assert ref.s3_uri == "s3://test-bucket/docs/n.md"


def test_seed_no_trigger_only_uploads(tmp_path, monkeypatch, started):
    path = tmp_path / "note.md"
    path.write_text("# heading\n")

    fake = _FakeS3()
    monkeypatch.setattr(seed, "s3_client", lambda: fake)
    monkeypatch.setattr(
        sys, "argv", ["seed", "--file", str(path), "--bucket", "test-bucket", "--no-trigger"]
    )

    seed.main()

    assert len(fake.calls) == 1
    assert started == []


def test_seed_repo_starts_one_ingest_per_uploaded_file(tmp_path, monkeypatch, started):
    docs = tmp_path / "docs"
    (docs / "sub").mkdir(parents=True)
    (docs / "a.md").write_text("# a\n")
    (docs / "sub" / "b.mdx").write_text("# b\n")
    (docs / "skip.txt").write_text("not markdown\n")

    monkeypatch.setattr(seed_repo, "s3_client", lambda: _FakeS3())
    monkeypatch.setattr(
        sys, "argv", ["seed_repo", str(docs), "--bucket", "test-bucket", "--prefix", "p"]
    )

    seed_repo.main()

    assert sorted(ref.key for ref in started) == ["p/a.md", "p/sub/b.mdx"]
