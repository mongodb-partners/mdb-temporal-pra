# ABOUTME: Tests for refs_from_s3_event — the shared S3/MinIO/SNS event parser.
# Covers SNS unwrapping, TestEvent filtering, key url-decoding, and event-type filtering.

from __future__ import annotations

import json

from pipeline.s3util import refs_from_s3_event


def _record(event_name: str, key: str) -> dict:
    return {
        "eventName": event_name,
        "s3": {"bucket": {"name": "b"}, "object": {"key": key, "eTag": "e", "size": 1}},
    }


def test_object_created_yields_ref():
    refs = refs_from_s3_event({"Records": [_record("s3:ObjectCreated:Put", "a.md")]})
    assert [(r.bucket, r.key) for r in refs] == [("b", "a.md")]


def test_object_removed_is_ignored():
    refs = refs_from_s3_event({"Records": [_record("s3:ObjectRemoved:Delete", "a.md")]})
    assert refs == []


def test_test_event_is_ignored():
    assert refs_from_s3_event({"Event": "s3:TestEvent"}) == []
    assert refs_from_s3_event({}) == []


def test_key_is_url_decoded():
    refs = refs_from_s3_event({"Records": [_record("ObjectCreated:Put", "docs/a%20b.md")]})
    assert refs[0].key == "docs/a b.md"


def test_sns_wrapped_message_is_unwrapped():
    inner = {"Records": [_record("ObjectCreated:Put", "a.md")]}
    refs = refs_from_s3_event({"Message": json.dumps(inner)})
    assert [(r.bucket, r.key) for r in refs] == [("b", "a.md")]


def test_json_string_input_is_parsed():
    refs = refs_from_s3_event(json.dumps({"Records": [_record("ObjectCreated:Put", "a.md")]}))
    assert refs[0].key == "a.md"
