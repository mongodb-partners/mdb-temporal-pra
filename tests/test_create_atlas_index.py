# Tests for the pure index-definition selection logic in
# infra/create_atlas_index.py, and that main() actually wires in the
# collection/index bootstrap step. No live Atlas connection is exercised;
# collection and index creation calls are stubbed out.

from __future__ import annotations

import json
import sys

import pytest

from infra import create_atlas_index as cai
from pipeline.config import settings


def _load_defs() -> dict:
    with open(cai._DEFS_PATH) as fh:
        return json.load(fh)["collections"]


def test_default_knowledge_collection_has_a_matching_definition():
    """settings.knowledge_collection must be a real key in atlas_indexes.json,
    or `make index` filters everything out and creates nothing while still
    printing a success message."""
    defs = _load_defs()
    assert settings.knowledge_collection in defs


def test_knowledge_v2_blue_green_target_is_still_defined():
    defs = _load_defs()
    assert "knowledge_v2" in defs


def test_select_definitions_filters_to_the_requested_collection():
    defs = _load_defs()
    selected = cai._select_definitions(defs, settings.knowledge_collection)
    assert set(selected) == {settings.knowledge_collection}


def test_select_definitions_returns_everything_when_no_collection_given():
    defs = _load_defs()
    assert cai._select_definitions(defs, None) == defs


def test_select_definitions_raises_on_an_unmatched_collection_name():
    defs = _load_defs()
    with pytest.raises(ValueError):
        cai._select_definitions(defs, "not_a_real_collection_name")


def test_main_wires_ensure_collections_and_indexes(monkeypatch):
    """ensure_collections_and_indexes is the only thing that creates the
    chunk_id unique index. It must actually be called from main(), not just
    defined."""
    calls: list[str] = []
    monkeypatch.setattr(
        cai, "ensure_collections_and_indexes", lambda: calls.append("collections")
    )
    monkeypatch.setattr(
        cai, "ensure_atlas_indexes", lambda collection=None, dim=None: calls.append("atlas")
    )
    monkeypatch.setattr(sys, "argv", ["create_atlas_index"])

    cai.main()

    assert "collections" in calls
