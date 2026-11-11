# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded predecessor streams preserve commitments and honor every SQL result."""

from types import SimpleNamespace

import pytest

from db import migration_rooted_publication_freeze as freeze
from process import provider_directory_rooted_graph_legacy_validation as validation
from tests.rooted_predecessor_component_support import LegacyPages
from tests.test_provider_directory_rooted_graph_publication_freeze import SCHEMA, _current_bodies


@pytest.mark.asyncio
async def test_legacy_streams_cross_pages_without_omissions(monkeypatch):
    database = LegacyPages()
    monkeypatch.setattr(validation, "PAGE_ROWS", 2)
    current = SimpleNamespace(dataset_id="edition", dataset_hash=database.dataset_hash, resource_count=3)
    digest, count, leaves = await validation.validate_legacy_content(
        database, current, {"candidate_acquisition_id": "acquisition"}
    )
    assert (digest, count) == (current.dataset_hash, 3)
    assert list(leaves.values()) == [{11}, {12}, {13}, {14}]
    assert len(database.calls) == 9
    assert all(parameters["page_rows"] == 2 and "LIMIT :page_rows" in sql for sql, parameters in database.calls)
    assert [parameters["identity"] for _, parameters in database.calls if "identity" in parameters] == ["B.2", "c3"] * 3
    assert all("acquisition_id" in parameters for _, parameters in database.calls[3:])


@pytest.mark.asyncio
async def test_unmatched_raw_row_remains_outside_published_provenance(monkeypatch):
    database = LegacyPages()
    database.raw.append(dict(leaf=13, npi=1000000099, resource_id="unmatched", work_leaf=None, valid=True))
    current = SimpleNamespace(dataset_id="edition", dataset_hash=database.dataset_hash, resource_count=3)
    monkeypatch.setattr(validation, "PAGE_ROWS", 2)
    digest, count, leaves = await validation.validate_legacy_content(
        database, current, {"candidate_acquisition_id": "acquisition"}
    )
    assert (digest, count) == (database.dataset_hash, 3)
    assert leaves["provider_directory_uhc_flex_practitioner_work"] == {14}


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["hash", "count", "family", "provenance", "reciprocal", "missing"])
async def test_legacy_stream_refuses_incomplete_evidence(monkeypatch, damage):
    database = LegacyPages()
    current = SimpleNamespace(dataset_id="edition", dataset_hash=database.dataset_hash, resource_count=3)
    monkeypatch.setattr(validation, "PAGE_ROWS", 2)
    if damage == "hash":
        current.dataset_hash = "0" * 64
    if damage == "count":
        current.resource_count = 4
    if damage == "family":
        database.resources[0]["resource_type"] = "Organization"
    if damage == "provenance":
        database.provenance[1]["valid"] = False
    if damage == "reciprocal":
        database.raw[1]["valid"] = False
    if damage == "missing":
        database.provenance.pop()
    with pytest.raises(RuntimeError, match="predecessor_invalid"):
        await validation.validate_legacy_content(database, current, {"candidate_acquisition_id": "acquisition"})


def test_legacy_metadata_keeps_reviewed_predicates():
    original = _current_bodies()[freeze.LEGACY]
    query = validation.legacy_metadata_query(original, SCHEMA)
    assert query.endswith(" AND header.status='published'")
    assert ":dataset_id" in query and "candidate_dataset_id" not in query
    successor = freeze.wrapped_body(freeze.LEGACY, SCHEMA, original)
    assert validation.legacy_metadata_query(successor, SCHEMA) == query
    with pytest.raises(RuntimeError, match="predecessor_invalid"):
        validation.legacy_metadata_query(original.replace("SELECT", "SELECT true OR", 1), SCHEMA)
