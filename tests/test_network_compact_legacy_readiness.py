# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Historical compact scopes compare by complete logical contents without altering evidence."""

import json
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.network_address_projection import PinnedAddressSource
from process.network_compact_custom_offices import logical_compact_custom_scope
from process.network_compact_dependencies import compact_slice_ref_digest
from process.network_compact_readiness import NetworkCompactReadinessError, compact_readiness
from process.network_membership_validation import (
    NetworkMembershipValidationError,
    _scope_report,
    _validated_replay,
)


async def test_compact_slice_accounting_reads_only_set_totals():
    connection = SimpleNamespace(fetchrow=AsyncMock(return_value={"rows": 12, "networks": 3, "slices": 4}))
    recorded = await compact_slice_ref_digest(connection, "candidate_example")
    connection.fetchrow.assert_awaited_once()
    query = connection.fetchrow.await_args.args[0]
    assert 'FROM "candidate_example".network_slice_ref' in query
    assert "count(*)" in query and "count(DISTINCT slice_id)" in query
    assert len(recorded) == 64


def legacy_custom_receipt(current=None):
    """Use the revision-one target serializer's zero-custom, merged-binding fixture."""
    document = json.loads((Path(__file__).parent / "fixtures/network_compact_revision1_receipt.json").read_text())
    if current is not None:
        document.update({key: value for key, value in current.items() if key in document})
        document["revision"] = 1
        document["address_table_oid"] = 124 if document["custom_pair_count"] else None
    return document


def _legacy_candidate():
    receipt = legacy_custom_receipt()
    candidate_by_field = {
        **receipt["target"],
        "schema_revision": 1,
        "approved_custom_revision": 0,
        "source_generations": {"unified_address": receipt["base_source"]["generation_id"]},
        "expected_head": 0,
        "expected_rows": 0,
        "accepted_rows": 0,
        "validation_json": {"compact_custom_offices": receipt},
        "index_ready": True,
    }
    scope = _scope_report(candidate_by_field, PinnedAddressSource(**receipt["base_source"]))
    report_by_field = {**scope, "compact_custom_offices": receipt, "valid": True}
    count_by_field = dict(membership_rows=0, distinct_memberships=0, projected_locations=0, orphan_bindings=0)
    report_by_field.update(count_by_field)
    historical_scope_by_field = {**scope, "compact_custom_offices": receipt}
    report_by_field["candidate_readiness"] = {
        "component": "compact_office_slices",
        "readiness_revision": 1,
        "ready": True,
        "scope": historical_scope_by_field,
        "ref_map_oid": 125,
        "edge_heap_oid": 126,
        **count_by_field,
    }
    report_by_field["serving_readiness"] = {
        "component": "compact_network_serving",
        "readiness_revision": 1,
        "ready": True,
        "scope": historical_scope_by_field,
        "index_profile": "compact_membership",
        "index_definition_sha256": "d" * 64,
        "address_artifact": {},
        "edge_artifacts": [],
        "slice_ref_sha256": "e" * 64,
    }
    candidate_by_field["validation_json"] = report_by_field
    return candidate_by_field, scope


def test_target_serialized_scope_replays_without_rewriting_raw_evidence():
    candidate, scope = _legacy_candidate()
    before = deepcopy(candidate)
    report = candidate["validation_json"]
    assert _validated_replay(candidate, scope) is report
    compact_readiness(candidate, report, scope)
    assert candidate == before
    assert logical_compact_custom_scope(scope) == logical_compact_custom_scope(report["serving_readiness"]["scope"])


@pytest.mark.parametrize(
    "field", ["base_artifact_sha256", "approved_records_sha256", "approved_revision", "control_schema"]
)
def test_legacy_scope_still_refuses_changed_logical_inputs(field):
    candidate, scope = _legacy_candidate()
    changed = deepcopy(scope)
    changed["compact_custom_offices"][field] = {"approved_revision": 1, "control_schema": "other_control"}.get(
        field, "f" * 64
    )
    with pytest.raises(NetworkMembershipValidationError, match="immutable pinned scope"):
        _validated_replay(candidate, changed)


@pytest.mark.parametrize("field", ["candidate_readiness", "serving_readiness"])
def test_each_legacy_readiness_scope_refuses_changed_source_generation(field):
    candidate, scope = _legacy_candidate()
    candidate["validation_json"][field]["scope"] = deepcopy(candidate["validation_json"][field]["scope"])
    candidate["validation_json"][field]["scope"]["compact_custom_offices"]["base_source"]["generation_id"] = "f" * 64
    with pytest.raises(NetworkCompactReadinessError):
        compact_readiness(candidate, candidate["validation_json"], scope)


@pytest.mark.parametrize("revision", [1, 3, True, None])
def test_unknown_named_index_proof_revision_is_refused(revision):
    candidate, scope = _legacy_candidate()
    candidate["validation_json"]["serving_readiness"]["index_definition_revision"] = revision
    with pytest.raises(NetworkCompactReadinessError):
        compact_readiness(candidate, candidate["validation_json"], scope)
