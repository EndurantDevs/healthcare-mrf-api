# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Final named candidate data validation and carried typed mapping boundaries."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import network_membership_validation as validation
from process import registry_fhir_serving_preservation as preservation
from process.network_membership_copy import MembershipCopyTarget
from tests.test_registry_typed_source_recipe import _recipe

COUNT_FIELDS = (
    "membership_rows",
    "distinct_memberships",
    "projected_locations",
    "orphan_bindings",
    "raw_rows",
    "batch_rows",
    "batch_count",
    "unknown_network_rows",
)


def _candidate():
    target = MembershipCopyTarget(
        *(str(UUID(int=index)) for index in range(1, 5)), "network_candidate_" + UUID(int=4).hex
    )
    candidate_by_field = {
        field: getattr(target, field)
        for field in ("dataset_id", "schema_id", "producer_id", "candidate_id", "schema_name")
    }
    candidate_by_field.update(
        state="ready",
        source_generations={},
        source_recipes_json=[],
        validation_json={
            "address_source": {
                "schema_name": "addresses",
                "table_name": "entity_address_unified",
                "generation_id": "address-generation",
            }
        },
    )
    return target, candidate_by_field


@pytest.mark.asyncio
async def test_final_validation_returns_checked_counts_without_control_writes(monkeypatch):
    target, candidate = _candidate()
    connection = SimpleNamespace(
        is_in_transaction=lambda: True, fetchval=AsyncMock(return_value="repeatable read"), execute=AsyncMock()
    )
    counts_by_field = {field: index for index, field in enumerate(COUNT_FIELDS)}
    proof = AsyncMock(return_value=counts_by_field)
    monkeypatch.setattr(validation, "_final_proof", proof)
    result = await validation.validate_network_candidate_final(connection, target, candidate, "control")
    assert result == {
        "candidate_id": target.candidate_id,
        "schema_name": target.schema_name,
        "complete": True,
        "source_generations": {},
        "counts": counts_by_field,
        "semantic_validation": None,
    }
    proof.assert_awaited_once_with(
        connection,
        target,
        candidate,
        "control",
        inventory_entries=None,
        source_witness_runtime=None,
    )
    connection.execute.assert_not_awaited()
    assert connection.fetchval.await_args.args == ("SHOW transaction_isolation",)


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["candidate", "state", "isolation", "inactive"])
async def test_final_validation_refuses_wrong_logical_snapshot_before_data_queries(monkeypatch, damage):
    target, candidate = _candidate()
    if damage == "candidate":
        candidate["producer_id"] = str(UUID(int=999))
    if damage == "state":
        candidate["state"] = "open"
    connection = SimpleNamespace(
        is_in_transaction=lambda: damage != "inactive",
        fetchval=AsyncMock(return_value="read committed" if damage == "isolation" else "repeatable read"),
    )
    proof = AsyncMock()
    monkeypatch.setattr(validation, "_final_proof", proof)
    with pytest.raises(ValueError):
        await validation.validate_network_candidate_final(connection, target, candidate, "control")
    proof.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("fresh", [False, True])
async def test_final_data_path_rechecks_retention_indexes_and_provider_inputs(monkeypatch, fresh):
    from process import network_compact_readiness as compact
    from process import network_em_provider_inputs as providers
    from process import network_membership_serving_indexes as indexes

    copy_target, candidate = _candidate()
    scope_by_field = {"candidate_id": copy_target.candidate_id}
    candidate["validation_json"]["serving_readiness"] = {
        "component": "unified_address_serving",
        "readiness_revision": 1,
        "ready": True,
        "index_profile": "serving",
        "scope": scope_by_field,
        "index_definition_sha256": "a" * 64,
    }
    selected = object()

    monkeypatch.setattr(preservation, "rederive_fhir_serving_preservation", AsyncMock(return_value=selected))
    monkeypatch.setattr(preservation, "verify_fhir_preservation_target_rows", AsyncMock())
    monkeypatch.setattr(preservation, "cleanup_fhir_serving_preservation", AsyncMock())
    checks_by_name = {}
    for module, name, check_result in (
        (preservation, "has_fresh_carried_typed_data", fresh),
        (validation, "verify_registry_source_retention_rows", None),
        (indexes, "_matching_inputs", ({field: 0 for field in COUNT_FIELDS}, scope_by_field)),
        (indexes, "_source_semantics", None),
        (indexes, "_index_digest", "a" * 64),
        (providers, "verify_candidate_em_provider_inputs", None),
    ):
        checks_by_name[name] = AsyncMock(return_value=check_result)
        monkeypatch.setattr(module, name, checks_by_name[name])
    monkeypatch.setattr(compact, "is_compact_candidate", lambda candidate: False)
    assert await validation._final_proof(object(), copy_target, candidate, "control") == {
        field: 0 for field in COUNT_FIELDS
    }
    for mock in checks_by_name.values():
        assert mock.await_count == 1
    checks_by_name["_index_digest"].return_value = "b" * 64
    with pytest.raises(ValueError, match="index definitions changed"):
        await validation._final_proof(object(), copy_target, candidate, "control")
    assert checks_by_name["verify_candidate_em_provider_inputs"].await_count == 1


@pytest.mark.asyncio
async def test_preservation_cleanup_preserves_original_failure(monkeypatch):
    selected = object()
    primary = asyncio.CancelledError()
    cleanup_error = RuntimeError("cleanup")
    monkeypatch.setattr(preservation, "rederive_fhir_serving_preservation", AsyncMock(return_value=selected))
    monkeypatch.setattr(preservation, "verify_fhir_preservation_target_rows", AsyncMock())
    monkeypatch.setattr(preservation, "has_fresh_carried_typed_data", AsyncMock(return_value=True))
    cleanup = AsyncMock(side_effect=cleanup_error)
    monkeypatch.setattr(preservation, "cleanup_fhir_serving_preservation", cleanup)
    with pytest.raises(asyncio.CancelledError) as captured:
        async with validation._preservation_scope(object(), object(), {}, "control", None):
            raise primary
    assert captured.value is primary
    assert primary._registry_preservation_cleanup_error is cleanup_error
    assert cleanup.await_args.kwargs["primary"] is primary


def _carry():
    return preservation.FHIRServingPreservation(
        SimpleNamespace(candidate_id=str(UUID(int=9)), schema_name="previous_named", approved_custom_revision=4),
        "pg_temp.selected_networks",
        "SELECT * FROM old_members",
        "SELECT * FROM old_bindings",
        "SELECT * FROM old_addresses",
        {"carried_rows": 2},
        temporary_tables=(
            "pg_temp.inventory",
            "pg_temp.coverage",
            "pg_temp.subjects",
            "pg_temp.current",
            "pg_temp.reviewed",
            "pg_temp.missing",
        ),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", [None, "membership", "office"])
async def test_carried_typed_mapping_compares_all_membership_and_office_fields(monkeypatch, damage):
    from process import registry_typed_membership_parity as parity

    carry = _carry()
    stage = SimpleNamespace(table_name="fresh_members", office_matches=SimpleNamespace(table_name="fresh_bindings"))
    compare = AsyncMock(return_value=damage == "membership")
    monkeypatch.setattr(parity, "has_multiset_difference", compare)
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=damage == "office"))
    if damage:
        with pytest.raises(ValueError, match="carried_typed_" + damage):
            await preservation._carried_typed_mapping_parity(
                connection, carry, stage, SimpleNamespace(approved_revision=4), "control"
            )
    else:
        await preservation._carried_typed_mapping_parity(
            connection, carry, stage, SimpleNamespace(approved_revision=4), "control"
        )
    _, actual, expected = compare.await_args.args
    assert "m.evidence_id" in actual and "m.evidence_id" in expected
    assert '"previous_named".network_membership_source' in actual
    assert 'pg_temp."fresh_members"' in expected and "selected_networks" in expected
    if damage != "membership":
        query = connection.fetchval.await_args.args[0]
        assert all(
            "b." + field in query
            for field in ("provider_system", "provider_id", "location_id", "location_key", "entity_type", "entity_id")
        )
        assert query.count("EXCEPT ALL") == 2
        assert "approved_revision=4" in query and "coalesce(custom.location_key,expected.location_key)" in query
    else:
        connection.fetchval.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", [None, "unknown_network", "mapping", "cancel"])
async def test_missing_previous_evidence_forces_fresh_source_and_mapping_with_cleanup(monkeypatch, damage):
    from process import network_source_contribution_custody as cleanup_module
    from process import registry_source_contribution_artifacts as artifacts

    carry = _carry()
    recipe = _recipe()
    previous_candidate_by_field = {"candidate_id": carry.predecessor.candidate_id}
    monkeypatch.setattr(
        artifacts,
        "_published_artifact_candidates",
        AsyncMock(return_value={carry.predecessor.candidate_id: previous_candidate_by_field}),
    )
    monkeypatch.setattr(artifacts, "load_published_artifact_validation", AsyncMock(return_value={"validation": None}))
    monkeypatch.setattr(preservation, "resolve_retained_registry_source_recipes", AsyncMock(return_value=(recipe,)))
    stage, approved = object(), object()
    rebuild = AsyncMock(return_value=(stage, approved))
    monkeypatch.setattr(preservation, "_carried_typed_stage", rebuild)
    error = asyncio.CancelledError() if damage == "cancel" else ValueError("mapping changed")
    compare = AsyncMock(side_effect=error if damage in {"mapping", "cancel"} else None)
    monkeypatch.setattr(preservation, "_carried_typed_mapping_parity", compare)
    cleanup = AsyncMock()
    monkeypatch.setattr(cleanup_module, "_cleanup_independent_source_stage", cleanup)
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=damage == "unknown_network"))
    if damage:
        with pytest.raises(asyncio.CancelledError if damage == "cancel" else ValueError):
            await preservation.has_fresh_carried_typed_data(connection, carry, control_schema="control")
    else:
        assert await preservation.has_fresh_carried_typed_data(connection, carry, control_schema="control") is True
    if damage == "unknown_network":
        rebuild.assert_not_awaited()
        cleanup.assert_not_awaited()
    else:
        rebuild.assert_awaited_once_with(connection, carry, (recipe,), previous_candidate_by_field, "control", None)
        cleanup.assert_awaited_once_with(connection, stage, error if damage in {"mapping", "cancel"} else None)


@pytest.mark.asyncio
@pytest.mark.parametrize("uncovered_network", [False, True])
async def test_ordinary_recipes_from_predecessor_before_upgrade_carry_after_coverage_replay(
    monkeypatch, uncovered_network
):
    from process import registry_source_contribution_artifacts as artifacts
    from tests.registry_source_recipe_support import _example

    carry = _carry()
    recipe = _example()
    monkeypatch.setattr(
        artifacts,
        "_published_artifact_candidates",
        AsyncMock(return_value={carry.predecessor.candidate_id: {"candidate_id": carry.predecessor.candidate_id}}),
    )
    monkeypatch.setattr(artifacts, "load_published_artifact_validation", AsyncMock(return_value={"validation": None}))
    monkeypatch.setattr(preservation, "resolve_retained_registry_source_recipes", AsyncMock(return_value=(recipe,)))
    rebuild = AsyncMock()
    monkeypatch.setattr(preservation, "_carried_typed_stage", rebuild)
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=uncovered_network))
    if uncovered_network:
        with pytest.raises(ValueError, match="carried_source_validation_required"):
            await preservation.has_fresh_carried_typed_data(connection, carry, control_schema="control")
    else:
        assert await preservation.has_fresh_carried_typed_data(connection, carry, control_schema="control") is True
    assert connection.fetchval.await_args.args[1] == [preservation._canonical(preservation._slot(recipe))]
    rebuild.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("typed", [False, True])
@pytest.mark.parametrize("foreign_reference", [False, True])
async def test_compact_final_checks_slice_data_and_returns_stored_accounting(monkeypatch, typed, foreign_reference):
    from process import network_compact_validation as compact
    from process import network_typed_membership_accounting as accounting
    from process import network_typed_slice_native as native
    from process.network_compact_layout import CompactAddressRelation
    from process.network_compact_readiness import NetworkCompactReadinessError

    copy_target, candidate = _candidate()
    candidate.update(accepted_rows=3, expected_rows=3)
    candidate["validation_json"].update({field: 3 for field in COUNT_FIELDS})
    layout = SimpleNamespace(
        artifacts=(SimpleNamespace(candidate_id=copy_target.candidate_id),),
        base=CompactAddressRelation("addresses", "entity_address_unified"),
        custom=None,
    )
    events = []
    monkeypatch.setattr(compact, "compact_readiness", lambda *arguments: (events.append("readiness"), {}))
    monkeypatch.setattr(compact, "_scope_report", lambda *arguments: {})
    monkeypatch.setattr(compact, "published_network_layout", lambda actual: layout)
    monkeypatch.setattr(
        compact, "verify_compact_slice_contents", AsyncMock(side_effect=lambda *arguments: events.append("slices"))
    )
    monkeypatch.setattr(compact, "is_typed_candidate", lambda actual: typed)
    monkeypatch.setattr(compact, "_has_binding_location_index", AsyncMock(return_value=False))
    monkeypatch.setattr(
        compact, "_membership_sets_differ", AsyncMock(side_effect=lambda *arguments: events.append("sets") or False)
    )
    monkeypatch.setattr(
        accounting, "prepare_accounting_work", AsyncMock(return_value=("pg_temp.work", {"membership_rows": 3}))
    )
    monkeypatch.setattr(native, "require_complete_facts", lambda *arguments: events.append("facts"))
    monkeypatch.setattr(native, "verify_work_refs", AsyncMock(side_effect=lambda *arguments: events.append("refs")))
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=not foreign_reference), execute=AsyncMock())
    if foreign_reference:
        with pytest.raises(NetworkCompactReadinessError):
            await compact.verify_compact_final_proof(connection, copy_target, candidate, control_schema="control")
        assert events == ["readiness", "slices"]
        return
    report = await compact.verify_compact_final_proof(connection, copy_target, candidate, control_schema="control")
    assert report is candidate["validation_json"]
    assert connection.fetchval.await_args.args[1] == [copy_target.candidate_id]
    assert events == ["readiness", "slices", *(["facts", "refs"] if typed else ["sets"])]
    if typed:
        connection.execute.assert_awaited_once_with("DROP TABLE pg_temp.work")
