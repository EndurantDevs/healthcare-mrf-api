# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Source data validation ordering; doubles provide no native Source authority."""

import asyncio
import hashlib
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import network_membership_validation as final
from process import network_source_contribution_custody as source
from process import registry_fhir_serving_preservation as preservation
from process import registry_source_office_custody as offices
from process import registry_verified_office_policy as policy
from tests.test_registry_fhir_serving_preservation import receipt
from tests.test_registry_source_inventory_custody import (
    _target,
)


async def test_carried_binding_union_keeps_custom_precedence_and_full_parity():
    driver = SimpleNamespace(fetchval=AsyncMock(return_value=False))
    proof = SimpleNamespace(binding_query="SELECT * FROM authenticated_carried_bindings")
    await offices._binding_parity(
        driver, _target(), 'pg_temp."current_offices"', {"approved_custom_revision": 7}, "controls", preservation=proof
    )
    query, revision = driver.fetchval.await_args.args
    assert (
        'SELECT provider_system,provider_id,location_id,location_key,entity_type,entity_id FROM pg_temp."current_offices" UNION'
        in query
    )
    assert "FROM (SELECT * FROM authenticated_carried_bindings) carried" in query
    assert "LEFT JOIN custom USING(provider_system,provider_id,location_id)" in query
    assert query.count("EXCEPT ALL") == 2
    assert "count(DISTINCT (provider_system,provider_id,entity_type,entity_id))>1" in query
    assert revision == 7


@pytest.mark.parametrize("carried", [0, 3])
def test_legacy_policy_pairs_current_identity_with_zero_or_nonzero_carry(carried):
    proof = receipt() | {"carried_rows": carried}
    if carried == 0:
        proof.update(missing_networks=0, binding_rows=0, address_rows=0)
    digest = preservation.fhir_serving_preservation_sha256(proof)
    candidate_by_field = {
        "approved_custom_revision": proof["approved_revision"],
        "validation_json": {proof["component"]: proof},
        "source_generations": {proof["component"]: digest},
    }
    current_generation = "e" * 64
    combined = hashlib.sha256(json.dumps([current_generation, digest], separators=(",", ":")).encode()).hexdigest()
    retention_by_field = {"source_generation_sha256": combined, "source_rows": 2 + carried}
    assert policy._has_matching_retention(candidate_by_field, retention_by_field, current_generation, 2)
    assert not policy._has_matching_retention(
        candidate_by_field, retention_by_field | {"source_generation_sha256": current_generation}, current_generation, 2
    )
    if carried:
        assert not policy._has_matching_retention(
            candidate_by_field, retention_by_field | {"source_rows": 2}, current_generation, 2
        )
    legacy_by_field = {"validation_json": {}, "source_generations": {}}
    assert policy._has_matching_retention(
        legacy_by_field, {"source_generation_sha256": current_generation, "source_rows": 2}, current_generation, 2
    )


@pytest.mark.parametrize("primary", [None, RuntimeError("source"), asyncio.CancelledError()])
async def test_source_stage_cleanup_keeps_primary_failure(monkeypatch, primary):
    cleanup_error = RuntimeError("drop")
    monkeypatch.setattr(source, "_drop_stage", AsyncMock(side_effect=cleanup_error))
    stage = SimpleNamespace(office_matches=None)
    if primary is None:
        with pytest.raises(RuntimeError) as caught:
            await source._cleanup_independent_source_stage(object(), stage, None)
        assert caught.value is cleanup_error
    else:
        await source._cleanup_independent_source_stage(object(), stage, primary)
        assert primary._registry_source_cleanup_error is cleanup_error


@pytest.mark.parametrize("failure", [None, RuntimeError("copy_target proof"), asyncio.CancelledError()])
async def test_preservation_scope_drains_cleanup_and_preserves_primary_identity(monkeypatch, failure):
    proof, entries, events = object(), object(), []

    async def rederive(*arguments, **options):
        assert options == {"control_schema": "controls", "inventory_entries": entries, "source_witness_runtime": None}
        events.append("rederive")
        return proof

    async def copy_target(*arguments, **options):
        assert arguments[3] is proof
        events.append("copy_target")
        if failure is not None:
            raise failure

    async def cleanup(_connection, actual, *, primary):
        assert actual is proof and primary is failure
        events.append("cleanup")
        if failure is not None:
            raise RuntimeError("cleanup")

    monkeypatch.setattr(preservation, "rederive_fhir_serving_preservation", rederive)
    monkeypatch.setattr(preservation, "verify_fhir_preservation_target_rows", copy_target)
    monkeypatch.setattr(preservation, "cleanup_fhir_serving_preservation", cleanup)
    monkeypatch.setattr(preservation, "has_fresh_carried_typed_data", AsyncMock(return_value=False))
    try:
        async with final._preservation_scope(object(), _target(), {}, "controls", entries) as actual:
            assert failure is None and actual is proof
            events.append("body")
    except BaseException as error:
        assert failure is not None and error is failure
        assert "cleanup failed" in error.__notes__[-1]
    assert events == (
        ["rederive", "copy_target", "body", "cleanup"] if failure is None else ["rederive", "copy_target", "cleanup"]
    )


async def test_compact_final_proof_keeps_carry_scope_through_last_proof_and_return(monkeypatch):
    from process import network_compact_readiness as readiness
    from process import network_compact_validation as compact
    from process import network_em_provider_inputs as provider_inputs

    events, proof, entries = [], SimpleNamespace(receipt={"carried_rows": 0}), object()

    async def enter(*arguments, **options):
        assert options["inventory_entries"] is entries
        events.append("enter")
        return proof

    async def cleanup(*arguments, **options):
        assert arguments[1] is proof
        events.append("cleanup")

    def recorder(event):
        async def record(*arguments, **options):
            events.append(event)

        return record

    candidate_by_field = {
        "validation_json": {
            "address_source": {
                "schema_name": "addresses",
                "table_name": "entity_address_unified",
                "generation_id": "a" * 64,
            }
        }
    }
    monkeypatch.setattr(preservation, "rederive_fhir_serving_preservation", enter)
    monkeypatch.setattr(preservation, "verify_fhir_preservation_target_rows", recorder("carried"))
    monkeypatch.setattr(preservation, "has_fresh_carried_typed_data", AsyncMock(return_value=False))
    monkeypatch.setattr(preservation, "cleanup_fhir_serving_preservation", cleanup)
    monkeypatch.setattr(final, "verify_registry_source_retention_rows", recorder("retention"))
    monkeypatch.setattr(provider_inputs, "verify_candidate_em_provider_inputs", recorder("provider"))
    monkeypatch.setattr(readiness, "is_compact_candidate", lambda _candidate: True)
    monkeypatch.setattr(compact, "verify_compact_final_proof", recorder("compact"))
    await final._final_proof(object(), _target(), candidate_by_field, "controls", inventory_entries=entries)
    assert events == ["enter", "carried", "retention", "compact", "provider", "cleanup"]
