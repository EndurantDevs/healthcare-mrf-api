# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Custody sequencing/SQL protocol regressions; doubles provide no native Source authority."""

import asyncio
import hashlib
import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import network_candidate_custody as final
from process import network_source_contribution_custody as source
from process import registry_fhir_serving_preservation as preservation
from process import registry_source_office_custody as offices
from process import registry_verified_office_policy as policy
from tests.test_registry_fhir_serving_preservation import receipt
from tests.test_registry_source_inventory_custody import (
    _APPROVED,
    _candidate,
    _custody_mocks,
    _inventory,
    _ptg_recipe,
    _target,
)


@pytest.mark.parametrize("failure", [None, RuntimeError("current facts"), asyncio.CancelledError()])
async def test_current_inventory_is_checked_before_carry_and_combined_retention(monkeypatch, failure):
    root, entries, stage, derivations = _inventory((_ptg_recipe(),))
    copy_target = _target()
    candidate_by_field = _candidate(copy_target, root, stage, derivations)
    events = []
    _custody_mocks(monkeypatch, stage, derivations, events)
    carried = SimpleNamespace(receipt={"carried_rows": 3})

    async def current_offices(*arguments, **options):
        context = arguments[3]
        assert context.preservation is carried
        events.append("offices")
        for derivation in derivations:
            context.derivation_sink(derivation)

    async def append(_connection, current, proof):
        assert proof is carried and current.generation_sha256 == stage.generation_sha256
        assert events == ["facts", "offices", "projection"]
        events.append("carry")
        return replace(current, selected_stage=current, membership_rows=3, generation_sha256="f" * 64)

    monkeypatch.setattr(offices, "_verify_inventory_offices", current_offices)
    copied = AsyncMock(side_effect=append)
    monkeypatch.setattr(preservation, "append_fhir_preservation_memberships", copied)
    if failure is not None:
        monkeypatch.setattr(source, "verify_registry_source_contributions_rows", AsyncMock(side_effect=failure))
    operation = source.verify_independent_source_contributions(
        object(),
        copy_target,
        candidate_by_field,
        control_schema="controls",
        inventory_entries=entries,
        preservation=carried,
    )
    if failure is None:
        assert await operation is carried
        assert events == ["facts", "offices", "projection", "carry", "retention", "commands", "composition", "drop"]
        combined = source.verify_registry_source_retention_rows.await_args.kwargs["source_stage"]
        assert combined.membership_rows == 3 and combined.selected_stage.membership_rows == 0
        assert combined.contribution_receipts == stage.contribution_receipts
    else:
        with pytest.raises(type(failure)) as caught:
            await operation
        assert caught.value is failure
        copied.assert_not_awaited()
        assert events == ["drop"]
    assert stage.membership_rows == 0 and stage.preservation is None


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


@pytest.mark.parametrize("failure", [None, RuntimeError("copy_target proof"), asyncio.CancelledError()])
async def test_preservation_scope_drains_cleanup_and_preserves_primary_identity(monkeypatch, failure):
    proof, entries, events = object(), object(), []

    async def rederive(*arguments, **options):
        assert options == {"control_schema": "controls", "inventory_entries": entries}
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


@pytest.mark.parametrize("inventory", [False, True])
async def test_reconstruction_keeps_full_vector_and_actual_retained_manifest(monkeypatch, inventory):
    recipes = (_ptg_recipe(),)
    root, _entries, stage, _derivations = _inventory(recipes)
    manifest = object()
    lookup = AsyncMock(return_value=(manifest, object()))
    replay = AsyncMock(return_value=stage)
    monkeypatch.setattr(source, "resolve_registry_retained_generation", lookup)
    monkeypatch.setattr(source, "resolve_network_serving_manifest", AsyncMock(side_effect=AssertionError("head")))
    monkeypatch.setattr(source, "stage_registry_source_recipes", replay)
    from process.registry_source_recipe_store import canonical_registry_source_recipes

    document = canonical_registry_source_recipes(recipes)
    candidate_by_field = {
        "expected_head": 99,
        "source_recipes_json": document,
        "source_generations": {
            "retained_network_source": "b" * 64,
            "registry_source_recipes": hashlib.sha256(document.encode()).hexdigest(),
        },
    }
    await source._independent_source_stage(
        object(),
        _target(),
        candidate_by_field,
        _APPROVED,
        expected={"revision": 3 if inventory else 2},
        control_schema="controls",
        office_authority=None,
        inventory=(root, recipes, ()) if inventory else None,
    )
    assert tuple(replay.await_args.args[1]) == recipes
    read_context = replay.await_args.kwargs["read_context"]
    assert read_context.source_manifest is manifest
    assert read_context.office_authority is None and read_context.verified_office_inputs is None
    lookup.assert_awaited_once()


async def test_compact_final_proof_keeps_carry_scope_through_last_proof_and_return(monkeypatch):
    from contextlib import asynccontextmanager

    from process import network_compact_readiness as readiness
    from process import network_compact_validation as compact
    from process import network_membership_publication as publication

    events, proof, entries = [], object(), object()

    @asynccontextmanager
    async def scope(*arguments):
        assert arguments[-1] is entries
        events.append("enter")
        try:
            yield proof
        finally:
            events.append("cleanup")

    def recorder(event):
        async def record(*arguments, **options):
            if event == "current":
                assert options["preservation"] is proof and options["inventory_entries"] is entries
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
    monkeypatch.setattr(final, "_preservation_scope", scope)
    monkeypatch.setattr(final, "verify_registry_source_retention_rows", recorder("retention"))
    monkeypatch.setattr(publication, "_publication_readiness", lambda _candidate: (None, None, None, {}))
    monkeypatch.setattr(publication, "_verify_provider_custody", recorder("provider"))
    monkeypatch.setattr(source, "verify_independent_source_contributions", recorder("current"))
    monkeypatch.setattr(readiness, "is_compact_candidate", lambda _candidate: True)
    monkeypatch.setattr(compact, "verify_compact_final_proof", recorder("compact"))
    await final._final_proof(object(), _target(), candidate_by_field, "controls", inventory_entries=entries)
    assert events == ["enter", "retention", "current", "compact", "provider", "cleanup"]


@pytest.mark.parametrize("carried", [0, 3])
async def test_legacy_resume_selects_current_policy_with_combined_retention(monkeypatch, carried):
    from process.registry_source_retention import registry_source_retention_sha256
    from tests.test_registry_verified_office_policy import _fixture, _native_pin

    example = _fixture()
    _native_pin(monkeypatch, example)
    proof = receipt() | {
        "approved_revision": example.approved.approved_revision,
        "carried_rows": carried,
        "current_recipes_sha256": example.candidate["source_generations"]["registry_source_recipes"],
    }
    if not carried:
        proof.update(missing_networks=0, binding_rows=0, address_rows=0)
    digest = preservation.fhir_serving_preservation_sha256(proof)
    example.candidate["expected_head"] = proof["expected_head"]
    example.candidate["validation_json"][proof["component"]] = proof
    example.candidate["source_generations"][proof["component"]] = digest
    retention_by_field = example.candidate["validation_json"]["registry_source_retention"]
    retention_by_field.update(
        source_rows=example.stage.membership_rows + carried,
        effective_rows=example.stage.membership_rows + carried,
        source_generation_sha256=hashlib.sha256(
            json.dumps([example.stage.generation_sha256, digest], separators=(",", ":")).encode()
        ).hexdigest(),
    )
    example.candidate["expected_rows"] = retention_by_field["effective_rows"]
    example.candidate["source_generations"]["registry_source_retention"] = registry_source_retention_sha256(
        retention_by_field
    )
    assert await policy.select_legacy_verified_office_inputs(object(), example.candidate) == example.inputs
    staged = AsyncMock(return_value=example.stage)
    monkeypatch.setattr(policy, "stage_registry_source_recipes", staged)
    monkeypatch.setattr(policy, "verified_retained_source_office_summary", AsyncMock(return_value=example.summary))
    actual = await policy.reconstruct_legacy_verified_office_stage(
        object(), example.candidate, example.approved, control_schema="controls", request_id=example.target.candidate_id
    )
    assert actual is example.stage and actual.membership_rows == 2 and actual.selected_stage is None
    staged.assert_awaited_once()


@pytest.mark.parametrize("primary", [None, RuntimeError("source"), asyncio.CancelledError()])
async def test_source_stage_cleanup_keeps_primary_failure(monkeypatch, primary):
    cleanup_error = RuntimeError("drop")
    monkeypatch.setattr(source, "_drop_stage", AsyncMock(side_effect=cleanup_error))
    stage = SimpleNamespace(office_matches=None)
    if primary is None:
        with pytest.raises(RuntimeError) as caught:
            await source._cleanup_independent_source_stage(object(), stage, None, None)
        assert caught.value is cleanup_error
    else:
        await source._cleanup_independent_source_stage(object(), stage, None, primary)
        assert primary._registry_source_cleanup_error is cleanup_error


async def test_inventory_office_cleanup_keeps_body_failure(monkeypatch):
    primary, cleanup_error = RuntimeError("office"), RuntimeError("drop")
    monkeypatch.setattr(offices, "_require_transaction", AsyncMock())
    monkeypatch.setattr(offices, "_native_candidate", AsyncMock())
    monkeypatch.setattr(offices, "verify_initial_source_office_receipt", lambda _candidate: None)
    monkeypatch.setattr(offices, "copy_source_office_binding_rows", AsyncMock())
    monkeypatch.setattr(offices, "_retained_bindings", AsyncMock(side_effect=primary))
    driver = SimpleNamespace(execute=AsyncMock(side_effect=cleanup_error))
    context = offices.InventoryOfficeCustodyContext((), _APPROVED, lambda _derivation: None)
    with pytest.raises(RuntimeError) as caught:
        await offices._verify_inventory_offices(driver, _target(), {}, context, control_schema="controls")
    assert caught.value is primary and primary._registry_office_cleanup_error is cleanup_error
