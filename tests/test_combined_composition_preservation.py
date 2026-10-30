# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Current inventory identity stays distinct from authenticated historical carry."""

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import registry_candidate_composition as composition
from process import registry_composition_resume as resume
from process import registry_initial_source_composition as offices
from process import registry_publication_queue as queue
from process.registry_source_recipe_composition import OFFICE_MAPPING_KEY
from tests.test_registry_source_recipe_store_postgres import _example
from tests.test_verified_inventory_v2 import rows as witness_rows


@pytest.mark.asyncio
async def test_inventory_binds_current_generation_before_carry_without_per_recipe_keys(monkeypatch):
    recipe = _example()
    stage = composition.RegistrySourceRecipeStage("current_members", 2, 1, "a" * 64, inventory=True)
    approved = SimpleNamespace(approved_revision=7, generation_id="b" * 64, total_rows=0)
    inputs = composition._CompositionInputs(object(), None, approved, None, object(), compact=True)
    generations_by_source = {"registry_source_office_mapping": "c" * 64}
    preservation = object()
    monkeypatch.setattr(composition, "_retained_source", AsyncMock(return_value=(generations_by_source, 2)))
    monkeypatch.setattr(
        composition, "_selected_composition_sources", AsyncMock(return_value=((recipe,), (recipe,), True))
    )
    monkeypatch.setattr(composition, "_prepare_preservation", AsyncMock(return_value=preservation))
    monkeypatch.setattr(composition, "_contribution_revision", AsyncMock(return_value=2))
    monkeypatch.setattr(composition, "stage_registry_source_recipes", AsyncMock(return_value=stage))

    async def append(_connection, current, carry):
        assert current is stage and carry is preservation
        assert generations_by_source["registry_source_membership"] == stage.generation_sha256
        return replace(stage, membership_rows=5, generation_sha256="d" * 64, selected_stage=stage), "e" * 64

    monkeypatch.setattr(composition, "_source_preservation", append)
    actual, rows, recipes, combined = await composition._composition_sources_for_format(
        inputs, UUID(int=8), "registry", expected_head=3, inventory=True
    )
    assert rows == 5 and recipes == (recipe,) and combined.selected_stage is stage
    assert actual["registry_source_membership"] == "a" * 64
    assert actual["registry_fhir_serving_preservation"] == "e" * 64
    assert not any(key.startswith("initial_source_recipe_") for key in actual)


@pytest.mark.asyncio
@pytest.mark.parametrize("authorized", [False, True])
async def test_historical_selection_requires_the_exact_durable_operation(monkeypatch, authorized):
    manifest = SimpleNamespace(generation_id=3)
    approved = SimpleNamespace(approved_revision=7)
    retained = (_example(),)
    request = UUID(int=8)
    proof = AsyncMock(return_value={"authorized": True} if authorized else None)
    monkeypatch.setattr(queue, "require_registry_source_rollback_composition", proof)
    operation = composition._selected_source_recipes(
        object(), retained, approved, manifest, ("registry", None, 4, request), inventory=True
    )
    if authorized:
        assert await operation is retained
    else:
        with pytest.raises(composition.RegistryCompositionError, match="rollback_operation_required"):
            await operation
    assert proof.await_args.args[1:5] == (request, manifest, 7, 4)


@pytest.mark.asyncio
async def test_office_recorder_receives_current_stage_while_retention_keeps_carry(monkeypatch):
    current = composition.RegistrySourceRecipeStage("current_members", 2, 0, "a" * 64, inventory=True)
    combined = replace(current, membership_rows=5, generation_sha256="b" * 64, selected_stage=current)
    recorder = AsyncMock()
    monkeypatch.setattr(offices, "record_verified_witness_offices", recorder)
    artifact = SimpleNamespace(address_source=object(), runtime_roles=("reader",))
    await composition._record_witness_offices(object(), object(), (), object(), artifact, combined, "registry")
    assert recorder.await_args.args[6] is current
    assert combined.membership_rows == 5 and current.membership_rows == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("compact", [False, True])
async def test_only_flat_carry_prepares_final_artifact_before_current_office_pruning(monkeypatch, compact):
    recipe, _, _ = witness_rows()
    approved = SimpleNamespace(approved_revision=7, generation_id="b" * 64, total_rows=0)
    native_offices = (object(), ("native_reader",))
    inputs = composition._CompositionInputs(
        object(), None, approved, None, None, verified_office_inputs=native_offices, compact=compact
    )
    stage = composition.RegistrySourceRecipeStage("current_members", 1, 0, "a" * 64, inventory=compact)
    artifact = SimpleNamespace(address_source=object(), runtime_roles=("artifact_reader",))
    events = []
    monkeypatch.setattr(composition, "_retained_source", AsyncMock(return_value=({}, 3)))
    monkeypatch.setattr(
        composition, "_selected_composition_sources", AsyncMock(return_value=((recipe,), (recipe,), False))
    )
    monkeypatch.setattr(
        composition, "_prepare_preservation", AsyncMock(return_value=SimpleNamespace(receipt={"carried_rows": 2}))
    )
    monkeypatch.setattr(composition, "_contribution_revision", AsyncMock(return_value=2))
    monkeypatch.setattr(composition, "_source_preservation", AsyncMock(return_value=(stage, "e" * 64)))

    async def prepare(*_args):
        assert events == []
        events.append("artifact")
        return artifact

    async def current(*_args, **options):
        assert events == ([] if compact else ["artifact"])
        assert options["read_context"] == composition.SourceRecipeReadContext(
            verified_office_inputs=native_offices if compact else (artifact.address_source, artifact.runtime_roles)
        )
        events.append("current")
        return stage

    monkeypatch.setattr(composition, "_prepare_composition_artifact", prepare)
    monkeypatch.setattr(composition, "stage_registry_source_recipes", current)
    metadata, prepared = await composition._prepare_composition_sources_for_format(
        inputs, UUID(int=8), "registry", inventory=compact, artifact_inputs=(object(), {})
    )
    assert metadata[-1] is stage
    assert prepared is (None if compact else artifact)


@pytest.mark.asyncio
async def test_verified_current_offices_precede_carry_alias_binding_union(monkeypatch):
    stage = composition.RegistrySourceRecipeStage(
        "current_members", 1, 0, "a" * 64, office_matches=object(), preservation=object()
    )
    inputs = composition._CompositionInputs(object(), None, object(), None, None)
    events = []

    async def current(*_args):
        assert events == []
        events.append("current")

    async def carry(*_args, **options):
        assert events == ["current"]
        assert options == {"retain_source_bindings": False, "source_stage": stage}
        events.append("carry")

    monkeypatch.setattr(composition, "_record_source_receipts", current)
    monkeypatch.setattr(composition, "_bindings", carry)
    await composition._record_prepared_composition(inputs, object(), ((), stage, object(), None, None, ()), "registry")
    assert events == ["current", "carry"]


@pytest.mark.asyncio
@pytest.mark.parametrize("has_marker", [False, True])
async def test_registered_legacy_mapping_shape_preserves_candidate_uuid(monkeypatch, has_marker):
    request = UUID(int=8)
    original_generation_by_kind = {"custom_membership": "a" * 64, "unified_address": "b" * 64}
    if has_marker:
        original_generation_by_kind[OFFICE_MAPPING_KEY] = "c" * 64
    original = composition._target(request, 7, 3, original_generation_by_kind.copy())
    monkeypatch.setattr(
        resume,
        "_registered_candidate",
        AsyncMock(return_value=(original, {"source_generations": original_generation_by_kind})),
    )
    selected = await resume.resolve_registered_legacy_mapping_marker(
        object(), request, 7, 3, "c" * 64, default=not has_marker, control_schema="registry"
    )
    recomputed_generation_by_kind = {"custom_membership": "a" * 64, "unified_address": "b" * 64}
    if selected:
        recomputed_generation_by_kind[OFFICE_MAPPING_KEY] = "c" * 64
    assert selected is has_marker
    assert composition._target(request, 7, 3, recomputed_generation_by_kind) == original


@pytest.mark.asyncio
async def test_registered_mapping_digest_cannot_be_replaced_on_replay(monkeypatch):
    monkeypatch.setattr(
        resume,
        "_registered_candidate",
        AsyncMock(return_value=(object(), {"source_generations": {OFFICE_MAPPING_KEY: "a" * 64}})),
    )
    with pytest.raises(resume.RegistryCompositionResumeError, match="mapping_changed"):
        await resume.resolve_registered_legacy_mapping_marker(
            object(), UUID(int=8), 7, 3, "b" * 64, default=False, control_schema="registry"
        )
