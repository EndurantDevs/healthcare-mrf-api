# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Producer orchestration keeps inventory bounds separate from legacy wire contracts."""

import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import registry_candidate_composition as composition
from process import registry_composition_resume as resume
from process import registry_ptg_office_selection as selection
from process import registry_source_recipe_composition as staging
from process.network_address_projection import PinnedAddressSource
from process.registry_compact_composition import CompactCompositionAddressSource
from process.registry_source_inventory_preparation import SourceInventoryPreparation
from process.registry_source_inventory_reference import INVENTORY_DIGEST_KEY, verify_source_recipe_metadata
from process.registry_source_recipe_store import RegistrySourceRecipeError, canonical_registry_source_recipes
from tests.test_registry_composition_resume import _connection, _registered
from tests.test_registry_source_inventory_preparation import _derivation, _descriptor
from tests.test_registry_source_recipe_store_postgres import _example


@asynccontextmanager
async def _transaction():
    yield


def _stage(recipes, *, inventory=True):
    return staging.RegistrySourceRecipeStage(
        "registry_source_page_" + "a" * 32,
        len(recipes),
        0,
        "c" * 64,
        tuple(recipe.source_pin.generation_id for recipe in recipes),
        contribution_table_name="registry_source_fact_" + "a" * 32,
        contribution_receipts=tuple(_descriptor(recipe) for recipe in recipes),
        inventory=inventory,
    )


@pytest.mark.parametrize("count", [101, 1000])
async def test_source_stage_uses_inventory_recipe_commitment_and_preserves_singletons(monkeypatch, count):
    recipes = tuple(_example(index) for index in reversed(range(count)))
    preparation = SourceInventoryPreparation(recipes)
    connection = SimpleNamespace(transaction=_transaction)
    calls = []

    async def contribution(_connection, recipe, *_arguments):
        calls.append(recipe)
        return recipe.source_pin.generation_id, (1, 0, 1, 1, 0, 0), _descriptor(recipe), 0

    monkeypatch.setattr(staging, "_require_source_stage_scope", AsyncMock())
    monkeypatch.setattr(staging, "retained_source_contributions", AsyncMock(return_value={}))
    monkeypatch.setattr(staging, "_create_source_stage_tables", AsyncMock())
    finish = AsyncMock()
    monkeypatch.setattr(staging, "_finish_source_stage", finish)
    monkeypatch.setattr(staging, "_stage_source_contribution", contribution)
    approved = SimpleNamespace(generation_id="b" * 64)
    stage = await staging.stage_registry_source_recipes(
        connection, recipes, approved, request_id=UUID(int=4), control_schema="fixture", inventory=True
    )
    assert calls == list(preparation.recipes)
    assert stage.inventory is True and stage.contribution_revision == 2
    assert stage.membership_rows == len(stage.contribution_receipts) == count
    assert stage.generation_sha256 == staging._source_stage_generation(
        preparation.recipe_root.recipes_sha256, approved, stage.source_generations, count, 0
    )
    finish.assert_awaited_once()
    with pytest.raises(RegistrySourceRecipeError):
        canonical_registry_source_recipes(recipes)


@pytest.mark.parametrize("inventory,revision", [(True, 1), (1, 2), ("true", 2)])
async def test_invalid_inventory_stage_mode_never_touches_source_storage(monkeypatch, inventory, revision):
    monkeypatch.setattr(staging, "_require_source_stage_scope", AsyncMock())
    retained = AsyncMock()
    monkeypatch.setattr(staging, "retained_source_contributions", retained)
    with pytest.raises(staging.RegistrySourceCompositionError):
        await staging.stage_registry_source_recipes(
            object(),
            (),
            object(),
            request_id=UUID(int=4),
            control_schema="fixture",
            inventory=inventory,
            contribution_revision=revision,
        )
    retained.assert_not_awaited()


@pytest.mark.parametrize("count,office_revision", [(0, None), (0, 1), (101, None), (101, 4)])
async def test_actual_registration_orchestration_finishes_inventory_before_candidate_identity(
    monkeypatch, count, office_revision
):
    recipes = tuple(_example(index) for index in range(count))
    stage = _stage(recipes)
    address = PinnedAddressSource("address_fixture", "entity_address_unified", "b" * 64)
    artifact = CompactCompositionAddressSource(address, (), "c" * 64)
    approved = SimpleNamespace(approved_revision=7, generation_id="d" * 64)
    inputs = composition._CompositionInputs(object(), None, approved, None, None)
    calls, generations_by_name = [], {}
    monkeypatch.setattr(
        composition, "resolve_registered_office_summary_revision", AsyncMock(return_value=office_revision)
    )

    async def prepare(*arguments):
        read_context = arguments[-2]
        assert type(read_context) is composition.SourceRecipeReadContext
        assert read_context.retained_fhir_reads is inputs.retained_fhir_reads
        assert read_context.office_matches is stage.office_matches
        preparation = arguments[-1][4]
        assert preparation is not None
        for recipe, descriptor in zip(recipes, stage.contribution_receipts, strict=True):
            preparation.add_derivation(_derivation(recipe, descriptor))
        calls.append("prepare")
        return artifact, None

    async def candidate(*arguments, **options):
        inventory = options["source_inventory"]
        assert options["source_recipes"] == ()
        assert options["source_generations"][INVENTORY_DIGEST_KEY] == inventory.root.entries_sha256
        calls.append("register")
        return {"state": "open"}

    async def inventory_write(_connection, _target, entries, **options):
        assert len(entries) == options["expected_root"].entry_count == count
        calls.append("inventory")

    monkeypatch.setattr(composition, "_composition_artifact", prepare)
    monkeypatch.setattr(composition, "require_registered_composition_target", AsyncMock())
    monkeypatch.setattr(composition, "create_network_candidate", candidate)
    from process import registry_source_inventory_store as store

    monkeypatch.setattr(store, "write_source_inventory", inventory_write)
    monkeypatch.setattr(composition, "_bindings", AsyncMock())
    monkeypatch.setattr(composition, "_record_source_receipts", AsyncMock())
    prepared = await composition._register_composition_snapshot(
        inputs,
        UUID(int=4),
        0,
        SimpleNamespace(compact=True),
        {"loader_roles": (), "reader_roles": ()},
        (generations_by_name, recipes, stage, object(), {"effective_rows": count}),
        "fixture",
    )
    assert calls == ["prepare", "register", "inventory"]
    assert len(generations_by_name) < 12
    assert not any(key.startswith("initial_source_recipe_") for key in generations_by_name)
    assert prepared[3] is stage


async def test_registered_inventory_aggregate_resumes_descriptor_revision_two():
    request = UUID(int=4)
    target, candidate = _registered(request, 2)
    recipe = _example()
    preparation = SourceInventoryPreparation((recipe,))
    descriptor = _descriptor(recipe)
    preparation.add_contribution(descriptor)
    preparation.add_derivation(_derivation(recipe, descriptor))
    inventory, _ = preparation.finish()
    stage = _stage((recipe,))
    generations_by_name = {}
    receipt = composition._bind_source_contributions(generations_by_name, stage, inventory)
    from process.network_membership_candidate_lifecycle import _candidate_source_metadata

    source_json, reference_json = _candidate_source_metadata(generations_by_name, (), inventory)
    candidate.update(
        source_generations=json.loads(source_json),
        source_recipes_json=reference_json,
        validation_json={receipt["component"]: receipt},
    )
    connection = _connection([candidate])
    assert verify_source_recipe_metadata(candidate).inventory == inventory
    assert await resume.resolve_registered_source_inventory(connection, request, 7, 9, control_schema="fixture") is True
    assert await resume.resolve_registered_contribution_resume(
        connection, request, 7, 9, control_schema="fixture"
    ) == resume.RegisteredContributionResume(target, 2)


def test_inventory_ptg_selection_normalization_keeps_all_non_ptg_recipes():
    recipes = tuple(_example(index) for index in range(101))
    assert selection._normalized_recipes(recipes, True) == SourceInventoryPreparation(recipes).recipes
    selection.require_ptg_startup_selection(recipes, (), inventory=True)
    with pytest.raises(RegistrySourceRecipeError):
        selection._normalized_recipes(recipes, False)
