# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Complete refresh commitments scale without granting Source authority or hydrating registered inventory."""

import asyncio
import hashlib
import json
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import registry_initial_source_composition as offices
from process import registry_membership_refresh_execution as execution
from process import registry_membership_refresh_runtime as runtime
from process import registry_membership_refresh_sources as sources
from process import registry_source_inventory_store as inventory_store
from process import registry_source_recipe_store as recipe_store
from process.network_address_projection import PinnedAddressSource
from process.registry_approval_store import RegistryApprovalConflict
from process.registry_candidate_composition import RegistryCompositionAddressSources
from process.registry_refresh_anchor_contract import RegistryRefreshAnchorUnavailable
from process.registry_source_inventory_preparation import SourceInventoryPreparation
from process.registry_source_inventory_reference import SourceInventoryReference
from tests.registry_membership_refresh_runtime_support import ObservationCase
from tests.test_registry_membership_refresh_runtime import scope as enrolled_scope
from tests.test_registry_ptg_office_membership_recipe import _recipe as _ptg_recipe
from tests.test_registry_source_inventory_preparation import _derivation

_ADDRESS = PinnedAddressSource("synthetic_addresses", "entity_address_unified", "a" * 64)


def _recipe(index):
    recipe = _ptg_recipe()
    coordinates = replace(recipe.binding_coordinates, source_id=f"source-{index:04}")
    return replace(
        recipe,
        binding_coordinates=coordinates,
        source_pin=replace(recipe.source_pin, coordinates=coordinates, capture_id=str(UUID(int=index + 1))),
    )


def _recipe_set(count):
    return tuple(_recipe(index) for index in reversed(range(count)))


def _inputs(recipes):
    return RegistryCompositionAddressSources(_ADDRESS, replacement_source_recipes=recipes, compact=True)


def _registered(recipes):
    preparation = SourceInventoryPreparation(recipes)
    for recipe in preparation.recipes:
        preparation.add_derivation(_derivation(recipe))
    reference, _entries = preparation.finish()
    generations_by_name = {"registry_source_inventory": reference.root.entries_sha256}
    if reference.root.entry_count:
        generations_by_name["registry_source_recipes"] = reference.root.recipes_sha256
    return {"source_recipes_json": reference.as_dict(), "source_generations": generations_by_name}


@pytest.mark.parametrize("count", [1, 100, 101, 1000])
def test_accepted_recipes_preserve_complete_wire(count):
    recipes = _recipe_set(count)
    accepted = sources._result(recipes)
    assert len(accepted.recipes) == len(accepted.accepted_sources) == count
    assert sources._recipe_commitment(accepted.recipes) == sources._recipe_commitment(recipes)
    count_and_sha = sources._recipe_commitment(recipes)
    if count <= 100:
        canonical = recipe_store.canonical_registry_source_recipes(recipes)
        assert count_and_sha == (count, hashlib.sha256(canonical.encode()).hexdigest())
    else:
        with pytest.raises(recipe_store.RegistrySourceRecipeError):
            recipe_store.canonical_registry_source_recipes(recipes)


def test_accepted_recipe_bound_and_duplicates():
    accepted = sources._result((_recipe(0),))
    for recipes in (_recipe_set(1001), (_recipe(0), _recipe(0))):
        with pytest.raises(ValueError):
            sources.AcceptedMembershipRefreshSources(accepted.accepted_sources, recipes)
    with pytest.raises(RegistryRefreshAnchorUnavailable):
        sources.AcceptedMembershipRefreshSources(accepted.accepted_sources, list(accepted.recipes))


@pytest.mark.parametrize("count", [0, 1, 100])
def test_legacy_complete_sets_keep_original_codec(count):
    recipes = _recipe_set(count)
    canonical = recipe_store.canonical_registry_source_recipes(recipes)
    candidate_by_field = {
        "source_recipes_json": json.loads(canonical),
        "source_generations": {"registry_source_recipes": hashlib.sha256(canonical.encode()).hexdigest()}
        if recipes
        else {},
    }
    inputs = _inputs(recipes)
    execution._require_prepared_recipes(candidate_by_field, inputs, inputs)


@pytest.mark.parametrize("count", [0, 1, 101, 1000])
def test_final_complete_equality_never_hydrates(monkeypatch, count):
    recipes = _recipe_set(count)
    candidate_by_field = _registered(recipes)
    original = _inputs(recipes)
    final = _inputs(tuple(reversed(recipes)))

    def forbidden(*_arguments, **_options):
        pytest.fail("Complete commitment equality must not hydrate registered inventory or decode legacy arrays")

    for name in ("read_source_inventory_page", "verify_source_inventory", "_load_locked_source_inventory"):
        monkeypatch.setattr(inventory_store, name, forbidden)
    monkeypatch.setattr(recipe_store, "verify_registry_source_recipes", forbidden)
    execution._require_prepared_recipes(candidate_by_field, original, final)
    assert set(candidate_by_field) == {"source_recipes_json", "source_generations"}


@pytest.mark.parametrize("storage", ["legacy", "inventory"])
@pytest.mark.parametrize("mismatch", ["extra", "missing", "different", "unexpected_when_empty"])
def test_registered_set_must_equal_replacement(storage, mismatch):
    expected = (_recipe(0),)
    actual = {
        "extra": (_recipe(0), _recipe(1)),
        "missing": (),
        "different": (_recipe(1),),
        "unexpected_when_empty": (_recipe(0),),
    }[mismatch]
    if mismatch == "unexpected_when_empty":
        expected = ()
    if storage == "inventory":
        candidate_by_field = _registered(actual)
    else:
        canonical = recipe_store.canonical_registry_source_recipes(actual)
        candidate_by_field = {
            "source_recipes_json": json.loads(canonical),
            "source_generations": {"registry_source_recipes": hashlib.sha256(canonical.encode()).hexdigest()}
            if actual
            else {},
        }
    inputs = _inputs(expected)
    with pytest.raises(RegistryApprovalConflict, match="candidate_recipes_changed"):
        execution._require_prepared_recipes(candidate_by_field, inputs, inputs)


def test_final_fence_preserves_source_and_address_guards():
    recipes = _recipe_set(101)
    original = _inputs(recipes)
    candidate_by_field = _registered(recipes)
    for final in (
        _inputs(recipes[:-1]),
        _inputs((_recipe(999), *recipes[:-1])),
        replace(original, base_source=replace(_ADDRESS, generation_id="b" * 64)),
    ):
        with pytest.raises(RegistryApprovalConflict, match="sources_changed"):
            execution._require_prepared_recipes(candidate_by_field, original, final)


@asynccontextmanager
async def _nested():
    yield


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [101, 1000])
async def test_readiness_selects_office4_without_receipt(monkeypatch, count):
    case = ObservationCase(monkeypatch)
    selected = sources._result(_recipe_set(count))
    session = SimpleNamespace(begin_nested=_nested)

    async def prepare(_connection, recipes, approved, pin, request):
        assert len(recipes) == count and approved == case.approved and pin == case.pin
        assert request.revision == 4 and request.inventory_root is None
        assert request.source_manifest is manifest and callable(request.derivation_sink)
        assert not asyncio.iscoroutinefunction(request.derivation_sink)
        for recipe in recipes:
            assert request.derivation_sink(_derivation(recipe)) is None
        return None

    manifest = object()
    proof = AsyncMock(side_effect=prepare)
    monkeypatch.setattr(offices, "prepare_source_offices", proof)
    actual = await runtime.RegistryMembershipRefreshInputsService._has_office_correspondence(
        case.service, session, case.approved, selected, case.pin, manifest
    )
    assert actual is True
    proof.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("mismatch", [None, "missing", "changed"])
async def test_observed_and_fenced_sets_compare_completely(monkeypatch, mismatch):
    case = ObservationCase(monkeypatch)
    case.selected = sources._result(_recipe_set(101))
    observed_recipes = _recipe_set(100) if mismatch == "missing" else (_recipe(999), *_recipe_set(100))
    observed = (
        case.selected
        if mismatch is None
        else sources.AcceptedMembershipRefreshSources(case.selected.accepted_sources, observed_recipes)
    )
    native_address = case.pin, case.descriptor
    if mismatch:
        with pytest.raises(runtime.RegistryMembershipRefreshUnavailable):
            await case.service._readiness(object(), case.approved, observed, [enrolled_scope(1)], None, native_address)
        case.prepare.assert_not_awaited()
        assert case.service._readiness_key is None
    else:
        actual = await case.service._readiness(
            object(), case.approved, observed, [enrolled_scope(1)], None, native_address
        )
        assert actual[1] == case.pin.generation_id
        case.prepare.assert_awaited_once()


@pytest.mark.asyncio
async def test_cancelled_readiness_never_becomes_ready(monkeypatch):
    case = ObservationCase(monkeypatch)
    case.selected = sources._result(_recipe_set(101))
    case.prepare.side_effect = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await case.service._readiness(
            object(), case.approved, case.selected, [enrolled_scope(1)], None, (case.pin, case.descriptor)
        )
    assert case.service._readiness_key is None and case.service._is_ready is False
