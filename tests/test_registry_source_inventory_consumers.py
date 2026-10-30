# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure consumer wiring checks; native custody/physical locks remain separate proof."""

import copy
import hashlib
import json
from contextlib import asynccontextmanager
from dataclasses import asdict, replace
from functools import cache
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID, uuid4

import pytest

from db.registry_source_inventory_schema import INVENTORY_TABLE
from process import network_candidate_custody as custody_module
from process import network_compact_address_carriers as carriers
from process import registry_aca_address_carry as aca_carry
from process import registry_ptg_address_carry as ptg_carry
from process import registry_required_target_coverage as coverage
from process import registry_source_inventory_store as inventory_store
from process import registry_source_recipe_store as recipes
from process.network_address_projection import PinnedAddressSource
from process.network_approved_membership_source import ApprovedMembershipSource
from process.network_membership_copy import MembershipCopyTarget
from process.network_serving_read import PinnedNetworkServingManifest
from process.registry_source_inventory_contract import make_source_inventory_entry, source_inventory_root
from process.registry_source_inventory_reference import INVENTORY_DIGEST_KEY, SourceInventoryReference
from tests.test_registry_ptg_office_membership_recipe import _recipe as _ptg_recipe
from tests.test_registry_source_inventory_preparation import _aca_recipe
from tests.test_registry_source_recipe_store_postgres import _example

CONTROL = "synthetic_control"
APPROVED = ApprovedMembershipSource(3, "b" * 64, 0)


def _inventory(count=1, *, legacy=False):
    selected_recipes = tuple(
        sorted(
            (_example(index) for index in range(count)),
            key=lambda recipe: recipes.canonical_registry_source_recipes((recipe,)),
        )
    )
    entries = tuple(make_source_inventory_entry(index, recipe) for index, recipe in enumerate(selected_recipes, 1))
    reference = SourceInventoryReference(source_inventory_root(entries))
    identity = str(uuid4())
    copy_target = MembershipCopyTarget(
        *(str(uuid4()) for _ in range(3)), identity, "network_candidate_" + identity.replace("-", "")
    )
    generations_by_source = {
        INVENTORY_DIGEST_KEY: reference.root.entries_sha256,
        "custom_membership": APPROVED.generation_id,
    }
    if count:
        generations_by_source[recipes.RECIPE_DIGEST_KEY] = reference.root.recipes_sha256
    candidate_by_field = {
        **asdict(copy_target),
        "state": "published",
        "source_generations": generations_by_source,
        "source_recipes_json": reference.as_dict(),
        "approved_custom_revision": APPROVED.approved_revision,
    }
    if legacy:
        candidate_by_field["source_recipes_json"] = json.loads(
            recipes.canonical_registry_source_recipes(selected_recipes)
        )
        generations_by_source.pop(INVENTORY_DIGEST_KEY)
    manifest = PinnedNetworkServingManifest(
        7,
        identity,
        copy_target.schema_name,
        1,
        copy.deepcopy(generations_by_source),
        APPROVED.approved_revision,
        "d" * 64,
        71,
    )
    return SimpleNamespace(
        selected_recipes=selected_recipes,
        entries=entries,
        reference=reference,
        copy_target=copy_target,
        candidate_by_field=candidate_by_field,
        manifest=manifest,
    )


@pytest.fixture(scope="module")
def full_inventory():
    return _inventory(1000)


def _retained_spies(monkeypatch, fixture):
    events = []
    connection = AsyncMock()
    connection.is_in_transaction = lambda: True
    connection.fetchval.return_value = "repeatable read"
    connection.fetchrow.return_value = fixture.candidate_by_field

    async def resolve(connection, *, generation_id, control_schema):
        assert generation_id == fixture.manifest.generation_id and control_schema == CONTROL
        events.append("manifest")
        return fixture.manifest

    async def custody(connection, copy_target, *, control_schema, required):
        assert (
            events == ["manifest"]
            and required is True
            and copy_target == fixture.copy_target
            and control_schema == CONTROL
        )
        events.append("custody")
        return {"receipt": {"closure": {"relation_oids": {INVENTORY_TABLE: 88}}}}

    async def inventory(connection, copy_target, candidate_by_field, *, expected_table_oid, control_schema):
        assert events == ["manifest", "custody"] and expected_table_oid == 88
        assert (
            copy_target == fixture.copy_target
            and candidate_by_field == fixture.candidate_by_field
            and control_schema == CONTROL
        )
        events.append("inventory")
        return fixture.reference, fixture.entries

    resolver, seal, loader = (
        AsyncMock(side_effect=resolve),
        AsyncMock(side_effect=custody),
        AsyncMock(side_effect=inventory),
    )
    monkeypatch.setattr(recipes, "resolve_network_serving_manifest", resolver)
    monkeypatch.setattr(custody_module, "verify_network_candidate_custody", seal)
    monkeypatch.setattr(inventory_store, "_load_locked_source_inventory", loader)
    return connection, resolver, seal, loader, events


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [0, 1, 101, 1000])
async def test_retained_resolver_loads_full_inventory_only_after_required_custody(monkeypatch, full_inventory, count):
    fixture = full_inventory if count == 1000 else _inventory(count)
    connection, _, seal, loader, events = _retained_spies(monkeypatch, fixture)
    result = await recipes.resolve_retained_registry_source_recipes(
        connection, fixture.manifest, control_schema=CONTROL
    )
    assert result == fixture.selected_recipes and len(result) == count
    assert events == ["manifest", "custody", "inventory"]
    seal.assert_awaited_once()
    loader.assert_awaited_once()
    assert connection.fetchval.await_args_list[0].args == ("SHOW transaction_isolation",)
    assert connection.fetchval.await_count == 1
    connection.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["manifest", "candidate", "schema", "generation", "state", "seal", "oid", "loader"])
async def test_retained_scope_or_custody_refusal_never_falls_back_to_raw_recipes(monkeypatch, damage):
    fixture = _inventory()
    connection, resolver, seal, loader, events = _retained_spies(monkeypatch, fixture)
    if damage == "manifest":
        resolver.side_effect = None
        resolver.return_value = replace(fixture.manifest, generation_id=8)
    if damage == "candidate":
        fixture.candidate_by_field["candidate_id"] = str(uuid4())
    if damage == "schema":
        fixture.candidate_by_field["schema_name"] = "network_candidate_" + uuid4().hex
    if damage == "generation":
        fixture.candidate_by_field["source_generations"]["custom_membership"] = "f" * 64
    if damage == "state":
        fixture.candidate_by_field["state"] = "ready"
    if damage == "seal":
        seal.side_effect = ValueError("synthetic custody refusal")
    if damage == "oid":
        seal.side_effect = None
        seal.return_value = {"receipt": {"closure": {"relation_oids": {}}}}
    if damage == "loader":
        loader.side_effect = ValueError("synthetic inventory refusal")
    with pytest.raises(recipes.RegistrySourceRecipeError, match="unavailable"):
        await recipes.resolve_retained_registry_source_recipes(connection, fixture.manifest, control_schema=CONTROL)
    assert events != ["manifest", "custody", "inventory"]


@pytest.mark.asyncio
async def test_legacy_retained_bytes_and_hash_caps_are_unchanged(monkeypatch):
    fixture = _inventory(2, legacy=True)
    connection, _, seal, loader, _ = _retained_spies(monkeypatch, fixture)
    assert (
        await recipes.resolve_retained_registry_source_recipes(connection, fixture.manifest, control_schema=CONTROL)
        == fixture.selected_recipes
    )
    seal.assert_not_awaited()
    loader.assert_not_awaited()
    with pytest.raises(recipes.RegistrySourceRecipeError):
        recipes.canonical_registry_source_recipes(_inventory(101).selected_recipes)


@pytest.mark.asyncio
async def test_carrier_projection_reads_registered_inventory_after_existing_scope(monkeypatch, full_inventory):
    fixture = full_inventory
    candidate_by_field = {**fixture.candidate_by_field, "state": "open"}
    connection = AsyncMock()
    events = []
    base = PinnedAddressSource("synthetic_base", "entity_address_unified", "base-generation")
    physical = PinnedAddressSource("synthetic_physical", "entity_address_unified", "base-generation")

    async def scope(*_arguments, **_options):
        events.append("scope")
        return candidate_by_field, {"descriptor_sha256": "a" * 64}, physical

    async def oid(query, relation):
        assert events == ["scope"] and query == "SELECT to_regclass($1)::oid::bigint"
        assert relation == '"' + fixture.copy_target.schema_name + '".network_source_inventory'
        events.append("oid")
        return 88

    async def inventory(connection, copy_target, current, *, expected_table_oid, control_schema):
        assert events == ["scope", "oid"] and expected_table_oid == 88 and control_schema == CONTROL
        assert current == candidate_by_field and copy_target == fixture.copy_target
        events.append("inventory")
        return fixture.reference, fixture.entries

    monkeypatch.setattr(carriers, "_scope", scope)
    connection.fetchval.side_effect = oid
    monkeypatch.setattr(inventory_store, "_load_locked_source_inventory", inventory)
    monkeypatch.setattr(
        carriers, "canonical_registry_source_recipes", lambda *_: pytest.fail("Whole-array legacy cap reached")
    )
    receipt, actual_physical, selected_recipes, reference = await carriers._projection(
        connection, fixture.copy_target, base, APPROVED, ("synthetic_reader",), CONTROL, object(), writing=True
    )
    assert (
        selected_recipes == fixture.selected_recipes and reference == fixture.reference and actual_physical == physical
    )
    assert receipt["source_recipes_sha256"] == fixture.reference.root.recipes_sha256
    assert events == ["scope", "oid", "inventory"]


@pytest.mark.parametrize("damage", ["same", "order", "missing", "substitute"])
def test_carrier_exact_singletons_cover_complete_inventory_without_legacy_cap(full_inventory, damage):
    selected_recipes = full_inventory.selected_recipes
    changed_recipes = list(selected_recipes)
    if damage == "order":
        changed_recipes.reverse()
    if damage == "missing":
        changed_recipes.pop()
    if damage == "substitute":
        changed_recipes[-1] = replace(
            changed_recipes[-1], source_pin=replace(changed_recipes[-1].source_pin, dataset_sha256="f" * 64)
        )
    assert carriers._inventory_recipes_match(changed_recipes, selected_recipes) is (damage == "same")


@pytest.mark.asyncio
async def test_required_target_collection_validates_metadata_without_hydrating_sources(monkeypatch, full_inventory):
    fixture = full_inventory
    connection = AsyncMock()
    connection.fetchrow.side_effect = [
        {"isolation": "repeatable read", "readonly": "on"},
        {"metadata_valid": True},
        {
            **fixture.candidate_by_field,
            "candidate_id": UUID(fixture.copy_target.candidate_id),
            "manifest_status": "current",
        },
        {"valid": True, "bounded": True, "native_input": "{}", "provenance": "[]"},
    ]
    monkeypatch.setattr(coverage, "resolve_network_serving_manifest", AsyncMock(return_value=fixture.manifest))
    monkeypatch.setattr(coverage, "_verify_ledger", Mock(return_value={"target_count": 0}))
    monkeypatch.setattr(coverage, "pin_retained_approved_membership_source", AsyncMock(return_value=APPROVED))
    monkeypatch.setattr(coverage, "_read_review_bundle", AsyncMock(return_value={}))
    monkeypatch.setattr(coverage, "required_target_addresses", lambda *_args, **_options: ("synthetic_addresses", ()))
    monkeypatch.setattr(coverage, "_native", lambda *_: {})
    monkeypatch.setattr(coverage, "_coverage_report", lambda *_: {"metadata_verified": True})
    pricing = AsyncMock(side_effect=lambda _connection, report, *_args, **_options: report)
    monkeypatch.setattr(coverage, "append_pricing_binding_metadata", pricing)
    verifier = Mock(wraps=coverage.verify_source_recipe_metadata)
    monkeypatch.setattr(coverage, "verify_source_recipe_metadata", verifier)
    monkeypatch.setattr(recipes, "verify_registry_source_recipes", lambda *_: pytest.fail("Hydrated inventory recipes"))
    assert await coverage._read(connection, uuid4(), fixture.manifest.generation_id, CONTROL) == {
        "metadata_verified": True
    }
    verifier.assert_called_once()
    pricing.assert_awaited_once()
    connection.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_carrier_prepare_reaches_source_checks_with_full_inventory(monkeypatch, full_inventory):
    fixture = full_inventory
    connection = AsyncMock()
    base = PinnedAddressSource("synthetic_base", "entity_address_unified", "base-generation")

    @asynccontextmanager
    async def transaction(*_arguments):
        yield

    connection.transaction = transaction
    connection.fetchval.return_value = True
    monkeypatch.setattr(
        carriers, "_projection", AsyncMock(return_value=({}, base, fixture.selected_recipes, fixture.reference))
    )
    monkeypatch.setattr(carriers, "_scratch", transaction)
    source_check = AsyncMock(side_effect=RuntimeError("synthetic SOURCE check reached"))
    monkeypatch.setattr(carriers, "_fill", source_check)
    with pytest.raises(RuntimeError, match="SOURCE check reached"):
        await carriers.prepare_compact_address_carriers(
            connection,
            fixture.copy_target,
            base,
            APPROVED,
            fixture.selected_recipes,
            runtime_roles=("synthetic_reader",),
            control_schema=CONTROL,
        )
    assert source_check.await_args.args[2] == fixture.selected_recipes
    assert source_check.await_args.kwargs == {"inventory": True}
    connection.execute.assert_not_awaited()


def _family_recipes(family, count):
    if family == "fhir":
        return tuple(_example(index) for index in range(count))
    recipe = _aca_recipe() if family == "aca" else _ptg_recipe()
    selected_recipes = []
    for index in range(count):
        if family == "aca":
            prepared = replace(
                recipe.source_pin.prepared,
                ownership=replace(recipe.source_pin.prepared.ownership, auxiliary_oid=10000 + index),
            )
            pin = replace(recipe.source_pin, prepared=prepared)
            coordinates = recipe.binding_coordinates
        else:
            coordinates = replace(recipe.binding_coordinates, dataset_id=f"dataset-{index}")
            pin = replace(recipe.source_pin, capture_id=str(UUID(int=index + 1)), coordinates=coordinates)
        selected_recipes.append(recipes.RegistrySourceMembershipRecipe(pin, coordinates))
    return tuple(selected_recipes)


async def _carry(family, selected_recipes, *, inventory):
    if family == "aca":
        return await aca_carry.prepare_aca_address_carry(
            AsyncMock(), selected_recipes, APPROVED, CONTROL, "synthetic_target", inventory=inventory
        )
    return await ptg_carry.prepare_ptg_address_carry(
        AsyncMock(), selected_recipes, APPROVED, CONTROL, object(), "synthetic_target", inventory=inventory
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("family", ["aca", "ptg"])
@pytest.mark.parametrize("count", [101, 1000])
async def test_inventory_carries_validate_full_other_family_without_inventing_sources(
    monkeypatch, full_inventory, family, count
):
    selected_recipes = full_inventory.selected_recipes if count == 1000 else _family_recipes("fhir", count)
    module = aca_carry if family == "aca" else ptg_carry
    source_check = AsyncMock(side_effect=AssertionError("Unexpected source-family admission"))
    monkeypatch.setattr(module, "_stream_recipe", source_check)
    carry = await _carry(family, selected_recipes, inventory=True)
    assert carry.sha256 is None and carry.pages == 0 and carry.rows == 0
    source_check.assert_not_awaited()
    with pytest.raises(recipes.RegistrySourceRecipeError):
        await _carry(family, selected_recipes, inventory=False)


@pytest.mark.asyncio
@pytest.mark.parametrize("family", ["aca", "ptg"])
@pytest.mark.parametrize("count", [101, 1000])
async def test_inventory_carries_keep_every_selected_source_and_digest_state(monkeypatch, family, count):
    if family == "aca":
        from process import reference_family_archive as archive

        # Reuse compiled model declarations; every manifest and recipe is still validated.
        monkeypatch.setattr(archive, "reference_family_spec", cache(archive.reference_family_spec))
    selected_recipes = _family_recipes(family, count)
    module = aca_carry if family == "aca" else ptg_carry
    source_check = AsyncMock(return_value=(0, 0))
    monkeypatch.setattr(module, "_stream_recipe", source_check)
    carry = await _carry(family, selected_recipes, inventory=True)
    assert source_check.await_count == count and carry.pages == 0 and carry.rows == 0
    consumed_recipes = tuple(call.args[1] for call in source_check.await_args_list)
    expected_recipes = tuple(
        sorted(selected_recipes, key=lambda recipe: recipes.canonical_registry_source_recipes((recipe,)))
    )
    assert consumed_recipes == expected_recipes
    canonical_array = (
        b"["
        + b",".join(recipes.canonical_registry_source_recipes((recipe,))[1:-1].encode() for recipe in expected_recipes)
        + b"]"
    )
    digest = hashlib.sha256(canonical_array)
    if family == "aca":
        digest.update(aca_carry._canonical({"approved_generation": APPROVED.generation_id}))
    digest.update(module._canonical({"pages": 0, "rows": 0}))
    assert carry.sha256 == digest.hexdigest()


@pytest.mark.asyncio
@pytest.mark.parametrize("family", ["aca", "ptg"])
async def test_inventory_mode_preserves_legacy_selected_array_and_page_digest(monkeypatch, family):
    selected_recipes = _family_recipes(family, 2)
    module = aca_carry if family == "aca" else ptg_carry

    async def source(connection, recipe, approved, control_schema, *arguments, **options):
        digest = arguments[1] if family == "aca" else arguments[1][0]
        digest.update(b"synthetic-native-page")
        return 1, 2

    monkeypatch.setattr(module, "_stream_recipe", source)
    assert await _carry(family, selected_recipes, inventory=True) == await _carry(
        family, selected_recipes, inventory=False
    )
    digest = recipes._source_inventory_recipe_digest(selected_recipes)
    digest.update(b"synthetic-pages")
    assert (
        digest.hexdigest()
        == hashlib.sha256(
            recipes.canonical_registry_source_recipes(selected_recipes).encode() + b"synthetic-pages"
        ).hexdigest()
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("family", ["aca", "ptg"])
async def test_inventory_carry_bound_refuses_before_any_source_use(monkeypatch, family):
    module = aca_carry if family == "aca" else ptg_carry
    source_check = AsyncMock()
    monkeypatch.setattr(module, "_stream_recipe", source_check)
    with pytest.raises(ValueError):
        await _carry(family, _family_recipes("fhir", 1001), inventory=True)
    source_check.assert_not_awaited()


@pytest.mark.asyncio
async def test_carrier_fill_passes_complete_recipe_set_and_inventory_mode(monkeypatch, full_inventory):
    selected_recipes = full_inventory.selected_recipes
    monkeypatch.setattr(carriers, "verify_approved_site_sources", AsyncMock(return_value=()))
    ptg = AsyncMock(return_value=ptg_carry.RegistryPTGAddressCarry())
    aca = AsyncMock(return_value=aca_carry.RegistryACAAddressCarry())
    monkeypatch.setattr(carriers, "prepare_ptg_address_carry", ptg)
    monkeypatch.setattr(carriers, "prepare_aca_address_carry", aca)
    await carriers._fill(AsyncMock(), APPROVED, selected_recipes, CONTROL, object(), None, inventory=True)
    assert ptg.await_args.args[1] == selected_recipes and ptg.await_args.kwargs == {"inventory": True}
    assert aca.await_args.args[1] == selected_recipes and aca.await_args.kwargs["inventory"] is True
