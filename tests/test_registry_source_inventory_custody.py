# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Inventory custody uses independently staged facts, native office parity and complete roots."""

import hashlib
import json
from contextlib import asynccontextmanager
from dataclasses import asdict, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest

from process import network_membership_candidate_lifecycle as lifecycle
from process import registry_source_contribution_artifacts as artifacts
from process import registry_source_contribution_store as store
from process import registry_source_office_custody as offices
from process.network_address_projection import PinnedAddressSource
from process.registry_source_inventory_preparation import SourceInventoryPreparation
from process.registry_source_inventory_reference import SourceInventoryReference
from process.registry_source_inventory_summaries import office_summary_v4
from process.registry_source_recipe_composition import RegistrySourceRecipeStage
from process.registry_source_recipe_store import canonical_registry_source_recipes
from process.registry_source_retention import registry_source_retention_sha256
from tests.registry_source_recipe_support import _capture_recipe, _derivation, _descriptor, _example
from tests.test_network_provider_only_projection import _target
from tests.test_registry_ptg_office_membership_recipe import _recipe as _ptg_recipe

_APPROVED = SimpleNamespace(approved_revision=7, generation_id="b" * 64)
_ADDRESS = PinnedAddressSource("synthetic_addresses", "entity_address_unified", "c" * 64)


def _inventory(recipes, *, provider_only=False, anchored=False):
    preparation = SourceInventoryPreparation(recipes)
    descriptors, derivations = [], []
    for recipe in preparation.recipes:
        descriptor = (
            _descriptor(recipe, anchored=anchored) if recipe.binding_coordinates.source_system == "fhir" else None
        )
        if descriptor is not None:
            if provider_only:
                descriptor["provider_only_rows"] = 1
            descriptors.append(descriptor)
            preparation.add_contribution(descriptor)
        derivation = _derivation(recipe, descriptor, facts=anchored)
        derivations.append(derivation)
        preparation.add_derivation(derivation)
    reference, entries = preparation.finish()
    stage = RegistrySourceRecipeStage(
        "registry_source_page_" + "a" * 32,
        0,
        0,
        "a" * 64,
        contribution_table_name="registry_source_fact_" + "b" * 32 if descriptors else None,
        contribution_receipts=tuple(descriptors),
        inventory=True,
    )
    return reference.root, entries, stage, derivations


def _candidate(copy_target, root, stage, derivations):
    candidate_by_field = {
        field: getattr(copy_target, field)
        for field in ("candidate_id", "dataset_id", "schema_id", "producer_id", "schema_name")
    }
    candidate_by_field.update(
        state="ready",
        expected_head=None,
        approved_custom_revision=7,
        source_recipes_json=SourceInventoryReference(root).as_dict(),
        source_generations={
            "registry_source_inventory": root.entries_sha256,
            "custom_membership": _APPROVED.generation_id,
        },
        validation_json={
            "address_source": asdict(_ADDRESS),
            "writer_closure": {"loader_roles": ["loader"], "reader_roles": ["reader"]},
        },
    )
    if root.entry_count:
        candidate_by_field["source_generations"]["registry_source_recipes"] = root.recipes_sha256
        office = office_summary_v4(
            root, _APPROVED, _ADDRESS, sorted(derivations, key=lambda d: d["recipe_sha256"]), None
        )
        candidate_by_field["validation_json"]["registry_initial_source_offices"] = office
        from process.registry_initial_source_composition import initial_source_office_receipt_sha256

        candidate_by_field["source_generations"]["registry_initial_source_offices"] = (
            initial_source_office_receipt_sha256(office, inventory_root=root)
        )
        candidate_by_field["source_generations"]["unified_address"] = _ADDRESS.generation_id
    if root.contribution_count:
        receipt_by_field = store.registry_source_contribution_receipt(stage, inventory_root=root)
        candidate_by_field["validation_json"][store.SUMMARY_KEY] = receipt_by_field
        candidate_by_field["source_generations"][store.SUMMARY_KEY] = store.registry_source_contribution_sha256(
            receipt_by_field, inventory_root=root
        )
    _bind_retention(candidate_by_field, stage)
    return candidate_by_field


def _bind_retention(candidate_by_field, stage):
    receipt_by_field = {
        "component": "registry_source_retention",
        "revision": 1,
        "table_name": "network_membership_source",
        "source_generation_sha256": stage.generation_sha256,
        "source_rows": stage.membership_rows,
        "add_rows": 0,
        "remove_rows": 0,
        "suppressed_source_rows": 0,
        "effective_rows": stage.membership_rows,
    }
    candidate_by_field["expected_rows"] = stage.membership_rows
    candidate_by_field["validation_json"]["registry_source_retention"] = receipt_by_field
    candidate_by_field["source_generations"]["registry_source_retention"] = registry_source_retention_sha256(
        receipt_by_field
    )


@pytest.mark.parametrize("count", [0, 1, 101, 1000])
def test_explicit_root_receipt_preserves_legacy(count):
    root, _entries, stage, _derivations = _inventory(tuple(_example(index) for index in range(count)))
    receipt_by_field = store.registry_source_contribution_receipt(stage, inventory_root=root)
    assert receipt_by_field["revision"] == 3 and receipt_by_field["source_count"] == count
    assert "sources" not in receipt_by_field
    if count == 0:
        assert store.registry_source_contribution_receipt(stage) is None
    elif count <= 100:
        assert store.registry_source_contribution_receipt(stage)["revision"] == 2
    else:
        with pytest.raises(store.RegistrySourceContributionError):
            store.registry_source_contribution_receipt(stage)


@pytest.mark.parametrize("anchored", [False, True])
def test_independent_inventory_requires_local_facts(anchored):
    from process.network_compact_layout import INDEPENDENT_STORAGE, INDEPENDENT_STORAGE_KEY

    root, entries, stage, derivations = _inventory((_capture_recipe(),), anchored=anchored)
    candidate = _candidate(_target(), root, stage, derivations)
    assert store._inventory_source_metadata(candidate, entries)[0] == root
    candidate["source_generations"][INDEPENDENT_STORAGE_KEY] = INDEPENDENT_STORAGE
    if anchored:
        with pytest.raises(store.RegistrySourceContributionError, match="inventory_invalid"):
            store._inventory_source_metadata(candidate, entries)
    else:
        assert store._inventory_source_metadata(candidate, entries)[0] == root


@pytest.mark.parametrize("damage", ["missing", "list", "partial", "reversed", "header", "incomplete"])
def test_supplied_inventory_requires_complete_fold(damage):
    root, entries, stage, derivations = _inventory((_example(0), _example(1)))
    candidate_by_field = _candidate(_target(), root, stage, derivations)
    supplied_entries = {"missing": None, "list": list(entries), "partial": entries[:1], "reversed": entries[::-1]}.get(
        damage, entries
    )
    if damage == "header":
        changed = replace(root, entries_sha256="0" * 64)
        candidate_by_field["source_recipes_json"] = SourceInventoryReference(changed).as_dict()
        candidate_by_field["source_generations"]["registry_source_inventory"] = changed.entries_sha256
    if damage == "incomplete":
        preparation = SourceInventoryPreparation((_example(0), _example(1)))
        supplied_entries = tuple(
            replace(entry, contribution_json=None, office_derivation_json=None) for entry in entries
        )
        candidate_by_field["source_recipes_json"] = SourceInventoryReference(preparation.recipe_root).as_dict()
        candidate_by_field["source_generations"]["registry_source_inventory"] = preparation.recipe_root.entries_sha256
    with pytest.raises(store.RegistrySourceContributionError, match="inventory_invalid"):
        store._inventory_source_metadata(candidate_by_field, supplied_entries)


@asynccontextmanager
async def _transaction():
    yield


def _row_connection(stage):
    counts = [
        {"recipe_sha256": descriptor["recipe_sha256"], "fact_rows": descriptor["fact_rows"]}
        for descriptor in stage.contribution_receipts
        if descriptor.get("fact_candidate_id") is None
    ]
    return SimpleNamespace(
        is_in_transaction=lambda: True,
        transaction=_transaction,
        fetch=AsyncMock(return_value=counts),
        fetchval=AsyncMock(return_value=False),
        execute=AsyncMock(),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["fhir", "anchored", "all_raw", "empty"])
async def test_native_rows_use_inventory_descriptors(monkeypatch, kind):
    recipes = (_capture_recipe(),) if kind in {"fhir", "anchored"} else (_ptg_recipe(),) if kind == "all_raw" else ()
    root, entries, stage, derivations = _inventory(recipes, anchored=kind == "anchored")
    copy_target = _target()
    candidate_by_field = _candidate(copy_target, root, stage, derivations)
    connection = _row_connection(stage)
    verify = AsyncMock(return_value={})
    monkeypatch.setattr(artifacts, "verify_anchored_source_fact_contents", verify)
    content = AsyncMock(
        return_value=(stage.contribution_receipts[0]["fact_sha256"], 1) if stage.contribution_receipts else None
    )
    monkeypatch.setattr(store, "contribution_fact_sha256", content)
    await store.verify_registry_source_contributions_rows(
        connection, copy_target, candidate_by_field, source_stage=stage, inventory_entries=entries
    )
    connection.fetch.assert_awaited_once()
    verify.assert_not_awaited()
    content.assert_not_awaited()
    connection.fetchval.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("recipes", [(), (_ptg_recipe(),)])
async def test_zero_contributions_reject_stray_facts(recipes):
    root, entries, stage, derivations = _inventory(recipes)
    copy_target = _target()
    candidate_by_field = _candidate(copy_target, root, stage, derivations)
    connection = _row_connection(stage)
    connection.fetch.return_value = [{"recipe_sha256": "0" * 64, "fact_rows": 1}]
    with pytest.raises(store.RegistrySourceContributionError, match="accounting_invalid"):
        await store.verify_registry_source_contributions_rows(
            connection, copy_target, candidate_by_field, inventory_entries=entries
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["scope", "count", "stage", "missing_inventory"])
async def test_native_rows_refuse_mismatch(monkeypatch, failure):
    root, entries, stage, derivations = _inventory((_capture_recipe(),))
    copy_target = _target()
    candidate_by_field = _candidate(copy_target, root, stage, derivations)
    connection = _row_connection(stage)
    monkeypatch.setattr(artifacts, "verify_anchored_source_fact_contents", AsyncMock(return_value={}))
    monkeypatch.setattr(
        store, "contribution_fact_sha256", AsyncMock(return_value=(stage.contribution_receipts[0]["fact_sha256"], 1))
    )
    match failure:
        case "scope":
            candidate_by_field["producer_id"] = str(UUID(int=77))
        case "count":
            connection.fetch.return_value = []
        case "stage":
            stage = replace(stage, contribution_receipts=())
    with pytest.raises(ValueError):
        await store.verify_registry_source_contributions_rows(
            connection,
            copy_target,
            candidate_by_field,
            source_stage=stage,
            inventory_entries=None if failure == "missing_inventory" else entries,
        )
    if failure == "missing_inventory":
        connection.fetch.assert_not_awaited()


@pytest.mark.asyncio
async def test_retention_uses_stage_owned_rows(monkeypatch):
    root, entries, stage, derivations = _inventory((_capture_recipe(),))
    copy_target = _target()
    candidate_by_field = _candidate(copy_target, root, stage, derivations)
    candidate_by_field["state"] = "open"
    connection = _row_connection(stage)
    monkeypatch.setattr(lifecycle, "_require_transaction", Mock())
    monkeypatch.setattr(lifecycle, "_locked_candidate", AsyncMock(return_value=candidate_by_field))
    verify = AsyncMock()
    monkeypatch.setattr(store, "verify_registry_source_contributions_rows", verify)
    await store.retain_registry_source_contributions(connection, copy_target, stage, inventory_entries=entries)
    assert any(call.args[0].startswith("INSERT INTO") for call in connection.execute.await_args_list)
    assert verify.await_args.kwargs["inventory_entries"] is entries


@pytest.mark.asyncio
async def test_legacy_retention_keeps_call_shape(monkeypatch):
    recipe = _capture_recipe()
    root, _entries, stage, derivations = _inventory((recipe,))
    copy_target = _target()
    candidate_by_field = _candidate(copy_target, root, stage, derivations)
    candidate_by_field["state"] = "open"
    candidate_by_field["source_recipes_json"] = json.loads(canonical_registry_source_recipes((recipe,)))
    receipt_by_field = store.registry_source_contribution_receipt(stage)
    candidate_by_field["validation_json"][store.SUMMARY_KEY] = receipt_by_field
    candidate_by_field["source_generations"][store.SUMMARY_KEY] = store.registry_source_contribution_sha256(
        receipt_by_field
    )
    connection = _row_connection(stage)
    monkeypatch.setattr(lifecycle, "_require_transaction", Mock())
    monkeypatch.setattr(lifecycle, "_locked_candidate", AsyncMock(return_value=candidate_by_field))
    verified_stages = []

    async def verify(_connection, _target, _candidate, *, source_stage, control_schema):
        verified_stages.append(source_stage)

    monkeypatch.setattr(store, "verify_registry_source_contributions_rows", verify)
    assert await store.retain_registry_source_contributions(connection, copy_target, stage) == receipt_by_field
    assert verified_stages == [stage]


@pytest.mark.asyncio
async def test_changed_verified_office_page_hash_refuses_before_copy(monkeypatch):
    from process import network_initial_cms_office_bindings as cms

    original = "[]"
    matches = SimpleNamespace(page_proofs_by_key={"page": (None, hashlib.sha256(original.encode()).hexdigest())})
    monkeypatch.setattr(cms, "_matched_page", AsyncMock(return_value='[{"changed":true}]'))
    copy = AsyncMock()
    monkeypatch.setattr(offices, "copy_source_office_binding_rows", copy)
    with pytest.raises(offices.RegistryInitialSourceError, match="summary_differs"):
        await offices._copy_verified_binding_pages(object(), matches, "expected_offices")
    copy.assert_not_awaited()
