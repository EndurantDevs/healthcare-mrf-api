# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Inventory custody uses independently staged facts, native office parity and complete roots."""

import hashlib
import json
from contextlib import asynccontextmanager, nullcontext
from dataclasses import asdict, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest

from process import network_membership_candidate_lifecycle as lifecycle
from process import network_provider_only_projection as projection
from process import network_source_contribution_custody as custody
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
from tests.test_network_provider_only_projection import _target
from tests.test_registry_ptg_office_membership_recipe import _recipe as _ptg_recipe
from tests.test_registry_source_inventory_preparation import _derivation, _descriptor
from tests.test_registry_source_inventory_store_postgres import _capture_recipe
from tests.test_registry_source_recipe_store_postgres import _example

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
    if stage.contribution_receipts:
        assert verify.await_args.args[1] == tuple(sorted(stage.contribution_receipts, key=lambda d: d["recipe_sha256"]))
    else:
        verify.assert_not_awaited()
    assert content.await_count == int(kind == "fhir")
    assert connection.fetchval.await_count == int(kind in {"fhir", "anchored"})


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
@pytest.mark.parametrize("failure", ["scope", "count", "content", "stage", "parity", "missing_inventory"])
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
        case "content":
            store.contribution_fact_sha256.return_value = ("0" * 64, 1)
        case "stage":
            stage = replace(stage, contribution_receipts=())
        case "parity":
            connection.fetchval.return_value = True
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
@pytest.mark.parametrize(
    "empty,retained,encoded",
    [(False, False, False), (True, False, False), (True, True, False), (True, True, True), (True, False, True)],
)
async def test_independent_stage_selects_revision_two(monkeypatch, empty, retained, encoded):
    recipes = () if empty else (_ptg_recipe(),)
    root, _entries, stage, _derivations = _inventory(recipes)
    fresh_stage = replace(stage, membership_rows=0, generation_sha256="f" * 64)
    retained_stage = replace(stage, membership_rows=3, generation_sha256="e" * 64)
    selected_by_phase = AsyncMock(return_value=fresh_stage)
    monkeypatch.setattr(custody, "stage_registry_source_recipes", selected_by_phase)
    previous = AsyncMock(return_value=retained_stage)
    monkeypatch.setattr(custody, "_retained_source_stage", previous)
    generations_by_name = {} if retained else {"registry_source_membership": "f" * 64}
    candidate_by_field = {
        "expected_head": None,
        "source_generations": json.dumps(generations_by_name) if encoded else generations_by_name,
    }
    actual = await custody._independent_source_stage(
        object(),
        _target(),
        candidate_by_field,
        _APPROVED,
        expected={"revision": 3},
        control_schema="controls",
        office_authority=None,
        inventory=(root, recipes, ()),
    )
    assert (actual.membership_rows, actual.generation_sha256) == ((3, "e" * 64) if retained else (0, "f" * 64))
    if retained:
        previous.assert_awaited_once()
        selected_by_phase.assert_not_awaited()
    else:
        assert selected_by_phase.await_args.kwargs["inventory"] is True
        assert selected_by_phase.await_args.kwargs["contribution_revision"] == 2


def _custody_mocks(monkeypatch, stage, derivations, events):
    monkeypatch.setattr(custody, "pin_retained_approved_membership_source", AsyncMock(return_value=_APPROVED))
    monkeypatch.setattr(custody, "_independent_source_stage", AsyncMock(return_value=stage))
    for name, event in [
        ("verify_registry_source_contributions_rows", "facts"),
        ("verify_registry_source_retention_rows", "retention"),
        ("stage_approved_membership_commands", "commands"),
        ("verify_registry_composition_rows", "composition"),
        ("_drop_stage", "drop"),
    ]:

        async def record(*_arguments, _event=event, **_options):
            events.append(_event)
            return object()

        monkeypatch.setattr(custody, name, AsyncMock(side_effect=record))

    async def office(*_arguments, derivation_sink, **_options):
        events.append("offices")
        for derivation in derivations:
            derivation_sink(derivation)

    async def provider(*_arguments, **_options):
        events.append("projection")

    monkeypatch.setattr(offices, "_verify_inventory_source_offices", AsyncMock(side_effect=office))
    monkeypatch.setattr(projection, "_verify_staged_provider_only_projection", AsyncMock(side_effect=provider))


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["all_raw", "empty", "retained_flat", "provider_only"])
async def test_custodian_rebuilds_before_projection(monkeypatch, kind):
    recipes = (_ptg_recipe(),) if kind == "all_raw" else (_capture_recipe(),) if kind == "provider_only" else ()
    root, entries, stage, derivations = _inventory(recipes, provider_only=kind == "provider_only")
    if kind == "retained_flat":
        stage = replace(stage, membership_rows=3)
    copy_target = _target()
    candidate_by_field = _candidate(copy_target, root, stage, derivations)
    events = []
    _custody_mocks(monkeypatch, stage, derivations, events)
    await custody.verify_independent_source_contributions(
        object(), copy_target, candidate_by_field, control_schema="controls", inventory_entries=entries
    )
    assert events == ["facts", "retention", "commands", "composition", "offices", "projection", "drop"]
    assert custody.verify_registry_source_contributions_rows.await_args.kwargs["inventory_entries"] is entries
    assert projection._verify_staged_provider_only_projection.await_args.args[3] is stage


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["facts", "retention", "composition", "offices", "root", "projection"])
async def test_custodian_failure_always_drops(monkeypatch, failure):
    root, entries, stage, derivations = _inventory((_capture_recipe(),))
    copy_target = _target()
    candidate_by_field = _candidate(copy_target, root, stage, derivations)
    events = []
    _custody_mocks(monkeypatch, stage, derivations, events)
    selected_by_phase = {
        "facts": custody.verify_registry_source_contributions_rows,
        "retention": custody.verify_registry_source_retention_rows,
        "composition": custody.verify_registry_composition_rows,
        "offices": offices._verify_inventory_source_offices,
        "projection": projection._verify_staged_provider_only_projection,
    }
    if failure == "root":
        derivations[0]["correspondence_sha256"] = "9" * 64
    else:
        selected_by_phase[failure].side_effect = ValueError("independent_failure")
    with pytest.raises(ValueError):
        await custody.verify_independent_source_contributions(
            object(), copy_target, candidate_by_field, control_schema="controls", inventory_entries=entries
        )
    assert events[-1] == "drop"
    if failure != "projection":
        projection._verify_staged_provider_only_projection.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("empty", [False, True])
@pytest.mark.parametrize("parity_error", [False, True])
async def test_inventory_offices_keep_native_parity(monkeypatch, empty, parity_error):
    recipes = () if empty else (_ptg_recipe(),)
    root, _entries, stage, derivations = _inventory(recipes)
    copy_target = _target()
    candidate_by_field = _candidate(copy_target, root, stage, derivations)
    connection = SimpleNamespace(execute=AsyncMock())
    monkeypatch.setattr(offices, "_require_transaction", AsyncMock())
    monkeypatch.setattr(offices, "_native_candidate", AsyncMock())
    copy = AsyncMock()
    monkeypatch.setattr(offices, "copy_source_office_binding_rows", copy)
    retained = AsyncMock()
    monkeypatch.setattr(offices, "_retained_bindings", retained)
    monkeypatch.setattr(offices, "resolve_registry_retained_generation", AsyncMock(return_value=None))
    parity = AsyncMock()
    if parity_error:
        parity.side_effect = ValueError("native_bindings_differ")
    monkeypatch.setattr(offices, "_binding_parity", parity)

    async def summary(*_arguments, context):
        assert context.revision == 4 and context.inventory_root is None
        await context.page_sink(("native_binding",))
        context.derivation_sink(derivations[0])

    derive = AsyncMock(side_effect=summary)
    monkeypatch.setattr(offices, "_office_summary", derive)
    emitted_documents = []
    with pytest.raises(ValueError, match="native_bindings_differ") if parity_error else nullcontext():
        await offices._verify_inventory_source_offices(
            connection,
            copy_target,
            candidate_by_field,
            recipes,
            _APPROVED,
            control_schema="controls",
            derivation_sink=emitted_documents.append,
        )
    parity.assert_awaited_once()
    assert copy.await_count == (1 if empty else 2)
    assert derive.await_count == int(not empty)
    assert retained.await_count == int(empty)
    assert emitted_documents == derivations
    assert connection.execute.await_args.args[0].startswith("DROP TABLE IF EXISTS")


@pytest.mark.asyncio
async def test_inventory_owner_verification_stays_bounded(monkeypatch):
    root, entries, stage, derivations = _inventory(tuple(_example(index) for index in range(101)))
    copy_target = _target()
    candidate_by_field = _candidate(copy_target, root, stage, derivations)
    connection = _row_connection(stage)
    verify = AsyncMock(return_value={})
    monkeypatch.setattr(artifacts, "verify_anchored_source_fact_contents", verify)
    by_sha = {descriptor["recipe_sha256"]: descriptor for descriptor in stage.contribution_receipts}

    async def content(_connection, _relation, identity):
        return by_sha[identity]["fact_sha256"], by_sha[identity]["fact_rows"]

    monkeypatch.setattr(store, "contribution_fact_sha256", AsyncMock(side_effect=content))
    await store.verify_registry_source_contributions_rows(
        connection, copy_target, candidate_by_field, inventory_entries=entries
    )
    assert [len(call.args[1]) for call in verify.await_args_list] == [100, 1]
    assert store.contribution_fact_sha256.await_count == 101


@pytest.mark.asyncio
@pytest.mark.parametrize("recipes", [(), (_ptg_recipe(),)])
async def test_custody_requires_entries_before_source(monkeypatch, recipes):
    root, _entries, stage, derivations = _inventory(recipes)
    copy_target = _target()
    candidate_by_field = _candidate(copy_target, root, stage, derivations)
    pin = AsyncMock()
    monkeypatch.setattr(custody, "pin_retained_approved_membership_source", pin)
    with pytest.raises(store.RegistrySourceContributionError, match="inventory_invalid"):
        await custody.verify_independent_source_contributions(
            object(), copy_target, candidate_by_field, control_schema="controls"
        )
    pin.assert_not_awaited()


@pytest.mark.asyncio
async def test_new_custody_requires_registered_retention(monkeypatch):
    root, entries, stage, derivations = _inventory(())
    copy_target = _target()
    candidate_by_field = _candidate(copy_target, root, stage, derivations)
    for field in ("source_generations", "validation_json"):
        del candidate_by_field[field]["registry_source_retention"]
    pin = AsyncMock()
    monkeypatch.setattr(custody, "pin_retained_approved_membership_source", pin)
    with pytest.raises(store.RegistrySourceContributionError, match="retention_required"):
        await custody.verify_independent_source_contributions(
            object(), copy_target, candidate_by_field, control_schema="controls", inventory_entries=entries
        )
    pin.assert_not_awaited()


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
@pytest.mark.parametrize("damage", ["missing", "state", "revision", "generation", "scope"])
async def test_office_custody_refuses_changed_ready_candidate_metadata(damage):
    root, _, stage, derivations = _inventory(())
    target = _target()
    candidate = _candidate(target, root, stage, derivations)
    actual_by_field = {**candidate, "source_generations": dict(candidate["source_generations"])}
    if damage == "missing":
        actual_by_field = None
    else:
        changes_by_name = {
            "state": ("state", "open"),
            "revision": ("approved_custom_revision", 999),
            "generation": ("source_generations", {}),
            "scope": ("schema_name", "unbound_candidate"),
        }
        field, value = changes_by_name[damage]
        actual_by_field[field] = value
    connection = SimpleNamespace(fetchrow=AsyncMock(return_value=actual_by_field))
    with pytest.raises(offices.RegistryInitialSourceError, match="candidate_differs"):
        await offices._native_candidate(connection, target, candidate, "controls")
    connection.fetchrow.assert_awaited_once()


@pytest.mark.asyncio
async def test_retained_office_rows_require_a_resolved_origin_before_copy(monkeypatch):
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=True), execute=AsyncMock())
    monkeypatch.setattr(offices, "resolve_registry_retained_generation", AsyncMock(return_value=None))
    with pytest.raises(offices.RegistryInitialSourceError, match="origin_unavailable"):
        await offices._retained_bindings(connection, _target(), {}, "pg_temp.offices", "controls")
    connection.execute.assert_not_awaited()


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
