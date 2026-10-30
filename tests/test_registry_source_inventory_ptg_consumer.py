# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure PTG attempt/replay wiring; spies provide no native SOURCE or publication proof."""

import copy
import json
from contextlib import asynccontextmanager
from dataclasses import asdict, replace
from functools import cache
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid5

import pytest

from db.registry_source_inventory_schema import INVENTORY_TABLE
from process import network_candidate_custody as custody
from process import reference_family_archive as archive
from process import registry_ptg_source_consumer as consumer
from process import registry_source_inventory_store as inventory_store
from process.network_address_projection import PinnedAddressSource
from process.network_approved_membership_source import ApprovedMembershipSource
from process.registry_candidate_composition import RegistryCompositionAddressSources, _target
from process.registry_publication_ptg_runtime import RegistryPublicationPTGRuntime
from process.registry_source_inventory_contract import make_source_inventory_entry, source_inventory_root
from process.registry_source_inventory_preparation import SourceInventoryPreparation
from process.registry_source_inventory_reference import INVENTORY_DIGEST_KEY, SourceInventoryReference
from process.registry_source_recipe_store import RECIPE_DIGEST_KEY, canonical_registry_source_recipes
from tests.test_registry_ptg_office_membership_recipe import _recipe as ptg_recipe
from tests.test_registry_ptg_source_consumer import config as ptg_consumer_config
from tests.test_registry_source_inventory_preparation import _aca_recipe
from tests.test_registry_source_recipe_store_postgres import _cms_custody_recipe, _example

CONTROL = "synthetic_control"
APPROVED = ApprovedMembershipSource(3, "b" * 64, 0)
SOURCE_SHA = "a" * 64
ADDRESS = PinnedAddressSource("synthetic_addresses", "entity_address_unified", "c" * 64)


def _recipes(count):
    original = _cms_custody_recipe()
    selected_recipes = tuple(
        replace(
            original,
            source_pin=replace(original.source_pin, dataset_id=f"dataset-{index}"),
            binding_coordinates=replace(original.binding_coordinates, dataset_id=f"dataset-{index}"),
        )
        for index in range(count - 1)
    )
    return (*selected_recipes, ptg_recipe())


@pytest.fixture(scope="module")
def full_recipes():
    return _recipes(1000)


@pytest.fixture
def wide_recipes(monkeypatch):
    # Only immutable compiled model declarations are reused; all recipe validation runs.
    monkeypatch.setattr(archive, "reference_family_spec", cache(archive.reference_family_spec))
    recipe = _aca_recipe()
    ownership = replace(
        recipe.source_pin.prepared.ownership,
        sequence_oids=tuple((f"sequence_{index:03d}", 10000 + index, "t" * 63, "c" * 63) for index in range(256)),
    )
    selected_recipes = tuple(
        replace(
            recipe,
            source_pin=replace(
                recipe.source_pin,
                prepared=replace(recipe.source_pin.prepared, ownership=replace(ownership, auxiliary_oid=20000 + index)),
            ),
        )
        for index in range(12)
    )
    return (*selected_recipes, ptg_recipe())


def _event():
    selected = ptg_recipe()
    return {
        "event_id": str(UUID(int=9)),
        "header": {
            "capture_id": selected.source_pin.capture_id,
            "approval_sha256": selected.source_pin.approval_sha256,
            "hold_sha256": selected.source_pin.hold_sha256,
        },
        "reader_identity_sha256": "f" * 64,
    }


def _config():
    return replace(ptg_consumer_config(), control_schema=CONTROL)


def _compose_spies(monkeypatch, compact):
    copy_target = _target(UUID(int=1), APPROVED.approved_revision, 7, {})
    compose = AsyncMock(return_value=(copy_target, ADDRESS))
    address_sources = RegistryCompositionAddressSources(ADDRESS, compact=compact)
    source = AsyncMock(return_value=address_sources)
    monkeypatch.setattr(consumer, "source_worker_address_sources", source)
    monkeypatch.setattr(RegistryPublicationPTGRuntime, "compose", compose)
    return _config(), compose, source


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [1, 100])
@pytest.mark.parametrize("compact", [False, True])
async def test_all_previously_supported_attempt_uuid_bytes_are_unchanged(monkeypatch, count, compact):
    selected_recipes = _recipes(count)
    cfg, compose, _ = _compose_spies(monkeypatch, compact)
    pin = SimpleNamespace(manifest_sha256=SOURCE_SHA, generation_id=7)
    event = _event()
    expected = uuid5(
        UUID(event["event_id"]),
        consumer._json((SOURCE_SHA, APPROVED.generation_id, canonical_registry_source_recipes(selected_recipes))),
    )
    attempt, _, _, inventory = await consumer._compose_source_attempt(
        AsyncMock(), (3, APPROVED, pin, selected_recipes), event, cfg
    )
    assert attempt == expected and inventory is compact
    assert compose.await_args.kwargs["request_id"] == expected
    assert compose.await_args.kwargs["address_sources"].compact is compact


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [101, 1000])
async def test_compact_wide_attempts_bind_complete_bounded_recipe_commitment(monkeypatch, full_recipes, count):
    selected_recipes = full_recipes if count == 1000 else _recipes(count)
    cfg, compose, source = _compose_spies(monkeypatch, True)
    pin = SimpleNamespace(manifest_sha256=SOURCE_SHA, generation_id=7)
    preparation = SourceInventoryPreparation(selected_recipes)
    commitment = ("registry-ptg-source-inventory/v1", count, preparation.recipe_root.recipes_sha256)
    event = _event()
    expected = uuid5(UUID(event["event_id"]), consumer._json((SOURCE_SHA, APPROVED.generation_id, commitment)))
    attempt, _, _, inventory = await consumer._compose_source_attempt(
        AsyncMock(), (3, APPROVED, pin, selected_recipes), event, cfg
    )
    assert attempt == expected and inventory is True
    assert compose.await_args.kwargs["address_sources"].replacement_source_recipes == preparation.recipes
    source.assert_awaited_once()


@pytest.mark.asyncio
async def test_small_count_above_legacy_byte_bound_uses_tagged_identity(monkeypatch, wide_recipes):
    preparation = SourceInventoryPreparation(wide_recipes)
    root = preparation.recipe_root
    assert root.entry_count < 100 and root.recipe_bytes > 524288
    cfg, compose, _ = _compose_spies(monkeypatch, True)
    pin = SimpleNamespace(manifest_sha256=SOURCE_SHA, generation_id=7)
    event = _event()
    expected = uuid5(
        UUID(event["event_id"]),
        consumer._json(
            (
                SOURCE_SHA,
                APPROVED.generation_id,
                ("registry-ptg-source-inventory/v1", root.entry_count, root.recipes_sha256),
            )
        ),
    )
    actual, *_ = await consumer._compose_source_attempt(AsyncMock(), (3, APPROVED, pin, wide_recipes), event, cfg)
    assert actual == expected and compose.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["count", "malformed", "duplicate", "legacy"])
async def test_unsupported_or_malformed_attempt_never_composes(monkeypatch, damage):
    selected_recipes = _recipes(1001 if damage == "count" else 101)
    if damage == "malformed":
        selected_recipes = (object(),)
    if damage == "duplicate":
        selected_recipes = (ptg_recipe(), ptg_recipe())
    cfg, compose, _ = _compose_spies(monkeypatch, damage != "legacy")
    with pytest.raises(ValueError):
        await consumer._compose_source_attempt(
            AsyncMock(),
            (3, APPROVED, SimpleNamespace(manifest_sha256=SOURCE_SHA, generation_id=7), selected_recipes),
            _event(),
            cfg,
        )
    compose.assert_not_awaited()


def _replay_fixture(selected_recipes, *, legacy=False):
    preparation = SourceInventoryPreparation(selected_recipes)
    selected_recipes = preparation.recipes
    entries = tuple(make_source_inventory_entry(index, recipe) for index, recipe in enumerate(selected_recipes, 1))
    reference = SourceInventoryReference(source_inventory_root(entries))
    root = reference.root
    commitment = (
        canonical_registry_source_recipes(selected_recipes)
        if root.entry_count <= 100 and root.recipe_bytes <= 524288
        else ("registry-ptg-source-inventory/v1", root.entry_count, root.recipes_sha256)
    )
    event = _event()
    attempt = uuid5(UUID(event["event_id"]), consumer._json((SOURCE_SHA, APPROVED.generation_id, commitment)))
    generations_by_source = {
        "retained_network_source": SOURCE_SHA,
        "custom_membership": APPROVED.generation_id,
        "unified_address": ADDRESS.generation_id,
    }
    if not legacy:
        generations_by_source[INVENTORY_DIGEST_KEY] = root.entries_sha256
    copy_target = _target(attempt, APPROVED.approved_revision, 7, generations_by_source)
    candidate_by_name = {
        **asdict(copy_target),
        "state": "published",
        "approved_custom_revision": APPROVED.approved_revision,
        "expected_head": 7,
        "source_generations": {**generations_by_source, RECIPE_DIGEST_KEY: root.recipes_sha256},
        "source_recipes_json": json.loads(canonical_registry_source_recipes(selected_recipes))
        if legacy
        else reference.as_dict(),
        "validation_json": {
            consumer._CHECKPOINT: {**event, "attempt_id": str(attempt)},
            "address_source": asdict(ADDRESS),
        },
    }
    return SimpleNamespace(
        candidate=candidate_by_name,
        entries=entries,
        reference=reference,
        selected=selected_recipes,
        target=copy_target,
        event=event,
    )


class ReplayConnection:
    def __init__(self, candidate):
        self.candidate = candidate
        self.active = False
        self.readonly = None
        self.reads = 0
        self.snapshot_candidate = None

    def is_in_transaction(self):
        return self.active

    async def fetch(self, query, *arguments):
        assert "state='published'" in query and "LIMIT 2" in query
        self.reads += 1
        return [self.candidate if self.reads == 1 or self.snapshot_candidate is None else self.snapshot_candidate]

    @asynccontextmanager
    async def transaction(self, *, isolation, readonly):
        assert not self.active and isolation == "repeatable_read" and readonly is True
        self.active, self.readonly = True, readonly
        try:
            yield
        finally:
            self.active, self.readonly = False, None


def _replay_spies(monkeypatch, fixture):
    connection = ReplayConnection(fixture.candidate)
    events = []

    async def seal(actual, copy_target, *, control_schema, required):
        assert actual is connection and actual.active and actual.readonly and actual.reads == 2
        assert copy_target == fixture.target and required is True and control_schema == CONTROL
        events.append("custody")
        return {"receipt": {"closure": {"relation_oids": {INVENTORY_TABLE: 88}}}}

    async def inventory(actual, copy_target, candidate, *, expected_table_oid, control_schema):
        assert actual is connection and actual.active and actual.readonly and events == ["custody"]
        assert copy_target == fixture.target and expected_table_oid == 88 and control_schema == CONTROL
        events.append("inventory")
        return fixture.reference, fixture.entries

    async def publish(actual, copy_target, address, **options):
        assert actual is connection and not actual.active and copy_target == fixture.target and address == ADDRESS
        events.append("pipeline")

    seal_spy, loader, publisher = (
        AsyncMock(side_effect=seal),
        AsyncMock(side_effect=inventory),
        AsyncMock(side_effect=publish),
    )
    monkeypatch.setattr(custody, "verify_network_candidate_custody", seal_spy)
    monkeypatch.setattr(inventory_store, "_load_locked_source_inventory", loader)
    monkeypatch.setattr(consumer, "prepare_and_publish_network_candidate", publisher)
    return connection, seal_spy, loader, publisher, events


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [1, 101, 1000])
async def test_new_reference_replay_holds_required_custody_for_membership_and_identity(
    monkeypatch, full_recipes, count
):
    fixture = _replay_fixture(full_recipes if count == 1000 else _recipes(count))
    connection, seal, loader, publish, events = _replay_spies(monkeypatch, fixture)
    assert await consumer._replay(connection, fixture.event, ptg_recipe(), _config()) == {"state": "replayed"}
    assert events == ["custody", "inventory", "pipeline"] and connection.reads == 2
    seal.assert_awaited_once()
    loader.assert_awaited_once()
    publish.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage", ["checkpoint", "attempt", "membership", "count", "target", "seal", "oid", "snapshot", "format"]
)
async def test_new_reference_replay_refuses_drift_before_publication(monkeypatch, damage):
    fixture = _replay_fixture(_recipes(2))
    connection, seal, loader, publish, _ = _replay_spies(monkeypatch, fixture)
    fresh_by_name = copy.deepcopy(fixture.candidate)
    connection.snapshot_candidate = fresh_by_name
    if damage == "checkpoint":
        fresh_by_name["validation_json"][consumer._CHECKPOINT]["header"]["hold_sha256"] = "f" * 64
    if damage == "attempt":
        fresh_by_name["validation_json"][consumer._CHECKPOINT]["attempt_id"] = str(UUID(int=77))
    if damage == "membership":
        fixture.entries = tuple(
            make_source_inventory_entry(entry.ordinal, _example(999))
            if json.loads(entry.recipe_json)["source_kind"] == "ptg"
            else entry
            for entry in fixture.entries
        )
    if damage == "count":
        fixture.entries = ()
    if damage == "target":
        fresh_by_name["expected_head"] = 8
    if damage == "seal":
        seal.side_effect = ValueError("synthetic custody refusal")
    if damage == "oid":
        seal.side_effect = None
        seal.return_value = {"receipt": {"closure": {"relation_oids": {}}}}
    if damage == "snapshot":
        fresh_by_name["candidate_id"] = str(UUID(int=78))
    if damage == "format":
        fresh_by_name["source_recipes_json"] = json.loads(canonical_registry_source_recipes(fixture.selected))
        fresh_by_name["source_generations"].pop(INVENTORY_DIGEST_KEY)
    with pytest.raises((ValueError, KeyError)):
        await consumer._replay(connection, fixture.event, ptg_recipe(), _config())
    publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_legacy_replay_keeps_existing_pipeline_without_inventory_lookup(monkeypatch):
    fixture = _replay_fixture(_recipes(2), legacy=True)
    connection, seal, loader, publish, events = _replay_spies(monkeypatch, fixture)
    assert await consumer._replay(connection, fixture.event, ptg_recipe(), _config()) == {"state": "replayed"}
    assert events == ["pipeline"] and connection.reads == 1
    seal.assert_not_awaited()
    loader.assert_not_awaited()
    publish.assert_awaited_once()


@pytest.mark.asyncio
async def test_compact_attempt_preserves_notice_and_final_approval_guards(monkeypatch, full_recipes):
    cfg, compose, _ = _compose_spies(monkeypatch, True)
    pin = SimpleNamespace(manifest_sha256=SOURCE_SHA, generation_id=7)
    monkeypatch.setattr(consumer, "_replacement_selection", AsyncMock(return_value=(3, APPROVED, pin, full_recipes)))
    monkeypatch.setattr(consumer, "prepare_registry_candidate_for_context", AsyncMock())
    monkeypatch.setattr(consumer, "_notice", AsyncMock(return_value=ptg_recipe()))
    monkeypatch.setattr(consumer, "pin_approved_membership_source", AsyncMock(return_value=APPROVED))
    guard, publish = AsyncMock(), AsyncMock()
    monkeypatch.setattr(consumer, "require_selected_ptg_office_recipes", guard)
    monkeypatch.setattr(consumer, "publish_network_candidate", publish)
    connection = ReplayConnection({})

    @asynccontextmanager
    async def transaction(*, isolation):
        assert isolation == "repeatable_read"
        yield

    connection.transaction = transaction
    connection.execute = AsyncMock(return_value="UPDATE 1")
    assert await consumer._attempt(connection, None, object(), "synthetic_reader", {}, _event(), ptg_recipe(), cfg) == {
        "state": "completed"
    }
    assert guard.await_args.kwargs["inventory"] is True
    assert guard.await_args.kwargs["candidate_recipes"] == full_recipes
    assert publish.await_count == 1 and compose.await_count == 1
    checkpoint = json.loads(connection.execute.await_args.args[2])
    assert set(checkpoint) == {*_event(), "attempt_id"}


@pytest.mark.asyncio
async def test_compact_selection_keeps_source_admission_check_in_inventory_mode(monkeypatch):
    connection = ReplayConnection({})
    connection.fetchval = AsyncMock(side_effect=[3, 7])
    old = replace(ptg_recipe(), source_pin=replace(ptg_recipe().source_pin, capture_id=str(UUID(int=2))))
    pin = SimpleNamespace(compact_layout=object())
    monkeypatch.setattr(consumer, "pin_approved_membership_source", AsyncMock(return_value=APPROVED))
    monkeypatch.setattr(consumer, "_selected", AsyncMock(return_value="synthetic_binding"))
    monkeypatch.setattr(consumer, "resolve_network_serving_manifest", AsyncMock(return_value=pin))
    monkeypatch.setattr(consumer, "resolve_retained_registry_source_recipes", AsyncMock(return_value=(old,)))
    monkeypatch.setattr(consumer, "_replacement", AsyncMock(return_value=(ptg_recipe(),)))
    guard = AsyncMock()
    monkeypatch.setattr(consumer, "require_selected_ptg_office_recipes", guard)
    assert await consumer._replacement_selection(connection, object(), ptg_recipe(), _config()) == (
        3,
        APPROVED,
        pin,
        (ptg_recipe(),),
    )
    assert guard.await_args.kwargs["inventory"] is True and guard.await_args.args[1] == (ptg_recipe(),)
