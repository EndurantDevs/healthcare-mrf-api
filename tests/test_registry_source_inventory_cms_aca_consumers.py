# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure CMS/ACA consumer wiring; native Source and publication proof is separate."""

import hashlib
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
from process import registry_aca_source_consumer as aca
from process import registry_cms_source_consumer as cms
from process import registry_source_inventory_store as inventory_store
from process.network_address_projection import PinnedAddressSource
from process.network_approved_membership_source import ApprovedMembershipSource
from process.network_legacy_membership_source import with_aca_global_epoch, with_aca_successor
from process.registry_aca_global_epoch import PinnedACAGlobalEpoch
from process.registry_candidate_composition import RegistryCompositionAddressSources, _target
from process.registry_source_inventory_contract import make_source_inventory_entry, source_inventory_root
from process.registry_source_inventory_preparation import SourceInventoryPreparation
from process.registry_source_inventory_reference import INVENTORY_DIGEST_KEY, SourceInventoryReference
from process.registry_source_recipe_store import RECIPE_DIGEST_KEY, canonical_registry_source_recipes
from tests.test_registry_aca_global_epoch import _ROLES, _global_prepared
from tests.test_registry_aca_source_consumer import recipe as admitted_aca_recipe
from tests.test_registry_cms_source_consumer import _notice as cms_notice
from tests.test_registry_cms_source_consumer import _recipe as cms_recipe
from tests.test_registry_source_inventory_preparation import _aca_recipe

CONTROL = "synthetic_control"
APPROVED = ApprovedMembershipSource(3, "b" * 64, 0)
ADDRESS = PinnedAddressSource("synthetic_addresses", "entity_address_unified", "c" * 64)
SOURCE_SHA = "a" * 64


@pytest.fixture(autouse=True)
def compiled_declarations(monkeypatch):
    # Reuse immutable declarations only; all manifest, Source and root validation runs.
    monkeypatch.setattr(archive, "reference_family_spec", cache(archive.reference_family_spec))


def _recipes(count, notified=None):
    return (
        *(cms_recipe(f"other-{index}", f"edition-{index}", producer=f"producer-{index}") for index in range(count - 1)),
        notified or cms_notice().pair.recipe,
    )


@pytest.fixture(scope="module")
def full_recipes():
    return _recipes(1000)


@pytest.fixture
def byte_wide_recipes():
    recipe = _aca_recipe()
    coordinates = replace(recipe.binding_coordinates, producer_id="other-producer")
    metadata = {
        **recipe.source_pin.prepared.manifest.source_metadata,
        "network_bindings": {**asdict(coordinates), "source_key_kind": "hios_plan_id"},
    }
    manifest = replace(
        recipe.source_pin.prepared.manifest,
        source_metadata=metadata,
        source_metadata_sha256=archive._source_metadata(metadata)[1],
    )
    validation_by_field = recipe.source_pin.validation.as_dict()
    validation_by_field.pop("validation_sha256")
    validation_by_field["manifest_sha256"] = hashlib.sha256(archive._canonical_json(manifest.as_dict())).hexdigest()
    validation = archive.validate_reference_family_validation_receipt(
        {**validation_by_field, "validation_sha256": archive._validation_digest(validation_by_field)}
    )
    recipe = replace(
        recipe,
        source_pin=replace(
            recipe.source_pin, prepared=replace(recipe.source_pin.prepared, manifest=manifest), validation=validation
        ),
        binding_coordinates=coordinates,
    )
    ownership = replace(
        recipe.source_pin.prepared.ownership,
        sequence_oids=tuple((f"sequence_{index:03d}", 10000 + index, "t" * 63, "c" * 63) for index in range(256)),
    )
    return tuple(
        replace(
            recipe,
            source_pin=replace(
                recipe.source_pin,
                prepared=replace(recipe.source_pin.prepared, ownership=replace(ownership, auxiliary_oid=20000 + index)),
            ),
        )
        for index in range(12)
    )


@pytest.mark.parametrize("family", ["cms", "aca"])
@pytest.mark.parametrize("count", [1, 100, 101, 1000])
async def test_commitments_preserve_all_fitting_legacy_bytes_and_bind_wide_sets(full_recipes, family, count):
    module = cms if family == "cms" else aca
    selected_recipes = full_recipes if count == 1000 else _recipes(count)
    if count <= 100:
        expected = canonical_registry_source_recipes(selected_recipes)
        assert module._recipe_attempt_commitment(selected_recipes, inventory=True) == expected
        assert module._recipe_attempt_commitment(selected_recipes) == expected
    else:
        root = SourceInventoryPreparation(selected_recipes).recipe_root
        assert module._recipe_attempt_commitment(selected_recipes, inventory=True) == (
            f"registry-{family}-source-inventory/v1",
            count,
            root.recipes_sha256,
        )
        with pytest.raises(ValueError):
            module._recipe_attempt_commitment(selected_recipes)


@pytest.mark.parametrize("family", ["cms", "aca"])
@pytest.mark.parametrize("damage", ["count", "duplicate", "malformed"])
def test_attempt_commitment_never_falls_back_after_validation_failure(family, damage):
    module = cms if family == "cms" else aca
    selected_recipes = (
        _recipes(1001)
        if damage == "count"
        else (cms_notice().pair.recipe,) * 2
        if damage == "duplicate"
        else (object(),)
    )
    with pytest.raises(ValueError):
        module._recipe_attempt_commitment(selected_recipes, inventory=True)


def _candidate(module, selected_recipes, event_by_name, *, base=SOURCE_SHA):
    preparation = SourceInventoryPreparation(selected_recipes)
    selected_recipes = preparation.recipes
    entries = tuple(make_source_inventory_entry(index, recipe) for index, recipe in enumerate(selected_recipes, 1))
    reference = SourceInventoryReference(source_inventory_root(entries))
    attempt = uuid5(
        UUID(event_by_name["event_id"]),
        module._json(
            (base, APPROVED.generation_id, module._recipe_attempt_commitment(selected_recipes, root=reference.root))
        ),
    )
    generations_by_source = {
        "custom_membership": APPROVED.generation_id,
        "unified_address": ADDRESS.generation_id,
        INVENTORY_DIGEST_KEY: reference.root.entries_sha256,
    }
    if base is not None:
        generations_by_source["retained_network_source"] = base
    copy_target = _target(attempt, APPROVED.approved_revision, 0 if base is None else 7, generations_by_source)
    candidate_by_name = {
        **asdict(copy_target),
        "state": "published",
        "approved_custom_revision": APPROVED.approved_revision,
        "expected_head": 0 if base is None else 7,
        "source_generations": {**generations_by_source, RECIPE_DIGEST_KEY: reference.root.recipes_sha256},
        "source_recipes_json": reference.as_dict(),
        "validation_json": {
            module._CHECKPOINT: {**event_by_name, "attempt_id": str(attempt)},
            "address_source": asdict(ADDRESS),
        },
    }
    return SimpleNamespace(
        candidate=candidate_by_name,
        entries=entries,
        reference=reference,
        target=copy_target,
        event_by_name=event_by_name,
    )


def _cms_case(count, *, initial=False):
    notice = cms_notice()
    coordinates = notice.pair.recipe.binding_coordinates
    cfg = cms.RegistryCMSSourceConsumerConfig(
        CONTROL,
        "executor",
        "physical_owner",
        coordinates,
        None if initial else replace(coordinates, dataset_id="previous", edition_id="previous-edition"),
    )
    event_by_name = cms._event(notice, cfg)
    fixture = _candidate(cms, _recipes(count, notice.pair.recipe), event_by_name, base=None if initial else SOURCE_SHA)
    fixture.config, fixture.notice = cfg, notice
    return fixture


def _aca_case(count):
    original = _aca_recipe()
    successor = replace(
        original.source_pin,
        prepared=replace(
            original.source_pin.prepared, ownership=replace(original.source_pin.prepared.ownership, auxiliary_oid=999)
        ),
    )
    accepted = replace(original, source_pin=successor)
    retained = replace(original, source_pin=with_aca_successor(original.source_pin, successor))
    cfg = aca.RegistryACASourceConsumerConfig(
        CONTROL,
        "executor",
        "physical_owner",
        original.source_pin.source_id,
        original.binding_coordinates.producer_id,
        original.source_pin.issuer_id,
        original.source_pin.year,
        source_authority_roles=("source_owner", "source_publisher"),
    )
    event_by_name = {"event_id": str(UUID(int=9)), "header": {"synthetic": True}, "reader_identity_sha256": "f" * 64}
    fixture = _candidate(aca, _recipes(count, retained), event_by_name)
    fixture.config, fixture.accepted, fixture.original = cfg, accepted, original
    return fixture


class Connection:
    def __init__(self, candidate):
        self.candidate = candidate
        self.snapshot_candidate = None
        self.active = False
        self.readonly = None
        self.fenced = False
        self.reads = 0

    def is_in_transaction(self):
        return self.active

    async def fetch(self, query, *arguments):
        assert "state='published'" in query and "LIMIT 2" in query
        self.reads += 1
        return [self.candidate if self.snapshot_candidate is None or not self.active else self.snapshot_candidate]

    async def fetchval(self, *arguments):
        raise AssertionError("Replay consulted the current head or selected a raw OID")

    @asynccontextmanager
    async def transaction(self, *, isolation, readonly=False):
        assert not self.active and isolation == "repeatable_read"
        self.active, self.readonly = True, readonly
        try:
            yield
        finally:
            self.active, self.readonly = False, None


def _spies(monkeypatch, fixture, module):
    connection = Connection(fixture.candidate)
    events = []

    async def seal(actual, copy_target, *, control_schema, required):
        assert actual is connection and actual.active and copy_target == fixture.target
        assert required is True and control_schema == CONTROL
        events.append("custody")
        return {
            "receipt": {
                "runtime_roles": ["source_owner", "source_publisher"],
                "closure": {"relation_oids": {INVENTORY_TABLE: 88}},
            }
        }

    async def load(actual, copy_target, candidate, *, expected_table_oid, control_schema):
        assert actual is connection and actual.active and copy_target == fixture.target
        assert events[-1] == "custody" and expected_table_oid == 88 and control_schema == CONTROL
        assert actual.readonly is (module is cms)
        events.append("inventory")
        return fixture.reference, fixture.entries

    @asynccontextmanager
    async def fence(*arguments):
        connection.fenced = True
        events.append("fence")
        try:
            yield
        finally:
            events.append("release")
            connection.fenced = False

    async def publish(actual, copy_target, *arguments, **options):
        assert actual is connection and copy_target == fixture.target
        assert (not actual.active) if module is cms else actual.active and actual.fenced
        events.append("publish")
        return {"synthetic": "published"}

    sealed, loader, publisher = AsyncMock(side_effect=seal), AsyncMock(side_effect=load), AsyncMock(side_effect=publish)
    monkeypatch.setattr(custody, "verify_network_candidate_custody", sealed)
    monkeypatch.setattr(aca, "verify_network_candidate_custody", sealed)
    monkeypatch.setattr(inventory_store, "_load_locked_source_inventory", loader)
    monkeypatch.setattr(cms, "prepare_and_publish_network_candidate", publisher)
    monkeypatch.setattr(aca, "publish_network_candidate", publisher)
    monkeypatch.setattr(aca, "current_aca_source_fence", fence)
    monkeypatch.setattr(aca, "pin_approved_membership_source", AsyncMock(return_value=APPROVED))
    selected, archived = AsyncMock(return_value=True), AsyncMock()
    monkeypatch.setattr(aca, "_selected_anchor", selected)
    monkeypatch.setattr(aca, "_require_archive", archived)
    return connection, sealed, loader, publisher, selected, archived, events


@pytest.mark.parametrize("family", ["cms", "aca"])
@pytest.mark.parametrize("count", [1, 101, 1000])
async def test_actual_newref_replay_loads_sealed_payload_then_keeps_family_authority(monkeypatch, family, count):
    module = cms if family == "cms" else aca
    fixture = _cms_case(count) if family == "cms" else _aca_case(count)
    connection, seal, loader, publish, selected, archived, events = _spies(monkeypatch, fixture, module)
    result = (
        await cms._replay(connection, fixture.event_by_name, fixture.config)
        if module is cms
        else await aca._replay(connection, fixture.event_by_name, fixture.accepted, fixture.config)
    )
    assert result["state"] == "replayed" and events[:2] == ["custody", "inventory"]
    seal.assert_awaited_once()
    loader.assert_awaited_once()
    publish.assert_awaited_once()
    if module is aca:
        assert events[-3:] == ["fence", "publish", "release"]
        assert selected.await_args.args[2] == fixture.original.binding_coordinates
        assert archived.await_args.args[1].prepared == fixture.original.source_pin.prepared


@pytest.mark.parametrize("family", ["cms", "aca"])
async def test_small_count_byte_wide_identity_and_actual_replay(monkeypatch, byte_wide_recipes, family):
    module = cms if family == "cms" else aca
    fixture = _cms_case(1) if module is cms else _aca_case(1)
    notified = (
        fixture.notice.pair.recipe
        if module is cms
        else replace(
            fixture.original, source_pin=with_aca_successor(fixture.original.source_pin, fixture.accepted.source_pin)
        )
    )
    selected_recipes = (*byte_wide_recipes, notified)
    fixture.__dict__.update(_candidate(module, selected_recipes, fixture.event_by_name).__dict__)
    root = fixture.reference.root
    assert root.entry_count == 13 and root.recipe_bytes > 524288
    assert module._recipe_attempt_commitment(selected_recipes, inventory=True) == (
        f"registry-{family}-source-inventory/v1",
        root.entry_count,
        root.recipes_sha256,
    )
    with pytest.raises(ValueError):
        module._recipe_attempt_commitment(selected_recipes)
    connection, _, _, publish, *_ = _spies(monkeypatch, fixture, module)
    if module is cms:
        assert (await cms._replay(connection, fixture.event_by_name, fixture.config))["state"] == "replayed"
    else:
        assert await aca._replay(connection, fixture.event_by_name, fixture.accepted, fixture.config) == {
            "state": "replayed"
        }
    publish.assert_awaited_once()


@pytest.mark.parametrize("damage", [None, "count", "base", "head"])
async def test_cms_initial_replay_retains_single_recipe_and_no_prior_base_contract(monkeypatch, damage):
    fixture = _cms_case(2 if damage == "count" else 1, initial=True)
    if damage == "base":
        fixture.candidate["source_generations"]["retained_network_source"] = SOURCE_SHA
    if damage == "head":
        fixture.candidate["expected_head"] = 7
    connection, _, _, publish, *_ = _spies(monkeypatch, fixture, cms)
    if damage is None:
        assert (await cms._replay(connection, fixture.event_by_name, fixture.config))["state"] == "replayed"
    else:
        with pytest.raises(ValueError):
            await cms._replay(connection, fixture.event_by_name, fixture.config)
        publish.assert_not_awaited()


async def test_cms_periodic_newref_recovers_saved_event_before_any_head_lookup(monkeypatch):
    fixture = _cms_case(101)
    connection, seal, loader, _, *_ = _spies(monkeypatch, fixture, cms)
    cfg, records = await cms._periodic_config(
        connection, fixture.notice, fixture.config, fixture.notice.pair.recipe.source_pin.endpoint_id
    )
    assert cfg == fixture.config and records == [fixture.candidate]
    assert seal.await_count == loader.await_count == 1
    assert (await cms._replay(connection, fixture.event_by_name, cfg, records))["state"] == "replayed"
    assert seal.await_count == loader.await_count == 2


async def test_cms_sync_newref_helper_does_not_hydrate_or_claim_authentication():
    fixture = _cms_case(1)
    with pytest.raises(ValueError):
        cms._replay_target(fixture.candidate, fixture.event_by_name)


@pytest.mark.parametrize("family", ["cms", "aca"])
@pytest.mark.parametrize("damage", ["checkpoint", "attempt", "scope", "custody", "oid", "payload"])
async def test_newref_replay_refuses_identity_and_payload_drift_before_publish(monkeypatch, family, damage):
    module = cms if family == "cms" else aca
    fixture = _cms_case(2) if module is cms else _aca_case(2)
    if damage == "checkpoint":
        fixture.candidate["validation_json"][module._CHECKPOINT]["event_id"] = str(UUID(int=99))
    if damage == "attempt":
        fixture.candidate["validation_json"][module._CHECKPOINT]["attempt_id"] = str(UUID(int=98))
    if damage == "scope":
        fixture.candidate["expected_head"] = 8
    connection, seal, loader, publish, *_ = _spies(monkeypatch, fixture, module)
    if damage == "custody":
        seal.side_effect = ValueError("synthetic custody refusal")
    if damage == "oid":
        seal.side_effect = None
        seal.return_value = {
            "receipt": {"runtime_roles": ["source_owner", "source_publisher"], "closure": {"relation_oids": {}}}
        }
    if damage == "payload":
        fixture.entries = ()
    with pytest.raises((ValueError, KeyError)):
        if module is cms:
            await cms._replay(connection, fixture.event_by_name, fixture.config)
        else:
            await aca._replay(connection, fixture.event_by_name, fixture.accepted, fixture.config)
    publish.assert_not_awaited()


async def test_aca_source_authority_census_precedes_inventory_and_archive_reads(monkeypatch):
    fixture = _aca_case(1)
    connection, seal, loader, publish, _, archived, _ = _spies(monkeypatch, fixture, aca)
    seal.side_effect = None
    seal.return_value = {
        "receipt": {"runtime_roles": ["source_owner"], "closure": {"relation_oids": {INVENTORY_TABLE: 88}}}
    }
    with pytest.raises(ValueError):
        await aca._replay(connection, fixture.event_by_name, fixture.accepted, fixture.config)
    loader.assert_not_awaited()
    archived.assert_not_awaited()
    publish.assert_not_awaited()


async def test_aca_successor_must_match_notified_physical_source(monkeypatch):
    fixture = _aca_case(1)
    different = replace(
        fixture.accepted.source_pin,
        prepared=replace(
            fixture.accepted.source_pin.prepared,
            ownership=replace(fixture.accepted.source_pin.prepared.ownership, auxiliary_oid=998),
        ),
    )
    connection, _, _, publish, *_ = _spies(monkeypatch, fixture, aca)
    with pytest.raises(ValueError):
        await aca._replay(
            connection, fixture.event_by_name, replace(fixture.accepted, source_pin=different), fixture.config
        )
    publish.assert_not_awaited()


@pytest.mark.parametrize("changed", [False, True])
async def test_newref_global_epoch_keeps_all_anchor_and_archive_checks(monkeypatch, changed):
    original = await admitted_aca_recipe(monkeypatch)
    prepared, validation = await _global_prepared(monkeypatch)
    epoch = PinnedACAGlobalEpoch(prepared, validation, _ROLES)
    retained = replace(original, source_pin=with_aca_global_epoch(original.source_pin, epoch))
    event_by_name = {"event_id": str(UUID(int=9)), "header": {"synthetic": True}, "reader_identity_sha256": "f" * 64}
    fixture = _candidate(aca, _recipes(101, retained), event_by_name)
    fixture.config = aca.RegistryACASourceConsumerConfig(
        CONTROL,
        "executor",
        "physical_owner",
        global_epoch=True,
        source_authority_roles=("source_owner", "source_publisher"),
    )
    connection, _, _, publish, selected, archived, _ = _spies(monkeypatch, fixture, aca)
    accepted = (
        replace(epoch, prepared=replace(epoch.prepared, ownership=replace(epoch.prepared.ownership, auxiliary_oid=998)))
        if changed
        else epoch
    )
    if changed:
        with pytest.raises(ValueError):
            await aca._replay(connection, event_by_name, accepted, fixture.config)
        publish.assert_not_awaited()
    else:
        assert await aca._replay(connection, event_by_name, accepted, fixture.config) == {"state": "replayed"}
        selected.assert_awaited_once()
        archived.assert_awaited_once()


async def test_cms_actual_wide_attempt_uses_tagged_identity_and_original_checkpoint(monkeypatch):
    fixture = _cms_case(101)
    selected_recipes = _recipes(101, fixture.notice.pair.recipe)
    pin = SimpleNamespace(manifest_sha256=SOURCE_SHA, generation_id=7, compact_layout=object())
    connection = Connection(fixture.candidate)
    connection.fetchval = AsyncMock(return_value=APPROVED.approved_revision)
    connection.execute = AsyncMock(return_value="UPDATE 1")
    monkeypatch.setattr(cms, "pin_approved_membership_source", AsyncMock(return_value=APPROVED))
    monkeypatch.setattr(cms, "require_approved_network_source_bindings", AsyncMock(return_value=1))
    monkeypatch.setattr(cms, "_attempt_sources", AsyncMock(return_value=(pin, selected_recipes, fixture.event_by_name)))
    prepare = AsyncMock(return_value=fixture.target)
    monkeypatch.setattr(cms, "_prepare_source_candidate", prepare)
    monkeypatch.setattr(cms, "publish_network_candidate", AsyncMock(return_value={}))
    assert (await cms._attempt(connection, fixture.notice, fixture.event_by_name, fixture.config))[
        "state"
    ] == "completed"
    expected = UUID(fixture.candidate["validation_json"][cms._CHECKPOINT]["attempt_id"])
    assert prepare.await_args.args[3] == expected
    checkpoint = connection.execute.await_args.args[2]
    assert checkpoint == cms._json({**fixture.event_by_name, "attempt_id": str(expected)})


async def test_aca_actual_wide_attempt_preserves_source_fence_and_selection(monkeypatch):
    fixture = _aca_case(101)
    connection, _, _, publish, *_ = _spies(monkeypatch, fixture, aca)
    selected_recipes = _recipes(
        101,
        replace(
            fixture.original, source_pin=with_aca_successor(fixture.original.source_pin, fixture.accepted.source_pin)
        ),
    )
    pin = SimpleNamespace(manifest_sha256=SOURCE_SHA, generation_id=7)
    monkeypatch.setattr(aca, "_selection", AsyncMock(return_value=(3, APPROVED, pin, selected_recipes)))
    addresses = RegistryCompositionAddressSources(ADDRESS, compact=True)
    monkeypatch.setattr(aca, "source_worker_address_sources", AsyncMock(return_value=addresses))
    runtime = SimpleNamespace(compose=AsyncMock(return_value=(fixture.target, ADDRESS)))
    cfg = SimpleNamespace(**{**fixture.config.__dict__, "composition_runtime": runtime, "writer_roles": {}})
    checkpoint = fixture.candidate["validation_json"][aca._CHECKPOINT]
    monkeypatch.setattr(aca, "_checkpoint", AsyncMock(return_value=(checkpoint, {"revision": 1})))
    monkeypatch.setattr(aca, "prepare_registry_candidate_for_context", AsyncMock())
    monkeypatch.setattr(aca, "_notice", AsyncMock(return_value=fixture.accepted))
    verification = AsyncMock()
    monkeypatch.setattr(aca, "_verify_selected_recipes", verification)
    connection.execute = AsyncMock(return_value="UPDATE 1")
    assert await aca._attempt(connection, None, None, {}, fixture.event_by_name, fixture.accepted, cfg) == {
        "state": "completed"
    }
    assert runtime.compose.await_args.kwargs["request_id"] == UUID(checkpoint["attempt_id"])
    verification.assert_awaited_once()
    publish.assert_awaited_once()
