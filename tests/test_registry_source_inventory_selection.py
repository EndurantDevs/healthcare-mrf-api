# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Selection metadata and fake-connection flow checks; native atomicity is separate."""

import copy
import hashlib
import json
from contextlib import asynccontextmanager
from dataclasses import asdict, replace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from process import registry_source_recipe_store as recipes
from process import registry_source_selection_receipt as selection
from process.network_approved_membership_source import ApprovedMembershipSource
from process.network_membership_copy import MembershipCopyTarget
from process.registry_source_contribution_store import locally_owned_source_descriptor
from process.registry_source_inventory_contract import make_source_inventory_entry, source_inventory_root
from process.registry_source_inventory_reference import INVENTORY_DIGEST_KEY, SourceInventoryReference
from process.registry_source_recipe_composition import RegistrySourceRecipeStage
from process.registry_source_retention import SUMMARY_KEY as RETENTION_KEY
from process.registry_source_retention import registry_source_retention_sha256
from tests.test_registry_source_contributions import _descriptor
from tests.test_registry_source_inventory_preparation import _aca_recipe
from tests.test_registry_source_recipe_store_postgres import _example

APPROVED = ApprovedMembershipSource(7, "b" * 64, 1)


class OneShot:
    def __init__(self, values):
        self.values, self.was_consumed = values, False

    def __iter__(self):
        assert not self.was_consumed, "Actual recipes were consumed twice"
        self.was_consumed = True
        yield from self.values


def _inventory(count, *, metadata=False):
    selected = sorted(
        (_example(index) for index in range(count)),
        key=lambda recipe: recipes.canonical_registry_source_recipes((recipe,)),
    )
    root = source_inventory_root(
        make_source_inventory_entry(
            index, recipe, contribution=locally_owned_source_descriptor(_descriptor(recipe)) if metadata else None
        )
        for index, recipe in enumerate(selected, 1)
    )
    return root, tuple(selected)


def _stage(root, *, mapped=3, omitted=2):
    generations = tuple(f"{index + 1:064x}" for index in range(root.entry_count))
    stage_sha = hashlib.sha256(
        json.dumps(
            [root.recipes_sha256, APPROVED.generation_id, sorted(generations), mapped, omitted], separators=(",", ":")
        ).encode()
    ).hexdigest()
    return RegistrySourceRecipeStage("registry_source_page_" + uuid4().hex, mapped, omitted, stage_sha, generations)


def _target():
    identity = str(uuid4())
    return MembershipCopyTarget(
        *(str(uuid4()) for _ in range(3)), identity, "network_candidate_" + identity.replace("-", "")
    )


def _candidate(root, stage, target=None):
    return {
        **asdict(target or _target()),
        "state": "open",
        "approved_custom_revision": APPROVED.approved_revision,
        "expected_rows": stage.membership_rows + APPROVED.total_rows,
        "source_recipes_json": SourceInventoryReference(root).as_dict(),
        "source_generations": {
            recipes.RECIPE_DIGEST_KEY: root.recipes_sha256,
            INVENTORY_DIGEST_KEY: root.entries_sha256,
            "custom_membership": APPROVED.generation_id,
            "registry_source_membership": stage.generation_sha256,
        },
        "validation_json": {"existing": True},
    }


@pytest.fixture(scope="module")
def example_inventory():
    root, selected = _inventory(2)
    return root, selected, _stage(root)


@pytest.fixture(scope="module")
def wide_inventory():
    recipe = _aca_recipe()
    ownership = replace(
        recipe.source_pin.prepared.ownership,
        sequence_oids=tuple((f"sequence_{index:03d}", 10000 + index, "t" * 63, "c" * 63) for index in range(256)),
    )
    selected_sources = tuple(
        replace(
            recipe,
            source_pin=replace(
                recipe.source_pin,
                prepared=replace(recipe.source_pin.prepared, ownership=replace(ownership, auxiliary_oid=20000 + index)),
            ),
        )
        for index in range(12)
    )
    selected_sources = tuple(
        sorted(selected_sources, key=lambda item: recipes.canonical_registry_source_recipes((item,)))
    )
    root = source_inventory_root(
        make_source_inventory_entry(index, item) for index, item in enumerate(selected_sources, 1)
    )
    assert len(selected_sources) < 100 and root.recipe_bytes > recipes.MAX_SOURCE_RECIPE_BYTES
    return root, selected_sources, _stage(root)


@pytest.mark.parametrize("count", [1, 101, 1000])
def test_v2_actual_recipe_fold_matches_full_inventory_without_descriptors(count, monkeypatch):
    root, selected = _inventory(count, metadata=True)
    stage = _stage(root)
    receipt = selection.build_registry_source_selection_receipt(OneShot(selected), APPROVED, stage, inventory_root=root)
    assert receipt["revision"] == 2 and receipt["recipe_count"] == count
    assert (
        receipt["recipe_sha256"] == root.recipes_sha256 and receipt["inventory_entries_sha256"] == root.entries_sha256
    )
    assert len(selection._canonical(receipt).encode()) < 1024
    candidate = _candidate(root, stage)
    candidate["validation_json"][selection.SUMMARY_KEY] = receipt

    def forbidden_payload(*_arguments, **_options):
        pytest.fail("Selection metadata verification hydrated recipes")

    monkeypatch.setattr(recipes, "decode_registry_source_recipes", forbidden_payload)
    assert selection.verify_registry_source_selection_receipt(candidate) == receipt


@pytest.mark.parametrize("damage", ["order", "duplicate", "substitute", "missing"])
def test_builder_rejects_wrong_actual_stream_even_with_matching_root_metadata(damage, example_inventory):
    root, selected, stage = example_inventory
    changed_recipes = list(selected)
    if damage == "order":
        changed_recipes.reverse()
    elif damage == "duplicate":
        changed_recipes[1] = changed_recipes[0]
    elif damage == "substitute":
        changed_recipes[1] = replace(
            changed_recipes[1], source_pin=replace(changed_recipes[1].source_pin, dataset_sha256="d" * 64)
        )
    else:
        changed_recipes.pop()
    with pytest.raises(selection.RegistrySourceSelectionError):
        selection.build_registry_source_selection_receipt(
            OneShot(changed_recipes), APPROVED, stage, inventory_root=root
        )


@pytest.mark.parametrize(
    "damage", ["stage_hash", "stage_count", "generation_shape", "mapped", "omitted", "table", "approved"]
)
def test_v2_preserves_exact_stage_and_approved_pin_checks(damage, example_inventory):
    root, selected, stage = example_inventory
    mutations_by_case = {
        "stage_hash": {"generation_sha256": "f" * 64},
        "stage_count": {"source_generations": ()},
        "generation_shape": {"source_generations": tuple("A" * 64 for _ in selected)},
        "mapped": {"membership_rows": 4},
        "omitted": {"omitted_rows": 3},
        "table": {"table_name": "untrusted"},
    }
    approved = replace(APPROVED, generation_id="e" * 64) if damage == "approved" else APPROVED
    stage = replace(stage, **mutations_by_case.get(damage, {}))
    with pytest.raises(selection.RegistrySourceSelectionError):
        selection.build_registry_source_selection_receipt(OneShot(selected), approved, stage, inventory_root=root)


def test_version_separation_and_legacy_canonical_receipt_are_unchanged(example_inventory):
    root, selected, stage = example_inventory
    legacy = selection.build_registry_source_selection_receipt(tuple(reversed(selected)), APPROVED, stage)
    expected_by_field = {
        "component": selection.SUMMARY_KEY,
        "revision": 1,
        "selection_policy": "approved-bindings-only",
        "recipe_sha256": root.recipes_sha256,
        "recipe_count": len(selected),
        "approved_revision": 7,
        "approved_generation_sha256": APPROVED.generation_id,
        "stage_generation_sha256": stage.generation_sha256,
        "mapped_rows": 3,
        "omitted_rows": 2,
    }
    assert selection._canonical(legacy) == json.dumps(expected_by_field, sort_keys=True, separators=(",", ":"))
    current = selection.build_registry_source_selection_receipt(selected, APPROVED, stage, inventory_root=root)
    with pytest.raises(selection.RegistrySourceSelectionError):
        selection.validate_registry_source_selection_receipt(current)
    with pytest.raises(selection.RegistrySourceSelectionError):
        selection.validate_registry_source_selection_receipt(legacy, inventory_root=root)
    large_root, large = _inventory(101)
    with pytest.raises(selection.RegistrySourceSelectionError):
        selection.build_registry_source_selection_receipt(large, APPROVED, _stage(large_root))


@pytest.mark.parametrize(
    "field",
    [
        "inventory_entries_sha256",
        "recipe_count",
        "recipe_sha256",
        "approved_revision",
        "approved_generation_sha256",
        "stage_generation_sha256",
    ],
)
def test_verifier_refuses_registered_scalar_drift(field, example_inventory):
    root, selected, stage = example_inventory
    receipt = selection.build_registry_source_selection_receipt(selected, APPROVED, stage, inventory_root=root)
    receipt[field] = 1 if field in {"recipe_count", "approved_revision"} else "f" * 64
    candidate = _candidate(root, stage)
    candidate["validation_json"][selection.SUMMARY_KEY] = receipt
    with pytest.raises(selection.RegistrySourceSelectionError):
        selection.verify_registry_source_selection_receipt(candidate)


def test_retention_and_staged_receipt_pairing_remain_mandatory(example_inventory):
    root, selected, stage = example_inventory
    candidate = _candidate(root, stage)
    with pytest.raises(selection.RegistrySourceSelectionError):
        selection.verify_registry_source_selection_receipt(candidate)
    candidate["source_generations"].pop("registry_source_membership")
    assert selection.verify_registry_source_selection_receipt(candidate) is None
    candidate = _candidate(root, stage)
    candidate["validation_json"][selection.SUMMARY_KEY] = selection.build_registry_source_selection_receipt(
        selected, APPROVED, stage, inventory_root=root
    )
    retained_by_field = {
        "component": RETENTION_KEY,
        "revision": 1,
        "table_name": "network_membership_source",
        "source_generation_sha256": stage.generation_sha256,
        "source_rows": 2,
        "add_rows": 1,
        "remove_rows": 0,
        "suppressed_source_rows": 0,
        "effective_rows": 3,
    }
    candidate["expected_rows"] = 3
    candidate["validation_json"][RETENTION_KEY] = retained_by_field
    candidate["source_generations"][RETENTION_KEY] = registry_source_retention_sha256(retained_by_field)
    with pytest.raises(selection.RegistrySourceSelectionError):
        selection.verify_registry_source_selection_receipt(candidate)


class MetadataConnection:
    """Exercise record control flow only; this supplies no PostgreSQL atomicity or ACL proof."""

    def __init__(self, candidate, count):
        self.candidate, self.count, self.writes = copy.deepcopy(candidate), count, 0

    def is_in_transaction(self):
        return True

    @asynccontextmanager
    async def transaction(self):
        yield

    async def fetchrow(self, query, candidate_id):
        assert query.endswith("FOR UPDATE")
        return self.candidate

    async def fetchval(self, query):
        assert query.startswith('SELECT count(*) FROM pg_temp."registry_source_page_')
        return self.count

    async def execute(self, query, candidate_id, key, encoded):
        assert "jsonb_set" in query and str(candidate_id) == self.candidate["candidate_id"]
        self.candidate["validation_json"][key] = json.loads(encoded)
        self.writes += 1


@pytest.mark.asyncio
async def test_record_uses_locked_root_and_exact_replay_does_not_write(monkeypatch, example_inventory):
    root, selected, stage = example_inventory
    target = _target()
    connection = MetadataConnection(_candidate(root, stage, target), stage.membership_rows)
    monkeypatch.setattr(selection, "pin_approved_membership_source", AsyncMock(return_value=APPROVED))
    first = await selection.record_registry_source_selection(connection, target, OneShot(selected), APPROVED, stage)
    assert first["inventory_entries_sha256"] == root.entries_sha256 and connection.writes == 1
    assert connection.candidate["validation_json"]["existing"] is True
    before = copy.deepcopy(connection.candidate)
    assert (
        await selection.record_registry_source_selection(connection, target, OneShot(selected), APPROVED, stage)
        == first
    )
    assert connection.writes == 1 and connection.candidate == before


@pytest.mark.asyncio
@pytest.mark.parametrize("inventory_reference", [True, False])
async def test_small_wide_recipe_array_requires_locked_inventory_reference(
    monkeypatch, wide_inventory, inventory_reference
):
    root, selected, stage = wide_inventory
    target = _target()
    candidate = _candidate(root, stage, target)
    if not inventory_reference:
        candidate["source_recipes_json"] = [
            json.loads(recipes.canonical_registry_source_recipes((recipe,)))[0] for recipe in selected
        ]
        candidate["source_generations"].pop(INVENTORY_DIGEST_KEY)
    connection = MetadataConnection(candidate, stage.membership_rows)
    current_pin = AsyncMock(return_value=APPROVED)
    monkeypatch.setattr(selection, "pin_approved_membership_source", current_pin)
    if not inventory_reference:
        with pytest.raises(selection.RegistrySourceSelectionError):
            await selection.record_registry_source_selection(connection, target, selected, APPROVED, stage)
        assert connection.writes == 0
        return
    receipt = await selection.record_registry_source_selection(connection, target, selected, APPROVED, stage)
    assert receipt["revision"] == 2 and receipt["recipe_count"] == len(selected)
    assert receipt["recipe_sha256"] == root.recipes_sha256
    assert receipt["inventory_entries_sha256"] == root.entries_sha256
    assert connection.writes == 1
    before = copy.deepcopy(connection.candidate)
    assert await selection.record_registry_source_selection(connection, target, selected, APPROVED, stage) == receipt
    assert connection.writes == 1 and connection.candidate == before
    assert current_pin.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage",
    [
        "dataset_id",
        "schema_id",
        "producer_id",
        "candidate_id",
        "schema_name",
        "closed",
        "expected_rows",
        "approved",
        "temp_count",
        "root",
        "retained",
    ],
)
async def test_record_refuses_scope_stage_or_replay_drift_before_write(monkeypatch, damage, example_inventory):
    root, selected, stage = example_inventory
    target = _target()
    candidate = _candidate(root, stage, target)
    if damage in {"dataset_id", "schema_id", "producer_id", "candidate_id", "schema_name"}:
        candidate[damage] = "different" if damage == "schema_name" else str(uuid4())
    changes_by_case = {"closed": {"state": "ready"}, "expected_rows": {"expected_rows": candidate["expected_rows"] + 1}}
    candidate.update(changes_by_case.get(damage, {}))
    if damage == "root":
        candidate["source_recipes_json"]["root"]["recipes_sha256"] = "f" * 64
        candidate["source_generations"][recipes.RECIPE_DIGEST_KEY] = "f" * 64
    if damage == "retained":
        receipt = selection.build_registry_source_selection_receipt(selected, APPROVED, stage, inventory_root=root)
        candidate["validation_json"][selection.SUMMARY_KEY] = {**receipt, "omitted_rows": 1}
    connection = MetadataConnection(candidate, stage.membership_rows + int(damage == "temp_count"))
    approved = replace(APPROVED, generation_id="e" * 64) if damage == "approved" else APPROVED
    monkeypatch.setattr(selection, "pin_approved_membership_source", AsyncMock(return_value=approved))
    with pytest.raises(selection.RegistrySourceSelectionError):
        await selection.record_registry_source_selection(connection, target, OneShot(selected), APPROVED, stage)
    assert connection.writes == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["stage", "recipe"])
async def test_bad_legacy_stage_still_refuses_before_any_database_call(monkeypatch, example_inventory, damage):
    _, selected, stage = example_inventory
    selected = (object(),) if damage == "recipe" else selected
    stage = replace(stage, generation_sha256="f" * 64) if damage == "stage" else stage
    connection = AsyncMock()
    with pytest.raises(selection.RegistrySourceSelectionError):
        await selection.record_registry_source_selection(connection, _target(), selected, APPROVED, stage)
    assert connection.mock_calls == []


def test_raw_bounds_duplicates_and_unknown_reference_are_closed(example_inventory):
    root, selected, stage = example_inventory
    receipt = selection.build_registry_source_selection_receipt(selected, APPROVED, stage, inventory_root=root)
    encoded = selection._canonical(receipt)
    assert (
        selection.validate_registry_source_selection_receipt(encoded + " " * (4096 - len(encoded)), inventory_root=root)
        == receipt
    )
    for content in (encoded + " " * (4097 - len(encoded)), encoded[:-1] + ',"revision":2}', "[NaN]"):
        with pytest.raises(selection.RegistrySourceSelectionError):
            selection.validate_registry_source_selection_receipt(content, inventory_root=root)
    candidate = _candidate(root, stage)
    candidate["validation_json"][selection.SUMMARY_KEY] = receipt
    for field in ("source_recipes_json", "source_generations", "validation_json"):
        changed = copy.deepcopy(candidate)
        changed[field] = " " * 1048577 + "{}"
        with pytest.raises(selection.RegistrySourceSelectionError):
            selection.verify_registry_source_selection_receipt(changed)
    candidate["source_recipes_json"] = {"component": "unknown"}
    with pytest.raises(selection.RegistrySourceSelectionError):
        selection.verify_registry_source_selection_receipt(candidate)


def test_legacy_hundred_recipe_boundary_remains_accepted():
    root, selected = _inventory(100)
    stage = _stage(root)
    receipt = selection.build_registry_source_selection_receipt(selected, APPROVED, stage)
    assert receipt["recipe_count"] == 100 and receipt["revision"] == 1
    assert receipt["recipe_sha256"] == recipes.registry_source_recipes_sha256(
        recipes.canonical_registry_source_recipes(selected)
    )


@pytest.mark.parametrize("field", ["source_recipes_json", "source_generations", "validation_json"])
def test_raw_metadata_bound_precedes_json_decoding(field, monkeypatch, example_inventory):
    root, selected, stage = example_inventory
    receipt = selection.build_registry_source_selection_receipt(selected, APPROVED, stage, inventory_root=root)
    candidate = _candidate(root, stage)
    candidate["validation_json"][selection.SUMMARY_KEY] = receipt
    candidate[field] = " " * 1048577 + "{}"

    def forbidden_decode(*_arguments, **_options):
        pytest.fail("Unbounded raw selection metadata reached JSON decoding")

    monkeypatch.setattr(selection.json, "loads", forbidden_decode)
    with pytest.raises(selection.RegistrySourceSelectionError):
        selection.verify_registry_source_selection_receipt(candidate)


@pytest.mark.asyncio
async def test_record_requires_caller_transaction_before_pin_lookup(monkeypatch, example_inventory):
    _, selected, stage = example_inventory
    connection = AsyncMock()
    connection.is_in_transaction = lambda: False
    current_pin = AsyncMock()
    monkeypatch.setattr(selection, "pin_approved_membership_source", current_pin)
    with pytest.raises(selection.RegistrySourceSelectionError):
        await selection.record_registry_source_selection(connection, _target(), selected, APPROVED, stage)
    current_pin.assert_not_awaited()
