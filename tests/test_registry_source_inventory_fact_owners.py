# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure retained-owner wiring checks; mocked native custody provides no physical proof."""

import copy
import json
from dataclasses import asdict, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from db.registry_source_inventory_schema import INVENTORY_TABLE
from process import registry_source_contribution_artifacts as artifacts
from process import registry_source_contribution_store as contributions
from process import registry_source_inventory_store as inventory_store
from process.registry_source_inventory_contract import make_source_inventory_entry, source_inventory_root
from process.registry_source_inventory_reference import INVENTORY_DIGEST_KEY, SourceInventoryReference
from process.registry_source_inventory_summaries import contribution_summary_v3
from process.registry_source_recipe_store import RECIPE_DIGEST_KEY, canonical_registry_source_recipes
from tests.test_registry_source_contributions import _descriptor
from tests.test_registry_source_recipe_store_postgres import _example

CONTROL = "synthetic_control"


def _owner(count, *, legacy=False, anchored=False):
    """Build exact source ownership metadata without supplying native authority."""
    recipes = tuple(
        sorted(
            (_example(index) for index in range(count)), key=lambda recipe: canonical_registry_source_recipes((recipe,))
        )
    )
    source_descriptors = tuple(contributions.locally_owned_source_descriptor(_descriptor(recipe)) for recipe in recipes)
    if anchored:
        source_descriptors = tuple(
            {**source_descriptor, **asdict(artifacts.SourceFactArtifactReference(str(uuid4()), 71, "d" * 64))}
            for source_descriptor in source_descriptors
        )
    entries = tuple(
        make_source_inventory_entry(index, recipe, contribution=source_descriptor)
        for index, (recipe, source_descriptor) in enumerate(zip(recipes, source_descriptors), 1)
    )
    reference = SourceInventoryReference(source_inventory_root(entries))
    summary_by_field = contribution_summary_v3(
        reference.root, sorted(source_descriptors, key=lambda source_descriptor: source_descriptor["recipe_sha256"])
    )
    identity = str(uuid4())
    candidate_by_field = {
        "dataset_id": str(uuid4()),
        "schema_id": str(uuid4()),
        "producer_id": str(uuid4()),
        "candidate_id": identity,
        "schema_name": "network_candidate_" + identity.replace("-", ""),
        "state": "published",
        "source_recipes_json": reference.as_dict(),
        "source_generations": {
            RECIPE_DIGEST_KEY: reference.root.recipes_sha256,
            INVENTORY_DIGEST_KEY: reference.root.entries_sha256,
        },
    }
    if legacy:
        candidate_by_field["source_recipes_json"] = json.loads(canonical_registry_source_recipes(recipes))
        candidate_by_field["source_generations"].pop(INVENTORY_DIGEST_KEY)
        summary_by_field = _legacy_owner_summary(source_descriptors)
    candidate_by_field["validation_json"] = {contributions.SUMMARY_KEY: summary_by_field}
    candidate_by_field["source_generations"][contributions.SUMMARY_KEY] = (
        contributions.registry_source_contribution_sha256(
            summary_by_field, inventory_root=None if legacy else reference.root
        )
    )
    custody_by_field = {
        "seal_sha256": "a" * 64,
        "receipt": {"closure": {"relation_oids": {contributions.CONTRIBUTION_TABLE: 42, INVENTORY_TABLE: 43}}},
    }
    return SimpleNamespace(
        candidate=candidate_by_field,
        custody=custody_by_field,
        reference=reference,
        entries=entries,
        sources=source_descriptors,
    )


@pytest.fixture(scope="module")
def full_owner():
    return _owner(1000)


def _connection(owner):
    connection = AsyncMock()
    connection.is_in_transaction = lambda: True
    connection.fetchval.return_value = "repeatable read"
    connection.fetch.return_value = [owner.candidate]
    return connection


def _native_spies(monkeypatch, owner):
    events = []

    async def custody(connection, target, *, control_schema, required):
        assert required is True and control_schema == CONTROL
        assert all(
            getattr(target, field) == owner.candidate[field] for field in owner.candidate if field in asdict(target)
        )
        events.append("custody")
        return owner.custody

    async def inventory(connection, target, candidate, *, expected_table_oid, control_schema):
        assert events[-1] == "custody"
        assert candidate == owner.candidate and expected_table_oid == 43 and control_schema == CONTROL
        assert all(getattr(target, field) == candidate[field] for field in candidate if field in asdict(target))
        events.append("inventory")
        return owner.reference, owner.entries

    native_custody, native_inventory = AsyncMock(side_effect=custody), AsyncMock(side_effect=inventory)
    monkeypatch.setattr(artifacts, "verify_network_candidate_custody", native_custody)
    monkeypatch.setattr(inventory_store, "_load_locked_source_inventory", native_inventory)
    return events, native_custody, native_inventory


def _selected(owner, sources=None):
    reference = artifacts.SourceFactArtifactReference(owner.candidate["candidate_id"], 42, owner.custody["seal_sha256"])
    return tuple({**source, **asdict(reference)} for source in (owner.sources if sources is None else sources))


@pytest.mark.asyncio
async def test_full_inventory_is_loaded_once_per_authenticated_owner(monkeypatch, full_owner):
    owner = full_owner
    connection = _connection(owner)
    events, custody, inventory = _native_spies(monkeypatch, owner)
    selected = _selected(owner, owner.sources[:3])
    resolved = await artifacts.authenticate_anchored_source_fact_metadata(connection, selected, control_schema=CONTROL)
    assert set(resolved) == {source["recipe_sha256"] for source in selected}
    assert events == ["custody", "inventory"]
    custody.assert_awaited_once()
    inventory.assert_awaited_once()
    assert all(
        artifact.reference.fact_candidate_id == owner.candidate["candidate_id"] for artifact in resolved.values()
    )
    assert all(artifact.descriptor in owner.sources for artifact in resolved.values())
    connection.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("legacy", [False, True])
async def test_retained_sources_keep_original_direct_owners(monkeypatch, legacy):
    owner = _owner(2, legacy=legacy, anchored=True)
    connection = _connection(owner)
    _, custody, inventory = _native_spies(monkeypatch, owner)
    expected = tuple(sorted(owner.sources, key=lambda source: source["recipe_sha256"])) if legacy else owner.sources
    assert (
        await artifacts.retained_source_fact_descriptors(
            connection, owner.candidate["candidate_id"], control_schema=CONTROL
        )
        == expected
    )
    custody.assert_awaited_once()
    assert inventory.await_count == int(not legacy)


@pytest.mark.asyncio
async def test_retained_full_inventory_acquires_local_original_anchors(monkeypatch, full_owner):
    owner = full_owner
    connection = _connection(owner)
    _, _, inventory = _native_spies(monkeypatch, owner)
    retained = await artifacts.retained_source_fact_descriptors(
        connection, owner.candidate["candidate_id"], control_schema=CONTROL
    )
    assert retained == _selected(owner) and len(retained) == 1000
    inventory.assert_awaited_once()


@pytest.mark.asyncio
async def test_inventory_header_never_bypasses_required_custody(monkeypatch):
    owner = _owner(1)
    connection = _connection(owner)
    _, custody, inventory = _native_spies(monkeypatch, owner)
    custody.side_effect = ValueError("synthetic custody refusal")
    with pytest.raises(ValueError, match="custody refusal"):
        await artifacts.retained_source_fact_descriptors(
            connection, owner.candidate["candidate_id"], control_schema=CONTROL
        )
    inventory.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("oid", [None, True, 0, 2**32])
async def test_inventory_oid_must_come_from_verified_custody(monkeypatch, oid):
    owner = _owner(1)
    owner.custody["receipt"]["closure"]["relation_oids"][INVENTORY_TABLE] = oid
    _, _, inventory = _native_spies(monkeypatch, owner)
    with pytest.raises(contributions.RegistrySourceContributionError, match="inventory_changed"):
        await artifacts.retained_source_fact_descriptors(
            _connection(owner), owner.candidate["candidate_id"], control_schema=CONTROL
        )
    inventory.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["summary", "descriptor", "root", "missing"])
async def test_inventory_sources_must_match_complete_registered_summary(monkeypatch, damage):
    owner = _owner(2)
    _, _, inventory = _native_spies(monkeypatch, owner)
    if damage == "summary":
        summary = owner.candidate["validation_json"][contributions.SUMMARY_KEY]
        summary["sources_sha256"] = "f" * 64
        owner.candidate["source_generations"][contributions.SUMMARY_KEY] = (
            contributions.registry_source_contribution_sha256(summary, inventory_root=owner.reference.root)
        )
    elif damage == "descriptor":
        changed = json.loads(owner.entries[-1].contribution_json)
        changed["fact_sha256"] = "f" * 64
        owner.entries = (
            *owner.entries[:-1],
            replace(owner.entries[-1], contribution_json=contributions._canonical(changed)),
        )
    elif damage == "root":
        inventory.side_effect = None
        inventory.return_value = (_owner(1).reference, owner.entries)
    else:
        owner.entries = owner.entries[:-1]
    with pytest.raises(contributions.RegistrySourceContributionError, match="inventory_changed"):
        await artifacts.retained_source_fact_descriptors(
            _connection(owner), owner.candidate["candidate_id"], control_schema=CONTROL
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["chain", "capture", "fact_hash", "fact_rows", "seal", "oid", "schema"])
async def test_inventory_owner_reuse_preserves_physical_and_source_identity(monkeypatch, damage):
    owner = _owner(1, anchored=damage == "chain")
    selected_sources = list(_selected(owner))
    field, value = {
        "capture": ("capture_sha256", "f" * 64),
        "fact_hash": ("fact_sha256", "f" * 64),
        "fact_rows": ("fact_rows", 2),
        "seal": ("fact_seal_sha256", "f" * 64),
        "oid": ("fact_table_oid", 99),
    }.get(damage, (None, None))
    if field is not None:
        selected_sources[0][field] = value
    if damage == "capture":
        selected_sources[0]["source_generation"] = contributions._digest([value, selected_sources[0]["mapping_sha256"]])
    if damage == "schema":
        owner.candidate["schema_name"] = "network_candidate_" + uuid4().hex
    _native_spies(monkeypatch, owner)
    with pytest.raises(ValueError if damage == "schema" else contributions.RegistrySourceContributionError):
        await artifacts.authenticate_anchored_source_fact_metadata(
            _connection(owner), selected_sources, control_schema=CONTROL
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [0, 101, 1000])
async def test_content_verification_pages_authentication_and_checks_every_fact(monkeypatch, count):
    descriptors = tuple({"recipe_sha256": f"{index:064x}"} for index in range(count))
    pages = []

    async def authenticate(connection, selected, *, control_schema):
        assert len(selected) <= 100 and control_schema == CONTROL
        pages.append(tuple(selected))
        return {
            source["recipe_sha256"]: SimpleNamespace(
                relation="synthetic_facts", descriptor={"fact_sha256": "e" * 64, "fact_rows": 1}
            )
            for source in selected
        }

    monkeypatch.setattr(artifacts, "authenticate_anchored_source_fact_metadata", authenticate)
    contents = AsyncMock(return_value=("e" * 64, 1))
    monkeypatch.setattr(artifacts, "contribution_fact_sha256", contents)
    resolved = await artifacts.verify_anchored_source_fact_contents(AsyncMock(), descriptors, control_schema=CONTROL)
    assert [source for page in pages for source in page] == list(descriptors)
    assert pages and len(resolved) == count and contents.await_count == count


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["duplicate", "bound"])
async def test_complete_content_request_rejects_cross_page_duplicates_and_overflow(monkeypatch, damage):
    descriptors = [{"recipe_sha256": f"{index:064x}"} for index in range(1001 if damage == "bound" else 101)]
    if damage == "duplicate":
        descriptors[-1] = copy.deepcopy(descriptors[0])
    authenticate = AsyncMock()
    monkeypatch.setattr(artifacts, "authenticate_anchored_source_fact_metadata", authenticate)
    with pytest.raises(contributions.RegistrySourceContributionError, match="reference_invalid"):
        await artifacts.verify_anchored_source_fact_contents(AsyncMock(), descriptors, control_schema=CONTROL)
    authenticate.assert_not_awaited()


@pytest.mark.asyncio
async def test_public_metadata_authentication_keeps_hundred_descriptor_cap():
    owner = _owner(1)
    connection = _connection(owner)
    with pytest.raises(contributions.RegistrySourceContributionError, match="reference_invalid"):
        await artifacts.authenticate_anchored_source_fact_metadata(
            connection, _selected(owner) * 101, control_schema=CONTROL
        )
    connection.fetch.assert_not_awaited()


@pytest.mark.asyncio
async def test_paged_content_hash_mismatch_is_terminal(monkeypatch):
    artifact = SimpleNamespace(relation="synthetic_facts", descriptor={"fact_sha256": "e" * 64, "fact_rows": 1})
    monkeypatch.setattr(
        artifacts, "authenticate_anchored_source_fact_metadata", AsyncMock(return_value={"a" * 64: artifact})
    )
    monkeypatch.setattr(artifacts, "contribution_fact_sha256", AsyncMock(return_value=("f" * 64, 1)))
    with pytest.raises(contributions.RegistrySourceContributionError, match="content_changed"):
        await artifacts.verify_anchored_source_fact_contents(
            AsyncMock(), ({"recipe_sha256": "a" * 64},), control_schema=CONTROL
        )


def _legacy_owner_summary(source_descriptors):
    """Keep the original closed legacy contribution summary shape."""
    return {
        "component": contributions.SUMMARY_KEY,
        "revision": 2,
        "table_name": contributions.CONTRIBUTION_TABLE,
        "sources": sorted(source_descriptors, key=lambda source_descriptor: source_descriptor["recipe_sha256"]),
    }
