# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Writer cleanup wiring only; mocked custody provides no native retention proof."""

import hashlib
from dataclasses import asdict
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import network_compact_retention as retention
from process import registry_source_contribution_artifacts as artifacts
from process import registry_source_inventory_store as inventory_store
from process.network_provider_only_projection import TABLE_NAME as PROVIDER_TABLE
from process.registry_source_contribution_store import (
    CONTRIBUTION_TABLE,
    SUMMARY_KEY,
    registry_source_contribution_sha256,
)
from process.registry_source_inventory_contract import make_source_inventory_entry, source_inventory_root
from process.registry_source_inventory_reference import INVENTORY_DIGEST_KEY, SourceInventoryReference
from process.registry_source_inventory_summaries import (
    contribution_summary_v3,
    contribution_summary_v4,
    contribution_summary_v5,
)
from process.registry_source_recipe_store import RECIPE_DIGEST_KEY, canonical_registry_source_recipes
from tests.test_registry_ptg_office_membership_recipe import _recipe as ptg_recipe
from tests.test_registry_source_inventory_fact_owners import _owner
from tests.test_registry_source_mapping_v3 import _mapping
from tests.test_registry_source_recipe_store_postgres import _example
from tests.test_verified_inventory_v2 import prepared, rows

CONTROL = "synthetic_control"


def _empty_reference(*, summary):
    fixture = _owner(0)
    fixture.candidate["source_generations"].pop(RECIPE_DIGEST_KEY)
    fixture.custody["receipt"]["closure"]["relation_oids"][PROVIDER_TABLE] = 44
    if not summary:
        fixture.candidate["source_generations"].pop(SUMMARY_KEY)
        fixture.candidate["validation_json"].pop(SUMMARY_KEY)
        fixture.custody["receipt"]["closure"]["relation_oids"].pop(CONTRIBUTION_TABLE)
    return fixture


def _mixed_reference():
    fixture = _owner(2)
    recipes = tuple(
        sorted(
            (_example(0), _example(1), ptg_recipe()), key=lambda recipe: canonical_registry_source_recipes((recipe,))
        )
    )
    by_recipe = {source["recipe_sha256"]: source for source in fixture.sources}
    reference = artifacts.SourceFactArtifactReference(str(UUID(int=99)), 71, "d" * 64)
    anchored_sha = sorted(by_recipe)[-1]
    by_recipe[anchored_sha] = {**by_recipe[anchored_sha], **asdict(reference)}
    entries = []
    for ordinal, recipe in enumerate(recipes, 1):
        identity = hashlib.sha256(canonical_registry_source_recipes((recipe,)).encode()).hexdigest()
        entries.append(make_source_inventory_entry(ordinal, recipe, contribution=by_recipe.get(identity)))
    fixture.entries = tuple(entries)
    fixture.reference = SourceInventoryReference(source_inventory_root(fixture.entries))
    fixture.sources = tuple(by_recipe[key] for key in sorted(by_recipe))
    fixture.candidate["source_recipes_json"] = fixture.reference.as_dict()
    fixture.candidate["source_generations"][RECIPE_DIGEST_KEY] = fixture.reference.root.recipes_sha256
    fixture.candidate["source_generations"][INVENTORY_DIGEST_KEY] = fixture.reference.root.entries_sha256
    summary = contribution_summary_v3(fixture.reference.root, fixture.sources)
    fixture.candidate["validation_json"][SUMMARY_KEY] = summary
    fixture.candidate["source_generations"][SUMMARY_KEY] = registry_source_contribution_sha256(
        summary, inventory_root=fixture.reference.root
    )
    fixture.custody["receipt"]["closure"]["relation_oids"][PROVIDER_TABLE] = 44
    return fixture


def _versioned_reference(revision):
    fixture = _owner(1)
    if revision == 4:
        recipe, descriptor_by_field, derivation = rows(anchored=True)
        root, entries = prepared([(recipe, descriptor_by_field, derivation)])
        summary = contribution_summary_v4(root, [descriptor_by_field])
    else:
        recipe = _example()
        reference = artifacts.SourceFactArtifactReference(str(UUID(int=99)), 71, "d" * 64)
        descriptor_by_field = {**_mapping(recipe, witness=False), **asdict(reference)}
        entries = (make_source_inventory_entry(1, recipe, contribution=descriptor_by_field),)
        root = source_inventory_root(entries)
        summary = contribution_summary_v5(root, [descriptor_by_field])
    fixture.reference = SourceInventoryReference(root)
    fixture.entries, fixture.sources = entries, (descriptor_by_field,)
    fixture.candidate["source_recipes_json"] = fixture.reference.as_dict()
    fixture.candidate["source_generations"].update(
        {
            RECIPE_DIGEST_KEY: root.recipes_sha256,
            INVENTORY_DIGEST_KEY: root.entries_sha256,
            SUMMARY_KEY: registry_source_contribution_sha256(summary, inventory_root=root),
        }
    )
    fixture.candidate["validation_json"][SUMMARY_KEY] = summary
    fixture.custody["receipt"]["closure"]["relation_oids"][PROVIDER_TABLE] = 44
    return fixture


@pytest.mark.parametrize("count", [1, 100])
@pytest.mark.parametrize("anchored", [False, True])
async def test_legacy_dependency_arrays_keep_existing_codec_and_owner_probe(monkeypatch, count, anchored):
    fixture = _owner(count, legacy=True, anchored=anchored)
    anchors = AsyncMock()
    loader = AsyncMock(side_effect=AssertionError("Legacy cleanup loaded a referenced inventory"))
    monkeypatch.setattr(retention, "_require_source_fact_anchors", anchors)
    monkeypatch.setattr(inventory_store, "_load_locked_source_inventory", loader)
    connection = SimpleNamespace()
    dependencies = await retention._source_fact_dependencies(
        connection, CONTROL, fixture.candidate, fixture.custody["receipt"]
    )
    assert dependencies == ({42, 71} if anchored else {42})
    assert len(anchors.await_args.args[2]) == (count if anchored else 0)
    loader.assert_not_awaited()


async def test_legacy_codec_still_refuses_101_recipes_before_cleanup():
    with pytest.raises(ValueError):
        _owner(101, legacy=True)


def _connection(fixture):
    connection = AsyncMock()
    connection.is_in_transaction = lambda: True
    connection.events, connection.anchor_pages = [], []

    async def fetchval(query, *arguments):
        if query == "SHOW transaction_isolation":
            return "read committed"
        assert query.startswith("WITH expected AS") and "eligible" not in query
        assert 1 <= len(arguments[0]) <= 100
        connection.events.append("anchors")
        connection.anchor_pages.append(tuple(arguments[0]))
        return True

    connection.fetchval.side_effect = fetchval
    connection.fetch.side_effect = [[{"seal_candidate_id": UUID(fixture.candidate["candidate_id"])}], []]
    return connection


def _maintenance_spy(monkeypatch, connection, fixture):
    from process import registry_source_inventory_maintenance as maintenance

    async def read(actual, target, *, control_schema):
        assert actual is connection and actual.is_in_transaction()
        assert await actual.fetchval("SHOW transaction_isolation") == "read committed"
        assert target.table_name == "network_membership"
        assert all(
            getattr(target, field) == fixture.candidate[field] for field in fixture.candidate if field in asdict(target)
        )
        assert control_schema == CONTROL
        connection.events.append("verified_inventory")
        return fixture.candidate, fixture.custody, fixture.entries

    checked = AsyncMock(side_effect=read)
    monkeypatch.setattr(maintenance, "read_sealed_source_inventory_for_maintenance", checked)
    forbidden = AsyncMock(side_effect=AssertionError("Cleanup used an RR loader or read fact contents"))
    monkeypatch.setattr(inventory_store, "_load_locked_source_inventory", forbidden)
    monkeypatch.setattr(artifacts, "contribution_fact_sha256", forbidden)
    return checked, forbidden


@pytest.mark.parametrize("summary", [False, True])
async def test_empty_newref_requires_verified_full_inventory_before_no_fact_references(monkeypatch, summary):
    fixture = _empty_reference(summary=summary)
    connection = _connection(fixture)
    checked, forbidden = _maintenance_spy(monkeypatch, connection, fixture)
    dependencies = await retention._source_fact_dependencies(
        connection, CONTROL, fixture.candidate, fixture.custody["receipt"]
    )
    assert dependencies == ({42} if summary else set())
    assert connection.events == ["verified_inventory"] and not connection.anchor_pages
    checked.assert_awaited_once()
    forbidden.assert_not_awaited()


async def test_non_fhir_newref_without_contribution_receipt_is_still_fully_checked(monkeypatch):
    fixture = _empty_reference(summary=False)
    fixture.entries = (make_source_inventory_entry(1, ptg_recipe()),)
    fixture.reference = SourceInventoryReference(source_inventory_root(fixture.entries))
    fixture.candidate["source_recipes_json"] = fixture.reference.as_dict()
    fixture.candidate["source_generations"].update(
        {
            RECIPE_DIGEST_KEY: fixture.reference.root.recipes_sha256,
            INVENTORY_DIGEST_KEY: fixture.reference.root.entries_sha256,
        }
    )
    connection = _connection(fixture)
    checked, _ = _maintenance_spy(monkeypatch, connection, fixture)
    assert (
        await retention._source_fact_dependencies(connection, CONTROL, fixture.candidate, fixture.custody["receipt"])
        == set()
    )
    checked.assert_awaited_once()
    assert connection.events == ["verified_inventory"]


@pytest.mark.parametrize("encoded", [False, True])
async def test_mixed_newref_keeps_local_and_original_fact_heaps_after_full_verification(monkeypatch, encoded):
    import json

    fixture = _mixed_reference()
    if encoded:
        fixture.candidate["source_recipes_json"] = json.dumps(fixture.candidate["source_recipes_json"])
    connection = _connection(fixture)
    checked, forbidden = _maintenance_spy(monkeypatch, connection, fixture)
    assert await retention._source_fact_dependencies(
        connection, CONTROL, fixture.candidate, fixture.custody["receipt"]
    ) == {42, 71}
    assert connection.events == ["verified_inventory", "anchors"]
    assert len(connection.anchor_pages) == 1 and len(connection.anchor_pages[0]) == 1
    checked.assert_awaited_once()
    forbidden.assert_not_awaited()


@pytest.fixture(scope="module")
def full_reference():
    return _owner(1000, anchored=True)


@pytest.mark.parametrize("count", [1, 101, 1000])
async def test_newref_original_owner_checks_page_at_hundred_without_lowering_total_bound(
    monkeypatch, full_reference, count
):
    fixture = full_reference if count == 1000 else _owner(count, anchored=True)
    connection = _connection(fixture)
    checked, _ = _maintenance_spy(monkeypatch, connection, fixture)
    assert await retention._source_fact_dependencies(
        connection, CONTROL, fixture.candidate, fixture.custody["receipt"]
    ) == {42, 71}
    assert [len(page) for page in connection.anchor_pages] == (
        [100] * (count // 100) + ([count % 100] if count % 100 else [])
    )
    assert connection.events[0] == "verified_inventory"
    checked.assert_awaited_once()


@pytest.mark.parametrize(
    "damage", ["custody", "root", "summary", "missing", "descriptor", "factheap", "maintenance", "anchor"]
)
@pytest.mark.parametrize("revision", [3, 4, 5])
async def test_newref_retention_refuses_changed_proof_and_descriptor_pairing(monkeypatch, damage, revision):
    import copy
    import json
    from dataclasses import replace

    fixture = _mixed_reference() if revision == 3 else _versioned_reference(revision)
    expected_receipt = copy.deepcopy(fixture.custody["receipt"])
    if damage == "custody":
        fixture.custody["receipt"]["runtime_roles"] = ["changed_role"]
    if damage == "root":
        fixture.candidate["source_generations"][INVENTORY_DIGEST_KEY] = "f" * 64
    if damage == "summary":
        summary = fixture.candidate["validation_json"][SUMMARY_KEY]
        summary["sources_sha256"] = "f" * 64
        fixture.candidate["source_generations"][SUMMARY_KEY] = registry_source_contribution_sha256(
            summary, inventory_root=fixture.reference.root
        )
    if damage == "missing":
        fixture.entries = fixture.entries[:-1]
    if damage == "descriptor":
        changed = next(entry for entry in fixture.entries if entry.contribution_json is not None)
        descriptor = json.loads(changed.contribution_json)
        descriptor["fact_sha256"] = "f" * 64
        fixture.entries = tuple(
            replace(entry, contribution_json=json.dumps(descriptor, sort_keys=True, separators=(",", ":")))
            if entry is changed
            else entry
            for entry in fixture.entries
        )
    if damage == "factheap":
        fixture.custody["receipt"]["closure"]["relation_oids"].pop(CONTRIBUTION_TABLE)
        expected_receipt = fixture.custody["receipt"]
    connection = _connection(fixture)
    checked, forbidden = _maintenance_spy(monkeypatch, connection, fixture)
    if damage == "maintenance":
        checked.side_effect = ValueError("synthetic required custody refusal")
    if damage == "anchor":
        connection.fetchval.side_effect = lambda query, *arguments: (
            "read committed" if query.startswith("SHOW") else False
        )
    with pytest.raises((retention.NetworkCompactRetentionError, ValueError)):
        await retention._source_fact_dependencies(connection, CONTROL, fixture.candidate, expected_receipt)
    if damage != "anchor":
        assert not connection.anchor_pages
    forbidden.assert_not_awaited()


async def test_helper_return_count_cannot_exceed_registered_thousand(monkeypatch, full_reference):
    fixture = SimpleNamespace(
        **{**full_reference.__dict__, "entries": (*full_reference.entries, full_reference.entries[0])}
    )
    connection = _connection(fixture)
    _maintenance_spy(monkeypatch, connection, fixture)
    with pytest.raises(retention.NetworkCompactRetentionError, match="source_inventory_changed"):
        await retention._source_fact_dependencies(connection, CONTROL, fixture.candidate, fixture.custody["receipt"])
    assert not connection.anchor_pages


@pytest.mark.parametrize("eligible", [False, True])
@pytest.mark.parametrize("requested", [42, 43, 44, 71, 99])
@pytest.mark.parametrize("revision", [3, 4, 5])
async def test_outer_census_retains_all_sealed_native_and_original_fact_dependencies(
    monkeypatch, eligible, requested, revision
):
    fixture = _mixed_reference() if revision == 3 else _versioned_reference(revision)
    fixture.candidate["eligible"] = eligible
    fixture.custody["receipt"]["revision"] = 4
    connection = _connection(fixture)
    checked, forbidden = _maintenance_spy(monkeypatch, connection, fixture)
    catalog_by_name = {name: index + 100 for index, name in enumerate(retention._TABLES)}
    monkeypatch.setattr(retention, "_inventory", AsyncMock(return_value=catalog_by_name))
    monkeypatch.setattr(retention, "_published_without_seal", AsyncMock(return_value=False))
    monkeypatch.setattr(retention.custody, "_control", AsyncMock(return_value={"synthetic": "control"}))
    monkeypatch.setattr(
        retention, "_authenticated_candidate", AsyncMock(return_value=(fixture.candidate, fixture.custody["receipt"]))
    )
    monkeypatch.setattr(
        retention, "_dependencies", lambda candidate, receipt: set(receipt["closure"]["relation_oids"].values())
    )
    if requested == 99:
        assert (
            await retention.require_network_compact_artifacts_unreferenced(
                connection, [requested], control_schema=CONTROL
            )
            is None
        )
    else:
        with pytest.raises(retention.NetworkCompactRetentionError, match="artifact_referenced"):
            await retention.require_network_compact_artifacts_unreferenced(
                connection, [requested], control_schema=CONTROL
            )
    checked.assert_awaited_once()
    forbidden.assert_not_awaited()
    assert all("eligible" not in call.args[0] for call in connection.fetch.await_args_list)
    assert all(str(call.args[0]).startswith("LOCK TABLE") for call in connection.execute.await_args_list)


async def test_writer_cleanup_fresh_snapshot_requirement_stays_read_committed(monkeypatch):
    fixture = _mixed_reference()
    connection = _connection(fixture)
    connection.fetchval.side_effect = None
    connection.fetchval.return_value = "repeatable read"
    checked, _ = _maintenance_spy(monkeypatch, connection, fixture)
    monkeypatch.setattr(
        retention,
        "_inventory",
        AsyncMock(return_value={name: index + 100 for index, name in enumerate(retention._TABLES)}),
    )
    with pytest.raises(retention.NetworkCompactRetentionError, match="fresh_snapshot_required"):
        await retention.require_network_compact_artifacts_unreferenced(connection, [99], control_schema=CONTROL)
    checked.assert_not_awaited()
