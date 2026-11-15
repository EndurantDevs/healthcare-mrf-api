# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retirement resolves complete source inventories and retained named fact generations."""

import hashlib
import json
from dataclasses import asdict, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import network_compact_retention as retention
from process import registry_source_contribution_artifacts as artifacts
from process import registry_source_contribution_store as contributions
from process.registry_source_inventory_contract import make_source_inventory_entry, source_inventory_root
from process.registry_source_inventory_reference import INVENTORY_DIGEST_KEY, SourceInventoryReference
from process.registry_source_inventory_summaries import (
    contribution_summary_v3,
    contribution_summary_v4,
    contribution_summary_v5,
)
from process.registry_source_recipe_store import RECIPE_DIGEST_KEY, canonical_registry_source_recipes
from tests.registry_source_recipe_support import _example
from tests.test_registry_ptg_office_membership_recipe import _recipe as ptg_recipe
from tests.test_registry_source_inventory_fact_owners import _owner
from tests.test_registry_source_mapping_v3 import _mapping
from tests.test_verified_inventory_v2 import prepared, rows

CONTROL = "synthetic_control"


def _set_inventory(fixture, recipes, source_descriptors, *, revision=3):
    by_recipe = {source_descriptor["recipe_sha256"]: source_descriptor for source_descriptor in source_descriptors}
    fixture.entries = tuple(
        make_source_inventory_entry(
            ordinal,
            recipe,
            contribution=by_recipe.get(
                hashlib.sha256(canonical_registry_source_recipes((recipe,)).encode()).hexdigest()
            ),
        )
        for ordinal, recipe in enumerate(recipes, 1)
    )
    fixture.reference = SourceInventoryReference(source_inventory_root(fixture.entries))
    fixture.sources = tuple(by_recipe[key] for key in sorted(by_recipe))
    summary = {3: contribution_summary_v3, 4: contribution_summary_v4, 5: contribution_summary_v5}[revision](
        fixture.reference.root, fixture.sources
    )
    fixture.candidate["source_recipes_json"] = fixture.reference.as_dict()
    fixture.candidate["source_generations"].update(
        {
            RECIPE_DIGEST_KEY: fixture.reference.root.recipes_sha256,
            INVENTORY_DIGEST_KEY: fixture.reference.root.entries_sha256,
            contributions.SUMMARY_KEY: contributions.registry_source_contribution_sha256(
                summary, inventory_root=fixture.reference.root
            ),
        }
    )
    if not recipes:
        fixture.candidate["source_generations"].pop(RECIPE_DIGEST_KEY, None)
    fixture.candidate["validation_json"][contributions.SUMMARY_KEY] = summary
    return fixture


def _mixed_reference():
    fixture = _owner(2)
    recipes = tuple(
        sorted(
            (_example(0), _example(1), ptg_recipe()), key=lambda recipe: canonical_registry_source_recipes((recipe,))
        )
    )
    source_descriptors = list(fixture.sources)
    reference = artifacts.SourceFactArtifactReference(
        str(UUID(int=99)), "original_facts", contributions.CONTRIBUTION_TABLE, "d" * 64
    )
    source_descriptors[-1] = {**source_descriptors[-1], **asdict(reference)}
    return _set_inventory(fixture, recipes, source_descriptors)


def _versioned_reference(revision):
    if revision == 3:
        return _mixed_reference()
    fixture = _owner(1)
    if revision == 4:
        recipe, descriptor_by_field, derivation = rows(anchored=True)
        root, entries = prepared([(recipe, descriptor_by_field, derivation)])
        assert root == source_inventory_root(entries)
    else:
        recipe = _example()
        reference = artifacts.SourceFactArtifactReference(
            str(UUID(int=99)), "original_facts", contributions.CONTRIBUTION_TABLE, "d" * 64
        )
        descriptor_by_field = {**_mapping(recipe, witness=False), **asdict(reference)}
    return _set_inventory(fixture, (recipe,), (descriptor_by_field,), revision=revision)


def _metadata_spies(monkeypatch, fixture):
    original_by_id = {}
    for source_descriptor in fixture.sources:
        reference = artifacts.source_fact_artifact_reference(source_descriptor)
        if reference is None:
            continue
        original = SimpleNamespace(
            candidate={
                **fixture.candidate,
                "candidate_id": reference.fact_candidate_id,
                "schema_name": reference.fact_schema_name,
            },
            sources=(contributions.locally_owned_source_descriptor(source_descriptor),),
            generation_id=reference.fact_generation_id,
        )
        original_by_id[reference.fact_candidate_id] = original
    requested_pages = []

    async def candidates(connection, schema, identities):
        assert schema == CONTROL and 1 <= len(identities) <= 4
        requested_pages.append(set(identities))
        return {identity: original_by_id[identity].candidate for identity in identities}

    async def evidence(connection, candidate, schema):
        original = original_by_id[candidate["candidate_id"]]
        assert schema == CONTROL and candidate == original.candidate
        return {
            "candidate_id": candidate["candidate_id"],
            "generation_id": original.generation_id,
            "validation": None,
            "semantic_validation": None,
        }

    owner_sources = artifacts._owner_sources

    async def sources(connection, candidate, schema, *, inventory_data):
        if candidate is fixture.candidate:
            return await owner_sources(connection, candidate, schema, inventory_data=inventory_data)
        return original_by_id[candidate["candidate_id"]].sources

    monkeypatch.setattr(artifacts, "_published_artifact_candidates", AsyncMock(side_effect=candidates))
    monkeypatch.setattr(artifacts, "load_published_artifact_validation", AsyncMock(side_effect=evidence))
    monkeypatch.setattr(artifacts, "_owner_sources", sources)
    monkeypatch.setattr(retention, "_retirement_inventory", AsyncMock(return_value=None))
    contents = AsyncMock(side_effect=AssertionError("Retirement read immutable fact payloads"))
    monkeypatch.setattr(artifacts, "contribution_fact_sha256", contents)
    return original_by_id, requested_pages, contents


@pytest.mark.parametrize("count", [1, 100])
@pytest.mark.parametrize("anchored", [False, True])
async def test_legacy_arrays_resolve_named_original_generations(monkeypatch, count, anchored):
    fixture = _owner(count, legacy=True, anchored=anchored)
    originals, pages, contents = _metadata_spies(monkeypatch, fixture)
    dependencies = await retention._source_fact_dependencies(object(), fixture.candidate, CONTROL, None)
    assert dependencies == ({("original_facts", contributions.CONTRIBUTION_TABLE)} if anchored else set())
    assert set().union(*pages) == set(originals) if pages else not originals
    contents.assert_not_awaited()


def test_legacy_recipe_count_bound_remains_hundred():
    with pytest.raises(ValueError):
        _owner(101, legacy=True)


@pytest.mark.parametrize("summary", [False, True])
async def test_empty_registered_inventory_has_no_original_fact_dependencies(monkeypatch, summary):
    fixture = _owner(0)
    if not summary:
        fixture.candidate["source_generations"].pop(contributions.SUMMARY_KEY)
        fixture.candidate["validation_json"].pop(contributions.SUMMARY_KEY)
    _, pages, contents = _metadata_spies(monkeypatch, fixture)
    assert (
        await retention._source_fact_dependencies(
            object(), fixture.candidate, CONTROL, (fixture.reference, fixture.entries)
        )
        == set()
    )
    assert not pages
    contents.assert_not_awaited()


@pytest.mark.parametrize("encoded", [False, True])
async def test_mixed_inventory_keeps_original_named_facts_after_complete_summary_validation(monkeypatch, encoded):
    fixture = _mixed_reference()
    if encoded:
        fixture.candidate["source_recipes_json"] = json.dumps(fixture.candidate["source_recipes_json"])
    originals, pages, contents = _metadata_spies(monkeypatch, fixture)
    assert await retention._source_fact_dependencies(
        object(), fixture.candidate, CONTROL, (fixture.reference, fixture.entries)
    ) == {("original_facts", contributions.CONTRIBUTION_TABLE)}
    assert set().union(*pages) == set(originals)
    contents.assert_not_awaited()


@pytest.mark.parametrize("count", [1, 101, 1000])
async def test_inventory_original_generation_queries_remain_bounded(monkeypatch, count):
    fixture = _owner(count, anchored=True)
    originals, pages, contents = _metadata_spies(monkeypatch, fixture)
    assert await retention._source_fact_dependencies(
        object(), fixture.candidate, CONTROL, (fixture.reference, fixture.entries)
    ) == {("original_facts", contributions.CONTRIBUTION_TABLE)}
    assert set().union(*pages) == set(originals) and sum(map(len, pages)) == count
    assert all(1 <= len(page) <= 4 for page in pages)
    contents.assert_not_awaited()


@pytest.mark.parametrize("revision", [3, 4, 5])
@pytest.mark.parametrize("damage", ["root", "summary", "missing", "descriptor", "generation", "source_identity"])
async def test_retention_refuses_changed_inventory_or_original_logical_scope(monkeypatch, damage, revision):
    fixture = _versioned_reference(revision)
    originals, _, contents = _metadata_spies(monkeypatch, fixture)
    match damage:
        case "root":
            fixture.candidate["source_generations"][INVENTORY_DIGEST_KEY] = "f" * 64
        case "summary":
            summary = fixture.candidate["validation_json"][contributions.SUMMARY_KEY]
            summary["sources_sha256"] = "f" * 64
            fixture.candidate["source_generations"][contributions.SUMMARY_KEY] = (
                contributions.registry_source_contribution_sha256(summary, inventory_root=fixture.reference.root)
            )
        case "missing":
            fixture.entries = fixture.entries[:-1]
        case "descriptor":
            entry = next(entry for entry in fixture.entries if entry.contribution_json is not None)
            descriptor = json.loads(entry.contribution_json)
            descriptor["fact_sha256"] = "f" * 64
            changed_entries = list(fixture.entries)
            changed_entries[changed_entries.index(entry)] = replace(
                entry, contribution_json=contributions._canonical(descriptor)
            )
            fixture.entries = tuple(changed_entries)
        case "generation":
            next(iter(originals.values())).generation_id = "b" * 64
        case _:
            original = next(iter(originals.values()))
            original.sources = ({**original.sources[0], "fact_sha256": "b" * 64},)
    with pytest.raises(contributions.RegistrySourceContributionError):
        await retention._source_fact_dependencies(
            object(), fixture.candidate, CONTROL, (fixture.reference, fixture.entries)
        )
    contents.assert_not_awaited()


@pytest.mark.parametrize("eligible", [False, True])
async def test_retirement_walk_keeps_all_retained_rollbacks_and_prepared_references(monkeypatch, eligible):
    fixture = _mixed_reference()
    fixture.candidate.update(eligible=eligible, bounded=True)
    connection = SimpleNamespace(fetch=AsyncMock(side_effect=[[fixture.candidate], []]))
    dependencies = AsyncMock(return_value={("original_facts", contributions.CONTRIBUTION_TABLE)})
    monkeypatch.setattr(retention, "_candidate_dependencies", dependencies)
    with pytest.raises(retention.NetworkCompactRetentionError, match="artifact_referenced"):
        await retention._walk_references(connection, {("original_facts", contributions.CONTRIBUTION_TABLE)}, CONTROL)
    dependencies.assert_awaited_once_with(connection, fixture.candidate, CONTROL)
    assert "eligible" not in connection.fetch.await_args.args[0]


@pytest.mark.parametrize("damage", ["oversized", "own_namespace"])
async def test_walk_refuses_unread_metadata_and_generation_namespace(monkeypatch, damage):
    fixture = _mixed_reference()
    fixture.candidate["bounded"] = damage != "oversized"
    connection = SimpleNamespace(fetch=AsyncMock(return_value=[fixture.candidate]))
    dependencies = AsyncMock()
    monkeypatch.setattr(retention, "_candidate_dependencies", dependencies)
    schema = fixture.candidate["schema_name"] if damage == "own_namespace" else "other_generation"
    with pytest.raises(retention.NetworkCompactRetentionError):
        await retention._walk_references(connection, {(schema, contributions.CONTRIBUTION_TABLE)}, CONTROL)
    dependencies.assert_not_awaited()
