# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Mapping metadata preserves raw formats and proves no Source or native custody."""

import copy
import hashlib
import json
from dataclasses import asdict
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import registry_source_contribution_artifacts as artifacts
from process import registry_source_contribution_store as contributions
from process import registry_source_inventory_contract as inventory
from process import registry_source_inventory_preparation as preparation
from process import registry_source_inventory_store as inventory_store
from process import registry_source_inventory_summaries as summaries
from process.network_fhir_membership_source import FHIR_SOURCE_WITNESS_TRANSFORM_V2
from process.registry_source_inventory_reference import INVENTORY_DIGEST_KEY, SourceInventoryReference
from process.registry_source_recipe_store import RECIPE_DIGEST_KEY, canonical_registry_source_recipes
from tests import test_verified_inventory_v2 as witness_metadata
from tests.test_registry_source_inventory_fact_owners import _owner
from tests.test_registry_source_inventory_preparation import _derivation as legacy_derivation
from tests.test_registry_source_inventory_preparation import _descriptor as legacy_descriptor
from tests.test_registry_source_inventory_summaries import OneShot
from tests.test_registry_source_recipe_store_postgres import _example


def _mapping(recipe=None, *, witness=True, **changes):
    recipe = recipe or witness_metadata.rows()[0]
    identity = contributions.verified_office_fact_identity if witness else contributions.recipe_contribution_identity
    recipe_sha256, capture_sha256 = identity(recipe)
    transform = FHIR_SOURCE_WITNESS_TRANSFORM_V2 if witness else contributions.fhir_fact_transform(recipe.source_pin)
    descriptor_by_field = {
        **dict.fromkeys(contributions._MAPPING_COUNTERS_V3, 0),
        **dict.fromkeys(contributions.FACT_ANCHOR_FIELDS),
        "descriptor_version": 3,
        "transform_version": transform,
        "recipe_sha256": recipe_sha256,
        "capture_sha256": capture_sha256,
        "mapping_sha256": "c" * 64,
        "fact_sha256": "d" * 64,
        "fact_rows": 2 if witness else 1,
        "membership_rows": 2,
        "raw_source_rows": 1,
        "accepted_raw_source_rows": 1,
        "accepted_input_rows": 1,
        "covered_network_ids": [7, 11],
        **changes,
    }
    descriptor_by_field["source_generation"] = contributions.source_mapping_generation_v3(
        descriptor_by_field["capture_sha256"],
        descriptor_by_field["mapping_sha256"],
        descriptor_by_field["covered_network_ids"],
    )
    return descriptor_by_field


def _inventory(descriptors, recipes):
    entries = [(recipe, descriptor) for recipe, descriptor in zip(recipes, descriptors, strict=True)]
    entries.sort(key=lambda pair: canonical_registry_source_recipes((pair[0],)))
    return inventory.source_inventory_root(
        inventory.make_source_inventory_entry(ordinal, recipe, contribution=descriptor)
        for ordinal, (recipe, descriptor) in enumerate(entries, 1)
    )


@pytest.mark.parametrize("kind", ["legacy", "generic", "witness"])
def test_raw_formats_keep_distinct_input_units(kind):
    recipe = _example() if kind == "legacy" else witness_metadata.rows()[0]
    descriptor = _mapping(recipe, witness=kind == "witness")
    checked = contributions.validate_source_mapping_descriptor_v3(json.dumps(descriptor))
    assert checked == descriptor and checked["membership_rows"] > checked["accepted_input_rows"]
    assert checked["fact_rows"] - checked["raw_source_rows"] == (1 if kind == "witness" else 0)
    assert contributions.validate_source_contribution_descriptor(descriptor) == descriptor


@pytest.mark.parametrize("provider_only", [False, True])
def test_duplicate_witnesses_deduplicate_only_output(provider_only):
    descriptor = _mapping(
        fact_rows=3,
        accepted_input_rows=2,
        covered_network_ids=[7],
        membership_rows=0 if provider_only else 1,
        provider_only_rows=int(provider_only),
        provider_only_evidence_rows=int(provider_only),
    )
    assert contributions.validate_source_mapping_descriptor_v3(descriptor) == descriptor


def test_empty_products_retain_explicit_coverage():
    descriptor = _mapping(**dict.fromkeys(contributions._MAPPING_COUNTERS_V3, 0), covered_network_ids=[7])
    assert contributions.validate_source_mapping_descriptor_v3(descriptor)["covered_network_ids"] == [7]
    descriptor["covered_network_ids"] = []
    with pytest.raises(contributions.RegistrySourceContributionError):
        contributions.validate_source_mapping_descriptor_v3(descriptor)


def test_legacy_omissions_use_ignored_input_count():
    descriptor = _mapping(
        _example(),
        witness=False,
        accepted_input_rows=0,
        membership_rows=0,
        ignored_input_rows=1,
        accepted_raw_source_rows=0,
        omitted_raw_source_rows=1,
    )
    assert contributions.validate_source_mapping_descriptor_v3(descriptor)["ignored_input_rows"] == 1


@pytest.mark.parametrize("counter", contributions._MAPPING_COUNTERS_V3)
@pytest.mark.parametrize("invalid", [True, -1, 2**63])
def test_mapping_counters_are_bounded_integers(counter, invalid):
    descriptor = _mapping()
    descriptor[counter] = invalid
    with pytest.raises(contributions.RegistrySourceContributionError):
        contributions.validate_source_mapping_descriptor_v3(descriptor)


@pytest.mark.parametrize("coverage", [None, (), [True], [0], [-1], [2**31], ["7"], [7, 7], [11, 7]])
def test_coverage_rejects_malformed_id_sets(coverage):
    descriptor = _mapping()
    descriptor["covered_network_ids"] = coverage
    with pytest.raises(contributions.RegistrySourceContributionError):
        contributions.validate_source_mapping_descriptor_v3(descriptor)


@pytest.mark.parametrize(
    "damage",
    [
        {"descriptor_version": True},
        {"descriptor_version": 4},
        {"transform_version": "unknown"},
        {"capture_sha256": "f" * 64},
        {"source_generation": "f" * 64},
        {"source_grant": True},
        {"fact_table_oid": 42},
        {"accepted_input_rows": 2},
        {"ignored_input_rows": 1},
        {"unresolved_rows": 1},
        {"raw_source_rows": 2},
        {"membership_rows": 3},
        {"membership_rows": 0},
        {"provider_only_evidence_rows": 1},
        {"provider_only_rows": 1},
    ],
)
def test_mapping_refuses_inconsistent_claims(damage):
    with pytest.raises(contributions.RegistrySourceContributionError):
        contributions.validate_source_mapping_descriptor_v3({**_mapping(), **damage})


def test_legacy_cannot_invent_unresolved_evidence():
    descriptor = _mapping(
        _example(),
        witness=False,
        fact_rows=2,
        unresolved_rows=1,
        accepted_raw_source_rows=0,
        unresolved_raw_source_rows=1,
    )
    with pytest.raises(contributions.RegistrySourceContributionError):
        contributions.validate_source_mapping_descriptor_v3(descriptor)


def test_coverage_and_descriptor_byte_limits(monkeypatch):
    descriptor = _mapping(covered_network_ids=list(range(1, 302)))
    assert contributions.validate_source_mapping_descriptor_v3(descriptor) == descriptor
    monkeypatch.setattr(contributions, "MAX_RECEIPT_BYTES", len(contributions._canonical(descriptor).encode()) - 1)
    with pytest.raises(contributions.RegistrySourceContributionError):
        contributions.validate_source_mapping_descriptor_v3(descriptor)
    monkeypatch.setattr(contributions, "MAX_RECEIPT_BYTES", 524288)
    with pytest.raises(contributions.RegistrySourceContributionError):
        contributions.source_mapping_generation_v3("a" * 64, "b" * 64, list(range(2_000_000_000, 2_000_050_000)))


def test_duplicate_json_keys_and_mutation_are_refused():
    descriptor = _mapping()
    encoded = contributions._canonical(descriptor)
    with pytest.raises(contributions.RegistrySourceContributionError):
        contributions.validate_source_mapping_descriptor_v3(encoded[:-1] + ',"descriptor_version":3}')
    checked = contributions.validate_source_mapping_descriptor_v3(descriptor)
    checked["covered_network_ids"].append(12)
    assert descriptor["covered_network_ids"] == [7, 11]


@pytest.mark.parametrize("version", [True, 0, 1, 4])
def test_descriptor_dispatch_refuses_unknown_versions(version):
    descriptor_by_field = {**_mapping(), "descriptor_version": version}
    with pytest.raises(contributions.RegistrySourceContributionError):
        contributions.validate_source_contribution_descriptor(descriptor_by_field)


def test_legacy_and_v2_roundtrip_bytes_stay_exact():
    recipe, witness, _ = witness_metadata.rows()
    legacy = legacy_descriptor(recipe)
    for descriptor in (witness, legacy):
        encoded = contributions._canonical(descriptor)
        checked = contributions.validate_source_contribution_descriptor(encoded)
        assert contributions._canonical(checked) == encoded
        entry = inventory.make_source_inventory_entry(1, recipe, contribution=descriptor)
        assert entry.contribution_json == encoded
    with pytest.raises(contributions.RegistrySourceContributionError):
        contributions.validate_source_witness_descriptor_v2(_mapping())


def test_mixed_raw_formats_have_exact_summary_inputs():
    recipes = [_example(), witness_metadata.rows()[0]]
    descriptors = [_mapping(recipes[0], witness=False), _mapping(recipes[1])]
    root = _inventory(descriptors, recipes)
    ordered = sorted(descriptors, key=lambda descriptor: descriptor["recipe_sha256"])
    summary = summaries.contribution_summary_v5(root, OneShot(ordered))
    assert summary["input_rows"] == summary["accepted_input_rows"] == 2
    assert summary["fact_rows"] - summary["raw_source_rows"] == 1
    assert summary["membership_rows"] == 4 and "transform_version" not in summary
    assert "covered_network_ids" not in summary
    assert summary["sources_sha256"] == hashlib.sha256(contributions._canonical(ordered).encode()).hexdigest()
    assert contributions.validate_registry_source_contribution_receipt(summary, inventory_root=root) == summary


def test_summary_refuses_old_children_and_poisoned_finish():
    recipe, descriptor, _ = witness_metadata.rows()
    root = _inventory([descriptor], [recipe])
    accumulator = summaries.MappingContributionSummaryAccumulator(root)
    with pytest.raises(inventory.RegistrySourceInventoryError):
        accumulator.accept(descriptor)
    with pytest.raises(inventory.RegistrySourceInventoryError):
        accumulator.finish()
    assert summaries.contribution_summary_v4(root, [descriptor])["descriptor_version"] == 2


@pytest.mark.parametrize(
    "damage",
    [
        {"revision": True},
        {"descriptor_version": 2},
        {"input_rows": 0},
        {"fact_rows": 0},
        {"source_count": 0},
        {"ignored_input_rows": 1},
        {"unresolved_raw_source_rows": 1},
        {"membership_rows": 0},
        {"covered_network_ids": [7]},
        {"fact_rows": 3, "raw_source_rows": 2, "accepted_raw_source_rows": 2},
        {
            "fact_rows": 4,
            "raw_source_rows": 2,
            "accepted_raw_source_rows": 2,
            "input_rows": 2,
            "accepted_input_rows": 2,
            "membership_rows": 1,
        },
        {
            "fact_rows": 6,
            "raw_source_rows": 2,
            "accepted_raw_source_rows": 0,
            "unresolved_raw_source_rows": 2,
            "input_rows": 4,
            "accepted_input_rows": 2,
            "unresolved_rows": 2,
            "membership_rows": 0,
            "provider_only_rows": 2,
            "provider_only_evidence_rows": 1,
        },
    ],
)
def test_summary_rejects_inconsistent_metadata(damage):
    recipe = witness_metadata.rows()[0]
    descriptor = _mapping(recipe)
    root = _inventory([descriptor], [recipe])
    summary = summaries.contribution_summary_v5(root, [descriptor])
    with pytest.raises(inventory.RegistrySourceInventoryError):
        summaries.validate_contribution_summary_v5({**summary, **damage}, root)


def test_summary_is_closed_defensive_and_complete():
    recipe = witness_metadata.rows()[0]
    descriptor = _mapping(recipe)
    root = _inventory([descriptor], [recipe])
    accumulator = summaries.MappingContributionSummaryAccumulator(root)
    with pytest.raises(inventory.RegistrySourceInventoryError):
        accumulator.finish()
    with pytest.raises(inventory.RegistrySourceInventoryError):
        accumulator.accept(descriptor)
    accumulator = summaries.MappingContributionSummaryAccumulator(root)
    accumulator.accept(descriptor)
    expected = accumulator.finish()
    accumulator.finish()["membership_rows"] = 999
    assert accumulator.finish() == expected
    with pytest.raises(inventory.RegistrySourceInventoryError):
        accumulator.accept(descriptor)
    empty = summaries.contribution_summary_v5(inventory.source_inventory_root(()), ())
    assert empty["input_rows"] == 0 and empty["sources_sha256"] == hashlib.sha256(b"[]").hexdigest()


def test_aggregate_bigint_overflow_poison_is_terminal():
    recipes = [_example(index) for index in range(2)]
    descriptors = [
        _mapping(recipe, witness=False, fact_rows=2**63 - 1, accepted_input_rows=2**63 - 1, membership_rows=1)
        for recipe in recipes
    ]
    root = _inventory(descriptors, recipes)
    accumulator = summaries.MappingContributionSummaryAccumulator(root)
    ordered = sorted(descriptors, key=lambda descriptor: descriptor["recipe_sha256"])
    accumulator.accept(ordered[0])
    with pytest.raises(inventory.RegistrySourceInventoryError):
        accumulator.accept(ordered[1])
    with pytest.raises(inventory.RegistrySourceInventoryError):
        accumulator.finish()


@pytest.mark.parametrize("order", ["duplicate", "reversed"])
def test_summary_refuses_reordered_source_stream(order):
    recipes = [_example(index) for index in range(2)]
    descriptors = [_mapping(recipe, witness=False) for recipe in recipes]
    root = _inventory(descriptors, recipes)
    ordered = sorted(descriptors, key=lambda descriptor: descriptor["recipe_sha256"])
    accumulator = summaries.MappingContributionSummaryAccumulator(root)
    accumulator.accept(ordered[1] if order == "reversed" else ordered[0])
    with pytest.raises(inventory.RegistrySourceInventoryError):
        accumulator.accept(ordered[0])
    with pytest.raises(inventory.RegistrySourceInventoryError):
        accumulator.finish()


def test_complete_inventory_limit_folds_without_coverage_truncation():
    recipes = [_example(index) for index in range(1000)]
    descriptors = [_mapping(recipe, witness=False) for recipe in recipes]
    root = _inventory(descriptors, recipes)
    ordered = sorted(descriptors, key=lambda descriptor: descriptor["recipe_sha256"])
    summary = summaries.contribution_summary_v5(root, OneShot(ordered))
    assert summary["source_count"] == summary["input_rows"] == 1000
    assert summary["membership_rows"] == 2000


def test_combined_output_count_cannot_overflow_bigint():
    descriptor = _mapping(
        fact_rows=2**63 - 1,
        accepted_input_rows=2**63 - 2,
        membership_rows=2**63 - 1,
        provider_only_rows=1,
        provider_only_evidence_rows=1,
    )
    with pytest.raises(contributions.RegistrySourceContributionError):
        contributions.validate_source_mapping_descriptor_v3(descriptor)


@pytest.mark.parametrize(
    "damage",
    [
        {"accepted_input_rows": 0, "ignored_input_rows": 1, "membership_rows": 0},
        {
            "fact_rows": 3,
            "raw_source_rows": 2,
            "accepted_raw_source_rows": 2,
            "accepted_input_rows": 1,
            "membership_rows": 2,
        },
        {
            "fact_rows": 4,
            "raw_source_rows": 2,
            "accepted_raw_source_rows": 2,
            "accepted_input_rows": 2,
            "membership_rows": 1,
        },
        {
            "fact_rows": 6,
            "raw_source_rows": 2,
            "accepted_raw_source_rows": 0,
            "unresolved_raw_source_rows": 2,
            "accepted_input_rows": 2,
            "unresolved_rows": 2,
            "membership_rows": 0,
            "provider_only_rows": 2,
            "provider_only_evidence_rows": 1,
        },
    ],
)
def test_resource_evidence_cannot_disappear_during_mapping(damage):
    with pytest.raises(contributions.RegistrySourceContributionError):
        contributions.validate_source_mapping_descriptor_v3({**_mapping(), **damage})


@pytest.mark.parametrize("witness", [False, True])
def test_inventory_pairs_the_actual_raw_format(witness):
    recipe = witness_metadata.rows()[0]
    descriptor = _mapping(recipe, witness=witness)
    derivation = witness_metadata.derivation(recipe, descriptor) if witness else legacy_derivation(recipe, descriptor)
    reference, entries = preparation.prepare_source_inventory([recipe], [descriptor], [derivation])
    assert reference.root.contribution_count == 1
    assert json.loads(entries[0].contribution_json) == descriptor
    wrong = legacy_derivation(recipe, descriptor) if witness else witness_metadata.derivation(recipe, descriptor)
    with pytest.raises(inventory.RegistrySourceInventoryError):
        preparation.prepare_source_inventory([recipe], [descriptor], [wrong])


@pytest.mark.parametrize("witness", [False, True])
def test_old_fact_owner_keeps_mapping_v3_metadata(witness):
    if witness:
        recipe, original, _ = witness_metadata.rows()
    else:
        recipe = _example()
        original = legacy_descriptor(recipe)
    original.update(fact_rows=2 if witness else 1, fact_sha256="d" * 64, accepted_raw_source_rows=1)
    if witness:
        original.update(unresolved_rows=0, unresolved_raw_source_rows=0)
    reference = artifacts.SourceFactArtifactReference(str(UUID(int=9)), 42, "f" * 64)
    artifact = artifacts.ResolvedSourceFactArtifact(reference, "synthetic_original", original)
    descriptor = _mapping(recipe, witness=witness)
    anchored = artifacts.anchored_source_fact_descriptor(descriptor, artifact)
    assert anchored == {**descriptor, **asdict(reference)}
    assert artifacts.source_fact_artifact_reference(anchored) == reference
    assert artifacts._validated_descriptor(anchored)["covered_network_ids"] == [7, 11]
    for field in ("fact_sha256", "fact_rows", "capture_sha256"):
        damaged = copy.deepcopy(descriptor)
        damaged[field] = 3 if field == "fact_rows" else "a" * 64
        with pytest.raises(contributions.RegistrySourceContributionError):
            artifacts.anchored_source_fact_descriptor(damaged, artifact)


def test_legacy_owner_cannot_be_relabelled_witness():
    recipe = _example()
    reference = artifacts.SourceFactArtifactReference(str(UUID(int=9)), 42, "f" * 64)
    artifact = artifacts.ResolvedSourceFactArtifact(reference, "synthetic_original", legacy_descriptor(recipe))
    with pytest.raises(contributions.RegistrySourceContributionError, match="identity_changed"):
        artifacts.anchored_source_fact_descriptor(_mapping(recipe), artifact)


@pytest.mark.asyncio
async def test_owner_inventory_authenticates_exact_summary_five(monkeypatch):
    owner = _owner(1)
    descriptor = _mapping(_example(), witness=False)
    entry = inventory.make_source_inventory_entry(1, _example(), contribution=descriptor)
    reference = SourceInventoryReference(inventory.source_inventory_root([entry]))
    summary = summaries.contribution_summary_v5(reference.root, [descriptor])
    candidate = copy.deepcopy(owner.candidate)
    candidate["source_recipes_json"] = reference.as_dict()
    candidate["validation_json"][contributions.SUMMARY_KEY] = summary
    candidate["source_generations"].update(
        {
            RECIPE_DIGEST_KEY: reference.root.recipes_sha256,
            INVENTORY_DIGEST_KEY: reference.root.entries_sha256,
            contributions.SUMMARY_KEY: contributions.registry_source_contribution_sha256(
                summary, inventory_root=reference.root
            ),
        }
    )
    load = AsyncMock(return_value=(reference, (entry,)))
    monkeypatch.setattr(inventory_store, "_load_locked_source_inventory", load)
    actual = await artifacts._owner_sources(object(), candidate, owner.custody, "synthetic_control")
    assert actual == (descriptor,)
    load.assert_awaited_once()
    corrupted_by_field = {**descriptor, "membership_rows": 1}
    load.return_value = (
        reference,
        (inventory.make_source_inventory_entry(1, _example(), contribution=corrupted_by_field),),
    )
    with pytest.raises(contributions.RegistrySourceContributionError, match="inventory_changed"):
        await artifacts._owner_sources(object(), candidate, owner.custody, "synthetic_control")
