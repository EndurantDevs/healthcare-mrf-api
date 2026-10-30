# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Metadata-only contribution dispatch never hydrates inventories or admits SOURCE."""

import copy
import hashlib
import json
from dataclasses import replace

import pytest

from process import registry_source_contribution_store as contributions
from process import registry_source_inventory_contract as inventory
from process import registry_source_inventory_reference as references
from process import registry_source_inventory_summaries as summary_module
from process import registry_source_recipe_store as recipes
from process.registry_source_inventory_summaries import contribution_summary_v3
from tests import test_registry_source_contributions as legacy_fixtures
from tests.test_registry_source_recipe_store_postgres import _example


def _inventory(count, *, contributions_present=True):
    source_recipes = [_example(index) for index in range(count)]
    source_recipes.sort(key=lambda recipe: recipes.canonical_registry_source_recipes((recipe,)))
    descriptors = [
        contributions.locally_owned_source_descriptor(legacy_fixtures._descriptor(recipe)) for recipe in source_recipes
    ]
    entries = (
        inventory.make_source_inventory_entry(
            ordinal, recipe, contribution=descriptor if contributions_present else None
        )
        for ordinal, (recipe, descriptor) in enumerate(zip(source_recipes, descriptors), 1)
    )
    root = inventory.source_inventory_root(entries)
    return root, sorted(descriptors, key=lambda descriptor: descriptor["recipe_sha256"]), source_recipes


def _candidate(root, descriptors=None):
    generations = {recipes.RECIPE_DIGEST_KEY: root.recipes_sha256} if root.entry_count else {}
    generations[references.INVENTORY_DIGEST_KEY] = root.entries_sha256
    validation_by_name = {}
    if descriptors is not None:
        receipt = contribution_summary_v3(root, iter(descriptors))
        validation_by_name[contributions.SUMMARY_KEY] = receipt
        generations[contributions.SUMMARY_KEY] = contributions.registry_source_contribution_sha256(
            receipt, inventory_root=root
        )
    return {
        "source_recipes_json": references.SourceInventoryReference(root).as_dict(),
        "source_generations": generations,
        "validation_json": validation_by_name,
    }


@pytest.fixture(scope="module")
def sample_inventory():
    return _inventory(1)


@pytest.mark.parametrize("count", [1, 101, 1000])
@pytest.mark.parametrize("encoded", [False, True])
def test_reference_receipt_binds_large_inventory_without_payload_hydration(count, encoded, monkeypatch):
    root, descriptors, _ = _inventory(count)
    candidate_by_field = _candidate(root, descriptors)
    expected = candidate_by_field["validation_json"][contributions.SUMMARY_KEY]
    if encoded:
        candidate_by_field = {field: json.dumps(value) for field, value in candidate_by_field.items()}

    def forbidden_payload(*_arguments, **_options):
        pytest.fail("Metadata dispatch attempted to hydrate source recipes")

    monkeypatch.setattr(recipes, "decode_registry_source_recipes", forbidden_payload)
    assert contributions.verify_registry_source_contributions(candidate_by_field) == expected
    assert "sources" not in expected and expected["source_count"] == count
    assert len(contributions._canonical(expected).encode()) < 1024
    assert (
        contributions.registry_source_contribution_sha256(expected, inventory_root=root)
        == hashlib.sha256(contributions._canonical(expected).encode()).hexdigest()
    )


@pytest.mark.parametrize("missing", ["receipt", "marker", "both"])
def test_reference_with_contributions_requires_both_registered_halves(missing, sample_inventory):
    root, descriptors, _ = sample_inventory
    candidate = _candidate(root, descriptors)
    if missing in {"receipt", "both"}:
        candidate["validation_json"].pop(contributions.SUMMARY_KEY)
    if missing in {"marker", "both"}:
        candidate["source_generations"].pop(contributions.SUMMARY_KEY)
    with pytest.raises(contributions.RegistrySourceContributionError):
        contributions.verify_registry_source_contributions(candidate)


@pytest.mark.parametrize("count", [0, 1])
def test_zero_contribution_reference_allows_neither_or_coherent_zero_pair(count):
    root, _, _ = _inventory(count, contributions_present=False)
    assert contributions.verify_registry_source_contributions(_candidate(root)) is None
    paired = _candidate(root, ())
    assert contributions.verify_registry_source_contributions(paired)["source_count"] == 0
    for missing in ("source_generations", "validation_json"):
        partial = copy.deepcopy(paired)
        partial[missing].pop(contributions.SUMMARY_KEY)
        with pytest.raises(contributions.RegistrySourceContributionError):
            contributions.verify_registry_source_contributions(partial)


@pytest.mark.parametrize(
    "damage", ["recipe_digest", "contribution_digest", "root_digest", "root_count", "receipt_root"]
)
def test_reference_or_registration_substitution_refuses(damage, sample_inventory):
    root, descriptors, _ = sample_inventory
    candidate = _candidate(root, descriptors)
    if damage in {"recipe_digest", "contribution_digest"}:
        candidate["source_generations"][
            recipes.RECIPE_DIGEST_KEY if damage == "recipe_digest" else contributions.SUMMARY_KEY
        ] = "f" * 64
    elif damage == "root_digest":
        candidate["source_recipes_json"]["root"]["entries_sha256"] = "f" * 64
    elif damage == "root_count":
        candidate["source_recipes_json"]["root"].update(entry_count=2, contribution_count=2)
    else:
        candidate["validation_json"][contributions.SUMMARY_KEY]["inventory_entries_sha256"] = "f" * 64
    with pytest.raises(contributions.RegistrySourceContributionError):
        contributions.verify_registry_source_contributions(candidate)


@pytest.mark.parametrize("revision", [1, 2])
def test_explicit_inventory_root_refuses_legacy_descriptor_receipts(revision, sample_inventory):
    root, descriptors, _ = sample_inventory
    descriptor_by_field = descriptors[0]
    if revision == 1:
        descriptor_by_field = {
            field: descriptor_value
            for field, descriptor_value in descriptor_by_field.items()
            if field not in contributions.FACT_ANCHOR_FIELDS
        }
    legacy_receipt_by_field = {
        "component": contributions.SUMMARY_KEY,
        "revision": revision,
        "table_name": contributions.CONTRIBUTION_TABLE,
        "sources": [descriptor_by_field],
    }
    assert (
        contributions.validate_registry_source_contribution_receipt(legacy_receipt_by_field) == legacy_receipt_by_field
    )
    for operation in (
        contributions.validate_registry_source_contribution_receipt,
        contributions.registry_source_contribution_sha256,
    ):
        with pytest.raises(contributions.RegistrySourceContributionError):
            operation(legacy_receipt_by_field, inventory_root=root)
    candidate = _candidate(root, descriptors)
    candidate["validation_json"][contributions.SUMMARY_KEY] = legacy_receipt_by_field
    candidate["source_generations"][contributions.SUMMARY_KEY] = contributions.registry_source_contribution_sha256(
        legacy_receipt_by_field
    )
    with pytest.raises(contributions.RegistrySourceContributionError):
        contributions.verify_registry_source_contributions(candidate)


def test_new_summary_requires_explicit_exact_root(sample_inventory):
    root, descriptors, _ = sample_inventory
    receipt = contribution_summary_v3(root, iter(descriptors))
    for operation in (
        contributions.validate_registry_source_contribution_receipt,
        contributions.registry_source_contribution_sha256,
    ):
        with pytest.raises(contributions.RegistrySourceContributionError):
            operation(receipt)
        for substituted in ({}, replace(root, entries_sha256="f" * 64)):
            with pytest.raises(contributions.RegistrySourceContributionError):
                operation(receipt, inventory_root=substituted)


@pytest.mark.parametrize("reference", [{}, {"component": "unknown"}, "{invalid", '{"root":{},"root":{}}'])
def test_object_recipe_metadata_never_falls_back_to_legacy_none(reference):
    candidate_by_field = {"source_recipes_json": reference, "source_generations": {}, "validation_json": {}}
    with pytest.raises(contributions.RegistrySourceContributionError):
        contributions.verify_registry_source_contributions(candidate_by_field)


@pytest.mark.parametrize("field", ["source_recipes_json", "source_generations", "validation_json"])
def test_raw_string_bound_precedes_json_or_whitespace_work(field, monkeypatch, sample_inventory):
    root, descriptors, _ = sample_inventory
    candidate = _candidate(root, descriptors)
    candidate[field] = " " * (recipes.MAX_SOURCE_RECIPE_INPUT_BYTES + 1) + "{}"

    def forbidden_decode(*_arguments, **_options):
        pytest.fail("Oversized metadata reached JSON decoding")

    monkeypatch.setattr(contributions.json, "loads", forbidden_decode)
    monkeypatch.setattr(recipes, "_decode_document", forbidden_decode)
    with pytest.raises(contributions.RegistrySourceContributionError):
        contributions.verify_registry_source_contributions(candidate)


def test_reference_validation_json_rejects_nested_duplicate_receipt_keys(sample_inventory):
    root, descriptors, _ = sample_inventory
    candidate = _candidate(root, descriptors)
    receipt = candidate["validation_json"][contributions.SUMMARY_KEY]
    encoded = contributions._canonical(receipt)[:-1] + ',"revision":3}'
    candidate["validation_json"] = '{"' + contributions.SUMMARY_KEY + '":' + encoded + "}"
    with pytest.raises(contributions.RegistrySourceContributionError):
        contributions.verify_registry_source_contributions(candidate)


@pytest.mark.parametrize(
    "operation", ["validate_registry_source_contribution_receipt", "registry_source_contribution_sha256"]
)
def test_explicit_root_bounds_raw_string_before_summary_parser(operation, monkeypatch, sample_inventory):
    root, _, _ = sample_inventory

    def forbidden_summary(*_arguments, **_options):
        pytest.fail("Unbounded raw string reached the summary parser")

    monkeypatch.setattr(summary_module, "validate_contribution_summary_v3", forbidden_summary)
    with pytest.raises(contributions.RegistrySourceContributionError):
        getattr(contributions, operation)(" " * (contributions.MAX_RECEIPT_BYTES + 1), inventory_root=root)


def test_legacy_no_receipt_none_does_not_newly_validate_recipes(monkeypatch):
    def forbidden_metadata(*_arguments, **_options):
        pytest.fail("Legacy no-receipt path acquired new metadata validation")

    monkeypatch.setattr(references, "verify_source_recipe_metadata", forbidden_metadata)
    for source in (None, "[]", "[malformed", []):
        candidate_by_field = {"source_recipes_json": source, "source_generations": {}, "validation_json": {}}
        assert contributions.verify_registry_source_contributions(candidate_by_field) is None


@pytest.mark.parametrize("revision", [1, 2])
def test_legacy_hundred_sources_and_canonical_hash_are_unchanged(revision):
    _, descriptors, source_recipes = _inventory(100)
    if revision == 1:
        descriptors = [
            {field: value for field, value in descriptor.items() if field not in contributions.FACT_ANCHOR_FIELDS}
            for descriptor in descriptors
        ]
    receipt_by_field = {
        "component": contributions.SUMMARY_KEY,
        "revision": revision,
        "table_name": contributions.CONTRIBUTION_TABLE,
        "sources": descriptors,
    }
    expected_digest = hashlib.sha256(contributions._canonical(receipt_by_field).encode()).hexdigest()
    assert contributions.registry_source_contribution_sha256(receipt_by_field) == expected_digest
    canonical_recipes = recipes.canonical_registry_source_recipes(tuple(source_recipes))
    candidate_by_field = {
        "source_recipes_json": canonical_recipes,
        "source_generations": {
            recipes.RECIPE_DIGEST_KEY: recipes.registry_source_recipes_sha256(canonical_recipes),
            contributions.SUMMARY_KEY: expected_digest,
        },
        "validation_json": {contributions.SUMMARY_KEY: receipt_by_field},
    }
    assert contributions.verify_registry_source_contributions(candidate_by_field) == receipt_by_field
    with pytest.raises(contributions.RegistrySourceContributionError):
        contributions.validate_registry_source_contribution_receipt(
            {**receipt_by_field, "sources": descriptors + [descriptors[-1]]}
        )


@pytest.mark.parametrize("count", [1, 101])
def test_candidate_registration_binds_inventory_contribution_summary(count):
    from process.network_membership_candidate_lifecycle import (
        _candidate_source_metadata,
        _contribution_metadata,
        _verify_candidate_metadata,
    )

    root, descriptors, _ = _inventory(count)
    generations, reference = _candidate_source_metadata({}, (), references.SourceInventoryReference(root))
    metadata_by_field = {
        "source_generations": generations,
        "source_recipes_json": reference,
        "validation_json": None,
        "approved_custom_revision": 0,
        "expected_head": 0,
        "expected_rows": 0,
    }
    with pytest.raises(contributions.RegistrySourceContributionError):
        _contribution_metadata(dict(metadata_by_field), None)
    receipt = contribution_summary_v3(root, iter(descriptors))
    registered_by_field = _contribution_metadata(dict(metadata_by_field), receipt)
    assert contributions.verify_registry_source_contributions(registered_by_field) == receipt
    _verify_candidate_metadata(registered_by_field, registered_by_field)
