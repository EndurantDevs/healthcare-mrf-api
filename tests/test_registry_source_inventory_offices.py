# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Office summary metadata is bound to its inventory without granting source custody."""

import copy
import json

import pytest

from process import registry_initial_source_composition as offices
from process import registry_source_recipe_store as recipes
from process.registry_source_inventory_reference import INVENTORY_DIGEST_KEY, SourceInventoryReference
from process.registry_source_inventory_summaries import office_summary_v4
from tests.test_registry_source_inventory_summaries import _ADDRESS, _APPROVED, _source_inventory


def _candidate(root, receipt):
    return {
        "source_recipes_json": SourceInventoryReference(root).as_dict(),
        "approved_custom_revision": _APPROVED.approved_revision,
        "source_generations": {
            INVENTORY_DIGEST_KEY: root.entries_sha256,
            recipes.RECIPE_DIGEST_KEY: root.recipes_sha256,
            "custom_membership": _APPROVED.generation_id,
            "unified_address": _ADDRESS.generation_id,
            offices.SUMMARY_KEY: offices.initial_source_office_receipt_sha256(receipt, inventory_root=root),
        },
        "validation_json": {offices.SUMMARY_KEY: receipt},
    }


@pytest.fixture(scope="module")
def sample_offices():
    root, _, derivations = _source_inventory(2)
    receipt = office_summary_v4(root, _APPROVED, _ADDRESS, iter(derivations), None)
    return root, receipt


@pytest.mark.parametrize("count", [1, 101, 1000])
def test_office_metadata_verification_does_not_hydrate_inventory(count, monkeypatch):
    root, _, derivations = _source_inventory(count)
    receipt = office_summary_v4(root, _APPROVED, _ADDRESS, iter(derivations), None)
    candidate_by_field = _candidate(root, receipt)

    def forbidden_payload(*_arguments, **_options):
        pytest.fail("Office metadata validation hydrated a referenced inventory")

    monkeypatch.setattr(recipes, "decode_registry_source_recipes", forbidden_payload)
    assert offices.verify_initial_source_office_receipt(candidate_by_field) == receipt
    encoded_by_field = {
        field: json.dumps(value) if field != "approved_custom_revision" else value
        for field, value in candidate_by_field.items()
    }
    assert offices.verify_initial_source_office_receipt(encoded_by_field) == receipt


@pytest.mark.parametrize(
    "field",
    ["registry_source_recipes", "custom_membership", "unified_address", offices.SUMMARY_KEY],
)
def test_office_metadata_rejects_registration_drift(field, sample_offices):
    root, receipt = sample_offices
    candidate = _candidate(root, receipt)
    candidate["source_generations"][field] = "0" * 64
    with pytest.raises(offices.RegistryInitialSourceError):
        offices.verify_initial_source_office_receipt(candidate)


@pytest.mark.parametrize("field", ["inventory_entries_sha256", "recipe_sha256", "derivation_count", "recipe_count"])
def test_office_summary_rejects_inventory_drift(field, sample_offices):
    root, receipt = sample_offices
    changed_by_field = {**receipt, field: receipt[field] + 1 if type(receipt[field]) is int else "0" * 64}
    with pytest.raises(offices.RegistryInitialSourceError):
        offices.validate_initial_source_office_receipt(changed_by_field, inventory_root=root)


def test_office_summary_versions_cannot_cross_storage_formats(sample_offices):
    root, receipt = sample_offices
    with pytest.raises(offices.RegistryInitialSourceError):
        offices.validate_initial_source_office_receipt(receipt)
    with pytest.raises(offices.RegistryInitialSourceError):
        offices.validate_initial_source_office_receipt({**receipt, "revision": 3}, inventory_root=root)
    candidate = _candidate(root, receipt)
    candidate["source_recipes_json"] = []
    candidate["source_generations"].pop(recipes.RECIPE_DIGEST_KEY)
    with pytest.raises(offices.RegistryInitialSourceError):
        offices.verify_initial_source_office_receipt(candidate)


def test_office_metadata_requires_both_registered_halves(sample_offices):
    root, receipt = sample_offices
    for field in ("source_generations", "validation_json"):
        candidate = _candidate(root, receipt)
        candidate[field].pop(offices.SUMMARY_KEY)
        with pytest.raises(offices.RegistryInitialSourceError):
            offices.verify_initial_source_office_receipt(candidate)


def test_office_metadata_preserves_approval_and_retained_origin_checks(sample_offices):
    root, receipt = sample_offices
    candidate = _candidate(root, receipt)
    candidate["approved_custom_revision"] += 1
    with pytest.raises(offices.RegistryInitialSourceError):
        offices.verify_initial_source_office_receipt(candidate)
    origin_receipt_by_field = {**receipt, "origin_manifest_sha256": "d" * 64}
    candidate = _candidate(root, origin_receipt_by_field)
    with pytest.raises(offices.RegistryInitialSourceError):
        offices.verify_initial_source_office_receipt(candidate)
    candidate["source_generations"]["retained_network_source"] = "d" * 64
    assert offices.verify_initial_source_office_receipt(candidate) == origin_receipt_by_field


def test_office_metadata_rejects_duplicate_json_fields(sample_offices):
    root, receipt = sample_offices
    candidate = copy.deepcopy(_candidate(root, receipt))
    encoded = json.dumps(receipt)[:-1] + ',"revision":4}'
    candidate["validation_json"] = '{"' + offices.SUMMARY_KEY + '":' + encoded + "}"
    with pytest.raises(offices.RegistryInitialSourceError):
        offices.verify_initial_source_office_receipt(candidate)
