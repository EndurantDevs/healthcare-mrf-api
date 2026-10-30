# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Current selection metadata remains distinct from separately authenticated carry; no native proof."""

from dataclasses import replace
from types import SimpleNamespace

import pytest

from process import registry_source_selection_receipt as selection
from process.registry_fhir_serving_preservation import fhir_serving_preservation_sha256
from process.registry_source_inventory_contract import make_source_inventory_entry, source_inventory_root
from process.registry_source_inventory_reference import INVENTORY_DIGEST_KEY
from process.registry_source_recipe_store import canonical_registry_source_recipes
from process.registry_source_retention import registry_source_retention_sha256
from tests.test_registry_fhir_serving_preservation import receipt as preservation_receipt
from tests.test_registry_fhir_serving_preservation import recipe as fhir_recipe
from tests.test_registry_source_inventory_selection import APPROVED, OneShot, _candidate, _inventory, _stage


@pytest.mark.parametrize("count", [2, 101, 1000])
def test_selected_current_stage_keeps_exact_inventory_commitment_when_heap_contains_carry(count):
    root, recipes = _inventory(count)
    current = _stage(root)
    combined = replace(
        current,
        membership_rows=current.membership_rows + 3,
        generation_sha256="f" * 64,
        selected_stage=current,
        preservation=object(),
    )
    before = selection.build_registry_source_selection_receipt(OneShot(recipes), APPROVED, current, inventory_root=root)
    after = selection.build_registry_source_selection_receipt(OneShot(recipes), APPROVED, combined, inventory_root=root)
    assert after == before
    assert after["mapped_rows"] == current.membership_rows
    assert after["stage_generation_sha256"] == current.generation_sha256
    assert after["inventory_entries_sha256"] == root.entries_sha256


@pytest.mark.parametrize("damage", ["nested", "wrong_table", "wrong_type", "bad_current_hash"])
def test_selected_stage_refuses_nested_or_substituted_current_identity(damage):
    root, recipes = _inventory(2)
    current = _stage(root)
    selected = replace(current, selected_stage=current) if damage == "nested" else current
    if damage == "wrong_type":
        selected = SimpleNamespace(**vars(current))
    if damage == "bad_current_hash":
        selected = replace(current, generation_sha256="f" * 64)
    combined = replace(
        current,
        selected_stage=selected,
        membership_rows=6,
        table_name="registry_source_page_" + "b" * 32 if damage == "wrong_table" else current.table_name,
    )
    with pytest.raises(selection.RegistrySourceSelectionError):
        selection.build_registry_source_selection_receipt(recipes, APPROVED, combined, inventory_root=root)


def _legacy_with_carry():
    recipes = tuple(
        sorted(
            (fhir_recipe(), fhir_recipe(endpoint="endpoint-two")),
            key=lambda item: canonical_registry_source_recipes((item,)),
        )
    )
    root = source_inventory_root(make_source_inventory_entry(index, recipe) for index, recipe in enumerate(recipes, 1))
    current = _stage(root)
    candidate = _candidate(root, current)
    candidate["expected_head"] = 2
    candidate["source_recipes_json"] = canonical_registry_source_recipes(recipes)
    candidate["source_generations"].pop(INVENTORY_DIGEST_KEY)
    receipt = selection.build_registry_source_selection_receipt(recipes, APPROVED, current)
    proof = preservation_receipt() | {
        "approved_revision": APPROVED.approved_revision,
        "current_recipes_sha256": root.recipes_sha256,
    }
    candidate["validation_json"].update({selection.SUMMARY_KEY: receipt, proof["component"]: proof})
    candidate["source_generations"][proof["component"]] = fhir_serving_preservation_sha256(proof)
    count = current.membership_rows + proof["carried_rows"]
    retention_by_field = {
        "component": "registry_source_retention",
        "revision": 1,
        "table_name": "network_membership_source",
        "source_generation_sha256": "f" * 64,
        "source_rows": count,
        "add_rows": APPROVED.total_rows,
        "remove_rows": 0,
        "suppressed_source_rows": 0,
        "effective_rows": count + APPROVED.total_rows,
    }
    candidate["expected_rows"] = retention_by_field["effective_rows"]
    candidate["validation_json"][retention_by_field["component"]] = retention_by_field
    candidate["source_generations"][retention_by_field["component"]] = registry_source_retention_sha256(
        retention_by_field
    )
    return candidate, recipes, receipt, proof


def test_selection_verifier_counts_current_plus_closed_carry_without_relabeling_current_metadata():
    candidate, recipes, receipt, proof = _legacy_with_carry()
    assert selection.verify_registry_source_selection_receipt(candidate) == receipt
    assert selection._registered_source_rows(candidate, receipt) == receipt["mapped_rows"] + proof["carried_rows"]
    assert selection._is_candidate_replay(candidate, recipes, receipt, APPROVED) is True


@pytest.mark.parametrize("damage", ["missing_proof", "bad_proof_hash", "current_only_retention"])
def test_selection_verifier_refuses_unpaired_or_missing_carried_rows(damage):
    candidate, _recipes, _receipt, proof = _legacy_with_carry()
    if damage == "missing_proof":
        candidate["validation_json"].pop(proof["component"])
    elif damage == "bad_proof_hash":
        candidate["source_generations"][proof["component"]] = "0" * 64
    else:
        retention_by_field = candidate["validation_json"]["registry_source_retention"]
        retention_by_field["source_rows"] -= proof["carried_rows"]
        retention_by_field["effective_rows"] -= proof["carried_rows"]
        candidate["expected_rows"] = retention_by_field["effective_rows"]
        candidate["source_generations"]["registry_source_retention"] = registry_source_retention_sha256(
            retention_by_field
        )
    with pytest.raises(selection.RegistrySourceSelectionError):
        selection.verify_registry_source_selection_receipt(candidate)
