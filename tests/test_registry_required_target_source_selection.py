# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Recipe inclusion is separate from directory, lineage and pricing evidence."""

import hashlib
import json
from copy import deepcopy
from dataclasses import asdict
from uuid import UUID

import pytest

from process import registry_required_target_coverage as coverage
from process.network_approved_membership_source import ApprovedMembershipSource
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_fhir_membership_source import PinnedFHIRMembershipSource
from process.network_serving_read import PinnedNetworkServingManifest
from process.registry_required_target_coverage_import import _report_json
from process.registry_source_recipe_composition import RegistrySourceRecipeStage
from process.registry_source_recipe_store import (
    RegistrySourceMembershipRecipe,
    canonical_registry_source_recipes,
    registry_source_recipes_sha256,
    verify_registry_source_recipes,
)
from process.registry_source_selection_receipt import build_registry_source_selection_receipt


def _source_recipe():
    source_pin = PinnedFHIRMembershipSource(
        "source_example",
        "source-example",
        "endpoint-example",
        "dataset-example",
        "a" * 64,
        "release-one",
        123,
        "medical",
        "2026-10-01",
    )
    coordinates = RegistryNetworkSourceCoordinates(
        "fhir",
        source_pin.source_id,
        source_pin.schema_name,
        source_pin.dataset_id,
        "producer-example",
        "edition-one",
    )
    return RegistrySourceMembershipRecipe(source_pin, coordinates)


def _inputs(*, staged=True, lineage="current"):
    """Build genuine immutable recipes and a staged receipt without database work."""
    recipe = _source_recipe()
    source_pin = recipe.source_pin
    recipes = (recipe,)
    canonical = canonical_registry_source_recipes(recipes)
    recipe_sha256 = registry_source_recipes_sha256(canonical)
    approved = ApprovedMembershipSource(4, "b" * 64)
    stage_identity = json.dumps(
        [recipe_sha256, approved.generation_id, [source_pin.generation_id], 1, 0], separators=(",", ":")
    )
    stage = RegistrySourceRecipeStage(
        "registry_source_page_" + "1" * 32,
        1,
        0,
        hashlib.sha256(stage_identity.encode()).hexdigest(),
        (source_pin.generation_id,),
    )
    receipt = build_registry_source_selection_receipt(recipes, approved, stage)
    generations_by_source = {"registry_source_recipes": recipe_sha256, "custom_membership": approved.generation_id}
    if staged:
        generations_by_source["registry_source_membership"] = stage.generation_sha256
    candidate_by_field = {
        "source_recipes_json": canonical,
        "source_generations": generations_by_source,
        "approved_custom_revision": approved.approved_revision,
        "validation_json": {"registry_source_selection": receipt} if staged else {},
        "dataset_id": UUID(int=1),
        "schema_id": UUID(int=2),
        "producer_id": UUID(int=3),
        "writer_closure": "{}",
        "current_head_in_snapshot": 1,
    }
    manifest = PinnedNetworkServingManifest(
        1, str(UUID(int=4)), "serving_example", 1, generations_by_source, 4, "c" * 64, 456
    )
    native_coverage_by_field = {
        "targets": [
            {"target_key": key, "source_status": lineage, "directory_available": True, "priceable": None}
            for key in ("first", "second", "third")
        ],
        "totals": {"ledger_targets": 3, "directory_available": 3, "pricing_evidence_count": None},
    }
    review_by_field = {
        "approved_exact_binding": True,
        "decision": {"resolution_status": "resolved", "source_binding": asdict(recipe.binding_coordinates)},
    }
    target_evidence_rows = [
        {"target_key": key, "review_provenance": deepcopy([review_by_field])} for key in ("first", "second", "third")
    ]
    return manifest, approved, candidate_by_field, native_coverage_by_field, target_evidence_rows


def _report(inputs):
    manifest, approved, candidate_by_field, native_coverage_by_field, target_evidence_rows = inputs
    return coverage._coverage_report(
        manifest,
        {"target_count": 3},
        approved,
        candidate_by_field,
        {"review_references": "[]"},
        {"provenance": json.dumps(target_evidence_rows)},
        native_coverage_by_field,
        verify_registry_source_recipes(candidate_by_field),
    )


def test_all_reviewed_coordinates_selected_once_and_native_results_unchanged():
    inputs = _inputs()
    inputs[-1][0]["review_provenance"] *= 3
    before = deepcopy(inputs[-2])
    report = _report(inputs)
    assessment = report["provenance"]["reviewed_source_selection"]
    assert assessment["totals"] == {"ledger_targets": 3, "selected": 3, "not_selected": 0, "not_assessed": 0}
    assert all(row["reviewed_source_count"] == row["selected_source_count"] == 1 for row in assessment["targets"])
    assert all(row["missing_sources"] == [] for row in assessment["targets"])
    assert report["targets"] == before["targets"] and report["totals"] == before["totals"]


@pytest.mark.parametrize(
    "field", ["source_system", "source_id", "dataset_schema", "dataset_id", "producer_id", "edition_id"]
)
def test_every_coordinate_is_required_and_missing_tuple_is_retained(field):
    inputs = _inputs()
    binding = inputs[-1][0]["review_provenance"][0]["decision"]["source_binding"]
    binding[field] = "aca" if field == "source_system" else "other_example"
    report = _report(inputs)
    assessment = report["provenance"]["reviewed_source_selection"]
    assert assessment["totals"] == {"ledger_targets": 3, "selected": 2, "not_selected": 1, "not_assessed": 0}
    assert assessment["targets"][0]["missing_sources"] == [binding]
    assert assessment["targets"][0]["selected_source_count"] == 0
    assert report["targets"][0]["directory_available"] is True


def test_mixed_review_tuples_require_every_coordinate_set_without_duplicate_inflation():
    inputs = _inputs()
    reviews = inputs[-1][0]["review_provenance"]
    missing = deepcopy(reviews[0])
    missing["decision"]["source_binding"]["edition_id"] = "older-edition"
    reviews.extend([missing, deepcopy(missing)])
    assessment = _report(inputs)["provenance"]["reviewed_source_selection"]
    first = assessment["targets"][0]
    assert first["status"] == "not_selected" and first["reviewed_source_count"] == 2
    assert first["selected_source_count"] == 1 and len(first["missing_sources"]) == 1


@pytest.mark.parametrize("staged", [True, False])
def test_no_approved_review_and_unstaged_recipes_never_claim_selection(staged):
    inputs = _inputs(staged=staged)
    inputs[-1][0]["review_provenance"][0]["approved_exact_binding"] = False
    inputs[-1][1]["review_provenance"] = []
    assessment = _report(inputs)["provenance"]["reviewed_source_selection"]
    assert assessment["totals"] == {
        "ledger_targets": 3,
        "selected": int(staged),
        "not_selected": 0,
        "not_assessed": 3 - int(staged),
    }
    assert all(row["missing_sources"] == [] for row in assessment["targets"])
    if not staged:
        assert all(row["selected_source_count"] is None for row in assessment["targets"])


@pytest.mark.parametrize("lineage", ["current", "historical"])
def test_selection_preserves_lineage_and_independent_pricing(lineage):
    report = _report(_inputs(lineage=lineage))
    assert all(row["source_status"] == lineage and row["priceable"] is None for row in report["targets"])
    assert report["assessment"]["source_status"] == "selected_serving_generation_lineage"
    assert report["assessment"]["pricing"] == "not_assessed"


def test_invalid_staged_receipt_is_refused_by_real_validator():
    inputs = _inputs()
    inputs[2]["validation_json"]["registry_source_selection"]["recipe_sha256"] = "d" * 64
    with pytest.raises(ValueError, match="registry_source_selection_invalid"):
        _report(inputs)


def test_unstaged_missing_edition_remains_unassessed():
    inputs = _inputs(staged=False)
    inputs[-1][0]["review_provenance"][0]["decision"]["source_binding"]["edition_id"] = "older-edition"
    assessment = _report(inputs)["provenance"]["reviewed_source_selection"]
    assert assessment["totals"]["not_assessed"] == 3
    assert assessment["targets"][0]["missing_sources"] == []
    assert assessment["targets"][0]["selected_source_count"] is None


def test_recipe_digest_drift_is_refused():
    inputs = _inputs()
    inputs[2]["source_generations"]["registry_source_recipes"] = "d" * 64
    with pytest.raises(ValueError, match="registry_source_recipes_invalid"):
        _report(inputs)


@pytest.mark.parametrize("damage", ["missing", "duplicate"])
def test_target_evidence_must_account_for_native_keys(damage):
    inputs = _inputs()
    if damage == "missing":
        inputs[-1].pop()
    else:
        inputs[-1][1] = deepcopy(inputs[-1][0])
    with pytest.raises(
        coverage.RegistryRequiredTargetCoverageUnavailable, match="registry_required_target_coverage_serving_invalid"
    ):
        _report(inputs)


def test_additive_diagnostics_remain_within_the_existing_report_byte_limit(monkeypatch):
    monkeypatch.setattr(coverage, "MAX_REPORT_BYTES", 100)
    with pytest.raises(
        coverage.RegistryRequiredTargetCoverageUnavailable, match="registry_required_target_coverage_limit"
    ):
        _report(_inputs())


@pytest.mark.asyncio
async def test_reader_consumes_verified_recipe_tuple(monkeypatch):
    inputs = _inputs()
    manifest, approved, candidate_by_field, native_coverage_by_field, target_evidence_rows = inputs
    candidate_by_field.update(candidate_id=UUID(manifest.candidate_id), manifest_status="current")
    statements = []

    class Reader:
        async def fetchrow(self, statement, *parameters):
            statements.append(statement)
            if "transaction_isolation" in statement:
                return {"isolation": "repeatable read", "readonly": "on"}
            if statement.startswith("SELECT snapshot.snapshot_id"):
                return {"metadata_valid": True}
            if statement.startswith("SELECT manifest.generation_id"):
                return candidate_by_field
            return {
                "valid": True,
                "bounded": True,
                "native_input": "{}",
                "provenance": json.dumps(target_evidence_rows),
            }

    async def resolve(*args, **kwargs):
        return manifest

    async def pin(*args, **kwargs):
        return approved

    async def bundle(*args):
        return {"review_references": "[]"}

    async def pricing(connection, report, *args, **kwargs):
        return report

    verified_recipe_batches = []
    original_report = coverage._coverage_report

    def verify(candidate_fields):
        report_by_field = verify_registry_source_recipes(candidate_fields)
        verified_recipe_batches.append(report_by_field)
        return report_by_field

    def report(*args):
        assert args[-1] is verified_recipe_batches[0]
        return original_report(*args)

    monkeypatch.setattr(coverage, "resolve_network_serving_manifest", resolve)
    monkeypatch.setattr(coverage, "pin_retained_approved_membership_source", pin)
    monkeypatch.setattr(coverage, "_read_review_bundle", bundle)
    monkeypatch.setattr(coverage, "_verify_ledger", lambda *args: {"target_count": 3})
    monkeypatch.setattr(coverage, "_native", lambda *args: deepcopy(native_coverage_by_field))
    monkeypatch.setattr(coverage, "verify_registry_source_recipes", verify)
    monkeypatch.setattr(coverage, "_coverage_report", report)
    monkeypatch.setattr(coverage, "append_pricing_binding_metadata", pricing)
    report_by_field = await coverage._read(Reader(), UUID(int=5), None, "control_example")
    assert report_by_field["provenance"]["reviewed_source_selection"]["totals"]["selected"] == 3
    assert len(statements) == 4


@pytest.mark.parametrize("status", ["selected", "not_selected", "not_assessed"])
def test_operator_serializes_verified_selection(status):
    """The operator emits the verified selection DTO assessment without changing evidence."""
    inputs = _inputs(staged=status != "not_assessed")
    if status == "not_selected":
        inputs[-1][0]["review_provenance"][0]["decision"]["source_binding"]["edition_id"] = "other-edition"
    recipes = verify_registry_source_recipes(inputs[2])
    assert type(recipes) is tuple and all(type(recipe) is RegistrySourceMembershipRecipe for recipe in recipes)
    report = _report(inputs)
    emitted = json.loads(_report_json(report))
    assert emitted == report
    selection = emitted["provenance"]["reviewed_source_selection"]
    assert selection["assessment"] == "recipe_coordinates_only"
    assert selection["targets"][0]["status"] == status
    assert emitted["targets"] == inputs[-2]["targets"]
    assert emitted["totals"] == inputs[-2]["totals"]
    assert emitted["assessment"]["pricing"] == "not_assessed"
    assert all(target["priceable"] is None for target in emitted["targets"])
