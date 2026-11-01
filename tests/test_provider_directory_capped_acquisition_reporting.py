# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Capped search completion cannot be inferred from generic success flags."""

import json

import pytest

from scripts.research import provider_directory_endpoint_acquisition_harness as harness
from scripts.research import provider_directory_endpoint_acquisition_reporting as reporting
from scripts.research import provider_directory_endpoint_acquisition_support as support

COUNT_FIELDS = (
    "last_updated_capped_count_pre",
    "last_updated_capped_count_post",
    "last_updated_exact_leaf_count_sum",
    "last_updated_pass1_unique",
    "last_updated_pass2_unique",
    "last_updated_staged_candidate_count",
)
ZERO_FIELDS = (
    "last_updated_capped_before_start_pre",
    "last_updated_capped_after_end_pre",
    "last_updated_capped_before_start_post",
    "last_updated_capped_after_end_post",
    "last_updated_unfiltered_pre",
    "last_updated_ranged_root_pre",
    "last_updated_ranged_root_post",
    "last_updated_unfiltered_post",
)
SOURCE_FIELDS = (
    "last_updated_partition_sources",
    "last_updated_completeness_verified_sources",
    "last_updated_capped_sources",
    "last_updated_capped_verified_sources",
    "last_updated_capped_versioned_sources",
)


def acquisition_entry():
    return {
        "entry_id": "example-directory",
        "owner_id": "example-directory",
        "canonical_base": "https://directory.example/fhir",
        "classification": "acquisition",
        "launch_mode": "create",
        "resource_profile": "G4",
        "source_ids": ["pdfhir_" + "a" * 24],
        "resources": list(harness.RESOURCE_PROFILES["G4"]),
        "capped_last_updated_resources": ["Practitioner"],
    }


def acquisition_metrics(entry, count=4):
    stats_by_resource = {
        resource: {"sources_completed": 1, "sources_failed": 0, "sources_bounded": 0} for resource in entry["resources"]
    }
    for resource in entry.get("capped_last_updated_resources", []):
        stats_by_resource[resource].update({name: len(entry["source_ids"]) for name in SOURCE_FIELDS})
        stats_by_resource[resource].update({name: count for name in COUNT_FIELDS})
        stats_by_resource[resource].update({name: 0 for name in ZERO_FIELDS})
    return {
        "source_ids": entry["source_ids"],
        "source_import_sources_selected": len(entry["source_ids"]),
        "source_import_groups_attempted": 1,
        "resource_fetch_completed_source_ids": {resource: entry["source_ids"] for resource in entry["resources"]},
        "resource_fetch_stats": stats_by_resource,
    }


def manifest(entry):
    return {
        "schema_version": 1,
        "campaign_id": "example-directory-acquisition",
        "engine": "example-engine",
        "importer": "provider-directory-fhir",
        "retest_results_url": "https://directory.example/retest.json",
        "parameter_profiles": {name: {} for name in ("acquisition", "bulk_acquisition", "probe_only")},
        "entries": [entry],
    }


@pytest.mark.parametrize("count", [0, 4])
@pytest.mark.parametrize("source_count", [1, 2])
def test_capped_counts_require_exact_source_and_population_reconciliation(count, source_count):
    entry = acquisition_entry()
    if source_count == 2:
        entry["source_ids"].append("pdfhir_" + "b" * 24)
    assert support.acquisition_metric_errors(entry, acquisition_metrics(entry, count)) == []


def test_manifest_requires_capped_proof_even_when_all_runtime_flags_are_absent():
    entry = acquisition_entry()
    metrics = acquisition_metrics(entry)
    metrics["resource_fetch_stats"]["Practitioner"] = {"sources_completed": 1}
    errors = support.acquisition_metric_errors(entry, metrics)
    assert any("capped last-updated source metrics" in error for error in errors)
    assert any("counts do not reconcile" in error for error in errors)
    assert any("not exact zero" in error for error in errors)


def test_nonzero_capped_runtime_flags_also_require_the_complete_proof():
    entry = acquisition_entry()
    metrics = acquisition_metrics(entry)
    entry.pop("capped_last_updated_resources")
    assert support.acquisition_metric_errors(entry, metrics) == []
    metrics["resource_fetch_stats"]["Practitioner"].pop("last_updated_capped_count_post")
    assert any("counts do not reconcile" in error for error in support.acquisition_metric_errors(entry, metrics))


@pytest.mark.parametrize("name", SOURCE_FIELDS)
@pytest.mark.parametrize("bad_value", [None, True, 0, 2])
def test_capped_source_metrics_reject_missing_boolean_or_wrong_source_counts(name, bad_value):
    entry = acquisition_entry()
    metrics = acquisition_metrics(entry)
    metrics["resource_fetch_stats"]["Practitioner"][name] = bad_value
    assert any(
        "capped last-updated source metrics" in error for error in support.acquisition_metric_errors(entry, metrics)
    )


@pytest.mark.parametrize("name", COUNT_FIELDS)
def test_each_capped_population_count_must_agree(name):
    entry = acquisition_entry()
    metrics = acquisition_metrics(entry)
    metrics["resource_fetch_stats"]["Practitioner"][name] = 5
    assert any("counts do not reconcile" in error for error in support.acquisition_metric_errors(entry, metrics))


@pytest.mark.parametrize("bad_value", [None, True, -1, 4.0, "4", []])
def test_capped_population_counts_require_nonnegative_integers(bad_value):
    entry = acquisition_entry()
    metrics = acquisition_metrics(entry)
    metrics["resource_fetch_stats"]["Practitioner"].update({name: bad_value for name in COUNT_FIELDS})
    assert any("counts do not reconcile" in error for error in support.acquisition_metric_errors(entry, metrics))


@pytest.mark.parametrize("name", ZERO_FIELDS)
@pytest.mark.parametrize("bad_value", [None, False, 1])
def test_capped_tails_and_unqueried_legacy_census_are_explicit_zero(name, bad_value):
    entry = acquisition_entry()
    metrics = acquisition_metrics(entry)
    metrics["resource_fetch_stats"]["Practitioner"][name] = bad_value
    assert any("not exact zero" in error for error in support.acquisition_metric_errors(entry, metrics))


@pytest.mark.parametrize("count", [0, 4])
def test_safe_report_retains_capped_zero_proofs_for_verification(count):
    entry = acquisition_entry()
    metrics = acquisition_metrics(entry, count)
    stats = metrics["resource_fetch_stats"]["Practitioner"]
    stats["debug_url"] = "https://example.test/debug"
    summary = reporting.run_summary({"status": "succeeded", "metrics": metrics})
    saved = summary["resource_outcomes"]["Practitioner"]
    assert "debug_url" not in saved
    for name in COUNT_FIELDS + ZERO_FIELDS:
        assert saved[name] == stats[name]
    metrics["resource_fetch_stats"] = summary["resource_outcomes"]
    assert support.acquisition_metric_errors(entry, metrics) == []


def test_safe_report_drops_invalid_proof_values_without_turning_them_into_zero():
    entry = acquisition_entry()
    metrics = acquisition_metrics(entry)
    metrics["resource_fetch_stats"]["Practitioner"]["last_updated_capped_before_start_pre"] = False
    saved = reporting.run_summary({"status": "succeeded", "metrics": metrics})["resource_outcomes"]
    assert "last_updated_capped_before_start_pre" not in saved["Practitioner"]
    metrics["resource_fetch_stats"] = saved
    assert any("not exact zero" in error for error in support.acquisition_metric_errors(entry, metrics))


def test_ordinary_report_shape_and_uncapped_verification_are_unchanged():
    entry = acquisition_entry()
    entry.pop("capped_last_updated_resources")
    metrics = acquisition_metrics(entry)
    metrics["resource_fetch_stats"]["Practitioner"]["last_updated_unfiltered_pre"] = 0
    saved = reporting.run_summary({"status": "succeeded", "metrics": metrics})["resource_outcomes"]
    assert "last_updated_unfiltered_pre" not in saved["Practitioner"]
    assert support.acquisition_metric_errors(entry, metrics) == []


@pytest.mark.parametrize("selected", [["Practitioner"], list(harness.RESOURCE_PROFILES["G4"])])
def test_manifest_capped_selection_may_cover_some_or_all_selected_resources(tmp_path, selected):
    entry = acquisition_entry()
    entry["capped_last_updated_resources"] = selected
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest(entry)))
    assert harness.load_manifest(path)["entries"][0]["capped_last_updated_resources"] == selected


@pytest.mark.parametrize("selected", [None, "Practitioner", [], ["Practitioner", "Practitioner"], ["Unknown"], [1], {}])
def test_manifest_rejects_invalid_capped_resource_selection(tmp_path, selected):
    entry = acquisition_entry()
    entry["capped_last_updated_resources"] = selected
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest(entry)))
    with pytest.raises(harness.ManifestError, match="capped_last_updated_resources"):
        harness.load_manifest(path)


@pytest.mark.parametrize("classification", ["bulk_acquisition", "manual_acquisition", "probe_only", "external"])
def test_manifest_capped_resources_require_rest_acquisition(tmp_path, classification):
    entry = acquisition_entry()
    entry["classification"] = classification
    if classification in {"probe_only", "external"}:
        entry.update(resource_profile="NONE", resources=[])
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest(entry)))
    with pytest.raises(harness.ManifestError, match="capped_last_updated_resources"):
        harness.load_manifest(path)


def test_manifest_defaults_do_not_gain_a_capped_selection(tmp_path):
    entry = acquisition_entry()
    entry.pop("capped_last_updated_resources")
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest(entry)))
    assert harness.load_manifest(path) == manifest(entry)
