# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retained repair uses the existing target policy and resource alternatives."""

import importlib
from types import SimpleNamespace

import pytest

fhir = importlib.import_module("process.provider_directory_fhir")


def fence(resources=None):
    selected_datasets = tuple(sorted(resources or fhir.RESOURCE_MODELS_BY_TYPE))
    dataset = SimpleNamespace(
        source_id="source_a",
        endpoint_id="endpoint_a",
        dataset_id="dataset_a",
        selected_resources=selected_datasets,
        expected_resources=selected_datasets,
        recorded_expected_resources=selected_datasets,
    )
    return SimpleNamespace(datasets=(dataset,))


def test_location_repair_includes_existing_corroboration_prerequisite_closure():
    targets = fhir.resolve_provider_directory_artifact_repair_targets(fence(), ("Location",))
    assert "corroboration" in targets and "address_overlay" in targets and "network_catalog" in targets
    assert "dataset_network_plan" not in targets
    assert targets == tuple(target for target in fhir.PROVIDER_DIRECTORY_PUBLISH_ARTIFACT_TARGETS if target in targets)


def test_configured_target_filter_and_disabled_corroboration_remain_effective():
    assert fhir.resolve_provider_directory_artifact_repair_targets(
        fence(),
        ("Location",),
        configured_targets={"location_contacts"},
    ) == ("location_contacts",)
    targets = fhir.resolve_provider_directory_artifact_repair_targets(
        fence(), ("Location",), publish_corroboration=False
    )
    assert "corroboration" not in targets and "network_catalog" not in targets


def test_explicitly_disabled_required_prerequisite_cannot_silently_widen_the_policy():
    with pytest.raises(RuntimeError, match="repair_prerequisite_disabled:network_catalog"):
        fhir.resolve_provider_directory_artifact_repair_targets(
            fence(),
            ("Location",),
            configured_targets={"corroboration", "address_overlay"},
        )


def test_existing_complete_profile_and_alternative_resource_validation_still_runs():
    incomplete = fence(("InsurancePlan", "Location"))
    with pytest.raises(RuntimeError, match="target_dependencies_missing:.*corroboration"):
        fhir.resolve_provider_directory_artifact_repair_targets(incomplete, ("Location",))
    partial = fence()
    partial.datasets[0].expected_resources = ("Practitioner",)
    with pytest.raises(RuntimeError, match="target_dependencies_missing:dataset_profile"):
        fhir.resolve_provider_directory_artifact_repair_targets(partial, ("Location",))


def test_unknown_resources_or_targets_refuse_before_building():
    with pytest.raises(ValueError, match="repair_resources_invalid"):
        fhir.resolve_provider_directory_artifact_repair_targets(fence(), ("Unknown",))
    with pytest.raises(ValueError, match="repair_targets_invalid"):
        fhir.resolve_provider_directory_artifact_repair_targets(fence(), ("Location",), configured_targets={"Unknown"})
