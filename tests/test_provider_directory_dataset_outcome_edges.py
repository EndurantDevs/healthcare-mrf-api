# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import dataclasses
import importlib

import pytest


importer = importlib.import_module("process.provider_directory_fhir")


def _artifact_dataset(**overrides):
    dataset = importer.ProviderDirectoryArtifactDataset(
        source_id="source-a",
        endpoint_id="endpoint-a",
        dataset_id="dataset-a",
        evidence_run_id="root-a",
    )
    return dataclasses.replace(dataset, **overrides)


@pytest.mark.parametrize(
    ("selection_state", "error_match"),
    [
        (
            {
                "should_promote_on_cutover": True,
                "validated_candidate_count": 1,
                "status": "published",
                "is_current": False,
                "current_dataset_count": 1,
                "current_dataset_id": "incumbent",
                "previous_dataset_id": "incumbent",
            },
            "validated_candidate_invalid",
        ),
        (
            {
                "should_promote_on_cutover": True,
                "validated_candidate_count": 1,
                "status": "validated",
                "is_current": False,
                "current_dataset_count": 2,
                "current_dataset_id": "incumbent",
                "previous_dataset_id": "incumbent",
            },
            "expected_incumbent_mismatch",
        ),
        (
            {
                "should_promote_on_cutover": False,
                "validated_candidate_count": 0,
                "status": "superseded",
                "is_current": False,
                "current_dataset_count": 0,
                "current_dataset_id": None,
                "previous_dataset_id": None,
            },
            "current_dataset_invalid",
        ),
    ],
)
def test_artifact_selection_rejects_inconsistent_sealed_states(
    selection_state,
    error_match,
):
    with pytest.raises(RuntimeError, match=error_match):
        importer._assert_artifact_dataset_selection_state(
            selection_state,
            "candidate",
            "endpoint-a",
        )


def test_validated_artifact_requires_complete_sealed_proof():
    dataset_row_map = {
        "status": "validated",
        "is_current": False,
        "promote_on_cutover": True,
        "current_dataset_count": 1,
        "current_dataset_id": "incumbent",
        "validated_candidate_count": 1,
        "previous_dataset_id": "incumbent",
        "dataset_hash": None,
        "validated_at": "2026-07-21T10:00:00",
        "publication_metadata_json": {"source_ids": ["source-a"]},
    }

    with pytest.raises(RuntimeError, match="validated_candidate_proof_invalid"):
        importer._artifact_dataset_state_from_row(
            dataset_row_map,
            "candidate",
            "endpoint-a",
        )


@pytest.mark.parametrize(
    ("rows_by_source_id", "requested_source_ids", "error_match"),
    [
        ({"source-a": []}, ["source-b"], "source_alias_missing:source-b"),
        ({}, [], "source_aliases_empty"),
    ],
)
def test_artifact_selection_rejects_missing_or_empty_source_scope(
    rows_by_source_id,
    requested_source_ids,
    error_match,
):
    with pytest.raises(RuntimeError, match=error_match):
        importer._artifact_dataset_source_ids(
            rows_by_source_id,
            requested_source_ids,
        )


@pytest.mark.parametrize(
    ("conflicting_dataset", "error_match"),
    [
        (
            _artifact_dataset(source_id="source-b", dataset_id="dataset-b"),
            "endpoint_dataset_ambiguous",
        ),
        (
            _artifact_dataset(
                source_id="source-b",
                selected_resources=("Practitioner",),
            ),
            "endpoint_metadata_ambiguous",
        ),
    ],
)
def test_endpoint_aliases_reject_dataset_or_metadata_divergence(
    conflicting_dataset,
    error_match,
):
    with pytest.raises(RuntimeError, match=error_match):
        importer._assert_artifact_endpoint_consistency([
            _artifact_dataset(),
            conflicting_dataset,
        ])


def test_source_summary_rejects_unsealed_or_invalid_counts():
    with pytest.raises(RuntimeError, match="address_records_invalid"):
        importer._source_summary_metric_count(
            {"address_records": True},
            "address_records",
        )
    with pytest.raises(RuntimeError, match="relation_proof_missing"):
        importer._source_summary_relation_edge_count({}, "dataset_network_plan")
    with pytest.raises(RuntimeError, match="relation_count_invalid"):
        importer._source_summary_relation_edge_count(
            {"dataset_network_plan": {"complete": True, "edge_count": True}},
            "dataset_network_plan",
        )


def test_artifact_dataset_rows_ignore_missing_source_identity():
    rows_by_source_id = importer._artifact_dataset_rows_by_source([
        {"source_id": " "},
        {"source_id": "source-a"},
    ])

    assert rows_by_source_id == {"source-a": [{"source_id": "source-a"}]}


def test_reviewed_promotion_scope_rejects_two_datasets_for_one_source():
    verification_field_map = {
        "promote_on_cutover": True,
        "verification_source_status": importer.PROVIDER_DIRECTORY_TWIN_ROOT_VERIFIED,
        "verification_campaign_id": "campaign-a",
        "verification_source_scope_hash": "scope-a",
        "verification_source_ids": ("shared-source",),
    }
    fence = importer.ProviderDirectoryArtifactDatasetFence((
        _artifact_dataset(**verification_field_map),
        _artifact_dataset(
            source_id="source-b",
            endpoint_id="endpoint-b",
            dataset_id="dataset-b",
            **verification_field_map,
        ),
    ))

    with pytest.raises(RuntimeError, match="promotion_source_ambiguous"):
        importer._reviewed_promotion_dataset_by_source_id(fence)


def test_recorded_dataset_profile_drift_is_reported_separately():
    fence = importer.ProviderDirectoryArtifactDatasetFence((
        _artifact_dataset(
            selected_resources=("Location",),
            expected_resources=("Location",),
            recorded_expected_resources=("Organization",),
        ),
    ))

    assert importer._artifact_dataset_profile_violations(fence) == [
        "dataset_recorded_profile:source-a:endpoint-a:dataset-a:Organization:Location"
    ]
