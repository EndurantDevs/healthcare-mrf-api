# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Single-root artifact verification-contract tests."""

from __future__ import annotations

import importlib

from tests import test_provider_directory_artifact_verification_contract as shared


importer = importlib.import_module("process.provider_directory_fhir")


def test_pending_policy_does_not_invalidate_serving_legacy_incumbent():
    source_record = shared._source_record(
        source_status=importer.PROVIDER_DIRECTORY_ROOT_POLICY_PENDING,
    )
    source_record["metadata_json"][
        importer.REVIEWED_ROOT_POLICY_METADATA_KEY
    ] = importer.ReviewedRootPolicy(1).document()

    dataset = importer._provider_directory_artifact_dataset_from_row(
        {
            "source_id": "source_a",
            "endpoint_id": "candidate_endpoint",
            "source_record_json": source_record,
            "dataset_id": "dataset_current",
            "evidence_run_id": "root_current",
            "selected_resources": ["Organization"],
            "recorded_expected_resources": ["Organization"],
            "status": importer.ENDPOINT_DATASET_PUBLISHED,
            "is_current": True,
            "current_dataset_count": 1,
            "current_dataset_id": "dataset_current",
            "validated_candidate_count": 0,
            "previous_dataset_id": None,
            "promote_on_cutover": False,
            "dataset_hash": "a" * 64,
            "resource_count": 1,
            "validated_at": "2026-08-09T00:00:00Z",
            "publication_metadata_json": {
                "selected_resources": ["Organization"],
                "expected_resources": ["Organization"],
            },
        }
    )

    assert dataset is not None
    assert dataset.reviewed_root_policy is None
