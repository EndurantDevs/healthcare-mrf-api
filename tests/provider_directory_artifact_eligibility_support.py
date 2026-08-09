# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Shared neutral proof fixture for artifact eligibility tests."""

from __future__ import annotations

import importlib


importer = importlib.import_module("process.provider_directory_fhir")


def content_proof(root_run_id: str) -> dict[str, object]:
    """Return one closed synthetic artifact content proof."""

    return {
        "endpoint_id": "candidate_endpoint",
        "acquisition_root_run_id": root_run_id,
        "source_ids": ["source_a", "source_b"],
        "selected_resources": ["Organization", "Practitioner"],
        "expected_resources": ["Organization", "Practitioner"],
        importer.TWIN_ROOT_VERIFICATION_CAMPAIGN_KEY: "reviewed-candidate-v1",
        importer.TWIN_ROOT_VERIFICATION_SOURCE_SCOPE_KEY: "scope-v1",
        "dataset_hash": "a" * 64,
        "resource_count": 2,
        "resource_hashes": {
            "Organization": "b" * 64,
            "Practitioner": "c" * 64,
        },
        "resource_counts": {"Organization": 1, "Practitioner": 1},
    }
