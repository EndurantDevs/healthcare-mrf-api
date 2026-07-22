# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from copy import deepcopy

import pytest

from process.provider_directory_projection_summary import (
    SEMANTIC_OUTCOME_FIELDS,
    semantic_source_summary,
    validated_semantic_source_summary,
)
from process.provider_directory_projection_types import (
    ProviderDirectoryProjectionError,
    stable_hash,
)
from tests.provider_directory_projection_semantic_support import (
    candidate_semantic_outcome,
    digest,
)


def _candidate_summary() -> dict:
    return semantic_source_summary(
        semantic_adapter_contract_id="fhir-bundle-decoder.v1",
        transform_contract_id="provider-directory-normalization.v1",
        dataset_hash=digest("dataset-a"),
        outcome_proof=candidate_semantic_outcome(),
    )


def test_candidate_source_summary_is_bounded_and_self_hashed():
    outcome_proof = candidate_semantic_outcome()
    summary = _candidate_summary()
    validated = validated_semantic_source_summary(
        summary,
        expected_semantic_adapter_contract_id="fhir-bundle-decoder.v1",
        expected_transform_contract_id="provider-directory-normalization.v1",
        expected_dataset_hash=digest("dataset-a"),
        expected_outcome_proof=outcome_proof,
    )

    assert validated == summary
    assert validated["summary_grain"] == "visible_source"
    assert tuple(validated["outcome_counts"]) == SEMANTIC_OUTCOME_FIELDS
    assert validated["semantic_outcome_proof_sha256"] == outcome_proof.proof_sha256
    assert validated["canonical_row_sha256"] == outcome_proof.canonical_row_sha256
    assert validated["outcome_counts"]["specialty_records"] == 3
    assert validated["outcome_counts"]["contact_records"] == 1
    assert validated["outcome_counts"]["reference_links"] == 8


def test_candidate_summary_rejects_rehashed_injected_outcome_counts():
    tampered = deepcopy(_candidate_summary())
    tampered["outcome_counts"]["specialty_records"] += 1
    tampered_summary_map = dict(tampered)
    tampered_summary_map.pop("summary_sha256")
    tampered["summary_sha256"] = stable_hash(
        tampered_summary_map,
        domain="provider-directory-projection-semantic-source-summary-v1",
    )

    with pytest.raises(
        ProviderDirectoryProjectionError,
        match="semantic_summary_mismatch",
    ):
        validated_semantic_source_summary(
            tampered,
            expected_semantic_adapter_contract_id="fhir-bundle-decoder.v1",
            expected_transform_contract_id="provider-directory-normalization.v1",
            expected_dataset_hash=digest("dataset-a"),
            expected_outcome_proof=candidate_semantic_outcome(),
        )


def test_candidate_summary_rejects_wrong_dataset_or_outcome():
    summary = _candidate_summary()
    mismatched_proof = candidate_semantic_outcome()
    mismatched_proof.outcome_counts["specialty_records"] += 1

    with pytest.raises(ProviderDirectoryProjectionError):
        validated_semantic_source_summary(
            summary,
            expected_semantic_adapter_contract_id="fhir-bundle-decoder.v1",
            expected_transform_contract_id="provider-directory-normalization.v1",
            expected_dataset_hash=digest("dataset-b"),
            expected_outcome_proof=candidate_semantic_outcome(),
        )
    with pytest.raises(
        ProviderDirectoryProjectionError,
        match="semantic_outcome_proof_mismatch",
    ):
        semantic_source_summary(
            semantic_adapter_contract_id="fhir-bundle-decoder.v1",
            transform_contract_id="provider-directory-normalization.v1",
            dataset_hash=digest("dataset-a"),
            outcome_proof=mismatched_proof,
        )
