# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from copy import deepcopy

import pytest

from process.provider_directory_projection_summary import (
    MAX_SUPPLEMENTAL_COUNT_FIELDS,
    SEMANTIC_OUTCOME_FIELDS,
    semantic_source_summary,
    validated_semantic_source_summary,
)
from process.provider_directory_projection_types import (
    ProviderDirectoryProjectionError,
    stable_hash,
)
from tests.provider_directory_projection_semantic_support import (
    digest,
    semantic_outcome_proof,
)


_SUPPLEMENTAL_COUNTS = {
    "raw_records": 1,
    "quality_rejections": 2,
    "plan_links_observed": 3,
}


def _summary() -> dict:
    return semantic_source_summary(
        semantic_adapter_contract_id="fhir-bundle-decoder.v1",
        transform_contract_id="provider-directory-normalization.v1",
        dataset_hash=digest("dataset-a"),
        outcome_proof=semantic_outcome_proof(),
        supplemental_counts=_SUPPLEMENTAL_COUNTS,
    )


def test_semantic_source_summary_is_proof_derived_bounded_and_self_hashed():
    outcome_proof = semantic_outcome_proof()
    summary = _summary()
    validated = validated_semantic_source_summary(
        summary,
        expected_semantic_adapter_contract_id="fhir-bundle-decoder.v1",
        expected_transform_contract_id="provider-directory-normalization.v1",
        expected_dataset_hash=digest("dataset-a"),
        expected_outcome_proof=outcome_proof,
        expected_supplemental_counts=_SUPPLEMENTAL_COUNTS,
    )

    assert validated == summary
    assert validated["summary_grain"] == "visible_source"
    assert tuple(validated["outcome_counts"]) == SEMANTIC_OUTCOME_FIELDS
    assert validated["semantic_outcome_proof_sha256"] == outcome_proof.proof_sha256
    assert validated["canonical_row_sha256"] == outcome_proof.canonical_row_sha256
    assert validated["outcome_counts"]["specialty_records"] == 3
    assert validated["outcome_counts"]["contact_records"] == 1
    assert validated["outcome_counts"]["reference_links"] == 3
    assert validated["supplemental_counts"] == {
        "plan_links_observed": 3,
        "quality_rejections": 2,
        "raw_records": 1,
    }


def test_semantic_source_summary_rejects_rehashed_injected_outcome_counts():
    tampered = deepcopy(_summary())
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
            expected_outcome_proof=semantic_outcome_proof(),
            expected_supplemental_counts=_SUPPLEMENTAL_COUNTS,
        )


def test_semantic_source_summary_rejects_rehashed_supplemental_count_tamper():
    tampered = deepcopy(_summary())
    tampered["supplemental_counts"]["raw_records"] = 999
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
            expected_outcome_proof=semantic_outcome_proof(),
            expected_supplemental_counts=_SUPPLEMENTAL_COUNTS,
        )


def test_semantic_source_summary_rejects_wrong_dataset_or_outcome_proof():
    summary = _summary()
    mismatched_proof = semantic_outcome_proof()
    mismatched_proof.outcome_counts["specialty_records"] += 1

    with pytest.raises(ProviderDirectoryProjectionError):
        validated_semantic_source_summary(
            summary,
            expected_semantic_adapter_contract_id="fhir-bundle-decoder.v1",
            expected_transform_contract_id="provider-directory-normalization.v1",
            expected_dataset_hash=digest("dataset-b"),
            expected_outcome_proof=semantic_outcome_proof(),
            expected_supplemental_counts=_SUPPLEMENTAL_COUNTS,
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


@pytest.mark.parametrize(
    "supplemental_counts",
    (
        {
            f"metric_{index}": index
            for index in range(MAX_SUPPLEMENTAL_COUNT_FIELDS + 1)
        },
        {"UHC raw rows": 1},
        {"raw_rows": True},
        {"specialty_records": 1},
    ),
)
def test_semantic_source_summary_rejects_unbounded_or_unsafe_supplemental_counts(
    supplemental_counts,
):
    with pytest.raises(ProviderDirectoryProjectionError):
        semantic_source_summary(
            semantic_adapter_contract_id="fhir-bundle-decoder.v1",
            transform_contract_id="provider-directory-normalization.v1",
            dataset_hash=digest("dataset-a"),
            outcome_proof=semantic_outcome_proof(),
            supplemental_counts=supplemental_counts,
        )
