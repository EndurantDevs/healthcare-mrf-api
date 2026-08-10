# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Boundary coverage for the closed official Flex cohort contract."""

from dataclasses import replace

import pytest

from process import uhc_flex_official_cohort_contract as contract


def _cohort():
    return contract.build_uhc_flex_official_cohort(
        official_endpoint_id="e" * 64,
        official_dataset_id="dataset-boundary",
        official_acquisition_root_run_id="r" * 64,
        official_dataset_hash="d" * 64,
        official_content_proof_sha256="c" * 64,
        practitioner_resource_count=2,
        npi_count=1,
    )


@pytest.mark.parametrize("value", [None, "", " padded", "x" * 65, "bad\ntext"])
def test_cohort_identity_rejects_unsafe_text(value):
    with pytest.raises(ValueError, match="official endpoint ID is invalid"):
        contract.uhc_flex_official_cohort_id(
            official_endpoint_id=value,
            official_dataset_id="dataset-boundary",
            official_acquisition_root_run_id="r" * 64,
            official_dataset_hash="d" * 64,
            official_content_proof_sha256="c" * 64,
            practitioner_resource_count=1,
            npi_count=1,
        )


@pytest.mark.parametrize("value", [None, True, "d" * 63, "G" * 64])
def test_cohort_identity_rejects_noncanonical_hash(value):
    with pytest.raises(ValueError, match="official dataset hash is invalid"):
        contract.uhc_flex_official_cohort_id(
            official_endpoint_id="e" * 64,
            official_dataset_id="dataset-boundary",
            official_acquisition_root_run_id="r" * 64,
            official_dataset_hash=value,
            official_content_proof_sha256="c" * 64,
            practitioner_resource_count=1,
            npi_count=1,
        )


@pytest.mark.parametrize("value", [True, 0, -1, 1 << 63])
def test_cohort_identity_rejects_nonpositive_or_unbounded_counts(value):
    with pytest.raises(ValueError, match="Practitioner count is invalid"):
        contract.uhc_flex_official_cohort_id(
            official_endpoint_id="e" * 64,
            official_dataset_id="dataset-boundary",
            official_acquisition_root_run_id="r" * 64,
            official_dataset_hash="d" * 64,
            official_content_proof_sha256="c" * 64,
            practitioner_resource_count=value,
            npi_count=1,
        )


def test_contract_error_falls_back_to_bounded_evidence_code():
    error = contract.UHCFlexOfficialCohortError("provider-secret")

    assert error.code == "evidence"
    assert "provider-secret" not in str(error)
    assert contract.canonical_uhc_flex_npi(1234567893) == 1234567893


@pytest.mark.parametrize(
    "change",
    [
        {"contract_id": "wrong"},
        {"official_source_id": "wrong"},
        {"resource_type": "Organization"},
        {"practitioner_resource_count": 1, "npi_count": 2},
        {"cohort_id": "not-a-cohort"},
    ],
)
def test_cohort_dataclass_rejects_remaining_identity_drift(change):
    with pytest.raises(ValueError, match="cohort is inconsistent"):
        replace(_cohort(), **change)
