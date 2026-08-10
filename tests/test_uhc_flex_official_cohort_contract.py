# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Pure contract tests for the official UHC Practitioner NPI cohort."""

from dataclasses import replace

import pytest

from process.uhc_flex_official_cohort_contract import (
    UHCFlexOfficialNPICohort,
    UHC_FLEX_OFFICIAL_AUTHORITY_ID,
    UHC_FLEX_OFFICIAL_COHORT_CONTRACT_ID,
    UHC_FLEX_OFFICIAL_RESOURCE_TYPE,
    build_uhc_flex_official_cohort,
    canonical_uhc_flex_npi,
)
from process.uhc_provider_file_source_identity import UHC_PROVIDER_FILE_SOURCE_ID


def _cohort() -> UHCFlexOfficialNPICohort:
    return build_uhc_flex_official_cohort(
        official_endpoint_id="e" * 64,
        official_dataset_id="dataset-2026",
        official_acquisition_root_run_id="r" * 64,
        official_dataset_hash="d" * 64,
        official_content_proof_sha256="c" * 64,
        practitioner_resource_count=3,
        npi_count=2,
    )


def test_contract_binds_exact_official_lineage_and_unique_npi_set():
    cohort = _cohort()

    assert cohort.cohort_id == (
        "pdufc_e83b66f37f239e7da59cdd36495af478c4594b38781189d5"
    )
    assert cohort.practitioner_resource_count == 3
    assert cohort.npi_count == 2
    assert cohort.contract_id == UHC_FLEX_OFFICIAL_COHORT_CONTRACT_ID
    assert cohort.authority_id == UHC_FLEX_OFFICIAL_AUTHORITY_ID
    assert cohort.official_source_id == UHC_PROVIDER_FILE_SOURCE_ID
    assert cohort.resource_type == UHC_FLEX_OFFICIAL_RESOURCE_TYPE
    assert cohort.cohort_complete is True
    assert cohort.endpoint_collection_complete is False
    assert cohort.endpoint_complete is False
    assert "1000000004" not in repr(cohort)


@pytest.mark.parametrize(
    "candidate",
    [True, "1000000004", 999999999, 3000000000, 1234567890],
)
def test_npi_validation_rejects_noncanonical_or_bad_checksum(candidate):
    with pytest.raises(ValueError, match="NPI is invalid"):
        canonical_uhc_flex_npi(candidate)


def test_builder_requires_one_valid_npi_for_every_practitioner():
    with pytest.raises(ValueError, match="evidence is incomplete"):
        build_uhc_flex_official_cohort(
            official_endpoint_id="e" * 64,
            official_dataset_id="dataset-2026",
            official_acquisition_root_run_id="r" * 64,
            official_dataset_hash="d" * 64,
            official_content_proof_sha256="c" * 64,
            practitioner_resource_count=2,
            npi_count=3,
        )


@pytest.mark.parametrize(
    "change",
    [
        {"cohort_id": "pdufc_" + "0" * 48},
        {"npi_count": 1},
        {"authority_id": "other"},
        {"cohort_complete": False},
        {"endpoint_collection_complete": True},
        {"endpoint_complete": True},
    ],
)
def test_dataclass_rejects_identity_or_completion_drift(change):
    with pytest.raises(ValueError, match="cohort is inconsistent"):
        replace(_cohort(), **change)
