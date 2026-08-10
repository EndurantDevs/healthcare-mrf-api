# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed identity contract for the official UHC Practitioner NPI cohort."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import re

from process.provider_directory_profile import is_valid_npi
from process.uhc_provider_file_source_identity import (
    UHC_PROVIDER_FILE_OWNER_ID,
    UHC_PROVIDER_FILE_SOURCE_ID,
)


UHC_FLEX_OFFICIAL_COHORT_CONTRACT_ID = (
    "healthporta.provider-directory.uhc-flex-official-practitioner-npi-cohort.v1"
)
UHC_FLEX_OFFICIAL_AUTHORITY_ID = UHC_PROVIDER_FILE_OWNER_ID
UHC_FLEX_OFFICIAL_RESOURCE_TYPE = "Practitioner"
UHC_FLEX_OFFICIAL_NPI_SYSTEM = "http://hl7.org/fhir/sid/us-npi"

_COHORT_ID_PATTERN = re.compile(r"pdufc_[0-9a-f]{48}\Z")
_HASH_PATTERN = re.compile(r"[0-9a-f]{64}\Z")


class UHCFlexOfficialCohortError(RuntimeError):
    """Expose one bounded cohort selection or persistence failure."""

    def __init__(self, code: str = "evidence") -> None:
        message_by_code = {
            "busy": "UHC Flex official cohort synchronization is busy",
            "evidence": "UHC Flex official cohort evidence is invalid",
            "missing": "UHC Flex official cohort source is missing",
            "state": "UHC Flex official cohort state is invalid",
        }
        self.code = code if code in message_by_code else "evidence"
        super().__init__(message_by_code[self.code])


def _strict_text(value: object, label: str, maximum_length: int) -> str:
    if (
        type(value) is not str
        or not value
        or len(value) > maximum_length
        or value != value.strip()
        or any(not character.isprintable() for character in value)
    ):
        raise ValueError(f"UHC Flex official cohort {label} is invalid")
    return value


def _strict_hash(value: object, label: str) -> str:
    if type(value) is not str or _HASH_PATTERN.fullmatch(value) is None:
        raise ValueError(f"UHC Flex official cohort {label} is invalid")
    return value


def canonical_uhc_flex_npi(value: object) -> int:
    """Require one canonical CMS-range NPI with a valid Luhn check digit."""

    if type(value) is not int or not is_valid_npi(value):
        raise ValueError("UHC Flex official cohort NPI is invalid")
    return value


def uhc_flex_official_cohort_id(
    *,
    official_endpoint_id: str,
    official_dataset_id: str,
    official_acquisition_root_run_id: str,
    official_dataset_hash: str,
    official_content_proof_sha256: str,
    practitioner_resource_count: int,
    npi_count: int,
) -> str:
    """Derive the stable header identity matched by the database seal."""

    identity_parts = (
        UHC_FLEX_OFFICIAL_COHORT_CONTRACT_ID,
        UHC_FLEX_OFFICIAL_AUTHORITY_ID,
        UHC_PROVIDER_FILE_SOURCE_ID,
        _strict_text(official_endpoint_id, "official endpoint ID", 64),
        _strict_text(official_dataset_id, "official dataset ID", 96),
        _strict_text(
            official_acquisition_root_run_id,
            "official acquisition root run ID",
            64,
        ),
        _strict_hash(official_dataset_hash, "official dataset hash"),
        _strict_hash(
            official_content_proof_sha256,
            "official content proof hash",
        ),
        UHC_FLEX_OFFICIAL_RESOURCE_TYPE,
        str(_positive_count(practitioner_resource_count, "Practitioner count")),
        str(_positive_count(npi_count, "NPI count")),
        "true",
        "false",
        "false",
    )
    identity = "\x1f".join(identity_parts).encode("utf-8")
    return "pdufc_" + hashlib.sha256(identity).hexdigest()[:48]


def _positive_count(value: object, label: str) -> int:
    if type(value) is not int or value < 1 or value > (1 << 63) - 1:
        raise ValueError(f"UHC Flex official cohort {label} is invalid")
    return value


@dataclass(frozen=True, slots=True, repr=False)
class UHCFlexOfficialNPICohort:
    """Bind an exact current official Practitioner dataset to its NPI set."""

    cohort_id: str
    official_endpoint_id: str
    official_dataset_id: str
    official_acquisition_root_run_id: str
    official_dataset_hash: str
    official_content_proof_sha256: str
    practitioner_resource_count: int
    npi_count: int
    contract_id: str = UHC_FLEX_OFFICIAL_COHORT_CONTRACT_ID
    authority_id: str = UHC_FLEX_OFFICIAL_AUTHORITY_ID
    official_source_id: str = UHC_PROVIDER_FILE_SOURCE_ID
    resource_type: str = UHC_FLEX_OFFICIAL_RESOURCE_TYPE
    cohort_complete: bool = True
    endpoint_collection_complete: bool = False
    endpoint_complete: bool = False

    def __post_init__(self) -> None:
        practitioner_count = _positive_count(
            self.practitioner_resource_count,
            "Practitioner count",
        )
        distinct_npi_count = _positive_count(self.npi_count, "NPI count")
        expected_cohort_id = uhc_flex_official_cohort_id(
            official_endpoint_id=self.official_endpoint_id,
            official_dataset_id=self.official_dataset_id,
            official_acquisition_root_run_id=(
                self.official_acquisition_root_run_id
            ),
            official_dataset_hash=self.official_dataset_hash,
            official_content_proof_sha256=self.official_content_proof_sha256,
            practitioner_resource_count=self.practitioner_resource_count,
            npi_count=self.npi_count,
        )
        if (
            self.contract_id != UHC_FLEX_OFFICIAL_COHORT_CONTRACT_ID
            or self.authority_id != UHC_FLEX_OFFICIAL_AUTHORITY_ID
            or self.official_source_id != UHC_PROVIDER_FILE_SOURCE_ID
            or self.resource_type != UHC_FLEX_OFFICIAL_RESOURCE_TYPE
            or self.cohort_complete is not True
            or self.endpoint_collection_complete is not False
            or self.endpoint_complete is not False
            or distinct_npi_count > practitioner_count
            or not _COHORT_ID_PATTERN.fullmatch(self.cohort_id)
            or self.cohort_id != expected_cohort_id
        ):
            raise ValueError("UHC Flex official cohort is inconsistent")


def build_uhc_flex_official_cohort(
    *,
    official_endpoint_id: str,
    official_dataset_id: str,
    official_acquisition_root_run_id: str,
    official_dataset_hash: str,
    official_content_proof_sha256: str,
    practitioner_resource_count: int,
    npi_count: int,
) -> UHCFlexOfficialNPICohort:
    """Build a header whose relational members are sealed by PostgreSQL."""

    expected_resource_count = _positive_count(
        practitioner_resource_count,
        "Practitioner count",
    )
    expected_npi_count = _positive_count(npi_count, "NPI count")
    if expected_npi_count > expected_resource_count:
        raise ValueError("UHC Flex official cohort Practitioner evidence is incomplete")
    cohort_id = uhc_flex_official_cohort_id(
        official_endpoint_id=official_endpoint_id,
        official_dataset_id=official_dataset_id,
        official_acquisition_root_run_id=official_acquisition_root_run_id,
        official_dataset_hash=official_dataset_hash,
        official_content_proof_sha256=official_content_proof_sha256,
        practitioner_resource_count=expected_resource_count,
        npi_count=expected_npi_count,
    )
    return UHCFlexOfficialNPICohort(
        cohort_id=cohort_id,
        official_endpoint_id=official_endpoint_id,
        official_dataset_id=official_dataset_id,
        official_acquisition_root_run_id=official_acquisition_root_run_id,
        official_dataset_hash=official_dataset_hash,
        official_content_proof_sha256=official_content_proof_sha256,
        practitioner_resource_count=expected_resource_count,
        npi_count=expected_npi_count,
    )


__all__ = (
    "build_uhc_flex_official_cohort",
    "canonical_uhc_flex_npi",
    "uhc_flex_official_cohort_id",
    "UHCFlexOfficialCohortError",
    "UHCFlexOfficialNPICohort",
    "UHC_FLEX_OFFICIAL_AUTHORITY_ID",
    "UHC_FLEX_OFFICIAL_COHORT_CONTRACT_ID",
    "UHC_FLEX_OFFICIAL_NPI_SYSTEM",
    "UHC_FLEX_OFFICIAL_RESOURCE_TYPE",
)
