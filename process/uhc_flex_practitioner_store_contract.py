# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed identities and result shapes for Flex Practitioner persistence."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import re

from process.uhc_flex_official_cohort_contract import (
    UHCFlexOfficialNPICohort,
    canonical_uhc_flex_npi,
)
from process.uhc_flex_practitioner_contract import (
    UHC_FLEX_PRACTITIONER_CONNECTOR_ID,
    UHC_FLEX_PRACTITIONER_QUERY_CONTRACT_ID,
    UHC_FLEX_PRACTITIONER_SOURCE_ID,
)
from process.uhc_flex_practitioner_query import (
    UHCFlexPractitionerQueryResult,
)


UHC_FLEX_PRACTITIONER_ACQUISITION_CONTRACT_ID = (
    "healthporta.provider-directory.uhc-flex-practitioner-acquisition.v1"
)
UHC_FLEX_PRACTITIONER_TERMINAL_RECORD_CONTRACT_ID = (
    "healthporta.provider-directory.uhc-flex-practitioner-terminal-record.v1"
)
UHC_FLEX_PRACTITIONER_ACQUISITION_ROLES = frozenset(
    {"baseline", "candidate"}
)

ACQUISITION_PATTERN = re.compile(r"pdufpa_[0-9a-f]{48}\Z")
COHORT_PATTERN = re.compile(r"pdufc_[0-9a-f]{48}\Z")
RUN_PATTERN = re.compile(r"pdufpr_[0-9a-f]{48}\Z")
INTENT_PATTERN = re.compile(r"pdufdi_[0-9a-f]{48}\Z")
HASH_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
ERROR_PATTERN = re.compile(r"[a-z][a-z0-9_]{0,127}\Z")


class UHCFlexPractitionerStoreError(RuntimeError):
    """Expose bounded persistence failures without response or NPI data."""

    def __init__(self, code: str = "state") -> None:
        message_by_code = {
            "identity": "Flex Practitioner acquisition identity is invalid",
            "lease_lost": "Flex Practitioner work lease was lost",
            "state": "Flex Practitioner acquisition state is invalid",
        }
        self.code = code if code in message_by_code else "state"
        super().__init__(message_by_code[self.code])


def strict_identifier(
    candidate: object,
    pattern: re.Pattern[str],
    label: str,
) -> str:
    """Require one storage identifier in its closed namespace."""

    if type(candidate) is not str or pattern.fullmatch(candidate) is None:
        raise ValueError(f"Flex Practitioner {label} is invalid")
    return candidate


def _acquisition_id(
    *,
    cohort_id: str,
    acquisition_role: str,
    run_id: str,
    dataset_intent_id: str,
    expected_npi_count: int,
) -> str:
    if (
        type(acquisition_role) is not str
        or acquisition_role not in UHC_FLEX_PRACTITIONER_ACQUISITION_ROLES
    ):
        raise ValueError("Flex Practitioner acquisition role is invalid")
    if type(expected_npi_count) is not int or expected_npi_count < 1:
        raise ValueError("Flex Practitioner NPI count is invalid")
    identity_parts = (
        UHC_FLEX_PRACTITIONER_ACQUISITION_CONTRACT_ID,
        strict_identifier(cohort_id, COHORT_PATTERN, "cohort ID"),
        acquisition_role,
        UHC_FLEX_PRACTITIONER_SOURCE_ID,
        UHC_FLEX_PRACTITIONER_CONNECTOR_ID,
        UHC_FLEX_PRACTITIONER_QUERY_CONTRACT_ID,
        strict_identifier(run_id, RUN_PATTERN, "run ID"),
        strict_identifier(dataset_intent_id, INTENT_PATTERN, "dataset intent ID"),
        str(expected_npi_count),
        "false",
        "false",
    )
    digest = hashlib.sha256("\x1f".join(identity_parts).encode("utf-8"))
    return "pdufpa_" + digest.hexdigest()[:48]


@dataclass(frozen=True, slots=True)
class UHCFlexPractitionerAcquisitionIdentity:
    """Bind one baseline or candidate run to one immutable cohort intent."""

    acquisition_id: str
    cohort_id: str
    acquisition_role: str
    run_id: str
    dataset_intent_id: str
    expected_npi_count: int
    storage_contract_id: str = UHC_FLEX_PRACTITIONER_ACQUISITION_CONTRACT_ID
    source_id: str = UHC_FLEX_PRACTITIONER_SOURCE_ID
    connector_id: str = UHC_FLEX_PRACTITIONER_CONNECTOR_ID
    query_contract_id: str = UHC_FLEX_PRACTITIONER_QUERY_CONTRACT_ID
    endpoint_collection_complete: bool = False
    endpoint_complete: bool = False

    def __post_init__(self) -> None:
        if (
            type(self.acquisition_role) is not str
            or self.acquisition_role
            not in UHC_FLEX_PRACTITIONER_ACQUISITION_ROLES
            or type(self.expected_npi_count) is not int
            or self.expected_npi_count < 1
            or self.expected_npi_count > (1 << 63) - 1
            or self.storage_contract_id
            != UHC_FLEX_PRACTITIONER_ACQUISITION_CONTRACT_ID
            or self.source_id != UHC_FLEX_PRACTITIONER_SOURCE_ID
            or self.connector_id != UHC_FLEX_PRACTITIONER_CONNECTOR_ID
            or self.query_contract_id
            != UHC_FLEX_PRACTITIONER_QUERY_CONTRACT_ID
            or self.endpoint_collection_complete is not False
            or self.endpoint_complete is not False
        ):
            raise ValueError("Flex Practitioner acquisition identity is invalid")
        expected_id = _acquisition_id(
            cohort_id=self.cohort_id,
            acquisition_role=self.acquisition_role,
            run_id=self.run_id,
            dataset_intent_id=self.dataset_intent_id,
            expected_npi_count=self.expected_npi_count,
        )
        if (
            type(self.acquisition_id) is not str
            or ACQUISITION_PATTERN.fullmatch(self.acquisition_id) is None
            or self.acquisition_id != expected_id
        ):
            raise ValueError("Flex Practitioner acquisition identity is invalid")


def build_uhc_flex_practitioner_acquisition_identity(
    cohort: UHCFlexOfficialNPICohort,
    *,
    acquisition_role: str,
    run_id: str,
    dataset_intent_id: str,
) -> UHCFlexPractitionerAcquisitionIdentity:
    """Build the identity independently recomputed by the database guard."""

    if type(cohort) is not UHCFlexOfficialNPICohort:
        raise ValueError("Flex Practitioner cohort is invalid")
    acquisition_id = _acquisition_id(
        cohort_id=cohort.cohort_id,
        acquisition_role=acquisition_role,
        run_id=run_id,
        dataset_intent_id=dataset_intent_id,
        expected_npi_count=cohort.npi_count,
    )
    return UHCFlexPractitionerAcquisitionIdentity(
        acquisition_id=acquisition_id,
        cohort_id=cohort.cohort_id,
        acquisition_role=acquisition_role,
        run_id=run_id,
        dataset_intent_id=dataset_intent_id,
        expected_npi_count=cohort.npi_count,
    )


@dataclass(frozen=True, slots=True, repr=False)
class UHCFlexPractitionerWorkClaim:
    """Identify one exact live lease generation without response data."""

    acquisition_id: str
    cohort_id: str
    requested_npi: int
    attempt: int
    lease_token: str = ""

    def __post_init__(self) -> None:
        try:
            canonical_npi = canonical_uhc_flex_npi(self.requested_npi)
        except ValueError:
            canonical_npi = None
        if (
            type(self.acquisition_id) is not str
            or ACQUISITION_PATTERN.fullmatch(self.acquisition_id) is None
            or type(self.cohort_id) is not str
            or COHORT_PATTERN.fullmatch(self.cohort_id) is None
            or canonical_npi != self.requested_npi
            or type(self.attempt) is not int
            or self.attempt < 1
            or type(self.lease_token) is not str
            or HASH_PATTERN.fullmatch(self.lease_token) is None
        ):
            raise ValueError("Flex Practitioner work claim is invalid")


@dataclass(frozen=True, slots=True)
class UHCFlexPractitionerAcquisitionSummary:
    """Return one sealed exact or retry-exhausted cohort census."""

    acquisition_id: str
    expected_npi_count: int
    matched_count: int
    unmatched_count: int
    error_count: int
    resource_count: int
    terminal_set_sha256: str
    cohort_complete: bool
    endpoint_collection_complete: bool
    endpoint_complete: bool

    def __post_init__(self) -> None:
        counts = (
            self.expected_npi_count,
            self.matched_count,
            self.unmatched_count,
            self.error_count,
            self.resource_count,
        )
        if (
            type(self.acquisition_id) is not str
            or ACQUISITION_PATTERN.fullmatch(self.acquisition_id) is None
            or any(type(count) is not int or count < 0 for count in counts)
            or self.expected_npi_count < 1
            or self.matched_count + self.unmatched_count + self.error_count
            != self.expected_npi_count
            or type(self.terminal_set_sha256) is not str
            or HASH_PATTERN.fullmatch(self.terminal_set_sha256) is None
            or self.cohort_complete is not (self.error_count == 0)
            or self.endpoint_collection_complete is not False
            or self.endpoint_complete is not False
        ):
            raise ValueError("Flex Practitioner acquisition summary is invalid")


@dataclass(frozen=True, slots=True, repr=False)
class UHCFlexPractitionerResourceRow:
    """One bounded, manifest-ready canonical resource payload."""

    requested_npi: int
    resource_id: str
    payload_sha256: str
    payload_json_text: str

    def __post_init__(self) -> None:
        try:
            canonical_npi = canonical_uhc_flex_npi(self.requested_npi)
            payload = json.loads(self.payload_json_text)
        except (MemoryError, RecursionError, TypeError, UnicodeError, ValueError):
            canonical_npi = None
            payload = None
        payload_hash = (
            hashlib.sha256(self.payload_json_text.encode("utf-8")).hexdigest()
            if type(self.payload_json_text) is str
            else None
        )
        if (
            canonical_npi != self.requested_npi
            or type(self.resource_id) is not str
            or re.fullmatch(r"[A-Za-z0-9.-]{1,64}", self.resource_id) is None
            or type(self.payload_sha256) is not str
            or HASH_PATTERN.fullmatch(self.payload_sha256) is None
            or payload_hash != self.payload_sha256
            or type(payload) is not dict
            or payload.get("resourceType") != "Practitioner"
            or payload.get("id") != self.resource_id
        ):
            raise ValueError("Flex Practitioner resource row is invalid")


def terminal_record_sha256(
    claim: UHCFlexPractitionerWorkClaim,
    *,
    status: str,
    result_sha256: str | None,
    resource_count: int,
    error_code: str | None,
) -> str:
    """Hash one canonical terminal work record."""

    fields = (
        UHC_FLEX_PRACTITIONER_TERMINAL_RECORD_CONTRACT_ID,
        str(claim.requested_npi),
        status,
        result_sha256 or "",
        str(resource_count),
        error_code or "",
    )
    return hashlib.sha256("\x1f".join(fields).encode("utf-8")).hexdigest()


def canonical_resource_fields_list(
    query_result: UHCFlexPractitionerQueryResult,
) -> list[dict[str, object]]:
    """Return at most sixteen canonical payload rows for set-wise insertion."""

    payloads = query_result.resource_payloads()
    hash_by_resource_id = dict(query_result.resource_sha256_by_id)
    resource_fields_list: list[dict[str, object]] = []
    for resource_id, practitioner_payload in zip(
        query_result.resource_ids,
        payloads,
        strict=True,
    ):
        payload_text = json.dumps(
            practitioner_payload,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        payload_sha256 = hashlib.sha256(payload_text.encode("utf-8")).hexdigest()
        if hash_by_resource_id.get(resource_id) != payload_sha256:
            raise UHCFlexPractitionerStoreError("state")
        resource_fields_list.append(
            {
                "resource_id": resource_id,
                "payload_sha256": payload_sha256,
                "payload_json_text": payload_text,
            }
        )
    return resource_fields_list


__all__ = (
    "build_uhc_flex_practitioner_acquisition_identity",
    "canonical_resource_fields_list",
    "strict_identifier",
    "terminal_record_sha256",
    "ACQUISITION_PATTERN",
    "COHORT_PATTERN",
    "ERROR_PATTERN",
    "UHCFlexPractitionerAcquisitionIdentity",
    "UHCFlexPractitionerAcquisitionSummary",
    "UHCFlexPractitionerResourceRow",
    "UHCFlexPractitionerStoreError",
    "UHCFlexPractitionerWorkClaim",
    "UHC_FLEX_PRACTITIONER_ACQUISITION_CONTRACT_ID",
    "UHC_FLEX_PRACTITIONER_ACQUISITION_ROLES",
    "UHC_FLEX_PRACTITIONER_TERMINAL_RECORD_CONTRACT_ID",
)
