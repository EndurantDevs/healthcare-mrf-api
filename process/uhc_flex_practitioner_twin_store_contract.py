# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded identities and results for sealed Practitioner twin admission."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import re

from process.uhc_flex_practitioner_store_contract import (
    ACQUISITION_PATTERN,
    COHORT_PATTERN,
    HASH_PATTERN,
    INTENT_PATTERN,
    RUN_PATTERN,
    UHC_FLEX_PRACTITIONER_ACQUISITION_CONTRACT_ID,
)
from process.uhc_flex_practitioner_twin_identity import (
    ADMISSION_PATTERN,
    ATTEMPT_PATTERN,
    UHC_FLEX_PRACTITIONER_DATASET_INTENT_DOMAIN,
    UHC_FLEX_PRACTITIONER_RUN_DOMAIN,
    UHC_FLEX_PRACTITIONER_TWIN_ADMISSION_CONTRACT_ID,
    UHC_FLEX_PRACTITIONER_TWIN_ATTEMPT_CONTRACT_ID,
    build_uhc_flex_practitioner_dataset_intent_id,
    build_uhc_flex_practitioner_run_id,
    canonical_semantic_projection_as_of,
    digest_identifier,
)


class UHCFlexPractitionerTwinStoreError(RuntimeError):
    """Expose one bounded comparison failure without response or NPI data."""

    def __init__(self, code: str = "state") -> None:
        message_by_code = {
            "identity": "Flex Practitioner twin identity is invalid",
            "mismatch": "Flex Practitioner twin comparison did not match",
            "missing": "Flex Practitioner twin admission is missing",
            "state": "Flex Practitioner twin state is invalid",
        }
        self.code = code if code in message_by_code else "state"
        super().__init__(message_by_code[self.code])


@dataclass(frozen=True, slots=True, repr=False)
class UHCFlexPractitionerSealedRoot:
    """Bounded sealed header fields used to compare one acquisition root."""

    acquisition_id: str
    cohort_id: str
    acquisition_role: str
    source_id: str
    connector_id: str
    query_contract_id: str
    storage_contract_id: str
    run_id: str
    dataset_intent_id: str
    expected_npi_count: int
    resource_count: int
    terminal_set_sha256: str

    def __post_init__(self) -> None:
        if (
            type(self.acquisition_id) is not str
            or ACQUISITION_PATTERN.fullmatch(self.acquisition_id) is None
            or type(self.cohort_id) is not str
            or COHORT_PATTERN.fullmatch(self.cohort_id) is None
            or self.acquisition_role not in {"baseline", "candidate"}
            or type(self.source_id) is not str
            or not 1 <= len(self.source_id) <= 64
            or type(self.connector_id) is not str
            or not 1 <= len(self.connector_id) <= 64
            or type(self.query_contract_id) is not str
            or not 1 <= len(self.query_contract_id) <= 96
            or self.storage_contract_id
            != UHC_FLEX_PRACTITIONER_ACQUISITION_CONTRACT_ID
            or type(self.run_id) is not str
            or RUN_PATTERN.fullmatch(self.run_id) is None
            or type(self.dataset_intent_id) is not str
            or INTENT_PATTERN.fullmatch(self.dataset_intent_id) is None
            or type(self.expected_npi_count) is not int
            or self.expected_npi_count < 1
            or type(self.resource_count) is not int
            or self.resource_count < 0
            or type(self.terminal_set_sha256) is not str
            or HASH_PATTERN.fullmatch(self.terminal_set_sha256) is None
        ):
            raise ValueError("Flex Practitioner sealed root is invalid")


def _validated_pair_context(
    baseline: UHCFlexPractitionerSealedRoot,
    candidate: UHCFlexPractitionerSealedRoot,
    *,
    semantic_projection_as_of: str,
    operation_key: str,
) -> tuple[str, bool]:
    if (
        type(baseline) is not UHCFlexPractitionerSealedRoot
        or type(candidate) is not UHCFlexPractitionerSealedRoot
        or baseline.acquisition_role != "baseline"
        or candidate.acquisition_role != "candidate"
        or baseline.acquisition_id == candidate.acquisition_id
        or baseline.run_id == candidate.run_id
    ):
        raise UHCFlexPractitionerTwinStoreError("identity")
    shared_fields = (
        "cohort_id",
        "dataset_intent_id",
        "source_id",
        "connector_id",
        "query_contract_id",
        "storage_contract_id",
        "expected_npi_count",
    )
    if any(
        getattr(baseline, field_name) != getattr(candidate, field_name)
        for field_name in shared_fields
    ):
        raise UHCFlexPractitionerTwinStoreError("identity")
    projection_date = canonical_semantic_projection_as_of(
        semantic_projection_as_of
    )
    expected_intent_id = build_uhc_flex_practitioner_dataset_intent_id(
        baseline.cohort_id,
        projection_date,
        operation_key,
    )
    if (
        baseline.dataset_intent_id != expected_intent_id
        or baseline.run_id
        != build_uhc_flex_practitioner_run_id(expected_intent_id, "baseline")
        or candidate.run_id
        != build_uhc_flex_practitioner_run_id(expected_intent_id, "candidate")
    ):
        raise UHCFlexPractitionerTwinStoreError("identity")
    is_matched = bool(
        baseline.terminal_set_sha256 == candidate.terminal_set_sha256
        and baseline.resource_count == candidate.resource_count
    )
    return projection_date, is_matched


def _attempt_identity_fields(
    baseline: UHCFlexPractitionerSealedRoot,
    candidate: UHCFlexPractitionerSealedRoot,
    projection_date: str,
    operation_key: str,
    is_matched: bool,
) -> tuple[object, ...]:
    return (
        UHC_FLEX_PRACTITIONER_TWIN_ATTEMPT_CONTRACT_ID,
        projection_date,
        operation_key,
        baseline.acquisition_id,
        candidate.acquisition_id,
        baseline.cohort_id,
        baseline.dataset_intent_id,
        baseline.source_id,
        baseline.connector_id,
        baseline.query_contract_id,
        baseline.storage_contract_id,
        baseline.run_id,
        candidate.run_id,
        baseline.expected_npi_count,
        baseline.terminal_set_sha256,
        candidate.terminal_set_sha256,
        baseline.resource_count,
        candidate.resource_count,
        str(is_matched).lower(),
    )


def build_uhc_flex_practitioner_twin_attempt(
    baseline: UHCFlexPractitionerSealedRoot,
    candidate: UHCFlexPractitionerSealedRoot,
    *,
    semantic_projection_as_of: str,
    operation_key: str,
    attempted_at: datetime,
) -> "UHCFlexPractitionerTwinAttempt":
    """Build the exact comparison identity independently checked in SQL."""

    projection_date, is_matched = _validated_pair_context(
        baseline,
        candidate,
        semantic_projection_as_of=semantic_projection_as_of,
        operation_key=operation_key,
    )
    identity_fields = _attempt_identity_fields(
        baseline,
        candidate,
        projection_date,
        operation_key,
        is_matched,
    )
    return UHCFlexPractitionerTwinAttempt(
        attempt_id=digest_identifier("pdufpta_", identity_fields),
        semantic_projection_as_of=projection_date,
        operation_key=operation_key,
        baseline_acquisition_id=baseline.acquisition_id,
        candidate_acquisition_id=candidate.acquisition_id,
        cohort_id=baseline.cohort_id,
        dataset_intent_id=baseline.dataset_intent_id,
        source_id=baseline.source_id,
        connector_id=baseline.connector_id,
        query_contract_id=baseline.query_contract_id,
        storage_contract_id=baseline.storage_contract_id,
        baseline_run_id=baseline.run_id,
        candidate_run_id=candidate.run_id,
        expected_npi_count=baseline.expected_npi_count,
        baseline_terminal_set_sha256=baseline.terminal_set_sha256,
        candidate_terminal_set_sha256=candidate.terminal_set_sha256,
        baseline_resource_count=baseline.resource_count,
        candidate_resource_count=candidate.resource_count,
        matched=is_matched,
        attempted_at=attempted_at,
    )


def uhc_flex_practitioner_twin_attempt_id(
    baseline: UHCFlexPractitionerSealedRoot,
    candidate: UHCFlexPractitionerSealedRoot,
    *,
    semantic_projection_as_of: str,
    operation_key: str,
) -> str:
    """Return the pure attempt ID without binding it to a wall-clock time."""

    reference_time = datetime.fromisoformat("2000-01-01T00:00:00+00:00")
    return build_uhc_flex_practitioner_twin_attempt(
        baseline,
        candidate,
        semantic_projection_as_of=semantic_projection_as_of,
        operation_key=operation_key,
        attempted_at=reference_time,
    ).attempt_id


def _is_match(candidate: object, pattern: re.Pattern[str]) -> bool:
    return type(candidate) is str and pattern.fullmatch(candidate) is not None


def _is_bounded_text(candidate: object, maximum_length: int) -> bool:
    return type(candidate) is str and 1 <= len(candidate) <= maximum_length


def _has_valid_lineage(candidate: object) -> bool:
    try:
        projection_date = canonical_semantic_projection_as_of(
            getattr(candidate, "semantic_projection_as_of")
        )
        expected_intent_id = build_uhc_flex_practitioner_dataset_intent_id(
            getattr(candidate, "cohort_id"),
            projection_date,
            getattr(candidate, "operation_key"),
        )
    except (AttributeError, ValueError):
        return False
    return bool(
        getattr(candidate, "dataset_intent_id") == expected_intent_id
        and getattr(candidate, "baseline_run_id")
        == build_uhc_flex_practitioner_run_id(expected_intent_id, "baseline")
        and getattr(candidate, "candidate_run_id")
        == build_uhc_flex_practitioner_run_id(expected_intent_id, "candidate")
    )


def _has_valid_coordinates(candidate: object) -> bool:
    return bool(
        _is_match(getattr(candidate, "operation_key", None), HASH_PATTERN)
        and _is_match(
            getattr(candidate, "baseline_acquisition_id", None),
            ACQUISITION_PATTERN,
        )
        and _is_match(
            getattr(candidate, "candidate_acquisition_id", None),
            ACQUISITION_PATTERN,
        )
        and getattr(candidate, "baseline_acquisition_id")
        != getattr(candidate, "candidate_acquisition_id")
        and _is_match(getattr(candidate, "cohort_id", None), COHORT_PATTERN)
        and _is_match(getattr(candidate, "dataset_intent_id", None), INTENT_PATTERN)
        and _is_match(getattr(candidate, "baseline_run_id", None), RUN_PATTERN)
        and _is_match(getattr(candidate, "candidate_run_id", None), RUN_PATTERN)
        and getattr(candidate, "baseline_run_id")
        != getattr(candidate, "candidate_run_id")
        and _has_valid_lineage(candidate)
    )


def _has_bounded_contracts(candidate: object) -> bool:
    return all(
        _is_bounded_text(field_value, maximum_length)
        for field_value, maximum_length in (
            (getattr(candidate, "source_id", None), 64),
            (getattr(candidate, "connector_id", None), 64),
            (getattr(candidate, "query_contract_id", None), 96),
            (getattr(candidate, "storage_contract_id", None), 96),
        )
    )


def _is_valid_attempt(attempt: object) -> bool:
    baseline_hash = getattr(attempt, "baseline_terminal_set_sha256", None)
    candidate_hash = getattr(attempt, "candidate_terminal_set_sha256", None)
    baseline_count = getattr(attempt, "baseline_resource_count", None)
    candidate_count = getattr(attempt, "candidate_resource_count", None)
    return bool(
        _is_match(getattr(attempt, "attempt_id", None), ATTEMPT_PATTERN)
        and _has_valid_coordinates(attempt)
        and _has_bounded_contracts(attempt)
        and type(getattr(attempt, "expected_npi_count", None)) is int
        and getattr(attempt, "expected_npi_count") > 0
        and _is_match(baseline_hash, HASH_PATTERN)
        and _is_match(candidate_hash, HASH_PATTERN)
        and type(baseline_count) is int
        and baseline_count >= 0
        and type(candidate_count) is int
        and candidate_count >= 0
        and type(getattr(attempt, "matched", None)) is bool
        and getattr(attempt, "matched")
        == (baseline_hash == candidate_hash and baseline_count == candidate_count)
        and type(getattr(attempt, "attempted_at", None)) is datetime
        and getattr(attempt, "attempted_at").tzinfo is not None
        and getattr(attempt, "attempt_contract_id", None)
        == UHC_FLEX_PRACTITIONER_TWIN_ATTEMPT_CONTRACT_ID
    )


@dataclass(frozen=True, slots=True, repr=False)
class UHCFlexPractitionerTwinAttempt:
    """Immutable result for one consumed baseline and candidate pair."""

    attempt_id: str
    semantic_projection_as_of: str
    operation_key: str
    baseline_acquisition_id: str
    candidate_acquisition_id: str
    cohort_id: str
    dataset_intent_id: str
    source_id: str
    connector_id: str
    query_contract_id: str
    storage_contract_id: str
    baseline_run_id: str
    candidate_run_id: str
    expected_npi_count: int
    baseline_terminal_set_sha256: str
    candidate_terminal_set_sha256: str
    baseline_resource_count: int
    candidate_resource_count: int
    matched: bool
    attempted_at: datetime
    attempt_contract_id: str = UHC_FLEX_PRACTITIONER_TWIN_ATTEMPT_CONTRACT_ID

    def __post_init__(self) -> None:
        if not _is_valid_attempt(self):
            raise ValueError("Flex Practitioner twin attempt is invalid")


def build_uhc_flex_practitioner_twin_admission(
    attempt: UHCFlexPractitionerTwinAttempt,
    *,
    admitted_at: datetime,
) -> "UHCFlexPractitionerTwinAdmission":
    """Build publication authority only from one exact matched attempt."""

    if type(attempt) is not UHCFlexPractitionerTwinAttempt or not attempt.matched:
        raise UHCFlexPractitionerTwinStoreError("mismatch")
    admission_id = uhc_flex_practitioner_twin_admission_id(attempt)
    return UHCFlexPractitionerTwinAdmission(
        admission_id=admission_id,
        semantic_projection_as_of=attempt.semantic_projection_as_of,
        operation_key=attempt.operation_key,
        attempt_id=attempt.attempt_id,
        baseline_acquisition_id=attempt.baseline_acquisition_id,
        candidate_acquisition_id=attempt.candidate_acquisition_id,
        cohort_id=attempt.cohort_id,
        dataset_intent_id=attempt.dataset_intent_id,
        source_id=attempt.source_id,
        connector_id=attempt.connector_id,
        query_contract_id=attempt.query_contract_id,
        storage_contract_id=attempt.storage_contract_id,
        baseline_run_id=attempt.baseline_run_id,
        candidate_run_id=attempt.candidate_run_id,
        expected_npi_count=attempt.expected_npi_count,
        terminal_set_sha256=attempt.candidate_terminal_set_sha256,
        resource_count=attempt.candidate_resource_count,
        publication_authority=True,
        admitted_at=admitted_at,
    )


def uhc_flex_practitioner_twin_admission_id(
    attempt: UHCFlexPractitionerTwinAttempt,
) -> str:
    """Return the pure matched-publication authority ID."""

    if type(attempt) is not UHCFlexPractitionerTwinAttempt or not attempt.matched:
        raise UHCFlexPractitionerTwinStoreError("mismatch")
    identity_fields = (
        UHC_FLEX_PRACTITIONER_TWIN_ADMISSION_CONTRACT_ID,
        attempt.semantic_projection_as_of,
        attempt.operation_key,
        attempt.attempt_id,
        attempt.baseline_acquisition_id,
        attempt.candidate_acquisition_id,
        attempt.cohort_id,
        attempt.dataset_intent_id,
        attempt.source_id,
        attempt.connector_id,
        attempt.query_contract_id,
        attempt.storage_contract_id,
        attempt.baseline_run_id,
        attempt.candidate_run_id,
        attempt.expected_npi_count,
        attempt.candidate_terminal_set_sha256,
        attempt.candidate_resource_count,
        "true",
    )
    return digest_identifier("pdufpad_", identity_fields)


def _is_valid_admission(admission: object) -> bool:
    admitted_at = getattr(admission, "admitted_at", None)
    return bool(
        _is_match(getattr(admission, "admission_id", None), ADMISSION_PATTERN)
        and _is_match(getattr(admission, "attempt_id", None), ATTEMPT_PATTERN)
        and _has_valid_coordinates(admission)
        and _has_bounded_contracts(admission)
        and type(getattr(admission, "expected_npi_count", None)) is int
        and getattr(admission, "expected_npi_count") > 0
        and _is_match(
            getattr(admission, "terminal_set_sha256", None),
            HASH_PATTERN,
        )
        and type(getattr(admission, "resource_count", None)) is int
        and getattr(admission, "resource_count") >= 0
        and getattr(admission, "publication_authority", None) is True
        and type(admitted_at) is datetime
        and admitted_at.tzinfo is not None
        and getattr(admission, "admission_contract_id", None)
        == UHC_FLEX_PRACTITIONER_TWIN_ADMISSION_CONTRACT_ID
    )


@dataclass(frozen=True, slots=True, repr=False)
class UHCFlexPractitionerTwinAdmission:
    """Bounded immutable authority for one matched candidate root."""

    admission_id: str
    semantic_projection_as_of: str
    operation_key: str
    attempt_id: str
    baseline_acquisition_id: str
    candidate_acquisition_id: str
    cohort_id: str
    dataset_intent_id: str
    source_id: str
    connector_id: str
    query_contract_id: str
    storage_contract_id: str
    baseline_run_id: str
    candidate_run_id: str
    expected_npi_count: int
    terminal_set_sha256: str
    resource_count: int
    publication_authority: bool
    admitted_at: datetime
    admission_contract_id: str = UHC_FLEX_PRACTITIONER_TWIN_ADMISSION_CONTRACT_ID

    def __post_init__(self) -> None:
        if not _is_valid_admission(self):
            raise ValueError("Flex Practitioner twin admission is invalid")


__all__ = (
    "build_uhc_flex_practitioner_dataset_intent_id",
    "build_uhc_flex_practitioner_run_id",
    "build_uhc_flex_practitioner_twin_admission",
    "build_uhc_flex_practitioner_twin_attempt",
    "canonical_semantic_projection_as_of",
    "ADMISSION_PATTERN",
    "ATTEMPT_PATTERN",
    "UHCFlexPractitionerSealedRoot",
    "UHCFlexPractitionerTwinAdmission",
    "UHCFlexPractitionerTwinAttempt",
    "UHCFlexPractitionerTwinStoreError",
    "UHC_FLEX_PRACTITIONER_TWIN_ADMISSION_CONTRACT_ID",
    "UHC_FLEX_PRACTITIONER_TWIN_ATTEMPT_CONTRACT_ID",
    "UHC_FLEX_PRACTITIONER_DATASET_INTENT_DOMAIN",
    "UHC_FLEX_PRACTITIONER_RUN_DOMAIN",
    "uhc_flex_practitioner_twin_admission_id",
    "uhc_flex_practitioner_twin_attempt_id",
)
