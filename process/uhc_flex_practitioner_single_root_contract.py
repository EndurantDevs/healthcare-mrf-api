# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed identities for reviewed single-root Flex Practitioner admission."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from types import SimpleNamespace
from typing import Any

from process.provider_directory_fhir_root_policy import ReviewedRootPolicy
from process.uhc_flex_official_cohort_contract import UHCFlexOfficialNPICohort
from process.uhc_flex_practitioner_acquisition_contract import (
    UHCFlexPractitionerRootReceipt,
    strict_nonnegative_seconds,
)
from process.uhc_flex_practitioner_contract import (
    UHC_FLEX_PRACTITIONER_CONNECTOR_ID,
    UHC_FLEX_PRACTITIONER_QUERY_CONTRACT_ID,
    UHC_FLEX_PRACTITIONER_SOURCE_ID,
)
from process.uhc_flex_practitioner_registration import (
    UHCFlexPractitionerRegistrationResult,
)
from process.uhc_flex_practitioner_store_contract import (
    _acquisition_id,
    ACQUISITION_PATTERN,
    build_uhc_flex_practitioner_acquisition_identity,
    COHORT_PATTERN,
    HASH_PATTERN,
    INTENT_PATTERN,
    RUN_PATTERN,
    UHCFlexPractitionerAcquisitionIdentity,
    UHCFlexPractitionerAcquisitionSummary,
    UHC_FLEX_PRACTITIONER_ACQUISITION_CONTRACT_ID,
)
from process.uhc_flex_practitioner_twin_identity import (
    ADMISSION_PATTERN,
    canonical_semantic_projection_as_of,
    digest_identifier,
)
from process.uhc_flex_practitioner_twin_store_contract import (
    UHCFlexPractitionerSealedRoot,
    UHCFlexPractitionerTwinAdmission,
)


UHC_FLEX_PRACTITIONER_SINGLE_ROOT_ADMISSION_CONTRACT_ID = (
    "healthporta.provider-directory.uhc-flex-practitioner-"
    "reviewed-single-root-admission.v1"
)
UHC_FLEX_PRACTITIONER_SINGLE_ROOT_DATASET_INTENT_DOMAIN = (
    "healthporta.provider-directory.uhc-flex-practitioner-"
    "reviewed-single-root-dataset-intent.v1"
)
UHC_FLEX_PRACTITIONER_SINGLE_ROOT_RUN_DOMAIN = (
    "healthporta.provider-directory.uhc-flex-practitioner-"
    "reviewed-single-root-acquisition-run.v1"
)
UHC_FLEX_PRACTITIONER_SINGLE_ROOT_POLICY = ReviewedRootPolicy(1)


class UHCFlexPractitionerSingleRootError(RuntimeError):
    """Expose a bounded single-root failure without member or payload data."""

    def __init__(self, code: str = "state") -> None:
        message_by_code = {
            "cohort_drift": "Flex Practitioner official cohort changed",
            "disabled": "Flex Practitioner single-root acquisition is disabled",
            "identity": "Flex Practitioner single-root identity is invalid",
            "missing": "Flex Practitioner single-root admission is missing",
            "source_drift": "Flex Practitioner exact source changed",
            "state": "Flex Practitioner single-root state is invalid",
        }
        self.code = code if code in message_by_code else "state"
        super().__init__(message_by_code[self.code])


def _single_root_admission_id(admission: object) -> str:
    return digest_identifier(
        "pdufpad_",
        (
            UHC_FLEX_PRACTITIONER_SINGLE_ROOT_ADMISSION_CONTRACT_ID,
            admission.semantic_projection_as_of,
            admission.operation_key,
            admission.candidate_acquisition_id,
            admission.cohort_id,
            admission.dataset_intent_id,
            admission.source_id,
            admission.connector_id,
            admission.query_contract_id,
            admission.storage_contract_id,
            admission.candidate_run_id,
            admission.expected_npi_count,
            admission.terminal_set_sha256,
            admission.resource_count,
            UHC_FLEX_PRACTITIONER_SINGLE_ROOT_POLICY.policy_version,
            UHC_FLEX_PRACTITIONER_SINGLE_ROOT_POLICY.required_root_count,
            "true",
        ),
    )


def single_root_dataset_intent_id(
    cohort_id: str,
    semantic_projection_as_of: str,
    operation_key: str,
) -> str:
    """Derive one restart-safe intent outside the retired twin namespace."""

    projection_date = canonical_semantic_projection_as_of(
        semantic_projection_as_of
    )
    if (
        type(cohort_id) is not str
        or COHORT_PATTERN.fullmatch(cohort_id) is None
        or type(operation_key) is not str
        or HASH_PATTERN.fullmatch(operation_key) is None
    ):
        raise ValueError("Flex Practitioner single-root identity is invalid")
    return digest_identifier(
        "pdufdi_",
        (
            UHC_FLEX_PRACTITIONER_SINGLE_ROOT_DATASET_INTENT_DOMAIN,
            cohort_id,
            projection_date,
            operation_key,
        ),
    )


def single_root_run_id(dataset_intent_id: str) -> str:
    """Derive the sole candidate run for one reviewed single-root intent."""

    if (
        type(dataset_intent_id) is not str
        or INTENT_PATTERN.fullmatch(dataset_intent_id) is None
    ):
        raise ValueError("Flex Practitioner single-root run identity is invalid")
    return digest_identifier(
        "pdufpr_",
        (
            UHC_FLEX_PRACTITIONER_SINGLE_ROOT_RUN_DOMAIN,
            dataset_intent_id,
            "candidate",
        ),
    )


@dataclass(frozen=True, slots=True)
class UHCFlexPractitionerSingleRootContext:
    """Inputs and sole acquisition identity for one reviewed candidate."""

    registration: UHCFlexPractitionerRegistrationResult
    cohort: UHCFlexOfficialNPICohort
    candidate_identity: UHCFlexPractitionerAcquisitionIdentity
    dataset_intent_id: str
    semantic_projection_as_of: str
    operation_key: str


def build_single_root_context(
    registration: UHCFlexPractitionerRegistrationResult,
    cohort: UHCFlexOfficialNPICohort,
    semantic_projection_as_of: str,
    operation_key: str,
) -> UHCFlexPractitionerSingleRootContext:
    """Build only the candidate identity in the distinct single-root domain."""

    dataset_intent_id = single_root_dataset_intent_id(
        cohort.cohort_id,
        semantic_projection_as_of,
        operation_key,
    )
    candidate_identity = build_uhc_flex_practitioner_acquisition_identity(
        cohort,
        acquisition_role="candidate",
        run_id=single_root_run_id(dataset_intent_id),
        dataset_intent_id=dataset_intent_id,
    )
    return UHCFlexPractitionerSingleRootContext(
        registration=registration,
        cohort=cohort,
        candidate_identity=candidate_identity,
        dataset_intent_id=dataset_intent_id,
        semantic_projection_as_of=semantic_projection_as_of,
        operation_key=operation_key,
    )


@dataclass(frozen=True, slots=True, repr=False)
class UHCFlexPractitionerSingleRootAdmission:
    """Immutable authority for one exact reviewed candidate root."""

    admission_id: str
    semantic_projection_as_of: str
    operation_key: str
    candidate_acquisition_id: str
    cohort_id: str
    dataset_intent_id: str
    source_id: str
    connector_id: str
    query_contract_id: str
    storage_contract_id: str
    candidate_run_id: str
    expected_npi_count: int
    terminal_set_sha256: str
    resource_count: int
    publication_authority: bool
    admitted_at: datetime
    reviewed_root_policy_json: dict[str, Any]
    attempt_id: None = None
    baseline_acquisition_id: None = None
    baseline_run_id: None = None
    admission_contract_id: str = (
        UHC_FLEX_PRACTITIONER_SINGLE_ROOT_ADMISSION_CONTRACT_ID
    )

    def __post_init__(self) -> None:
        """Reject any field or digest outside the closed admission identity."""

        projection_date = canonical_semantic_projection_as_of(
            self.semantic_projection_as_of
        )
        expected_intent = single_root_dataset_intent_id(
            self.cohort_id,
            projection_date,
            self.operation_key,
        )
        if (
            ADMISSION_PATTERN.fullmatch(self.admission_id) is None
            or self.admission_id != _single_root_admission_id(self)
            or ACQUISITION_PATTERN.fullmatch(self.candidate_acquisition_id) is None
            or self.candidate_acquisition_id
            != _acquisition_id(
                cohort_id=self.cohort_id,
                acquisition_role="candidate",
                run_id=self.candidate_run_id,
                dataset_intent_id=self.dataset_intent_id,
                expected_npi_count=self.expected_npi_count,
            )
            or self.dataset_intent_id != expected_intent
            or self.candidate_run_id != single_root_run_id(expected_intent)
            or self.source_id != UHC_FLEX_PRACTITIONER_SOURCE_ID
            or self.connector_id != UHC_FLEX_PRACTITIONER_CONNECTOR_ID
            or self.query_contract_id
            != UHC_FLEX_PRACTITIONER_QUERY_CONTRACT_ID
            or self.storage_contract_id
            != UHC_FLEX_PRACTITIONER_ACQUISITION_CONTRACT_ID
            or type(self.expected_npi_count) is not int
            or self.expected_npi_count < 1
            or HASH_PATTERN.fullmatch(self.terminal_set_sha256) is None
            or type(self.resource_count) is not int
            or self.resource_count < 0
            or self.publication_authority is not True
            or type(self.admitted_at) is not datetime
            or self.admitted_at.tzinfo is None
            or type(self.reviewed_root_policy_json) is not dict
            or self.reviewed_root_policy_json
            != UHC_FLEX_PRACTITIONER_SINGLE_ROOT_POLICY.document()
            or self.attempt_id is not None
            or self.baseline_acquisition_id is not None
            or self.baseline_run_id is not None
            or self.admission_contract_id
            != UHC_FLEX_PRACTITIONER_SINGLE_ROOT_ADMISSION_CONTRACT_ID
        ):
            raise ValueError("Flex Practitioner single-root admission is invalid")


def build_single_root_admission(
    candidate: UHCFlexPractitionerSealedRoot,
    *,
    semantic_projection_as_of: str,
    operation_key: str,
    admitted_at: datetime,
) -> UHCFlexPractitionerSingleRootAdmission:
    """Build the exact admission identity independently checked by PostgreSQL."""

    expected_acquisition_id = _acquisition_id(
        cohort_id=candidate.cohort_id,
        acquisition_role="candidate",
        run_id=candidate.run_id,
        dataset_intent_id=candidate.dataset_intent_id,
        expected_npi_count=candidate.expected_npi_count,
    )
    if (
        candidate.acquisition_role != "candidate"
        or candidate.acquisition_id != expected_acquisition_id
    ):
        raise UHCFlexPractitionerSingleRootError("identity")
    admission_by_field = dict(
        semantic_projection_as_of=semantic_projection_as_of,
        operation_key=operation_key,
        candidate_acquisition_id=candidate.acquisition_id,
        cohort_id=candidate.cohort_id,
        dataset_intent_id=candidate.dataset_intent_id,
        source_id=candidate.source_id,
        connector_id=candidate.connector_id,
        query_contract_id=candidate.query_contract_id,
        storage_contract_id=candidate.storage_contract_id,
        candidate_run_id=candidate.run_id,
        expected_npi_count=candidate.expected_npi_count,
        terminal_set_sha256=candidate.terminal_set_sha256,
        resource_count=candidate.resource_count,
        publication_authority=True,
        admitted_at=admitted_at,
        reviewed_root_policy_json=UHC_FLEX_PRACTITIONER_SINGLE_ROOT_POLICY.document(),
    )
    return UHCFlexPractitionerSingleRootAdmission(
        admission_id=_single_root_admission_id(SimpleNamespace(**admission_by_field)),
        **admission_by_field,
    )


@dataclass(frozen=True, slots=True, repr=False)
class UHCFlexPractitionerSingleRootReceipt:
    """Aggregate-only acquisition and admission proof; it publishes nothing."""

    operation_key: str = field(repr=False)
    semantic_projection_as_of: str
    source_id: str = field(repr=False)
    endpoint_id: str = field(repr=False)
    cohort_id: str = field(repr=False)
    official_dataset_id: str = field(repr=False)
    official_dataset_hash: str = field(repr=False)
    official_content_proof_sha256: str = field(repr=False)
    dataset_intent_id: str = field(repr=False)
    expected_npi_count: int
    candidate: UHCFlexPractitionerRootReceipt
    admission_id: str = field(repr=False)
    reviewed_root_policy_json: dict[str, Any]
    elapsed_seconds: float

    def __post_init__(self) -> None:
        if (
            self.source_id != UHC_FLEX_PRACTITIONER_SOURCE_ID
            or type(self.endpoint_id) is not str
            or HASH_PATTERN.fullmatch(self.endpoint_id) is None
            or self.dataset_intent_id
            != single_root_dataset_intent_id(
                self.cohort_id,
                self.semantic_projection_as_of,
                self.operation_key,
            )
            or type(self.official_dataset_id) is not str
            or not self.official_dataset_id
            or HASH_PATTERN.fullmatch(self.official_dataset_hash) is None
            or HASH_PATTERN.fullmatch(self.official_content_proof_sha256) is None
            or type(self.expected_npi_count) is not int
            or self.expected_npi_count < 1
            or type(self.candidate) is not UHCFlexPractitionerRootReceipt
            or self.candidate.acquisition_role != "candidate"
            or self.candidate.run_id != single_root_run_id(self.dataset_intent_id)
            or self.candidate.matched_count + self.candidate.unmatched_count
            != self.expected_npi_count
            or ADMISSION_PATTERN.fullmatch(self.admission_id) is None
            or self.reviewed_root_policy_json
            != UHC_FLEX_PRACTITIONER_SINGLE_ROOT_POLICY.document()
        ):
            raise ValueError("Flex Practitioner single-root receipt is invalid")
        strict_nonnegative_seconds(self.elapsed_seconds, "total timing")


def is_exact_single_root_admission(
    context: UHCFlexPractitionerSingleRootContext,
    candidate: UHCFlexPractitionerAcquisitionSummary,
    admission: object,
) -> bool:
    """Return whether admission is the exact authority for this sealed root."""

    return (
        type(admission) is UHCFlexPractitionerSingleRootAdmission
        and admission.candidate_acquisition_id == candidate.acquisition_id
        and admission.cohort_id == context.cohort.cohort_id
        and admission.dataset_intent_id == context.dataset_intent_id
        and admission.candidate_run_id == context.candidate_identity.run_id
        and admission.expected_npi_count == context.cohort.npi_count
        and admission.terminal_set_sha256 == candidate.terminal_set_sha256
        and admission.resource_count == candidate.resource_count
        and admission.reviewed_root_policy_json
        == UHC_FLEX_PRACTITIONER_SINGLE_ROOT_POLICY.document()
        and admission.publication_authority is True
    )


def build_single_root_receipt(
    context: UHCFlexPractitionerSingleRootContext,
    candidate: UHCFlexPractitionerRootReceipt,
    admission: UHCFlexPractitionerSingleRootAdmission,
    elapsed_seconds: float,
) -> UHCFlexPractitionerSingleRootReceipt:
    """Build aggregate-only proof after exact admission revalidation."""

    cohort = context.cohort
    return UHCFlexPractitionerSingleRootReceipt(
        operation_key=context.operation_key,
        semantic_projection_as_of=context.semantic_projection_as_of,
        source_id=context.registration.source_id,
        endpoint_id=context.registration.endpoint_id,
        cohort_id=cohort.cohort_id,
        official_dataset_id=cohort.official_dataset_id,
        official_dataset_hash=cohort.official_dataset_hash,
        official_content_proof_sha256=cohort.official_content_proof_sha256,
        dataset_intent_id=context.dataset_intent_id,
        expected_npi_count=cohort.npi_count,
        candidate=candidate,
        admission_id=admission.admission_id,
        reviewed_root_policy_json=admission.reviewed_root_policy_json,
        elapsed_seconds=elapsed_seconds,
    )


UHCFlexPractitionerAdmission = (
    UHCFlexPractitionerTwinAdmission | UHCFlexPractitionerSingleRootAdmission
)


__all__ = (
    "build_single_root_admission",
    "build_single_root_context",
    "build_single_root_receipt",
    "is_exact_single_root_admission",
    "single_root_dataset_intent_id",
    "single_root_run_id",
    "UHCFlexPractitionerAdmission",
    "UHCFlexPractitionerSingleRootAdmission",
    "UHCFlexPractitionerSingleRootContext",
    "UHCFlexPractitionerSingleRootError",
    "UHCFlexPractitionerSingleRootReceipt",
    "UHC_FLEX_PRACTITIONER_SINGLE_ROOT_ADMISSION_CONTRACT_ID",
    "UHC_FLEX_PRACTITIONER_SINGLE_ROOT_DATASET_INTENT_DOMAIN",
    "UHC_FLEX_PRACTITIONER_SINGLE_ROOT_POLICY",
    "UHC_FLEX_PRACTITIONER_SINGLE_ROOT_RUN_DOMAIN",
)
