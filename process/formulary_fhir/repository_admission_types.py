# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded immutable contracts for formulary twin admission."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from typing import Any

from process.formulary_fhir.repository_shared import DatasetVerification
from process.formulary_fhir.repository_shared import strict_hash
from process.formulary_fhir.repository_shared import strict_text
from process.formulary_fhir.repository_shared import utc_timestamp


ERROR_MESSAGES = {
    "admission": "FHIR formulary twin admission evidence is inconsistent",
    "attempt": "FHIR formulary twin attempt evidence is inconsistent",
    "evidence": "FHIR formulary twin content evidence is invalid",
    "independence": "FHIR formulary independent acquisition proof is invalid",
    "invalid_request": "FHIR formulary twin admission request is invalid",
    "missing": "FHIR formulary twin admission is required",
    "mismatch": "FHIR formulary twin acquisitions do not match",
    "pointer": "FHIR formulary twin admission predecessor is stale",
    "source": "FHIR formulary twin source configuration is invalid",
    "storage": "FHIR formulary twin admission storage failed",
}


class TwinAdmissionError(RuntimeError):
    """Expose a bounded admission failure without source or row details."""

    def __init__(self, code: str) -> None:
        self.code = code if code in ERROR_MESSAGES else "storage"
        super().__init__(ERROR_MESSAGES[self.code])


@dataclass(frozen=True, slots=True)
class AlternativeProof:
    """Count and hash every alternative edge in one exact dataset."""

    count: int
    evidence_hash: str = field(repr=False)

    def __post_init__(self) -> None:
        if type(self.count) is not int or self.count < 0:
            raise ValueError("FHIR formulary alternative proof count is invalid")
        strict_hash(self.evidence_hash, "alternative proof hash")


@dataclass(frozen=True, slots=True, repr=False)
class TwinAttemptResult:
    """Validated immutable evidence for one consumed acquisition pair."""

    source_id: str
    baseline_dataset_id: str
    baseline_run_id: str
    candidate_dataset_id: str
    candidate_run_id: str
    cutoff_at: dt.datetime
    source_configuration_hash: str = field(repr=False)
    acquisition_contract_hash: str = field(repr=False)
    baseline_evidence_hash: str = field(repr=False)
    candidate_evidence_hash: str = field(repr=False)
    matched: bool
    attempted_at: dt.datetime

    def __post_init__(self) -> None:
        strict_text(self.source_id, "source id", 64)
        for label, identifier_value in (
            ("baseline dataset id", self.baseline_dataset_id),
            ("baseline run id", self.baseline_run_id),
            ("candidate dataset id", self.candidate_dataset_id),
            ("candidate run id", self.candidate_run_id),
        ):
            strict_text(identifier_value, label, 64)
        if (
            self.baseline_dataset_id == self.candidate_dataset_id
            or self.baseline_run_id == self.candidate_run_id
        ):
            raise ValueError("FHIR formulary attempt identities are invalid")
        utc_timestamp(self.cutoff_at, "attempt cutoff")
        utc_timestamp(self.attempted_at, "attempt timestamp")
        for label, evidence_hash in (
            ("source configuration hash", self.source_configuration_hash),
            ("acquisition contract hash", self.acquisition_contract_hash),
            ("baseline evidence hash", self.baseline_evidence_hash),
            ("candidate evidence hash", self.candidate_evidence_hash),
        ):
            strict_hash(evidence_hash, label)
        if type(self.matched) is not bool or self.matched != (
            self.baseline_evidence_hash == self.candidate_evidence_hash
        ):
            raise ValueError("FHIR formulary attempt match state is invalid")

    def __repr__(self) -> str:
        return (
            "TwinAttemptResult("
            f"baseline_dataset_id={self.baseline_dataset_id!r}, "
            f"candidate_dataset_id={self.candidate_dataset_id!r}, "
            f"matched={self.matched!r}, attempted_at={self.attempted_at!r})"
        )


@dataclass(frozen=True, slots=True, repr=False)
class TwinAdmissionResult:
    """Validated immutable evidence read back from PostgreSQL."""

    source_id: str
    baseline_dataset_id: str
    baseline_run_id: str
    candidate_dataset_id: str
    candidate_run_id: str
    predecessor_dataset_id: str | None
    cutoff_at: dt.datetime
    source_configuration_hash: str = field(repr=False)
    acquisition_contract_hash: str = field(repr=False)
    verification: DatasetVerification
    alternative: AlternativeProof
    baseline_verified_at: dt.datetime
    candidate_verified_at: dt.datetime
    admitted_at: dt.datetime

    def __post_init__(self) -> None:
        strict_text(self.source_id, "source id", 64)
        for label, identifier_value in (
            ("baseline dataset id", self.baseline_dataset_id),
            ("baseline run id", self.baseline_run_id),
            ("candidate dataset id", self.candidate_dataset_id),
            ("candidate run id", self.candidate_run_id),
        ):
            strict_text(identifier_value, label, 64)
        if self.predecessor_dataset_id is not None:
            strict_text(self.predecessor_dataset_id, "predecessor dataset id", 64)
        strict_hash(self.source_configuration_hash, "source configuration hash")
        strict_hash(self.acquisition_contract_hash, "acquisition contract hash")
        cutoff_at = utc_timestamp(self.cutoff_at, "admission cutoff")
        baseline_at = utc_timestamp(
            self.baseline_verified_at,
            "baseline verification timestamp",
        )
        candidate_at = utc_timestamp(
            self.candidate_verified_at,
            "candidate verification timestamp",
        )
        admitted_at = utc_timestamp(self.admitted_at, "admission timestamp")
        verification = self.verification
        if not (
            type(verification) is DatasetVerification
            and verification.source_id == self.source_id
            and verification.dataset_id == self.candidate_dataset_id
            and type(verification.list_count) is int
            and verification.list_count > 0
            and type(verification.alias_count) is int
            and verification.alias_count > 0
            and type(verification.medication_membership_count) is int
            and verification.medication_membership_count > 0
        ):
            raise ValueError("FHIR formulary admission verification is invalid")
        strict_hash(verification.coverage_hash, "admission coverage hash")
        strict_hash(verification.membership_hash, "admission membership hash")
        if not baseline_at <= candidate_at <= admitted_at or cutoff_at != self.cutoff_at:
            raise ValueError("FHIR formulary admission timestamps are invalid")

    def __repr__(self) -> str:
        return (
            "TwinAdmissionResult("
            f"baseline_dataset_id={self.baseline_dataset_id!r}, "
            f"candidate_dataset_id={self.candidate_dataset_id!r}, "
            f"admitted_at={self.admitted_at!r})"
        )


def raise_admission_error(code: str) -> None:
    """Raise one fixed error while suppressing sensitive causes."""

    raise TwinAdmissionError(code) from None


def verification_values(verification: DatasetVerification) -> tuple[Any, ...]:
    """Return content fields that must match across independent roots."""

    return (
        verification.list_count,
        verification.alias_count,
        verification.medication_membership_count,
        verification.coverage_hash,
        verification.membership_hash,
    )


def result_from_row(admission_by_field: dict[str, Any]) -> TwinAdmissionResult:
    """Strictly decode one stored admission row."""

    verification = DatasetVerification(
        admission_by_field.get("source_id"),
        admission_by_field.get("candidate_dataset_id"),
        admission_by_field.get("list_count"),
        admission_by_field.get("alias_count"),
        admission_by_field.get("medication_count"),
        admission_by_field.get("coverage_hash"),
        admission_by_field.get("membership_hash"),
    )
    return TwinAdmissionResult(
        source_id=admission_by_field.get("source_id"),
        baseline_dataset_id=admission_by_field.get("baseline_dataset_id"),
        baseline_run_id=admission_by_field.get("baseline_run_id"),
        candidate_dataset_id=admission_by_field.get("candidate_dataset_id"),
        candidate_run_id=admission_by_field.get("candidate_run_id"),
        predecessor_dataset_id=admission_by_field.get("predecessor_dataset_id"),
        cutoff_at=utc_timestamp(
            admission_by_field.get("cutoff_at"),
            "stored admission cutoff",
        ),
        source_configuration_hash=admission_by_field.get(
            "source_configuration_hash"
        ),
        acquisition_contract_hash=admission_by_field.get(
            "acquisition_contract_hash"
        ),
        verification=verification,
        alternative=AlternativeProof(
            admission_by_field.get("alternative_count"),
            admission_by_field.get("alternative_hash"),
        ),
        baseline_verified_at=utc_timestamp(
            admission_by_field.get("baseline_verified_at"),
            "stored baseline verification timestamp",
        ),
        candidate_verified_at=utc_timestamp(
            admission_by_field.get("candidate_verified_at"),
            "stored candidate verification timestamp",
        ),
        admitted_at=utc_timestamp(
            admission_by_field.get("admitted_at"),
            "stored admission timestamp",
        ),
    )


def attempt_from_row(attempt_by_field: dict[str, Any]) -> TwinAttemptResult:
    """Strictly decode one stored attempt row."""

    return TwinAttemptResult(
        source_id=attempt_by_field.get("source_id"),
        baseline_dataset_id=attempt_by_field.get("baseline_dataset_id"),
        baseline_run_id=attempt_by_field.get("baseline_run_id"),
        candidate_dataset_id=attempt_by_field.get("candidate_dataset_id"),
        candidate_run_id=attempt_by_field.get("candidate_run_id"),
        cutoff_at=utc_timestamp(
            attempt_by_field.get("cutoff_at"),
            "stored attempt cutoff",
        ),
        source_configuration_hash=attempt_by_field.get(
            "source_configuration_hash"
        ),
        acquisition_contract_hash=attempt_by_field.get(
            "acquisition_contract_hash"
        ),
        baseline_evidence_hash=attempt_by_field.get("baseline_evidence_hash"),
        candidate_evidence_hash=attempt_by_field.get("candidate_evidence_hash"),
        matched=attempt_by_field.get("matched"),
        attempted_at=utc_timestamp(
            attempt_by_field.get("attempted_at"),
            "stored attempt timestamp",
        ),
    )


__all__ = (
    "AlternativeProof",
    "TwinAdmissionError",
    "TwinAdmissionResult",
    "TwinAttemptResult",
)
