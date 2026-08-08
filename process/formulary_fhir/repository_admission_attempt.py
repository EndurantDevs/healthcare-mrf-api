# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Immutable attempt persistence that burns both acquisition roots."""

from __future__ import annotations

from typing import Any

from process.formulary_fhir.repository_admission_types import TwinAdmissionError
from process.formulary_fhir.repository_admission_types import TwinAttemptResult
from process.formulary_fhir.repository_admission_types import attempt_from_row
from process.formulary_fhir.repository_admission_types import raise_admission_error
from process.formulary_fhir.repository_shared import DatasetRef
from process.formulary_fhir.repository_shared import row_mapping
from process.formulary_fhir.repository_shared import strict_hash
from process.formulary_fhir.repository_shared import table_name


ATTEMPT_COLUMNS = (
    "source_id",
    "baseline_dataset_id",
    "baseline_run_id",
    "candidate_dataset_id",
    "candidate_run_id",
    "cutoff_at",
    "source_configuration_hash",
    "acquisition_contract_hash",
    "baseline_evidence_hash",
    "candidate_evidence_hash",
    "matched",
    "attempted_at",
)


def _has_exact_attempt(
    attempt: TwinAttemptResult,
    baseline: DatasetRef,
    candidate: DatasetRef,
    source_configuration_hash: str,
    baseline_evidence_hash: str,
    candidate_evidence_hash: str,
) -> bool:
    return bool(
        attempt.source_id == candidate.source_id
        and attempt.baseline_dataset_id == baseline.dataset_id
        and attempt.baseline_run_id == baseline.run_id
        and attempt.candidate_dataset_id == candidate.dataset_id
        and attempt.candidate_run_id == candidate.run_id
        and attempt.cutoff_at == candidate.cutoff_at
        and attempt.source_configuration_hash == source_configuration_hash
        and attempt.acquisition_contract_hash
        == candidate.acquisition_contract_hash
        and attempt.baseline_evidence_hash == baseline_evidence_hash
        and attempt.candidate_evidence_hash == candidate_evidence_hash
        and attempt.matched
        == (baseline_evidence_hash == candidate_evidence_hash)
    )


async def _root_attempts(
    database: Any,
    baseline_dataset_id: str,
    candidate_dataset_id: str,
) -> tuple[TwinAttemptResult, ...]:
    try:
        database_rows = await database.all(
            f"SELECT {', '.join(ATTEMPT_COLUMNS)} FROM "
            f"{table_name('fhir_formulary_twin_attempt')} WHERE "
            "baseline_dataset_id IN (:baseline_dataset_id, :candidate_dataset_id) "
            "OR candidate_dataset_id IN ("
            ":baseline_dataset_id, :candidate_dataset_id) "
            "ORDER BY source_id, baseline_dataset_id, candidate_dataset_id;",
            baseline_dataset_id=baseline_dataset_id,
            candidate_dataset_id=candidate_dataset_id,
        )
    except Exception:
        raise_admission_error("storage")
    try:
        return tuple(
            attempt_from_row(row_mapping(database_row))
            for database_row in database_rows
        )
    except Exception:
        raise_admission_error("attempt")


def _require_one_exact_attempt(
    attempts: tuple[TwinAttemptResult, ...],
    baseline: DatasetRef,
    candidate: DatasetRef,
    source_configuration_hash: str,
    baseline_evidence_hash: str,
    candidate_evidence_hash: str,
) -> TwinAttemptResult | None:
    if not attempts:
        return None
    if len(attempts) != 1 or not _has_exact_attempt(
        attempts[0],
        baseline,
        candidate,
        source_configuration_hash,
        baseline_evidence_hash,
        candidate_evidence_hash,
    ):
        raise_admission_error("attempt")
    return attempts[0]


async def _insert_attempt(
    database: Any,
    baseline: DatasetRef,
    candidate: DatasetRef,
    source_configuration_hash: str,
    baseline_evidence_hash: str,
    candidate_evidence_hash: str,
) -> None:
    try:
        strict_hash(source_configuration_hash, "source configuration hash")
        strict_hash(baseline_evidence_hash, "baseline evidence hash")
        strict_hash(candidate_evidence_hash, "candidate evidence hash")
        inserted_count = await database.status(
            f"INSERT INTO {table_name('fhir_formulary_twin_attempt')} ("
            f"{', '.join(ATTEMPT_COLUMNS[:-1])}) VALUES ("
            ":source_id, :baseline_dataset_id, :baseline_run_id, "
            ":candidate_dataset_id, :candidate_run_id, :cutoff_at, "
            ":source_configuration_hash, :acquisition_contract_hash, "
            ":baseline_evidence_hash, :candidate_evidence_hash, :matched) "
            "ON CONFLICT DO NOTHING;",
            source_id=candidate.source_id,
            baseline_dataset_id=baseline.dataset_id,
            baseline_run_id=baseline.run_id,
            candidate_dataset_id=candidate.dataset_id,
            candidate_run_id=candidate.run_id,
            cutoff_at=candidate.cutoff_at,
            source_configuration_hash=source_configuration_hash,
            acquisition_contract_hash=candidate.acquisition_contract_hash,
            baseline_evidence_hash=baseline_evidence_hash,
            candidate_evidence_hash=candidate_evidence_hash,
            matched=baseline_evidence_hash == candidate_evidence_hash,
        )
        if type(inserted_count) is not int or inserted_count not in {0, 1}:
            raise_admission_error("storage")
    except TwinAdmissionError:
        raise
    except Exception:
        raise_admission_error("storage")


async def persist_twin_attempt(
    database: Any,
    baseline: DatasetRef,
    candidate: DatasetRef,
    source_configuration_hash: str,
    baseline_evidence_hash: str,
    candidate_evidence_hash: str,
) -> TwinAttemptResult:
    """Insert or replay one exact attempt while rejecting any root reuse."""

    attempts = await _root_attempts(
        database,
        baseline.dataset_id,
        candidate.dataset_id,
    )
    exact_attempt = _require_one_exact_attempt(
        attempts,
        baseline,
        candidate,
        source_configuration_hash,
        baseline_evidence_hash,
        candidate_evidence_hash,
    )
    if exact_attempt is not None:
        return exact_attempt
    await _insert_attempt(
        database,
        baseline,
        candidate,
        source_configuration_hash,
        baseline_evidence_hash,
        candidate_evidence_hash,
    )
    stored_attempts = await _root_attempts(
        database,
        baseline.dataset_id,
        candidate.dataset_id,
    )
    stored_attempt = _require_one_exact_attempt(
        stored_attempts,
        baseline,
        candidate,
        source_configuration_hash,
        baseline_evidence_hash,
        candidate_evidence_hash,
    )
    if stored_attempt is None:
        raise_admission_error("attempt")
    return stored_attempt


async def require_exact_twin_attempt(
    database: Any,
    baseline: DatasetRef,
    candidate: DatasetRef,
    source_configuration_hash: str,
    baseline_evidence_hash: str,
    candidate_evidence_hash: str,
) -> TwinAttemptResult:
    """Require a matched attempt without mutating admission state."""

    attempts = await _root_attempts(
        database,
        baseline.dataset_id,
        candidate.dataset_id,
    )
    exact_attempt = _require_one_exact_attempt(
        attempts,
        baseline,
        candidate,
        source_configuration_hash,
        baseline_evidence_hash,
        candidate_evidence_hash,
    )
    if exact_attempt is None or exact_attempt.matched is not True:
        raise_admission_error("attempt")
    return exact_attempt


__all__ = ()
