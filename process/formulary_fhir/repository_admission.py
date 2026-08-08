# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Immutable admission for two independently acquired formulary datasets."""

from __future__ import annotations

from typing import Any

from process.formulary_fhir.repository_admission_attempt import persist_twin_attempt
from process.formulary_fhir.repository_admission_attempt import (
    require_exact_twin_attempt,
)
from process.formulary_fhir.repository_admission_proof import DatasetEvidence
from process.formulary_fhir.repository_admission_proof import dataset_evidence_hash
from process.formulary_fhir.repository_admission_proof import lock_pair_evidence
from process.formulary_fhir.repository_admission_proof import recompute_alternative_proof
from process.formulary_fhir.repository_admission_proof import require_admissible_pair
from process.formulary_fhir.repository_admission_proof import require_matching_pair
from process.formulary_fhir.repository_admission_types import AlternativeProof
from process.formulary_fhir.repository_admission_types import TwinAdmissionError
from process.formulary_fhir.repository_admission_types import TwinAdmissionResult
from process.formulary_fhir.repository_admission_types import raise_admission_error
from process.formulary_fhir.repository_admission_types import result_from_row
from process.formulary_fhir.repository_admission_types import verification_values
from process.formulary_fhir.repository_shared import DatasetRef
from process.formulary_fhir.repository_shared import lock_source
from process.formulary_fhir.repository_shared import row_mapping
from process.formulary_fhir.repository_shared import strict_text
from process.formulary_fhir.repository_shared import table_name
from process.formulary_fhir.source import EnabledSourceBinding
from process.formulary_fhir.source import load_enabled_source


ADMISSION_COLUMNS = (
    "source_id",
    "baseline_dataset_id",
    "baseline_run_id",
    "candidate_dataset_id",
    "candidate_run_id",
    "predecessor_dataset_id",
    "cutoff_at",
    "source_configuration_hash",
    "acquisition_contract_hash",
    "list_count",
    "alias_count",
    "medication_count",
    "coverage_hash",
    "membership_hash",
    "alternative_count",
    "alternative_hash",
    "baseline_verified_at",
    "candidate_verified_at",
    "admitted_at",
)


def _validate_pair(
    source_id: str,
    baseline: DatasetRef,
    candidate: DatasetRef,
    *,
    candidate_statuses: set[str],
) -> None:
    is_valid = bool(
        type(baseline) is DatasetRef
        and type(candidate) is DatasetRef
        and baseline.source_id == source_id == candidate.source_id
        and baseline.dataset_id != candidate.dataset_id
        and baseline.run_id != candidate.run_id
        and baseline.previous_dataset_id == candidate.previous_dataset_id
        and baseline.cutoff_at == candidate.cutoff_at
        and baseline.acquisition_contract_hash
        == candidate.acquisition_contract_hash
        and baseline.intent == "none"
        and candidate.intent == "requested"
        and baseline.status == "verified"
        and candidate.status in candidate_statuses
    )
    if not is_valid:
        raise_admission_error("invalid_request")


async def _locked_predecessor(database: Any, source_id: str) -> str | None:
    try:
        current = row_mapping(
            await database.first(
                f"SELECT dataset_id FROM {table_name('fhir_formulary_current')} "
                "WHERE source_id = :source_id FOR UPDATE;",
                source_id=source_id,
            )
        )
        if not current:
            return None
        return strict_text(current.get("dataset_id"), "current dataset id", 64)
    except Exception:
        raise_admission_error("pointer")


async def _read_admission(
    database: Any,
    source_id: str,
    candidate_dataset_id: str,
) -> TwinAdmissionResult | None:
    try:
        row = row_mapping(
            await database.first(
                f"SELECT {', '.join(ADMISSION_COLUMNS)} FROM "
                f"{table_name('fhir_formulary_twin_admission')} "
                "WHERE source_id = :source_id "
                "AND candidate_dataset_id = :candidate_dataset_id;",
                source_id=source_id,
                candidate_dataset_id=candidate_dataset_id,
            )
        )
    except Exception:
        raise_admission_error("storage")
    if not row:
        return None
    try:
        return result_from_row(row)
    except Exception:
        raise_admission_error("admission")


def _has_exact_result(
    admission_result: TwinAdmissionResult,
    binding_hash: str,
    baseline: DatasetRef,
    candidate: DatasetRef,
    baseline_evidence: DatasetEvidence,
    candidate_evidence: DatasetEvidence,
) -> bool:
    return bool(
        admission_result.source_id == candidate.source_id
        and admission_result.baseline_dataset_id == baseline.dataset_id
        and admission_result.baseline_run_id == baseline.run_id
        and admission_result.candidate_dataset_id == candidate.dataset_id
        and admission_result.candidate_run_id == candidate.run_id
        and admission_result.predecessor_dataset_id == candidate.previous_dataset_id
        and admission_result.cutoff_at == candidate.cutoff_at
        and admission_result.source_configuration_hash == binding_hash
        and admission_result.acquisition_contract_hash
        == candidate.acquisition_contract_hash
        and verification_values(admission_result.verification)
        == verification_values(candidate_evidence.verification)
        and admission_result.alternative == candidate_evidence.alternative
        and admission_result.baseline_verified_at == baseline_evidence.verified_at
        and admission_result.candidate_verified_at == candidate_evidence.verified_at
    )


async def _insert_admission(
    database: Any,
    binding_hash: str,
    baseline: DatasetRef,
    candidate: DatasetRef,
    baseline_evidence: DatasetEvidence,
    candidate_evidence: DatasetEvidence,
) -> None:
    verification = candidate_evidence.verification
    alternative = candidate_evidence.alternative
    try:
        inserted_count = await database.status(
            f"INSERT INTO {table_name('fhir_formulary_twin_admission')} ("
            f"{', '.join(ADMISSION_COLUMNS[:-1])}) VALUES ("
            ":source_id, :baseline_dataset_id, :baseline_run_id, "
            ":candidate_dataset_id, :candidate_run_id, "
            ":predecessor_dataset_id, :cutoff_at, :source_configuration_hash, "
            ":acquisition_contract_hash, :list_count, :alias_count, "
            ":medication_count, :coverage_hash, :membership_hash, "
            ":alternative_count, :alternative_hash, :baseline_verified_at, "
            ":candidate_verified_at) ON CONFLICT DO NOTHING;",
            source_id=candidate.source_id,
            baseline_dataset_id=baseline.dataset_id,
            baseline_run_id=baseline.run_id,
            candidate_dataset_id=candidate.dataset_id,
            candidate_run_id=candidate.run_id,
            predecessor_dataset_id=candidate.previous_dataset_id,
            cutoff_at=candidate.cutoff_at,
            source_configuration_hash=binding_hash,
            acquisition_contract_hash=candidate.acquisition_contract_hash,
            list_count=verification.list_count,
            alias_count=verification.alias_count,
            medication_count=verification.medication_membership_count,
            coverage_hash=verification.coverage_hash,
            membership_hash=verification.membership_hash,
            alternative_count=alternative.count,
            alternative_hash=alternative.evidence_hash,
            baseline_verified_at=baseline_evidence.verified_at,
            candidate_verified_at=candidate_evidence.verified_at,
        )
        if type(inserted_count) is not int or inserted_count not in {0, 1}:
            raise_admission_error("storage")
    except TwinAdmissionError:
        raise
    except Exception:
        raise_admission_error("storage")


async def _current_configuration_hash(database: Any, source_id: str) -> str:
    try:
        binding = await load_enabled_source(source_id, database=database)
        return binding.configuration_hash
    except Exception:
        raise_admission_error("source")


async def _persist_exact_admission(
    database: Any,
    binding_hash: str,
    baseline: DatasetRef,
    candidate: DatasetRef,
    baseline_evidence: DatasetEvidence,
    candidate_evidence: DatasetEvidence,
) -> TwinAdmissionResult:
    """Insert when absent, then require one exact immutable readback."""

    admission_result = await _read_admission(
        database,
        candidate.source_id,
        candidate.dataset_id,
    )
    if admission_result is None:
        await _insert_admission(
            database,
            binding_hash,
            baseline,
            candidate,
            baseline_evidence,
            candidate_evidence,
        )
        admission_result = await _read_admission(
            database,
            candidate.source_id,
            candidate.dataset_id,
        )
    if admission_result is None or not _has_exact_result(
        admission_result,
        binding_hash,
        baseline,
        candidate,
        baseline_evidence,
        candidate_evidence,
    ):
        raise_admission_error("admission")
    return admission_result


def _pair_evidence_hashes(
    binding_hash: str,
    baseline: DatasetRef,
    candidate: DatasetRef,
    baseline_evidence: DatasetEvidence,
    candidate_evidence: DatasetEvidence,
) -> tuple[str, str]:
    """Build both comparable hashes from evidence recomputed under locks."""

    return (
        dataset_evidence_hash(baseline, binding_hash, baseline_evidence),
        dataset_evidence_hash(candidate, binding_hash, candidate_evidence),
    )


async def _persist_pair_attempt(
    database: Any,
    binding_hash: str,
    baseline: DatasetRef,
    candidate: DatasetRef,
    baseline_evidence: DatasetEvidence,
    candidate_evidence: DatasetEvidence,
):
    """Persist one attempt from freshly recomputed pair evidence."""

    baseline_hash, candidate_hash = _pair_evidence_hashes(
        binding_hash,
        baseline,
        candidate,
        baseline_evidence,
        candidate_evidence,
    )
    return await persist_twin_attempt(
        database,
        baseline,
        candidate,
        binding_hash,
        baseline_hash,
        candidate_hash,
    )


async def _require_pair_attempt(
    database: Any,
    binding_hash: str,
    baseline: DatasetRef,
    candidate: DatasetRef,
    baseline_evidence: DatasetEvidence,
    candidate_evidence: DatasetEvidence,
) -> None:
    """Require one matched attempt for freshly recomputed pair evidence."""

    baseline_hash, candidate_hash = _pair_evidence_hashes(
        binding_hash,
        baseline,
        candidate,
        baseline_evidence,
        candidate_evidence,
    )
    await require_exact_twin_attempt(
        database,
        baseline,
        candidate,
        binding_hash,
        baseline_hash,
        candidate_hash,
    )


async def _admit_verified_twins(
    database: Any,
    binding: EnabledSourceBinding,
    baseline: DatasetRef,
    candidate: DatasetRef,
) -> TwinAdmissionResult | None:
    """Run the ordered lock and persistence protocol in one transaction."""

    if type(binding) is not EnabledSourceBinding:
        raise_admission_error("invalid_request")
    _validate_pair(
        binding.source_id,
        baseline,
        candidate,
        candidate_statuses={"verified"},
    )
    async with database.transaction():
        try:
            await lock_source(database, binding.source_id)
        except Exception:
            raise_admission_error("source")
        binding_hash = await _current_configuration_hash(database, binding.source_id)
        if binding_hash != binding.configuration_hash:
            raise_admission_error("source")
        baseline_evidence, candidate_evidence = await lock_pair_evidence(
            database,
            baseline,
            candidate,
            candidate_statuses={"verified"},
        )
        require_admissible_pair(baseline_evidence, candidate_evidence)
        predecessor = await _locked_predecessor(database, binding.source_id)
        if predecessor != candidate.previous_dataset_id:
            raise_admission_error("pointer")
        attempt = await _persist_pair_attempt(
            database,
            binding_hash,
            baseline,
            candidate,
            baseline_evidence,
            candidate_evidence,
        )
        if attempt.matched is not True:
            return None
        require_matching_pair(baseline_evidence, candidate_evidence)
        return await _persist_exact_admission(
            database,
            binding_hash,
            baseline,
            candidate,
            baseline_evidence,
            candidate_evidence,
        )


async def admit_verified_twins(
    *,
    database: Any,
    binding: EnabledSourceBinding,
    baseline: DatasetRef,
    candidate: DatasetRef,
) -> TwinAdmissionResult:
    """Atomically admit two matching, independently acquired datasets."""

    try:
        admission_result = await _admit_verified_twins(
            database,
            binding,
            baseline,
            candidate,
        )
    except TwinAdmissionError:
        raise
    except Exception:
        raise_admission_error("storage")
    if admission_result is None:
        raise_admission_error("mismatch")
    return admission_result


async def _verify_twin_admission_for_publication(
    database: Any,
    source_id: str,
    candidate: DatasetRef,
) -> tuple[TwinAdmissionResult, dict[str, Any]]:
    """Run transaction-neutral publication revalidation under a source lock."""

    normalized_source = strict_text(source_id, "source id", 64)
    if type(candidate) is not DatasetRef:
        raise_admission_error("invalid_request")
    admission_result = await _read_admission(
        database,
        normalized_source,
        candidate.dataset_id,
    )
    if admission_result is None:
        raise_admission_error("missing")
    baseline = DatasetRef(
        admission_result.source_id,
        admission_result.baseline_dataset_id,
        admission_result.baseline_run_id,
        admission_result.predecessor_dataset_id,
        admission_result.cutoff_at,
        admission_result.acquisition_contract_hash,
        "none",
        "verified",
    )
    _validate_pair(
        normalized_source,
        baseline,
        candidate,
        candidate_statuses={"verified", "published"},
    )
    binding_hash = await _current_configuration_hash(database, normalized_source)
    baseline_evidence, candidate_evidence = await lock_pair_evidence(
        database,
        baseline,
        candidate,
        candidate_statuses={"verified", "published"},
    )
    require_matching_pair(baseline_evidence, candidate_evidence)
    await _require_pair_attempt(
        database,
        binding_hash,
        baseline,
        candidate,
        baseline_evidence,
        candidate_evidence,
    )
    if not _has_exact_result(
        admission_result,
        binding_hash,
        baseline,
        candidate,
        baseline_evidence,
        candidate_evidence,
    ):
        raise_admission_error("admission")
    return admission_result, candidate_evidence.row


async def verify_twin_admission_for_publication(
    database: Any,
    source_id: str,
    candidate: DatasetRef,
) -> tuple[TwinAdmissionResult, dict[str, Any]]:
    """Reverify an admission after a caller locks its source, before current."""

    try:
        return await _verify_twin_admission_for_publication(
            database,
            source_id,
            candidate,
        )
    except TwinAdmissionError:
        raise
    except Exception:
        raise_admission_error("storage")


__all__ = (
    "AlternativeProof",
    "TwinAdmissionError",
    "TwinAdmissionResult",
    "admit_verified_twins",
    "recompute_alternative_proof",
    "verify_twin_admission_for_publication",
)
