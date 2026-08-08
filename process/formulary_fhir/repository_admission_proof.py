# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Recomputed content and independent-acquisition proofs for twin admission."""

from __future__ import annotations

import hashlib
import datetime as dt
from dataclasses import dataclass
from typing import Any

from process.formulary_fhir.repository_admission_types import AlternativeProof
from process.formulary_fhir.repository_admission_types import TwinAdmissionError
from process.formulary_fhir.repository_admission_types import raise_admission_error
from process.formulary_fhir.repository_admission_types import verification_values
from process.formulary_fhir.repository_shared import DatasetRef
from process.formulary_fhir.repository_shared import DatasetVerification
from process.formulary_fhir.repository_shared import WRITE_BATCH_SIZE
from process.formulary_fhir.repository_shared import json_object
from process.formulary_fhir.repository_shared import json_text
from process.formulary_fhir.repository_shared import lock_dataset
from process.formulary_fhir.repository_shared import row_mapping
from process.formulary_fhir.repository_shared import strict_hash
from process.formulary_fhir.repository_shared import strict_text
from process.formulary_fhir.repository_shared import table_name
from process.formulary_fhir.repository_shared import utc_timestamp
from process.formulary_fhir.repository_verify import _is_stored_verification_exact
from process.formulary_fhir.repository_verify import _recompute_dataset_verification


ALTERNATIVE_EVIDENCE_DOMAIN = "fhir-formulary-alternative-evidence-v1"
TWIN_EVIDENCE_DOMAIN = "fhir-formulary-twin-evidence-v1"


@dataclass(frozen=True, slots=True)
class DatasetEvidence:
    """Recomputed evidence retained while dataset locks are held."""

    row: dict[str, Any]
    verification: DatasetVerification
    alternative: AlternativeProof
    verified_at: dt.datetime


def dataset_evidence_hash(
    dataset: DatasetRef,
    source_configuration_hash: str,
    dataset_evidence: DatasetEvidence,
) -> str:
    """Hash comparable evidence while excluding root identity and timing."""

    try:
        verification = dataset_evidence.verification
        alternative = dataset_evidence.alternative
        if not (
            type(dataset) is DatasetRef
            and type(dataset_evidence) is DatasetEvidence
            and verification.source_id == dataset.source_id
            and verification.dataset_id == dataset.dataset_id
        ):
            raise ValueError("dataset evidence identity mismatch")
        strict_hash(source_configuration_hash, "source configuration hash")
        strict_hash(verification.coverage_hash, "dataset coverage hash")
        strict_hash(verification.membership_hash, "dataset membership hash")
        cutoff_at = utc_timestamp(dataset.cutoff_at, "dataset cutoff")
        cutoff_text = cutoff_at.isoformat().replace("+00:00", "Z")
        evidence_by_field = {
            "acquisition_contract_hash": dataset.acquisition_contract_hash,
            "alias_count": verification.alias_count,
            "alternative_count": alternative.count,
            "alternative_hash": alternative.evidence_hash,
            "coverage_hash": verification.coverage_hash,
            "cutoff_at": cutoff_text,
            "list_count": verification.list_count,
            "medication_count": verification.medication_membership_count,
            "membership_hash": verification.membership_hash,
            "predecessor_dataset_id": dataset.previous_dataset_id,
            "source_configuration_hash": source_configuration_hash,
            "source_id": dataset.source_id,
        }
        digest = hashlib.sha256()
        digest.update(TWIN_EVIDENCE_DOMAIN.encode("ascii") + b"\n")
        digest.update(json_text(evidence_by_field).encode("utf-8"))
        return digest.hexdigest()
    except TwinAdmissionError:
        raise
    except Exception:
        raise_admission_error("evidence")


def _optional_text(value: object, label: str, maximum_length: int) -> str | None:
    if value is None:
        return None
    return strict_text(value, label, maximum_length)


def _alternative_record(database_row: Any) -> tuple[tuple[str, ...], str]:
    alternative_by_field = row_mapping(database_row)
    alias_id = strict_text(
        alternative_by_field.get("alias_id"),
        "stored alias id",
        64,
    )
    version_id = strict_text(
        alternative_by_field.get("alias_version_id"),
        "stored alias version id",
        64,
    )
    medication_id = strict_text(
        alternative_by_field.get("upstream_medication_id"),
        "stored medication id",
        256,
    )
    raw_reference = strict_text(
        alternative_by_field.get("raw_reference"),
        "stored alternative reference",
        16_384,
    )
    corrected = _optional_text(
        alternative_by_field.get("corrected_reference"),
        "stored corrected reference",
        16_384,
    )
    resolved_id = _optional_text(
        alternative_by_field.get("resolved_medication_id"),
        "stored resolved medication id",
        256,
    )
    rule_version = _optional_text(
        alternative_by_field.get("rule_version"),
        "stored correction rule version",
        64,
    )
    resolved = alternative_by_field.get("resolved")
    if type(resolved) is not bool or resolved != (resolved_id is not None):
        raise ValueError("stored alternative resolution mismatch")
    proof_by_field = {
        "alias_id": alias_id,
        "alias_version_id": version_id,
        "corrected_reference": corrected,
        "evidence": json_object(alternative_by_field.get("evidence_json")),
        "raw_reference": raw_reference,
        "resolved": resolved,
        "resolved_medication_id": resolved_id,
        "rule_version": rule_version,
        "upstream_medication_id": medication_id,
    }
    return (
        (alias_id, version_id, medication_id, raw_reference),
        json_text(proof_by_field),
    )


async def recompute_alternative_proof(
    database: Any,
    source_id: str,
    dataset_id: str,
) -> AlternativeProof:
    """Stream a deterministic proof without retaining alternative rows."""

    try:
        normalized_source = strict_text(source_id, "source id", 64)
        normalized_dataset = strict_text(dataset_id, "dataset id", 64)
        digest = hashlib.sha256()
        digest.update(ALTERNATIVE_EVIDENCE_DOMAIN.encode("ascii") + b"\n")
        last_key = ("", "", "", "")
        count = 0
        while True:
            alternative_rows = await database.all(
                f"SELECT link.alias_id, link.alias_version_id, "
                "alternative.upstream_medication_id, alternative.raw_reference, "
                "alternative.corrected_reference, "
                "alternative.resolved_medication_id, alternative.resolved, "
                "alternative.rule_version, alternative.evidence_json FROM "
                f"{table_name('fhir_formulary_dataset_alias')} AS link JOIN "
                f"{table_name('fhir_formulary_alternative')} AS alternative "
                "ON alternative.alias_version_id = link.alias_version_id "
                "WHERE link.source_id = :source_id "
                "AND link.dataset_id = :dataset_id AND ("
                "link.alias_id, link.alias_version_id, "
                "alternative.upstream_medication_id, "
                "alternative.raw_reference) > ("
                ":last_alias_id, :last_version_id, :last_medication_id, "
                ":last_raw_reference) ORDER BY link.alias_id, "
                "link.alias_version_id, alternative.upstream_medication_id, "
                "alternative.raw_reference LIMIT :batch_size;",
                source_id=normalized_source,
                dataset_id=normalized_dataset,
                last_alias_id=last_key[0],
                last_version_id=last_key[1],
                last_medication_id=last_key[2],
                last_raw_reference=last_key[3],
                batch_size=WRITE_BATCH_SIZE,
            )
            if not alternative_rows:
                break
            for database_row in alternative_rows:
                key, proof_text = _alternative_record(database_row)
                if key <= last_key:
                    raise ValueError("stored alternative order mismatch")
                digest.update(proof_text.encode("utf-8") + b"\n")
                last_key = key
                count += 1
            if len(alternative_rows) < WRITE_BATCH_SIZE:
                break
        return AlternativeProof(count, digest.hexdigest())
    except TwinAdmissionError:
        raise
    except Exception:
        raise_admission_error("evidence")


async def _lifecycle_row(
    database: Any,
    source_id: str,
    dataset_id: str,
) -> dict[str, Any]:
    lifecycle = row_mapping(
        await database.first(
            f"SELECT source_id, dataset_id, verified_at, failed_at, error_json "
            f"FROM {table_name('fhir_formulary_dataset')} "
            "WHERE source_id = :source_id AND dataset_id = :dataset_id;",
            source_id=source_id,
            dataset_id=dataset_id,
        )
    )
    if (
        lifecycle.get("source_id") != source_id
        or lifecycle.get("dataset_id") != dataset_id
        or lifecycle.get("failed_at") is not None
        or lifecycle.get("error_json") is not None
    ):
        raise_admission_error("evidence")
    try:
        lifecycle["verified_at"] = utc_timestamp(
            lifecycle.get("verified_at"),
            "stored verification timestamp",
        )
    except ValueError:
        raise_admission_error("evidence")
    return lifecycle


async def require_full_checkpoints(
    database: Any,
    dataset: DatasetRef,
    alias_count: int,
) -> None:
    """Require one completed full checkpoint for every dataset alias."""

    try:
        checkpoint_rows = await database.all(
            f"SELECT checkpoint.alias_id, link.alias_id AS linked_alias_id, "
            "checkpoint.cutoff_at, checkpoint.acquisition_mode, "
            "checkpoint.expected_count, checkpoint.processed_count, "
            f"checkpoint.membership_hash, checkpoint.completed FROM "
            f"{table_name('fhir_formulary_checkpoint')} AS checkpoint LEFT JOIN "
            f"{table_name('fhir_formulary_dataset_alias')} AS link ON "
            "link.source_id = checkpoint.source_id "
            "AND link.dataset_id = checkpoint.dataset_id "
            "AND link.alias_id = checkpoint.alias_id "
            "WHERE checkpoint.source_id = :source_id "
            "AND checkpoint.dataset_id = :dataset_id "
            "AND checkpoint.run_id = :run_id ORDER BY checkpoint.alias_id;",
            source_id=dataset.source_id,
            dataset_id=dataset.dataset_id,
            run_id=dataset.run_id,
        )
        if len(checkpoint_rows) != alias_count:
            raise_admission_error("independence")
        prior_alias_id = ""
        for database_row in checkpoint_rows:
            checkpoint_by_field = row_mapping(database_row)
            alias_id = strict_text(
                checkpoint_by_field.get("alias_id"),
                "checkpoint alias id",
                64,
            )
            expected_count = checkpoint_by_field.get("expected_count")
            is_full = bool(
                alias_id > prior_alias_id
                and checkpoint_by_field.get("linked_alias_id") == alias_id
                and checkpoint_by_field.get("acquisition_mode") == "full"
                and checkpoint_by_field.get("completed") is True
                and type(expected_count) is int
                and expected_count >= 0
                and checkpoint_by_field.get("processed_count") == expected_count
                and utc_timestamp(
                    checkpoint_by_field.get("cutoff_at"),
                    "checkpoint cutoff",
                ) == dataset.cutoff_at
            )
            strict_hash(
                checkpoint_by_field.get("membership_hash"),
                "checkpoint membership hash",
            )
            if not is_full:
                raise_admission_error("independence")
            prior_alias_id = alias_id
    except TwinAdmissionError:
        raise
    except Exception:
        raise_admission_error("independence")


async def _dataset_evidence(
    database: Any,
    dataset: DatasetRef,
    dataset_row: dict[str, Any],
) -> DatasetEvidence:
    try:
        lifecycle = await _lifecycle_row(
            database,
            dataset.source_id,
            dataset.dataset_id,
        )
        verification = await _recompute_dataset_verification(
            database,
            dataset.source_id,
            dataset,
        )
        if not _is_stored_verification_exact(dataset_row, verification):
            raise_admission_error("evidence")
        await require_full_checkpoints(database, dataset, verification.alias_count)
        alternative = await recompute_alternative_proof(
            database,
            dataset.source_id,
            dataset.dataset_id,
        )
        return DatasetEvidence(
            {**dataset_row, **lifecycle},
            verification,
            alternative,
            lifecycle["verified_at"],
        )
    except TwinAdmissionError:
        raise
    except Exception:
        raise_admission_error("evidence")


async def lock_pair_evidence(
    database: Any,
    baseline: DatasetRef,
    candidate: DatasetRef,
    *,
    candidate_statuses: set[str],
) -> tuple[DatasetEvidence, DatasetEvidence]:
    """Lock both datasets in stable identity order and recompute evidence."""

    rows_by_id: dict[str, dict[str, Any]] = {}
    try:
        for dataset in sorted((baseline, candidate), key=lambda item: item.dataset_id):
            allowed = (
                candidate_statuses
                if dataset.dataset_id == candidate.dataset_id
                else {"verified"}
            )
            rows_by_id[dataset.dataset_id] = await lock_dataset(
                database,
                dataset.source_id,
                dataset,
                allowed_statuses=allowed,
            )
    except TwinAdmissionError:
        raise
    except Exception:
        raise_admission_error("evidence")
    baseline_evidence = await _dataset_evidence(
        database,
        baseline,
        rows_by_id[baseline.dataset_id],
    )
    candidate_evidence = await _dataset_evidence(
        database,
        candidate,
        rows_by_id[candidate.dataset_id],
    )
    return baseline_evidence, candidate_evidence


def require_matching_pair(
    baseline: DatasetEvidence,
    candidate: DatasetEvidence,
) -> None:
    """Require identical nonempty graph and alternative evidence."""

    require_admissible_pair(baseline, candidate)
    if (
        verification_values(baseline.verification)
        != verification_values(candidate.verification)
        or baseline.alternative != candidate.alternative
    ):
        raise_admission_error("mismatch")


def require_admissible_pair(
    baseline: DatasetEvidence,
    candidate: DatasetEvidence,
) -> None:
    """Require two nonempty verified roots in intended completion order."""

    for dataset_evidence in (baseline, candidate):
        if type(dataset_evidence) is not DatasetEvidence:
            raise_admission_error("evidence")
        verification = dataset_evidence.verification
        if (
            verification.list_count <= 0
            or verification.alias_count <= 0
            or verification.medication_membership_count <= 0
        ):
            raise_admission_error("evidence")
    if baseline.verified_at > candidate.verified_at:
        raise_admission_error("evidence")


__all__ = ("dataset_evidence_hash", "recompute_alternative_proof")
