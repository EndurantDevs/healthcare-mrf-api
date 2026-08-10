# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Durable restart-safe evidence for one admitted UHC drug candidate."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from typing import Any

from db.models import db
import process.formulary_fhir.manual_lock as manual_lock
from process.formulary_fhir.repository_admission_types import TwinAdmissionResult
from process.formulary_fhir.repository_shared import DatasetRef
from process.formulary_fhir.repository_shared import stable_id
from process.formulary_fhir.repository_shared import strict_hash
from process.formulary_fhir.repository_shared import utc_timestamp
from process.formulary_fhir.source import EnabledSourceBinding
from process.formulary_fhir.source import require_source_unchanged
from process.formulary_fhir.source_artifact_contract import (
    VerifiedSourceArtifactSet,
)
from process.formulary_fhir.source_artifacts import (
    load_complete_source_artifact_set,
)
from process.formulary_fhir.source_artifacts import (
    reopen_source_artifact_set,
)
from process.formulary_fhir.uhc_drug_receipt_store import insert_uhc_receipt
from process.formulary_fhir.uhc_drug_receipt_store import (
    load_uhc_receipt_admission,
)
from process.formulary_fhir.uhc_drug_receipt_store import load_uhc_receipt_row
from process.formulary_fhir.uhc_drug_receipt_store import (
    validate_uhc_drug_receipt_id,
)
from process.formulary_fhir.uhc_drug_parser_contract import UHCDrugSpoolEvidence
from process.formulary_fhir.uhc_drug_sync_contract import (
    uhc_drug_sync_contract_hash,
)
from process.formulary_fhir.uhc_drug_twin import UHCDrugTwinResult
from process.formulary_fhir.uhc_source import register_uhc_formulary_source
from process.formulary_fhir.uhc_source import UHC_FORMULARY_SOURCE_ID


UHC_DRUG_RECEIPT_LOCK_WAIT_SECONDS = 5.0
UHC_DRUG_RECEIPT_LOCK_RETRY_SECONDS = 0.1


def uhc_drug_receipt_id(
    source_id: str,
    candidate_dataset_id: str,
    source_observation_sha256: str,
    source_file_set_sha256: str,
    artifact_set_sha256: str,
    spool_content_sha256: str,
) -> str:
    """Derive the one stable receipt identity for exact admitted evidence."""

    return stable_id(
        "ffur_",
        source_id,
        candidate_dataset_id,
        strict_hash(source_observation_sha256, "source observation hash"),
        strict_hash(source_file_set_sha256, "source file set hash"),
        strict_hash(artifact_set_sha256, "artifact set hash"),
        strict_hash(spool_content_sha256, "spool content hash"),
    )


@dataclass(frozen=True, slots=True, repr=False)
class UHCDrugAdmissionReceipt:
    """Bind exact retained artifacts and spool evidence to one admission."""

    receipt_id: str
    source_observation_sha256: str = field(repr=False)
    artifact_set_sha256: str = field(repr=False)
    admission: TwinAdmissionResult = field(repr=False)
    evidence: UHCDrugSpoolEvidence = field(repr=False)
    recorded_at: dt.datetime

    def __post_init__(self) -> None:
        admission = self.admission
        evidence = self.evidence
        if not (
            type(admission) is TwinAdmissionResult
            and type(evidence) is UHCDrugSpoolEvidence
            and admission.source_id == UHC_FORMULARY_SOURCE_ID
            and evidence.source_id == admission.source_id
            and evidence.artifact_set_sha256 == self.artifact_set_sha256
            and evidence.file_count == 48
            and evidence.plan_count == admission.verification.list_count
            and evidence.plan_count == admission.verification.alias_count
            and evidence.medication_membership_count
            == admission.verification.medication_membership_count
            and evidence.max_last_updated_at is not None
            and evidence.max_last_updated_at <= admission.cutoff_at
        ):
            raise ValueError("UHC drug admission receipt is inconsistent")
        strict_hash(self.source_observation_sha256, "source observation hash")
        strict_hash(self.artifact_set_sha256, "artifact set hash")
        recorded_at = utc_timestamp(self.recorded_at, "receipt timestamp")
        if recorded_at < admission.admitted_at:
            raise ValueError("UHC drug admission receipt is inconsistent")
        expected_receipt_id = uhc_drug_receipt_id(
            admission.source_id,
            admission.candidate_dataset_id,
            self.source_observation_sha256,
            evidence.source_file_set_sha256,
            self.artifact_set_sha256,
            evidence.spool_content_sha256,
        )
        if self.receipt_id != expected_receipt_id:
            raise ValueError("UHC drug admission receipt is inconsistent")

    @property
    def source_id(self) -> str:
        """Return the admitted source owner."""

        return self.admission.source_id

    @property
    def candidate_dataset_id(self) -> str:
        """Return the admitted requested dataset."""

        return self.admission.candidate_dataset_id


@dataclass(frozen=True, slots=True, repr=False)
class UHCDrugPublicationInputs:
    """Reconstructed retained and repository authority for publication."""

    receipt: UHCDrugAdmissionReceipt = field(repr=False)
    binding: EnabledSourceBinding = field(repr=False)
    artifacts: VerifiedSourceArtifactSet = field(repr=False)
    candidate: DatasetRef = field(repr=False)

    def __post_init__(self) -> None:
        admission = self.receipt.admission
        if not (
            type(self.binding) is EnabledSourceBinding
            and type(self.artifacts) is VerifiedSourceArtifactSet
            and type(self.candidate) is DatasetRef
            and self.binding.source_id == admission.source_id
            and self.artifacts.source_id == admission.source_id
            and self.artifacts.source_file_set_sha256
            == self.receipt.evidence.source_file_set_sha256
            and self.artifacts.artifact_set_sha256
            == self.receipt.artifact_set_sha256
            and self.candidate.dataset_id == admission.candidate_dataset_id
            and self.candidate.intent == "requested"
            and self.candidate.status == "verified"
        ):
            raise ValueError("UHC drug publication inputs are inconsistent")


@dataclass(frozen=True, slots=True, repr=False)
class UHCDrugRecordedAdmission:
    """Return an in-process twin result with its durable receipt."""

    twin_result: UHCDrugTwinResult = field(repr=False)
    receipt: UHCDrugAdmissionReceipt = field(repr=False)

    def __post_init__(self) -> None:
        if not (
            type(self.twin_result) is UHCDrugTwinResult
            and type(self.receipt) is UHCDrugAdmissionReceipt
            and self.receipt.admission == self.twin_result.admission
            and self.receipt.evidence == self.twin_result.candidate.evidence
        ):
            raise ValueError("UHC drug recorded admission is inconsistent")


def _candidate_from_admission(admission: TwinAdmissionResult) -> DatasetRef:
    return DatasetRef(
        source_id=admission.source_id,
        dataset_id=admission.candidate_dataset_id,
        run_id=admission.candidate_run_id,
        previous_dataset_id=admission.predecessor_dataset_id,
        cutoff_at=admission.cutoff_at,
        acquisition_contract_hash=admission.acquisition_contract_hash,
        intent="requested",
        status="verified",
    )


def _evidence_from_row(receipt_by_field: dict[str, Any]) -> UHCDrugSpoolEvidence:
    return UHCDrugSpoolEvidence(
        source_id=receipt_by_field.get("source_id"),
        source_file_set_sha256=receipt_by_field.get(
            "source_file_set_sha256"
        ),
        artifact_set_sha256=receipt_by_field.get("artifact_set_sha256"),
        spool_content_sha256=receipt_by_field.get("spool_content_sha256"),
        file_count=receipt_by_field.get("file_count"),
        raw_record_count=receipt_by_field.get("raw_record_count"),
        raw_plan_entry_count=receipt_by_field.get("raw_plan_entry_count"),
        plan_count=receipt_by_field.get("plan_count"),
        medication_membership_count=receipt_by_field.get(
            "medication_membership_count"
        ),
        duplicate_count=receipt_by_field.get("duplicate_count"),
        superseded_count=receipt_by_field.get("superseded_count"),
        max_last_updated_at=utc_timestamp(
            receipt_by_field.get("max_last_updated_at"),
            "stored maximum update timestamp",
        ),
    )


async def load_uhc_drug_admission_receipt(
    *,
    receipt_id: str,
    database: Any = db,
) -> UHCDrugAdmissionReceipt:
    """Load one receipt and its generic admission from durable storage."""

    try:
        receipt_by_field = await load_uhc_receipt_row(
            receipt_id,
            database=database,
        )
        admission = await load_uhc_receipt_admission(
            receipt_by_field.get("source_id"),
            receipt_by_field.get("candidate_dataset_id"),
            database=database,
        )
        return UHCDrugAdmissionReceipt(
            receipt_id=receipt_by_field.get("receipt_id"),
            source_observation_sha256=receipt_by_field.get(
                "source_observation_sha256"
            ),
            artifact_set_sha256=receipt_by_field.get("artifact_set_sha256"),
            admission=admission,
            evidence=_evidence_from_row(receipt_by_field),
            recorded_at=utc_timestamp(
                receipt_by_field.get("recorded_at"),
                "stored receipt timestamp",
            ),
        )
    except RuntimeError:
        raise
    except Exception:
        raise RuntimeError("UHC drug admission receipt is invalid") from None


def _require_record_contract(
    binding: EnabledSourceBinding,
    artifacts: VerifiedSourceArtifactSet,
    twin_result: UHCDrugTwinResult,
) -> None:
    admission = twin_result.admission
    evidence = twin_result.candidate.evidence
    if not (
        twin_result.baseline.evidence == evidence
        and artifacts.source_id == admission.source_id == binding.source_id
        and artifacts.source_file_set_sha256
        == evidence.source_file_set_sha256
        and artifacts.artifact_set_sha256 == evidence.artifact_set_sha256
        and admission.source_configuration_hash == binding.configuration_hash
        and uhc_drug_sync_contract_hash(
            binding,
            artifacts,
            evidence,
            admission.cutoff_at,
        )
        == admission.acquisition_contract_hash
    ):
        raise RuntimeError("UHC drug admission receipt contract is inconsistent")


async def _record_receipt_under_lease(
    *,
    source_observation_sha256: str,
    artifacts: VerifiedSourceArtifactSet,
    twin_result: UHCDrugTwinResult,
    database: Any,
) -> UHCDrugAdmissionReceipt:
    """Record exact UHC evidence while the caller owns the source lease."""

    observation_hash = strict_hash(
        source_observation_sha256,
        "source observation hash",
    )
    binding = await register_uhc_formulary_source(database=database)
    current_artifacts = await load_complete_source_artifact_set(
        tuple(artifact.identity for artifact in artifacts.artifacts),
        database=database,
    )
    if current_artifacts != artifacts:
        raise RuntimeError("UHC drug admission receipt artifacts changed")
    stored_admission = await load_uhc_receipt_admission(
        twin_result.admission.source_id,
        twin_result.admission.candidate_dataset_id,
        database=database,
    )
    if stored_admission != twin_result.admission:
        raise RuntimeError("UHC drug admission receipt admission changed")
    _require_record_contract(binding, current_artifacts, twin_result)
    await require_source_unchanged(binding, database=database)
    async with database.transaction():
        receipt_id = uhc_drug_receipt_id(
            twin_result.admission.source_id,
            twin_result.admission.candidate_dataset_id,
            observation_hash,
            twin_result.candidate.evidence.source_file_set_sha256,
            twin_result.candidate.evidence.artifact_set_sha256,
            twin_result.candidate.evidence.spool_content_sha256,
        )
        await insert_uhc_receipt(
            receipt_id,
            observation_hash,
            twin_result.admission,
            twin_result.candidate.evidence,
            database=database,
        )
        stored_receipt = await load_uhc_drug_admission_receipt(
            receipt_id=receipt_id,
            database=database,
        )
    if not (
        stored_receipt.source_observation_sha256 == observation_hash
        and stored_receipt.artifact_set_sha256
        == current_artifacts.artifact_set_sha256
        and stored_receipt.admission == stored_admission
        and stored_receipt.evidence == twin_result.candidate.evidence
    ):
        raise RuntimeError("UHC drug admission receipt changed")
    await require_source_unchanged(binding, database=database)
    return stored_receipt


async def record_uhc_drug_admission_receipt(
    *,
    source_observation_sha256: str,
    artifacts: VerifiedSourceArtifactSet,
    twin_result: UHCDrugTwinResult,
    database: Any = db,
) -> UHCDrugAdmissionReceipt:
    """Persist or replay one exact receipt under a fresh source lease."""

    if (
        type(artifacts) is not VerifiedSourceArtifactSet
        or type(twin_result) is not UHCDrugTwinResult
    ):
        raise ValueError("UHC drug admission receipt input is invalid")
    async with manual_lock.manual_source_lease(
        database,
        UHC_FORMULARY_SOURCE_ID,
        wait_seconds=UHC_DRUG_RECEIPT_LOCK_WAIT_SECONDS,
        retry_seconds=UHC_DRUG_RECEIPT_LOCK_RETRY_SECONDS,
    ):
        return await _record_receipt_under_lease(
            source_observation_sha256=source_observation_sha256,
            artifacts=artifacts,
            twin_result=twin_result,
            database=database,
        )


async def reconstruct_uhc_drug_publication_inputs(
    *,
    receipt_id: str,
    database: Any = db,
    cancel_check: Any | None = None,
) -> UHCDrugPublicationInputs:
    """Reopen one receipt, exact CAS set, source, and candidate authority."""

    receipt = await load_uhc_drug_admission_receipt(
        receipt_id=receipt_id,
        database=database,
    )
    binding = await register_uhc_formulary_source(database=database)
    artifacts = await reopen_source_artifact_set(
        receipt.source_id,
        receipt.evidence.source_file_set_sha256,
        receipt.artifact_set_sha256,
        database=database,
        cancel_check=cancel_check,
    )
    expected_contract_hash = uhc_drug_sync_contract_hash(
        binding,
        artifacts,
        receipt.evidence,
        receipt.admission.cutoff_at,
    )
    if not (
        binding.configuration_hash
        == receipt.admission.source_configuration_hash
        and expected_contract_hash
        == receipt.admission.acquisition_contract_hash
    ):
        raise RuntimeError("UHC drug publication receipt contract changed")
    await require_source_unchanged(binding, database=database)
    return UHCDrugPublicationInputs(
        receipt=receipt,
        binding=binding,
        artifacts=artifacts,
        candidate=_candidate_from_admission(receipt.admission),
    )


__all__ = (
    "UHCDrugAdmissionReceipt",
    "UHCDrugPublicationInputs",
    "UHCDrugRecordedAdmission",
    "load_uhc_drug_admission_receipt",
    "record_uhc_drug_admission_receipt",
    "reconstruct_uhc_drug_publication_inputs",
    "uhc_drug_receipt_id",
    "validate_uhc_drug_receipt_id",
)
