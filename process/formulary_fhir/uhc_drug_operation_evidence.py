# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Safe aggregate evidence returned by UHC formulary operations."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field

from process.formulary_fhir.repository_shared import strict_hash
from process.formulary_fhir.repository_shared import strict_text
from process.formulary_fhir.repository_shared import utc_timestamp
from process.formulary_fhir.uhc_drug_receipt import UHCDrugAdmissionReceipt
from process.formulary_fhir.uhc_drug_receipt import validate_uhc_drug_receipt_id


def _validate_identifiers(evidence: "UHCDrugReceiptOperationEvidence") -> None:
    validate_uhc_drug_receipt_id(evidence.receipt_id)
    for identifier_label, identifier_value in (
        ("baseline run id", evidence.baseline_run_id),
        ("baseline dataset id", evidence.baseline_dataset_id),
        ("candidate run id", evidence.candidate_run_id),
        ("candidate dataset id", evidence.candidate_dataset_id),
    ):
        strict_text(identifier_value, identifier_label, 64)
    if evidence.predecessor_dataset_id is not None:
        strict_text(
            evidence.predecessor_dataset_id,
            "predecessor dataset id",
            64,
        )
    if (
        evidence.baseline_run_id == evidence.candidate_run_id
        or evidence.baseline_dataset_id == evidence.candidate_dataset_id
    ):
        raise ValueError("UHC formulary receipt evidence is invalid")


def _validate_hashes(evidence: "UHCDrugReceiptOperationEvidence") -> None:
    for digest_value in (
        evidence.source_observation_sha256,
        evidence.source_file_set_sha256,
        evidence.artifact_set_sha256,
        evidence.spool_content_sha256,
        evidence.source_configuration_hash,
        evidence.acquisition_contract_hash,
        evidence.coverage_hash,
        evidence.membership_hash,
    ):
        strict_hash(digest_value, "operation evidence hash")


def _validate_counts(evidence: "UHCDrugReceiptOperationEvidence") -> None:
    counts = (
        evidence.file_count,
        evidence.expected_file_count,
        evidence.excluded_file_count,
        evidence.raw_record_count,
        evidence.raw_plan_entry_count,
        evidence.plan_count,
        evidence.medication_membership_count,
        evidence.duplicate_count,
        evidence.superseded_count,
    )
    if not (
        all(type(count) is int and count >= 0 for count in counts)
        and evidence.expected_file_count == 48
        and 1 <= evidence.file_count <= evidence.expected_file_count
        and evidence.excluded_file_count
        == evidence.expected_file_count - evidence.file_count
        and evidence.raw_record_count > 0
        and evidence.raw_plan_entry_count > 0
        and evidence.plan_count > 0
        and evidence.medication_membership_count > 0
    ):
        raise ValueError("UHC formulary receipt evidence is invalid")


def _validate_timestamps(evidence: "UHCDrugReceiptOperationEvidence") -> None:
    ordered_timestamps = (
        utc_timestamp(
            evidence.max_last_updated_at,
            "maximum source update timestamp",
        ),
        utc_timestamp(evidence.cutoff_at, "operation cutoff"),
        utc_timestamp(
            evidence.baseline_verified_at,
            "baseline verification timestamp",
        ),
        utc_timestamp(
            evidence.candidate_verified_at,
            "candidate verification timestamp",
        ),
        utc_timestamp(evidence.admitted_at, "admission timestamp"),
        utc_timestamp(evidence.recorded_at, "receipt timestamp"),
    )
    if tuple(sorted(ordered_timestamps)) != ordered_timestamps:
        raise ValueError("UHC formulary receipt evidence is invalid")


@dataclass(frozen=True, slots=True, repr=False)
class UHCDrugReceiptOperationEvidence:
    """Expose the exact durable receipt and generic admission evidence."""

    receipt_id: str
    source_observation_sha256: str = field(repr=False)
    source_file_set_sha256: str = field(repr=False)
    artifact_set_sha256: str = field(repr=False)
    spool_content_sha256: str = field(repr=False)
    source_configuration_hash: str = field(repr=False)
    acquisition_contract_hash: str = field(repr=False)
    coverage_hash: str = field(repr=False)
    membership_hash: str = field(repr=False)
    baseline_run_id: str
    baseline_dataset_id: str
    candidate_run_id: str
    candidate_dataset_id: str
    predecessor_dataset_id: str | None
    cutoff_at: dt.datetime
    file_count: int
    expected_file_count: int
    excluded_file_count: int
    raw_record_count: int
    raw_plan_entry_count: int
    plan_count: int
    medication_membership_count: int
    duplicate_count: int
    superseded_count: int
    max_last_updated_at: dt.datetime
    baseline_verified_at: dt.datetime
    candidate_verified_at: dt.datetime
    admitted_at: dt.datetime
    recorded_at: dt.datetime

    def __post_init__(self) -> None:
        """Revalidate all receipt fields at every construction boundary."""

        _validate_identifiers(self)
        _validate_hashes(self)
        _validate_counts(self)
        _validate_timestamps(self)

    @property
    def coverage_status(self) -> str:
        """Return the aggregate artifact-coverage classification."""

        return "complete" if self.excluded_file_count == 0 else "partial"


def receipt_operation_evidence(
    receipt: UHCDrugAdmissionReceipt,
) -> UHCDrugReceiptOperationEvidence:
    """Project one validated durable receipt into safe operator evidence."""

    if type(receipt) is not UHCDrugAdmissionReceipt:
        raise ValueError("UHC formulary receipt evidence is invalid")
    admission = receipt.admission
    spool_evidence = receipt.evidence
    verification = admission.verification
    return UHCDrugReceiptOperationEvidence(
        receipt_id=receipt.receipt_id,
        source_observation_sha256=receipt.source_observation_sha256,
        source_file_set_sha256=spool_evidence.source_file_set_sha256,
        artifact_set_sha256=receipt.artifact_set_sha256,
        spool_content_sha256=spool_evidence.spool_content_sha256,
        source_configuration_hash=admission.source_configuration_hash,
        acquisition_contract_hash=admission.acquisition_contract_hash,
        coverage_hash=verification.coverage_hash,
        membership_hash=verification.membership_hash,
        baseline_run_id=admission.baseline_run_id,
        baseline_dataset_id=admission.baseline_dataset_id,
        candidate_run_id=admission.candidate_run_id,
        candidate_dataset_id=admission.candidate_dataset_id,
        predecessor_dataset_id=admission.predecessor_dataset_id,
        cutoff_at=admission.cutoff_at,
        file_count=spool_evidence.file_count,
        expected_file_count=receipt.expected_file_count,
        excluded_file_count=receipt.excluded_file_count,
        raw_record_count=spool_evidence.raw_record_count,
        raw_plan_entry_count=spool_evidence.raw_plan_entry_count,
        plan_count=spool_evidence.plan_count,
        medication_membership_count=(
            spool_evidence.medication_membership_count
        ),
        duplicate_count=spool_evidence.duplicate_count,
        superseded_count=spool_evidence.superseded_count,
        max_last_updated_at=spool_evidence.max_last_updated_at,
        baseline_verified_at=admission.baseline_verified_at,
        candidate_verified_at=admission.candidate_verified_at,
        admitted_at=admission.admitted_at,
        recorded_at=receipt.recorded_at,
    )


def _utc_text(timestamp: dt.datetime) -> str:
    return utc_timestamp(timestamp, "operation evidence timestamp").isoformat().replace(
        "+00:00",
        "Z",
    )


def receipt_operation_payload(
    receipt_evidence: UHCDrugReceiptOperationEvidence,
) -> dict[str, object]:
    """Serialize common receipt evidence without source identifiers."""

    if type(receipt_evidence) is not UHCDrugReceiptOperationEvidence:
        raise ValueError("UHC formulary receipt evidence is invalid")
    receipt_evidence.__post_init__()
    return {
        "acquisition_contract_hash": (
            receipt_evidence.acquisition_contract_hash
        ),
        "admitted_at": _utc_text(receipt_evidence.admitted_at),
        "artifact_set_sha256": receipt_evidence.artifact_set_sha256,
        "baseline_dataset_id": receipt_evidence.baseline_dataset_id,
        "baseline_run_id": receipt_evidence.baseline_run_id,
        "candidate_dataset_id": receipt_evidence.candidate_dataset_id,
        "candidate_run_id": receipt_evidence.candidate_run_id,
        "coverage_hash": receipt_evidence.coverage_hash,
        "coverage": {
            "status": receipt_evidence.coverage_status,
            "expected_artifact_count": receipt_evidence.expected_file_count,
            "included_artifact_count": receipt_evidence.file_count,
            "missing_artifact_count": receipt_evidence.excluded_file_count,
        },
        "cutoff": _utc_text(receipt_evidence.cutoff_at),
        "duplicate_count": receipt_evidence.duplicate_count,
        "file_count": receipt_evidence.file_count,
        "max_last_updated_at": _utc_text(
            receipt_evidence.max_last_updated_at
        ),
        "medication_membership_count": (
            receipt_evidence.medication_membership_count
        ),
        "membership_hash": receipt_evidence.membership_hash,
        "plan_count": receipt_evidence.plan_count,
        "raw_plan_entry_count": receipt_evidence.raw_plan_entry_count,
        "raw_record_count": receipt_evidence.raw_record_count,
        "receipt_id": receipt_evidence.receipt_id,
        "recorded_at": _utc_text(receipt_evidence.recorded_at),
        "source_configuration_hash": (
            receipt_evidence.source_configuration_hash
        ),
        "source_file_set_sha256": receipt_evidence.source_file_set_sha256,
        "source_observation_sha256": (
            receipt_evidence.source_observation_sha256
        ),
        "spool_content_sha256": receipt_evidence.spool_content_sha256,
        "superseded_count": receipt_evidence.superseded_count,
    }


__all__ = (
    "UHCDrugReceiptOperationEvidence",
    "receipt_operation_evidence",
    "receipt_operation_payload",
)
