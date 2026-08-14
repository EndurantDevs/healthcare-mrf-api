# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL persistence helpers for immutable UHC admission receipts."""

from __future__ import annotations

import re
from typing import Any

from process.formulary_fhir.repository_admission import ADMISSION_COLUMNS
from process.formulary_fhir.repository_admission_types import TwinAdmissionResult
from process.formulary_fhir.repository_admission_types import result_from_row
from process.formulary_fhir.repository_shared import row_mapping
from process.formulary_fhir.repository_shared import strict_hash
from process.formulary_fhir.repository_shared import strict_text
from process.formulary_fhir.repository_shared import table_name
from process.formulary_fhir.uhc_drug_parser_contract import UHCDrugSpoolEvidence


UHC_DRUG_RECEIPT_ID_PATTERN = re.compile(r"ffur_[0-9a-f]{48}\Z")
UHC_DRUG_PARTIAL_EXCLUSION_CODE = "not_selected"

_RECEIPT_COLUMNS = (
    "receipt_id",
    "source_id",
    "source_observation_sha256",
    "source_file_set_sha256",
    "artifact_set_sha256",
    "candidate_dataset_id",
    "spool_content_sha256",
    "file_count",
    "expected_file_count",
    "excluded_file_count",
    "selected_source_file_ids",
    "exclusion_code",
    "raw_record_count",
    "raw_plan_entry_count",
    "plan_count",
    "medication_membership_count",
    "duplicate_count",
    "superseded_count",
    "max_last_updated_at",
    "recorded_at",
)


def selected_source_file_ids(selected_ids: object) -> tuple[str, ...]:
    """Normalize one private canonical artifact selection from storage."""

    if type(selected_ids) is list:
        selected_ids = tuple(selected_ids)
    if (
        type(selected_ids) is not tuple
        or not 1 <= len(selected_ids) <= 48
        or len(set(selected_ids)) != len(selected_ids)
    ):
        raise ValueError("UHC drug receipt artifact selection is invalid")
    for source_file_id in selected_ids:
        strict_hash(source_file_id, "receipt source file id")
    return selected_ids


class UHCDrugReceiptStoreError(RuntimeError):
    """Expose one bounded durable-receipt lookup failure."""

    def __init__(self, code: str) -> None:
        self.code = "missing" if code == "missing" else "evidence"
        super().__init__(f"UHC drug admission receipt {self.code}")


def validate_uhc_drug_receipt_id(receipt_id: object) -> str:
    """Require one canonical UHC admission receipt identifier."""

    normalized_receipt_id = strict_text(receipt_id, "receipt id", 53)
    if not UHC_DRUG_RECEIPT_ID_PATTERN.fullmatch(normalized_receipt_id):
        raise ValueError("FHIR formulary receipt id is invalid")
    return normalized_receipt_id


async def load_uhc_receipt_row(
    receipt_id: str,
    *,
    database: Any,
) -> dict[str, Any]:
    """Load one receipt plus its exact retained-observation binding."""

    normalized_receipt_id = validate_uhc_drug_receipt_id(receipt_id)
    receipt_by_field = row_mapping(
        await database.first(
            f"SELECT {', '.join('receipt.' + column for column in _RECEIPT_COLUMNS)}, "
            "observation.source_file_set_sha256 AS observed_file_set_sha256 "
            f"FROM {table_name('fhir_formulary_uhc_admission_receipt')} "
            "AS receipt JOIN "
            f"{table_name('fhir_formulary_source_artifact_observation')} "
            "AS observation ON observation.source_id = receipt.source_id AND "
            "observation.source_observation_sha256 = "
            "receipt.source_observation_sha256 WHERE "
            "receipt.receipt_id = :receipt_id;",
            receipt_id=normalized_receipt_id,
        )
    )
    if not receipt_by_field:
        raise UHCDrugReceiptStoreError("missing")
    if receipt_by_field.get("observed_file_set_sha256") != receipt_by_field.get(
        "source_file_set_sha256"
    ):
        raise RuntimeError("UHC drug admission receipt is inconsistent")
    return receipt_by_field


async def load_uhc_receipt_admission(
    source_id: str,
    candidate_dataset_id: str,
    *,
    database: Any,
) -> TwinAdmissionResult:
    """Load the generic twin admission linked by a UHC receipt."""

    admission_by_field = row_mapping(
        await database.first(
            f"SELECT {', '.join(ADMISSION_COLUMNS)} FROM "
            f"{table_name('fhir_formulary_twin_admission')} WHERE "
            "source_id = :source_id AND "
            "candidate_dataset_id = :candidate_dataset_id;",
            source_id=strict_text(source_id, "source id", 64),
            candidate_dataset_id=strict_text(
                candidate_dataset_id,
                "candidate dataset id",
                64,
            ),
        )
    )
    if not admission_by_field:
        raise RuntimeError("UHC drug twin admission is missing")
    return result_from_row(admission_by_field)


async def insert_uhc_receipt(
    receipt_id: str,
    source_observation_sha256: str,
    admission: TwinAdmissionResult,
    evidence: UHCDrugSpoolEvidence,
    *,
    selected_source_file_ids_value: tuple[str, ...],
    exclusion_code: str | None,
    database: Any,
) -> None:
    """Insert one receipt if absent; semantic readback resolves conflicts."""

    selected_ids = selected_source_file_ids(selected_source_file_ids_value)
    await database.status(
        f"INSERT INTO {table_name('fhir_formulary_uhc_admission_receipt')} ("
        "receipt_id, source_id, source_observation_sha256, "
        "source_file_set_sha256, artifact_set_sha256, candidate_dataset_id, "
        "spool_content_sha256, file_count, expected_file_count, "
        "excluded_file_count, selected_source_file_ids, exclusion_code, "
        "raw_record_count, "
        "raw_plan_entry_count, plan_count, medication_membership_count, "
        "duplicate_count, superseded_count, max_last_updated_at) VALUES ("
        ":receipt_id, :source_id, :source_observation_sha256, "
        ":source_file_set_sha256, :artifact_set_sha256, "
        ":candidate_dataset_id, :spool_content_sha256, :file_count, "
        ":expected_file_count, :excluded_file_count, "
        ":selected_source_file_ids, :exclusion_code, "
        ":raw_record_count, :raw_plan_entry_count, :plan_count, "
        ":medication_membership_count, :duplicate_count, :superseded_count, "
        ":max_last_updated_at) ON CONFLICT DO NOTHING;",
        receipt_id=validate_uhc_drug_receipt_id(receipt_id),
        source_id=admission.source_id,
        source_observation_sha256=source_observation_sha256,
        source_file_set_sha256=evidence.source_file_set_sha256,
        artifact_set_sha256=evidence.artifact_set_sha256,
        candidate_dataset_id=admission.candidate_dataset_id,
        spool_content_sha256=evidence.spool_content_sha256,
        file_count=evidence.file_count,
        expected_file_count=evidence.expected_file_count,
        excluded_file_count=evidence.excluded_file_count,
        selected_source_file_ids=selected_ids,
        exclusion_code=exclusion_code,
        raw_record_count=evidence.raw_record_count,
        raw_plan_entry_count=evidence.raw_plan_entry_count,
        plan_count=evidence.plan_count,
        medication_membership_count=evidence.medication_membership_count,
        duplicate_count=evidence.duplicate_count,
        superseded_count=evidence.superseded_count,
        max_last_updated_at=evidence.max_last_updated_at,
    )


__all__ = (
    "insert_uhc_receipt",
    "load_uhc_receipt_admission",
    "load_uhc_receipt_row",
    "selected_source_file_ids",
    "validate_uhc_drug_receipt_id",
    "UHC_DRUG_PARTIAL_EXCLUSION_CODE",
    "UHCDrugReceiptStoreError",
)
