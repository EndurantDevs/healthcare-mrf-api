# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Default-off acquisition and admission phase for UHC drug artifacts."""

from __future__ import annotations

import asyncio
from typing import Any

from db.models import db
from process.formulary_fhir.repository_shared import strict_hash
from process.formulary_fhir.uhc_drug_acquisition import (
    acquire_current_uhc_drug_artifacts,
)
from process.formulary_fhir.uhc_drug_operation import (
    require_uhc_acquisition_gate,
)
from process.formulary_fhir.uhc_drug_operation import (
    UHCDrugAdmissionOperationResult,
)
from process.formulary_fhir.uhc_drug_operation import UHCDrugOperationError
from process.formulary_fhir.uhc_drug_operation import UHCDrugRunIdentities
from process.formulary_fhir.uhc_drug_operation import receipt_operation_evidence
from process.formulary_fhir.uhc_drug_operation import uhc_drug_run_identities
from process.formulary_fhir.uhc_drug_operation import uhc_drug_work_directory
from process.formulary_fhir.uhc_drug_operation import uhc_operation_error
from process.formulary_fhir.uhc_drug_receipt import UHCDrugRecordedAdmission
from process.formulary_fhir.uhc_drug_twin import (
    verify_and_record_uhc_drug_twins,
)


def _admission_result(
    acquisition: Any,
    recorded: UHCDrugRecordedAdmission,
    identities: UHCDrugRunIdentities,
) -> UHCDrugAdmissionOperationResult:
    receipt = recorded.receipt
    evidence = receipt.evidence
    admission = receipt.admission
    if not (
        receipt.source_observation_sha256 == acquisition.source_observation_sha256
        and receipt.artifact_set_sha256 == acquisition.artifact_set_sha256
        and evidence.source_file_set_sha256 == acquisition.source_file_set_sha256
        and admission.baseline_run_id == identities.baseline_run_id
        and admission.candidate_run_id == identities.candidate_run_id
        and admission.cutoff_at == identities.cutoff_at
    ):
        raise UHCDrugOperationError("evidence")
    try:
        return UHCDrugAdmissionOperationResult(
            evidence=receipt_operation_evidence(receipt),
            downloaded_file_count=acquisition.downloaded_file_count,
            reused_file_count=acquisition.reused_file_count,
            downloaded_byte_count=acquisition.downloaded_byte_count,
        )
    except (TypeError, ValueError):
        raise UHCDrugOperationError("evidence") from None


async def acquire_and_admit_uhc_drugs(
    *,
    raw_set_sha256: str,
    database: Any = db,
) -> UHCDrugAdmissionOperationResult:
    """Acquire under its source claim, then build and receipt exact twins."""

    require_uhc_acquisition_gate()
    try:
        selected_raw_set = strict_hash(raw_set_sha256, "raw set hash")
        work_directory = uhc_drug_work_directory()
        acquisition = await acquire_current_uhc_drug_artifacts(
            raw_set_sha256=selected_raw_set,
            database=database,
        )
        cutoff_at = max(
            artifact.verified_at for artifact in acquisition.artifacts.artifacts
        )
        identities = uhc_drug_run_identities(
            acquisition.source_observation_sha256,
            acquisition.source_file_set_sha256,
            acquisition.artifact_set_sha256,
            cutoff_at,
        )
        recorded = await verify_and_record_uhc_drug_twins(
            source_observation_sha256=acquisition.source_observation_sha256,
            artifacts=acquisition.artifacts,
            baseline_run_id=identities.baseline_run_id,
            candidate_run_id=identities.candidate_run_id,
            cutoff=identities.cutoff_at,
            work_directory=work_directory,
            database=database,
        )
        return _admission_result(acquisition, recorded, identities)
    except (asyncio.CancelledError, TimeoutError):
        raise
    except UHCDrugOperationError:
        raise
    except (TypeError, ValueError):
        raise UHCDrugOperationError("invalid_request") from None
    except Exception as error:
        raise uhc_operation_error(error, "acquisition") from None


__all__ = ("acquire_and_admit_uhc_drugs",)
