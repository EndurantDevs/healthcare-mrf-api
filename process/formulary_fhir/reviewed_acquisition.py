# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Default-off acquisition of one fixed reviewed formulary twin pair."""

from __future__ import annotations

import asyncio
import datetime as dt
from dataclasses import dataclass, field
from typing import Any

from db.models import db
from process.formulary_fhir.client import FHIRFormularyClient
from process.formulary_fhir.repository_admission import TwinAdmissionError
from process.formulary_fhir.repository_admission_types import TwinAdmissionResult
from process.formulary_fhir.repository_shared import json_text
from process.formulary_fhir.repository_shared import stable_id
from process.formulary_fhir.repository_shared import strict_hash
from process.formulary_fhir.repository_shared import strict_text
from process.formulary_fhir.repository_shared import utc_timestamp
from process.formulary_fhir.reviewed_operation import require_acquisition_gate
from process.formulary_fhir.reviewed_operation import ReviewedOperationError
from process.formulary_fhir.reviewed_operation import ReviewedRunIdentities
from process.formulary_fhir.reviewed_operation import reviewed_run_identities
from process.formulary_fhir.reviewed_source import ReviewedSourceError
from process.formulary_fhir.reviewed_source import reviewed_source_manifest
from process.formulary_fhir.reviewed_twin import verify_reviewed_source_twins
from process.formulary_fhir.synchronizer import ClientFactory


@dataclass(frozen=True, slots=True, repr=False)
class ReviewedAcquisitionResult:
    """Expose bounded immutable admission evidence for operator recording."""

    baseline_run_id: str
    candidate_run_id: str
    baseline_dataset_id: str
    candidate_dataset_id: str
    cutoff_at: dt.datetime
    source_configuration_hash: str = field(repr=False)
    acquisition_contract_hash: str = field(repr=False)
    list_count: int
    alias_count: int
    medication_count: int
    coverage_hash: str = field(repr=False)
    membership_hash: str = field(repr=False)
    alternative_count: int
    alternative_hash: str = field(repr=False)
    admitted_at: dt.datetime

    def __post_init__(self) -> None:
        for label, identifier_value in (
            ("baseline run id", self.baseline_run_id),
            ("candidate run id", self.candidate_run_id),
            ("baseline dataset id", self.baseline_dataset_id),
            ("candidate dataset id", self.candidate_dataset_id),
        ):
            strict_text(identifier_value, label, 64)
        utc_timestamp(self.cutoff_at, "reviewed acquisition cutoff")
        utc_timestamp(self.admitted_at, "reviewed admission timestamp")
        for label, evidence_hash in (
            ("source configuration hash", self.source_configuration_hash),
            ("acquisition contract hash", self.acquisition_contract_hash),
            ("coverage hash", self.coverage_hash),
            ("membership hash", self.membership_hash),
            ("alternative hash", self.alternative_hash),
        ):
            strict_hash(evidence_hash, label)
        for count_value in (
            self.list_count,
            self.alias_count,
            self.medication_count,
        ):
            if type(count_value) is not int or count_value <= 0:
                raise ValueError("reviewed acquisition count is invalid")
        if type(self.alternative_count) is not int or self.alternative_count < 0:
            raise ValueError("reviewed alternative count is invalid")


def _acquisition_result(
    admission: TwinAdmissionResult,
    identities: ReviewedRunIdentities,
) -> ReviewedAcquisitionResult:
    manifest = reviewed_source_manifest()
    expected_baseline_dataset_id = stable_id(
        "ffd_",
        manifest.source_id,
        identities.baseline_run_id,
    )
    expected_candidate_dataset_id = stable_id(
        "ffd_",
        manifest.source_id,
        identities.candidate_run_id,
    )
    if not (
        type(admission) is TwinAdmissionResult
        and admission.source_id == manifest.source_id
        and admission.baseline_run_id == identities.baseline_run_id
        and admission.candidate_run_id == identities.candidate_run_id
        and admission.baseline_dataset_id == expected_baseline_dataset_id
        and admission.candidate_dataset_id == expected_candidate_dataset_id
        and admission.cutoff_at == identities.cutoff_at
    ):
        raise ReviewedOperationError("evidence")
    verification = admission.verification
    alternative = admission.alternative
    try:
        return ReviewedAcquisitionResult(
            identities.baseline_run_id,
            identities.candidate_run_id,
            admission.baseline_dataset_id,
            admission.candidate_dataset_id,
            admission.cutoff_at,
            admission.source_configuration_hash,
            admission.acquisition_contract_hash,
            verification.list_count,
            verification.alias_count,
            verification.medication_membership_count,
            verification.coverage_hash,
            verification.membership_hash,
            alternative.count,
            alternative.evidence_hash,
            admission.admitted_at,
        )
    except (TypeError, ValueError):
        raise ReviewedOperationError("evidence") from None


def _acquisition_error(error: BaseException) -> ReviewedOperationError:
    error_code = getattr(error, "code", "acquisition")
    if error_code not in {"busy", "invalid_request", "mismatch"}:
        error_code = "acquisition"
    return ReviewedOperationError(error_code)


async def acquire_reviewed_twins(
    *,
    cutoff: dt.datetime,
    database: Any = db,
    client_factory: ClientFactory = FHIRFormularyClient,
) -> ReviewedAcquisitionResult:
    """Acquire and admit two full roots without changing the current pointer."""

    require_acquisition_gate()
    identities = reviewed_run_identities(cutoff)
    try:
        admission = await verify_reviewed_source_twins(
            baseline_run_id=identities.baseline_run_id,
            candidate_run_id=identities.candidate_run_id,
            cutoff=identities.cutoff_at,
            database=database,
            client_factory=client_factory,
        )
        return _acquisition_result(admission, identities)
    except (asyncio.CancelledError, TimeoutError):
        raise
    except (ReviewedSourceError, TwinAdmissionError) as error:
        raise _acquisition_error(error) from None
    except ReviewedOperationError:
        raise
    except Exception:
        raise ReviewedOperationError("acquisition") from None


def acquisition_result_json(result: ReviewedAcquisitionResult) -> str:
    """Serialize only safe, validated acquisition and admission evidence."""

    if type(result) is not ReviewedAcquisitionResult:
        raise ReviewedOperationError("evidence")
    try:
        result.__post_init__()
    except (TypeError, ValueError):
        raise ReviewedOperationError("evidence") from None
    return json_text(
        {
            "status": "admitted",
            "baseline_run_id": result.baseline_run_id,
            "candidate_run_id": result.candidate_run_id,
            "baseline_dataset_id": result.baseline_dataset_id,
            "candidate_dataset_id": result.candidate_dataset_id,
            "cutoff": result.cutoff_at.isoformat().replace("+00:00", "Z"),
            "source_configuration_hash": result.source_configuration_hash,
            "acquisition_contract_hash": result.acquisition_contract_hash,
            "list_count": result.list_count,
            "alias_count": result.alias_count,
            "medication_count": result.medication_count,
            "coverage_hash": result.coverage_hash,
            "membership_hash": result.membership_hash,
            "alternative_count": result.alternative_count,
            "alternative_hash": result.alternative_hash,
            "admitted_at": result.admitted_at.isoformat().replace("+00:00", "Z"),
        }
    )


__all__ = (
    "ReviewedAcquisitionResult",
    "acquire_reviewed_twins",
    "acquisition_result_json",
)
