# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Independent UHC drug normalization, repository roots, and admission."""

from __future__ import annotations

import asyncio
import datetime as dt
from dataclasses import dataclass, field
import os
from pathlib import Path
import stat
import tempfile
from typing import Any
from typing import TYPE_CHECKING

from db.models import db
from process.formulary_fhir.async_safety import cancellable_to_thread
from process.formulary_fhir.async_safety import drain_operation
import process.formulary_fhir.manual_lock as manual_lock
from process.formulary_fhir.uhc_drug_acquisition import (
    UHCDrugArtifactAcquisitionResult,
)
from process.formulary_fhir.repository import FHIRFormularyRepository
from process.formulary_fhir.repository_admission import admit_verified_twins
from process.formulary_fhir.repository_admission_types import TwinAdmissionResult
from process.formulary_fhir.repository_admission_types import verification_values
from process.formulary_fhir.repository_shared import utc_timestamp
from process.formulary_fhir.repository_shared import strict_hash
from process.formulary_fhir.source import require_source_unchanged
from process.formulary_fhir.source_artifact_contract import (
    VerifiedSourceArtifactSet,
)
from process.formulary_fhir.source_artifacts import (
    load_selected_source_artifact_set,
)
from process.formulary_fhir.source_artifacts import (
    load_source_artifact_identities,
)
from process.formulary_fhir.uhc_drug_parser_contract import UHCDrugSpoolEvidence
from process.formulary_fhir.uhc_drug_spool import materialize_uhc_drug_spool
from process.formulary_fhir.uhc_drug_spool_reader import (
    verify_spooled_uhc_evidence,
)
from process.formulary_fhir.uhc_drug_sync import (
    _synchronize_requested_uhc_drug_dataset,
)
from process.formulary_fhir.uhc_drug_sync import synchronize_uhc_drug_dataset
from process.formulary_fhir.uhc_drug_sync_contract import (
    UHCDrugSynchronizationResult,
)
from process.formulary_fhir.uhc_drug_sync_contract import (
    validate_uhc_drug_run_id,
)
from process.formulary_fhir.uhc_drug_sync_contract import (
    uhc_drug_sync_contract_hash,
)
from process.formulary_fhir.uhc_source import register_uhc_formulary_source
from process.formulary_fhir.uhc_source import UHC_FORMULARY_SOURCE_ID


if TYPE_CHECKING:
    from process.formulary_fhir.uhc_drug_receipt import UHCDrugRecordedAdmission


UHC_DRUG_TWIN_TIMEOUT_SECONDS = 604_800
UHC_DRUG_LOCK_WAIT_SECONDS = 5.0
UHC_DRUG_LOCK_RETRY_SECONDS = 0.1


@dataclass(frozen=True, slots=True, repr=False)
class UHCDrugTwinResult:
    """Expose two independently built roots and their immutable admission."""

    admission: TwinAdmissionResult = field(repr=False)
    baseline: UHCDrugSynchronizationResult = field(repr=False)
    candidate: UHCDrugSynchronizationResult = field(repr=False)

    def __post_init__(self) -> None:
        baseline_dataset = self.baseline.dataset
        candidate_dataset = self.candidate.dataset
        admission = self.admission
        if not (
            type(admission) is TwinAdmissionResult
            and type(self.baseline) is UHCDrugSynchronizationResult
            and type(self.candidate) is UHCDrugSynchronizationResult
            and self.baseline.evidence == self.candidate.evidence
            and baseline_dataset.source_id == UHC_FORMULARY_SOURCE_ID
            and candidate_dataset.source_id == UHC_FORMULARY_SOURCE_ID
            and baseline_dataset.intent == "none"
            and candidate_dataset.intent == "requested"
            and baseline_dataset.previous_dataset_id
            == candidate_dataset.previous_dataset_id
            and admission.baseline_dataset_id == baseline_dataset.dataset_id
            and admission.baseline_run_id == baseline_dataset.run_id
            and admission.candidate_dataset_id == candidate_dataset.dataset_id
            and admission.candidate_run_id == candidate_dataset.run_id
            and admission.predecessor_dataset_id
            == candidate_dataset.previous_dataset_id
            and admission.cutoff_at == candidate_dataset.cutoff_at
            and admission.acquisition_contract_hash
            == baseline_dataset.acquisition_contract_hash
            == candidate_dataset.acquisition_contract_hash
            and verification_values(self.baseline.verification)
            == verification_values(self.candidate.verification)
            == verification_values(admission.verification)
            and admission.alternative.count == 0
        ):
            raise ValueError("UHC drug twin result is inconsistent")


@dataclass(frozen=True, slots=True)
class _TwinRequest:
    artifacts: VerifiedSourceArtifactSet
    baseline_run_id: str
    candidate_run_id: str
    cutoff_at: dt.datetime
    work_directory: Path
    database: Any
    repository: Any | None


def _validated_work_directory(work_directory: Path | str) -> Path:
    try:
        exact_path = Path(work_directory)
        resolved_path = exact_path.resolve(strict=True)
        metadata = exact_path.lstat()
    except (OSError, TypeError, ValueError):
        raise ValueError("UHC drug twin work directory is invalid") from None
    if (
        not exact_path.is_absolute()
        or exact_path != resolved_path
        or not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != os.geteuid()
        or stat.S_IMODE(metadata.st_mode) & 0o077
    ):
        raise ValueError("UHC drug twin work directory is invalid")
    return exact_path


def _validated_twin_request(
    artifacts: VerifiedSourceArtifactSet,
    baseline_run_id: str,
    candidate_run_id: str,
    cutoff: dt.datetime,
    work_directory: Path | str,
    database: Any,
    repository: Any | None,
) -> _TwinRequest:
    normalized_baseline = validate_uhc_drug_run_id(baseline_run_id)
    normalized_candidate = validate_uhc_drug_run_id(candidate_run_id)
    normalized_cutoff = utc_timestamp(cutoff, "UHC drug twin cutoff")
    if (
        type(artifacts) is not VerifiedSourceArtifactSet
        or artifacts.source_id != UHC_FORMULARY_SOURCE_ID
        or normalized_baseline == normalized_candidate
        or normalized_cutoff > dt.datetime.now(dt.UTC)
    ):
        raise ValueError("UHC drug twin request is invalid")
    return _TwinRequest(
        artifacts=artifacts,
        baseline_run_id=normalized_baseline,
        candidate_run_id=normalized_candidate,
        cutoff_at=normalized_cutoff,
        work_directory=_validated_work_directory(work_directory),
        database=database,
        repository=repository,
    )


async def _materialize_independent_spools(
    artifacts: VerifiedSourceArtifactSet,
    work_directory: Path,
) -> tuple[Path, UHCDrugSpoolEvidence, Path, UHCDrugSpoolEvidence]:
    baseline_path = work_directory / "baseline.sqlite"
    candidate_path = work_directory / "candidate.sqlite"
    baseline_evidence = await cancellable_to_thread(
        materialize_uhc_drug_spool,
        artifacts,
        spool_path=baseline_path,
    )
    candidate_evidence = await cancellable_to_thread(
        materialize_uhc_drug_spool,
        artifacts,
        spool_path=candidate_path,
    )
    baseline_state = baseline_path.stat()
    candidate_state = candidate_path.stat()
    if (
        baseline_evidence != candidate_evidence
        or (baseline_state.st_dev, baseline_state.st_ino)
        == (candidate_state.st_dev, candidate_state.st_ino)
    ):
        raise RuntimeError("UHC drug independent normalizations do not match")
    await cancellable_to_thread(
        verify_spooled_uhc_evidence,
        baseline_path,
        baseline_evidence,
        artifacts,
    )
    await cancellable_to_thread(
        verify_spooled_uhc_evidence,
        candidate_path,
        candidate_evidence,
        artifacts,
    )
    return (
        baseline_path,
        baseline_evidence,
        candidate_path,
        candidate_evidence,
    )


async def _require_artifacts_unchanged(
    request: _TwinRequest,
) -> None:
    identities = await load_source_artifact_identities(
        request.artifacts.source_id,
        request.artifacts.source_file_set_sha256,
        database=request.database,
    )
    selected_source_file_ids = tuple(
        artifact.identity.source_file_id
        for artifact in request.artifacts.artifacts
    )
    current_artifacts = await load_selected_source_artifact_set(
        identities,
        selected_source_file_ids=selected_source_file_ids,
        database=request.database,
    )
    if current_artifacts != request.artifacts:
        raise RuntimeError("UHC drug retained artifacts changed during twin build")


def _require_matching_contract_hashes(
    binding: Any,
    request: _TwinRequest,
    baseline_evidence: UHCDrugSpoolEvidence,
    candidate_evidence: UHCDrugSpoolEvidence,
) -> None:
    """Reject independent normalizations with different repository contracts."""

    baseline_contract_hash = uhc_drug_sync_contract_hash(
        binding,
        request.artifacts,
        baseline_evidence,
        request.cutoff_at,
    )
    candidate_contract_hash = uhc_drug_sync_contract_hash(
        binding,
        request.artifacts,
        candidate_evidence,
        request.cutoff_at,
    )
    if baseline_contract_hash != candidate_contract_hash:
        raise RuntimeError("UHC drug independent contracts do not match")


async def _synchronize_twin_roots(
    request: _TwinRequest,
    work_directory: Path,
    binding: Any,
    repository: Any,
) -> tuple[UHCDrugSynchronizationResult, UHCDrugSynchronizationResult]:
    """Build baseline and requested roots from independently parsed spools."""

    (
        baseline_path,
        baseline_evidence,
        candidate_path,
        candidate_evidence,
    ) = await _materialize_independent_spools(request.artifacts, work_directory)
    _require_matching_contract_hashes(
        binding,
        request,
        baseline_evidence,
        candidate_evidence,
    )
    baseline = await synchronize_uhc_drug_dataset(
        binding=binding,
        artifacts=request.artifacts,
        spool_path=baseline_path,
        evidence=baseline_evidence,
        run_id=request.baseline_run_id,
        cutoff_at=request.cutoff_at,
        database=request.database,
        repository=repository,
    )
    candidate = await _synchronize_requested_uhc_drug_dataset(
        binding=binding,
        artifacts=request.artifacts,
        spool_path=candidate_path,
        evidence=candidate_evidence,
        run_id=request.candidate_run_id,
        cutoff_at=request.cutoff_at,
        database=request.database,
        repository=repository,
    )
    return baseline, candidate


async def _build_and_admit_twins(
    request: _TwinRequest,
    work_directory: Path,
) -> UHCDrugTwinResult:
    """Build two exact roots, persist their admission, and recheck inputs."""

    binding = await register_uhc_formulary_source(database=request.database)
    selected_repository = (
        request.repository
        if request.repository is not None
        else FHIRFormularyRepository(
            source_id=binding.source_id,
            database=request.database,
        )
    )
    baseline, candidate = await _synchronize_twin_roots(
        request,
        work_directory,
        binding,
        selected_repository,
    )
    await _require_artifacts_unchanged(request)
    await require_source_unchanged(binding, database=request.database)
    admission = await admit_verified_twins(
        database=request.database,
        binding=binding,
        baseline=baseline.dataset,
        candidate=candidate.dataset,
    )
    return UHCDrugTwinResult(admission, baseline, candidate)


async def _verify_twins_under_lease(request: _TwinRequest) -> UHCDrugTwinResult:
    with tempfile.TemporaryDirectory(
        prefix="uhc-formulary-twin-",
        dir=request.work_directory,
    ) as temporary_name:
        return await _build_and_admit_twins(request, Path(temporary_name))


async def verify_uhc_drug_twins(
    *,
    artifacts: VerifiedSourceArtifactSet,
    baseline_run_id: str,
    candidate_run_id: str,
    cutoff: dt.datetime,
    work_directory: Path | str,
    database: Any = db,
    repository: Any | None = None,
) -> UHCDrugTwinResult:
    """Independently normalize, verify, and admit without publication."""

    request = _validated_twin_request(
        artifacts,
        baseline_run_id,
        candidate_run_id,
        cutoff,
        work_directory,
        database,
        repository,
    )
    async with manual_lock.manual_source_lease(
        database,
        UHC_FORMULARY_SOURCE_ID,
        wait_seconds=UHC_DRUG_LOCK_WAIT_SECONDS,
        retry_seconds=UHC_DRUG_LOCK_RETRY_SECONDS,
    ):
        async with asyncio.timeout(UHC_DRUG_TWIN_TIMEOUT_SECONDS):
            return await _verify_twins_under_lease(request)


async def _verify_and_record_under_lease(
    request: _TwinRequest,
    acquisition: UHCDrugArtifactAcquisitionResult,
) -> UHCDrugRecordedAdmission:
    """Build, admit, and durably record before releasing the source lease."""

    if (
        type(acquisition) is not UHCDrugArtifactAcquisitionResult
        or acquisition.artifacts != request.artifacts
    ):
        raise ValueError("UHC drug artifact acquisition result is invalid")

    from process.formulary_fhir.uhc_drug_receipt import (
        _record_receipt_under_lease,
    )
    from process.formulary_fhir.uhc_drug_receipt import UHCDrugRecordedAdmission

    with tempfile.TemporaryDirectory(
        prefix="uhc-formulary-twin-",
        dir=request.work_directory,
    ) as temporary_name:
        twin_result = await _build_and_admit_twins(
            request,
            Path(temporary_name),
        )
        receipt = await drain_operation(
            _record_receipt_under_lease(
                acquisition=acquisition,
                twin_result=twin_result,
                database=request.database,
            ),
            preserve_cancellation=True,
        )
        return UHCDrugRecordedAdmission(twin_result, receipt)


async def verify_and_record_uhc_drug_twins(
    *,
    acquisition: UHCDrugArtifactAcquisitionResult,
    baseline_run_id: str,
    candidate_run_id: str,
    cutoff: dt.datetime,
    work_directory: Path | str,
    database: Any = db,
    repository: Any | None = None,
) -> UHCDrugRecordedAdmission:
    """Independently build, admit, and record one restart-safe receipt."""

    if type(acquisition) is not UHCDrugArtifactAcquisitionResult:
        raise ValueError("UHC drug artifact acquisition result is invalid")
    request = _validated_twin_request(
        acquisition.artifacts,
        baseline_run_id,
        candidate_run_id,
        cutoff,
        work_directory,
        database,
        repository,
    )
    async with manual_lock.manual_source_lease(
        database,
        UHC_FORMULARY_SOURCE_ID,
        wait_seconds=UHC_DRUG_LOCK_WAIT_SECONDS,
        retry_seconds=UHC_DRUG_LOCK_RETRY_SECONDS,
    ):
        async with asyncio.timeout(UHC_DRUG_TWIN_TIMEOUT_SECONDS):
            return await _verify_and_record_under_lease(
                request,
                acquisition,
            )


__all__ = (
    "UHCDrugTwinResult",
    "verify_and_record_uhc_drug_twins",
    "verify_uhc_drug_twins",
)
