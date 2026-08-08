# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fixed library-only twin verification for the reviewed formulary source."""

from __future__ import annotations

import asyncio
import datetime as dt
from dataclasses import dataclass
from typing import Any

from db.models import db
from process.formulary_fhir.client import FHIRFormularyClient
import process.formulary_fhir.manual_lock as manual_lock
from process.formulary_fhir.repository import DatasetRef
from process.formulary_fhir.repository import FHIRFormularyRepository
from process.formulary_fhir.repository_admission import admit_verified_twins
from process.formulary_fhir.repository_admission import TwinAdmissionResult
from process.formulary_fhir.repository_shared import row_mapping
from process.formulary_fhir.repository_shared import table_name
from process.formulary_fhir.reviewed_source import CANDIDATE_TIMEOUT_SECONDS
from process.formulary_fhir.reviewed_source import LOCK_RETRY_SECONDS
from process.formulary_fhir.reviewed_source import LOCK_WAIT_SECONDS
from process.formulary_fhir.reviewed_source import _candidate_request
from process.formulary_fhir.reviewed_source import _current_pointer
from process.formulary_fhir.reviewed_source import _is_exact_source
from process.formulary_fhir.reviewed_source import _matching_source_rows
from process.formulary_fhir.reviewed_source import _register_manifest
from process.formulary_fhir.reviewed_source import ReviewedSourceError
from process.formulary_fhir.reviewed_source import ReviewedSourceManifest
from process.formulary_fhir.reviewed_source import reviewed_source_manifest
from process.formulary_fhir.source import EnabledSourceBinding
from process.formulary_fhir.source import require_source_unchanged
from process.formulary_fhir.synchronizer import ClientFactory
from process.formulary_fhir.synchronizer import SynchronizationResult
from process.formulary_fhir.synchronizer import _run_verified_sync


@dataclass(frozen=True, slots=True)
class _TwinContext:
    database: Any
    binding: EnabledSourceBinding
    repository: FHIRFormularyRepository
    client_factory: ClientFactory
    manifest: ReviewedSourceManifest
    previous_pointer: str | None


def _twin_request(
    baseline_run_id: object,
    candidate_run_id: object,
    cutoff: object,
) -> tuple[str, str, dt.datetime]:
    baseline_id, baseline_cutoff = _candidate_request(
        baseline_run_id,
        cutoff,
    )
    candidate_id, candidate_cutoff = _candidate_request(
        candidate_run_id,
        cutoff,
    )
    if baseline_id == candidate_id:
        raise ReviewedSourceError("invalid_request")
    return baseline_id, candidate_id, baseline_cutoff


async def _synchronize_candidate(
    database: Any,
    binding: EnabledSourceBinding,
    repository: FHIRFormularyRepository,
    client_factory: ClientFactory,
    run_id: str,
    cutoff_at: dt.datetime,
    *,
    publish_requested: bool,
) -> SynchronizationResult:
    await require_source_unchanged(binding, database=database)
    intent = "requested" if publish_requested else "none"
    async with client_factory(binding.config) as client:
        return await _run_verified_sync(
            binding=binding,
            client=client,
            repository=repository,
            database=database,
            run_id=run_id,
            cutoff_at=cutoff_at,
            intent=intent,
            force_full=True,
        )


async def _require_candidate_state(
    database: Any,
    manifest: ReviewedSourceManifest,
    synchronization: SynchronizationResult,
    previous_pointer: str | None,
    *,
    publish_requested: bool,
) -> None:
    source_table = table_name("fhir_formulary_source")
    async with database.transaction():
        await database.status(
            f"LOCK TABLE {source_table} IN SHARE ROW EXCLUSIVE MODE;"
        )
        source_rows = await _matching_source_rows(database, manifest)
        if len(source_rows) != 1 or not _is_exact_source(
            source_rows[0],
            manifest,
        ):
            raise ReviewedSourceError("catalog")
        dataset_by_field = row_mapping(
            await database.first(
                f"SELECT status, publish_requested, seed_eligible FROM "
                f"{table_name('fhir_formulary_dataset')} WHERE "
                "source_id = :source_id AND dataset_id = :dataset_id;",
                source_id=manifest.source_id,
                dataset_id=synchronization.dataset_id,
            )
        )
        expected_dataset_by_field = {
            "status": "verified",
            "publish_requested": publish_requested,
            "seed_eligible": False,
        }
        current_pointer = await _current_pointer(database, manifest.source_id)
        if (
            dataset_by_field != expected_dataset_by_field
            or current_pointer != previous_pointer
        ):
            raise ReviewedSourceError("source")
        return None


async def _verified_dataset_ref(
    repository: FHIRFormularyRepository,
    synchronization: SynchronizationResult,
    run_id: str,
    cutoff_at: dt.datetime,
    *,
    publish_requested: bool,
) -> DatasetRef:
    intent = "requested" if publish_requested else "none"
    dataset = await repository.begin_dataset(
        run_id=run_id,
        cutoff_at=cutoff_at,
        acquisition_contract_hash=synchronization.acquisition_contract_hash,
        intent=intent,
    )
    if (
        dataset.dataset_id != synchronization.dataset_id
        or dataset.status != "verified"
    ):
        raise ReviewedSourceError("source")
    return dataset


async def _verified_candidate(
    context: _TwinContext,
    run_id: str,
    cutoff_at: dt.datetime,
    *,
    publish_requested: bool,
) -> tuple[SynchronizationResult, DatasetRef]:
    synchronization = await _synchronize_candidate(
        context.database,
        context.binding,
        context.repository,
        context.client_factory,
        run_id,
        cutoff_at,
        publish_requested=publish_requested,
    )
    await _require_candidate_state(
        context.database,
        context.manifest,
        synchronization,
        context.previous_pointer,
        publish_requested=publish_requested,
    )
    dataset = await _verified_dataset_ref(
        context.repository,
        synchronization,
        run_id,
        cutoff_at,
        publish_requested=publish_requested,
    )
    return synchronization, dataset


async def _verify_twins(
    database: Any,
    client_factory: ClientFactory,
    baseline_run_id: str,
    candidate_run_id: str,
    cutoff_at: dt.datetime,
) -> TwinAdmissionResult:
    manifest = reviewed_source_manifest()
    binding = await _register_manifest(database, manifest)
    context = _TwinContext(
        database=database,
        binding=binding,
        repository=FHIRFormularyRepository(
            source_id=manifest.source_id,
            database=database,
        ),
        client_factory=client_factory,
        manifest=manifest,
        previous_pointer=await _current_pointer(database, manifest.source_id),
    )
    _baseline_result, baseline = await _verified_candidate(
        context,
        baseline_run_id,
        cutoff_at,
        publish_requested=False,
    )
    candidate_result, candidate = await _verified_candidate(
        context,
        candidate_run_id,
        cutoff_at,
        publish_requested=True,
    )
    admission = await admit_verified_twins(
        database=database,
        binding=binding,
        baseline=baseline,
        candidate=candidate,
    )
    await _require_candidate_state(
        database,
        manifest,
        candidate_result,
        context.previous_pointer,
        publish_requested=True,
    )
    return admission


async def verify_reviewed_source_twins(
    *,
    baseline_run_id: str,
    candidate_run_id: str,
    cutoff: dt.datetime,
    database: Any = db,
    client_factory: ClientFactory = FHIRFormularyClient,
) -> TwinAdmissionResult:
    """Verify and admit two fixed-source acquisitions without publication."""

    normalized_baseline, normalized_candidate, cutoff_at = _twin_request(
        baseline_run_id,
        candidate_run_id,
        cutoff,
    )
    manifest = reviewed_source_manifest()
    try:
        async with manual_lock.manual_source_lease(
            database,
            manifest.source_id,
            wait_seconds=LOCK_WAIT_SECONDS,
            retry_seconds=LOCK_RETRY_SECONDS,
        ):
            async with asyncio.timeout(CANDIDATE_TIMEOUT_SECONDS):
                return await _verify_twins(
                    database,
                    client_factory,
                    normalized_baseline,
                    normalized_candidate,
                    cutoff_at,
                )
    except manual_lock.ManualSourceLockError as error:
        raise ReviewedSourceError(error.code) from None


__all__ = ("verify_reviewed_source_twins",)
