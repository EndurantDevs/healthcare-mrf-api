# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Resumable verify-only repository synchronization for UHC drug artifacts."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from db.models import db
from process.formulary_fhir.async_safety import cancellable_to_thread
from process.formulary_fhir.repository import CurrentSnapshot
from process.formulary_fhir.repository import DatasetRef
from process.formulary_fhir.repository import FHIRFormularyRepository
from process.formulary_fhir.repository_admission_proof import (
    require_full_checkpoints,
)
from process.formulary_fhir.repository_shared import PublicationIntent
from process.formulary_fhir.repository_shared import utc_timestamp
from process.formulary_fhir.source import EnabledSourceBinding
from process.formulary_fhir.source import require_source_unchanged
from process.formulary_fhir.source_artifact_contract import (
    VerifiedSourceArtifactSet,
)
from process.formulary_fhir.sync_lifecycle import record_synchronization_failure
from process.formulary_fhir.uhc_drug_parser import load_spooled_uhc_plan
from process.formulary_fhir.uhc_drug_parser import spooled_uhc_plan_keys
from process.formulary_fhir.uhc_drug_parser import verify_spooled_uhc_evidence
from process.formulary_fhir.uhc_drug_spool_reader import pin_uhc_drug_spool
from process.formulary_fhir.uhc_drug_spool_reader import PinnedUHCDrugSpool
from process.formulary_fhir.uhc_drug_spool_reader import (
    verify_and_bind_uhc_drug_spool,
)
from process.formulary_fhir.uhc_drug_spool_reader import _VerifiedUHCDrugSpool
from process.formulary_fhir.uhc_drug_parser_contract import UHCDrugPlanKey
from process.formulary_fhir.uhc_drug_parser_contract import (
    UHCDrugPlanMaterialization,
)
from process.formulary_fhir.uhc_drug_parser_contract import UHCDrugSpoolEvidence
from process.formulary_fhir.uhc_drug_sync_contract import (
    UHCDrugSynchronizationResult,
)
from process.formulary_fhir.uhc_drug_repository_writer import (
    require_exact_uhc_dataset,
)
from process.formulary_fhir.uhc_drug_repository_writer import (
    write_or_resume_uhc_plan,
)
from process.formulary_fhir.uhc_drug_sync_contract import (
    require_exact_predecessor,
)
from process.formulary_fhir.uhc_drug_sync_contract import (
    require_exact_verification,
)
from process.formulary_fhir.uhc_drug_sync_contract import (
    uhc_drug_sync_contract_hash,
)
from process.formulary_fhir.uhc_drug_sync_contract import (
    validate_uhc_drug_run_id,
)


@dataclass(frozen=True, slots=True)
class _SyncRequest:
    binding: EnabledSourceBinding
    artifacts: VerifiedSourceArtifactSet
    spool_path: Path | str
    evidence: UHCDrugSpoolEvidence
    run_id: str
    cutoff_at: dt.datetime
    intent: PublicationIntent
    database: Any
    repository: Any | None


@dataclass(frozen=True, slots=True)
class _SyncContext:
    binding: EnabledSourceBinding
    artifacts: VerifiedSourceArtifactSet
    spool_path: Path | str
    evidence: UHCDrugSpoolEvidence
    run_id: str
    cutoff_at: dt.datetime
    intent: PublicationIntent
    contract_hash: str
    database: Any
    repository: Any


@dataclass(slots=True)
class _WriteCensus:
    plan_count: int = 0
    medication_count: int = 0
    resumed_alias_count: int = 0

    def observe(self, medication_count: int, *, resumed: bool) -> None:
        """Accumulate one complete full-alias outcome."""

        self.plan_count += 1
        self.medication_count += medication_count
        self.resumed_alias_count += int(resumed)


async def _drained_to_thread(operation: Any, *args: Any, **kwargs: Any) -> Any:
    return await cancellable_to_thread(operation, *args, **kwargs)


async def _spool_keys(
    spool_path: _VerifiedUHCDrugSpool,
    evidence: UHCDrugSpoolEvidence,
) -> tuple[UHCDrugPlanKey, ...]:
    if type(spool_path) is not _VerifiedUHCDrugSpool:
        raise ValueError("UHC drug spool snapshot is invalid")
    plan_keys = await _drained_to_thread(spooled_uhc_plan_keys, spool_path)
    if len(plan_keys) != evidence.plan_count:
        raise RuntimeError("UHC drug spool plan census is inconsistent")
    return plan_keys


async def _reverified_spool_keys(
    spool_path: _VerifiedUHCDrugSpool,
    evidence: UHCDrugSpoolEvidence,
    artifacts: VerifiedSourceArtifactSet,
) -> tuple[UHCDrugPlanKey, ...]:
    await _drained_to_thread(
        verify_spooled_uhc_evidence,
        spool_path,
        evidence,
        artifacts,
    )
    return await _spool_keys(spool_path, evidence)


async def _verified_spool_snapshot(
    spool_path: PinnedUHCDrugSpool,
    evidence: UHCDrugSpoolEvidence,
    artifacts: VerifiedSourceArtifactSet,
) -> _VerifiedUHCDrugSpool:
    return await _drained_to_thread(
        verify_and_bind_uhc_drug_spool,
        spool_path,
        evidence,
        artifacts,
    )


async def _materialized_plan(
    spool_path: _VerifiedUHCDrugSpool,
    plan_key: UHCDrugPlanKey,
    binding: EnabledSourceBinding,
    evidence: UHCDrugSpoolEvidence,
) -> UHCDrugPlanMaterialization:
    return await _drained_to_thread(
        load_spooled_uhc_plan,
        spool_path,
        plan_key,
        source_id=binding.source_id,
        canonical_base=binding.config.canonical_base,
        evidence=evidence,
    )


async def _begin_exact_dataset(
    context: _SyncContext,
) -> DatasetRef:
    dataset = await context.repository.begin_dataset(
        run_id=context.run_id,
        cutoff_at=context.cutoff_at,
        acquisition_contract_hash=context.contract_hash,
        intent=context.intent,
    )
    require_exact_uhc_dataset(
        dataset,
        binding=context.binding,
        run_id=context.run_id,
        cutoff_at=context.cutoff_at,
        contract_hash=context.contract_hash,
        intent=context.intent,
    )
    return dataset


async def _reload_verified_dataset(
    context: _SyncContext,
) -> DatasetRef:
    verified_dataset = await _begin_exact_dataset(context)
    if verified_dataset.status != "verified":
        raise RuntimeError("UHC drug repository verification did not persist")
    return verified_dataset


async def _finish_verified_dataset(
    context: _SyncContext,
    dataset: DatasetRef,
    spool_snapshot: _VerifiedUHCDrugSpool,
    expected_plan_keys: tuple[UHCDrugPlanKey, ...],
    *,
    resumed_alias_count: int,
) -> UHCDrugSynchronizationResult:
    await require_full_checkpoints(
        context.database,
        dataset,
        context.evidence.plan_count,
    )
    verification = await context.repository.verify_dataset(dataset=dataset)
    require_exact_verification(dataset, context.evidence, verification)
    final_plan_keys = await _reverified_spool_keys(
        spool_snapshot,
        context.evidence,
        context.artifacts,
    )
    if final_plan_keys != expected_plan_keys:
        raise RuntimeError("UHC drug spool changed during synchronization")
    await require_source_unchanged(
        context.binding,
        database=context.database,
    )
    verified_dataset = await _reload_verified_dataset(context)
    return UHCDrugSynchronizationResult(
        dataset=verified_dataset,
        verification=verification,
        evidence=context.evidence,
        full_alias_count=context.evidence.plan_count,
        resumed_alias_count=resumed_alias_count,
    )


async def _finish_replayed_dataset(
    context: _SyncContext,
    dataset: DatasetRef,
    spool_snapshot: _VerifiedUHCDrugSpool,
    plan_keys: tuple[UHCDrugPlanKey, ...],
) -> UHCDrugSynchronizationResult:
    await require_source_unchanged(
        context.binding,
        database=context.database,
    )
    return await _finish_verified_dataset(
        context,
        dataset,
        spool_snapshot,
        plan_keys,
        resumed_alias_count=context.evidence.plan_count,
    )


async def _build_and_finish_dataset(
    context: _SyncContext,
    dataset: DatasetRef,
    plan_keys: tuple[UHCDrugPlanKey, ...],
    spool_snapshot: _VerifiedUHCDrugSpool,
) -> UHCDrugSynchronizationResult:
    current_snapshot = await context.repository.current_snapshot()
    if type(current_snapshot) is not CurrentSnapshot:
        raise RuntimeError("UHC drug current snapshot is invalid")
    require_exact_predecessor(dataset, current_snapshot.dataset)
    write_census = _WriteCensus()
    for plan_key in plan_keys:
        materialized_plan = await _materialized_plan(
            spool_snapshot,
            plan_key,
            context.binding,
            context.evidence,
        )
        write_outcome = await write_or_resume_uhc_plan(
            context.repository,
            dataset,
            materialized_plan,
        )
        write_census.observe(
            len(materialized_plan.medications),
            resumed=write_outcome == "resumed",
        )
    if (
        write_census.plan_count != context.evidence.plan_count
        or write_census.medication_count
        != context.evidence.medication_membership_count
    ):
        raise RuntimeError("UHC drug repository write census is inconsistent")
    await require_source_unchanged(
        context.binding,
        database=context.database,
    )
    return await _finish_verified_dataset(
        context,
        dataset,
        spool_snapshot,
        plan_keys,
        resumed_alias_count=write_census.resumed_alias_count,
    )


async def _run_uhc_drug_sync(
    context: _SyncContext,
) -> UHCDrugSynchronizationResult:
    """Build or replay one exact verify-only UHC formulary dataset."""

    with pin_uhc_drug_spool(context.spool_path) as pinned_spool:
        spool_snapshot = await _verified_spool_snapshot(
            pinned_spool,
            context.evidence,
            context.artifacts,
        )
        plan_keys = await _spool_keys(
            spool_snapshot,
            context.evidence,
        )
        await require_source_unchanged(
            context.binding,
            database=context.database,
        )
        dataset = await _begin_exact_dataset(context)
        try:
            if dataset.status == "verified":
                return await _finish_replayed_dataset(
                    context,
                    dataset,
                    spool_snapshot,
                    plan_keys,
                )
            return await _build_and_finish_dataset(
                context,
                dataset,
                plan_keys,
                spool_snapshot,
            )
        except BaseException as error:
            await record_synchronization_failure(
                context.repository,
                dataset,
                error,
            )
            raise


async def _synchronize_request(
    request: _SyncRequest,
) -> UHCDrugSynchronizationResult:
    selected_repository = (
        request.repository
        if request.repository is not None
        else FHIRFormularyRepository(
            source_id=request.binding.source_id,
            database=request.database,
        )
    )
    normalized_run_id = validate_uhc_drug_run_id(request.run_id)
    normalized_cutoff = utc_timestamp(request.cutoff_at, "UHC drug cutoff")
    contract_hash = uhc_drug_sync_contract_hash(
        request.binding,
        request.artifacts,
        request.evidence,
        normalized_cutoff,
    )
    context = _SyncContext(
        binding=request.binding,
        artifacts=request.artifacts,
        spool_path=request.spool_path,
        evidence=request.evidence,
        run_id=normalized_run_id,
        cutoff_at=normalized_cutoff,
        intent=request.intent,
        contract_hash=contract_hash,
        database=request.database,
        repository=selected_repository,
    )
    return await _run_uhc_drug_sync(context)


async def _synchronize_requested_uhc_drug_dataset(
    *,
    binding: EnabledSourceBinding,
    artifacts: VerifiedSourceArtifactSet,
    spool_path: Path | str,
    evidence: UHCDrugSpoolEvidence,
    run_id: str,
    cutoff_at: dt.datetime,
    database: Any = db,
    repository: Any | None = None,
) -> UHCDrugSynchronizationResult:
    """Build the requested twin candidate without admitting or publishing it."""

    return await _synchronize_request(
        _SyncRequest(
            binding=binding,
            artifacts=artifacts,
            spool_path=spool_path,
            evidence=evidence,
            run_id=run_id,
            cutoff_at=cutoff_at,
            intent="requested",
            database=database,
            repository=repository,
        )
    )


async def synchronize_uhc_drug_dataset(
    *,
    binding: EnabledSourceBinding,
    artifacts: VerifiedSourceArtifactSet,
    spool_path: Path | str,
    evidence: UHCDrugSpoolEvidence,
    run_id: str,
    cutoff_at: dt.datetime,
    database: Any = db,
    repository: Any | None = None,
) -> UHCDrugSynchronizationResult:
    """Build and verify one non-publishable full UHC repository candidate."""

    return await _synchronize_request(
        _SyncRequest(
            binding=binding,
            artifacts=artifacts,
            spool_path=spool_path,
            evidence=evidence,
            run_id=run_id,
            cutoff_at=cutoff_at,
            intent="none",
            database=database,
            repository=repository,
        )
    )


__all__ = (
    "UHCDrugSynchronizationResult",
    "synchronize_uhc_drug_dataset",
)
