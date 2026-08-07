# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Serial verify-only orchestration for dormant FHIR formulary sources."""

from __future__ import annotations

import asyncio
import datetime as dt
from dataclasses import dataclass
from typing import Any, Callable

import asyncpg
from db.models import db
from process.formulary_fhir.client import FHIRFormularyClient
from process.formulary_fhir.continuation import FHIRTransportError
from process.formulary_fhir.planner import AliasCensusPlan
from process.formulary_fhir.planner import CoverageCensusPlan
from process.formulary_fhir.planner import CoverageWork
from process.formulary_fhir.planner import plan_alias_census
from process.formulary_fhir.planner import plan_coverage_census
from process.formulary_fhir.repository import AliasRef
from process.formulary_fhir.repository import AliasVersionResult
from process.formulary_fhir.repository import AliasVersionWrite
from process.formulary_fhir.repository import CompletedAliasCheckpoint
from process.formulary_fhir.repository import CurrentSnapshot
from process.formulary_fhir.repository import DatasetRef
from process.formulary_fhir.repository import DatasetVerification
from process.formulary_fhir.repository import FHIRFormularyRepository
from process.formulary_fhir.repository import PriorAliasState
from process.formulary_fhir.repository_shared import strict_text
from process.formulary_fhir.repository_shared import utc_timestamp
from process.formulary_fhir.source import EnabledSourceBinding
from process.formulary_fhir.source import load_enabled_source
from process.formulary_fhir.source import require_source_unchanged
from sqlalchemy import exc as sqlalchemy_error


ClientFactory = Callable[[Any], Any]


@dataclass(frozen=True, slots=True)
class SynchronizationResult:
    """Expose bounded verification evidence without source locations."""

    dataset_id: str
    acquisition_contract_hash: str
    list_count: int
    alias_count: int
    medication_membership_count: int
    coverage_hash: str
    membership_hash: str
    full_aliases: int
    reused_aliases: int
    resumed_aliases: int
    request_count: int
    transient_retry_count: int
    throttle_count: int


@dataclass(frozen=True, slots=True)
class _AliasWorkItem:
    work: CoverageWork
    alias: AliasRef


@dataclass(frozen=True, slots=True)
class _AliasOutcome:
    mode: str
    expected_count: int
    resumed: bool


def _require_predecessor(
    dataset: DatasetRef,
    current: CurrentSnapshot,
) -> None:
    current_dataset_id = current.dataset.dataset_id if current.dataset else None
    if current_dataset_id != dataset.previous_dataset_id:
        raise RuntimeError("FHIR formulary predecessor changed during synchronization")


def _alias_key(work: CoverageWork) -> tuple[str, str]:
    return work.plan.public_id, work.source_plan_identifier


async def _persist_coverage_plans(
    repository: FHIRFormularyRepository,
    dataset: DatasetRef,
    coverage_plan: CoverageCensusPlan,
) -> tuple[_AliasWorkItem, ...]:
    work_by_key = {_alias_key(work): work for work in coverage_plan.work_items}
    work_items: list[_AliasWorkItem] = []
    for plan in coverage_plan.plans:
        write_result = await repository.put_coverage_plan(
            dataset=dataset,
            plan=plan,
        )
        if write_result.dataset != dataset:
            raise RuntimeError("FHIR formulary coverage write changed the dataset")
        expected_aliases = set(plan.source_plan_identifiers)
        actual_aliases = {
            alias.source_plan_identifier for alias in write_result.aliases
        }
        if actual_aliases != expected_aliases:
            raise RuntimeError("FHIR formulary coverage aliases are inconsistent")
        for alias in write_result.aliases:
            work = work_by_key.get((plan.public_id, alias.source_plan_identifier))
            if work is None:
                raise RuntimeError("FHIR formulary coverage work is incomplete")
            work_items.append(_AliasWorkItem(work, alias))
    return tuple(
        sorted(
            work_items,
            key=lambda item: (
                item.work.plan.public_id,
                item.work.source_plan_identifier,
            ),
        )
    )


def _prior_alias(
    current: CurrentSnapshot,
    work_item: _AliasWorkItem,
) -> PriorAliasState | None:
    return current.aliases.get(_alias_key(work_item.work))


def _completed_outcome(
    checkpoint: CompletedAliasCheckpoint,
) -> _AliasOutcome:
    return _AliasOutcome(
        mode=checkpoint.acquisition_mode,
        expected_count=checkpoint.expected_count,
        resumed=True,
    )


def _require_alias_result(
    result: AliasVersionResult,
    dataset: DatasetRef,
    alias: AliasRef,
    alias_plan: AliasCensusPlan,
) -> None:
    expected_result = (
        dataset.source_id,
        dataset.dataset_id,
        alias.alias_id,
        alias_plan.expected_count,
        alias_plan.membership_hash,
        alias_plan.mode,
    )
    actual_result = (
        result.source_id,
        result.dataset_id,
        result.alias_id,
        result.membership_count,
        result.membership_hash,
        result.acquisition_mode,
    )
    if actual_result != expected_result:
        raise RuntimeError("FHIR formulary alias write result is inconsistent")


async def _write_alias_plan(
    repository: FHIRFormularyRepository,
    dataset: DatasetRef,
    work_item: _AliasWorkItem,
    alias_plan: AliasCensusPlan,
    prior: PriorAliasState | None,
    fence_token: int,
) -> _AliasOutcome:
    if alias_plan.mode == "reuse":
        if prior is None:
            raise RuntimeError("FHIR formulary alias reuse has no predecessor")
        result = await repository.link_reused_alias(
            dataset=dataset,
            alias=work_item.alias,
            prior=prior,
            fence_token=fence_token,
        )
    else:
        result = await repository.put_alias_version(
            AliasVersionWrite(
                dataset=dataset,
                alias=work_item.alias,
                expected_count=alias_plan.expected_count,
                medications=alias_plan.medications,
                fence_token=fence_token,
            )
        )
    _require_alias_result(result, dataset, work_item.alias, alias_plan)
    return _AliasOutcome(alias_plan.mode, alias_plan.expected_count, False)


async def _synchronize_alias(
    *,
    binding: EnabledSourceBinding,
    client: Any,
    repository: FHIRFormularyRepository,
    database: Any,
    dataset: DatasetRef,
    current: CurrentSnapshot,
    work_item: _AliasWorkItem,
) -> _AliasOutcome:
    checkpoint = await repository.completed_alias_checkpoint(
        dataset=dataset,
        alias=work_item.alias,
    )
    if checkpoint is not None:
        return _completed_outcome(checkpoint)
    completion_fence = await repository.next_alias_completion_fence(
        dataset=dataset,
        alias=work_item.alias,
    )
    await require_source_unchanged(binding, database=database)
    census = await client.medication_current_census(
        work_item.work.source_plan_identifier,
        cutoff=dataset.cutoff_at,
    )
    prior = _prior_alias(current, work_item)
    alias_plan = plan_alias_census(
        binding,
        work_item.work,
        census,
        dataset.cutoff_at,
        prior,
    )
    if (
        completion_fence.prior_acquisition_mode == "full"
        and alias_plan.mode == "reuse"
    ):
        alias_plan = AliasCensusPlan(
            medications=alias_plan.medications,
            expected_count=alias_plan.expected_count,
            membership_hash=alias_plan.membership_hash,
            mode="full",
        )
    return await _write_alias_plan(
        repository,
        dataset,
        work_item,
        alias_plan,
        prior,
        completion_fence.fence_token,
    )


def _metric(client: Any, name: str) -> int:
    metric_value = getattr(client, name, 0)
    if type(metric_value) is not int or metric_value < 0:
        raise RuntimeError("FHIR formulary client metrics are invalid")
    return metric_value


def _result(
    dataset: DatasetRef,
    verification: DatasetVerification,
    outcomes: tuple[_AliasOutcome, ...],
    client: Any,
) -> SynchronizationResult:
    return SynchronizationResult(
        dataset_id=dataset.dataset_id,
        acquisition_contract_hash=dataset.acquisition_contract_hash,
        list_count=verification.list_count,
        alias_count=verification.alias_count,
        medication_membership_count=verification.medication_membership_count,
        coverage_hash=verification.coverage_hash,
        membership_hash=verification.membership_hash,
        full_aliases=sum(outcome.mode == "full" for outcome in outcomes),
        reused_aliases=sum(outcome.mode == "reuse" for outcome in outcomes),
        resumed_aliases=sum(outcome.resumed for outcome in outcomes),
        request_count=_metric(client, "request_count"),
        transient_retry_count=_metric(client, "transient_retry_count"),
        throttle_count=_metric(client, "throttle_count"),
    )


def _is_resumable(error: BaseException) -> bool:
    if isinstance(error, (asyncio.CancelledError, TimeoutError)):
        return True
    if isinstance(error, FHIRTransportError):
        return error.is_transient is True
    transient_database_errors = (
        asyncpg.CannotConnectNowError,
        asyncpg.DeadlockDetectedError,
        asyncpg.PostgresConnectionError,
        asyncpg.SerializationError,
        asyncpg.TooManyConnectionsError,
    )
    if isinstance(error, transient_database_errors):
        return True
    if isinstance(
        error,
        (
            sqlalchemy_error.DisconnectionError,
            sqlalchemy_error.InterfaceError,
            sqlalchemy_error.OperationalError,
            sqlalchemy_error.TimeoutError,
        ),
    ):
        return True
    return isinstance(
        getattr(error, "orig", None),
        transient_database_errors,
    )


async def _shield_lifecycle(update: Any) -> None:
    lifecycle_task = asyncio.create_task(update)
    while not lifecycle_task.done():
        try:
            await asyncio.shield(lifecycle_task)
        except asyncio.CancelledError:
            continue
        except BaseException:
            break
    if lifecycle_task.done():
        try:
            lifecycle_task.result()
        except BaseException:
            return


async def _record_failure(
    repository: FHIRFormularyRepository,
    dataset: DatasetRef,
    error: BaseException,
) -> None:
    update = (
        repository.interrupt_dataset(dataset, error)
        if _is_resumable(error)
        else repository.fail_dataset(dataset, error)
    )
    await _shield_lifecycle(update)


async def _run_verified_sync(
    *,
    binding: EnabledSourceBinding,
    client: Any,
    repository: FHIRFormularyRepository,
    database: Any,
    run_id: str,
    cutoff_at: dt.datetime,
) -> SynchronizationResult:
    coverage_census = await client.coverage_plan_current_census(cutoff=cutoff_at)
    coverage_plan = plan_coverage_census(binding, coverage_census, cutoff_at)
    await require_source_unchanged(binding, database=database)
    dataset = await repository.begin_dataset(
        run_id=run_id,
        cutoff_at=cutoff_at,
        acquisition_contract_hash=coverage_plan.acquisition_contract_hash,
        intent="none",
    )
    try:
        if dataset.status == "verified":
            await require_source_unchanged(binding, database=database)
            verification = await repository.verify_dataset(dataset=dataset)
            resumed_outcomes = tuple(
                _AliasOutcome("verified", 0, True)
                for _index in range(verification.alias_count)
            )
            return _result(dataset, verification, resumed_outcomes, client)
        current = await repository.current_snapshot()
        _require_predecessor(dataset, current)
        work_items = await _persist_coverage_plans(
            repository,
            dataset,
            coverage_plan,
        )
        outcomes = tuple(
            [
                await _synchronize_alias(
                    binding=binding,
                    client=client,
                    repository=repository,
                    database=database,
                    dataset=dataset,
                    current=current,
                    work_item=work_item,
                )
                for work_item in work_items
            ]
        )
        await require_source_unchanged(binding, database=database)
        verification = await repository.verify_dataset(dataset=dataset)
        return _result(dataset, verification, outcomes, client)
    except BaseException as error:
        await _record_failure(repository, dataset, error)
        raise


async def synchronize_verified_dataset(
    *,
    source_id: str,
    run_id: str,
    cutoff: dt.datetime,
    database: Any = db,
    client_factory: ClientFactory = FHIRFormularyClient,
) -> SynchronizationResult:
    """Build and verify one generation under an external single-owner fence.

    This dormant core performs no publication and has no worker, CLI, route,
    or schedule registration. A later adapter must hold a source-scoped lock.
    """

    normalized_source_id = strict_text(source_id, "source id", 64)
    normalized_run_id = strict_text(run_id, "run id", 64)
    cutoff_at = utc_timestamp(cutoff, "synchronization cutoff")
    binding = await load_enabled_source(normalized_source_id, database=database)
    repository = FHIRFormularyRepository(
        source_id=normalized_source_id,
        database=database,
    )
    async with client_factory(binding.config) as client:
        return await _run_verified_sync(
            binding=binding,
            client=client,
            repository=repository,
            database=database,
            run_id=normalized_run_id,
            cutoff_at=cutoff_at,
        )


__all__ = ("SynchronizationResult", "synchronize_verified_dataset")
