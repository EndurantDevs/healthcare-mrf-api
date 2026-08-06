# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Weekday alias synchronizer with copy-on-write and fail-closed publication."""

from __future__ import annotations

import asyncio
import datetime as dt
import re
from dataclasses import dataclass
from typing import Protocol

from process.formulary_fhir.client import FHIRTransportError
from process.formulary_fhir.parser import parse_medication_knowledge
from process.formulary_fhir.planner import AliasSyncDecision
from process.formulary_fhir.planner import AliasSyncObservation
from process.formulary_fhir.planner import decide_alias_sync
from process.formulary_fhir.planner import delta_window_start
from process.formulary_fhir.planner import is_rolling_reconciliation_due
from process.formulary_fhir.repository import AliasVersionWrite
from process.formulary_fhir.repository import CheckpointWrite
from process.formulary_fhir.repository import CurrentSnapshot
from process.formulary_fhir.repository import FHIRFormularyRepository
from process.formulary_fhir.repository import PriorAliasState
from process.formulary_fhir.repository import medication_variant_hash
from process.formulary_fhir.repository import membership_hash
from process.formulary_fhir.types import CoveragePlanRecord, MedicationRecord


class FormularyClient(Protocol):
    """Describe the bounded client surface consumed by synchronization."""

    base_url: str
    throttle_count: int

    def coverage_plans(self, *, cutoff: dt.datetime):
        """Yield CoveragePlan resources before one fixed cutoff."""

        raise NotImplementedError

    async def coverage_plan_count(self, *, cutoff: dt.datetime) -> int:
        """Return the exact CoveragePlan census before one fixed cutoff."""

        raise NotImplementedError

    async def alias_count(self, alias: str, *, cutoff: dt.datetime) -> int:
        """Return the exact MedicationKnowledge census for one alias."""

        raise NotImplementedError

    def medications(
        self,
        alias: str,
        *,
        cutoff: dt.datetime,
        updated_since: dt.datetime | None = None,
    ):
        """Yield full or delta MedicationKnowledge resources for one alias."""

        raise NotImplementedError


@dataclass(frozen=True)
class AliasWork:
    plan: CoveragePlanRecord
    source_plan_identifier: str
    alias_id: str


@dataclass(frozen=True)
class AliasResult:
    public_id: str
    source_plan_identifier: str
    acquisition_mode: str
    exact_count: int
    alias_version_id: str
    resumed: bool = False


@dataclass(frozen=True)
class _AliasSyncContext:
    client: FormularyClient
    repository: FHIRFormularyRepository
    dataset_id: str
    run_id: str
    cutoff: dt.datetime
    current: CurrentSnapshot
    rolling_ordinal: int


def business_day_ordinal(day: dt.date) -> int:
    """Return a monotonic weekday number; weekend values collapse to Friday."""

    zero_based = day.toordinal() - 1
    weeks, weekday = divmod(zero_based, 7)
    return weeks * 5 + min(weekday, 4)


def _is_california_plan(work: AliasWork) -> bool:
    plan_evidence = " ".join(
        filter(
            None,
            (
                work.plan.title,
                work.plan.name,
                work.source_plan_identifier,
            ),
        )
    ).upper()
    return bool(
        work.source_plan_identifier.upper().startswith("MI-")
        or re.search(r"(?:CALIFORNIA|\bNCAL\b|\bSCAL\b)", plan_evidence)
    )


async def _run_alias_wave(coroutines) -> list[AliasResult]:
    """Cancel and drain sibling aliases before a failed generation is marked."""

    tasks = [asyncio.create_task(coroutine) for coroutine in coroutines]
    try:
        return list(await asyncio.gather(*tasks))
    except BaseException:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise


def _is_resumable_interruption(exc: BaseException) -> bool:
    return isinstance(exc, (asyncio.CancelledError, TimeoutError)) or (
        isinstance(exc, FHIRTransportError) and exc.retryable
    )


async def _collect_medications(
    client: FormularyClient,
    alias: str,
    *,
    cutoff: dt.datetime,
    updated_since: dt.datetime | None = None,
) -> tuple[MedicationRecord, ...]:
    medications: list[MedicationRecord] = []
    seen_ids: set[str] = set()
    async for resource in client.medications(
        alias,
        cutoff=cutoff,
        updated_since=updated_since,
    ):
        medication = parse_medication_knowledge(resource)
        if medication.upstream_medication_id in seen_ids:
            raise RuntimeError(
                "FHIR formulary cursor returned a duplicate MedicationKnowledge id"
            )
        if (
            medication.source_plan_identifiers
            and alias not in medication.source_plan_identifiers
        ):
            raise RuntimeError(
                "FHIR formulary alias cursor crossed DrugPlan membership"
            )
        seen_ids.add(medication.upstream_medication_id)
        medications.append(medication)
    return tuple(medications)


def _prior_alias(
    context: _AliasSyncContext,
    work: AliasWork,
) -> PriorAliasState | None:
    alias_key = (work.plan.public_id, work.source_plan_identifier)
    return context.current.aliases.get(alias_key)


async def _resumed_alias_result(
    context: _AliasSyncContext,
    work: AliasWork,
) -> AliasResult | None:
    completed = await context.repository.completed_alias_checkpoint(
        dataset_id=context.dataset_id,
        run_id=context.run_id,
        alias_id=work.alias_id,
        source_plan_identifier=work.source_plan_identifier,
        cutoff_at=context.cutoff,
    )
    if completed is None:
        return None
    return AliasResult(
        work.plan.public_id,
        work.source_plan_identifier,
        completed.acquisition_mode,
        completed.expected_count,
        completed.alias_version_id,
        resumed=True,
    )


async def _delta_medications(
    context: _AliasSyncContext,
    work: AliasWork,
    prior: PriorAliasState | None,
) -> tuple[MedicationRecord, ...]:
    if prior is None or context.current.cutoff_at is None:
        return ()
    return await _collect_medications(
        context.client,
        work.source_plan_identifier,
        cutoff=context.cutoff,
        updated_since=delta_window_start(context.current.cutoff_at),
    )


def _alias_decision(
    context: _AliasSyncContext,
    work: AliasWork,
    prior: PriorAliasState | None,
    exact_count: int,
    delta_medications: tuple[MedicationRecord, ...],
) -> AliasSyncDecision:
    return decide_alias_sync(
        AliasSyncObservation(
            source_plan_identifier=work.source_plan_identifier,
            exact_count=exact_count,
            prior_count=prior.expected_count if prior else None,
            delta_ids=frozenset(
                medication.upstream_medication_id for medication in delta_medications
            ),
            prior_membership_ids=(prior.membership_ids if prior else frozenset()),
            rolling_reconciliation_due=is_rolling_reconciliation_due(
                work.source_plan_identifier,
                business_day_ordinal=context.rolling_ordinal,
            ),
        )
    )


def _needs_prior_membership(
    context: _AliasSyncContext,
    work: AliasWork,
    prior: PriorAliasState | None,
    exact_count: int,
    delta_medications: tuple[MedicationRecord, ...],
) -> bool:
    if prior is None or exact_count != prior.expected_count:
        return False
    if not delta_medications:
        return False
    return not is_rolling_reconciliation_due(
        work.source_plan_identifier,
        business_day_ordinal=context.rolling_ordinal,
    )


async def _require_stable_alias_count(
    context: _AliasSyncContext,
    work: AliasWork,
    expected_count: int,
) -> None:
    post_count = await context.client.alias_count(
        work.source_plan_identifier,
        cutoff=context.cutoff,
    )
    if post_count != expected_count:
        raise RuntimeError("FHIR formulary alias count drifted during acquisition")


def _completed_checkpoint(
    context: _AliasSyncContext,
    work: AliasWork,
    *,
    acquisition_mode: str,
    exact_count: int,
    membership_hash_value: str,
) -> CheckpointWrite:
    return CheckpointWrite(
        alias_id=work.alias_id,
        source_plan_identifier=work.source_plan_identifier,
        run_id=context.run_id,
        dataset_id=context.dataset_id,
        fence_token=1,
        cutoff_at=context.cutoff,
        acquisition_mode=acquisition_mode,
        expected_count=exact_count,
        processed_count=exact_count,
        membership_hash_value=membership_hash_value,
        is_completed=True,
    )


async def _reuse_alias(
    context: _AliasSyncContext,
    work: AliasWork,
    prior: PriorAliasState,
    exact_count: int,
) -> AliasResult:
    await _require_stable_alias_count(context, work, exact_count)
    await context.repository.link_reused_alias(
        dataset_id=context.dataset_id,
        prior=prior,
    )
    await context.repository.save_checkpoint(
        _completed_checkpoint(
            context,
            work,
            acquisition_mode="reuse",
            exact_count=exact_count,
            membership_hash_value=(
                prior.membership_hash_value
                or membership_hash(prior.variants_by_medication_id)
            ),
        )
    )
    return AliasResult(
        work.plan.public_id,
        work.source_plan_identifier,
        "reuse",
        exact_count,
        prior.alias_version_id,
    )


async def _selected_medications(
    context: _AliasSyncContext,
    work: AliasWork,
    decision: AliasSyncDecision,
    delta_medications: tuple[MedicationRecord, ...],
) -> tuple[str, tuple[MedicationRecord, ...]]:
    if decision == AliasSyncDecision.DELTA:
        return "delta", delta_medications
    medications = await _collect_medications(
        context.client,
        work.source_plan_identifier,
        cutoff=context.cutoff,
    )
    return "full", medications


def _completed_variants_by_id(
    prior: PriorAliasState | None,
    acquisition_mode: str,
    medications: tuple[MedicationRecord, ...],
) -> dict[str, str]:
    variants_by_id = (
        dict(prior.variants_by_medication_id)
        if acquisition_mode == "delta" and prior
        else {}
    )
    variants_by_id.update(
        {
            medication.upstream_medication_id: medication_variant_hash(medication)
            for medication in medications
        }
    )
    return variants_by_id


async def _write_changed_alias(
    context: _AliasSyncContext,
    work: AliasWork,
    prior: PriorAliasState | None,
    exact_count: int,
    acquisition_mode: str,
    medications: tuple[MedicationRecord, ...],
) -> AliasResult:
    await _require_stable_alias_count(context, work, exact_count)
    alias_version_id = await context.repository.put_alias_version(
        AliasVersionWrite(
            dataset_id=context.dataset_id,
            alias_id=work.alias_id,
            expected_count=exact_count,
            cutoff_at=context.cutoff,
            medications=medications,
            acquisition_mode=acquisition_mode,
            prior=prior,
            apply_california_rule=_is_california_plan(work),
        )
    )
    variants_by_id = _completed_variants_by_id(
        prior,
        acquisition_mode,
        medications,
    )
    await context.repository.save_checkpoint(
        _completed_checkpoint(
            context,
            work,
            acquisition_mode=acquisition_mode,
            exact_count=exact_count,
            membership_hash_value=membership_hash(variants_by_id),
        )
    )
    return AliasResult(
        work.plan.public_id,
        work.source_plan_identifier,
        acquisition_mode,
        exact_count,
        alias_version_id,
    )


async def _sync_alias(
    context: _AliasSyncContext,
    work: AliasWork,
) -> AliasResult:
    """Synchronize one alias while preserving sequential cursor traversal."""

    resumed_result = await _resumed_alias_result(context, work)
    if resumed_result is not None:
        return resumed_result
    prior = _prior_alias(context, work)
    exact_count = await context.client.alias_count(
        work.source_plan_identifier,
        cutoff=context.cutoff,
    )
    delta_medications = await _delta_medications(context, work, prior)
    if _needs_prior_membership(
        context,
        work,
        prior,
        exact_count,
        delta_medications,
    ):
        assert prior is not None
        prior = await context.repository.load_prior_alias_state(prior)
    decision = _alias_decision(
        context,
        work,
        prior,
        exact_count,
        delta_medications,
    )
    if decision == AliasSyncDecision.REUSE:
        assert prior is not None
        return await _reuse_alias(context, work, prior, exact_count)
    acquisition_mode, medications = await _selected_medications(
        context,
        work,
        decision,
        delta_medications,
    )
    return await _write_changed_alias(
        context,
        work,
        prior,
        exact_count,
        acquisition_mode,
        medications,
    )


async def synchronize(
    *,
    client: FormularyClient,
    repository: FHIRFormularyRepository,
    run_id: str,
    cutoff: dt.datetime,
    publish: bool = False,
    seed_eligible: bool = False,
    alias_concurrency: int = 4,
) -> dict[str, Any]:
    """Build, verify, and optionally publish one fixed-cutoff generation."""

    from process.formulary_fhir.synchronizer_run import synchronize_generation

    return await synchronize_generation(
        client=client,
        repository=repository,
        run_id=run_id,
        cutoff=cutoff,
        publish=publish,
        seed_eligible=seed_eligible,
        alias_concurrency=alias_concurrency,
    )
