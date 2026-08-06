# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Generation-level orchestration for the formulary alias synchronizer."""

from __future__ import annotations

import asyncio
import datetime as dt
from typing import Any

from process.formulary_fhir.parser import parse_coverage_plan
from process.formulary_fhir.planner import AdaptiveAliasConcurrency
from process.formulary_fhir.repository import FHIRFormularyRepository
from process.formulary_fhir.synchronizer import AliasResult
from process.formulary_fhir.synchronizer import AliasWork
from process.formulary_fhir.synchronizer import FormularyClient
from process.formulary_fhir.synchronizer import _AliasSyncContext
from process.formulary_fhir.synchronizer import _is_resumable_interruption
from process.formulary_fhir.synchronizer import _run_alias_wave
from process.formulary_fhir.synchronizer import _sync_alias
from process.formulary_fhir.synchronizer import business_day_ordinal
from process.formulary_fhir.types import CoveragePlanRecord


async def _coverage_plans(
    client: FormularyClient,
    cutoff: dt.datetime,
) -> tuple[tuple[CoveragePlanRecord, ...], int]:
    pre_list_count = await client.coverage_plan_count(cutoff=cutoff)
    plans: list[CoveragePlanRecord] = []
    public_ids: set[str] = set()
    canonical_identities: set[str] = set()
    async for resource in client.coverage_plans(cutoff=cutoff):
        plan = parse_coverage_plan(resource, canonical_base=client.base_url)
        is_duplicate = bool(
            plan.public_id in public_ids
            or plan.canonical_identity in canonical_identities
        )
        if is_duplicate:
            raise RuntimeError("FHIR formulary List enumeration contains duplicates")
        public_ids.add(plan.public_id)
        canonical_identities.add(plan.canonical_identity)
        plans.append(plan)
    if not plans:
        raise RuntimeError("FHIR formulary List enumeration returned no CoveragePlans")
    post_list_count = await client.coverage_plan_count(cutoff=cutoff)
    has_exact_census = bool(
        pre_list_count == len(plans) and post_list_count == pre_list_count
    )
    if not has_exact_census:
        raise RuntimeError(
            "FHIR formulary List census does not match unique enumeration"
        )
    return tuple(plans), pre_list_count


async def _alias_work_items(
    repository: FHIRFormularyRepository,
    dataset_id: str,
    plans: tuple[CoveragePlanRecord, ...],
) -> list[AliasWork]:
    work_items: list[AliasWork] = []
    for plan in sorted(plans, key=lambda candidate: candidate.public_id):
        aliases_by_identifier = await repository.put_coverage_plan(
            dataset_id=dataset_id,
            plan=plan,
        )
        for source_plan_identifier, alias_id in sorted(
            aliases_by_identifier.items()
        ):
            work_items.append(AliasWork(plan, source_plan_identifier, alias_id))
    return work_items


async def _synchronize_aliases(
    context: _AliasSyncContext,
    work_items: list[AliasWork],
    alias_concurrency: int,
) -> tuple[list[AliasResult], AdaptiveAliasConcurrency]:
    controller = AdaptiveAliasConcurrency(
        configured=alias_concurrency,
        current=alias_concurrency,
    )
    alias_results: list[AliasResult] = []
    pending_work_items = list(work_items)
    while pending_work_items:
        throttle_count_before = context.client.throttle_count
        wave = pending_work_items[: controller.current]
        pending_work_items = pending_work_items[controller.current :]
        wave_results = await _run_alias_wave(
            (_sync_alias(context, work) for work in wave)
        )
        alias_results.extend(wave_results)
        has_throttled = context.client.throttle_count > throttle_count_before
        if has_throttled:
            controller.record_throttling()
        else:
            controller.record_clean_window()
    return alias_results, controller


def _mode_counts(alias_results: list[AliasResult]) -> dict[str, int]:
    counts_by_mode = {mode: 0 for mode in ("reuse", "delta", "full")}
    for alias_result in alias_results:
        counts_by_mode[alias_result.acquisition_mode] += 1
    return counts_by_mode


def _success_payload(
    *,
    dataset_id: str,
    generation: int | None,
    cutoff: dt.datetime,
    plans: tuple[CoveragePlanRecord, ...],
    list_census_count: int,
    alias_results: list[AliasResult],
    controller: AdaptiveAliasConcurrency,
    proof_by_field: dict[str, Any],
) -> dict[str, Any]:
    return {
        "dataset_id": dataset_id,
        "published": generation is not None,
        "generation": generation,
        "cutoff": cutoff.isoformat(),
        "lists": len(plans),
        "list_census_count": list_census_count,
        "aliases": len(alias_results),
        "alias_modes": _mode_counts(alias_results),
        "resumed_aliases": sum(
            alias_result.resumed for alias_result in alias_results
        ),
        "final_alias_concurrency": controller.current,
        **proof_by_field,
    }


async def synchronize_generation(
    *,
    client: FormularyClient,
    repository: FHIRFormularyRepository,
    run_id: str,
    cutoff: dt.datetime,
    publish: bool = False,
    alias_concurrency: int = 4,
) -> dict[str, Any]:
    """Build, verify, and optionally publish one fixed-cutoff generation."""

    if cutoff.tzinfo is None:
        raise ValueError("FHIR formulary run cutoff must be timezone-aware")
    dataset_id = await repository.begin_dataset(
        run_id=run_id,
        cutoff_at=cutoff,
        publish_requested=publish,
    )
    try:
        plans, list_census_count = await _coverage_plans(client, cutoff)
        work_items = await _alias_work_items(repository, dataset_id, plans)
        current = await repository.current_snapshot()
        context = _AliasSyncContext(
            client=client,
            repository=repository,
            dataset_id=dataset_id,
            run_id=run_id,
            cutoff=cutoff,
            current=current,
            rolling_ordinal=business_day_ordinal(cutoff.date()),
        )
        alias_results, controller = await _synchronize_aliases(
            context,
            work_items,
            alias_concurrency,
        )
        proof_by_field = await repository.verify_dataset(dataset_id)
        generation = (
            await repository.publish_dataset(dataset_id) if publish else None
        )
        return _success_payload(
            dataset_id=dataset_id,
            generation=generation,
            cutoff=cutoff,
            plans=plans,
            list_census_count=list_census_count,
            alias_results=alias_results,
            controller=controller,
            proof_by_field=proof_by_field,
        )
    except BaseException as exc:
        lifecycle_update = (
            repository.interrupt_dataset(dataset_id, exc)
            if _is_resumable_interruption(exc)
            else repository.fail_dataset(dataset_id, exc)
        )
        await asyncio.shield(lifecycle_update)
        raise
