# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Measure factorized pricing-projection work without publishing any rows."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from sqlalchemy import text

from api import plan_pricing_projection_v3 as projection
from api.plan_pricing_projection_contract import projection_id, provider_signature
from api.plan_pricing_projection_source import BindingProjection
from api.plan_pricing_projection_v3_code import (
    _PriceHydrationReadLimitError,
    _PriceMembershipMetadataReadLimitError,
)
from api.plan_pricing_projection_v3_types import _BuildState
from db.connection import db
from scripts.ptg_v4_dev_canary_io import utc_now_text, write_json
from scripts.research.plan_pricing_projection_v3_census_contract import (
    RESOURCE_PROOF_LIMITATIONS,
    census_parser as _census_parser,
    expected_target as _expected_target,
    fixed_cap_gates as _fixed_cap_gates,
    is_accepted as _is_accepted,
    observed_work_limits as _observed_work_limits,
    require_expected_target as _require_expected_target,
    seal_cardinality_census,
    seal_source_only,
)
from scripts.research.plan_pricing_projection_v3_census_support import (
    ReleaseInput,
    capture_source_identity,
    load_binding_projections,
    locked_release_input,
    memory_sample,
    postflight,
    projection_row_counts,
    runtime_identity,
)
from scripts.research.plan_pricing_projection_v3_census_transaction import (
    declared_occurrence_rows as _declared_occurrence_rows,
    price_membership_cache_counts as _price_membership_cache_counts,
    projection_stage_counts as _projection_stage_counts,
    rollback_only,
)

RECEIPT_CONTRACT = "healthporta.plan-pricing-v3-work-census.v1"
OPT_IN_ENV = "HLTHPRT_PLAN_PRICING_V3_CENSUS"
CHECKPOINT_INTERVAL = 64
MAX_CENSUS_RUNTIME_SECONDS = 12 * 60 * 60
DIAGNOSTIC_CODE_MEMBERSHIP_LIMIT = 8_000_000
DIAGNOSTIC_CODE_MEMBER_CELL_LIMIT = 8_000_000
MEASURED_WORK_FIELDS = (
    "normalized_occurrence_rows",
    "staged_price_atom_membership_rows",
    "maximum_price_key_atom_membership_rows",
    "membership_probe_rows",
    "member_cell_rows",
    "eligible_member_cell_rows",
    "set_cell_rows",
    "profile_join_rows",
    "aggregate_join_rows",
    "profile_rate_count_sum",
    "profile_rate_count_max",
    "profile_distinct_rate_count_max",
    "aggregate_rate_count_sum",
    "aggregate_rate_count_max",
)
_STAGED_PRICE_METRICS_SQL = """
    WITH price_set_atoms AS MATERIALIZED (
        SELECT binding_ordinal, price_set_id, SUM(rate_multiplicity)::bigint
                   AS atom_count
          FROM plan_pricing_price_rate_stage
         GROUP BY binding_ordinal, price_set_id
    )
    SELECT COALESCE(SUM(atom_count), 0)::bigint AS staged_price_atom_membership_rows,
           COALESCE(MAX(atom_count), 0)::bigint
               AS maximum_price_key_atom_membership_rows
      FROM price_set_atoms
"""
_ERROR_DIMENSION_BY_TYPE = {
    _PriceMembershipMetadataReadLimitError: "price_membership_metadata",
    _PriceHydrationReadLimitError: "price_hydration",
}


@dataclass(frozen=True)
class _CensusContext:
    release_input: ReleaseInput
    serving_shape: dict[str, Any]
    provider_generation_signature: str
    candidate_projection_id: str
    binding_projections: list[BindingProjection]
    state: _BuildState
    code_identities: list[tuple[str, str]]
    persistent_counts_before: dict[str, int | None]


def _empty_metrics() -> dict[str, dict[str, int]]:
    return {
        field_name: {"total": 0, "maximum_per_code": 0}
        for field_name in MEASURED_WORK_FIELDS
    }


def _record_work(
    metrics_by_field: dict[str, dict[str, int]],
    code_work: Any,
    eligible_member_cell_rows: int,
    staged_price_atom_membership_rows: int,
) -> None:
    values_by_field = {
        field_name: int(getattr(code_work, field_name))
        for field_name in MEASURED_WORK_FIELDS
        if field_name
        not in {
            "eligible_member_cell_rows",
            "normalized_occurrence_rows",
            "staged_price_atom_membership_rows",
            "maximum_price_key_atom_membership_rows",
        }
    }
    values_by_field["eligible_member_cell_rows"] = eligible_member_cell_rows
    values_by_field["staged_price_atom_membership_rows"] = (
        staged_price_atom_membership_rows
    )
    for field_name, value in values_by_field.items():
        _record_metric(metrics_by_field, field_name, value)


def _record_metric(
    metrics_by_field: dict[str, dict[str, int]],
    field_name: str,
    value: int,
) -> None:
    """Accumulate one exact per-code metric."""

    metric_by_field = metrics_by_field[field_name]
    metric_by_field["total"] += value
    metric_by_field["maximum_per_code"] = max(
        metric_by_field["maximum_per_code"], value
    )


async def _prepare_context(
    session: Any,
    args: argparse.Namespace,
) -> _CensusContext:
    release_input = await locked_release_input(session, args.plan_release_id)
    serving_shape = _require_expected_target(args, release_input)
    generation_signature = await provider_signature(session)
    candidate_id = projection_id(
        release_input.identity["binding_set_digest"], generation_signature
    )
    lock_result = await session.execute(
        text("SELECT pg_try_advisory_xact_lock(hashtextextended(:key, 0))"),
        {"key": candidate_id},
    )
    if lock_result.scalar_one() is not True:
        raise RuntimeError("pricing projection census lock is unavailable")
    if await locked_release_input(session, args.plan_release_id) != release_input:
        raise RuntimeError("pricing projection census release changed before work")
    persistent_counts = await projection_row_counts(session, candidate_id)
    binding_projections = await load_binding_projections(
        session, release_input.binding_manifest
    )
    state = _BuildState(hashlib.sha256())
    code_identities = sorted(
        {
            code_identity
            for binding_projection in binding_projections
            for code_identity in binding_projection.code_rows_by_identity
        }
    )
    return _CensusContext(
        release_input,
        serving_shape,
        generation_signature,
        candidate_id,
        binding_projections,
        state,
        code_identities,
        persistent_counts,
    )


async def _has_measured_code(
    session: Any,
    context: _CensusContext,
    code_identity: tuple[str, str],
    metrics_by_field: dict[str, dict[str, int]],
    set_stage: Any = None,
) -> bool:
    _record_metric(
        metrics_by_field,
        "normalized_occurrence_rows",
        _declared_occurrence_rows(context.binding_projections, code_identity),
    )
    if set_stage is not None:
        set_stage("measuring code inputs", "measuring bounded code inputs")
    if not await projection._has_staged_code_inputs(
        session, context.state, code_identity, context.binding_projections
    ):
        return False
    staged_input_result = await session.execute(text(_STAGED_PRICE_METRICS_SQL))
    staged_input_by_field = staged_input_result.mappings().one()
    _record_metric(
        metrics_by_field,
        "maximum_price_key_atom_membership_rows",
        int(staged_input_by_field["maximum_price_key_atom_membership_rows"]),
    )
    if set_stage is not None:
        set_stage("measuring provider cells", "measuring frozen provider cells")
    await projection._materialize_provider_cells(
        session, context.candidate_projection_id, context.state
    )
    if set_stage is not None:
        set_stage("measuring code work", "measuring bounded code work")
    code_work = await projection._stage_code_work(
        session,
        context.candidate_projection_id,
        code_identity,
        DIAGNOSTIC_CODE_MEMBERSHIP_LIMIT,
        DIAGNOSTIC_CODE_MEMBER_CELL_LIMIT,
    )
    eligible_result = await session.execute(
        text("SELECT COUNT(*) FROM plan_pricing_eligible_member_cell_stage")
    )
    _record_work(
        metrics_by_field,
        code_work,
        int(eligible_result.scalar_one()),
        int(staged_input_by_field["staged_price_atom_membership_rows"]),
    )
    return True


async def _measure_codes(
    session: Any,
    context: _CensusContext,
    checkpoint: Any,
    set_stage: Any = None,
) -> tuple[dict[str, dict[str, int]], int]:
    metrics_by_field = _empty_metrics()
    measured_code_count = 0
    checkpoint(
        {
            "code_identity_count": len(context.code_identities),
            "code_identities_processed": 0,
            "codes_with_rates_measured": 0,
            "work": metrics_by_field,
            "price_membership_metadata": _price_membership_cache_counts(context.state),
            "memory": memory_sample(),
        }
    )
    for code_ordinal, code_identity in enumerate(context.code_identities, start=1):
        measured_code_count += int(
            await _has_measured_code(
                session,
                context,
                code_identity,
                metrics_by_field,
                set_stage,
            )
        )
        if code_ordinal % CHECKPOINT_INTERVAL == 0:
            checkpoint(
                {
                    "code_identity_count": len(context.code_identities),
                    "code_identities_processed": code_ordinal,
                    "codes_with_rates_measured": measured_code_count,
                    "work": metrics_by_field,
                    "price_membership_metadata": (
                        _price_membership_cache_counts(context.state)
                    ),
                    "memory": memory_sample(),
                }
            )
        await asyncio.sleep(0)
    return metrics_by_field, measured_code_count


async def _measurement_result(
    session: Any,
    context: _CensusContext,
    metrics_by_field: dict[str, dict[str, int]],
    measured_code_count: int,
) -> dict[str, Any]:
    stage_counts_by_field = await _projection_stage_counts(session)
    stage_counts_by_field.update(_price_membership_cache_counts(context.state))
    expected_stage_counts = (
        context.state.staged_provider_set_count,
        context.state.provider_membership_count,
        context.state.provider_cell_count,
        context.state.provider_fragment_byte_count,
        0,
    )
    observed_stage_counts = tuple(
        stage_counts_by_field[field_name]
        for field_name in (
            "provider_set_count",
            "provider_membership_count",
            "provider_cell_count",
            "provider_fragment_byte_count",
            "pending_npi_count",
        )
    )
    if observed_stage_counts != expected_stage_counts:
        raise RuntimeError("pricing projection census staging is incomplete")
    if await provider_signature(session) != context.provider_generation_signature:
        raise RuntimeError("pricing projection census provider identity changed")
    if (
        await locked_release_input(
            session, context.release_input.identity["plan_release_id"]
        )
        != context.release_input
    ):
        raise RuntimeError("pricing projection census release changed during work")
    return {
        "release": context.release_input.identity,
        "serving_shape": context.serving_shape,
        "provider_signature": context.provider_generation_signature,
        "projection_id": context.candidate_projection_id,
        "code_identity_count": len(context.code_identities),
        "codes_with_rates_measured": measured_code_count,
        "work": metrics_by_field,
        "observed_work_limits": _observed_work_limits(metrics_by_field),
        "staged": stage_counts_by_field,
        "fixed_cap_gates": _fixed_cap_gates(metrics_by_field, stage_counts_by_field),
        "persistent_counts_before": context.persistent_counts_before,
        "memory": memory_sample(),
    }


async def _measure_release(
    session: Any,
    args: argparse.Namespace,
    checkpoint: Any,
    set_stage: Any = None,
) -> dict[str, Any]:
    if set_stage is not None:
        set_stage("preparing release context", "preparing bounded release context")
    checkpoint(
        {
            "stage": "preparing_release_context",
            "code_identities_processed": 0,
            "codes_with_rates_measured": 0,
        }
    )
    context = await _prepare_context(session, args)
    metrics_by_field, measured_code_count = await _measure_codes(
        session, context, checkpoint, set_stage
    )
    return await _measurement_result(
        session, context, metrics_by_field, measured_code_count
    )


def _source_identity(args: argparse.Namespace) -> dict[str, Any]:
    return capture_source_identity(
        Path(__file__),
        args.expected_source_sha,
        args.expected_source_manifest_sha256,
        args.expected_harness_manifest_sha256,
    )


async def _execute_census(
    args: argparse.Namespace,
    receipt_by_field: dict[str, Any],
) -> dict[str, Any]:
    def checkpoint(progress_by_field: Mapping[str, Any]) -> None:
        """Persist one atomic progress snapshot."""

        receipt_by_field["progress"] = dict(progress_by_field)
        write_json(args.receipt, receipt_by_field)

    def set_stage(phase: str, message: str) -> None:
        """Retain only a closed privacy-safe phase and message."""

        receipt_by_field["phase"] = phase
        receipt_by_field["message"] = message

    receipt_by_field["runtime"] = runtime_identity(args.expected_image_digest)
    set_stage("connecting database", "connecting to the census database")
    await db.connect()
    try:
        set_stage("measuring release", "measuring the factorized release")
        measured_result = await rollback_only(
            receipt_by_field,
            lambda session: _measure_release(session, args, checkpoint, set_stage),
        )
        receipt_by_field["measurement"] = measured_result
        set_stage("verifying rollback", "verifying census rollback")
        receipt_by_field["postflight"] = await postflight(
            args.plan_release_id, measured_result
        )
        return measured_result
    finally:
        await db.disconnect()


async def run_census(
    args: argparse.Namespace,
    receipt_by_field: dict[str, Any],
) -> int:
    """Bind source, run an opted-in rollback-only census, and seal its receipt."""
    started_monotonic = time.monotonic()
    receipt_by_field["expected_target"] = _expected_target(args)
    receipt_by_field["maximum_runtime_seconds"] = MAX_CENSUS_RUNTIME_SECONDS
    receipt_by_field["resource_proof_limitations"] = RESOURCE_PROOF_LIMITATIONS
    receipt_by_field["external_pod_image_id_attestation_required"] = True
    receipt_by_field["phase"] = "binding source"
    if args.source_only:
        source_before = _source_identity(args)
        receipt_by_field["source_before"] = source_before
        return seal_source_only(
            receipt_by_field,
            source_before,
            utc_now_text(),
            time.monotonic() - started_monotonic,
        )
    if os.getenv(OPT_IN_ENV) != "1":
        raise RuntimeError(f"set {OPT_IN_ENV}=1 to authorize the census")
    receipt_by_field["mode"] = "cardinality_census"
    remaining_seconds = MAX_CENSUS_RUNTIME_SECONDS - (
        time.monotonic() - started_monotonic
    )
    if remaining_seconds <= 0:
        raise TimeoutError("pricing projection census deadline elapsed")
    try:
        async with asyncio.timeout(remaining_seconds):
            source_before = _source_identity(args)
            receipt_by_field["source_before"] = source_before
            measured_result = await _execute_census(args, receipt_by_field)
            source_after = _source_identity(args)
            receipt_by_field["source_after"] = source_after
    finally:
        receipt_by_field["elapsed_seconds"] = time.monotonic() - started_monotonic
    is_accepted = (
        _is_accepted(receipt_by_field, measured_result, source_after == source_before)
        and receipt_by_field["elapsed_seconds"] <= MAX_CENSUS_RUNTIME_SECONDS
    )
    return seal_cardinality_census(
        receipt_by_field,
        is_accepted,
        utc_now_text(),
    )


def census_main() -> int:
    """Run the census and persist a sanitized receipt on every exit."""

    args = _census_parser(__doc__).parse_args()
    receipt_by_field: dict[str, Any] = {
        "contract": RECEIPT_CONTRACT,
        "status": "failed",
        "accepted": False,
        "mode": "source_only" if args.source_only else "cardinality_census",
        "cap_calibration_admissible": False,
        "resource_proof_admissible": False,
        "started_at": utc_now_text(),
    }
    exit_code = 1
    try:
        exit_code = asyncio.run(run_census(args, receipt_by_field))
    except BaseException as exc:
        try:
            receipt_by_field["source_after"] = _source_identity(args)
        except BaseException as source_exc:
            receipt_by_field["source_after_error"] = {"type": type(source_exc).__name__}
        error_by_field = {"type": type(exc).__name__}
        error_dimension = _ERROR_DIMENSION_BY_TYPE.get(type(exc))
        if error_dimension is not None:
            error_by_field["dimension"] = error_dimension
        receipt_by_field.update(
            status="failed",
            accepted=False,
            finished_at=utc_now_text(),
            error=error_by_field,
        )
    write_json(args.receipt.resolve(), receipt_by_field)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(census_main())
