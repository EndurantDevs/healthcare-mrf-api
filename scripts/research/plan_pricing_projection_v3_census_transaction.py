# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Rollback-only PostgreSQL transaction fence for the projection-v3 census."""

from __future__ import annotations

import asyncio
import hashlib
import re
import sys
from typing import Any, Awaitable, Callable, Coroutine, Iterable, Mapping, TypeVar

from sqlalchemy import text

from api import plan_pricing_projection_v3 as projection
from api.plan_pricing_projection_contract import lock_provider_generation
from api.plan_pricing_projection_v3_types import _BuildState
from api.ptg2_db_sidecars import (
    PRICE_MEMBERSHIP_ALIAS_INDEX_RETAINED_BYTES_PER_BLOCK,
    PRICE_MEMBERSHIP_TRANSIENT_BYTES_PER_FRAGMENT,
)
from db.connection import db

CENSUS_DATABASE_APPLICATION_PREFIX = "hp-pv3-census"
CENSUS_DATABASE_RUN_TOKEN_PATTERN = re.compile(r"[0-9a-f]{12}")
CENSUS_DATABASE_STAGE_KEYS = frozenset(
    {
        "preparing_release_context",
        "reset_code_inputs",
        "code_layout",
        "price_membership_metadata",
        "price_hydration",
        "provider_set_staging",
        "code_occurrence_staging",
        "staged_price_metrics",
        "provider_cells",
        "reset_code_work",
        "membership_probe",
        "member_cell_staging",
        "member_cell_count",
        "taxonomy_filter",
        "rate_profile_cardinality",
        "set_cell_staging",
        "rate_frequency_staging",
        "work_metrics",
        "eligible_member_cells",
        "final_measurement",
        "measurement_complete",
    }
)
_CENSUS_DATABASE_SETTINGS = {
    "transaction_read_only": "off",
    "jit": "off",
    "max_parallel_workers_per_gather": "0",
    "temp_buffers": "8MB",
    "work_mem": "4MB",
    "hash_mem_multiplier": "1",
    "plan_cache_mode": "force_custom_plan",
    "temp_file_limit": "256MB",
    "lock_timeout": "5s",
    "statement_timeout": "20min",
}
TEMP_RELATIONS = (
    "plan_pricing_provider_set_stage",
    "plan_pricing_provider_member_stage",
    "plan_pricing_provider_npi_materialized_stage",
    "plan_pricing_provider_npi_pending_stage",
    "plan_pricing_code_occurrence_stage",
    "plan_pricing_price_rate_stage",
    "plan_pricing_rate_frequency_stage",
    "plan_pricing_provider_cell_stage",
    "plan_pricing_eligible_member_cell_stage",
    "plan_pricing_set_cell_stage",
)
STAGED_PRICE_METRICS_SQL = """
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
_Result = TypeVar("_Result")


def census_database_run_token(runtime_by_field: Mapping[str, Any]) -> str:
    """Return a short non-secret identity for one exact Job/Pod/image tuple."""

    identity = "\0".join(
        str(runtime_by_field.get(field_name) or "").strip()
        for field_name in ("job_name", "pod_uid", "image_digest")
    )
    if identity.startswith("\0") or "\0\0" in identity or identity.endswith("\0"):
        raise ValueError("pricing projection census database identity is incomplete")
    return hashlib.sha256(identity.encode()).hexdigest()[:12]


def census_database_application_name(
    run_token: str,
    stage: str,
    code_ordinal: int | None = None,
) -> str:
    """Return one PostgreSQL-safe exact-run stage marker."""

    if not CENSUS_DATABASE_RUN_TOKEN_PATTERN.fullmatch(run_token):
        raise ValueError("pricing projection census database identity is invalid")
    if stage not in CENSUS_DATABASE_STAGE_KEYS and stage != "setup":
        raise ValueError("pricing projection census database stage is invalid")
    if code_ordinal is not None and (
        type(code_ordinal) is not int or code_ordinal <= 0
    ):
        raise ValueError("pricing projection census code ordinal is invalid")
    ordinal_suffix = "" if code_ordinal is None else f":{code_ordinal}"
    application_name = (
        f"{CENSUS_DATABASE_APPLICATION_PREFIX}:{run_token}:{stage}{ordinal_suffix}"
    )
    if len(application_name.encode()) > 63:
        raise ValueError("pricing projection census database stage is too long")
    return application_name


def expected_census_database_settings(run_token: str) -> dict[str, str]:
    """Return the exact session settings for one runtime identity."""

    return {
        "application_name": census_database_application_name(run_token, "setup"),
        **_CENSUS_DATABASE_SETTINGS,
    }


async def _configure_census_database_session(
    session: Any,
    expected_settings: Mapping[str, str],
) -> None:
    """Apply the census's query-local PostgreSQL limits."""

    statements = (
        ("SET LOCAL application_name = " f"'{expected_settings['application_name']}'"),
        "SET LOCAL jit = off",
        "SET LOCAL max_parallel_workers_per_gather = 0",
        "SET LOCAL temp_buffers = '8MB'",
        "SET LOCAL work_mem = '4MB'",
        "SET LOCAL hash_mem_multiplier = 1",
        "SET LOCAL plan_cache_mode = force_custom_plan",
        "SET LOCAL temp_file_limit = '256MB'",
        "SET LOCAL lock_timeout = '5s'",
        "SET LOCAL statement_timeout = '20min'",
    )
    for statement in statements:
        await session.execute(text(statement))


async def _attested_census_database_settings(
    session: Any,
    expected_settings: Mapping[str, str],
) -> tuple[dict[str, str], int]:
    """Read back the exact PostgreSQL limits after acquisition locks."""

    settings_result = await session.execute(text("""
            SELECT current_setting('application_name') AS application_name,
                   current_setting('transaction_read_only')
                       AS transaction_read_only,
                   current_setting('jit') AS jit,
                   current_setting('max_parallel_workers_per_gather')
                       AS max_parallel_workers_per_gather,
                   current_setting('temp_buffers') AS temp_buffers,
                   current_setting('work_mem') AS work_mem,
                   current_setting('hash_mem_multiplier') AS hash_mem_multiplier,
                   current_setting('plan_cache_mode') AS plan_cache_mode,
                   current_setting('temp_file_limit') AS temp_file_limit,
                   current_setting('lock_timeout') AS lock_timeout,
                   current_setting('statement_timeout') AS statement_timeout,
                   pg_backend_pid() AS backend_pid
            """))
    row_by_field = dict(settings_result.mappings().one())
    backend_pid = int(row_by_field.pop("backend_pid"))
    settings_by_field = {
        field_name: str(field_value) for field_name, field_value in row_by_field.items()
    }
    if settings_by_field != expected_settings or backend_pid <= 0:
        raise RuntimeError("pricing projection census database limits changed")
    return settings_by_field, backend_pid


async def set_census_database_stage(
    session: Any,
    run_token: str,
    stage: str,
    expected_previous_application_name: str,
    code_ordinal: int | None = None,
) -> dict[str, int | str]:
    """Bind one exact-run substage and sample only the owning backend."""

    application_name = census_database_application_name(
        run_token,
        stage,
        code_ordinal,
    )
    sample_result = await session.execute(text("""
            SELECT current_setting('application_name') AS application_name,
                   pg_backend_pid() AS backend_pid,
                   COALESCE((
                       SELECT SUM(total_bytes)::bigint
                         FROM pg_backend_memory_contexts
                   ), 0)::bigint AS backend_memory_context_bytes,
                   COALESCE((
                       SELECT SUM(pg_total_relation_size(class.oid))::bigint
                         FROM pg_class AS class
                        WHERE class.relnamespace = pg_my_temp_schema()
                          AND class.relkind = 'r'
                   ), 0)::bigint AS temporary_relation_bytes
            """))
    sample_by_field = {
        field_name: (
            str(field_value) if field_name == "application_name" else int(field_value)
        )
        for field_name, field_value in sample_result.mappings().one().items()
    }
    if (
        sample_by_field["application_name"] != expected_previous_application_name
        or sample_by_field["backend_pid"] <= 0
        or sample_by_field["backend_memory_context_bytes"] < 0
        or sample_by_field["temporary_relation_bytes"] < 0
    ):
        raise RuntimeError("pricing projection census database stage changed")
    application_result = await session.execute(
        text("SELECT set_config('application_name', :application_name, true)"),
        {"application_name": application_name},
    )
    if application_result.scalar_one() != application_name:
        raise RuntimeError("pricing projection census database stage changed")
    return {**sample_by_field, "application_name": application_name}


def declared_occurrence_rows(
    binding_projections: Iterable[Any],
    code_identity: tuple[str, str],
) -> int:
    """Match the executable occurrence cap before empty-price filtering."""

    from api import ptg2_serving as serving

    return sum(
        serving._declared_geo_rate_count(
            binding.code_rows_by_identity.get(code_identity) or ()
        )
        for binding in binding_projections
    )


async def projection_stage_counts(session: Any) -> dict[str, int]:
    """Return the exact staged provider counts retained by the census."""

    counts_result = await session.execute(text("""
            SELECT
                (SELECT COUNT(*) FROM plan_pricing_provider_set_stage)
                    AS provider_set_count,
                (SELECT COUNT(*) FROM plan_pricing_provider_member_stage)
                    AS provider_membership_count,
                (SELECT COALESCE(MAX(membership_count), 0) FROM
                    plan_pricing_provider_set_stage)
                    AS maximum_provider_set_membership_count,
                (SELECT COUNT(*) FROM plan_pricing_provider_cell_stage)
                    AS provider_cell_count,
                (SELECT COALESCE(SUM(OCTET_LENGTH(fragment)), 0)
                   FROM plan_pricing_provider_cell_stage)
                    AS provider_fragment_byte_count,
                (SELECT COUNT(*) FROM plan_pricing_provider_npi_materialized_stage)
                    AS provider_npi_count,
                (SELECT COUNT(*)
                   FROM plan_pricing_provider_npi_pending_stage)
                    AS pending_npi_count,
                (SELECT COUNT(*) FROM plan_pricing_provider_set_stage
                  WHERE membership_count = 0)
                    AS referenced_empty_provider_set_count
            """))
    return {
        field_name: int(field_value)
        for field_name, field_value in counts_result.mappings().one().items()
    }


def price_membership_cache_counts(state: _BuildState) -> dict[str, int]:
    """Return source bounds for retained indexes and unsplittable reads, not RSS."""

    cache = state.price_membership_alias_cache
    cached_block_count = len(cache.identity_by_block)
    retained_identity_bytes = (
        cached_block_count * PRICE_MEMBERSHIP_ALIAS_INDEX_RETAINED_BYTES_PER_BLOCK
    )
    return {
        "price_membership_cached_block_count": cached_block_count,
        "price_membership_identity_retained_bytes": retained_identity_bytes,
        "price_membership_metadata_fragment_count": cache.metadata_record_count,
        "price_membership_maximum_fragments_per_block": (cache.maximum_fragment_count),
        "price_membership_singleton_peak_bytes": (
            retained_identity_bytes
            + cache.maximum_fragment_count
            * PRICE_MEMBERSHIP_TRANSIENT_BYTES_PER_FRAGMENT
        ),
    }


async def _temporary_relation_residue(session: Any) -> list[str]:
    residue_result = await session.execute(
        text("""
            SELECT relname
              FROM pg_class
             WHERE relnamespace = pg_my_temp_schema()
               AND relname = ANY(CAST(:relation_names AS text[]))
            """),
        {"relation_names": list(TEMP_RELATIONS)},
    )
    return sorted(str(name) for name in residue_result.scalars())


async def _finish_rollback(
    session: Any,
    transaction: Any,
    receipt_by_field: dict[str, Any],
) -> None:
    if transaction.is_active:
        await transaction.rollback()
    receipt_by_field["rollback_complete"] = not session.in_transaction()
    receipt_by_field["temporary_relations_after_rollback"] = (
        await _temporary_relation_residue(session)
    )
    await session.rollback()


async def _await_cleanup_task(
    task: asyncio.Task[Any],
    *,
    propagate_cancellation: bool,
) -> Any:
    """Drain cleanup through repeated cancellation."""

    pending_cancellation: asyncio.CancelledError | None = None
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError as exc:
            pending_cancellation = exc
    result = task.result()
    if pending_cancellation is not None and propagate_cancellation:
        raise pending_cancellation
    return result


async def _require_owned_temporary_relations(session: Any, transaction: Any) -> None:
    if not transaction.is_active or tuple(
        await _temporary_relation_residue(session)
    ) != tuple(sorted(TEMP_RELATIONS)):
        raise RuntimeError("pricing projection census TEMP staging is incomplete")


async def _rollback_only(
    receipt_by_field: dict[str, Any],
    operation: Callable[[Any], Awaitable[dict[str, Any]]],
    run_token: str,
) -> dict[str, Any]:
    """Create owned TEMP stages and always roll back the owning session."""

    async with db.session() as session:
        transaction = await session.begin()
        try:
            expected_settings = expected_census_database_settings(run_token)
            await _configure_census_database_session(session, expected_settings)
            await lock_provider_generation(session)
            settings_by_field, backend_pid = await _attested_census_database_settings(
                session,
                expected_settings,
            )
            receipt_by_field["database_run_token"] = run_token
            receipt_by_field["database_backend_pid"] = backend_pid
            receipt_by_field["database_session_settings"] = settings_by_field
            await projection._create_stage_tables(session)
            await _require_owned_temporary_relations(session, transaction)
            return await operation(session)
        finally:
            active_error = sys.exc_info()[1]
            cleanup_task = asyncio.create_task(
                _finish_rollback(session, transaction, receipt_by_field)
            )
            try:
                await _await_cleanup_task(
                    cleanup_task,
                    propagate_cancellation=active_error is None,
                )
            except BaseException as cleanup_error:
                if active_error is None:
                    raise
                receipt_by_field["rollback_error"] = {
                    "type": type(cleanup_error).__name__,
                }


async def _drain_cancelled_task(
    task: asyncio.Task[Any],
    cancellation: asyncio.CancelledError,
    receipt_by_field: dict[str, Any] | None,
) -> None:
    if not task.done():
        task.cancel()
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            continue
    try:
        task.result()
    except asyncio.CancelledError:
        raise cancellation
    except BaseException as task_error:
        if receipt_by_field is not None:
            receipt_by_field["rollback_task_error"] = {
                "type": type(task_error).__name__,
            }
    raise cancellation


async def cancellation_safe(
    operation: Coroutine[Any, Any, _Result],
    receipt_by_field: dict[str, Any] | None = None,
) -> _Result:
    """Cancel an owned task once, then drain it through caller cancellation."""

    task = asyncio.create_task(operation)
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError as cancellation:
        await _drain_cancelled_task(task, cancellation, receipt_by_field)
        raise AssertionError("cancelled task drain returned")


async def rollback_only(
    receipt_by_field: dict[str, Any],
    operation: Callable[[Any], Awaitable[dict[str, Any]]],
    *,
    run_token: str,
) -> dict[str, Any]:
    """Run one rollback-only session and drain it through repeated cancellation."""

    return await cancellation_safe(
        _rollback_only(receipt_by_field, operation, run_token),
        receipt_by_field,
    )
