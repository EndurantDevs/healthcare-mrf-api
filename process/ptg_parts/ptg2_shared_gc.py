# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Lifecycle cleanup for immutable PTG V3 shared layouts and blocks."""

from __future__ import annotations

import argparse
import asyncio
import os
from dataclasses import dataclass
from typing import Any, Iterable, Sequence

from db.connection import db
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_shared_blocks import (
    PTG2_V3_CLEANUP_GENERATIONS,
    PTG2_V3_DENSE_LAYOUT_TABLES,
    PTG2_V3_SHARED_GENERATION,
)


PTG2_V3_BUILDING_MAX_AGE_SECONDS_ENV = "HLTHPRT_PTG2_V3_BUILDING_MAX_AGE_SECONDS"
PTG2_V3_BLOCK_GC_GRACE_SECONDS_ENV = "HLTHPRT_PTG2_V3_BLOCK_GC_GRACE_SECONDS"
PTG2_V3_BLOCK_GC_MAX_BYTES_ENV = "HLTHPRT_PTG2_V3_BLOCK_GC_MAX_BYTES"
PTG2_V3_LAYOUT_GC_BATCH_ROWS_ENV = "HLTHPRT_PTG2_V3_LAYOUT_GC_BATCH_ROWS"
PTG2_V3_BLOCK_GC_MAX_ROWS_ENV = "HLTHPRT_PTG2_V3_BLOCK_GC_MAX_ROWS"

PTG2_V3_BUILDING_MAX_AGE_SECONDS_DEFAULT = 21_600
PTG2_V3_BLOCK_GC_GRACE_SECONDS_DEFAULT = 21_600
PTG2_V3_BLOCK_GC_MAX_BYTES_DEFAULT = 80 * 1024 * 1024 * 1024
PTG2_V3_LAYOUT_GC_BATCH_ROWS_DEFAULT = 400
PTG2_V3_BLOCK_GC_MAX_ROWS_DEFAULT = 1_000

PTG2_V3_MIGRATION_OWNED_TABLE_NAMES = (
    "ptg2_v3_snapshot_layout",
    "ptg2_v3_layout_fingerprint",
    "ptg2_v3_snapshot_binding",
    "ptg2_v3_snapshot_scope",
    "ptg2_v3_snapshot_plan_scope",
    "ptg2_v3_snapshot_source",
    "ptg2_v3_block",
    "ptg2_v3_snapshot_block",
    "ptg2_v3_graph_owner",
    "ptg2_v3_code",
    "ptg2_v3_provider_group",
    "ptg2_v3_provider_set",
    "ptg2_v3_price_attr",
    "ptg2_v3_npi_scope",
    "ptg2_v3_audit_occurrence",
    "ptg2_v3_source_audit_witness",
    "ptg2_v3_candidate_audit_attestation",
    "ptg2_v3_gc_candidate",
)

# Compatibility for cleanup callers and tests that predate explicit ownership.
_SHARED_TABLE_NAMES = PTG2_V3_MIGRATION_OWNED_TABLE_NAMES


@dataclass(frozen=True)
class PTG2SharedLayoutGCStats:
    logical_layout_count: int = 0
    candidate_hash_count: int = 0
    stored_bytes: int = 0
    tables_available: bool = True


@dataclass(frozen=True)
class PTG2SharedBlockSweepPlan:
    selected_hashes: tuple[bytes, ...] = ()
    stored_bytes: int = 0
    tables_available: bool = True

    @property
    def selected_hash_count(self) -> int:
        """Return the number of block hashes selected for deletion."""

        return len(self.selected_hashes)


@dataclass(frozen=True)
class PTG2SharedGCPlan:
    layouts: PTG2SharedLayoutGCStats
    sweep: PTG2SharedBlockSweepPlan

    @property
    def logical_layout_count(self) -> int:
        """Return the number of layouts released by the layout phase."""

        return self.layouts.logical_layout_count

    @property
    def candidate_hash_count(self) -> int:
        """Return the number of block hashes queued by layout release."""

        return self.layouts.candidate_hash_count

    @property
    def stored_bytes(self) -> int:
        """Return stored bytes referenced by the released layouts."""

        return self.layouts.stored_bytes

    @property
    def selected_hashes(self) -> tuple[bytes, ...]:
        """Return block hashes selected by the sweep phase."""

        return self.sweep.selected_hashes

    @property
    def selected_hash_count(self) -> int:
        """Return the number of block hashes selected by the sweep phase."""

        return self.sweep.selected_hash_count


def _row_mapping(row: Any) -> dict[str, Any]:
    if row is None:
        return {}
    if isinstance(row, dict):
        return row
    return dict(getattr(row, "_mapping", row))


def resolve_ptg2_schema(schema_name: str | None = None) -> str:
    """Resolve the PTG schema with the same conflict rules as Alembic."""

    if schema_name:
        return str(schema_name)
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise RuntimeError(
            "DB_SCHEMA and HLTHPRT_DB_SCHEMA must identify the same schema"
        )
    return runtime_schema or legacy_schema or "mrf"


async def missing_migration_owned_tables(
    executor: Any,
    schema_name: str,
) -> tuple[str, ...]:
    """Return strict V3 tables that must have been created by Alembic."""

    table_records = await executor.all(
        """
        SELECT table_name
          FROM information_schema.tables
         WHERE table_schema = :schema_name
           AND table_name = ANY(CAST(:table_names AS text[]))
        """,
        schema_name=schema_name,
        table_names=list(PTG2_V3_MIGRATION_OWNED_TABLE_NAMES),
    )
    present_table_names = {
        str(_row_mapping(table_record).get("table_name") or "")
        for table_record in table_records
        if _row_mapping(table_record).get("table_name")
    }
    return tuple(
        sorted(set(PTG2_V3_MIGRATION_OWNED_TABLE_NAMES) - present_table_names)
    )


async def require_migration_owned_tables(
    executor: Any,
    schema_name: str,
) -> None:
    """Fail before runtime DDL or cleanup when the V3 migration is absent."""

    missing = await missing_migration_owned_tables(
        executor,
        schema_name,
    )
    if missing:
        raise RuntimeError(
            "PTG V3 migration-owned tables are missing from schema "
            f"{schema_name}: {', '.join(missing)}; run alembic upgrade head"
        )


def _env_non_negative_int(name: str, default: int) -> int:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    try:
        return max(int(str(raw_value).strip()), 0)
    except ValueError:
        return default


def _env_positive_int(name: str, default: int) -> int:
    return max(_env_non_negative_int(name, default), 1)


def _building_max_age_seconds(value: int | None = None) -> int:
    if value is not None:
        return max(int(value), 0)
    return _env_non_negative_int(
        PTG2_V3_BUILDING_MAX_AGE_SECONDS_ENV,
        PTG2_V3_BUILDING_MAX_AGE_SECONDS_DEFAULT,
    )


def _block_gc_grace_seconds(value: int | None = None) -> int:
    if value is not None:
        return max(int(value), 0)
    return _env_non_negative_int(
        PTG2_V3_BLOCK_GC_GRACE_SECONDS_ENV,
        PTG2_V3_BLOCK_GC_GRACE_SECONDS_DEFAULT,
    )


def _block_gc_max_bytes(value: int | None = None) -> int:
    if value is not None:
        return max(int(value), 0)
    return _env_non_negative_int(
        PTG2_V3_BLOCK_GC_MAX_BYTES_ENV,
        PTG2_V3_BLOCK_GC_MAX_BYTES_DEFAULT,
    )


def _layout_batch_rows(value: int | None = None) -> int:
    if value is not None:
        return max(int(value), 1)
    return _env_positive_int(
        PTG2_V3_LAYOUT_GC_BATCH_ROWS_ENV,
        PTG2_V3_LAYOUT_GC_BATCH_ROWS_DEFAULT,
    )


def _block_gc_max_rows(value: int | None = None) -> int:
    if value is not None:
        return max(int(value), 1)
    return _env_positive_int(
        PTG2_V3_BLOCK_GC_MAX_ROWS_ENV,
        PTG2_V3_BLOCK_GC_MAX_ROWS_DEFAULT,
    )


def is_shared_blocks_cleanup_manifest(serving_index: dict[str, Any] | None) -> bool:
    """Return whether a serving index names a shared-block cleanup generation."""

    if not isinstance(serving_index, dict):
        return False
    return any(
        str(serving_index.get(field_name) or "").strip().lower()
        in PTG2_V3_CLEANUP_GENERATIONS
        for field_name in ("storage_generation", "generation", "storage")
    )


def is_shared_blocks_v1_manifest(serving_index: dict[str, Any] | None) -> bool:
    """Compatibility name used only by cleanup callers; V1 is never serveable."""

    return is_shared_blocks_cleanup_manifest(serving_index)


async def _has_shared_manifest(executor: Any, schema_name: str) -> bool:
    existence_records = await executor.all(
        f"""
        SELECT EXISTS (
            SELECT 1
              FROM {_quote_ident(schema_name)}.ptg2_snapshot
             WHERE lower(COALESCE(manifest->'serving_index'->>'storage_generation', ''))
                       = ANY(CAST(:cleanup_generations AS text[]))
                OR lower(COALESCE(manifest->'serving_index'->>'generation', ''))
                       = ANY(CAST(:cleanup_generations AS text[]))
                OR lower(COALESCE(manifest->'serving_index'->>'storage', ''))
                       = ANY(CAST(:cleanup_generations AS text[]))
        ) AS involved
        """,
        cleanup_generations=list(PTG2_V3_CLEANUP_GENERATIONS),
    )
    return (
        bool(_row_mapping(existence_records[0]).get("involved"))
        if existence_records
        else False
    )


async def _has_shared_binding(executor: Any, schema_name: str) -> bool:
    existence_records = await executor.all(
        f"""
        SELECT EXISTS (
            SELECT 1
              FROM {_quote_ident(schema_name)}.ptg2_v3_snapshot_binding
        ) AS involved
        """
    )
    return (
        bool(_row_mapping(existence_records[0]).get("involved"))
        if existence_records
        else False
    )


async def _has_shared_scope(executor: Any, schema_name: str) -> bool:
    existence_records = await executor.all(
        f"""
        SELECT EXISTS (
            SELECT 1
              FROM {_quote_ident(schema_name)}.ptg2_v3_snapshot_scope
        ) AS involved
        """
    )
    return (
        bool(_row_mapping(existence_records[0]).get("involved"))
        if existence_records
        else False
    )


async def _has_shared_tables(
    executor: Any,
    schema_name: str,
    *,
    require_shared: bool,
) -> bool:
    table_names = ["ptg2_snapshot", *_SHARED_TABLE_NAMES]
    table_records = await executor.all(
        """
        SELECT table_name
          FROM information_schema.tables
         WHERE table_schema = :schema_name
           AND table_name = ANY(CAST(:table_names AS text[]))
        """,
        schema_name=schema_name,
        table_names=table_names,
    )
    present_table_names = {
        str(_row_mapping(table_record).get("table_name") or "")
        for table_record in table_records
        if _row_mapping(table_record).get("table_name")
    }
    missing = sorted(set(_SHARED_TABLE_NAMES) - present_table_names)
    if not missing:
        return True

    manifest_involved = require_shared
    if not manifest_involved and "ptg2_snapshot" in present_table_names:
        manifest_involved = await _has_shared_manifest(executor, schema_name)
    has_shared_binding = False
    if "ptg2_v3_snapshot_binding" in present_table_names:
        has_shared_binding = await _has_shared_binding(executor, schema_name)
    has_shared_scope = False
    if "ptg2_v3_snapshot_scope" in present_table_names:
        has_shared_scope = await _has_shared_scope(executor, schema_name)
    if manifest_involved or has_shared_binding or has_shared_scope:
        raise RuntimeError(
            "shared-block cleanup requires the complete shared schema; "
            f"missing tables: {', '.join(missing)}"
        )
    return False


def _layout_plan_sql(schema_name: str) -> str:
    schema = _quote_ident(schema_name)
    return f"""
        WITH eligible_layouts AS MATERIALIZED (
            SELECT layout.snapshot_key
              FROM {schema}.ptg2_v3_snapshot_layout AS layout
             WHERE layout.generation = ANY(CAST(:cleanup_generations AS text[]))
               AND (
                    (
                        layout.state = 'sealed'
                        AND COALESCE(layout.lease_until, '-infinity'::timestamptz)
                            <= transaction_timestamp()
                    )
                    OR (
                        layout.state = 'building'
                        AND layout.heartbeat_at
                            < transaction_timestamp()
                              - (:building_max_age_seconds * INTERVAL '1 second')
                        AND COALESCE(layout.lease_until, '-infinity'::timestamptz)
                            <= transaction_timestamp()
                    )
               )
               AND NOT EXISTS (
                    SELECT 1
                      FROM {schema}.ptg2_v3_snapshot_binding AS binding
                     WHERE binding.snapshot_key = layout.snapshot_key
                       AND NOT (
                            binding.snapshot_id = ANY(
                                CAST(:removing_snapshot_ids AS varchar[])
                            )
                       )
               )
               AND (
                    cardinality(CAST(:removing_snapshot_ids AS varchar[])) = 0
                    OR EXISTS (
                        SELECT 1
                          FROM {schema}.ptg2_v3_snapshot_binding AS candidate_binding
                         WHERE candidate_binding.snapshot_key = layout.snapshot_key
                           AND candidate_binding.snapshot_id = ANY(
                                CAST(:removing_snapshot_ids AS varchar[])
                           )
                    )
               )
             ORDER BY layout.created_at, layout.snapshot_key
             LIMIT :layout_limit
        ),
        mapped_blocks AS MATERIALIZED (
            SELECT DISTINCT mapping.block_hash
              FROM {schema}.ptg2_v3_snapshot_block AS mapping
              JOIN eligible_layouts AS layout
                ON layout.snapshot_key = mapping.snapshot_key
        )
        SELECT (SELECT COUNT(*) FROM eligible_layouts) AS logical_layout_count,
               COUNT(block.block_hash) AS candidate_hash_count,
               COALESCE(SUM(block.stored_byte_count), 0) AS stored_bytes
          FROM mapped_blocks AS mapped
          JOIN {schema}.ptg2_v3_block AS block
            ON block.block_hash = mapped.block_hash
    """


async def _build_layout_release_plan_ready(
    executor: Any,
    *,
    schema_name: str,
    removing_snapshot_ids: Sequence[str],
    building_max_age_seconds: int,
    layout_limit: int | None,
) -> PTG2SharedLayoutGCStats:
    aggregate_records = await executor.all(
        _layout_plan_sql(schema_name),
        cleanup_generations=list(PTG2_V3_CLEANUP_GENERATIONS),
        removing_snapshot_ids=list(dict.fromkeys(str(value) for value in removing_snapshot_ids)),
        building_max_age_seconds=building_max_age_seconds,
        layout_limit=layout_limit,
    )
    aggregate_record = _row_mapping(aggregate_records[0]) if aggregate_records else {}
    return PTG2SharedLayoutGCStats(
        logical_layout_count=int(aggregate_record.get("logical_layout_count") or 0),
        candidate_hash_count=int(aggregate_record.get("candidate_hash_count") or 0),
        stored_bytes=int(aggregate_record.get("stored_bytes") or 0),
    )


async def build_shared_layout_release_plan(
    *,
    schema_name: str | None = None,
    executor: Any | None = None,
    removing_snapshot_ids: Sequence[str] = (),
    building_max_age_seconds: int | None = None,
    max_layouts: int | None = None,
    all_eligible_layouts: bool = False,
    require_shared: bool = False,
) -> PTG2SharedLayoutGCStats:
    """Plan layout release without reading block payloads or changing state."""

    schema_name = resolve_ptg2_schema(schema_name)
    executor = executor or db
    if not await _has_shared_tables(
        executor,
        schema_name,
        require_shared=require_shared,
    ):
        return PTG2SharedLayoutGCStats(tables_available=False)
    return await _build_layout_release_plan_ready(
        executor,
        schema_name=schema_name,
        removing_snapshot_ids=removing_snapshot_ids,
        building_max_age_seconds=_building_max_age_seconds(building_max_age_seconds),
        layout_limit=None if all_eligible_layouts else _layout_batch_rows(max_layouts),
    )


def _lock_layouts_sql(schema_name: str) -> str:
    schema = _quote_ident(schema_name)
    return f"""
        SELECT layout.snapshot_key
          FROM {schema}.ptg2_v3_snapshot_layout AS layout
         WHERE layout.generation = ANY(CAST(:cleanup_generations AS text[]))
           AND (
                NOT :restrict_layout_keys
                OR layout.snapshot_key = ANY(CAST(:layout_keys AS bigint[]))
           )
           AND NOT EXISTS (
                SELECT 1
                  FROM {schema}.ptg2_v3_snapshot_binding AS binding
                 WHERE binding.snapshot_key = layout.snapshot_key
           )
           AND (
                (
                    layout.state = 'sealed'
                    AND COALESCE(layout.lease_until, '-infinity'::timestamptz)
                        <= transaction_timestamp()
                )
                OR (
                    layout.state = 'building'
                    AND layout.heartbeat_at
                        < transaction_timestamp()
                          - (:building_max_age_seconds * INTERVAL '1 second')
                    AND COALESCE(layout.lease_until, '-infinity'::timestamptz)
                        <= transaction_timestamp()
                )
           )
         ORDER BY layout.created_at, layout.snapshot_key
         LIMIT :layout_limit
           FOR UPDATE OF layout SKIP LOCKED
    """


def _release_layouts_sql(schema_name: str) -> str:
    """Build guarded SQL that queues block GC and deletes unbound layouts."""

    schema = _quote_ident(schema_name)
    dense_delete_ctes: list[str] = []
    dense_delete_dependencies: list[str] = []
    for table_name in PTG2_V3_DENSE_LAYOUT_TABLES:
        cte_name = f"deleted_{table_name.removeprefix('ptg2_v3_')}"
        dense_delete_ctes.append(
            f"""
        {cte_name} AS (
            DELETE FROM {schema}.{_quote_ident(table_name)} AS payload
             USING layout_batch AS selected
             WHERE payload.snapshot_key = selected.snapshot_key
            RETURNING payload.snapshot_key
        ),"""
        )
        dense_delete_dependencies.append(
            f"AND (SELECT COUNT(*) FROM {cte_name}) >= 0"
        )
    dense_delete_sql = "\n".join(dense_delete_ctes)
    dense_dependency_sql = "\n               ".join(dense_delete_dependencies)
    return f"""
        WITH layout_batch AS MATERIALIZED (
            SELECT layout.snapshot_key
              FROM {schema}.ptg2_v3_snapshot_layout AS layout
             WHERE layout.snapshot_key = ANY(CAST(:layout_keys AS bigint[]))
               AND layout.generation = ANY(CAST(:cleanup_generations AS text[]))
               AND NOT EXISTS (
                    SELECT 1
                      FROM {schema}.ptg2_v3_snapshot_binding AS binding
                     WHERE binding.snapshot_key = layout.snapshot_key
               )
               AND (
                    (
                        layout.state = 'sealed'
                        AND COALESCE(layout.lease_until, '-infinity'::timestamptz)
                            <= transaction_timestamp()
                    )
                    OR (
                        layout.state = 'building'
                        AND layout.heartbeat_at
                            < transaction_timestamp()
                              - (:building_max_age_seconds * INTERVAL '1 second')
                        AND COALESCE(layout.lease_until, '-infinity'::timestamptz)
                            <= transaction_timestamp()
                    )
               )
        ),
        mapped_blocks AS MATERIALIZED (
            SELECT DISTINCT mapping.block_hash
              FROM {schema}.ptg2_v3_snapshot_block AS mapping
              JOIN layout_batch AS layout
                ON layout.snapshot_key = mapping.snapshot_key
        ),
        queued_candidates AS (
            INSERT INTO {schema}.ptg2_v3_gc_candidate AS candidate
                (block_hash, eligible_at, queued_at)
            SELECT mapped.block_hash,
                   transaction_timestamp()
                       + (:grace_seconds * INTERVAL '1 second'),
                   transaction_timestamp()
              FROM mapped_blocks AS mapped
            ON CONFLICT (block_hash) DO UPDATE
                SET eligible_at = GREATEST(
                    candidate.eligible_at,
                    EXCLUDED.eligible_at
                )
            RETURNING block_hash
        ),
        {dense_delete_sql}
        deleted_layouts AS (
            DELETE FROM {schema}.ptg2_v3_snapshot_layout AS layout
             USING layout_batch AS selected
             WHERE layout.snapshot_key = selected.snapshot_key
               AND NOT EXISTS (
                    SELECT 1
                      FROM {schema}.ptg2_v3_snapshot_binding AS binding
                     WHERE binding.snapshot_key = layout.snapshot_key
               )
               AND (SELECT COUNT(*) FROM queued_candidates) >= 0
               {dense_dependency_sql}
            RETURNING layout.snapshot_key
        )
        SELECT (SELECT COUNT(*) FROM deleted_layouts) AS logical_layout_count,
               (SELECT COUNT(*) FROM mapped_blocks) AS candidate_hash_count,
               COALESCE((
                   SELECT SUM(block.stored_byte_count)
                     FROM mapped_blocks AS mapped
                     JOIN {schema}.ptg2_v3_block AS block
                       ON block.block_hash = mapped.block_hash
               ), 0) AS stored_bytes
    """


async def _release_layouts_ready(
    executor: Any,
    *,
    schema_name: str,
    building_max_age_seconds: int,
    grace_seconds: int,
    max_layouts: int,
    layout_keys: Sequence[int] | None,
) -> PTG2SharedLayoutGCStats:
    locked_rows = await executor.all(
        _lock_layouts_sql(schema_name),
        cleanup_generations=list(PTG2_V3_CLEANUP_GENERATIONS),
        building_max_age_seconds=building_max_age_seconds,
        layout_limit=max_layouts,
        restrict_layout_keys=layout_keys is not None,
        layout_keys=list(layout_keys or ()),
    )
    layout_keys = [
        int(_row_mapping(locked_layout_row).get("snapshot_key"))
        for locked_layout_row in locked_rows
        if _row_mapping(locked_layout_row).get("snapshot_key") is not None
    ]
    if not layout_keys:
        return PTG2SharedLayoutGCStats()
    aggregate_records = await executor.all(
        _release_layouts_sql(schema_name),
        layout_keys=layout_keys,
        cleanup_generations=list(PTG2_V3_CLEANUP_GENERATIONS),
        building_max_age_seconds=building_max_age_seconds,
        grace_seconds=grace_seconds,
    )
    aggregate_record = _row_mapping(aggregate_records[0]) if aggregate_records else {}
    return PTG2SharedLayoutGCStats(
        logical_layout_count=int(aggregate_record.get("logical_layout_count") or 0),
        candidate_hash_count=int(aggregate_record.get("candidate_hash_count") or 0),
        stored_bytes=int(aggregate_record.get("stored_bytes") or 0),
    )


async def release_unbound_ptg2_shared_layouts(
    *,
    schema_name: str | None = None,
    executor: Any | None = None,
    building_max_age_seconds: int | None = None,
    grace_seconds: int | None = None,
    max_layouts: int | None = None,
    require_shared: bool = False,
    layout_keys: Sequence[int] | None = None,
) -> PTG2SharedLayoutGCStats:
    """Queue hashes and delete one bounded batch of eligible unbound layouts."""

    schema_name = resolve_ptg2_schema(schema_name)
    building_age = _building_max_age_seconds(building_max_age_seconds)
    grace = _block_gc_grace_seconds(grace_seconds)
    layout_limit = _layout_batch_rows(max_layouts)

    async def _run(connection: Any) -> PTG2SharedLayoutGCStats:
        if not await _has_shared_tables(
            connection,
            schema_name,
            require_shared=require_shared,
        ):
            return PTG2SharedLayoutGCStats(tables_available=False)
        return await _release_layouts_ready(
            connection,
            schema_name=schema_name,
            building_max_age_seconds=building_age,
            grace_seconds=grace,
            max_layouts=layout_limit,
            layout_keys=layout_keys,
        )

    if executor is not None and executor is not db:
        return await _run(executor)
    async with db.acquire() as connection:
        return await _run(connection)


def _eligible_blocks_sql(schema_name: str, *, lock_rows: bool) -> str:
    schema = _quote_ident(schema_name)
    lock_clause = "FOR UPDATE OF candidate, block SKIP LOCKED" if lock_rows else ""
    return f"""
        SELECT candidate.block_hash,
               block.stored_byte_count,
               candidate.eligible_at
          FROM {schema}.ptg2_v3_gc_candidate AS candidate
          JOIN {schema}.ptg2_v3_block AS block
            ON block.block_hash = candidate.block_hash
         WHERE candidate.eligible_at <= transaction_timestamp()
           AND block.stored_byte_count <= :max_bytes
           AND NOT EXISTS (
                SELECT 1
                  FROM {schema}.ptg2_v3_snapshot_block AS mapping
                 WHERE mapping.block_hash = candidate.block_hash
           )
         ORDER BY candidate.eligible_at, candidate.block_hash
         LIMIT :max_rows
         {lock_clause}
    """


def _select_hashes_under_byte_cap(
    rows: Iterable[Any],
    *,
    max_bytes: int,
) -> PTG2SharedBlockSweepPlan:
    selected_hashes: list[bytes] = []
    selected_bytes = 0
    for raw_row in rows:
        row = _row_mapping(raw_row)
        stored_bytes = max(int(row.get("stored_byte_count") or 0), 0)
        if selected_bytes + stored_bytes > max_bytes:
            continue
        block_hash = bytes(row.get("block_hash") or b"")
        if not block_hash:
            continue
        selected_hashes.append(block_hash)
        selected_bytes += stored_bytes
    return PTG2SharedBlockSweepPlan(
        selected_hashes=tuple(selected_hashes),
        stored_bytes=selected_bytes,
    )


async def _build_sweep_plan_ready(
    executor: Any,
    *,
    schema_name: str,
    max_bytes: int,
    max_rows: int,
    lock_rows: bool,
) -> PTG2SharedBlockSweepPlan:
    rows = await executor.all(
        _eligible_blocks_sql(schema_name, lock_rows=lock_rows),
        max_bytes=max_bytes,
        max_rows=max_rows,
    )
    return _select_hashes_under_byte_cap(rows, max_bytes=max_bytes)


async def build_shared_block_sweep_plan(
    *,
    schema_name: str | None = None,
    executor: Any | None = None,
    max_bytes: int | None = None,
    max_rows: int | None = None,
    require_shared: bool = False,
) -> PTG2SharedBlockSweepPlan:
    """Select the exact eligible block hashes a bounded sweep would delete."""

    schema_name = resolve_ptg2_schema(schema_name)
    executor = executor or db
    if not await _has_shared_tables(
        executor,
        schema_name,
        require_shared=require_shared,
    ):
        return PTG2SharedBlockSweepPlan(tables_available=False)
    return await _build_sweep_plan_ready(
        executor,
        schema_name=schema_name,
        max_bytes=_block_gc_max_bytes(max_bytes),
        max_rows=_block_gc_max_rows(max_rows),
        lock_rows=False,
    )


def _delete_blocks_sql(schema_name: str) -> str:
    schema = _quote_ident(schema_name)
    return f"""
        DELETE FROM {schema}.ptg2_v3_block AS block
         WHERE block.block_hash = ANY(CAST(:block_hashes AS bytea[]))
           AND NOT EXISTS (
                SELECT 1
                  FROM {schema}.ptg2_v3_snapshot_block AS mapping
                 WHERE mapping.block_hash = block.block_hash
           )
        RETURNING block.block_hash, block.stored_byte_count
    """


def _remove_orphan_candidates_sql(schema_name: str) -> str:
    schema = _quote_ident(schema_name)
    return f"""
        WITH orphaned AS (
            SELECT candidate.block_hash
              FROM {schema}.ptg2_v3_gc_candidate AS candidate
             WHERE NOT EXISTS (
                    SELECT 1
                      FROM {schema}.ptg2_v3_block AS block
                     WHERE block.block_hash = candidate.block_hash
             )
             ORDER BY candidate.block_hash
             LIMIT :max_rows
        )
        DELETE FROM {schema}.ptg2_v3_gc_candidate AS candidate
         USING orphaned
         WHERE candidate.block_hash = orphaned.block_hash
    """


async def _sweep_ready(
    executor: Any,
    *,
    schema_name: str,
    max_bytes: int,
    max_rows: int,
) -> PTG2SharedBlockSweepPlan:
    selected = await _build_sweep_plan_ready(
        executor,
        schema_name=schema_name,
        max_bytes=max_bytes,
        max_rows=max_rows,
        lock_rows=True,
    )
    deleted_rows: list[Any] = []
    if selected.selected_hashes:
        deleted_rows = await executor.all(
            _delete_blocks_sql(schema_name),
            block_hashes=list(selected.selected_hashes),
        )
    await executor.status(
        _remove_orphan_candidates_sql(schema_name),
        max_rows=max_rows,
    )
    deleted_bytes_by_hash = {
        bytes(_row_mapping(deleted_block_row).get("block_hash") or b""): int(
            _row_mapping(deleted_block_row).get("stored_byte_count") or 0
        )
        for deleted_block_row in deleted_rows
        if _row_mapping(deleted_block_row).get("block_hash")
    }
    deleted_hashes = tuple(
        block_hash
        for block_hash in selected.selected_hashes
        if block_hash in deleted_bytes_by_hash
    )
    return PTG2SharedBlockSweepPlan(
        selected_hashes=deleted_hashes,
        stored_bytes=sum(deleted_bytes_by_hash[block_hash] for block_hash in deleted_hashes),
    )


async def sweep_ptg2_shared_blocks(
    *,
    schema_name: str | None = None,
    executor: Any | None = None,
    max_bytes: int | None = None,
    max_rows: int | None = None,
    require_shared: bool = False,
) -> PTG2SharedBlockSweepPlan:
    """Delete one eligible, byte-bounded block batch after a final mapping check."""

    schema_name = resolve_ptg2_schema(schema_name)
    byte_limit = _block_gc_max_bytes(max_bytes)
    row_limit = _block_gc_max_rows(max_rows)

    async def _run(connection: Any) -> PTG2SharedBlockSweepPlan:
        if not await _has_shared_tables(
            connection,
            schema_name,
            require_shared=require_shared,
        ):
            return PTG2SharedBlockSweepPlan(tables_available=False)
        return await _sweep_ready(
            connection,
            schema_name=schema_name,
            max_bytes=byte_limit,
            max_rows=row_limit,
        )

    if executor is not None:
        return await _run(executor)
    async with db.acquire() as connection:
        return await _run(connection)


async def build_ptg2_shared_gc_plan(
    *,
    schema_name: str | None = None,
    executor: Any | None = None,
    removing_snapshot_ids: Sequence[str] = (),
    building_max_age_seconds: int | None = None,
    max_layouts: int | None = None,
    max_bytes: int | None = None,
    max_rows: int | None = None,
    require_shared: bool = False,
) -> PTG2SharedGCPlan:
    """Build a metadata-only dry-run plan for layout release and block sweep."""

    schema_name = resolve_ptg2_schema(schema_name)
    executor = executor or db
    if not await _has_shared_tables(
        executor,
        schema_name,
        require_shared=require_shared,
    ):
        return PTG2SharedGCPlan(
            layouts=PTG2SharedLayoutGCStats(tables_available=False),
            sweep=PTG2SharedBlockSweepPlan(tables_available=False),
        )
    layouts = await _build_layout_release_plan_ready(
        executor,
        schema_name=schema_name,
        removing_snapshot_ids=removing_snapshot_ids,
        building_max_age_seconds=_building_max_age_seconds(building_max_age_seconds),
        layout_limit=_layout_batch_rows(max_layouts),
    )
    sweep = await _build_sweep_plan_ready(
        executor,
        schema_name=schema_name,
        max_bytes=_block_gc_max_bytes(max_bytes),
        max_rows=_block_gc_max_rows(max_rows),
        lock_rows=False,
    )
    return PTG2SharedGCPlan(layouts=layouts, sweep=sweep)


missing_ptg2_v3_migration_owned_tables = missing_migration_owned_tables
require_ptg2_v3_migration_owned_tables = require_migration_owned_tables
build_ptg2_shared_layout_release_plan = build_shared_layout_release_plan
build_ptg2_shared_block_sweep_plan = build_shared_block_sweep_plan


def _print_plan(plan: PTG2SharedGCPlan) -> None:
    print(f"shared_tables_available={str(plan.layouts.tables_available).lower()}")
    print(f"logical_layouts={plan.logical_layout_count}")
    print(f"candidate_hashes={plan.candidate_hash_count}")
    print(f"candidate_stored_bytes={plan.stored_bytes}")
    print(f"selected_hashes={plan.selected_hash_count}")
    print(f"selected_stored_bytes={plan.sweep.stored_bytes}")
    for block_hash in plan.selected_hashes:
        print(f"  selected_hash={block_hash.hex()}")


def _non_negative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


async def _amain(argv: Iterable[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Clean PTG V3 shared layouts and blocks.")
    parser.add_argument("--schema", default=os.getenv("HLTHPRT_DB_SCHEMA") or "mrf")
    parser.add_argument("--execute", action="store_true", help="Apply cleanup. Default is dry-run.")
    parser.add_argument("--max-layouts", type=_non_negative_int, default=None)
    parser.add_argument("--max-rows", type=_non_negative_int, default=None)
    parser.add_argument("--max-bytes", type=_non_negative_int, default=None)
    args = parser.parse_args(list(argv) if argv is not None else None)
    if not args.execute:
        plan = await build_ptg2_shared_gc_plan(
            schema_name=args.schema,
            max_layouts=args.max_layouts,
            max_rows=args.max_rows,
            max_bytes=args.max_bytes,
        )
        _print_plan(plan)
        print("cleanup_executed=false")
        return

    layouts = await release_unbound_ptg2_shared_layouts(
        schema_name=args.schema,
        max_layouts=args.max_layouts,
    )
    sweep = await sweep_ptg2_shared_blocks(
        schema_name=args.schema,
        max_rows=args.max_rows,
        max_bytes=args.max_bytes,
    )
    _print_plan(PTG2SharedGCPlan(layouts=layouts, sweep=sweep))
    print("cleanup_executed=true")


if __name__ == "__main__":
    asyncio.run(_amain())
