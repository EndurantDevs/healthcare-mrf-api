# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Garbage collect non-current PTG2 source snapshot tables.

This helper targets terminal snapshots and abandoned building snapshots that
are not referenced by any current pointer table. A building snapshot remains
protected while its import run is active and heartbeating. Validated candidates
are never admitted by age-based GC and require explicit authenticated removal.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Iterable

from db.connection import db
from process.ptg_parts.allowed_amounts import PTG2_ALLOWED_AMOUNT_CONTRACT
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_artifact_blobs import ensure_ptg2_artifact_blob_table
from process.ptg_parts.ptg2_lifecycle_lock import PTG2_SOURCE_POINTER_GC_LOCK_KEY
from process.ptg_parts.ptg2_shared_gc import (
    PTG2_V3_MIGRATION_OWNED_TABLE_NAMES,
    build_ptg2_shared_layout_release_plan,
    release_unbound_ptg2_shared_layouts,
    require_ptg2_v3_migration_owned_tables,
    resolve_ptg2_schema,
)
from process.ptg_parts.snapshot_cleanup import (
    is_strict_ptg2_v3_shared_blocks_manifest,
)


_GC_CANDIDATE_STATUSES = frozenset({"published", "failed"})
_AGE_GC_PROTECTED_STATUSES = frozenset({"validated"})
_STALE_BUILD_SECONDS_ENV = "HLTHPRT_PTG2_STALE_BUILD_SECONDS"
_STALE_BUILD_SECONDS_DEFAULT = 21_600
_CURRENT_SNAPSHOT_IDS_SQL = """
SELECT DISTINCT snapshot_id
  FROM (
        SELECT snapshot_id FROM __SCHEMA__.ptg2_current_snapshot WHERE snapshot_id IS NOT NULL
        UNION ALL
        SELECT previous_snapshot_id AS snapshot_id FROM __SCHEMA__.ptg2_current_snapshot WHERE previous_snapshot_id IS NOT NULL
        UNION ALL
        SELECT snapshot_id FROM __SCHEMA__.ptg2_current_source_snapshot WHERE snapshot_id IS NOT NULL
        UNION ALL
        SELECT previous_snapshot_id AS snapshot_id FROM __SCHEMA__.ptg2_current_source_snapshot WHERE previous_snapshot_id IS NOT NULL
        UNION ALL
        SELECT snapshot_id FROM __SCHEMA__.ptg2_current_plan_source WHERE snapshot_id IS NOT NULL
        UNION ALL
        SELECT previous_snapshot_id AS snapshot_id FROM __SCHEMA__.ptg2_current_plan_source WHERE previous_snapshot_id IS NOT NULL
  ) refs
 ORDER BY snapshot_id
"""
_SNAPSHOT_GC_ROWS_SQL = """
SELECT snapshot.snapshot_id,
       snapshot.status,
       snapshot.previous_snapshot_id,
       snapshot.manifest,
       snapshot.manifest->'serving_index' AS serving_index,
       COALESCE(
           snapshot.manifest->'serving_index'->>'source_key',
           snapshot.manifest->>'source_key'
       ) AS source_key,
       (
           snapshot.status = 'building'
           AND snapshot.created_at
               < timezone('UTC', transaction_timestamp())
                 - (:stale_build_seconds * INTERVAL '1 second')
           AND (
               import_run.import_run_id IS NULL
               OR import_run.status NOT IN ('pending', 'running', 'building')
               OR COALESCE(
                   import_run.heartbeat_at,
                   import_run.started_at,
                   '-infinity'::timestamp
               ) < timezone('UTC', transaction_timestamp())
                   - (:stale_build_seconds * INTERVAL '1 second')
           )
       ) AS stale_building
  FROM __SCHEMA__.ptg2_snapshot AS snapshot
  LEFT JOIN __SCHEMA__.ptg2_import_run AS import_run
    ON import_run.import_run_id = snapshot.import_run_id
 ORDER BY snapshot.created_at, snapshot.snapshot_id
"""
_CANDIDATE_LAYOUT_KEYS_SQL = """
SELECT DISTINCT snapshot_key
  FROM __SCHEMA__.ptg2_v3_snapshot_binding
 WHERE snapshot_id = ANY(:snapshot_ids)
 ORDER BY snapshot_key
"""
_DELETE_SNAPSHOT_SCOPE_SQL = """
DELETE FROM __SCHEMA__.ptg2_v3_snapshot_scope
 WHERE snapshot_id = ANY(:snapshot_ids)
"""
_DELETE_SNAPSHOT_BINDING_SQL = """
DELETE FROM __SCHEMA__.ptg2_v3_snapshot_binding
 WHERE snapshot_id = ANY(:snapshot_ids)
"""
_DELETE_ARTIFACT_BLOB_CHUNKS_SQL = """
DELETE FROM __SCHEMA__.ptg2_artifact_blob_chunk
 WHERE artifact_id IN (
    SELECT artifact_id
      FROM __SCHEMA__.ptg2_artifact_manifest
     WHERE snapshot_id = ANY(:snapshot_ids)
 )
"""
_DELETE_ARTIFACT_MANIFEST_SQL = """
DELETE FROM __SCHEMA__.ptg2_artifact_manifest
 WHERE snapshot_id = ANY(:snapshot_ids)
"""
_DELETE_GC_SNAPSHOTS_SQL = """
DELETE FROM __SCHEMA__.ptg2_snapshot
 WHERE snapshot_id = ANY(:snapshot_ids)
   AND status IN ('published', 'failed', 'building')
   AND (
        (
            lower(COALESCE(manifest->'serving_index'->>'arch_version', ''))
                = 'postgres_binary_v3'
            AND lower(COALESCE(manifest->'serving_index'->>'storage_generation', ''))
                = 'shared_blocks_v3'
        )
        OR (
            manifest->'allowed_amount_index'->>'contract'
                = :allowed_amount_contract
            AND lower(COALESCE(
                manifest->'allowed_amount_index'->>'arch_version',
                ''
            )) = 'postgres_binary_v3'
            AND lower(COALESCE(
                manifest->'allowed_amount_index'->>'storage',
                ''
            )) = 'postgresql'
            AND lower(COALESCE(
                manifest->'allowed_amount_index'->>'snapshot_scoped',
                ''
            )) = 'true'
        )
   )
"""


@dataclass(frozen=True)
class PTG2SnapshotGCTable:
    snapshot_id: str
    source_key: str
    table_name: str
    bytes: int


@dataclass(frozen=True)
class _PTG2SnapshotGCCandidate:
    snapshot_id: str
    source_key: str
    table_names: tuple[str, ...]
    is_shared: bool
    reason: str


@dataclass(frozen=True)
class _SnapshotGCPlanContext:
    schema_name: str
    executor: Any
    current_snapshot_ids: tuple[str, ...]
    candidates: tuple[_PTG2SnapshotGCCandidate, ...]
    retained_table_refs: frozenset[str]
    last_candidate_index_by_table: dict[str, int]
    size_by_table: dict[str, int]


@dataclass(frozen=True)
class PTG2SourceSnapshotGCPlan:
    current_snapshot_ids: tuple[str, ...]
    candidate_snapshot_ids: tuple[str, ...]
    tables: tuple[PTG2SnapshotGCTable, ...]
    candidate_reasons: tuple[tuple[str, str], ...] = ()
    shared_snapshot_ids: tuple[str, ...] = ()
    shared_layout_count: int = 0
    shared_candidate_hash_count: int = 0
    shared_stored_bytes: int = 0

    @property
    def total_bytes(self) -> int:
        """Return the aggregate size of tables selected for garbage collection."""
        return sum(table.bytes for table in self.tables) + self.shared_stored_bytes

    @property
    def table_count(self) -> int:
        """Return the number of distinct tables selected for garbage collection."""
        return len({table.table_name for table in self.tables})

    @property
    def has_actions(self) -> bool:
        """Return whether the garbage-collection plan selects snapshots or tables."""
        return bool(
            self.candidate_snapshot_ids
            or self.tables
            or self.shared_layout_count
            or self.shared_candidate_hash_count
        )


def _row_mapping(row: Any) -> dict[str, Any]:
    if row is None:
        return {}
    if isinstance(row, dict):
        return row
    return dict(getattr(row, "_mapping", row))


def _stale_build_seconds(value: int | None) -> int:
    raw_value: Any = (
        value
        if value is not None
        else os.getenv(_STALE_BUILD_SECONDS_ENV, _STALE_BUILD_SECONDS_DEFAULT)
    )
    try:
        return max(int(raw_value), 0)
    except (TypeError, ValueError):
        return _STALE_BUILD_SECONDS_DEFAULT


def _manifest_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _serving_index(row: dict[str, Any]) -> dict[str, Any]:
    serving_index = row.get("serving_index")
    if isinstance(serving_index, dict):
        return serving_index
    manifest = _manifest_dict(row.get("manifest"))
    serving_index = manifest.get("serving_index")
    return serving_index if isinstance(serving_index, dict) else {}


def _is_allowed_amount_snapshot(row: dict[str, Any]) -> bool:
    manifest = _manifest_dict(row.get("manifest"))
    allowed_amount_index = manifest.get("allowed_amount_index")
    return bool(
        isinstance(allowed_amount_index, dict)
        and allowed_amount_index.get("contract")
        == PTG2_ALLOWED_AMOUNT_CONTRACT
        and allowed_amount_index.get("arch_version")
        == "postgres_binary_v3"
        and allowed_amount_index.get("storage") == "postgresql"
        and allowed_amount_index.get("snapshot_scoped") is True
    )


def _protected_snapshot_lineage_ids(
    snapshot_rows: Iterable[Any],
    current_snapshot_ids: tuple[str, ...],
    retain_current_lineage: int,
) -> set[str]:
    """Return current snapshot IDs plus their bounded previous-snapshot lineage."""
    previous_snapshot_by_id = {
        str(_row_mapping(raw_row).get("snapshot_id") or ""): str(
            _row_mapping(raw_row).get("previous_snapshot_id") or ""
        )
        for raw_row in snapshot_rows
    }
    protected_snapshot_ids = set(current_snapshot_ids)
    for current_snapshot_id in current_snapshot_ids:
        lineage_snapshot_id = current_snapshot_id
        for _lineage_depth in range(1, max(int(retain_current_lineage), 1)):
            lineage_snapshot_id = previous_snapshot_by_id.get(lineage_snapshot_id, "")
            if not lineage_snapshot_id or lineage_snapshot_id in protected_snapshot_ids:
                break
            protected_snapshot_ids.add(lineage_snapshot_id)
    return protected_snapshot_ids


def _schema_sql(sql_template: str, schema_name: str) -> str:
    return sql_template.replace("__SCHEMA__", _quote_ident(schema_name))


async def _load_current_snapshot_ids(
    executor: Any,
    schema_name: str,
) -> tuple[str, ...]:
    pointer_records = await executor.all(
        _schema_sql(_CURRENT_SNAPSHOT_IDS_SQL, schema_name)
    )
    return tuple(
        str(_row_mapping(pointer_record).get("snapshot_id"))
        for pointer_record in pointer_records
    )


async def _load_snapshot_gc_rows(
    executor: Any,
    schema_name: str,
    stale_build_seconds: int,
) -> list[Any]:
    return await executor.all(
        _schema_sql(_SNAPSHOT_GC_ROWS_SQL, schema_name),
        stale_build_seconds=stale_build_seconds,
    )


def _classify_snapshot_gc_candidates(
    snapshot_rows: Iterable[Any],
    current_snapshot_ids: tuple[str, ...],
    retain_current_lineage: int,
) -> tuple[list[_PTG2SnapshotGCCandidate], set[str]]:
    retained_table_refs: set[str] = set()
    protected_snapshot_ids = _protected_snapshot_lineage_ids(
        snapshot_rows,
        current_snapshot_ids,
        retain_current_lineage,
    )
    candidates: list[_PTG2SnapshotGCCandidate] = []
    for raw_snapshot_record in snapshot_rows:
        snapshot_by_field = _row_mapping(raw_snapshot_record)
        snapshot_id = str(snapshot_by_field.get("snapshot_id") or "")
        snapshot_status = str(snapshot_by_field.get("status") or "")
        serving_index = _serving_index(snapshot_by_field)
        is_shared_strict_v3 = is_strict_ptg2_v3_shared_blocks_manifest(
            serving_index
        )
        is_allowed_strict_v3 = _is_allowed_amount_snapshot(snapshot_by_field)
        is_strict_v3 = is_shared_strict_v3 or is_allowed_strict_v3
        table_names: tuple[str, ...] = ()
        if snapshot_status in _AGE_GC_PROTECTED_STATUSES:
            continue
        is_terminal_candidate = (
            bool(snapshot_id)
            and is_strict_v3
            and snapshot_status in _GC_CANDIDATE_STATUSES
            and snapshot_id not in protected_snapshot_ids
        )
        is_stale_building_candidate = (
            bool(snapshot_id)
            and is_strict_v3
            and snapshot_status == "building"
            and bool(snapshot_by_field.get("stale_building"))
            and snapshot_id not in protected_snapshot_ids
        )
        if not (is_terminal_candidate or is_stale_building_candidate):
            retained_table_refs.update(table_names)
            continue
        source_key = str(
            snapshot_by_field.get("source_key")
            or serving_index.get("source_key")
            or ""
        )
        candidates.append(
            _PTG2SnapshotGCCandidate(
                snapshot_id=snapshot_id,
                source_key=source_key,
                table_names=table_names,
                is_shared=is_shared_strict_v3,
                reason="stale_building" if is_stale_building_candidate else "terminal",
            )
        )
    return candidates, retained_table_refs


async def _build_snapshot_gc_context(
    *,
    schema_name: str,
    executor: Any,
    current_snapshot_ids: tuple[str, ...],
    candidates: list[_PTG2SnapshotGCCandidate],
    retained_table_refs: set[str],
    snapshot_limit: int,
) -> _SnapshotGCPlanContext:
    last_candidate_index_by_table: dict[str, int] = {}
    for candidate_index, candidate in enumerate(candidates):
        for table_name in candidate.table_names:
            last_candidate_index_by_table[table_name] = candidate_index
    candidate_table_names = sorted(
        {
            table_name
            for candidate in candidates[:snapshot_limit]
            for table_name in candidate.table_names
            if table_name not in retained_table_refs
            and last_candidate_index_by_table[table_name] < snapshot_limit
        }
    )
    size_by_table: dict[str, int] = {}
    if candidate_table_names:
        size_records = await executor.all(
            """
            SELECT c.relname AS table_name,
                   pg_total_relation_size(c.oid) AS bytes
              FROM pg_class c
              JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = :schema_name
               AND c.relkind IN ('r', 'p')
               AND c.relname = ANY(:table_names)
             ORDER BY c.relname
            """,
            schema_name=schema_name,
            table_names=candidate_table_names,
        )
        size_by_table = {
            str(_row_mapping(size_record).get("table_name")): int(
                _row_mapping(size_record).get("bytes") or 0
            )
            for size_record in size_records
        }
    return _SnapshotGCPlanContext(
        schema_name=schema_name,
        executor=executor,
        current_snapshot_ids=current_snapshot_ids,
        candidates=tuple(candidates),
        retained_table_refs=frozenset(retained_table_refs),
        last_candidate_index_by_table=last_candidate_index_by_table,
        size_by_table=size_by_table,
    )


def _selected_gc_tables(
    plan_context: _SnapshotGCPlanContext,
    candidate_count: int,
) -> tuple[PTG2SnapshotGCTable, ...]:
    selected_tables: list[PTG2SnapshotGCTable] = []
    selected_table_names: set[str] = set()
    for candidate in plan_context.candidates[:candidate_count]:
        for table_name in candidate.table_names:
            if (
                table_name in selected_table_names
                or table_name not in plan_context.size_by_table
                or table_name in plan_context.retained_table_refs
                or plan_context.last_candidate_index_by_table[table_name]
                >= candidate_count
            ):
                continue
            selected_table_names.add(table_name)
            selected_tables.append(
                PTG2SnapshotGCTable(
                    snapshot_id=candidate.snapshot_id,
                    source_key=candidate.source_key,
                    table_name=table_name,
                    bytes=plan_context.size_by_table[table_name],
                )
            )
    return tuple(selected_tables)


async def _shared_gc_metrics(
    plan_context: _SnapshotGCPlanContext,
    candidate_count: int,
) -> tuple[tuple[str, ...], int, int, int]:
    shared_snapshot_ids = tuple(
        candidate.snapshot_id
        for candidate in plan_context.candidates[:candidate_count]
        if candidate.is_shared
    )
    if not shared_snapshot_ids:
        return shared_snapshot_ids, 0, 0, 0
    shared_plan = await build_ptg2_shared_layout_release_plan(
        schema_name=plan_context.schema_name,
        executor=plan_context.executor,
        removing_snapshot_ids=shared_snapshot_ids,
        all_eligible_layouts=True,
        require_shared=True,
    )
    return (
        shared_snapshot_ids,
        shared_plan.logical_layout_count,
        shared_plan.candidate_hash_count,
        shared_plan.stored_bytes,
    )


async def _gc_plan_for_candidate_count(
    plan_context: _SnapshotGCPlanContext,
    candidate_count: int,
) -> PTG2SourceSnapshotGCPlan:
    selected_candidates = plan_context.candidates[:candidate_count]
    (
        shared_snapshot_ids,
        shared_layout_count,
        shared_candidate_hash_count,
        shared_stored_bytes,
    ) = await _shared_gc_metrics(plan_context, candidate_count)
    return PTG2SourceSnapshotGCPlan(
        current_snapshot_ids=plan_context.current_snapshot_ids,
        candidate_snapshot_ids=tuple(
            candidate.snapshot_id for candidate in selected_candidates
        ),
        tables=_selected_gc_tables(plan_context, candidate_count),
        candidate_reasons=tuple(
            (candidate.snapshot_id, candidate.reason)
            for candidate in selected_candidates
        ),
        shared_snapshot_ids=shared_snapshot_ids,
        shared_layout_count=shared_layout_count,
        shared_candidate_hash_count=shared_candidate_hash_count,
        shared_stored_bytes=shared_stored_bytes,
    )


async def _select_bounded_gc_plan(
    plan_context: _SnapshotGCPlanContext,
    *,
    snapshot_limit: int,
    table_limit: int | None,
    byte_limit: int | None,
) -> PTG2SourceSnapshotGCPlan:
    if table_limit is None and byte_limit is None:
        return await _gc_plan_for_candidate_count(
            plan_context,
            snapshot_limit,
        )
    selected_plan = await _gc_plan_for_candidate_count(plan_context, 0)
    lower_bound = 1
    upper_bound = snapshot_limit
    while lower_bound <= upper_bound:
        candidate_count = (lower_bound + upper_bound) // 2
        candidate_plan = await _gc_plan_for_candidate_count(
            plan_context,
            candidate_count,
        )
        is_within_table_limit = (
            table_limit is None or candidate_plan.table_count <= table_limit
        )
        is_within_byte_limit = (
            byte_limit is None or candidate_plan.total_bytes <= byte_limit
        )
        if is_within_table_limit and is_within_byte_limit:
            selected_plan = candidate_plan
            lower_bound = candidate_count + 1
        else:
            upper_bound = candidate_count - 1
    return selected_plan


async def build_ptg2_source_snapshot_gc_plan(
    *,
    schema_name: str | None = None,
    executor: Any | None = None,
    retain_current_lineage: int = 4,
    stale_build_seconds: int | None = None,
    max_snapshots: int | None = None,
    max_tables: int | None = None,
    max_bytes: int | None = None,
) -> PTG2SourceSnapshotGCPlan:
    """Build a bounded plan while retaining lineage behind every current pointer."""
    schema_name = resolve_ptg2_schema(schema_name)
    executor = executor or db
    await require_ptg2_v3_migration_owned_tables(executor, schema_name)
    current_snapshot_ids = await _load_current_snapshot_ids(
        executor,
        schema_name,
    )
    snapshot_rows = await _load_snapshot_gc_rows(
        executor,
        schema_name,
        _stale_build_seconds(stale_build_seconds),
    )
    candidates, retained_table_refs = _classify_snapshot_gc_candidates(
        snapshot_rows,
        current_snapshot_ids,
        retain_current_lineage,
    )
    snapshot_limit = (
        len(candidates)
        if max_snapshots is None
        else min(max(int(max_snapshots), 0), len(candidates))
    )
    table_limit = None if max_tables is None else max(int(max_tables), 0)
    byte_limit = None if max_bytes is None else max(int(max_bytes), 0)
    plan_context = await _build_snapshot_gc_context(
        schema_name=schema_name,
        executor=executor,
        current_snapshot_ids=current_snapshot_ids,
        candidates=candidates,
        retained_table_refs=retained_table_refs,
        snapshot_limit=snapshot_limit,
    )
    return await _select_bounded_gc_plan(
        plan_context,
        snapshot_limit=snapshot_limit,
        table_limit=table_limit,
        byte_limit=byte_limit,
    )


def validate_ptg2_source_snapshot_gc_plan(
    plan: PTG2SourceSnapshotGCPlan,
    *,
    max_snapshots: int,
    max_tables: int,
    max_bytes: int,
) -> None:
    """Raise when a source-snapshot cleanup plan exceeds configured safety limits."""
    if len(plan.candidate_snapshot_ids) > max_snapshots:
        raise RuntimeError(
            f"Refusing cleanup: candidate snapshot count {len(plan.candidate_snapshot_ids)} "
            f"exceeds safety bound {max_snapshots}"
        )
    if plan.table_count > max_tables:
        raise RuntimeError(
            f"Refusing cleanup: candidate table count {plan.table_count} exceeds safety bound {max_tables}"
        )
    if plan.total_bytes > max_bytes:
        raise RuntimeError(
            f"Refusing cleanup: candidate bytes {plan.total_bytes} exceeds safety bound {max_bytes}"
        )


async def _lock_ptg2_pointer_state(connection: Any, schema_name: str) -> None:
    await connection.status(
        "SELECT pg_advisory_xact_lock(hashtext(:publish_lock_key))",
        publish_lock_key=PTG2_SOURCE_POINTER_GC_LOCK_KEY,
    )
    await connection.status(
        f"""
        LOCK TABLE {_quote_ident(schema_name)}.ptg2_snapshot
        IN SHARE ROW EXCLUSIVE MODE
        """
    )
    await connection.status(
        f"""
        LOCK TABLE {_quote_ident(schema_name)}.ptg2_current_snapshot,
                   {_quote_ident(schema_name)}.ptg2_current_source_snapshot,
                   {_quote_ident(schema_name)}.ptg2_current_plan_source
        IN SHARE MODE
        """
    )


async def _candidate_layout_keys(
    connection: Any,
    schema_name: str,
    candidate_snapshot_ids: list[str],
) -> tuple[int, ...]:
    layout_records = await connection.all(
        _schema_sql(_CANDIDATE_LAYOUT_KEYS_SQL, schema_name),
        snapshot_ids=candidate_snapshot_ids,
    )
    return tuple(
        int(_row_mapping(layout_record).get("snapshot_key"))
        for layout_record in layout_records
        if _row_mapping(layout_record).get("snapshot_key") is not None
    )


async def _delete_snapshot_gc_metadata(
    connection: Any,
    schema_name: str,
    candidate_snapshot_ids: list[str],
) -> int:
    query_parameters_by_name = {"snapshot_ids": candidate_snapshot_ids}
    await connection.status(
        _schema_sql(_DELETE_SNAPSHOT_SCOPE_SQL, schema_name),
        **query_parameters_by_name,
    )
    deleted_binding_count = await connection.status(
        _schema_sql(_DELETE_SNAPSHOT_BINDING_SQL, schema_name),
        **query_parameters_by_name,
    )
    await connection.status(
        _schema_sql(_DELETE_ARTIFACT_BLOB_CHUNKS_SQL, schema_name),
        **query_parameters_by_name,
    )
    await connection.status(
        _schema_sql(_DELETE_ARTIFACT_MANIFEST_SQL, schema_name),
        **query_parameters_by_name,
    )
    await connection.status(
        _schema_sql(_DELETE_GC_SNAPSHOTS_SQL, schema_name),
        **query_parameters_by_name,
        allowed_amount_contract=PTG2_ALLOWED_AMOUNT_CONTRACT,
    )
    return int(deleted_binding_count or 0)


async def _execute_snapshot_gc_actions(
    connection: Any,
    schema_name: str,
    plan: PTG2SourceSnapshotGCPlan,
) -> None:
    for table_name in sorted({table.table_name for table in plan.tables}):
        await connection.status(
            f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(table_name)}"
        )
    if not plan.candidate_snapshot_ids:
        return
    candidate_snapshot_ids = list(plan.candidate_snapshot_ids)
    candidate_layout_keys = await _candidate_layout_keys(
        connection,
        schema_name,
        candidate_snapshot_ids,
    )
    deleted_binding_count = await _delete_snapshot_gc_metadata(
        connection,
        schema_name,
        candidate_snapshot_ids,
    )
    if deleted_binding_count > 0:
        await release_unbound_ptg2_shared_layouts(
            schema_name=schema_name,
            executor=connection,
            require_shared=True,
            layout_keys=candidate_layout_keys,
        )


async def execute_ptg2_source_snapshot_gc_plan(
    *,
    schema_name: str | None = None,
    max_snapshots: int = 400,
    max_tables: int = 2000,
    max_bytes: int = 80 * 1024 * 1024 * 1024,
    lock_timeout: str = "5s",
    retain_current_lineage: int = 4,
    stale_build_seconds: int | None = None,
) -> PTG2SourceSnapshotGCPlan:
    """Recompute and execute a bounded cleanup plan in one transaction."""
    schema_name = resolve_ptg2_schema(schema_name)
    await ensure_ptg2_artifact_blob_table(schema_name)
    async with db.acquire() as connection:
        await connection.status("SELECT set_config('lock_timeout', :lock_timeout, true)", lock_timeout=lock_timeout)
        await _lock_ptg2_pointer_state(connection, schema_name)
        plan = await build_ptg2_source_snapshot_gc_plan(
            schema_name=schema_name,
            executor=connection,
            retain_current_lineage=retain_current_lineage,
            stale_build_seconds=stale_build_seconds,
            max_snapshots=max_snapshots,
            max_tables=max_tables,
            max_bytes=max_bytes,
        )
        validate_ptg2_source_snapshot_gc_plan(
            plan,
            max_snapshots=max_snapshots,
            max_tables=max_tables,
            max_bytes=max_bytes,
        )
        await _execute_snapshot_gc_actions(
            connection,
            schema_name,
            plan,
        )
        return plan


if __name__ == "__main__":
    from process.ptg_parts.ptg2_source_snapshot_gc_cli import main

    main()
