# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Garbage collect non-current PTG2 source snapshot tables.

This helper is intentionally narrower than the legacy PTG2 cleanup: it only
targets published manifest-backed source snapshots that are not referenced by
any current pointer table.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from dataclasses import dataclass
from typing import Any, Iterable

from db.connection import db
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_artifact_blobs import ensure_ptg2_artifact_blob_table
from process.ptg_parts.snapshot_cleanup import _snapshot_manifest_table_names


@dataclass(frozen=True)
class PTG2SnapshotGCTable:
    snapshot_id: str
    source_key: str
    table_name: str
    bytes: int


@dataclass(frozen=True)
class PTG2SourceSnapshotGCPlan:
    current_snapshot_ids: tuple[str, ...]
    candidate_snapshot_ids: tuple[str, ...]
    tables: tuple[PTG2SnapshotGCTable, ...]

    @property
    def total_bytes(self) -> int:
        return sum(table.bytes for table in self.tables)

    @property
    def table_count(self) -> int:
        return len({table.table_name for table in self.tables})

    @property
    def has_actions(self) -> bool:
        return bool(self.candidate_snapshot_ids or self.tables)


def _row_mapping(row: Any) -> dict[str, Any]:
    if row is None:
        return {}
    if isinstance(row, dict):
        return row
    return dict(getattr(row, "_mapping", row))


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


async def build_ptg2_source_snapshot_gc_plan(
    *,
    schema_name: str | None = None,
    executor: Any | None = None,
) -> PTG2SourceSnapshotGCPlan:
    """Build a plan for published, manifest-backed snapshots no current pointer uses."""

    schema_name = schema_name or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    executor = executor or db
    current_rows = await executor.all(
        f"""
        SELECT DISTINCT snapshot_id
          FROM (
                SELECT snapshot_id FROM {_quote_ident(schema_name)}.ptg2_current_snapshot WHERE snapshot_id IS NOT NULL
                UNION ALL
                SELECT snapshot_id FROM {_quote_ident(schema_name)}.ptg2_current_source_snapshot WHERE snapshot_id IS NOT NULL
                UNION ALL
                SELECT snapshot_id FROM {_quote_ident(schema_name)}.ptg2_current_plan_source WHERE snapshot_id IS NOT NULL
          ) refs
         ORDER BY snapshot_id
        """
    )
    current_snapshot_ids = tuple(str(_row_mapping(row).get("snapshot_id")) for row in current_rows)
    current_snapshot_set = set(current_snapshot_ids)
    snapshot_rows = await executor.all(
        f"""
        SELECT snapshot_id,
               manifest,
               manifest->'serving_index' AS serving_index,
               manifest->'serving_index'->>'source_key' AS source_key
          FROM {_quote_ident(schema_name)}.ptg2_snapshot
         WHERE status = 'published'
           AND COALESCE(manifest->'serving_index'->>'storage', '') = 'manifest_snapshot'
         ORDER BY snapshot_id
        """
    )
    current_table_refs: set[str] = set()
    candidate_table_rows: list[tuple[str, str, str]] = []
    candidate_snapshot_ids: list[str] = []
    for raw_row in snapshot_rows:
        row = _row_mapping(raw_row)
        snapshot_id = str(row.get("snapshot_id") or "")
        serving_index = _serving_index(row)
        table_names = _snapshot_manifest_table_names(serving_index)
        if snapshot_id in current_snapshot_set:
            current_table_refs.update(table_names)
            continue
        candidate_snapshot_ids.append(snapshot_id)
        source_key = str(row.get("source_key") or serving_index.get("source_key") or "")
        candidate_table_rows.extend((snapshot_id, source_key, table_name) for table_name in table_names)
    candidate_table_names = sorted({table_name for _, _, table_name in candidate_table_rows} - current_table_refs)
    size_by_table: dict[str, int] = {}
    if candidate_table_names:
        size_rows = await executor.all(
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
            str(_row_mapping(row).get("table_name")): int(_row_mapping(row).get("bytes") or 0)
            for row in size_rows
        }
    tables = tuple(
        PTG2SnapshotGCTable(
            snapshot_id=snapshot_id,
            source_key=source_key,
            table_name=table_name,
            bytes=size_by_table[table_name],
        )
        for snapshot_id, source_key, table_name in candidate_table_rows
        if table_name in size_by_table and table_name not in current_table_refs
    )
    return PTG2SourceSnapshotGCPlan(
        current_snapshot_ids=current_snapshot_ids,
        candidate_snapshot_ids=tuple(dict.fromkeys(candidate_snapshot_ids)),
        tables=tables,
    )


def validate_ptg2_source_snapshot_gc_plan(
    plan: PTG2SourceSnapshotGCPlan,
    *,
    max_snapshots: int,
    max_tables: int,
    max_bytes: int,
) -> None:
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


async def execute_ptg2_source_snapshot_gc_plan(
    *,
    schema_name: str | None = None,
    max_snapshots: int = 400,
    max_tables: int = 2000,
    max_bytes: int = 80 * 1024 * 1024 * 1024,
    lock_timeout: str = "5s",
) -> PTG2SourceSnapshotGCPlan:
    """Recompute and execute a bounded cleanup plan in one transaction."""

    schema_name = schema_name or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    await ensure_ptg2_artifact_blob_table(schema_name)
    async with db.acquire() as connection:
        await connection.status("SELECT set_config('lock_timeout', :lock_timeout, true)", lock_timeout=lock_timeout)
        plan = await build_ptg2_source_snapshot_gc_plan(schema_name=schema_name, executor=connection)
        validate_ptg2_source_snapshot_gc_plan(
            plan,
            max_snapshots=max_snapshots,
            max_tables=max_tables,
            max_bytes=max_bytes,
        )
        for table_name in sorted({table.table_name for table in plan.tables}):
            await connection.status(
                f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(table_name)}"
            )
        if plan.candidate_snapshot_ids:
            await connection.status(
                f"""
                DELETE FROM {_quote_ident(schema_name)}.ptg2_artifact_blob_chunk
                 WHERE artifact_id IN (
                    SELECT artifact_id
                      FROM {_quote_ident(schema_name)}.ptg2_artifact_manifest
                     WHERE snapshot_id = ANY(:snapshot_ids)
                 )
                """,
                snapshot_ids=list(plan.candidate_snapshot_ids),
            )
            await connection.status(
                f"""
                DELETE FROM {_quote_ident(schema_name)}.ptg2_artifact_manifest
                 WHERE snapshot_id = ANY(:snapshot_ids)
                """,
                snapshot_ids=list(plan.candidate_snapshot_ids),
            )
            await connection.status(
                f"""
                DELETE FROM {_quote_ident(schema_name)}.ptg2_snapshot
                 WHERE snapshot_id = ANY(:snapshot_ids)
                """,
                snapshot_ids=list(plan.candidate_snapshot_ids),
            )
        return plan


def _bytes_to_gib(value: int) -> float:
    return value / (1024 ** 3)


def _print_plan(plan: PTG2SourceSnapshotGCPlan) -> None:
    print(f"current_snapshot_refs={len(plan.current_snapshot_ids)}")
    print(f"candidate_snapshots={len(plan.candidate_snapshot_ids)}")
    print(f"candidate_tables={plan.table_count}")
    print(f"candidate_bytes={plan.total_bytes}")
    print(f"candidate_gib={_bytes_to_gib(plan.total_bytes):.2f}")
    families: dict[str, tuple[int, int]] = {}
    for table in plan.tables:
        family = table.table_name.rsplit("_", 1)[0] + "_*"
        count, bytes_value = families.get(family, (0, 0))
        families[family] = (count + 1, bytes_value + table.bytes)
    for family, (count, bytes_value) in sorted(families.items(), key=lambda item: item[1][1], reverse=True):
        print(f"family={family} tables={count} bytes={bytes_value} gib={_bytes_to_gib(bytes_value):.2f}")


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


async def _amain(argv: Iterable[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Clean non-current PTG2 manifest source snapshots.")
    parser.add_argument("--schema", default=os.getenv("HLTHPRT_DB_SCHEMA") or "mrf")
    parser.add_argument("--execute", action="store_true", help="Apply cleanup. Default is dry-run.")
    parser.add_argument("--max-snapshots", type=_positive_int, default=400)
    parser.add_argument("--max-tables", type=_positive_int, default=2000)
    parser.add_argument("--max-bytes-gb", type=_positive_int, default=80)
    parser.add_argument("--lock-timeout", default="5s")
    args = parser.parse_args(list(argv) if argv is not None else None)
    max_bytes = args.max_bytes_gb * 1024 * 1024 * 1024
    if args.execute:
        plan = await execute_ptg2_source_snapshot_gc_plan(
            schema_name=args.schema,
            max_snapshots=args.max_snapshots,
            max_tables=args.max_tables,
            max_bytes=max_bytes,
            lock_timeout=args.lock_timeout,
        )
        _print_plan(plan)
        print("cleanup_executed=true")
    else:
        plan = await build_ptg2_source_snapshot_gc_plan(schema_name=args.schema)
        validate_ptg2_source_snapshot_gc_plan(
            plan,
            max_snapshots=args.max_snapshots,
            max_tables=args.max_tables,
            max_bytes=max_bytes,
        )
        _print_plan(plan)
        print("cleanup_executed=false")


if __name__ == "__main__":
    asyncio.run(_amain())
