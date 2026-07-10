# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Local PTG2 legacy cleanup helpers.

The cleanup intentionally targets snapshot-scoped legacy PTG tables and metadata.
It does not remove raw payer downloads.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
from dataclasses import dataclass
from typing import Any

from db.connection import db
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_artifact_blobs import ensure_ptg2_artifact_blob_table
from process.ptg_parts.snapshot_cleanup import _snapshot_manifest_table_names
from process.ptg_parts.source_pointers import PTG2_SOURCE_POINTER_GC_LOCK_KEY

PTG2_MANIFEST_STORAGE = "manifest_snapshot"
PTG2_LEGACY_SNAPSHOT_TABLE_RE = re.compile(
    r"^ptg2_(?:"
    r"serving(?:_binary|_rate(?:_compact)?)?|code_count|"
    r"procedure|price_code_set|price_atom(?:_dict)?|price_set(?:_atom|_entry)?|"
    r"provider_set(?:_dict)?|provider_set_component|provider_set_entry|"
    r"provider_entry_component|provider_group_member|provider_npi_scope|provider_group_location|"
    r"provider_group_rate_scope"
    r")_[0-9a-f]{12,16}(?:_p[0-9]{2})?$"
)


@dataclass(frozen=True)
class PTG2ManifestCleanupPlan:
    tables: tuple[str, ...]
    snapshot_ids: tuple[str, ...]
    artifact_manifest_ids: tuple[str, ...]
    current_snapshot_slots: tuple[str, ...]
    current_source_rows: tuple[str, ...]
    current_plan_rows: tuple[str, ...]

    @property
    def has_actions(self) -> bool:
        return any(
            (
                self.tables,
                self.snapshot_ids,
                self.artifact_manifest_ids,
                self.current_snapshot_slots,
                self.current_source_rows,
                self.current_plan_rows,
            )
        )


def _row_value(row: Any, key: str, default: Any = None) -> Any:
    return row.get(key, default) if isinstance(row, dict) else getattr(row, key, default)


def _manifest_referenced_tables(manifest: Any) -> set[str]:
    if isinstance(manifest, str):
        try:
            manifest = json.loads(manifest)
        except json.JSONDecodeError:
            return set()
    if not isinstance(manifest, dict):
        return set()
    serving_index = manifest.get("serving_index")
    if isinstance(serving_index, dict):
        payload = serving_index
    else:
        payload = manifest
    return set(_snapshot_manifest_table_names(payload))


async def build_ptg2_manifest_cleanup_plan(
    *,
    schema_name: str | None = None,
    executor: Any | None = None,
) -> PTG2ManifestCleanupPlan:
    schema_name = schema_name or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    executor = executor or db
    manifest_snapshot_rows = await executor.all(
        f"""
        SELECT snapshot_id, status, manifest
          FROM {_quote_ident(schema_name)}.ptg2_snapshot
         WHERE status = 'building'
            OR COALESCE(manifest->'serving_index'->>'storage', '') = :manifest_storage
        """,
        manifest_storage=PTG2_MANIFEST_STORAGE,
    )
    has_building_snapshot = any(
        str(_row_value(row, "status") or "") == "building" for row in manifest_snapshot_rows
    )
    referenced_tables: set[str] = set()
    for row in manifest_snapshot_rows:
        referenced_tables.update(_manifest_referenced_tables(_row_value(row, "manifest")))
    table_rows = await executor.all(
        """
        SELECT c.relname AS table_name
          FROM pg_class c
          JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = :schema_name
           AND c.relkind IN ('r', 'p')
           AND c.relname LIKE 'ptg2_%'
         ORDER BY c.relname
        """,
        schema_name=schema_name,
    )
    tables = () if has_building_snapshot else tuple(
        str(_row_value(row, "table_name"))
        for row in table_rows
        if PTG2_LEGACY_SNAPSHOT_TABLE_RE.match(str(_row_value(row, "table_name") or ""))
        and str(_row_value(row, "table_name") or "") not in referenced_tables
    )
    snapshot_rows = await executor.all(
        f"""
        SELECT snapshot_id
          FROM {_quote_ident(schema_name)}.ptg2_snapshot
         WHERE status IN ('published', 'failed')
           AND COALESCE(manifest->'serving_index'->>'storage', '') <> :manifest_storage
         ORDER BY snapshot_id
        """,
        manifest_storage=PTG2_MANIFEST_STORAGE,
    )
    snapshot_ids = tuple(str(_row_value(row, "snapshot_id")) for row in snapshot_rows)
    artifact_rows = await executor.all(
        f"""
        SELECT artifact_id
         FROM {_quote_ident(schema_name)}.ptg2_artifact_manifest
         WHERE snapshot_id = ANY(:snapshot_ids)
         ORDER BY artifact_id
        """,
        snapshot_ids=list(snapshot_ids),
    ) if snapshot_ids else []
    current_source_rows = await executor.all(
        f"""
        SELECT source_key
          FROM {_quote_ident(schema_name)}.ptg2_current_source_snapshot
         WHERE snapshot_id = ANY(:snapshot_ids)
         ORDER BY source_key
        """,
        snapshot_ids=list(snapshot_ids),
    ) if snapshot_ids else []
    current_snapshot_rows = await executor.all(
        f"""
        SELECT slot
          FROM {_quote_ident(schema_name)}.ptg2_current_snapshot
         WHERE snapshot_id = ANY(:snapshot_ids)
         ORDER BY slot
        """,
        snapshot_ids=list(snapshot_ids),
    ) if snapshot_ids else []
    current_plan_rows = await executor.all(
        f"""
        SELECT plan_source_key
          FROM {_quote_ident(schema_name)}.ptg2_current_plan_source
         WHERE snapshot_id = ANY(:snapshot_ids)
         ORDER BY plan_source_key
        """,
        snapshot_ids=list(snapshot_ids),
    ) if snapshot_ids else []
    return PTG2ManifestCleanupPlan(
        tables=tables,
        snapshot_ids=snapshot_ids,
        artifact_manifest_ids=tuple(str(_row_value(row, "artifact_id")) for row in artifact_rows),
        current_snapshot_slots=tuple(str(_row_value(row, "slot")) for row in current_snapshot_rows),
        current_source_rows=tuple(str(_row_value(row, "source_key")) for row in current_source_rows),
        current_plan_rows=tuple(str(_row_value(row, "plan_source_key")) for row in current_plan_rows),
    )


async def _lock_ptg2_manifest_cleanup_state(connection: Any, schema_name: str) -> None:
    await connection.status(
        "SELECT pg_advisory_xact_lock(hashtext(:publish_lock_key))",
        publish_lock_key=PTG2_SOURCE_POINTER_GC_LOCK_KEY,
    )
    await connection.status(
        f"""
        LOCK TABLE {_quote_ident(schema_name)}.ptg2_snapshot,
                   {_quote_ident(schema_name)}.ptg2_current_snapshot,
                   {_quote_ident(schema_name)}.ptg2_current_source_snapshot,
                   {_quote_ident(schema_name)}.ptg2_current_plan_source
        IN SHARE ROW EXCLUSIVE MODE
        """
    )


async def _execute_ptg2_manifest_cleanup_plan(
    connection: Any,
    plan: PTG2ManifestCleanupPlan,
    schema_name: str,
) -> None:
    for table_name in plan.tables:
        await connection.status(
            f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(table_name)} CASCADE;"
        )
    if plan.current_snapshot_slots:
        await connection.status(
            f"DELETE FROM {_quote_ident(schema_name)}.ptg2_current_snapshot WHERE slot = ANY(:slots)",
            slots=list(plan.current_snapshot_slots),
        )
    if plan.current_plan_rows:
        await connection.status(
            f"DELETE FROM {_quote_ident(schema_name)}.ptg2_current_plan_source WHERE plan_source_key = ANY(:keys)",
            keys=list(plan.current_plan_rows),
        )
    if plan.current_source_rows:
        await connection.status(
            f"DELETE FROM {_quote_ident(schema_name)}.ptg2_current_source_snapshot WHERE source_key = ANY(:keys)",
            keys=list(plan.current_source_rows),
        )
    if plan.artifact_manifest_ids:
        await connection.status(
            f"DELETE FROM {_quote_ident(schema_name)}.ptg2_artifact_blob_chunk WHERE artifact_id = ANY(:ids)",
            ids=list(plan.artifact_manifest_ids),
        )
        await connection.status(
            f"DELETE FROM {_quote_ident(schema_name)}.ptg2_artifact_manifest WHERE artifact_id = ANY(:ids)",
            ids=list(plan.artifact_manifest_ids),
        )
    if plan.snapshot_ids:
        await connection.status(
            f"""
            DELETE FROM {_quote_ident(schema_name)}.ptg2_snapshot
             WHERE snapshot_id = ANY(:snapshot_ids)
               AND status IN ('published', 'failed')
               AND COALESCE(manifest->'serving_index'->>'storage', '') <> :manifest_storage
            """,
            snapshot_ids=list(plan.snapshot_ids),
            manifest_storage=PTG2_MANIFEST_STORAGE,
        )


async def execute_ptg2_manifest_cleanup_plan(
    plan: PTG2ManifestCleanupPlan,
    *,
    schema_name: str | None = None,
    lock_timeout: str = "5s",
) -> PTG2ManifestCleanupPlan:
    """Lock publication state, rebuild the plan, and execute it atomically."""

    schema_name = schema_name or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    await ensure_ptg2_artifact_blob_table(schema_name)
    async with db.acquire() as connection:
        await connection.status("SELECT set_config('lock_timeout', :lock_timeout, true)", lock_timeout=lock_timeout)
        await _lock_ptg2_manifest_cleanup_state(connection, schema_name)
        plan = await build_ptg2_manifest_cleanup_plan(
            schema_name=schema_name,
            executor=connection,
        )
        await _execute_ptg2_manifest_cleanup_plan(connection, plan, schema_name)
        return plan


def _print_plan(plan: PTG2ManifestCleanupPlan) -> None:
    print(f"legacy_tables={len(plan.tables)}")
    for value in plan.tables:
        print(f"  drop_table={value}")
    print(f"legacy_snapshots={len(plan.snapshot_ids)}")
    for value in plan.snapshot_ids:
        print(f"  delete_snapshot={value}")
    print(f"artifact_manifest_rows={len(plan.artifact_manifest_ids)}")
    print(f"current_snapshot_rows={len(plan.current_snapshot_slots)}")
    print(f"current_source_rows={len(plan.current_source_rows)}")
    print(f"current_plan_rows={len(plan.current_plan_rows)}")


async def _amain() -> None:
    parser = argparse.ArgumentParser(description="Clean local legacy PTG2 data after the manifest-backed serving cutover.")
    parser.add_argument("--schema", default=os.getenv("HLTHPRT_DB_SCHEMA") or "mrf")
    parser.add_argument("--execute", action="store_true", help="Apply the cleanup. Default is dry-run.")
    parser.add_argument("--lock-timeout", default="5s")
    args = parser.parse_args()
    plan = await build_ptg2_manifest_cleanup_plan(schema_name=args.schema)
    if args.execute:
        plan = await execute_ptg2_manifest_cleanup_plan(
            plan,
            schema_name=args.schema,
            lock_timeout=args.lock_timeout,
        )
        _print_plan(plan)
        print("cleanup_executed=true")
    else:
        _print_plan(plan)
        print("cleanup_executed=false")


if __name__ == "__main__":
    asyncio.run(_amain())
