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

PTG2_MANIFEST_STORAGE = "manifest_snapshot"
PTG2_LEGACY_SNAPSHOT_TABLE_RE = re.compile(
    r"^ptg2_(?:"
    r"serving_rate(?:_compact)?|"
    r"procedure|price_code_set|price_atom|price_set|price_set_entry|"
    r"provider_set|provider_set_component|provider_set_entry|"
    r"provider_entry_component|provider_group_member|provider_group_location|"
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


def _table_name(value: Any) -> str | None:
    if not value:
        return None
    text_value = str(value).strip()
    if not text_value:
        return None
    return text_value.rsplit(".", 1)[-1]


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
    table_names = {
        _table_name(payload.get("table")),
        _table_name(payload.get("price_atom_table")),
        _table_name(payload.get("provider_group_member_table")),
        _table_name(payload.get("provider_group_rate_scope_table")),
        _table_name(payload.get("code_count_table")),
    }
    return {table_name for table_name in table_names if table_name}


async def build_ptg2_manifest_cleanup_plan(*, schema_name: str | None = None) -> PTG2ManifestCleanupPlan:
    schema_name = schema_name or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    manifest_snapshot_rows = await db.all(
        f"""
        SELECT manifest
          FROM {_quote_ident(schema_name)}.ptg2_snapshot
         WHERE COALESCE(manifest->'serving_index'->>'storage', '') = :manifest_storage
        """,
        manifest_storage=PTG2_MANIFEST_STORAGE,
    )
    referenced_tables: set[str] = set()
    for row in manifest_snapshot_rows:
        referenced_tables.update(_manifest_referenced_tables(_row_value(row, "manifest")))
    table_rows = await db.all(
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
    tables = tuple(
        str(_row_value(row, "table_name"))
        for row in table_rows
        if PTG2_LEGACY_SNAPSHOT_TABLE_RE.match(str(_row_value(row, "table_name") or ""))
        and str(_row_value(row, "table_name") or "") not in referenced_tables
    )
    snapshot_rows = await db.all(
        f"""
        SELECT snapshot_id
          FROM {_quote_ident(schema_name)}.ptg2_snapshot
         WHERE COALESCE(manifest->'serving_index'->>'storage', '') <> :manifest_storage
         ORDER BY snapshot_id
        """,
        manifest_storage=PTG2_MANIFEST_STORAGE,
    )
    snapshot_ids = tuple(str(_row_value(row, "snapshot_id")) for row in snapshot_rows)
    artifact_rows = await db.all(
        f"""
        SELECT artifact_id
         FROM {_quote_ident(schema_name)}.ptg2_artifact_manifest
         WHERE snapshot_id = ANY(:snapshot_ids)
         ORDER BY artifact_id
        """,
        snapshot_ids=list(snapshot_ids),
    ) if snapshot_ids else []
    current_source_rows = await db.all(
        f"""
        SELECT source_key
          FROM {_quote_ident(schema_name)}.ptg2_current_source_snapshot
         WHERE snapshot_id = ANY(:snapshot_ids)
         ORDER BY source_key
        """,
        snapshot_ids=list(snapshot_ids),
    ) if snapshot_ids else []
    current_snapshot_rows = await db.all(
        f"""
        SELECT slot
          FROM {_quote_ident(schema_name)}.ptg2_current_snapshot
         WHERE snapshot_id = ANY(:snapshot_ids)
         ORDER BY slot
        """,
        snapshot_ids=list(snapshot_ids),
    ) if snapshot_ids else []
    current_plan_rows = await db.all(
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


async def execute_ptg2_manifest_cleanup_plan(plan: PTG2ManifestCleanupPlan, *, schema_name: str | None = None) -> None:
    schema_name = schema_name or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    for table_name in plan.tables:
        await db.status(f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(table_name)} CASCADE;")
    if plan.current_snapshot_slots:
        await db.status(
            f"DELETE FROM {_quote_ident(schema_name)}.ptg2_current_snapshot WHERE slot = ANY(:slots)",
            slots=list(plan.current_snapshot_slots),
        )
    if plan.current_plan_rows:
        await db.status(
            f"DELETE FROM {_quote_ident(schema_name)}.ptg2_current_plan_source WHERE plan_source_key = ANY(:keys)",
            keys=list(plan.current_plan_rows),
        )
    if plan.current_source_rows:
        await db.status(
            f"DELETE FROM {_quote_ident(schema_name)}.ptg2_current_source_snapshot WHERE source_key = ANY(:keys)",
            keys=list(plan.current_source_rows),
        )
    if plan.artifact_manifest_ids:
        await db.status(
            f"DELETE FROM {_quote_ident(schema_name)}.ptg2_artifact_manifest WHERE artifact_id = ANY(:ids)",
            ids=list(plan.artifact_manifest_ids),
        )
    if plan.snapshot_ids:
        await db.status(
            f"DELETE FROM {_quote_ident(schema_name)}.ptg2_snapshot WHERE snapshot_id = ANY(:snapshot_ids)",
            snapshot_ids=list(plan.snapshot_ids),
        )


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
    args = parser.parse_args()
    plan = await build_ptg2_manifest_cleanup_plan(schema_name=args.schema)
    _print_plan(plan)
    if args.execute:
        await execute_ptg2_manifest_cleanup_plan(plan, schema_name=args.schema)
        print("cleanup_executed=true")
    else:
        print("cleanup_executed=false")


if __name__ == "__main__":
    asyncio.run(_amain())
