# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Rust scanner COPY stage helpers for PTG2 imports."""

from __future__ import annotations

import logging
import os
import re
from pathlib import Path

from db.connection import db
from process.ptg_parts.config import (
    PTG2_COMPACT_BULK_DROP_INDEXES_ENV,
    PTG2_FAST_FINAL_REBUILD_ENV,
    PTG2_UNLOGGED_STAGE_ENV,
    _env_bool,
    _uses_ptg2_stage_copy_dedupe,
)
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.snapshot_tables import _ptg2_snapshot_index_name

logger = logging.getLogger(__name__)


_RUST_COPY_TABLE_SPECS = {
    "serving_rate_compact": (
        "ptg2_serving_rate_compact",
        [
            "serving_rate_id",
            "snapshot_id",
            "plan_id",
            "procedure_hash",
            "procedure_code",
            "reported_code_system",
            "reported_code",
            "provider_set_hash",
            "provider_count",
            "price_set_hash",
            "source_trace_set_hash",
            "network_names",
        ],
        ["serving_rate_id"],
    ),
    "procedure": (
        "ptg2_procedure",
        ["procedure_hash", "billing_code_type", "billing_code_type_version", "billing_code", "name", "description"],
        ["procedure_hash"],
    ),
    "price_code_set": (
        "ptg2_price_code_set",
        ["code_set_hash", "codes"],
        ["code_set_hash"],
    ),
    "price_atom": (
        "ptg2_price_atom",
        [
            "price_atom_hash",
            "negotiated_type",
            "negotiated_rate",
            "expiration_date",
            "service_code_set_hash",
            "billing_class",
            "setting",
            "billing_code_modifier_set_hash",
            "additional_information",
        ],
        ["price_atom_hash"],
    ),
    "price_set_entry": (
        "ptg2_price_set_entry",
        ["price_set_hash", "price_atom_hash"],
        ["price_set_hash", "price_atom_hash"],
    ),
    "provider_set": (
        "ptg2_provider_set",
        ["provider_set_hash", "provider_count"],
        ["provider_set_hash"],
    ),
    "provider_group_member": (
        "ptg2_provider_group_member",
        ["provider_group_hash", "npi"],
        ["provider_group_hash", "npi"],
    ),
    "provider_set_component": (
        "ptg2_provider_set_component",
        ["provider_set_hash", "provider_group_hash"],
        ["provider_set_hash", "provider_group_hash"],
    ),
}


_PTG2_PRICE_ATOM_COMPACT_NULL_COLUMNS = {
    "canonical_payload",
}


_PTG2_PRICE_SET_COMPACT_NULL_COLUMNS = {
    "price_atom_hashes",
    "canonical_payload",
}


_PTG2_PROVIDER_SET_COMPACT_NULL_COLUMNS = {
    "npi",
    "provider_group_hashes",
    "tin_type",
    "tin_value",
    "canonical_payload",
}


def _ptg2_dictionary_select_columns(kind: str, columns: list[str]) -> str:
    select_expressions: list[str] = []
    for column in columns:
        quoted = _quote_ident(column)
        if kind == "price_atom" and column in _PTG2_PRICE_ATOM_COMPACT_NULL_COLUMNS:
            select_expressions.append(f"NULL AS {quoted}")
        elif kind == "price_set" and column in _PTG2_PRICE_SET_COMPACT_NULL_COLUMNS:
            select_expressions.append(f"NULL AS {quoted}")
        elif kind == "provider_set" and column in _PTG2_PROVIDER_SET_COMPACT_NULL_COLUMNS:
            select_expressions.append(f"NULL AS {quoted}")
        else:
            select_expressions.append(quoted)
    return ", ".join(select_expressions)


PTG2_SERVING_STAGE_LANE_PREFIX = "serving_rate_compact_lane_"


def _rust_copy_stage_table_name(kind: str, token: str) -> str:
    safe_kind = re.sub(r"[^a-z0-9_]+", "_", kind.lower()).strip("_")
    safe_token = re.sub(r"[^a-z0-9_]+", "_", token.lower()).strip("_")
    return f"ptg2_rust_stage_{safe_kind}_{safe_token}"[:63]


def _serving_stage_lane_key(lane: int) -> str:
    return f"{PTG2_SERVING_STAGE_LANE_PREFIX}{lane:04d}"


def _serving_stage_tables(stage_tables: dict[str, str]) -> list[str]:
    tables: list[str] = []
    base_table = stage_tables.get("serving_rate_compact")
    if base_table:
        tables.append(base_table)
    for key in sorted(stage_tables):
        if key.startswith(PTG2_SERVING_STAGE_LANE_PREFIX):
            table = stage_tables.get(key)
            if table:
                tables.append(table)
    return tables


def _serving_stage_table_for_copy(stage_tables: dict[str, str], copy_file: Path) -> str:
    match = re.search(r"\.worker(\d+)(?:\.|$)", copy_file.name)
    if match:
        lane_table = stage_tables.get(_serving_stage_lane_key(int(match.group(1))))
        if lane_table:
            return lane_table
    return stage_tables.get("serving_rate_compact", "ptg2_serving_rate_compact")


async def _create_one_rust_copy_stage_table(
    *,
    kind: str,
    schema_name: str,
    storage_mode: str,
    stage_table: str,
    target_table: str,
    columns: list[str],
    conflict_targets: list[str] | None = None,
) -> None:
    await db.status(f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(stage_table)};")
    await db.status(
        f"""
        CREATE {storage_mode}TABLE {_quote_ident(schema_name)}.{_quote_ident(stage_table)}
        (LIKE {_quote_ident(schema_name)}.{_quote_ident(target_table)} INCLUDING DEFAULTS);
        """
    )
    existing_columns = await db.all(
        """
        SELECT column_name
          FROM information_schema.columns
         WHERE table_schema = :schema_name
           AND table_name = :stage_table
        """,
        schema_name=schema_name,
        stage_table=stage_table,
    )
    is_serving_stage = kind == "serving_rate_compact" or kind.startswith(
        PTG2_SERVING_STAGE_LANE_PREFIX
    )
    keep_columns = None if is_serving_stage else set(columns) | {"created_at"}
    for column_row in existing_columns:
        column_name = column_row.get("column_name") if isinstance(column_row, dict) else getattr(column_row, "column_name", None)
        if keep_columns is not None and column_name and column_name not in keep_columns:
            await db.status(
                f"ALTER TABLE {_quote_ident(schema_name)}.{_quote_ident(stage_table)} "
                f"DROP COLUMN IF EXISTS {_quote_ident(column_name)};"
            )
    if _env_bool(PTG2_UNLOGGED_STAGE_ENV, True):
        try:
            await db.status(f"ALTER TABLE {_quote_ident(schema_name)}.{_quote_ident(stage_table)} SET UNLOGGED;")
        except Exception as exc:
            logger.debug("Skipping PTG2 Rust stage unlogged ensure for %s: %s", stage_table, exc)
    try:
        await db.status(
            f"ALTER TABLE {_quote_ident(schema_name)}.{_quote_ident(stage_table)} "
            "SET (autovacuum_enabled = false, toast.autovacuum_enabled = false);"
        )
    except Exception as exc:
        logger.debug("Skipping PTG2 Rust stage autovacuum disable for %s: %s", stage_table, exc)
    if conflict_targets and _uses_ptg2_stage_copy_dedupe(kind):
        dedupe_index_name = _ptg2_snapshot_index_name(stage_table, "copy_dedupe_idx")
        await db.status(
            f"""
            CREATE UNIQUE INDEX IF NOT EXISTS {_quote_ident(dedupe_index_name)}
            ON {_quote_ident(schema_name)}.{_quote_ident(stage_table)}
            ({", ".join(_quote_ident(column) for column in conflict_targets)});
            """
        )


async def _create_rust_copy_stage_tables(token: str, *, serving_lanes: int = 1) -> dict[str, str]:
    schema_name = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    storage_mode = "UNLOGGED " if _env_bool(PTG2_UNLOGGED_STAGE_ENV, True) else ""
    stage_table_by_kind: dict[str, str] = {}
    for kind, (target_table, columns, conflict_targets) in _RUST_COPY_TABLE_SPECS.items():
        stage_table = _rust_copy_stage_table_name(kind, token)
        stage_table_by_kind[kind] = stage_table
        await _create_one_rust_copy_stage_table(
            kind=kind,
            schema_name=schema_name,
            storage_mode=storage_mode,
            stage_table=stage_table,
            target_table=target_table,
            columns=columns,
            conflict_targets=conflict_targets if kind != "serving_rate_compact" else None,
        )
        if kind == "serving_rate_compact":
            for lane in range(1, max(serving_lanes, 1)):
                lane_key = _serving_stage_lane_key(lane)
                lane_table = _rust_copy_stage_table_name(f"serving_rate_compact_w{lane:04d}", token)
                stage_table_by_kind[lane_key] = lane_table
                await _create_one_rust_copy_stage_table(
                    kind=lane_key,
                    schema_name=schema_name,
                    storage_mode=storage_mode,
                    stage_table=lane_table,
                    target_table=target_table,
                    columns=columns,
                    conflict_targets=None,
                )
    return stage_table_by_kind


async def _merge_rust_copy_stage_tables(stage_tables: dict[str, str], *, drop: bool = True) -> None:
    schema_name = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    merge_order = (
        "procedure",
        "price_code_set",
        "price_atom",
        "price_set_entry",
        "provider_entry_component",
        "provider_group_member",
        "provider_set",
        "provider_set_entry",
        "serving_rate_compact",
    )
    fast_rebuild = _env_bool(PTG2_FAST_FINAL_REBUILD_ENV, False)
    for kind in merge_order:
        stage_table_names = (
            _serving_stage_tables(stage_tables)
            if kind == "serving_rate_compact"
            else [stage_tables[kind]] if stage_tables.get(kind) else []
        )
        if not stage_table_names:
            continue
        target_table, columns, conflict_targets = _RUST_COPY_TABLE_SPECS[kind]
        if fast_rebuild and kind != "procedure" and len(stage_table_names) == 1:
            stage_table = stage_table_names[0]
            await db.status(f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(target_table)};")
            await db.status(
                f"""
                ALTER TABLE {_quote_ident(schema_name)}.{_quote_ident(stage_table)}
                RENAME TO {_quote_ident(target_table)};
                """
            )
            continue
        quoted_columns = ", ".join(_quote_ident(column) for column in columns)
        select_columns = _ptg2_dictionary_select_columns(kind, columns)
        quoted_conflict = ", ".join(_quote_ident(column) for column in conflict_targets)
        conflict_sql = ""
        if not fast_rebuild and not (
            kind == "serving_rate_compact" and _env_bool(PTG2_COMPACT_BULK_DROP_INDEXES_ENV, True)
        ):
            conflict_sql = f"ON CONFLICT ({quoted_conflict}) DO NOTHING"
        for stage_table in stage_table_names:
            await db.status(
                f"""
                INSERT INTO {_quote_ident(schema_name)}.{_quote_ident(target_table)} ({quoted_columns})
                SELECT {select_columns}
                FROM {_quote_ident(schema_name)}.{_quote_ident(stage_table)}
                {conflict_sql};
                """
            )
            if drop:
                await db.status(f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(stage_table)};")
