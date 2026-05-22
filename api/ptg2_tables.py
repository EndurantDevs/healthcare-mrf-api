# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PTG2 serving table discovery and validation helpers."""

from __future__ import annotations

import json
import os
import re
from typing import Any

from sqlalchemy import text

from api.ptg2_types import PTG2ServingTables

PTG2_SCHEMA = os.getenv("HLTHPRT_DB_SCHEMA", "mrf")
PTG2_SERVING_TABLE_ENV = "HLTHPRT_PTG2_SERVING_TABLE"


async def _serving_table_available(session, table_name: str) -> bool:
    try:
        result = await session.execute(
            text("SELECT to_regclass(:table_name)"),
            {"table_name": table_name},
        )
        return bool(result.scalar())
    except Exception:
        return False


async def _index_available(session, index_name: str) -> bool:
    try:
        result = await session.execute(
            text("SELECT to_regclass(:index_name)"),
            {"index_name": index_name},
        )
        return bool(result.scalar())
    except Exception:
        return False


async def _gin_index_available_for_column(session, table_name: str, column_name: str) -> bool:
    try:
        result = await session.execute(
            text(
                """
                SELECT EXISTS (
                    SELECT 1
                      FROM pg_index ix
                      JOIN pg_class table_class ON table_class.oid = ix.indrelid
                      JOIN pg_namespace table_schema ON table_schema.oid = table_class.relnamespace
                      JOIN pg_class index_class ON index_class.oid = ix.indexrelid
                      JOIN pg_am index_am ON index_am.oid = index_class.relam
                      JOIN pg_attribute attr
                        ON attr.attrelid = table_class.oid
                       AND attr.attnum = ANY(ix.indkey)
                     WHERE table_class.oid = to_regclass(:table_name)
                       AND attr.attname = :column_name
                       AND index_am.amname = 'gin'
                       AND ix.indisvalid
                       AND ix.indisready
                )
                """
            ),
            {"table_name": table_name, "column_name": column_name},
        )
        return bool(result.scalar())
    except Exception:
        return False


def _serving_table_name() -> str:
    configured = str(os.getenv(PTG2_SERVING_TABLE_ENV) or "ptg2_serving_rate").strip()
    if "." in configured:
        return configured
    return f"{PTG2_SCHEMA}.{configured}"


def _safe_table_name(value: Any, *, default_schema: str = PTG2_SCHEMA) -> str | None:
    if not value:
        return None
    text_value = str(value).strip()
    if not text_value:
        return None
    parts = text_value.split(".", 1)
    if len(parts) == 1:
        schema_name = default_schema
        table_name = parts[0]
    else:
        schema_name, table_name = parts
    ident_re = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")
    if not ident_re.fullmatch(schema_name) or not ident_re.fullmatch(table_name):
        return None
    return f"{schema_name}.{table_name}"


def _serving_table_candidates() -> list[str]:
    primary = _safe_table_name(_serving_table_name()) or f"{PTG2_SCHEMA}.ptg2_serving_rate"
    stage = f"{PTG2_SCHEMA}.ptg2_serving_rate_stage"
    candidates = [primary]
    if stage not in candidates:
        candidates.append(stage)
    return candidates


async def snapshot_serving_table(session, snapshot_id: str) -> str | None:
    tables = await snapshot_serving_tables(session, snapshot_id)
    return tables.serving_table


async def snapshot_serving_tables(session, snapshot_id: str) -> PTG2ServingTables:
    result = await session.execute(
        text(
            f"""
            SELECT manifest->'serving_index'
              FROM {PTG2_SCHEMA}.ptg2_snapshot
             WHERE snapshot_id = :snapshot_id
             LIMIT 1
            """
        ),
        {"snapshot_id": snapshot_id},
    )
    value = result.scalar()
    if not value:
        return PTG2ServingTables()
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return PTG2ServingTables()
    if not isinstance(value, dict):
        return PTG2ServingTables()
    return PTG2ServingTables(
        serving_table=_safe_table_name(value.get("table")),
        price_code_set_table=_safe_table_name(value.get("price_code_set_table")),
        price_atom_table=_safe_table_name(value.get("price_atom_table")),
        price_set_entry_table=_safe_table_name(value.get("price_set_entry_table")),
        procedure_table=_safe_table_name(value.get("procedure_table")) or f"{PTG2_SCHEMA}.ptg2_procedure",
        provider_set_table=_safe_table_name(value.get("provider_set_table")),
        provider_set_component_table=_safe_table_name(value.get("provider_set_component_table")),
        provider_set_entry_table=_safe_table_name(value.get("provider_set_entry_table")),
        provider_entry_component_table=_safe_table_name(value.get("provider_entry_component_table")),
        provider_group_member_table=_safe_table_name(value.get("provider_group_member_table")),
        provider_group_location_table=_safe_table_name(value.get("provider_group_location_table")),
    )


def _ordered_serving_table_candidates(preferred_table: str | None = None) -> list[str]:
    candidates: list[str] = []
    if preferred_table:
        safe_preferred = _safe_table_name(preferred_table)
        if safe_preferred:
            candidates.append(safe_preferred)
    for candidate in _serving_table_candidates():
        if candidate not in candidates:
            candidates.append(candidate)
    return candidates


def _is_compact_serving_table(table_name: str) -> bool:
    return table_name.rsplit(".", 1)[-1].startswith("ptg2_serving_rate_compact")
