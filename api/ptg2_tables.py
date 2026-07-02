# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PTG2 serving table discovery and validation helpers."""

from __future__ import annotations

import json
import os
import re
import time
from typing import Any

from sqlalchemy import text

from api.ptg2_types import PTG2ServingTables

PTG2_SCHEMA = os.getenv("HLTHPRT_DB_SCHEMA", "mrf")
PTG2_SERVING_TABLE_ENV = "HLTHPRT_PTG2_SERVING_TABLE"
_PTG2_TABLE_CACHE_TTL_SECONDS = max(float(os.getenv("HLTHPRT_PTG2_TABLE_CACHE_TTL_SECONDS", "300")), 0.0)
_PTG2_TABLE_AVAILABLE_CACHE: dict[str, tuple[float, bool]] = {}
_PTG2_SNAPSHOT_TABLES_CACHE: dict[str, tuple[float, PTG2ServingTables]] = {}


def _metadata_cache_enabled(session: Any) -> bool:
    return hasattr(session, "sync_session")


async def _serving_table_available(session, table_name: str) -> bool:
    cache_enabled = _metadata_cache_enabled(session)
    if cache_enabled:
        cached = _PTG2_TABLE_AVAILABLE_CACHE.get(table_name)
        if cached is not None:
            cached_at, value = cached
            if _PTG2_TABLE_CACHE_TTL_SECONDS == 0 or (time.monotonic() - cached_at) <= _PTG2_TABLE_CACHE_TTL_SECONDS:
                return value
    try:
        result = await session.execute(
            text("SELECT to_regclass(:table_name)"),
            {"table_name": table_name},
        )
        value = bool(result.scalar())
        if cache_enabled:
            _PTG2_TABLE_AVAILABLE_CACHE[table_name] = (time.monotonic(), value)
        return value
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


def _serving_table_name(snapshot_id: str | None = None) -> str:
    configured = _safe_table_name(os.getenv(PTG2_SERVING_TABLE_ENV))
    if configured:
        return configured
    if snapshot_id:
        suffix = re.sub(r"[^A-Za-z0-9_]", "_", str(snapshot_id)).strip("_")[:48]
        if suffix:
            return f"{PTG2_SCHEMA}.ptg2_serving_rate_{suffix}"
    return f"{PTG2_SCHEMA}.ptg2_serving_rate"


def _serving_table_candidates(snapshot_id: str | None = None) -> list[str]:
    candidates = [_serving_table_name(snapshot_id)]
    if f"{PTG2_SCHEMA}.ptg2_serving_rate" not in candidates:
        candidates.append(f"{PTG2_SCHEMA}.ptg2_serving_rate")
    return candidates


def _ordered_serving_table_candidates(snapshot_id: str | None = None) -> list[str]:
    return _serving_table_candidates(snapshot_id)


def _is_compact_serving_table(table_name: str | None) -> bool:
    return "compact" in str(table_name or "").lower()


async def snapshot_serving_table(session, snapshot_id: str) -> str | None:
    tables = await snapshot_serving_tables(session, snapshot_id)
    if tables.serving_table:
        return tables.serving_table
    for table_name in _ordered_serving_table_candidates(snapshot_id):
        if await _serving_table_available(session, table_name):
            return table_name
    return None


async def snapshot_serving_tables(session, snapshot_id: str) -> PTG2ServingTables:
    cache_enabled = _metadata_cache_enabled(session)
    if cache_enabled:
        cached = _PTG2_SNAPSHOT_TABLES_CACHE.get(snapshot_id)
        if cached is not None:
            cached_at, tables = cached
            if _PTG2_TABLE_CACHE_TTL_SECONDS == 0 or (time.monotonic() - cached_at) <= _PTG2_TABLE_CACHE_TTL_SECONDS:
                return tables

    def _cache(value: PTG2ServingTables) -> PTG2ServingTables:
        if cache_enabled:
            _PTG2_SNAPSHOT_TABLES_CACHE[snapshot_id] = (time.monotonic(), value)
        return value

    result = await session.execute(
        text(
            f"""
            SELECT manifest
              FROM {PTG2_SCHEMA}.ptg2_snapshot
             WHERE snapshot_id = :snapshot_id
             LIMIT 1
            """
        ),
        {"snapshot_id": snapshot_id},
    )
    value = result.scalar()
    if not value:
        return _cache(PTG2ServingTables())
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return _cache(PTG2ServingTables())
    if not isinstance(value, dict):
        return _cache(PTG2ServingTables())
    manifest = value
    serving_index = manifest.get("serving_index")
    if isinstance(serving_index, str):
        try:
            serving_index = json.loads(serving_index)
        except json.JSONDecodeError:
            serving_index = None
    if not isinstance(serving_index, dict):
        serving_index = manifest
    artifact_uri = (
        serving_index.get("artifact_uri")
        or serving_index.get("storage_uri")
        or manifest.get("artifact_uri")
        or manifest.get("storage_uri")
    )
    return _cache(PTG2ServingTables(
        storage=str(serving_index.get("storage") or "").strip() or None,
        type=str(serving_index.get("type") or "").strip() or None,
        snapshot_scoped=bool(serving_index.get("snapshot_scoped")),
        source_key=str(serving_index.get("source_key") or "").strip() or None,
        artifact_uri=str(artifact_uri or "").strip() or None,
        artifacts=dict(serving_index.get("artifacts") or {}) if isinstance(serving_index.get("artifacts"), dict) else None,
        id_storage=str(serving_index.get("id_storage") or "hex").strip().lower() or "hex",
        serving_table=_safe_table_name(serving_index.get("table")),
        price_code_set_table=_safe_table_name(serving_index.get("price_code_set_table")),
        price_atom_table=_safe_table_name(serving_index.get("price_atom_table")),
        price_set_entry_table=_safe_table_name(serving_index.get("price_set_entry_table")),
        procedure_table=_safe_table_name(serving_index.get("procedure_table")),
        code_count_table=_safe_table_name(serving_index.get("code_count_table")),
        provider_set_table=_safe_table_name(serving_index.get("provider_set_table")),
        provider_set_component_table=_safe_table_name(serving_index.get("provider_set_component_table")),
        provider_set_entry_table=_safe_table_name(serving_index.get("provider_set_entry_table")),
        provider_entry_component_table=_safe_table_name(serving_index.get("provider_entry_component_table")),
        provider_group_member_table=_safe_table_name(serving_index.get("provider_group_member_table")),
        provider_group_location_table=_safe_table_name(serving_index.get("provider_group_location_table")),
        provider_group_rate_scope_table=_safe_table_name(serving_index.get("provider_group_rate_scope_table")),
    ))
