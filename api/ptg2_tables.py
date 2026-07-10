# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PTG2 serving table discovery and validation helpers."""

from __future__ import annotations

import json
import logging
import os
import re
import time
from typing import Any

from sqlalchemy import text

from process.ptg_parts.ptg2_artifact_blobs import hydrate_ptg2_artifact_entry_from_db

from api.ptg2_types import PTG2ServingTables

PTG2_SCHEMA = os.getenv("HLTHPRT_DB_SCHEMA", "mrf")
PTG2_SERVING_TABLE_ENV = "HLTHPRT_PTG2_SERVING_TABLE"
PTG2_ARTIFACT_DB_MATERIALIZE_ON_READ_ENV = "HLTHPRT_PTG2_ARTIFACT_DB_MATERIALIZE_ON_READ"
_PTG2_TABLE_CACHE_TTL_SECONDS = max(float(os.getenv("HLTHPRT_PTG2_TABLE_CACHE_TTL_SECONDS", "300")), 0.0)
_PTG2_TABLE_AVAILABLE_CACHE: dict[str, tuple[float, bool]] = {}
_PTG2_SNAPSHOT_TABLES_CACHE: dict[str, tuple[float, PTG2ServingTables]] = {}
logger = logging.getLogger(__name__)


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


def _serving_index_arch_version(serving_index: dict[str, Any]) -> str:
    return str(serving_index.get("arch_version") or "").strip().lower()


def _optional_integer(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _serving_index_atom_key_bits(serving_index: dict[str, Any]) -> int | None:
    direct_bits = _optional_integer(serving_index.get("atom_key_bits"))
    if direct_bits is not None:
        return direct_bits
    serving_binary = serving_index.get("serving_binary")
    if not isinstance(serving_binary, dict):
        return None
    dense_atom_keys = serving_binary.get("dense_atom_keys")
    if isinstance(dense_atom_keys, dict):
        dense_bits = _optional_integer(dense_atom_keys.get("atom_key_bits"))
        if dense_bits is not None:
            return dense_bits
    price_atoms = serving_binary.get("price_atoms_v3")
    if isinstance(price_atoms, dict):
        return _optional_integer(price_atoms.get("atom_key_bits"))
    return None


def _serving_binary_section_integer(
    serving_index: dict[str, Any],
    section_name: str,
    field_name: str,
) -> int | None:
    serving_binary = serving_index.get("serving_binary")
    if not isinstance(serving_binary, dict):
        return None
    section_fields = serving_binary.get(section_name)
    if not isinstance(section_fields, dict):
        return None
    return _optional_integer(section_fields.get(field_name))


def _serving_binary_section_storage_integer(
    serving_index: dict[str, Any],
    section_name: str,
    field_name: str,
) -> int | None:
    serving_binary = serving_index.get("serving_binary")
    if not isinstance(serving_binary, dict):
        return None
    section_fields = serving_binary.get(section_name)
    if not isinstance(section_fields, dict):
        return None
    storage_fields = section_fields.get("storage")
    if not isinstance(storage_fields, dict):
        return None
    return _optional_integer(storage_fields.get(field_name))


def _is_postgres_binary_serving_index(serving_index: dict[str, Any]) -> bool:
    if _serving_index_arch_version(serving_index) in {
        "postgres_binary_v1",
        "postgres_binary_v2",
        "postgres_binary_v3",
    }:
        return True
    if _safe_table_name(serving_index.get("serving_binary_table")):
        return True
    materialized_tables = serving_index.get("materialized_tables")
    return isinstance(materialized_tables, dict) and bool(_safe_table_name(materialized_tables.get("serving_binary")))


def _materialize_db_artifacts_on_read(serving_index: dict[str, Any]) -> bool:
    if _is_postgres_binary_serving_index(serving_index):
        return False
    raw = os.getenv(PTG2_ARTIFACT_DB_MATERIALIZE_ON_READ_ENV, "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


async def _hydrate_snapshot_artifacts(session, artifacts: Any, serving_index: dict[str, Any]) -> dict[str, Any] | None:
    if not isinstance(artifacts, dict):
        return None
    if not _materialize_db_artifacts_on_read(serving_index):
        return dict(artifacts)

    async def hydrate_entry(value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        try:
            return await hydrate_ptg2_artifact_entry_from_db(session, value, schema_name=PTG2_SCHEMA)
        except Exception as exc:
            logger.warning("Failed to hydrate PTG2 artifact %s from PostgreSQL: %s", value.get("name"), exc)
            return value

    hydrated: dict[str, Any] = {}
    for key, value in artifacts.items():
        if key == "sidecars" and isinstance(value, list):
            hydrated[key] = [await hydrate_entry(item) for item in value]
        else:
            hydrated[key] = await hydrate_entry(value)
    return hydrated


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
               AND status = 'published'
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
    network_names = serving_index.get("network_names")
    artifacts = await _hydrate_snapshot_artifacts(session, serving_index.get("artifacts"), serving_index)
    return _cache(PTG2ServingTables(
        storage=str(serving_index.get("storage") or "").strip() or None,
        type=str(serving_index.get("type") or "").strip() or None,
        snapshot_scoped=bool(serving_index.get("snapshot_scoped")),
        source_key=str(serving_index.get("source_key") or "").strip() or None,
        artifact_uri=str(artifact_uri or "").strip() or None,
        artifacts=artifacts,
        arch_version=str(serving_index.get("arch_version") or "").strip() or None,
        provider_scope_strategy=str(serving_index.get("provider_scope_strategy") or "").strip() or None,
        materialized_tables=(
            dict(serving_index.get("materialized_tables") or {})
            if isinstance(serving_index.get("materialized_tables"), dict)
            else None
        ),
        id_storage=str(serving_index.get("id_storage") or "hex").strip().lower() or "hex",
        serving_table_layout=str(serving_index.get("serving_table_layout") or "").strip() or None,
        source_trace_set_hash=str(serving_index.get("source_trace_set_hash") or "").strip() or None,
        network_names=[str(value) for value in network_names] if isinstance(network_names, list) else None,
        serving_table=_safe_table_name(serving_index.get("table")),
        price_code_set_table=_safe_table_name(serving_index.get("price_code_set_table")),
        price_atom_table=_safe_table_name(serving_index.get("price_atom_table")),
        price_atom_table_layout=str(serving_index.get("price_atom_table_layout") or "").strip() or None,
        price_atom_dictionary_table=_safe_table_name(serving_index.get("price_atom_dictionary_table")),
        price_atom_constant_keys=(
            dict(serving_index.get("price_atom_constant_keys") or {})
            if isinstance(serving_index.get("price_atom_constant_keys"), dict)
            else None
        ),
        price_atom_constant_values=(
            dict(serving_index.get("price_atom_constant_values") or {})
            if isinstance(serving_index.get("price_atom_constant_values"), dict)
            else None
        ),
        price_set_entry_table=_safe_table_name(serving_index.get("price_set_entry_table")),
        procedure_table=_safe_table_name(serving_index.get("procedure_table")),
        code_count_table=_safe_table_name(serving_index.get("code_count_table")),
        provider_set_table=_safe_table_name(serving_index.get("provider_set_table")),
        provider_set_component_table=_safe_table_name(serving_index.get("provider_set_component_table")),
        provider_set_entry_table=_safe_table_name(serving_index.get("provider_set_entry_table")),
        provider_entry_component_table=_safe_table_name(serving_index.get("provider_entry_component_table")),
        provider_group_member_table=_safe_table_name(serving_index.get("provider_group_member_table")),
        provider_npi_scope_table=_safe_table_name(
            serving_index.get("provider_npi_scope_table")
            or (
                serving_index.get("materialized_tables", {}).get("provider_npi_scope")
                if isinstance(serving_index.get("materialized_tables"), dict)
                else None
            )
        ),
        provider_group_location_table=_safe_table_name(serving_index.get("provider_group_location_table")),
        provider_group_rate_scope_table=_safe_table_name(serving_index.get("provider_group_rate_scope_table")),
        provider_set_dictionary_table=_safe_table_name(serving_index.get("provider_set_dictionary_table")),
        serving_binary_table=_safe_table_name(
            serving_index.get("serving_binary_table")
            or (
                serving_index.get("materialized_tables", {}).get("serving_binary")
                if isinstance(serving_index.get("materialized_tables"), dict)
                else None
            )
        ),
        price_dictionary_item_count=_serving_binary_section_integer(
            serving_index,
            "price_dictionary",
            "price_set_count",
        ),
        price_dictionary_block_bytes=_serving_binary_section_integer(
            serving_index,
            "price_dictionary",
            "block_bytes",
        ),
        price_dictionary_compressed_records=_serving_binary_section_storage_integer(
            serving_index,
            "price_dictionary",
            "compressed_records",
        ),
        atom_key_bits=_serving_index_atom_key_bits(serving_index),
        price_key_block_span=_serving_binary_section_integer(
            serving_index,
            "price_set_atom_memberships_v3",
            "block_span",
        ),
        atom_key_block_span=_serving_binary_section_integer(
            serving_index,
            "price_atoms_v3",
            "block_span",
        ),
    ))
