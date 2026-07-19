# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PTG2 source-scoped manifest-backed serving stage and publish helpers."""

from __future__ import annotations

import asyncio
import logging
import os
import stat
import subprocess
import tempfile
import uuid
from pathlib import Path
from typing import Any, Mapping

from api.ptg2_code_filters import INFERRED_PROVIDER_TAXONOMY_RULES
from db.connection import db
from process.ptg_parts.config import (
    PTG2_SNAPSHOT_ARCH_POSTGRES_BINARY_V3,
    PTG2_UNLOGGED_STAGE_ENV, _env_bool, _is_postgres_binary_snapshot_arch,
    _is_postgres_binary_v3_arch, _ptg2_snapshot_arch_from_env)
from process.ptg_parts.artifacts import resolve_ptg2_artifact_dir
from process.ptg_parts.ptg2_artifact_blobs import (
    ptg2_artifact_db_retain_local_cache,
    ptg2_artifact_id_from_db_uri,
    ptg2_artifact_db_store_enabled,
    store_ptg2_artifact_file_in_db,
)
from process.ptg_parts.db_tables import (_exact_table_rows, _quote_ident,
                                         _table_exists, _table_has_rows)
from process.ptg_parts.live_progress import write_live_progress
from process.ptg_parts.ptg2_manifest_artifacts import (
    PTG2ManifestArtifactError,
    PTG2_PROVIDER_MEMBERSHIP_GRAPH_ARTIFACT_NAMES,
    PTG2_PROVIDER_MEMBERSHIP_GRAPH_CHUNK_BYTES,
    PTG2_PROVIDER_MEMBERSHIP_GRAPH_VERSION,
    PTG2_SERVING_BY_CODE_ARTIFACT_KIND,
    PTG2_SERVING_BY_CODE_FORMAT,
    PTG2_SERVING_BY_CODE_MAGIC,
    PTG2_SERVING_BY_PROVIDER_SET_ARTIFACT_KIND,
    PTG2_SERVING_BY_PROVIDER_SET_FORMAT,
    PTG2_SERVING_BY_PROVIDER_SET_MAGIC,
    _existing_serving_sidecar_path_entry,
    membership_index_fence_metadata,
    read_global_sidecar_entries,
    validate_v3_graph_db_entry,
    write_serving_by_code_sidecar_async,
    write_serving_by_provider_set_sidecar_async,
)
from process.ptg_parts.rust_scanner import _ptg2_rust_scanner_binary
from process.ptg_parts.snapshot_tables import (_ptg2_snapshot_index_name,
                                               _ptg2_snapshot_table_token)

PTG2_MANIFEST_SERVING_COPY_ENV = "HLTHPRT_PTG2_MANIFEST_SERVING_COPY_PATH"
PTG2_MANIFEST_LEAN_SERVING_COPY_ENV = "HLTHPRT_PTG2_MANIFEST_LEAN_SERVING_COPY_PATH"
PTG2_MANIFEST_LEAN_DIRECT_COPY_ENV = "HLTHPRT_PTG2_MANIFEST_LEAN_DIRECT_COPY"
PTG2_MANIFEST_SERVING_LAYOUT_LEAN_PROVIDER_KEY = "lean_provider_key_v1"
PTG2_MANIFEST_PRICE_ATOM_LAYOUT_LEAN_DICT_V2 = "lean_dict_v2"
PTG2_MANIFEST_PROVIDER_GROUP_LOCATION_TABLE_ENV = "HLTHPRT_PTG2_MANIFEST_PROVIDER_GROUP_LOCATION_TABLE"
PTG2_MANIFEST_PROVIDER_GROUP_LOCATION_INDEX_PROFILE_ENV = (
    "HLTHPRT_PTG2_MANIFEST_PROVIDER_GROUP_LOCATION_INDEX_PROFILE"
)
PTG2_MANIFEST_PROVIDER_SET_COMPONENT_TABLE_ENV = "HLTHPRT_PTG2_MANIFEST_PROVIDER_SET_COMPONENT_TABLE"
PTG2_MANIFEST_PROVIDER_GROUP_RATE_SCOPE_TABLE_ENV = "HLTHPRT_PTG2_MANIFEST_PROVIDER_GROUP_RATE_SCOPE_TABLE"
PTG2_MANIFEST_SERVING_SIDECARS_ENABLED_ENV = "HLTHPRT_PTG2_MANIFEST_SERVING_SIDECARS_ENABLED"
PTG2_MANIFEST_DROP_SERVING_TABLE_AFTER_SIDECARS_ENV = "HLTHPRT_PTG2_MANIFEST_DROP_SERVING_TABLE_AFTER_SIDECARS"
PTG2_MANIFEST_SERVING_SIDECAR_RUST_ENV = "HLTHPRT_PTG2_MANIFEST_SERVING_SIDECAR_RUST"
PTG2_MANIFEST_LEAN_REWRITE_PARALLEL_DICTS_ENV = "HLTHPRT_PTG2_MANIFEST_LEAN_REWRITE_PARALLEL_DICTS"
PTG2_MANIFEST_POSTGRES_BINARY_NATURAL_LEAN_STREAM_ENV = "HLTHPRT_PTG2_POSTGRES_BINARY_NATURAL_LEAN_STREAM"
logger = logging.getLogger(__name__)
_MAX_PARTIAL_ZIP_TAXONOMY_INDEX_CODES = 12
_PROVIDER_GROUP_LOCATION_INDEX_PROFILE_FULL = "full"
_PROVIDER_GROUP_LOCATION_INDEX_PROFILE_LEAN = "lean"
_MANIFEST_PUBLISH_DETAIL_START_PCT = 96.38
_MANIFEST_PUBLISH_DETAIL_END_PCT = 97.24
_PROVIDER_MEMBERSHIP_GRAPH_ARTIFACT_NAMES = PTG2_PROVIDER_MEMBERSHIP_GRAPH_ARTIFACT_NAMES
_PROVIDER_MEMBERSHIP_GRAPH_V2_VERSION = "provider_membership_graph_v2"


def _row_value(row: Any, key: str, position: int = 0) -> Any:
    mapping = getattr(row, "_mapping", None)
    if mapping is not None:
        return mapping.get(key)
    if isinstance(row, dict):
        return row.get(key)
    try:
        keyed_value = row[key]
    except Exception:
        keyed_value = None
    else:
        return keyed_value
    value = getattr(row, key, None)
    if value is not None:
        return value
    try:
        return row[position]
    except Exception:
        return None


def _emit_ptg2_manifest_publish_progress(
    publish_step: str,
    *,
    done: int,
    total: int,
    message: str | None = None,
    pct: float | None = None,
    **progress_details: Any,
) -> None:
    total_steps = max(int(total or 1), 1)
    completed_steps = max(0, min(int(done), total_steps))
    if pct is None:
        pct = _MANIFEST_PUBLISH_DETAIL_START_PCT + (
            (completed_steps / total_steps)
            * (_MANIFEST_PUBLISH_DETAIL_END_PCT - _MANIFEST_PUBLISH_DETAIL_START_PCT)
        )
    progress_message = message or f"publishing {publish_step}"
    progress_payload_map = {
        "phase": f"publishing: {publish_step}"[:128],
        "unit": "manifest_publish_steps",
        "done": completed_steps,
        "total": total_steps,
        "pct": pct,
        "phase_pct": (completed_steps / total_steps) * 100.0,
        "message": progress_message,
        "detail": progress_message,
        "source": "ptg2-manifest-publish-progress",
        "confidence": "live",
        "publish_step": publish_step,
        **{
            key: detail_value
            for key, detail_value in progress_details.items()
            if detail_value is not None
        },
    }
    try:
        write_live_progress(**progress_payload_map)
    except Exception:
        logger.debug("Failed to write PTG2 manifest publish live progress", exc_info=True)


def _path_byte_count(path: Path | None) -> int | None:
    if path is None:
        return None
    try:
        return path.stat().st_size if path.exists() else None
    except OSError:
        return None


def _ptg2_manifest_sidecar_upload_count(sidecar_artifacts: Mapping[str, Any] | None) -> int:
    if not sidecar_artifacts:
        return 0
    count = 0
    for name, value in sidecar_artifacts.items():
        candidates = value if name == "sidecars" and isinstance(value, list) else [value]
        for candidate in candidates:
            if not isinstance(candidate, Mapping):
                continue
            storage_uri = str(candidate.get("storage_uri") or "").strip()
            raw_path = str(candidate.get("path") or "").strip()
            if raw_path and not storage_uri.startswith("db://ptg2_artifact/"):
                count += 1
    return count


def _artifact_chunk_count(entry: Mapping[str, Any]) -> int | None:
    try:
        byte_count = int(entry.get("byte_count") or 0)
        chunk_bytes = int(entry.get("chunk_bytes") or 0)
    except (TypeError, ValueError):
        return None
    if byte_count <= 0 or chunk_bytes <= 0:
        return None
    return (byte_count + chunk_bytes - 1) // chunk_bytes


PTG2_MANIFEST_SERVING_COLUMNS = [
    "serving_content_hash_128",
    "plan_id",
    "reported_code_system",
    "reported_code",
    "procedure_global_id_128",
    "provider_set_global_id_128",
    "provider_count",
    "price_set_global_id_128",
    "source_trace_set_hash",
    "network_names",
]
PTG2_MANIFEST_LEAN_SERVING_COLUMNS = [
    "plan_id",
    "reported_code_system",
    "reported_code",
    "provider_set_global_id_128",
    "provider_count",
    "price_set_global_id_128",
]
PTG2_MANIFEST_PRICE_ATOM_COLUMNS = [
    "price_atom_global_id_128",
    "negotiated_type",
    "negotiated_rate",
    "expiration_date",
    "service_code",
    "billing_class",
    "setting",
    "billing_code_modifier",
    "additional_information",
]
PTG2_MANIFEST_PRICE_SET_ATOM_COLUMNS = [
    "price_set_global_id_128",
    "price_atom_global_id_128",
]
PTG2_MANIFEST_PRICE_SET_SUMMARY_COLUMNS = [
    "price_set_global_id_128",
    "minimum_negotiated_rate",
]
PTG2_MANIFEST_PROVIDER_GROUP_MEMBER_COLUMNS = [
    "provider_group_global_id_128",
    "npi",
]
PTG2_MANIFEST_PROVIDER_NPI_SCOPE_COLUMNS = ["npi"]
PTG2_MANIFEST_CODE_COUNT_COLUMNS = [
    "plan_id",
    "reported_code_system",
    "reported_code",
    "rate_count",
]
PTG2_MANIFEST_PROVIDER_SET_DICTIONARY_COLUMNS = [
    "provider_set_global_id_128",
]
PTG2_MANIFEST_PROVIDER_SET_COMPONENT_COLUMNS = [
    "provider_set_global_id_128",
    "provider_group_global_id_128",
]
_PROVIDER_GROUP_LOCATION_CREATE_SQL = """
        CREATE UNLOGGED TABLE {qualified_location_table} (
            provider_group_global_id_128 {id_type} NOT NULL,
            npi bigint NOT NULL,
            address_key uuid,
            premise_key uuid,
            zip5 varchar(5),
            state_name varchar,
            city_name varchar,
            lat numeric,
            long numeric,
            address_precision varchar,
            taxonomy_array int[] NOT NULL DEFAULT '{{0}}',
            address_type varchar,
            address_checksum varchar,
            first_line varchar,
            second_line varchar,
            postal_code varchar,
            country_code varchar,
            formatted_address varchar,
            telephone_number varchar,
            fax_number varchar,
            phone_number varchar,
            phone_extension varchar,
            fax_number_digits varchar,
            fax_extension varchar
        );
        """
_PROVIDER_GROUP_LOCATION_INSERT_SQL = """
        INSERT INTO {qualified_location_table} (
            provider_group_global_id_128,
            npi,
            address_key,
            premise_key,
            zip5,
            state_name,
            city_name,
            lat,
            long,
            address_precision,
            taxonomy_array,
            address_type,
            address_checksum,
            first_line,
            second_line,
            postal_code,
            country_code,
            formatted_address,
            telephone_number,
            fax_number,
            phone_number,
            phone_extension,
            fax_number_digits,
            fax_extension
        )
        SELECT DISTINCT
            pgm.provider_group_global_id_128,
            pgm.npi,
            addr.address_key,
            addr.premise_key,
            COALESCE(addr.zip5, LEFT(COALESCE(addr.postal_code, ''), 5)::varchar)::varchar(5) AS zip5,
            addr.state_name::varchar,
            addr.city_name::varchar,
            addr.lat,
            addr.long,
            addr.address_precision::varchar,
            COALESCE(addr.taxonomy_array, ARRAY[0]::int[])::int[] AS taxonomy_array,
            addr.type::varchar AS address_type,
            addr.checksum::varchar AS address_checksum,
            addr.first_line::varchar,
            addr.second_line::varchar,
            addr.postal_code::varchar,
            addr.country_code::varchar,
            addr.formatted_address::varchar,
            addr.telephone_number::varchar,
            addr.fax_number::varchar,
            addr.phone_number::varchar,
            addr.phone_extension::varchar,
            addr.fax_number_digits::varchar,
            addr.fax_extension::varchar
          FROM {qualified_member_table} pgm
          JOIN {qualified_entity_address_table} addr
            ON addr.npi = pgm.npi
         WHERE addr.type IN ('primary', 'secondary', 'practice', 'site')
           AND (
                NULLIF(COALESCE(addr.zip5, LEFT(COALESCE(addr.postal_code, ''), 5)::varchar), '') IS NOT NULL
             OR NULLIF(addr.state_name, '') IS NOT NULL
             OR NULLIF(addr.city_name, '') IS NOT NULL
             OR (addr.lat IS NOT NULL AND addr.long IS NOT NULL)
           );
        """


def _ptg2_id_storage() -> str:
    return "uuid"


def _ptg2_id_sql_type() -> str:
    return "uuid" if _ptg2_id_storage() == "uuid" else "char(32)"


def _ptg2_manifest_serving_layout() -> str:
    return PTG2_MANIFEST_SERVING_LAYOUT_LEAN_PROVIDER_KEY


def _use_direct_lean_manifest_copy() -> bool:
    return True


def _ptg2_manifest_price_atom_layout() -> str:
    return PTG2_MANIFEST_PRICE_ATOM_LAYOUT_LEAN_DICT_V2


def _is_provider_location_enabled() -> bool:
    return False


def _provider_group_location_index_profile() -> str:
    raw_value = os.getenv(
        PTG2_MANIFEST_PROVIDER_GROUP_LOCATION_INDEX_PROFILE_ENV,
        _PROVIDER_GROUP_LOCATION_INDEX_PROFILE_LEAN,
    )
    value = str(raw_value or "").strip().lower()
    if value in {"", _PROVIDER_GROUP_LOCATION_INDEX_PROFILE_LEAN, "minimal"}:
        return _PROVIDER_GROUP_LOCATION_INDEX_PROFILE_LEAN
    if value in {_PROVIDER_GROUP_LOCATION_INDEX_PROFILE_FULL, "legacy"}:
        return _PROVIDER_GROUP_LOCATION_INDEX_PROFILE_FULL
    logger.warning(
        "Unknown %s=%r; using %s provider-group location indexes",
        PTG2_MANIFEST_PROVIDER_GROUP_LOCATION_INDEX_PROFILE_ENV,
        raw_value,
        _PROVIDER_GROUP_LOCATION_INDEX_PROFILE_LEAN,
    )
    return _PROVIDER_GROUP_LOCATION_INDEX_PROFILE_LEAN


def _ptg2_manifest_snapshot_arch() -> str:
    return _ptg2_snapshot_arch_from_env()


def _use_parallel_dictionary_rewrite() -> bool:
    if os.getenv(PTG2_MANIFEST_LEAN_REWRITE_PARALLEL_DICTS_ENV) is not None:
        return _env_bool(PTG2_MANIFEST_LEAN_REWRITE_PARALLEL_DICTS_ENV, True)
    return _is_postgres_binary_snapshot_arch(_ptg2_manifest_snapshot_arch())


def _is_natural_lean_stream_enabled() -> bool:
    if os.getenv(PTG2_MANIFEST_POSTGRES_BINARY_NATURAL_LEAN_STREAM_ENV) is not None:
        return _env_bool(PTG2_MANIFEST_POSTGRES_BINARY_NATURAL_LEAN_STREAM_ENV, True)
    return _is_postgres_binary_snapshot_arch(_ptg2_manifest_snapshot_arch())


def _is_provider_component_enabled() -> bool:
    return False


def _is_provider_rate_scope_enabled() -> bool:
    return False


def _ptg2_manifest_stage_table_name(token: str) -> str:
    safe_token = "".join(ch if ch.isalnum() else "_" for ch in token.lower()).strip("_")
    return f"ptg2_manifest_stage_serving_{safe_token}"[:63]


def _ptg2_manifest_stage_suffix(serving_stage_table: str) -> str:
    prefix = "ptg2_manifest_stage_serving_"
    return serving_stage_table[len(prefix):] if serving_stage_table.startswith(prefix) else serving_stage_table


def _ptg2_manifest_support_stage_table(serving_stage_table: str, kind: str) -> str:
    return f"ptg2_manifest_stage_{kind}_{_ptg2_manifest_stage_suffix(serving_stage_table)}"[:63]


async def _create_serving_stage_table(token: str) -> str:
    """Create the PostgreSQL stages consumed by strict V3 publish."""

    if _ptg2_manifest_snapshot_arch() != PTG2_SNAPSHOT_ARCH_POSTGRES_BINARY_V3:
        raise RuntimeError("only postgres_binary_v3 staging is supported")
    schema_name = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    stage_table = _ptg2_manifest_stage_table_name(token)
    await db.status(f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(stage_table)};")
    await _create_price_atom_stage_table(stage_table)
    await _create_price_atom_member_stage_table(stage_table)
    await _create_price_set_summary_stage_table(stage_table)
    return stage_table


async def _create_price_atom_stage_table(serving_stage_table: str) -> str:
    schema_name = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    stage_table = _ptg2_manifest_support_stage_table(serving_stage_table, "price_atom")
    storage_mode = "UNLOGGED " if _env_bool(PTG2_UNLOGGED_STAGE_ENV, True) else ""
    id_type = _ptg2_id_sql_type()
    await db.status(f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(stage_table)};")
    await db.status(
        f"""
        CREATE {storage_mode}TABLE {_quote_ident(schema_name)}.{_quote_ident(stage_table)} (
            price_atom_global_id_128 {id_type} NOT NULL,
            negotiated_type varchar(64),
            negotiated_rate text,
            expiration_date varchar(32),
            service_code text[] NOT NULL DEFAULT '{{}}',
            billing_class varchar(64),
            setting varchar(64),
            billing_code_modifier text[] NOT NULL DEFAULT '{{}}',
            additional_information text
        );
        """
    )
    return stage_table


async def _create_price_atom_member_stage_table(serving_stage_table: str) -> str:
    schema_name = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    stage_table = _ptg2_manifest_support_stage_table(serving_stage_table, "price_set_atom")
    storage_mode = "UNLOGGED " if _env_bool(PTG2_UNLOGGED_STAGE_ENV, True) else ""
    id_type = _ptg2_id_sql_type()
    await db.status(f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(stage_table)};")
    await db.status(
        f"""
        CREATE {storage_mode}TABLE {_quote_ident(schema_name)}.{_quote_ident(stage_table)} (
            price_set_global_id_128 {id_type} NOT NULL,
            price_atom_global_id_128 {id_type} NOT NULL
        );
        """
    )
    return stage_table


async def _create_price_set_summary_stage_table(serving_stage_table: str) -> str:
    schema_name = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    stage_table = _ptg2_manifest_support_stage_table(
        serving_stage_table,
        "price_set_summary",
    )
    storage_mode = "UNLOGGED " if _env_bool(PTG2_UNLOGGED_STAGE_ENV, True) else ""
    id_type = _ptg2_id_sql_type()
    await db.status(
        f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(stage_table)};"
    )
    await db.status(
        f"""
        CREATE {storage_mode}TABLE {_quote_ident(schema_name)}.{_quote_ident(stage_table)} (
            price_set_global_id_128 {id_type} NOT NULL,
            minimum_negotiated_rate numeric NOT NULL
        );
        """
    )
    return stage_table


async def _create_provider_group_member_stage(serving_stage_table: str) -> str:
    schema_name = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    stage_table = _ptg2_manifest_support_stage_table(serving_stage_table, "provider_group_member")
    storage_mode = "UNLOGGED " if _env_bool(PTG2_UNLOGGED_STAGE_ENV, True) else ""
    id_type = _ptg2_id_sql_type()
    await db.status(f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(stage_table)};")
    await db.status(
        f"""
        CREATE {storage_mode}TABLE {_quote_ident(schema_name)}.{_quote_ident(stage_table)} (
            provider_group_global_id_128 {id_type} NOT NULL,
            npi bigint NOT NULL
        );
        """
    )
    return stage_table


async def _create_provider_npi_scope_stage(serving_stage_table: str) -> str:
    schema_name = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    stage_table = _ptg2_manifest_support_stage_table(serving_stage_table, "provider_npi_scope")
    await db.status(f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(stage_table)};")
    await db.status(
        f"""
        CREATE TABLE {_quote_ident(schema_name)}.{_quote_ident(stage_table)} (
            npi bigint NOT NULL
        );
        """
    )
    return stage_table


async def _create_code_count_stage_table(serving_stage_table: str) -> str:
    schema_name = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    stage_table = _ptg2_manifest_support_stage_table(serving_stage_table, "code_count")
    storage_mode = "UNLOGGED " if _env_bool(PTG2_UNLOGGED_STAGE_ENV, True) else ""
    await db.status(f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(stage_table)};")
    await db.status(
        f"""
        CREATE {storage_mode}TABLE {_quote_ident(schema_name)}.{_quote_ident(stage_table)} (
            plan_id varchar(64) NOT NULL,
            reported_code_system varchar(64),
            reported_code varchar(64),
            rate_count bigint NOT NULL
        );
        """
    )
    return stage_table


async def _create_provider_set_dictionary_stage(serving_stage_table: str) -> str:
    schema_name = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    stage_table = _ptg2_manifest_support_stage_table(serving_stage_table, "provider_set_dictionary")
    storage_mode = "UNLOGGED " if _env_bool(PTG2_UNLOGGED_STAGE_ENV, True) else ""
    id_type = _ptg2_id_sql_type()
    await db.status(f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(stage_table)};")
    await db.status(
        f"""
        CREATE {storage_mode}TABLE {_quote_ident(schema_name)}.{_quote_ident(stage_table)} (
            provider_set_global_id_128 {id_type} NOT NULL
        );
        """
    )
    return stage_table


async def _ensure_materialized_tables_logged(materialized_tables: Mapping[str, str]) -> None:
    """Make every retained snapshot relation durable before publishing its manifest."""
    for qualified_table_name in dict.fromkeys(materialized_tables.values()):
        schema_name, separator, table_name = str(qualified_table_name).partition(".")
        if not separator or not schema_name or not table_name:
            raise RuntimeError(f"Invalid PTG2 materialized table name: {qualified_table_name!r}")
        await db.status(
            f"""
            DO $ptg2_logged$
            DECLARE
                relation_persistence "char";
            BEGIN
                SELECT relation.relpersistence
                  INTO relation_persistence
                  FROM pg_class relation
                  JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
                 WHERE namespace.nspname = {_quote_sql_literal(schema_name)}
                   AND relation.relname = {_quote_sql_literal(table_name)};
                IF relation_persistence IS NULL THEN
                    RAISE EXCEPTION 'PTG2 materialized table is missing: %',
                        {_quote_sql_literal(qualified_table_name)};
                ELSIF relation_persistence = 'u' THEN
                    ALTER TABLE {_quote_ident(schema_name)}.{_quote_ident(table_name)} SET LOGGED;
                    SELECT relation.relpersistence
                      INTO relation_persistence
                      FROM pg_class relation
                     WHERE relation.oid = {_quote_sql_literal(qualified_table_name)}::regclass;
                END IF;
                IF relation_persistence <> 'p' THEN
                    RAISE EXCEPTION 'PTG2 materialized table is not logged: % (persistence=%)',
                        {_quote_sql_literal(qualified_table_name)}, relation_persistence;
                END IF;
            END
            $ptg2_logged$;
            """
        )


async def _copy_ptg2_manifest_serving_file(copy_path: Path, *, target_table: str) -> None:
    await _copy_ptg2_manifest_file(copy_path, target_table=target_table, columns=PTG2_MANIFEST_SERVING_COLUMNS)


async def _copy_lean_manifest_serving_file(copy_path: Path, *, target_table: str) -> None:
    await _copy_ptg2_manifest_file(
        copy_path,
        target_table=target_table,
        columns=PTG2_MANIFEST_LEAN_SERVING_COLUMNS,
    )


async def _copy_price_atom_file(copy_path: Path, *, target_table: str) -> None:
    await _copy_ptg2_manifest_file(copy_path, target_table=target_table, columns=PTG2_MANIFEST_PRICE_ATOM_COLUMNS)


async def _copy_price_atom_member_file(copy_path: Path, *, target_table: str) -> None:
    await _copy_ptg2_manifest_file(copy_path, target_table=target_table, columns=PTG2_MANIFEST_PRICE_SET_ATOM_COLUMNS)


async def _copy_price_set_summary_file(copy_path: Path, *, target_table: str) -> None:
    await _copy_ptg2_manifest_file(
        copy_path,
        target_table=target_table,
        columns=PTG2_MANIFEST_PRICE_SET_SUMMARY_COLUMNS,
    )


async def _copy_provider_group_member_file(copy_path: Path, *, target_table: str) -> None:
    await _copy_ptg2_manifest_file(copy_path, target_table=target_table, columns=PTG2_MANIFEST_PROVIDER_GROUP_MEMBER_COLUMNS)


async def _copy_provider_npi_scope_file(copy_path: Path, *, target_table: str) -> None:
    await _copy_ptg2_manifest_file(
        copy_path,
        target_table=target_table,
        columns=PTG2_MANIFEST_PROVIDER_NPI_SCOPE_COLUMNS,
    )


async def _copy_code_count_file(copy_path: Path, *, target_table: str) -> None:
    await _copy_ptg2_manifest_file(copy_path, target_table=target_table, columns=PTG2_MANIFEST_CODE_COUNT_COLUMNS)


async def _copy_provider_set_dictionary_file(copy_path: Path, *, target_table: str) -> None:
    await _copy_ptg2_manifest_file(
        copy_path,
        target_table=target_table,
        columns=PTG2_MANIFEST_PROVIDER_SET_DICTIONARY_COLUMNS,
    )


async def _copy_manifest_component_file(copy_path: Path, *, target_table: str) -> None:
    await _copy_ptg2_manifest_file(
        copy_path,
        target_table=target_table,
        columns=PTG2_MANIFEST_PROVIDER_SET_COMPONENT_COLUMNS,
    )


async def _copy_ptg2_manifest_file(copy_path: Path, *, target_table: str, columns: list[str]) -> None:
    if not copy_path.exists():
        return
    file_status = copy_path.stat()
    if file_status.st_size <= 0 and not stat.S_ISFIFO(file_status.st_mode):
        return
    schema_name = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    async with db.acquire() as conn:
        raw_conn = conn.raw_connection
        driver_conn = getattr(raw_conn, "driver_connection", raw_conn)
        copy_to_table = getattr(driver_conn, "copy_to_table", None)
        if copy_to_table is None:
            raise NotImplementedError("Active database driver does not expose copy_to_table")
        with copy_path.open("rb") as source:
            await copy_to_table(
                target_table,
                source=source,
                schema_name=schema_name,
                columns=columns,
                format="text",
                delimiter="\t",
                null="\\N",
            )


def _is_unique_index_duplicate(exc: Exception) -> bool:
    parts = [str(exc)]
    orig = getattr(exc, "orig", None)
    if orig is not None:
        parts.append(str(orig))
        cause = getattr(orig, "__cause__", None)
        if cause is not None:
            parts.append(str(cause))
    message = " ".join(parts).lower()
    return (
        "uniqueviolation" in message
        or "unique violation" in message
        or (
            "could not create unique index" in message
            and ("duplicate" in message or "duplicated" in message)
        )
    )


def _manifest_provider_inverted_entry(
    sidecar_artifacts: Mapping[str, Any] | None,
    artifacts: Mapping[str, Any] | None = None,
) -> dict[str, Any] | None:
    for payload in (sidecar_artifacts, artifacts):
        if not payload:
            continue
        direct = payload.get("provider_inverted")
        if isinstance(direct, Mapping):
            return dict(direct)
        sidecars = payload.get("sidecars")
        if isinstance(sidecars, list):
            for value in sidecars:
                if isinstance(value, Mapping) and value.get("name") == "provider_inverted":
                    return dict(value)
        for value in payload.values():
            if isinstance(value, Mapping) and value.get("name") == "provider_inverted":
                return dict(value)
    return None


def _ptg2_manifest_publish_sidecar_path(
    entry: Mapping[str, Any],
    artifacts: Mapping[str, Any] | None,
) -> Path:
    raw_path = str(entry.get("path") or "").strip()
    path = Path(raw_path)
    if path.is_absolute() or path.exists():
        return path
    manifest_uri = str((artifacts or {}).get("manifest_uri") or "").strip()
    if manifest_uri.startswith("file://"):
        manifest_path = Path(manifest_uri.removeprefix("file://"))
        candidate = manifest_path.parent / path
        if candidate.exists():
            return candidate
    return path


def _is_serving_sidecars_enabled() -> bool:
    return _env_bool(PTG2_MANIFEST_SERVING_SIDECARS_ENABLED_ENV, True)


def _should_drop_serving_table() -> bool:
    return _env_bool(PTG2_MANIFEST_DROP_SERVING_TABLE_AFTER_SIDECARS_ENV, True)


def _should_drop_manifest_serving_table(arch_version: str | None) -> bool:
    return _is_postgres_binary_v3_arch(arch_version) or _should_drop_serving_table()


def _should_analyze_manifest_serving_table(
    arch_version: str | None,
    *,
    skip_final_serving_table: bool,
) -> bool:
    return not skip_final_serving_table or _is_postgres_binary_v3_arch(arch_version)


def _require_v3_serving_layout(arch_version: str | None) -> None:
    if (
        _is_postgres_binary_v3_arch(arch_version)
        and _ptg2_manifest_serving_layout() != PTG2_MANIFEST_SERVING_LAYOUT_LEAN_PROVIDER_KEY
    ):
        raise RuntimeError("postgres_binary_v3 requires the lean provider-key serving layout")


def _ptg2_manifest_serving_sidecar_dir(
    *,
    artifacts: Mapping[str, Any] | None,
    source_key: str,
    snapshot_id: str,
) -> Path:
    payload = artifacts or {}
    for value in payload.values():
        if isinstance(value, Mapping):
            raw_path = str(value.get("path") or "").strip()
            if raw_path:
                return Path(raw_path).parent
    sidecars = payload.get("sidecars")
    if isinstance(sidecars, list):
        for value in sidecars:
            if isinstance(value, Mapping):
                raw_path = str(value.get("path") or "").strip()
                if raw_path:
                    return Path(raw_path).parent
    return resolve_ptg2_artifact_dir() / "serving" / _ptg2_snapshot_table_token(source_key, snapshot_id)


async def _iter_ptg2_serving_sidecar_rows(sql: str):
    async with db.session() as session:
        result = await session.stream(db.text(sql))
        async for row in result:
            yield row


def _is_rust_sidecar_enabled() -> bool:
    return _env_bool(PTG2_MANIFEST_SERVING_SIDECAR_RUST_ENV, True)


def _new_ptg2_temp_path(directory: Path, *, prefix: str, suffix: str) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=prefix, suffix=suffix, dir=directory)
    os.close(fd)
    path = Path(name)
    path.unlink(missing_ok=True)
    return path


async def _copy_ptg2_query_to_file(sql: str, output_path: Path) -> None:
    async with db.acquire() as conn:
        raw_conn = conn.raw_connection
        driver_conn = getattr(raw_conn, "driver_connection", raw_conn)
        copy_from_query = getattr(driver_conn, "copy_from_query", None)
        if copy_from_query is None:
            raise NotImplementedError("Active database driver does not expose copy_from_query")
        with output_path.open("wb") as output:
            await copy_from_query(
                sql,
                output=output,
                format="text",
                delimiter="\t",
                null="\\N",
            )


def _run_serving_sidecar_from_key_copy(kind: str, copy_path: Path, output_path: Path) -> None:
    binary = _ptg2_rust_scanner_binary()
    if binary is None:
        raise RuntimeError("PTG2 Rust serving sidecar encoder is enabled but no scanner binary was found")
    completed = subprocess.run(
        [
            str(binary),
            "--serving-sidecar-from-key-copy",
            kind,
            str(copy_path),
            str(output_path),
        ],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if completed.returncode != 0:
        stderr_text = completed.stderr.decode("utf-8", errors="replace")
        stdout_text = completed.stdout.decode("utf-8", errors="replace")
        raise RuntimeError(
            "PTG2 Rust serving sidecar encoder failed "
            f"for {kind}: stdout={stdout_text[-500:]} stderr={stderr_text[-1000:]}"
        )


async def _write_serving_sidecars_rust(
    *,
    schema_name: str,
    final_table: str,
    artifact_dir: Path,
    expected_row_count: int | None,
) -> dict[str, Any] | None:
    """Write or reuse both serving sidecars with Rust when available."""
    if not _is_rust_sidecar_enabled() or _ptg2_rust_scanner_binary() is None:
        return None

    qualified_table = f"{_quote_ident(schema_name)}.{_quote_ident(final_table)}"
    by_code_path = artifact_dir / "serving_by_code_v1.ptg2sbc"
    by_provider_set_path = artifact_dir / "serving_by_provider_set_v1.ptg2sbp"
    existing_by_code = _existing_serving_sidecar_path_entry(
        name="serving_by_code",
        path=by_code_path,
        magic=PTG2_SERVING_BY_CODE_MAGIC,
        expected_format=PTG2_SERVING_BY_CODE_FORMAT,
        kind=PTG2_SERVING_BY_CODE_ARTIFACT_KIND,
        expected_row_count=expected_row_count,
    )
    existing_by_provider_set = _existing_serving_sidecar_path_entry(
        name="serving_by_provider_set",
        path=by_provider_set_path,
        magic=PTG2_SERVING_BY_PROVIDER_SET_MAGIC,
        expected_format=PTG2_SERVING_BY_PROVIDER_SET_FORMAT,
        kind=PTG2_SERVING_BY_PROVIDER_SET_ARTIFACT_KIND,
        expected_row_count=expected_row_count,
    )
    if existing_by_code is not None and existing_by_provider_set is not None:
        _emit_ptg2_manifest_publish_progress(
            "serving sidecars reused",
            done=8,
            total=8,
            message="serving sidecars already exist",
            serving_by_code_bytes=existing_by_code.get("byte_count"),
            serving_by_provider_set_bytes=existing_by_provider_set.get("byte_count"),
        )
        return {
            "serving_by_code": existing_by_code,
            "serving_by_provider_set": existing_by_provider_set,
        }

    by_code_copy = _new_ptg2_temp_path(artifact_dir, prefix="serving_by_code_", suffix=".copy")
    by_provider_set_copy = _new_ptg2_temp_path(artifact_dir, prefix="serving_by_provider_set_", suffix=".copy")
    try:
        _emit_ptg2_manifest_publish_progress(
            "serving sidecars export by code",
            done=1,
            total=8,
            message="exporting serving rows ordered by code",
            expected_row_count=expected_row_count,
        )
        await _copy_ptg2_query_to_file(
            f"""
            SELECT code_key, provider_set_key, provider_count, price_set_global_id_128
            FROM {qualified_table}
            ORDER BY code_key, provider_set_key, price_set_global_id_128
            """,
            by_code_copy,
        )
        _emit_ptg2_manifest_publish_progress(
            "serving sidecars export by code complete",
            done=2,
            total=8,
            message="serving rows ordered by code exported",
            copy_bytes=_path_byte_count(by_code_copy),
            expected_row_count=expected_row_count,
        )
        _emit_ptg2_manifest_publish_progress(
            "serving sidecars encode by code",
            done=3,
            total=8,
            message="encoding serving_by_code sidecar",
            copy_bytes=_path_byte_count(by_code_copy),
        )
        await asyncio.to_thread(
            _run_serving_sidecar_from_key_copy,
            "by_code",
            by_code_copy,
            by_code_path,
        )
        _emit_ptg2_manifest_publish_progress(
            "serving sidecars encode by code complete",
            done=4,
            total=8,
            message="serving_by_code sidecar encoded",
            sidecar_bytes=_path_byte_count(by_code_path),
        )
        _emit_ptg2_manifest_publish_progress(
            "serving sidecars export reverse",
            done=5,
            total=8,
            message="exporting serving rows ordered by provider set",
            expected_row_count=expected_row_count,
        )
        await _copy_ptg2_query_to_file(
            f"""
            SELECT provider_set_key, code_key, provider_count, price_set_global_id_128
            FROM {qualified_table}
            ORDER BY provider_set_key, code_key, price_set_global_id_128
            """,
            by_provider_set_copy,
        )
        _emit_ptg2_manifest_publish_progress(
            "serving sidecars export reverse complete",
            done=6,
            total=8,
            message="serving rows ordered by provider set exported",
            copy_bytes=_path_byte_count(by_provider_set_copy),
            expected_row_count=expected_row_count,
        )
        _emit_ptg2_manifest_publish_progress(
            "serving sidecars encode reverse",
            done=7,
            total=8,
            message="encoding serving_by_provider_set sidecar",
            copy_bytes=_path_byte_count(by_provider_set_copy),
        )
        await asyncio.to_thread(
            _run_serving_sidecar_from_key_copy,
            "by_provider_set",
            by_provider_set_copy,
            by_provider_set_path,
        )
        _emit_ptg2_manifest_publish_progress(
            "serving sidecars complete",
            done=8,
            total=8,
            message="serving sidecars encoded",
            serving_by_code_bytes=_path_byte_count(by_code_path),
            serving_by_provider_set_bytes=_path_byte_count(by_provider_set_path),
            expected_row_count=expected_row_count,
        )
    finally:
        by_code_copy.unlink(missing_ok=True)
        by_provider_set_copy.unlink(missing_ok=True)

    by_code = _existing_serving_sidecar_path_entry(
        name="serving_by_code",
        path=by_code_path,
        magic=PTG2_SERVING_BY_CODE_MAGIC,
        expected_format=PTG2_SERVING_BY_CODE_FORMAT,
        kind=PTG2_SERVING_BY_CODE_ARTIFACT_KIND,
        expected_row_count=expected_row_count,
    )
    by_provider_set = _existing_serving_sidecar_path_entry(
        name="serving_by_provider_set",
        path=by_provider_set_path,
        magic=PTG2_SERVING_BY_PROVIDER_SET_MAGIC,
        expected_format=PTG2_SERVING_BY_PROVIDER_SET_FORMAT,
        kind=PTG2_SERVING_BY_PROVIDER_SET_ARTIFACT_KIND,
        expected_row_count=expected_row_count,
    )
    if by_code is None or by_provider_set is None:
        raise RuntimeError("PTG2 Rust serving sidecar encoder did not produce valid sidecar files")
    return {
        "serving_by_code": by_code,
        "serving_by_provider_set": by_provider_set,
    }


async def _write_ptg2_manifest_serving_sidecars(
    *,
    schema_name: str,
    final_table: str,
    artifacts: Mapping[str, Any] | None,
    source_key: str,
    snapshot_id: str,
    expected_row_count: int | None = None,
) -> dict[str, Any]:
    """Write serving sidecars with Rust, falling back to Python streaming."""
    if not _is_serving_sidecars_enabled():
        return {}
    artifact_dir = _ptg2_manifest_serving_sidecar_dir(
        artifacts=artifacts,
        source_key=source_key,
        snapshot_id=snapshot_id,
    )
    artifact_dir.mkdir(parents=True, exist_ok=True)
    qualified_table = f"{_quote_ident(schema_name)}.{_quote_ident(final_table)}"
    try:
        rust_sidecars = await _write_serving_sidecars_rust(
            schema_name=schema_name,
            final_table=final_table,
            artifact_dir=artifact_dir,
            expected_row_count=expected_row_count,
        )
        if rust_sidecars:
            return rust_sidecars
    except Exception:
        logger.warning("PTG2 Rust serving sidecar generation failed; falling back to Python row streaming", exc_info=True)
    by_code_sql = f"""
        SELECT code_key, provider_set_key, provider_count, price_set_global_id_128
        FROM {qualified_table}
        ORDER BY code_key, provider_set_key, price_set_global_id_128
    """
    by_provider_set_sql = f"""
        SELECT provider_set_key, code_key, provider_count, price_set_global_id_128
        FROM {qualified_table}
        ORDER BY provider_set_key, code_key, price_set_global_id_128
    """
    _emit_ptg2_manifest_publish_progress(
        "serving sidecars python by code",
        done=1,
        total=4,
        message="streaming serving_by_code sidecar in Python",
        expected_row_count=expected_row_count,
    )
    by_code = await write_serving_by_code_sidecar_async(
        artifact_dir / "serving_by_code_v1.ptg2sbc",
        _iter_ptg2_serving_sidecar_rows(by_code_sql),
        name="serving_by_code",
        expected_row_count=expected_row_count,
    )
    _emit_ptg2_manifest_publish_progress(
        "serving sidecars python by code complete",
        done=2,
        total=4,
        message="serving_by_code sidecar streamed in Python",
        sidecar_bytes=by_code.get("byte_count") if isinstance(by_code, Mapping) else None,
    )
    _emit_ptg2_manifest_publish_progress(
        "serving sidecars python reverse",
        done=3,
        total=4,
        message="streaming serving_by_provider_set sidecar in Python",
        expected_row_count=expected_row_count,
    )
    by_provider_set = await write_serving_by_provider_set_sidecar_async(
        artifact_dir / "serving_by_provider_set_v1.ptg2sbp",
        _iter_ptg2_serving_sidecar_rows(by_provider_set_sql),
        name="serving_by_provider_set",
        expected_row_count=expected_row_count,
    )
    _emit_ptg2_manifest_publish_progress(
        "serving sidecars python complete",
        done=4,
        total=4,
        message="serving sidecars streamed in Python",
        serving_by_code_bytes=by_code.get("byte_count") if isinstance(by_code, Mapping) else None,
        serving_by_provider_set_bytes=(
            by_provider_set.get("byte_count") if isinstance(by_provider_set, Mapping) else None
        ),
    )
    return {
        "serving_by_code": by_code,
        "serving_by_provider_set": by_provider_set,
    }


def _postgresql_artifact_manifest_entry(entry: Mapping[str, Any]) -> dict[str, Any]:
    """Remove transient file locations from a PostgreSQL-owned artifact entry."""

    manifest_entry_map = dict(entry)
    if (
        ptg2_artifact_id_from_db_uri(str(manifest_entry_map.get("storage_uri") or ""))
        and not ptg2_artifact_db_retain_local_cache()
    ):
        manifest_entry_map.pop("path", None)
        manifest_entry_map.pop("cache_path", None)
    return manifest_entry_map


async def _store_sidecar_artifacts_in_db(
    *,
    schema_name: str,
    snapshot_id: str,
    sidecar_artifacts: Mapping[str, Any] | None,
    require_db_storage: bool = False,
) -> dict[str, Any]:
    """Store eligible sidecars in PostgreSQL and return durable manifest entries."""
    if not sidecar_artifacts or (
        not require_db_storage and not ptg2_artifact_db_store_enabled()
    ):
        return dict(sidecar_artifacts or {})

    uploaded_by_key: dict[tuple[str, str, str], dict[str, Any]] = {}
    upload_total = _ptg2_manifest_sidecar_upload_count(sidecar_artifacts)
    upload_progress_by_name = {"done": 0}

    async def upload_entry(
        default_name: str, artifact_data: Any
    ) -> dict[str, Any] | None:
        """Store one local artifact once and return its durable manifest form."""
        if not isinstance(artifact_data, Mapping):
            return None
        artifact_entry_map = dict(artifact_data)
        storage_uri = str(artifact_entry_map.get("storage_uri") or "").strip()
        if ptg2_artifact_id_from_db_uri(storage_uri):
            return _postgresql_artifact_manifest_entry(artifact_entry_map)
        raw_path = str(artifact_entry_map.get("path") or "").strip()
        if not raw_path:
            return artifact_entry_map
        path = Path(raw_path)
        artifact_name = (
            str(artifact_entry_map.get("name") or default_name).strip()
            or default_name
        )
        if (
            require_db_storage
            and artifact_name in _PROVIDER_MEMBERSHIP_GRAPH_ARTIFACT_NAMES
        ):
            artifact_entry_map.update(membership_index_fence_metadata(path))
        if require_db_storage and artifact_name in _PROVIDER_MEMBERSHIP_GRAPH_ARTIFACT_NAMES:
            artifact_entry_map["chunk_bytes"] = (
                PTG2_PROVIDER_MEMBERSHIP_GRAPH_CHUNK_BYTES
            )
        if upload_total:
            _emit_ptg2_manifest_publish_progress(
                "artifact upload",
                done=upload_progress_by_name["done"],
                total=upload_total,
                message=f"uploading {artifact_name} sidecar to PostgreSQL",
                pct=_MANIFEST_PUBLISH_DETAIL_END_PCT,
                artifact_name=artifact_name,
                artifact_bytes=_path_byte_count(path),
            )
        cache_key = (
            str(path),
            str(artifact_entry_map.get("sha256") or ""),
            artifact_name,
        )
        cached = uploaded_by_key.get(cache_key)
        if cached is not None:
            merged_entry_map = dict(cached)
            merged_entry_map["name"] = artifact_name
            if "source_shard_id" in artifact_entry_map:
                merged_entry_map["source_shard_id"] = artifact_entry_map[
                    "source_shard_id"
                ]
            if upload_total:
                upload_progress_by_name["done"] += 1
                _emit_ptg2_manifest_publish_progress(
                    "artifact upload reused",
                    done=upload_progress_by_name["done"],
                    total=upload_total,
                    message=f"reused uploaded {artifact_name} sidecar",
                    pct=_MANIFEST_PUBLISH_DETAIL_END_PCT,
                    artifact_name=artifact_name,
                    artifact_bytes=merged_entry_map.get("byte_count"),
                    artifact_chunks=_artifact_chunk_count(merged_entry_map),
                )
            return merged_entry_map
        if not path.exists() or path.stat().st_size <= 0:
            if upload_total:
                upload_progress_by_name["done"] += 1
                _emit_ptg2_manifest_publish_progress(
                    "artifact upload skipped",
                    done=upload_progress_by_name["done"],
                    total=upload_total,
                    message=f"skipped missing {artifact_name} sidecar upload",
                    pct=_MANIFEST_PUBLISH_DETAIL_END_PCT,
                    artifact_name=artifact_name,
                )
            return artifact_entry_map
        uploaded = await store_ptg2_artifact_file_in_db(
            path,
            snapshot_id=snapshot_id,
            artifact_kind=str(artifact_entry_map.get("kind") or artifact_name),
            name=artifact_name,
            schema_name=schema_name,
            metadata=artifact_entry_map,
        )
        uploaded = _postgresql_artifact_manifest_entry(uploaded)
        uploaded_by_key[cache_key] = uploaded
        if upload_total:
            upload_progress_by_name["done"] += 1
            _emit_ptg2_manifest_publish_progress(
                "artifact upload complete",
                done=upload_progress_by_name["done"],
                total=upload_total,
                message=f"uploaded {artifact_name} sidecar to PostgreSQL",
                pct=_MANIFEST_PUBLISH_DETAIL_END_PCT,
                artifact_name=artifact_name,
                artifact_bytes=uploaded.get("byte_count"),
                artifact_chunks=_artifact_chunk_count(uploaded),
                storage_uri=uploaded.get("storage_uri"),
            )
        return dict(uploaded)

    stored_artifact_map: dict[str, Any] = {}
    for name, artifact_data in sidecar_artifacts.items():
        if name == "sidecars" and isinstance(artifact_data, list):
            sidecars: list[Any] = []
            for index, sidecar in enumerate(artifact_data):
                sidecars.append(await upload_entry(f"sidecar_{index}", sidecar) or sidecar)
            stored_artifact_map[name] = sidecars
            continue
        stored_entry = await upload_entry(str(name), artifact_data)
        stored_artifact_map[name] = (
            stored_entry if stored_entry is not None else artifact_data
        )
    return stored_artifact_map


def _merge_ptg2_manifest_sidecar_artifacts(
    *sidecar_artifacts: Mapping[str, Any] | None,
) -> dict[str, Any]:
    combined_artifact_map: dict[str, Any] = {}
    for artifacts in sidecar_artifacts:
        if not artifacts:
            continue
        for name, value in artifacts.items():
            if name == "sidecars" and isinstance(value, list):
                existing = combined_artifact_map.get("sidecars")
                if isinstance(existing, list):
                    existing.extend(value)
                elif existing is None:
                    combined_artifact_map["sidecars"] = list(value)
                else:
                    combined_artifact_map["sidecars"] = [existing, *value]
                continue
            combined_artifact_map[name] = value
    return combined_artifact_map


def _ptg2_sidecar_entry_name(name: str, value: Any) -> str:
    if isinstance(value, Mapping):
        return str(value.get("name") or value.get("kind") or name).strip()
    return str(name or "").strip()


def _provider_membership_graph_version(arch_version: str) -> str:
    """Return the graph contract actually emitted by one snapshot architecture."""

    if _is_postgres_binary_v3_arch(arch_version):
        return PTG2_PROVIDER_MEMBERSHIP_GRAPH_VERSION
    return _PROVIDER_MEMBERSHIP_GRAPH_V2_VERSION


def _record_v3_graph_shard(
    observed_names_by_shard: dict[str, set[str]],
    metadata: Mapping[str, Any],
    artifact_name: str,
) -> None:
    """Track one graph direction without allowing shard identity loss."""

    source_shard_id = str(metadata.get("source_shard_id") or "").strip()
    if not source_shard_id:
        raise RuntimeError(
            f"PTG2 postgres_binary_v3 provider graph artifact {artifact_name!r} lacks source shard identity"
        )
    observed_names = observed_names_by_shard.setdefault(source_shard_id, set())
    if artifact_name in observed_names:
        raise RuntimeError(
            f"PTG2 postgres_binary_v3 provider graph source shard {source_shard_id!r} "
            f"has duplicate {artifact_name!r} artifacts"
        )
    observed_names.add(artifact_name)


def _require_complete_v3_graph_shards(
    observed_names_by_shard: Mapping[str, set[str]],
) -> None:
    """Require all graph directions independently for every source shard."""

    if not observed_names_by_shard:
        raise RuntimeError(
            "PTG2 postgres_binary_v3 provider graph is incomplete; no source shards"
        )
    for source_shard_id, observed_names in observed_names_by_shard.items():
        missing_names = _PROVIDER_MEMBERSHIP_GRAPH_ARTIFACT_NAMES - observed_names
        if missing_names:
            raise RuntimeError(
                f"PTG2 postgres_binary_v3 provider graph source shard {source_shard_id!r} "
                "is incomplete; missing "
                + ", ".join(sorted(missing_names))
            )


def _retain_v3_provider_graph_artifacts(
    sidecar_artifacts: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Keep every provider-graph shard and omit v3-obsolete sidecars."""

    artifact_by_name: dict[str, Any] = {}
    observed_names_by_shard: dict[str, set[str]] = {}
    for name, artifact_metadata in dict(sidecar_artifacts or {}).items():
        if name == "sidecars" and isinstance(artifact_metadata, list):
            retained_sidecars = []
            for sidecar_metadata in artifact_metadata:
                artifact_name = _ptg2_sidecar_entry_name("", sidecar_metadata)
                if (
                    isinstance(sidecar_metadata, Mapping)
                    and artifact_name in _PROVIDER_MEMBERSHIP_GRAPH_ARTIFACT_NAMES
                ):
                    retained_sidecar_map = dict(sidecar_metadata)
                    retained_sidecars.append(retained_sidecar_map)
                    _record_v3_graph_shard(
                        observed_names_by_shard,
                        retained_sidecar_map,
                        artifact_name,
                    )
            if retained_sidecars:
                artifact_by_name[name] = retained_sidecars
            continue
        artifact_name = _ptg2_sidecar_entry_name(str(name), artifact_metadata)
        if (
            isinstance(artifact_metadata, Mapping)
            and artifact_name in _PROVIDER_MEMBERSHIP_GRAPH_ARTIFACT_NAMES
        ):
            retained_artifact_map = dict(artifact_metadata)
            artifact_by_name[str(name)] = retained_artifact_map
            _record_v3_graph_shard(
                observed_names_by_shard,
                retained_artifact_map,
                artifact_name,
            )
    _require_complete_v3_graph_shards(observed_names_by_shard)
    return artifact_by_name


def _v3_graph_db_entry(
    default_name: str,
    artifact_metadata: Any,
    observed_names_by_shard: dict[str, set[str]],
) -> dict[str, Any]:
    if not isinstance(artifact_metadata, Mapping):
        raise RuntimeError("PTG2 postgres_binary_v3 provider graph entry is not metadata")
    metadata = dict(artifact_metadata)
    try:
        artifact_name = validate_v3_graph_db_entry(metadata, default_name)
    except PTG2ManifestArtifactError as exc:
        raise RuntimeError(str(exc)) from exc
    _record_v3_graph_shard(observed_names_by_shard, metadata, artifact_name)
    metadata.pop("path", None)
    metadata.pop("cache_path", None)
    metadata.pop("local_path", None)
    return metadata


def _require_v3_graph_db_artifacts(
    sidecar_artifacts: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Validate DB ownership and remove runtime filesystem references."""

    stored_artifact_by_name: dict[str, Any] = {}
    observed_names_by_shard: dict[str, set[str]] = {}

    for name, value in dict(sidecar_artifacts or {}).items():
        if name == "sidecars" and isinstance(value, list):
            stored_artifact_by_name[name] = [
                _v3_graph_db_entry(
                    f"sidecar_{index}",
                    sidecar,
                    observed_names_by_shard,
                )
                for index, sidecar in enumerate(value)
            ]
            continue
        stored_artifact_by_name[name] = _v3_graph_db_entry(
            str(name), value, observed_names_by_shard
        )
    _require_complete_v3_graph_shards(observed_names_by_shard)
    return stored_artifact_by_name


def _omit_price_forward_artifacts(
    sidecar_artifacts: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Return sidecar artifacts without the duplicate price_forward artifact."""

    artifacts_by_name: dict[str, Any] = {}
    for name, value in dict(sidecar_artifacts or {}).items():
        if name == "sidecars" and isinstance(value, list):
            retained_sidecars = [
                dict(item)
                for item in value
                if not (
                    isinstance(item, Mapping)
                    and str(item.get("name") or item.get("kind") or "").strip() == "price_forward"
                )
            ]
            if retained_sidecars:
                artifacts_by_name[name] = retained_sidecars
            continue
        if _ptg2_sidecar_entry_name(str(name), value) == "price_forward":
            continue
        artifacts_by_name[str(name)] = value
    return artifacts_by_name


def _has_serving_binary_price_atoms(serving_binary_manifest: Mapping[str, Any] | None) -> bool:
    """Return true when serving-binary blocks include price-set atom mappings."""

    if not isinstance(serving_binary_manifest, Mapping):
        return False
    if _is_postgres_binary_v3_arch(serving_binary_manifest.get("arch_version")):
        membership_summary = serving_binary_manifest.get("price_set_atom_memberships_v3")
        atom_summary = serving_binary_manifest.get("price_atoms_v3")
        return isinstance(membership_summary, Mapping) and isinstance(atom_summary, Mapping)
    price_set_atoms = serving_binary_manifest.get("price_set_atoms")
    if not isinstance(price_set_atoms, Mapping):
        return False
    return int(price_set_atoms.get("price_set_count") or 0) > 0


def _serving_binary_atom_key_bits(serving_binary_manifest: Mapping[str, Any] | None) -> int | None:
    if not isinstance(serving_binary_manifest, Mapping):
        return None
    dense_atom_keys = serving_binary_manifest.get("dense_atom_keys")
    if not isinstance(dense_atom_keys, Mapping):
        return None
    try:
        return int(dense_atom_keys.get("atom_key_bits"))
    except (TypeError, ValueError):
        return None


def _split_ptg2_manifest_base_artifacts(
    artifacts: Mapping[str, Any] | None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    base_artifact_map = dict(artifacts or {})
    raw_sidecars = base_artifact_map.pop("sidecars", None)
    if isinstance(raw_sidecars, list):
        return base_artifact_map, {"sidecars": list(raw_sidecars)}
    if isinstance(raw_sidecars, Mapping):
        return base_artifact_map, dict(raw_sidecars)
    if raw_sidecars:
        return base_artifact_map, {"sidecars": [raw_sidecars]}
    return base_artifact_map, {}


def _manifest_sql_id(value: bytes) -> str:
    if _ptg2_id_storage() == "uuid":
        return str(uuid.UUID(bytes=value))
    return value.hex()


def _write_manifest_component_copy(
    sidecar_path: Path,
    sidecar_entry: Mapping[str, Any],
) -> tuple[Path | None, int]:
    tmp_path: Path | None = None
    row_count = 0
    with tempfile.NamedTemporaryFile("wb", delete=False) as component_file:
        tmp_path = Path(component_file.name)
        for sidecar_owner in read_global_sidecar_entries(sidecar_path, metadata=sidecar_entry):
            provider_group_id = _manifest_sql_id(sidecar_owner.owner)
            for member in sidecar_owner.members:
                provider_set_id = _manifest_sql_id(member)
                component_file.write(f"{provider_set_id}\t{provider_group_id}\n".encode("ascii"))
                row_count += 1
    return tmp_path, row_count


async def _index_manifest_components(schema_name: str, table_name: str) -> None:
    primary_index = _ptg2_snapshot_index_name(table_name, "primary")
    group_index = _ptg2_snapshot_index_name(table_name, "group_idx")
    await db.status(
        f"""
        CREATE UNIQUE INDEX {_quote_ident(primary_index)}
        ON {_quote_ident(schema_name)}.{_quote_ident(table_name)}
        (provider_set_global_id_128, provider_group_global_id_128);
        """
    )
    await db.status(
        f"""
        CREATE INDEX {_quote_ident(group_index)}
        ON {_quote_ident(schema_name)}.{_quote_ident(table_name)}
        (provider_group_global_id_128, provider_set_global_id_128);
        """
    )


async def _create_optional_manifest_provider_geo_index(
    *,
    schema_name: str,
    provider_group_location_table: str,
) -> None:
    try:
        await db.status(
            f"CREATE INDEX IF NOT EXISTS {_quote_ident(_ptg2_snapshot_index_name(provider_group_location_table, 'geo_gist_idx'))} "
            f"ON {_quote_ident(schema_name)}.{_quote_ident(provider_group_location_table)} "
            "USING gist (geography(st_makepoint(long::float8, lat::float8))) "
            "WHERE lat IS NOT NULL AND long IS NOT NULL;"
        )
    except Exception as exc:  # pragma: no cover - exact driver exception varies by environment.
        logger.warning(
            "Skipping optional PTG2 manifest provider geo GiST index for %s.%s: %s",
            schema_name,
            provider_group_location_table,
            exc,
        )


async def _create_inferred_taxonomy_zip_indexes(
    *,
    schema_name: str,
    provider_group_location_table: str,
    group_column: str,
) -> None:
    for idx, rule in enumerate(INFERRED_PROVIDER_TAXONOMY_RULES):
        if not rule.taxonomy_codes or len(rule.taxonomy_codes) > _MAX_PARTIAL_ZIP_TAXONOMY_INDEX_CODES:
            continue
        try:
            taxonomy_rows = await db.all(
                f"""
                SELECT int_code
                  FROM {_quote_ident(schema_name)}.nucc_taxonomy
                 WHERE code = ANY(:taxonomy_codes)
                   AND int_code IS NOT NULL
                 ORDER BY int_code
                """,
                taxonomy_codes=list(rule.taxonomy_codes),
            )
            int_codes = sorted(
                {
                    int(int_code_value)
                    for taxonomy_row in taxonomy_rows
                    if (
                        int_code_value := _row_value(taxonomy_row, "int_code")
                    )
                    is not None
                }
            )
            if not int_codes:
                continue
            taxonomy_sql = "ARRAY[" + ",".join(str(code) for code in int_codes) + "]::integer[]"
            await db.status(
                f"CREATE INDEX IF NOT EXISTS {_quote_ident(_ptg2_snapshot_index_name(provider_group_location_table, f'zip_taxonomy_rule_{idx}_idx'))} "
                f"ON {_qualified_table(schema_name, provider_group_location_table)} "
                f"(zip5, address_type, {group_column}, npi, address_checksum) "
                f"WHERE npi IS NOT NULL AND taxonomy_array && {taxonomy_sql};"
            )
        except Exception as exc:  # pragma: no cover - exact driver exception varies by environment.
            logger.warning(
                "Skipping optional PTG2 inferred-taxonomy zip index for %s.%s rule=%s: %s",
                schema_name,
                provider_group_location_table,
                idx,
                exc,
            )


def _qualified_table(schema_name: str, table_name: str) -> str:
    return f"{_quote_ident(schema_name)}.{_quote_ident(table_name)}"


def _quote_sql_literal(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


async def _create_provider_group_location_table(
    *,
    schema_name: str,
    provider_group_location_table: str,
) -> None:
    id_type = _ptg2_id_sql_type()
    qualified_location_table = _qualified_table(schema_name, provider_group_location_table)
    await db.status(f"DROP TABLE IF EXISTS {qualified_location_table} CASCADE;")
    await db.status(
        _PROVIDER_GROUP_LOCATION_CREATE_SQL.format(
            qualified_location_table=qualified_location_table,
            id_type=id_type,
        )
    )


async def _populate_provider_group_locations(
    *,
    schema_name: str,
    provider_group_member_table: str,
    provider_group_location_table: str,
) -> None:
    await db.status(
        _PROVIDER_GROUP_LOCATION_INSERT_SQL.format(
            qualified_location_table=_qualified_table(schema_name, provider_group_location_table),
            qualified_member_table=_qualified_table(schema_name, provider_group_member_table),
            qualified_entity_address_table=_qualified_table(schema_name, "entity_address_unified"),
        )
    )


async def _index_provider_group_locations(
    *,
    schema_name: str,
    provider_group_location_table: str,
) -> None:
    profile = _provider_group_location_index_profile()
    if profile == _PROVIDER_GROUP_LOCATION_INDEX_PROFILE_FULL:
        location_indexes = [
            ("group_zip_idx", "(provider_group_global_id_128, zip5, npi)"),
            ("zip_group_idx", "(zip5, provider_group_global_id_128, npi)"),
            (
                "zip_type_cover_idx",
                "(zip5, address_type, provider_group_global_id_128, npi, address_checksum) "
                "INCLUDE (taxonomy_array) WHERE npi IS NOT NULL",
            ),
            ("state_city_group_idx", "(state_name, city_name, provider_group_global_id_128, npi)"),
            ("state_city_npi_group_idx", "(state_name, city_name, npi, provider_group_global_id_128)"),
            (
                "group_state_city_npi_addr_idx",
                "(provider_group_global_id_128, state_name, city_name, npi, address_checksum)",
            ),
            ("npi_group_idx", "(npi, provider_group_global_id_128)"),
            ("group_npi_idx", "(provider_group_global_id_128, npi)"),
            (
                "lat_long_group_idx",
                "(lat, long, provider_group_global_id_128, npi) WHERE lat IS NOT NULL AND long IS NOT NULL",
            ),
            ("address_key_idx", "(address_key, provider_group_global_id_128, npi) WHERE address_key IS NOT NULL"),
            ("taxonomy_array_gin_idx", "USING gin (taxonomy_array gin__int_ops)"),
        ]
    else:
        location_indexes = [
            ("zip_group_idx", "(zip5, provider_group_global_id_128, npi)"),
            ("state_city_npi_group_idx", "(state_name, city_name, npi, provider_group_global_id_128)"),
            ("npi_group_idx", "(npi, provider_group_global_id_128)"),
            ("taxonomy_array_gin_idx", "USING gin (taxonomy_array gin__int_ops)"),
        ]
    for index_role, columns_sql in location_indexes:
        await db.status(
            f"CREATE INDEX IF NOT EXISTS {_quote_ident(_ptg2_snapshot_index_name(provider_group_location_table, index_role))} "
            f"ON {_qualified_table(schema_name, provider_group_location_table)} {columns_sql};"
        )
    if profile == _PROVIDER_GROUP_LOCATION_INDEX_PROFILE_FULL:
        await _create_optional_manifest_provider_geo_index(
            schema_name=schema_name,
            provider_group_location_table=provider_group_location_table,
        )
        await _create_inferred_taxonomy_zip_indexes(
            schema_name=schema_name,
            provider_group_location_table=provider_group_location_table,
            group_column="provider_group_global_id_128",
        )
    await db.status(f"ANALYZE {_qualified_table(schema_name, provider_group_location_table)};")


async def _build_manifest_provider_group_location_table(
    *,
    schema_name: str,
    provider_group_member_table: str,
    provider_group_location_table: str,
) -> str | None:
    """Materialize provider-group locations from unified addresses for manifest serving."""
    if not await _table_exists(schema_name, provider_group_member_table):
        return None
    await _create_provider_group_location_table(
        schema_name=schema_name,
        provider_group_location_table=provider_group_location_table,
    )
    await _populate_provider_group_locations(
        schema_name=schema_name,
        provider_group_member_table=provider_group_member_table,
        provider_group_location_table=provider_group_location_table,
    )
    await _index_provider_group_locations(
        schema_name=schema_name,
        provider_group_location_table=provider_group_location_table,
    )
    return provider_group_location_table


async def _materialize_manifest_components(
    *,
    schema_name: str,
    table_name: str,
    artifacts: Mapping[str, Any] | None,
    sidecar_artifacts: Mapping[str, Any] | None,
) -> str | None:
    """Persist provider-inverted sidecar membership as an indexed SQL table."""

    sidecar_entry = _manifest_provider_inverted_entry(sidecar_artifacts, artifacts)
    if not sidecar_entry:
        return None
    if not str(sidecar_entry.get("path") or "").strip():
        return None
    sidecar_path = _ptg2_manifest_publish_sidecar_path(sidecar_entry, artifacts)
    if not sidecar_path.exists() or sidecar_path.stat().st_size <= 0:
        return None

    id_type = _ptg2_id_sql_type()
    await db.status(f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(table_name)} CASCADE;")
    await db.status(
        f"""
        CREATE TABLE {_quote_ident(schema_name)}.{_quote_ident(table_name)} (
            provider_set_global_id_128 {id_type} NOT NULL,
            provider_group_global_id_128 {id_type} NOT NULL
        );
        """
    )

    copy_path: Path | None = None
    try:
        copy_path, row_count = _write_manifest_component_copy(sidecar_path, sidecar_entry)
        if row_count <= 0 or copy_path is None:
            await db.status(f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(table_name)} CASCADE;")
            return None
        await _copy_manifest_component_file(copy_path, target_table=table_name)
    finally:
        if copy_path is not None:
            try:
                copy_path.unlink(missing_ok=True)
            except OSError:
                logger.warning("Failed to remove PTG2 provider-set component copy file: %s", copy_path)

    await _index_manifest_components(schema_name, table_name)
    return table_name


async def _materialize_manifest_provider_group_rate_scope(
    *,
    schema_name: str,
    table_name: str,
    serving_table: str,
    code_count_table: str,
    provider_set_component_table: str,
    provider_set_dictionary_table: str | None,
    lean_provider_key_layout: bool,
) -> str | None:
    """Persist plan/code/provider-group membership for location filtering."""

    if not await _table_exists(schema_name, provider_set_component_table):
        return None
    if not await _table_exists(schema_name, serving_table):
        return None
    if lean_provider_key_layout and (
        not provider_set_dictionary_table
        or not await _table_exists(schema_name, provider_set_dictionary_table)
        or not await _table_exists(schema_name, code_count_table)
    ):
        return None

    storage_mode = "UNLOGGED " if _env_bool(PTG2_UNLOGGED_STAGE_ENV, True) else ""
    id_type = _ptg2_id_sql_type()
    qualified_table = _qualified_table(schema_name, table_name)
    await db.status(f"DROP TABLE IF EXISTS {qualified_table} CASCADE;")
    await db.status(
        f"""
        CREATE {storage_mode}TABLE {qualified_table} (
            plan_id varchar(64) NOT NULL,
            reported_code_system varchar(64),
            reported_code varchar(64),
            provider_group_global_id_128 {id_type} NOT NULL
        );
        """
    )
    if lean_provider_key_layout:
        await db.status(
            f"""
            INSERT INTO {qualified_table} (
                plan_id,
                reported_code_system,
                reported_code,
                provider_group_global_id_128
            )
            SELECT DISTINCT
                code_count.plan_id,
                code_count.reported_code_system,
                code_count.reported_code,
                component.provider_group_global_id_128
            FROM {_qualified_table(schema_name, serving_table)} serving
            JOIN {_qualified_table(schema_name, code_count_table)} code_count
              ON code_count.code_key = serving.code_key
            JOIN {_qualified_table(schema_name, provider_set_dictionary_table or "")} provider_set_dictionary
              ON provider_set_dictionary.provider_set_key = serving.provider_set_key
            JOIN {_qualified_table(schema_name, provider_set_component_table)} component
              ON component.provider_set_global_id_128 = provider_set_dictionary.provider_set_global_id_128;
            """
        )
    else:
        await db.status(
            f"""
            INSERT INTO {qualified_table} (
                plan_id,
                reported_code_system,
                reported_code,
                provider_group_global_id_128
            )
            SELECT DISTINCT
                serving.plan_id,
                serving.reported_code_system,
                serving.reported_code,
                component.provider_group_global_id_128
            FROM {_qualified_table(schema_name, serving_table)} serving
            JOIN {_qualified_table(schema_name, provider_set_component_table)} component
              ON component.provider_set_global_id_128 = serving.provider_set_global_id_128;
            """
        )

    if not await _table_has_rows(schema_name, table_name):
        await db.status(f"DROP TABLE IF EXISTS {qualified_table} CASCADE;")
        return None

    primary_index = _ptg2_snapshot_index_name(table_name, "primary")
    group_index = _ptg2_snapshot_index_name(table_name, "group_idx")
    await db.status(
        f"""
        CREATE UNIQUE INDEX {_quote_ident(primary_index)}
        ON {qualified_table}
        (plan_id, reported_code, reported_code_system, provider_group_global_id_128);
        """
    )
    await db.status(
        f"""
        CREATE INDEX {_quote_ident(group_index)}
        ON {qualified_table}
        (provider_group_global_id_128, plan_id, reported_code, reported_code_system);
        """
    )
    await db.status(f"ANALYZE {qualified_table};")
    return table_name


async def _dedupe_ptg2_manifest_serving_table(schema_name: str, final_table: str) -> dict[str, int]:
    serving_rows_before_dedupe = await _exact_table_rows(schema_name, final_table)
    dedup_table = _ptg2_snapshot_index_name(final_table, "dedup")
    await db.status(f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(dedup_table)};")
    await db.status(
        f"""
        CREATE UNLOGGED TABLE {_quote_ident(schema_name)}.{_quote_ident(dedup_table)} AS
        SELECT DISTINCT ON (serving_content_hash_128)
            serving_content_hash_128,
            plan_id,
            procedure_global_id_128,
            reported_code_system,
            reported_code,
            provider_set_global_id_128,
            provider_count,
            price_set_global_id_128,
            source_trace_set_hash,
            network_names
        FROM {_quote_ident(schema_name)}.{_quote_ident(final_table)}
        ORDER BY serving_content_hash_128, source_trace_set_hash NULLS LAST;
        """
    )
    await db.status(f"DROP TABLE {_quote_ident(schema_name)}.{_quote_ident(final_table)};")
    await db.status(
        f"""
        ALTER TABLE {_quote_ident(schema_name)}.{_quote_ident(dedup_table)}
        RENAME TO {_quote_ident(final_table)};
        """
    )
    serving_rows_after_dedupe = await _exact_table_rows(schema_name, final_table)
    return {
        "before": serving_rows_before_dedupe,
        "after": serving_rows_after_dedupe,
        "dropped": max(serving_rows_before_dedupe - serving_rows_after_dedupe, 0),
    }


def _known_or_deduped_serving_rows(
    *,
    known_counts: Mapping[str, int | None] | None,
    dedupe_metrics: Mapping[str, Any],
    serving_deduped: bool,
) -> int | None:
    """Return a trustworthy row count after any normal serving dedupe."""
    if dedupe_metrics.get("rescue"):
        return None
    if serving_deduped:
        serving_metrics = dedupe_metrics.get("serving")
        if not isinstance(serving_metrics, Mapping) or serving_metrics.get("after") is None:
            return None
        return max(int(serving_metrics["after"]), 0)
    known_serving_rows = (known_counts or {}).get("serving_rows")
    return max(int(known_serving_rows), 0) if known_serving_rows is not None else None


async def _dedupe_price_atom_table(schema_name: str, price_atom_table: str) -> dict[str, int]:
    price_atom_rows_before_dedupe = await _exact_table_rows(schema_name, price_atom_table)
    price_atom_dedup_table = _ptg2_snapshot_index_name(price_atom_table, "dedup")
    await db.status(f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(price_atom_dedup_table)};")
    await db.status(
        f"""
        CREATE UNLOGGED TABLE {_quote_ident(schema_name)}.{_quote_ident(price_atom_dedup_table)} AS
        SELECT DISTINCT ON (price_atom_global_id_128)
            price_atom_global_id_128,
            negotiated_type,
            negotiated_rate,
            expiration_date,
            service_code,
            billing_class,
            setting,
            billing_code_modifier,
            additional_information
        FROM {_quote_ident(schema_name)}.{_quote_ident(price_atom_table)}
        ORDER BY price_atom_global_id_128;
        """
    )
    await db.status(f"DROP TABLE {_quote_ident(schema_name)}.{_quote_ident(price_atom_table)};")
    await db.status(
        f"""
        ALTER TABLE {_quote_ident(schema_name)}.{_quote_ident(price_atom_dedup_table)}
        RENAME TO {_quote_ident(price_atom_table)};
        """
    )
    price_atom_rows_after_dedupe = await _exact_table_rows(schema_name, price_atom_table)
    return {
        "before": price_atom_rows_before_dedupe,
        "after": price_atom_rows_after_dedupe,
        "dropped": max(price_atom_rows_before_dedupe - price_atom_rows_after_dedupe, 0),
    }


def _price_atom_dictionary_columns_by_attr() -> dict[str, str]:
    """Map price-atom dictionary attributes to lean-table key columns."""
    return {
        "negotiated_type": "negotiated_type_key",
        "expiration_date": "expiration_date_key",
        "service_code": "service_code_key",
        "billing_class": "billing_class_key",
        "setting": "setting_key",
        "billing_code_modifier": "billing_code_modifier_key",
        "additional_information": "additional_information_key",
    }


async def _ptg2_price_atom_constant_metadata(
    *,
    schema_name: str,
    price_atom_dictionary_table: str,
) -> tuple[dict[str, int], dict[str, Any]]:
    """Return v2 constant dictionary keys and values that can move to metadata."""
    constant_key_by_column: dict[str, int] = {}
    constant_value_by_kind: dict[str, Any] = {}
    columns_by_attr = _price_atom_dictionary_columns_by_attr()
    constant_rows = await db.all(
        f"""
        SELECT
            dictionary.attr_kind,
            dictionary.attr_key::integer AS attr_key,
            dictionary.text_value,
            dictionary.text_array
          FROM {_quote_ident(schema_name)}.{_quote_ident(price_atom_dictionary_table)} dictionary
          JOIN (
            SELECT attr_kind
              FROM {_quote_ident(schema_name)}.{_quote_ident(price_atom_dictionary_table)}
             WHERE attr_kind IN (
                'negotiated_type',
                'expiration_date',
                'service_code',
                'billing_class',
                'setting',
                'billing_code_modifier',
                'additional_information'
             )
             GROUP BY attr_kind
            HAVING COUNT(*) = 1
          ) constant_attr
            ON constant_attr.attr_kind = dictionary.attr_kind;
        """
    )
    for constant_row in constant_rows:
        attr_kind = str(_row_value(constant_row, "attr_kind", 0) or "")
        column_name = columns_by_attr.get(attr_kind)
        if not column_name:
            continue
        constant_key_by_column[column_name] = int(_row_value(constant_row, "attr_key", 1) or 0)
        if attr_kind in {"service_code", "billing_code_modifier"}:
            constant_value_by_kind[attr_kind] = list(_row_value(constant_row, "text_array", 3) or [])
        else:
            constant_value_by_kind[attr_kind] = _row_value(constant_row, "text_value", 2)
    return constant_key_by_column, constant_value_by_kind


async def _rewrite_price_atom_lean_dictionary(
    *,
    schema_name: str,
    price_atom_table: str,
    price_atom_dictionary_table: str,
) -> dict[str, Any] | None:
    """Rewrite an existing price-atom table to a lean keyed layout."""
    if not await _table_exists(schema_name, price_atom_table):
        return None

    temporary_storage_mode = "UNLOGGED " if _env_bool(PTG2_UNLOGGED_STAGE_ENV, True) else ""
    id_type = _ptg2_id_sql_type()
    lean_table = _ptg2_snapshot_index_name(price_atom_table, "lean")
    keyed_dictionary_table = _ptg2_snapshot_index_name(price_atom_dictionary_table, "lookup")
    await db.status(
        f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(price_atom_dictionary_table)} CASCADE;"
    )
    await db.status(f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(lean_table)} CASCADE;")
    await db.status(f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(keyed_dictionary_table)} CASCADE;")
    await db.status(
        f"""
        CREATE {temporary_storage_mode}TABLE {_quote_ident(schema_name)}.{_quote_ident(price_atom_dictionary_table)} AS
        WITH dictionary_source AS (
            SELECT CASE
                       WHEN GROUPING(negotiated_type) = 0
                           THEN 'negotiated_type'::varchar(64)
                       WHEN GROUPING(expiration_date) = 0
                           THEN 'expiration_date'::varchar(64)
                       WHEN GROUPING(billing_class) = 0
                           THEN 'billing_class'::varchar(64)
                       ELSE 'setting'::varchar(64)
                   END AS attr_kind,
                   CASE
                       WHEN GROUPING(negotiated_type) = 0
                           THEN negotiated_type::text
                       WHEN GROUPING(expiration_date) = 0
                           THEN expiration_date::text
                       WHEN GROUPING(billing_class) = 0
                           THEN billing_class::text
                       ELSE setting::text
                   END AS text_value,
                   NULL::text[] AS text_array
              FROM {_quote_ident(schema_name)}.{_quote_ident(price_atom_table)}
             GROUP BY GROUPING SETS (
                 (negotiated_type),
                 (expiration_date),
                 (billing_class),
                 (setting)
             )
            UNION ALL
            SELECT 'service_code'::varchar(64), NULL::text, service_code::text[]
              FROM {_quote_ident(schema_name)}.{_quote_ident(price_atom_table)}
             GROUP BY service_code
            UNION ALL
            SELECT 'billing_code_modifier'::varchar(64), NULL::text, billing_code_modifier::text[]
              FROM {_quote_ident(schema_name)}.{_quote_ident(price_atom_table)}
             GROUP BY billing_code_modifier
            UNION ALL
            SELECT 'additional_information'::varchar(64), additional_information::text, NULL::text[]
              FROM {_quote_ident(schema_name)}.{_quote_ident(price_atom_table)}
             GROUP BY additional_information
        )
        SELECT
            attr_kind,
            (row_number() OVER (
                PARTITION BY attr_kind
                ORDER BY text_value NULLS FIRST, text_array NULLS FIRST
            ) - 1)::integer AS attr_key,
            text_value,
            text_array
        FROM dictionary_source;
        """
    )
    await db.status(
        f"""
        CREATE {temporary_storage_mode}TABLE {_quote_ident(schema_name)}.{_quote_ident(keyed_dictionary_table)} AS
        SELECT
            attr_kind,
            attr_key,
            text_value,
            text_array,
            CASE
                WHEN attr_kind IN ('service_code', 'billing_code_modifier') THEN NULL
                WHEN text_value IS NULL THEN 0::bigint
                ELSE hashtextextended(text_value, 0)
            END AS text_lookup_key,
            CASE
                WHEN attr_kind NOT IN ('service_code', 'billing_code_modifier') THEN NULL
                WHEN text_array IS NULL THEN 0::bigint
                ELSE hash_array_extended(text_array, 0)
            END AS array_lookup_key
        FROM {_quote_ident(schema_name)}.{_quote_ident(price_atom_dictionary_table)};
        """
    )
    if int(
        await db.scalar(
            f"""
            SELECT count(*)
              FROM (
                SELECT attr_kind, text_lookup_key AS lookup_key
                  FROM {_quote_ident(schema_name)}.{_quote_ident(keyed_dictionary_table)}
                 WHERE text_lookup_key IS NOT NULL
                 GROUP BY attr_kind, text_lookup_key
                HAVING count(*) > 1
                UNION ALL
                SELECT attr_kind, array_lookup_key
                  FROM {_quote_ident(schema_name)}.{_quote_ident(keyed_dictionary_table)}
                 WHERE array_lookup_key IS NOT NULL
                 GROUP BY attr_kind, array_lookup_key
                HAVING count(*) > 1
              ) collisions;
            """
        )
        or 0
    ) > 0:
        raise RuntimeError(
            f"PTG2 price atom dictionary lookup key collision in {schema_name}.{price_atom_dictionary_table}"
        )
    price_atom_layout = _ptg2_manifest_price_atom_layout()
    constant_key_by_column: dict[str, int] = {}
    constant_value_by_kind: dict[str, Any] = {}
    if price_atom_layout == PTG2_MANIFEST_PRICE_ATOM_LAYOUT_LEAN_DICT_V2:
        constant_key_by_column, constant_value_by_kind = await _ptg2_price_atom_constant_metadata(
            schema_name=schema_name,
            price_atom_dictionary_table=price_atom_dictionary_table,
        )
    keyed_text_index = _ptg2_snapshot_index_name(keyed_dictionary_table, "text_key_idx")
    keyed_array_index = _ptg2_snapshot_index_name(keyed_dictionary_table, "array_key_idx")
    await db.status(
        f"""
        CREATE UNIQUE INDEX {_quote_ident(keyed_text_index)}
        ON {_quote_ident(schema_name)}.{_quote_ident(keyed_dictionary_table)}
        (attr_kind, text_lookup_key)
        WHERE text_lookup_key IS NOT NULL;
        """
    )
    await db.status(
        f"""
        CREATE UNIQUE INDEX {_quote_ident(keyed_array_index)}
        ON {_quote_ident(schema_name)}.{_quote_ident(keyed_dictionary_table)}
        (attr_kind, array_lookup_key)
        WHERE array_lookup_key IS NOT NULL;
        """
    )
    await db.status(f"ANALYZE {_quote_ident(schema_name)}.{_quote_ident(keyed_dictionary_table)};")
    select_columns = [
        f"price_atom.price_atom_global_id_128::{id_type} AS price_atom_global_id_128",
        "price_atom.negotiated_rate::text AS negotiated_rate",
    ]
    join_sql_parts: list[str] = []
    dictionary_join_specs = [
        ("negotiated_type", "text_lookup_key", "negotiated_type"),
        ("expiration_date", "text_lookup_key", "expiration_date"),
        ("service_code", "array_lookup_key", "service_code"),
        ("billing_class", "text_lookup_key", "billing_class"),
        ("setting", "text_lookup_key", "setting"),
        ("billing_code_modifier", "array_lookup_key", "billing_code_modifier"),
        ("additional_information", "text_lookup_key", "additional_information"),
    ]
    for attr_kind, lookup_column, source_column in dictionary_join_specs:
        key_column = _price_atom_dictionary_columns_by_attr()[attr_kind]
        if key_column in constant_key_by_column:
            continue
        select_columns.append(f"{attr_kind}.attr_key::integer AS {key_column}")
        is_array_lookup = lookup_column == "array_lookup_key"
        source_lookup_expr = (
            f"hash_array_extended(price_atom.{source_column}, 0)"
            if is_array_lookup
            else f"hashtextextended(price_atom.{source_column}, 0)"
        )
        dictionary_value_column = "text_array" if is_array_lookup else "text_value"
        join_sql_parts.append(
            f"""
            JOIN {_quote_ident(schema_name)}.{_quote_ident(keyed_dictionary_table)} {attr_kind}
              ON {attr_kind}.attr_kind = '{attr_kind}'
             AND {attr_kind}.{lookup_column} = CASE
                    WHEN price_atom.{source_column} IS NULL THEN 0::bigint
                    ELSE {source_lookup_expr}
                 END
             AND {attr_kind}.{dictionary_value_column}
                    IS NOT DISTINCT FROM price_atom.{source_column}
            """
        )
    await db.status(
        f"""
        CREATE {temporary_storage_mode}TABLE {_quote_ident(schema_name)}.{_quote_ident(lean_table)} AS
        SELECT
            {", ".join(select_columns)}
        FROM {_quote_ident(schema_name)}.{_quote_ident(price_atom_table)} price_atom
        {" ".join(join_sql_parts)};
        """
    )
    not_null_columns = [
        "price_atom_global_id_128",
        *[
            column
            for column in _price_atom_dictionary_columns_by_attr().values()
            if column not in constant_key_by_column
        ],
    ]
    await db.status(
        f"""
        ALTER TABLE {_quote_ident(schema_name)}.{_quote_ident(lean_table)}
            {", ".join(f"ALTER COLUMN {column} SET NOT NULL" for column in not_null_columns)};
        """
    )
    await db.status(f"DROP TABLE {_quote_ident(schema_name)}.{_quote_ident(price_atom_table)};")
    await db.status(
        f"""
        ALTER TABLE {_quote_ident(schema_name)}.{_quote_ident(lean_table)}
        RENAME TO {_quote_ident(price_atom_table)};
        """
    )
    await db.status(
        f"ANALYZE {_quote_ident(schema_name)}.{_quote_ident(price_atom_table)};"
    )
    await db.status(f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(keyed_dictionary_table)};")
    should_keep_dictionary_table = (
        price_atom_layout != PTG2_MANIFEST_PRICE_ATOM_LAYOUT_LEAN_DICT_V2
        or len(constant_key_by_column) < len(_price_atom_dictionary_columns_by_attr())
    )
    if should_keep_dictionary_table and price_atom_layout != PTG2_MANIFEST_PRICE_ATOM_LAYOUT_LEAN_DICT_V2:
        dictionary_index = _ptg2_snapshot_index_name(price_atom_dictionary_table, "key_idx")
        await db.status(
            f"""
            CREATE UNIQUE INDEX {_quote_ident(dictionary_index)}
            ON {_quote_ident(schema_name)}.{_quote_ident(price_atom_dictionary_table)}
            (attr_kind, attr_key);
            """
        )
    if should_keep_dictionary_table:
        await db.status(f"ANALYZE {_quote_ident(schema_name)}.{_quote_ident(price_atom_dictionary_table)};")
    else:
        await db.status(
            f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(price_atom_dictionary_table)} CASCADE;"
        )
    return {
        "price_atom_table_layout": price_atom_layout,
        "price_atom_dictionary_table": (
            f"{schema_name}.{price_atom_dictionary_table}" if should_keep_dictionary_table else None
        ),
        "price_atom_constant_keys": constant_key_by_column,
        "price_atom_constant_values": constant_value_by_kind,
    }


async def _dedupe_provider_group_members(
    schema_name: str,
    provider_group_member_table: str,
) -> dict[str, int]:
    provider_group_member_rows_before_dedupe = await _exact_table_rows(schema_name, provider_group_member_table)
    provider_group_member_dedup_table = _ptg2_snapshot_index_name(provider_group_member_table, "dedup")
    await db.status(
        f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(provider_group_member_dedup_table)};"
    )
    await db.status(
        f"""
        CREATE UNLOGGED TABLE {_quote_ident(schema_name)}.{_quote_ident(provider_group_member_dedup_table)} AS
        SELECT DISTINCT ON (provider_group_global_id_128, npi)
            provider_group_global_id_128,
            npi
        FROM {_quote_ident(schema_name)}.{_quote_ident(provider_group_member_table)}
        ORDER BY provider_group_global_id_128, npi;
        """
    )
    await db.status(f"DROP TABLE {_quote_ident(schema_name)}.{_quote_ident(provider_group_member_table)};")
    await db.status(
        f"""
        ALTER TABLE {_quote_ident(schema_name)}.{_quote_ident(provider_group_member_dedup_table)}
        RENAME TO {_quote_ident(provider_group_member_table)};
        """
    )
    provider_group_member_rows_after_dedupe = await _exact_table_rows(schema_name, provider_group_member_table)
    return {
        "before": provider_group_member_rows_before_dedupe,
        "after": provider_group_member_rows_after_dedupe,
        "dropped": max(
            provider_group_member_rows_before_dedupe - provider_group_member_rows_after_dedupe,
            0,
        ),
    }


async def _dedupe_provider_npi_scope_table(
    schema_name: str,
    provider_npi_scope_table: str,
) -> dict[str, int]:
    """Delete duplicate NPI scope rows in PostgreSQL before unique indexing."""
    scope_rows_before = await _exact_table_rows(schema_name, provider_npi_scope_table)
    qualified_scope = f"{_quote_ident(schema_name)}.{_quote_ident(provider_npi_scope_table)}"
    await db.status(
        f"""
        DELETE FROM {qualified_scope} AS provider_scope
        USING (
            SELECT duplicate_ctid
            FROM (
                SELECT
                    ctid AS duplicate_ctid,
                    ROW_NUMBER() OVER (PARTITION BY npi ORDER BY ctid) AS duplicate_rank
                FROM {qualified_scope}
            ) ranked_scope
            WHERE duplicate_rank > 1
        ) duplicate_scope
        WHERE provider_scope.ctid = duplicate_scope.duplicate_ctid;
        """
    )
    scope_rows_after = await _exact_table_rows(schema_name, provider_npi_scope_table)
    return {
        "before": scope_rows_before,
        "after": scope_rows_after,
        "dropped": max(scope_rows_before - scope_rows_after, 0),
    }


async def _finalize_provider_npi_scope_table(
    schema_name: str,
    provider_npi_scope_table: str,
) -> dict[str, int]:
    dedupe_metrics = await _dedupe_provider_npi_scope_table(
        schema_name,
        provider_npi_scope_table,
    )
    provider_npi_scope_index = _ptg2_snapshot_index_name(provider_npi_scope_table, "npi_uidx")
    await db.status(
        f"""
        CREATE UNIQUE INDEX {_quote_ident(provider_npi_scope_index)}
        ON {_quote_ident(schema_name)}.{_quote_ident(provider_npi_scope_table)} (npi);
        """
    )
    return dedupe_metrics


def _coerce_network_names(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return [str(item) for item in value if str(item or "").strip()]
    return [str(value)] if str(value or "").strip() else []


async def _ptg2_manifest_serving_constants(
    schema_name: str,
    table_name: str,
) -> dict[str, Any] | None:
    rows = await db.all(
        f"""
        SELECT source_trace_set_hash, network_names
        FROM {_quote_ident(schema_name)}.{_quote_ident(table_name)}
        GROUP BY source_trace_set_hash, network_names
        LIMIT 2
        """
    )
    if len(rows) != 1:
        return None
    row = rows[0]
    return {
        "source_trace_set_hash": _row_value(row, "source_trace_set_hash", 0),
        "network_names": _coerce_network_names(_row_value(row, "network_names", 1)),
    }


def _manifest_constants_from_artifacts(
    artifacts: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    if not artifacts:
        return None
    source_trace_set_hash = str(artifacts.get("source_trace_set_hash") or "").strip()
    network_names = _coerce_network_names(artifacts.get("network_names"))
    if not source_trace_set_hash and not network_names:
        return None
    return {
        "source_trace_set_hash": source_trace_set_hash or None,
        "network_names": network_names,
    }


async def _rewrite_serving_lean_provider_key(
    *,
    schema_name: str,
    final_table: str,
    code_count_table: str,
    provider_set_dictionary_table: str,
) -> dict[str, Any] | None:
    """Rewrite a constant-scope serving table to a lean provider-key layout."""
    constants = await _ptg2_manifest_serving_constants(schema_name, final_table)
    if constants is None:
        logger.info(
            "PTG2 lean manifest serving layout skipped for %s.%s: source trace or network names vary by row",
            schema_name,
            final_table,
        )
        return None

    temporary_storage_mode = "UNLOGGED " if _env_bool(PTG2_UNLOGGED_STAGE_ENV, True) else ""
    id_type = _ptg2_id_sql_type()
    lean_table = _ptg2_snapshot_index_name(final_table, "lean")
    await db.status(f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(code_count_table)} CASCADE;")
    await db.status(
        f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(provider_set_dictionary_table)} CASCADE;"
    )
    await db.status(f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(lean_table)} CASCADE;")
    await db.status(
        f"""
        CREATE TABLE {_quote_ident(schema_name)}.{_quote_ident(code_count_table)} AS
        SELECT
            row_number() OVER (ORDER BY plan_id, reported_code_system, reported_code)::integer AS code_key,
            plan_id,
            reported_code_system,
            reported_code,
            COUNT(*)::bigint AS rate_count
        FROM {_quote_ident(schema_name)}.{_quote_ident(final_table)}
        GROUP BY plan_id, reported_code_system, reported_code;
        """
    )
    await db.status(
        f"""
        CREATE TABLE {_quote_ident(schema_name)}.{_quote_ident(provider_set_dictionary_table)} AS
        SELECT
            row_number() OVER (ORDER BY provider_set_global_id_128)::integer AS provider_set_key,
            provider_set_global_id_128
        FROM (
            SELECT DISTINCT provider_set_global_id_128
            FROM {_quote_ident(schema_name)}.{_quote_ident(final_table)}
        ) provider_sets;
        """
    )
    await db.status(
        f"""
        CREATE {temporary_storage_mode}TABLE {_quote_ident(schema_name)}.{_quote_ident(lean_table)} AS
        SELECT
            code_count.code_key,
            provider_set_dictionary.provider_set_key,
            serving.provider_count,
            serving.price_set_global_id_128
        FROM {_quote_ident(schema_name)}.{_quote_ident(final_table)} serving
        JOIN {_quote_ident(schema_name)}.{_quote_ident(code_count_table)} code_count
          ON code_count.plan_id = serving.plan_id
         AND code_count.reported_code_system IS NOT DISTINCT FROM serving.reported_code_system
         AND code_count.reported_code IS NOT DISTINCT FROM serving.reported_code
        JOIN {_quote_ident(schema_name)}.{_quote_ident(provider_set_dictionary_table)} provider_set_dictionary
          ON provider_set_dictionary.provider_set_global_id_128 = serving.provider_set_global_id_128;
        """
    )
    await db.status(f"DROP TABLE {_quote_ident(schema_name)}.{_quote_ident(final_table)};")
    await db.status(
        f"""
        ALTER TABLE {_quote_ident(schema_name)}.{_quote_ident(lean_table)}
        RENAME TO {_quote_ident(final_table)};
        """
    )
    lookup_index = _ptg2_snapshot_index_name(final_table, "lean_code_lookup_idx")
    code_lookup_index = _ptg2_snapshot_index_name(code_count_table, "lean_code_idx")
    provider_set_key_index = _ptg2_snapshot_index_name(provider_set_dictionary_table, "key_idx")
    provider_set_global_index = _ptg2_snapshot_index_name(provider_set_dictionary_table, "global_idx")
    await db.status(
        f"""
        CREATE INDEX {_quote_ident(code_lookup_index)}
        ON {_quote_ident(schema_name)}.{_quote_ident(code_count_table)}
        (reported_code_system, reported_code)
        INCLUDE (code_key, plan_id, rate_count);
        """
    )
    await db.status(
        f"""
        CREATE INDEX {_quote_ident(lookup_index)}
        ON {_quote_ident(schema_name)}.{_quote_ident(final_table)}
        (code_key);
        """
    )
    await db.status(
        f"""
        CREATE UNIQUE INDEX {_quote_ident(provider_set_key_index)}
        ON {_quote_ident(schema_name)}.{_quote_ident(provider_set_dictionary_table)}
        (provider_set_key)
        INCLUDE (provider_set_global_id_128);
        """
    )
    await db.status(
        f"""
        CREATE UNIQUE INDEX {_quote_ident(provider_set_global_index)}
        ON {_quote_ident(schema_name)}.{_quote_ident(provider_set_dictionary_table)}
        (provider_set_global_id_128)
        INCLUDE (provider_set_key);
        """
    )
    await db.status(f"ANALYZE {_quote_ident(schema_name)}.{_quote_ident(provider_set_dictionary_table)};")
    return {
        "serving_table_layout": PTG2_MANIFEST_SERVING_LAYOUT_LEAN_PROVIDER_KEY,
        "provider_set_dictionary_table": f"{schema_name}.{provider_set_dictionary_table}",
        "source_trace_set_hash": constants.get("source_trace_set_hash"),
        "network_names": constants.get("network_names") or [],
    }


async def _dedupe_lean_manifest_stage(schema_name: str, final_table: str) -> dict[str, int]:
    serving_rows_before_dedupe = await _exact_table_rows(schema_name, final_table)
    dedup_table = _ptg2_snapshot_index_name(final_table, "dedup")
    await db.status(f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(dedup_table)};")
    await db.status(
        f"""
        CREATE UNLOGGED TABLE {_quote_ident(schema_name)}.{_quote_ident(dedup_table)} AS
        SELECT DISTINCT ON (
            plan_id,
            reported_code_system,
            reported_code,
            provider_set_global_id_128,
            price_set_global_id_128
        )
            plan_id,
            reported_code_system,
            reported_code,
            provider_set_global_id_128,
            provider_count,
            price_set_global_id_128
        FROM {_quote_ident(schema_name)}.{_quote_ident(final_table)}
        ORDER BY
            plan_id,
            reported_code_system,
            reported_code,
            provider_set_global_id_128,
            price_set_global_id_128,
            provider_count DESC NULLS LAST;
        """
    )
    await db.status(f"DROP TABLE {_quote_ident(schema_name)}.{_quote_ident(final_table)};")
    await db.status(
        f"""
        ALTER TABLE {_quote_ident(schema_name)}.{_quote_ident(dedup_table)}
        RENAME TO {_quote_ident(final_table)};
        """
    )
    serving_rows_after_dedupe = await _exact_table_rows(schema_name, final_table)
    return {
        "before": serving_rows_before_dedupe,
        "after": serving_rows_after_dedupe,
        "dropped": max(serving_rows_before_dedupe - serving_rows_after_dedupe, 0),
    }


async def _build_direct_lean_code_counts(
    *,
    schema_name: str,
    final_table: str,
    code_count_table: str,
    storage_mode: str,
    code_count_stage_table: str | None = None,
) -> None:
    await db.status(f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(code_count_table)} CASCADE;")
    if code_count_stage_table and await _table_exists(schema_name, code_count_stage_table):
        await db.status(
            f"""
            CREATE {storage_mode}TABLE {_quote_ident(schema_name)}.{_quote_ident(code_count_table)} AS
            SELECT
                row_number() OVER (ORDER BY plan_id, reported_code_system, reported_code)::integer AS code_key,
                plan_id,
                reported_code_system,
                reported_code,
                SUM(rate_count)::bigint AS rate_count
            FROM {_quote_ident(schema_name)}.{_quote_ident(code_count_stage_table)}
            GROUP BY plan_id, reported_code_system, reported_code;
            """
        )
        await db.status(
            f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(code_count_stage_table)} CASCADE;"
        )
        return
    await db.status(
        f"""
        CREATE {storage_mode}TABLE {_quote_ident(schema_name)}.{_quote_ident(code_count_table)} AS
        SELECT
            row_number() OVER (ORDER BY plan_id, reported_code_system, reported_code)::integer AS code_key,
            plan_id,
            reported_code_system,
            reported_code,
            COUNT(*)::bigint AS rate_count
        FROM {_quote_ident(schema_name)}.{_quote_ident(final_table)}
        GROUP BY plan_id, reported_code_system, reported_code;
        """
    )


async def _build_direct_lean_provider_sets(
    *,
    schema_name: str,
    final_table: str,
    provider_set_dictionary_table: str,
    storage_mode: str,
    provider_set_dictionary_stage_table: str | None = None,
) -> None:
    await db.status(
        f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(provider_set_dictionary_table)} CASCADE;"
    )
    if provider_set_dictionary_stage_table and await _table_exists(schema_name, provider_set_dictionary_stage_table):
        await db.status(
            f"""
            CREATE {storage_mode}TABLE {_quote_ident(schema_name)}.{_quote_ident(provider_set_dictionary_table)} AS
            SELECT
                row_number() OVER (ORDER BY provider_set_global_id_128)::integer AS provider_set_key,
                provider_set_global_id_128
            FROM (
                SELECT DISTINCT provider_set_global_id_128
                FROM {_quote_ident(schema_name)}.{_quote_ident(provider_set_dictionary_stage_table)}
            ) provider_sets;
            """
        )
        await db.status(
            f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(provider_set_dictionary_stage_table)} CASCADE;"
        )
        return
    await db.status(
        f"""
        CREATE {storage_mode}TABLE {_quote_ident(schema_name)}.{_quote_ident(provider_set_dictionary_table)} AS
        SELECT
            row_number() OVER (ORDER BY provider_set_global_id_128)::integer AS provider_set_key,
            provider_set_global_id_128
        FROM (
            SELECT DISTINCT provider_set_global_id_128
            FROM {_quote_ident(schema_name)}.{_quote_ident(final_table)}
        ) provider_sets;
        """
    )


async def _swap_direct_lean_stage(
    *,
    schema_name: str,
    final_table: str,
    lean_table: str,
    code_count_table: str,
    provider_set_dictionary_table: str,
    storage_mode: str,
) -> None:
    await db.status(f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(lean_table)} CASCADE;")
    await db.status(
        f"""
        CREATE {storage_mode}TABLE {_quote_ident(schema_name)}.{_quote_ident(lean_table)} AS
        SELECT
            code_count.code_key,
            provider_set_dictionary.provider_set_key,
            serving.provider_count,
            serving.price_set_global_id_128
        FROM {_quote_ident(schema_name)}.{_quote_ident(final_table)} serving
        JOIN {_quote_ident(schema_name)}.{_quote_ident(code_count_table)} code_count
          ON code_count.plan_id = serving.plan_id
         AND code_count.reported_code_system IS NOT DISTINCT FROM serving.reported_code_system
         AND code_count.reported_code IS NOT DISTINCT FROM serving.reported_code
        JOIN {_quote_ident(schema_name)}.{_quote_ident(provider_set_dictionary_table)} provider_set_dictionary
          ON provider_set_dictionary.provider_set_global_id_128 = serving.provider_set_global_id_128;
        """
    )
    await db.status(f"DROP TABLE {_quote_ident(schema_name)}.{_quote_ident(final_table)};")
    await db.status(
        f"""
        ALTER TABLE {_quote_ident(schema_name)}.{_quote_ident(lean_table)}
        RENAME TO {_quote_ident(final_table)};
        """
    )


async def _index_direct_lean_tables(
    *,
    schema_name: str,
    final_table: str,
    code_count_table: str,
    provider_set_dictionary_table: str,
    create_serving_lookup_index: bool = True,
) -> None:
    lookup_index = _ptg2_snapshot_index_name(final_table, "lean_code_lookup_idx")
    code_lookup_index = _ptg2_snapshot_index_name(code_count_table, "lean_code_idx")
    provider_set_key_index = _ptg2_snapshot_index_name(provider_set_dictionary_table, "key_idx")
    provider_set_global_index = _ptg2_snapshot_index_name(provider_set_dictionary_table, "global_idx")
    await db.status(
        f"""
        CREATE INDEX {_quote_ident(code_lookup_index)}
        ON {_quote_ident(schema_name)}.{_quote_ident(code_count_table)}
        (reported_code_system, reported_code)
        INCLUDE (code_key, plan_id, rate_count);
        """
    )
    if create_serving_lookup_index:
        await db.status(
            f"""
            CREATE INDEX {_quote_ident(lookup_index)}
            ON {_quote_ident(schema_name)}.{_quote_ident(final_table)}
            (code_key);
            """
        )
    await db.status(
        f"""
        CREATE UNIQUE INDEX {_quote_ident(provider_set_key_index)}
        ON {_quote_ident(schema_name)}.{_quote_ident(provider_set_dictionary_table)}
        (provider_set_key)
        INCLUDE (provider_set_global_id_128);
        """
    )
    await db.status(
        f"""
        CREATE UNIQUE INDEX {_quote_ident(provider_set_global_index)}
        ON {_quote_ident(schema_name)}.{_quote_ident(provider_set_dictionary_table)}
        (provider_set_global_id_128)
        INCLUDE (provider_set_key);
        """
    )
    await db.status(f"ANALYZE {_quote_ident(schema_name)}.{_quote_ident(provider_set_dictionary_table)};")


async def _rewrite_direct_lean_manifest_stage(
    *,
    schema_name: str,
    final_table: str,
    code_count_table: str,
    provider_set_dictionary_table: str,
    constants: Mapping[str, Any] | None,
    code_count_stage_table: str | None = None,
    provider_set_dictionary_stage_table: str | None = None,
    create_serving_lookup_index: bool = True,
    materialize_serving_table: bool = True,
) -> dict[str, Any]:
    """Rewrite the narrow direct-copy stage into lean provider-key serving tables."""
    temporary_storage_mode = "UNLOGGED " if _env_bool(PTG2_UNLOGGED_STAGE_ENV, True) else ""
    lean_table = _ptg2_snapshot_index_name(final_table, "lean")
    if _use_parallel_dictionary_rewrite():
        await asyncio.gather(
            _build_direct_lean_code_counts(
                schema_name=schema_name,
                final_table=final_table,
                code_count_table=code_count_table,
                storage_mode="",
                code_count_stage_table=code_count_stage_table,
            ),
            _build_direct_lean_provider_sets(
                schema_name=schema_name,
                final_table=final_table,
                provider_set_dictionary_table=provider_set_dictionary_table,
                storage_mode="",
                provider_set_dictionary_stage_table=provider_set_dictionary_stage_table,
            ),
        )
    else:
        await _build_direct_lean_code_counts(
            schema_name=schema_name,
            final_table=final_table,
            code_count_table=code_count_table,
            storage_mode="",
            code_count_stage_table=code_count_stage_table,
        )
        await _build_direct_lean_provider_sets(
            schema_name=schema_name,
            final_table=final_table,
            provider_set_dictionary_table=provider_set_dictionary_table,
            storage_mode="",
            provider_set_dictionary_stage_table=provider_set_dictionary_stage_table,
        )
    if materialize_serving_table:
        await _swap_direct_lean_stage(
            schema_name=schema_name,
            final_table=final_table,
            lean_table=lean_table,
            code_count_table=code_count_table,
            provider_set_dictionary_table=provider_set_dictionary_table,
            storage_mode=temporary_storage_mode,
        )
    await _index_direct_lean_tables(
        schema_name=schema_name,
        final_table=final_table,
        code_count_table=code_count_table,
        provider_set_dictionary_table=provider_set_dictionary_table,
        create_serving_lookup_index=create_serving_lookup_index and materialize_serving_table,
    )
    return {
        "serving_table_layout": PTG2_MANIFEST_SERVING_LAYOUT_LEAN_PROVIDER_KEY,
        "provider_set_dictionary_table": f"{schema_name}.{provider_set_dictionary_table}",
        "source_trace_set_hash": (constants or {}).get("source_trace_set_hash"),
        "network_names": _coerce_network_names((constants or {}).get("network_names")),
        "serving_stage_layout": "lean_source_v1" if materialize_serving_table else "natural_lean_source_v1",
        "dictionary_source": (
            "scanner_support"
            if code_count_stage_table or provider_set_dictionary_stage_table
            else "serving_stage_scan"
        ),
    }


def _ptg2_manifest_artifacts_manifest(
    *,
    artifacts: Mapping[str, Any] | None = None,
    sidecar_artifacts: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    artifact_manifest_map = dict(artifacts or {})
    if not sidecar_artifacts:
        return artifact_manifest_map
    existing_sidecars = artifact_manifest_map.get("sidecars")
    sidecars: list[Any] = list(existing_sidecars) if isinstance(existing_sidecars, list) else []
    for name, sidecar_value in sidecar_artifacts.items():
        if sidecar_value is None:
            continue
        if name == "sidecars" and isinstance(sidecar_value, list):
            for index, sidecar_data in enumerate(sidecar_value):
                if isinstance(sidecar_data, Mapping):
                    sidecars.append(dict(sidecar_data))
                else:
                    sidecars.append(
                        {"name": f"sidecar_{index}", "path": str(sidecar_data)}
                    )
            continue
        if isinstance(sidecar_value, Mapping):
            artifact_entry_map = dict(sidecar_value)
            artifact_entry_map.setdefault("name", str(name))
            sidecars.append(artifact_entry_map)
        else:
            sidecars.append({"name": str(name), "path": str(sidecar_value)})
    artifact_manifest_map["sidecars"] = sidecars
    return artifact_manifest_map


PTG2_MANIFEST_LEAN_SOURCE_UNIQUE_GUARD_ENV = "HLTHPRT_PTG2_MANIFEST_LEAN_SOURCE_UNIQUE_GUARD"


def should_use_lean_source_guard(*, arch_version: str, skip_final_serving_table: bool) -> bool:
    """Return whether publish should build the transient lean-source uniqueness guard."""
    if os.getenv(PTG2_MANIFEST_LEAN_SOURCE_UNIQUE_GUARD_ENV) is not None:
        return _env_bool(PTG2_MANIFEST_LEAN_SOURCE_UNIQUE_GUARD_ENV, True)
    if _is_postgres_binary_snapshot_arch(arch_version) and skip_final_serving_table:
        return False
    return True
