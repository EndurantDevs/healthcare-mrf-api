# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PTG2 source-scoped manifest-backed serving stage and publish helpers."""

from __future__ import annotations

import logging
import os
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any, Mapping

from api.ptg2_code_filters import INFERRED_PROVIDER_TAXONOMY_RULES
from db.connection import db
from process.ptg_parts.config import (
    PTG2_BINARY_IDS_ENV, PTG2_MANIFEST_PUBLISH_DB_DEDUPE_FALLBACK_ENV,
    PTG2_UNLOGGED_STAGE_ENV, _env_bool)
from process.ptg_parts.db_tables import (_exact_table_rows, _quote_ident,
                                         _table_exists, _table_has_rows)
from process.ptg_parts.ptg2_manifest_artifacts import read_global_sidecar_entries
from process.ptg_parts.snapshot_tables import (_ptg2_snapshot_index_name,
                                               _ptg2_snapshot_table_name)

PTG2_MANIFEST_SERVING_COPY_ENV = "HLTHPRT_PTG2_MANIFEST_SERVING_COPY_PATH"
PTG2_MANIFEST_SERVING_LAYOUT_ENV = "HLTHPRT_PTG2_MANIFEST_SERVING_LAYOUT"
PTG2_MANIFEST_SERVING_LAYOUT_LEAN_PROVIDER_KEY = "lean_provider_key_v1"
PTG2_MANIFEST_PRICE_ATOM_LAYOUT_ENV = "HLTHPRT_PTG2_MANIFEST_PRICE_ATOM_LAYOUT"
PTG2_MANIFEST_PRICE_ATOM_LAYOUT_LEAN_DICT = "lean_dict_v1"
PTG2_MANIFEST_PROVIDER_GROUP_LOCATION_TABLE_ENV = "HLTHPRT_PTG2_MANIFEST_PROVIDER_GROUP_LOCATION_TABLE"
PTG2_MANIFEST_PROVIDER_SET_COMPONENT_TABLE_ENV = "HLTHPRT_PTG2_MANIFEST_PROVIDER_SET_COMPONENT_TABLE"
PTG2_MANIFEST_PROVIDER_GROUP_RATE_SCOPE_TABLE_ENV = "HLTHPRT_PTG2_MANIFEST_PROVIDER_GROUP_RATE_SCOPE_TABLE"
logger = logging.getLogger(__name__)
_MAX_PARTIAL_ZIP_TAXONOMY_INDEX_CODES = 12


def _row_value(row: Any, key: str, position: int = 0) -> Any:
    if isinstance(row, dict):
        return row.get(key)
    value = getattr(row, key, None)
    if value is not None:
        return value
    try:
        return row[position]
    except Exception:
        return None
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
PTG2_MANIFEST_PROVIDER_GROUP_MEMBER_COLUMNS = [
    "provider_group_global_id_128",
    "npi",
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
    return "uuid" if _env_bool(PTG2_BINARY_IDS_ENV, True) else "hex"


def _ptg2_id_sql_type() -> str:
    return "uuid" if _ptg2_id_storage() == "uuid" else "char(32)"


def _ptg2_manifest_serving_layout() -> str:
    return str(os.getenv(PTG2_MANIFEST_SERVING_LAYOUT_ENV) or "").strip().lower()


def _ptg2_manifest_price_atom_layout() -> str:
    configured = str(os.getenv(PTG2_MANIFEST_PRICE_ATOM_LAYOUT_ENV) or "").strip().lower()
    if configured:
        return configured
    if _ptg2_manifest_serving_layout() == PTG2_MANIFEST_SERVING_LAYOUT_LEAN_PROVIDER_KEY:
        return PTG2_MANIFEST_PRICE_ATOM_LAYOUT_LEAN_DICT
    return ""


def _ptg2_manifest_provider_group_location_enabled() -> bool:
    if os.getenv(PTG2_MANIFEST_PROVIDER_GROUP_LOCATION_TABLE_ENV) is not None:
        return _env_bool(PTG2_MANIFEST_PROVIDER_GROUP_LOCATION_TABLE_ENV, True)
    return _ptg2_manifest_serving_layout() != PTG2_MANIFEST_SERVING_LAYOUT_LEAN_PROVIDER_KEY


def _ptg2_manifest_provider_set_component_enabled() -> bool:
    if os.getenv(PTG2_MANIFEST_PROVIDER_SET_COMPONENT_TABLE_ENV) is not None:
        return _env_bool(PTG2_MANIFEST_PROVIDER_SET_COMPONENT_TABLE_ENV, True)
    if _ptg2_manifest_provider_group_rate_scope_enabled():
        return True
    return _ptg2_manifest_serving_layout() != PTG2_MANIFEST_SERVING_LAYOUT_LEAN_PROVIDER_KEY


def _ptg2_manifest_provider_group_rate_scope_enabled() -> bool:
    return _env_bool(PTG2_MANIFEST_PROVIDER_GROUP_RATE_SCOPE_TABLE_ENV, False)


def _ptg2_manifest_stage_table_name(token: str) -> str:
    safe_token = "".join(ch if ch.isalnum() else "_" for ch in token.lower()).strip("_")
    return f"ptg2_manifest_stage_serving_{safe_token}"[:63]


def _ptg2_manifest_stage_suffix(serving_stage_table: str) -> str:
    prefix = "ptg2_manifest_stage_serving_"
    return serving_stage_table[len(prefix):] if serving_stage_table.startswith(prefix) else serving_stage_table


def _ptg2_manifest_support_stage_table(serving_stage_table: str, kind: str) -> str:
    return f"ptg2_manifest_stage_{kind}_{_ptg2_manifest_stage_suffix(serving_stage_table)}"[:63]


async def _create_ptg2_manifest_serving_stage_table(token: str) -> str:
    schema_name = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    stage_table = _ptg2_manifest_stage_table_name(token)
    storage_mode = "UNLOGGED " if _env_bool(PTG2_UNLOGGED_STAGE_ENV, True) else ""
    id_type = _ptg2_id_sql_type()
    await db.status(f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(stage_table)};")
    await db.status(
        f"""
        CREATE {storage_mode}TABLE {_quote_ident(schema_name)}.{_quote_ident(stage_table)} (
            serving_content_hash_128 {id_type} NOT NULL,
            plan_id varchar(64) NOT NULL,
            procedure_global_id_128 {id_type} NOT NULL,
            reported_code_system varchar(64),
            reported_code varchar(64),
            provider_set_global_id_128 {id_type} NOT NULL,
            provider_count integer,
            price_set_global_id_128 {id_type} NOT NULL,
            source_trace_set_hash varchar(64),
            network_names varchar[] NOT NULL DEFAULT '{{}}'
        );
        """
    )
    try:
        await db.status(
            f"ALTER TABLE {_quote_ident(schema_name)}.{_quote_ident(stage_table)} "
            "SET (autovacuum_enabled = false, toast.autovacuum_enabled = false);"
        )
    except Exception as exc:
        logger.debug("failed to disable autovacuum on PTG2 manifest stage table %s: %s", stage_table, exc)
    await _create_ptg2_manifest_price_atom_stage_table(stage_table)
    await _create_ptg2_manifest_provider_group_member_stage_table(stage_table)
    return stage_table


async def _create_ptg2_manifest_price_atom_stage_table(serving_stage_table: str) -> str:
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
            negotiated_rate varchar(64),
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


async def _create_ptg2_manifest_provider_group_member_stage_table(serving_stage_table: str) -> str:
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


async def _copy_ptg2_manifest_serving_file(copy_path: Path, *, target_table: str) -> None:
    await _copy_ptg2_manifest_file(copy_path, target_table=target_table, columns=PTG2_MANIFEST_SERVING_COLUMNS)


async def _copy_ptg2_manifest_price_atom_file(copy_path: Path, *, target_table: str) -> None:
    await _copy_ptg2_manifest_file(copy_path, target_table=target_table, columns=PTG2_MANIFEST_PRICE_ATOM_COLUMNS)


async def _copy_ptg2_manifest_provider_group_member_file(copy_path: Path, *, target_table: str) -> None:
    await _copy_ptg2_manifest_file(copy_path, target_table=target_table, columns=PTG2_MANIFEST_PROVIDER_GROUP_MEMBER_COLUMNS)


async def _copy_manifest_component_file(copy_path: Path, *, target_table: str) -> None:
    await _copy_ptg2_manifest_file(
        copy_path,
        target_table=target_table,
        columns=PTG2_MANIFEST_PROVIDER_SET_COMPONENT_COLUMNS,
    )


async def _copy_ptg2_manifest_file(copy_path: Path, *, target_table: str, columns: list[str]) -> None:
    if not copy_path.exists() or copy_path.stat().st_size <= 0:
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


def _looks_like_unique_index_duplicate(exc: Exception) -> bool:
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
            rows = await db.all(
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
                    int(value)
                    for row in rows
                    if (value := _row_value(row, "int_code")) is not None
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
        ("group_state_city_npi_addr_idx", "(provider_group_global_id_128, state_name, city_name, npi, address_checksum)"),
        ("npi_group_idx", "(npi, provider_group_global_id_128)"),
        ("group_npi_idx", "(provider_group_global_id_128, npi)"),
        ("lat_long_group_idx", "(lat, long, provider_group_global_id_128, npi) WHERE lat IS NOT NULL AND long IS NOT NULL"),
        ("address_key_idx", "(address_key, provider_group_global_id_128, npi) WHERE address_key IS NOT NULL"),
        ("taxonomy_array_gin_idx", "USING gin (taxonomy_array gin__int_ops)"),
    ]
    for index_role, columns_sql in location_indexes:
        await db.status(
            f"CREATE INDEX IF NOT EXISTS {_quote_ident(_ptg2_snapshot_index_name(provider_group_location_table, index_role))} "
            f"ON {_qualified_table(schema_name, provider_group_location_table)} {columns_sql};"
        )
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


async def _dedupe_ptg2_manifest_price_atom_table(schema_name: str, price_atom_table: str) -> dict[str, int]:
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


async def _rewrite_ptg2_manifest_price_atom_table_lean_dict(
    *,
    schema_name: str,
    price_atom_table: str,
    price_atom_dictionary_table: str,
) -> dict[str, Any] | None:
    if not await _table_exists(schema_name, price_atom_table):
        return None

    storage_mode = "UNLOGGED " if _env_bool(PTG2_UNLOGGED_STAGE_ENV, True) else ""
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
        CREATE {storage_mode}TABLE {_quote_ident(schema_name)}.{_quote_ident(price_atom_dictionary_table)} AS
        WITH dictionary_source AS (
            SELECT 'negotiated_type'::varchar(64) AS attr_kind,
                   negotiated_type::text AS text_value,
                   NULL::text[] AS text_array
              FROM {_quote_ident(schema_name)}.{_quote_ident(price_atom_table)}
             GROUP BY negotiated_type
            UNION ALL
            SELECT 'expiration_date'::varchar(64), expiration_date::text, NULL::text[]
              FROM {_quote_ident(schema_name)}.{_quote_ident(price_atom_table)}
             GROUP BY expiration_date
            UNION ALL
            SELECT 'service_code'::varchar(64), NULL::text, service_code::text[]
              FROM {_quote_ident(schema_name)}.{_quote_ident(price_atom_table)}
             GROUP BY service_code
            UNION ALL
            SELECT 'billing_class'::varchar(64), billing_class::text, NULL::text[]
              FROM {_quote_ident(schema_name)}.{_quote_ident(price_atom_table)}
             GROUP BY billing_class
            UNION ALL
            SELECT 'setting'::varchar(64), setting::text, NULL::text[]
              FROM {_quote_ident(schema_name)}.{_quote_ident(price_atom_table)}
             GROUP BY setting
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
        CREATE {storage_mode}TABLE {_quote_ident(schema_name)}.{_quote_ident(keyed_dictionary_table)} AS
        SELECT
            attr_kind,
            attr_key,
            CASE
                WHEN attr_kind IN ('service_code', 'billing_code_modifier') THEN NULL
                WHEN text_value IS NULL THEN 'NULL'
                ELSE 'TEXT:' || md5(text_value)
            END AS text_lookup_key,
            CASE
                WHEN attr_kind NOT IN ('service_code', 'billing_code_modifier') THEN NULL
                WHEN text_array IS NULL THEN 'NULL'
                ELSE 'ARRAY:' || md5(to_json(text_array)::text)
            END AS array_lookup_key
        FROM {_quote_ident(schema_name)}.{_quote_ident(price_atom_dictionary_table)};
        """
    )
    lookup_collisions = await db.scalar(
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
    if int(lookup_collisions or 0) > 0:
        raise RuntimeError(
            f"PTG2 price atom dictionary lookup key collision in {schema_name}.{price_atom_dictionary_table}"
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
    await db.status(
        f"""
        CREATE {storage_mode}TABLE {_quote_ident(schema_name)}.{_quote_ident(lean_table)} AS
        SELECT
            price_atom.price_atom_global_id_128::{id_type} AS price_atom_global_id_128,
            negotiated_type.attr_key::integer AS negotiated_type_key,
            price_atom.negotiated_rate::varchar(64) AS negotiated_rate,
            expiration_date.attr_key::integer AS expiration_date_key,
            service_code.attr_key::integer AS service_code_key,
            billing_class.attr_key::integer AS billing_class_key,
            setting.attr_key::integer AS setting_key,
            billing_code_modifier.attr_key::integer AS billing_code_modifier_key,
            additional_information.attr_key::integer AS additional_information_key
        FROM {_quote_ident(schema_name)}.{_quote_ident(price_atom_table)} price_atom
        JOIN {_quote_ident(schema_name)}.{_quote_ident(keyed_dictionary_table)} negotiated_type
          ON negotiated_type.attr_kind = 'negotiated_type'
         AND negotiated_type.text_lookup_key = CASE
                WHEN price_atom.negotiated_type IS NULL THEN 'NULL'
                ELSE 'TEXT:' || md5(price_atom.negotiated_type)
             END
        JOIN {_quote_ident(schema_name)}.{_quote_ident(keyed_dictionary_table)} expiration_date
          ON expiration_date.attr_kind = 'expiration_date'
         AND expiration_date.text_lookup_key = CASE
                WHEN price_atom.expiration_date IS NULL THEN 'NULL'
                ELSE 'TEXT:' || md5(price_atom.expiration_date)
             END
        JOIN {_quote_ident(schema_name)}.{_quote_ident(keyed_dictionary_table)} service_code
          ON service_code.attr_kind = 'service_code'
         AND service_code.array_lookup_key = CASE
                WHEN price_atom.service_code IS NULL THEN 'NULL'
                ELSE 'ARRAY:' || md5(to_json(price_atom.service_code)::text)
             END
        JOIN {_quote_ident(schema_name)}.{_quote_ident(keyed_dictionary_table)} billing_class
          ON billing_class.attr_kind = 'billing_class'
         AND billing_class.text_lookup_key = CASE
                WHEN price_atom.billing_class IS NULL THEN 'NULL'
                ELSE 'TEXT:' || md5(price_atom.billing_class)
             END
        JOIN {_quote_ident(schema_name)}.{_quote_ident(keyed_dictionary_table)} setting
          ON setting.attr_kind = 'setting'
         AND setting.text_lookup_key = CASE
                WHEN price_atom.setting IS NULL THEN 'NULL'
                ELSE 'TEXT:' || md5(price_atom.setting)
             END
        JOIN {_quote_ident(schema_name)}.{_quote_ident(keyed_dictionary_table)} billing_code_modifier
          ON billing_code_modifier.attr_kind = 'billing_code_modifier'
         AND billing_code_modifier.array_lookup_key = CASE
                WHEN price_atom.billing_code_modifier IS NULL THEN 'NULL'
                ELSE 'ARRAY:' || md5(to_json(price_atom.billing_code_modifier)::text)
             END
        JOIN {_quote_ident(schema_name)}.{_quote_ident(keyed_dictionary_table)} additional_information
          ON additional_information.attr_kind = 'additional_information'
         AND additional_information.text_lookup_key = CASE
                WHEN price_atom.additional_information IS NULL THEN 'NULL'
                ELSE 'TEXT:' || md5(price_atom.additional_information)
             END;
        """
    )
    await db.status(
        f"""
        ALTER TABLE {_quote_ident(schema_name)}.{_quote_ident(lean_table)}
            ALTER COLUMN price_atom_global_id_128 SET NOT NULL,
            ALTER COLUMN negotiated_type_key SET NOT NULL,
            ALTER COLUMN expiration_date_key SET NOT NULL,
            ALTER COLUMN service_code_key SET NOT NULL,
            ALTER COLUMN billing_class_key SET NOT NULL,
            ALTER COLUMN setting_key SET NOT NULL,
            ALTER COLUMN billing_code_modifier_key SET NOT NULL,
            ALTER COLUMN additional_information_key SET NOT NULL;
        """
    )
    await db.status(f"DROP TABLE {_quote_ident(schema_name)}.{_quote_ident(price_atom_table)};")
    await db.status(
        f"""
        ALTER TABLE {_quote_ident(schema_name)}.{_quote_ident(lean_table)}
        RENAME TO {_quote_ident(price_atom_table)};
        """
    )
    await db.status(f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(keyed_dictionary_table)};")
    dictionary_index = _ptg2_snapshot_index_name(price_atom_dictionary_table, "key_idx")
    await db.status(
        f"""
        CREATE UNIQUE INDEX {_quote_ident(dictionary_index)}
        ON {_quote_ident(schema_name)}.{_quote_ident(price_atom_dictionary_table)}
        (attr_kind, attr_key);
        """
    )
    await db.status(f"ANALYZE {_quote_ident(schema_name)}.{_quote_ident(price_atom_dictionary_table)};")
    return {
        "price_atom_table_layout": PTG2_MANIFEST_PRICE_ATOM_LAYOUT_LEAN_DICT,
        "price_atom_dictionary_table": f"{schema_name}.{price_atom_dictionary_table}",
    }


async def _dedupe_ptg2_manifest_provider_group_member_table(
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


async def _rewrite_ptg2_manifest_serving_table_lean_provider_key(
    *,
    schema_name: str,
    final_table: str,
    code_count_table: str,
    provider_set_dictionary_table: str,
) -> dict[str, Any] | None:
    constants = await _ptg2_manifest_serving_constants(schema_name, final_table)
    if constants is None:
        logger.info(
            "PTG2 lean manifest serving layout skipped for %s.%s: source trace or network names vary by row",
            schema_name,
            final_table,
        )
        return None

    storage_mode = "UNLOGGED " if _env_bool(PTG2_UNLOGGED_STAGE_ENV, True) else ""
    id_type = _ptg2_id_sql_type()
    lean_table = _ptg2_snapshot_index_name(final_table, "lean")
    await db.status(f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(code_count_table)} CASCADE;")
    await db.status(
        f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(provider_set_dictionary_table)} CASCADE;"
    )
    await db.status(f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(lean_table)} CASCADE;")
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
         AND code_count.reported_code = serving.reported_code
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
    await db.status(f"ANALYZE {_quote_ident(schema_name)}.{_quote_ident(provider_set_dictionary_table)};")
    return {
        "serving_table_layout": PTG2_MANIFEST_SERVING_LAYOUT_LEAN_PROVIDER_KEY,
        "provider_set_dictionary_table": f"{schema_name}.{provider_set_dictionary_table}",
        "source_trace_set_hash": constants.get("source_trace_set_hash"),
        "network_names": constants.get("network_names") or [],
    }


async def _publish_ptg2_manifest_serving_snapshot(
    stage_table: str,
    *,
    snapshot_id: str,
    source_key: str,
    artifacts: dict[str, Any] | None = None,
    sidecar_artifacts: Mapping[str, Any] | None = None,
    db_dedupe_fallback: bool | None = None,
) -> dict[str, Any]:
    schema_name = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    if not await _table_exists(schema_name, stage_table):
        raise RuntimeError(f"PTG2 serving stage table does not exist: {schema_name}.{stage_table}")
    if not await _table_has_rows(schema_name, stage_table):
        raise RuntimeError(f"PTG2 serving stage table is empty: {schema_name}.{stage_table}")

    final_table = _ptg2_snapshot_table_name("serving", source_key, snapshot_id)
    price_atom_stage = _ptg2_manifest_support_stage_table(stage_table, "price_atom")
    provider_group_member_stage = _ptg2_manifest_support_stage_table(stage_table, "provider_group_member")
    price_atom_table = _ptg2_snapshot_table_name("price_atom", source_key, snapshot_id)
    provider_group_member_table = _ptg2_snapshot_table_name("provider_group_member", source_key, snapshot_id)
    provider_group_location_table = _ptg2_snapshot_table_name("provider_group_location", source_key, snapshot_id)
    provider_set_component_table = _ptg2_snapshot_table_name("provider_set_component", source_key, snapshot_id)
    provider_group_rate_scope_table = _ptg2_snapshot_table_name("provider_group_rate_scope", source_key, snapshot_id)
    provider_set_dictionary_table = _ptg2_snapshot_table_name("provider_set_dict", source_key, snapshot_id)
    price_atom_dictionary_table = _ptg2_snapshot_table_name("price_atom_dict", source_key, snapshot_id)
    code_count_table = _ptg2_snapshot_table_name("code_count", source_key, snapshot_id)
    started_at = time.monotonic()
    await db.status(f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(final_table)} CASCADE;")
    await db.status(f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(price_atom_table)} CASCADE;")
    await db.status(
        f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(provider_group_member_table)} CASCADE;"
    )
    await db.status(
        f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(provider_group_location_table)} CASCADE;"
    )
    await db.status(
        f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(provider_set_component_table)} CASCADE;"
    )
    await db.status(
        f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(provider_group_rate_scope_table)} CASCADE;"
    )
    await db.status(
        f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(provider_set_dictionary_table)} CASCADE;"
    )
    await db.status(
        f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(price_atom_dictionary_table)} CASCADE;"
    )
    await db.status(f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(code_count_table)} CASCADE;")
    await db.status(
        f"""
        ALTER TABLE {_quote_ident(schema_name)}.{_quote_ident(stage_table)}
        RENAME TO {_quote_ident(final_table)};
        """
    )
    if await _table_exists(schema_name, price_atom_stage):
        await db.status(
            f"""
            ALTER TABLE {_quote_ident(schema_name)}.{_quote_ident(price_atom_stage)}
            RENAME TO {_quote_ident(price_atom_table)};
            """
        )
    if await _table_exists(schema_name, provider_group_member_stage):
        await db.status(
            f"""
            ALTER TABLE {_quote_ident(schema_name)}.{_quote_ident(provider_group_member_stage)}
            RENAME TO {_quote_ident(provider_group_member_table)};
            """
        )
    use_db_dedupe = (
        _env_bool(PTG2_MANIFEST_PUBLISH_DB_DEDUPE_FALLBACK_ENV, True)
        if db_dedupe_fallback is None
        else bool(db_dedupe_fallback)
    )
    dedupe_metrics: dict[str, Any] = {"db_dedupe": use_db_dedupe}
    serving_deduped = False
    if use_db_dedupe:
        dedupe_metrics["serving"] = await _dedupe_ptg2_manifest_serving_table(schema_name, final_table)
        serving_deduped = True
    unique_index = _ptg2_snapshot_index_name(final_table, "content_uidx")
    lookup_index = _ptg2_snapshot_index_name(final_table, "plan_code_lookup_idx")
    provider_set_lookup_index = _ptg2_snapshot_index_name(final_table, "plan_code_provider_set_idx")
    try:
        await db.status(
            f"""
            CREATE UNIQUE INDEX {_quote_ident(unique_index)}
            ON {_quote_ident(schema_name)}.{_quote_ident(final_table)}
            (serving_content_hash_128);
            """
        )
    except Exception as exc:
        if serving_deduped or not _looks_like_unique_index_duplicate(exc):
            raise
        logger.warning(
            "PTG2 manifest serving unique index found duplicate rows after direct publish; "
            "running DB dedupe rescue",
            exc_info=True,
        )
        dedupe_metrics["db_dedupe"] = True
        dedupe_metrics["rescue"] = True
        dedupe_metrics["serving"] = await _dedupe_ptg2_manifest_serving_table(schema_name, final_table)
        serving_deduped = True
        await db.status(
            f"""
            CREATE UNIQUE INDEX {_quote_ident(unique_index)}
            ON {_quote_ident(schema_name)}.{_quote_ident(final_table)}
            (serving_content_hash_128);
            """
        )
    # The unique index is a publish-time correctness guard. Once it builds, the
    # immutable snapshot has proven there are no duplicate manifest serving identities;
    # retaining the 128-bit-only btree adds several GB and is not on the API hot path.
    await db.status(f"DROP INDEX {_quote_ident(schema_name)}.{_quote_ident(unique_index)};")
    lean_serving_manifest: dict[str, Any] | None = None
    if _ptg2_manifest_serving_layout() == PTG2_MANIFEST_SERVING_LAYOUT_LEAN_PROVIDER_KEY:
        lean_serving_manifest = await _rewrite_ptg2_manifest_serving_table_lean_provider_key(
            schema_name=schema_name,
            final_table=final_table,
            code_count_table=code_count_table,
            provider_set_dictionary_table=provider_set_dictionary_table,
        )
    if lean_serving_manifest is None:
        await db.status(
            f"""
            CREATE INDEX {_quote_ident(lookup_index)}
            ON {_quote_ident(schema_name)}.{_quote_ident(final_table)}
            (plan_id, reported_code_system, reported_code, provider_count DESC NULLS LAST, serving_content_hash_128);
            """
        )
        await db.status(
            f"""
            CREATE INDEX {_quote_ident(provider_set_lookup_index)}
            ON {_quote_ident(schema_name)}.{_quote_ident(final_table)}
            (plan_id, reported_code_system, reported_code, provider_set_global_id_128, provider_count DESC NULLS LAST, serving_content_hash_128);
            """
        )
    lean_price_atom_manifest: dict[str, Any] | None = None
    if await _table_exists(schema_name, price_atom_table):
        price_atom_deduped = False
        if use_db_dedupe:
            dedupe_metrics["price_atom"] = await _dedupe_ptg2_manifest_price_atom_table(
                schema_name,
                price_atom_table,
            )
            price_atom_deduped = True
        if _ptg2_manifest_price_atom_layout() == PTG2_MANIFEST_PRICE_ATOM_LAYOUT_LEAN_DICT:
            lean_price_atom_manifest = await _rewrite_ptg2_manifest_price_atom_table_lean_dict(
                schema_name=schema_name,
                price_atom_table=price_atom_table,
                price_atom_dictionary_table=price_atom_dictionary_table,
            )
        price_atom_index = _ptg2_snapshot_index_name(price_atom_table, "primary")
        try:
            await db.status(
                f"""
                CREATE UNIQUE INDEX {_quote_ident(price_atom_index)}
                ON {_quote_ident(schema_name)}.{_quote_ident(price_atom_table)}
                (price_atom_global_id_128);
                """
            )
        except Exception as exc:
            if price_atom_deduped or not _looks_like_unique_index_duplicate(exc):
                raise
            logger.warning(
                "PTG2 manifest price atom unique index found duplicate rows after direct publish; "
                "running DB dedupe rescue",
                exc_info=True,
            )
            dedupe_metrics["db_dedupe"] = True
            dedupe_metrics["rescue"] = True
            dedupe_metrics["price_atom"] = await _dedupe_ptg2_manifest_price_atom_table(
                schema_name,
                price_atom_table,
            )
            price_atom_deduped = True
            await db.status(
                f"""
                CREATE UNIQUE INDEX {_quote_ident(price_atom_index)}
                ON {_quote_ident(schema_name)}.{_quote_ident(price_atom_table)}
                (price_atom_global_id_128);
                """
            )
    if await _table_exists(schema_name, provider_group_member_table):
        if use_db_dedupe:
            dedupe_metrics["provider_group_member"] = await _dedupe_ptg2_manifest_provider_group_member_table(
                schema_name,
                provider_group_member_table,
            )
        provider_group_member_index = _ptg2_snapshot_index_name(provider_group_member_table, "group_npi_idx")
        provider_group_member_npi_index = _ptg2_snapshot_index_name(provider_group_member_table, "npi_idx")
        await db.status(
            f"""
            CREATE INDEX {_quote_ident(provider_group_member_index)}
            ON {_quote_ident(schema_name)}.{_quote_ident(provider_group_member_table)}
            (provider_group_global_id_128, npi);
            """
        )
        await db.status(
            f"""
            CREATE INDEX {_quote_ident(provider_group_member_npi_index)}
            ON {_quote_ident(schema_name)}.{_quote_ident(provider_group_member_table)}
            (npi, provider_group_global_id_128);
            """
        )
    materialized_provider_set_component_table = None
    if _ptg2_manifest_provider_set_component_enabled():
        materialized_provider_set_component_table = await _materialize_manifest_components(
            schema_name=schema_name,
            table_name=provider_set_component_table,
            artifacts=artifacts,
            sidecar_artifacts=sidecar_artifacts,
        )
    materialized_provider_group_rate_scope_table = None
    if materialized_provider_set_component_table and _ptg2_manifest_provider_group_rate_scope_enabled():
        materialized_provider_group_rate_scope_table = await _materialize_manifest_provider_group_rate_scope(
            schema_name=schema_name,
            table_name=provider_group_rate_scope_table,
            serving_table=final_table,
            code_count_table=code_count_table,
            provider_set_component_table=materialized_provider_set_component_table,
            provider_set_dictionary_table=provider_set_dictionary_table if lean_serving_manifest else None,
            lean_provider_key_layout=lean_serving_manifest is not None,
        )
    materialized_provider_group_location_table = None
    if _ptg2_manifest_provider_group_location_enabled():
        materialized_provider_group_location_table = await _build_manifest_provider_group_location_table(
            schema_name=schema_name,
            provider_group_member_table=provider_group_member_table,
            provider_group_location_table=provider_group_location_table,
        )
    if lean_serving_manifest is None:
        await db.status(
            f"""
            CREATE TABLE {_quote_ident(schema_name)}.{_quote_ident(code_count_table)} AS
            SELECT
                plan_id,
                reported_code_system,
                reported_code,
                COUNT(*)::bigint AS rate_count
            FROM {_quote_ident(schema_name)}.{_quote_ident(final_table)}
            GROUP BY plan_id, reported_code_system, reported_code;
            """
        )
        code_count_index = _ptg2_snapshot_index_name(code_count_table, "primary")
        await db.status(
            f"""
            CREATE UNIQUE INDEX {_quote_ident(code_count_index)}
            ON {_quote_ident(schema_name)}.{_quote_ident(code_count_table)}
            (plan_id, reported_code_system, reported_code);
            """
        )
    await db.status(f"ANALYZE {_quote_ident(schema_name)}.{_quote_ident(final_table)};")
    await db.status(f"ANALYZE {_quote_ident(schema_name)}.{_quote_ident(code_count_table)};")
    if await _table_exists(schema_name, price_atom_table):
        await db.status(f"ANALYZE {_quote_ident(schema_name)}.{_quote_ident(price_atom_table)};")
    if await _table_exists(schema_name, provider_group_member_table):
        await db.status(f"ANALYZE {_quote_ident(schema_name)}.{_quote_ident(provider_group_member_table)};")
    if materialized_provider_set_component_table:
        await db.status(
            f"ANALYZE {_quote_ident(schema_name)}.{_quote_ident(materialized_provider_set_component_table)};"
        )
    if materialized_provider_group_location_table:
        await db.status(
            f"ANALYZE {_quote_ident(schema_name)}.{_quote_ident(materialized_provider_group_location_table)};"
        )
    row_count = await _exact_table_rows(schema_name, final_table)
    elapsed_seconds = time.monotonic() - started_at
    artifact_manifest = _ptg2_manifest_artifacts_manifest(artifacts=artifacts, sidecar_artifacts=sidecar_artifacts)
    serving_manifest = {
        "storage": "manifest_snapshot",
        "type": "ptg2_serving",
        "id_storage": _ptg2_id_storage(),
        "snapshot_scoped": True,
        "source_key": source_key,
        "table": f"{schema_name}.{final_table}",
        "price_atom_table": f"{schema_name}.{price_atom_table}",
        "price_atom_table_layout": (lean_price_atom_manifest or {}).get("price_atom_table_layout"),
        "price_atom_dictionary_table": (lean_price_atom_manifest or {}).get("price_atom_dictionary_table"),
        "provider_group_member_table": f"{schema_name}.{provider_group_member_table}",
        "provider_group_location_table": (
            f"{schema_name}.{materialized_provider_group_location_table}"
            if materialized_provider_group_location_table
            else None
        ),
        "provider_set_component_table": (
            f"{schema_name}.{materialized_provider_set_component_table}"
            if materialized_provider_set_component_table
            else None
        ),
        "provider_group_rate_scope_table": (
            f"{schema_name}.{materialized_provider_group_rate_scope_table}"
            if materialized_provider_group_rate_scope_table
            else None
        ),
        "code_count_table": f"{schema_name}.{code_count_table}",
        "rate_count": row_count,
        "serving_rates": row_count,
        "row_count": row_count,
        "artifacts": artifact_manifest,
        "timings": {"publish_seconds": elapsed_seconds},
        "dedupe": dedupe_metrics,
    }
    if lean_serving_manifest:
        serving_manifest.update(lean_serving_manifest)
    return serving_manifest


def _ptg2_manifest_artifacts_manifest(
    *,
    artifacts: Mapping[str, Any] | None = None,
    sidecar_artifacts: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    manifest = dict(artifacts or {})
    if not sidecar_artifacts:
        return manifest
    existing_sidecars = manifest.get("sidecars")
    sidecars: list[Any] = list(existing_sidecars) if isinstance(existing_sidecars, list) else []
    for name, value in sidecar_artifacts.items():
        if value is None:
            continue
        if isinstance(value, Mapping):
            entry = dict(value)
            entry.setdefault("name", str(name))
            sidecars.append(entry)
        else:
            sidecars.append({"name": str(name), "path": str(value)})
    manifest["sidecars"] = sidecars
    return manifest
