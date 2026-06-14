# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
# pylint: disable=too-many-lines

from __future__ import annotations

import asyncio
import datetime
import hashlib
import logging
import os
from typing import Iterable

from arq import create_pool

from db.models import (
    EntityAddressEvidence,
    EntityAddressMedicationBridge,
    EntityAddressNetworkBridge,
    EntityAddressPTGBridge,
    EntityAddressPlanBridge,
    EntityAddressProcedureBridge,
    EntityAddressUnified,
    PTGAddress,
    db,
)
from process.control_lifecycle import mark_control_run
from process.ext.utils import ensure_database, make_class, my_init_db, print_time_info
from process.live_progress import enqueue_live_progress
from process.redis_config import build_redis_settings
from process.serialization import deserialize_job, serialize_job

logger = logging.getLogger(__name__)

ENTITY_ADDRESS_UNIFIED_QUEUE_NAME = "arq:EntityAddressUnified"
POSTGRES_IDENTIFIER_MAX_LENGTH = 63
DEFAULT_MIN_ROWS = 1_000_000
DEFAULT_TEST_LIMIT_PER_SOURCE = 20_000
DEFAULT_SOURCE_CONCURRENCY = 3
DEFAULT_AGGREGATE_SHARDS = 16
DEFAULT_AGGREGATE_CONCURRENCY = 4
DEFAULT_SOURCE_TABLE_SHARDS = 1
DEFAULT_ENRICH_SHARDS = 1
DEFAULT_ENRICH_CONCURRENCY = 1
DEFAULT_EVIDENCE_SHARDS = 16
DEFAULT_EVIDENCE_CONCURRENCY = 4
ARCHIVE_IDENTITY_VERSION = "v1"
BASE_ADDRESS_VERSION = "address_archive_v2:v1"
SUPPORT_TABLE_MODELS = (
    EntityAddressEvidence,
    EntityAddressPlanBridge,
    EntityAddressNetworkBridge,
    EntityAddressPTGBridge,
    EntityAddressProcedureBridge,
    EntityAddressMedicationBridge,
)


def _normalize_import_id(raw: str | None) -> str:
    if raw:
        cleaned = "".join(ch for ch in str(raw) if ch.isalnum())
        if cleaned:
            return cleaned[:32]
    return datetime.datetime.now().strftime("%Y%m%d")


def _archived_identifier(name: str, suffix: str = "_old") -> str:
    candidate = f"{name}{suffix}"
    if len(candidate) <= POSTGRES_IDENTIFIER_MAX_LENGTH:
        return candidate
    digest = hashlib.sha1(name.encode("utf-8")).hexdigest()[:8]
    trim_to = max(1, POSTGRES_IDENTIFIER_MAX_LENGTH - len(suffix) - len(digest) - 1)
    return f"{name[:trim_to]}_{digest}{suffix}"


def _validate_schema_name(schema: str) -> str:
    cleaned = (schema or "").strip()
    if not cleaned or not (cleaned[0].isalpha() or cleaned[0] == "_"):
        raise ValueError(f"Invalid schema name: {schema!r}")
    if not all(ch.isalnum() or ch == "_" for ch in cleaned):
        raise ValueError(f"Invalid schema name: {schema!r}")
    return cleaned


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int, minimum: int = 0) -> int:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return default
    return max(int(raw), minimum)


def _sql_literal(value: str | None) -> str:
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


async def _ensure_schema_exists(db_schema: str) -> None:
    db_schema = _validate_schema_name(db_schema)
    try:
        await db.status(f"CREATE SCHEMA IF NOT EXISTS {db_schema};")
    except Exception as exc:
        exists = bool(await db.scalar(f"SELECT to_regnamespace('{db_schema}') IS NOT NULL;"))
        if exists:
            logger.warning(
                "Schema %s already exists but CREATE SCHEMA failed (%s); continuing",
                db_schema,
                exc,
            )
            return
        raise


def _stage_index_name(stage_table: str, index_name: str) -> str:
    return f"{stage_table}_idx_{index_name}"


def _support_stage_classes(import_date: str) -> dict[type, type]:
    return {model: make_class(model, import_date) for model in SUPPORT_TABLE_MODELS}


async def _create_stage_indexes(stage_cls, db_schema: str) -> None:
    if hasattr(stage_cls, "__my_index_elements__") and stage_cls.__my_index_elements__:
        await db.status(
            f"CREATE UNIQUE INDEX IF NOT EXISTS {stage_cls.__tablename__}_idx_primary "
            f"ON {db_schema}.{stage_cls.__tablename__} "
            f"({', '.join(stage_cls.__my_index_elements__)});"
        )

    if hasattr(stage_cls, "__my_additional_indexes__") and stage_cls.__my_additional_indexes__:
        for index in stage_cls.__my_additional_indexes__:
            index_name = index.get("name", "_".join(index.get("index_elements")))
            using = f"USING {index.get('using')} " if index.get("using") else ""
            where = f" WHERE {index.get('where')}" if index.get("where") else ""
            stmt = (
                f"CREATE INDEX IF NOT EXISTS "
                f"{_stage_index_name(stage_cls.__tablename__, index_name)} "
                f"ON {db_schema}.{stage_cls.__tablename__} {using}"
                f"({', '.join(index.get('index_elements'))}){where};"
            )
            try:
                await db.status(stmt)
            except Exception as exc:
                msg = str(exc).lower()
                if "st_makepoint" in msg or "geography" in msg or "postgis" in msg:
                    logger.warning(
                        "Skipping geo index %s because PostGIS is unavailable in current DB: %s",
                        index_name,
                        exc,
                    )
                    continue
                raise


async def _prepare_support_stage_tables(db_schema: str, import_date: str) -> dict[type, type]:
    stage_classes = _support_stage_classes(import_date)
    for stage_cls in stage_classes.values():
        await db.status(f"DROP TABLE IF EXISTS {db_schema}.{stage_cls.__tablename__};")
        await db.create_table(stage_cls.__table__, checkfirst=True)
    return stage_classes


async def _create_support_stage_indexes(stage_classes: dict[type, type], db_schema: str) -> None:
    for stage_cls in stage_classes.values():
        await _create_stage_indexes(stage_cls, db_schema)


async def _swap_stage_table(db_schema: str, live_cls, stage_cls) -> None:
    table = live_cls.__main_table__
    await db.status(f"DROP TABLE IF EXISTS {db_schema}.{table}_old;")
    await db.status(f"ALTER TABLE IF EXISTS {db_schema}.{table} RENAME TO {table}_old;")
    await db.status(f"ALTER TABLE IF EXISTS {db_schema}.{stage_cls.__tablename__} RENAME TO {table};")

    archived = _archived_identifier(f"{table}_idx_primary")
    await db.status(f"DROP INDEX IF EXISTS {db_schema}.{archived};")
    await db.status(f"ALTER INDEX IF EXISTS {db_schema}.{table}_idx_primary RENAME TO {archived};")
    await db.status(
        f"ALTER INDEX IF EXISTS {db_schema}.{stage_cls.__tablename__}_idx_primary "
        f"RENAME TO {table}_idx_primary;"
    )

    for index in getattr(stage_cls, "__my_additional_indexes__", []) or []:
        index_name = index.get("name", "_".join(index.get("index_elements")))
        old_live_name = f"{table}_idx_{index_name}"
        archived_live_name = _archived_identifier(old_live_name)
        await db.status(f"DROP INDEX IF EXISTS {db_schema}.{archived_live_name};")
        await db.status(f"ALTER INDEX IF EXISTS {db_schema}.{old_live_name} RENAME TO {archived_live_name};")
        await db.status(
            f"ALTER INDEX IF EXISTS {db_schema}.{_stage_index_name(stage_cls.__tablename__, index_name)} "
            f"RENAME TO {old_live_name};"
        )


async def _table_exists(db_schema: str, table_name: str) -> bool:
    return bool(await db.scalar(f"SELECT to_regclass('{db_schema}.{table_name}') IS NOT NULL;"))


async def _address_canon_available(db_schema: str) -> bool:
    value = await db.scalar(
        "SELECT to_regprocedure(:signature);",
        signature=f"{db_schema}.addr_key_v1(text,text,text,text,text,text)",
    )
    return isinstance(value, str) and bool(value)


def _npi_entity_name_expr(alias: str = "n") -> str:
    return (
        "NULLIF(TRIM("
        f"COALESCE({alias}.provider_organization_name, '') || ' ' || "
        f"COALESCE({alias}.provider_other_organization_name, '') || ' ' || "
        f"COALESCE({alias}.provider_first_name, '') || ' ' || "
        f"COALESCE({alias}.provider_last_name, '')"
        "), '')"
    )


def _npi_entity_subtype_expr(alias: str = "n") -> str:
    return (
        f"CASE WHEN {alias}.entity_type_code = 1 THEN 'individual' "
        f"WHEN {alias}.entity_type_code = 2 THEN 'organization' ELSE NULL END"
    )


def _address_checksum_expr(
    *,
    entity_type_expr: str = "entity_type",
    entity_id_expr: str = "entity_id",
    type_expr: str = "type",
    first_line_expr: str = "first_line",
    second_line_expr: str = "second_line",
    city_name_expr: str = "city_name",
    state_name_expr: str = "state_name",
    postal_code_expr: str = "postal_code",
    country_code_expr: str = "country_code",
    telephone_number_expr: str = "telephone_number",
) -> str:
    return (
        "(('x' || substr(md5(lower(concat_ws('|', "
        f"COALESCE({entity_type_expr}, ''), "
        f"COALESCE({entity_id_expr}, ''), "
        f"COALESCE({type_expr}, ''), "
        f"COALESCE({first_line_expr}, ''), "
        f"COALESCE({second_line_expr}, ''), "
        f"COALESCE({city_name_expr}, ''), "
        f"COALESCE({state_name_expr}, ''), "
        f"COALESCE({postal_code_expr}, ''), "
        f"COALESCE({country_code_expr}, ''), "
        f"COALESCE({telephone_number_expr}, '')"
        "))), 1, 8))::bit(32)::int)"
    )


def _alnum_norm_expr(expr: str) -> str:
    return (
        "NULLIF("
        f"regexp_replace(lower(COALESCE({expr}, '')), '[^a-z0-9]', '', 'g')"
        ", '')"
    )


def _state_norm_expr(expr: str) -> str:
    return f"NULLIF(upper(trim(COALESCE({expr}, ''))), '')"


def _zip5_norm_expr(expr: str) -> str:
    return (
        "NULLIF("
        f"LEFT(regexp_replace(COALESCE({expr}, ''), '[^0-9]', '', 'g'), 5)"
        ", '')"
    )


def _phone_norm_expr(expr: str) -> str:
    return f"NULLIF(regexp_replace(COALESCE({expr}, ''), '[^0-9]', '', 'g'), '')"


def _source_priority_expr(expr: str) -> str:
    return (
        "CASE "
        f"WHEN {expr} = 'nppes' THEN 0 "
        f"WHEN {expr} = 'cms_doctors' THEN 1 "
        f"WHEN {expr} = 'provider_enrollment_ffs' THEN 2 "
        f"WHEN {expr} = 'provider_enrollment_ffs_address' THEN 3 "
        f"WHEN {expr} LIKE 'facility_anchor:%' THEN 4 "
        f"WHEN {expr} = 'mrf' THEN 5 "
        "ELSE 9 END"
    )


def _source_id_expr(expr: str) -> str:
    return (
        "CASE "
        f"WHEN {expr} = 'nppes' THEN 1 "
        f"WHEN {expr} = 'mrf' THEN 2 "
        f"WHEN {expr} = 'cms_doctors' THEN 3 "
        f"WHEN {expr} = 'provider_enrollment_ffs' THEN 4 "
        f"WHEN {expr} = 'provider_enrollment_ffs_address' THEN 5 "
        f"WHEN {expr} LIKE 'facility_anchor:%' THEN 6 "
        f"WHEN {expr} = 'ptg' THEN 7 "
        "ELSE 0 END"
    )


def _source_mask_expr(expr: str) -> str:
    return (
        "CASE "
        f"WHEN {expr} = 'nppes' THEN 1::bigint "
        f"WHEN {expr} = 'mrf' THEN 2::bigint "
        f"WHEN {expr} = 'cms_doctors' THEN 4::bigint "
        f"WHEN {expr} = 'provider_enrollment_ffs' THEN 8::bigint "
        f"WHEN {expr} = 'provider_enrollment_ffs_address' THEN 16::bigint "
        f"WHEN {expr} LIKE 'facility_anchor:%' THEN 32::bigint "
        f"WHEN {expr} = 'ptg' THEN 64::bigint "
        "ELSE 0::bigint END"
    )


def _address_role_id_expr(expr: str) -> str:
    return (
        "CASE "
        f"WHEN {expr} = 'primary' THEN 1 "
        f"WHEN {expr} = 'mail' THEN 2 "
        f"WHEN {expr} = 'secondary' THEN 3 "
        f"WHEN {expr} = 'practice' THEN 4 "
        f"WHEN {expr} = 'site' THEN 5 "
        f"WHEN {expr} = 'billing' THEN 6 "
        f"WHEN {expr} = 'inferred' THEN 7 "
        "ELSE 0 END"
    )


def _location_key_expr(
    *,
    entity_type: str = "entity_type",
    entity_id: str = "entity_id",
    npi: str = "npi",
    inferred_npi: str = "inferred_npi",
    address_role_id: str = "address_role_id",
    row_origin: str = "row_origin",
    address_key: str = "address_key",
    source_id: str = "source_id",
    source_record_id: str = "source_record_id",
    zip5: str = "zip5",
    state_code: str = "state_code",
    city_norm: str = "city_norm",
) -> str:
    identity = (
        "CASE WHEN "
        f"{address_key} IS NOT NULL THEN concat_ws('|', "
        "'v1', "
        f"COALESCE({entity_type}, ''), COALESCE({entity_id}, ''), "
        f"COALESCE({npi}::text, ''), COALESCE({inferred_npi}::text, ''), "
        "''::text, "
        f"COALESCE({address_role_id}::text, ''), COALESCE({row_origin}, ''), "
        f"COALESCE({address_key}::text, '')) "
        "ELSE concat_ws('|', "
        "'v1', 'fallback', "
        f"COALESCE({entity_type}, ''), COALESCE({entity_id}, ''), "
        f"COALESCE({npi}::text, ''), COALESCE({inferred_npi}::text, ''), "
        "''::text, "
        f"COALESCE({address_role_id}::text, ''), COALESCE({row_origin}, ''), "
        f"COALESCE({source_id}::text, ''), COALESCE({source_record_id}, ''), "
        f"COALESCE({zip5}, ''), COALESCE({state_code}, ''), COALESCE({city_norm}, '')) END"
    )
    return f"encode(sha256(convert_to(({identity}), 'UTF8')), 'hex')"


def _street_soft_norm_expr(expr: str) -> str:
    # Canonicalize common street word variants so cross-source evidence can converge.
    return (
        "regexp_replace("
        "regexp_replace("
        "regexp_replace("
        "regexp_replace("
        "regexp_replace("
        "regexp_replace("
        "regexp_replace("
        "regexp_replace("
        "regexp_replace("
        "regexp_replace("
        "regexp_replace("
        "regexp_replace(' ' || lower(COALESCE("
        f"{expr}"
        ", '')) || ' ', "
        "'\\mwest\\M', ' w ', 'g'), "
        "'\\meast\\M', ' e ', 'g'), "
        "'\\mnorth\\M', ' n ', 'g'), "
        "'\\msouth\\M', ' s ', 'g'), "
        "'\\mstreet\\M', ' st ', 'g'), "
        "'\\mavenue\\M', ' ave ', 'g'), "
        "'\\mroad\\M', ' rd ', 'g'), "
        "'\\mboulevard\\M', ' blvd ', 'g'), "
        "'\\mdrive\\M', ' dr ', 'g'), "
        "'\\mlane\\M', ' ln ', 'g'), "
        "'\\mhighway\\M', ' hwy ', 'g'), "
        "'[^a-z0-9]', '', 'g')"
    )


def _source_selects(
    db_schema: str,
    available: dict[str, bool],
    *,
    test_limit_per_source: int | None = None,
) -> list[str]:
    selects: list[str] = []
    has_npi = available.get("npi", False)
    has_npi_address = available.get("npi_address", False)
    has_geo = available.get("geo_zip_lookup", False)
    has_doctors = available.get("doctor_clinician_address", False)
    has_ffs = available.get("provider_enrollment_ffs", False)
    has_ffs_address = available.get("provider_enrollment_ffs_address", False)
    has_facility = available.get("facility_anchor", False)
    has_mrf_address = available.get("mrf_address", False)
    has_ptg_address = available.get("ptg_address", False)
    has_archive = available.get("address_archive_v2", False)

    npi_join = f"LEFT JOIN {db_schema}.npi AS n ON n.npi = a.npi" if has_npi else ""
    doctors_npi_join = f"LEFT JOIN {db_schema}.npi AS n ON n.npi = d.npi" if has_npi else ""
    ffs_npi_join = f"LEFT JOIN {db_schema}.npi AS n ON n.npi = f.npi" if has_npi else ""
    pa_from = (
        f"LEFT JOIN LATERAL ("
        f"SELECT pa.taxonomy_array, pa.plans_network_array, pa.procedures_array, pa.medications_array "
        f"FROM {db_schema}.npi_address AS pa WHERE pa.npi = d.npi AND pa.type = 'primary' "
        f"ORDER BY pa.checksum LIMIT 1) AS pa ON TRUE"
        if has_npi_address
        else ""
    )
    ffs_pa_from = (
        f"LEFT JOIN LATERAL ("
        f"SELECT pa.taxonomy_array, pa.plans_network_array, pa.procedures_array, pa.medications_array "
        f"FROM {db_schema}.npi_address AS pa WHERE pa.npi = f.npi AND pa.type = 'primary' "
        f"ORDER BY pa.checksum LIMIT 1) AS pa ON TRUE"
        if has_npi_address
        else ""
    )
    mrf_pa_from = (
        f"LEFT JOIN LATERAL ("
        f"SELECT pa.taxonomy_array, pa.plans_network_array, pa.procedures_array, pa.medications_array "
        f"FROM {db_schema}.npi_address AS pa WHERE pa.npi = a.npi AND pa.type = 'primary' "
        f"ORDER BY pa.checksum LIMIT 1) AS pa ON TRUE"
        if has_npi_address
        else ""
    )
    ptg_pa_from = (
        f"LEFT JOIN LATERAL ("
        f"SELECT pa.taxonomy_array, pa.plans_network_array, pa.procedures_array, pa.medications_array "
        f"FROM {db_schema}.npi_address AS pa WHERE pa.npi = p.npi AND pa.type = 'primary' "
        f"ORDER BY pa.checksum LIMIT 1) AS pa ON TRUE"
        if has_npi_address
        else ""
    )
    ptg_npi_join = f"LEFT JOIN {db_schema}.npi AS n ON n.npi = p.npi" if has_npi else ""
    ptg_archive_join = (
        f"LEFT JOIN {db_schema}.address_archive_v2 AS aa ON aa.address_key = p.address_key"
        if has_archive
        else ""
    )
    geo_join_d = (
        f"LEFT JOIN {db_schema}.geo_zip_lookup AS gz ON gz.zip_code = LEFT(COALESCE(d.zip_code, ''), 5)"
        if has_geo
        else ""
    )
    geo_join_f = (
        f"LEFT JOIN {db_schema}.geo_zip_lookup AS gz ON gz.zip_code = LEFT(COALESCE(f.zip_code, ''), 5)"
        if has_geo
        else ""
    )
    geo_join_fa = (
        f"LEFT JOIN {db_schema}.geo_zip_lookup AS gz ON gz.zip_code = LEFT(COALESCE(fa.zip_code, ''), 5)"
        if has_geo
        else ""
    )

    if has_npi_address:
        selects.append(
            f"""
            SELECT
                'npi'::varchar AS entity_type,
                a.npi::varchar AS entity_id,
                a.npi::bigint AS npi,
                NULL::bigint AS inferred_npi,
                NULL::float8 AS inference_confidence,
                NULL::varchar AS inference_method,
                {(_npi_entity_name_expr('n') if has_npi else 'NULL::varchar')} AS entity_name,
                {(_npi_entity_subtype_expr('n') if has_npi else 'NULL::varchar')} AS entity_subtype,
                COALESCE(NULLIF(a.type, ''), 'primary')::varchar AS type,
                COALESCE(a.taxonomy_array, ARRAY[0]::int[])::int[] AS taxonomy_array,
                COALESCE(a.plans_network_array, ARRAY[0]::int[])::int[] AS plans_network_array,
                COALESCE(a.procedures_array, ARRAY[0]::int[])::int[] AS procedures_array,
                COALESCE(a.medications_array, ARRAY[0]::int[])::int[] AS medications_array,
                a.first_line::varchar AS first_line,
                a.second_line::varchar AS second_line,
                a.city_name::varchar AS city_name,
                a.state_name::varchar AS state_name,
                a.postal_code::varchar AS postal_code,
                COALESCE(NULLIF(a.country_code, ''), 'US')::varchar AS country_code,
                a.telephone_number::varchar AS telephone_number,
                a.fax_number::varchar AS fax_number,
                a.formatted_address::varchar AS formatted_address,
                a.lat::numeric AS lat,
                a.long::numeric AS long,
                a.date_added::date AS date_added,
                a.place_id::varchar AS place_id,
                NOW()::timestamp AS updated_at,
                'nppes'::varchar AS address_source,
                ('nppes:' || a.npi::varchar || ':' || COALESCE(a.type, '') || ':' || COALESCE(a.checksum::varchar, '0'))::varchar AS source_record_id
              FROM {db_schema}.npi_address AS a
              {npi_join}
             WHERE a.npi IS NOT NULL
            """
        )

    if has_doctors:
        selects.append(
            f"""
            SELECT
                'npi'::varchar AS entity_type,
                d.npi::varchar AS entity_id,
                d.npi::bigint AS npi,
                NULL::bigint AS inferred_npi,
                NULL::float8 AS inference_confidence,
                NULL::varchar AS inference_method,
                {(_npi_entity_name_expr('n') if has_npi else 'NULL::varchar')} AS entity_name,
                {(_npi_entity_subtype_expr('n') if has_npi else 'NULL::varchar')} AS entity_subtype,
                'practice'::varchar AS type,
                {('COALESCE(pa.taxonomy_array, ARRAY[0]::int[])::int[]' if has_npi_address else 'ARRAY[0]::int[]')} AS taxonomy_array,
                {('COALESCE(pa.plans_network_array, ARRAY[0]::int[])::int[]' if has_npi_address else 'ARRAY[0]::int[]')} AS plans_network_array,
                {('COALESCE(pa.procedures_array, ARRAY[0]::int[])::int[]' if has_npi_address else 'ARRAY[0]::int[]')} AS procedures_array,
                {('COALESCE(pa.medications_array, ARRAY[0]::int[])::int[]' if has_npi_address else 'ARRAY[0]::int[]')} AS medications_array,
                d.address_line1::varchar AS first_line,
                d.address_line2::varchar AS second_line,
                d.city::varchar AS city_name,
                d.state::varchar AS state_name,
                LEFT(d.zip_code, 5)::varchar AS postal_code,
                'US'::varchar AS country_code,
                NULL::varchar AS telephone_number,
                NULL::varchar AS fax_number,
                NULL::varchar AS formatted_address,
                {('COALESCE(d.latitude, gz.latitude)::numeric' if has_geo else 'd.latitude::numeric')} AS lat,
                {('COALESCE(d.longitude, gz.longitude)::numeric' if has_geo else 'd.longitude::numeric')} AS long,
                NULL::date AS date_added,
                NULL::varchar AS place_id,
                COALESCE(d.updated_at, NOW())::timestamp AS updated_at,
                'cms_doctors'::varchar AS address_source,
                ('cms_doctors:' || d.npi::varchar || ':' || COALESCE(d.address_checksum::varchar, '0'))::varchar AS source_record_id
              FROM {db_schema}.doctor_clinician_address AS d
              {doctors_npi_join}
              {pa_from}
              {geo_join_d}
             WHERE d.npi IS NOT NULL
            """
        )

    if has_ffs:
        selects.append(
            f"""
            SELECT
                'npi'::varchar AS entity_type,
                f.npi::varchar AS entity_id,
                f.npi::bigint AS npi,
                NULL::bigint AS inferred_npi,
                NULL::float8 AS inference_confidence,
                NULL::varchar AS inference_method,
                {(_npi_entity_name_expr('n') if has_npi else 'NULL::varchar')} AS entity_name,
                {(_npi_entity_subtype_expr('n') if has_npi else 'NULL::varchar')} AS entity_subtype,
                'primary'::varchar AS type,
                {('COALESCE(pa.taxonomy_array, ARRAY[0]::int[])::int[]' if has_npi_address else 'ARRAY[0]::int[]')} AS taxonomy_array,
                {('COALESCE(pa.plans_network_array, ARRAY[0]::int[])::int[]' if has_npi_address else 'ARRAY[0]::int[]')} AS plans_network_array,
                {('COALESCE(pa.procedures_array, ARRAY[0]::int[])::int[]' if has_npi_address else 'ARRAY[0]::int[]')} AS procedures_array,
                {('COALESCE(pa.medications_array, ARRAY[0]::int[])::int[]' if has_npi_address else 'ARRAY[0]::int[]')} AS medications_array,
                f.address_line_1::varchar AS first_line,
                f.address_line_2::varchar AS second_line,
                f.city::varchar AS city_name,
                f.state::varchar AS state_name,
                LEFT(f.zip_code, 5)::varchar AS postal_code,
                'US'::varchar AS country_code,
                NULL::varchar AS telephone_number,
                NULL::varchar AS fax_number,
                NULL::varchar AS formatted_address,
                {('COALESCE(gz.latitude, NULL)::numeric' if has_geo else 'NULL::numeric')} AS lat,
                {('COALESCE(gz.longitude, NULL)::numeric' if has_geo else 'NULL::numeric')} AS long,
                f.reporting_period_end::date AS date_added,
                NULL::varchar AS place_id,
                COALESCE(f.imported_at, NOW())::timestamp AS updated_at,
                'provider_enrollment_ffs'::varchar AS address_source,
                ('provider_enrollment_ffs:' || COALESCE(f.enrollment_id, f.record_hash::varchar))::varchar AS source_record_id
              FROM {db_schema}.provider_enrollment_ffs AS f
              {ffs_npi_join}
              {ffs_pa_from}
              {geo_join_f}
             WHERE f.npi IS NOT NULL
               AND (
                    NULLIF(f.address_line_1, '') IS NOT NULL
                    OR NULLIF(f.city, '') IS NOT NULL
                    OR NULLIF(f.state, '') IS NOT NULL
                    OR LEFT(COALESCE(f.zip_code, ''), 5) <> ''
               )
            """
        )

    if has_ffs and has_ffs_address:
        selects.append(
            f"""
            SELECT
                'npi'::varchar AS entity_type,
                f.npi::varchar AS entity_id,
                f.npi::bigint AS npi,
                NULL::bigint AS inferred_npi,
                NULL::float8 AS inference_confidence,
                NULL::varchar AS inference_method,
                {(_npi_entity_name_expr('n') if has_npi else 'NULL::varchar')} AS entity_name,
                {(_npi_entity_subtype_expr('n') if has_npi else 'NULL::varchar')} AS entity_subtype,
                'secondary'::varchar AS type,
                {('COALESCE(pa.taxonomy_array, ARRAY[0]::int[])::int[]' if has_npi_address else 'ARRAY[0]::int[]')} AS taxonomy_array,
                {('COALESCE(pa.plans_network_array, ARRAY[0]::int[])::int[]' if has_npi_address else 'ARRAY[0]::int[]')} AS plans_network_array,
                {('COALESCE(pa.procedures_array, ARRAY[0]::int[])::int[]' if has_npi_address else 'ARRAY[0]::int[]')} AS procedures_array,
                {('COALESCE(pa.medications_array, ARRAY[0]::int[])::int[]' if has_npi_address else 'ARRAY[0]::int[]')} AS medications_array,
                NULL::varchar AS first_line,
                NULL::varchar AS second_line,
                fa.city::varchar AS city_name,
                fa.state::varchar AS state_name,
                LEFT(fa.zip_code, 5)::varchar AS postal_code,
                'US'::varchar AS country_code,
                NULL::varchar AS telephone_number,
                NULL::varchar AS fax_number,
                NULL::varchar AS formatted_address,
                {('COALESCE(gz.latitude, NULL)::numeric' if has_geo else 'NULL::numeric')} AS lat,
                {('COALESCE(gz.longitude, NULL)::numeric' if has_geo else 'NULL::numeric')} AS long,
                fa.reporting_period_end::date AS date_added,
                NULL::varchar AS place_id,
                COALESCE(f.imported_at, NOW())::timestamp AS updated_at,
                'provider_enrollment_ffs_address'::varchar AS address_source,
                ('provider_enrollment_ffs_address:' || COALESCE(fa.enrollment_id, fa.record_hash::varchar))::varchar AS source_record_id
              FROM {db_schema}.provider_enrollment_ffs_address AS fa
              JOIN {db_schema}.provider_enrollment_ffs AS f
                ON f.enrollment_id = fa.enrollment_id
              {ffs_npi_join}
              {ffs_pa_from}
              {geo_join_f}
             WHERE f.npi IS NOT NULL
            """
        )

    if has_facility:
        selects.append(
            f"""
            SELECT
                'facility_anchor'::varchar AS entity_type,
                fa.id::varchar AS entity_id,
                NULL::bigint AS npi,
                NULL::bigint AS inferred_npi,
                NULL::float8 AS inference_confidence,
                NULL::varchar AS inference_method,
                fa.name::varchar AS entity_name,
                fa.facility_type::varchar AS entity_subtype,
                'site'::varchar AS type,
                ARRAY[0]::int[] AS taxonomy_array,
                ARRAY[0]::int[] AS plans_network_array,
                ARRAY[0]::int[] AS procedures_array,
                ARRAY[0]::int[] AS medications_array,
                fa.address_line1::varchar AS first_line,
                NULL::varchar AS second_line,
                fa.city::varchar AS city_name,
                fa.state::varchar AS state_name,
                LEFT(fa.zip_code, 5)::varchar AS postal_code,
                'US'::varchar AS country_code,
                NULL::varchar AS telephone_number,
                NULL::varchar AS fax_number,
                NULL::varchar AS formatted_address,
                {('COALESCE(fa.latitude, gz.latitude)::numeric' if has_geo else 'fa.latitude::numeric')} AS lat,
                {('COALESCE(fa.longitude, gz.longitude)::numeric' if has_geo else 'fa.longitude::numeric')} AS long,
                NULL::date AS date_added,
                NULL::varchar AS place_id,
                COALESCE(fa.updated_at, NOW())::timestamp AS updated_at,
                ('facility_anchor:' || LOWER(COALESCE(fa.source_dataset, 'unknown')))::varchar AS address_source,
                ('facility_anchor:' || COALESCE(fa.id, 'unknown'))::varchar AS source_record_id
              FROM {db_schema}.facility_anchor AS fa
              {geo_join_fa}
            """
        )

    if has_mrf_address:
        selects.append(
            f"""
            SELECT
                'npi'::varchar AS entity_type,
                a.npi::varchar AS entity_id,
                a.npi::bigint AS npi,
                NULL::bigint AS inferred_npi,
                NULL::float8 AS inference_confidence,
                NULL::varchar AS inference_method,
                {(_npi_entity_name_expr('n') if has_npi else 'NULL::varchar')} AS entity_name,
                {(_npi_entity_subtype_expr('n') if has_npi else 'NULL::varchar')} AS entity_subtype,
                COALESCE(NULLIF(a.type, ''), 'practice')::varchar AS type,
                {('COALESCE(pa.taxonomy_array, ARRAY[0]::int[])::int[]' if has_npi_address else 'ARRAY[0]::int[]')} AS taxonomy_array,
                {('COALESCE(pa.plans_network_array, ARRAY[0]::int[])::int[]' if has_npi_address else 'ARRAY[0]::int[]')} AS plans_network_array,
                {('COALESCE(pa.procedures_array, ARRAY[0]::int[])::int[]' if has_npi_address else 'ARRAY[0]::int[]')} AS procedures_array,
                {('COALESCE(pa.medications_array, ARRAY[0]::int[])::int[]' if has_npi_address else 'ARRAY[0]::int[]')} AS medications_array,
                a.first_line::varchar AS first_line,
                a.second_line::varchar AS second_line,
                a.city_name::varchar AS city_name,
                a.state_name::varchar AS state_name,
                a.postal_code::varchar AS postal_code,
                COALESCE(NULLIF(a.country_code, ''), 'US')::varchar AS country_code,
                a.telephone_number::varchar AS telephone_number,
                a.fax_number::varchar AS fax_number,
                a.formatted_address::varchar AS formatted_address,
                a.lat::numeric AS lat,
                a.long::numeric AS long,
                a.date_added::date AS date_added,
                a.place_id::varchar AS place_id,
                NOW()::timestamp AS updated_at,
                'mrf'::varchar AS address_source,
                ('mrf:' || a.npi::varchar || ':' || COALESCE(a.type, '') || ':' || COALESCE(a.checksum::varchar, '0'))::varchar AS source_record_id
              FROM {db_schema}.mrf_address AS a
              {npi_join}
              {mrf_pa_from}
             WHERE a.npi IS NOT NULL
            """
        )

    if has_ptg_address:
        selects.append(
            f"""
            SELECT
                'npi'::varchar AS entity_type,
                p.npi::varchar AS entity_id,
                p.npi::bigint AS npi,
                NULL::bigint AS inferred_npi,
                NULL::float8 AS inference_confidence,
                NULL::varchar AS inference_method,
                {(_npi_entity_name_expr('n') if has_npi else 'NULL::varchar')} AS entity_name,
                {(_npi_entity_subtype_expr('n') if has_npi else 'NULL::varchar')} AS entity_subtype,
                'practice'::varchar AS type,
                {('COALESCE(pa.taxonomy_array, ARRAY[0]::int[])::int[]' if has_npi_address else 'ARRAY[0]::int[]')} AS taxonomy_array,
                {('COALESCE(pa.plans_network_array, ARRAY[0]::int[])::int[]' if has_npi_address else 'ARRAY[0]::int[]')} AS plans_network_array,
                {('COALESCE(pa.procedures_array, ARRAY[0]::int[])::int[]' if has_npi_address else 'ARRAY[0]::int[]')} AS procedures_array,
                {('COALESCE(pa.medications_array, ARRAY[0]::int[])::int[]' if has_npi_address else 'ARRAY[0]::int[]')} AS medications_array,
                {('aa.first_line::varchar' if has_archive else 'NULL::varchar')} AS first_line,
                {('aa.second_line::varchar' if has_archive else 'NULL::varchar')} AS second_line,
                {('COALESCE(aa.city_name, p.city_norm)::varchar' if has_archive else 'p.city_norm::varchar')} AS city_name,
                {('COALESCE(aa.state_name, p.state_code)::varchar' if has_archive else 'p.state_code::varchar')} AS state_name,
                {('COALESCE(aa.postal_code, p.zip5)::varchar' if has_archive else 'p.zip5::varchar')} AS postal_code,
                'US'::varchar AS country_code,
                NULL::varchar AS telephone_number,
                NULL::varchar AS fax_number,
                {('aa.formatted_address::varchar' if has_archive else 'NULL::varchar')} AS formatted_address,
                p.lat::numeric AS lat,
                p.long::numeric AS long,
                NULL::date AS date_added,
                NULL::varchar AS place_id,
                COALESCE(p.updated_at, NOW())::timestamp AS updated_at,
                'ptg'::varchar AS address_source,
                ('ptg:' || p.source_key || ':' || p.snapshot_id || ':' || p.location_key)::varchar AS source_record_id
              FROM {db_schema}.ptg_address AS p
              {ptg_npi_join}
              {ptg_pa_from}
              {ptg_archive_join}
             WHERE p.npi IS NOT NULL
            """
        )

    if test_limit_per_source and test_limit_per_source > 0:
        return [
            "(\n"
            f"SELECT * FROM (\n{select.strip()}\n) AS src LIMIT {int(test_limit_per_source)}\n"
            ")"
            for select in selects
        ]
    return selects


def _integer_ranges(min_value: int | None, max_value: int | None, shards: int) -> list[tuple[int, int]]:
    if min_value is None or max_value is None or shards <= 1 or min_value > max_value:
        return []
    span = max_value - min_value + 1
    step = max(1, (span + shards - 1) // shards)
    ranges: list[tuple[int, int]] = []
    start = min_value
    while start <= max_value:
        stop = min(start + step, max_value + 1)
        ranges.append((start, stop))
        start = stop
    return ranges


def _shard_source_selects(
    db_schema: str,
    source_selects: list[str],
    *,
    npi_address_ranges: list[tuple[int, int]] | None = None,
    mrf_address_ranges: list[tuple[int, int]] | None = None,
    doctor_clinician_address_ranges: list[tuple[int, int]] | None = None,
    provider_enrollment_ffs_ranges: list[tuple[int, int]] | None = None,
) -> list[str]:
    npi_address_ranges = npi_address_ranges or []
    mrf_address_ranges = mrf_address_ranges or []
    doctor_clinician_address_ranges = doctor_clinician_address_ranges or []
    provider_enrollment_ffs_ranges = provider_enrollment_ffs_ranges or []
    sharded: list[str] = []
    shard_specs = [
        (
            f"FROM {db_schema}.npi_address AS a",
            "'nppes'::varchar AS address_source",
            "WHERE a.npi IS NOT NULL",
            "a",
            npi_address_ranges,
        ),
        (
            f"FROM {db_schema}.mrf_address AS a",
            "'mrf'::varchar AS address_source",
            "WHERE a.npi IS NOT NULL",
            "a",
            mrf_address_ranges,
        ),
        (
            f"FROM {db_schema}.doctor_clinician_address AS d",
            "'cms_doctors'::varchar AS address_source",
            "WHERE d.npi IS NOT NULL",
            "d",
            doctor_clinician_address_ranges,
        ),
        (
            f"FROM {db_schema}.provider_enrollment_ffs AS f",
            "'provider_enrollment_ffs'::varchar AS address_source",
            "WHERE f.npi IS NOT NULL",
            "f",
            provider_enrollment_ffs_ranges,
        ),
        (
            f"FROM {db_schema}.provider_enrollment_ffs_address AS fa",
            "'provider_enrollment_ffs_address'::varchar AS address_source",
            "WHERE f.npi IS NOT NULL",
            "f",
            provider_enrollment_ffs_ranges,
        ),
    ]

    for select_sql in source_selects:
        ranges: list[tuple[int, int]] = []
        where_marker = ""
        alias = ""
        for table_marker, source_marker, candidate_where, candidate_alias, candidate_ranges in shard_specs:
            if table_marker in select_sql and source_marker in select_sql:
                ranges = candidate_ranges
                where_marker = candidate_where
                alias = candidate_alias
                break
        if not ranges or not where_marker or where_marker not in select_sql:
            sharded.append(select_sql)
            continue
        for low, high in ranges:
            predicate = f"{where_marker}\n               AND {alias}.npi >= {low}\n               AND {alias}.npi < {high}"
            sharded.append(select_sql.replace(where_marker, predicate, 1))
    return sharded


async def _npi_table_ranges(db_schema: str, table_name: str, shards: int) -> list[tuple[int, int]]:
    if shards <= 1:
        return []
    row = await db.first(
        f"SELECT MIN(npi)::bigint AS min_npi, MAX(npi)::bigint AS max_npi "
        f"FROM {db_schema}.{table_name} WHERE npi IS NOT NULL;"
    )
    if not row:
        return []
    values = row._mapping
    return _integer_ranges(values.get("min_npi"), values.get("max_npi"), shards)


def _raw_stage_table_name(stage_table: str) -> str:
    return f"{stage_table}_raw"


def _evidence_stage_table_name(stage_table: str) -> str:
    return _archived_identifier(stage_table, "_evidence")


def _prepare_raw_stage_sql(db_schema: str, raw_table: str, *, unlogged: bool = True) -> str:
    storage_mode = "UNLOGGED " if unlogged else ""
    return f"""
    CREATE {storage_mode}TABLE {db_schema}.{raw_table} (
        entity_type varchar(64) NOT NULL,
        entity_id varchar(128) NOT NULL,
        npi bigint,
        inferred_npi bigint,
        inference_confidence float8,
        inference_method varchar(64),
        entity_name varchar(256),
        entity_subtype varchar(64),
        type varchar(32) NOT NULL,
        taxonomy_array int[] NOT NULL,
        plans_network_array int[] NOT NULL,
        procedures_array int[] NOT NULL,
        medications_array int[] NOT NULL,
        first_line varchar,
        second_line varchar,
        city_name varchar,
        state_name varchar,
        postal_code varchar,
        country_code varchar,
        telephone_number varchar,
        fax_number varchar,
        formatted_address varchar,
        lat numeric(11,8),
        long numeric(11,8),
        date_added date,
        place_id varchar,
        updated_at timestamp,
        source_priority int NOT NULL,
        address_source varchar,
        source_record_id varchar,
        address_key uuid,
        premise_key uuid,
        archive_identity_version varchar(16) NOT NULL DEFAULT '{ARCHIVE_IDENTITY_VERSION}',
        address_precision varchar(32) NOT NULL DEFAULT 'unknown',
        zip5 varchar(5),
        state_code varchar(2),
        city_norm varchar,
        county_fips varchar(5),
        source_id smallint NOT NULL DEFAULT 0,
        source_mask bigint NOT NULL DEFAULT 0,
        address_source_mask bigint NOT NULL DEFAULT 0,
        address_role_id smallint NOT NULL DEFAULT 0,
        location_confidence_id smallint NOT NULL DEFAULT 0,
        row_origin varchar(32) NOT NULL DEFAULT 'base',
        location_key varchar(64),
        aca_plan_array varchar[] NOT NULL DEFAULT '{{}}',
        aca_network_array varchar[] NOT NULL DEFAULT '{{}}',
        ptg_plan_array varchar[] NOT NULL DEFAULT '{{}}',
        ptg_source_array varchar[] NOT NULL DEFAULT '{{}}',
        group_plan_array varchar[] NOT NULL DEFAULT '{{}}',
        base_address_version varchar(64),
        ptg_address_version varchar(64),
        checksum bigint NOT NULL
    );
    """


def _address_key_expr(db_schema: str, available: bool) -> str:
    if available:
        return (
            f"{db_schema}.addr_key_v1("
            "first_line, second_line, city_name, state_name, postal_code, country_code"
            ")"
        )
    return "NULL::uuid"


def _enrich_raw_stage_sql(
    db_schema: str,
    raw_table: str,
    *,
    archive_available: bool = True,
    checksum_min: int | None = None,
    checksum_max: int | None = None,
) -> str:
    archive_join = ""
    archive_fields = (
        "a.premise_key, "
        "'v' || COALESCE(a.identity_version, 1)::text AS archive_identity_version, "
        "COALESCE(a.precision, 'unknown') AS address_precision, "
        "a.zip5 AS archive_zip5, "
        "NULLIF(upper(left(a.state_code, 2)), '') AS archive_state_code, "
        "a.city_norm AS archive_city_norm, "
        "NULL::varchar AS archive_county_fips"
    )
    if archive_available:
        archive_join = (
            f"LEFT JOIN {db_schema}.address_archive_v2 a "
            "ON a.address_key = r.address_key AND a.merged_into IS NULL"
        )
    else:
        archive_fields = (
            "NULL::uuid AS premise_key, "
            f"'{ARCHIVE_IDENTITY_VERSION}'::varchar AS archive_identity_version, "
            "CASE WHEN r.address_key IS NULL THEN 'unknown' ELSE 'street' END::varchar AS address_precision, "
            "NULL::varchar AS archive_zip5, "
            "NULL::varchar AS archive_state_code, "
            "NULL::varchar AS archive_city_norm, "
            "NULL::varchar AS archive_county_fips"
        )
    checksum_where = ""
    if checksum_min is not None and checksum_max is not None:
        checksum_where = f"WHERE r.checksum >= {int(checksum_min)} AND r.checksum < {int(checksum_max)}"
    return f"""
    WITH enriched AS (
        SELECT
            r.ctid AS row_id,
            {archive_fields},
            NULLIF(LEFT(REGEXP_REPLACE(COALESCE(r.postal_code, ''), '[^0-9]', '', 'g'), 5), '')::varchar AS source_zip5,
            NULLIF(upper(left(BTRIM(COALESCE(r.state_name, '')), 2)), '')::varchar AS source_state_code,
            NULLIF(regexp_replace(lower(COALESCE(r.city_name, '')), '[^a-z0-9]', '', 'g'), '')::varchar AS source_city_norm,
            {_source_id_expr("r.address_source")}::smallint AS source_id,
            {_source_mask_expr("r.address_source")}::bigint AS source_mask,
            {_address_role_id_expr("r.type")}::smallint AS address_role_id
          FROM {db_schema}.{raw_table} r
          {archive_join}
         {checksum_where}
    ),
    keyed AS (
        SELECT
            row_id,
            premise_key,
            archive_identity_version,
            address_precision,
            COALESCE(archive_zip5, source_zip5)::varchar AS zip5,
            COALESCE(archive_state_code, source_state_code)::varchar AS state_code,
            COALESCE(archive_city_norm, source_city_norm)::varchar AS city_norm,
            archive_county_fips::varchar AS county_fips,
            source_id,
            source_mask,
            CASE WHEN source_id IN (1, 2, 3, 4, 5, 6) THEN source_mask ELSE 0::bigint END AS address_source_mask,
            address_role_id,
            CASE WHEN source_id = 7 THEN 'ptg_overlay' ELSE 'base' END::varchar AS row_origin,
            CASE
                WHEN source_id = 7 THEN 4
                WHEN address_precision = 'city_zip' THEN 6
                WHEN source_id = 1 THEN 2
                WHEN source_id IN (2, 3, 4, 5, 6) THEN 1
                ELSE 0
            END::smallint AS location_confidence_id
          FROM enriched
    )
    UPDATE {db_schema}.{raw_table} r
       SET premise_key = k.premise_key,
           archive_identity_version = k.archive_identity_version,
           address_precision = k.address_precision,
           zip5 = k.zip5,
           state_code = k.state_code,
           city_norm = k.city_norm,
           county_fips = k.county_fips,
           source_id = k.source_id,
           source_mask = k.source_mask,
           address_source_mask = k.address_source_mask,
           address_role_id = k.address_role_id,
           location_confidence_id = k.location_confidence_id,
           row_origin = k.row_origin,
           ptg_source_array = CASE WHEN k.source_id = 7 THEN ARRAY[r.address_source]::varchar[] ELSE ptg_source_array END,
           base_address_version = '{BASE_ADDRESS_VERSION}',
           location_key = {_location_key_expr(
               entity_type='r.entity_type',
               entity_id='r.entity_id',
               npi='r.npi',
               inferred_npi='r.inferred_npi',
               address_role_id='k.address_role_id',
               row_origin='k.row_origin',
               address_key='r.address_key',
               source_id='k.source_id',
               source_record_id='r.source_record_id',
               zip5='k.zip5',
               state_code='k.state_code',
               city_norm='k.city_norm',
           )}
      FROM keyed k
     WHERE r.ctid = k.row_id;
    """


def _key_v2_enabled() -> bool:
    return _env_bool("HLTHPRT_ENTITY_ADDRESS_UNIFIED_KEY_V2", False)


def _dedupe_key_expr(address_canon_available: bool) -> str:
    if not _key_v2_enabled():
        return "checksum::text"
    if not address_canon_available:
        raise RuntimeError(
            "HLTHPRT_ENTITY_ADDRESS_UNIFIED_KEY_V2 requires canonical address SQL functions"
        )
    return "COALESCE(address_key::text, checksum::text)"


def _validate_publish_row_count(
    *,
    stage_rows: int,
    previous_rows: int,
    test_mode: bool,
    min_rows_required: int,
) -> None:
    if test_mode:
        return
    if stage_rows < min_rows_required:
        raise RuntimeError(
            f"EntityAddressUnified stage row count {stage_rows} below minimum {min_rows_required}; aborting publish."
        )
    min_delta_rows = int(previous_rows * 0.8)
    if previous_rows > 0 and stage_rows < min_delta_rows:
        raise RuntimeError(
            "EntityAddressUnified stage row count "
            f"{stage_rows} below 80% of previous publish {previous_rows}; aborting publish."
        )


def _insert_raw_from_source_sql(
    db_schema: str,
    raw_table: str,
    source_select: str,
    *,
    address_canon_available: bool = True,
) -> str:
    return f"""
    INSERT INTO {db_schema}.{raw_table} (
        entity_type,
        entity_id,
        npi,
        inferred_npi,
        inference_confidence,
        inference_method,
        entity_name,
        entity_subtype,
        type,
        taxonomy_array,
        plans_network_array,
        procedures_array,
        medications_array,
        first_line,
        second_line,
        city_name,
        state_name,
        postal_code,
        country_code,
        telephone_number,
        fax_number,
        formatted_address,
        lat,
        long,
        date_added,
        place_id,
        updated_at,
        source_priority,
        address_source,
        source_record_id,
        address_key,
        checksum
    )
    WITH base_rows AS (
        {source_select.strip()}
    ),
    sanitized AS (
        SELECT
            entity_type,
            entity_id,
            npi,
            inferred_npi,
            inference_confidence,
            inference_method,
            NULLIF(TRIM(entity_name), '')::varchar AS entity_name,
            NULLIF(TRIM(entity_subtype), '')::varchar AS entity_subtype,
            COALESCE(NULLIF(TRIM(type), ''), 'primary')::varchar AS type,
            COALESCE(taxonomy_array, ARRAY[0]::int[])::int[] AS taxonomy_array,
            COALESCE(plans_network_array, ARRAY[0]::int[])::int[] AS plans_network_array,
            COALESCE(procedures_array, ARRAY[0]::int[])::int[] AS procedures_array,
            COALESCE(medications_array, ARRAY[0]::int[])::int[] AS medications_array,
            NULLIF(TRIM(first_line), '')::varchar AS first_line,
            NULLIF(TRIM(second_line), '')::varchar AS second_line,
            NULLIF(TRIM(city_name), '')::varchar AS city_name,
            NULLIF(TRIM(state_name), '')::varchar AS state_name,
            NULLIF(TRIM(postal_code), '')::varchar AS postal_code,
            COALESCE(NULLIF(TRIM(country_code), ''), 'US')::varchar AS country_code,
            NULLIF(TRIM(telephone_number), '')::varchar AS telephone_number,
            NULLIF(TRIM(fax_number), '')::varchar AS fax_number,
            NULLIF(TRIM(formatted_address), '')::varchar AS formatted_address,
            lat::numeric AS lat,
            long::numeric AS long,
            date_added::date AS date_added,
            NULLIF(TRIM(place_id), '')::varchar AS place_id,
            {_source_priority_expr("address_source")}::int AS source_priority,
            address_source::varchar AS address_source,
            source_record_id::varchar AS source_record_id,
            updated_at::timestamp AS updated_at,
            {_address_key_expr(db_schema, address_canon_available)} AS address_key
          FROM base_rows
    ),
    normalized AS (
        SELECT
            entity_type,
            entity_id,
            npi,
            inferred_npi,
            inference_confidence,
            inference_method,
            entity_name,
            entity_subtype,
            type,
            taxonomy_array,
            plans_network_array,
            procedures_array,
            medications_array,
            first_line,
            second_line,
            city_name,
            state_name,
            postal_code,
            country_code,
            telephone_number,
            fax_number,
            formatted_address,
            lat,
            long,
            date_added,
            place_id,
            source_priority,
            address_source,
            source_record_id,
            updated_at,
            address_key,
            {_address_checksum_expr(
                first_line_expr=_alnum_norm_expr("first_line"),
                second_line_expr=_alnum_norm_expr("second_line"),
                city_name_expr=_alnum_norm_expr("city_name"),
                state_name_expr=_state_norm_expr("state_name"),
                postal_code_expr=_zip5_norm_expr("postal_code"),
                country_code_expr=_state_norm_expr("country_code"),
                telephone_number_expr=_phone_norm_expr("telephone_number"),
            )} AS checksum
          FROM sanitized
    )
    SELECT
        entity_type,
        entity_id,
        npi,
        inferred_npi,
        inference_confidence,
        inference_method,
        entity_name,
        entity_subtype,
        type,
        taxonomy_array,
        plans_network_array,
        procedures_array,
        medications_array,
        first_line,
        second_line,
        city_name,
        state_name,
        postal_code,
        country_code,
        telephone_number,
        fax_number,
        formatted_address,
        lat,
        long,
        date_added,
        place_id,
        updated_at,
        source_priority,
        address_source,
        source_record_id,
        address_key,
        checksum
      FROM normalized;
    """


def _materialize_from_raw_sql(
    db_schema: str,
    stage_table: str,
    raw_table: str,
    *,
    checksum_modulo: int | None = None,
    checksum_remainder: int | None = None,
    address_canon_available: bool = True,
) -> str:
    dedupe_key_expr = _dedupe_key_expr(address_canon_available)
    shard_filter = ""
    if checksum_modulo and checksum_modulo > 1 and checksum_remainder is not None:
        shard_filter = (
            f" WHERE ((hashtext({dedupe_key_expr}) % {int(checksum_modulo)} "
            f"+ {int(checksum_modulo)}) % {int(checksum_modulo)}) = {int(checksum_remainder)}"
        )
    return f"""
    INSERT INTO {db_schema}.{stage_table} (
        entity_type,
        entity_id,
        npi,
        inferred_npi,
        inference_confidence,
        inference_method,
        entity_name,
        entity_subtype,
        location_key,
        row_origin,
        archive_identity_version,
        address_precision,
        premise_key,
        zip5,
        state_code,
        city_norm,
        county_fips,
        source_mask,
        address_source_mask,
        source_count,
        independent_source_count,
        multi_source_confirmed,
        location_confidence_id,
        confidence_score,
        freshness_score,
        address_sources,
        source_record_ids,
        aca_plan_array,
        aca_network_array,
        ptg_plan_array,
        ptg_source_array,
        group_plan_array,
        base_address_version,
        ptg_address_version,
        checksum,
        type,
        taxonomy_array,
        plans_network_array,
        procedures_array,
        medications_array,
        first_line,
        second_line,
        city_name,
        state_name,
        postal_code,
        country_code,
        telephone_number,
        fax_number,
        formatted_address,
        lat,
        long,
        date_added,
        place_id,
        address_key,
        updated_at,
        last_seen_at
    )
    WITH aggregated AS (
        SELECT
            entity_type,
            entity_id,
            type,
            (ARRAY_AGG(location_key ORDER BY source_priority ASC, updated_at DESC NULLS LAST))[1]::varchar AS location_key,
            (ARRAY_AGG(row_origin ORDER BY source_priority ASC, updated_at DESC NULLS LAST))[1]::varchar AS row_origin,
            (ARRAY_AGG(archive_identity_version ORDER BY source_priority ASC, updated_at DESC NULLS LAST))[1]::varchar AS archive_identity_version,
            (ARRAY_AGG(address_precision ORDER BY source_priority ASC, updated_at DESC NULLS LAST))[1]::varchar AS address_precision,
            (ARRAY_AGG(premise_key ORDER BY source_priority ASC, (premise_key IS NULL), updated_at DESC NULLS LAST))[1]::uuid AS premise_key,
            (ARRAY_AGG(zip5 ORDER BY source_priority ASC, (zip5 IS NULL), updated_at DESC NULLS LAST))[1]::varchar AS zip5,
            (ARRAY_AGG(state_code ORDER BY source_priority ASC, (state_code IS NULL), updated_at DESC NULLS LAST))[1]::varchar AS state_code,
            (ARRAY_AGG(city_norm ORDER BY source_priority ASC, (city_norm IS NULL), updated_at DESC NULLS LAST))[1]::varchar AS city_norm,
            (ARRAY_AGG(county_fips ORDER BY source_priority ASC, (county_fips IS NULL), updated_at DESC NULLS LAST))[1]::varchar AS county_fips,
            bit_or(COALESCE(source_mask, 0))::bigint AS source_mask,
            bit_or(COALESCE(address_source_mask, 0))::bigint AS address_source_mask,
            MIN(COALESCE(location_confidence_id, 0))::smallint AS location_confidence_id,
            (ARRAY_AGG(base_address_version ORDER BY source_priority ASC, updated_at DESC NULLS LAST))[1]::varchar AS base_address_version,
            (ARRAY_AGG(ptg_address_version ORDER BY source_priority ASC, updated_at DESC NULLS LAST))[1]::varchar AS ptg_address_version,
            (ARRAY_AGG(checksum ORDER BY source_priority ASC, updated_at DESC NULLS LAST))[1]::bigint AS checksum,
            MAX(npi)::bigint AS npi,
            MAX(inferred_npi)::bigint AS inferred_npi,
            MAX(inference_confidence)::float8 AS inference_confidence,
            MAX(inference_method)::varchar AS inference_method,
            (ARRAY_AGG(entity_name ORDER BY source_priority ASC, (entity_name IS NULL), LENGTH(COALESCE(entity_name, '')) DESC, updated_at DESC NULLS LAST))[1]::varchar AS entity_name,
            MAX(entity_subtype)::varchar AS entity_subtype,
            COALESCE(MAX(taxonomy_array), ARRAY[0]::int[])::int[] AS taxonomy_array,
            COALESCE(MAX(plans_network_array), ARRAY[0]::int[])::int[] AS plans_network_array,
            COALESCE(MAX(procedures_array), ARRAY[0]::int[])::int[] AS procedures_array,
            COALESCE(MAX(medications_array), ARRAY[0]::int[])::int[] AS medications_array,
            (ARRAY_AGG(first_line ORDER BY source_priority ASC, (first_line IS NULL), LENGTH(COALESCE(first_line, '')) DESC, updated_at DESC NULLS LAST))[1]::varchar AS first_line,
            (ARRAY_AGG(second_line ORDER BY source_priority ASC, (second_line IS NULL), LENGTH(COALESCE(second_line, '')) DESC, updated_at DESC NULLS LAST))[1]::varchar AS second_line,
            (ARRAY_AGG(city_name ORDER BY source_priority ASC, (city_name IS NULL), LENGTH(COALESCE(city_name, '')) DESC, updated_at DESC NULLS LAST))[1]::varchar AS city_name,
            (ARRAY_AGG(state_name ORDER BY source_priority ASC, (state_name IS NULL), LENGTH(COALESCE(state_name, '')) DESC, updated_at DESC NULLS LAST))[1]::varchar AS state_name,
            (ARRAY_AGG(postal_code ORDER BY source_priority ASC, (postal_code IS NULL), LENGTH(COALESCE(postal_code, '')) DESC, updated_at DESC NULLS LAST))[1]::varchar AS postal_code,
            (ARRAY_AGG(country_code ORDER BY source_priority ASC, (country_code IS NULL), LENGTH(COALESCE(country_code, '')) DESC, updated_at DESC NULLS LAST))[1]::varchar AS country_code,
            (ARRAY_AGG(telephone_number ORDER BY source_priority ASC, (telephone_number IS NULL), LENGTH(COALESCE(telephone_number, '')) DESC, updated_at DESC NULLS LAST))[1]::varchar AS telephone_number,
            (ARRAY_AGG(fax_number ORDER BY source_priority ASC, (fax_number IS NULL), LENGTH(COALESCE(fax_number, '')) DESC, updated_at DESC NULLS LAST))[1]::varchar AS fax_number,
            (ARRAY_AGG(formatted_address ORDER BY source_priority ASC, (formatted_address IS NULL), LENGTH(COALESCE(formatted_address, '')) DESC, updated_at DESC NULLS LAST))[1]::varchar AS formatted_address,
            (ARRAY_AGG(lat ORDER BY source_priority ASC, (lat IS NULL), updated_at DESC NULLS LAST))[1]::numeric AS lat,
            (ARRAY_AGG(long ORDER BY source_priority ASC, (long IS NULL), updated_at DESC NULLS LAST))[1]::numeric AS long,
            MAX(date_added)::date AS date_added,
            MAX(place_id)::varchar AS place_id,
            (ARRAY_AGG(address_key ORDER BY source_priority ASC, (address_key IS NULL), updated_at DESC NULLS LAST))[1]::uuid AS address_key,
            ARRAY_REMOVE(ARRAY_AGG(DISTINCT address_source ORDER BY address_source), NULL)::varchar[] AS address_sources,
            ARRAY_REMOVE(ARRAY_AGG(DISTINCT source_record_id ORDER BY source_record_id), NULL)::varchar[] AS source_record_ids,
            ARRAY_REMOVE(ARRAY_AGG(DISTINCT aca_plan.value ORDER BY aca_plan.value), NULL)::varchar[] AS aca_plan_array,
            ARRAY_REMOVE(ARRAY_AGG(DISTINCT aca_network.value ORDER BY aca_network.value), NULL)::varchar[] AS aca_network_array,
            ARRAY_REMOVE(ARRAY_AGG(DISTINCT ptg_plan.value ORDER BY ptg_plan.value), NULL)::varchar[] AS ptg_plan_array,
            ARRAY_REMOVE(ARRAY_AGG(DISTINCT ptg_source.value ORDER BY ptg_source.value), NULL)::varchar[] AS ptg_source_array,
            ARRAY_REMOVE(ARRAY_AGG(DISTINCT group_plan.value ORDER BY group_plan.value), NULL)::varchar[] AS group_plan_array,
            MAX(updated_at)::timestamp AS updated_at
          FROM {db_schema}.{raw_table}
          LEFT JOIN LATERAL unnest(COALESCE(aca_plan_array, ARRAY[]::varchar[])) AS aca_plan(value) ON TRUE
          LEFT JOIN LATERAL unnest(COALESCE(aca_network_array, ARRAY[]::varchar[])) AS aca_network(value) ON TRUE
          LEFT JOIN LATERAL unnest(COALESCE(ptg_plan_array, ARRAY[]::varchar[])) AS ptg_plan(value) ON TRUE
          LEFT JOIN LATERAL unnest(COALESCE(ptg_source_array, ARRAY[]::varchar[])) AS ptg_source(value) ON TRUE
          LEFT JOIN LATERAL unnest(COALESCE(group_plan_array, ARRAY[]::varchar[])) AS group_plan(value) ON TRUE
         {shard_filter}
         GROUP BY entity_type, entity_id, type, {dedupe_key_expr}
    )
    SELECT
        entity_type,
        entity_id,
        npi,
        inferred_npi,
        inference_confidence,
        inference_method,
        entity_name,
        entity_subtype,
        location_key,
        COALESCE(row_origin, 'base') AS row_origin,
        COALESCE(archive_identity_version, '{ARCHIVE_IDENTITY_VERSION}') AS archive_identity_version,
        COALESCE(address_precision, 'unknown') AS address_precision,
        premise_key,
        zip5,
        state_code,
        city_norm,
        county_fips,
        COALESCE(source_mask, 0)::bigint AS source_mask,
        COALESCE(address_source_mask, 0)::bigint AS address_source_mask,
        COALESCE(CARDINALITY(address_sources), 0)::int AS source_count,
        COALESCE(CARDINALITY(address_sources), 0)::int AS independent_source_count,
        (COALESCE(CARDINALITY(address_sources), 0) > 1) AS multi_source_confirmed,
        COALESCE(location_confidence_id, 0)::smallint AS location_confidence_id,
        LEAST(
            100,
            GREATEST(
                0,
                (CASE WHEN address_precision = 'city_zip' THEN 5 WHEN address_precision = 'unknown' THEN 0 ELSE 35 END)
                + (CASE WHEN address_key IS NOT NULL THEN 25 ELSE 3 END)
                + LEAST(COALESCE(CARDINALITY(address_sources), 0) * 5, 20)
                + (CASE WHEN lat IS NOT NULL AND long IS NOT NULL THEN 10 ELSE 0 END)
                - (CASE WHEN address_precision = 'city_zip' THEN 25 ELSE 0 END)
            )
        )::smallint AS confidence_score,
        (CASE WHEN updated_at >= NOW() - INTERVAL '12 months' THEN 10 ELSE 0 END)::smallint AS freshness_score,
        COALESCE(address_sources, ARRAY[]::varchar[]) AS address_sources,
        COALESCE(source_record_ids, ARRAY[]::varchar[]) AS source_record_ids,
        COALESCE(aca_plan_array, ARRAY[]::varchar[]) AS aca_plan_array,
        COALESCE(aca_network_array, ARRAY[]::varchar[]) AS aca_network_array,
        COALESCE(ptg_plan_array, ARRAY[]::varchar[]) AS ptg_plan_array,
        COALESCE(ptg_source_array, ARRAY[]::varchar[]) AS ptg_source_array,
        COALESCE(group_plan_array, ARRAY[]::varchar[]) AS group_plan_array,
        base_address_version,
        ptg_address_version,
        checksum,
        type,
        taxonomy_array,
        plans_network_array,
        procedures_array,
        medications_array,
        first_line,
        second_line,
        city_name,
        state_name,
        postal_code,
        country_code,
        telephone_number,
        fax_number,
        formatted_address,
        lat,
        long,
        date_added,
        place_id,
        address_key,
        updated_at,
        updated_at AS last_seen_at
      FROM aggregated;
    """


def _materialize_sql(
    db_schema: str,
    stage_table: str,
    source_selects: Iterable[str],
    *,
    address_canon_available: bool = True,
) -> str:
    selects_sql = "\nUNION ALL\n".join(select.strip() for select in source_selects)
    dedupe_key_expr = _dedupe_key_expr(address_canon_available)
    return f"""
    INSERT INTO {db_schema}.{stage_table} (
        entity_type,
        entity_id,
        npi,
        inferred_npi,
        inference_confidence,
        inference_method,
        entity_name,
        entity_subtype,
        source_count,
        multi_source_confirmed,
        address_sources,
        source_record_ids,
        checksum,
        type,
        taxonomy_array,
        plans_network_array,
        procedures_array,
        medications_array,
        first_line,
        second_line,
        city_name,
        state_name,
        postal_code,
        country_code,
        telephone_number,
        fax_number,
        formatted_address,
        lat,
        long,
        date_added,
        place_id,
        address_key,
        updated_at
    )
    WITH base_rows AS (
        {selects_sql}
    ),
    sanitized AS (
        SELECT
            entity_type,
            entity_id,
            npi,
            inferred_npi,
            inference_confidence,
            inference_method,
            NULLIF(TRIM(entity_name), '')::varchar AS entity_name,
            NULLIF(TRIM(entity_subtype), '')::varchar AS entity_subtype,
            COALESCE(NULLIF(TRIM(type), ''), 'primary')::varchar AS type,
            COALESCE(taxonomy_array, ARRAY[0]::int[])::int[] AS taxonomy_array,
            COALESCE(plans_network_array, ARRAY[0]::int[])::int[] AS plans_network_array,
            COALESCE(procedures_array, ARRAY[0]::int[])::int[] AS procedures_array,
            COALESCE(medications_array, ARRAY[0]::int[])::int[] AS medications_array,
            NULLIF(TRIM(first_line), '')::varchar AS first_line,
            NULLIF(TRIM(second_line), '')::varchar AS second_line,
            NULLIF(TRIM(city_name), '')::varchar AS city_name,
            NULLIF(TRIM(state_name), '')::varchar AS state_name,
            NULLIF(TRIM(postal_code), '')::varchar AS postal_code,
            COALESCE(NULLIF(TRIM(country_code), ''), 'US')::varchar AS country_code,
            NULLIF(TRIM(telephone_number), '')::varchar AS telephone_number,
            NULLIF(TRIM(fax_number), '')::varchar AS fax_number,
            NULLIF(TRIM(formatted_address), '')::varchar AS formatted_address,
            lat::numeric AS lat,
            long::numeric AS long,
            date_added::date AS date_added,
            NULLIF(TRIM(place_id), '')::varchar AS place_id,
            {_source_priority_expr("address_source")}::int AS source_priority,
            address_source::varchar AS address_source,
            source_record_id::varchar AS source_record_id,
            updated_at::timestamp AS updated_at,
            {_address_key_expr(db_schema, address_canon_available)} AS address_key
          FROM base_rows
    ),
    normalized AS (
        SELECT
            entity_type,
            entity_id,
            npi,
            inferred_npi,
            inference_confidence,
            inference_method,
            entity_name,
            entity_subtype,
            type,
            taxonomy_array,
            plans_network_array,
            procedures_array,
            medications_array,
            first_line,
            second_line,
            city_name,
            state_name,
            postal_code,
            country_code,
            telephone_number,
            fax_number,
            formatted_address,
            lat,
            long,
            date_added,
            place_id,
            source_priority,
            address_source,
            source_record_id,
            updated_at,
            address_key,
            {_address_checksum_expr(
                first_line_expr=_alnum_norm_expr("first_line"),
                second_line_expr=_alnum_norm_expr("second_line"),
                city_name_expr=_alnum_norm_expr("city_name"),
                state_name_expr=_state_norm_expr("state_name"),
                postal_code_expr=_zip5_norm_expr("postal_code"),
                country_code_expr=_state_norm_expr("country_code"),
                telephone_number_expr=_phone_norm_expr("telephone_number"),
            )} AS checksum
          FROM sanitized
    ),
    aggregated AS (
        SELECT
            entity_type,
            entity_id,
            type,
            (ARRAY_AGG(checksum ORDER BY source_priority ASC, updated_at DESC NULLS LAST))[1]::bigint AS checksum,
            MAX(npi)::bigint AS npi,
            MAX(inferred_npi)::bigint AS inferred_npi,
            MAX(inference_confidence)::float8 AS inference_confidence,
            MAX(inference_method)::varchar AS inference_method,
            (ARRAY_AGG(entity_name ORDER BY source_priority ASC, (entity_name IS NULL), LENGTH(COALESCE(entity_name, '')) DESC, updated_at DESC NULLS LAST))[1]::varchar AS entity_name,
            MAX(entity_subtype)::varchar AS entity_subtype,
            COALESCE(MAX(taxonomy_array), ARRAY[0]::int[])::int[] AS taxonomy_array,
            COALESCE(MAX(plans_network_array), ARRAY[0]::int[])::int[] AS plans_network_array,
            COALESCE(MAX(procedures_array), ARRAY[0]::int[])::int[] AS procedures_array,
            COALESCE(MAX(medications_array), ARRAY[0]::int[])::int[] AS medications_array,
            (ARRAY_AGG(first_line ORDER BY source_priority ASC, (first_line IS NULL), LENGTH(COALESCE(first_line, '')) DESC, updated_at DESC NULLS LAST))[1]::varchar AS first_line,
            (ARRAY_AGG(second_line ORDER BY source_priority ASC, (second_line IS NULL), LENGTH(COALESCE(second_line, '')) DESC, updated_at DESC NULLS LAST))[1]::varchar AS second_line,
            (ARRAY_AGG(city_name ORDER BY source_priority ASC, (city_name IS NULL), LENGTH(COALESCE(city_name, '')) DESC, updated_at DESC NULLS LAST))[1]::varchar AS city_name,
            (ARRAY_AGG(state_name ORDER BY source_priority ASC, (state_name IS NULL), LENGTH(COALESCE(state_name, '')) DESC, updated_at DESC NULLS LAST))[1]::varchar AS state_name,
            (ARRAY_AGG(postal_code ORDER BY source_priority ASC, (postal_code IS NULL), LENGTH(COALESCE(postal_code, '')) DESC, updated_at DESC NULLS LAST))[1]::varchar AS postal_code,
            (ARRAY_AGG(country_code ORDER BY source_priority ASC, (country_code IS NULL), LENGTH(COALESCE(country_code, '')) DESC, updated_at DESC NULLS LAST))[1]::varchar AS country_code,
            (ARRAY_AGG(telephone_number ORDER BY source_priority ASC, (telephone_number IS NULL), LENGTH(COALESCE(telephone_number, '')) DESC, updated_at DESC NULLS LAST))[1]::varchar AS telephone_number,
            (ARRAY_AGG(fax_number ORDER BY source_priority ASC, (fax_number IS NULL), LENGTH(COALESCE(fax_number, '')) DESC, updated_at DESC NULLS LAST))[1]::varchar AS fax_number,
            (ARRAY_AGG(formatted_address ORDER BY source_priority ASC, (formatted_address IS NULL), LENGTH(COALESCE(formatted_address, '')) DESC, updated_at DESC NULLS LAST))[1]::varchar AS formatted_address,
            (ARRAY_AGG(lat ORDER BY source_priority ASC, (lat IS NULL), updated_at DESC NULLS LAST))[1]::numeric AS lat,
            (ARRAY_AGG(long ORDER BY source_priority ASC, (long IS NULL), updated_at DESC NULLS LAST))[1]::numeric AS long,
            MAX(date_added)::date AS date_added,
            MAX(place_id)::varchar AS place_id,
            (ARRAY_AGG(address_key ORDER BY source_priority ASC, (address_key IS NULL), updated_at DESC NULLS LAST))[1]::uuid AS address_key,
            ARRAY_REMOVE(ARRAY_AGG(DISTINCT address_source ORDER BY address_source), NULL)::varchar[] AS address_sources,
            ARRAY_REMOVE(ARRAY_AGG(DISTINCT source_record_id ORDER BY source_record_id), NULL)::varchar[] AS source_record_ids,
            MAX(updated_at)::timestamp AS updated_at
          FROM normalized
      GROUP BY entity_type, entity_id, type, {dedupe_key_expr}
    )
    SELECT
        entity_type,
        entity_id,
        npi,
        inferred_npi,
        inference_confidence,
        inference_method,
        entity_name,
        entity_subtype,
        COALESCE(CARDINALITY(address_sources), 0)::int AS source_count,
        (COALESCE(CARDINALITY(address_sources), 0) > 1) AS multi_source_confirmed,
        COALESCE(address_sources, ARRAY[]::varchar[]) AS address_sources,
        COALESCE(source_record_ids, ARRAY[]::varchar[]) AS source_record_ids,
        checksum,
        type,
        taxonomy_array,
        plans_network_array,
        procedures_array,
        medications_array,
        first_line,
        second_line,
        city_name,
        state_name,
        postal_code,
        country_code,
        telephone_number,
        fax_number,
        formatted_address,
        lat,
        long,
        date_added,
        place_id,
        address_key,
        updated_at
      FROM aggregated;
    """


def _evidence_group_hash_expr(evidence_shards: int) -> str:
    shards = max(int(evidence_shards), 1)
    return f"""
        (((hashtext(CONCAT_WS(
            '|',
            COALESCE(entity_type, ''),
            COALESCE(entity_id, ''),
            COALESCE(street_key, ''),
            COALESCE(city_key, ''),
            COALESCE(state_key, ''),
            COALESCE(zip_key, ''),
            COALESCE(country_key, '')
        )) % {shards}) + {shards}) % {shards})::int
    """


def _prepare_multi_source_evidence_table_sql(
    db_schema: str,
    evidence_table: str,
    *,
    unlogged: bool = True,
) -> str:
    storage_mode = "UNLOGGED " if unlogged else ""
    return f"""
    CREATE {storage_mode}TABLE {db_schema}.{evidence_table} (
        location_key varchar(64) PRIMARY KEY,
        evidence_shard int NOT NULL,
        evidence_sources varchar[] NOT NULL DEFAULT '{{}}',
        evidence_record_ids varchar[] NOT NULL DEFAULT '{{}}'
    );
    """


def _insert_multi_source_evidence_shard_sql(
    db_schema: str,
    stage_table: str,
    evidence_table: str,
    *,
    evidence_shards: int,
    evidence_shard: int,
) -> str:
    group_hash_expr = _evidence_group_hash_expr(evidence_shards)
    return f"""
    WITH normalized AS (
        SELECT
            location_key,
            entity_type,
            entity_id,
            {_street_soft_norm_expr("first_line")}::varchar AS street_key,
            {_alnum_norm_expr("city_name")}::varchar AS city_key,
            {_state_norm_expr("state_name")}::varchar AS state_key,
            {_zip5_norm_expr("postal_code")}::varchar AS zip_key,
            {_state_norm_expr("country_code")}::varchar AS country_key
          FROM {db_schema}.{stage_table}
         WHERE location_key IS NOT NULL
    ),
    keyed AS MATERIALIZED (
        SELECT
            location_key,
            entity_type,
            entity_id,
            street_key,
            city_key,
            state_key,
            zip_key,
            country_key,
            {group_hash_expr} AS evidence_shard
          FROM normalized
         WHERE {group_hash_expr} = {int(evidence_shard)}
    ),
    source_evidence AS (
        SELECT
            k.entity_type,
            k.entity_id,
            k.street_key,
            k.city_key,
            k.state_key,
            k.zip_key,
            k.country_key,
            ARRAY_REMOVE(ARRAY_AGG(DISTINCT src.src ORDER BY src.src), NULL)::varchar[] AS evidence_sources
          FROM keyed AS k
          JOIN {db_schema}.{stage_table} AS t
            ON t.location_key = k.location_key
          LEFT JOIN LATERAL unnest(COALESCE(t.address_sources, ARRAY[]::varchar[])) AS src(src) ON TRUE
         GROUP BY
            k.entity_type,
            k.entity_id,
            k.street_key,
            k.city_key,
            k.state_key,
            k.zip_key,
            k.country_key
    ),
    record_evidence AS (
        SELECT
            k.entity_type,
            k.entity_id,
            k.street_key,
            k.city_key,
            k.state_key,
            k.zip_key,
            k.country_key,
            ARRAY_REMOVE(ARRAY_AGG(DISTINCT rid.rid ORDER BY rid.rid), NULL)::varchar[] AS evidence_record_ids
          FROM keyed AS k
          JOIN {db_schema}.{stage_table} AS t
            ON t.location_key = k.location_key
          LEFT JOIN LATERAL unnest(COALESCE(t.source_record_ids, ARRAY[]::varchar[])) AS rid(rid) ON TRUE
         GROUP BY
            k.entity_type,
            k.entity_id,
            k.street_key,
            k.city_key,
            k.state_key,
            k.zip_key,
            k.country_key
    )
    INSERT INTO {db_schema}.{evidence_table} (
        location_key,
        evidence_shard,
        evidence_sources,
        evidence_record_ids
    )
    SELECT
        k.location_key,
        k.evidence_shard,
        COALESCE(se.evidence_sources, ARRAY[]::varchar[]) AS evidence_sources,
        COALESCE(re.evidence_record_ids, ARRAY[]::varchar[]) AS evidence_record_ids
      FROM keyed AS k
      LEFT JOIN source_evidence AS se
        ON se.entity_type = k.entity_type
       AND se.entity_id = k.entity_id
       AND se.street_key IS NOT DISTINCT FROM k.street_key
       AND se.city_key IS NOT DISTINCT FROM k.city_key
       AND se.state_key IS NOT DISTINCT FROM k.state_key
       AND se.zip_key IS NOT DISTINCT FROM k.zip_key
       AND se.country_key IS NOT DISTINCT FROM k.country_key
      LEFT JOIN record_evidence AS re
        ON re.entity_type = k.entity_type
       AND re.entity_id = k.entity_id
       AND re.street_key IS NOT DISTINCT FROM k.street_key
       AND re.city_key IS NOT DISTINCT FROM k.city_key
       AND re.state_key IS NOT DISTINCT FROM k.state_key
       AND re.zip_key IS NOT DISTINCT FROM k.zip_key
       AND re.country_key IS NOT DISTINCT FROM k.country_key;
    """


def _index_multi_source_evidence_table_sql(db_schema: str, evidence_table: str) -> str:
    index_name = _archived_identifier(f"{evidence_table}_idx_shard_location", "")
    return f"""
    CREATE INDEX IF NOT EXISTS {index_name}
        ON {db_schema}.{evidence_table} (evidence_shard, location_key);
    """


def _apply_multi_source_evidence_sql(
    db_schema: str,
    stage_table: str,
    evidence_table: str,
    *,
    evidence_shard: int,
) -> str:
    return f"""
    UPDATE {db_schema}.{stage_table} AS t
       SET address_sources = e.evidence_sources,
           source_record_ids = e.evidence_record_ids,
           source_count = COALESCE(CARDINALITY(e.evidence_sources), 0)::int,
           independent_source_count = COALESCE(CARDINALITY(e.evidence_sources), 0)::int,
           multi_source_confirmed = COALESCE(CARDINALITY(e.evidence_sources), 0) > 1
      FROM {db_schema}.{evidence_table} AS e
     WHERE e.evidence_shard = {int(evidence_shard)}
       AND t.location_key = e.location_key
       AND (
            COALESCE(t.address_sources, ARRAY[]::varchar[]) IS DISTINCT FROM e.evidence_sources
            OR COALESCE(t.source_record_ids, ARRAY[]::varchar[]) IS DISTINCT FROM e.evidence_record_ids
            OR COALESCE(t.source_count, 0) IS DISTINCT FROM COALESCE(CARDINALITY(e.evidence_sources), 0)
            OR COALESCE(t.independent_source_count, 0)
                IS DISTINCT FROM COALESCE(CARDINALITY(e.evidence_sources), 0)
            OR COALESCE(t.multi_source_confirmed, FALSE)
                IS DISTINCT FROM (COALESCE(CARDINALITY(e.evidence_sources), 0) > 1)
       );
    """


def _truncate_support_stage_sql(db_schema: str, stage_tables: dict[type, str]) -> str:
    table_names = ", ".join(f"{db_schema}.{table}" for table in stage_tables.values())
    return f"TRUNCATE TABLE {table_names};"


def _evidence_from_raw_sql(
    db_schema: str,
    evidence_stage_table: str,
    raw_table: str,
    *,
    source_run_id: str,
    node_id: str | None,
) -> str:
    return f"""
    INSERT INTO {db_schema}.{evidence_stage_table} (
        evidence_id,
        location_key,
        address_key,
        premise_key,
        archive_identity_version,
        entity_type,
        entity_id,
        npi,
        tin,
        source_id,
        source_record_key,
        source_run_id,
        source_snapshot_id,
        node_id,
        plan_id,
        network_id,
        ptg_plan_id,
        ptg_source_key,
        ptg_snapshot_id,
        market_type,
        address_role_id,
        location_confidence_id,
        address_precision,
        observed_at,
        last_seen_at,
        retired_at
    )
    SELECT
        ROW_NUMBER() OVER (ORDER BY location_key, source_id, source_record_id)::bigint AS evidence_id,
        location_key,
        address_key,
        premise_key,
        archive_identity_version,
        entity_type,
        entity_id,
        npi,
        NULL::varchar AS tin,
        source_id,
        source_record_id AS source_record_key,
        {_sql_literal(source_run_id)}::varchar AS source_run_id,
        CASE WHEN address_source = 'ptg' THEN NULLIF(split_part(source_record_id, ':', 3), '') ELSE NULL END::varchar
            AS source_snapshot_id,
        {_sql_literal(node_id)}::varchar AS node_id,
        NULL::varchar AS plan_id,
        NULL::varchar AS network_id,
        CASE WHEN CARDINALITY(ptg_plan_array) = 1 THEN ptg_plan_array[1] ELSE NULL END::varchar AS ptg_plan_id,
        CASE WHEN address_source = 'ptg' THEN NULLIF(split_part(source_record_id, ':', 2), '') ELSE NULL END::varchar
            AS ptg_source_key,
        CASE WHEN address_source = 'ptg' THEN NULLIF(split_part(source_record_id, ':', 3), '') ELSE NULL END::varchar
            AS ptg_snapshot_id,
        NULL::varchar AS market_type,
        address_role_id,
        location_confidence_id,
        address_precision,
        updated_at::timestamptz AS observed_at,
        updated_at::timestamptz AS last_seen_at,
        NULL::timestamptz AS retired_at
      FROM {db_schema}.{raw_table}
     WHERE location_key IS NOT NULL;
    """


def _evidence_from_stage_sql(
    db_schema: str,
    evidence_stage_table: str,
    stage_table: str,
    *,
    source_run_id: str,
    node_id: str | None,
) -> str:
    return f"""
    INSERT INTO {db_schema}.{evidence_stage_table} (
        evidence_id,
        location_key,
        address_key,
        premise_key,
        archive_identity_version,
        entity_type,
        entity_id,
        npi,
        tin,
        source_id,
        source_record_key,
        source_run_id,
        source_snapshot_id,
        node_id,
        plan_id,
        network_id,
        ptg_plan_id,
        ptg_source_key,
        ptg_snapshot_id,
        market_type,
        address_role_id,
        location_confidence_id,
        address_precision,
        observed_at,
        last_seen_at,
        retired_at
    )
    SELECT
        ROW_NUMBER() OVER (ORDER BY t.location_key)::bigint AS evidence_id,
        t.location_key,
        t.address_key,
        t.premise_key,
        t.archive_identity_version,
        t.entity_type,
        t.entity_id,
        t.npi,
        NULL::varchar AS tin,
        0::smallint AS source_id,
        t.location_key AS source_record_key,
        {_sql_literal(source_run_id)}::varchar AS source_run_id,
        NULL::varchar AS source_snapshot_id,
        {_sql_literal(node_id)}::varchar AS node_id,
        NULL::varchar AS plan_id,
        NULL::varchar AS network_id,
        CASE WHEN CARDINALITY(t.ptg_plan_array) = 1 THEN t.ptg_plan_array[1] ELSE NULL END::varchar AS ptg_plan_id,
        CASE WHEN CARDINALITY(t.ptg_source_array) = 1 THEN t.ptg_source_array[1] ELSE NULL END::varchar AS ptg_source_key,
        NULL::varchar AS ptg_snapshot_id,
        NULL::varchar AS market_type,
        NULL::smallint AS address_role_id,
        t.location_confidence_id,
        t.address_precision,
        t.updated_at::timestamptz AS observed_at,
        t.last_seen_at::timestamptz AS last_seen_at,
        NULL::timestamptz AS retired_at
      FROM {db_schema}.{stage_table} AS t
     WHERE t.location_key IS NOT NULL;
    """


def _plan_bridge_sql(db_schema: str, plan_stage_table: str, stage_table: str) -> str:
    return f"""
    INSERT INTO {db_schema}.{plan_stage_table} (location_key, entity_type, entity_id, plan_id, market_type)
    SELECT DISTINCT
        t.location_key,
        t.entity_type,
        t.entity_id,
        plan_id.value AS plan_id,
        NULL::varchar AS market_type
      FROM {db_schema}.{stage_table} AS t
      JOIN LATERAL unnest(COALESCE(t.aca_plan_array, ARRAY[]::varchar[])) AS plan_id(value) ON TRUE
     WHERE t.location_key IS NOT NULL
       AND NULLIF(plan_id.value, '') IS NOT NULL;
    """


def _network_bridge_sql(db_schema: str, network_stage_table: str, stage_table: str) -> str:
    return f"""
    INSERT INTO {db_schema}.{network_stage_table} (location_key, entity_type, entity_id, network_id)
    SELECT DISTINCT location_key, entity_type, entity_id, network_id
      FROM (
        SELECT
            t.location_key,
            t.entity_type,
            t.entity_id,
            legacy_network.value::text AS network_id
          FROM {db_schema}.{stage_table} AS t
          JOIN LATERAL unnest(COALESCE(t.plans_network_array, ARRAY[]::int[])) AS legacy_network(value) ON TRUE
         WHERE t.location_key IS NOT NULL
           AND legacy_network.value <> 0
        UNION ALL
        SELECT
            t.location_key,
            t.entity_type,
            t.entity_id,
            aca_network.value AS network_id
          FROM {db_schema}.{stage_table} AS t
          JOIN LATERAL unnest(COALESCE(t.aca_network_array, ARRAY[]::varchar[])) AS aca_network(value) ON TRUE
         WHERE t.location_key IS NOT NULL
           AND NULLIF(aca_network.value, '') IS NOT NULL
      ) AS bridge_rows;
    """


def _ptg_bridge_sql(db_schema: str, ptg_stage_table: str, stage_table: str) -> str:
    return f"""
    INSERT INTO {db_schema}.{ptg_stage_table} (
        location_key,
        entity_type,
        entity_id,
        source_key,
        snapshot_id,
        ptg_plan_id
    )
    SELECT DISTINCT
        t.location_key,
        t.entity_type,
        t.entity_id,
        NULLIF(split_part(record_id.value, ':', 2), '') AS source_key,
        NULLIF(split_part(record_id.value, ':', 3), '') AS snapshot_id,
        plan_id.value AS ptg_plan_id
      FROM {db_schema}.{stage_table} AS t
      JOIN LATERAL unnest(COALESCE(t.source_record_ids, ARRAY[]::varchar[])) AS record_id(value) ON TRUE
      JOIN LATERAL unnest(COALESCE(t.ptg_plan_array, ARRAY[]::varchar[])) AS plan_id(value) ON TRUE
     WHERE t.location_key IS NOT NULL
       AND record_id.value LIKE 'ptg:%'
       AND NULLIF(plan_id.value, '') IS NOT NULL
       AND NULLIF(split_part(record_id.value, ':', 2), '') IS NOT NULL
       AND NULLIF(split_part(record_id.value, ':', 3), '') IS NOT NULL;
    """


def _procedure_bridge_sql(db_schema: str, procedure_stage_table: str, stage_table: str) -> str:
    return f"""
    INSERT INTO {db_schema}.{procedure_stage_table} (location_key, npi, code_system, code)
    SELECT DISTINCT
        t.location_key,
        t.npi,
        'HP_PROCEDURE_CODE'::varchar AS code_system,
        procedure_code.value::text AS code
      FROM {db_schema}.{stage_table} AS t
      JOIN LATERAL unnest(COALESCE(t.procedures_array, ARRAY[]::int[])) AS procedure_code(value) ON TRUE
     WHERE t.location_key IS NOT NULL
       AND t.npi IS NOT NULL
       AND procedure_code.value <> 0;
    """


def _medication_bridge_sql(db_schema: str, medication_stage_table: str, stage_table: str) -> str:
    return f"""
    INSERT INTO {db_schema}.{medication_stage_table} (location_key, npi, code_system, code)
    SELECT DISTINCT
        t.location_key,
        t.npi,
        'HP_RX_CODE'::varchar AS code_system,
        medication_code.value::text AS code
      FROM {db_schema}.{stage_table} AS t
      JOIN LATERAL unnest(COALESCE(t.medications_array, ARRAY[]::int[])) AS medication_code(value) ON TRUE
     WHERE t.location_key IS NOT NULL
       AND t.npi IS NOT NULL
       AND medication_code.value <> 0;
    """


def _support_stage_sql(
    db_schema: str,
    stage_table: str,
    stage_classes: dict[type, type],
    *,
    source_run_id: str,
    node_id: str | None,
    raw_table: str | None = None,
) -> list[str]:
    stage_tables = {model: stage_cls.__tablename__ for model, stage_cls in stage_classes.items()}
    evidence_sql = (
        _evidence_from_raw_sql(
            db_schema,
            stage_tables[EntityAddressEvidence],
            raw_table,
            source_run_id=source_run_id,
            node_id=node_id,
        )
        if raw_table
        else _evidence_from_stage_sql(
            db_schema,
            stage_tables[EntityAddressEvidence],
            stage_table,
            source_run_id=source_run_id,
            node_id=node_id,
        )
    )
    return [
        _truncate_support_stage_sql(db_schema, stage_tables),
        evidence_sql,
        _plan_bridge_sql(db_schema, stage_tables[EntityAddressPlanBridge], stage_table),
        _network_bridge_sql(db_schema, stage_tables[EntityAddressNetworkBridge], stage_table),
        _ptg_bridge_sql(db_schema, stage_tables[EntityAddressPTGBridge], stage_table),
        _procedure_bridge_sql(db_schema, stage_tables[EntityAddressProcedureBridge], stage_table),
        _medication_bridge_sql(db_schema, stage_tables[EntityAddressMedicationBridge], stage_table),
    ]


async def _populate_support_stage_tables(
    db_schema: str,
    stage_table: str,
    stage_classes: dict[type, type],
    *,
    source_run_id: str,
    node_id: str | None,
    raw_table: str | None = None,
) -> dict[str, int]:
    for statement in _support_stage_sql(
        db_schema,
        stage_table,
        stage_classes,
        source_run_id=source_run_id,
        node_id=node_id,
        raw_table=raw_table,
    ):
        await db.status(statement)
    counts: dict[str, int] = {}
    for model, stage_cls in stage_classes.items():
        counts[model.__tablename__] = int(
            await db.scalar(f"SELECT COUNT(*) FROM {db_schema}.{stage_cls.__tablename__};")
            or 0
        )
    return counts


def _inference_sql(
    db_schema: str,
    stage_table: str,
    *,
    include_hospital_enrollment: bool,
    include_fqhc_enrollment: bool,
) -> str:
    hospital_ccn_candidates_sql = (
        f"""
        SELECT
            t.entity_type,
            t.entity_id,
            t.type,
            t.checksum,
            MIN(h.npi)::bigint AS candidate_npi,
            COUNT(DISTINCT h.npi)::int AS candidate_npi_count
          FROM {db_schema}.{stage_table} AS t
          JOIN {db_schema}.provider_enrollment_hospital AS h
            ON h.ccn = t.entity_id
           AND h.npi IS NOT NULL
         WHERE t.npi IS NULL
           AND t.inferred_npi IS NULL
           AND t.entity_type = 'facility_anchor'
           AND COALESCE(t.entity_subtype, '') = 'Hospital'
         GROUP BY t.entity_type, t.entity_id, t.type, t.checksum
        """
        if include_hospital_enrollment
        else """
        SELECT
            NULL::varchar AS entity_type,
            NULL::varchar AS entity_id,
            NULL::varchar AS type,
            NULL::bigint AS checksum,
            NULL::bigint AS candidate_npi,
            0::int AS candidate_npi_count
         WHERE FALSE
        """
    )

    fqhc_exact_candidates_sql = (
        f"""
        SELECT
            t.entity_type,
            t.entity_id,
            t.type,
            t.checksum,
            MIN(f.npi)::bigint AS candidate_npi,
            COUNT(DISTINCT f.npi)::int AS candidate_npi_count
          FROM {db_schema}.{stage_table} AS t
          JOIN {db_schema}.provider_enrollment_fqhc AS f
            ON f.npi IS NOT NULL
           AND LEFT(COALESCE(f.zip_code, ''), 5) = LEFT(COALESCE(t.postal_code, ''), 5)
           AND UPPER(COALESCE(f.state, '')) = UPPER(COALESCE(t.state_name, ''))
           AND regexp_replace(LOWER(COALESCE(f.address_line_1, '')), '[^a-z0-9]', '', 'g')
               = regexp_replace(LOWER(COALESCE(t.first_line, '')), '[^a-z0-9]', '', 'g')
           AND (
                regexp_replace(LOWER(COALESCE(f.organization_name, '')), '[^a-z0-9]', '', 'g')
                    = regexp_replace(LOWER(COALESCE(t.entity_name, '')), '[^a-z0-9]', '', 'g')
                OR regexp_replace(LOWER(COALESCE(f.doing_business_as_name, '')), '[^a-z0-9]', '', 'g')
                    = regexp_replace(LOWER(COALESCE(t.entity_name, '')), '[^a-z0-9]', '', 'g')
           )
         WHERE t.npi IS NULL
           AND t.inferred_npi IS NULL
           AND t.entity_type = 'facility_anchor'
           AND COALESCE(t.entity_subtype, '') = 'FQHC'
         GROUP BY t.entity_type, t.entity_id, t.type, t.checksum
        """
        if include_fqhc_enrollment
        else """
        SELECT
            NULL::varchar AS entity_type,
            NULL::varchar AS entity_id,
            NULL::varchar AS type,
            NULL::bigint AS checksum,
            NULL::bigint AS candidate_npi,
            0::int AS candidate_npi_count
         WHERE FALSE
        """
    )

    return f"""
    WITH hospital_ccn_candidates AS (
        {hospital_ccn_candidates_sql}
    ),
    hospital_ccn_winners AS (
        SELECT
            entity_type,
            entity_id,
            type,
            checksum,
            candidate_npi,
            0.99::double precision AS winner_confidence,
            'hospital_ccn_match'::varchar AS winner_method
          FROM hospital_ccn_candidates
         WHERE candidate_npi_count = 1
    ),
    fqhc_exact_candidates AS (
        {fqhc_exact_candidates_sql}
    ),
    fqhc_exact_winners AS (
        SELECT
            entity_type,
            entity_id,
            type,
            checksum,
            candidate_npi,
            0.97::double precision AS winner_confidence,
            'fqhc_enrollment_exact_match'::varchar AS winner_method
          FROM fqhc_exact_candidates
         WHERE candidate_npi_count = 1
    ),
    preselected_winners AS (
        SELECT * FROM hospital_ccn_winners
        UNION ALL
        SELECT * FROM fqhc_exact_winners
    ),
    target AS (
        SELECT
            entity_type,
            entity_id,
            type,
            checksum,
            entity_subtype AS facility_type,
            entity_name,
            first_line,
            state_name,
            LEFT(COALESCE(postal_code, ''), 5) AS zip5
          FROM {db_schema}.{stage_table}
         WHERE npi IS NULL
           AND inferred_npi IS NULL
           AND entity_type <> 'npi'
           AND COALESCE(entity_name, '') <> ''
           AND NOT EXISTS (
                SELECT 1
                  FROM preselected_winners AS pw
                 WHERE pw.entity_type = {db_schema}.{stage_table}.entity_type
                   AND pw.entity_id = {db_schema}.{stage_table}.entity_id
                   AND pw.type = {db_schema}.{stage_table}.type
                   AND pw.checksum = {db_schema}.{stage_table}.checksum
           )
    ),
    candidates AS (
        SELECT
            t.entity_type,
            t.entity_id,
            t.type,
            t.checksum,
            t.facility_type,
            t.entity_name AS source_entity_name,
            p.npi::bigint AS candidate_npi,
            CASE
                WHEN p.type = 'primary' THEN 0
                WHEN p.type = 'secondary' THEN 1
                WHEN p.type = 'practice' THEN 2
                ELSE 3
            END::int AS addr_type_rank,
            (
                CASE
                    WHEN regexp_replace(LOWER(COALESCE(t.first_line, '')), '[^a-z0-9]', '', 'g') <> ''
                     AND regexp_replace(LOWER(COALESCE(t.first_line, '')), '[^a-z0-9]', '', 'g')
                         = regexp_replace(LOWER(COALESCE(p.first_line, '')), '[^a-z0-9]', '', 'g')
                    THEN 1 ELSE 0
                END
            )::int AS street_exact,
            (
                CASE
                    WHEN regexp_replace(LOWER(COALESCE(t.first_line, '')), '[^a-z0-9]', '', 'g') <> ''
                     AND regexp_replace(LOWER(COALESCE(t.first_line, '')), '[^a-z0-9]', '', 'g')
                         = regexp_replace(LOWER(COALESCE(p.first_line, '')), '[^a-z0-9]', '', 'g')
                    THEN 2 ELSE 0
                END
                +
                CASE
                    WHEN t.zip5 <> ''
                     AND t.zip5 = LEFT(COALESCE(p.postal_code, ''), 5)
                    THEN 1 ELSE 0
                END
                +
                CASE
                    WHEN COALESCE(t.state_name, '') <> ''
                     AND UPPER(t.state_name) = UPPER(COALESCE(p.state_name, ''))
                    THEN 1 ELSE 0
                END
            )::int AS match_score
          FROM target AS t
          JOIN {db_schema}.npi AS n
            ON regexp_replace(LOWER(COALESCE(t.entity_name, '')), '[^a-z0-9]', '', 'g')
               = regexp_replace(
                    LOWER(
                        COALESCE(NULLIF(n.provider_organization_name, ''), '')
                        || ' ' || COALESCE(NULLIF(n.provider_other_organization_name, ''), '')
                    ),
                    '[^a-z0-9]',
                    '',
                    'g'
               )
          JOIN {db_schema}.npi_address AS p
            ON p.npi = n.npi
           AND p.type IN ('primary', 'secondary', 'practice')
           AND (
                (t.zip5 <> '' AND t.zip5 = LEFT(COALESCE(p.postal_code, ''), 5))
                OR (
                    COALESCE(t.state_name, '') <> ''
                    AND UPPER(t.state_name) = UPPER(COALESCE(p.state_name, ''))
                )
           )
    ),
    candidates_ranked_per_npi AS (
        SELECT
            entity_type,
            entity_id,
            type,
            checksum,
            facility_type,
            source_entity_name,
            candidate_npi,
            match_score,
            street_exact,
            addr_type_rank,
            ROW_NUMBER() OVER (
                PARTITION BY entity_type, entity_id, type, checksum, candidate_npi
                ORDER BY street_exact DESC, match_score DESC, addr_type_rank ASC, candidate_npi
            ) AS candidate_row_rank
          FROM candidates
         WHERE match_score >= 2
    ),
    candidates_by_npi AS (
        SELECT
            entity_type,
            entity_id,
            type,
            checksum,
            facility_type,
            source_entity_name,
            candidate_npi,
            match_score,
            street_exact,
            addr_type_rank
          FROM candidates_ranked_per_npi
         WHERE candidate_row_rank = 1
    ),
    primary_taxonomy AS (
        SELECT
            npi::bigint AS npi,
            healthcare_provider_taxonomy_code AS taxonomy_code
          FROM {db_schema}.npi_taxonomy
         WHERE healthcare_provider_primary_taxonomy_switch = 'Y'
    ),
    candidate_enriched AS (
        SELECT
            c.entity_type,
            c.entity_id,
            c.type,
            c.checksum,
            c.facility_type,
            c.candidate_npi,
            COALESCE(np.provider_organization_name, '') AS candidate_org_name,
            COALESCE(np.do_business_as_text, '') AS candidate_dba_name,
            COALESCE(np.entity_type_code, 0)::int AS candidate_entity_type_code,
            np.provider_enumeration_date AS candidate_enumeration_date,
            np.npi_deactivation_date AS candidate_deactivation_date,
            c.match_score,
            c.street_exact,
            c.addr_type_rank,
            COALESCE(pes.has_hospital_enrollment, FALSE) AS has_hospital_enrollment,
            COALESCE(pes.has_fqhc_enrollment, FALSE) AS has_fqhc_enrollment,
            pt.taxonomy_code,
            COALESCE(nu.classification, '') AS taxonomy_classification,
            COALESCE(nu.specialization, '') AS taxonomy_specialization,
            CASE
                WHEN c.entity_type = 'facility_anchor'
                 AND COALESCE(c.facility_type, '') = 'Hospital'
                THEN CASE
                    WHEN COALESCE(pes.has_hospital_enrollment, FALSE) THEN 3
                    WHEN pt.taxonomy_code = '282N00000X'
                      OR COALESCE(nu.classification, '') = 'General Acute Care Hospital' THEN 2
                    ELSE 1
                END
                WHEN c.entity_type = 'facility_anchor'
                 AND COALESCE(c.facility_type, '') = 'FQHC'
                THEN CASE
                    WHEN COALESCE(pes.has_fqhc_enrollment, FALSE) THEN 3
                    WHEN pt.taxonomy_code = '261QF0400X'
                      OR (
                          COALESCE(nu.classification, '') = 'Clinic/Center'
                          AND COALESCE(nu.specialization, '') ILIKE '%federally qualified health center%'
                      ) THEN 2
                    ELSE 1
                END
                ELSE 0
            END::int AS facility_tier,
            CASE
                WHEN c.entity_type = 'facility_anchor'
                 AND COALESCE(c.facility_type, '') = 'Hospital'
                THEN CASE
                    WHEN pt.taxonomy_code = '282N00000X'
                      OR COALESCE(nu.classification, '') = 'General Acute Care Hospital' THEN 4
                    WHEN COALESCE(nu.classification, '') ILIKE '%hospital%' THEN 3
                    WHEN COALESCE(nu.classification, '') ILIKE '%unit%' THEN 1
                    ELSE 0
                END
                WHEN c.entity_type = 'facility_anchor'
                 AND COALESCE(c.facility_type, '') = 'FQHC'
                THEN CASE
                    WHEN pt.taxonomy_code = '261QF0400X'
                      OR (
                          COALESCE(nu.classification, '') = 'Clinic/Center'
                          AND COALESCE(nu.specialization, '') ILIKE '%federally qualified health center%'
                      ) THEN 2
                    ELSE 0
                END
                ELSE 0
            END::int AS facility_subtype_rank
          FROM candidates_by_npi AS c
          LEFT JOIN {db_schema}.npi AS np
            ON np.npi = c.candidate_npi
          LEFT JOIN {db_schema}.provider_enrichment_summary AS pes
            ON pes.npi = c.candidate_npi
          LEFT JOIN primary_taxonomy AS pt
            ON pt.npi = c.candidate_npi
          LEFT JOIN {db_schema}.nucc_taxonomy AS nu
            ON nu.code = pt.taxonomy_code
    ),
    ranked AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY entity_type, entity_id, type, checksum
                ORDER BY match_score DESC, candidate_npi
            ) AS rn,
            COUNT(*) OVER (
                PARTITION BY entity_type, entity_id, type, checksum
            ) AS candidate_count,
            DENSE_RANK() OVER (
                PARTITION BY entity_type, entity_id, type, checksum
                ORDER BY facility_tier DESC, facility_subtype_rank DESC, street_exact DESC, match_score DESC, addr_type_rank ASC
            ) AS facility_rank
          FROM candidate_enriched
    ),
    ranked_with_tie_count AS (
        SELECT
            *,
            COUNT(*) OVER (
                PARTITION BY entity_type, entity_id, type, checksum, facility_rank
            ) AS facility_rank_count,
            ROW_NUMBER() OVER (
                PARTITION BY entity_type, entity_id, type, checksum, facility_rank
                ORDER BY
                    CASE WHEN candidate_entity_type_code = 2 THEN 1 ELSE 0 END DESC,
                    CASE WHEN candidate_deactivation_date IS NULL THEN 1 ELSE 0 END DESC,
                    candidate_enumeration_date ASC NULLS LAST,
                    candidate_npi ASC
            ) AS facility_deterministic_rank
          FROM ranked
    ),
    ranked_org_counts AS (
        SELECT
            entity_type,
            entity_id,
            type,
            checksum,
            facility_rank,
            COUNT(DISTINCT regexp_replace(LOWER(COALESCE(candidate_org_name, '')), '[^a-z0-9]', '', 'g')) AS facility_rank_org_count
          FROM ranked_with_tie_count
         GROUP BY entity_type, entity_id, type, checksum, facility_rank
    ),
    ranked_with_org_count AS (
        SELECT
            r.*,
            COALESCE(o.facility_rank_org_count, 0)::int AS facility_rank_org_count
          FROM ranked_with_tie_count AS r
          LEFT JOIN ranked_org_counts AS o
            ON o.entity_type = r.entity_type
           AND o.entity_id = r.entity_id
           AND o.type = r.type
           AND o.checksum = r.checksum
           AND o.facility_rank = r.facility_rank
    ),
    ranked_winners AS (
        SELECT
            r.entity_type,
            r.entity_id,
            r.type,
            r.checksum,
            r.candidate_npi,
            CASE
                WHEN r.candidate_count = 1 AND r.match_score >= 3 THEN 0.95
                WHEN r.candidate_count = 1 THEN 0.85
                WHEN r.entity_type = 'facility_anchor'
                 AND r.facility_rank = 1
                 AND r.facility_rank_count = 1
                 AND r.facility_tier >= 3
                 AND r.match_score >= 4 THEN 0.96
                WHEN r.entity_type = 'facility_anchor'
                 AND r.facility_rank = 1
                 AND r.facility_rank_count = 1
                 AND r.facility_tier >= 3
                 AND r.match_score >= 3 THEN 0.94
                WHEN r.entity_type = 'facility_anchor'
                 AND r.facility_rank = 1
                 AND r.facility_rank_count = 1
                 AND r.facility_tier >= 2 THEN 0.90
                WHEN r.entity_type = 'facility_anchor'
                 AND COALESCE(r.facility_type, '') = 'FQHC'
                 AND r.facility_rank = 1
                 AND r.facility_rank_count > 1
                 AND r.facility_rank_org_count = 1
                 AND r.facility_tier >= 3
                 AND r.facility_deterministic_rank = 1 THEN 0.88
                WHEN r.entity_type = 'facility_anchor'
                 AND COALESCE(r.facility_type, '') = 'Hospital'
                 AND r.facility_rank = 1
                 AND r.facility_rank_count > 1
                 AND r.facility_rank_org_count = 1
                 AND r.facility_tier >= 3
                 AND r.facility_deterministic_rank = 1 THEN 0.89
                ELSE 0.85
            END::double precision AS winner_confidence,
            CASE
                WHEN r.candidate_count = 1 THEN 'name_zip_street_match'
                WHEN r.entity_type = 'facility_anchor'
                 AND COALESCE(r.facility_type, '') = 'FQHC'
                 AND r.facility_rank = 1
                 AND r.facility_rank_count > 1
                 AND r.facility_rank_org_count = 1
                 AND r.facility_tier >= 3
                 AND r.facility_deterministic_rank = 1 THEN 'name_zip_street_facility_rank_deterministic'
                WHEN r.entity_type = 'facility_anchor'
                 AND COALESCE(r.facility_type, '') = 'Hospital'
                 AND r.facility_rank = 1
                 AND r.facility_rank_count > 1
                 AND r.facility_rank_org_count = 1
                 AND r.facility_tier >= 3
                 AND r.facility_deterministic_rank = 1 THEN 'name_zip_street_facility_rank_deterministic'
                ELSE 'name_zip_street_facility_rank'
            END::varchar AS winner_method
          FROM ranked_with_org_count AS r
         WHERE (
                 (r.rn = 1 AND r.candidate_count = 1)
                 OR (
                     r.entity_type = 'facility_anchor'
                     AND r.facility_rank = 1
                     AND r.facility_rank_count = 1
                     AND r.facility_tier >= 2
                 )
                 OR (
                     r.entity_type = 'facility_anchor'
                     AND COALESCE(r.facility_type, '') = 'FQHC'
                     AND r.facility_rank = 1
                     AND r.facility_rank_count > 1
                     AND r.facility_rank_org_count = 1
                     AND r.facility_tier >= 3
                     AND r.facility_deterministic_rank = 1
                 )
                 OR (
                     r.entity_type = 'facility_anchor'
                     AND COALESCE(r.facility_type, '') = 'Hospital'
                     AND r.facility_rank = 1
                     AND r.facility_rank_count > 1
                     AND r.facility_rank_org_count = 1
                     AND r.facility_tier >= 3
                     AND r.facility_deterministic_rank = 1
                 )
               )
    ),
    inference_winners AS (
        SELECT * FROM preselected_winners
        UNION ALL
        SELECT * FROM ranked_winners
    )
    UPDATE {db_schema}.{stage_table} AS t
       SET inferred_npi = w.candidate_npi,
           inference_confidence = w.winner_confidence,
           inference_method = w.winner_method
      FROM inference_winners AS w
     WHERE t.entity_type = w.entity_type
       AND t.entity_id = w.entity_id
       AND t.type = w.type
       AND t.checksum = w.checksum
       AND t.npi IS NULL
       AND t.inferred_npi IS NULL;
    """


async def process_data(ctx, task=None):
    task = task or {}
    ctx.setdefault("context", {})
    context = ctx["context"]
    run_id = str(context.get("control_run_id") or ctx.get("control_run_id") or "").strip()

    if "test_mode" in task:
        context["test_mode"] = bool(task.get("test_mode"))
    test_mode = bool(context.get("test_mode", False))

    await ensure_database(test_mode)

    import_date = ctx["import_date"]
    db_schema = os.getenv("HLTHPRT_DB_SCHEMA") if os.getenv("HLTHPRT_DB_SCHEMA") else "mrf"
    stage_cls = make_class(EntityAddressUnified, import_date)
    stage_table = stage_cls.__tablename__

    if not ctx["context"].get("stage_prepared"):
        await _ensure_schema_exists(db_schema)
        await db.status(f"DROP TABLE IF EXISTS {db_schema}.{stage_table};")
        await db.create_table(stage_cls.__table__, checkfirst=True)
        ctx["context"]["stage_prepared"] = True
        ctx["context"]["stage_indexes_prepared"] = False
        ctx["context"]["support_stage_prepared"] = False
        ctx["context"]["support_stage_indexes_prepared"] = False

    if not ctx["context"].get("support_stage_prepared"):
        support_stage_classes = await _prepare_support_stage_tables(db_schema, import_date)
        ctx["context"]["support_stage_prepared"] = True
        ctx["context"]["support_stage_indexes_prepared"] = False
    else:
        support_stage_classes = _support_stage_classes(import_date)

    required_checks = [
        "npi",
        "npi_address",
        "provider_enrollment_hospital",
        "provider_enrollment_fqhc",
        "doctor_clinician_address",
        "provider_enrollment_ffs",
        "provider_enrollment_ffs_address",
        "facility_anchor",
        "mrf_address",
        "ptg_address",
        "geo_zip_lookup",
        "address_archive_v2",
    ]
    available = {table: await _table_exists(db_schema, table) for table in required_checks}
    test_limit_per_source: int | None = None
    limit_any_mode_raw = os.getenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_LIMIT_PER_SOURCE")
    if limit_any_mode_raw not in (None, ""):
        test_limit_per_source = max(int(limit_any_mode_raw), 0)
    elif test_mode:
        test_limit_per_source = int(
            os.getenv(
                "HLTHPRT_ENTITY_ADDRESS_UNIFIED_TEST_LIMIT_PER_SOURCE",
                str(DEFAULT_TEST_LIMIT_PER_SOURCE),
            )
        )
    source_selects = _source_selects(
        db_schema,
        available,
        test_limit_per_source=test_limit_per_source,
    )
    source_table_shards = _env_int(
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_SOURCE_TABLE_SHARDS",
        DEFAULT_SOURCE_TABLE_SHARDS,
        minimum=1,
    )
    if source_table_shards > 1 and not test_limit_per_source:
        source_selects = _shard_source_selects(
            db_schema,
            source_selects,
            npi_address_ranges=(
                await _npi_table_ranges(db_schema, "npi_address", source_table_shards)
                if available.get("npi_address", False)
                else []
            ),
            mrf_address_ranges=(
                await _npi_table_ranges(db_schema, "mrf_address", source_table_shards)
                if available.get("mrf_address", False)
                else []
            ),
            doctor_clinician_address_ranges=(
                await _npi_table_ranges(db_schema, "doctor_clinician_address", source_table_shards)
                if available.get("doctor_clinician_address", False)
                else []
            ),
            provider_enrollment_ffs_ranges=(
                await _npi_table_ranges(db_schema, "provider_enrollment_ffs", source_table_shards)
                if available.get("provider_enrollment_ffs", False)
                else []
            ),
        )
    address_canon_available = await _address_canon_available(db_schema)
    if not address_canon_available:
        message = (
            "canonical address SQL functions are not available; "
            "entity_address_unified will publish with NULL address_key values"
        )
        if run_id:
            enqueue_live_progress(
                run_id=run_id,
                importer="entity-address-unified",
                status="warning",
                phase="entity-address-unified canonical unavailable",
                unit="address_key",
                done=0,
                total=1,
                pct=0,
                message=message,
            )
        if _key_v2_enabled():
            raise RuntimeError(
                "HLTHPRT_ENTITY_ADDRESS_UNIFIED_KEY_V2 requires canonical address SQL functions"
            )
        logger.warning(
            "Canonical address SQL functions are not available in schema %s; "
            "entity_address_unified will publish with NULL address_key values.",
            db_schema,
        )
    if not source_selects:
        raise RuntimeError("No source tables are available for entity_address_unified materialization.")
    if run_id:
        enqueue_live_progress(
            run_id=run_id,
            importer="entity-address-unified",
            status="running",
            phase="entity-address-unified sources discovered",
            unit="sources",
            done=0,
            total=len(source_selects),
            message=f"{len(source_selects)} sources discovered",
        )

    chunked_load = _env_bool("HLTHPRT_ENTITY_ADDRESS_UNIFIED_CHUNKED_LOAD", True)
    raw_table: str | None = None
    if chunked_load:
        source_concurrency = _env_int(
            "HLTHPRT_ENTITY_ADDRESS_UNIFIED_SOURCE_CONCURRENCY",
            DEFAULT_SOURCE_CONCURRENCY,
            minimum=1,
        )
        aggregate_shards = _env_int(
            "HLTHPRT_ENTITY_ADDRESS_UNIFIED_AGGREGATE_SHARDS",
            DEFAULT_AGGREGATE_SHARDS,
            minimum=1,
        )
        aggregate_concurrency = min(
            _env_int(
                "HLTHPRT_ENTITY_ADDRESS_UNIFIED_AGGREGATE_CONCURRENCY",
                DEFAULT_AGGREGATE_CONCURRENCY,
                minimum=1,
            ),
            aggregate_shards,
        )
        enrich_shards = _env_int(
            "HLTHPRT_ENTITY_ADDRESS_UNIFIED_ENRICH_SHARDS",
            DEFAULT_ENRICH_SHARDS,
            minimum=1,
        )
        enrich_concurrency = min(
            _env_int(
                "HLTHPRT_ENTITY_ADDRESS_UNIFIED_ENRICH_CONCURRENCY",
                DEFAULT_ENRICH_CONCURRENCY,
                minimum=1,
            ),
            enrich_shards,
        )
        raw_table = _raw_stage_table_name(stage_table)
        use_unlogged_raw = _env_bool("HLTHPRT_ENTITY_ADDRESS_UNIFIED_UNLOGGED_RAW_STAGE", True)

        await db.status(f"DROP TABLE IF EXISTS {db_schema}.{raw_table};")
        await db.status(_prepare_raw_stage_sql(db_schema, raw_table, unlogged=use_unlogged_raw))

        sem = asyncio.Semaphore(source_concurrency)
        source_progress_lock = asyncio.Lock()
        loaded_sources = 0

        async def _load_source(select_sql: str) -> None:
            nonlocal loaded_sources
            async with sem:
                await db.status(
                    _insert_raw_from_source_sql(
                        db_schema,
                        raw_table,
                        select_sql,
                        address_canon_available=address_canon_available,
                    )
                )
            if run_id:
                async with source_progress_lock:
                    loaded_sources += 1
                    enqueue_live_progress(
                        run_id=run_id,
                        importer="entity-address-unified",
                        status="running",
                        phase="entity-address-unified loading sources",
                        unit="sources",
                        done=loaded_sources,
                        total=len(source_selects),
                        message=f"loaded {loaded_sources}/{len(source_selects)} sources",
                    )

        if source_concurrency > 1 and len(source_selects) > 1:
            await asyncio.gather(*(_load_source(select_sql) for select_sql in source_selects))
        else:
            for select_sql in source_selects:
                await _load_source(select_sql)

        if enrich_shards > 1:
            await db.status(
                f"CREATE INDEX IF NOT EXISTS {raw_table}_idx_checksum "
                f"ON {db_schema}.{raw_table} (checksum);"
            )
            await db.status(f"ANALYZE {db_schema}.{raw_table};")
            if run_id:
                enqueue_live_progress(
                    run_id=run_id,
                    importer="entity-address-unified",
                    status="running",
                    phase="entity-address-unified enriching raw",
                    unit="shards",
                    done=0,
                    total=enrich_shards,
                    message=f"enriching {enrich_shards} checksum shards",
                )
            enrich_sem = asyncio.Semaphore(enrich_concurrency)
            enrich_progress_lock = asyncio.Lock()
            enriched_shards = 0
            checksum_ranges = _integer_ranges(-(2**31), 2**31 - 1, enrich_shards)

            async def _enrich_shard(checksum_min: int, checksum_max: int) -> None:
                nonlocal enriched_shards
                async with enrich_sem:
                    await db.status(
                        _enrich_raw_stage_sql(
                            db_schema,
                            raw_table,
                            archive_available=available.get("address_archive_v2", False),
                            checksum_min=checksum_min,
                            checksum_max=checksum_max,
                        )
                    )
                if run_id:
                    async with enrich_progress_lock:
                        enriched_shards += 1
                        enqueue_live_progress(
                            run_id=run_id,
                            importer="entity-address-unified",
                            status="running",
                            phase="entity-address-unified enriching raw",
                            unit="shards",
                            done=enriched_shards,
                            total=enrich_shards,
                            message=f"enriched {enriched_shards}/{enrich_shards} raw shards",
                        )

            await asyncio.gather(*(_enrich_shard(low, high) for low, high in checksum_ranges))
        else:
            await db.status(
                _enrich_raw_stage_sql(
                    db_schema,
                    raw_table,
                    archive_available=available.get("address_archive_v2", False),
                )
            )
        await db.status(
            f"CREATE INDEX IF NOT EXISTS {raw_table}_idx_group_key "
            f"ON {db_schema}.{raw_table} "
            "(entity_type, entity_id, type, (COALESCE(address_key::text, checksum::text)));"
        )
        await db.status(
            f"CREATE INDEX IF NOT EXISTS {raw_table}_idx_location_key "
            f"ON {db_schema}.{raw_table} (location_key);"
        )
        await db.status(f"TRUNCATE TABLE {db_schema}.{stage_table};")
        if run_id:
            enqueue_live_progress(
                run_id=run_id,
                importer="entity-address-unified",
                status="running",
                phase="entity-address-unified aggregating",
                unit="shards",
                done=0,
                total=aggregate_shards,
                message=f"aggregating {aggregate_shards} shards",
            )
        aggregate_progress_lock = asyncio.Lock()
        aggregated_shards = 0

        async def _aggregate_shard(remainder: int) -> None:
            nonlocal aggregated_shards
            await db.status(
                _materialize_from_raw_sql(
                    db_schema,
                    stage_table,
                    raw_table,
                    checksum_modulo=aggregate_shards,
                    checksum_remainder=remainder,
                    address_canon_available=address_canon_available,
                )
            )
            if run_id:
                async with aggregate_progress_lock:
                    aggregated_shards += 1
                    enqueue_live_progress(
                        run_id=run_id,
                        importer="entity-address-unified",
                        status="running",
                        phase="entity-address-unified aggregating",
                        unit="shards",
                        done=aggregated_shards,
                        total=aggregate_shards,
                        message=f"aggregated {aggregated_shards}/{aggregate_shards} shards",
                    )

        if aggregate_shards > 1:
            agg_sem = asyncio.Semaphore(aggregate_concurrency)

            async def _guarded_aggregate(remainder: int) -> None:
                async with agg_sem:
                    await _aggregate_shard(remainder)

            await asyncio.gather(*(_guarded_aggregate(i) for i in range(aggregate_shards)))
        else:
            await db.status(
                _materialize_from_raw_sql(
                    db_schema,
                    stage_table,
                    raw_table,
                    address_canon_available=address_canon_available,
                )
            )

    else:
        if run_id:
            enqueue_live_progress(
                run_id=run_id,
                importer="entity-address-unified",
                status="running",
                phase="entity-address-unified materializing",
                unit="sources",
                done=0,
                total=len(source_selects),
                message="materializing sources",
            )
        await db.status(f"TRUNCATE TABLE {db_schema}.{stage_table};")
        await db.status(
            _materialize_sql(
                db_schema,
                stage_table,
                source_selects,
                address_canon_available=address_canon_available,
            )
        )
        if run_id:
            enqueue_live_progress(
                run_id=run_id,
                importer="entity-address-unified",
                status="running",
                phase="entity-address-unified materialized",
                unit="sources",
                done=len(source_selects),
                total=len(source_selects),
                message="sources materialized",
            )

    enable_inference = str(
        os.getenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_ENABLE_INFERENCE", "true")
    ).strip().lower() in {"1", "true", "yes", "on"}
    if test_mode:
        enable_inference = str(
            os.getenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_TEST_ENABLE_INFERENCE", "false")
        ).strip().lower() in {"1", "true", "yes", "on"}

    if enable_inference and available.get("npi", False) and available.get("npi_address", False):
        await db.status(
            _inference_sql(
                db_schema,
                stage_table,
                include_hospital_enrollment=available.get("provider_enrollment_hospital", False),
                include_fqhc_enrollment=available.get("provider_enrollment_fqhc", False),
            )
        )

    evidence_shards = _env_int(
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_EVIDENCE_SHARDS",
        DEFAULT_EVIDENCE_SHARDS,
        minimum=1,
    )
    evidence_concurrency = min(
        _env_int(
            "HLTHPRT_ENTITY_ADDRESS_UNIFIED_EVIDENCE_CONCURRENCY",
            DEFAULT_EVIDENCE_CONCURRENCY,
            minimum=1,
        ),
        evidence_shards,
    )
    use_unlogged_evidence = _env_bool(
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_UNLOGGED_EVIDENCE_STAGE",
        True,
    )
    evidence_table = _evidence_stage_table_name(stage_table)
    await db.status(f"DROP TABLE IF EXISTS {db_schema}.{evidence_table};")
    if run_id:
        enqueue_live_progress(
            run_id=run_id,
            importer="entity-address-unified",
            status="running",
            phase="entity-address-unified preparing source evidence",
            unit="tables",
            done=0,
            total=1,
            message=f"building {evidence_shards}-shard evidence work table",
        )
    await db.status(
        _prepare_multi_source_evidence_table_sql(
            db_schema,
            evidence_table,
            unlogged=use_unlogged_evidence,
        )
    )
    evidence_build_progress_lock = asyncio.Lock()
    evidence_build_done = 0

    async def _build_evidence_shard(remainder: int) -> None:
        nonlocal evidence_build_done
        await db.status(
            _insert_multi_source_evidence_shard_sql(
                db_schema,
                stage_table,
                evidence_table,
                evidence_shards=evidence_shards,
                evidence_shard=remainder,
            )
        )
        if run_id:
            async with evidence_build_progress_lock:
                evidence_build_done += 1
                enqueue_live_progress(
                    run_id=run_id,
                    importer="entity-address-unified",
                    status="running",
                    phase="entity-address-unified preparing source evidence",
                    unit="shards",
                    done=evidence_build_done,
                    total=evidence_shards,
                    message=f"prepared {evidence_build_done}/{evidence_shards} evidence shards",
                )

    if evidence_shards > 1:
        evidence_build_sem = asyncio.Semaphore(evidence_concurrency)

        async def _guarded_build_evidence_shard(remainder: int) -> None:
            async with evidence_build_sem:
                await _build_evidence_shard(remainder)

        await asyncio.gather(
            *(_guarded_build_evidence_shard(i) for i in range(evidence_shards))
        )
    else:
        await _build_evidence_shard(0)
    await db.status(_index_multi_source_evidence_table_sql(db_schema, evidence_table))
    await db.status(f"ANALYZE {db_schema}.{evidence_table};")
    if run_id:
        enqueue_live_progress(
            run_id=run_id,
            importer="entity-address-unified",
            status="running",
            phase="entity-address-unified applying source evidence",
            unit="shards",
            done=0,
            total=evidence_shards,
            message=f"applying evidence across {evidence_shards} shards",
        )
    evidence_progress_lock = asyncio.Lock()
    evidence_done = 0

    async def _apply_evidence_shard(remainder: int) -> None:
        nonlocal evidence_done
        await db.status(
            _apply_multi_source_evidence_sql(
                db_schema,
                stage_table,
                evidence_table,
                evidence_shard=remainder,
            )
        )
        if run_id:
            async with evidence_progress_lock:
                evidence_done += 1
                enqueue_live_progress(
                    run_id=run_id,
                    importer="entity-address-unified",
                    status="running",
                    phase="entity-address-unified applying source evidence",
                    unit="shards",
                    done=evidence_done,
                    total=evidence_shards,
                    message=f"applied {evidence_done}/{evidence_shards} evidence shards",
                )

    if evidence_shards > 1:
        evidence_sem = asyncio.Semaphore(evidence_concurrency)

        async def _guarded_evidence_shard(remainder: int) -> None:
            async with evidence_sem:
                await _apply_evidence_shard(remainder)

        await asyncio.gather(*(_guarded_evidence_shard(i) for i in range(evidence_shards)))
    else:
        await _apply_evidence_shard(0)
    await db.status(f"DROP TABLE IF EXISTS {db_schema}.{evidence_table};")

    node_id = str(os.getenv("HLTHPRT_IMPORT_NODE_ID") or "").strip() or None
    support_counts = await _populate_support_stage_tables(
        db_schema,
        stage_table,
        support_stage_classes,
        source_run_id=import_date,
        node_id=node_id,
        raw_table=raw_table,
    )
    if raw_table:
        await db.status(f"DROP TABLE IF EXISTS {db_schema}.{raw_table};")

    if not ctx["context"].get("stage_indexes_prepared"):
        if run_id:
            enqueue_live_progress(
                run_id=run_id,
                importer="entity-address-unified",
                status="running",
                phase="entity-address-unified indexing",
                unit="run",
                done=0,
                total=1,
                pct=90,
                message="building indexes",
            )
        await _create_stage_indexes(stage_cls, db_schema)
        context["stage_indexes_prepared"] = True

    if not ctx["context"].get("support_stage_indexes_prepared"):
        await _create_support_stage_indexes(support_stage_classes, db_schema)
        context["support_stage_indexes_prepared"] = True

    staged_rows = int(await db.scalar(f"SELECT COUNT(*) FROM {db_schema}.{stage_table};") or 0)
    npi_rows = int(
        await db.scalar(f"SELECT COUNT(*) FROM {db_schema}.{stage_table} WHERE entity_type = 'npi';")
        or 0
    )
    inferred_rows = int(
        await db.scalar(f"SELECT COUNT(*) FROM {db_schema}.{stage_table} WHERE inferred_npi IS NOT NULL;")
        or 0
    )
    multi_source_rows = int(
        await db.scalar(
            f"SELECT COUNT(*) FROM {db_schema}.{stage_table} WHERE multi_source_confirmed IS TRUE;"
        )
        or 0
    )

    context["run"] = context.get("run", 0) + 1
    context["staged_rows"] = staged_rows
    context["npi_rows"] = npi_rows
    context["inferred_rows"] = inferred_rows
    context["multi_source_rows"] = multi_source_rows
    context["support_counts"] = support_counts
    if run_id:
        enqueue_live_progress(
            run_id=run_id,
            importer="entity-address-unified",
            status="running",
            phase="entity-address-unified staged",
            unit="rows",
            done=staged_rows,
            total=staged_rows,
            pct=95,
            message=f"staged {staged_rows} rows",
        )
    logger.info(
        "EntityAddressUnified materialization done: rows=%d npi_rows=%d inferred_rows=%d multi_source_rows=%d",
        staged_rows,
        npi_rows,
        inferred_rows,
        multi_source_rows,
    )


async def startup(ctx):
    await my_init_db(db)
    ctx["context"] = {}
    ctx["context"]["start"] = datetime.datetime.utcnow()
    ctx["context"]["run"] = 0
    ctx["context"]["test_mode"] = False
    ctx["context"]["stage_prepared"] = False
    ctx["context"]["stage_indexes_prepared"] = False
    ctx["context"]["support_stage_prepared"] = False
    ctx["context"]["support_stage_indexes_prepared"] = False
    await ensure_database(False)

    override_import_id = os.getenv("HLTHPRT_IMPORT_ID_OVERRIDE")
    ctx["import_date"] = _normalize_import_id(override_import_id)
    logger.info(
        "EntityAddressUnified startup ready: import_date=%s (stage will be prepared in active DB during process_data)",
        ctx["import_date"],
    )


async def shutdown(ctx):
    import_date = ctx.get("import_date")
    context = ctx.get("context") or {}
    run_id = str(context.get("control_run_id") or ctx.get("control_run_id") or "").strip()

    if not context.get("run"):
        logger.info("No EntityAddressUnified jobs ran; skipping shutdown.")
        return

    await ensure_database(bool(context.get("test_mode")))

    db_schema = os.getenv("HLTHPRT_DB_SCHEMA") if os.getenv("HLTHPRT_DB_SCHEMA") else "mrf"
    stage_cls = make_class(EntityAddressUnified, import_date)
    support_stage_classes = _support_stage_classes(import_date)

    stage_rows = int(await db.scalar(f"SELECT COUNT(*) FROM {db_schema}.{stage_cls.__tablename__};") or 0)
    min_rows_required = int(
        os.getenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_MIN_ROWS", str(DEFAULT_MIN_ROWS))
    )
    previous_rows = 0
    if await _table_exists(db_schema, EntityAddressUnified.__main_table__):
        previous_rows = int(
            await db.scalar(f"SELECT COUNT(*) FROM {db_schema}.{EntityAddressUnified.__main_table__};")
            or 0
        )
    if context.get("test_mode"):
        logger.info("EntityAddressUnified test mode: staged rows=%d", stage_rows)
    _validate_publish_row_count(
        stage_rows=stage_rows,
        previous_rows=previous_rows,
        test_mode=bool(context.get("test_mode")),
        min_rows_required=min_rows_required,
    )

    async with db.transaction():
        table = EntityAddressUnified.__main_table__
        await db.status(f"DROP TABLE IF EXISTS {db_schema}.{table}_old;")
        await db.status(f"ALTER TABLE IF EXISTS {db_schema}.{table} RENAME TO {table}_old;")
        await db.status(f"ALTER TABLE IF EXISTS {db_schema}.{stage_cls.__tablename__} RENAME TO {table};")

        archived = _archived_identifier(f"{table}_idx_primary")
        await db.status(f"DROP INDEX IF EXISTS {db_schema}.{archived};")
        await db.status(f"ALTER INDEX IF EXISTS {db_schema}.{table}_idx_primary RENAME TO {archived};")
        await db.status(
            f"ALTER INDEX IF EXISTS {db_schema}.{stage_cls.__tablename__}_idx_primary "
            f"RENAME TO {table}_idx_primary;"
        )

        if hasattr(stage_cls, "__my_additional_indexes__") and stage_cls.__my_additional_indexes__:
            for index in stage_cls.__my_additional_indexes__:
                index_name = index.get("name", "_".join(index.get("index_elements")))
                old_live_name = f"{table}_idx_{index_name}"
                archived_live_name = _archived_identifier(old_live_name)
                await db.status(f"DROP INDEX IF EXISTS {db_schema}.{archived_live_name};")
                await db.status(
                    f"ALTER INDEX IF EXISTS {db_schema}.{old_live_name} "
                    f"RENAME TO {archived_live_name};"
                )
                await db.status(
                    f"ALTER INDEX IF EXISTS "
                    f"{db_schema}.{_stage_index_name(stage_cls.__tablename__, index_name)} "
                    f"RENAME TO {old_live_name};"
                )

        for live_cls, support_stage_cls in support_stage_classes.items():
            await _swap_stage_table(db_schema, live_cls, support_stage_cls)

    logger.info("EntityAddressUnified publish complete: rows=%d", stage_rows)
    await mark_control_run(
        run_id,
        status="succeeded",
        phase_detail="entity-address-unified published",
        progress_message="succeeded",
        progress={
            "unit": "rows",
            "done": stage_rows,
            "total": stage_rows,
            "pct": 100,
            "message": "succeeded",
            "phase": "entity-address-unified published",
        },
        metrics={
            "rows": stage_rows,
            "npi_rows": int(context.get("npi_rows") or 0),
            "inferred_rows": int(context.get("inferred_rows") or 0),
            "multi_source_rows": int(context.get("multi_source_rows") or 0),
            "support_counts": context.get("support_counts") or {},
        },
    )
    print_time_info(context.get("start"))


async def main(test_mode: bool = False):
    redis = await create_pool(
        build_redis_settings(),
        job_serializer=serialize_job,
        job_deserializer=deserialize_job,
    )
    payload = {"test_mode": bool(test_mode)}
    await redis.enqueue_job("process_data", payload, _queue_name=ENTITY_ADDRESS_UNIFIED_QUEUE_NAME)
