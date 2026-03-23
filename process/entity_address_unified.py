# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import datetime
import hashlib
import logging
import os
from typing import Iterable

from arq import create_pool

from db.models import EntityAddressUnified, db
from process.ext.utils import ensure_database, make_class, my_init_db, print_time_info
from process.redis_config import build_redis_settings
from process.serialization import deserialize_job, serialize_job

logger = logging.getLogger(__name__)

ENTITY_ADDRESS_UNIFIED_QUEUE_NAME = "arq:EntityAddressUnified"
POSTGRES_IDENTIFIER_MAX_LENGTH = 63
DEFAULT_MIN_ROWS = 10_000
DEFAULT_TEST_LIMIT_PER_SOURCE = 20_000
DEFAULT_SOURCE_CONCURRENCY = 3
DEFAULT_AGGREGATE_SHARDS = 16
DEFAULT_AGGREGATE_CONCURRENCY = 4


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


async def _table_exists(db_schema: str, table_name: str) -> bool:
    return bool(await db.scalar(f"SELECT to_regclass('{db_schema}.{table_name}') IS NOT NULL;"))


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
        "ELSE 9 END"
    )


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
                f.address_line_1::varchar AS first_line,
                f.address_line_2::varchar AS second_line,
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

    if test_limit_per_source and test_limit_per_source > 0:
        return [
            "(\n"
            f"SELECT * FROM (\n{select.strip()}\n) AS src LIMIT {int(test_limit_per_source)}\n"
            ")"
            for select in selects
        ]
    return selects


def _raw_stage_table_name(stage_table: str) -> str:
    return f"{stage_table}_raw"


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
        checksum int NOT NULL
    );
    """


def _insert_raw_from_source_sql(db_schema: str, raw_table: str, source_select: str) -> str:
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
            updated_at::timestamp AS updated_at
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
) -> str:
    shard_filter = ""
    if checksum_modulo and checksum_modulo > 1 and checksum_remainder is not None:
        shard_filter = (
            " WHERE mod(abs(COALESCE(checksum, 0)), "
            f"{int(checksum_modulo)}) = {int(checksum_remainder)}"
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
        updated_at
    )
    WITH aggregated AS (
        SELECT
            entity_type,
            entity_id,
            type,
            checksum,
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
            ARRAY_REMOVE(ARRAY_AGG(DISTINCT address_source ORDER BY address_source), NULL)::varchar[] AS address_sources,
            ARRAY_REMOVE(ARRAY_AGG(DISTINCT source_record_id ORDER BY source_record_id), NULL)::varchar[] AS source_record_ids,
            MAX(updated_at)::timestamp AS updated_at
          FROM {db_schema}.{raw_table}
         {shard_filter}
         GROUP BY entity_type, entity_id, type, checksum
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
        updated_at
      FROM aggregated;
    """


def _materialize_sql(db_schema: str, stage_table: str, source_selects: Iterable[str]) -> str:
    selects_sql = "\nUNION ALL\n".join(select.strip() for select in source_selects)
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
            updated_at::timestamp AS updated_at
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
            checksum,
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
            ARRAY_REMOVE(ARRAY_AGG(DISTINCT address_source ORDER BY address_source), NULL)::varchar[] AS address_sources,
            ARRAY_REMOVE(ARRAY_AGG(DISTINCT source_record_id ORDER BY source_record_id), NULL)::varchar[] AS source_record_ids,
            MAX(updated_at)::timestamp AS updated_at
          FROM normalized
      GROUP BY entity_type, entity_id, type, checksum
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
        updated_at
      FROM aggregated;
    """


def _apply_multi_source_evidence_sql(db_schema: str, stage_table: str) -> str:
    return f"""
    WITH keyed AS (
        SELECT
            entity_type,
            entity_id,
            type,
            checksum,
            {_street_soft_norm_expr("first_line")}::varchar AS street_key,
            {_alnum_norm_expr("city_name")}::varchar AS city_key,
            {_state_norm_expr("state_name")}::varchar AS state_key,
            {_zip5_norm_expr("postal_code")}::varchar AS zip_key,
            {_state_norm_expr("country_code")}::varchar AS country_key
          FROM {db_schema}.{stage_table}
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
            ON t.entity_type = k.entity_type
           AND t.entity_id = k.entity_id
           AND t.type = k.type
           AND t.checksum = k.checksum
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
            ON t.entity_type = k.entity_type
           AND t.entity_id = k.entity_id
           AND t.type = k.type
           AND t.checksum = k.checksum
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
    UPDATE {db_schema}.{stage_table} AS t
       SET address_sources = COALESCE(se.evidence_sources, t.address_sources, ARRAY[]::varchar[]),
           source_record_ids = COALESCE(re.evidence_record_ids, t.source_record_ids, ARRAY[]::varchar[]),
           source_count = COALESCE(
                CARDINALITY(COALESCE(se.evidence_sources, t.address_sources, ARRAY[]::varchar[])),
                0
           )::int,
           multi_source_confirmed = COALESCE(
                CARDINALITY(COALESCE(se.evidence_sources, t.address_sources, ARRAY[]::varchar[])),
                0
           ) > 1
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
       AND re.country_key IS NOT DISTINCT FROM k.country_key
     WHERE t.entity_type = k.entity_type
       AND t.entity_id = k.entity_id
       AND t.type = k.type
       AND t.checksum = k.checksum;
    """


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
            NULL::integer AS checksum,
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
            NULL::integer AS checksum,
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

    if "test_mode" in task:
        ctx["context"]["test_mode"] = bool(task.get("test_mode"))
    test_mode = bool(ctx["context"].get("test_mode", False))

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

    required_checks = [
        "npi",
        "npi_address",
        "provider_enrollment_hospital",
        "provider_enrollment_fqhc",
        "doctor_clinician_address",
        "provider_enrollment_ffs",
        "provider_enrollment_ffs_address",
        "facility_anchor",
        "geo_zip_lookup",
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
    if not source_selects:
        raise RuntimeError("No source tables are available for entity_address_unified materialization.")

    chunked_load = _env_bool("HLTHPRT_ENTITY_ADDRESS_UNIFIED_CHUNKED_LOAD", True)
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
        raw_table = _raw_stage_table_name(stage_table)
        use_unlogged_raw = _env_bool("HLTHPRT_ENTITY_ADDRESS_UNIFIED_UNLOGGED_RAW_STAGE", True)

        await db.status(f"DROP TABLE IF EXISTS {db_schema}.{raw_table};")
        await db.status(_prepare_raw_stage_sql(db_schema, raw_table, unlogged=use_unlogged_raw))

        sem = asyncio.Semaphore(source_concurrency)

        async def _load_source(select_sql: str) -> None:
            async with sem:
                await db.status(_insert_raw_from_source_sql(db_schema, raw_table, select_sql))

        if source_concurrency > 1 and len(source_selects) > 1:
            await asyncio.gather(*(_load_source(select_sql) for select_sql in source_selects))
        else:
            for select_sql in source_selects:
                await _load_source(select_sql)

        await db.status(
            f"CREATE INDEX IF NOT EXISTS {raw_table}_idx_group_key "
            f"ON {db_schema}.{raw_table} (entity_type, entity_id, type, checksum);"
        )
        await db.status(f"TRUNCATE TABLE {db_schema}.{stage_table};")

        async def _aggregate_shard(remainder: int) -> None:
            await db.status(
                _materialize_from_raw_sql(
                    db_schema,
                    stage_table,
                    raw_table,
                    checksum_modulo=aggregate_shards,
                    checksum_remainder=remainder,
                )
            )

        if aggregate_shards > 1:
            agg_sem = asyncio.Semaphore(aggregate_concurrency)

            async def _guarded_aggregate(remainder: int) -> None:
                async with agg_sem:
                    await _aggregate_shard(remainder)

            await asyncio.gather(*(_guarded_aggregate(i) for i in range(aggregate_shards)))
        else:
            await db.status(_materialize_from_raw_sql(db_schema, stage_table, raw_table))

        await db.status(f"DROP TABLE IF EXISTS {db_schema}.{raw_table};")
    else:
        await db.status(f"TRUNCATE TABLE {db_schema}.{stage_table};")
        await db.status(_materialize_sql(db_schema, stage_table, source_selects))

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

    await db.status(_apply_multi_source_evidence_sql(db_schema, stage_table))

    if not ctx["context"].get("stage_indexes_prepared"):
        await _create_stage_indexes(stage_cls, db_schema)
        ctx["context"]["stage_indexes_prepared"] = True

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

    ctx["context"]["run"] = ctx["context"].get("run", 0) + 1
    ctx["context"]["staged_rows"] = staged_rows
    ctx["context"]["npi_rows"] = npi_rows
    ctx["context"]["inferred_rows"] = inferred_rows
    ctx["context"]["multi_source_rows"] = multi_source_rows
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

    if not context.get("run"):
        logger.info("No EntityAddressUnified jobs ran; skipping shutdown.")
        return

    await ensure_database(bool(context.get("test_mode")))

    db_schema = os.getenv("HLTHPRT_DB_SCHEMA") if os.getenv("HLTHPRT_DB_SCHEMA") else "mrf"
    stage_cls = make_class(EntityAddressUnified, import_date)

    stage_rows = int(await db.scalar(f"SELECT COUNT(*) FROM {db_schema}.{stage_cls.__tablename__};") or 0)
    min_rows_required = int(
        os.getenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_MIN_ROWS", str(DEFAULT_MIN_ROWS))
    )
    if context.get("test_mode"):
        logger.info("EntityAddressUnified test mode: staged rows=%d", stage_rows)
    elif stage_rows < min_rows_required:
        raise RuntimeError(
            f"EntityAddressUnified stage row count {stage_rows} below minimum {min_rows_required}; aborting publish."
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

    logger.info("EntityAddressUnified publish complete: rows=%d", stage_rows)
    print_time_info(context.get("start"))


async def main(test_mode: bool = False):
    redis = await create_pool(
        build_redis_settings(),
        job_serializer=serialize_job,
        job_deserializer=deserialize_job,
    )
    payload = {"test_mode": bool(test_mode)}
    await redis.enqueue_job("process_data", payload, _queue_name=ENTITY_ADDRESS_UNIFIED_QUEUE_NAME)
