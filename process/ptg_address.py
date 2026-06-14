# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Build the fast PTG provider-location address projection."""

from __future__ import annotations

import datetime
import json
import logging
import os
import re
from typing import Any

from arq import create_pool

from db.models import PTGAddress, db
from process.control_lifecycle import mark_control_run
from process.entity_address_unified import (
    ARCHIVE_IDENTITY_VERSION,
    BASE_ADDRESS_VERSION,
    _archived_identifier,
    _create_stage_indexes,
    _invalid_coordinate_count,
    _location_key_expr,
    _normalize_import_id,
    _stage_index_name,
    _table_exists,
    _validate_schema_name,
)
from process.ext.utils import ensure_database, make_class, my_init_db, print_time_info
from process.live_progress import enqueue_live_progress
from process.ptg_parts.db_tables import _quote_ident
from process.redis_config import build_redis_settings
from process.serialization import deserialize_job, serialize_job

logger = logging.getLogger(__name__)

PTG_ADDRESS_QUEUE_NAME = "arq:PTGAddress"
DEFAULT_MIN_ROWS = 1
PTG_PROVIDER_GROUP_LOCATION_PREFIX = "ptg2_provider_group_location_"
PTG_PROVIDER_GROUP_MEMBER_PREFIX = "ptg2_provider_group_member_"
PTG_SERVING_RATE_COMPACT_PREFIX = "ptg2_serving_rate_compact_"
PTG_PROVIDER_SET_COMPONENT_PREFIX = "ptg2_provider_set_component_"
SNAPSHOT_TABLE_NAME_RE = re.compile(r"[a-z0-9_]{1,63}\Z")


def _sql_literal(value: str | None) -> str:
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def _row_get(row: Any, key: str) -> Any:
    if row is None:
        return None
    if isinstance(row, dict):
        return row.get(key)
    mapping = getattr(row, "_mapping", None)
    if mapping is not None:
        return mapping.get(key)
    return getattr(row, key, None)


def _clean_optional(value: Any) -> str | None:
    cleaned = str(value or "").strip()
    return cleaned or None


def _coerce_json_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return decoded if isinstance(decoded, dict) else {}
    return {}


def _manifest_serving_index(manifest: Any) -> dict[str, Any]:
    data = _coerce_json_mapping(manifest)
    serving_index = data.get("serving_index")
    if isinstance(serving_index, dict):
        return serving_index
    return data


def _snapshot_table_name(value: Any, db_schema: str, *, prefix: str) -> str | None:
    raw = _clean_optional(value)
    if not raw:
        return None
    parts = raw.split(".", 1)
    if len(parts) == 2:
        schema_name, table_name = parts[0].strip(), parts[1].strip()
    else:
        schema_name, table_name = db_schema, parts[0].strip()
    if schema_name != db_schema:
        return None
    if not table_name.startswith(prefix):
        return None
    if not SNAPSHOT_TABLE_NAME_RE.fullmatch(table_name):
        return None
    return table_name


def _snapshot_provider_group_location_table_name(value: Any, db_schema: str) -> str | None:
    return _snapshot_table_name(value, db_schema, prefix=PTG_PROVIDER_GROUP_LOCATION_PREFIX)


def _snapshot_provider_group_member_table_name(value: Any, db_schema: str) -> str | None:
    return _snapshot_table_name(value, db_schema, prefix=PTG_PROVIDER_GROUP_MEMBER_PREFIX)


def _qualified_table_ref(db_schema: str, table_name: str) -> str:
    return f"{_quote_ident(db_schema)}.{_quote_ident(table_name)}"


async def _address_canon_available(db_schema: str) -> bool:
    value = await db.scalar(
        "SELECT to_regprocedure(:signature);",
        signature=f"{db_schema}.addr_key_v1(text,text,text,text,text,text)",
    )
    return isinstance(value, str) and bool(value)


async def _current_source_snapshot(db_schema: str, source_key: str | None) -> tuple[str, str] | None:
    if not await _table_exists(db_schema, "ptg2_current_source_snapshot"):
        return None
    if source_key:
        rows = await db.all(
            f"""
            SELECT source_key, snapshot_id
              FROM {_quote_ident(db_schema)}.ptg2_current_source_snapshot
             WHERE source_key = :source_key
             LIMIT 1
            """,
            source_key=source_key,
        )
    else:
        rows = [
            {"source_key": source_key, "snapshot_id": snapshot_id}
            for source_key, snapshot_id in (await _current_source_snapshots(db_schema))[:1]
        ]
    if not rows:
        return None
    resolved_source_key = _clean_optional(_row_get(rows[0], "source_key"))
    resolved_snapshot_id = _clean_optional(_row_get(rows[0], "snapshot_id"))
    if not resolved_source_key or not resolved_snapshot_id:
        return None
    return resolved_source_key, resolved_snapshot_id


async def _current_source_snapshots(db_schema: str) -> list[tuple[str, str]]:
    if not await _table_exists(db_schema, "ptg2_current_source_snapshot"):
        return []
    rows = await db.all(
        f"""
        SELECT source_key, snapshot_id
          FROM {_quote_ident(db_schema)}.ptg2_current_source_snapshot
         ORDER BY updated_at DESC NULLS LAST, source_key
        """
    )
    snapshots: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for row in rows:
        source_key = _clean_optional(_row_get(row, "source_key"))
        snapshot_id = _clean_optional(_row_get(row, "snapshot_id"))
        if not source_key or not snapshot_id:
            continue
        key = (source_key, snapshot_id)
        if key in seen:
            continue
        seen.add(key)
        snapshots.append(key)
    return snapshots


async def _snapshot_manifest_tables(db_schema: str, snapshot_id: str | None) -> dict[str, str | None]:
    if not snapshot_id or not await _table_exists(db_schema, "ptg2_snapshot"):
        return {
            "provider_group_location_table": None,
            "provider_group_member_table": None,
            "serving_rate_compact_table": None,
            "provider_set_component_table": None,
        }
    manifest = await db.scalar(
        f"""
        SELECT manifest
          FROM {_quote_ident(db_schema)}.ptg2_snapshot
         WHERE snapshot_id = :snapshot_id
         LIMIT 1
        """,
        snapshot_id=snapshot_id,
    )
    serving_index = _manifest_serving_index(manifest)
    candidates = {
        "provider_group_location_table": _snapshot_table_name(
            serving_index.get("provider_group_location_table"),
            db_schema,
            prefix=PTG_PROVIDER_GROUP_LOCATION_PREFIX,
        ),
        "provider_group_member_table": _snapshot_table_name(
            serving_index.get("provider_group_member_table"),
            db_schema,
            prefix=PTG_PROVIDER_GROUP_MEMBER_PREFIX,
        ),
        "serving_rate_compact_table": _snapshot_table_name(
            serving_index.get("table"),
            db_schema,
            prefix=PTG_SERVING_RATE_COMPACT_PREFIX,
        ),
        "provider_set_component_table": _snapshot_table_name(
            serving_index.get("provider_set_component_table"),
            db_schema,
            prefix=PTG_PROVIDER_SET_COMPONENT_PREFIX,
        ),
    }
    resolved: dict[str, str | None] = {}
    for key, table_name in candidates.items():
        resolved[key] = table_name if table_name and await _table_exists(db_schema, table_name) else None
    return resolved


async def _resolve_ptg_address_input(
    db_schema: str,
    *,
    source_key: str | None,
    snapshot_id: str | None,
    import_date: str,
) -> tuple[str, str, dict[str, str | None]]:
    resolved_source_key = _clean_optional(source_key)
    resolved_snapshot_id = _clean_optional(snapshot_id)
    if not resolved_snapshot_id:
        current = await _current_source_snapshot(db_schema, resolved_source_key)
        if current is not None:
            resolved_source_key, resolved_snapshot_id = current
    resolved_source_key = resolved_source_key or "ptg2"
    resolved_snapshot_id = resolved_snapshot_id or import_date
    manifest_tables = await _snapshot_manifest_tables(
        db_schema,
        resolved_snapshot_id,
    )
    return resolved_source_key, resolved_snapshot_id, manifest_tables


async def _resolve_ptg_address_inputs(
    db_schema: str,
    *,
    source_key: str | None,
    snapshot_id: str | None,
    import_date: str,
) -> list[tuple[str, str, dict[str, str | None]]]:
    if _clean_optional(source_key) or _clean_optional(snapshot_id):
        return [
            await _resolve_ptg_address_input(
                db_schema,
                source_key=source_key,
                snapshot_id=snapshot_id,
                import_date=import_date,
            )
        ]

    current_snapshots = await _current_source_snapshots(db_schema)
    if not current_snapshots:
        return [
            await _resolve_ptg_address_input(
                db_schema,
                source_key=None,
                snapshot_id=None,
                import_date=import_date,
            )
        ]

    inputs: list[tuple[str, str, dict[str, str | None]]] = []
    for resolved_source_key, resolved_snapshot_id in current_snapshots:
        inputs.append(
            (
                resolved_source_key,
                resolved_snapshot_id,
                await _snapshot_manifest_tables(db_schema, resolved_snapshot_id),
            )
        )
    return inputs


def _provider_location_source_ctes(
    db_schema: str,
    provider_group_location_table: str | None,
    provider_group_member_table: str | None = None,
) -> str:
    if provider_group_location_table:
        return f"""
    source_locations AS (
        SELECT
            (
                'provider_group_location:' || loc.provider_group_hash::text || ':' ||
                loc.npi::text || ':' ||
                COALESCE(NULLIF(loc.address_checksum::text, ''), md5(concat_ws('|',
                    COALESCE(loc.first_line::text, ''),
                    COALESCE(loc.second_line::text, ''),
                    COALESCE(loc.city_name::text, ''),
                    COALESCE(loc.state_name::text, ''),
                    COALESCE(loc.postal_code::text, ''),
                    COALESCE(loc.country_code::text, '')
                )))
            )::varchar AS location_hash,
            loc.npi::bigint AS npi,
            'provider_group_location'::varchar AS location_source,
            'provider_group_location'::varchar AS confidence_code,
            NULLIF(BTRIM(loc.state_name), '')::varchar AS state,
            NULLIF(BTRIM(loc.city_name), '')::varchar AS city,
            NULLIF(
                LEFT(REGEXP_REPLACE(COALESCE(loc.zip5, loc.postal_code, ''), '[^0-9]', '', 'g'), 5),
                ''
            )::varchar AS zip5,
            loc.lat::numeric AS lat,
            loc."long"::numeric AS long,
            NULLIF(BTRIM(loc.first_line), '')::varchar AS first_line,
            NULLIF(BTRIM(loc.second_line), '')::varchar AS second_line,
            NULLIF(BTRIM(COALESCE(loc.postal_code, loc.zip5)), '')::varchar AS postal_code,
            COALESCE(NULLIF(BTRIM(loc.country_code), ''), 'US')::varchar AS country_code,
            NULLIF(loc.provider_group_hash::text, '')::varchar AS provider_group_id,
            NULL::varchar AS provider_set_id,
            NULL::varchar AS tin,
            NULL::timestamptz AS created_at
          FROM {_qualified_table_ref(db_schema, provider_group_location_table)} loc
         WHERE loc.npi IS NOT NULL
    ),
    shaped AS (
        SELECT * FROM source_locations
    )
        """
    if provider_group_member_table:
        return f"""
    source_locations AS (
        SELECT
            (
                'provider_group_member_npi_address:' || pgm.provider_group_global_id_128::text || ':' ||
                a.npi::text || ':' ||
                COALESCE(NULLIF(a.checksum::text, ''), md5(concat_ws('|',
                    COALESCE(a.first_line::text, ''),
                    COALESCE(a.second_line::text, ''),
                    COALESCE(a.city_name::text, ''),
                    COALESCE(a.state_name::text, ''),
                    COALESCE(a.postal_code::text, ''),
                    COALESCE(a.country_code::text, '')
                )))
            )::varchar AS location_hash,
            a.npi::bigint AS npi,
            'npi_address'::varchar AS location_source,
            'provider_group_member_npi_address'::varchar AS confidence_code,
            NULLIF(BTRIM(a.state_name), '')::varchar AS state,
            NULLIF(BTRIM(a.city_name), '')::varchar AS city,
            NULLIF(
                LEFT(REGEXP_REPLACE(COALESCE(a.postal_code, ''), '[^0-9]', '', 'g'), 5),
                ''
            )::varchar AS zip5,
            a.lat::numeric AS lat,
            a."long"::numeric AS long,
            NULLIF(BTRIM(a.first_line), '')::varchar AS first_line,
            NULLIF(BTRIM(a.second_line), '')::varchar AS second_line,
            NULLIF(BTRIM(a.postal_code), '')::varchar AS postal_code,
            COALESCE(NULLIF(BTRIM(a.country_code), ''), 'US')::varchar AS country_code,
            NULLIF(pgm.provider_group_global_id_128::text, '')::varchar AS provider_group_id,
            NULL::varchar AS provider_set_id,
            NULL::varchar AS tin,
            a.date_added::timestamptz AS created_at
          FROM {_qualified_table_ref(db_schema, provider_group_member_table)} pgm
          JOIN {_quote_ident(db_schema)}.npi_address a
            ON a.npi = pgm.npi
           AND a.type = 'primary'
         WHERE pgm.npi IS NOT NULL
           AND pgm.provider_group_global_id_128 IS NOT NULL
    ),
    shaped AS (
        SELECT * FROM source_locations
    )
        """
    return f"""
    source_locations AS (
        SELECT
            loc.location_hash,
            loc.npi::bigint AS npi,
            NULLIF(BTRIM(loc.location_source), '')::varchar AS location_source,
            NULLIF(BTRIM(loc.confidence_code), '')::varchar AS confidence_code,
            NULLIF(BTRIM(loc.state), '')::varchar AS state,
            NULLIF(BTRIM(loc.city), '')::varchar AS city,
            NULLIF(BTRIM(loc.zip5), '')::varchar AS zip5,
            loc.lat::numeric AS lat,
            loc.lon::numeric AS long,
            COALESCE(loc.address_payload::jsonb, '{{}}'::jsonb) AS payload,
            loc.created_at
          FROM {db_schema}.ptg2_provider_location loc
         WHERE loc.npi IS NOT NULL
    ),
    shaped AS (
        SELECT
            location_hash,
            npi,
            location_source,
            confidence_code,
            COALESCE(NULLIF(payload->>'first_line', ''), NULLIF(payload->>'address_line1', ''))::varchar AS first_line,
            COALESCE(
                NULLIF(payload->>'second_line', ''),
                NULLIF(payload->>'address_line2', '')
            )::varchar AS second_line,
            COALESCE(NULLIF(payload->>'city', ''), city)::varchar AS city,
            COALESCE(NULLIF(payload->>'state', ''), state)::varchar AS state,
            COALESCE(NULLIF(payload->>'postal_code', ''), NULLIF(payload->>'zip5', ''), zip5)::varchar AS postal_code,
            'US'::varchar AS country_code,
            NULL::varchar AS provider_group_id,
            NULL::varchar AS provider_set_id,
            NULL::varchar AS tin,
            lat,
            long,
            created_at
          FROM source_locations
    )
    """


def _provider_group_plan_cte(
    db_schema: str,
    *,
    snapshot_id: str,
    serving_rate_compact_table: str | None,
    provider_set_component_table: str | None,
) -> str:
    if not serving_rate_compact_table or not provider_set_component_table:
        return """
    ,
    provider_group_plans AS (
        SELECT NULL::varchar AS provider_group_id, ARRAY[]::varchar[] AS ptg_plan_array
        WHERE FALSE
    )
        """
    return f"""
    ,
    provider_group_plans AS (
        SELECT
            NULLIF(c.provider_group_hash::text, '')::varchar AS provider_group_id,
            ARRAY_REMOVE(
                ARRAY_AGG(DISTINCT NULLIF(r.plan_id::text, '') ORDER BY NULLIF(r.plan_id::text, '')),
                NULL
            )::varchar[] AS ptg_plan_array
          FROM {_qualified_table_ref(db_schema, serving_rate_compact_table)} r
          JOIN {_qualified_table_ref(db_schema, provider_set_component_table)} c
            ON c.provider_set_hash = r.provider_set_hash
         WHERE r.snapshot_id = {_sql_literal(snapshot_id)}
           AND NULLIF(c.provider_group_hash::text, '') IS NOT NULL
           AND NULLIF(r.plan_id::text, '') IS NOT NULL
         GROUP BY NULLIF(c.provider_group_hash::text, '')
    )
        """


def _ptg_address_insert_sql(
    db_schema: str,
    stage_table: str,
    *,
    source_key: str,
    snapshot_id: str,
    node_id: str | None,
    address_canon_available: bool,
    archive_available: bool,
    provider_group_location_table: str | None = None,
    provider_group_member_table: str | None = None,
    serving_rate_compact_table: str | None = None,
    provider_set_component_table: str | None = None,
) -> str:
    source_ctes = _provider_location_source_ctes(
        db_schema,
        provider_group_location_table,
        provider_group_member_table=provider_group_member_table,
    )
    plan_cte = _provider_group_plan_cte(
        db_schema,
        snapshot_id=snapshot_id,
        serving_rate_compact_table=serving_rate_compact_table,
        provider_set_component_table=provider_set_component_table,
    )
    country_expr = "country_code" if provider_group_location_table or provider_group_member_table else "'US'"
    address_key_expr = (
        f"{db_schema}.addr_key_v1(first_line, second_line, city, state, postal_code, {country_expr})"
        if address_canon_available
        else "NULL::uuid"
    )
    archive_join = (
        f"LEFT JOIN {db_schema}.address_archive_v2 a "
        "ON a.address_key = k.address_key AND a.merged_into IS NULL"
        if archive_available
        else ""
    )
    archive_fields = (
        "a.premise_key, "
        "'v' || COALESCE(a.identity_version, 1)::text AS archive_identity_version, "
        "COALESCE(a.precision, CASE WHEN k.address_key IS NULL THEN 'unknown' ELSE 'street' END) AS address_precision, "
        "COALESCE(a.zip5, k.source_zip5) AS zip5, "
        "COALESCE(NULLIF(upper(left(a.state_code, 2)), ''), k.source_state_code) AS state_code, "
        "COALESCE(a.city_norm, k.source_city_norm) AS city_norm, "
        "NULL::varchar AS county_fips"
        if archive_available
        else (
            "NULL::uuid AS premise_key, "
            f"'{ARCHIVE_IDENTITY_VERSION}'::varchar AS archive_identity_version, "
            "CASE WHEN k.address_key IS NULL THEN 'unknown' ELSE 'street' END::varchar AS address_precision, "
            "k.source_zip5 AS zip5, "
            "k.source_state_code AS state_code, "
            "k.source_city_norm AS city_norm, "
            "NULL::varchar AS county_fips"
        )
    )
    entity_id_expr = (
        "CASE WHEN e.provider_group_id IS NOT NULL "
        "THEN 'provider_group:' || e.provider_group_id || ':npi:' || e.npi::text "
        "ELSE 'npi:' || e.npi::text END"
    )
    location_key = _location_key_expr(
        entity_type="'ptg'",
        entity_id=entity_id_expr,
        npi="e.npi",
        inferred_npi="NULL::bigint",
        address_role_id="4",
        row_origin="'ptg_overlay'",
        address_key="e.address_key",
        source_id="7",
        source_record_id="e.location_hash",
        zip5="e.zip5",
        state_code="e.state_code",
        city_norm="e.city_norm",
    )
    return f"""
    INSERT INTO {db_schema}.{stage_table} (
        node_id,
        source_key,
        snapshot_id,
        plan_id,
        ptg_plan_id,
        market_type,
        provider_group_id,
        provider_set_id,
        npi,
        tin,
        location_key,
        address_key,
        premise_key,
        archive_identity_version,
        address_precision,
        address_source_id,
        address_source_record_key,
        address_role_id,
        location_confidence_id,
        zip5,
        state_code,
        city_norm,
        county_fips,
        lat,
        long,
        ptg_plan_array,
        ptg_source_array,
        group_plan_array,
        base_address_version,
        ptg_snapshot_published_at,
        observed_at,
        updated_at
    )
    WITH {source_ctes}
    {plan_cte},
    keyed AS (
        SELECT
            *,
            NULLIF(LEFT(REGEXP_REPLACE(COALESCE(postal_code, ''), '[^0-9]', '', 'g'), 5), '')::varchar AS source_zip5,
            NULLIF(upper(left(BTRIM(COALESCE(state, '')), 2)), '')::varchar AS source_state_code,
            NULLIF(regexp_replace(lower(COALESCE(city, '')), '[^a-z0-9]', '', 'g'), '')::varchar AS source_city_norm,
            {address_key_expr} AS address_key
          FROM shaped
    ),
    enriched AS (
        SELECT
            k.*,
            {archive_fields}
          FROM keyed k
          {archive_join}
    )
    SELECT DISTINCT ON ({location_key})
        {_sql_literal(node_id)}::varchar AS node_id,
        {_sql_literal(source_key)}::varchar AS source_key,
        {_sql_literal(snapshot_id)}::varchar AS snapshot_id,
        CASE WHEN CARDINALITY(COALESCE(pgp.ptg_plan_array, ARRAY[]::varchar[])) = 1
             THEN pgp.ptg_plan_array[1]
             ELSE NULL
        END::varchar AS plan_id,
        CASE WHEN CARDINALITY(COALESCE(pgp.ptg_plan_array, ARRAY[]::varchar[])) = 1
             THEN pgp.ptg_plan_array[1]
             ELSE NULL
        END::varchar AS ptg_plan_id,
        NULL::varchar AS market_type,
        e.provider_group_id,
        e.provider_set_id,
        e.npi,
        e.tin,
        {location_key} AS location_key,
        e.address_key,
        e.premise_key,
        e.archive_identity_version,
        e.address_precision,
        CASE
            WHEN e.location_source = 'npi_address' THEN 1
            WHEN e.location_source = 'doctor_clinician_address' THEN 3
            WHEN e.location_source = 'entity_address_unified' THEN 0
            ELSE 7
        END::smallint AS address_source_id,
        e.location_hash::varchar AS address_source_record_key,
        4::smallint AS address_role_id,
        CASE
            WHEN e.location_source = 'npi_address' THEN 4
            WHEN e.location_source = 'doctor_clinician_address' THEN 5
            WHEN e.location_source = 'entity_address_unified' THEN 2
            ELSE 0
        END::smallint AS location_confidence_id,
        e.zip5,
        e.state_code,
        e.city_norm,
        e.county_fips,
        e.lat,
        e.long,
        COALESCE(pgp.ptg_plan_array, ARRAY[]::varchar[]) AS ptg_plan_array,
        ARRAY[{_sql_literal(source_key)}]::varchar[] AS ptg_source_array,
        COALESCE(pgp.ptg_plan_array, ARRAY[]::varchar[]) AS group_plan_array,
        {_sql_literal(BASE_ADDRESS_VERSION)}::varchar AS base_address_version,
        NULL::timestamptz AS ptg_snapshot_published_at,
        COALESCE(e.created_at, NOW())::timestamptz AS observed_at,
        NOW()::timestamptz AS updated_at
      FROM enriched e
      LEFT JOIN provider_group_plans pgp
        ON pgp.provider_group_id = e.provider_group_id
     WHERE e.zip5 IS NOT NULL OR e.state_code IS NOT NULL OR e.address_key IS NOT NULL
     ORDER BY {location_key}, e.created_at DESC NULLS LAST, e.location_hash;
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
    db_schema = _validate_schema_name(os.getenv("HLTHPRT_DB_SCHEMA") or "mrf")

    import_date = ctx["import_date"]
    stage_cls = make_class(PTGAddress, import_date)
    stage_table = stage_cls.__tablename__
    if not context.get("stage_prepared"):
        await db.status(f"DROP TABLE IF EXISTS {db_schema}.{stage_table};")
        await db.create_table(stage_cls.__table__, checkfirst=True)
        context["stage_prepared"] = True

    explicit_source_key = task.get("source_key") or os.getenv("HLTHPRT_PTG_ADDRESS_SOURCE_KEY")
    explicit_snapshot_id = task.get("snapshot_id") or os.getenv("HLTHPRT_PTG_ADDRESS_SNAPSHOT_ID")
    explicit_input = bool(_clean_optional(explicit_source_key) or _clean_optional(explicit_snapshot_id))
    address_inputs = await _resolve_ptg_address_inputs(
        db_schema,
        source_key=explicit_source_key,
        snapshot_id=explicit_snapshot_id,
        import_date=import_date,
    )
    context["source_keys"] = [source_key for source_key, _, _ in address_inputs]
    context["snapshot_ids"] = [snapshot_id for _, snapshot_id, _ in address_inputs]
    if len(address_inputs) == 1:
        context["source_key"] = address_inputs[0][0]
        context["snapshot_id"] = address_inputs[0][1]
    else:
        context["source_key"] = ",".join(context["source_keys"])
        context["snapshot_id"] = ",".join(context["snapshot_ids"])
    node_id = str(os.getenv("HLTHPRT_IMPORT_NODE_ID") or "").strip() or None

    await db.status(f"TRUNCATE TABLE {db_schema}.{stage_table};")
    address_canon_available = await _address_canon_available(db_schema)
    archive_available = await _table_exists(db_schema, "address_archive_v2")
    legacy_provider_location_available = await _table_exists(db_schema, "ptg2_provider_location")
    source_contexts: list[dict[str, str | None]] = []
    skipped_sources: list[dict[str, str | None]] = []
    for source_key, snapshot_id, manifest_tables in address_inputs:
        provider_group_location_table = manifest_tables.get("provider_group_location_table")
        provider_group_member_table = manifest_tables.get("provider_group_member_table")
        serving_rate_compact_table = manifest_tables.get("serving_rate_compact_table")
        provider_set_component_table = manifest_tables.get("provider_set_component_table")
        if not provider_group_location_table and not provider_group_member_table and not legacy_provider_location_available:
            if not explicit_input and len(address_inputs) > 1:
                logger.warning(
                    "Skipping PTG address source %s/%s because its snapshot has no provider-location table",
                    source_key,
                    snapshot_id,
                )
                skipped_sources.append(
                    {
                        "source_key": source_key,
                        "snapshot_id": snapshot_id,
                        "reason": "missing_provider_location_table",
                    }
                )
                continue
            raise RuntimeError(
                f"No PTG provider-location source is available for {source_key}/{snapshot_id}; "
                "publish a compact PTG snapshot with provider_group_location_table or run the legacy "
                "ptg2_provider_location projection."
            )
        source_contexts.append(
            {
                "source_key": source_key,
                "snapshot_id": snapshot_id,
                "provider_group_location_table": provider_group_location_table,
                "provider_group_member_table": provider_group_member_table,
                "serving_rate_compact_table": serving_rate_compact_table,
                "provider_set_component_table": provider_set_component_table,
            }
        )
        await db.status(
            _ptg_address_insert_sql(
                db_schema,
                stage_table,
                source_key=source_key,
                snapshot_id=snapshot_id,
                node_id=node_id,
                address_canon_available=address_canon_available,
                archive_available=archive_available,
                provider_group_location_table=provider_group_location_table,
                provider_group_member_table=provider_group_member_table,
                serving_rate_compact_table=serving_rate_compact_table,
                provider_set_component_table=provider_set_component_table,
            )
        )
    context["sources"] = source_contexts
    context["skipped_sources"] = skipped_sources
    if not source_contexts:
        raise RuntimeError(
            "No PTG provider-location sources were available; publish at least one compact PTG snapshot "
            "with provider_group_location_table or run the legacy ptg2_provider_location projection."
        )
    await _create_stage_indexes(stage_cls, db_schema)
    await db.status(f"ANALYZE {db_schema}.{stage_table};")
    row_count = int(await db.scalar(f"SELECT COUNT(*) FROM {db_schema}.{stage_table};") or 0)
    context["run"] = context.get("run", 0) + 1
    context["staged_rows"] = row_count
    if run_id:
        enqueue_live_progress(
            run_id=run_id,
            importer="ptg-address",
            status="running",
            phase="ptg-address staged",
            unit="rows",
            done=row_count,
            total=row_count,
            pct=95,
            message=f"staged {row_count} PTG address rows",
        )


async def _validate_publish_integrity(
    db_schema: str,
    stage_table: str,
    *,
    test_mode: bool,
) -> dict[str, int]:
    if test_mode:
        return {}

    failures: list[str] = []
    metrics: dict[str, int] = {}

    null_location_keys = int(
        await db.scalar(f"SELECT COUNT(*) FROM {db_schema}.{stage_table} WHERE location_key IS NULL;")
        or 0
    )
    metrics["null_location_keys"] = null_location_keys
    if null_location_keys:
        failures.append(f"{null_location_keys} staged rows have NULL location_key")

    invalid_coordinate_rows = await _invalid_coordinate_count(db_schema, stage_table, db_client=db)
    metrics["invalid_coordinate_rows"] = invalid_coordinate_rows
    if invalid_coordinate_rows:
        failures.append(f"{invalid_coordinate_rows} staged rows have invalid latitude/longitude values")

    unresolved_merged_into_rows = 0
    if await _table_exists(db_schema, "address_archive_v2"):
        unresolved_merged_into_rows = int(
            await db.scalar(
                f"""
                SELECT COUNT(*)
                  FROM {db_schema}.{stage_table} AS t
                  JOIN {db_schema}.address_archive_v2 AS a
                    ON a.address_key = t.address_key
                 WHERE t.address_key IS NOT NULL
                   AND a.merged_into IS NOT NULL;
                """
            )
            or 0
        )
    metrics["unresolved_merged_into_rows"] = unresolved_merged_into_rows
    if unresolved_merged_into_rows:
        failures.append(
            f"{unresolved_merged_into_rows} staged rows point to address_archive_v2.merged_into redirects"
        )

    archive_identity_mismatch_rows = int(
        await db.scalar(
            f"""
            SELECT COUNT(*)
              FROM {db_schema}.{stage_table}
             WHERE COALESCE(archive_identity_version, '') <> '{ARCHIVE_IDENTITY_VERSION}';
            """
        )
        or 0
    )
    metrics["archive_identity_mismatch_rows"] = archive_identity_mismatch_rows
    if archive_identity_mismatch_rows:
        failures.append(
            f"{archive_identity_mismatch_rows} staged rows use a non-current archive_identity_version"
        )

    base_archive_identity_mismatch_rows = 0
    if await _table_exists(db_schema, "entity_address_unified"):
        base_archive_identity_mismatch_rows = int(
            await db.scalar(
                f"""
                SELECT COUNT(*)
                  FROM {db_schema}.{stage_table} AS t
                 WHERE t.archive_identity_version IS NOT NULL
                   AND NOT EXISTS (
                         SELECT 1
                           FROM {db_schema}.entity_address_unified AS e
                          WHERE e.archive_identity_version = t.archive_identity_version
                     );
                """
            )
            or 0
        )
    metrics["base_archive_identity_mismatch_rows"] = base_archive_identity_mismatch_rows
    if base_archive_identity_mismatch_rows:
        failures.append(
            f"{base_archive_identity_mismatch_rows} staged rows use an archive_identity_version absent from entity_address_unified"
        )

    if failures:
        raise RuntimeError("PTGAddress publish integrity validation failed: " + "; ".join(failures))
    return metrics


async def startup(ctx):
    await my_init_db(db)
    ctx["context"] = {
        "start": datetime.datetime.utcnow(),
        "run": 0,
        "test_mode": False,
        "stage_prepared": False,
    }
    await ensure_database(False)
    ctx["import_date"] = _normalize_import_id(os.getenv("HLTHPRT_IMPORT_ID_OVERRIDE"))


async def shutdown(ctx):
    context = ctx.get("context") or {}
    run_id = str(context.get("control_run_id") or ctx.get("control_run_id") or "").strip()
    if not context.get("run"):
        logger.info("No PTGAddress jobs ran; skipping shutdown.")
        return
    await ensure_database(bool(context.get("test_mode")))
    db_schema = _validate_schema_name(os.getenv("HLTHPRT_DB_SCHEMA") or "mrf")
    stage_cls = make_class(PTGAddress, ctx.get("import_date"))
    stage_rows = int(await db.scalar(f"SELECT COUNT(*) FROM {db_schema}.{stage_cls.__tablename__};") or 0)
    min_rows = 0 if context.get("test_mode") else int(os.getenv("HLTHPRT_PTG_ADDRESS_MIN_ROWS", str(DEFAULT_MIN_ROWS)))
    if stage_rows < min_rows:
        raise RuntimeError(f"PTGAddress stage row count {stage_rows} below minimum {min_rows}; aborting publish.")
    publish_validation = await _validate_publish_integrity(
        db_schema,
        stage_cls.__tablename__,
        test_mode=bool(context.get("test_mode")),
    )
    context["publish_validation"] = publish_validation

    async with db.transaction():
        table = PTGAddress.__main_table__
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
    await mark_control_run(
        run_id,
        status="succeeded",
        phase_detail="ptg-address published",
        progress_message="succeeded",
        progress={
            "unit": "rows",
            "done": stage_rows,
            "total": stage_rows,
            "pct": 100,
            "message": "succeeded",
            "phase": "ptg-address published",
        },
        metrics={
            "rows": stage_rows,
            "publish_validation": context.get("publish_validation") or {},
        },
    )
    print_time_info(context.get("start"))


async def main(test_mode: bool = False):
    redis = await create_pool(
        build_redis_settings(),
        job_serializer=serialize_job,
        job_deserializer=deserialize_job,
    )
    await redis.enqueue_job("process_data", {"test_mode": bool(test_mode)}, _queue_name=PTG_ADDRESS_QUEUE_NAME)
