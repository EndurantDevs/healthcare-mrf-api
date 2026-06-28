# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PTG2 serving-table maintenance helpers."""

from __future__ import annotations

import logging
import os
from typing import Any

from db.connection import db
from process.ptg_parts.config import (
    PTG2_FAST_FINAL_REBUILD_ENV,
    PTG2_KEEP_PRICE_SET_STAGE_ENV,
    PTG2_KEEP_SERVING_RATE_STAGE_ENV,
    _env_bool,
)
from process.ptg_parts.copy_load import _copy_ignore_ptg2_objects
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.domain import PTG2_CONFIDENCE_NPPES_PRACTICE_LOCATION

logger = logging.getLogger(__name__)


async def _count_compact_serving_rate_rows(
    snapshot_id: str,
    plan_id: str | None = None,
    *,
    table_name: str = "ptg2_serving_rate_compact",
) -> int:
    schema_name = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    plan_filter = "AND plan_id = :plan_id" if plan_id is not None else ""
    rows = await db.all(
        f"""
        SELECT COUNT(*) AS row_count
          FROM {_quote_ident(schema_name)}.{_quote_ident(table_name)}
         WHERE snapshot_id = :snapshot_id
         {plan_filter}
        """,
        snapshot_id=snapshot_id,
        plan_id=plan_id,
    )
    if not rows:
        return 0
    row = rows[0]
    if isinstance(row, dict):
        return int(row.get("row_count") or 0)
    return int(getattr(row, "row_count", 0) or 0)


async def _copy_simple_rows(rows: list[dict[str, Any]], cls) -> None:
    if not rows:
        return
    await _copy_ignore_ptg2_objects(rows, cls)


async def _merge_staged_price_sets(snapshot_id: str) -> None:
    schema_name = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    await db.status(
        f"""
        INSERT INTO {schema_name}.ptg2_price_set (
            price_set_hash,
            created_at
        )
        SELECT DISTINCT ON (price_set_hash)
            price_set_hash,
            created_at
        FROM {schema_name}.ptg2_price_set_stage
        WHERE snapshot_id = :snapshot_id
        ORDER BY price_set_hash, created_at DESC NULLS LAST
        ON CONFLICT (price_set_hash) DO NOTHING;
        """,
        snapshot_id=snapshot_id,
    )
    if not _env_bool(PTG2_KEEP_PRICE_SET_STAGE_ENV, False):
        await db.status(
            f"DELETE FROM {schema_name}.ptg2_price_set_stage WHERE snapshot_id = :snapshot_id",
            snapshot_id=snapshot_id,
        )


async def _merge_staged_serving_rates(snapshot_id: str) -> None:
    schema_name = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    await db.status(
        f"""
        INSERT INTO {schema_name}.ptg2_serving_rate (
            serving_rate_id,
            snapshot_id,
            plan_id,
            plan_name,
            plan_id_type,
            plan_market_type,
            issuer_name,
            plan_sponsor_name,
            procedure_code,
            reported_code_system,
            reported_code,
            billing_code,
            billing_code_type,
            procedure_name,
            procedure_description,
            procedure_display_name,
            rate_pack_hash,
            provider_set_hash,
            provider_set_hashes,
            provider_count,
            provider_set_count,
            price_set_hash,
            source_trace_set_hash,
            network_names,
            confidence_code,
            prices,
            source_trace,
            confidence,
            created_at
        )
        SELECT
            serving_rate_id,
            snapshot_id,
            plan_id,
            plan_name,
            plan_id_type,
            plan_market_type,
            issuer_name,
            plan_sponsor_name,
            procedure_code,
            reported_code_system,
            reported_code,
            billing_code,
            billing_code_type,
            procedure_name,
            procedure_description,
            procedure_display_name,
            rate_pack_hash,
            provider_set_hash,
            COALESCE(provider_set_hashes, ARRAY[]::varchar[]) AS provider_set_hashes,
            provider_count,
            provider_set_count,
            price_set_hash,
            source_trace_set_hash,
            COALESCE(network_names, ARRAY[]::varchar[]) AS network_names,
            confidence_code,
            prices,
            source_trace,
            confidence,
            created_at
        FROM (
            SELECT
                serving_rate_id,
                snapshot_id,
                plan_id,
                plan_name,
                plan_id_type,
                plan_market_type,
                issuer_name,
                plan_sponsor_name,
                procedure_code,
                reported_code_system,
                reported_code,
                billing_code,
                billing_code_type,
                procedure_name,
                procedure_description,
                procedure_display_name,
                rate_pack_hash,
                provider_set_hash,
                provider_set_hashes,
                provider_count,
                provider_set_count,
                price_set_hash,
                source_trace_set_hash,
                network_names,
                confidence_code,
                prices,
                source_trace,
                confidence,
                created_at
            FROM {schema_name}.ptg2_serving_rate_stage
            WHERE snapshot_id = :snapshot_id
        ) AS s
        ON CONFLICT (serving_rate_id) DO NOTHING;
        """,
        snapshot_id=snapshot_id,
    )
    if not _env_bool(PTG2_KEEP_SERVING_RATE_STAGE_ENV, False):
        await db.status(
            f"DELETE FROM {schema_name}.ptg2_serving_rate_stage WHERE snapshot_id = :snapshot_id",
            snapshot_id=snapshot_id,
        )


async def _build_ptg2_provider_locations(snapshot_id: str) -> None:
    schema_name = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    confidence_code = PTG2_CONFIDENCE_NPPES_PRACTICE_LOCATION
    try:
        await db.status(f"ALTER TABLE {schema_name}.ptg2_provider_location ALTER COLUMN state TYPE varchar(64);")
    except Exception as exc:
        logger.debug("Skipping ptg2_provider_location state width ensure: %s", exc)
    if _env_bool(PTG2_FAST_FINAL_REBUILD_ENV, False):
        selected_npis_sql = f"""
            SELECT DISTINCT npi
            FROM {schema_name}.ptg2_provider_group_member
        """
    else:
        selected_npis_sql = f"""
            SELECT DISTINCT pgm.npi
            FROM {schema_name}.ptg2_serving_rate_compact r
            JOIN {schema_name}.ptg2_provider_set ps
              ON ps.provider_set_hash = r.provider_set_hash
            JOIN LATERAL jsonb_array_elements_text(
                COALESCE(
                    to_jsonb(ps.provider_group_hashes),
                    ps.canonical_payload::jsonb->'provider_group_hashes',
                    '[]'::jsonb
                )
            ) AS psc(provider_group_hash) ON TRUE
            JOIN {schema_name}.ptg2_provider_group_member pgm
              ON pgm.provider_group_hash = psc.provider_group_hash::bigint
            WHERE r.snapshot_id = :snapshot_id
        """
    await db.status(
        f"""
        INSERT INTO {schema_name}.ptg2_provider_location (
            location_hash,
            npi,
            state,
            city,
            city_norm,
            zip5,
            lat,
            lon,
            location_source,
            confidence_code,
            address_payload,
            created_at
        )
        WITH selected_npis AS (
            {selected_npis_sql}
        ),
        address_candidates AS (
            SELECT
                n.npi::bigint AS npi,
                1 AS priority,
                'doctor_clinician_address'::varchar AS location_source,
                NULLIF(BTRIM(d.state), '')::varchar AS state,
                NULLIF(BTRIM(d.city), '')::varchar AS city,
                LOWER(NULLIF(BTRIM(d.city), ''))::varchar AS city_norm,
                NULLIF(LEFT(REGEXP_REPLACE(COALESCE(d.zip_code, ''), '[^0-9]', '', 'g'), 5), '')::varchar AS zip5,
                d.latitude::float AS lat,
                d.longitude::float AS lon,
                jsonb_build_object(
                    'address_line1', d.address_line1,
                    'address_line2', d.address_line2,
                    'city', d.city,
                    'state', d.state,
                    'zip5', NULLIF(LEFT(REGEXP_REPLACE(COALESCE(d.zip_code, ''), '[^0-9]', '', 'g'), 5), ''),
                    'address_key', NULL,
                    'source', 'doctor_clinician_address'
                ) AS address_payload
            FROM selected_npis n
            JOIN {schema_name}.doctor_clinician_address d ON d.npi = n.npi
            WHERE NULLIF(BTRIM(COALESCE(d.state, d.city, d.zip_code, '')), '') IS NOT NULL

            UNION ALL

            SELECT
                n.npi::bigint AS npi,
                2 AS priority,
                'entity_address_unified'::varchar AS location_source,
                NULLIF(BTRIM(e.state_name), '')::varchar AS state,
                NULLIF(BTRIM(e.city_name), '')::varchar AS city,
                LOWER(NULLIF(BTRIM(e.city_name), ''))::varchar AS city_norm,
                NULLIF(LEFT(REGEXP_REPLACE(COALESCE(e.postal_code, ''), '[^0-9]', '', 'g'), 5), '')::varchar AS zip5,
                e.lat::float AS lat,
                e.long::float AS lon,
                jsonb_build_object(
                    'address_line1', e.first_line,
                    'address_line2', e.second_line,
                    'city', e.city_name,
                    'state', e.state_name,
                    'zip5', NULLIF(LEFT(REGEXP_REPLACE(COALESCE(e.postal_code, ''), '[^0-9]', '', 'g'), 5), ''),
                    'address_key', e.address_key::text,
                    'formatted_address', e.formatted_address,
                    'source', 'entity_address_unified'
                ) AS address_payload
            FROM selected_npis n
            JOIN {schema_name}.entity_address_unified e ON COALESCE(e.npi, e.inferred_npi) = n.npi
            WHERE e.type IN ('practice', 'primary', 'secondary', 'site')
              AND NULLIF(BTRIM(COALESCE(e.state_name, e.city_name, e.postal_code, '')), '') IS NOT NULL

            UNION ALL

            SELECT
                n.npi::bigint AS npi,
                3 AS priority,
                'npi_address'::varchar AS location_source,
                NULLIF(BTRIM(a.state_name), '')::varchar AS state,
                NULLIF(BTRIM(a.city_name), '')::varchar AS city,
                LOWER(NULLIF(BTRIM(a.city_name), ''))::varchar AS city_norm,
                NULLIF(LEFT(REGEXP_REPLACE(COALESCE(a.postal_code, ''), '[^0-9]', '', 'g'), 5), '')::varchar AS zip5,
                a.lat::float AS lat,
                a.long::float AS lon,
                jsonb_build_object(
                    'address_line1', a.first_line,
                    'address_line2', a.second_line,
                    'city', a.city_name,
                    'state', a.state_name,
                    'zip5', NULLIF(LEFT(REGEXP_REPLACE(COALESCE(a.postal_code, ''), '[^0-9]', '', 'g'), 5), ''),
                    'address_key', a.address_key::text,
                    'formatted_address', a.formatted_address,
                    'source', 'npi_address'
                ) AS address_payload
            FROM selected_npis n
            JOIN {schema_name}.npi_address a ON a.npi = n.npi
            WHERE a.type IN ('practice', 'primary', 'secondary')
              AND NULLIF(BTRIM(COALESCE(a.state_name, a.city_name, a.postal_code, '')), '') IS NOT NULL
        ),
        ranked AS (
            SELECT
                *,
                row_number() OVER (
                    PARTITION BY npi
                    ORDER BY
                        priority,
                        CASE WHEN zip5 IS NOT NULL AND state IS NOT NULL THEN 0 ELSE 1 END,
                        COALESCE(zip5, ''),
                        COALESCE(city_norm, '')
                ) AS rn
            FROM address_candidates
        )
        SELECT
            md5(concat_ws('|', npi::text, COALESCE(location_source, ''), COALESCE(zip5, ''), COALESCE(city_norm, ''), COALESCE(state, ''))) AS location_hash,
            npi,
            state,
            city,
            city_norm,
            zip5,
            lat,
            lon,
            location_source,
            :confidence_code,
            address_payload::json,
            NOW()
        FROM ranked
        WHERE rn = 1
        ON CONFLICT (location_hash) DO UPDATE
        SET
            npi = excluded.npi,
            state = excluded.state,
            city = excluded.city,
            city_norm = excluded.city_norm,
            zip5 = excluded.zip5,
            lat = excluded.lat,
            lon = excluded.lon,
            location_source = excluded.location_source,
            confidence_code = excluded.confidence_code,
            address_payload = excluded.address_payload,
            created_at = excluded.created_at;
        """,
        snapshot_id=snapshot_id,
        confidence_code=confidence_code,
    )
    try:
        await db.status(f"ANALYZE {schema_name}.ptg2_provider_location;")
    except Exception as exc:
        logger.debug("Skipping ptg2_provider_location ANALYZE: %s", exc)
