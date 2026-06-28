# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import json
import logging
import os
import sys
from typing import Any

from db.connection import db
from db.models import CodeCatalog, CodeSynonym
from process.ptg_parts.config import (
    PTG2_DEFER_PROVIDER_LOCATIONS_ENV,
    PTG2_SERVING_ROW_LIMIT_ENV,
    PTG2_SKIP_COMPACT_FINALIZE_ENV,
    _env_bool,
)
from process.ptg_parts.domain import (
    PTG2_CONFIDENCE_NPPES_PRACTICE_LOCATION,
    PTG2_CONFIDENCE_TIC_RATE_NPI_TIN,
    ptg2_confidence_statement,
)
from process.ptg_parts.serving_maintenance import _build_ptg2_provider_locations


logger = logging.getLogger(__name__)

PTG2_SOURCE_OBSERVED_PROCEDURE_SOURCE = "ptg2_source_observed_procedure"
PTG2_SOURCE_OBSERVED_PROCEDURE_ATTRIBUTION = (
    "Source-observed label from payer pricing data; not an AMA CPT or ADA CDT reference import."
)
PTG2_SOURCE_OBSERVED_CODE_SYSTEMS = "('CPT', 'HCPCS', 'CDT', 'MS_DRG')"
PTG2_PRICING_PROCEDURE_FALLBACK_SYSTEMS = "('CPT', 'HCPCS', 'CDT')"


def _normalized_code_system_sql(expr: str) -> str:
    return (
        f"""
        CASE NULLIF(UPPER(BTRIM({expr})), '')
            WHEN 'MS-DRG' THEN 'MS_DRG'
            WHEN 'MSDRG' THEN 'MS_DRG'
            WHEN 'DRG' THEN 'MS_DRG'
            WHEN 'ICD-10-PCS' THEN 'ICD10PCS'
            ELSE NULLIF(UPPER(BTRIM({expr})), '')
        END
        """
    )


async def _build_provider_locations_from_facade(snapshot_id: str) -> None:
    ptg_module = sys.modules.get("process.ptg")
    builder = getattr(ptg_module, "_build_ptg2_provider_locations", _build_ptg2_provider_locations)
    await builder(snapshot_id)


async def _ptg2_table_available(schema: str, table_name: str) -> bool:
    try:
        value = await db.scalar("SELECT to_regclass(:table_name)", table_name=f"{schema}.{table_name}")
    except Exception as exc:
        logger.debug("Unable to check availability of %s.%s: %s", schema, table_name, exc)
        return False
    return bool(value)


async def _materialize_ptg2_source_observed_terms(snapshot_id: str, schema: str) -> None:
    await db.create_table(CodeCatalog.__table__, checkfirst=True)
    await db.create_table(CodeSynonym.__table__, checkfirst=True)
    await db.status(
        f"ALTER TABLE {schema}.{CodeCatalog.__tablename__} ADD COLUMN IF NOT EXISTS source_attribution TEXT;"
    )
    await db.status(
        f"ALTER TABLE {schema}.{CodeSynonym.__tablename__} ADD COLUMN IF NOT EXISTS source_attribution TEXT;"
    )
    await db.status(
        f"""
        WITH snapshot_procedure AS (
            SELECT DISTINCT
                {_normalized_code_system_sql("proc.billing_code_type")} AS code_system,
                NULLIF(UPPER(BTRIM(proc.billing_code)), '') AS code,
                NULLIF(BTRIM(proc.name), '') AS procedure_name,
                NULLIF(BTRIM(proc.description), '') AS procedure_description
            FROM {schema}.ptg2_plan_month pm
            JOIN {schema}.ptg2_plan_rate_set prs ON prs.plan_month_id = pm.plan_month_id
            JOIN {schema}.ptg2_rate_set rs ON rs.rate_set_hash = prs.rate_set_hash
            JOIN LATERAL unnest(rs.chunk_hashes) AS chunk_ref(fact_chunk_hash) ON true
            JOIN {schema}.ptg2_fact_chunk fc ON fc.fact_chunk_hash = chunk_ref.fact_chunk_hash
            JOIN {schema}.ptg2_procedure proc ON proc.procedure_hash = fc.procedure_hash
            WHERE pm.snapshot_id = :snapshot_id
        ),
        src AS (
            SELECT
                code_system,
                code,
                procedure_name,
                procedure_description,
                COALESCE(procedure_name, procedure_description, code) AS display_name
            FROM snapshot_procedure
            WHERE code_system IN {PTG2_SOURCE_OBSERVED_CODE_SYSTEMS}
              AND code IS NOT NULL
        )
        INSERT INTO {schema}.{CodeCatalog.__tablename__}
            (
                code_system,
                code,
                display_name,
                short_description,
                long_description,
                is_active,
                source,
                source_attribution,
                updated_at
            )
        SELECT
            code_system,
            code,
            display_name,
            COALESCE(procedure_name, procedure_description),
            procedure_description,
            TRUE,
            :source,
            :source_attribution,
            NOW()
        FROM src
        ON CONFLICT (code_system, code) DO UPDATE
        SET
            display_name = EXCLUDED.display_name,
            short_description = EXCLUDED.short_description,
            long_description = EXCLUDED.long_description,
            is_active = EXCLUDED.is_active,
            source = EXCLUDED.source,
            source_attribution = EXCLUDED.source_attribution,
            updated_at = EXCLUDED.updated_at;
        """,
        snapshot_id=snapshot_id,
        source=PTG2_SOURCE_OBSERVED_PROCEDURE_SOURCE,
        source_attribution=PTG2_SOURCE_OBSERVED_PROCEDURE_ATTRIBUTION,
    )
    await db.status(
        f"""
        WITH snapshot_procedure AS (
            SELECT DISTINCT
                {_normalized_code_system_sql("proc.billing_code_type")} AS code_system,
                NULLIF(UPPER(BTRIM(proc.billing_code)), '') AS code,
                NULLIF(BTRIM(proc.name), '') AS procedure_name,
                NULLIF(BTRIM(proc.description), '') AS procedure_description
            FROM {schema}.ptg2_plan_month pm
            JOIN {schema}.ptg2_plan_rate_set prs ON prs.plan_month_id = pm.plan_month_id
            JOIN {schema}.ptg2_rate_set rs ON rs.rate_set_hash = prs.rate_set_hash
            JOIN LATERAL unnest(rs.chunk_hashes) AS chunk_ref(fact_chunk_hash) ON true
            JOIN {schema}.ptg2_fact_chunk fc ON fc.fact_chunk_hash = chunk_ref.fact_chunk_hash
            JOIN {schema}.ptg2_procedure proc ON proc.procedure_hash = fc.procedure_hash
            WHERE pm.snapshot_id = :snapshot_id
        ),
        terms AS (
            SELECT code_system, code, procedure_name AS synonym, 'source_name' AS term_type
            FROM snapshot_procedure
            WHERE code_system IN {PTG2_SOURCE_OBSERVED_CODE_SYSTEMS}
              AND code IS NOT NULL
              AND procedure_name IS NOT NULL
            UNION
            SELECT code_system, code, procedure_description AS synonym, 'source_description' AS term_type
            FROM snapshot_procedure
            WHERE code_system IN {PTG2_SOURCE_OBSERVED_CODE_SYSTEMS}
              AND code IS NOT NULL
              AND procedure_description IS NOT NULL
        )
        INSERT INTO {schema}.{CodeSynonym.__tablename__}
            (code_system, code, synonym, term_type, language, source, source_attribution, updated_at)
        SELECT
            code_system,
            code,
            synonym,
            term_type,
            'en',
            :source,
            :source_attribution,
            NOW()
        FROM terms
        ON CONFLICT (code_system, code, synonym, term_type) DO UPDATE
        SET
            language = EXCLUDED.language,
            source = EXCLUDED.source,
            source_attribution = EXCLUDED.source_attribution,
            updated_at = EXCLUDED.updated_at;
        """,
        snapshot_id=snapshot_id,
        source=PTG2_SOURCE_OBSERVED_PROCEDURE_SOURCE,
        source_attribution=PTG2_SOURCE_OBSERVED_PROCEDURE_ATTRIBUTION,
    )


async def build_ptg2_db_serving_index(snapshot_id: str, import_run_id: str) -> dict[str, Any] | None:
    """Materialize the compact PTG2 serving view into Postgres, without local JSON artifacts."""
    del import_run_id  # The serving table is tied to snapshot_id; import run remains on the snapshot record.
    schema = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    raw_limit = os.getenv(PTG2_SERVING_ROW_LIMIT_ENV, "").strip()
    limit_clause = ""
    if raw_limit:
        try:
            limit_clause = f" LIMIT {max(int(raw_limit), 1)}"
        except ValueError:
            logger.warning("Ignoring invalid %s=%s", PTG2_SERVING_ROW_LIMIT_ENV, raw_limit)

    await _materialize_ptg2_source_observed_terms(snapshot_id, schema)

    has_code_crosswalk = await _ptg2_table_available(schema, "code_crosswalk")
    has_pricing_procedure = await _ptg2_table_available(schema, "pricing_procedure")
    has_code_catalog = await _ptg2_table_available(schema, "code_catalog")

    mapped_code_join = (
        f"""
            LEFT JOIN LATERAL (
                SELECT cw.to_code::bigint AS procedure_code
                FROM {schema}.code_crosswalk cw
                WHERE UPPER(BTRIM(cw.from_system)) = {_normalized_code_system_sql("proc.billing_code_type")}
                  AND UPPER(BTRIM(cw.from_code)) = UPPER(BTRIM(proc.billing_code))
                  AND UPPER(BTRIM(cw.to_system)) = 'HP_PROCEDURE_CODE'
                  AND cw.to_code ~ '^[0-9]+$'
                ORDER BY cw.confidence DESC NULLS LAST, cw.updated_at DESC NULLS LAST
                LIMIT 1
            ) mapped_code ON true
        """
        if has_code_crosswalk
        else """
            LEFT JOIN LATERAL (
                SELECT NULL::bigint AS procedure_code
            ) mapped_code ON true
        """
    )
    pricing_procedure_join = (
        f"""
            LEFT JOIN LATERAL (
                SELECT
                    pp.procedure_code,
                    pp.service_description,
                    pp.reported_code
                FROM {schema}.pricing_procedure pp
                WHERE (
                        mapped_code.procedure_code IS NOT NULL
                    AND pp.procedure_code = mapped_code.procedure_code
                )
                   OR (
                        mapped_code.procedure_code IS NULL
                    AND UPPER(BTRIM(pp.reported_code)) = UPPER(BTRIM(proc.billing_code))
                    AND {_normalized_code_system_sql("proc.billing_code_type")} IN {PTG2_PRICING_PROCEDURE_FALLBACK_SYSTEMS}
                )
                ORDER BY
                    CASE WHEN pp.procedure_code = mapped_code.procedure_code THEN 0 ELSE 1 END,
                    pp.source_year DESC NULLS LAST,
                    pp.total_allowed_amount DESC NULLS LAST,
                    pp.procedure_code
                LIMIT 1
            ) pricing_proc ON true
        """
        if has_pricing_procedure
        else """
            LEFT JOIN LATERAL (
                SELECT
                    NULL::bigint AS procedure_code,
                    NULL::text AS service_description,
                    NULL::text AS reported_code
            ) pricing_proc ON true
        """
    )
    catalog_joins = (
        f"""
            LEFT JOIN {schema}.code_catalog external_catalog
              ON UPPER(BTRIM(external_catalog.code_system)) = {_normalized_code_system_sql("proc.billing_code_type")}
             AND UPPER(BTRIM(external_catalog.code)) = UPPER(BTRIM(proc.billing_code))
            LEFT JOIN {schema}.code_catalog internal_catalog
              ON UPPER(BTRIM(internal_catalog.code_system)) = 'HP_PROCEDURE_CODE'
             AND internal_catalog.code = COALESCE(mapped_code.procedure_code, pricing_proc.procedure_code)::text
        """
        if has_code_catalog
        else """
            LEFT JOIN LATERAL (
                SELECT NULL::text AS display_name, NULL::text AS short_description
            ) external_catalog ON true
            LEFT JOIN LATERAL (
                SELECT NULL::text AS display_name, NULL::text AS short_description
            ) internal_catalog ON true
        """
    )
    confidence_payload = {
        "network": PTG2_CONFIDENCE_TIC_RATE_NPI_TIN,
        "location": PTG2_CONFIDENCE_NPPES_PRACTICE_LOCATION,
        "acceptance_statement": ptg2_confidence_statement(PTG2_CONFIDENCE_TIC_RATE_NPI_TIN),
    }

    await db.status(f"DELETE FROM {schema}.ptg2_serving_rate WHERE snapshot_id = :snapshot_id", snapshot_id=snapshot_id)
    await db.status(
        f"""
        WITH raw_keys AS (
            SELECT DISTINCT
                pm.snapshot_id,
                p.plan_id,
                p.plan_name,
                p.plan_id_type,
                p.plan_market_type,
                p.issuer_name,
                p.plan_sponsor_name,
                fc.procedure_hash,
                pack_ref.rate_pack_hash
            FROM {schema}.ptg2_plan_month pm
            JOIN {schema}.ptg2_plan p ON p.plan_hash = pm.plan_hash
            JOIN {schema}.ptg2_plan_rate_set prs ON prs.plan_month_id = pm.plan_month_id
            JOIN {schema}.ptg2_rate_set rs ON rs.rate_set_hash = prs.rate_set_hash
            JOIN LATERAL unnest(rs.chunk_hashes) AS chunk_ref(fact_chunk_hash) ON true
            JOIN {schema}.ptg2_fact_chunk fc ON fc.fact_chunk_hash = chunk_ref.fact_chunk_hash
            JOIN {schema}.ptg2_procedure proc_filter ON proc_filter.procedure_hash = fc.procedure_hash
            JOIN LATERAL unnest(fc.rate_pack_hashes) AS pack_ref(rate_pack_hash) ON true
            WHERE pm.snapshot_id = :snapshot_id
              AND p.plan_id IS NOT NULL
              AND proc_filter.billing_code IS NOT NULL
        ),
        procedure_map AS (
            SELECT
                proc.procedure_hash,
                COALESCE(mapped_code.procedure_code, pricing_proc.procedure_code) AS procedure_code,
                {_normalized_code_system_sql("proc.billing_code_type")} AS reported_code_system,
                NULLIF(UPPER(BTRIM(proc.billing_code)), '') AS reported_code,
                proc.billing_code,
                proc.billing_code_type,
                proc.name AS procedure_name,
                proc.description AS procedure_description,
                COALESCE(
                    internal_catalog.display_name,
                    internal_catalog.short_description,
                    external_catalog.display_name,
                    external_catalog.short_description,
                    pricing_proc.service_description,
                    proc.name,
                    proc.description
                ) AS procedure_display_name
            FROM (SELECT DISTINCT procedure_hash FROM raw_keys) raw_proc
            JOIN {schema}.ptg2_procedure proc ON proc.procedure_hash = raw_proc.procedure_hash
            {mapped_code_join}
            {pricing_procedure_join}
            {catalog_joins}
        ),
        rate_payloads AS (
            SELECT
                rp.rate_pack_hash,
                rp.provider_set_hash,
                providers.provider_set_hashes,
                providers.provider_count,
                providers.provider_set_count,
                rp.price_set_hash,
                prices.prices,
                traces.source_trace
            FROM (SELECT DISTINCT rate_pack_hash FROM raw_keys) raw_pack
            JOIN {schema}.ptg2_rate_pack rp ON rp.rate_pack_hash = raw_pack.rate_pack_hash
            LEFT JOIN {schema}.ptg2_price_set price_set ON price_set.price_set_hash = rp.price_set_hash
            LEFT JOIN LATERAL (
                SELECT COALESCE(
                    jsonb_agg(
                        jsonb_build_object(
                            'negotiated_type', pa.negotiated_type,
                            'negotiated_rate',
                                CASE
                                    WHEN pa.negotiated_rate ~ '^-?[0-9]+(\\.[0-9]+)?$'
                                        THEN (pa.negotiated_rate)::numeric
                                    ELSE NULL
                                END,
                            'expiration_date', pa.expiration_date::text,
                            'service_code', COALESCE(pa.service_code, ARRAY[]::text[]),
                            'billing_class', pa.billing_class,
                            'setting', pa.setting,
                            'billing_code_modifier', COALESCE(pa.billing_code_modifier, ARRAY[]::text[]),
                            'additional_information', pa.additional_information
                        )
                        ORDER BY pa.price_atom_hash
                    ),
                    '[]'::jsonb
                ) AS prices
                FROM jsonb_array_elements_text(
                    COALESCE(price_set.canonical_payload::jsonb->'price_atom_hashes', to_jsonb(price_set.price_atom_hashes), '[]'::jsonb)
                ) AS pah(price_atom_hash)
                JOIN {schema}.ptg2_price_atom pa ON pa.price_atom_hash = pah.price_atom_hash
            ) prices ON true
            LEFT JOIN LATERAL (
                SELECT COALESCE(
                    jsonb_agg(
                        jsonb_build_object(
                            'source_file_version_id', st.source_file_version_id,
                            'url', st.original_url,
                            'canonical_url', st.canonical_url,
                            'statement', 'Published negotiated rate from Transparency in Coverage source file.'
                        )
                        ORDER BY st.source_trace_hash
                    ),
                    '[]'::jsonb
                ) AS source_trace
                FROM {schema}.ptg2_source_trace_set sts
                JOIN LATERAL unnest(COALESCE(sts.source_trace_hashes, ARRAY[]::varchar[])) AS sth(source_trace_hash) ON true
                JOIN {schema}.ptg2_source_trace st ON st.source_trace_hash = sth.source_trace_hash
                WHERE sts.source_trace_set_hash = rp.source_trace_set_hash
            ) traces ON true
            LEFT JOIN LATERAL (
                WITH refs AS (
                    SELECT DISTINCT psh.provider_set_hash
                    FROM jsonb_array_elements_text(COALESCE(rp.canonical_payload::jsonb->'provider_set_hashes', '[]'::jsonb)) AS psh(provider_set_hash)
                    UNION
                    SELECT rp.provider_set_hash
                    WHERE NOT (rp.canonical_payload::jsonb ? 'provider_set_hashes')
                      AND rp.provider_set_hash IS NOT NULL
                )
                SELECT
                    COALESCE(array_agg(refs.provider_set_hash ORDER BY refs.provider_set_hash), ARRAY[]::varchar[]) AS provider_set_hashes,
                    count(refs.provider_set_hash)::int AS provider_set_count,
                    COALESCE(sum(ps.provider_count), 0)::int AS provider_count
                FROM refs
                LEFT JOIN {schema}.ptg2_provider_set ps ON ps.provider_set_hash = refs.provider_set_hash
            ) providers ON true
        ),
        selected_rates AS (
            SELECT
                raw_keys.snapshot_id,
                raw_keys.plan_id,
                raw_keys.plan_name,
                raw_keys.plan_id_type,
                raw_keys.plan_market_type,
                raw_keys.issuer_name,
                raw_keys.plan_sponsor_name,
                procedure_map.procedure_code,
                procedure_map.reported_code_system,
                procedure_map.reported_code,
                procedure_map.billing_code,
                procedure_map.billing_code_type,
                procedure_map.procedure_name,
                procedure_map.procedure_description,
                procedure_map.procedure_display_name,
                rate_payloads.rate_pack_hash,
                rate_payloads.provider_set_hash,
                rate_payloads.provider_set_hashes,
                rate_payloads.provider_count,
                rate_payloads.provider_set_count,
                rate_payloads.price_set_hash,
                rate_payloads.prices,
                rate_payloads.source_trace
            FROM raw_keys
            JOIN procedure_map ON procedure_map.procedure_hash = raw_keys.procedure_hash
            JOIN rate_payloads ON rate_payloads.rate_pack_hash = raw_keys.rate_pack_hash
            ORDER BY raw_keys.snapshot_id, raw_keys.plan_id, procedure_map.billing_code, rate_payloads.rate_pack_hash
            {limit_clause}
        )
        INSERT INTO {schema}.ptg2_serving_rate (
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
            prices,
            source_trace,
            confidence,
            created_at
        )
        SELECT
            md5(concat_ws('|', snapshot_id, plan_id, COALESCE(billing_code, ''), rate_pack_hash)) AS serving_rate_id,
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
            prices::json,
            source_trace::json,
            CAST(:confidence_json AS json),
            NOW()
        FROM selected_rates
        ON CONFLICT (serving_rate_id) DO UPDATE
        SET
            plan_name = excluded.plan_name,
            plan_id_type = excluded.plan_id_type,
            plan_market_type = excluded.plan_market_type,
            issuer_name = excluded.issuer_name,
            plan_sponsor_name = excluded.plan_sponsor_name,
            procedure_code = excluded.procedure_code,
            reported_code_system = excluded.reported_code_system,
            reported_code = excluded.reported_code,
            billing_code = excluded.billing_code,
            billing_code_type = excluded.billing_code_type,
            procedure_name = excluded.procedure_name,
            procedure_description = excluded.procedure_description,
            procedure_display_name = excluded.procedure_display_name,
            provider_set_hash = excluded.provider_set_hash,
            provider_set_hashes = excluded.provider_set_hashes,
            provider_count = excluded.provider_count,
            provider_set_count = excluded.provider_set_count,
            price_set_hash = excluded.price_set_hash,
            prices = excluded.prices,
            source_trace = excluded.source_trace,
            confidence = excluded.confidence,
            created_at = excluded.created_at;
        """,
        snapshot_id=snapshot_id,
        confidence_json=json.dumps(confidence_payload),
    )
    try:
        await db.status(f"ANALYZE {schema}.ptg2_serving_rate;")
    except Exception as exc:
        logger.debug("Skipping ptg2_serving_rate ANALYZE: %s", exc)

    count_params = {"snapshot_id": snapshot_id}
    rate_count = int(
        await db.scalar(
            f"SELECT COUNT(*) FROM {schema}.ptg2_serving_rate WHERE snapshot_id = :snapshot_id",
            **count_params,
        )
        or 0
    )
    if rate_count == 0:
        return None
    return {
        "storage": "db",
        "table": f"{schema}.ptg2_serving_rate",
        "snapshot_id": snapshot_id,
        "rate_count": rate_count,
        "plan_count": int(
            await db.scalar(
                f"SELECT COUNT(DISTINCT plan_id) FROM {schema}.ptg2_serving_rate WHERE snapshot_id = :snapshot_id",
                **count_params,
            )
            or 0
        ),
        "procedure_count": int(
            await db.scalar(
                f"""
                SELECT COUNT(DISTINCT COALESCE(procedure_code::text, reported_code, billing_code))
                FROM {schema}.ptg2_serving_rate
                WHERE snapshot_id = :snapshot_id
                """,
                **count_params,
            )
            or 0
        ),
        "provider_reference_count": int(
            await db.scalar(
                f"""
                SELECT COALESCE(SUM(provider_count), 0)
                FROM {schema}.ptg2_serving_rate
                WHERE snapshot_id = :snapshot_id
                """,
                **count_params,
            )
            or 0
        ),
        "provider_granularity": "provider_set",
        "procedure_consolidation": {
            "system": "HP_PROCEDURE_CODE",
            "code_crosswalk_available": has_code_crosswalk,
            "pricing_procedure_available": has_pricing_procedure,
            "code_catalog_available": has_code_catalog,
        },
    }


async def finalize_ptg2_incremental_serving_index(snapshot_id: str) -> dict[str, Any] | None:
    schema = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    try:
        await db.status(f"ANALYZE {schema}.ptg2_serving_rate;")
    except Exception as exc:
        logger.debug("Skipping ptg2_serving_rate ANALYZE: %s", exc)
    count_params = {"snapshot_id": snapshot_id}
    rate_count = int(
        await db.scalar(
            f"SELECT COUNT(*) FROM {schema}.ptg2_serving_rate WHERE snapshot_id = :snapshot_id",
            **count_params,
        )
        or 0
    )
    if rate_count == 0:
        return None
    return {
        "storage": "db",
        "table": f"{schema}.ptg2_serving_rate",
        "snapshot_id": snapshot_id,
        "rate_count": rate_count,
        "plan_count": int(
            await db.scalar(
                f"SELECT COUNT(DISTINCT plan_id) FROM {schema}.ptg2_serving_rate WHERE snapshot_id = :snapshot_id",
                **count_params,
            )
            or 0
        ),
        "procedure_count": int(
            await db.scalar(
                f"""
                SELECT COUNT(DISTINCT COALESCE(procedure_code::text, reported_code, billing_code))
                FROM {schema}.ptg2_serving_rate
                WHERE snapshot_id = :snapshot_id
                """,
                **count_params,
            )
            or 0
        ),
        "provider_reference_count": int(
            await db.scalar(
                f"""
                SELECT COALESCE(SUM(provider_count), 0)
                FROM {schema}.ptg2_serving_rate
                WHERE snapshot_id = :snapshot_id
                """,
                **count_params,
            )
            or 0
        ),
        "provider_granularity": "provider_set",
        "procedure_consolidation": {
            "system": "HP_PROCEDURE_CODE",
            "source": "streaming_import",
        },
    }


async def build_ptg2_stage_serving_index(snapshot_id: str, import_run_id: str) -> dict[str, Any] | None:
    schema = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    try:
        await db.status(f"ANALYZE {schema}.ptg2_serving_rate_stage;")
    except Exception as exc:
        logger.debug("Skipping ptg2_serving_rate_stage ANALYZE: %s", exc)
    reltuples = await db.scalar(
        """
        SELECT reltuples::bigint
          FROM pg_class
         WHERE oid = to_regclass(:table_name)
        """,
        table_name=f"{schema}.ptg2_serving_rate_stage",
    )
    estimated_rate_count = int(reltuples or 0)
    if estimated_rate_count <= 0:
        return None
    stats_rows = await db.all(
        """
        SELECT attname, n_distinct
          FROM pg_stats
         WHERE schemaname = :schema
           AND tablename = 'ptg2_serving_rate_stage'
           AND attname IN ('plan_id', 'billing_code', 'procedure_code')
        """,
        schema=schema,
    )
    stats = {str(row[0]): float(row[1] or 0) for row in stats_rows}

    def estimated_distinct(column: str) -> int | None:
        value = stats.get(column)
        if value is None:
            return None
        if value < 0:
            return max(int(abs(value) * estimated_rate_count), 1)
        return max(int(value), 1)

    return {
        "storage": "db_stage",
        "table": f"{schema}.ptg2_serving_rate_stage",
        "snapshot_id": snapshot_id,
        "import_run_id": import_run_id,
        "estimated_rate_count": estimated_rate_count,
        "estimated_plan_count": estimated_distinct("plan_id"),
        "estimated_procedure_count": estimated_distinct("procedure_code") or estimated_distinct("billing_code"),
        "provider_granularity": "provider_set",
        "procedure_consolidation": {
            "system": "HP_PROCEDURE_CODE",
            "source": "streaming_import",
        },
    }


async def build_ptg2_compact_serving_index(snapshot_id: str, import_run_id: str) -> dict[str, Any] | None:
    schema = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    compact_table = f"{schema}.ptg2_serving_rate_compact"
    if _env_bool(PTG2_SKIP_COMPACT_FINALIZE_ENV, False):
        logger.info("Skipping PTG2 compact serving finalize/index build for %s", snapshot_id)
        return {
            "storage": "db_compact",
            "table": compact_table,
            "snapshot_id": snapshot_id,
            "import_run_id": import_run_id,
            "finalize_skipped": True,
        }
    compact_indexes = [
        (
            "ptg2_serving_rate_compact_billing_idx",
            f"CREATE INDEX IF NOT EXISTS ptg2_serving_rate_compact_billing_idx ON {compact_table} (snapshot_id, plan_id, billing_code)",
        ),
        (
            "ptg2_serving_rate_compact_reported_idx",
            f"CREATE INDEX IF NOT EXISTS ptg2_serving_rate_compact_reported_idx ON {compact_table} (snapshot_id, plan_id, reported_code)",
        ),
        (
            "ptg2_serving_rate_compact_hp_idx",
            f"CREATE INDEX IF NOT EXISTS ptg2_serving_rate_compact_hp_idx ON {compact_table} (snapshot_id, plan_id, procedure_code)",
        ),
        (
            "ptg2_serving_rate_compact_billing_order_idx",
            f"CREATE INDEX IF NOT EXISTS ptg2_serving_rate_compact_billing_order_idx ON {compact_table} (snapshot_id, plan_id, billing_code, provider_count DESC, serving_rate_id)",
        ),
        (
            "ptg2_serving_rate_compact_reported_order_idx",
            f"CREATE INDEX IF NOT EXISTS ptg2_serving_rate_compact_reported_order_idx ON {compact_table} (snapshot_id, plan_id, reported_code, provider_count DESC, serving_rate_id)",
        ),
        (
            "ptg2_serving_rate_compact_provider_idx",
            f"CREATE INDEX IF NOT EXISTS ptg2_serving_rate_compact_provider_idx ON {compact_table} (provider_set_hash)",
        ),
        (
            "ptg2_serving_rate_compact_price_idx",
            f"CREATE INDEX IF NOT EXISTS ptg2_serving_rate_compact_price_idx ON {compact_table} (price_set_hash)",
        ),
        (
            "ptg2_provider_set_component_group_idx",
            f"CREATE INDEX IF NOT EXISTS ptg2_provider_set_component_group_idx ON {schema}.ptg2_provider_set_component (provider_group_hash, provider_set_hash)",
        ),
        (
            "ptg2_provider_set_pkey",
            f"CREATE UNIQUE INDEX IF NOT EXISTS ptg2_provider_set_pkey ON {schema}.ptg2_provider_set (provider_set_hash)",
        ),
        (
            "ptg2_provider_set_component_pkey",
            f"CREATE UNIQUE INDEX IF NOT EXISTS ptg2_provider_set_component_pkey ON {schema}.ptg2_provider_set_component (provider_set_hash, provider_group_hash)",
        ),
        (
            "ptg2_provider_group_member_pkey",
            f"CREATE UNIQUE INDEX IF NOT EXISTS ptg2_provider_group_member_pkey ON {schema}.ptg2_provider_group_member (provider_group_hash, npi)",
        ),
        (
            "ptg2_provider_group_member_npi_idx",
            f"CREATE INDEX IF NOT EXISTS ptg2_provider_group_member_npi_idx ON {schema}.ptg2_provider_group_member (npi, provider_group_hash)",
        ),
        (
            "ptg2_price_set_pkey",
            f"CREATE UNIQUE INDEX IF NOT EXISTS ptg2_price_set_pkey ON {schema}.ptg2_price_set (price_set_hash)",
        ),
    ]
    for index_name, statement in compact_indexes:
        try:
            await db.status(statement)
        except Exception as exc:
            logger.warning("Failed to ensure compact serving index %s: %s", index_name, exc)
    try:
        await db.status(f"ALTER TABLE {compact_table} RESET (autovacuum_enabled, toast.autovacuum_enabled);")
        await db.status(f"ALTER TABLE {schema}.ptg2_provider_set_component RESET (autovacuum_enabled);")
        await db.status(f"ALTER TABLE {schema}.ptg2_provider_group_member RESET (autovacuum_enabled);")
        await db.status(f"ALTER TABLE {schema}.ptg2_price_set RESET (autovacuum_enabled, toast.autovacuum_enabled);")
    except Exception as exc:
        logger.debug("Skipping PTG2 compact autovacuum reset: %s", exc)
    try:
        await db.status(f"ANALYZE {compact_table};")
        await db.status(f"ANALYZE {schema}.ptg2_provider_set_component;")
        await db.status(f"ANALYZE {schema}.ptg2_provider_group_member;")
        await db.status(f"ANALYZE {schema}.ptg2_price_set;")
    except Exception as exc:
        logger.debug("Skipping PTG2 compact ANALYZE: %s", exc)
    if not _env_bool(PTG2_DEFER_PROVIDER_LOCATIONS_ENV, True):
        await _build_provider_locations_from_facade(snapshot_id)
    count_params = {"snapshot_id": snapshot_id}
    rate_count = int(
        await db.scalar(
            f"SELECT COUNT(*) FROM {schema}.ptg2_serving_rate_compact WHERE snapshot_id = :snapshot_id",
            **count_params,
        )
        or 0
    )
    if rate_count <= 0:
        return None
    return {
        "storage": "db_compact",
        "table": f"{schema}.ptg2_serving_rate_compact",
        "snapshot_id": snapshot_id,
        "import_run_id": import_run_id,
        "rate_count": rate_count,
        "plan_count": int(
            await db.scalar(
                f"SELECT COUNT(DISTINCT plan_id) FROM {schema}.ptg2_serving_rate_compact WHERE snapshot_id = :snapshot_id",
                **count_params,
            )
            or 0
        ),
        "procedure_count": int(
            await db.scalar(
                f"SELECT COUNT(DISTINCT COALESCE(procedure_code::text, reported_code, billing_code)) FROM {schema}.ptg2_serving_rate_compact WHERE snapshot_id = :snapshot_id",
                **count_params,
            )
            or 0
        ),
        "provider_granularity": "provider",
        "procedure_consolidation": {
            "system": "HP_PROCEDURE_CODE",
            "source": "streaming_import",
        },
        "json_serving_artifact": False,
    }
