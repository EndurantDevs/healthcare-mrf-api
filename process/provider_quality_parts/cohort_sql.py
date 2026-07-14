# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Cohort materialization SQL builders for provider quality imports."""

from __future__ import annotations

from typing import Any

from db.models import (
    DoctorClinicianAddress,
    EntityAddressUnified,
    NPIAddress,
    NPIData,
    NPIDataTaxonomy,
    NUCCTaxonomy,
    PricingProvider,
    PricingProviderProcedure,
    ProviderEnrichmentSummary,
)
from process.provider_quality_parts.config import (
    PROVIDER_QUALITY_DEFAULT_SVI,
    PROVIDER_QUALITY_MAX_YEAR,
    PROVIDER_QUALITY_MINHASH_BANDS,
    PROVIDER_QUALITY_MINHASH_NUM_PERM,
    PROVIDER_QUALITY_MINHASH_ROWS_PER_BAND,
    PROVIDER_QUALITY_MIN_STATE_PEER_N,
    PROVIDER_QUALITY_MIN_YEAR,
    PROVIDER_QUALITY_NATIONAL_MIN_PEER_N,
    PROVIDER_QUALITY_PROCEDURE_MATCH_BANDS,
    PROVIDER_QUALITY_SHRINKAGE_ALPHA,
    PROVIDER_QUALITY_ZIP_MIN_PEER_N,
)
from process.provider_quality_parts.lifecycle import _npi_shard_predicate
from process.provider_quality_parts.sql_helpers import (
    _provider_class_case_sql,
    _state_code_sql,
)


def _cohort_sql_phase_1_build_features(ctx: dict[str, Any]) -> str:
    """Build phase-one provider feature SQL."""
    schema = ctx["schema"]
    feature_table = ctx["feature_table"]
    feature_insert_cols_sql = ctx["feature_insert_cols_sql"]
    feature_select_cols_sql = ctx["feature_select_cols_sql"]
    provider_class_expr = _provider_class_case_sql("COALESCE(nd.entity_type_code, 0)", "pe")
    claims_state_expr = _state_code_sql("p.state")
    doctor_state_expr = _state_code_sql("d.state")
    unified_state_expr = _state_code_sql("e.state_name")
    npi_state_expr = _state_code_sql("a.state_name")
    unified_confirmed_order_sql = (
        "CASE WHEN COALESCE(e.multi_source_confirmed, FALSE) THEN 0 ELSE 1 END,"
        if ctx["unified_address_has_multi_source_confirmed"]
        else ""
    )
    unified_source_count_order_sql = (
        "COALESCE(e.source_count, 0) DESC,"
        if ctx["unified_address_has_source_count"]
        else ""
    )
    unified_checksum_order_sql = "e.checksum" if ctx["unified_address_has_checksum"] else "COALESCE(e.entity_id, 0)"
    classification_select_sql = (
        "COALESCE(NULLIF(LOWER(BTRIM(COALESCE(nt.classification, ''))), ''), 'unknown')::varchar"
        if ctx["nucc_table_exists"] and ctx["nucc_has_classification"]
        else "'unknown'::varchar"
    )
    npi_base_cte = (
        f"""
        npi_base AS (
            SELECT
                n.npi::bigint AS npi,
                n.entity_type_code
            FROM {schema}.{NPIData.__tablename__} n
        ),
        """
        if ctx["npi_table_exists"]
        else """
        npi_base AS (
            SELECT
                NULL::bigint AS npi,
                NULL::int AS entity_type_code
            WHERE FALSE
        ),
        """
    )
    taxonomy_cte = (
        f"""
        taxonomy_ranked AS (
            SELECT
                t.npi::bigint AS npi,
                NULLIF(BTRIM(COALESCE(t.healthcare_provider_taxonomy_code, '')), '')::varchar AS taxonomy_code,
                ROW_NUMBER() OVER (
                    PARTITION BY t.npi
                    ORDER BY
                        CASE
                            WHEN UPPER(COALESCE(t.healthcare_provider_primary_taxonomy_switch, '')) = 'Y' THEN 0
                            ELSE 1
                        END,
                        t.checksum
                ) AS rn
            FROM {schema}.{NPIDataTaxonomy.__tablename__} t
            WHERE NULLIF(BTRIM(COALESCE(t.healthcare_provider_taxonomy_code, '')), '') IS NOT NULL
        ),
        taxonomy_choice AS (
            SELECT
                tr.npi,
                LOWER(BTRIM(tr.taxonomy_code))::varchar AS taxonomy
            FROM taxonomy_ranked tr
            WHERE tr.rn = 1
        ),
        """
        if ctx["taxonomy_table_exists"]
        else """
        taxonomy_choice AS (
            SELECT
                NULL::bigint AS npi,
                NULL::varchar AS taxonomy
            WHERE FALSE
        ),
        """
    )
    provider_enrichment_cte = (
        f"""
        provider_enrichment_choice AS (
            SELECT
                pe.npi::bigint AS npi,
                COALESCE(pe.has_any_enrollment, FALSE)::boolean AS has_any_enrollment,
                COALESCE(pe.has_hospital_enrollment, FALSE)::boolean AS has_hospital_enrollment,
                COALESCE(pe.has_hha_enrollment, FALSE)::boolean AS has_hha_enrollment,
                COALESCE(pe.has_hospice_enrollment, FALSE)::boolean AS has_hospice_enrollment,
                COALESCE(pe.has_fqhc_enrollment, FALSE)::boolean AS has_fqhc_enrollment,
                COALESCE(pe.has_rhc_enrollment, FALSE)::boolean AS has_rhc_enrollment,
                COALESCE(pe.has_snf_enrollment, FALSE)::boolean AS has_snf_enrollment,
                COALESCE(pe.has_medicare_claims, FALSE)::boolean AS has_medicare_claims
            FROM {schema}.{ProviderEnrichmentSummary.__tablename__} pe
        ),
        """
        if ctx["provider_enrichment_exists"]
        else """
        provider_enrichment_choice AS (
            SELECT
                NULL::bigint AS npi,
                FALSE::boolean AS has_any_enrollment,
                FALSE::boolean AS has_hospital_enrollment,
                FALSE::boolean AS has_hha_enrollment,
                FALSE::boolean AS has_hospice_enrollment,
                FALSE::boolean AS has_fqhc_enrollment,
                FALSE::boolean AS has_rhc_enrollment,
                FALSE::boolean AS has_snf_enrollment,
                FALSE::boolean AS has_medicare_claims
            WHERE FALSE
        ),
        """
    )
    doctor_address_cte = (
        f"""
        doctor_address_choice AS (
            SELECT DISTINCT ON (d.npi)
                d.npi::bigint AS npi,
                ({doctor_state_expr})::varchar AS state_key,
                NULLIF(LEFT(REGEXP_REPLACE(COALESCE(d.zip_code, ''), '[^0-9]', '', 'g'), 5), '')::varchar AS zip5
            FROM {schema}.{DoctorClinicianAddress.__tablename__} d
            WHERE NULLIF(BTRIM(COALESCE(d.state, '')), '') IS NOT NULL
               OR NULLIF(BTRIM(COALESCE(d.zip_code, '')), '') IS NOT NULL
            ORDER BY
                d.npi,
                CASE
                    WHEN NULLIF(LEFT(REGEXP_REPLACE(COALESCE(d.zip_code, ''), '[^0-9]', '', 'g'), 5), '') IS NOT NULL
                     AND NULLIF(BTRIM(COALESCE(d.state, '')), '') IS NOT NULL THEN 0
                    ELSE 1
                END,
                d.address_checksum
        ),
        """
        if ctx["doctor_clinician_exists"]
        else """
        doctor_address_choice AS (
            SELECT
                NULL::bigint AS npi,
                NULL::varchar AS state_key,
                NULL::varchar AS zip5
            WHERE FALSE
        ),
        """
    )
    unified_address_cte = (
        f"""
        unified_address_choice AS (
            SELECT DISTINCT ON (COALESCE(e.npi, e.inferred_npi))
                COALESCE(e.npi, e.inferred_npi)::bigint AS npi,
                ({unified_state_expr})::varchar AS state_key,
                NULLIF(LEFT(REGEXP_REPLACE(COALESCE(e.postal_code, ''), '[^0-9]', '', 'g'), 5), '')::varchar AS zip5
            FROM {schema}.{EntityAddressUnified.__tablename__} e
            WHERE COALESCE(e.npi, e.inferred_npi) IS NOT NULL
              AND e.type IN ('practice', 'primary', 'secondary', 'site')
              AND (
                    NULLIF(BTRIM(COALESCE(e.state_name, '')), '') IS NOT NULL
                 OR NULLIF(BTRIM(COALESCE(e.postal_code, '')), '') IS NOT NULL
              )
            ORDER BY
                COALESCE(e.npi, e.inferred_npi),
                {unified_confirmed_order_sql}
                {unified_source_count_order_sql}
                CASE e.type
                    WHEN 'practice' THEN 0
                    WHEN 'primary' THEN 1
                    WHEN 'secondary' THEN 2
                    ELSE 3
                END,
                {unified_checksum_order_sql}
        ),
        """
        if ctx["unified_address_exists"]
        else """
        unified_address_choice AS (
            SELECT
                NULL::bigint AS npi,
                NULL::varchar AS state_key,
                NULL::varchar AS zip5
            WHERE FALSE
        ),
        """
    )
    npi_address_cte = (
        f"""
        npi_address_choice AS (
            SELECT DISTINCT ON (a.npi)
                a.npi::bigint AS npi,
                ({npi_state_expr})::varchar AS state_key,
                NULLIF(LEFT(REGEXP_REPLACE(COALESCE(a.postal_code, ''), '[^0-9]', '', 'g'), 5), '')::varchar AS zip5
            FROM {schema}.{NPIAddress.__tablename__} a
            WHERE a.type IN ('primary', 'practice', 'secondary')
              AND (
                    NULLIF(BTRIM(COALESCE(a.state_name, '')), '') IS NOT NULL
                 OR NULLIF(BTRIM(COALESCE(a.postal_code, '')), '') IS NOT NULL
              )
            ORDER BY
                a.npi,
                CASE a.type
                    WHEN 'practice' THEN 0
                    WHEN 'primary' THEN 1
                    WHEN 'secondary' THEN 2
                    ELSE 3
                END,
                a.checksum
        ),
        """
        if ctx["npi_address_exists"]
        else """
        npi_address_choice AS (
            SELECT
                NULL::bigint AS npi,
                NULL::varchar AS state_key,
                NULL::varchar AS zip5
            WHERE FALSE
        ),
        """
    )
    nucc_join = (
        f"""
            LEFT JOIN {schema}.{NUCCTaxonomy.__tablename__} nt
              ON LOWER(BTRIM(COALESCE(nt.code, ''))) = tc.taxonomy
        """
        if ctx["nucc_table_exists"]
        else ""
    )
    return f"""
        INSERT INTO {schema}.{feature_table}
        (
            {feature_insert_cols_sql}
        )
        WITH provider_base AS (
            SELECT
                p.npi,
                p.year,
                p.provider_type,
                NULLIF(BTRIM(COALESCE(p.zip5, '')), '')::varchar AS claims_zip5,
                ({claims_state_expr})::varchar AS claims_state
            FROM {schema}.{PricingProvider.__tablename__} p
            WHERE p.year BETWEEN {PROVIDER_QUALITY_MIN_YEAR} AND {PROVIDER_QUALITY_MAX_YEAR}
        ),
        {npi_base_cte}
        {taxonomy_cte}
        {provider_enrichment_cte}
        procedure_counts AS (
            SELECT
                pp.npi,
                pp.year,
                COUNT(DISTINCT pp.procedure_code)::int AS procedure_count
            FROM {schema}.{PricingProviderProcedure.__tablename__} pp
            WHERE pp.year BETWEEN {PROVIDER_QUALITY_MIN_YEAR} AND {PROVIDER_QUALITY_MAX_YEAR}
            GROUP BY pp.npi, pp.year
        ),
        {doctor_address_cte}
        {unified_address_cte}
        {npi_address_cte}
        features_src AS (
            SELECT
                p.npi,
                p.year,
                COALESCE(da.zip5, ua.zip5, na.zip5, p.claims_zip5)::varchar AS zip5,
                COALESCE(da.state_key, ua.state_key, na.state_key, p.claims_state)::varchar AS state,
                {provider_class_expr} AS provider_class,
                CASE
                    WHEN da.npi IS NOT NULL THEN 'doctor_clinician_address'
                    WHEN ua.npi IS NOT NULL THEN 'entity_address_unified'
                    WHEN na.npi IS NOT NULL THEN 'npi_address'
                    WHEN p.claims_zip5 IS NOT NULL OR p.claims_state IS NOT NULL THEN 'claims_pricing'
                    ELSE 'unknown'
                END::varchar AS location_source,
                COALESCE(pe.has_any_enrollment, FALSE) AS has_enrollment,
                COALESCE(pe.has_medicare_claims, FALSE) AS has_medicare_claims,
                COALESCE(NULLIF(LOWER(BTRIM(COALESCE(p.provider_type, ''))), ''), 'unknown')::varchar AS specialty,
                COALESCE(tc.taxonomy, 'unknown')::varchar AS taxonomy,
                {classification_select_sql} AS classification,
                COALESCE(pc.procedure_count, 0)::int AS procedure_count,
                'bucket:none'::varchar AS procedure_bucket,
                'provider_quality'::varchar AS source,
                NOW() AS updated_at
            FROM provider_base p
            LEFT JOIN npi_base nd
              ON nd.npi = p.npi
            LEFT JOIN provider_enrichment_choice pe
              ON pe.npi = p.npi
            LEFT JOIN taxonomy_choice tc
              ON tc.npi = p.npi
            {nucc_join}
            LEFT JOIN procedure_counts pc
              ON pc.npi = p.npi
             AND pc.year = p.year
            LEFT JOIN doctor_address_choice da
              ON da.npi = p.npi
            LEFT JOIN unified_address_choice ua
              ON ua.npi = p.npi
            LEFT JOIN npi_address_choice na
              ON na.npi = p.npi
        )
        SELECT
            {feature_select_cols_sql}
        FROM features_src f;
    """


def _cohort_sql_phase_2_build_lsh_shard(ctx: dict[str, Any]) -> str:
    """Build phase-two LSH shard SQL."""
    schema = ctx["schema"]
    lsh_table = ctx["lsh_table"]
    lsh_insert_cols_sql = ctx["lsh_insert_cols_sql"]
    lsh_select_cols_sql = ctx["lsh_select_cols_sql"]
    return f"""
        WITH deleted AS (
            DELETE FROM {schema}.{lsh_table} d
            WHERE d.year = :year
              AND {_npi_shard_predicate("d.npi")}
            RETURNING 1
        ),
        provider_proc AS (
            SELECT DISTINCT
                pp.npi,
                pp.year,
                pp.procedure_code::varchar AS procedure_key
            FROM {schema}.{PricingProviderProcedure.__tablename__} pp
            WHERE pp.year = :year
              AND {_npi_shard_predicate("pp.npi")}
        ),
        perms AS (
            SELECT generate_series(0, {PROVIDER_QUALITY_MINHASH_NUM_PERM} - 1) AS perm_no
        ),
        signatures AS (
            SELECT
                p.npi,
                p.year,
                perm.perm_no,
                MIN(ABS(hashtextextended(p.procedure_key, perm.perm_no::bigint)))::bigint AS min_hash
            FROM provider_proc p
            CROSS JOIN perms perm
            GROUP BY p.npi, p.year, perm.perm_no
        ),
        bands AS (
            SELECT
                s.npi,
                s.year,
                FLOOR(s.perm_no / {PROVIDER_QUALITY_MINHASH_ROWS_PER_BAND})::int AS band_no,
                STRING_AGG(
                    LPAD(TO_HEX((s.min_hash % 4294967295)::bigint), 8, '0'),
                    '|' ORDER BY s.perm_no
                )::varchar AS band_signature,
                COUNT(*)::int AS rows_in_band
            FROM signatures s
            GROUP BY s.npi, s.year, FLOOR(s.perm_no / {PROVIDER_QUALITY_MINHASH_ROWS_PER_BAND})
        ),
        lsh_src AS (
            SELECT
                b.npi,
                b.year,
                b.band_no,
                MD5(b.band_signature)::varchar AS band_hash,
                b.rows_in_band,
                'provider_quality'::varchar AS source,
                NOW() AS updated_at
            FROM bands b
            WHERE b.band_no < {PROVIDER_QUALITY_MINHASH_BANDS}
        )
        INSERT INTO {schema}.{lsh_table}
        (
            {lsh_insert_cols_sql}
        )
        SELECT
            {lsh_select_cols_sql}
        FROM lsh_src s;
    """


def _cohort_sql_phase_3_update_procedure_bucket(ctx: dict[str, Any]) -> str | None:
    if not ctx["feature_procedure_bucket_col"]:
        return None
    schema = ctx["schema"]
    lsh_table = ctx["lsh_table"]
    feature_table = ctx["feature_table"]
    return f"""
        WITH bucket_source AS (
            SELECT
                l.{ctx["lsh_npi_col"]} AS npi,
                l.{ctx["lsh_year_col"]} AS year,
                COALESCE(
                    CASE
                        WHEN COUNT(*) FILTER (WHERE l.{ctx["lsh_band_no_col"]} < {PROVIDER_QUALITY_PROCEDURE_MATCH_BANDS})
                             >= {PROVIDER_QUALITY_PROCEDURE_MATCH_BANDS}
                        THEN MD5(
                            STRING_AGG(
                                l.{ctx["lsh_band_hash_col"]},
                                '|' ORDER BY l.{ctx["lsh_band_no_col"]}
                            ) FILTER (WHERE l.{ctx["lsh_band_no_col"]} < {PROVIDER_QUALITY_PROCEDURE_MATCH_BANDS})
                        )::varchar
                        ELSE NULL
                    END,
                    MAX(CASE WHEN l.{ctx["lsh_band_no_col"]} = 0 THEN l.{ctx["lsh_band_hash_col"]} END),
                    'bucket:none'
                )::varchar AS procedure_bucket
            FROM {schema}.{lsh_table} l
            GROUP BY l.{ctx["lsh_npi_col"]}, l.{ctx["lsh_year_col"]}
        )
        UPDATE {schema}.{feature_table} f
        SET {ctx["feature_procedure_bucket_col"]} = b.procedure_bucket
        FROM bucket_source b
        WHERE f.{ctx["feature_npi_col"]} = b.npi
          AND f.{ctx["feature_year_col"]} = b.year;
    """


def _cohort_sql_phase_4_build_peer_targets(ctx: dict[str, Any]) -> str:
    """Build phase-four peer-target SQL."""
    schema = ctx["schema"]
    qpp_table = ctx["qpp_table"]
    svi_table = ctx["svi_table"]
    feature_table = ctx["feature_table"]
    peer_target_table = ctx["peer_target_table"]
    return f"""
        INSERT INTO {schema}.{peer_target_table}
        (
            {ctx["peer_target_insert_cols_sql"]}
        )
        WITH provider_base AS (
            SELECT
                p.npi,
                p.year,
                p.provider_type,
                NULLIF(BTRIM(COALESCE(p.zip5, '')), '')::varchar AS zip5,
                UPPER(NULLIF(BTRIM(COALESCE(p.state, '')), ''))::varchar AS state_key,
                COALESCE(p.total_services, 0.0)::float8 AS total_services,
                COALESCE(p.total_beneficiaries, 0.0)::float8 AS total_beneficiaries,
                COALESCE(p.total_allowed_amount, 0.0)::float8 AS total_allowed_amount,
                CASE
                    WHEN COALESCE(p.total_beneficiaries, 0.0) > 0
                    THEN COALESCE(p.total_services, 0.0)::float8 / NULLIF(COALESCE(p.total_beneficiaries, 0.0), 0.0)
                    ELSE NULL
                END AS utilization_rate
            FROM {schema}.{PricingProvider.__tablename__} p
            WHERE p.year BETWEEN {PROVIDER_QUALITY_MIN_YEAR} AND {PROVIDER_QUALITY_MAX_YEAR}
        ),
        {ctx["provider_rx_cte"]}
        provider_plus AS (
            SELECT
                b.*,
                q.quality_score AS qpp_quality_score,
                q.cost_score AS qpp_cost_score,
                COALESCE(rx.total_rx_claims, 0.0)::float8 AS total_rx_claims,
                COALESCE(rx.total_rx_beneficiaries, 0.0)::float8 AS total_rx_beneficiaries,
                COALESCE(s.svi_overall, {PROVIDER_QUALITY_DEFAULT_SVI}) AS svi_overall,
                COALESCE(
                    b.utilization_rate / NULLIF(1.0 + 0.2 * (COALESCE(s.svi_overall, {PROVIDER_QUALITY_DEFAULT_SVI}) - 0.5), 0.0),
                    b.utilization_rate
                ) AS utilization_adjusted,
                COALESCE(
                    b.total_allowed_amount / NULLIF(1.0 + 0.2 * (COALESCE(s.svi_overall, {PROVIDER_QUALITY_DEFAULT_SVI}) - 0.5), 0.0),
                    b.total_allowed_amount
                ) AS cost_adjusted,
                CASE
                    WHEN COALESCE(rx.total_rx_beneficiaries, 0.0) > 0
                    THEN COALESCE(rx.total_rx_claims, 0.0)::float8 / NULLIF(COALESCE(rx.total_rx_beneficiaries, 0.0), 0.0)
                    ELSE NULL
                END AS rx_claim_rate
            FROM provider_base b
            LEFT JOIN {schema}.{qpp_table} q
              ON q.npi = b.npi
             AND q.year = b.year
            LEFT JOIN provider_rx rx
              ON rx.npi = b.npi
             AND rx.year = b.year
            LEFT JOIN {schema}.{svi_table} s
              ON s.zcta = b.zip5
             AND s.year = b.year
        ),
        provider_featured AS (
            SELECT
                p.*,
                {ctx["feature_zip_expr"]} AS resolved_zip5,
                {ctx["feature_state_expr"]} AS resolved_state_key,
                {ctx["feature_provider_class_expr"]} AS provider_class,
                {ctx["feature_location_source_expr"]} AS location_source,
                {ctx["feature_has_enrollment_expr"]} AS has_enrollment,
                {ctx["feature_has_medicare_claims_expr"]} AS has_medicare_claims,
                {ctx["feature_specialty_expr"]} AS specialty,
                {ctx["feature_taxonomy_expr"]} AS taxonomy,
                {ctx["feature_classification_expr"]} AS classification,
                {ctx["feature_procedure_bucket_expr"]} AS procedure_bucket
            FROM provider_plus p
            LEFT JOIN {schema}.{feature_table} f
              ON f.{ctx["feature_npi_col"]} = p.npi
             AND f.{ctx["feature_year_col"]} = p.year
        ),
        benchmark_modes AS (
            SELECT v.benchmark_mode
            FROM (
                VALUES
                {ctx["benchmark_mode_values"]}
            ) AS v(benchmark_mode)
        ),
        cohort_expanded AS (
            SELECT
                p.year,
                bm.benchmark_mode,
                gs.geography_scope,
                CASE
                    WHEN gs.geography_scope = 'zip' THEN p.resolved_zip5
                    WHEN gs.geography_scope = 'state' THEN p.resolved_state_key
                    ELSE 'US'
                END::varchar AS geography_value,
                lv.cohort_level,
                CASE
                    WHEN lv.cohort_level IN ('L0', 'L1', 'L2') THEN p.specialty
                    ELSE NULL
                END::varchar AS specialty,
                CASE
                    WHEN lv.cohort_level IN ('L0', 'L1') THEN p.taxonomy
                    ELSE NULL
                END::varchar AS taxonomy,
                CASE
                    WHEN lv.cohort_level IN ('L0', 'L1') THEN p.classification
                    ELSE NULL
                END::varchar AS classification,
                CASE
                    WHEN lv.cohort_level = 'L0' THEN p.procedure_bucket
                    ELSE NULL
                END::varchar AS procedure_bucket,
                p.utilization_adjusted,
                p.cost_adjusted,
                p.qpp_quality_score,
                p.qpp_cost_score,
                p.rx_claim_rate
            FROM provider_featured p
            CROSS JOIN benchmark_modes bm
            CROSS JOIN (VALUES ('national'), ('state'), ('zip')) AS gs(geography_scope)
            CROSS JOIN (VALUES ('L0'), ('L1'), ('L2'), ('L3')) AS lv(cohort_level)
            WHERE (
                (bm.benchmark_mode = 'zip' AND gs.geography_scope IN ('zip', 'state', 'national'))
                OR (bm.benchmark_mode = 'state' AND gs.geography_scope IN ('state', 'national'))
                OR (bm.benchmark_mode = 'national' AND gs.geography_scope = 'national')
            )
            AND (
                gs.geography_scope = 'national'
                OR (gs.geography_scope = 'state' AND p.resolved_state_key IS NOT NULL)
                OR (gs.geography_scope = 'zip' AND p.resolved_zip5 IS NOT NULL)
            )
        ),
        peer_target_src AS (
            SELECT
                c.year,
                c.benchmark_mode,
                c.geography_scope,
                c.geography_value,
                c.cohort_level,
                COALESCE(c.specialty, 'unknown')::varchar AS specialty,
                COALESCE(c.taxonomy, 'unknown')::varchar AS taxonomy,
                CASE
                    WHEN c.cohort_level IN ('L0', 'L1') THEN MAX(c.classification)
                    ELSE NULL
                END::varchar AS classification,
                COALESCE(c.procedure_bucket, 'bucket:none')::varchar AS procedure_bucket,
                COUNT(*)::int AS peer_n,
                AVG(NULLIF(c.utilization_adjusted, 0.0)) AS target_appropriateness,
                AVG(NULLIF(c.cost_adjusted, 0.0)) AS target_cost,
                AVG(NULLIF(c.qpp_quality_score, 0.0)) AS target_effectiveness,
                AVG(NULLIF(c.qpp_cost_score, 0.0)) AS target_qpp_cost,
                AVG(NULLIF(c.rx_claim_rate, 0.0)) AS target_rx_appropriateness,
                'provider_quality'::varchar AS source,
                NOW() AS updated_at
            FROM cohort_expanded c
            GROUP BY
                c.year,
                c.benchmark_mode,
                c.geography_scope,
                c.geography_value,
                c.cohort_level,
                COALESCE(c.specialty, 'unknown')::varchar,
                COALESCE(c.taxonomy, 'unknown')::varchar,
                COALESCE(c.procedure_bucket, 'bucket:none')::varchar
        )
        SELECT
            {ctx["peer_target_select_cols_sql"]}
        FROM peer_target_src t;
    """


def _cohort_sql_phase_5_build_measure_shard(ctx: dict[str, Any]) -> str:
    """Build phase-five measure shard SQL."""
    schema = ctx["schema"]
    qpp_table = ctx["qpp_table"]
    svi_table = ctx["svi_table"]
    feature_table = ctx["feature_table"]
    peer_target_table = ctx["peer_target_table"]
    return f"""
        WITH deleted AS (
            DELETE FROM {schema}.{ctx["measure_table"]} d
            WHERE d.year = :year
              AND {_npi_shard_predicate("d.npi")}
            RETURNING 1
        ),
        provider_base AS (
            SELECT
                p.npi,
                p.year,
                p.provider_type,
                NULLIF(BTRIM(COALESCE(p.zip5, '')), '')::varchar AS zip5,
                UPPER(NULLIF(BTRIM(COALESCE(p.state, '')), ''))::varchar AS state_key,
                COALESCE(p.total_services, 0.0)::float8 AS total_services,
                COALESCE(p.total_beneficiaries, 0.0)::float8 AS total_beneficiaries,
                COALESCE(p.total_allowed_amount, 0.0)::float8 AS total_allowed_amount,
                CASE
                    WHEN COALESCE(p.total_beneficiaries, 0.0) > 0
                    THEN COALESCE(p.total_services, 0.0)::float8 / NULLIF(COALESCE(p.total_beneficiaries, 0.0), 0.0)
                    ELSE NULL
                END AS utilization_rate
            FROM {schema}.{PricingProvider.__tablename__} p
            WHERE p.year = :year
              AND {_npi_shard_predicate("p.npi")}
        ),
        {ctx["provider_rx_cte"]}
        provider_plus AS (
            SELECT
                b.*,
                q.quality_score AS qpp_quality_score,
                q.cost_score AS qpp_cost_score,
                COALESCE(rx.total_rx_claims, 0.0)::float8 AS total_rx_claims,
                COALESCE(rx.total_rx_beneficiaries, 0.0)::float8 AS total_rx_beneficiaries,
                COALESCE(s.svi_overall, {PROVIDER_QUALITY_DEFAULT_SVI}) AS svi_overall,
                COALESCE(
                    b.utilization_rate / NULLIF(1.0 + 0.2 * (COALESCE(s.svi_overall, {PROVIDER_QUALITY_DEFAULT_SVI}) - 0.5), 0.0),
                    b.utilization_rate
                ) AS utilization_adjusted,
                COALESCE(
                    b.total_allowed_amount / NULLIF(1.0 + 0.2 * (COALESCE(s.svi_overall, {PROVIDER_QUALITY_DEFAULT_SVI}) - 0.5), 0.0),
                    b.total_allowed_amount
                ) AS cost_adjusted,
                CASE
                    WHEN COALESCE(rx.total_rx_beneficiaries, 0.0) > 0
                    THEN COALESCE(rx.total_rx_claims, 0.0)::float8 / NULLIF(COALESCE(rx.total_rx_beneficiaries, 0.0), 0.0)
                    ELSE NULL
                END AS rx_claim_rate
            FROM provider_base b
            LEFT JOIN {schema}.{qpp_table} q
              ON q.npi = b.npi
             AND q.year = b.year
            LEFT JOIN provider_rx rx
              ON rx.npi = b.npi
             AND rx.year = b.year
            LEFT JOIN {schema}.{svi_table} s
              ON s.zcta = b.zip5
             AND s.year = b.year
        ),
        provider_featured AS (
            SELECT
                p.*,
                {ctx["feature_zip_expr"]} AS resolved_zip5,
                {ctx["feature_state_expr"]} AS resolved_state_key,
                {ctx["feature_provider_class_expr"]} AS provider_class,
                {ctx["feature_location_source_expr"]} AS location_source,
                {ctx["feature_has_enrollment_expr"]} AS has_enrollment,
                {ctx["feature_has_medicare_claims_expr"]} AS has_medicare_claims,
                {ctx["feature_specialty_expr"]} AS specialty,
                {ctx["feature_taxonomy_expr"]} AS taxonomy,
                {ctx["feature_classification_expr"]} AS classification,
                {ctx["feature_procedure_bucket_expr"]} AS procedure_bucket
            FROM provider_plus p
            LEFT JOIN {schema}.{feature_table} f
              ON f.{ctx["feature_npi_col"]} = p.npi
             AND f.{ctx["feature_year_col"]} = p.year
        ),
        benchmark_modes AS (
            SELECT v.benchmark_mode
            FROM (
                VALUES
                {ctx["benchmark_mode_values"]}
            ) AS v(benchmark_mode)
        ),
        target_candidates AS (
            SELECT
                p.npi,
                p.year,
                bm.benchmark_mode,
                ({ctx["peer_scope_expr"]})::varchar AS selected_geography_scope,
                ({ctx["peer_geo_value_expr"]})::varchar AS selected_geography_value,
                ({ctx["peer_cohort_level_expr"]})::varchar AS selected_cohort_level,
                ({ctx["peer_specialty_expr"]})::varchar AS selected_specialty,
                ({ctx["peer_taxonomy_expr"]})::varchar AS selected_taxonomy,
                ({ctx["peer_classification_expr"]})::varchar AS selected_classification,
                ({ctx["peer_procedure_bucket_expr"]})::varchar AS selected_procedure_bucket,
                ({ctx["peer_peer_n_expr"]})::float8 AS selected_peer_n,
                t.{ctx["peer_target_approp_col"]}::float8 AS target_appropriateness,
                t.{ctx["peer_target_cost_col"]}::float8 AS target_cost,
                t.{ctx["peer_target_effect_col"]}::float8 AS target_effectiveness,
                t.{ctx["peer_target_qpp_cost_col"]}::float8 AS target_qpp_cost,
                t.{ctx["peer_target_rx_appropr_col"]}::float8 AS target_rx_appropriateness,
                CASE
                    WHEN bm.benchmark_mode = 'zip' THEN
                        CASE
                            WHEN {ctx["peer_scope_expr"]} = 'zip' THEN 1
                            WHEN {ctx["peer_scope_expr"]} = 'state' THEN 2
                            WHEN {ctx["peer_scope_expr"]} = 'national' THEN 3
                            ELSE 9
                        END
                    WHEN bm.benchmark_mode = 'state' THEN
                        CASE
                            WHEN {ctx["peer_scope_expr"]} = 'state' THEN 1
                            WHEN {ctx["peer_scope_expr"]} = 'national' THEN 2
                            ELSE 9
                        END
                    ELSE
                        CASE WHEN {ctx["peer_scope_expr"]} = 'national' THEN 1 ELSE 9 END
                END AS geography_rank,
                CASE
                    WHEN {ctx["peer_cohort_level_expr"]} = 'L0' THEN 1
                    WHEN {ctx["peer_cohort_level_expr"]} = 'L1' THEN 2
                    WHEN {ctx["peer_cohort_level_expr"]} = 'L2' THEN 3
                    ELSE 4
                END AS level_rank,
                CASE
                    WHEN {ctx["peer_scope_expr"]} = 'zip' THEN ({ctx["peer_peer_n_expr"]}) >= {PROVIDER_QUALITY_ZIP_MIN_PEER_N}
                    WHEN {ctx["peer_scope_expr"]} = 'state' THEN ({ctx["peer_peer_n_expr"]}) >= {PROVIDER_QUALITY_MIN_STATE_PEER_N}
                    ELSE ({ctx["peer_peer_n_expr"]}) >= {PROVIDER_QUALITY_NATIONAL_MIN_PEER_N}
                END AS threshold_met
            FROM provider_featured p
            CROSS JOIN benchmark_modes bm
            JOIN {schema}.{peer_target_table} t
              ON t.{ctx["peer_target_year_col"]} = p.year
             AND {ctx["peer_benchmark_mode_expr"]} = bm.benchmark_mode
             AND (
                    ({ctx["peer_scope_expr"]} = 'zip' AND bm.benchmark_mode = 'zip' AND p.resolved_zip5 IS NOT NULL AND {ctx["peer_geo_value_expr"]} = p.resolved_zip5)
                    OR (
                        {ctx["peer_scope_expr"]} = 'state'
                        AND bm.benchmark_mode IN ('zip', 'state')
                        AND p.resolved_state_key IS NOT NULL
                        AND UPPER({ctx["peer_geo_value_expr"]}) = p.resolved_state_key
                    )
                    OR ({ctx["peer_scope_expr"]} = 'national' AND bm.benchmark_mode IN ('zip', 'state', 'national'))
                )
             AND (
                    ({ctx["peer_cohort_level_expr"]} = 'L0' AND {ctx["peer_specialty_match"]} AND {ctx["peer_taxonomy_match"]} AND {ctx["peer_procedure_bucket_match"]})
                    OR ({ctx["peer_cohort_level_expr"]} = 'L1' AND {ctx["peer_specialty_match"]} AND {ctx["peer_taxonomy_match"]})
                    OR ({ctx["peer_cohort_level_expr"]} = 'L2' AND {ctx["peer_specialty_match"]})
                    OR ({ctx["peer_cohort_level_expr"]} = 'L3')
                )
        ),
        target_selected AS (
            SELECT DISTINCT ON (c.npi, c.year, c.benchmark_mode)
                c.npi,
                c.year,
                c.benchmark_mode,
                c.selected_geography_scope,
                c.selected_geography_value,
                c.selected_cohort_level,
                c.selected_specialty,
                c.selected_taxonomy,
                c.selected_classification,
                c.selected_procedure_bucket,
                c.selected_peer_n,
                c.target_appropriateness,
                c.target_cost,
                c.target_effectiveness,
                c.target_qpp_cost,
                c.target_rx_appropriateness
            FROM target_candidates c
            WHERE c.geography_rank < 9
            ORDER BY
                c.npi,
                c.year,
                c.benchmark_mode,
                CASE WHEN c.threshold_met THEN 0 ELSE 1 END,
                c.geography_rank,
                c.level_rank,
                c.selected_peer_n DESC,
                c.selected_geography_scope,
                c.selected_geography_value,
                c.selected_cohort_level
        ),
        measure_inputs AS (
            SELECT
                p.npi,
                p.year,
                ts.benchmark_mode,
                p.total_services,
                p.total_beneficiaries,
                p.utilization_adjusted,
                p.cost_adjusted,
                p.qpp_quality_score,
                p.qpp_cost_score,
                p.rx_claim_rate,
                ts.target_appropriateness,
                ts.target_cost,
                ts.target_effectiveness,
                ts.target_qpp_cost,
                ts.target_rx_appropriateness,
                ts.selected_cohort_level,
                ts.selected_geography_scope,
                ts.selected_geography_value,
                ts.selected_specialty,
                ts.selected_taxonomy,
                ts.selected_classification,
                ts.selected_procedure_bucket,
                ts.selected_peer_n,
                (
                    'cohort:'
                    || ts.selected_geography_scope
                    || ':'
                    || COALESCE(ts.selected_geography_value, 'US')
                    || ':'
                    || ts.selected_cohort_level
                )::varchar AS peer_group,
                GREATEST(COALESCE(p.total_services, 0.0), COALESCE(p.total_beneficiaries, 0.0), 1.0) AS evidence_n_claims,
                GREATEST(COALESCE(p.total_rx_claims, 0.0), COALESCE(p.total_rx_beneficiaries, 0.0), 1.0) AS evidence_n_rx
            FROM provider_featured p
            JOIN target_selected ts
              ON ts.npi = p.npi
             AND ts.year = p.year
        ),
        expanded AS (
            SELECT
                npi,
                year,
                benchmark_mode,
                'appropriateness_utilization'::varchar AS measure_id,
                'appropriateness'::varchar AS domain,
                utilization_adjusted::float8 AS observed_value,
                target_appropriateness::float8 AS target_value,
                CASE
                    WHEN target_appropriateness IS NULL OR target_appropriateness <= 0 THEN 1.0
                    WHEN utilization_adjusted IS NULL OR utilization_adjusted <= 0 THEN 1.0
                    ELSE utilization_adjusted / target_appropriateness
                END AS rr_raw,
                evidence_n_claims AS evidence_n,
                peer_group,
                'higher_worse'::varchar AS direction,
                'claims_pricing'::varchar AS source,
                selected_cohort_level,
                selected_geography_scope,
                selected_geography_value,
                selected_specialty,
                selected_taxonomy,
                selected_classification,
                selected_procedure_bucket,
                selected_peer_n
            FROM measure_inputs
            UNION ALL
            SELECT
                npi,
                year,
                benchmark_mode,
                'appropriateness_drug_proxy'::varchar AS measure_id,
                'appropriateness'::varchar AS domain,
                rx_claim_rate::float8 AS observed_value,
                target_rx_appropriateness::float8 AS target_value,
                CASE
                    WHEN target_rx_appropriateness IS NULL OR target_rx_appropriateness <= 0 THEN 1.0
                    WHEN rx_claim_rate IS NULL OR rx_claim_rate <= 0 THEN 1.0
                    ELSE rx_claim_rate / target_rx_appropriateness
                END AS rr_raw,
                evidence_n_rx AS evidence_n,
                peer_group,
                'higher_worse'::varchar AS direction,
                'drug_claims'::varchar AS source,
                selected_cohort_level,
                selected_geography_scope,
                selected_geography_value,
                selected_specialty,
                selected_taxonomy,
                selected_classification,
                selected_procedure_bucket,
                selected_peer_n
            FROM measure_inputs
            UNION ALL
            SELECT
                npi,
                year,
                benchmark_mode,
                'effectiveness_qpp_quality'::varchar AS measure_id,
                'effectiveness'::varchar AS domain,
                qpp_quality_score::float8 AS observed_value,
                target_effectiveness::float8 AS target_value,
                CASE
                    WHEN qpp_quality_score IS NULL OR qpp_quality_score <= 0 THEN 1.0
                    WHEN target_effectiveness IS NULL OR target_effectiveness <= 0 THEN 1.0
                    ELSE target_effectiveness / qpp_quality_score
                END AS rr_raw,
                evidence_n_claims AS evidence_n,
                peer_group,
                'lower_worse'::varchar AS direction,
                'qpp'::varchar AS source,
                selected_cohort_level,
                selected_geography_scope,
                selected_geography_value,
                selected_specialty,
                selected_taxonomy,
                selected_classification,
                selected_procedure_bucket,
                selected_peer_n
            FROM measure_inputs
            UNION ALL
            SELECT
                npi,
                year,
                benchmark_mode,
                'cost_qpp_component'::varchar AS measure_id,
                'cost'::varchar AS domain,
                qpp_cost_score::float8 AS observed_value,
                target_qpp_cost::float8 AS target_value,
                CASE
                    WHEN qpp_cost_score IS NULL OR qpp_cost_score <= 0 THEN 1.0
                    WHEN target_qpp_cost IS NULL OR target_qpp_cost <= 0 THEN 1.0
                    ELSE target_qpp_cost / qpp_cost_score
                END AS rr_raw,
                evidence_n_claims AS evidence_n,
                peer_group,
                'lower_worse'::varchar AS direction,
                'qpp'::varchar AS source,
                selected_cohort_level,
                selected_geography_scope,
                selected_geography_value,
                selected_specialty,
                selected_taxonomy,
                selected_classification,
                selected_procedure_bucket,
                selected_peer_n
            FROM measure_inputs
            UNION ALL
            SELECT
                npi,
                year,
                benchmark_mode,
                'cost_allowed_amount'::varchar AS measure_id,
                'cost'::varchar AS domain,
                cost_adjusted::float8 AS observed_value,
                target_cost::float8 AS target_value,
                CASE
                    WHEN target_cost IS NULL OR target_cost <= 0 THEN 1.0
                    WHEN cost_adjusted IS NULL OR cost_adjusted <= 0 THEN 1.0
                    ELSE cost_adjusted / target_cost
                END AS rr_raw,
                evidence_n_claims AS evidence_n,
                peer_group,
                'higher_worse'::varchar AS direction,
                'claims_pricing'::varchar AS source,
                selected_cohort_level,
                selected_geography_scope,
                selected_geography_value,
                selected_specialty,
                selected_taxonomy,
                selected_classification,
                selected_procedure_bucket,
                selected_peer_n
            FROM measure_inputs
        ),
        shrunk AS (
            SELECT
                e.*,
                LEAST(1.0, GREATEST(0.0, e.evidence_n / NULLIF(e.evidence_n + {PROVIDER_QUALITY_SHRINKAGE_ALPHA}, 0.0))) AS shrink_weight,
                GREATEST(0.0001, SQRT(1.0 / NULLIF(e.evidence_n + 1.0, 0.0))) AS stderr
            FROM expanded e
        )
        INSERT INTO {schema}.{ctx["measure_table"]}
        (
            {ctx["measure_insert_cols_sql"]}
        )
        SELECT
            {ctx["measure_select_cols_sql"]}
        FROM shrunk s;
    """


def _cohort_sql_phase_6_build_domain_shard(ctx: dict[str, Any]) -> str:
    schema = ctx["schema"]
    return f"""
        WITH deleted AS (
            DELETE FROM {schema}.{ctx["domain_table"]} d
            WHERE d.year = :year
              AND {_npi_shard_predicate("d.npi")}
            RETURNING 1
        )
        INSERT INTO {schema}.{ctx["domain_table"]}
        (
            npi,
            year,
            benchmark_mode,
            domain,
            risk_ratio,
            score_0_100,
            ci75_low,
            ci75_high,
            ci90_low,
            ci90_high,
            evidence_n,
            measure_count,
            updated_at
        )
        SELECT
            m.npi,
            m.year,
            m.benchmark_mode,
            m.domain,
            EXP(AVG(LN(GREATEST(m.risk_ratio, 0.0001))))::float8 AS risk_ratio,
            ROUND((LEAST(100.0, GREATEST(0.0, 50.0 - 125.0 * (EXP(AVG(LN(GREATEST(m.risk_ratio, 0.0001)))) - 1.0))))::numeric, 2)::float8 AS score_0_100,
            EXP(AVG(LN(GREATEST(m.ci75_low, 0.0001))))::float8 AS ci75_low,
            EXP(AVG(LN(GREATEST(m.ci75_high, 0.0001))))::float8 AS ci75_high,
            EXP(AVG(LN(GREATEST(m.ci90_low, 0.0001))))::float8 AS ci90_low,
            EXP(AVG(LN(GREATEST(m.ci90_high, 0.0001))))::float8 AS ci90_high,
            SUM(COALESCE(m.evidence_n, 0))::int AS evidence_n,
            COUNT(*)::int AS measure_count,
            NOW() AS updated_at
        FROM {schema}.{ctx["measure_table"]} m
        WHERE m.year = :year
          AND {_npi_shard_predicate("m.npi")}
        GROUP BY m.npi, m.year, m.benchmark_mode, m.domain;
    """


def _cohort_sql_phase_7_build_score_shard(ctx: dict[str, Any]) -> str:
    """Build phase-seven score shard SQL."""
    schema = ctx["schema"]
    cohort_meta_cte = ""
    cohort_meta_join_scored = ""
    cohort_meta_join_final = ""
    if ctx["score_optional_pairs"] and ctx["measure_meta_sources"]:
        cohort_meta_cte = (
            ",\n        cohort_meta AS (\n"
            "            SELECT\n"
            "                m.npi,\n"
            "                m.year,\n"
            "                m.benchmark_mode,\n"
            + ",\n".join(
                f"                MAX(m.{column_name}) AS {alias}"
                for alias, column_name in ctx["measure_meta_sources"].items()
            )
            + f"\n            FROM {schema}.{ctx['measure_table']} m\n"
            "            WHERE m.year = :year\n"
            f"              AND {_npi_shard_predicate('m.npi')}\n"
            "            GROUP BY m.npi, m.year, m.benchmark_mode\n"
            "        )\n"
        )
        cohort_meta_join_scored = (
            "\n        LEFT JOIN cohort_meta cm\n"
            "          ON cm.npi = b.npi\n"
            "         AND cm.year = b.year\n"
            "         AND cm.benchmark_mode = b.benchmark_mode"
        )
        cohort_meta_join_final = (
            "\n        LEFT JOIN cohort_meta cm\n"
            "          ON cm.npi = c.npi\n"
            "         AND cm.year = c.year\n"
            "         AND cm.benchmark_mode = c.benchmark_mode"
        )
    return f"""
        WITH deleted AS (
            DELETE FROM {schema}.{ctx["score_table"]} d
            WHERE d.year = :year
              AND {_npi_shard_predicate("d.npi")}
            RETURNING 1
        ),
        domain_pivot AS (
            SELECT
                d.npi,
                d.year,
                d.benchmark_mode,
                MAX(CASE WHEN d.domain = 'appropriateness' THEN d.risk_ratio END) AS rr_appropr,
                MAX(CASE WHEN d.domain = 'effectiveness' THEN d.risk_ratio END) AS rr_effect,
                MAX(CASE WHEN d.domain = 'cost' THEN d.risk_ratio END) AS rr_cost,
                MAX(CASE WHEN d.domain = 'appropriateness' THEN d.ci75_low END) AS ci75_appropr_low,
                MAX(CASE WHEN d.domain = 'effectiveness' THEN d.ci75_low END) AS ci75_effect_low,
                MAX(CASE WHEN d.domain = 'cost' THEN d.ci75_low END) AS ci75_cost_low,
                MAX(CASE WHEN d.domain = 'appropriateness' THEN d.ci75_high END) AS ci75_appropr_high,
                MAX(CASE WHEN d.domain = 'effectiveness' THEN d.ci75_high END) AS ci75_effect_high,
                MAX(CASE WHEN d.domain = 'cost' THEN d.ci75_high END) AS ci75_cost_high,
                MAX(CASE WHEN d.domain = 'appropriateness' THEN d.ci90_low END) AS ci90_appropr_low,
                MAX(CASE WHEN d.domain = 'effectiveness' THEN d.ci90_low END) AS ci90_effect_low,
                MAX(CASE WHEN d.domain = 'cost' THEN d.ci90_low END) AS ci90_cost_low,
                MAX(CASE WHEN d.domain = 'appropriateness' THEN d.ci90_high END) AS ci90_appropr_high,
                MAX(CASE WHEN d.domain = 'effectiveness' THEN d.ci90_high END) AS ci90_effect_high,
                MAX(CASE WHEN d.domain = 'cost' THEN d.ci90_high END) AS ci90_cost_high
            FROM {schema}.{ctx["domain_table"]} d
            WHERE d.year = :year
              AND {_npi_shard_predicate("d.npi")}
            GROUP BY d.npi, d.year, d.benchmark_mode
        ){cohort_meta_cte},
        blended AS (
            SELECT
                p.npi,
                p.year,
                p.benchmark_mode,
                COALESCE(p.rr_appropr, 1.0) AS rr_appropr,
                COALESCE(p.rr_effect, 1.0) AS rr_effect,
                COALESCE(p.rr_cost, 1.0) AS rr_cost,
                SQRT(COALESCE(p.rr_appropr, 1.0) * COALESCE(p.rr_effect, 1.0)) AS rr_clinical,
                EXP(
                    0.5 * LN(GREATEST(SQRT(COALESCE(p.rr_appropr, 1.0) * COALESCE(p.rr_effect, 1.0)), 0.0001))
                    + 0.5 * LN(GREATEST(COALESCE(p.rr_cost, 1.0), 0.0001))
                ) AS rr_overall,
                EXP(
                    0.5 * LN(GREATEST(SQRT(COALESCE(p.ci75_appropr_low, 1.0) * COALESCE(p.ci75_effect_low, 1.0)), 0.0001))
                    + 0.5 * LN(GREATEST(COALESCE(p.ci75_cost_low, 1.0), 0.0001))
                ) AS ci75_low,
                EXP(
                    0.5 * LN(GREATEST(SQRT(COALESCE(p.ci75_appropr_high, 1.0) * COALESCE(p.ci75_effect_high, 1.0)), 0.0001))
                    + 0.5 * LN(GREATEST(COALESCE(p.ci75_cost_high, 1.0), 0.0001))
                ) AS ci75_high,
                EXP(
                    0.5 * LN(GREATEST(SQRT(COALESCE(p.ci90_appropr_low, 1.0) * COALESCE(p.ci90_effect_low, 1.0)), 0.0001))
                    + 0.5 * LN(GREATEST(COALESCE(p.ci90_cost_low, 1.0), 0.0001))
                ) AS ci90_low,
                EXP(
                    0.5 * LN(GREATEST(SQRT(COALESCE(p.ci90_appropr_high, 1.0) * COALESCE(p.ci90_effect_high, 1.0)), 0.0001))
                    + 0.5 * LN(GREATEST(COALESCE(p.ci90_cost_high, 1.0), 0.0001))
                ) AS ci90_high
            FROM domain_pivot p
        ),
        score_meta AS (
            SELECT
                m.npi,
                m.year,
                m.benchmark_mode,
                BOOL_OR(m.measure_id = 'appropriateness_utilization' AND COALESCE(m.observed, 0.0) > 0)::boolean AS has_claims,
                BOOL_OR(m.measure_id = 'effectiveness_qpp_quality' AND COALESCE(m.observed, 0.0) > 0)::boolean AS has_qpp,
                BOOL_OR(m.measure_id = 'appropriateness_drug_proxy' AND COALESCE(m.observed, 0.0) > 0)::boolean AS has_rx
            FROM {schema}.{ctx["measure_table"]} m
            WHERE m.year = :year
              AND {_npi_shard_predicate("m.npi")}
            GROUP BY m.npi, m.year, m.benchmark_mode
        ),
        feature_meta AS (
            SELECT
                f.npi,
                f.year,
                COALESCE(NULLIF(LOWER(BTRIM(COALESCE(f.provider_class, ''))), ''), 'unknown')::varchar AS provider_class,
                COALESCE(NULLIF(BTRIM(COALESCE(f.location_source, '')), ''), 'claims_pricing')::varchar AS location_source,
                COALESCE(f.has_enrollment, FALSE)::boolean AS has_enrollment,
                COALESCE(f.has_medicare_claims, FALSE)::boolean AS has_medicare_claims
            FROM {schema}.{ctx["feature_table"]} f
            WHERE f.year = :year
              AND {_npi_shard_predicate("f.npi")}
        ),
        scored AS (
            SELECT
                b.*,
                COALESCE(sm.has_claims, FALSE)::boolean AS has_claims,
                COALESCE(sm.has_qpp, FALSE)::boolean AS has_qpp,
                COALESCE(sm.has_rx, FALSE)::boolean AS has_rx,
                COALESCE(fm.provider_class, 'unknown')::varchar AS provider_class,
                COALESCE(fm.location_source, 'claims_pricing')::varchar AS location_source,
                COALESCE(fm.has_enrollment, FALSE)::boolean AS has_enrollment,
                COALESCE(fm.has_medicare_claims, FALSE)::boolean AS has_medicare_claims,
                COALESCE(cm.selected_peer_n, 0)::float8 AS selected_peer_n,
                (b.rr_overall >= 1.12)::boolean AS low_score_check,
                (b.ci90_low >= 1.08)::boolean AS low_confidence_check,
                (b.rr_overall <= 0.88)::boolean AS high_score_check,
                (b.ci75_high < 1.0)::boolean AS high_confidence_check,
                LEAST(
                    100.0,
                    (
                        CASE WHEN COALESCE(sm.has_claims, FALSE) THEN 40.0 ELSE 0.0 END
                        + CASE WHEN COALESCE(sm.has_qpp, FALSE) THEN 25.0 ELSE 0.0 END
                        + CASE WHEN COALESCE(sm.has_rx, FALSE) THEN 15.0 ELSE 0.0 END
                        + CASE WHEN COALESCE(fm.has_enrollment, FALSE) THEN 10.0 ELSE 0.0 END
                        + CASE
                            WHEN COALESCE(fm.location_source, 'claims_pricing') NOT IN ('claims_pricing', 'unknown', '')
                            THEN 10.0
                            ELSE 5.0
                          END
                    )
                )::float8 AS data_coverage_0_100,
                LEAST(
                    100.0,
                    (
                        CASE WHEN COALESCE(sm.has_claims, FALSE) THEN 35.0 ELSE 0.0 END
                        + CASE WHEN COALESCE(sm.has_qpp, FALSE) THEN 20.0 ELSE 0.0 END
                        + CASE WHEN COALESCE(sm.has_rx, FALSE) THEN 10.0 ELSE 0.0 END
                        + CASE WHEN COALESCE(fm.has_enrollment, FALSE) THEN 10.0 ELSE 0.0 END
                        + CASE WHEN COALESCE(fm.has_medicare_claims, FALSE) THEN 5.0 ELSE 0.0 END
                        + CASE
                            WHEN COALESCE(cm.selected_peer_n, 0) >= 100 THEN 10.0
                            WHEN COALESCE(cm.selected_peer_n, 0) >= 30 THEN 5.0
                            ELSE 0.0
                          END
                        + CASE
                            WHEN COALESCE(fm.location_source, 'claims_pricing') NOT IN ('claims_pricing', 'unknown', '')
                            THEN 10.0
                            ELSE 0.0
                          END
                    )
                )::float8 AS confidence_0_100
            FROM blended b
            LEFT JOIN score_meta sm
              ON sm.npi = b.npi
             AND sm.year = b.year
             AND sm.benchmark_mode = b.benchmark_mode
            LEFT JOIN feature_meta fm
              ON fm.npi = b.npi
             AND fm.year = b.year{cohort_meta_join_scored}
        ),
        finalized AS (
            SELECT
                s.*,
                CASE
                    WHEN s.has_claims AND s.has_qpp THEN 'direct'
                    WHEN s.has_claims OR s.has_qpp OR s.has_rx THEN 'mixed'
                    ELSE 'unavailable'
                END::varchar AS score_method,
                CASE
                    WHEN s.has_claims THEN 'direct'
                    WHEN COALESCE(s.selected_peer_n, 0) > 0 THEN 'peer_estimated'
                    ELSE 'unavailable'
                END::varchar AS cost_source,
                CASE
                    WHEN s.confidence_0_100 >= 80 THEN 'high'
                    WHEN s.confidence_0_100 >= 55 THEN 'medium'
                    WHEN s.confidence_0_100 > 0 THEN 'low'
                    ELSE 'none'
                END::varchar AS confidence_band,
                CASE
                    WHEN s.confidence_0_100 < 55
                     AND (
                            (s.low_score_check AND s.low_confidence_check)
                         OR (s.high_score_check AND s.high_confidence_check)
                         )
                    THEN 'acceptable'
                    WHEN s.low_score_check AND s.low_confidence_check THEN 'low'
                    WHEN s.high_score_check AND s.high_confidence_check THEN 'high'
                    ELSE 'acceptable'
                END::varchar AS final_tier
            FROM scored s
        )
        INSERT INTO {schema}.{ctx["score_table"]}
        (
            {ctx["score_insert_cols_sql"]}
        )
        SELECT
            {ctx["score_select_cols_sql"]}
        FROM finalized c{cohort_meta_join_final};
    """

