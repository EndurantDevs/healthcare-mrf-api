# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Cohort materialization context assembly for provider-quality imports."""

from __future__ import annotations

from typing import Any, Awaitable, Callable

from db.models import (
    DoctorClinicianAddress,
    EntityAddressUnified,
    NPIAddress,
    NPIData,
    NPIDataTaxonomy,
    NUCCTaxonomy,
    PricingProviderPrescription,
    ProviderEnrichmentSummary,
)
from process.provider_quality_parts.config import (
    PROVIDER_QUALITY_BENCHMARK_MODE,
    PROVIDER_QUALITY_MAX_YEAR,
    PROVIDER_QUALITY_MIN_YEAR,
    PROVIDER_QUALITY_MODEL_VERSION,
    _benchmark_modes_for_materialization,
)
from process.provider_quality_parts.model_helpers import (
    _first_existing_column,
    _model_columns,
    _optional_column_pairs,
)
from process.provider_quality_parts.table_helpers import _table_columns, _table_exists


async def _build_cohort_materialization_context(
    classes: dict[str, type],
    schema: str,
    *,
    table_exists: Callable[[str, str], Awaitable[bool]] = _table_exists,
    table_columns: Callable[[str, str], Awaitable[set[str]]] = _table_columns,
) -> dict[str, Any]:
    qpp_table = classes["PricingQppProvider"].__tablename__
    svi_table = classes["PricingSviZcta"].__tablename__
    measure_table = classes["PricingProviderQualityMeasure"].__tablename__
    domain_table = classes["PricingProviderQualityDomain"].__tablename__
    score_table = classes["PricingProviderQualityScore"].__tablename__
    feature_table = classes["PricingProviderQualityFeature"].__tablename__
    lsh_table = classes["PricingProviderQualityProcedureLSH"].__tablename__
    peer_target_table = classes["PricingProviderQualityPeerTarget"].__tablename__
    rx_table = PricingProviderPrescription.__tablename__
    rx_table_exists = await table_exists(schema, rx_table)
    npi_table_exists = await table_exists(schema, NPIData.__tablename__)
    taxonomy_table_exists = await table_exists(schema, NPIDataTaxonomy.__tablename__)
    nucc_table_exists = await table_exists(schema, NUCCTaxonomy.__tablename__)
    provider_enrichment_exists = await table_exists(schema, ProviderEnrichmentSummary.__tablename__)
    doctor_clinician_exists = await table_exists(schema, DoctorClinicianAddress.__tablename__)
    unified_address_exists = await table_exists(schema, EntityAddressUnified.__tablename__)
    npi_address_exists = await table_exists(schema, NPIAddress.__tablename__)
    unified_address_columns = (
        await table_columns(schema, EntityAddressUnified.__tablename__)
        if unified_address_exists
        else set()
    )
    nucc_columns = (
        await table_columns(schema, NUCCTaxonomy.__tablename__)
        if nucc_table_exists
        else set()
    )

    feature_columns = _model_columns(classes.get("PricingProviderQualityFeature"))
    lsh_columns = _model_columns(classes.get("PricingProviderQualityProcedureLSH"))
    peer_target_columns = _model_columns(classes.get("PricingProviderQualityPeerTarget"))
    measure_columns = _model_columns(classes.get("PricingProviderQualityMeasure"))
    score_columns = _model_columns(classes.get("PricingProviderQualityScore"))

    feature_npi_col = _first_existing_column(feature_columns, "npi")
    feature_year_col = _first_existing_column(feature_columns, "year")
    if not feature_npi_col or not feature_year_col:
        raise RuntimeError("cohort feature model must expose npi/year columns")

    lsh_npi_col = _first_existing_column(lsh_columns, "npi")
    lsh_year_col = _first_existing_column(lsh_columns, "year")
    lsh_band_no_col = _first_existing_column(lsh_columns, "band_no", "band_index")
    lsh_band_hash_col = _first_existing_column(lsh_columns, "band_hash", "hash_value")
    if not lsh_npi_col or not lsh_year_col or not lsh_band_no_col or not lsh_band_hash_col:
        raise RuntimeError("cohort LSH model must expose npi/year/band_no/band_hash columns")

    peer_target_year_col = _first_existing_column(peer_target_columns, "year")
    peer_target_benchmark_mode_col = _first_existing_column(peer_target_columns, "benchmark_mode")
    peer_target_geo_scope_col = _first_existing_column(peer_target_columns, "geography_scope", "geo_scope")
    peer_target_geo_value_col = _first_existing_column(peer_target_columns, "geography_value", "geo_value")
    peer_target_cohort_level_col = _first_existing_column(peer_target_columns, "cohort_level", "cohort_tier")
    peer_target_peer_n_col = _first_existing_column(peer_target_columns, "peer_n", "peer_count", "provider_count")
    peer_target_specialty_col = _first_existing_column(peer_target_columns, "specialty", "specialty_key")
    peer_target_taxonomy_col = _first_existing_column(peer_target_columns, "taxonomy", "taxonomy_code")
    peer_target_classification_col = _first_existing_column(peer_target_columns, "classification")
    peer_target_procedure_bucket_col = _first_existing_column(peer_target_columns, "procedure_bucket")
    peer_target_appropr_col = _first_existing_column(peer_target_columns, "target_appropriateness")
    peer_target_cost_col = _first_existing_column(peer_target_columns, "target_cost")
    peer_target_effect_col = _first_existing_column(peer_target_columns, "target_effectiveness")
    peer_target_qpp_cost_col = _first_existing_column(peer_target_columns, "target_qpp_cost")
    peer_target_rx_appropr_col = _first_existing_column(peer_target_columns, "target_rx_appropriateness")
    if (
        not peer_target_year_col
        or not peer_target_geo_scope_col
        or not peer_target_geo_value_col
        or not peer_target_cohort_level_col
        or not peer_target_peer_n_col
        or not peer_target_appropr_col
        or not peer_target_cost_col
        or not peer_target_effect_col
        or not peer_target_qpp_cost_col
        or not peer_target_rx_appropr_col
    ):
        raise RuntimeError("cohort peer-target model is missing required columns")

    feature_insert_pairs = _optional_column_pairs(
        feature_columns,
        (
            ("npi", "f.npi"),
            ("year", "f.year"),
            ("zip5", "f.zip5"),
            ("state", "f.state"),
            ("provider_class", "f.provider_class"),
            ("location_source", "f.location_source"),
            ("has_enrollment", "f.has_enrollment"),
            ("has_medicare_claims", "f.has_medicare_claims"),
            ("specialty", "f.specialty"),
            ("specialty_key", "f.specialty"),
            ("taxonomy", "f.taxonomy"),
            ("taxonomy_code", "f.taxonomy"),
            ("classification", "f.classification"),
            ("procedure_count", "f.procedure_count"),
            ("procedure_bucket", "f.procedure_bucket"),
            ("source", "f.source"),
            ("updated_at", "f.updated_at"),
        ),
    )
    lsh_insert_pairs = _optional_column_pairs(
        lsh_columns,
        (
            ("npi", "s.npi"),
            ("year", "s.year"),
            ("band_no", "s.band_no"),
            ("band_index", "s.band_no"),
            ("band_hash", "s.band_hash"),
            ("hash_value", "s.band_hash"),
            ("rows_in_band", "s.rows_in_band"),
            ("source", "s.source"),
            ("updated_at", "s.updated_at"),
        ),
    )
    peer_target_insert_pairs = _optional_column_pairs(
        peer_target_columns,
        (
            ("year", "t.year"),
            ("benchmark_mode", "t.benchmark_mode"),
            ("geography_scope", "t.geography_scope"),
            ("geo_scope", "t.geography_scope"),
            ("geography_value", "t.geography_value"),
            ("geo_value", "t.geography_value"),
            ("cohort_level", "t.cohort_level"),
            ("cohort_tier", "t.cohort_level"),
            ("specialty", "t.specialty"),
            ("specialty_key", "t.specialty"),
            ("taxonomy", "t.taxonomy"),
            ("taxonomy_code", "t.taxonomy"),
            ("classification", "t.classification"),
            ("procedure_bucket", "t.procedure_bucket"),
            ("peer_n", "t.peer_n"),
            ("peer_count", "t.peer_n"),
            ("provider_count", "t.peer_n"),
            ("target_appropriateness", "t.target_appropriateness"),
            ("target_cost", "t.target_cost"),
            ("target_effectiveness", "t.target_effectiveness"),
            ("target_qpp_cost", "t.target_qpp_cost"),
            ("target_rx_appropriateness", "t.target_rx_appropriateness"),
            ("source", "t.source"),
            ("updated_at", "t.updated_at"),
        ),
    )
    if not feature_insert_pairs or not lsh_insert_pairs or not peer_target_insert_pairs:
        raise RuntimeError("cohort models do not expose insertable columns")

    feature_insert_cols_sql = ",\n            ".join(column for column, _ in feature_insert_pairs)
    feature_select_cols_sql = ",\n            ".join(expr for _, expr in feature_insert_pairs)
    lsh_insert_cols_sql = ",\n            ".join(column for column, _ in lsh_insert_pairs)
    lsh_select_cols_sql = ",\n            ".join(expr for _, expr in lsh_insert_pairs)
    peer_target_insert_cols_sql = ",\n            ".join(column for column, _ in peer_target_insert_pairs)
    peer_target_select_cols_sql = ",\n            ".join(expr for _, expr in peer_target_insert_pairs)

    feature_specialty_col = _first_existing_column(feature_columns, "specialty", "specialty_key")
    feature_taxonomy_col = _first_existing_column(feature_columns, "taxonomy", "taxonomy_code")
    feature_classification_col = _first_existing_column(feature_columns, "classification")
    feature_procedure_bucket_col = _first_existing_column(feature_columns, "procedure_bucket")
    feature_state_col = _first_existing_column(feature_columns, "state")
    feature_zip_col = _first_existing_column(feature_columns, "zip5")
    feature_provider_class_col = _first_existing_column(feature_columns, "provider_class")
    feature_location_source_col = _first_existing_column(feature_columns, "location_source")
    feature_has_enrollment_col = _first_existing_column(feature_columns, "has_enrollment")
    feature_has_medicare_claims_col = _first_existing_column(feature_columns, "has_medicare_claims")

    feature_specialty_expr = (
        f"COALESCE(NULLIF(LOWER(BTRIM(COALESCE(f.{feature_specialty_col}, ''))), ''), 'unknown')"
        if feature_specialty_col
        else "COALESCE(NULLIF(LOWER(BTRIM(COALESCE(p.provider_type, ''))), ''), 'unknown')"
    )
    feature_taxonomy_expr = (
        f"COALESCE(NULLIF(LOWER(BTRIM(COALESCE(f.{feature_taxonomy_col}, ''))), ''), 'unknown')"
        if feature_taxonomy_col
        else "'unknown'::varchar"
    )
    feature_classification_expr = (
        f"COALESCE(NULLIF(LOWER(BTRIM(COALESCE(f.{feature_classification_col}, ''))), ''), 'unknown')"
        if feature_classification_col
        else "'unknown'::varchar"
    )
    feature_procedure_bucket_expr = (
        f"COALESCE(NULLIF(BTRIM(COALESCE(f.{feature_procedure_bucket_col}, '')), ''), 'bucket:none')"
        if feature_procedure_bucket_col
        else "'bucket:none'::varchar"
    )
    feature_state_expr = (
        f"COALESCE(NULLIF(UPPER(BTRIM(COALESCE(f.{feature_state_col}, ''))), ''), p.state_key)"
        if feature_state_col
        else "p.state_key"
    )
    feature_zip_expr = (
        f"COALESCE(NULLIF(BTRIM(COALESCE(f.{feature_zip_col}, '')), ''), p.zip5)"
        if feature_zip_col
        else "p.zip5"
    )
    feature_provider_class_expr = (
        f"COALESCE(NULLIF(LOWER(BTRIM(COALESCE(f.{feature_provider_class_col}, ''))), ''), 'unknown')"
        if feature_provider_class_col
        else "'unknown'::varchar"
    )
    feature_location_source_expr = (
        f"COALESCE(NULLIF(BTRIM(COALESCE(f.{feature_location_source_col}, '')), ''), 'claims_pricing')"
        if feature_location_source_col
        else "'claims_pricing'::varchar"
    )
    feature_has_enrollment_expr = (
        f"COALESCE(f.{feature_has_enrollment_col}, FALSE)"
        if feature_has_enrollment_col
        else "FALSE"
    )
    feature_has_medicare_claims_expr = (
        f"COALESCE(f.{feature_has_medicare_claims_col}, FALSE)"
        if feature_has_medicare_claims_col
        else "FALSE"
    )

    peer_scope_expr = f"t.{peer_target_geo_scope_col}"
    peer_benchmark_mode_expr = (
        f"LOWER(COALESCE(t.{peer_target_benchmark_mode_col}, 'national'))"
        if peer_target_benchmark_mode_col
        else "bm.benchmark_mode"
    )
    peer_geo_value_expr = f"t.{peer_target_geo_value_col}"
    peer_cohort_level_expr = f"t.{peer_target_cohort_level_col}"
    peer_peer_n_expr = f"t.{peer_target_peer_n_col}"
    peer_specialty_expr = (
        f"t.{peer_target_specialty_col}"
        if peer_target_specialty_col
        else "'unknown'::varchar"
    )
    peer_taxonomy_expr = (
        f"t.{peer_target_taxonomy_col}"
        if peer_target_taxonomy_col
        else "'unknown'::varchar"
    )
    peer_classification_expr = (
        f"t.{peer_target_classification_col}"
        if peer_target_classification_col
        else "'unknown'::varchar"
    )
    peer_procedure_bucket_expr = (
        f"t.{peer_target_procedure_bucket_col}"
        if peer_target_procedure_bucket_col
        else "'bucket:none'::varchar"
    )
    peer_specialty_match = f"COALESCE({peer_specialty_expr}, 'unknown') = COALESCE(p.specialty, 'unknown')"
    peer_taxonomy_match = f"COALESCE({peer_taxonomy_expr}, 'unknown') = COALESCE(p.taxonomy, 'unknown')"
    peer_procedure_bucket_match = f"COALESCE({peer_procedure_bucket_expr}, 'bucket:none') = COALESCE(p.procedure_bucket, 'bucket:none')"

    measure_optional_pairs = _optional_column_pairs(
        measure_columns,
        (
            ("cohort_level", "s.selected_cohort_level"),
            ("cohort_tier", "s.selected_cohort_level"),
            ("cohort_geography_scope", "s.selected_geography_scope"),
            ("cohort_geo_scope", "s.selected_geography_scope"),
            ("cohort_geography_value", "s.selected_geography_value"),
            ("cohort_geo_value", "s.selected_geography_value"),
            ("cohort_specialty", "s.selected_specialty"),
            ("cohort_specialty_key", "s.selected_specialty"),
            ("cohort_taxonomy", "s.selected_taxonomy"),
            ("cohort_taxonomy_code", "s.selected_taxonomy"),
            ("cohort_classification", "s.selected_classification"),
            ("cohort_procedure_bucket", "s.selected_procedure_bucket"),
            ("cohort_peer_n", "ROUND(s.selected_peer_n)::int"),
        ),
    )
    measure_insert_columns = [
        "npi",
        "year",
        "benchmark_mode",
        "measure_id",
        "domain",
        "observed",
        "target",
        "risk_ratio",
        "ci75_low",
        "ci75_high",
        "ci90_low",
        "ci90_high",
        "evidence_n",
        "peer_group",
        "direction",
        "source",
        "updated_at",
    ]
    measure_select_columns = [
        "s.npi",
        "s.year",
        "s.benchmark_mode",
        "s.measure_id",
        "s.domain",
        "s.observed_value",
        "s.target_value",
        "(1.0 + (s.rr_raw - 1.0) * s.shrink_weight)::float8 AS risk_ratio",
        "GREATEST(0.01, (1.0 + (s.rr_raw - 1.0) * s.shrink_weight) - 1.15 * s.stderr)::float8 AS ci75_low",
        "((1.0 + (s.rr_raw - 1.0) * s.shrink_weight) + 1.15 * s.stderr)::float8 AS ci75_high",
        "GREATEST(0.01, (1.0 + (s.rr_raw - 1.0) * s.shrink_weight) - 1.64 * s.stderr)::float8 AS ci90_low",
        "((1.0 + (s.rr_raw - 1.0) * s.shrink_weight) + 1.64 * s.stderr)::float8 AS ci90_high",
        "ROUND(s.evidence_n)::int",
        "s.peer_group",
        "s.direction",
        "s.source",
        "NOW() AS updated_at",
    ]
    measure_insert_columns.extend(column for column, _ in measure_optional_pairs)
    measure_select_columns.extend(expr for _, expr in measure_optional_pairs)
    measure_insert_cols_sql = ",\n            ".join(measure_insert_columns)
    measure_select_cols_sql = ",\n            ".join(measure_select_columns)

    measure_meta_sources = {
        "selected_cohort_level": _first_existing_column(measure_columns, "cohort_level", "cohort_tier"),
        "selected_geography_scope": _first_existing_column(measure_columns, "cohort_geography_scope", "cohort_geo_scope"),
        "selected_geography_value": _first_existing_column(measure_columns, "cohort_geography_value", "cohort_geo_value"),
        "selected_specialty": _first_existing_column(measure_columns, "cohort_specialty", "cohort_specialty_key"),
        "selected_taxonomy": _first_existing_column(measure_columns, "cohort_taxonomy", "cohort_taxonomy_code"),
        "selected_classification": _first_existing_column(measure_columns, "cohort_classification"),
        "selected_procedure_bucket": _first_existing_column(measure_columns, "cohort_procedure_bucket"),
        "selected_peer_n": _first_existing_column(measure_columns, "cohort_peer_n"),
    }
    measure_meta_sources = {alias: column for alias, column in measure_meta_sources.items() if column}

    score_optional_mapping: list[tuple[str, str]] = []
    if "selected_cohort_level" in measure_meta_sources:
        score_optional_mapping.extend(
            (
                ("cohort_level", "cm.selected_cohort_level"),
                ("cohort_tier", "cm.selected_cohort_level"),
            )
        )
    if "selected_geography_scope" in measure_meta_sources:
        score_optional_mapping.extend(
            (
                ("cohort_geography_scope", "cm.selected_geography_scope"),
                ("cohort_geo_scope", "cm.selected_geography_scope"),
            )
        )
    if "selected_geography_value" in measure_meta_sources:
        score_optional_mapping.extend(
            (
                ("cohort_geography_value", "cm.selected_geography_value"),
                ("cohort_geo_value", "cm.selected_geography_value"),
            )
        )
    if "selected_specialty" in measure_meta_sources:
        score_optional_mapping.extend(
            (
                ("cohort_specialty", "cm.selected_specialty"),
                ("cohort_specialty_key", "cm.selected_specialty"),
            )
        )
    if "selected_taxonomy" in measure_meta_sources:
        score_optional_mapping.extend(
            (
                ("cohort_taxonomy", "cm.selected_taxonomy"),
                ("cohort_taxonomy_code", "cm.selected_taxonomy"),
            )
        )
    if "selected_classification" in measure_meta_sources:
        score_optional_mapping.append(("cohort_classification", "cm.selected_classification"))
    if "selected_procedure_bucket" in measure_meta_sources:
        score_optional_mapping.append(("cohort_procedure_bucket", "cm.selected_procedure_bucket"))
    if "selected_peer_n" in measure_meta_sources:
        score_optional_mapping.append(("cohort_peer_n", "ROUND(cm.selected_peer_n)::int"))
    score_optional_pairs = _optional_column_pairs(score_columns, tuple(score_optional_mapping))

    score_insert_columns = [
        "npi",
        "year",
        "model_version",
        "benchmark_mode",
        "risk_ratio_point",
        "ci75_low",
        "ci75_high",
        "ci90_low",
        "ci90_high",
        "score_0_100",
        "tier",
        "borderline_status",
        "low_score_threshold_failed",
        "low_confidence_threshold_failed",
        "high_score_threshold_passed",
        "high_confidence_threshold_passed",
        "estimated_cost_level",
        "score_method",
        "cost_source",
        "confidence_0_100",
        "confidence_band",
        "data_coverage_0_100",
        "provider_class",
        "location_source",
        "has_claims",
        "has_qpp",
        "has_rx",
        "has_enrollment",
        "has_medicare_claims",
        "run_id",
        "updated_at",
    ]
    score_select_columns = [
        "c.npi",
        "c.year",
        f"'{PROVIDER_QUALITY_MODEL_VERSION}'::varchar AS model_version",
        "c.benchmark_mode::varchar AS benchmark_mode",
        "c.rr_overall::float8 AS risk_ratio_point",
        "c.ci75_low::float8",
        "c.ci75_high::float8",
        "c.ci90_low::float8",
        "c.ci90_high::float8",
        "ROUND((LEAST(100.0, GREATEST(0.0, 50.0 - 125.0 * (c.rr_overall - 1.0))))::numeric, 2)::float8 AS score_0_100",
        "c.final_tier::varchar AS tier",
        "((c.low_score_check::int + c.low_confidence_check::int) = 1)::boolean AS borderline_status",
        "c.low_score_check",
        "c.low_confidence_check",
        "c.high_score_check",
        "c.high_confidence_check",
        "CASE WHEN c.rr_cost <= 0.80 THEN '$' WHEN c.rr_cost <= 0.90 THEN '$$' "
        "WHEN c.rr_cost <= 1.10 THEN '$$$' WHEN c.rr_cost <= 1.25 THEN '$$$$' ELSE '$$$$$' END::varchar AS estimated_cost_level",
        "c.score_method",
        "c.cost_source",
        "c.confidence_0_100",
        "c.confidence_band",
        "c.data_coverage_0_100",
        "c.provider_class",
        "c.location_source",
        "c.has_claims",
        "c.has_qpp",
        "c.has_rx",
        "c.has_enrollment",
        "c.has_medicare_claims",
        "CAST(:run_id AS varchar) AS run_id",
        "NOW() AS updated_at",
    ]
    score_insert_columns.extend(column for column, _ in score_optional_pairs)
    score_select_columns.extend(expr for _, expr in score_optional_pairs)
    score_insert_cols_sql = ",\n            ".join(score_insert_columns)
    score_select_cols_sql = ",\n            ".join(score_select_columns)

    benchmark_modes = _benchmark_modes_for_materialization(PROVIDER_QUALITY_BENCHMARK_MODE)
    benchmark_mode_values = ",\n                ".join(f"('{mode}'::varchar)" for mode in benchmark_modes)

    if rx_table_exists:
        provider_rx_cte = f"""
        provider_rx AS (
            SELECT
                r.npi,
                r.year,
                COALESCE(SUM(COALESCE(r.total_claims, 0.0)), 0.0)::float8 AS total_rx_claims,
                COALESCE(SUM(COALESCE(r.total_benes, 0.0)), 0.0)::float8 AS total_rx_beneficiaries
            FROM {schema}.{rx_table} r
            WHERE r.year BETWEEN {PROVIDER_QUALITY_MIN_YEAR} AND {PROVIDER_QUALITY_MAX_YEAR}
            GROUP BY r.npi, r.year
        ),
        """
    else:
        provider_rx_cte = """
        provider_rx AS (
            SELECT
                NULL::bigint AS npi,
                NULL::int AS year,
                0.0::float8 AS total_rx_claims,
                0.0::float8 AS total_rx_beneficiaries
            WHERE FALSE
        ),
        """

    return {
        "schema": schema,
        "qpp_table": qpp_table,
        "svi_table": svi_table,
        "measure_table": measure_table,
        "domain_table": domain_table,
        "score_table": score_table,
        "feature_table": feature_table,
        "lsh_table": lsh_table,
        "peer_target_table": peer_target_table,
        "feature_npi_col": feature_npi_col,
        "feature_year_col": feature_year_col,
        "feature_procedure_bucket_col": feature_procedure_bucket_col,
        "lsh_npi_col": lsh_npi_col,
        "lsh_year_col": lsh_year_col,
        "lsh_band_no_col": lsh_band_no_col,
        "lsh_band_hash_col": lsh_band_hash_col,
        "peer_target_year_col": peer_target_year_col,
        "peer_target_approp_col": peer_target_appropr_col,
        "peer_target_cost_col": peer_target_cost_col,
        "peer_target_effect_col": peer_target_effect_col,
        "peer_target_qpp_cost_col": peer_target_qpp_cost_col,
        "peer_target_rx_appropr_col": peer_target_rx_appropr_col,
        "feature_insert_cols_sql": feature_insert_cols_sql,
        "feature_select_cols_sql": feature_select_cols_sql,
        "lsh_insert_cols_sql": lsh_insert_cols_sql,
        "lsh_select_cols_sql": lsh_select_cols_sql,
        "peer_target_insert_cols_sql": peer_target_insert_cols_sql,
        "peer_target_select_cols_sql": peer_target_select_cols_sql,
        "measure_insert_cols_sql": measure_insert_cols_sql,
        "measure_select_cols_sql": measure_select_cols_sql,
        "score_insert_cols_sql": score_insert_cols_sql,
        "score_select_cols_sql": score_select_cols_sql,
        "feature_specialty_expr": feature_specialty_expr,
        "feature_taxonomy_expr": feature_taxonomy_expr,
        "feature_classification_expr": feature_classification_expr,
        "feature_procedure_bucket_expr": feature_procedure_bucket_expr,
        "feature_state_expr": feature_state_expr,
        "feature_zip_expr": feature_zip_expr,
        "feature_provider_class_expr": feature_provider_class_expr,
        "feature_location_source_expr": feature_location_source_expr,
        "feature_has_enrollment_expr": feature_has_enrollment_expr,
        "feature_has_medicare_claims_expr": feature_has_medicare_claims_expr,
        "peer_scope_expr": peer_scope_expr,
        "peer_benchmark_mode_expr": peer_benchmark_mode_expr,
        "peer_geo_value_expr": peer_geo_value_expr,
        "peer_cohort_level_expr": peer_cohort_level_expr,
        "peer_peer_n_expr": peer_peer_n_expr,
        "peer_specialty_expr": peer_specialty_expr,
        "peer_taxonomy_expr": peer_taxonomy_expr,
        "peer_classification_expr": peer_classification_expr,
        "peer_procedure_bucket_expr": peer_procedure_bucket_expr,
        "peer_specialty_match": peer_specialty_match,
        "peer_taxonomy_match": peer_taxonomy_match,
        "peer_procedure_bucket_match": peer_procedure_bucket_match,
        "provider_rx_cte": provider_rx_cte,
        "benchmark_mode_values": benchmark_mode_values,
        "measure_meta_sources": measure_meta_sources,
        "score_optional_pairs": score_optional_pairs,
        "npi_table_exists": npi_table_exists,
        "taxonomy_table_exists": taxonomy_table_exists,
        "nucc_table_exists": nucc_table_exists,
        "provider_enrichment_exists": provider_enrichment_exists,
        "doctor_clinician_exists": doctor_clinician_exists,
        "unified_address_exists": unified_address_exists,
        "npi_address_exists": npi_address_exists,
        "unified_address_has_multi_source_confirmed": "multi_source_confirmed" in unified_address_columns,
        "unified_address_has_source_count": "source_count" in unified_address_columns,
        "unified_address_has_checksum": "checksum" in unified_address_columns,
        "nucc_has_classification": "classification" in nucc_columns,
    }

