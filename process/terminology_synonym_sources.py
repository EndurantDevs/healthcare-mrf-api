# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from db.models import (
    CodeCatalog,
    CodeSynonym,
    NUCCTaxonomy,
    PricingProvider,
    PricingProviderPrescription,
    PricingProviderProcedure,
    TerminologySynonym,
    db,
)

SOURCE_CURATED = "healthporta_curated_terminology_synonyms"
SOURCE_CODE_CATALOG = "healthporta_code_catalog"
SOURCE_CODE_SYNONYM = "healthporta_code_synonym"
SOURCE_PRICING_PROVIDERS = "cms_pricing_provider_observed"
SOURCE_PRICING_PROCEDURES = "cms_pricing_provider_procedure_observed"
SOURCE_PRICING_PRESCRIPTIONS = "cms_partd_provider_prescription_observed"
SOURCE_NUCC = "nucc_taxonomy"

PUBLIC_ATTRIBUTION = (
    "Derived from public CMS/NUCC/imported HealthPorta reference data and curated "
    "non-license-restricted search aliases. This importer intentionally does not load "
    "official proprietary CPT/CDT descriptors or synonym files."
)

PROCEDURE_CODE_SYSTEMS = ("CPT", "HCPCS", "CDT", "HP_PROCEDURE_CODE")
MEDICATION_CODE_SYSTEMS = ("RXNORM", "NDC", "HP_RX_CODE")


def _status_count(status: object) -> int:
    if status is None:
        return 0
    if isinstance(status, int):
        return status
    status_parts = str(status).strip().split()
    if status_parts and status_parts[-1].isdigit():
        return int(status_parts[-1])
    return 0


def _normalized_term_sql(expression: str) -> str:
    return (
        "LOWER(BTRIM(REGEXP_REPLACE("
        f"REGEXP_REPLACE(BTRIM({expression}), '[^A-Za-z0-9]+', ' ', 'g'), "
        "'\\s+', ' ', 'g')))"
    )


def _upsert_columns() -> str:
    column_names = [
        column.name for column in TerminologySynonym.__table__.columns
    ]
    return ", ".join(column_names)


def _insert_sql(stage_table: str, select_sql: str) -> str:
    columns = _upsert_columns()
    return f"""
        INSERT INTO {stage_table} ({columns})
        {select_sql}
        ON CONFLICT (domain, term_key, target_system, target_code) DO UPDATE SET
            synonym = EXCLUDED.synonym,
            term_type = EXCLUDED.term_type,
            target_display = EXCLUDED.target_display,
            canonical_term = EXCLUDED.canonical_term,
            is_broad = EXCLUDED.is_broad,
            confidence = EXCLUDED.confidence,
            source = EXCLUDED.source,
            source_attribution = EXCLUDED.source_attribution,
            license_status = EXCLUDED.license_status,
            metadata_json = EXCLUDED.metadata_json,
            updated_at = EXCLUDED.updated_at;
    """


async def _insert_code_catalog_rows(schema: str, stage_table: str) -> int:
    quoted_code_systems_sql = ", ".join(
        f"'{code_system}'"
        for code_system in (*PROCEDURE_CODE_SYSTEMS, *MEDICATION_CODE_SYSTEMS)
    )
    domain_case = (
        "CASE WHEN UPPER(code_system) IN ('RXNORM', 'NDC', 'HP_RX_CODE') "
        "THEN 'medication' ELSE 'procedure' END"
    )
    term_expression = (
        "COALESCE(NULLIF(display_name, ''), NULLIF(short_description, ''), code)"
    )
    select_sql = f"""
        SELECT DISTINCT ON ({domain_case}, {_normalized_term_sql(term_expression)}, UPPER(code_system), code)
            {domain_case} AS domain,
            {_normalized_term_sql(term_expression)} AS term_key,
            {term_expression} AS synonym,
            'catalog_display' AS term_type,
            UPPER(code_system) AS target_system,
            code::varchar AS target_code,
            COALESCE(NULLIF(display_name, ''), NULLIF(short_description, ''), code) AS target_display,
            COALESCE(NULLIF(display_name, ''), NULLIF(short_description, ''), code) AS canonical_term,
            false AS is_broad,
            0.9400 AS confidence,
            '{SOURCE_CODE_CATALOG}' AS source,
            source_attribution,
            'source_import' AS license_status,
            jsonb_build_object('catalog_source', source, 'code_type', code_type)::text AS metadata_json,
            NOW() AT TIME ZONE 'UTC' AS updated_at
          FROM {schema}.{CodeCatalog.__tablename__}
         WHERE UPPER(code_system) IN ({quoted_code_systems_sql})
           AND COALESCE(NULLIF(display_name, ''), NULLIF(short_description, ''), code) IS NOT NULL
           AND {_normalized_term_sql(term_expression)} <> ''
    """
    return _status_count(await db.status(_insert_sql(stage_table, select_sql)))


async def _insert_code_synonym_rows(schema: str, stage_table: str) -> int:
    quoted_code_systems_sql = ", ".join(
        f"'{code_system}'"
        for code_system in (*PROCEDURE_CODE_SYSTEMS, *MEDICATION_CODE_SYSTEMS)
    )
    domain_case = (
        "CASE WHEN UPPER(s.code_system) IN ('RXNORM', 'NDC', 'HP_RX_CODE') "
        "THEN 'medication' ELSE 'procedure' END"
    )
    select_sql = f"""
        SELECT DISTINCT ON ({domain_case}, {_normalized_term_sql('s.synonym')}, UPPER(s.code_system), s.code)
            {domain_case} AS domain,
            {_normalized_term_sql('s.synonym')} AS term_key,
            s.synonym,
            COALESCE(NULLIF(s.term_type, ''), 'source_synonym') AS term_type,
            UPPER(s.code_system) AS target_system,
            s.code::varchar AS target_code,
            COALESCE(NULLIF(c.display_name, ''), NULLIF(c.short_description, ''), s.synonym) AS target_display,
            COALESCE(NULLIF(c.display_name, ''), NULLIF(c.short_description, ''), s.synonym) AS canonical_term,
            false AS is_broad,
            0.9700 AS confidence,
            '{SOURCE_CODE_SYNONYM}' AS source,
            s.source_attribution,
            'source_import' AS license_status,
            jsonb_build_object('synonym_source', s.source, 'catalog_source', c.source)::text AS metadata_json,
            NOW() AT TIME ZONE 'UTC' AS updated_at
          FROM {schema}.{CodeSynonym.__tablename__} s
          LEFT JOIN {schema}.{CodeCatalog.__tablename__} c
            ON UPPER(c.code_system) = UPPER(s.code_system)
           AND c.code = s.code
         WHERE UPPER(s.code_system) IN ({quoted_code_systems_sql})
           AND s.synonym IS NOT NULL
           AND {_normalized_term_sql('s.synonym')} <> ''
    """
    return _status_count(await db.status(_insert_sql(stage_table, select_sql)))


async def _insert_observed_provider_rows(schema: str, stage_table: str) -> int:
    term_expression = "provider_type"
    select_sql = f"""
        SELECT DISTINCT ON ({_normalized_term_sql(term_expression)}, provider_type)
            'provider_type' AS domain,
            {_normalized_term_sql(term_expression)} AS term_key,
            provider_type AS synonym,
            'observed_provider_type' AS term_type,
            'PROVIDER_TYPE' AS target_system,
            provider_type AS target_code,
            provider_type AS target_display,
            provider_type AS canonical_term,
            false AS is_broad,
            1.0000 AS confidence,
            '{SOURCE_PRICING_PROVIDERS}' AS source,
            '{PUBLIC_ATTRIBUTION}' AS source_attribution,
            'source_import' AS license_status,
            jsonb_build_object('source_table', '{PricingProvider.__tablename__}')::text AS metadata_json,
            NOW() AT TIME ZONE 'UTC' AS updated_at
          FROM {schema}.{PricingProvider.__tablename__}
         WHERE provider_type IS NOT NULL
           AND {_normalized_term_sql(term_expression)} <> ''
    """
    return _status_count(await db.status(_insert_sql(stage_table, select_sql)))


async def _insert_nucc_rows(schema: str, stage_table: str) -> int:
    select_sql = f"""
        WITH terms AS (
            SELECT code,
                   display_name,
                   classification,
                   specialization,
                   grouping,
                   unnest(ARRAY[
                       display_name,
                       classification,
                       specialization,
                       grouping,
                       code
                   ]) AS synonym
              FROM {schema}.{NUCCTaxonomy.__tablename__}
        )
        SELECT DISTINCT ON ({_normalized_term_sql('synonym')}, code)
            'provider_type' AS domain,
            {_normalized_term_sql('synonym')} AS term_key,
            synonym,
            'nucc_term' AS term_type,
            'NUCC' AS target_system,
            code AS target_code,
            COALESCE(NULLIF(display_name, ''), NULLIF(classification, ''), code) AS target_display,
            COALESCE(NULLIF(classification, ''), NULLIF(display_name, ''), code) AS canonical_term,
            false AS is_broad,
            0.9200 AS confidence,
            '{SOURCE_NUCC}' AS source,
            '{PUBLIC_ATTRIBUTION}' AS source_attribution,
            'public_source' AS license_status,
            jsonb_build_object('grouping', grouping, 'classification', classification, 'specialization', specialization)::text AS metadata_json,
            NOW() AT TIME ZONE 'UTC' AS updated_at
          FROM terms
         WHERE synonym IS NOT NULL
           AND {_normalized_term_sql('synonym')} <> ''
    """
    return _status_count(await db.status(_insert_sql(stage_table, select_sql)))


async def _insert_observed_procedure_rows(
    schema: str,
    stage_table: str,
) -> int:
    select_sql = f"""
        SELECT DISTINCT ON ({_normalized_term_sql('service_description')}, procedure_code::varchar)
            'procedure' AS domain,
            {_normalized_term_sql('service_description')} AS term_key,
            service_description AS synonym,
            'observed_claims_service_description' AS term_type,
            'HP_PROCEDURE_CODE' AS target_system,
            procedure_code::varchar AS target_code,
            service_description AS target_display,
            service_description AS canonical_term,
            false AS is_broad,
            0.9300 AS confidence,
            '{SOURCE_PRICING_PROCEDURES}' AS source,
            '{PUBLIC_ATTRIBUTION}' AS source_attribution,
            'source_import' AS license_status,
            jsonb_build_object('reported_code', reported_code)::text AS metadata_json,
            NOW() AT TIME ZONE 'UTC' AS updated_at
          FROM {schema}.{PricingProviderProcedure.__tablename__}
         WHERE service_description IS NOT NULL
           AND procedure_code IS NOT NULL
           AND {_normalized_term_sql('service_description')} <> ''
    """
    return _status_count(await db.status(_insert_sql(stage_table, select_sql)))


async def _insert_observed_prescription_rows(
    schema: str,
    stage_table: str,
) -> int:
    select_sql = f"""
        WITH terms AS (
            SELECT rx_code_system,
                   rx_code,
                   rx_name,
                   generic_name,
                   brand_name,
                   unnest(ARRAY[rx_name, generic_name, brand_name]) AS synonym
              FROM {schema}.{PricingProviderPrescription.__tablename__}
             WHERE rx_code_system IS NOT NULL
               AND rx_code IS NOT NULL
        )
        SELECT DISTINCT ON ({_normalized_term_sql('synonym')}, UPPER(rx_code_system), rx_code)
            'medication' AS domain,
            {_normalized_term_sql('synonym')} AS term_key,
            synonym,
            'observed_prescription_name' AS term_type,
            UPPER(rx_code_system) AS target_system,
            rx_code AS target_code,
            COALESCE(NULLIF(rx_name, ''), NULLIF(generic_name, ''), NULLIF(brand_name, ''), synonym) AS target_display,
            COALESCE(NULLIF(generic_name, ''), NULLIF(brand_name, ''), NULLIF(rx_name, ''), synonym) AS canonical_term,
            false AS is_broad,
            0.9300 AS confidence,
            '{SOURCE_PRICING_PRESCRIPTIONS}' AS source,
            '{PUBLIC_ATTRIBUTION}' AS source_attribution,
            'source_import' AS license_status,
            jsonb_build_object('rx_name', rx_name, 'generic_name', generic_name, 'brand_name', brand_name)::text AS metadata_json,
            NOW() AT TIME ZONE 'UTC' AS updated_at
          FROM terms
         WHERE synonym IS NOT NULL
           AND {_normalized_term_sql('synonym')} <> ''
    """
    return _status_count(await db.status(_insert_sql(stage_table, select_sql)))
