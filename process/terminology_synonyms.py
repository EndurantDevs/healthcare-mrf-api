# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import datetime
import hashlib
import json
import os
import re
from typing import Any

from db.connection import init_db
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
from process.ext.utils import ensure_database, make_class, push_objects

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


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def _now() -> datetime.datetime:
    return datetime.datetime.utcnow()


def _normalize_term_key(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", value.lower())).strip()


def _import_id(raw: str | None) -> str:
    cleaned = "".join(ch for ch in str(raw or "") if ch.isalnum())
    if cleaned:
        return cleaned[:32]
    return _now().strftime("%Y%m%d%H%M%S")


def _status_count(status: Any) -> int:
    if status is None:
        return 0
    if isinstance(status, int):
        return status
    parts = str(status).strip().split()
    if parts and parts[-1].isdigit():
        return int(parts[-1])
    return 0


def _row_mapping(row: Any) -> Any:
    return row._mapping if hasattr(row, "_mapping") else row


def _stage_index_name(stage_table: str, index_name: str) -> str:
    raw = f"{stage_table}_idx_{index_name}"
    if len(raw) <= 63:
        return raw
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:8]
    return f"{raw[:54]}_{digest}"


async def _create_indexes(stage_cls, schema: str) -> None:
    for index in getattr(stage_cls, "__my_additional_indexes__", []) or []:
        index_name = index.get("name", "_".join(index.get("index_elements")))
        using = f"USING {index.get('using')} " if index.get("using") else ""
        where = f" WHERE {index.get('where')}" if index.get("where") else ""
        await db.status(
            f"CREATE INDEX IF NOT EXISTS {_stage_index_name(stage_cls.__tablename__, index_name)} "
            f"ON {schema}.{stage_cls.__tablename__} {using}"
            f"({', '.join(index.get('index_elements'))}){where};"
        )


def _record(
    *,
    domain: str,
    synonym: str,
    term_type: str,
    target_system: str,
    target_code: str,
    target_display: str | None = None,
    canonical_term: str | None = None,
    is_broad: bool = False,
    confidence: float = 1.0,
    source: str = SOURCE_CURATED,
    source_attribution: str = PUBLIC_ATTRIBUTION,
    license_status: str = "public_or_curated",
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    term_key = _normalize_term_key(synonym)
    if not (term_key and target_system and target_code):
        return None
    return {
        "domain": domain,
        "term_key": term_key,
        "synonym": synonym,
        "term_type": term_type,
        "target_system": target_system.upper(),
        "target_code": str(target_code),
        "target_display": target_display or canonical_term or synonym,
        "canonical_term": canonical_term or target_display or synonym,
        "is_broad": bool(is_broad),
        "confidence": confidence,
        "source": source,
        "source_attribution": source_attribution,
        "license_status": license_status,
        "metadata_json": json.dumps(metadata or {}, sort_keys=True),
        "updated_at": _now(),
    }


def _provider_rows() -> list[dict[str, Any]]:
    seeds = [
        ("Family Practice", "207Q00000X", ["family medicine", "family physician", "family doctor", "pcp", "primary care"]),
        ("Internal Medicine", "207R00000X", ["internist", "internal medicine physician", "primary care physician"]),
        ("General Practice", "208D00000X", ["general practitioner", "gp", "general medicine"]),
        ("Nurse Practitioner", "363L00000X", ["np", "advanced practice nurse", "advanced practice registered nurse", "aprn"]),
        ("Physician Assistant", "363A00000X", ["pa", "physician associate"]),
        ("Emergency Medicine", "207P00000X", ["er doctor", "emergency physician", "emergency room doctor"]),
        ("Dermatology", "207N00000X", ["dermatologist", "skin doctor"]),
        ("Cardiology", "207RC0000X", ["cardiologist", "cardiovascular disease", "heart doctor"]),
        ("Diagnostic Radiology", "2085R0202X", ["radiologist", "diagnostic radiologist", "radiology"]),
        ("Psychiatry", "2084P0800X", ["psychiatrist", "mental health physician"]),
        ("Obstetrics/Gynecology", "207V00000X", ["obgyn", "ob gyn", "gynecology", "obstetrics", "obstetrician gynecologist"]),
        ("Orthopedic Surgery", "207X00000X", ["orthopedics", "orthopedic surgeon", "orthopaedic surgery"]),
        ("Ophthalmology", "207W00000X", ["ophthalmologist", "eye doctor"]),
        ("Optometry", "152W00000X", ["optometrist", "eye care"]),
        ("Dentist", "122300000X", ["dental provider", "general dentist"]),
    ]
    rows: list[dict[str, Any]] = []
    for provider_type, nucc_code, aliases in seeds:
        terms = [provider_type, nucc_code, *aliases]
        for synonym in terms:
            row = _record(
                domain="provider_type",
                synonym=synonym,
                term_type="curated_provider_alias",
                target_system="PROVIDER_TYPE",
                target_code=provider_type,
                target_display=provider_type,
                canonical_term=provider_type,
                confidence=0.96 if synonym != nucc_code else 1.0,
                metadata={"nucc_code": nucc_code},
            )
            if row:
                rows.append(row)
    return rows


def _procedure_rows() -> list[dict[str, Any]]:
    seeds = [
        ("office visit", "CPT", ["99202", "99203", "99204", "99205", "99211", "99212", "99213", "99214", "99215"], True),
        ("established patient visit", "CPT", ["99211", "99212", "99213", "99214", "99215"], True),
        ("new patient visit", "CPT", ["99202", "99203", "99204", "99205"], True),
        ("ekg", "CPT", ["93000", "93005", "93010"], True),
        ("ecg", "CPT", ["93000", "93005", "93010"], True),
        ("echocardiogram", "CPT", ["93306"], False),
        ("chest xray", "CPT", ["71045", "71046"], True),
        ("chest x-ray", "CPT", ["71045", "71046"], True),
        ("blood draw", "CPT", ["36415"], False),
        ("venipuncture", "CPT", ["36415"], False),
        ("a1c", "CPT", ["83036"], False),
        ("hemoglobin a1c", "CPT", ["83036"], False),
        ("lipid panel", "CPT", ["80061"], False),
        ("metabolic panel", "CPT", ["80053"], False),
        ("comprehensive metabolic panel", "CPT", ["80053"], False),
        ("complete blood count", "CPT", ["85025"], False),
        ("cbc", "CPT", ["85025"], False),
        ("screening mammogram", "CPT", ["77067"], False),
        ("mammogram", "CPT", ["77067"], True),
        ("colonoscopy", "CPT", ["45378", "G0121", "G0105"], True),
        ("brain mri", "CPT", ["70551", "70552", "70553"], True),
        ("knee xray", "CPT", ["73560", "73562", "73564"], True),
    ]
    rows: list[dict[str, Any]] = []
    for synonym, system, codes, is_broad in seeds:
        for code in codes:
            row = _record(
                domain="procedure",
                synonym=synonym,
                term_type="curated_procedure_alias",
                target_system=system,
                target_code=code,
                target_display=synonym,
                canonical_term=synonym,
                is_broad=is_broad,
                confidence=0.90 if is_broad else 0.96,
            )
            if row:
                rows.append(row)
    return rows


def _specialty_alias_rows() -> list[dict[str, Any]]:
    """Mirror the API's curated specialty alias dict into the synonym table.

    The dict in api.provider_specialty_filters is the bootstrap tier of the
    shared specialty resolution cache; seeding the same aliases here keeps the
    DB tier (and every terminology consumer) in lockstep with it. Multi-code
    bundles become one row per NUCC code — the (domain, term_key,
    target_system, target_code) primary key makes that natural.
    """
    from api.provider_specialty_filters import _SPECIALTY_TAXONOMY_CODE_ALIASES

    rows: list[dict[str, Any]] = []
    for alias, taxonomy_codes in _SPECIALTY_TAXONOMY_CODE_ALIASES.items():
        for taxonomy_code in taxonomy_codes:
            row = _record(
                domain="provider_type",
                synonym=alias,
                term_type="curated_specialty_alias",
                target_system="NUCC",
                target_code=taxonomy_code,
                canonical_term=alias,
                confidence=0.95,
                metadata={"nucc_code": taxonomy_code, "alias_bundle": alias},
            )
            if row:
                rows.append(row)
    return rows


def _curated_rows() -> list[dict[str, Any]]:
    return _provider_rows() + _specialty_alias_rows() + _procedure_rows()


def _norm_sql(expr: str) -> str:
    return (
        "LOWER(BTRIM(REGEXP_REPLACE("
        f"REGEXP_REPLACE(BTRIM({expr}), '[^A-Za-z0-9]+', ' ', 'g'), "
        "'\\s+', ' ', 'g')))"
    )


def _upsert_columns() -> str:
    columns = [column.name for column in TerminologySynonym.__table__.columns]
    return ", ".join(columns)


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
    system_list = ", ".join(f"'{value}'" for value in (*PROCEDURE_CODE_SYSTEMS, *MEDICATION_CODE_SYSTEMS))
    domain_case = (
        "CASE WHEN UPPER(code_system) IN ('RXNORM', 'NDC', 'HP_RX_CODE') "
        "THEN 'medication' ELSE 'procedure' END"
    )
    term_expr = "COALESCE(NULLIF(display_name, ''), NULLIF(short_description, ''), code)"
    select_sql = f"""
        SELECT DISTINCT ON ({domain_case}, {_norm_sql(term_expr)}, UPPER(code_system), code)
            {domain_case} AS domain,
            {_norm_sql(term_expr)} AS term_key,
            {term_expr} AS synonym,
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
         WHERE UPPER(code_system) IN ({system_list})
           AND COALESCE(NULLIF(display_name, ''), NULLIF(short_description, ''), code) IS NOT NULL
           AND {_norm_sql(term_expr)} <> ''
    """
    return _status_count(await db.status(_insert_sql(stage_table, select_sql)))


async def _insert_code_synonym_rows(schema: str, stage_table: str) -> int:
    system_list = ", ".join(f"'{value}'" for value in (*PROCEDURE_CODE_SYSTEMS, *MEDICATION_CODE_SYSTEMS))
    domain_case = (
        "CASE WHEN UPPER(s.code_system) IN ('RXNORM', 'NDC', 'HP_RX_CODE') "
        "THEN 'medication' ELSE 'procedure' END"
    )
    select_sql = f"""
        SELECT DISTINCT ON ({domain_case}, {_norm_sql('s.synonym')}, UPPER(s.code_system), s.code)
            {domain_case} AS domain,
            {_norm_sql('s.synonym')} AS term_key,
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
         WHERE UPPER(s.code_system) IN ({system_list})
           AND s.synonym IS NOT NULL
           AND {_norm_sql('s.synonym')} <> ''
    """
    return _status_count(await db.status(_insert_sql(stage_table, select_sql)))


async def _insert_observed_provider_rows(schema: str, stage_table: str) -> int:
    term_expr = "provider_type"
    select_sql = f"""
        SELECT DISTINCT ON ({_norm_sql(term_expr)}, provider_type)
            'provider_type' AS domain,
            {_norm_sql(term_expr)} AS term_key,
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
           AND {_norm_sql(term_expr)} <> ''
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
        SELECT DISTINCT ON ({_norm_sql('synonym')}, code)
            'provider_type' AS domain,
            {_norm_sql('synonym')} AS term_key,
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
           AND {_norm_sql('synonym')} <> ''
    """
    return _status_count(await db.status(_insert_sql(stage_table, select_sql)))


async def _insert_observed_procedure_rows(schema: str, stage_table: str) -> int:
    select_sql = f"""
        SELECT DISTINCT ON ({_norm_sql('service_description')}, procedure_code::varchar)
            'procedure' AS domain,
            {_norm_sql('service_description')} AS term_key,
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
           AND {_norm_sql('service_description')} <> ''
    """
    return _status_count(await db.status(_insert_sql(stage_table, select_sql)))


async def _insert_observed_prescription_rows(schema: str, stage_table: str) -> int:
    term_sql = f"""
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
        SELECT DISTINCT ON ({_norm_sql('synonym')}, UPPER(rx_code_system), rx_code)
            'medication' AS domain,
            {_norm_sql('synonym')} AS term_key,
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
           AND {_norm_sql('synonym')} <> ''
    """
    return _status_count(await db.status(_insert_sql(stage_table, term_sql)))


async def _publish_stage(schema: str, stage_cls) -> None:
    live_table = TerminologySynonym.__tablename__
    old_table = f"{live_table}_old"
    await db.status(f"DROP TABLE IF EXISTS {schema}.{old_table};")
    await db.status(f"ALTER TABLE IF EXISTS {schema}.{live_table} RENAME TO {old_table};")
    await db.status(f"ALTER TABLE {schema}.{stage_cls.__tablename__} RENAME TO {live_table};")
    await db.status(f"DROP TABLE IF EXISTS {schema}.{old_table};")


async def import_terminology_synonyms(
    *,
    test_mode: bool = False,
    import_id: str | None = None,
) -> dict[str, Any]:
    """Import normalized terminology synonyms into a staged snapshot."""
    schema = _schema()
    suffix = _import_id(import_id)
    if test_mode:
        suffix = f"test_{suffix}"
    stage_cls = make_class(TerminologySynonym, suffix)
    stage_table = f"{schema}.{stage_cls.__tablename__}"

    await db.status(f"CREATE SCHEMA IF NOT EXISTS {schema};")
    await db.status(f"DROP TABLE IF EXISTS {stage_table};")
    await db.create_table(stage_cls.__table__, checkfirst=True)

    curated_rows = _curated_rows()
    await push_objects(curated_rows, stage_cls, rewrite=True)

    source_counts = {
        "curated_rows": len(curated_rows),
        "code_catalog_rows": await _insert_code_catalog_rows(schema, stage_table),
        "code_synonym_rows": await _insert_code_synonym_rows(schema, stage_table),
        "observed_provider_rows": await _insert_observed_provider_rows(schema, stage_table),
        "nucc_rows": await _insert_nucc_rows(schema, stage_table),
        "observed_procedure_rows": await _insert_observed_procedure_rows(schema, stage_table),
        "observed_prescription_rows": await _insert_observed_prescription_rows(schema, stage_table),
    }
    await _create_indexes(stage_cls, schema)
    rows = await db.all(f"SELECT count(*)::bigint AS row_count FROM {stage_table};")
    row_count = int(_row_mapping(rows[0])["row_count"]) if rows else 0
    await _publish_stage(schema, stage_cls)

    result = {
        "import_id": suffix,
        "test_mode": bool(test_mode),
        "table": f"{schema}.{TerminologySynonym.__tablename__}",
        "row_count": row_count,
        "source_counts": source_counts,
    }
    print(f"Terminology synonym import done: {result}")
    return result


async def main(
    test_mode: bool = False,
    import_id: str | None = None,
    run_id: str | None = None,
) -> dict[str, Any]:
    """Run the terminology synonym import entry point."""
    _ = run_id
    await init_db(db)
    try:
        await ensure_database(test_mode)
        return await import_terminology_synonyms(test_mode=test_mode, import_id=import_id)
    finally:
        await db.disconnect()
