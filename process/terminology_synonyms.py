# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import datetime
import hashlib
import json
import os
import re
from dataclasses import dataclass
from typing import Any

from db.connection import init_db
from db.models import TerminologySynonym, db
from process.ext.utils import ensure_database, make_class, push_objects
from process.terminology_synonym_sources import (
    MEDICATION_CODE_SYSTEMS,
    PROCEDURE_CODE_SYSTEMS,
    PUBLIC_ATTRIBUTION,
    SOURCE_CODE_CATALOG,
    SOURCE_CODE_SYNONYM,
    SOURCE_CURATED,
    SOURCE_NUCC,
    SOURCE_PRICING_PRESCRIPTIONS,
    SOURCE_PRICING_PROCEDURES,
    SOURCE_PRICING_PROVIDERS,
    _insert_code_catalog_rows,
    _insert_code_synonym_rows,
    _insert_nucc_rows,
    _insert_observed_prescription_rows,
    _insert_observed_procedure_rows,
    _insert_observed_provider_rows,
    _status_count,
)


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


@dataclass(frozen=True)
class TerminologyRecordOptions:
    """Optional display, confidence, provenance, and metadata fields."""

    target_display: str | None = None
    canonical_term: str | None = None
    is_broad: bool = False
    confidence: float = 1.0
    provenance_source: str = SOURCE_CURATED
    source_attribution: str = PUBLIC_ATTRIBUTION
    license_status: str = "public_or_curated"
    metadata: dict[str, Any] | None = None


def _record(
    *,
    domain: str,
    synonym: str,
    term_type: str,
    target_system: str,
    target_code: str,
    options: TerminologyRecordOptions | None = None,
) -> dict[str, Any] | None:
    record_options = options or TerminologyRecordOptions()
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
        "target_display": (
            record_options.target_display
            or record_options.canonical_term
            or synonym
        ),
        "canonical_term": (
            record_options.canonical_term
            or record_options.target_display
            or synonym
        ),
        "is_broad": bool(record_options.is_broad),
        "confidence": record_options.confidence,
        "source": record_options.provenance_source,
        "source_attribution": record_options.source_attribution,
        "license_status": record_options.license_status,
        "metadata_json": json.dumps(record_options.metadata or {}, sort_keys=True),
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
    provider_alias_rows: list[dict[str, Any]] = []
    for provider_type, nucc_code, aliases in seeds:
        terms = [provider_type, nucc_code, *aliases]
        for synonym in terms:
            provider_alias_record = _record(
                domain="provider_type",
                synonym=synonym,
                term_type="curated_provider_alias",
                target_system="PROVIDER_TYPE",
                target_code=provider_type,
                options=TerminologyRecordOptions(
                    target_display=provider_type,
                    canonical_term=provider_type,
                    confidence=0.96 if synonym != nucc_code else 1.0,
                    metadata={"nucc_code": nucc_code},
                ),
            )
            if provider_alias_record:
                provider_alias_rows.append(provider_alias_record)
    return provider_alias_rows


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
    procedure_alias_rows: list[dict[str, Any]] = []
    for synonym, system, codes, is_broad in seeds:
        for code in codes:
            procedure_alias_record = _record(
                domain="procedure",
                synonym=synonym,
                term_type="curated_procedure_alias",
                target_system=system,
                target_code=code,
                options=TerminologyRecordOptions(
                    target_display=synonym,
                    canonical_term=synonym,
                    is_broad=is_broad,
                    confidence=0.90 if is_broad else 0.96,
                ),
            )
            if procedure_alias_record:
                procedure_alias_rows.append(procedure_alias_record)
    return procedure_alias_rows


def _specialty_alias_rows() -> list[dict[str, Any]]:
    """Mirror the API's curated specialty alias dict into the synonym table.

    The dict in api.provider_specialty_filters is the bootstrap tier of the
    shared specialty resolution cache; seeding the same aliases here keeps the
    DB tier (and every terminology consumer) in lockstep with it. Multi-code
    bundles become one row per NUCC code — the (domain, term_key,
    target_system, target_code) primary key makes that natural.
    """
    from api.provider_specialty_filters import _SPECIALTY_TAXONOMY_CODE_ALIASES

    specialty_alias_rows: list[dict[str, Any]] = []
    for alias, taxonomy_codes in _SPECIALTY_TAXONOMY_CODE_ALIASES.items():
        for taxonomy_code in taxonomy_codes:
            specialty_alias_record = _record(
                domain="provider_type",
                synonym=alias,
                term_type="curated_specialty_alias",
                target_system="NUCC",
                target_code=taxonomy_code,
                options=TerminologyRecordOptions(
                    canonical_term=alias,
                    confidence=0.95,
                    metadata={
                        "nucc_code": taxonomy_code,
                        "alias_bundle": alias,
                    },
                ),
            )
            if specialty_alias_record:
                specialty_alias_rows.append(specialty_alias_record)
    return specialty_alias_rows


def _curated_rows() -> list[dict[str, Any]]:
    return _provider_rows() + _specialty_alias_rows() + _procedure_rows()


async def _table_row_count(table: str) -> int:
    count_query_rows = await db.all(f"SELECT count(*)::bigint AS row_count FROM {table};")
    return int(_row_mapping(count_query_rows[0])["row_count"]) if count_query_rows else 0


async def _publish_stage(schema: str, stage_cls, expected_row_count: int) -> None:
    live_table = TerminologySynonym.__tablename__
    old_table = f"{live_table}_old"
    async with db.transaction():
        await db.status(f"DROP TABLE IF EXISTS {schema}.{old_table};")
        await db.status(f"ALTER TABLE IF EXISTS {schema}.{live_table} RENAME TO {old_table};")
        await db.status(f"ALTER TABLE {schema}.{stage_cls.__tablename__} RENAME TO {live_table};")
        promoted_row_count = await _table_row_count(f"{schema}.{live_table}")
        if promoted_row_count != expected_row_count:
            raise RuntimeError(
                f"promoted row count {promoted_row_count} does not match staged row count {expected_row_count}"
            )


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
    try:
        await db.status(f"DROP TABLE IF EXISTS {stage_table};")
        await db.create_table(stage_cls.__table__, checkfirst=True)

        curated_rows = _curated_rows()
        await push_objects(curated_rows, stage_cls, rewrite=True)

        source_row_count_by_type = {
            "curated_rows": len(curated_rows),
            "code_catalog_rows": await _insert_code_catalog_rows(schema, stage_table),
            "code_synonym_rows": await _insert_code_synonym_rows(schema, stage_table),
            "observed_provider_rows": await _insert_observed_provider_rows(schema, stage_table),
            "nucc_rows": await _insert_nucc_rows(schema, stage_table),
            "observed_procedure_rows": await _insert_observed_procedure_rows(schema, stage_table),
            "observed_prescription_rows": await _insert_observed_prescription_rows(schema, stage_table),
        }
        await _create_indexes(stage_cls, schema)
        row_count = await _table_row_count(stage_table)
        await _publish_stage(schema, stage_cls, row_count)

        import_summary_map = {
            "import_id": suffix,
            "test_mode": bool(test_mode),
            "table": f"{schema}.{TerminologySynonym.__tablename__}",
            "row_count": row_count,
            "source_counts": source_row_count_by_type,
        }
        print(f"Terminology synonym import done: {import_summary_map}")
        return import_summary_map
    finally:
        await db.status(f"DROP TABLE IF EXISTS {stage_table};")


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
