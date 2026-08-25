# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""SQL shared by staged and transitional provider geo assurance."""

from __future__ import annotations

import re


GEO_ASSURANCE_VERSION = 1
GEO_ASSURANCE_STATE_TABLE = "entity_address_geo_assurance_state"
GEO_EVIDENCE_NONE = 0
GEO_EVIDENCE_NPPES = 1
GEO_EVIDENCE_MRF = 2
GEO_EVIDENCE_CMS = 3
GEO_EVIDENCE_SOURCE_IDS = (
    GEO_EVIDENCE_NONE,
    GEO_EVIDENCE_NPPES,
    GEO_EVIDENCE_MRF,
    GEO_EVIDENCE_CMS,
)

_SQL_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")
_PROJECTION_DEPENDENCIES = (
    (None, "npi_address"),
    (None, "mrf_address"),
    (None, "doctor_clinician_address"),
    (None, "geo_zip_lookup"),
    ("tiger", "zip_state"),
    ("tiger", "zcta5"),
)


def _sql_identifier(value: str, *, field_name: str) -> str:
    identifier = str(value or "")
    if not _SQL_IDENTIFIER.fullmatch(identifier):
        raise ValueError(f"{field_name} must be a simple PostgreSQL identifier")
    return identifier


def projection_relation_signature_sql(schema_name: str) -> str:
    """Return the exact relation identity used by one stored projection."""

    schema_name = _sql_identifier(schema_name, field_name="address schema")
    signature_fields: list[str] = []
    for dependency_schema, table_name in _PROJECTION_DEPENDENCIES:
        relation_schema = dependency_schema or schema_name
        qualified_name = f"{relation_schema}.{table_name}"
        signature_fields.extend(
            (
                repr(qualified_name),
                "jsonb_build_array("
                f"COALESCE(to_regclass('{qualified_name}')::oid::bigint, -1), "
                "COALESCE(pg_relation_filenode("
                f"to_regclass('{qualified_name}'))::bigint, -1))",
            )
        )
    return f"jsonb_build_object({', '.join(signature_fields)})"


def projection_dependency_lock_sql(schema_name: str) -> str:
    """Hold every swap-published input stable through projection receipt."""

    schema_name = _sql_identifier(schema_name, field_name="address schema")
    relations = (
        f"{dependency_schema or schema_name}.{table_name}"
        for dependency_schema, table_name in _PROJECTION_DEPENDENCIES
    )
    return f"LOCK TABLE {', '.join(relations)} IN ACCESS SHARE MODE;"


def projection_state_available_sql(schema_name: str) -> str:
    """Require the active projection to match every published dependency."""

    schema_name = _sql_identifier(schema_name, field_name="address schema")
    live_table = f"{schema_name}.entity_address_unified"
    signature_checks: list[str] = []
    for dependency_schema, table_name in _PROJECTION_DEPENDENCIES:
        relation_schema = dependency_schema or schema_name
        qualified_name = f"{relation_schema}.{table_name}"
        signature_checks.extend(
            (
                "COALESCE(geo_assurance_state.active_relation_signature #>> "
                f"ARRAY[{qualified_name!r}, '0'], '') = "
                f"COALESCE(to_regclass('{qualified_name}')::oid::bigint, -1)::text",
                "COALESCE(geo_assurance_state.active_relation_signature #>> "
                f"ARRAY[{qualified_name!r}, '1'], '') = "
                "COALESCE(pg_relation_filenode("
                f"to_regclass('{qualified_name}'))::bigint, -1)::text",
            )
        )
    signature_match_sql = "\n           AND ".join(signature_checks)
    return f"""EXISTS (
        SELECT 1
          FROM {schema_name}.{GEO_ASSURANCE_STATE_TABLE} AS geo_assurance_state
         WHERE geo_assurance_state.singleton IS TRUE
           AND geo_assurance_state.active_geo_assurance_version = {GEO_ASSURANCE_VERSION}
           AND geo_assurance_state.active_table_oid = to_regclass('{live_table}')::oid
           AND {signature_match_sql}
    )"""


def nonblank_array_value_sql(array_sql: str) -> str:
    """Require one nonblank element from a SQL array expression."""

    return f"""EXISTS (
        SELECT 1
          FROM UNNEST({array_sql}) AS array_values(array_value)
         WHERE NULLIF(BTRIM(array_value::text), '') IS NOT NULL
    )"""


def independent_issuer_sql(issuer_array_sql: str) -> str:
    """Require two distinct normalized issuer identities."""

    return f"""(
        SELECT COUNT(DISTINCT LOWER(BTRIM(issuer_name)))
          FROM UNNEST(COALESCE({issuer_array_sql}, ARRAY[]::varchar[])) AS issuer_names(issuer_name)
         WHERE NULLIF(BTRIM(issuer_name), '') IS NOT NULL
    ) >= 2"""


def mrf_lineage_complete_sql(alias: str) -> str:
    """Require durable import identity and retrieval time for an MRF row."""

    alias = _sql_identifier(alias, field_name="MRF address alias")
    return f"""(
        (
            {nonblank_array_value_sql(f'{alias}.source_import_ids')}
            OR {alias}.date_added IS NOT NULL
        )
        AND (
            {nonblank_array_value_sql(f'{alias}.source_import_dates')}
            OR {alias}.date_added IS NOT NULL
        )
    )"""


def evidence_source_id_case_sql(
    *,
    nppes_condition_sql: str,
    mrf_condition_sql: str,
    cms_condition_sql: str,
) -> str:
    """Map ordered evidence predicates to their compact source identifier."""

    return f"""CASE
        WHEN {nppes_condition_sql} THEN {GEO_EVIDENCE_NPPES}
        WHEN {mrf_condition_sql} THEN {GEO_EVIDENCE_MRF}
        WHEN {cms_condition_sql} THEN {GEO_EVIDENCE_CMS}
        ELSE {GEO_EVIDENCE_NONE}
    END::smallint"""


def evidence_level_case_sql(
    *,
    nppes_condition_sql: str,
    mrf_condition_sql: str,
    cms_condition_sql: str,
) -> str:
    """Map ordered evidence predicates to their public evidence label."""

    return f"""CASE
        WHEN {nppes_condition_sql} THEN 'nppes_registry_address'
        WHEN {mrf_condition_sql} THEN 'multi_issuer_marketplace_address'
        WHEN {cms_condition_sql} THEN 'cms_doctors_source_with_nppes_identity_anchor'
        ELSE NULL::varchar
    END"""


def evidence_level_from_source_id_sql(source_id_sql: str) -> str:
    """Map a compact evidence source expression to its public label."""

    return f"""CASE ({source_id_sql})
        WHEN {GEO_EVIDENCE_NPPES} THEN 'nppes_registry_address'
        WHEN {GEO_EVIDENCE_MRF} THEN 'multi_issuer_marketplace_address'
        WHEN {GEO_EVIDENCE_CMS} THEN 'cms_doctors_source_with_nppes_identity_anchor'
        ELSE NULL::varchar
    END"""


def _legacy_nppes_evidence_sql(alias: str, schema_name: str) -> str:
    """Build the original record-complete NPPES predicate."""

    return f"""(
        ({alias}.address_source_mask & 1) <> 0
        AND {alias}.address_key IS NOT NULL
        AND EXISTS (
            SELECT 1
              FROM {schema_name}.npi_address AS geo_nppes
             WHERE geo_nppes.npi = {alias}.npi
               AND geo_nppes.address_key = {alias}.address_key
               AND geo_nppes.date_added IS NOT NULL
        )
    )"""


def _legacy_mrf_evidence_sql(alias: str, schema_name: str) -> str:
    """Build the original multi-issuer MRF predicate."""

    return f"""EXISTS (
        SELECT 1
          FROM {schema_name}.mrf_address AS geo_mrf
         WHERE geo_mrf.npi = {alias}.npi
           AND geo_mrf.address_key = {alias}.address_key
           AND {independent_issuer_sql('geo_mrf.source_issuer_names')}
           AND {mrf_lineage_complete_sql('geo_mrf')}
    )"""


def _legacy_cms_evidence_sql(
    alias: str,
    schema_name: str,
    unified_table_name: str,
) -> str:
    """Build the original CMS predicate with its exact NPPES anchor."""

    return f"""(
        ({alias}.address_source_mask & 4) <> 0
        AND {alias}.address_key IS NOT NULL
        AND {alias}.premise_key IS NOT NULL
        AND EXISTS (
            SELECT 1
              FROM {schema_name}.doctor_clinician_address AS geo_doctor
             WHERE geo_doctor.npi = {alias}.npi
               AND geo_doctor.address_key = {alias}.address_key
               AND geo_doctor.updated_at IS NOT NULL
        )
        AND EXISTS (
            SELECT 1
              FROM {schema_name}.{unified_table_name} AS geo_nppes_anchor
              JOIN {schema_name}.npi_address AS geo_nppes_anchor_source
                ON geo_nppes_anchor_source.npi = geo_nppes_anchor.npi
               AND geo_nppes_anchor_source.address_key = geo_nppes_anchor.address_key
               AND geo_nppes_anchor_source.date_added IS NOT NULL
             WHERE geo_nppes_anchor.npi = {alias}.npi
               AND geo_nppes_anchor.premise_key = {alias}.premise_key
               AND (geo_nppes_anchor.address_source_mask & 1) <> 0
               AND geo_nppes_anchor.type IN (
                   'primary', 'secondary', 'practice', 'site'
               )
        )
    )"""


def legacy_evidence_source_id_sql(
    alias: str,
    *,
    schema_name: str,
    unified_table_name: str = "entity_address_unified",
) -> str:
    """Return the exact pre-projection evidence classifier for one address."""

    alias = _sql_identifier(alias, field_name="unified address alias")
    schema_name = _sql_identifier(schema_name, field_name="address schema")
    unified_table_name = _sql_identifier(
        unified_table_name,
        field_name="unified address table",
    )
    return evidence_source_id_case_sql(
        nppes_condition_sql=_legacy_nppes_evidence_sql(alias, schema_name),
        mrf_condition_sql=_legacy_mrf_evidence_sql(alias, schema_name),
        cms_condition_sql=_legacy_cms_evidence_sql(
            alias,
            schema_name,
            unified_table_name,
        ),
    )


def projected_evidence_level_sql(
    alias: str,
    *,
    schema_name: str,
    legacy_level_sql: str,
) -> str:
    """Prefer a valid stored evidence class and otherwise evaluate legacy SQL."""

    alias = _sql_identifier(alias, field_name="unified address alias")
    projected_level = evidence_level_from_source_id_sql(
        f"{alias}.geo_evidence_source_id"
    )
    return f"""CASE
        WHEN {projected_evidence_available_sql(alias, schema_name=schema_name)}
        THEN {projected_level}
        ELSE ({legacy_level_sql})
    END"""


def projected_evidence_available_sql(alias: str, *, schema_name: str) -> str:
    """Return the predicate for a supported stored evidence projection."""

    alias = _sql_identifier(alias, field_name="unified address alias")
    schema_name = _sql_identifier(schema_name, field_name="address schema")
    valid_source_ids = ", ".join(str(value) for value in GEO_EVIDENCE_SOURCE_IDS)
    return (
        f"({alias}.geo_assurance_version = {GEO_ASSURANCE_VERSION} "
        f"AND {alias}.geo_evidence_source_id IN ({valid_source_ids})"
        f" AND {projection_state_available_sql(schema_name)})"
    )


def projected_boolean_sql(
    alias: str,
    column_name: str,
    *,
    schema_name: str,
    legacy_sql: str,
) -> str:
    """Prefer a valid stored boolean and otherwise evaluate legacy SQL."""

    alias = _sql_identifier(alias, field_name="unified address alias")
    column_name = _sql_identifier(column_name, field_name="geo assurance column")
    schema_name = _sql_identifier(schema_name, field_name="address schema")
    return f"""CASE
        WHEN {alias}.geo_assurance_version = {GEO_ASSURANCE_VERSION}
         AND {alias}.{column_name} IS NOT NULL
         AND {projection_state_available_sql(schema_name)}
        THEN {alias}.{column_name}
        ELSE ({legacy_sql})
    END"""


__all__ = [
    "GEO_ASSURANCE_VERSION",
    "GEO_ASSURANCE_STATE_TABLE",
    "GEO_EVIDENCE_SOURCE_IDS",
    "evidence_level_case_sql",
    "evidence_level_from_source_id_sql",
    "evidence_source_id_case_sql",
    "independent_issuer_sql",
    "legacy_evidence_source_id_sql",
    "mrf_lineage_complete_sql",
    "nonblank_array_value_sql",
    "projected_boolean_sql",
    "projected_evidence_available_sql",
    "projected_evidence_level_sql",
    "projection_dependency_lock_sql",
    "projection_relation_signature_sql",
    "projection_state_available_sql",
]
