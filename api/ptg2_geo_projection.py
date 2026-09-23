# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""SQL shared by staged and transitional provider geo assurance."""

from __future__ import annotations

import re
from collections.abc import Mapping

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


def validate_projection_dependency_bindings(schema_name: str, bindings: object) -> dict:
    """Copy one closed, native-identity-bound map supplied by a trusted caller."""

    schema_name = _sql_identifier(schema_name, field_name="address schema")
    expected_names = {f"{schema or schema_name}.{table}" for schema, table in _PROJECTION_DEPENDENCIES}
    fields = {"schema_name", "table_name", "relation_oid", "relfilenode"}
    if not isinstance(bindings, Mapping) or set(bindings) != expected_names:
        raise ValueError("geo projection dependency binding family is invalid")
    bindings_by_name = {}
    for name in sorted(expected_names):
        supplied_binding = bindings[name]
        if not isinstance(supplied_binding, Mapping):
            raise ValueError("geo projection dependency binding fields are invalid")
        binding_by_field = dict(supplied_binding)
        if set(binding_by_field) != fields:
            raise ValueError("geo projection dependency binding fields are invalid")
        for field in ("schema_name", "table_name"):
            if type(binding_by_field[field]) is not str:
                raise ValueError("geo projection dependency identifier is invalid")
            _sql_identifier(binding_by_field[field], field_name=field)
        if any(
            type(binding_by_field[field]) is not int or not 0 < binding_by_field[field] < 2**32
            for field in ("relation_oid", "relfilenode")
        ):
            raise ValueError("geo projection dependency identity is invalid")
        bindings_by_name[name] = binding_by_field
    relation_oids = {binding["relation_oid"] for binding in bindings_by_name.values()}
    physical_names = {(binding["schema_name"], binding["table_name"]) for binding in bindings_by_name.values()}
    if len(relation_oids) != len(expected_names) or len(physical_names) != len(expected_names):
        raise ValueError("geo projection dependency identities are duplicated")
    return bindings_by_name


def projection_dependency_relation_sql(schema_name: str, canonical_name: str, *, dependency_bindings=None) -> str:
    """Render only a reviewed canonical dependency or its validated held relation."""

    schema_name = _sql_identifier(schema_name, field_name="address schema")
    expected_names = {f"{schema or schema_name}.{table}" for schema, table in _PROJECTION_DEPENDENCIES}
    if canonical_name not in expected_names:
        raise ValueError("geo projection dependency name is invalid")
    if dependency_bindings is None:
        return canonical_name
    binding = validate_projection_dependency_bindings(schema_name, dependency_bindings)[canonical_name]
    return f'"{binding["schema_name"]}"."{binding["table_name"]}"'


def projection_dependency_bindings_match_sql(schema_name: str, dependency_bindings) -> str:
    """Recapture exact physical identities after the caller locks every held heap."""

    bindings_by_name = validate_projection_dependency_bindings(schema_name, dependency_bindings)
    checks = []
    for name, binding in bindings_by_name.items():
        relation = projection_dependency_relation_sql(schema_name, name, dependency_bindings=bindings_by_name)
        checks.append(
            f"EXISTS (SELECT 1 FROM pg_catalog.pg_class held WHERE held.oid=to_regclass('{relation}') "
            f"AND held.oid={binding['relation_oid']} AND pg_relation_filenode(held.oid)={binding['relfilenode']} "
            "AND held.relkind='r' AND held.relpersistence='p' AND NOT held.relispartition "
            "AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_inherits WHERE inhrelid=held.oid OR inhparent=held.oid))"
        )
    return " AND ".join(checks)


def projection_relation_signature_sql(schema_name: str, *, dependency_bindings=None) -> str:
    """Return the exact relation identity used by one stored projection."""

    schema_name = _sql_identifier(schema_name, field_name="address schema")
    signature_fields: list[str] = []
    for dependency_schema, table_name in _PROJECTION_DEPENDENCIES:
        relation_schema = dependency_schema or schema_name
        qualified_name = f"{relation_schema}.{table_name}"
        physical_name = projection_dependency_relation_sql(
            schema_name, qualified_name, dependency_bindings=dependency_bindings
        )
        signature_fields.extend(
            (
                repr(qualified_name),
                "jsonb_build_array("
                f"COALESCE(to_regclass('{physical_name}')::oid::bigint, -1), "
                "COALESCE(pg_relation_filenode("
                f"to_regclass('{physical_name}'))::bigint, -1))",
            )
        )
    return f"jsonb_build_object({', '.join(signature_fields)})"


def projection_dependency_lock_sql(schema_name: str, *, dependency_bindings=None) -> str:
    """Hold every swap-published input stable through projection receipt."""

    schema_name = _sql_identifier(schema_name, field_name="address schema")
    if dependency_bindings is not None:
        bindings_by_name = validate_projection_dependency_bindings(schema_name, dependency_bindings)
        relations = [
            "ONLY " + projection_dependency_relation_sql(schema_name, name, dependency_bindings=bindings_by_name)
            for name in sorted(bindings_by_name)
        ]
        return f"LOCK TABLE {', '.join(relations)} IN ACCESS SHARE MODE;"
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


def projection_external_evidence_ctes_sql(schema_name: str, *, dependency_bindings=None) -> str:
    """Build set-wise NPPES and MRF evidence against canonical or held inputs."""

    npi_relation = projection_dependency_relation_sql(
        schema_name, f"{schema_name}.npi_address", dependency_bindings=dependency_bindings
    )
    mrf_relation = projection_dependency_relation_sql(
        schema_name, f"{schema_name}.mrf_address", dependency_bindings=dependency_bindings
    )
    return f""" projection_nppes AS MATERIALIZED (
        SELECT DISTINCT projection_target.npi, projection_target.address_key
          FROM projection_targets AS projection_target
          JOIN {npi_relation} AS source_nppes
            ON source_nppes.npi = projection_target.npi
           AND source_nppes.address_key = projection_target.address_key
           AND source_nppes.date_added IS NOT NULL
         WHERE (projection_target.address_source_mask & 1) <> 0
           AND projection_target.address_key IS NOT NULL
    ), projection_mrf AS MATERIALIZED (
        SELECT DISTINCT projection_target.npi, projection_target.address_key
          FROM projection_targets AS projection_target
          JOIN {mrf_relation} AS source_mrf
            ON source_mrf.npi = projection_target.npi
           AND source_mrf.address_key = projection_target.address_key
           AND {independent_issuer_sql('source_mrf.source_issuer_names')}
           AND {mrf_lineage_complete_sql('source_mrf')}
         WHERE projection_target.address_key IS NOT NULL
    ),"""


def projection_cms_anchor_ctes_sql(schema_name: str, stage_table: str, *, dependency_bindings=None) -> str:
    """Build CMS target premises and their durable NPPES anchors."""

    schema_name = _sql_identifier(schema_name, field_name="address schema")
    stage_table = _sql_identifier(stage_table, field_name="address stage")
    npi_relation = projection_dependency_relation_sql(
        schema_name, f"{schema_name}.npi_address", dependency_bindings=dependency_bindings
    )
    return f""" projection_cms_premises AS MATERIALIZED (
        SELECT DISTINCT projection_target.npi, projection_target.premise_key
          FROM projection_targets AS projection_target
         WHERE (projection_target.address_source_mask & 4) <> 0
           AND projection_target.address_key IS NOT NULL
           AND projection_target.premise_key IS NOT NULL
    ), projection_nppes_anchors AS MATERIALIZED (
        SELECT DISTINCT requested.npi, requested.premise_key
          FROM projection_cms_premises AS requested
          JOIN {schema_name}.{stage_table} AS candidate
            ON candidate.npi = requested.npi
           AND candidate.premise_key = requested.premise_key
           AND (candidate.address_source_mask & 1) <> 0
           AND candidate.type IN ('primary', 'secondary', 'practice', 'site')
          JOIN {npi_relation} AS anchor_source
            ON anchor_source.npi = candidate.npi
           AND anchor_source.address_key = candidate.address_key
           AND anchor_source.date_added IS NOT NULL
    ),"""


def projection_cms_evidence_cte_sql(schema_name: str, *, dependency_bindings=None) -> str:
    """Build CMS evidence admitted keys from source rows and anchors."""

    cms_relation = projection_dependency_relation_sql(
        schema_name, f"{schema_name}.doctor_clinician_address", dependency_bindings=dependency_bindings
    )
    return f""" projection_cms AS MATERIALIZED (
        SELECT DISTINCT projection_target.location_key
          FROM projection_targets AS projection_target
          JOIN {cms_relation} AS source_doctor
            ON source_doctor.npi = projection_target.npi
           AND source_doctor.address_key = projection_target.address_key
           AND source_doctor.updated_at IS NOT NULL
          JOIN projection_nppes_anchors AS anchor
            ON anchor.npi = projection_target.npi
           AND anchor.premise_key = projection_target.premise_key
         WHERE (projection_target.address_source_mask & 4) <> 0
           AND projection_target.address_key IS NOT NULL
           AND projection_target.premise_key IS NOT NULL
    ),"""


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
    "projection_dependency_bindings_match_sql",
    "projection_dependency_relation_sql",
    "projection_external_evidence_ctes_sql",
    "projection_cms_anchor_ctes_sql",
    "projection_cms_evidence_cte_sql",
    "projection_relation_signature_sql",
    "projection_state_available_sql",
    "validate_projection_dependency_bindings",
]
