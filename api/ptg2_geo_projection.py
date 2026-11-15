# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""SQL shared by staged and transitional provider geo assurance."""

from __future__ import annotations

import re
from collections.abc import Mapping
from uuid import UUID

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
    """Decode the closed old map edition; physical tokens remain opaque compatibility data."""

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
        bindings_by_name[name] = binding_by_field
    qualified_names = {(binding["schema_name"], binding["table_name"]) for binding in bindings_by_name.values()}
    if len(qualified_names) != len(expected_names):
        raise ValueError("geo projection dependency identities are duplicated")
    return bindings_by_name


def validate_projection_dependency_qnames(schema_name: str, bindings: object) -> dict:
    """Copy the closed QName family for named reads, without assigning projection freshness."""
    if type(schema_name) is not str:
        raise ValueError("geo projection dependency identifier is invalid")
    schema_name = _sql_identifier(schema_name, field_name="address schema")
    expected_names = {f"{schema or schema_name}.{table}" for schema, table in _PROJECTION_DEPENDENCIES}
    if not isinstance(bindings, Mapping) or set(bindings) != expected_names:
        raise ValueError("geo projection dependency name family is invalid")
    bindings_by_name = {}
    for name in sorted(expected_names):
        supplied = bindings[name]
        if not isinstance(supplied, Mapping) or set(supplied) != {"schema_name", "table_name"}:
            raise ValueError("geo projection dependency name fields are invalid")
        binding_by_field = dict(supplied)
        for field, value in binding_by_field.items():
            if type(value) is not str:
                raise ValueError("geo projection dependency identifier is invalid")
            _sql_identifier(value, field_name=field)
        bindings_by_name[name] = binding_by_field
    if len({(value["schema_name"], value["table_name"]) for value in bindings_by_name.values()}) != len(expected_names):
        raise ValueError("geo projection dependency names are duplicated")
    return bindings_by_name


ProjectionRelationSignature = tuple[tuple[str, object, object], ...]


def projection_relation_signature_tuple(
    schema_name: str, value: object, *, allow_opaque_legacy=False
) -> ProjectionRelationSignature:
    """Read six name pairs and an optional canonical NPI origin, without assigning freshness."""
    signature_by_name = value
    schema_name = _sql_identifier(schema_name, field_name="address schema")
    expected_names = {f"{schema or schema_name}.{table}" for schema, table in _PROJECTION_DEPENDENCIES}
    generation_name = f"{schema_name}.npi_result_generation"
    if not isinstance(signature_by_name, Mapping) or set(signature_by_name) not in (
        expected_names,
        expected_names | {generation_name},
    ):
        raise ValueError("geo projection dependency signature is invalid")
    pairs_by_name = {name: signature_by_name[name] for name in expected_names}
    if any(not isinstance(pair, (list, tuple)) or len(pair) != 2 for pair in pairs_by_name.values()):
        raise ValueError("geo projection dependency signature is invalid")
    if all(type(component) is str for pair in pairs_by_name.values() for component in pair):
        validate_projection_dependency_qnames(
            schema_name, {name: {"schema_name": pair[0], "table_name": pair[1]} for name, pair in pairs_by_name.items()}
        )
    elif not allow_opaque_legacy and not all(
        type(component) is int and component > 0 for pair in pairs_by_name.values() for component in pair
    ):
        raise ValueError("geo projection dependency signature is invalid")
    if generation_name in signature_by_name:
        pairs_by_name[generation_name] = _npi_signature_pair(schema_name, signature_by_name)
    return tuple(sorted((name, pair[0], pair[1]) for name, pair in pairs_by_name.items()))


def projection_named_relation_signature(schema_name, value, dependency_bindings=None):
    """Normalize old declarations to names; preserve only an explicitly supplied logical origin."""
    signature = projection_relation_signature_tuple(schema_name, value, allow_opaque_legacy=True)
    generation_name = f"{schema_name}.npi_result_generation"
    relations = tuple(entry for entry in signature if entry[0] != generation_name)
    if all(type(namespace) is str and type(table) is str for _, namespace, table in relations):
        return signature
    selected = (
        {name: {"schema_name": name.split(".", 1)[0], "table_name": name.split(".", 1)[1]} for name, _, _ in relations}
        if dependency_bindings is None
        else {
            name: {field: binding[field] for field in ("schema_name", "table_name")}
            for name, binding in dependency_bindings.items()
        }
    )
    named = validate_projection_dependency_qnames(schema_name, selected)
    return tuple((name, binding["schema_name"], binding["table_name"]) for name, binding in sorted(named.items()))


def _is_named_binding_family(dependency_bindings) -> bool:
    return isinstance(dependency_bindings, Mapping) and all(
        isinstance(binding, Mapping) and set(binding) == {"schema_name", "table_name"}
        for binding in dependency_bindings.values()
    )


def projection_dependency_relation_sql(schema_name: str, canonical_name: str, *, dependency_bindings=None) -> str:
    """Render only a reviewed canonical dependency or its validated held relation."""

    schema_name = _sql_identifier(schema_name, field_name="address schema")
    expected_names = {f"{schema or schema_name}.{table}" for schema, table in _PROJECTION_DEPENDENCIES}
    if canonical_name not in expected_names:
        raise ValueError("geo projection dependency name is invalid")
    if dependency_bindings is None:
        return canonical_name
    validator = (
        validate_projection_dependency_qnames
        if _is_named_binding_family(dependency_bindings)
        else validate_projection_dependency_bindings
    )
    binding = validator(schema_name, dependency_bindings)[canonical_name]
    return f'"{binding["schema_name"]}"."{binding["table_name"]}"'


def projection_named_dependency_lock_sql(schema_name: str, dependency_bindings) -> str:
    """Pin canonical roots recursively and separate held leaves independently."""
    bindings = validate_projection_dependency_qnames(schema_name, dependency_bindings)
    relations = ", ".join(
        (
            ""
            if name in {"tiger.zip_state", "tiger.zcta5"} and name == f"{value['schema_name']}.{value['table_name']}"
            else "ONLY "
        )
        + f'"{value["schema_name"]}"."{value["table_name"]}"'
        for name, value in bindings.items()
    )
    return f"LOCK TABLE {relations} IN ACCESS SHARE MODE;"


def projection_named_dependency_match_sql(schema_name: str, dependency_bindings) -> str:
    """Check current native shape; logical generation and data validation govern freshness."""
    bindings = validate_projection_dependency_qnames(schema_name, dependency_bindings)
    checks = []
    for name, binding in bindings.items():
        relation = f'"{binding["schema_name"]}"."{binding["table_name"]}"'
        checks.append(
            f"EXISTS (SELECT 1 FROM pg_catalog.pg_class held WHERE held.oid=to_regclass('{relation}') "
            "AND held.relkind='r' AND held.relpersistence='p' AND NOT held.relispartition "
            "AND NOT held.relrowsecurity AND NOT held.relforcerowsecurity "
            f"AND {(_canonical_tiger_inheritance_sql() if name in {'tiger.zip_state', 'tiger.zcta5'} and name == binding['schema_name'] + '.' + binding['table_name'] else _held_inheritance_sql(name))})"
        )
    return " AND ".join(checks)


def projection_dependency_bindings_match_sql(schema_name: str, dependency_bindings) -> str:
    """Check saved logical names and native shape, ignoring the old numeric token values."""
    validator = (
        validate_projection_dependency_qnames
        if _is_named_binding_family(dependency_bindings)
        else validate_projection_dependency_bindings
    )
    saved_bindings = validator(schema_name, dependency_bindings)
    names_by_dependency = {
        name: {field: binding[field] for field in ("schema_name", "table_name")}
        for name, binding in saved_bindings.items()
    }
    return projection_named_dependency_match_sql(schema_name, names_by_dependency)


def _held_inheritance_sql(canonical_name):
    """A TIGER held leaf may inherit only its fixed canonical extension parent."""
    allowed_parent = (
        f" AND inhparent IS DISTINCT FROM to_regclass('{canonical_name}')"
        if canonical_name in {"tiger.zip_state", "tiger.zcta5"}
        else ""
    )
    return (
        "NOT EXISTS(SELECT 1 FROM pg_catalog.pg_inherits WHERE inhparent=held.oid "
        f"OR (inhrelid=held.oid{allowed_parent}))"
    )


def projection_relation_signature_sql(schema_name: str, *, dependency_bindings=None, include_generation=True) -> str:
    """Describe only the complete declared dependency QNames, for either saved map edition."""
    schema_name = _sql_identifier(schema_name, field_name="address schema")
    if dependency_bindings is None:
        bindings_by_name = {
            f"{schema or schema_name}.{table}": {"schema_name": schema or schema_name, "table_name": table}
            for schema, table in _PROJECTION_DEPENDENCIES
        }
    else:
        validator = (
            validate_projection_dependency_qnames
            if _is_named_binding_family(dependency_bindings)
            else validate_projection_dependency_bindings
        )
        bindings_by_name = validator(schema_name, dependency_bindings)
    fields = []
    for name, binding in sorted(bindings_by_name.items()):
        fields.extend((repr(name), f"jsonb_build_array({binding['schema_name']!r}, {binding['table_name']!r})"))
    signature = f"jsonb_build_object({', '.join(fields)})"
    selected_npi = bindings_by_name[f"{schema_name}.npi_address"]
    if (
        include_generation
        and selected_npi["schema_name"] == schema_name
        and selected_npi["table_name"] == "npi_address"
    ):
        signature += (
            f" || jsonb_build_object('{schema_name}.npi_result_generation',({_npi_generation_sql(schema_name)}))"
        )
    return signature


def projection_dependency_lock_sql(schema_name: str, *, dependency_bindings=None) -> str:
    """Hold every swap-published input stable through projection receipt."""

    schema_name = _sql_identifier(schema_name, field_name="address schema")
    if dependency_bindings is not None:
        validator = (
            validate_projection_dependency_qnames
            if _is_named_binding_family(dependency_bindings)
            else validate_projection_dependency_bindings
        )
        bindings_by_name = validator(schema_name, dependency_bindings)
        bindings_by_name = {
            name: {field: binding[field] for field in ("schema_name", "table_name")}
            for name, binding in bindings_by_name.items()
        }
        return projection_named_dependency_lock_sql(schema_name, bindings_by_name)
    relations = (
        f"{dependency_schema or schema_name}.{table_name}" for dependency_schema, table_name in _PROJECTION_DEPENDENCIES
    )
    return f"LOCK TABLE {', '.join(relations)} IN ACCESS SHARE MODE;"


def projection_stored_bindings_sql(schema_name: str, column: str) -> tuple[str, str]:
    """Read either closed edition while current maps use only their declared names."""
    schema_name = _sql_identifier(schema_name, field_name="address schema")
    if column not in {"active_dependency_bindings", "candidate_dependency_bindings"}:
        raise ValueError("geo projection binding column is invalid")
    signature, matches = _stored_named_bindings_sql(schema_name, column)
    legacy_signature, legacy_matches = _legacy_stored_bindings_sql(schema_name, column)
    return (
        f"CASE WHEN {matches} THEN {signature} ELSE ({legacy_signature}) END",
        f"(({matches}) OR ({legacy_matches}))",
    )


def _stored_named_bindings_sql(schema_name, column, *, legacy=False):
    """Require the complete finite names and native read shape, without physical tokens."""
    names = [f"{schema or schema_name}.{table}" for schema, table in _PROJECTION_DEPENDENCIES]
    allowed_fields = (
        "'schema_name','table_name','relation_oid','relfilenode'" if legacy else "'schema_name','table_name'"
    )
    legacy_shape = " AND {entry} ? 'relation_oid' AND {entry} ? 'relfilenode'" if legacy else ""
    safe_object = f"CASE WHEN jsonb_typeof({column})='object' THEN {column} ELSE '{{}}'::jsonb END"
    checks = [
        f"(SELECT count(*) FROM jsonb_object_keys({safe_object}))=6",
        f"(SELECT count(DISTINCT (value->>'schema_name',value->>'table_name')) FROM jsonb_each({safe_object}))=6",
    ]
    fields = []
    for name in names:
        entry = f"({column}->'{name}')"
        relation = (
            f"CASE WHEN {entry}->>'schema_name' ~ '^[A-Za-z_][A-Za-z0-9_]{{0,62}}$' "
            f"AND {entry}->>'table_name' ~ '^[A-Za-z_][A-Za-z0-9_]{{0,62}}$' "
            f"THEN to_regclass(format('%I.%I',{entry}->>'schema_name',{entry}->>'table_name')) END"
        )
        fields.extend((repr(name), f"jsonb_build_array({entry}->>'schema_name', {entry}->>'table_name')"))
        checks.append(
            f"(jsonb_typeof({entry})='object' AND "
            f"{entry} - ARRAY[{allowed_fields}]='{{}}'::jsonb "
            f"AND jsonb_typeof({entry}->'schema_name')='string' "
            f"AND jsonb_typeof({entry}->'table_name')='string' "
            f"{legacy_shape.format(entry=entry)} "
            f"AND EXISTS (SELECT 1 FROM pg_catalog.pg_class held WHERE held.oid=({relation}) "
            "AND held.relkind='r' AND held.relpersistence='p' AND NOT held.relispartition "
            "AND NOT held.relrowsecurity AND NOT held.relforcerowsecurity "
            f"AND (CASE WHEN {entry}->>'schema_name'||'.'||({entry}->>'table_name')='{name}' "
            f"AND '{name}' IN ('tiger.zip_state','tiger.zcta5') THEN {_canonical_tiger_inheritance_sql()} "
            f"ELSE {_held_inheritance_sql(name)} END)))"
        )
    return f"jsonb_build_object({', '.join(fields)})", "(" + " AND ".join(checks) + ")"


def _legacy_stored_bindings_sql(schema_name: str, column: str) -> tuple[str, str]:
    """Project old maps by recorded names; immutable old numeric signatures stay opaque."""
    signature, matches = _stored_named_bindings_sql(schema_name, column, legacy=True)
    canonical = projection_relation_signature_sql(schema_name, include_generation=False)
    present = " AND ".join(
        f"to_regclass('{schema or schema_name}.{table}') IS NOT NULL" for schema, table in _PROJECTION_DEPENDENCIES
    )
    return (
        f"CASE WHEN {column} IS NULL THEN {canonical} ELSE {signature} END",
        f"(({column} IS NULL AND ({present}) AND NOT ({retained_tiger_children_sql()})) OR {matches})",
    )


def retained_tiger_children_sql() -> str:
    """Require explicit selection when fixed TIGER roots include retained child QNames."""
    return """EXISTS (
        SELECT 1 FROM pg_catalog.pg_inherits edge
        JOIN pg_catalog.pg_class child ON child.oid=edge.inhrelid
        JOIN pg_catalog.pg_namespace namespace ON namespace.oid=child.relnamespace
        WHERE edge.inhparent IN (to_regclass('tiger.zip_state'),to_regclass('tiger.zcta5'))
          AND namespace.nspname ~ '^reference_family_archive_[0-9a-f]{32}$'
    )"""


def projection_state_available_sql(schema_name: str, *, dependency_bindings=None, has_canonical_generation=True) -> str:
    """Require current logical NPI publication while held inputs use their immutable names."""
    schema_name = _sql_identifier(schema_name, field_name="address schema")
    signature, binding_matches = projection_stored_bindings_sql(schema_name, "active_dependency_bindings")
    generation_matches = projection_npi_generation_match_sql(
        schema_name,
        "active",
        dependency_bindings=dependency_bindings,
        has_canonical_generation=has_canonical_generation,
    )
    return f"""EXISTS (SELECT 1 FROM {schema_name}.{GEO_ASSURANCE_STATE_TABLE} AS geo_assurance_state
        WHERE singleton IS TRUE AND active_geo_assurance_version = {GEO_ASSURANCE_VERSION}
        AND {binding_matches} AND active_relation_signature - '{schema_name}.npi_result_generation' = ({signature})
        AND {generation_matches}
        AND EXISTS(SELECT 1 FROM pg_class live WHERE live.oid=to_regclass('{schema_name}.entity_address_unified')
          AND live.relkind='r' AND live.relpersistence='p' AND NOT live.relrowsecurity AND NOT live.relforcerowsecurity))"""


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
           AND {independent_issuer_sql("source_mrf.source_issuer_names")}
           AND {mrf_lineage_complete_sql("source_mrf")}
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
            {nonblank_array_value_sql(f"{alias}.source_import_ids")}
            OR {alias}.date_added IS NOT NULL
        )
        AND (
            {nonblank_array_value_sql(f"{alias}.source_import_dates")}
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


def _legacy_nppes_evidence_sql(alias: str, schema_name: str, *, dependency_bindings=None) -> str:
    """Build the original record-complete NPPES predicate."""

    relation = projection_dependency_relation_sql(
        schema_name, f"{schema_name}.npi_address", dependency_bindings=dependency_bindings
    )
    return f"""(
        ({alias}.address_source_mask & 1) <> 0
        AND {alias}.address_key IS NOT NULL
        AND EXISTS (
            SELECT 1
              FROM {relation} AS geo_nppes
             WHERE geo_nppes.npi = {alias}.npi
               AND geo_nppes.address_key = {alias}.address_key
               AND geo_nppes.date_added IS NOT NULL
        )
    )"""


def _legacy_mrf_evidence_sql(alias: str, schema_name: str, *, dependency_bindings=None) -> str:
    """Build the original multi-issuer MRF predicate."""

    relation = projection_dependency_relation_sql(
        schema_name, f"{schema_name}.mrf_address", dependency_bindings=dependency_bindings
    )
    return f"""EXISTS (
        SELECT 1
          FROM {relation} AS geo_mrf
         WHERE geo_mrf.npi = {alias}.npi
           AND geo_mrf.address_key = {alias}.address_key
           AND {independent_issuer_sql("geo_mrf.source_issuer_names")}
           AND {mrf_lineage_complete_sql("geo_mrf")}
    )"""


def _legacy_cms_evidence_sql(
    alias: str,
    schema_name: str,
    unified_table_name: str,
    *,
    dependency_bindings=None,
    unified_schema_name: str | None = None,
) -> str:
    """Build the original CMS predicate with its exact NPPES anchor."""

    doctor_relation = projection_dependency_relation_sql(
        schema_name, f"{schema_name}.doctor_clinician_address", dependency_bindings=dependency_bindings
    )
    npi_relation = projection_dependency_relation_sql(
        schema_name, f"{schema_name}.npi_address", dependency_bindings=dependency_bindings
    )
    unified_relation = f"{schema_name}.{unified_table_name}"
    if unified_schema_name is not None:
        unified_schema_name = _sql_identifier(unified_schema_name, field_name="unified address schema")
        unified_relation = f'"{unified_schema_name}"."{unified_table_name}"'
    return f"""(
        ({alias}.address_source_mask & 4) <> 0
        AND {alias}.address_key IS NOT NULL
        AND {alias}.premise_key IS NOT NULL
        AND EXISTS (
            SELECT 1
              FROM {doctor_relation} AS geo_doctor
             WHERE geo_doctor.npi = {alias}.npi
               AND geo_doctor.address_key = {alias}.address_key
               AND geo_doctor.updated_at IS NOT NULL
        )
        AND EXISTS (
            SELECT 1
              FROM {unified_relation} AS geo_nppes_anchor
              JOIN {npi_relation} AS geo_nppes_anchor_source
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
    dependency_bindings=None,
    unified_schema_name: str | None = None,
) -> str:
    """Return the exact pre-projection evidence classifier for one address."""

    alias = _sql_identifier(alias, field_name="unified address alias")
    schema_name = _sql_identifier(schema_name, field_name="address schema")
    unified_table_name = _sql_identifier(
        unified_table_name,
        field_name="unified address table",
    )
    return evidence_source_id_case_sql(
        nppes_condition_sql=_legacy_nppes_evidence_sql(alias, schema_name, dependency_bindings=dependency_bindings),
        mrf_condition_sql=_legacy_mrf_evidence_sql(alias, schema_name, dependency_bindings=dependency_bindings),
        cms_condition_sql=_legacy_cms_evidence_sql(
            alias,
            schema_name,
            unified_table_name,
            dependency_bindings=dependency_bindings,
            unified_schema_name=unified_schema_name,
        ),
    )


def projected_evidence_level_sql(
    alias: str,
    *,
    schema_name: str,
    legacy_level_sql: str,
) -> str:
    """Recheck source semantics until a portable logical freshness proof is available."""
    _sql_identifier(alias, field_name="unified address alias")
    _sql_identifier(schema_name, field_name="address schema")
    return f"({legacy_level_sql})"


def projected_evidence_available_sql(alias: str, *, schema_name: str) -> str:
    """Named metadata alone cannot authorize skipping current source evidence."""
    _sql_identifier(alias, field_name="unified address alias")
    _sql_identifier(schema_name, field_name="address schema")
    return "FALSE"


def projected_boolean_sql(
    alias: str,
    column_name: str,
    *,
    schema_name: str,
    legacy_sql: str,
) -> str:
    """Recheck mutable postal inputs while retaining the selected immutable TIGER point."""
    alias = _sql_identifier(alias, field_name="unified address alias")
    column_name = _sql_identifier(column_name, field_name="geo assurance column")
    schema_name = _sql_identifier(schema_name, field_name="address schema")
    if column_name != "geo_point_coherent":
        return f"({legacy_sql})"
    return f"""CASE
        WHEN {alias}.geo_assurance_version = {GEO_ASSURANCE_VERSION}
         AND {alias}.{column_name} IS NOT NULL
         AND {_read_projection_available_sql(schema_name)}
         AND EXISTS (
             SELECT 1 FROM {schema_name}.{GEO_ASSURANCE_STATE_TABLE} point_state
             WHERE point_state.singleton IS TRUE
               AND point_state.active_dependency_bindings->'tiger.zcta5'->>'schema_name'
                   ~ '^reference_family_archive_[0-9a-f]{{32}}$'
               AND point_state.active_dependency_bindings->'tiger.zcta5'->>'table_name' = 'zcta5'
               AND point_state.active_dependency_bindings->'tiger.zip_state'->>'schema_name'
                   = point_state.active_dependency_bindings->'tiger.zcta5'->>'schema_name'
               AND point_state.active_dependency_bindings->'tiger.zip_state'->>'table_name' = 'zip_state'
         )
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
    "projection_named_dependency_lock_sql",
    "projection_named_dependency_match_sql",
    "projection_external_evidence_ctes_sql",
    "projection_cms_anchor_ctes_sql",
    "projection_cms_evidence_cte_sql",
    "projection_relation_signature_sql",
    "projection_state_available_sql",
    "validate_projection_dependency_bindings",
    "validate_projection_dependency_qnames",
]


def _canonical_tiger_inheritance_sql():
    """Keep ordinary geocoder descendants while excluding retained archive children by name."""
    return """NOT EXISTS(SELECT 1 FROM pg_catalog.pg_inherits edge
        JOIN pg_catalog.pg_class child ON child.oid=edge.inhrelid
        JOIN pg_catalog.pg_namespace namespace ON namespace.oid=child.relnamespace
        WHERE edge.inhparent=held.oid AND namespace.nspname ~ '^reference_family_archive_[0-9a-f]{32}$')
        AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_inherits WHERE inhrelid=held.oid)"""


def _npi_generation_sql(schema_name):
    """Read only the portable origin columns shared by ordinary and adopted publications."""
    return f"SELECT jsonb_build_array(origin_lineage_id::text,origin_generation) FROM {schema_name}.npi_result_generation WHERE origin_lineage_id IS NOT NULL AND origin_generation > 0"


def projection_npi_generation_match_sql(schema_name, phase, *, dependency_bindings=None, has_canonical_generation=True):
    """Require the captured canonical origin without writing another profile's geo state."""
    schema_name = _sql_identifier(schema_name, field_name="address schema")
    if phase not in {"active", "candidate"}:
        raise ValueError("geo projection phase is invalid")
    binding = f"{phase}_dependency_bindings->'{schema_name}.npi_address'"
    if dependency_bindings is not None:
        selected = dependency_bindings[f"{schema_name}.npi_address"]
        npi_binding_by_field = {field: selected[field] for field in ("schema_name", "table_name")}
        validate_projection_dependency_qnames(
            schema_name,
            {
                name: {field: entry[field] for field in ("schema_name", "table_name")}
                for name, entry in dependency_bindings.items()
            },
        )
        if npi_binding_by_field != {"schema_name": schema_name, "table_name": "npi_address"}:
            return f"({binding}->>'schema_name'='{npi_binding_by_field['schema_name']}' AND {binding}->>'table_name'='{npi_binding_by_field['table_name']}' AND NOT ({phase}_relation_signature ? '{schema_name}.npi_result_generation'))"
    held = f"({binding}->>'schema_name'<>'{schema_name}' OR {binding}->>'table_name'<>'npi_address')"
    if not has_canonical_generation:
        return held
    return f"({held} OR ({phase}_relation_signature->'{schema_name}.npi_result_generation')=({_npi_generation_sql(schema_name)}))"


def _read_projection_available_sql(schema_name):
    """Use pinned bindings; outside a metadata snapshot, retain live evidence evaluation."""
    from api.provider_profile_snapshot import snapshot_geo_dependency_bindings, snapshot_relation_available

    available = snapshot_relation_available(f"{schema_name}.npi_result_generation")
    if available is None:
        return "FALSE"
    return projection_state_available_sql(
        schema_name,
        dependency_bindings=snapshot_geo_dependency_bindings(schema_name),
        has_canonical_generation=available,
    )


def _npi_signature_pair(schema_name, signature_by_name):
    """Validate only an explicitly captured canonical origin beside the six name pairs."""
    token = signature_by_name[f"{schema_name}.npi_result_generation"]
    npi_pair = signature_by_name[f"{schema_name}.npi_address"]
    if not isinstance(token, (list, tuple)) or len(token) != 2 or tuple(npi_pair) != (schema_name, "npi_address"):
        raise ValueError("geo projection NPI origin is invalid")
    lineage, generation = token
    if (
        type(lineage) is not str
        or str(UUID(lineage)) != lineage
        or type(generation) is not int
        or not 0 < generation < 2**63
    ):
        raise ValueError("geo projection NPI origin is invalid")
    return token
