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
    """Copy the fixed named input map and discard legacy physical metadata."""

    schema_name = _sql_identifier(schema_name, field_name="address schema")
    expected_names = {f"{schema or schema_name}.{table}" for schema, table in _PROJECTION_DEPENDENCIES}
    fields = {"schema_name", "table_name"}
    if not isinstance(bindings, Mapping) or set(bindings) != expected_names:
        raise ValueError("geo projection dependency binding family is invalid")
    bindings_by_name = {}
    for name in sorted(expected_names):
        supplied_binding = bindings[name]
        if not isinstance(supplied_binding, Mapping):
            raise ValueError("geo projection dependency binding fields are invalid")
        binding_by_field = dict(supplied_binding)
        if set(binding_by_field) not in (fields, fields | {"relation_oid", "relfilenode"}):
            raise ValueError("geo projection dependency binding fields are invalid")
        for field in ("schema_name", "table_name"):
            if type(binding_by_field[field]) is not str:
                raise ValueError("geo projection dependency identifier is invalid")
            _sql_identifier(binding_by_field[field], field_name=field)
        bindings_by_name[name] = {field: binding_by_field[field] for field in fields}
    physical_names = {(binding["schema_name"], binding["table_name"]) for binding in bindings_by_name.values()}
    if len(physical_names) != len(expected_names):
        raise ValueError("geo projection dependency names are duplicated")
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
    """Validate persistent input layout after the caller locks its named heaps."""

    bindings_by_name = validate_projection_dependency_bindings(schema_name, dependency_bindings)
    checks = []
    for name in bindings_by_name:
        relation = projection_dependency_relation_sql(schema_name, name, dependency_bindings=bindings_by_name)
        binding = bindings_by_name[name]
        inheritance = (
            _canonical_tiger_inheritance_sql()
            if name in {"tiger.zip_state", "tiger.zcta5"}
            and f"{binding['schema_name']}.{binding['table_name']}" == name
            else _held_inheritance_sql(name)
        )
        checks.append(
            f"EXISTS (SELECT 1 FROM pg_catalog.pg_class held WHERE held.oid=to_regclass('{relation}') "
            "AND held.relkind='r' AND held.relpersistence='p' AND NOT held.relispartition "
            "AND NOT held.relrowsecurity AND NOT held.relforcerowsecurity "
            f"AND {inheritance})"
        )
    return " AND ".join(checks)


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


def _canonical_tiger_inheritance_sql():
    """Ordinary geocoder children remain readable; retained snapshots need explicit leaf bindings."""
    return """NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_inherits edge
        JOIN pg_catalog.pg_class child ON child.oid=edge.inhrelid
        JOIN pg_catalog.pg_namespace namespace ON namespace.oid=child.relnamespace
        WHERE edge.inhparent=held.oid
          AND namespace.nspname ~ '^reference_family_archive_[0-9a-f]{32}$'
    )"""


def projection_relation_signature_sql(schema_name: str, *, dependency_bindings=None) -> str:
    """Bind canonical NPI inputs to their portable logical publication generation."""

    schema_name = _sql_identifier(schema_name, field_name="address schema")
    signature_fields: list[str] = []
    for dependency_schema, table_name in _PROJECTION_DEPENDENCIES:
        relation_schema = dependency_schema or schema_name
        qualified_name = f"{relation_schema}.{table_name}"
        binding = (
            {"schema_name": relation_schema, "table_name": table_name}
            if dependency_bindings is None
            else validate_projection_dependency_bindings(schema_name, dependency_bindings)[qualified_name]
        )
        fields = f"'schema_name','{binding['schema_name']}','table_name','{binding['table_name']}'"
        if table_name == "npi_address" and binding == {"schema_name": schema_name, "table_name": table_name}:
            fields += f",'generation',({_npi_generation_sql(schema_name)})"
        signature_fields.extend((repr(qualified_name), f"jsonb_build_object({fields})"))
    return f"jsonb_build_object({', '.join(signature_fields)})"


def _npi_generation_sql(schema_name: str) -> str:
    """Read only the origin fields shared by ordinary and adopted NPI publications."""
    return (
        "SELECT jsonb_build_object('origin_lineage_id',origin_lineage_id::text,"
        "'origin_generation',origin_generation) "
        f"FROM {schema_name}.npi_result_generation "
        "WHERE origin_lineage_id IS NOT NULL AND origin_generation > 0"
    )


def validate_projection_relation_signature(schema_name: str, signature_by_name, *, dependency_bindings=None) -> dict:
    """Reject unversioned canonical evidence; retained named inputs need no live token."""
    if not isinstance(signature_by_name, Mapping):
        raise ValueError("geo projection dependency signature is invalid")
    if signature_by_name and all(
        isinstance(entry, (list, tuple)) and len(entry) == 2 for entry in signature_by_name.values()
    ):
        if dependency_bindings is None or set(signature_by_name) != set(dependency_bindings):
            raise ValueError("legacy geo projection dependencies require named held inputs")
        signature_by_name = dependency_bindings
    names_by_key = {
        name: {field: field_value for field, field_value in entry.items() if field != "generation"}
        if isinstance(entry, Mapping)
        else entry
        for name, entry in signature_by_name.items()
    }
    bindings = validate_projection_dependency_bindings(schema_name, names_by_key)
    npi_name = f"{schema_name}.npi_address"
    generation = signature_by_name[npi_name].get("generation")
    is_canonical_npi = bindings[npi_name] == {"schema_name": schema_name, "table_name": "npi_address"}
    if any("generation" in signature_by_name[name] for name in signature_by_name if name != npi_name):
        raise ValueError("geo projection generation belongs only to canonical NPI")
    if not is_canonical_npi:
        if generation is not None:
            raise ValueError("held geo projection inputs cannot carry a canonical NPI generation")
        return bindings
    if not isinstance(generation, Mapping) or set(generation) != {"origin_lineage_id", "origin_generation"}:
        raise ValueError("canonical geo projection requires its NPI origin generation")
    lineage, number = generation["origin_lineage_id"], generation["origin_generation"]
    if (
        not isinstance(lineage, str)
        or str(UUID(lineage)) != lineage
        or type(number) is not int
        or not 0 < number < 2**63
    ):
        raise ValueError("geo projection NPI origin generation is invalid")
    bindings[npi_name]["generation"] = dict(generation)
    return bindings


def projection_npi_generation_match_sql(
    schema_name: str, phase: str, *, dependency_bindings=None, has_canonical_generation=True
) -> str:
    """Compare canonical evidence without coupling an NPI writer to geo-state writes."""
    schema_name = _sql_identifier(schema_name, field_name="address schema")
    if phase not in {"active", "candidate"}:
        raise ValueError("geo projection phase is invalid")
    binding = f"{phase}_dependency_bindings->'{schema_name}.npi_address'"
    if dependency_bindings is not None:
        selected = validate_projection_dependency_bindings(schema_name, dependency_bindings)[
            f"{schema_name}.npi_address"
        ]
        if selected != {"schema_name": schema_name, "table_name": "npi_address"}:
            return (
                f"({binding}->>'schema_name'='{selected['schema_name']}' "
                f"AND {binding}->>'table_name'='{selected['table_name']}')"
            )
    held = f"({binding}->>'schema_name'<>'{schema_name}' OR {binding}->>'table_name'<>'npi_address')"
    if not has_canonical_generation:
        return held
    token = f"{phase}_relation_signature->'{schema_name}.npi_address'->'generation'"
    return f"({held} OR ({token})=({_npi_generation_sql(schema_name)}))"


def projection_dependency_lock_sql(schema_name: str, *, dependency_bindings=None) -> str:
    """Hold every swap-published input stable through projection receipt."""

    schema_name = _sql_identifier(schema_name, field_name="address schema")
    if dependency_bindings is not None:
        bindings_by_name = validate_projection_dependency_bindings(schema_name, dependency_bindings)
        relations = []
        for name in sorted(bindings_by_name):
            binding = bindings_by_name[name]
            is_canonical_tiger = (
                name in {"tiger.zip_state", "tiger.zcta5"}
                and f"{binding['schema_name']}.{binding['table_name']}" == name
            )
            relations.append(
                ("" if is_canonical_tiger else "ONLY ")
                + projection_dependency_relation_sql(schema_name, name, dependency_bindings=bindings_by_name)
            )
        return f"LOCK TABLE {', '.join(relations)} IN ACCESS SHARE MODE;"
    relations = (
        f"{dependency_schema or schema_name}.{table_name}" for dependency_schema, table_name in _PROJECTION_DEPENDENCIES
    )
    return f"LOCK TABLE {', '.join(relations)} IN ACCESS SHARE MODE;"


def projection_stored_bindings_sql(schema_name: str, column: str) -> tuple[str, str]:
    """Read named persisted inputs and validate layout without physical identity checks."""
    schema_name = _sql_identifier(schema_name, field_name="address schema")
    if column not in {"active_dependency_bindings", "candidate_dependency_bindings"}:
        raise ValueError("geo projection binding column is invalid")
    names = [f"{schema or schema_name}.{table}" for schema, table in _PROJECTION_DEPENDENCIES]
    safe_object = f"CASE WHEN jsonb_typeof({column})='object' THEN {column} ELSE '{{}}'::jsonb END"
    checks = [
        f"(SELECT count(*) FROM jsonb_object_keys({safe_object}))=6",
        "(SELECT count(DISTINCT jsonb_build_array(value->>'schema_name',value->>'table_name')) "
        f"FROM jsonb_each({safe_object}))=6",
    ]
    pairs = []
    for name in names:
        entry = f"({column}->'{name}')"
        inheritance = _held_inheritance_sql(name)
        if name in {"tiger.zip_state", "tiger.zcta5"}:
            inheritance = (
                f"CASE WHEN {entry}->>'schema_name'='tiger' AND {entry}->>'table_name'='{name.split('.')[1]}' "
                f"THEN {_canonical_tiger_inheritance_sql()} ELSE {inheritance} END"
            )
        physical = (
            f"CASE WHEN {entry}->>'schema_name' ~ '^[A-Za-z_][A-Za-z0-9_]{{0,62}}$' "
            f"AND {entry}->>'table_name' ~ '^[A-Za-z_][A-Za-z0-9_]{{0,62}}$' "
            f"THEN to_regclass(format('%I.%I',{entry}->>'schema_name',{entry}->>'table_name')) END"
        )
        pairs.extend(
            (
                repr(name),
                f"jsonb_build_object('schema_name',{entry}->>'schema_name','table_name',{entry}->>'table_name')",
            )
        )
        checks.append(
            f"(jsonb_typeof({entry})='object' AND "
            f"{entry} - ARRAY['schema_name','table_name','relation_oid','relfilenode']='{{}}'::jsonb "
            f"AND jsonb_typeof({entry}->'schema_name')='string' "
            f"AND jsonb_typeof({entry}->'table_name')='string' "
            f"AND EXISTS (SELECT 1 FROM pg_catalog.pg_class held WHERE held.oid=({physical}) "
            "AND held.relkind='r' AND held.relpersistence='p' AND NOT held.relispartition "
            "AND NOT held.relrowsecurity AND NOT held.relforcerowsecurity "
            f"AND ({inheritance})))"
        )
    signature = (
        f"CASE WHEN {column} IS NULL THEN {projection_relation_signature_sql(schema_name)} "
        f"ELSE jsonb_build_object({', '.join(pairs)}) END"
    )
    canonical_bindings_by_name = {
        name: dict(zip(("schema_name", "table_name"), name.split("."), strict=True)) for name in names
    }
    canonical_matches = projection_dependency_bindings_match_sql(schema_name, canonical_bindings_by_name)
    return signature, f"(({column} IS NULL AND ({canonical_matches})) OR ({' AND '.join(checks)}))"


def projection_state_available_sql(schema_name: str, *, dependency_bindings=None, has_canonical_generation=True) -> str:
    """Require the active projection to match every published dependency."""
    from api.provider_profile_snapshot import snapshot_geo_dependency_bindings, snapshot_relation_available

    schema_name = _sql_identifier(schema_name, field_name="address schema")
    if dependency_bindings is None:
        dependency_bindings = snapshot_geo_dependency_bindings(schema_name)
    if snapshot_relation_available(f"{schema_name}.npi_result_generation") is False:
        has_canonical_generation = False
    generation_matches = projection_npi_generation_match_sql(
        schema_name,
        "active",
        dependency_bindings=dependency_bindings,
        has_canonical_generation=has_canonical_generation,
    )
    if snapshot_relation_available(f"{schema_name}.{GEO_ASSURANCE_STATE_TABLE}") is False:
        return "FALSE"
    live_table = f"{schema_name}.entity_address_unified"
    state_relation = f"{schema_name}.{GEO_ASSURANCE_STATE_TABLE}"
    _signature, binding_matches = projection_stored_bindings_sql(schema_name, "active_dependency_bindings")
    return f"""EXISTS (
        SELECT 1
          FROM {state_relation} AS geo_assurance_state
         WHERE geo_assurance_state.singleton IS TRUE
           AND geo_assurance_state.active_geo_assurance_version = {GEO_ASSURANCE_VERSION}
           AND EXISTS (SELECT 1 FROM pg_catalog.pg_class held WHERE held.oid=to_regclass('{live_table}')
               AND held.relkind='r' AND held.relpersistence='p'
               AND NOT held.relrowsecurity AND NOT held.relforcerowsecurity)
           AND {binding_matches}
           AND {generation_matches}
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
    """Prefer a valid stored evidence class and otherwise evaluate legacy SQL."""

    alias = _sql_identifier(alias, field_name="unified address alias")
    projected_level = evidence_level_from_source_id_sql(f"{alias}.geo_evidence_source_id")
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
        f" AND {_read_projection_available_sql(schema_name)})"
    )


def _read_projection_available_sql(schema_name):
    """Use live evidence when the read has not pinned readable logical metadata."""
    from api.provider_profile_snapshot import snapshot_relation_available

    return projection_state_available_sql(
        schema_name,
        has_canonical_generation=snapshot_relation_available(f"{schema_name}.npi_result_generation") is True,
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
         AND {_read_projection_available_sql(schema_name)}
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
