# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Read the complete desired address overlay before creating any serving scratch."""

from __future__ import annotations

import json
from dataclasses import dataclass

from sqlalchemy import String, bindparam, text
from sqlalchemy.dialects import postgresql

from process.provider_directory_address_overlay_components import (
    ADDRESS_OVERLAY_COMPONENTS,
    ADDRESS_OVERLAY_DUPLICATE_ORDER,
    address_overlay_alias_columns,
    address_overlay_archive_coordinate_predicate,
    address_overlay_component_select_sql,
    address_overlay_existing_select_sql,
    address_overlay_formatted_columns,
)
from process.provider_directory_cms_desired_fence import resolve_desired_fence
from process.provider_directory_cms_native_inputs import capture_native_address_input_fence
from process.provider_directory_cms_preparation import desired_fence_hash
from process.provider_directory_cms_serving_receipt import capture_native_dependencies

_INPUT_TYPES = {
    "organization_table": "Organization",
    "practitioner_table": "Practitioner",
    "location_table": "Location",
    "healthcare_service_table": "HealthcareService",
    "practitioner_role_table": "PractitionerRole",
    "affiliation_table": "OrganizationAffiliation",
}


@dataclass(frozen=True)
class DesiredOverlayProjection:
    """Internal read-only SQL tied to the exact desired source and native input fences."""

    statement_sql: str
    native_address_input_hash: str
    desired_fence_hash: str


def _literal_query(statement: str, values: dict) -> str:
    """Render only engine-derived bind values, keeping identifiers and SQL out of request payloads."""
    parameters = [
        bindparam(name, value=value, type_=postgresql.ARRAY(String) if isinstance(value, list) else String)
        for name, value in values.items()
    ]
    return str(
        text(statement)
        .bindparams(*parameters)
        .compile(dialect=postgresql.dialect(paramstyle="named"), compile_kwargs={"literal_binds": True})
    )


def _resource_cte(fhir, schema, fence, resource_type):
    """Project exactly the retained resource families materialized by the full artifact scope."""
    model = fhir.RESOURCE_MODELS_BY_TYPE[resource_type]
    datasets = [dataset for dataset in fence.datasets if resource_type in dataset.artifact_resources]
    query = _literal_query(
        fhir._provider_directory_artifact_resource_select_sql(model, schema),
        {
            "source_ids": [dataset.source_id for dataset in datasets],
            "dataset_ids": [dataset.dataset_id for dataset in datasets],
            "evidence_run_ids": [dataset.evidence_run_id for dataset in datasets],
            "resource_type": resource_type,
        },
    )
    name = fhir._q("desired_overlay_" + model.__tablename__)
    columns = ", ".join(fhir._q(column.name) for column in model.__table__.columns)
    return name, f"{name} ({columns}) AS MATERIALIZED ({query})"


def _columns(fhir, alias, replacements_by_column=None):
    """Keep the physical overlay column order through every virtual transform."""
    replacements_by_column = replacements_by_column or {}
    return ", ".join(
        f"{replacements_by_column.get(column, f'{alias}.{column}')} AS {column}"
        for column in fhir._provider_directory_address_overlay_columns()
    )


def _component_query(fhir, context, component):
    """Use the real component mapping, including CMS work-address and postal-code rules."""
    query = address_overlay_component_select_sql(fhir.ADDRESS_OVERLAY_COMPONENT_INSERT_TEMPLATES[component], context)
    replacements_by_column = {
        "premise_key": "NULL::uuid",
        "formatted_address": "NULL::varchar",
        "formatted_address_version": "NULL::smallint",
        "formatted_address_source": "NULL::varchar",
        "published_at": "component_rows.published_at::timestamp",
        "source_updated_at": "component_rows.source_updated_at::timestamp",
    }
    return f"SELECT {_columns(fhir, 'component_rows', replacements_by_column)} FROM ({query}) component_rows"


def _input_ctes(fhir, schema, fence):
    """Replace all six component inputs with exact immutable desired dataset SELECTs."""
    context = fhir._address_overlay_sql_context(schema, None, None)
    ctes = []
    for field, resource_type in _INPUT_TYPES.items():
        name, statement = _resource_cte(fhir, schema, fence, resource_type)
        context[field] = name
        ctes.append(statement)
    # The full composite bundle requests every selected source and no single run filter.
    # These scope tables contain only those selected immutable rows.
    context["component_scope"] = ""
    component_queries = [_component_query(fhir, context, component) for component in ADDRESS_OVERLAY_COMPONENTS]
    source_filter = _literal_query(
        "source_id = ANY(CAST(:source_ids AS varchar[]))",
        {
            "source_ids": [dataset.source_id for dataset in fence.datasets],
        },
    )
    incumbent = address_overlay_existing_select_sql(
        fhir._unscoped_qt(schema, fhir.PROVIDER_DIRECTORY_ADDRESS_OVERLAY_TABLE),
        ", ".join(fhir._provider_directory_address_overlay_columns()),
        source_filter,
    )
    ctes.append("overlay_input AS MATERIALIZED (" + " UNION ALL ".join([incumbent, *component_queries]) + ")")
    return ctes, source_filter


def _country_cte(fhir):
    """Match the full-stage normalization, preserving unrecognized incumbent country text."""
    country = fhir._country_restore_sql("stage_row.country_code")
    replacements_by_column = {
        "country_code": f"CASE WHEN NULLIF({country}, '') IS NOT NULL THEN {country} ELSE stage_row.country_code END"
    }
    return (
        f"overlay_countries AS MATERIALIZED (SELECT {_columns(fhir, 'stage_row', replacements_by_column)} "
        "FROM overlay_input stage_row)"
    )


def _alias_ctes(fhir, schema):
    """Validate the exact native alias contract and use its shared rewrite assignments."""
    aliases = fhir._unscoped_qt(schema, fhir.address_alias_sql.ADDRESS_ALIAS_TABLE)
    archive = fhir._qt(schema, "address_archive_v2")
    violation = (
        fhir._ADDRESS_OVERLAY_ALIAS_VIOLATION_SQL.format(
            quoted_schema=fhir._q(schema),
            stage_ref="overlay_countries",
            aliases=aliases,
            archive=archive,
        )
        .strip()
        .removesuffix(";")
    )
    replacements_by_column = {
        column: f"CASE WHEN active.source_address_key IS NOT NULL THEN {value} ELSE stage_row.{column} END"
        for column, value in address_overlay_alias_columns().items()
    }
    return [
        f"overlay_alias_violation AS MATERIALIZED ({violation})",
        """overlay_alias_valid AS MATERIALIZED (SELECT
            (CASE WHEN EXISTS (SELECT 1 FROM overlay_alias_violation)
                  THEN 'provider_directory_overlay_alias_integrity_violation' ELSE '1' END)::integer AS accepted)""",
        f"""overlay_aliases AS MATERIALIZED (SELECT {_columns(fhir, "stage_row", replacements_by_column)}
            FROM overlay_countries stage_row
            LEFT JOIN {aliases} active ON active.source_address_key=stage_row.address_key AND active.revoked_at IS NULL
            LEFT JOIN {archive} target ON target.address_key=active.target_address_key AND target.merged_into IS NULL)""",
    ]


def _archive_cte(fhir, schema, source_filter):
    """Apply the same premise replacement, scoped formatter and coordinate eligibility."""
    formatter = address_overlay_formatted_columns(
        f"{fhir._q(schema)}.{fhir.ADDRESS_FORMAT_FUNCTION}",
        fhir.ADDRESS_FORMAT_VERSION,
        fhir.ADDRESS_FORMAT_SOURCE,
    )
    replacements_by_column = {
        column: f"CASE WHEN stage_row.address_key IS NOT NULL AND ({source_filter}) "
        f"THEN {value} ELSE stage_row.{column} END"
        for column, value in formatter.items()
    }
    replacements_by_column["premise_key"] = (
        "CASE WHEN stage_row.address_key IS NOT NULL THEN archive.premise_key ELSE stage_row.premise_key END"
    )
    for column in ("lat", "long"):
        replacements_by_column[column] = (
            f"CASE WHEN {address_overlay_archive_coordinate_predicate()} "
            f"THEN COALESCE(stage_row.{column}, archive.{column}) ELSE stage_row.{column} END"
        )
    return f"""overlay_hydrated AS MATERIALIZED (SELECT {_columns(fhir, "stage_row", replacements_by_column)}
        FROM overlay_aliases stage_row
        LEFT JOIN {fhir._qt(schema, "address_archive_v2")} archive
          ON archive.address_key=stage_row.address_key AND archive.merged_into IS NULL)"""


def desired_overlay_statement_sql(fhir, schema, fence):
    """Assemble a complete virtual overlay using the same desired scope and post-processing SQL."""
    ctes, source_filter = _input_ctes(fhir, schema, fence)
    ctes.extend([_country_cte(fhir), *_alias_ctes(fhir, schema), _archive_cte(fhir, schema, source_filter)])
    ctes.append(f"""overlay_ranked AS (SELECT overlay_hydrated.*, row_number() OVER (
        PARTITION BY source_record_id ORDER BY {ADDRESS_OVERLAY_DUPLICATE_ORDER}) AS duplicate_rank
        FROM overlay_hydrated WHERE {source_filter})""")
    columns = ", ".join(fhir._provider_directory_address_overlay_columns())
    # OFFSET validates aliases before a downstream filter can produce zero rows.
    return (
        "WITH "
        + ",\n".join(ctes)
        + f"""
        SELECT {columns} FROM overlay_ranked WHERE duplicate_rank=1
        UNION ALL SELECT {columns} FROM overlay_hydrated WHERE NOT ({source_filter})
        OFFSET (SELECT accepted - 1 FROM overlay_alias_valid)"""
    )


async def assert_desired_overlay_inputs(fhir, execution, fence, address):
    """Check selection and native dependencies inside the caller's existing read snapshot."""
    binding = fhir.db._transaction_binding()
    if binding is None or fhir._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.get():
        raise RuntimeError("cms_overlay_projection_requires_unmodified_read_snapshot")
    snapshot = (
        (
            await binding.session.execute(
                text("""SELECT
        current_setting('transaction_isolation') AS isolation,
        current_setting('transaction_read_only') AS read_only""")
            )
        )
        .mappings()
        .one()
    )
    if snapshot["isolation"] not in ("repeatable read", "serializable") or snapshot["read_only"] != "on":
        raise RuntimeError("cms_overlay_projection_requires_unmodified_read_snapshot")
    expected = json.loads(address.input_json)
    if (
        address.fhir is not fhir
        or address.execution is not execution
        or desired_fence_hash(fence) != expected["desired_fence_hash"]
        or address._current_input(fence, expected) != address.input_json
    ):
        raise RuntimeError("cms_overlay_projection_inputs_changed")
    refreshed = await resolve_desired_fence(fhir, execution)
    fhir._assert_artifact_fence_selection_unchanged(fence, refreshed)
    if desired_fence_hash(refreshed) != expected["desired_fence_hash"]:
        raise RuntimeError("cms_overlay_projection_desired_fence_changed")
    actual = await capture_native_dependencies(binding.session, fhir._schema())
    native = await capture_native_address_input_fence(binding.session, fhir._schema())
    alias_generation = await fhir._address_alias_generation(fhir._schema())
    if (
        actual != expected["native_dependencies"]
        or native != expected["native_input_fence"]
        or alias_generation != actual["alias_generation"]
    ):
        raise RuntimeError("cms_overlay_projection_native_inputs_changed")


async def _projection_in_snapshot(fhir, execution, fence, address):
    """Return SQL only after both sides of construction agree on all consumed inputs."""
    await assert_desired_overlay_inputs(fhir, execution, fence, address)
    statement = desired_overlay_statement_sql(fhir, fhir._schema(), fence)
    await assert_desired_overlay_inputs(fhir, execution, fence, address)
    return DesiredOverlayProjection(statement, address.input_hash, desired_fence_hash(fence))


async def desired_overlay_projection(fhir, execution, fence, address):
    """Prepare an internal read-only projection; it grants no storage or publication admission."""
    if fhir.db._transaction_binding() is not None:
        return await _projection_in_snapshot(fhir, execution, fence, address)
    async with fhir.db.transaction() as session:
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
        return await _projection_in_snapshot(fhir, execution, fence, address)
