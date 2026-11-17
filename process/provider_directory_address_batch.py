# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Reuse distinct native address fields throughout an ordinary overlay build."""

from contextlib import asynccontextmanager
from contextvars import ContextVar

from process.ext import address_batch_stage as batches
from process.ext.address_format import ADDRESS_FORMAT_SOURCE, ADDRESS_FORMAT_VERSION

_ACTIVE = ContextVar("provider_directory_address_batch", default=None)


def current():
    """Return this build's prepared exact-input address relation."""
    return _ACTIVE.get()


def _location_inputs(fhir, schema, source_ids):
    country = fhir._country_restore_default_us_sql("loc.country_code")
    scope = "WHERE loc.source_id = ANY(CAST(:address_batch_sources AS varchar[]))" if source_ids else ""
    return (
        "SELECT loc.first_line, loc.second_line, loc.city_name, "
        "COALESCE(NULLIF(loc.state_name,''),loc.state_code) AS state_name, loc.postal_code, "
        f"{country} AS country_code FROM {fhir._qt(schema, 'provider_directory_location')} loc {scope}"
    )


def _json_inputs(fhir, schema, run_id, source_ids, component):
    is_practitioner = component == "practitioner_address"
    table = "provider_directory_practitioner" if is_practitioner else "provider_directory_organization"
    column = "addresses" if is_practitioner else "address_json"
    postal = "NULLIF(TRIM(addr.value->>'postalCode'),'')"
    if is_practitioner:
        postal = (
            "COALESCE(CASE WHEN item.source_id='cms-npd' THEN NULLIF(TRIM(addr.value->>'postal_code'),'') END,"
            + postal
            + ")"
        )
    country = fhir._country_restore_default_us_sql("addr.value->>'country'")
    alias = "practitioner" if is_practitioner else "organization"
    scope = fhir._provider_directory_overlay_scope_filter(alias, run_id=run_id, source_ids=source_ids)
    return (
        f"WITH eligible AS MATERIALIZED (SELECT {alias}.source_id,{alias}.{column} "
        f"FROM {fhir._qt(schema, table)} {alias} WHERE {alias}.npi BETWEEN 1000000000 AND 9999999999 "
        f"AND {alias}.active IS DISTINCT FROM false {scope}) "
        "SELECT NULLIF(TRIM(addr.value->'line'->>0),'') AS first_line, "
        "NULLIF(TRIM(addr.value->'line'->>1),'') AS second_line, "
        "NULLIF(TRIM(addr.value->>'city'),'') AS city_name, "
        f"NULLIF(TRIM(addr.value->>'state'),'') AS state_name, {postal} AS postal_code, "
        f"{country} AS country_code FROM eligible item "
        f"CROSS JOIN LATERAL jsonb_array_elements(COALESCE(item.{column}::jsonb,'[]'::jsonb)) addr(value)"
    )


@asynccontextmanager
async def scope(fhir, schema, run_id, source_ids, components):
    """Prepare a visible cache without holding the control run's progress transaction."""
    if fhir.overlay_keys._native_location_batch(fhir) is None:
        yield
        return
    inputs = (
        [_location_inputs(fhir, schema, source_ids)]
        if fhir.overlay_keys.LOCATION_KEYED_COMPONENTS.intersection(components)
        else []
    )
    inputs.extend(
        _json_inputs(fhir, schema, run_id, source_ids, component)
        for component in ("organization_address", "practitioner_address")
        if component in components
    )
    table = fhir._address_overlay_stage_table_name(fhir.overlay_keys._BUILD_SCOPE.get()[0] or run_id)
    async with batches.staged_address_fields(
        fhir.db,
        schema,
        table,
        " UNION ALL ".join(f"({query})" for query in inputs),
        parameters={"address_batch_sources": source_ids, "source_ids": source_ids, "run_id": run_id},
        committed=True,
    ) as relation:
        token = _ACTIVE.set(relation)
        try:
            yield
        finally:
            _ACTIVE.reset(token)


def _lookup(alias):
    return batches.lookup_key_sql([f"{alias}.{name}" for name in batches.ADDRESS_FIELDS])


def key_fields_context(quoted_schema):
    """Select exact native fields while keeping SQL for unsupported input tuples."""
    inputs = ",".join(f"key_parts.{field}" for field in batches.ADDRESS_FIELDS)
    key = batches.field_sql("batch_fields", "address_key", f"{quoted_schema}.addr_key_v1({inputs})::text")
    state = batches.field_sql("batch_fields", "state_code", f"{quoted_schema}.addr_state_code_v1(key_parts.state_name)")
    return {
        "address_fields_table": current() or "(SELECT NULL::text lookup_key,NULL::jsonb native_fields WHERE FALSE)",
        "batch_address_key": f"({key})::uuid",
        "batch_state_code": f"({state})::varchar",
    }


def component_insert_sql(fhir, context, select_sql):
    """Write prepared labels during the component INSERT instead of rewriting each row."""
    relation = current()
    if relation is None:
        return f"INSERT INTO {context['stage_ref']} ({context['columns']}) {select_sql};"
    columns = context["columns"] + ", formatted_address, formatted_address_version, formatted_address_source"
    fallback = (
        f"{context['qschema']}.{fhir.ADDRESS_FORMAT_FUNCTION}("
        + ",".join(f"address_rows.{name}" for name in batches.ADDRESS_FIELDS)
        + ")"
    )
    label = batches.field_sql("address_fields", "formatted_address", fallback)
    return (
        f"INSERT INTO {context['stage_ref']} ({columns}) SELECT address_rows.*, {label}, "
        f"{ADDRESS_FORMAT_VERSION}::smallint, '{ADDRESS_FORMAT_SOURCE}'::varchar "
        f"FROM ({select_sql}) address_rows LEFT JOIN {relation} address_fields "
        f"ON address_fields.lookup_key={_lookup('address_rows')};"
    )


async def stage_location_keys(fhir, schema, key_table, source_ids, relation):
    """Reuse prepared keys under the existing Location row-version guard."""
    context = fhir._address_overlay_sql_context(schema, None, None)
    country = context["location_country_expr"]
    fields = [
        "loc.first_line",
        "loc.second_line",
        "loc.city_name",
        "COALESCE(NULLIF(loc.state_name,''),loc.state_code)",
        "loc.postal_code",
        country,
    ]
    source_scope = "AND loc.source_id = ANY(CAST(:source_ids AS varchar[]))" if source_ids else ""
    result = await fhir.db.status(
        f"INSERT INTO {fhir._unscoped_qt(schema, key_table)} (source_id,resource_id,address_key,row_version) "
        "SELECT loc.source_id,loc.resource_id,address_fields.native_fields->>'address_key',loc.xmin::text "
        f"FROM {context['location_table']} loc JOIN {relation} address_fields "
        f"ON address_fields.lookup_key={batches.lookup_key_sql(fields)} "
        f"WHERE (loc.address_key ~* '{context['uuid_re']}') IS NOT TRUE "
        f"AND address_fields.native_fields IS NOT NULL {source_scope}",
        **({"source_ids": source_ids} if source_ids else {}),
    )
    return fhir._coerce_rowcount(result)


async def backfill_formatted(fhir, schema, stage_ref, source_ids):
    """Refresh native labels and render only distinct unsupported input tuples."""
    relation = current()
    if relation is None:
        return None
    scope = "AND stage_row.source_id = ANY(CAST(:formatted_source_ids AS varchar[]))" if source_ids else ""
    parameters = {"formatted_source_ids": source_ids} if source_ids else {}
    label = "address_fields.native_fields->>'formatted_address'"
    result = await fhir.db.status(
        f"UPDATE {stage_ref} stage_row SET formatted_address={label}, "
        f"formatted_address_version={ADDRESS_FORMAT_VERSION}, formatted_address_source='{ADDRESS_FORMAT_SOURCE}' "
        f"FROM {relation} address_fields WHERE stage_row.address_key IS NOT NULL {scope} "
        f"AND address_fields.lookup_key={_lookup('stage_row')} AND address_fields.native_fields IS NOT NULL "
        f"AND ROW(stage_row.formatted_address,stage_row.formatted_address_version,stage_row.formatted_address_source) "
        f"IS DISTINCT FROM ROW({label},{ADDRESS_FORMAT_VERSION}::smallint,'{ADDRESS_FORMAT_SOURCE}'::varchar)",
        **parameters,
    )
    fallback_result = await fhir.db.status(
        _fallback_formatted_sql(fhir, schema, stage_ref, relation, scope), **parameters
    )
    return fhir._coerce_rowcount(result) + fhir._coerce_rowcount(fallback_result)


def _fallback_formatted_sql(fhir, schema, stage_ref, relation, scope):
    fields = ",".join(f"stage_row.{field}" for field in batches.ADDRESS_FIELDS)
    renderer = f"{fhir._q(schema)}.{fhir.ADDRESS_FORMAT_FUNCTION}({','.join(batches.ADDRESS_FIELDS)})"
    # Materialization prevents the planner from rendering native-accepted rows before the anti-join.
    return (
        f'WITH fallback_inputs AS MATERIALIZED (SELECT DISTINCT ON ({_lookup("stage_row")} COLLATE "C") '
        f"{fields} FROM {stage_ref} stage_row "
        f"WHERE stage_row.address_key IS NOT NULL {scope} AND NOT EXISTS "
        f"(SELECT FROM {relation} address_fields WHERE address_fields.lookup_key={_lookup('stage_row')} "
        "AND address_fields.native_fields IS NOT NULL)), "
        f"rendered AS MATERIALIZED (SELECT {batches.lookup_key_sql(batches.ADDRESS_FIELDS)} AS lookup_key, "
        f"{renderer} AS formatted_address FROM fallback_inputs) "
        f"UPDATE {stage_ref} stage_row SET formatted_address=rendered.formatted_address, "
        f"formatted_address_version={ADDRESS_FORMAT_VERSION}, formatted_address_source='{ADDRESS_FORMAT_SOURCE}' "
        f"FROM rendered WHERE stage_row.address_key IS NOT NULL {scope} "
        f"AND rendered.lookup_key={_lookup('stage_row')} "
        "AND ROW(stage_row.formatted_address,stage_row.formatted_address_version,stage_row.formatted_address_source) "
        f"IS DISTINCT FROM ROW(rendered.formatted_address,{ADDRESS_FORMAT_VERSION}::smallint,"
        f"'{ADDRESS_FORMAT_SOURCE}'::varchar)"
    )


async def _finish_stage(fhir, schema, stage_table, stage_ref, source_ids, metrics):
    metrics["countries_normalized"] = await fhir._normalize_address_overlay_stage_countries(stage_ref)
    metrics.update(await fhir._materialize_address_overlay_aliases(schema, stage_ref))
    metrics.update(await fhir._hydrate_address_overlay_stage_from_archive(schema, stage_ref, source_ids=source_ids))
    selected = metrics["components"]
    resources = (
        None
        if set(selected) == set(fhir.ADDRESS_OVERLAY_COMPONENTS)
        else [fhir.ADDRESS_OVERLAY_COMPONENT_RESOURCE_TYPES[component] for component in selected]
    )
    metrics["duplicates_removed"] = await fhir._dedupe_address_overlay_stage(
        stage_ref, source_ids=source_ids, resource_types=resources
    )
    metrics["stage_rows"] = int(await fhir.db.scalar(f"SELECT COUNT(*) FROM {stage_ref};") or 0)
    await fhir._create_address_overlay_stage_indexes(schema, stage_table)
    await fhir.db.status(f"ANALYZE {stage_ref};")
    return metrics


async def populate(fhir, schema, stage_table, stage_ref, run_id, source_ids, query_parameters, components):
    """Populate the same candidate and preserve all post-processing and validation."""
    selected = fhir._clean_address_overlay_components(components)
    resources = (
        None
        if set(selected) == set(fhir.ADDRESS_OVERLAY_COMPONENTS)
        else [fhir.ADDRESS_OVERLAY_COMPONENT_RESOURCE_TYPES[component] for component in selected]
    )
    async with scope(fhir, schema, run_id, source_ids, selected):
        copied = await fhir._copy_existing_address_overlay(
            stage_ref,
            fhir._qt(schema, fhir.PROVIDER_DIRECTORY_ADDRESS_OVERLAY_TABLE),
            ", ".join(fhir._provider_directory_address_overlay_columns()),
            source_ids,
            refresh_resource_types=resources,
        )
        inserted, seconds = await fhir._insert_address_overlay_components(
            schema, stage_table, run_id, source_ids, query_parameters, selected
        )
        return await _finish_stage(
            fhir,
            schema,
            stage_table,
            stage_ref,
            source_ids,
            {
                "copied_existing": copied,
                "inserted": sum(inserted.values()),
                "inserted_by_component": inserted,
                "component_seconds": seconds,
                "components": selected,
            },
        )
