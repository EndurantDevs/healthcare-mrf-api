# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Read a partial refresh's current overlay once before NPI enrichment."""

from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field

from db.plan_release_storage import storage_relation
from process import entity_address_preparation_admission as admission
from process import provider_directory_generic_source_policy as source_policy
from process.ext.address_batch_stage import lookup_key_sql
from process.uhc_flex_practitioner_async_safety import drain_operation

_ACTIVE = ContextVar("entity_address_partial_projection", default=None)
_FIELDS = (
    "source_record_id",
    "source_id",
    "last_seen_run_id",
    "resource_type",
    "resource_id",
    "npi",
    "address_key",
    "first_line",
    "second_line",
    "city_name",
    "state_name",
    "state_code",
    "postal_code",
    "country_code",
    "telephone_number",
    "fax_number",
    "formatted_address",
    "lat",
    "long",
    "source_updated_at",
    "published_at",
)
_RAW_FIELDS = ("first_line", "second_line", "city_name", "state_name", "postal_code", "country_code")


@dataclass
class _Projection:
    relation: str
    scope_relation: str
    context_target: str
    source_ids: tuple[str, ...]
    run_id: str | None
    context_relation: str | None = None
    created_relations: list[str] = field(default_factory=list)


def _desired_scope_sql(native, schema, inputs, source_ids, run_id):
    preparation = native.candidate_preparation
    selected = f"WHERE source_id=ANY({native._string_array_literal(list(source_ids))})" if source_ids else ""
    run_filter = f"WHERE dataset.run_id={native._sql_literal(run_id)}" if run_id else ""
    return f"""WITH {preparation._pins_cte(inputs)},desired_datasets AS MATERIALIZED (
        {preparation._desired_datasets_sql(schema, inputs.ordinary_serving_peer_pins)}
    ), requested_endpoints AS MATERIALIZED (
        SELECT DISTINCT endpoint_id FROM desired_dataset_pins {selected}
    ), datasets AS MATERIALIZED (
        SELECT DISTINCT endpoint_id,dataset_id,run_id FROM desired_datasets
    ) SELECT alias.source_id,dataset.endpoint_id,dataset.dataset_id,dataset.run_id
        FROM desired_dataset_pins alias JOIN requested_endpoints requested USING(endpoint_id)
        JOIN datasets dataset ON dataset.endpoint_id=alias.endpoint_id {run_filter}"""


def _scope_sql(native, schema, source_ids=None, run_id=None):
    inputs = native.candidate_preparation.current()
    if inputs is not None:
        return _desired_scope_sql(native, schema, inputs, source_ids, run_id)
    source_relation = storage_relation(schema, "provider_directory_source")
    datasets = storage_relation(schema, "provider_directory_endpoint_dataset")
    selected = (
        f"source.source_id=ANY({native._string_array_literal(list(source_ids))})"
        if source_ids
        else "source.endpoint_id IS NOT NULL"
    )
    run_filter = (
        f"AND COALESCE(dataset.acquisition_root_run_id,dataset.import_run_id)={native._sql_literal(run_id)}"
        if run_id
        else ""
    )
    return f"""WITH requested_endpoints AS MATERIALIZED (
        SELECT DISTINCT source.endpoint_id FROM {source_relation} source
        WHERE {selected} AND source.endpoint_id IS NOT NULL
    ), current_endpoint_counts AS MATERIALIZED (
        SELECT endpoint_id FROM {datasets} WHERE is_current IS TRUE
        GROUP BY endpoint_id HAVING count(*)=1
    ) SELECT sibling.source_id,dataset.endpoint_id,dataset.dataset_id,
        COALESCE(dataset.acquisition_root_run_id,dataset.import_run_id) AS run_id
    FROM requested_endpoints requested JOIN {source_relation} sibling USING(endpoint_id)
    JOIN {datasets} dataset ON dataset.endpoint_id=requested.endpoint_id
    JOIN current_endpoint_counts unique_current ON unique_current.endpoint_id=dataset.endpoint_id
    WHERE dataset.is_current IS TRUE AND dataset.status='published'
        AND dataset.published_at IS NOT NULL AND dataset.superseded_at IS NULL
        AND COALESCE(dataset.acquisition_root_run_id,dataset.import_run_id) IS NOT NULL {run_filter}"""


def _projected_fields(alias):
    projection_by_field = {
        "city_name": f"COALESCE({alias}.city_name,'')::varchar",
        "state_name": f"COALESCE({alias}.state_name,{alias}.state_code,'')::varchar",
        "country_code": f"COALESCE(NULLIF({alias}.country_code,''),'US')::varchar",
    }
    return ",".join(f"{projection_by_field.get(field, alias + '.' + field)} AS {field}" for field in _FIELDS)


def _selected_scope(native, relation, source_ids, run_id):
    filters = []
    if source_ids:
        filters.append(
            f"scope.endpoint_id IN (SELECT endpoint_id FROM {relation} WHERE source_id=ANY({native._string_array_literal(list(source_ids))}))"
        )
    if run_id:
        filters.append(f"scope.run_id={native._sql_literal(run_id)}")
    return f"SELECT scope.* FROM {relation} scope" + (" WHERE " + " AND ".join(filters) if filters else "")


def _raw_sql(native, schema, *, source_ids=None, run_id=None, affected_group_table=None, exclude=None):
    overlay = storage_relation(schema, native.candidate_preparation.table_name("provider_directory_address_overlay"))
    members = storage_relation(schema, "provider_directory_dataset_resource")
    active = _ACTIVE.get()
    selected = (
        _selected_scope(native, active.scope_relation, source_ids, run_id)
        if active
        else _scope_sql(native, schema, source_ids, run_id)
    )
    ctes = [f"dataset_scope AS MATERIALIZED ({selected})"]
    excluded = ""
    if exclude is not None:
        refreshed = _selected_scope(native, exclude.scope_relation, exclude.source_ids, exclude.run_id)
        ctes.append(f"refresh_scope AS MATERIALIZED ({refreshed})")
        excluded = "AND NOT EXISTS(SELECT FROM refresh_scope refreshed WHERE refreshed.source_id=overlay.source_id AND refreshed.run_id=overlay.last_seen_run_id AND refreshed.dataset_id=scope.dataset_id)"
    scope_match = "overlay.source_id=scope.source_id AND overlay.last_seen_run_id=scope.run_id"
    if affected_group_table:
        groups = storage_relation(schema, affected_group_table)
        ctes.append(
            f"affected_npis AS MATERIALIZED (SELECT DISTINCT entity_npi FROM {groups} WHERE entity_npi IS NOT NULL)"
        )
        driving = "affected_npis affected"
        lookup = "overlay.npi=affected.entity_npi"
        scope_join = f"JOIN dataset_scope scope ON {scope_match}"
    else:
        driving, lookup, scope_join = "dataset_scope scope", scope_match, ""
    statement = f"""WITH {",".join(ctes)} SELECT {_projected_fields("overlay")},scope.dataset_id,scope.run_id AS dataset_run_id
        FROM {driving} JOIN LATERAL (
            SELECT {",".join("overlay." + field for field in _FIELDS)} FROM {overlay} overlay
            WHERE {lookup} AND overlay.npi BETWEEN 1000000000 AND 9999999999 OFFSET 0
        ) overlay ON TRUE {scope_join}
        JOIN LATERAL (SELECT 1 FROM {members} member
            WHERE member.dataset_id=scope.dataset_id AND member.resource_type=overlay.resource_type
                AND member.resource_id=overlay.resource_id LIMIT 1) membership ON TRUE
        WHERE TRUE {excluded}"""
    return native.candidate_preparation.source_sql(schema, statement)


def _can_prepare(native, available):
    return (
        admission._ADMISSION.get() is None
        and source_policy._FACTORY.get() is None
        and not native.candidate_preparation.has_source_query()
        and not native._missing_provider_directory_fence_relations(available)
        and all(callable(getattr(native.db, name, None)) for name in ("transaction", "status", "scalar"))
        and all(
            getattr(native.db, name, lambda: None)() is None for name in ("_transaction_binding", "_reader_binding")
        )
    )


async def _has_npi_index(native, schema):
    """Require an ordinary usable leading NPI index before parameterized lookups."""
    return bool(
        await native.db.scalar(
            """SELECT EXISTS(SELECT FROM pg_index indexed
        JOIN pg_class relation ON relation.oid=indexed.indrelid
        JOIN pg_namespace namespace ON namespace.oid=relation.relnamespace
        JOIN pg_class index_relation ON index_relation.oid=indexed.indexrelid
        JOIN pg_am method ON method.oid=index_relation.relam
        JOIN pg_attribute column_meta ON column_meta.attrelid=relation.oid AND column_meta.attnum=indexed.indkey[0]
        WHERE namespace.nspname=:schema AND relation.relname=:table AND indexed.indisvalid AND indexed.indisready
            AND indexed.indpred IS NULL AND method.amname='btree' AND column_meta.attname='npi')""",
            schema=schema,
            table=native.candidate_preparation.table_name("provider_directory_address_overlay"),
        )
    )


async def _drop(database, relations):
    async with database.transaction():
        await database.status("DROP TABLE IF EXISTS " + ",".join(reversed(relations)))


@asynccontextmanager
async def scoped_projection(native, schema, stage_table, available, *, source_ids, run_id):
    """Commit narrow inputs before group/load readers; retire them after readers drain."""
    if not _can_prepare(native, available) or not await _has_npi_index(native, schema):
        yield None
        return
    relation = storage_relation(schema, native.address_batches.scratch_name(native, stage_table, "_pd_projection"))
    scope_relation = storage_relation(schema, native.address_batches.scratch_name(native, stage_table, "_pd_scope"))
    context_target = storage_relation(schema, native.address_batches.scratch_name(native, stage_table, "_pd_npi"))
    projection = _Projection(relation, scope_relation, context_target, tuple(source_ids or ()), run_id)
    token = _ACTIVE.set(projection)
    try:
        async with native.db.transaction():
            selected_scope = native.candidate_preparation.source_sql(schema, _scope_sql(native, schema))
            await native.db.status(f"CREATE UNLOGGED TABLE {scope_relation} AS {selected_scope}")
            projection.created_relations.append(scope_relation)
            await native.db.status(f"ANALYZE {scope_relation}")
            selected = _raw_sql(native, schema, source_ids=source_ids, run_id=run_id)
            key = lookup_key_sql([f"projected.{field}" for field in _RAW_FIELDS])
            await native.db.status(
                f"CREATE UNLOGGED TABLE {relation} AS SELECT projected.*,{key} AS address_lookup_key FROM ({selected}) projected"
            )
            projection.created_relations.append(relation)
            await native.db.status(f"CREATE INDEX ON {relation}(npi)")
            await native.db.status(f"ANALYZE {relation}")
        yield relation
    finally:
        _ACTIVE.reset(token)
        if projection.created_relations:
            await drain_operation(_drop(native.db, projection.created_relations), preserve_cancellation=True)


async def prepare_load(native, schema, available, affected_group_table):
    """Look up names and primary-address arrays once per affected NPI after groups close."""
    projection = _ACTIVE.get()
    if projection is None or projection.context_relation is not None:
        return
    relation = projection.context_target
    groups = storage_relation(schema, affected_group_table)
    npi_join, primary_join, arrays = native._partial_overlay_npi_context_sql(schema, available)
    entity_name = native._npi_entity_name_expr("n") if npi_join else "NULL::varchar"
    entity_subtype = native._npi_entity_subtype_expr("n") if npi_join else "NULL::varchar"
    fields = ",".join(
        f"{expression} AS {name}"
        for expression, name in zip(
            arrays, ("taxonomy_array", "plans_network_array", "procedures_array", "medications_array"), strict=True
        )
    )
    selected = f"SELECT overlay.npi,{entity_name} AS entity_name,{entity_subtype} AS entity_subtype,{fields} FROM (SELECT DISTINCT entity_npi AS npi FROM {groups} WHERE entity_npi IS NOT NULL) overlay {npi_join} {primary_join}"
    selected = native.candidate_preparation.source_sql(schema, selected)
    async with native.db.transaction():
        await native.db.status(f"CREATE UNLOGGED TABLE {relation} AS {selected}")
        projection.created_relations.append(relation)
        await native.db.status(f"CREATE UNIQUE INDEX ON {relation}(npi)")
        await native.db.status(f"ANALYZE {relation}")
    projection.context_relation = relation


def _cached_membership_join(native, schema):
    members = storage_relation(schema, "provider_directory_dataset_resource")
    statement = f"JOIN LATERAL (SELECT 1 FROM {members} member WHERE member.dataset_id=overlay.dataset_id AND member.resource_type=overlay.resource_type AND member.resource_id=overlay.resource_id LIMIT 1) cached_membership ON TRUE"
    return native.candidate_preparation.source_sql(schema, statement)


def group_input_sql(native, schema):
    """Expose only raw group inputs; NPI taxonomy/plan arrays are never read here."""
    projection = _ACTIVE.get()
    if projection is None:
        return None
    fields = ",".join("overlay." + field for field in _RAW_FIELDS)
    return (
        f"SELECT 'npi'::varchar AS entity_type,overlay.npi::varchar AS entity_id,overlay.address_key,"
        f"{fields},overlay.address_lookup_key FROM {projection.relation} AS overlay {_cached_membership_join(native, schema)}"
    )


def is_cached_source_sql(source_select):
    """Recognize only the active projection's exact qualified relation marker."""
    projection = _ACTIVE.get()
    return projection is not None and f"FROM {projection.relation} AS overlay" in source_select


def _current_rows(native, schema, affected_group_table):
    projection = _ACTIVE.get()
    fields = ",".join("overlay." + field for field in (*_FIELDS, "dataset_id", "dataset_run_id", "address_lookup_key"))
    cached = f"SELECT {fields} FROM {projection.relation} AS overlay {_cached_membership_join(native, schema)}"
    if affected_group_table:
        groups = storage_relation(schema, affected_group_table)
        cached += f" WHERE EXISTS(SELECT FROM {groups} affected WHERE affected.entity_npi=overlay.npi)"
        if projection.source_ids or projection.run_id:
            other = _raw_sql(native, schema, affected_group_table=affected_group_table, exclude=projection)
            key = lookup_key_sql([f"projected.{field}" for field in _RAW_FIELDS])
            return f"{cached} UNION ALL (SELECT projected.*,{key} AS address_lookup_key FROM ({other}) projected)"
    return cached


def cached_overlay_source(native, schema, available, *, source_ids=None, run_id=None, affected_group_table=None):
    """Enrich cached refreshed rows and retain every other current affected source."""
    projection = _ACTIVE.get()
    if (
        projection is None
        or not affected_group_table
        and (tuple(source_ids or ()) != projection.source_ids or run_id != projection.run_id)
    ):
        return None
    npi_join, primary_join, arrays = native._partial_overlay_npi_context_sql(schema, available)
    entity_name = native._npi_entity_name_expr("n") if npi_join else "NULL::varchar"
    entity_subtype = native._npi_entity_subtype_expr("n") if npi_join else "NULL::varchar"
    if projection.context_relation is not None:
        npi_join = f"LEFT JOIN {projection.context_relation} npi_context ON npi_context.npi=overlay.npi"
        primary_join = ""
        arrays = tuple(
            "npi_context." + field
            for field in ("taxonomy_array", "plans_network_array", "procedures_array", "medications_array")
        )
        entity_name, entity_subtype = "npi_context.entity_name", "npi_context.entity_subtype"
    predicate = native.address_batches.keyable_sql(native, "overlay")
    prepared_join = native.address_batches.join_sql("overlay", lookup_key="overlay.address_lookup_key")
    return native._PROVIDER_DIRECTORY_PARTIAL_OVERLAY_SOURCE_TEMPLATE.format(
        current_overlay_ctes=f"WITH current_overlay AS MATERIALIZED ({_current_rows(native, schema, affected_group_table)})",
        entity_name=entity_name,
        entity_subtype=entity_subtype,
        taxonomy_array=arrays[0],
        plans_network_array=arrays[1],
        procedures_array=arrays[2],
        medications_array=arrays[3],
        base_address_version=native.BASE_ADDRESS_VERSION,
        overlay_updated_at=native.candidate_preparation.overlay_updated_at_sql(),
        npi_join=npi_join + prepared_join,
        primary_npi_address_join=primary_join,
        address_predicate=predicate,
    )
