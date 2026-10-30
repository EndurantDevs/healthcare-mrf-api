# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepare and apply only the verified ordinary Address location closure."""

import asyncio
import datetime
import importlib
from contextvars import ContextVar
from dataclasses import dataclass, field
from weakref import WeakSet

from api import ptg2_geo_projection as projection
from process import entity_address_ordinary_overlay as overlay

_ACTIVE = ContextVar("ordinary_address_affected_build", default=None)
_ISSUED = WeakSet()


def _require(condition):
    if not condition:
        raise RuntimeError("entity_address_affected_patch_invalid")


def _capture(scope):
    from process.provider_directory_ordinary_address_admission import require_capture

    directory = importlib.import_module("process.provider_directory_fhir")
    return require_capture(directory, scope.admission.plan, scope.admission.captured_inputs)


def is_active():
    """Identify only the original verified ordinary preparation scope."""
    return _ACTIVE.get() is not None


def enter(scope):
    """Bind a genuine ordinary plan and original server capture to this build."""
    from process.provider_directory_serving_capacity_plan import OrdinaryServingAdmissionPlan

    if scope is None or type(scope.admission.plan) is not OrdinaryServingAdmissionPlan:
        return _ACTIVE.set(None)
    capture = _capture(scope)
    _require(bool(capture.address_task) and not capture.is_unchanged)
    return _ACTIVE.set(_AffectedBuild(scope, capture, asyncio.current_task()))


def exit_scope(token):
    """Restore the previous preparation scope after this build finishes."""
    _ACTIVE.reset(token)


@dataclass
class _AffectedBuild:
    scope: object
    capture: object
    task: object
    incumbent: object = None
    descriptor: object = None
    overlay_native_json: str | None = None


@dataclass(frozen=True, eq=False)
class AffectedAddressPatch:
    """Actual owned relations and the unchanged live identity, never a worker payload."""

    schema: str
    stage_table: str
    stage_oid: int
    affected_location_table: str
    affected_location_oid: int
    affected_group_table: str
    affected_group_oid: int
    live_table_oid: int
    live_index_oids: tuple[int, ...]
    source_ids: tuple[str, ...]
    admission: object = field(repr=False, compare=False)
    capture: object = field(repr=False, compare=False)
    _build: object = field(repr=False, compare=False)
    support_stages: tuple[tuple[str, str, int], ...] = ()
    facility_generated: bool = False
    transition_at: str = ""
    overlay_dependency: object = field(default=None, repr=False, compare=False)

    @property
    def owned_relations(self):
        """Expose exactly the candidate and its closed location/group scopes."""
        return (
            ("entity_address_unified", self.stage_table, self.stage_oid),
            *self.support_stages,
            (self.affected_location_table, self.affected_location_table, self.affected_location_oid),
            (self.affected_group_table, self.affected_group_table, self.affected_group_oid),
        )


def current_capture():
    """Recheck and return the original signed server capture."""
    build = _ACTIVE.get()
    if build is None:
        return None
    _require(_capture(build.scope) is build.capture)
    return build.capture


async def _live_identity(native, schema):
    live = f"{schema}.entity_address_unified"
    identity = await native.db.first(
        "SELECT c.oid::bigint AS oid, c.relowner::bigint AS owner, "
        "EXISTS(SELECT 1 FROM pg_namespace n WHERE n.nspname='hp_snapshot_retention' "
        "AND n.nspowner=c.relowner) AS protected FROM pg_class c WHERE c.oid=to_regclass(:relation)",
        relation=live,
    )
    _require(identity is not None)
    mapped = native.result_generation._row_mapping(identity)
    _require(mapped["protected"] is False)
    indexes = await native.db.all(
        "SELECT indexrelid::bigint FROM pg_index WHERE indrelid=:oid "
        "AND indisvalid AND indisready AND indislive ORDER BY indexrelid",
        oid=int(mapped["oid"]),
    )
    authority = await native.result_generation._locked_authority(native.db, schema)
    return int(mapped["oid"]), tuple(int(index[0]) for index in indexes), authority.as_dict()


async def capture_incumbent(native, schema):
    """Fence the mutable live result and existing geo authority before building."""
    build = _ACTIVE.get()
    if build is None:
        return
    _require(build.incumbent is None and _capture(build.scope) is build.capture)
    from process.provider_directory_cms_preparation import nonprofile_sql_transaction

    directory = importlib.import_module("process.provider_directory_fhir")
    async with nonprofile_sql_transaction(directory, build.scope.admission):
        build.incumbent = await _live_identity(native, schema)
        build.overlay_native_json = await overlay.capture(native, schema, build)
        _require(await native.db.scalar(f"SELECT {projection.projection_state_available_sql(schema)}"))


def logical_overlay_sql(native, schema, inputs):
    """Unchanged Sources stay visible without copying their Overlay rows."""
    capture = current_capture()
    if capture is None:
        return None
    sources = native._string_array_literal(capture.affected_scope["source_ids"])
    columns = ",".join(
        importlib.import_module("process.provider_directory_fhir")._provider_directory_address_overlay_columns()
    )
    return (
        f"(SELECT {columns} FROM {schema}.provider_directory_address_overlay "
        f"WHERE NOT(source_id=ANY({sources})) UNION ALL "
        f"SELECT {columns} FROM {schema}.{inputs.overlay_table} WHERE source_id=ANY({sources}))"
    )


def source_filter_sql(native, alias):
    """Match affected Sources from persisted evidence rather than mutable aliases."""
    capture = current_capture()
    if capture is None:
        return None
    sources = native._string_array_literal(capture.affected_scope["source_ids"])
    return (
        f" AND EXISTS(SELECT 1 FROM unnest(COALESCE({alias}.source_record_ids,ARRAY[]::varchar[])) rid "
        f"WHERE rid LIKE 'provider_directory_fhir:%' AND split_part(rid,':',3)=ANY({sources}))"
    )


async def prepare_location_scope(native, schema, stage, groups, context):
    """Create the existing exact old/new location closure under owned DDL."""
    build = _ACTIVE.get()
    if build is None:
        return
    _require(build.incumbent is not None and groups and _capture(build.scope) is build.capture)
    locations = native._affected_live_location_table(stage)
    from process import entity_address_preparation_admission as admission

    await admission.create_stage_sql(
        schema,
        locations,
        native._prepare_affected_live_locations_sql(
            schema,
            live_table="entity_address_unified",
            affected_group_table=groups,
            replacement_lookup_table=stage,
            affected_location_table=locations,
        ),
    )
    await admission.stage_status(native._index_affected_live_locations_sql(schema, locations))
    context["partial_provider_directory_affected_live_location_table"] = locations
    context["partial_provider_directory_coordinate_scope_table"] = locations
    context["partial_provider_directory_unaffected_live_rows_copied"] = 0


async def merged_row_count(native, schema, stage, context, previous_rows):
    """Account for deleted and replacement rows without copying unrelated rows."""
    if not is_active():
        return int(context["staged_rows"])
    locations = context["partial_provider_directory_affected_live_location_table"]
    removed = int(
        await native.db.scalar(
            f"SELECT count(*) FROM {schema}.entity_address_unified live JOIN {schema}.{locations} scope "
            "ON scope.location_key=live.location_key"
        )
        or 0
    )
    return previous_rows - removed + int(context["staged_rows"])


async def finalize(native, schema, stage, context):
    """Issue an exact descriptor only after native candidate validation."""
    build = _ACTIVE.get()
    _require(build is not None and build.incumbent is not None)
    groups = context["partial_provider_directory_affected_group_table"]
    locations = context["partial_provider_directory_affected_live_location_table"]
    names = (stage, locations, groups)
    oids = tuple(
        [
            int(await native.db.scalar("SELECT to_regclass(:relation)::oid::bigint", relation=f"{schema}.{name}"))
            for name in names
        ]
    )
    _require(all(build.scope.owned_oids.get(name) == oid for name, oid in zip(names, oids, strict=True)))
    descriptor = AffectedAddressPatch(
        schema,
        stage,
        oids[0],
        locations,
        oids[1],
        groups,
        oids[2],
        build.incumbent[0],
        build.incumbent[1],
        tuple(build.capture.affected_scope["source_ids"]),
        build.scope.admission,
        build.capture,
        build,
        tuple(entry for entry in build.scope.stage_oids if entry[0] != "entity_address_unified"),
        bool(context.get("affected_facility_generated")),
        await preparation_timestamp(native),
        overlay.bind(native, build),
    )
    from process import entity_address_affected_support as support

    await support.finalize_identities(native, descriptor, context)
    _ISSUED.add(descriptor)
    build.descriptor = descriptor
    await validate_merged_support(native, descriptor)
    return descriptor


async def validate_merged_support(native, descriptor):
    """Validate all six actual affected support stages against replacement locations."""
    from process import entity_address_affected_support as support

    await support.validate(native, descriptor)


def require_descriptor(prepared):
    """Reject forged descriptors, changed captures and a different publishing task."""
    descriptor = prepared.affected_patch
    _require(type(descriptor) is AffectedAddressPatch and descriptor in _ISSUED)
    _require(descriptor._build.descriptor is descriptor and descriptor._build.task is asyncio.current_task())
    _require(_capture(descriptor._build.scope) is descriptor.capture and not prepared.committed)
    return descriptor


def patch_statements(native, descriptor):
    """Replace only the closed affected location set using native set-based SQL."""
    schema, stage, locations = descriptor.schema, descriptor.stage_table, descriptor.affected_location_table
    columns = native._entity_address_column_list()
    return (
        f"DELETE FROM {schema}.entity_address_unified live USING {schema}.{locations} scope "
        "WHERE live.location_key=scope.location_key",
        f"INSERT INTO {schema}.entity_address_unified ({columns}) SELECT {columns} FROM {schema}.{stage}",
    )


async def publish(native, prepared, callbacks):
    """Apply a closed candidate in the owner's transaction and retain native identity."""
    native.require_caller_owned_cutover_transaction(native.db)
    descriptor = require_descriptor(prepared)
    schema = descriptor.schema
    await descriptor.admission.assert_ready(
        importlib.import_module("process.provider_directory_fhir"), schema, cutover=True
    )
    locked = await native.db.scalar(
        "SELECT pg_try_advisory_xact_lock(hashtextextended(:name,0))", name=f"entity-address-unified:{schema}"
    )
    _require(locked is True)
    live_targets = sorted(
        ["entity_address_unified", *(target_relation for target_relation, _stage, _oid in descriptor.support_stages)]
    )
    await native.db.status(
        "LOCK TABLE "
        + ",".join(f"{schema}.{target_relation}" for target_relation in live_targets)
        + " IN SHARE ROW EXCLUSIVE MODE NOWAIT"
    )
    _require(await _live_identity(native, schema) == descriptor._build.incumbent)
    await native.lock_selected_publication_dependencies(native.db, schema)
    _require(await native.db.scalar(f"SELECT {projection.projection_state_available_sql(schema)}"))
    await native.db.scalar(native.address_alias_sql.alias_advisory_xact_lock_sql())
    _require(await native._address_alias_generation(schema) == prepared.preparation_input.address_alias_generation)
    await overlay.assert_applied(native, prepared, descriptor)
    await native.candidate_preparation.assert_dataset_pins(schema, prepared.preparation_input)
    from process.entity_address_ordinary_preparation import require_prepared

    require_prepared(prepared, descriptor.admission)
    await native.candidate_preparation._assert_prepared_stages(prepared)
    await native.candidate_preparation.prepared_doctors.assert_applied_dependencies(
        native.db, prepared.native_dependencies
    )
    if callbacks is not None and callbacks.before_cutover is not None:
        await callbacks.before_cutover()
    await validate_merged_support(native, descriptor)
    for statement in patch_statements(native, descriptor):
        await native.db.status(statement)
    from process import entity_address_affected_support as support

    for statement in support.apply_statements(native, descriptor, prepared.context):
        await native.db.status(statement)
    projected_rows = int(prepared.context["merged_result_rows"])
    binding_options = (
        {}
        if prepared.native_dependencies is None
        else {"dependency_bindings": prepared.native_dependencies.dependency_bindings}
    )
    await native.db.scalar(
        native._record_geo_assurance_candidate_sql(schema, "entity_address_unified", projected_rows, **binding_options)
    )
    _require(await native.db.scalar(native._activate_geo_assurance_candidate_sql(schema)) == descriptor.live_table_oid)
    authority = await native.result_generation.publish_local_entity_address_generation(native.db, schema_name=schema)
    prepared.context["result_generation"] = authority.as_dict()
    if callbacks is not None and callbacks.after_publish is not None:
        await callbacks.after_publish()
    await descriptor.admission.assert_cutover_complete()
    _require((await _live_identity(native, schema))[:2] == descriptor._build.incumbent[:2])
    return prepared.context["result_generation"]


def current_group_select_sql(native, source_select: str) -> str:
    """Keep legacy group SQL while adding inferred-provider closure for ordinary work."""
    npi_expression = (
        native._entity_address_row_npi_expr("src")
        if is_active()
        else "CASE WHEN src.entity_type = 'npi' AND src.entity_id ~ '^[0-9]+$' THEN src.entity_id::bigint ELSE NULL::bigint END"
    )
    return f"""
    SELECT DISTINCT
        src.entity_type::varchar AS entity_type,
        src.entity_id::varchar AS entity_id,
        {npi_expression} AS entity_npi,
        src.address_key::uuid AS address_key,
        {native._street_soft_norm_expr("src.first_line")}::varchar AS street_key,
        {native._alnum_norm_expr("src.city_name")}::varchar AS city_key,
        {native._state_norm_expr("src.state_name")}::varchar AS state_key,
        {native._zip5_norm_expr("src.postal_code")}::varchar AS zip_key,
        {native._state_norm_expr("src.country_code")}::varchar AS country_key
      FROM (
            {source_select.strip()}
      ) AS src
     WHERE src.entity_type IS NOT NULL
       AND src.entity_id IS NOT NULL
       AND (
            src.address_key IS NOT NULL
         OR NULLIF(TRIM(src.first_line), '') IS NOT NULL
         OR NULLIF(TRIM(src.city_name), '') IS NOT NULL
       )
    """


def index_location_scope_sql(
    native,
    db_schema: str,
    affected_location_table: str,
) -> str:
    """Retain the existing unique index over the exact location-key closure."""
    index_name = native._archived_identifier(f"{affected_location_table}_idx_location", "")
    return f"""
    CREATE UNIQUE INDEX {index_name}
        ON {db_schema}.{affected_location_table} (location_key);
    """


def affected_source_select_sql(native, schema, source_select, groups):
    """Recompute every selected provider, including existing inferred facility identities."""
    selected = native._prefilter_npi_source_select_sql(schema, source_select, groups)
    provider_npi = native._entity_address_provider_npi_expr("src")
    live_npi = native._entity_address_provider_npi_expr("previous")
    return f"""
    SELECT src.* FROM ({selected}) src
     WHERE EXISTS(SELECT 1 FROM {schema}.{groups} affected
                   WHERE affected.entity_npi IS NOT NULL AND affected.entity_npi={provider_npi})
        OR (src.npi IS NULL AND EXISTS(
              SELECT 1 FROM {schema}.entity_address_unified previous
              JOIN {schema}.{groups} affected ON affected.entity_npi={live_npi}
               WHERE previous.entity_type=src.entity_type AND previous.entity_id=src.entity_id
                 AND affected.entity_npi IS NOT NULL))
    """


def row_npi_sql(row_alias: str) -> str:
    """Preserve ordinary inferred-provider closure without changing legacy NPI matching."""
    if is_active():
        return f"COALESCE({row_alias}.npi,{row_alias}.inferred_npi,CASE WHEN {row_alias}.entity_type='npi' AND {row_alias}.entity_id ~ '^[0-9]+$' THEN {row_alias}.entity_id::bigint END)"
    return (
        f"COALESCE({row_alias}.npi, CASE\n"
        f"            WHEN {row_alias}.entity_type = 'npi' AND {row_alias}.entity_id ~ '^[0-9]+$'\n"
        f"                THEN {row_alias}.entity_id::bigint\n"
        f"            ELSE NULL::bigint\n"
        f"        END)"
    )


async def preparation_timestamp(native):
    """Freeze one actual observed time for preparation annotations and their row receipts."""
    observed = await native.db.scalar("SELECT clock_timestamp()")
    _require(type(observed) is datetime.datetime and observed.utcoffset() is not None)
    return observed.astimezone(datetime.timezone.utc).isoformat(timespec="microseconds")
