# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Share exact native address tuples across ordinary unified-address phases."""

from __future__ import annotations

import asyncio
import importlib
from contextlib import AsyncExitStack, asynccontextmanager
from contextvars import ContextVar
from functools import wraps
from uuid import uuid4

from db.plan_release_storage import storage_relation
from process import entity_address_partial_projection as projection
from process import entity_address_preparation_admission as admission
from process import provider_directory_generic_source_policy as source
from process.ext import address_batch_stage as stage
from process.ext.address_format import ADDRESS_FORMAT_FUNCTION, ADDRESS_FORMAT_SOURCE, ADDRESS_FORMAT_VERSION
from process.uhc_flex_practitioner_async_safety import drain_operation

_SCOPE = ContextVar("entity_address_batch_scope", default=None)
_TABLE = ContextVar("entity_address_batch_table", default=None)
_LIVE = ContextVar("entity_address_batch_live_inputs", default=None)
_OWNER = ContextVar("entity_address_scratch_control_run", default=None)
PENDING_LABEL_SOURCE = "aggregate-label-pending"


def scope(operation):
    """Retire each completed ordinary cache after its final dependent operation."""

    @wraps(operation)
    async def run(*args, **kwargs):
        """Close every cache before returning to the enclosing preparation owner."""
        ctx = args[0] if args else kwargs.get("ctx")
        owner = None
        if isinstance(ctx, dict):
            owner = (
                str((ctx.get("context") or {}).get("control_run_id") or ctx.get("control_run_id") or "").strip() or None
            )
        async with AsyncExitStack() as stack:
            scope_token, table_token, live_token = _SCOPE.set(stack), _TABLE.set(None), _LIVE.set(None)
            owner_token = _OWNER.set(owner)
            try:
                return await operation(*args, **kwargs)
            finally:
                _SCOPE.reset(scope_token)
                _TABLE.reset(table_token)
                _LIVE.reset(live_token)
                _OWNER.reset(owner_token)

    return run


def scratch_name(native, stage_table, purpose):
    """Let the existing terminal-run reaper discover committed ordinary scratch."""
    if (owner := _OWNER.get()) is not None:
        return importlib.import_module("process.provider_directory_fhir")._address_overlay_stage_table_name(owner)
    return native._archived_identifier(stage_table, purpose + "_" + uuid4().hex[:12])


def _can_stage():
    """Keep admitted source profiles on their existing bounded SQL contract."""
    return admission._ADMISSION.get() is None and source._FACTORY.get() is None


async def prepare_raw(sql, schema, raw_table):
    """Commit one distinct cache before independent raw shards borrow it."""
    relation = storage_relation(schema, raw_table)
    await prepare_inputs(sql, schema, raw_table, f"SELECT {','.join(stage.ADDRESS_FIELDS)} FROM {relation}")


async def prepare_inputs(sql, schema, stage_table, source, *, inputs_are_missing=False):
    """Extend the ordinary cache only for previously unseen exact component tuples."""
    stack = _SCOPE.get()
    if stack is None or not _can_stage() or sql.row_preparation.native_preparation_module() is None:
        return
    name = scratch_name(sql, stage_table, "_address_batch")
    previous = _TABLE.get()
    if previous is not None:
        if not await stage._has_supported_case(sql.db, source, {}):
            _TABLE.set(None)
            return
        if not inputs_are_missing:
            key = stage.lookup_key_sql([f"r.{field}" for field in stage.ADDRESS_FIELDS])
            source = f"SELECT r.* FROM ({source}) r WHERE NOT EXISTS(SELECT FROM {previous} cached WHERE cached.lookup_key={key})"
    table = await stack.enter_async_context(stage.staged_address_fields(sql.db, schema, name, source, committed=True))
    if previous is not None and table is not None:
        table = f"(SELECT * FROM {previous} UNION ALL SELECT * FROM {table})"
    _TABLE.set(table or previous)
    return table


@asynccontextmanager
async def prepared_source(sql, schema, source_select, *, enabled):
    """Keep a direct build's label inputs on its already materialized source."""
    if not enabled or not _can_stage():
        yield
        return
    expressions = [f"b.{name}" for name in stage.ADDRESS_FIELDS]
    source = f"SELECT {stage.input_projection_sql(expressions)} FROM ({source_select}) b"
    async with stage.staged_address_fields(
        sql.db, schema, "address_rows_input_" + uuid4().hex, source, temporary=True
    ) as table:
        token = _TABLE.set(table)
        try:
            yield
        finally:
            _TABLE.reset(token)


def join_sql(alias, *, lookup_key=None):
    """Expose only cache payload fields so unqualified source columns stay unique."""
    table = _TABLE.get()
    if table is None:
        return ""
    key = lookup_key or stage.lookup_key_sql([f"{alias}.{name}" for name in stage.ADDRESS_FIELDS])
    return f" LEFT JOIN (SELECT lookup_key,native_fields FROM {table}) batch_fields ON batch_fields.lookup_key={key} "


def field_sql(field, fallback):
    """Use accepted native NULL values and retain SQL for absent cached results."""
    return fallback if _TABLE.get() is None else stage.field_sql("batch_fields", field, fallback)


async def prepare_partial(sql, schema, stage_table, available, *, source_ids, run_id):
    """Read pinned overlay inputs once, then cache their and live rows' exact keys."""
    stack = _SCOPE.get()
    if stack is None or not _can_stage():
        return
    await stack.enter_async_context(
        projection.scoped_projection(sql, schema, stage_table, available, source_ids=source_ids, run_id=run_id)
    )
    current = projection.group_input_sql(sql, schema)
    if current is None or sql.row_preparation.native_preparation_module() is None:
        return
    live = await stack.enter_async_context(_prepared_live(sql, schema, stage_table, source_ids))
    _LIVE.set(live)
    for selected_inputs in (f"SELECT {','.join(stage.ADDRESS_FIELDS)} FROM {live}", current):
        if not await stage._has_supported_case(sql.db, selected_inputs, {}):
            _TABLE.set(None)
            return
    selected = (
        f"SELECT {','.join(stage.ADDRESS_FIELDS)} FROM {live} UNION ALL "
        f"SELECT {','.join(stage.ADDRESS_FIELDS)} FROM ({current}) current_inputs"
    )
    name = scratch_name(sql, stage_table, "_address_batch")
    table = await stack.enter_async_context(stage.staged_address_fields(sql.db, schema, name, selected, committed=True))
    _TABLE.set(table)


async def _drop_live(database, table):
    """Retire only the exact run input after its final dependent group/cache read."""
    async with database.transaction():
        await database.status(f"DROP TABLE IF EXISTS {table}")


@asynccontextmanager
async def _prepared_live(sql, schema, stage_table, source_ids):
    """Evaluate expensive live evidence selection once for cache and group readers."""
    table = storage_relation(schema, scratch_name(sql, stage_table, "_pd_live_inputs"))
    live = storage_relation(schema, sql.EntityAddressUnified.__main_table__)
    fields = ",".join(f"live.{field}" for field in stage.ADDRESS_FIELDS)
    selected = sql._provider_directory_live_source_filter_sql(schema, "live", source_ids=source_ids)
    is_created = False
    try:
        async with sql.db.transaction():
            await sql.db.status(f"""CREATE UNLOGGED TABLE {table} AS SELECT live.entity_type,live.entity_id,
                live.address_key,{fields} FROM {live} live
                WHERE live.address_sources @> ARRAY['provider_directory_fhir']::varchar[]
                AND live.location_key IS NOT NULL {selected}""")
            is_created = True
            await sql.db.status(f"ANALYZE {table}")
        yield table
    finally:
        if is_created:
            await drain_operation(_drop_live(sql.db, table), preserve_cancellation=True)


def keyable_sql(sql, alias):
    """Use exact entity keyability without changing unsupported source tuples."""
    fallback = sql._address_source_keyable_predicate(
        first_line=f"{alias}.first_line",
        city=f"{alias}.city_name",
        state=f"{alias}.state_name",
        zip_code=f"{alias}.postal_code",
        country=f"{alias}.country_code",
    )
    return f"({field_sql('entity_keyable', f'({fallback})::text')})::boolean"


def group_keys(sql, alias, *, payload_alias="batch_fields"):
    """Resolve entity-specific group keys, preserving empty street keys and SQL NULL."""
    fallback_by_key = {
        "street": sql._street_soft_norm_expr(f"{alias}.first_line"),
        "city": sql._alnum_norm_expr(f"{alias}.city_name"),
        "state": sql._state_norm_expr(f"{alias}.state_name"),
        "zip": sql._zip5_norm_expr(f"{alias}.postal_code"),
        "country": sql._state_norm_expr(f"{alias}.country_code"),
    }
    native_field_by_key = dict(
        street="entity_street_key",
        city="entity_city_norm",
        state="entity_state_code",
        zip="entity_zip5",
        country="entity_country_norm",
    )
    if _TABLE.get() is None:
        return fallback_by_key
    return {
        key: stage.field_sql(payload_alias, native_field_by_key[key], fallback)
        for key, fallback in fallback_by_key.items()
    }


def current_group_select_sql(sql, source_select, *, keyable=False):
    """Build distinct current groups with the same source admission predicates."""
    keys = group_keys(sql, "src")
    lookup = "src.address_lookup_key" if keyable else None
    predicate = "AND " + keyable_sql(sql, "src") if keyable else ""
    return f"""SELECT DISTINCT src.entity_type::varchar AS entity_type,src.entity_id::varchar AS entity_id,
        CASE WHEN src.entity_type='npi' AND src.entity_id ~ '^[0-9]+$' THEN src.entity_id::bigint END AS entity_npi,
        src.address_key::uuid AS address_key,{keys["street"]}::varchar AS street_key,
        {keys["city"]}::varchar AS city_key,{keys["state"]}::varchar AS state_key,
        {keys["zip"]}::varchar AS zip_key,{keys["country"]}::varchar AS country_key
        FROM ({source_select.strip()}) src {join_sql("src", lookup_key=lookup)}
        WHERE src.entity_type IS NOT NULL AND src.entity_id IS NOT NULL {predicate}
        AND (src.address_key IS NOT NULL OR NULLIF(TRIM(src.first_line),'') IS NOT NULL
            OR NULLIF(TRIM(src.city_name),'') IS NOT NULL)"""


def prepare_partial_groups_sql(sql, schema, group_table, source_selects, *, source_ids=None):
    """Use narrow cached current inputs and exact live inputs for the affected set."""
    current = projection.group_input_sql(sql, schema)
    provider_selects = [
        selected for selected in source_selects if sql._is_provider_directory_source_select(schema, selected)
    ]
    if not provider_selects:
        raise RuntimeError(
            "entity-address-unified provider-directory-partial refresh requires "
            "available Provider Directory source tables."
        )
    current_groups = (
        current_group_select_sql(sql, current, keyable=True)
        if current
        else "\nUNION\n".join(current_group_select_sql(sql, selected) for selected in provider_selects)
    )
    keys = group_keys(sql, "live")
    live = _LIVE.get() or storage_relation(schema, sql.EntityAddressUnified.__main_table__)
    selected = (
        ""
        if _LIVE.get()
        else (
            "WHERE live.address_sources @> ARRAY['provider_directory_fhir']::varchar[] "
            "AND live.location_key IS NOT NULL "
            + sql._provider_directory_live_source_filter_sql(schema, "live", source_ids=source_ids)
        )
    )
    return f"""CREATE UNLOGGED TABLE {schema}.{group_table} AS SELECT DISTINCT
        live.entity_type::varchar AS entity_type,live.entity_id::varchar AS entity_id,
        CASE WHEN live.entity_type='npi' AND live.entity_id ~ '^[0-9]+$' THEN live.entity_id::bigint END AS entity_npi,
        live.address_key::uuid AS address_key,{keys["street"]}::varchar AS street_key,
        {keys["city"]}::varchar AS city_key,{keys["state"]}::varchar AS state_key,
        {keys["zip"]}::varchar AS zip_key,{keys["country"]}::varchar AS country_key
        FROM {live} live {join_sql("live")}
        {selected} UNION {current_groups};"""


def group_match_sql(sql, group_alias, row_alias):
    """Compare cached exact entity keys after the existing canonical-key shortcut."""
    keys = group_keys(sql, row_alias)
    match = " AND ".join(f"{group_alias}.{key}_key IS NOT DISTINCT FROM {value}" for key, value in keys.items())
    if _TABLE.get() is not None:
        match = f"EXISTS(SELECT 1 FROM (SELECT 1) anchor {join_sql(row_alias)} WHERE {match})"
    return f"""{group_alias}.entity_type = {row_alias}.entity_type
        AND {group_alias}.entity_id = {row_alias}.entity_id AND (
        ({group_alias}.address_key IS NOT NULL AND {row_alias}.address_key IS NOT NULL
         AND {group_alias}.address_key = {row_alias}.address_key) OR ({match}))"""


def evidence_keys(sql, alias, *, payload_alias="batch_fields"):
    """Preserve normalized archive evidence precedence over raw entity keys."""
    keys = group_keys(sql, alias, payload_alias=payload_alias)
    for key, normalized in (("city", "city_norm"), ("state", "state_code"), ("zip", "zip5")):
        keys[key] = f"COALESCE(NULLIF({alias}.{normalized},''),{keys[key]})"
    return {key: f"CASE WHEN {alias}.address_key IS NULL THEN {value} END" for key, value in keys.items()}


def raw_normalized_sql(sql):
    """Keep the raw stage's short-state rule separate from full entity state keys."""
    zip_sql = field_sql("entity_zip5", sql._zip5_norm_expr("r.postal_code"))
    city_sql = field_sql("entity_city_norm", sql._alnum_norm_expr("r.city_name"))
    state_sql = "NULLIF(upper(left(BTRIM(COALESCE(r.state_name, '')), 2)), '')"
    payload = "NULL::jsonb"
    if _TABLE.get() is not None:
        state_sql = (
            "CASE WHEN batch_fields.native_fields IS NOT NULL THEN "
            "NULLIF(left(batch_fields.native_fields->>'entity_state_code',2),'') ELSE " + state_sql + " END"
        )
        payload = "batch_fields.native_fields"
    return (
        f"{zip_sql}::varchar AS source_zip5,{state_sql}::varchar AS source_state_code,"
        f"{city_sql}::varchar AS source_city_norm,{payload} AS native_fields"
    )


def key_sql(fallback, address_source, available):
    """Retain the PTG key exclusion and the caller's canonical-function availability."""
    if _TABLE.get() is None or not available:
        return fallback
    native = field_sql("address_key", f"({fallback})::text")
    return f"CASE WHEN {address_source}='ptg' THEN NULL::uuid ELSE ({native})::uuid END"


def identity_sql(schema, alias):
    """Resolve a source identity from its exact staged input tuple."""
    values = ", ".join(f"{alias}.{name}" for name in stage.ADDRESS_FIELDS)
    return field_sql("identity_key", f"{schema}.addr_identity_key_v1({values})")


def formatted_sql(schema):
    """Render final aggregate inputs, falling back when their tuple was not staged."""
    values = ", ".join(stage.ADDRESS_FIELDS)
    return field_sql("formatted_address", f"{schema}.{ADDRESS_FORMAT_FUNCTION}({values})")


async def gather(*operations, return_exceptions=False):
    """Drain dependent shards before a failed ordinary cache is retired."""
    if _TABLE.get() is None and _LIVE.get() is None and projection._ACTIVE.get() is None:
        return await admission.gather(*operations, return_exceptions=return_exceptions)
    tasks = [asyncio.create_task(operation) for operation in operations]
    try:
        return await asyncio.gather(*tasks, return_exceptions=return_exceptions)
    except BaseException:
        for task in tasks:
            if not task.done() and not task.cancelling():
                task.cancel()

        async def drain():
            """Settle all cache readers before propagating the original failure."""
            return await asyncio.gather(*tasks, return_exceptions=True)

        await drain_operation(drain(), preserve_cancellation=True)
        raise


def _formatted_fields_sql(db_schema, defer_labels):
    """Keep accepted NULL labels separate from previously unseen final tuples."""
    label = formatted_sql(db_schema)
    label_source = f"'{ADDRESS_FORMAT_SOURCE}'::varchar"
    if defer_labels and _TABLE.get() is not None:
        label = f"CASE WHEN batch_fields.lookup_key IS NULL THEN NULL::varchar ELSE {label} END"
        label_source = f"CASE WHEN batch_fields.lookup_key IS NULL THEN '{PENDING_LABEL_SOURCE}' ELSE '{ADDRESS_FORMAT_SOURCE}' END::varchar"
    return label, label_source


def raw_materialize_select_address_sql(db_schema: str, array_join: str, *, defer_labels=False) -> str:
    """Build address fields and the final raw aggregation join."""
    label, label_source = _formatted_fields_sql(db_schema, defer_labels)
    joined = join_sql("aggregated")
    return f"""
        base_address_version,
        checksum,
        type,
        taxonomy_array,
        plans_network_array,
        procedures_array,
        medications_array,
        first_line,
        second_line,
        city_name,
        state_name,
        postal_code,
        country_code,
        telephone_number,
        fax_number,
        phone_number,
        phone_extension,
        fax_number_digits,
        fax_extension,
        {label} AS formatted_address,
        {ADDRESS_FORMAT_VERSION}::smallint AS formatted_address_version,
        {label_source} AS formatted_address_source,
        lat,
        long,
        date_added,
        place_id,
        address_key,
        updated_at,
        updated_at AS last_seen_at
      FROM aggregated{array_join}{joined};
    """


def direct_materialize_select_sql(db_schema: str, *, defer_labels=False) -> str:
    """Build the final direct materialization projection."""
    label, label_source = _formatted_fields_sql(db_schema, defer_labels)
    return f"""
    SELECT
        entity_type,
        entity_id,
        npi,
        inferred_npi,
        inference_confidence,
        inference_method,
        entity_name,
        entity_subtype,
        location_key,
        COALESCE(CARDINALITY(address_sources), 0)::int AS source_count,
        (COALESCE(CARDINALITY(address_sources), 0) > 1) AS multi_source_confirmed,
        COALESCE(address_sources, ARRAY[]::varchar[]) AS address_sources,
        COALESCE(source_record_ids, ARRAY[]::varchar[]) AS source_record_ids,
        checksum,
        type,
        taxonomy_array,
        plans_network_array,
        procedures_array,
        medications_array,
        first_line,
        second_line,
        city_name,
        state_name,
        postal_code,
        country_code,
        telephone_number,
        fax_number,
        phone_number,
        phone_extension,
        fax_number_digits,
        fax_extension,
        {label} AS formatted_address,
        {ADDRESS_FORMAT_VERSION}::smallint AS formatted_address_version,
        {label_source} AS formatted_address_source,
        lat,
        long,
        date_added,
        place_id,
        address_key,
        updated_at
      FROM aggregated{join_sql("aggregated")};
    """


ALIAS_INTEGRITY_SQL = """
    WITH raw_candidates AS (
        SELECT DISTINCT
            raw.address_key,
            raw.address_source,
            raw.first_line,
            raw.second_line,
            raw.city_name,
            raw.state_name,
            raw.postal_code,
            raw.country_code
        FROM {db_schema}.{raw_table} AS raw
        {checksum_where}
    ), matching AS (
        SELECT
            active.source_address_key,
            active.target_address_key,
            active.source_identity_key,
            active.target_identity_key AS recorded_target_identity_key,
            {source_identity_key} AS current_source_identity_key,
            target.identity_key AS current_target_identity_key,
            target.address_key AS current_target_address_key
        FROM raw_candidates AS raw
        {batch_join}
        CROSS JOIN LATERAL (
            VALUES (raw.address_key, 0), ({computed_address_key}, 1)
        ) AS candidate(address_key, priority)
        JOIN {db_schema}.{alias_table} AS active
          ON active.source_address_key = candidate.address_key
         AND active.revoked_at IS NULL
        LEFT JOIN {db_schema}.{archive_relation} AS target
          ON target.address_key = active.target_address_key
         AND target.merged_into IS NULL
        WHERE candidate.address_key IS NOT NULL
          AND (candidate.priority = 0 OR candidate.address_key IS DISTINCT FROM raw.address_key)
    ), violations AS (
        SELECT
            CASE
                WHEN source_identity_key IS DISTINCT FROM current_source_identity_key
                    THEN 'source_identity_mismatch'
                WHEN current_target_address_key IS NULL
                    THEN 'missing_or_merged_target'
                WHEN recorded_target_identity_key IS DISTINCT FROM current_target_identity_key
                    THEN 'target_identity_mismatch'
                ELSE NULL
            END AS violation_kind,
            source_address_key,
            target_address_key
        FROM matching
        UNION ALL
        SELECT
            'multi_hop_alias',
            matching.source_address_key,
            matching.target_address_key
        FROM matching
        JOIN {db_schema}.{alias_table} AS downstream
          ON downstream.source_address_key = matching.target_address_key
         AND downstream.revoked_at IS NULL
    )
    SELECT *
    FROM violations
    WHERE violation_kind IS NOT NULL
    ORDER BY violation_kind, source_address_key
    LIMIT 1;
"""
