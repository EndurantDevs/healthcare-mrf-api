# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Read immutable named LOCAL payloads with their logical publication controls."""

from __future__ import annotations

import json

import asyncpg

from process.ptg_parts import ptg2_physical_binding as physical
from process.ptg_parts import result_archive_candidate_validation as validation
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_schema import resolve_ptg2_schema

_STORAGE_ERRORS = (asyncpg.UndefinedTableError, asyncpg.UndefinedColumnError)
_BOUNDED_ROWS_SQL = """WITH selected AS MATERIALIZED ({sql}), bounds AS (
 SELECT coalesce(sum(octet_length(to_jsonb(selected)::text)+2),0)+2 AS byte_count FROM selected
) SELECT selected.* FROM selected CROSS JOIN bounds WHERE byte_count<=${parameter}::bigint"""


def _bounded_arguments(arguments, max_report_bytes):
    if type(max_report_bytes) is not int or max_report_bytes < 1:
        raise ValueError("registry_pricing_local_bound_invalid")
    encoded = json.dumps(arguments, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()
    if len(encoded) > max_report_bytes:
        raise physical.PTG2PhysicalBindingError("PTG local request is oversized")


async def _bounded_fetch(connection, sql, arguments, max_report_bytes):
    _bounded_arguments(arguments, max_report_bytes)
    if len(sql.encode()) > max_report_bytes:
        raise physical.PTG2PhysicalBindingError("PTG local statement is oversized")
    if not connection.is_in_transaction():
        raise ValueError("registry_pricing_local_transaction_invalid")
    query = _BOUNDED_ROWS_SQL.format(sql=sql, parameter=len(arguments) + 1)
    selected = await connection.fetch(query, *arguments, max_report_bytes)
    if not connection.is_in_transaction():
        raise ValueError("registry_pricing_local_transaction_invalid")
    return [dict(native_row_by_field) for native_row_by_field in selected]


def _json_fields(native_row_by_field, names):
    decoded_by_field = dict(native_row_by_field)
    for name in names:
        if isinstance(decoded_by_field.get(name), str):
            decoded_by_field[name] = json.loads(decoded_by_field[name])
    return decoded_by_field


async def _installed_bindings(connection, snapshot_ids, max_report_bytes):
    schema = _quote_ident(resolve_ptg2_schema())
    sql = f"SELECT * FROM {schema}.ptg2_installed_physical_binding WHERE destination_snapshot_id=ANY($1::text[])"
    native_rows = await _bounded_fetch(connection, sql, (list(snapshot_ids),), max_report_bytes)
    by_snapshot = {}
    json_names = (
        "native_validation",
        "native_publication",
        "relation_inventory",
        "sequence_inventory",
        "generation_inventory",
    )
    for native_row_by_field in native_rows:
        native_row_by_field = _json_fields(native_row_by_field, json_names)
        identity = native_row_by_field["destination_snapshot_id"]
        if identity not in snapshot_ids or identity in by_snapshot:
            raise physical.PTG2PhysicalBindingError("PTG local installed identity differs")
        binding, normalized = validation._local_read_authority_binding(native_row_by_field, identity, is_prepared=False)
        by_snapshot[identity] = (binding, normalized)
    return by_snapshot


async def _lock_families(connection, by_snapshot, max_report_bytes):
    relations = sorted(
        {binding.relation(name) for binding, _ in by_snapshot.values() for _schema, name in binding.relations}
    )
    length = len("LOCK TABLE  IN ACCESS SHARE MODE NOWAIT")
    fragments = []
    for relation in relations:
        fragment = f"ONLY {relation}"
        length += len(fragment.encode()) + 2
        if length > max_report_bytes:
            raise physical.PTG2PhysicalBindingError("PTG local lock statement is oversized")
        fragments.append(fragment)
    if fragments:
        await connection.execute("LOCK TABLE " + ",".join(fragments) + " IN ACCESS SHARE MODE NOWAIT")


def _union_sql(fragments, max_report_bytes):
    length, selected = 0, []
    for fragment in fragments:
        length += len(fragment.encode()) + len(" UNION ALL ")
        if length > max_report_bytes:
            raise physical.PTG2PhysicalBindingError("PTG local set statement is oversized")
        selected.append("(" + fragment + ")")
    return " UNION ALL ".join(selected)


async def _published_controls(connection, entries, max_report_bytes):
    fragments = (
        physical._local_candidate_control_query(
            resolve_ptg2_schema(),
            binding,
            snapshot_parameter=f"($1::text[])[{number}]",
            payload_parameter=f"($2::bigint[])[{number}]",
            lock_controls=False,
        ).replace("SELECT snapshot.*", f"SELECT {number}::bigint AS request_no,snapshot.*", 1)
        for number, (binding, _) in enumerate(entries, 1)
    )
    sql = _union_sql(fragments, max_report_bytes)
    controls = await _bounded_fetch(
        connection,
        sql,
        (
            [binding.snapshot_id for binding, _ in entries],
            [binding.payload_snapshot_key for binding, _ in entries],
        ),
        max_report_bytes,
    )
    plans = await _bounded_fetch(
        connection,
        f"SELECT snapshot_id,plan_id,plan_market_type FROM {_quote_ident(resolve_ptg2_schema())}.ptg2_v3_snapshot_plan_scope "
        "WHERE snapshot_id=ANY($1::text[]) ORDER BY snapshot_id,plan_id,plan_market_type",
        ([binding.snapshot_id for binding, _ in entries],),
        max_report_bytes,
    )
    if any(plan["snapshot_id"] not in {binding.snapshot_id for binding, _ in entries} for plan in plans):
        raise physical.PTG2PhysicalBindingError("PTG local plan identity differs")
    by_request = {}
    for control in controls:
        number = control.pop("request_no")
        if type(number) is not int or not 1 <= number <= len(entries) or number in by_request:
            raise physical.PTG2PhysicalBindingError("PTG local published control identity differs")
        binding, authority = entries[number - 1]
        if control.get("snapshot_id") != binding.snapshot_id:
            raise physical.PTG2PhysicalBindingError("PTG local published snapshot identity differs")
        control = _json_fields(
            control, ("manifest", "layout_manifest", "options", "run_report", "audit_report", "frozen_binding_payload")
        )
        selected_plans = [
            plan_by_field for plan_by_field in plans if plan_by_field["snapshot_id"] == binding.snapshot_id
        ]
        physical._require_local_published_postimage(
            control, selected_plans, authority["native_validation"], authority["native_publication"], binding
        )
        by_request[number] = control
    if set(by_request) != set(range(1, len(entries) + 1)):
        raise physical.PTG2PhysicalBindingError("PTG local published controls are unavailable")
    return by_request


async def _source_dictionaries(connection, entries, max_report_bytes):
    fragments = (
        f"SELECT {number}::bigint AS request_no,{','.join(physical._SOURCE_FIELDS)} "
        f"FROM {binding.relation('ptg2_v3_snapshot_source')} WHERE snapshot_id=($1::text[])[{number}] "
        "ORDER BY source_key LIMIT 257"
        for number, (binding, _) in enumerate(entries, 1)
    )
    selected = await _bounded_fetch(
        connection,
        _union_sql(fragments, max_report_bytes) + " ORDER BY request_no,source_key",
        ([binding.payload_snapshot_id for binding, _ in entries],),
        max_report_bytes,
    )
    by_request = {number: [] for number in range(1, len(entries) + 1)}
    for native_row_by_field in selected:
        number = native_row_by_field.pop("request_no")
        if type(number) is not int or number not in by_request:
            raise physical.PTG2PhysicalBindingError("PTG local source identity differs")
        by_request[number].append(native_row_by_field)
    for native_rows in by_request.values():
        if not 0 < len(native_rows) <= 256:
            raise physical.PTG2PhysicalBindingError("PTG local serving source dictionary differs")
    return by_request


async def _installed_local_rows(connection, published_rows, max_report_bytes):
    original_by_snapshot_id = {
        catalog_row_by_field["snapshot_id"]: catalog_row_by_field for catalog_row_by_field in published_rows
    }
    by_snapshot = await _installed_bindings(connection, tuple(original_by_snapshot_id), max_report_bytes)
    if not by_snapshot:
        return {}
    entries = list(by_snapshot.values())
    by_schema = {}
    for binding, _authority in entries:
        scope = (
            binding.dataset_id,
            binding.payload_snapshot_id,
            binding.payload_snapshot_key,
            binding.relations,
            binding.sequences,
        )
        if binding.schema_name in by_schema and by_schema[binding.schema_name] != scope:
            raise physical.PTG2PhysicalBindingError("PTG local family aliases differ")
        by_schema[binding.schema_name] = scope
    await _lock_families(connection, by_snapshot, max_report_bytes)
    controls = await _published_controls(connection, entries, max_report_bytes)
    source_by_request = await _source_dictionaries(connection, entries, max_report_bytes)
    resolved_by_snapshot_id = {}
    for number, (binding, _) in enumerate(entries, 1):
        native_row_by_field = validation._local_serving_row_fields(
            original_by_snapshot_id[binding.snapshot_id], controls[number], binding
        )
        source_keys = [catalog_row_by_field["source_key"] for catalog_row_by_field in source_by_request[number]]
        native_row_by_field.update(
            source_row_count=len(source_keys),
            distinct_source_key_count=len(set(source_keys)),
            minimum_source_key=min(source_keys),
            maximum_source_key=max(source_keys),
            source_identity_rows=source_by_request[number],
        )
        resolved_by_snapshot_id[binding.snapshot_id] = (native_row_by_field, binding)
    return resolved_by_snapshot_id


async def read_pricing_local_rows(connection, published_rows, *, max_report_bytes):
    """Resolve installed named payloads inside the caller's read-only snapshot transaction."""
    if not published_rows:
        return {}
    if not connection.is_in_transaction():
        raise ValueError("registry_pricing_local_transaction_invalid")
    settings = await connection.fetchrow(
        "SELECT pg_catalog.current_setting('transaction_isolation') AS isolation,"
        "pg_catalog.current_setting('transaction_read_only') AS read_only,"
        "pg_catalog.current_setting('search_path') AS search_path"
    )
    if settings["isolation"] not in {"repeatable read", "serializable"} or settings["read_only"] != "on":
        raise ValueError("registry_pricing_local_transaction_invalid")
    savepoint = connection.transaction()
    await savepoint.start()
    try:
        await connection.execute("SELECT pg_catalog.set_config('search_path','pg_catalog, pg_temp',true)")
        resolved_by_snapshot_id = await _installed_local_rows(connection, published_rows, max_report_bytes)
        await connection.execute("SELECT pg_catalog.set_config('search_path',$1,true)", settings["search_path"])
    except BaseException as primary_error:
        try:
            await savepoint.rollback()
        except BaseException:
            primary_error.add_note("optional LOCAL lookup rollback was incomplete")
            raise primary_error
        if not connection.is_in_transaction():
            raise primary_error
        if isinstance(primary_error, (*_STORAGE_ERRORS, physical.PTG2PhysicalBindingError)):
            return {}
        raise
    await savepoint.commit()
    if not connection.is_in_transaction():
        raise ValueError("registry_pricing_local_transaction_invalid")
    return resolved_by_snapshot_id
