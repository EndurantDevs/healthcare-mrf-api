# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Installed ordinary LOCAL custody on one native reader; no readiness promotion."""

from __future__ import annotations

import json

import asyncpg

from process import reference_family_archive as archive
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


async def _installed_owner(connection, max_report_bytes):
    names = [physical._PREPARATION + suffix for suffix in ("", "_relation", "_sequence")]
    sql = physical._local_preparation_catalog_sql()
    sql = sql.replace("SELECT c.relowner", "SELECT requested.table_name,c.relowner", 1)
    sql = sql.replace("FROM pg_class c", "FROM unnest($1::text[]) requested(table_name) CROSS JOIN pg_class c", 1)
    sql = sql.replace(":table", "requested.table_name")
    sql = sql.replace(":child", "(requested.table_name<>$2::text)")
    sql = sql.replace("CAST(:initial_columns AS text[])", "$3::text[]")
    native_rows = await _bounded_fetch(
        connection, sql, (names, physical._PREPARATION, list(physical._INITIAL_PREPARATION_COLUMNS)), max_report_bytes
    )
    proof_by_name = {}
    owner_oid = None
    for proof in native_rows:
        name = proof["table_name"]
        if name not in names or name in proof_by_name or proof.get("protected") is not True:
            raise physical.PTG2PhysicalBindingError("PTG local preparation control catalog is not protected")
        if owner_oid is not None and proof["owner_oid"] != owner_oid:
            raise physical.PTG2PhysicalBindingError("PTG local preparation owner differs")
        owner_oid = proof["owner_oid"]
        proof_by_name[name] = proof
    if set(proof_by_name) != set(names):
        raise physical.PTG2PhysicalBindingError("PTG local preparation control catalog is unavailable")
    return owner_oid


async def _view_proof(connection, sql, owner_oid, schema_name, max_report_bytes):
    original_path = await connection.fetchval("SELECT pg_catalog.current_setting('search_path')")
    try:
        await connection.execute("SELECT pg_catalog.set_config('search_path','pg_catalog, pg_temp',true)")
        bounded_sql = _BOUNDED_ROWS_SQL.format(sql=sql, parameter=4)
        proof = await connection.fetchrow(
            bounded_sql, owner_oid, schema_name, "ptg2_installed_physical_binding", max_report_bytes
        )
    except BaseException as primary_error:
        try:
            await connection.execute("SELECT pg_catalog.set_config('search_path',$1,true)", original_path)
        except BaseException:
            primary_error.add_note("LOCAL deparser settings restoration was incomplete")
        raise
    else:
        await connection.execute("SELECT pg_catalog.set_config('search_path',$1,true)", original_path)
        return proof


async def _qualify_installed_view(connection, max_report_bytes):
    expected_sha = physical._qualified_local_read_view_sha(False)
    owner_oid = await _installed_owner(connection, max_report_bytes)
    schema = resolve_ptg2_schema()
    ordinary = await connection.fetchval(
        "SELECT NOT pg_has_role(current_user,$1::oid,'MEMBER') "
        "AND NOT has_schema_privilege(current_user,to_regnamespace($2),'CREATE')",
        owner_oid,
        schema,
    )
    if ordinary is not True:
        raise physical.PTG2PhysicalBindingError("PTG local reader privileges differ")
    query = physical._local_physical_view_catalog_sql()
    for name, parameter in ((":owner_oid", "$1::oid"), (":schema", "$2::text"), (":view", "$3::text")):
        query = query.replace(name, parameter)
    first_oid = None
    for lock_held in (False, True):
        if lock_held:
            await connection.execute(
                f"LOCK TABLE ONLY {_quote_ident(schema)}.ptg2_installed_physical_binding IN ACCESS SHARE MODE NOWAIT"
            )
        proof = await _view_proof(connection, query, owner_oid, schema, max_report_bytes)
        first_oid = physical._validate_local_physical_view_catalog(
            proof, expected_sha, is_prepared=False, first_oid=first_oid, lock_held=lock_held
        )
    return owner_oid


async def _installed_bindings(connection, snapshot_ids, owner_oid, max_report_bytes):
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
        ownership, binding = validation._local_read_authority_binding(
            native_row_by_field, identity, owner_oid, is_prepared=False
        )
        by_snapshot[identity] = (ownership, binding, native_row_by_field)
    return by_snapshot


async def _lock_families(connection, by_snapshot, max_report_bytes):
    relations = sorted(
        {binding.relation(name) for _, binding, _ in by_snapshot.values() for name, _ in binding.relation_oids}
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


def _family_inventory(ownership, relations, sequences):
    expected_heap_by_name = dict(ownership.relation_oids)
    observed_heap_by_name = {}
    observed_sequence_by_name = {}
    expected_sequences = archive._expected_model_family_sequences(
        physical.local_data_family_spec(), include_identity=True
    )
    sequence_tuples = tuple(
        (
            catalog_row_by_field["sequence_name"],
            catalog_row_by_field["sequence_oid"],
            catalog_row_by_field["table_name"],
            catalog_row_by_field["column_name"],
        )
        for catalog_row_by_field in sequences
    )
    if tuple((name, table, column) for name, _, table, column in sequence_tuples) != expected_sequences:
        raise physical.PTG2PhysicalBindingError("PTG local identity sequence set differs")
    if sequence_tuples != ownership.sequence_oids:
        raise physical.PTG2PhysicalBindingError("PTG local identity sequence OIDs differ")
    observed_oids = set()
    for native_row_by_field in relations:
        if native_row_by_field["oid"] in observed_oids:
            raise physical.PTG2PhysicalBindingError("PTG local duplicate native object identity")
        observed_oids.add(native_row_by_field["oid"])
        if native_row_by_field["schema_name"] != ownership.schema_name:
            raise physical.PTG2PhysicalBindingError("PTG local schema identity differs")
        oid, kind, name = native_row_by_field["oid"], native_row_by_field["relkind"], native_row_by_field["relname"]
        if kind == "r" and expected_heap_by_name.get(name) == oid and native_row_by_field["ordinary"] is True:
            observed_heap_by_name[name] = oid
        elif kind == "i" and native_row_by_field["index_table_oid"] in expected_heap_by_name.values():
            continue
        elif kind == "S" and (name, oid) in {(entry[0], entry[1]) for entry in sequence_tuples}:
            observed_sequence_by_name[name] = oid
        else:
            raise physical.PTG2PhysicalBindingError("PTG local namespace inventory differs")
    if observed_sequence_by_name != {entry[0]: entry[1] for entry in sequence_tuples}:
        raise physical.PTG2PhysicalBindingError("PTG local complete sequence inventory differs")
    if observed_heap_by_name != expected_heap_by_name:
        raise physical.PTG2PhysicalBindingError("PTG local complete heap inventory differs")


async def _verify_families(connection, by_schema, max_report_bytes):
    schema_oids = list(by_schema)
    namespace_sql = """SELECT n.oid::bigint AS schema_oid,n.nspname AS schema_name,c.oid::bigint AS oid,c.relname,
 c.relkind::text AS relkind,i.indrelid::bigint AS index_table_oid,
 c.relpersistence='p' AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity AS ordinary
 FROM pg_namespace n JOIN pg_class c ON c.relnamespace=n.oid LEFT JOIN pg_index i ON i.indexrelid=c.oid
 WHERE n.oid=ANY($1::oid[]) ORDER BY n.oid,c.oid"""
    sequence_sql = """SELECT s.relnamespace::bigint AS schema_oid,s.relname AS sequence_name,s.oid::bigint AS sequence_oid,
 t.relname AS table_name,a.attname AS column_name FROM pg_class s JOIN pg_depend d
 ON d.classid='pg_class'::regclass AND d.objid=s.oid AND d.objsubid=0 AND d.refclassid='pg_class'::regclass
 AND d.deptype IN ('a','i') JOIN pg_class t ON t.oid=d.refobjid
 JOIN pg_attribute a ON a.attrelid=t.oid AND a.attnum=d.refobjsubid
 WHERE s.relnamespace=ANY($1::oid[]) AND s.relkind='S' ORDER BY s.relnamespace,s.relname"""
    relations = await _bounded_fetch(connection, namespace_sql, (schema_oids,), max_report_bytes)
    sequences = await _bounded_fetch(connection, sequence_sql, (schema_oids,), max_report_bytes)
    if any(native_row_by_field["schema_oid"] not in by_schema for native_row_by_field in (*relations, *sequences)):
        raise physical.PTG2PhysicalBindingError("PTG local foreign schema identity")
    for schema_oid, (ownership, _, _) in by_schema.items():
        _family_inventory(
            ownership,
            [
                catalog_row_by_field
                for catalog_row_by_field in relations
                if catalog_row_by_field["schema_oid"] == schema_oid
            ],
            [
                catalog_row_by_field
                for catalog_row_by_field in sequences
                if catalog_row_by_field["schema_oid"] == schema_oid
            ],
        )


async def _closed_custody(connection, by_schema, max_report_bytes):
    heaps = [
        (oid, schema_oid) for schema_oid, (ownership, _, _) in by_schema.items() for _, oid in ownership.relation_oids
    ]
    sequences = [
        (oid, schema_oid)
        for schema_oid, (ownership, _, _) in by_schema.items()
        for _, oid, _, _ in ownership.sequence_oids
    ]
    requested = """WITH requested AS (
 SELECT schema_oid,owner_oid,
 ARRAY(SELECT oid FROM unnest($3::oid[],$4::oid[]) h(oid,owner_schema) WHERE owner_schema=r.schema_oid) AS heaps,
 ARRAY(SELECT oid FROM unnest($5::oid[],$6::oid[]) s(oid,owner_schema) WHERE owner_schema=r.schema_oid) AS sequences
 FROM unnest($1::oid[],$2::oid[]) r(schema_oid,owner_oid))"""
    proof_sql = physical._closed_local_custody_query(
        owner_parameter="request.owner_oid",
        schema_parameter="request.schema_oid",
        heaps_parameter="request.heaps",
        sequences_parameter="request.sequences",
        objects_parameter="request.heaps||request.sequences",
    )
    sql = (
        requested
        + " SELECT request.schema_oid,custody.* FROM requested request CROSS JOIN LATERAL ("
        + proof_sql
        + ") custody"
    )
    arguments = (
        list(by_schema),
        [binding.owner_oid for _, binding, _ in by_schema.values()],
        [x[0] for x in heaps],
        [x[1] for x in heaps],
        [x[0] for x in sequences],
        [x[1] for x in sequences],
    )
    native_rows = await _bounded_fetch(connection, sql, arguments, max_report_bytes)
    observed_by_schema = {}
    for native_row_by_field in native_rows:
        schema_oid = native_row_by_field["schema_oid"]
        if schema_oid not in by_schema or schema_oid in observed_by_schema:
            raise physical.PTG2PhysicalBindingError("PTG local custody identity differs")
        physical._require_closed_local_custody_proof(by_schema[schema_oid][0], native_row_by_field)
        observed_by_schema[schema_oid] = True
    if set(observed_by_schema) != set(by_schema):
        raise physical.PTG2PhysicalBindingError("PTG local custody is unavailable")


async def _catalog_digests(connection, by_schema, max_report_bytes):
    object_sql, column_sql = physical._local_catalog_queries("$1", "$1", "$2")
    object_sql = object_sql.replace("c.relnamespace=$1", "c.relnamespace=ANY($1::oid[])")
    for kind in ("relation", "index", "constraint"):
        object_sql = object_sql.replace(f"SELECT '{kind}'", f"SELECT c.relnamespace::bigint AS schema_oid,'{kind}'")
    column_sql = column_sql.replace("SELECT c.relname", "SELECT n.oid::bigint AS schema_oid,c.relname", 1)
    column_sql = column_sql.replace("n.nspname=$1", "n.nspname=ANY($1::text[])")
    objects = await _bounded_fetch(connection, object_sql, (list(by_schema),), max_report_bytes)
    columns = await _bounded_fetch(
        connection,
        column_sql,
        (
            [ownership.schema_name for ownership, _, _ in by_schema.values()],
            list(physical.local_data_family_spec().table_names),
        ),
        max_report_bytes,
    )
    if any(native_row_by_field["schema_oid"] not in by_schema for native_row_by_field in (*objects, *columns)):
        raise physical.PTG2PhysicalBindingError("PTG local catalog schema identity differs")
    for schema_oid, (ownership, _, authority) in by_schema.items():
        selected_objects = [
            {
                field_name: field_value
                for field_name, field_value in catalog_row_by_field.items()
                if field_name != "schema_oid"
            }
            for catalog_row_by_field in objects
            if catalog_row_by_field["schema_oid"] == schema_oid
        ]
        selected_columns = [
            {
                field_name: field_value
                for field_name, field_value in catalog_row_by_field.items()
                if field_name != "schema_oid"
            }
            for catalog_row_by_field in columns
            if catalog_row_by_field["schema_oid"] == schema_oid
        ]
        digest = physical._local_catalog_digest(ownership, selected_objects, selected_columns)
        if digest != authority["native_validation"]["catalog_sha256"]:
            raise physical.PTG2PhysicalBindingError("PTG local read catalog changed")


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
        for number, (_, binding, _) in enumerate(entries, 1)
    )
    sql = _union_sql(fragments, max_report_bytes)
    controls = await _bounded_fetch(
        connection,
        sql,
        (
            [binding.snapshot_id for _, binding, _ in entries],
            [binding.payload_snapshot_key for _, binding, _ in entries],
        ),
        max_report_bytes,
    )
    plans = await _bounded_fetch(
        connection,
        f"SELECT snapshot_id,plan_id,plan_market_type FROM {_quote_ident(resolve_ptg2_schema())}.ptg2_v3_snapshot_plan_scope "
        "WHERE snapshot_id=ANY($1::text[]) ORDER BY snapshot_id,plan_id,plan_market_type",
        ([binding.snapshot_id for _, binding, _ in entries],),
        max_report_bytes,
    )
    if any(plan["snapshot_id"] not in {binding.snapshot_id for _, binding, _ in entries} for plan in plans):
        raise physical.PTG2PhysicalBindingError("PTG local plan identity differs")
    by_request = {}
    for control in controls:
        number = control.pop("request_no")
        if type(number) is not int or not 1 <= number <= len(entries) or number in by_request:
            raise physical.PTG2PhysicalBindingError("PTG local published control identity differs")
        _, binding, authority = entries[number - 1]
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
        for number, (_, binding, _) in enumerate(entries, 1)
    )
    selected = await _bounded_fetch(
        connection,
        _union_sql(fragments, max_report_bytes) + " ORDER BY request_no,source_key",
        ([binding.payload_snapshot_id for _, binding, _ in entries],),
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
    owner_oid = await _qualify_installed_view(connection, max_report_bytes)
    original_by_snapshot_id = {
        catalog_row_by_field["snapshot_id"]: catalog_row_by_field for catalog_row_by_field in published_rows
    }
    by_snapshot = await _installed_bindings(connection, tuple(original_by_snapshot_id), owner_oid, max_report_bytes)
    if not by_snapshot:
        return {}
    entries = list(by_snapshot.values())
    by_schema = {}
    for ownership, binding, authority in entries:
        previous = by_schema.get(binding.schema_oid)
        if previous is not None and (
            previous[0] != ownership
            or previous[2]["native_validation"]["catalog_sha256"] != authority["native_validation"]["catalog_sha256"]
        ):
            raise physical.PTG2PhysicalBindingError("PTG local family aliases differ")
        by_schema[binding.schema_oid] = (ownership, binding, authority)
    await _lock_families(connection, by_snapshot, max_report_bytes)
    await _verify_families(connection, by_schema, max_report_bytes)
    await _closed_custody(connection, by_schema, max_report_bytes)
    await _catalog_digests(connection, by_schema, max_report_bytes)
    controls = await _published_controls(connection, entries, max_report_bytes)
    source_by_request = await _source_dictionaries(connection, entries, max_report_bytes)
    resolved_by_snapshot_id = {}
    for number, (_, binding, _) in enumerate(entries, 1):
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
    """Complete installed custody only; payload graph/finalizer and release readiness remain separate."""
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
