# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded native loading and carry-forward for isolated FHIR metadata candidates."""

import json
import re
from datetime import date, datetime
from uuid import UUID, uuid4

from db.plan_release_storage import storage_identifier
from db.registry_fhir_catalog import table_sql
from process.registry_fhir_catalog_custody import (
    PHYSICAL_TABLES,
    FHIRCatalogTarget,
    verify_fhir_catalog_snapshot,
)
from process.registry_fhir_catalog_projection import BINDING_COLUMNS, NETWORK_COLUMNS, RELATIONSHIP_COLUMNS

TABLE_COLUMNS = {
    "registry_fhir_catalog_source": (
        "source_generation",
        "source_id",
        "descriptor_json",
        "profile",
        "source_rows",
        "observation_rows",
        "accepted_at",
        "created_at",
    ),
    "registry_fhir_catalog_observation": (
        "source_generation",
        "resource_type",
        "resource_id",
        "observation_kind",
        "ordinal",
        "document_json",
    ),
    "registry_fhir_catalog_network": NETWORK_COLUMNS,
    "registry_fhir_catalog_relationship": RELATIONSHIP_COLUMNS,
    "registry_fhir_catalog_binding": BINDING_COLUMNS,
}
_KEYS = {
    "registry_fhir_catalog_source": (("source_generation", "text"),),
    "registry_fhir_catalog_observation": (
        ("source_generation", "text"),
        ("resource_type", "text"),
        ("resource_id", "text"),
        ("observation_kind", "text"),
        ("ordinal", "integer"),
    ),
    "registry_fhir_catalog_network": (("metadata_generation", "text"), ("network_id", "integer")),
    "registry_fhir_catalog_relationship": (("metadata_generation", "text"), ("relationship_id", "text")),
    "registry_fhir_catalog_binding": (("metadata_generation", "text"), ("binding_id", "uuid")),
}
DEFAULT_ROWS = 1000
DEFAULT_BYTES = 8 * 1024 * 1024
MAX_BYTES = 34 * 1024 * 1024


def _invalid(reason):
    raise ValueError("registry_fhir_catalog_store_" + reason)


def _target(target):
    if type(target) is not FHIRCatalogTarget:
        _invalid("target_invalid")
    return storage_identifier(target.schema_name)


def _bounds(max_rows, max_bytes):
    if (
        type(max_rows) is not int
        or not 1 <= max_rows <= 10000
        or type(max_bytes) is not int
        or not 1 <= max_bytes <= MAX_BYTES
    ):
        _invalid("bounds_invalid")


def _digest(generation):
    if type(generation) is not str or re.fullmatch(r"[0-9a-f]{64}", generation) is None:
        _invalid("generation_invalid")


def _transaction(connection):
    if not connection.is_in_transaction():
        _invalid("transaction_required")


def _table(table, columns=None):
    if table not in TABLE_COLUMNS:
        _invalid("table_invalid")
    selected_columns = TABLE_COLUMNS[table] if columns is None else tuple(columns)
    allowed_columns = (TABLE_COLUMNS[table],)
    if table == "registry_fhir_catalog_source":
        allowed_columns += (TABLE_COLUMNS[table][:-1],)
    if selected_columns not in allowed_columns:
        _invalid("columns_invalid")
    return selected_columns


async def _unregistered(connection, target, control_schema):
    if not connection.is_in_transaction():
        _invalid("transaction_required")
    if await connection.fetchval(
        f"SELECT EXISTS(SELECT FROM {storage_identifier(control_schema)}.registry_fhir_catalog_snapshot WHERE schema_name=$1)",
        target.schema_name,
    ):
        _invalid("snapshot_immutable")


async def _mutable(connection, target, loader_role, control_schema):
    namespace = _target(target)
    storage_identifier(loader_role)
    if not connection.is_in_transaction():
        _invalid("transaction_required")
    heaps = ",".join(namespace + "." + table for table in sorted(PHYSICAL_TABLES))
    await connection.execute("LOCK TABLE " + heaps + " IN ROW EXCLUSIVE MODE")
    await _unregistered(connection, target, control_schema)
    native = await connection.fetchrow(
        "SELECT n.nspowner::bigint,(SELECT oid::bigint FROM pg_roles WHERE rolname=$2) AS loader_oid,"
        "array_agg(c.relname::text ORDER BY c.relname) FILTER(WHERE c.relkind='r') AS heaps,"
        "bool_or(c.relkind NOT IN ('r','i') OR c.relpersistence<>'p' OR c.relowner<>n.nspowner "
        "OR c.relrowsecurity OR c.relforcerowsecurity OR c.relispartition "
        "OR EXISTS(SELECT FROM pg_trigger WHERE tgrelid=c.oid AND NOT tgisinternal) "
        "OR EXISTS(SELECT FROM pg_rewrite WHERE ev_class=c.oid) "
        "OR EXISTS(SELECT FROM pg_inherits WHERE inhrelid=c.oid OR inhparent=c.oid)) AS unsupported "
        "FROM pg_namespace n LEFT JOIN pg_class c ON c.relnamespace=n.oid WHERE n.nspname=$1 GROUP BY n.oid,n.nspowner",
        target.schema_name,
        loader_role,
    )
    if (
        native is None
        or native["nspowner"] != native["loader_oid"]
        or set(native["heaps"] or ()) != PHYSICAL_TABLES
        or native["unsupported"]
    ):
        _invalid("owner_invalid")


async def create_fhir_catalog_candidate(connection, target, *, loader_role, control_schema):
    """Create only fresh heaps and their essential primary-key indexes."""
    namespace = _target(target)
    role = storage_identifier(loader_role)
    await _unregistered(connection, target, control_schema)
    async with connection.transaction():
        await connection.execute(f"CREATE SCHEMA {namespace} AUTHORIZATION {role}")
        own_role = await connection.fetchval("SELECT current_user")
        for table in sorted(PHYSICAL_TABLES):
            await connection.execute(table_sql(namespace, table)[0])
            if own_role != loader_role:
                await connection.execute(f"ALTER TABLE {namespace}.{table} OWNER TO {role}")
        await _mutable(connection, target, loader_role, control_schema)
    return len(PHYSICAL_TABLES)


async def has_discarded_fhir_catalog_candidate(connection, target, *, loader_role, control_schema):
    """Remove only an unregistered candidate still owned by its original loader."""
    _transaction(connection)
    namespace = _target(target)
    if not await connection.fetchval("SELECT current_user=session_user AND current_user::text=$1", loader_role):
        _invalid("owner_invalid")
    await _unregistered(connection, target, control_schema)
    if not await connection.fetchval("SELECT to_regnamespace($1) IS NOT NULL", target.schema_name):
        return False
    await _mutable(connection, target, loader_role, control_schema)
    heaps = ",".join(namespace + "." + table for table in sorted(PHYSICAL_TABLES))
    await connection.execute("LOCK TABLE " + heaps + " IN ACCESS EXCLUSIVE MODE")
    await _unregistered(connection, target, control_schema)
    await connection.execute("DROP TABLE " + heaps)
    await connection.execute("DROP SCHEMA " + namespace)
    return True


discard_fhir_catalog_candidate = has_discarded_fhir_catalog_candidate


async def create_fhir_catalog_indexes(connection, target, *, loader_role, control_schema, trigram_schema="public"):
    """Prepare secondary indexes and statistics on the completed candidate."""
    namespace = _target(target)
    _transaction(connection)
    trigram = storage_identifier(trigram_schema)
    async with connection.transaction():
        await _mutable(connection, target, loader_role, control_schema)
        count = 0
        for table in sorted(PHYSICAL_TABLES):
            for statement in table_sql(namespace, table, trigram_schema=trigram)[1:]:
                await connection.execute(statement)
                count += 1
            await connection.execute(f"ANALYZE {namespace}.{table}")
    return count


def _payload_size(value):
    if value is None:
        return 0
    if isinstance(value, str):
        return len(value.encode("utf-8")) + 1
    if isinstance(value, UUID):
        return 16
    if isinstance(value, bool):
        return 1
    if isinstance(value, int):
        return 8
    if isinstance(value, datetime | date):
        return max(8, len(value.isoformat().encode()))
    if isinstance(value, list | tuple):
        return 20 + sum(4 + _payload_size(entry) for entry in value)
    _invalid("row_value_invalid")


def _batches(rows, columns, max_rows, max_bytes):
    batch_rows, batch_bytes = [], 21
    for row in rows:
        if not isinstance(row, tuple | list) or len(row) != len(columns):
            _invalid("row_shape_invalid")
        row_bytes = 2 + sum(8 + _payload_size(value) for value in row)
        if row_bytes + 21 > max_bytes:
            _invalid("row_too_large")
        if batch_rows and (len(batch_rows) == max_rows or batch_bytes + row_bytes > max_bytes):
            yield batch_rows, batch_bytes
            batch_rows, batch_bytes = [], 21
        batch_rows.append(tuple(row))
        batch_bytes += row_bytes
    if batch_rows:
        yield batch_rows, batch_bytes


def _conflict(table, columns, mode):
    keys = tuple(name for name, _ in _KEYS[table])
    if mode == "preserve":
        return "ON CONFLICT (" + ",".join(keys) + ") DO NOTHING"
    if mode != "replace":
        _invalid("mode_invalid")
    assignments = ",".join(name + "=EXCLUDED." + name for name in columns if name not in keys)
    return "ON CONFLICT (" + ",".join(keys) + ") DO UPDATE SET " + assignments


def _write_options(options):
    default_by_name = {"mode": "replace", "columns": None, "max_rows": DEFAULT_ROWS, "max_bytes": DEFAULT_BYTES}
    if set(options) - set(default_by_name):
        _invalid("options_invalid")
    option_by_name = {**default_by_name, **options}
    return tuple(option_by_name[name] for name in ("mode", "columns", "max_rows", "max_bytes"))


async def write_fhir_catalog_rows(
    connection,
    copy_target,
    table,
    copy_rows,
    *,
    loader_role,
    control_schema,
    **options,
):
    """COPY bounded landing batches and apply one set upsert per batch."""
    mode, columns, max_rows, max_bytes = _write_options(options)
    selected_columns = _table(table, columns)
    _transaction(connection)
    conflict = _conflict(table, selected_columns, mode)
    _bounds(max_rows, max_bytes)
    namespace = _target(copy_target)
    landing = "fhir_landing_" + uuid4().hex
    column_sql = ",".join(selected_columns)
    keys = ",".join(name for name, _ in _KEYS[table])
    accounting_by_name = {"input_rows": 0, "affected_rows": 0, "batches": 0, "payload_bytes": 0}
    async with connection.transaction():
        await _mutable(connection, copy_target, loader_role, control_schema)
        await connection.execute(
            f"CREATE TEMP TABLE {landing} (LIKE {namespace}.{table} INCLUDING DEFAULTS) ON COMMIT DROP"
        )
        for batch_rows, batch_bytes in _batches(copy_rows, selected_columns, max_rows, max_bytes):
            await connection.execute(f"TRUNCATE pg_temp.{landing}")
            await connection.copy_records_to_table(
                landing, records=batch_rows, columns=selected_columns, schema_name="pg_temp"
            )
            command_status = await connection.execute(
                f"INSERT INTO {namespace}.{table} ({column_sql}) SELECT DISTINCT ON ({keys}) {column_sql} "
                f"FROM pg_temp.{landing} ORDER BY {keys},ctid DESC {conflict}"
            )
            accounting_by_name["input_rows"] += len(batch_rows)
            accounting_by_name["affected_rows"] += int(command_status.rsplit(" ", 1)[-1])
            accounting_by_name["batches"] += 1
            accounting_by_name["payload_bytes"] += batch_bytes
        await connection.execute(f"DROP TABLE pg_temp.{landing}")
    return accounting_by_name


async def replace_fhir_catalog_relationships(connection, target, source_generations, *, loader_role, control_schema):
    """Remove selected derived edges only; observations and old editions stay retained."""
    generations = tuple(source_generations)
    _transaction(connection)
    for generation in generations:
        _digest(generation)
    if len(generations) > 10000:
        _invalid("bounds_invalid")
    namespace = _target(target)
    async with connection.transaction():
        await _mutable(connection, target, loader_role, control_schema)
        result = await connection.execute(
            f"DELETE FROM {namespace}.registry_fhir_catalog_relationship WHERE source_generation=ANY($1::text[])",
            generations,
        )
    return int(result.rsplit(" ", 1)[-1])


def _copy_sql(previous_schema, candidate_schema, table):
    columns = TABLE_COLUMNS[table]
    keys = _KEYS[table]
    order = ",".join(name + (' COLLATE "C"' if kind == "text" else "") for name, kind in keys)
    compare = "(" + order + ")>(" + ",".join(f"${index + 1}::{kind}" for index, (_, kind) in enumerate(keys)) + ")"
    expressions = [
        "$" + str(len(keys) + 3) + "::text"
        if name == "metadata_generation"
        else 'document_json||\'{"status":"last_good"}\'::jsonb'
        if table.endswith("_binding") and name == "document_json"
        else name
        for name in columns
    ]
    byte_size = "2+" + "+".join(
        f"8+coalesce(greatest(octet_length(({expression})::text),pg_column_size({expression})),0)"
        for expression in expressions
    )
    column_sql = ",".join(columns)
    return f"""WITH ordered AS MATERIALIZED (
      SELECT *,{byte_size} AS row_bytes FROM {previous_schema}.{table}
      WHERE ($1::{keys[0][1]} IS NULL OR {compare}) AND ${len(keys) + 3}::text IS NOT NULL
      ORDER BY {order} LIMIT ${len(keys) + 1}::integer
    ), windowed AS MATERIALIZED (
      SELECT *,sum(row_bytes) OVER(ORDER BY {order}) AS running_bytes FROM ordered
    ), chosen AS MATERIALIZED (
      SELECT * FROM windowed WHERE running_bytes+21<=${len(keys) + 2}::bigint
    ), inserted AS (
      INSERT INTO {candidate_schema}.{table} ({column_sql}) SELECT {",".join(expressions)} FROM chosen
      {_conflict(table, columns, "preserve")} RETURNING 1
    ) SELECT (SELECT count(*) FROM ordered)::bigint AS seen,
      (SELECT row_bytes FROM ordered ORDER BY {order} LIMIT 1)::bigint AS first_bytes,
      (SELECT count(*) FROM chosen)::bigint AS copied,
      coalesce((SELECT sum(row_bytes) FROM chosen),0)::bigint+21 AS payload_bytes,
      (SELECT jsonb_build_array({",".join(name for name, _ in keys)})::text FROM chosen
        ORDER BY {",".join(name + (' COLLATE "C"' if kind == "text" else "") + " DESC" for name, kind in keys)} LIMIT 1) AS cursor_json,
      (SELECT count(*) FROM inserted)::bigint AS affected"""


async def copy_fhir_catalog_snapshot(
    connection,
    previous_snapshot,
    copy_target,
    metadata_generation,
    *,
    loader_role,
    control_schema,
    max_rows=DEFAULT_ROWS,
    max_bytes=DEFAULT_BYTES,
):
    """Carry all sealed metadata heaps forward without touching old indexes or serving pins."""
    _bounds(max_rows, max_bytes)
    _transaction(connection)
    _digest(metadata_generation)
    namespace = _target(copy_target)
    previous = storage_identifier(previous_snapshot["schema_name"])
    if previous == namespace:
        _invalid("snapshot_immutable")
    await verify_fhir_catalog_snapshot(connection, previous_snapshot)
    accounting_by_name = {"input_rows": 0, "affected_rows": 0, "batches": 0, "payload_bytes": 0}
    async with connection.transaction():
        await _mutable(connection, copy_target, loader_role, control_schema)
        for table in sorted(PHYSICAL_TABLES):
            keys = _KEYS[table]
            cursor_values = [None] * len(keys)
            sql = _copy_sql(previous, namespace, table)
            while True:
                page = await connection.fetchrow(sql, *cursor_values, max_rows, max_bytes, metadata_generation)
                if page["seen"] and page["first_bytes"] + 21 > max_bytes:
                    _invalid("row_too_large")
                if not page["copied"]:
                    break
                for name, key in (
                    ("input_rows", "copied"),
                    ("affected_rows", "affected"),
                    ("payload_bytes", "payload_bytes"),
                ):
                    accounting_by_name[name] += page[key]
                accounting_by_name["batches"] += 1
                cursor_values = json.loads(page["cursor_json"])
                cursor_values = [
                    UUID(key_value) if kind == "uuid" else key_value
                    for key_value, (_, kind) in zip(cursor_values, keys, strict=True)
                ]
                if page["seen"] < max_rows and page["copied"] == page["seen"]:
                    break
    return accounting_by_name
