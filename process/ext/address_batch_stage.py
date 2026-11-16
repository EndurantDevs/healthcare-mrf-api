# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Distinct native address inputs shared by publication and ordinary stamping."""

from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager

from db.plan_release_storage import storage_identifier, storage_relation
from process.ext import address_batch
from process.uhc_flex_practitioner_async_safety import drain_operation

ADDRESS_FIELDS = ("first_line", "second_line", "city_name", "state_name", "postal_code", "country_code")


def lookup_key_sql(expressions) -> str:
    """Preserve exact six-field tuples, including NULL versus empty strings."""
    if len(expressions) != len(ADDRESS_FIELDS):
        raise ValueError("Address lookup requires six input expressions")
    return "jsonb_build_array(" + ",".join(f"({expression})::text" for expression in expressions) + ")::text"


def input_projection_sql(expressions) -> str:
    """Name the fixed input tuple without changing its source collation."""
    if len(expressions) != len(ADDRESS_FIELDS):
        raise ValueError("Address projection requires six input expressions")
    return ",".join(f"({value})::text AS {name}" for name, value in zip(ADDRESS_FIELDS, expressions, strict=True))


def field_sql(alias: str, field: str, fallback: str) -> str:
    """An accepted native NULL is a result; only an absent result falls back."""
    if field not in address_batch.RESULT_FIELDS:
        raise ValueError("Address native field differs")
    native = storage_identifier(alias) + ".native_fields"
    return f"CASE WHEN {native} IS NOT NULL THEN {native}->>'{field}' ELSE {fallback} END"


async def _has_supported_case(database, source_sql, parameters):
    probes = []
    for field in ADDRESS_FIELDS:
        prefix = f"left(COALESCE((source.{field})::text,''),0)"
        for operation, text, expected in (
            ("lower", "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"),
            ("upper", "abcdefghijklmnopqrstuvwxyz", "ABCDEFGHIJKLMNOPQRSTUVWXYZ"),
        ):
            probes.append(f"convert_to({operation}({prefix}||'{text}'),'UTF8')=convert_to('{expected}','UTF8')")
    rows = await database.all(f"SELECT {','.join(probes)} FROM ({source_sql}) source LIMIT 1", **parameters)
    return not rows or all(value is True for value in rows[0])


def _is_permission_denied(error):
    while error is not None:
        if getattr(error, "sqlstate", None) == "42501" or getattr(error, "pgcode", None) == "42501":
            return True
        error = getattr(error, "orig", None) or error.__cause__
    return False


async def _has_created_stage(database, table, temporary):
    columns = ",".join(f'{field} text COLLATE "C"' for field in ADDRESS_FIELDS)
    kind = "TEMP" if temporary else "UNLOGGED"
    commit = " ON COMMIT DROP" if temporary else ""
    try:
        async with database.transaction():
            await database.status(
                f"CREATE {kind} TABLE {table} (row_id bigint NOT NULL,{columns},"
                f'lookup_key text COLLATE "C" NOT NULL,native_fields jsonb){commit}'
            )
    except Exception as error:
        if temporary and _is_permission_denied(error):
            return False
        raise
    return True


async def _insert_inputs(database, table, source_sql, parameters):
    columns = ",".join(ADDRESS_FIELDS)
    selected = ",".join(f'(source.{field})::text COLLATE "C" AS {field}' for field in ADDRESS_FIELDS)
    key = lookup_key_sql(ADDRESS_FIELDS)
    await database.status(
        f"INSERT INTO {table} (row_id,{columns},lookup_key) "
        f"SELECT row_number() OVER (),{columns},{key} "
        f"FROM (SELECT DISTINCT {selected} FROM ({source_sql}) source) inputs",
        **parameters,
    )
    await database.status(f"CREATE UNIQUE INDEX ON {table}(row_id)")


def _page_end(headers):
    total = 0
    last = None
    for row_id, size, bounded in headers:
        total += size
        if not bounded or total > address_batch.MAX_INPUT_BYTES:
            break
        last = row_id
    return last


async def _write_native_page(database, table, start, end):
    rows = list(
        await database.all(
            f"SELECT row_id,{','.join(ADDRESS_FIELDS)} FROM {table} "
            "WHERE row_id>:start AND row_id<=:end ORDER BY row_id",
            start=start,
            end=end,
        )
    )
    prepared = await drain_operation(
        asyncio.to_thread(address_batch.prepare_address_batch, [tuple(row[1:]) for row in rows]),
        preserve_cancellation=True,
    )
    if prepared is None:
        return
    accepted_rows = [(row[0], value) for row, value in zip(rows, prepared, strict=True) if value is not None]
    if accepted_rows:
        await database.status(
            f"UPDATE {table} t SET native_fields=CAST(p.fields AS jsonb) FROM "
            "unnest(CAST(:row_ids AS bigint[]),CAST(:native_fields AS text[])) p(row_id,fields) "
            "WHERE t.row_id=p.row_id",
            row_ids=[row[0] for row in accepted_rows],
            native_fields=[json.dumps(row[1], ensure_ascii=False, separators=(",", ":")) for row in accepted_rows],
        )


async def _fill_native_pages(database, table, cancel_check):
    lengths = [f"COALESCE(octet_length({field}),0)::bigint" for field in ADDRESS_FIELDS]
    query = (
        f"SELECT row_id,{'+'.join(lengths)},GREATEST({','.join(lengths)})<=:field_limit "
        f"FROM {table} WHERE row_id>:after ORDER BY row_id LIMIT :page_rows"
    )
    cursor = 0
    while True:
        if cancel_check is not None:
            await cancel_check()
        headers = list(
            await database.all(
                query,
                after=cursor,
                page_rows=address_batch.MAX_ROWS,
                field_limit=address_batch.MAX_FIELD_BYTES,
            )
        )
        if not headers:
            return
        end = _page_end(headers)
        if end is not None:
            await _write_native_page(database, table, cursor, end)
        cursor = headers[0][0] if end is None else end


async def _has_prepared_stage(database, table, source_sql, parameters, cancel_check, temporary):
    if not await _has_supported_case(database, source_sql, parameters):
        return False
    if not await _has_created_stage(database, table, temporary):
        return False
    await _insert_inputs(database, table, source_sql, parameters)
    await _fill_native_pages(database, table, cancel_check)
    await database.status(f"CREATE INDEX ON {table} USING hash(lookup_key)")
    await database.status(f"ANALYZE {table}")
    return True


async def _drop_committed_stage(database, table):
    async with database.transaction():
        await database.status(f"DROP TABLE IF EXISTS {table}")


@asynccontextmanager
async def _committed_stage(database, table, source_sql, parameters, cancel_check):
    is_created = False
    try:
        async with database.transaction():
            is_created = await _has_prepared_stage(database, table, source_sql, parameters, cancel_check, False)
        yield table if is_created else None
    finally:
        if is_created:
            await drain_operation(_drop_committed_stage(database, table), preserve_cancellation=True)


@asynccontextmanager
async def staged_address_fields(
    database, schema, table_name, source_sql, *, parameters=None, cancel_check=None, temporary=False, committed=False
):
    """Stage distinct inputs; an explicit commit lets independent readers reuse them."""
    if committed and temporary:
        raise ValueError("Committed address stages cannot be temporary")
    if not address_batch.is_native_available() or not all(
        callable(getattr(database, name, None)) for name in ("transaction", "all", "status")
    ):
        yield None
        return
    source_sql = source_sql.strip().removesuffix(";")
    parameters = {} if parameters is None else parameters
    table = storage_relation("pg_temp" if temporary else schema, table_name)
    if committed:
        if any(
            getattr(database, name, lambda: None)() is not None for name in ("_transaction_binding", "_reader_binding")
        ):
            yield None
            return
        async with _committed_stage(database, table, source_sql, parameters, cancel_check) as prepared:
            yield prepared
        return
    async with database.transaction():
        if not await _has_prepared_stage(database, table, source_sql, parameters, cancel_check, temporary):
            yield None
            return
        yield table
        await database.status(f"DROP TABLE {table}")
