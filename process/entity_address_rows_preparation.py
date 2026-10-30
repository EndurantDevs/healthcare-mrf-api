# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Prepare one source snapshot through admitted, identity-owned address scratch."""

from __future__ import annotations

import asyncio
import io
from contextlib import asynccontextmanager
from uuid import uuid4

from db.plan_release_storage import storage_relation
from process import entity_address_preparation_admission as admission
from process import entity_address_rows_native as rows
from process.provider_directory_projection_stage import _copy_driver


def _normalized_expressions(sql):
    return {
        "first_line_norm": sql._alnum_norm_expr("s.first_line"),
        "second_line_norm": sql._alnum_norm_expr("s.second_line"),
        "city_norm": sql._alnum_norm_expr("s.city_name"),
        "state_code": sql._state_norm_expr("s.state_name"),
        "zip5": sql._zip5_norm_expr("s.postal_code"),
        "country_norm": sql._state_norm_expr("s.country_code"),
        "telephone_digits": sql._phone_norm_expr("s.telephone_number"),
    }


def _prepared_schema_select(sql, candidate, include_base_address_version):
    expression_by_field = rows.source_value_expressions(sql)
    if not include_base_address_version:
        expression_by_field["base_address_version"] = "NULL::text"
    sanitized_sql = ", ".join(f"({expression})::text AS {field}" for field, expression in expression_by_field.items())
    normalized_sql = ", ".join(
        f"({expression})::text AS {field}" for field, expression in _normalized_expressions(sql).items()
    )
    return (
        f"WITH s AS (SELECT b.row_id, {sanitized_sql} FROM {candidate} b) "
        f"SELECT s.*, {normalized_sql}, FALSE AS native_case_safe, FALSE AS native_contact_safe FROM s"
    )


async def _create_scratch(sql, schema, name, statement, oid_by_name, row_scratch):
    if row_scratch is not None:
        await admission.create_stage_sql(schema, name, statement, row_scratch=row_scratch)
        oid_by_name[name] = admission._ADMISSION.get().owned_oids[name]
        return

    async def create():
        """Capture successful creation before forwarding any pending cancellation."""
        await admission.create_stage_sql(schema, name, statement)
        scope = admission._ADMISSION.get()
        oid = (
            scope.owned_oids.get(name)
            if scope is not None
            else await admission.read_scalar(
                sql.db, "SELECT to_regclass(:relation)::oid::bigint", relation=f"{schema}.{name}"
            )
        )
        if type(oid) is not int or oid <= 0:
            raise RuntimeError("address row scratch identity is missing")
        oid_by_name[name] = oid

    await admission._finish_owned_operation(create)


async def _drop_scratch(sql, schema, names, oid_by_name):
    scope = admission._ADMISSION.get()
    for name in reversed(names):
        if scope is not None:
            if name in scope.owned_oids:
                await admission.drop_stage(schema, name)
            continue
        expected_oid = oid_by_name.get(name)
        if expected_oid is None:
            continue
        async with admission.native_transaction():
            relation = storage_relation(schema, name)
            await sql.db.status("SET LOCAL lock_timeout='500ms'")
            await sql.db.status(f"LOCK TABLE {relation} IN ACCESS EXCLUSIVE MODE NOWAIT")
            actual_oid = await sql.db.scalar("SELECT to_regclass(:relation)::oid::bigint", relation=f"{schema}.{name}")
            if actual_oid != expected_oid:
                raise RuntimeError("address row scratch identity changed")
            await admission.drop_stage(schema, name)


@asynccontextmanager
async def _scratch_tables(sql, schema, source_select, include_base_address_version, row_scratch):
    suffix = uuid4().hex
    names = (f"address_rows_input_{suffix}", f"address_rows_prepared_{suffix}")
    candidate = storage_relation(schema, names[0])
    prepared = storage_relation(schema, names[1])
    oid_by_name = {}
    try:
        await _create_scratch(
            sql,
            schema,
            names[0],
            f"CREATE TABLE {candidate} AS SELECT row_number() OVER ()::bigint AS row_id, b.* "
            f"FROM ({source_select}) b WITH NO DATA",
            oid_by_name,
            row_scratch,
        )
        schema_select = _prepared_schema_select(sql, candidate, include_base_address_version)
        await _create_scratch(
            sql, schema, names[1], f"CREATE TABLE {prepared} AS {schema_select} WITH NO DATA", oid_by_name, row_scratch
        )
        yield names, oid_by_name, schema_select
    finally:
        await admission._finish_owned_operation(lambda: _drop_scratch(sql, schema, names, oid_by_name))
        admission.finish_row_scratch(row_scratch)


async def _candidate_metadata(driver, schema, name):
    return await driver.fetch(
        "SELECT a.attname, CASE WHEN c.collnamespace='pg_catalog'::regnamespace AND "
        "((c.collname IN ('C','POSIX') AND c.collprovider='c' AND c.collversion IS NULL) OR "
        "(c.collname='default' AND d.datlocprovider='c' AND d.datcollate IN ('C','POSIX') "
        "AND d.datctype IN ('C','POSIX'))) THEN TRUE ELSE FALSE END AS is_case_qualified "
        "FROM pg_attribute a LEFT JOIN pg_collation c ON c.oid=a.attcollation "
        "CROSS JOIN pg_database d WHERE a.attrelid=to_regclass($1) AND a.attnum>0 "
        "AND NOT a.attisdropped AND d.datname=current_database() ORDER BY a.attnum",
        f"{schema}.{name}",
    )


def _source_projection(sql, metadata, candidate, prepared):
    qualified_by_name = {entry["attname"]: entry["is_case_qualified"] for entry in metadata}
    expression_by_field = rows.prepared_value_expressions(
        sql,
        case_qualified=all(
            qualified_by_name.get(field, False)
            for field in ("first_line", "second_line", "city_name", "state_name", "country_code")
        ),
        contact_qualified=all(
            qualified_by_name.get(field, False) for field in ("country_code", "telephone_number", "fax_number")
        ),
    )
    columns = [
        f"({expression_by_field[field]}) AS {field}" if field in rows.TEXT_COLUMNS else f'b."{field}"'
        for field in qualified_by_name
        if field != "row_id"
    ]
    columns.extend(f"({expression_by_field[field]}) AS {field}" for field in rows.CONTACT_COLUMNS)
    columns.extend(f"({expression_by_field[field]}) AS prepared_{field}" for field in rows.NORMALIZED_COLUMNS)
    return f"SELECT {', '.join(columns)} FROM {candidate} b JOIN {prepared} p USING(row_id)"


def _input_lengths(include_base_address_version):
    return [
        "0"
        if field == "base_address_version" and not include_base_address_version
        else f"COALESCE(octet_length(b.{field}::text),0)"
        for field in rows.TEXT_COLUMNS
    ]


async def _batch_end(driver, candidate, first_id, include_base_address_version):
    lengths = _input_lengths(include_base_address_version)
    return await driver.fetchval(
        "WITH batch AS (SELECT row_id, 66::bigint+" + "+".join(lengths) + " AS row_bytes, "
        f"GREATEST({','.join(lengths)})<=$3 AS is_bounded FROM {candidate} b "
        "WHERE row_id BETWEEN $1 AND $2), budget AS (SELECT row_id, "
        "sum(row_bytes) OVER (ORDER BY row_id) AS total_bytes, "
        "bool_and(is_bounded) OVER (ORDER BY row_id) AS is_bounded FROM batch) "
        "SELECT max(row_id) FILTER (WHERE total_bytes<=$4 AND is_bounded) FROM budget",
        first_id,
        first_id + rows.MAX_ROWS - 1,
        rows.MAX_FIELD_BYTES,
        rows.MAX_INPUT_BYTES - 21,
    )


class _BoundedCopyBuffer(io.BytesIO):
    def write(self, value):
        """Refuse bytes beyond the exact batch budget before retaining the chunk."""
        if self.tell() + len(value) > rows.MAX_INPUT_BYTES:
            raise RuntimeError("address row export exceeded its measured byte budget")
        return super().write(value)


async def _prepare_bytes(payload):
    from process.uhc_flex_practitioner_async_safety import drain_operation

    return await drain_operation(
        asyncio.to_thread(rows.prepare_entity_address_rows, payload), preserve_cancellation=True
    )


async def _copy_batch(driver, schema, names, first_id, last_id, include_base_address_version):
    candidate = storage_relation(schema, names[0])
    input_bytes = _BoundedCopyBuffer()
    await driver.copy_from_query(
        f"SELECT {rows.input_projection_sql(include_base_address_version=include_base_address_version)} "
        f"FROM {candidate} b WHERE row_id BETWEEN $1 AND $2 ORDER BY row_id",
        first_id,
        last_id,
        output=input_bytes,
        format="binary",
    )
    prepared, count = await _prepare_bytes(input_bytes.getvalue())
    copied = await driver.copy_to_table(names[1], schema_name=schema, source=io.BytesIO(prepared), format="binary")
    if count != last_id - first_id + 1 or copied != f"COPY {count}":
        raise RuntimeError("address row native COPY accounting differs")


async def _fill_prepared_rows(driver, schema, names, schema_select, include_base_address_version):
    candidate = storage_relation(schema, names[0])
    prepared = storage_relation(schema, names[1])
    last_id = await driver.fetchval(f"SELECT COALESCE(max(row_id),0) FROM {candidate}")
    first_id = 1
    while first_id <= last_id:
        await admission.check_owned_storage()
        batch_end = await _batch_end(driver, candidate, first_id, include_base_address_version)
        if batch_end is None:
            await driver.execute(f"INSERT INTO {prepared} {schema_select} WHERE s.row_id=$1", first_id)
            batch_end = first_id
        else:
            await _copy_batch(driver, schema, names, first_id, batch_end, include_base_address_version)
        first_id = batch_end + 1
    await driver.execute(f"LOCK TABLE {prepared} IN SHARE MODE NOWAIT")
    if await driver.fetchval(
        f"SELECT (SELECT count(*) FROM {prepared})<>$1 OR EXISTS(SELECT FROM {candidate} b "
        f"LEFT JOIN {prepared} p USING(row_id) WHERE p.row_id IS NULL)",
        last_id,
    ):
        raise RuntimeError("address row candidate census differs")


async def _consume_candidate(
    sql, schema, source_select, names, schema_select, include_base_address_version, consume, row_scratch
):
    candidate = storage_relation(schema, names[0])
    prepared = storage_relation(schema, names[1])
    async with admission.native_transaction(row_scratch=row_scratch) as session:
        await sql._apply_entity_address_transaction_settings()
        await sql.db.status(f"CREATE UNIQUE INDEX ON {candidate}(row_id)")
        await sql.db.status(f"CREATE UNIQUE INDEX ON {prepared}(row_id)")
        driver = await _copy_driver(session)
        if driver.is_in_transaction() is not True:
            raise RuntimeError("address row COPY requires its executing transaction")
        await admission.check_owned_storage()
        await driver.execute(
            f"INSERT INTO {candidate} SELECT row_number() OVER ()::bigint, b.* FROM ({source_select}) b"
        )
        await driver.execute(f"LOCK TABLE {candidate} IN SHARE MODE NOWAIT")
        metadata = await _candidate_metadata(driver, schema, names[0])
        await _fill_prepared_rows(driver, schema, names, schema_select, include_base_address_version)
        await admission.check_owned_storage()
        return await consume(_source_projection(sql, metadata, candidate, prepared), native_prepared=True)


async def run_prepared_source(sql, schema, source_select, consume, *, include_base_address_version=True):
    """Execute one source once, consume prepared rows atomically, and retire exact scratch."""
    if rows.native_preparation_module() is None:
        if admission._ADMISSION.get() is not None:
            raise RuntimeError("admitted address row native preparation is unavailable")
        return await consume(source_select, native_prepared=False)
    row_scratch = admission.issue_row_scratch()
    async with _scratch_tables(sql, schema, source_select, include_base_address_version, row_scratch) as (
        names,
        _oids,
        schema_select,
    ):
        return await _consume_candidate(
            sql, schema, source_select, names, schema_select, include_base_address_version, consume, row_scratch
        )


async def run_prepared_source_phase(
    sql, db_schema, source_select, build_sql, phase_options, *, include_base_address_version=True
):
    """Share bounded source preparation across raw loading and direct materialization."""

    async def consume(select_sql, *, native_prepared):
        """Keep source progress and the final insert on the original admitted backend."""
        return await sql._run_sql_phase(build_sql(select_sql, native_prepared=native_prepared), **phase_options)

    return await run_prepared_source(
        sql,
        db_schema,
        source_select,
        consume,
        include_base_address_version=include_base_address_version,
    )
