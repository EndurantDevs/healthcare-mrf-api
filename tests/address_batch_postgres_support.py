# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Isolated PostgreSQL fixtures for shared native address preparation."""

import importlib.util
import os
from contextlib import asynccontextmanager
from pathlib import Path
from uuid import uuid4

import pytest

from db.connection import Database
from process.ext import address_batch, address_batch_stamp, address_canon

FIELDS = dict(
    zip(
        ("first_line", "second_line", "city", "state", "zip", "country"),
        ("first_line", "second_line", "city_name", "state_name", "postal_code", "country_code"),
        strict=True,
    )
)


@asynccontextmanager
async def stamp_database(monkeypatch):
    if "test" not in os.getenv("HLTHPRT_DB_DATABASE", "").lower():
        pytest.skip("native address stamping requires a disposable test database")
    if not address_batch.is_native_available():
        pytest.skip("native address preparation is unavailable")
    database = Database()
    await database.connect()
    schema = "address_stamp_batch_test_" + uuid4().hex[:12]
    await database.status(f'CREATE SCHEMA "{schema}"')
    monkeypatch.setattr(address_canon, "db", database)
    monkeypatch.setenv("HLTHPRT_ADDRESS_CANON_ZIP_RESTORE_ENABLED", "0")
    try:
        path = Path(__file__).resolve().parents[1] / "alembic/versions/20260611100000_address_canonical_foundation.py"
        spec = importlib.util.spec_from_file_location("address_batch_foundation", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        async with database.engine.begin() as connection:
            await connection.run_sync(lambda bind: module._exec_sql_batch(bind, module._create_functions_sql(schema)))
        yield database, schema
    finally:
        await database.status(f'DROP SCHEMA "{schema}" CASCADE')
        await database.disconnect()


async def seed_stamp_inputs(database, schema, table):
    await database.status(
        f'CREATE TABLE "{schema}"."{table}" (id int PRIMARY KEY,address_key uuid,first_line text,'
        "second_line text,city_name text,state_name text,postal_code text,country_code text)"
    )
    await database.status(
        f'INSERT INTO "{schema}"."{table}" VALUES '
        "(1,NULL,'10 Main Street',NULL,'Boston','MA','02101','US'),"
        "(2,NULL,'10 Main Street',NULL,'Boston','MA','02101','US'),"
        "(3,NULL,'100 Main Street Suite 200  A','200 A','Austin','TX','78701','US'),"
        "(4,NULL,'1 Main Street',NULL,'San José','CA','95112','US'),"
        "(5,'11111111-1111-1111-1111-111111111111',NULL,NULL,NULL,NULL,NULL,NULL),"
        "(6,NULL,'',NULL,NULL,NULL,NULL,NULL),"
        "(7,'11111111-1111-1111-1111-111111111111','20 Main Street',NULL,'Boston','MA','02101','US')"
    )


async def keys(database, schema, table):
    return list(map(tuple, await database.all(f'SELECT id,address_key::text FROM "{schema}"."{table}" ORDER BY id')))


async def assert_stamp_parity(monkeypatch, update_existing):
    async with stamp_database(monkeypatch) as (database, schema):
        await seed_stamp_inputs(database, schema, "sql_rows")
        await database.status(f'CREATE TABLE "{schema}".native_rows AS TABLE "{schema}".sql_rows')
        with monkeypatch.context() as fallback:
            fallback.setattr(address_batch, "is_native_available", lambda: False)
            sql_count = await address_canon.stamp_address_keys(
                "sql_rows", FIELDS, schema=schema, shards=1, update_existing=update_existing
            )
        native_count = await address_canon.stamp_address_keys(
            "native_rows", FIELDS, schema=schema, shards=1, update_existing=update_existing
        )
        assert native_count == sql_count
        assert await keys(database, schema, "native_rows") == await keys(database, schema, "sql_rows")


async def assert_stamp_cancellation(monkeypatch):
    async with stamp_database(monkeypatch) as (database, schema):
        await seed_stamp_inputs(database, schema, "input_rows")
        monkeypatch.setattr(address_batch, "MAX_ROWS", 2)
        token = uuid4().hex
        monkeypatch.setattr(address_batch_stamp, "uuid4", lambda: type("Token", (), {"hex": token})())
        checks = []

        async def cancel_check():
            checks.append(None)
            if len(checks) == 3:
                raise RuntimeError("cancel requested")

        async with database.transaction():
            before = await keys(database, schema, "input_rows")
            with pytest.raises(RuntimeError, match="cancel requested"):
                await address_canon.stamp_address_keys(
                    "input_rows", FIELDS, schema=schema, shards=1, cancel_check=cancel_check
                )
            assert await keys(database, schema, "input_rows") == before
            assert await database.scalar("SELECT to_regclass(:name) IS NULL", name="pg_temp.address_stamp_" + token)
            assert await address_canon.stamp_address_keys("input_rows", FIELDS, schema=schema, shards=1) > 0


async def assert_distinct_native_nulls(monkeypatch):
    from process.ext import address_batch_stage as stage

    async with stamp_database(monkeypatch) as (database, schema):
        await seed_stamp_inputs(database, schema, "input_rows")
        source = f'SELECT {",".join(stage.ADDRESS_FIELDS)} FROM "{schema}".input_rows'
        async with stage.staged_address_fields(database, schema, "native_inputs", source) as prepared:
            assert await database.scalar(f"SELECT count(*) FROM {prepared}") == 6
            computed = stage.field_sql("prepared", "address_key", "'SQL fallback'")
            rows = await database.all(
                f"SELECT first_line,{computed} FROM {prepared} prepared "
                "WHERE city_name='San José' OR first_line IS NULL OR first_line='' ORDER BY first_line NULLS FIRST"
            )
            assert list(map(tuple, rows)) == [(None, None), ("", None), ("1 Main Street", "SQL fallback")]
        assert await database.scalar("SELECT to_regclass(:name) IS NULL", name=f"{schema}.native_inputs")


async def _check_committed_visibility(database, schema, source, cancelled):
    import asyncio

    from process.ext import address_batch_stage as stage

    try:
        async with stage.staged_address_fields(database, schema, "shared_inputs", source, committed=True) as prepared:
            counts = await asyncio.gather(*(database.scalar(f"SELECT count(*) FROM {prepared}") for _ in range(2)))
            assert counts == [6, 6]
            if cancelled:
                raise asyncio.CancelledError()
    except asyncio.CancelledError:
        assert cancelled
    assert await database.scalar("SELECT to_regclass(:name) IS NULL", name=f"{schema}.shared_inputs")


async def assert_committed_parallel_visibility(monkeypatch):
    from process.ext import address_batch_stage as stage

    async with stamp_database(monkeypatch) as (database, schema):
        await seed_stamp_inputs(database, schema, "input_rows")
        source = f'SELECT {",".join(stage.ADDRESS_FIELDS)} FROM "{schema}".input_rows'
        for cancelled in (False, True):
            await _check_committed_visibility(database, schema, source, cancelled)
        async with database.transaction():
            async with stage.staged_address_fields(
                database, schema, "shared_inputs", source, committed=True
            ) as prepared:
                assert prepared is None


async def assert_unsupported_case_fallback(monkeypatch):
    from process.ext import address_batch_stage as stage

    async with stamp_database(monkeypatch) as (database, schema):
        collations = await database.all(
            "SELECT collname FROM pg_collation WHERE collname IN ('tr-x-icu','tr-TR-x-icu')"
        )
        if not collations:
            pytest.skip("Turkish ICU collation is unavailable")
        await seed_stamp_inputs(database, schema, "input_rows")
        columns = ",".join(stage.ADDRESS_FIELDS[1:])
        collation = '"' + collations[0][0].replace('"', '""') + '"'
        source = f'SELECT first_line COLLATE {collation} AS first_line,{columns} FROM "{schema}".input_rows'
        async with stage.staged_address_fields(database, schema, "unsupported_case", source) as prepared:
            assert prepared is None
        assert await database.scalar("SELECT to_regclass(:name) IS NULL", name=f"{schema}.unsupported_case")


async def _seed_archive_refresh(database, schema):
    path = Path(__file__).resolve().parents[1] / "alembic/versions/20260815010000_address_formatted_display_v2.py"
    spec = importlib.util.spec_from_file_location("address_batch_display", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    await database.status(module._humanize_component_function_sql(schema))
    await database.status(module._formatted_address_function_sql(schema))
    await database.status(
        f'CREATE TABLE "{schema}".address_archive_v2 ('
        "address_key uuid PRIMARY KEY,merged_into uuid,first_line text,second_line text,city_name text,"
        "state_name text,postal_code text,country_code text,formatted_address text,"
        "formatted_address_version smallint,formatted_address_source text)"
    )
    await database.status(
        f'INSERT INTO "{schema}".address_archive_v2 '
        "(address_key,first_line,city_name,state_name,postal_code,country_code) VALUES "
        "('11111111-1111-1111-1111-111111111111','1 Old Street','Boston','MA','02101','US')"
    )


async def _wait_archive_update_lock(database, schema, task):
    import asyncio

    async with asyncio.timeout(15):
        while not await database.scalar(
            "SELECT EXISTS(SELECT FROM pg_stat_activity WHERE datname=current_database() "
            "AND wait_event_type='Lock' AND query LIKE :needle)",
            needle=f'%UPDATE "{schema}"."address_archive_v2"%',
        ):
            if task.done():
                task.result()
                pytest.fail("archive refresh completed without waiting for the concurrent writer")
            await asyncio.sleep(0.01)


async def assert_archive_refresh_current_tuple(monkeypatch):
    import asyncio
    import importlib

    from sqlalchemy import text

    worker = importlib.import_module("process.address_formatted_address")
    prepared_pairs = []
    native_prepare = address_batch.prepare_address_batch

    def observe_native(rows):
        inputs = list(rows)
        results = native_prepare(inputs)
        assert results is not None
        prepared_pairs.extend(zip(inputs, results, strict=True))
        return results

    async with stamp_database(monkeypatch) as (database, schema):
        await _seed_archive_refresh(database, schema)
        monkeypatch.setattr(worker, "db", database)
        monkeypatch.setattr(address_batch, "prepare_address_batch", observe_native)
        token = uuid4()
        monkeypatch.setattr(worker, "uuid4", lambda: token)
        task = None
        try:
            async with database.engine.begin() as writer:
                await writer.execute(text(f"UPDATE \"{schema}\".address_archive_v2 SET first_line='2 New Street'"))
                task = asyncio.create_task(worker.refresh_address_archive_formatted_addresses(schema=schema))
                await _wait_archive_update_lock(database, schema, task)
                assert len(prepared_pairs) == 1
                inputs, fields = prepared_pairs[0]
                assert tuple(inputs) == ("1 Old Street", None, "Boston", "MA", "02101", "US")
                assert fields["formatted_address"] == "1 Old Street, Boston, MA 02101"
            refresh_stats = await task
            archive_row = await database.first(
                f'SELECT first_line,formatted_address,formatted_address="{schema}".addr_formatted_address_v2('
                "first_line,second_line,city_name,state_name,postal_code,country_code) AS matches_sql "
                f'FROM "{schema}".address_archive_v2'
            )
            assert archive_row.first_line == "2 New Street"
            assert archive_row.formatted_address == "2 New Street, Boston, MA 02101"
            assert archive_row.matches_sql
            assert refresh_stats.scanned == refresh_stats.updated == refresh_stats.batches == 1
            assert not await database.scalar(
                "SELECT EXISTS(SELECT FROM pg_class WHERE relname=:name)", name="address_format_" + token.hex
            )
        finally:
            if task is not None:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
