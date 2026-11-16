# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Connected ordinary-address comparisons in one disposable test schema."""

import importlib
import os
from contextlib import asynccontextmanager
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from db.connection import Database
from process import entity_address_batch as batch
from process import entity_address_ordinary_preparation as ordinary
from tests import entity_address_rows_preparation_support as fixtures


@asynccontextmanager
async def database_fixture(monkeypatch, *, mixed=False):
    dsn = os.getenv("NETWORK_REGISTRY_TEST_DSN") or os.getenv("HLTHPRT_PROVIDER_DIRECTORY_PROJECTION_POSTGRES_DSN", "")
    if "test" not in dsn.rsplit("/", 1)[-1]:
        pytest.skip("an explicitly selected disposable test database is required")
    if not batch.stage.address_batch.is_native_available():
        pytest.skip("native address batches are unavailable")
    sql = importlib.import_module("process.entity_address_unified")
    engine = create_async_engine(dsn.replace("postgresql://", "postgresql+asyncpg://"), hide_parameters=True)
    database = Database(engine=engine, session_factory=async_sessionmaker(engine, expire_on_commit=False))
    schema = "entity_address_batch_test_" + uuid4().hex[:12]
    await database.status(f"CREATE SCHEMA {schema}")
    monkeypatch.setattr(sql, "db", database)
    monkeypatch.setattr(sql, "_entity_address_sql_settings", lambda: [])
    try:
        await fixtures._create_source_fixture(database, schema)
        await fixtures._create_output_fixture(database, schema, sql)
        await database.status(
            f"UPDATE {schema}.source_rows SET state_name='New York',postal_code='2138' WHERE entity_id='office-4'"
        )
        if mixed:
            await database.status(f"UPDATE {schema}.source_rows SET city_name=NULL WHERE address_source='nppes'")
            await database.status(
                f"UPDATE {schema}.source_rows SET first_line=NULL "
                "WHERE entity_id='office-one' AND address_source='provider_directory_fhir'"
            )
        yield sql, database, schema
    finally:
        await database.status(f"DROP SCHEMA {schema} CASCADE")
        await database.disconnect()


async def create_archive(database, schema):
    await database.status(
        f"CREATE TABLE {schema}.address_alias_state_v1 (singleton boolean PRIMARY KEY,generation bigint,"
        "schema_version int DEFAULT 2,active_ruleset_version int DEFAULT 1)"
    )
    await database.status(f"INSERT INTO {schema}.address_alias_state_v1(singleton,generation) VALUES (TRUE,0)")
    await database.status(
        f"CREATE TABLE {schema}.address_archive_v2 (address_key uuid PRIMARY KEY,identity_key text NOT NULL,"
        "identity_version smallint DEFAULT 2,precision text DEFAULT 'street',premise_key uuid,zip5 text,"
        "state_code text,city_norm text,lat numeric,long numeric,place_id text,merged_into uuid)"
    )
    await database.status(
        f"INSERT INTO {schema}.address_archive_v2(address_key,identity_key) SELECT DISTINCT "
        f"{schema}.addr_key_v1(first_line,second_line,city_name,state_name,postal_code,country_code),"
        f"{schema}.addr_identity_key_v1(first_line,second_line,city_name,state_name,postal_code,country_code) "
        f"FROM {schema}.source_rows"
    )
    await database.status(
        f"CREATE TABLE {schema}.address_alias_v1 (source_address_key uuid PRIMARY KEY,target_address_key uuid,"
        "source_identity_key text,target_identity_key text,revoked_at timestamptz)"
    )
    await database.status(
        f"INSERT INTO {schema}.address_alias_v1 SELECT '00000000-0000-4000-8000-000000000002',"
        "address_key,identity_key,identity_key,NULL "
        f"FROM {schema}.address_archive_v2 WHERE identity_key LIKE '%12mainst%' LIMIT 1"
    )


async def run_raw(sql, database, schema, *, native, inline=False):
    raw = "raw_actual" if native else "raw_expected"
    output = "direct_actual" if native else "direct_expected"
    source = fixtures.source_select_sql(schema)

    @batch.scope
    async def materialize():
        if native:
            await sql._run_prepared_source_phase(
                schema,
                source,
                lambda prepared, **options: sql._insert_raw_from_source_sql(schema, raw, prepared, **options),
                {"context": {}, "phase": "loading synthetic source"},
            )
            await batch.prepare_raw(sql, schema, raw)
        else:
            await database.status(sql._insert_raw_from_source_sql(schema, raw, source))
        await database.status(sql._enrich_raw_stage_sql(schema, raw, evidence_shards=3))
        await sql._validate_raw_alias_integrity(schema, raw, is_address_canon_available=True)
        await database.status(sql._materialize_from_raw_sql(schema, output, raw, inline_source_evidence=inline))

    await materialize()


async def compare_builders(monkeypatch, mode, *, mixed=False):
    async with database_fixture(monkeypatch, mixed=mixed) as (sql, database, schema):
        if mode.startswith("raw"):
            await create_archive(database, schema)
            await run_raw(sql, database, schema, native=False, inline=mode == "raw_inline")
            await run_raw(sql, database, schema, native=True, inline=mode == "raw_inline")
        else:
            source = fixtures.source_select_sql(schema)
            await database.status(sql._materialize_sql(schema, "direct_expected", [source]))
            await sql._run_prepared_source_phase(
                schema,
                source,
                lambda prepared, **options: sql._materialize_sql(schema, "direct_actual", [prepared], **options),
                {"context": {}, "phase": "materializing synthetic source"},
                include_base_address_version=False,
                stage_table="direct_actual",
            )
        expected = await database.all(f"SELECT * FROM {schema}.direct_expected ORDER BY location_key")
        actual = await database.all(f"SELECT * FROM {schema}.direct_actual ORDER BY location_key")
        assert [tuple(row) for row in actual] == [tuple(row) for row in expected]
        assert len(actual) == (4 if mode.startswith("raw") and mixed else 3)
        await fixtures.assert_no_row_scratch(database, schema)
        assert not await database.scalar(
            "SELECT EXISTS(SELECT FROM information_schema.tables WHERE table_schema=:schema "
            "AND table_name LIKE '%_address_batch_%')",
            schema=schema,
        )


async def assert_alias_failure_cleanup(monkeypatch):
    async with database_fixture(monkeypatch) as (sql, database, schema):
        await create_archive(database, schema)
        await database.status(sql._insert_raw_from_source_sql(schema, "raw_actual", fixtures.source_select_sql(schema)))
        await database.status(f"UPDATE {schema}.address_alias_v1 SET source_identity_key='changed'")
        await database.status(f"CREATE TABLE {schema}.raw_actual_address_batch (sentinel int)")
        await database.status(f"INSERT INTO {schema}.raw_actual_address_batch VALUES (7)")
        caches = []

        @batch.scope
        async def validate():
            await batch.prepare_raw(sql, schema, "raw_actual")
            assert batch._TABLE.get() is not None
            caches.append(batch._TABLE.get())
            await sql._validate_raw_alias_integrity(schema, "raw_actual", is_address_canon_available=True)

        with pytest.raises(RuntimeError, match="source_identity_mismatch"):
            await validate()
        assert await database.scalar("SELECT to_regclass(:name) IS NULL", name=caches[0])
        assert await database.scalar(f"SELECT sentinel FROM {schema}.raw_actual_address_batch") == 7


async def assert_direct_failure_rollback(monkeypatch):
    async with database_fixture(monkeypatch) as (sql, database, schema):
        from process import entity_address_aggregation_batch as aggregation

        counted_insert = aggregation._run_counted_insert

        async def fail_after_insert(*args, **options):
            assert batch._TABLE.get() is not None
            await counted_insert(*args, **options)
            raise RuntimeError("consumer stopped")

        monkeypatch.setattr(aggregation, "_run_counted_insert", fail_after_insert)
        with pytest.raises(RuntimeError, match="consumer stopped"):
            await sql._run_prepared_source_phase(
                schema,
                fixtures.source_select_sql(schema),
                lambda prepared, **options: sql._materialize_sql(schema, "direct_actual", [prepared], **options),
                {"context": {}, "phase": "materializing synthetic source"},
                include_base_address_version=False,
                stage_table="direct_actual",
            )
        assert await database.scalar(f"SELECT count(*) FROM {schema}.direct_actual") == 0
        await fixtures.assert_no_row_scratch(database, schema)


async def assert_ordinary_cache_activation(monkeypatch):
    """Exercise the real ordinary owner around the connected raw publication flow."""
    async with database_fixture(monkeypatch) as (sql, database, schema):
        await create_archive(database, schema)
        await run_raw(sql, database, schema, native=False)
        prepare, native = batch.prepare_raw, batch.stage.address_batch.prepare_address_batch
        cache_relations, native_sizes = [], []

        def observe_native(rows):
            native_sizes.append(len(rows))
            return native(rows)

        async def observe_cache(*args):
            await prepare(*args)
            table = batch._TABLE.get()
            assert table is not None
            cache_relations.append(table)
            assert await database.scalar(f"SELECT count(*) FROM {table} WHERE native_fields IS NOT NULL") > 0

        monkeypatch.setattr(batch.stage.address_batch, "prepare_address_batch", observe_native)
        monkeypatch.setattr(batch, "prepare_raw", observe_cache)
        async with ordinary.scope(schema, set()) as owner:
            assert ordinary.current() is owner
            await run_raw(sql, database, schema, native=True)
            assert native_sizes and cache_relations
            assert await database.scalar("SELECT to_regclass(:name) IS NULL", name=cache_relations[0])
            assert ordinary.current() is owner
        expected = await database.all(f"SELECT * FROM {schema}.direct_expected ORDER BY location_key")
        actual = await database.all(f"SELECT * FROM {schema}.direct_actual ORDER BY location_key")
        assert list(map(tuple, actual)) == list(map(tuple, expected))
        await fixtures.assert_no_row_scratch(database, schema)
