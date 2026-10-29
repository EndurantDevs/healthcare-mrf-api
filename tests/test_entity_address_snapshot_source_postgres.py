# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import importlib
import os
import subprocess
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.schema import MetaData

from process import entity_address_dependency_bindings as dependencies
from process import tiger_held_inputs
from tests.test_entity_address_snapshot_destination import _owned_native_database
from tests.test_entity_address_snapshot_stage_postgres import _native_test_connection

source = importlib.import_module("process.entity_address_snapshot_source")


def _dsn() -> str:
    dsn = os.environ.get("HLTHPRT_ENTITY_ADDRESS_ARCHIVE_TEST_DSN", "")
    if not dsn:
        pytest.skip("HLTHPRT_ENTITY_ADDRESS_ARCHIVE_TEST_DSN is not set")
    return dsn.replace("postgresql://", "postgresql+asyncpg://", 1)


async def _create_model_family(engine, schema_name: str) -> None:
    """Create the real seven-table family in a disposable test schema."""

    async with engine.begin() as connection:
        await connection.execute(text(f'CREATE SCHEMA "{schema_name}"'))
        metadata = MetaData(schema=schema_name)
        for model in (
            source.entity_address_unified.EntityAddressUnified,
            *source.entity_address_unified.SUPPORT_TABLE_MODELS,
        ):
            model.__table__.to_metadata(metadata, schema=schema_name)
        await connection.run_sync(metadata.create_all)


def _dump_capture(capture, *, dump_path, schema_name: str) -> None:
    """Write one native archive from exactly the relations held by ``capture``."""

    dump = subprocess.run(
        [
            os.environ["HLTHPRT_ENTITY_ADDRESS_ARCHIVE_TEST_PG_DUMP"],
            "--format=custom",
            "--file",
            str(dump_path),
            f"--snapshot={capture.postgres_snapshot}",
            *[f"--table={schema_name}.{relation.table_name}" for relation in capture.relations],
        ],
        capture_output=True,
        check=False,
        env=os.environ.copy(),
        text=True,
        timeout=30,
    )
    assert dump.returncode == 0, dump.stderr


async def _assert_capture_blocks_ddl(sessions, schema: str, relations) -> None:
    """Assert that the source pin blocks conflicting rename and drop operations."""

    async with sessions() as contender:
        await contender.execute(text("SET lock_timeout = '100ms'"))
        with pytest.raises(DBAPIError) as rename_error:
            await contender.execute(text(f'ALTER TABLE {schema}."{relations[0].table_name}" RENAME TO blocked'))
        assert rename_error.value.orig.sqlstate == "55P03"
    async with sessions() as contender:
        await contender.execute(text("SET lock_timeout = '100ms'"))
        with pytest.raises(DBAPIError) as drop_error:
            await contender.execute(text(f'DROP TABLE {schema}."{relations[1].table_name}"'))
        assert drop_error.value.orig.sqlstate == "55P03"


@pytest.mark.parametrize("driver", ["postgresql", "postgresql+asyncpg"])
def test_source_archive_dsn_selects_async_driver(monkeypatch, driver):
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_ARCHIVE_TEST_DSN", f"{driver}://reader@localhost/archive_test")
    assert _dsn() == "postgresql+asyncpg://reader@localhost/archive_test"


@pytest.mark.asyncio
async def test_native_capture_pins_model_family_until_transaction_end(tmp_path):
    """The source-capture contract blocks DDL until native archive export returns."""

    engine = create_async_engine(_dsn())
    schema_name = "address_archive_" + uuid4().hex
    schema = f'"{schema_name}"'
    relations = source.entity_address_archive_relations()
    try:
        await _create_model_family(engine, schema_name)
        sessions = async_sessionmaker(engine, expire_on_commit=False)

        async def archive_copy(capture):
            """Consume the pin before asserting the lock contract."""

            assert [relation.table_name for relation in capture.relations] == [
                relation.table_name for relation in relations
            ]
            _dump_capture(capture, dump_path=tmp_path / "source.dump", schema_name=schema_name)
            await _assert_capture_blocks_ddl(sessions, schema, relations)

        manifest = await source.export_entity_address_archive_source(
            sessions,
            schema_name=schema_name,
            archive_copy=archive_copy,
        )
        assert manifest.relations == relations
        async with sessions() as contender, contender.begin():
            await contender.execute(text(f'ALTER TABLE {schema}."{relations[0].table_name}" RENAME TO released'))
            await contender.execute(text(f'ALTER TABLE {schema}.released RENAME TO "{relations[0].table_name}"'))
    finally:
        async with engine.begin() as connection:
            await connection.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
        await engine.dispose()


@pytest.mark.parametrize("initial_mb,expected_mb", [(4, 4), (16, 16), (64, 64), (128, 64)])
@pytest.mark.asyncio
async def test_receipt_normalization_respects_stricter_memory_budget(initial_mb, expected_mb):
    from process.entity_address_snapshot_receipt import _normalize_receipt_session

    engine = create_async_engine(_dsn(), connect_args={"server_settings": {"work_mem": f"{initial_mb}MB"}})
    try:
        async with engine.connect() as connection:
            async with connection.begin():
                await connection.execute(text("SET TRANSACTION READ ONLY"))
                await _normalize_receipt_session(connection, "pg_catalog")
                assert await connection.scalar(text("SELECT pg_size_bytes(current_setting('work_mem'))")) == (
                    expected_mb * 1024 * 1024
                )
                assert await connection.scalar(text("SHOW transaction_read_only")) == "on"
                assert await connection.scalar(text("SHOW TimeZone")) == "UTC"
                assert await connection.scalar(text("SHOW DateStyle")) == "ISO, YMD"
                assert await connection.scalar(text("SHOW IntervalStyle")) == "iso_8601"
            assert await connection.scalar(text("SELECT pg_size_bytes(current_setting('work_mem'))")) == (
                initial_mb * 1024 * 1024
            )
    finally:
        await engine.dispose()


async def _create_selected_inputs(database):
    for schema in ("mrf", "tiger"):
        await database.status(f"CREATE SCHEMA {schema}")
    names = [f"{schema or 'mrf'}.{name}" for schema, name in dependencies.projection._PROJECTION_DEPENDENCIES]
    names.append("tiger.captured_support")
    for name in names:
        await database.status(f"CREATE TABLE {name} (marker text)")
    await database.status("CREATE TABLE mrf.publication (marker text)")
    await database.status("INSERT INTO mrf.publication VALUES ('old')")
    relations = await database.all(
        "SELECT n.nspname AS schema_name,c.relname AS relation_name,c.oid::bigint AS relation_oid,"
        "pg_relation_filenode(c.oid)::bigint AS relfilenode "
        "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='tiger'"
    )
    return names, {"relations": [dict(relation._mapping) for relation in relations]}


@asynccontextmanager
async def _selected_inputs(monkeypatch):
    """Own all native fixture state and close the short-timeout guard on every exit."""
    dsn, _ = _native_test_connection()
    async with _owned_native_database(dsn, monkeypatch) as database:
        names, inventory_by_field = await _create_selected_inputs(database)

        async def capture(session, **_options):
            await session.execute(
                text("LOCK TABLE tiger.zip_state,tiger.zcta5,tiger.captured_support IN ACCESS SHARE MODE")
            )
            return {"tiger": inventory_by_field}

        monkeypatch.setattr(tiger_held_inputs, "selected_captured_inventories", capture)
        monkeypatch.setattr(tiger_held_inputs, "selected_tiger_inventory", AsyncMock(side_effect=AssertionError))
        guard_engine = create_async_engine(
            database.engine.url,
            pool_size=1,
            max_overflow=0,
            connect_args={
                "server_settings": {
                    "idle_in_transaction_session_timeout": "1s",
                    "transaction_timeout": "2s",
                }
            },
        )
        factory = database.session_factory
        guard_factory = async_sessionmaker(guard_engine, expire_on_commit=False)
        try:
            # Only the dependency owner has the short timeout; cutover keeps its ordinary pool.
            database.session_factory = guard_factory
            manager = dependencies.selected_publication_dependencies(database, "mrf")
            selected = await manager.__aenter__()
            database.session_factory = factory
            try:
                yield database, selected, names, guard_engine
            except BaseException as error:
                await manager.__aexit__(type(error), error, error.__traceback__)
                raise
            else:
                await manager.__aexit__(None, None, None)
                async with guard_engine.connect() as released:
                    assert await released.scalar(text("SHOW idle_in_transaction_session_timeout")) == "1s"
                    assert await released.scalar(text("SHOW transaction_timeout")) == "2s"
        finally:
            database.session_factory = factory
            await guard_engine.dispose()


@pytest.mark.asyncio
async def test_native_guard_survives_idle_timeout_and_overlaps_complete_family_locks(monkeypatch):
    async with _selected_inputs(monkeypatch) as (database, selected, names, guard_engine):
        held = dependencies._HELD_PUBLICATION_DEPENDENCIES.get()
        assert len(selected) == 6 and len(held.relations_by_name) == 7
        assert await held.session.scalar(text("SHOW idle_in_transaction_session_timeout")) == "0"
        assert await held.session.scalar(text("SHOW transaction_timeout")) == "0"
        await asyncio.sleep(2.2)
        async with database.transaction() as cutover:
            await dependencies.lock_selected_publication_dependencies(database, "mrf")
            owner_count = await cutover.scalar(
                text(
                    "SELECT count(DISTINCT pid) FROM pg_locks WHERE granted AND mode='AccessShareLock' "
                    "AND relation='tiger.captured_support'::regclass"
                )
            )
            assert owner_count == 2
            await database.status("UPDATE mrf.publication SET marker='new'")
        assert await held.session.scalar(text("SELECT pg_backend_pid()")) == held.backend_id
        assert await database.scalar("SELECT marker FROM mrf.publication") == "new"
        assert len(names) == 7
    assert dependencies._HELD_PUBLICATION_DEPENDENCIES.get() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("after_overlap", (False, True))
async def test_native_lost_dependency_owner_prevents_publication(monkeypatch, after_overlap):
    async with (
        _selected_inputs(monkeypatch) as (database, _selected, _names, _engine),
        database.engine.connect() as observer,
    ):
        held = dependencies._HELD_PUBLICATION_DEPENDENCIES.get()

        async def terminate():
            assert await observer.scalar(text("SELECT pg_terminate_backend(:pid)"), {"pid": held.backend_id})

        if not after_overlap:
            await terminate()
        with pytest.raises(DBAPIError) as disconnected:
            async with database.transaction() as cutover:
                execute = cutover.execute

                async def overlap_then_terminate(statement, *args, **kwargs):
                    result = await execute(statement, *args, **kwargs)
                    if str(statement).startswith("LOCK TABLE"):
                        await terminate()
                    return result

                if after_overlap:
                    monkeypatch.setattr(cutover, "execute", overlap_then_terminate)
                await dependencies.lock_selected_publication_dependencies(database, "mrf")
                await database.status("UPDATE mrf.publication SET marker='new'")
        assert disconnected.value.connection_invalidated
        assert await database.scalar("SELECT marker FROM mrf.publication") == "old"
        # End the failed, read-only owner transaction without attempting its COMMIT.
        await held.session.rollback()


@pytest.mark.asyncio
async def test_native_cutover_retains_family_locks_after_original_owner_disconnects(monkeypatch):
    is_body_complete = False
    with pytest.raises(DBAPIError) as disconnected:
        async with (
            _selected_inputs(monkeypatch) as (database, _selected, _names, _engine),
            database.engine.connect() as observer,
        ):
            held = dependencies._HELD_PUBLICATION_DEPENDENCIES.get()
            async with database.transaction():
                await dependencies.lock_selected_publication_dependencies(database, "mrf")
                assert await observer.scalar(text("SELECT pg_terminate_backend(:pid)"), {"pid": held.backend_id})
                with pytest.raises(DBAPIError) as blocked:
                    await observer.execute(text("LOCK TABLE tiger.captured_support IN ACCESS EXCLUSIVE MODE NOWAIT"))
                assert blocked.value.orig.sqlstate == "55P03"
                await observer.rollback()
                await database.status("UPDATE mrf.publication SET marker='new'")
            assert await database.scalar("SELECT marker FROM mrf.publication") == "new"
            await observer.execute(text("LOCK TABLE tiger.captured_support IN ACCESS EXCLUSIVE MODE NOWAIT"))
            await observer.rollback()
            is_body_complete = True
    assert is_body_complete
    assert disconnected.value.connection_invalidated
