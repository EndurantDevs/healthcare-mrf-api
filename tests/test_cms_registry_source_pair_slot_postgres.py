# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native forward-structure component proof, not producer or role-profile admission."""

import asyncio
import json
from contextlib import asynccontextmanager
from uuid import UUID, uuid4

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine

from db.cms_registry_source_pair_slot import CHECK_NAME, COLUMN, INDEX_NAME, MAX_BYTES, NATIVE_CHECK_SQL, TABLE
from process.provider_directory_cms_serving_receipt import has_registry_source_pair_receipt
from tests.cms_npd_admission_postgres_support import _database_url
from tests.test_provider_directory_cms_resource_batch_postgres import cms_resource_template as cms_resource_template
from tests.test_provider_directory_cms_serving_receipt_postgres import _migration


def _apply(connection, schema, operation):
    migration = _migration("20261016010000")
    migration.plan_release_schema = lambda: schema
    with Operations.context(MigrationContext.configure(connection)):
        getattr(migration, operation)()


@asynccontextmanager
async def _structure(tmp_path):
    """Use only an admitted local fixture database; register before any creation."""
    engine = create_async_engine(_database_url())
    schema = "cms_pair_slot_" + uuid4().hex
    journal = tmp_path / "slot-structure.json"
    journal.write_text(json.dumps({"schema": schema, "phase": "registered"}))
    try:
        async with engine.connect() as connection:
            transaction = await connection.begin()
            try:
                assert await connection.scalar(text("SELECT to_regnamespace(:schema)"), {"schema": schema}) is None
                await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
                await connection.execute(
                    text(f'CREATE TABLE "{schema}".{TABLE}(receipt_id text PRIMARY KEY,payload jsonb NOT NULL)')
                )
                await connection.run_sync(lambda sync: _apply(sync, schema, "upgrade"))
                yield connection, schema
            finally:
                if transaction.is_active:
                    await transaction.rollback()
            assert await connection.scalar(text("SELECT to_regnamespace(:schema)"), {"schema": schema}) is None
            await connection.rollback()
    finally:
        await engine.dispose()
    journal.write_text(json.dumps({"schema": schema, "phase": "rollback_verified"}))


async def _insert(connection, schema, key, document):
    await connection.execute(
        text(f"INSERT INTO \"{schema}\".{TABLE}(receipt_id,payload,{COLUMN}) VALUES(:key,'{{}}',CAST(:slot AS jsonb))"),
        {"key": key, "slot": json.dumps(document) if document is not None else None},
    )


@pytest.mark.asyncio
async def test_native_null_compatibility_capture_unique_and_canonical_check(tmp_path):
    async with _structure(tmp_path) as (connection, schema):
        await _insert(connection, schema, "old1", None)
        await _insert(connection, schema, "old2", None)
        slot_by_field = {"protocol": 1, "capture_id": str(uuid4()), "prepared_pair": {}}
        await _insert(connection, schema, "new", slot_by_field)
        assert await has_registry_source_pair_receipt(connection, schema, UUID(slot_by_field["capture_id"]))
        async with connection.begin_nested() as savepoint:
            with pytest.raises(DBAPIError, match="cms_serving_registry_capture_unique"):
                await _insert(connection, schema, "duplicate", slot_by_field)
            await savepoint.rollback()
        check = await connection.scalar(
            text(
                "SELECT pg_get_constraintdef(oid,true) FROM pg_constraint WHERE conrelid=to_regclass(:table) AND conname=:name"
            ),
            {"table": schema + "." + TABLE, "name": CHECK_NAME},
        )
        assert check == NATIVE_CHECK_SQL
        assert await connection.scalar(text(f'SELECT count(*) FROM "{schema}".{TABLE}')) == 3


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage", ["protocol", "boolean", "missing", "extra", "capture", "zero", "prepared", "oversize"]
)
async def test_native_malformed_slots_fail_in_original_transaction(tmp_path, damage):
    slot_by_field = {"protocol": 1, "capture_id": str(uuid4()), "prepared_pair": {}}
    replacements_by_damage = {
        "protocol": ("protocol", 1.5),
        "boolean": ("protocol", True),
        "extra": ("unbound", True),
        "capture": ("capture_id", "NOT-A-UUID"),
        "zero": ("capture_id", "00000000-0000-0000-0000-000000000000"),
        "prepared": ("prepared_pair", []),
        "oversize": ("prepared_pair", {"wide": "x" * MAX_BYTES}),
    }
    if damage == "missing":
        del slot_by_field["capture_id"]
    else:
        field, value = replacements_by_damage[damage]
        slot_by_field[field] = value
    async with _structure(tmp_path) as (connection, schema):
        async with connection.begin_nested() as savepoint:
            with pytest.raises(DBAPIError, match=CHECK_NAME):
                await _insert(connection, schema, "invalid", slot_by_field)
            await savepoint.rollback()
        assert await connection.scalar(text(f'SELECT count(*) FROM "{schema}".{TABLE}')) == 0


@pytest.mark.asyncio
async def test_native_downgrade_refuses_to_erase_finalized_locator(tmp_path):
    async with _structure(tmp_path) as (connection, schema):
        slot_by_field = {"protocol": 1, "capture_id": str(uuid4()), "prepared_pair": {}}
        await _insert(connection, schema, "new", slot_by_field)
        with pytest.raises(RuntimeError, match="slot_in_use"):
            await connection.run_sync(lambda sync: _apply(sync, schema, "downgrade"))
        assert await connection.scalar(text(f'SELECT {COLUMN} FROM "{schema}".{TABLE}')) == slot_by_field


@pytest.mark.asyncio
async def test_native_empty_downgrade_preserves_legacy_columns(tmp_path):
    async with _structure(tmp_path) as (connection, schema):
        await connection.run_sync(lambda sync: _apply(sync, schema, "downgrade"))
        columns = (
            (
                await connection.execute(
                    text(
                        "SELECT attname FROM pg_attribute WHERE attrelid=to_regclass(:table) AND attnum>0 AND NOT attisdropped ORDER BY attnum"
                    ),
                    {"table": schema + "." + TABLE},
                )
            )
            .scalars()
            .all()
        )
        assert columns == ["receipt_id", "payload"]


@asynccontextmanager
async def _committed_slot_structure(tmp_path):
    """Commit the isolated fixture so an independent publisher can reach it."""
    engine = create_async_engine(_database_url())
    schema = "cms_pair_race_" + uuid4().hex
    journal = tmp_path / "slot-downgrade-race.json"
    journal.write_text(json.dumps({"schema": schema, "phase": "registered"}))
    schema_oid = None
    try:
        async with engine.begin() as connection:
            assert await connection.scalar(text("SELECT to_regnamespace(:schema)"), {"schema": schema}) is None
            await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
            schema_oid = await connection.scalar(text("SELECT to_regnamespace(:schema)::oid"), {"schema": schema})
            await connection.execute(
                text(f'CREATE TABLE "{schema}".{TABLE}(receipt_id text PRIMARY KEY,payload jsonb NOT NULL)')
            )
            await connection.run_sync(lambda sync: _apply(sync, schema, "upgrade"))
        yield engine, schema
    finally:
        try:
            if schema_oid is not None:
                async with engine.begin() as connection:
                    assert (
                        await connection.scalar(text("SELECT to_regnamespace(:schema)::oid"), {"schema": schema})
                        == schema_oid
                    )
                    await connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
                    assert await connection.scalar(text("SELECT to_regnamespace(:schema)"), {"schema": schema}) is None
        finally:
            await engine.dispose()
    journal.write_text(json.dumps({"schema": schema, "phase": "cleanup_verified"}))


async def _wait_for_downgrade_lock(observer, schema, pid):
    async with asyncio.timeout(5):
        while not await observer.scalar(
            text(
                "SELECT EXISTS(SELECT FROM pg_locks WHERE pid=:pid AND relation=to_regclass(:table) "
                "AND mode='AccessExclusiveLock' AND NOT granted)"
            ),
            {"pid": pid, "table": schema + "." + TABLE},
        ):
            await asyncio.sleep(0.01)
    await observer.rollback()


async def _downgrade_slot(connection, schema):
    async with connection.begin():
        await connection.run_sync(lambda sync: _apply(sync, schema, "downgrade"))


@pytest.mark.asyncio
async def test_native_downgrade_rechecks_after_concurrent_receipt_commit(tmp_path):
    async with _committed_slot_structure(tmp_path) as (engine, schema):
        async with engine.connect() as publisher, engine.connect() as downgrader, engine.connect() as observer:
            pid = await downgrader.scalar(text("SELECT pg_backend_pid()"))
            await downgrader.rollback()
            slot_by_name = {"protocol": 1, "capture_id": str(uuid4()), "prepared_pair": {}}
            task = None
            try:
                await _insert(publisher, schema, "concurrent", slot_by_name)
                task = asyncio.create_task(_downgrade_slot(downgrader, schema))
                await _wait_for_downgrade_lock(observer, schema, pid)
                await publisher.commit()
                with pytest.raises(RuntimeError, match="slot_in_use"):
                    await asyncio.wait_for(task, timeout=5)
                assert await observer.scalar(text(f'SELECT {COLUMN} FROM "{schema}".{TABLE}')) == slot_by_name
                assert await observer.scalar(
                    text("SELECT to_regclass(:index) IS NOT NULL"), {"index": schema + "." + INDEX_NAME}
                )
                assert await observer.scalar(
                    text(
                        "SELECT EXISTS(SELECT FROM pg_constraint WHERE conrelid=to_regclass(:table) AND conname=:name)"
                    ),
                    {"table": schema + "." + TABLE, "name": CHECK_NAME},
                )
            finally:
                if task is not None and not task.done():
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
                await publisher.rollback()


@pytest.mark.asyncio
@pytest.mark.parametrize("isolation", ["REPEATABLE READ", "SERIALIZABLE"])
async def test_native_downgrade_refuses_retained_snapshot_isolation(tmp_path, isolation):
    async with _committed_slot_structure(tmp_path) as (engine, schema):
        async with engine.connect() as connection:
            await connection.execution_options(isolation_level=isolation)
            async with connection.begin():
                with pytest.raises(RuntimeError, match="downgrade_isolation_invalid"):
                    await connection.run_sync(lambda sync: _apply(sync, schema, "downgrade"))
                assert await connection.scalar(
                    text(
                        "SELECT EXISTS(SELECT FROM pg_attribute WHERE attrelid=to_regclass(:table) "
                        "AND attname=:column AND NOT attisdropped)"
                    ),
                    {"table": schema + "." + TABLE, "column": COLUMN},
                )
