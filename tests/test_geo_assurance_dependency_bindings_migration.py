# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Nullable legacy downgrade preserves state and serializes binding publication."""

import asyncio
import importlib.util
from pathlib import Path
from unittest.mock import Mock

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from tests.test_entity_address_alias_guard_postgres import _owned_guard_database

_SCHEMA = "binding_migration"
_TABLE = f'"{_SCHEMA}".entity_address_geo_assurance_state'
_BINDINGS = ("active_dependency_bindings", "candidate_dependency_bindings")


def _migration(revision):
    path = Path(__file__).resolve().parents[1] / "alembic/versions" / f"{revision}.py"
    spec = importlib.util.spec_from_file_location(revision, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


async def _apply(connection, migration, action):
    def run(sync):
        with Operations.context(MigrationContext.configure(sync)):
            getattr(migration, action)()

    await connection.run_sync(run)


async def _setup(resources, monkeypatch):
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", _SCHEMA)
    monkeypatch.setenv("DB_SCHEMA", _SCHEMA)
    await resources.admin.execute(f'CREATE SCHEMA "{_SCHEMA}" AUTHORIZATION "{resources.roles["publisher"]}"')
    migration = _migration("20261004000000_geo_assurance_dependency_bindings")
    async with resources.publisher.engine.begin() as connection:
        await _apply(connection, _migration("20260825090000_geo_assurance_projection"), "upgrade")
        await connection.execute(
            text(f"UPDATE {_TABLE} SET active_geo_assurance_version=1, candidate_projected_rows=7")
        )
    legacy = await _state(resources)
    async with resources.publisher.engine.begin() as connection:
        await _apply(connection, migration, "upgrade")
    return migration, legacy


async def _state(resources):
    return await resources.admin.fetchval(f"SELECT to_jsonb(state) FROM {_TABLE} state")


@pytest.mark.parametrize("action", ("upgrade", "downgrade"))
@pytest.mark.parametrize("schema,legacy", (("unsafe.schema", "unsafe.schema"), ("schema_one", "schema_two")))
def test_bindings_migration_refuses_invalid_schema_before_sql(monkeypatch, action, schema, legacy):
    migration = _migration("20261004000000_geo_assurance_dependency_bindings")
    recorder = Mock()
    monkeypatch.setattr(migration, "op", recorder)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.setenv("DB_SCHEMA", legacy)
    with pytest.raises(RuntimeError, match="one matching simple identifier"):
        getattr(migration, action)()
    assert not recorder.mock_calls


@pytest.mark.asyncio
async def test_nullable_legacy_downgrade_reupgrade_and_held_refusal(monkeypatch):
    async with _owned_guard_database(monkeypatch) as resources:
        migration, legacy = await _setup(resources, monkeypatch)
        original = await _state(resources)
        await _assert_downgrade_rollback(resources, migration)
        assert await _state(resources) == original
        async with resources.publisher.engine.begin() as connection:
            await _apply(connection, migration, "downgrade")
        assert await _state(resources) == legacy
        async with resources.publisher.engine.begin() as connection:
            await _apply(connection, migration, "upgrade")
        assert await _state(resources) == original
        await _assert_held_refusals(resources, migration)


async def _assert_downgrade_rollback(resources, migration):
    async with resources.publisher.engine.connect() as connection:
        transaction = await connection.begin()
        try:
            await _apply(connection, migration, "downgrade")
            async with resources.publisher.engine.begin() as writer:
                with pytest.raises(DBAPIError, match="could not obtain lock"):
                    await writer.execute(text(f"LOCK TABLE {_TABLE} IN ROW EXCLUSIVE MODE NOWAIT"))
        finally:
            await transaction.rollback()


async def _assert_held_refusals(resources, migration):
    for column in _BINDINGS:
        for payload in ('{"held":true}', "null"):
            await resources.admin.execute(f"UPDATE {_TABLE} SET {column}=$1::jsonb", payload)
            held = await _state(resources)
            with pytest.raises(RuntimeError, match="explicit retirement"):
                async with resources.publisher.engine.begin() as connection:
                    await _apply(connection, migration, "downgrade")
            assert await _state(resources) == held
            await resources.admin.execute(f"UPDATE {_TABLE} SET {column}=NULL")


async def _wait_for_blocked_downgrade(admin, publisher_pid):
    while not await admin.fetchval(
        "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE pid=$1 AND mode='AccessExclusiveLock' AND NOT granted)",
        publisher_pid,
    ):
        await asyncio.sleep(0.01)


@pytest.mark.asyncio
async def test_downgrade_waits_for_writer_before_testing_bindings(monkeypatch):
    async with _owned_guard_database(monkeypatch) as resources:
        migration, _legacy = await _setup(resources, monkeypatch)
        async with resources.publisher.engine.connect() as writer, resources.publisher.engine.connect() as publisher:
            write_transaction = await writer.begin()
            await writer.execute(text(f"UPDATE {_TABLE} SET candidate_dependency_bindings='null'::jsonb"))
            publisher_pid = await publisher.scalar(text("SELECT pg_backend_pid()"))
            await publisher.commit()

            async def downgrade():
                async with publisher.begin():
                    await publisher.execute(text("SET LOCAL statement_timeout='5s'"))
                    await _apply(publisher, migration, "downgrade")

            task = asyncio.create_task(downgrade())
            try:
                await asyncio.wait_for(_wait_for_blocked_downgrade(resources.admin, publisher_pid), timeout=3)
                assert not task.done()
                await write_transaction.commit()
                with pytest.raises(RuntimeError, match="explicit retirement"):
                    await asyncio.wait_for(task, timeout=3)
                assert await resources.admin.fetchval(f"SELECT candidate_dependency_bindings IS NOT NULL FROM {_TABLE}")
                assert (
                    await resources.admin.fetchval(
                        "SELECT count(*) FROM information_schema.columns WHERE table_schema=$1 "
                        "AND table_name='entity_address_geo_assurance_state' AND column_name=ANY($2::text[])",
                        _SCHEMA,
                        list(_BINDINGS),
                    )
                    == 2
                )
            finally:
                if write_transaction.is_active:
                    await write_transaction.rollback()
                if not task.done():
                    task.cancel()
                await asyncio.gather(task, return_exceptions=True)
