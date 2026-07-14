# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import contextlib
import importlib.util
import os
from pathlib import Path
import uuid

import asyncpg
from alembic.migration import MigrationContext
from alembic.operations import Operations
import pytest
from sqlalchemy.ext.asyncio import create_async_engine


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic"
    / "versions"
    / "20260714140000_mrf_file_source_cursor_index.py"
)


def _load_migration():
    module_spec = importlib.util.spec_from_file_location(
        "mrf_file_source_cursor_index_migration",
        MIGRATION_PATH,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration_module = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration_module)
    return migration_module


class _QueryResult:
    def __init__(self, *, scalar_value=None, first_value=None):
        self.scalar_value = scalar_value
        self.first_value = first_value

    def scalar(self):
        return self.scalar_value

    def first(self):
        return self.first_value


class _BindRecorder:
    def __init__(self, relation_exists: bool, index_state=None):
        self.relation_exists = relation_exists
        self.index_state = index_state
        self.driver_statements: list[str] = []

    def execute(self, _statement, parameters):
        if parameters == {"schema": "fixture", "table_name": "mrf_file"}:
            return _QueryResult(scalar_value=self.relation_exists)
        assert parameters == {
            "schema": "fixture",
            "index_name": "mrf_file_source_cursor_idx",
        }
        return _QueryResult(first_value=self.index_state)

    def exec_driver_sql(self, statement: str):
        self.driver_statements.append(statement)


class _MigrationContext:
    def __init__(self, *, as_sql: bool = False):
        self.as_sql = as_sql

    @contextlib.contextmanager
    def autocommit_block(self):
        yield


class _OperationsRecorder:
    def __init__(self, relation_exists: bool, index_state=None, *, as_sql=False):
        self.bind_recorder = _BindRecorder(relation_exists, index_state)
        self.migration_context = _MigrationContext(as_sql=as_sql)

    def get_bind(self):
        return self.bind_recorder

    def get_context(self):
        return self.migration_context


def test_upgrade_builds_concurrent_index_with_unqualified_index_name(monkeypatch):
    migration_module = _load_migration()
    operations_recorder = _OperationsRecorder(relation_exists=True)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "fixture")
    monkeypatch.setattr(migration_module, "op", operations_recorder)
    monkeypatch.setattr(
        migration_module,
        "has_matching_index",
        lambda *_args, **_kwargs: True,
    )

    migration_module.upgrade()

    assert operations_recorder.bind_recorder.driver_statements == [
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS "mrf_file_source_cursor_idx" '
        'ON "fixture"."mrf_file" ("source_id", "mrf_file_id");'
    ]
    assert '"fixture"."mrf_file_source_cursor_idx"' not in (
        operations_recorder.bind_recorder.driver_statements[0]
    )


def test_upgrade_replaces_interrupted_invalid_index(monkeypatch):
    migration_module = _load_migration()
    operations_recorder = _OperationsRecorder(
        relation_exists=True,
        index_state=("fixture", "mrf_file", False, False, True),
    )
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "fixture")
    monkeypatch.setattr(migration_module, "op", operations_recorder)
    monkeypatch.setattr(
        migration_module,
        "has_matching_index",
        lambda *_args, **_kwargs: True,
    )

    migration_module.upgrade()

    assert operations_recorder.bind_recorder.driver_statements == [
        'DROP INDEX CONCURRENTLY IF EXISTS "fixture"."mrf_file_source_cursor_idx";',
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS "mrf_file_source_cursor_idx" '
        'ON "fixture"."mrf_file" ("source_id", "mrf_file_id");',
    ]


def test_upgrade_rejects_same_named_index_owned_by_another_table(monkeypatch):
    migration_module = _load_migration()
    operations_recorder = _OperationsRecorder(
        relation_exists=True,
        index_state=("fixture", "another_table", False, False, True),
    )
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "fixture")
    monkeypatch.setattr(migration_module, "op", operations_recorder)

    with pytest.raises(
        RuntimeError,
        match="existing_schema_index_mismatch:fixture.mrf_file_source_cursor_idx",
    ):
        migration_module.upgrade()

    assert operations_recorder.bind_recorder.driver_statements == []


def test_offline_upgrade_skips_optional_concurrent_index(monkeypatch):
    migration_module = _load_migration()
    operations_recorder = _OperationsRecorder(
        relation_exists=False,
        as_sql=True,
    )
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "fixture")
    monkeypatch.setattr(migration_module, "op", operations_recorder)

    migration_module.upgrade()

    assert operations_recorder.bind_recorder.driver_statements == []


def test_upgrade_is_noop_when_optional_mrf_file_table_is_absent(monkeypatch):
    migration_module = _load_migration()
    operations_recorder = _OperationsRecorder(relation_exists=False)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "fixture")
    monkeypatch.setattr(migration_module, "op", operations_recorder)
    monkeypatch.setattr(
        migration_module,
        "has_matching_index",
        lambda *_args, **_kwargs: pytest.fail("index shape must not be probed"),
    )

    migration_module.upgrade()

    assert operations_recorder.bind_recorder.driver_statements == []


def _sqlalchemy_async_dsn(database_dsn: str) -> str:
    if database_dsn.startswith("postgresql://"):
        return database_dsn.replace("postgresql://", "postgresql+asyncpg://", 1)
    if database_dsn.startswith("postgres://"):
        return database_dsn.replace("postgres://", "postgresql+asyncpg://", 1)
    return database_dsn


async def _run_alembic_action(
    async_engine,
    migration_module,
    monkeypatch,
    action_name: str,
) -> None:
    """Run one migration action through Alembic's real autocommit context."""
    async with async_engine.connect() as async_connection:
        def run_action(sync_connection):
            migration_context = MigrationContext.configure(sync_connection)
            monkeypatch.setattr(
                migration_module,
                "op",
                Operations(migration_context),
            )
            with migration_context.begin_transaction():
                getattr(migration_module, action_name)()

        await async_connection.run_sync(run_action)


@pytest.mark.asyncio
async def test_cursor_index_upgrade_and_downgrade_execute_on_postgresql(monkeypatch):
    """Exercise upgrade, idempotent retry, and downgrade on PostgreSQL."""
    database_dsn = os.getenv("HLTHPRT_MRF_FILE_CURSOR_MIGRATION_POSTGRES_DSN")
    if not database_dsn:
        pytest.skip("set the cursor migration PostgreSQL DSN")
    migration_module = _load_migration()
    schema_name = f"MrfCursor{uuid.uuid4().hex[:16]}"
    connection = await asyncpg.connect(database_dsn)
    async_engine = create_async_engine(_sqlalchemy_async_dsn(database_dsn))
    try:
        await connection.execute(f'CREATE SCHEMA "{schema_name}"')
        await connection.execute(
            f'CREATE TABLE "{schema_name}"."mrf_file" ('
            "source_id varchar(64) NOT NULL, "
            "mrf_file_id bigint NOT NULL PRIMARY KEY)"
        )
        monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema_name)

        await _run_alembic_action(
            async_engine,
            migration_module,
            monkeypatch,
            "upgrade",
        )
        await _run_alembic_action(
            async_engine,
            migration_module,
            monkeypatch,
            "upgrade",
        )

        index_definition = await connection.fetchval(
            "SELECT indexdef FROM pg_indexes "
            "WHERE schemaname = $1 AND indexname = $2",
            schema_name,
            migration_module.INDEX_NAME,
        )
        assert index_definition is not None
        assert "(source_id, mrf_file_id)" in index_definition

        await _run_alembic_action(
            async_engine,
            migration_module,
            monkeypatch,
            "downgrade",
        )

        remaining_index = await connection.fetchval(
            "SELECT indexname FROM pg_indexes "
            "WHERE schemaname = $1 AND indexname = $2",
            schema_name,
            migration_module.INDEX_NAME,
        )
    finally:
        await async_engine.dispose()
        await connection.execute(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE')
        await connection.close()

    assert remaining_index is None
