# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import contextlib
import importlib.util
import os
from pathlib import Path
import uuid

import asyncpg
import pytest


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


class _ScalarResult:
    def __init__(self, scalar_value):
        self.scalar_value = scalar_value

    def scalar(self):
        return self.scalar_value


class _BindRecorder:
    def __init__(self, relation_exists: bool):
        self.relation_exists = relation_exists
        self.driver_statements: list[str] = []

    def execute(self, _statement, parameters):
        assert parameters == {"relation_name": "fixture.mrf_file"}
        return _ScalarResult("fixture.mrf_file" if self.relation_exists else None)

    def exec_driver_sql(self, statement: str):
        self.driver_statements.append(statement)


class _MigrationContext:
    as_sql = False

    @contextlib.contextmanager
    def autocommit_block(self):
        yield


class _OperationsRecorder:
    def __init__(self, relation_exists: bool):
        self.bind_recorder = _BindRecorder(relation_exists)
        self.migration_context = _MigrationContext()

    def get_bind(self):
        return self.bind_recorder

    def get_context(self):
        return self.migration_context


def test_upgrade_builds_concurrent_index_with_unqualified_index_name(monkeypatch):
    migration_module = _load_migration()
    operations_recorder = _OperationsRecorder(relation_exists=True)
    matching_results = iter((False, True))
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "fixture")
    monkeypatch.setattr(migration_module, "op", operations_recorder)
    monkeypatch.setattr(
        migration_module,
        "has_matching_index",
        lambda *_args, **_kwargs: next(matching_results),
    )

    migration_module.upgrade()

    assert operations_recorder.bind_recorder.driver_statements == [
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS "mrf_file_source_cursor_idx" '
        'ON "fixture"."mrf_file" ("source_id", "mrf_file_id");'
    ]
    assert '"fixture"."mrf_file_source_cursor_idx"' not in (
        operations_recorder.bind_recorder.driver_statements[0]
    )


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


@pytest.mark.asyncio
async def test_cursor_index_sql_executes_on_postgresql():
    database_dsn = os.getenv("HLTHPRT_MRF_FILE_CURSOR_MIGRATION_POSTGRES_DSN")
    if not database_dsn:
        pytest.skip("set the cursor migration PostgreSQL DSN")
    migration_module = _load_migration()
    schema_name = f"mrf_cursor_{uuid.uuid4().hex[:16]}"
    connection = await asyncpg.connect(database_dsn)
    try:
        await connection.execute(f'CREATE SCHEMA "{schema_name}"')
        await connection.execute(
            f'CREATE TABLE "{schema_name}"."mrf_file" ('
            "source_id varchar(64) NOT NULL, "
            "mrf_file_id bigint NOT NULL PRIMARY KEY)"
        )
        create_index_sql = migration_module._create_index_sql(schema_name)
        await connection.execute(create_index_sql)
        await connection.execute(create_index_sql)
        index_definition = await connection.fetchval(
            "SELECT indexdef FROM pg_indexes "
            "WHERE schemaname = $1 AND indexname = $2",
            schema_name,
            migration_module.INDEX_NAME,
        )
    finally:
        await connection.execute(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE')
        await connection.close()

    assert index_definition is not None
    assert "(source_id, mrf_file_id)" in index_definition
