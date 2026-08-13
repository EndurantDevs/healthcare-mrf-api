# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import contextlib
import importlib.util
import io
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
    / "20260813010000_provider_directory_observed_npi_index.py"
)
POSTGRES_DSN_ENV = "HLTHPRT_OBSERVED_NPI_INDEX_MIGRATION_POSTGRES_DSN"
INDEX_STATE_SQL = """
    SELECT index_record.oid, index_meta.indisvalid, index_meta.indisready
      FROM pg_index AS index_meta
      JOIN pg_class AS index_record
        ON index_record.oid = index_meta.indexrelid
      JOIN pg_namespace AS namespace
        ON namespace.oid = index_record.relnamespace
     WHERE namespace.nspname = $1
       AND index_record.relname = $2
"""


def _load_migration():
    module_spec = importlib.util.spec_from_file_location(
        "provider_directory_observed_npi_index_migration",
        MIGRATION_PATH,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


def _index_record(**overrides):
    record_dict = {
        "table_schema": "fixture",
        "table_name": "provider_directory_dataset_resource",
        "is_valid": True,
        "is_ready": True,
        "is_live": True,
        "access_method": "btree",
        "is_unique": False,
        "key_count": 3,
        "attribute_count": 3,
        "key_one": "(payload_json::jsonb ->> 'npi'::text)",
        "key_two": "dataset_id",
        "key_three": "resource_type",
        "predicate": (
            "((resource_type)::text = ANY ((ARRAY["
            "'Practitioner'::character varying, "
            "'PractitionerRole'::character varying])::text[]))"
        ),
    }
    record_dict.update(overrides)
    return record_dict


class _QueryResult:
    def __init__(self, row):
        self.row = row

    def mappings(self):
        return self

    def one_or_none(self):
        return self.row


class _BindRecorder:
    def __init__(self, rows):
        self.rows = list(rows)
        self.driver_statements: list[str] = []

    def execute(self, _statement, parameters):
        assert parameters == {
            "schema": "fixture",
            "index_name": "provider_directory_dataset_resource_observed_npi_idx",
        }
        return _QueryResult(self.rows.pop(0))

    def exec_driver_sql(self, statement: str):
        self.driver_statements.append(statement.strip())


class _MigrationContext:
    def __init__(self, *, as_sql: bool = False):
        self.as_sql = as_sql

    @contextlib.contextmanager
    def autocommit_block(self):
        yield


class _OperationsRecorder:
    def __init__(self, rows=(), *, as_sql: bool = False):
        self.bind = _BindRecorder(rows)
        self.context = _MigrationContext(as_sql=as_sql)
        self.offline_statements: list[str] = []

    def get_bind(self):
        return self.bind

    def get_context(self):
        return self.context

    def execute(self, statement: str):
        self.offline_statements.append(statement.strip())


def _run_upgrade(monkeypatch, rows=(), *, as_sql: bool = False):
    migration = _load_migration()
    operations = _OperationsRecorder(rows, as_sql=as_sql)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "fixture")
    monkeypatch.setattr(migration, "op", operations)
    migration.upgrade()
    return migration, operations


def test_absent_index_is_created(monkeypatch):
    migration, operations = _run_upgrade(monkeypatch, [None, _index_record()])

    assert operations.bind.driver_statements == [
        migration._create_index_sql("fixture").strip()
    ]


@pytest.mark.parametrize(
    "key_one",
    [
        "(payload_json ->> 'npi'::text)",
        "(payload_json::jsonb ->> 'npi'::text)",
    ],
)
def test_valid_expected_index_is_adopted(monkeypatch, key_one):
    _, operations = _run_upgrade(monkeypatch, [_index_record(key_one=key_one)])

    assert operations.bind.driver_statements == []


@pytest.mark.parametrize(
    "invalid_record",
    [
        _index_record(is_valid=False),
        _index_record(is_ready=False),
        _index_record(is_live=False),
    ],
)
def test_invalid_or_unready_index_is_dropped_then_rebuilt(
    monkeypatch,
    invalid_record,
):
    migration, operations = _run_upgrade(
        monkeypatch,
        [invalid_record, _index_record()],
    )

    assert operations.bind.driver_statements == [
        migration._drop_index_sql("fixture"),
        migration._create_index_sql("fixture").strip(),
    ]


@pytest.mark.parametrize(
    "wrong_record",
    [
        _index_record(key_one="resource_id"),
        _index_record(key_one="(payload_json::jsonb ->> 'npi_suffix'::text)"),
        _index_record(access_method="brin"),
        _index_record(is_unique=True),
        _index_record(key_count=4, attribute_count=4),
        _index_record(attribute_count=4),
        _index_record(
            predicate=(
                "(resource_type <> ALL (ARRAY["
                "'Practitioner'::text, 'PractitionerRole'::text]))"
            )
        ),
        _index_record(
            predicate=(
                "((resource_type)::text = ANY ((ARRAY["
                "'practitioner'::character varying, "
                "'PractitionerRole'::character varying])::text[]))"
            )
        ),
    ],
)
def test_valid_wrong_shape_is_not_dropped(monkeypatch, wrong_record):
    migration = _load_migration()
    operations = _OperationsRecorder([wrong_record])
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "fixture")
    monkeypatch.setattr(migration, "op", operations)

    with pytest.raises(
        RuntimeError,
        match=(
            "existing_schema_index_mismatch:fixture\\."
            "provider_directory_dataset_resource_observed_npi_idx"
        ),
    ):
        migration.upgrade()

    assert operations.bind.driver_statements == []


def test_offline_sql_remains_create_only(monkeypatch):
    migration, operations = _run_upgrade(monkeypatch, as_sql=True)

    assert operations.offline_statements == [
        migration._create_index_sql("fixture").strip()
    ]
    assert operations.bind.driver_statements == []


def test_offline_downgrade_emits_concurrent_drop(monkeypatch):
    migration = _load_migration()
    output_buffer = io.StringIO()
    context = MigrationContext.configure(
        dialect_name="postgresql",
        opts={"as_sql": True, "output_buffer": output_buffer},
    )
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "fixture")
    monkeypatch.setattr(migration, "op", Operations(context))

    migration.downgrade()

    assert output_buffer.getvalue().strip() == (
        'COMMIT;\n\nDROP INDEX CONCURRENTLY IF EXISTS "fixture".'
        '"provider_directory_dataset_resource_observed_npi_idx";\n\nBEGIN;'
    )


def _sqlalchemy_async_dsn(database_dsn: str) -> str:
    if database_dsn.startswith("postgresql://"):
        return database_dsn.replace("postgresql://", "postgresql+asyncpg://", 1)
    if database_dsn.startswith("postgres://"):
        return database_dsn.replace("postgres://", "postgresql+asyncpg://", 1)
    return database_dsn


async def _run_upgrade_on_postgres(async_engine, migration, monkeypatch) -> None:
    async with async_engine.connect() as async_connection:

        def upgrade(sync_connection):
            context = MigrationContext.configure(sync_connection)
            monkeypatch.setattr(migration, "op", Operations(context))
            with context.begin_transaction():
                migration.upgrade()

        await async_connection.run_sync(upgrade)


async def _postgres_index_state(connection, schema: str, index_name: str):
    return await connection.fetchrow(INDEX_STATE_SQL, schema, index_name)


@pytest.mark.asyncio
async def test_postgres_retry_replaces_failed_concurrent_index(monkeypatch):
    database_dsn = os.getenv(POSTGRES_DSN_ENV)
    if not database_dsn:
        pytest.skip(f"{POSTGRES_DSN_ENV} is required")
    migration = _load_migration()
    schema = f"observed_npi_{uuid.uuid4().hex[:16]}"
    quoted_schema = f'"{schema}"'
    connection = await asyncpg.connect(database_dsn)
    async_engine = create_async_engine(_sqlalchemy_async_dsn(database_dsn))
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    try:
        await connection.execute(f"CREATE SCHEMA {quoted_schema}")
        await connection.execute(
            f"""
            CREATE TABLE {quoted_schema}.provider_directory_dataset_resource (
                payload_json text NOT NULL,
                dataset_id varchar(96) NOT NULL,
                resource_type varchar(64) NOT NULL
            )
            """
        )
        await connection.execute(
            f"INSERT INTO {quoted_schema}.provider_directory_dataset_resource "
            "VALUES ('{not-json', 'dataset', 'Practitioner')"
        )
        with pytest.raises(asyncpg.PostgresError):
            await connection.execute(migration._create_index_sql(schema))
        invalid_state = await _postgres_index_state(
            connection, schema, migration.INDEX_NAME
        )
        assert invalid_state is not None
        assert invalid_state["indisvalid"] is False

        await connection.execute(
            f"DELETE FROM {quoted_schema}.provider_directory_dataset_resource"
        )
        await _run_upgrade_on_postgres(async_engine, migration, monkeypatch)
        built_state = await _postgres_index_state(
            connection, schema, migration.INDEX_NAME
        )
        assert built_state is not None
        assert built_state["indisvalid"] is True
        assert built_state["indisready"] is True

        await _run_upgrade_on_postgres(async_engine, migration, monkeypatch)
        assert await connection.fetchval(
            "SELECT to_regclass($1)::oid",
            f"{schema}.{migration.INDEX_NAME}",
        ) == built_state["oid"]
    finally:
        await async_engine.dispose()
        await connection.execute(f"DROP SCHEMA IF EXISTS {quoted_schema} CASCADE")
        await connection.close()


@pytest.mark.asyncio
async def test_postgres_jsonb_index_is_adopted_without_rebuild(monkeypatch):
    database_dsn = os.getenv(POSTGRES_DSN_ENV)
    if not database_dsn:
        pytest.skip(f"{POSTGRES_DSN_ENV} is required")
    migration = _load_migration()
    schema = f"observed_npi_{uuid.uuid4().hex[:16]}"
    quoted_schema = f'"{schema}"'
    connection = await asyncpg.connect(database_dsn)
    async_engine = create_async_engine(_sqlalchemy_async_dsn(database_dsn))
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    try:
        await connection.execute(f"CREATE SCHEMA {quoted_schema}")
        await connection.execute(
            f"""
            CREATE TABLE {quoted_schema}.provider_directory_dataset_resource (
                payload_json jsonb NOT NULL,
                dataset_id varchar(96) NOT NULL,
                resource_type varchar(64) NOT NULL
            )
            """
        )
        await _run_upgrade_on_postgres(async_engine, migration, monkeypatch)
        built_state = await _postgres_index_state(
            connection, schema, migration.INDEX_NAME
        )
        assert built_state is not None
        assert built_state["indisvalid"] is True
        assert built_state["indisready"] is True

        await _run_upgrade_on_postgres(async_engine, migration, monkeypatch)
        assert await connection.fetchval(
            "SELECT to_regclass($1)::oid",
            f"{schema}.{migration.INDEX_NAME}",
        ) == built_state["oid"]
    finally:
        await async_engine.dispose()
        await connection.execute(f"DROP SCHEMA IF EXISTS {quoted_schema} CASCADE")
        await connection.close()
