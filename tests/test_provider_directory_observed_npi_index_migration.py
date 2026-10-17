# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import contextlib
import importlib.util
import io
import os
import uuid
from pathlib import Path

import asyncpg
import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
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

    assert operations.bind.driver_statements == [migration._create_index_sql("fixture").strip()]


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
        _index_record(predicate=("(resource_type <> ALL (ARRAY['Practitioner'::text, 'PractitionerRole'::text]))")),
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
        match=("existing_schema_index_mismatch:fixture\\.provider_directory_dataset_resource_observed_npi_idx"),
    ):
        migration.upgrade()

    assert operations.bind.driver_statements == []


def test_offline_sql_remains_create_only(monkeypatch):
    migration, operations = _run_upgrade(monkeypatch, as_sql=True)

    assert operations.offline_statements == [migration._create_index_sql("fixture").strip()]
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
        invalid_state = await _postgres_index_state(connection, schema, migration.INDEX_NAME)
        assert invalid_state is not None
        assert invalid_state["indisvalid"] is False

        await connection.execute(f"DELETE FROM {quoted_schema}.provider_directory_dataset_resource")
        await _run_upgrade_on_postgres(async_engine, migration, monkeypatch)
        built_state = await _postgres_index_state(connection, schema, migration.INDEX_NAME)
        assert built_state is not None
        assert built_state["indisvalid"] is True
        assert built_state["indisready"] is True

        await _run_upgrade_on_postgres(async_engine, migration, monkeypatch)
        assert (
            await connection.fetchval(
                "SELECT to_regclass($1)::oid",
                f"{schema}.{migration.INDEX_NAME}",
            )
            == built_state["oid"]
        )
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
        built_state = await _postgres_index_state(connection, schema, migration.INDEX_NAME)
        assert built_state is not None
        assert built_state["indisvalid"] is True
        assert built_state["indisready"] is True

        await _run_upgrade_on_postgres(async_engine, migration, monkeypatch)
        assert (
            await connection.fetchval(
                "SELECT to_regclass($1)::oid",
                f"{schema}.{migration.INDEX_NAME}",
            )
            == built_state["oid"]
        )
    finally:
        await async_engine.dispose()
        await connection.execute(f"DROP SCHEMA IF EXISTS {quoted_schema} CASCADE")
        await connection.close()


def _load_readiness_migration():
    path = MIGRATION_PATH.with_name("20261017210000_provider_directory_readiness_indexes.py")
    spec = importlib.util.spec_from_file_location("provider_directory_readiness_indexes_migration", path)
    assert spec is not None and spec.loader is not None
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    return migration


@pytest.mark.parametrize("metadata_key", ["dataset_network_plan", "dataset_affiliation_organization"])
def test_readiness_migration_keeps_its_predicate_independent_of_runtime(monkeypatch, metadata_key):
    from db import provider_directory_readiness

    expected = provider_directory_readiness.dataset_relation_ready_predicate(metadata_key, version="1")
    monkeypatch.setattr(
        provider_directory_readiness, "dataset_relation_ready_predicate", lambda *_args, **_kwargs: "FALSE"
    )
    migration = _load_readiness_migration()
    assert migration._ready_predicate(metadata_key) == expected
    assert migration._create_sql("synthetic", metadata_key, "synthetic_ready_idx").endswith(f"WHERE {expected}")


@pytest.fixture
async def readiness_database(monkeypatch, request):
    """Use the existing CI DSN, with all objects confined to one disposable schema."""
    database_dsn = os.getenv(POSTGRES_DSN_ENV)
    if not database_dsn:
        pytest.skip(f"{POSTGRES_DSN_ENV} is required")
    metadata_type = getattr(request, "param", "json")
    assert metadata_type in {"json", "jsonb"}
    schema = f"readiness_{uuid.uuid4().hex}"
    connection = await asyncpg.connect(database_dsn)
    engine = create_async_engine(_sqlalchemy_async_dsn(database_dsn), pool_size=1, max_overflow=0, hide_parameters=True)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.setenv("DB_SCHEMA", schema)
    is_schema_created = False
    try:
        if "test" not in str(await connection.fetchval("SELECT current_database()")).lower():
            pytest.fail("Readiness index tests require an explicit test database")
        await connection.execute(f'CREATE SCHEMA "{schema}"')
        is_schema_created = True
        await connection.execute(f"""
            CREATE TABLE "{schema}".provider_directory_endpoint_dataset (
                dataset_id varchar(96) PRIMARY KEY,
                endpoint_id varchar(64) NOT NULL,
                import_run_id varchar(64) DEFAULT 'import',
                acquisition_root_run_id varchar(64),
                status varchar(32) NOT NULL DEFAULT 'published',
                is_current boolean NOT NULL DEFAULT true,
                published_at timestamp DEFAULT '2026-01-01',
                superseded_at timestamp,
                publication_metadata_json {metadata_type}
            );
            CREATE TABLE "{schema}".provider_directory_source (
                source_id varchar(64) PRIMARY KEY,
                endpoint_id varchar(64),
                canonical_api_base text DEFAULT 'https://directory.example.test/fhir'
            );
            CREATE TABLE "{schema}".provider_directory_dataset_resource (
                dataset_id varchar(96), resource_type text, resource_id text, payload_json jsonb
            );
        """)
        yield connection, schema, engine, _load_readiness_migration()
    finally:
        await engine.dispose()
        try:
            if is_schema_created:
                await connection.execute(f'DROP SCHEMA "{schema}" CASCADE')
                assert await connection.fetchval("SELECT to_regnamespace($1)", schema) is None
        finally:
            await connection.close()


def _readiness_metadata(dataset_id):
    return {
        key: {"complete": True, "version": 1, "dataset_id": dataset_id}
        for key in ("dataset_network_plan", "dataset_affiliation_organization")
    }


async def _readiness_insert(database, dataset_id, metadata, *, endpoint_id=None):
    import json

    connection, schema, _engine, _migration = database
    endpoint_id = endpoint_id or f"endpoint_{dataset_id}"
    source_id = f"source_{dataset_id}"
    await connection.execute(
        f'INSERT INTO "{schema}".provider_directory_endpoint_dataset '
        "(dataset_id, endpoint_id, publication_metadata_json) VALUES ($1, $2, $3)",
        dataset_id,
        endpoint_id,
        None if metadata is None else json.dumps(metadata),
    )
    await connection.execute(
        f'INSERT INTO "{schema}".provider_directory_source (source_id, endpoint_id) VALUES ($1, $2)',
        source_id,
        endpoint_id,
    )
    return source_id


async def _readiness_rows(database, source_ids, *, indexed):
    from sqlalchemy import text

    from api.endpoint import npi

    _connection, schema, engine, _migration = database
    ctes = npi._provider_directory_current_resource_ctes_sql(schema, has_dataset_readiness_indexes=indexed)
    async with engine.connect() as connection:
        rows = await connection.execute(
            text(f"""
            WITH {ctes}
            SELECT dataset_id, run_id, dataset_network_plan_complete,
                   dataset_affiliation_organization_complete
              FROM current_datasets ORDER BY dataset_id
        """),
            {"source_ids": source_ids},
        )
        return [tuple(row) for row in rows]


async def _readiness_capabilities(database):
    from api.endpoint import npi

    _connection, _schema, engine, migration = database
    async with engine.connect() as connection:
        flags = await npi._provider_directory_evidence_tables(
            connection,
            required_names=(migration.TABLE,),
            optional_names=(),
        )
    assert flags is not None
    return tuple(flags[name] for _key, name in migration.INDEXES)


async def _readiness_seed_values(database):
    readiness_cases = (
        ("boolean", {"complete": True, "version": 1}, True),
        ("strings", {"complete": "true", "version": "1"}, True),
        ("missing_complete", {"version": 1}, False),
        ("missing_version", {"complete": True}, False),
        ("missing_dataset", {"complete": True, "version": 1}, False),
        ("false", {"complete": False, "version": 1}, False),
        ("null_complete", {"complete": None, "version": 1}, False),
        ("null_version", {"complete": True, "version": None}, False),
        ("uppercase", {"complete": "TRUE", "version": 1}, False),
        ("version_two", {"complete": True, "version": 2}, False),
        ("decimal_version", {"complete": True, "version": 1.0}, False),
        ("wrong_dataset", {"complete": True, "version": 1, "dataset_id": "other"}, False),
        ("null_dataset", {"complete": True, "version": 1, "dataset_id": None}, False),
        ("null_child", None, False),
        ("scalar_child", "true", False),
    )
    source_ids, expected_rows = [], []
    for metadata_position, metadata_key in enumerate(_readiness_metadata("unused")):
        for case_name, metadata_value, is_complete in readiness_cases:
            dataset_id = f"case_{metadata_position}_{case_name}"
            metadata = _readiness_metadata(dataset_id)
            metadata[metadata_key] = (
                {"dataset_id": dataset_id, **metadata_value} if isinstance(metadata_value, dict) else metadata_value
            )
            if case_name == "missing_dataset":
                metadata[metadata_key].pop("dataset_id")
            source_ids.append(await _readiness_insert(database, dataset_id, metadata))
            completeness_flags = [True, True]
            completeness_flags[metadata_position] = is_complete
            expected_rows.append((dataset_id, "import", *completeness_flags))
    for dataset_id, metadata in (("missing_children", {}), ("sql_null", None), ("scalar_root", [])):
        source_ids.append(await _readiness_insert(database, dataset_id, metadata))
        expected_rows.append((dataset_id, "import", False, False))
    connection, schema, _engine, _migration = database
    source_ids.append(await _readiness_insert(database, "json_null", {}))
    await connection.execute(
        f'UPDATE "{schema}".provider_directory_endpoint_dataset '
        "SET publication_metadata_json = 'null' WHERE dataset_id = 'json_null'"
    )
    expected_rows.append(("json_null", "import", False, False))
    return source_ids, expected_rows


async def _readiness_seed_scope(database, source_ids, expected_rows):
    connection, schema, _engine, _migration = database
    table = f'"{schema}".provider_directory_endpoint_dataset'
    for dataset_id, change in (
        ("not_current", "is_current = false"),
        ("not_published", "status = 'validated'"),
        ("no_publication_time", "published_at = NULL"),
        ("superseded", "superseded_at = '2026-01-02'"),
        ("no_run", "import_run_id = NULL"),
    ):
        source_ids.append(await _readiness_insert(database, dataset_id, _readiness_metadata(dataset_id)))
        await connection.execute(f"UPDATE {table} SET {change} WHERE dataset_id = $1", dataset_id)
    source_ids.append(await _readiness_insert(database, "duplicate", _readiness_metadata("duplicate")))
    await _readiness_insert(
        database, "unrequested_sibling", _readiness_metadata("unrequested_sibling"), endpoint_id="endpoint_duplicate"
    )
    await connection.execute(f"UPDATE {table} SET status = 'acquiring' WHERE dataset_id = 'unrequested_sibling'")
    await _readiness_insert(database, "unrequested", _readiness_metadata("unrequested"))
    source_ids.append(await _readiness_insert(database, "aliases", _readiness_metadata("aliases")))
    await connection.execute(
        f'INSERT INTO "{schema}".provider_directory_source (source_id, endpoint_id) '
        "VALUES ('second_alias', 'endpoint_aliases'), ('unrequested_alias', 'endpoint_aliases')"
    )
    source_ids.extend(("second_alias", "second_alias"))
    expected_rows.append(("aliases", "import", True, True))
    source_ids.append(await _readiness_insert(database, "root_run", _readiness_metadata("root_run")))
    await connection.execute(f"UPDATE {table} SET acquisition_root_run_id = 'root' WHERE dataset_id = 'root_run'")
    expected_rows.append(("root_run", "root", True, True))
    source_ids.append(await _readiness_insert(database, "root_only", _readiness_metadata("root_only")))
    await connection.execute(
        f"UPDATE {table} SET acquisition_root_run_id = 'root', import_run_id = NULL WHERE dataset_id = 'root_only'"
    )
    expected_rows.append(("root_only", "root", True, True))


@pytest.mark.asyncio
@pytest.mark.parametrize("readiness_database", ["json", "jsonb"], indirect=True)
async def test_readiness_ctes_preserve_values_scope_and_publication(readiness_database, monkeypatch):
    source_ids, expected_rows = await _readiness_seed_values(readiness_database)
    await _readiness_seed_scope(readiness_database, source_ids, expected_rows)
    expected_rows.sort()
    assert await _readiness_rows(readiness_database, source_ids, indexed=False) == expected_rows
    assert await _readiness_rows(readiness_database, source_ids, indexed=True) == expected_rows
    _connection, _schema, engine, migration = readiness_database
    await _run_upgrade_on_postgres(engine, migration, monkeypatch)
    assert await _readiness_rows(readiness_database, source_ids, indexed=True) == expected_rows


async def _readiness_limits(engine, timeout_values=None):
    from sqlalchemy import text

    async with engine.connect() as connection:
        if timeout_values is not None:
            await connection.execute(
                text(
                    "SELECT set_config('lock_timeout', :lock, false), "
                    "set_config('statement_timeout', :statement, false)"
                ),
                {"lock": timeout_values[0], "statement": timeout_values[1]},
            )
        result = await connection.execute(
            text("SELECT pg_backend_pid(), current_setting('lock_timeout'), current_setting('statement_timeout')")
        )
        observed_settings = tuple(result.one())
        await connection.commit()
        return observed_settings


@pytest.mark.asyncio
async def test_readiness_capabilities_partial_upgrade_and_idempotence(readiness_database, monkeypatch):
    connection, schema, engine, migration = readiness_database
    assert await _readiness_capabilities(readiness_database) == (False, False)
    metadata_key, first_name = migration.INDEXES[0]
    await connection.execute(migration._create_sql(schema, metadata_key, first_name))
    first_state = await _postgres_index_state(connection, schema, first_name)
    assert await _readiness_capabilities(readiness_database) == (True, False)
    limits = await _readiness_limits(engine, ("0", "0"))
    await _run_upgrade_on_postgres(engine, migration, monkeypatch)
    assert await _readiness_limits(engine) == limits
    assert await _readiness_capabilities(readiness_database) == (True, True)
    built_states = [await _postgres_index_state(connection, schema, name) for _key, name in migration.INDEXES]
    assert built_states[0]["oid"] == first_state["oid"]
    await _run_upgrade_on_postgres(engine, migration, monkeypatch)
    assert [await _postgres_index_state(connection, schema, name) for _key, name in migration.INDEXES] == built_states


@pytest.mark.asyncio
async def test_readiness_failed_build_retries_and_restores_timeouts(readiness_database, monkeypatch):
    from sqlalchemy.exc import DBAPIError

    connection, schema, engine, migration = readiness_database
    metadata = _readiness_metadata("malformed") | {"unselected": "\x00"}
    source = await _readiness_insert(readiness_database, "malformed", metadata)
    for indexed in (False, True):
        with pytest.raises(DBAPIError, match="Unicode|\\\\u0000"):
            await _readiness_rows(readiness_database, [source], indexed=indexed)
    limits = await _readiness_limits(engine, ("137ms", "19s"))
    with pytest.raises(DBAPIError, match="Unicode|\\\\u0000"):
        await _run_upgrade_on_postgres(engine, migration, monkeypatch)
    assert await _readiness_limits(engine) == limits
    first_name = migration.INDEXES[0][1]
    failed = await _postgres_index_state(connection, schema, first_name)
    assert failed is not None and failed["indisvalid"] is False
    assert await _readiness_capabilities(readiness_database) == (False, False)
    await connection.execute(f'DELETE FROM "{schema}".provider_directory_endpoint_dataset')
    await _run_upgrade_on_postgres(engine, migration, monkeypatch)
    rebuilt = await _postgres_index_state(connection, schema, first_name)
    assert rebuilt["indisvalid"] and rebuilt["indisready"] and rebuilt["oid"] != failed["oid"]
    assert await _readiness_capabilities(readiness_database) == (True, True)
    assert await _readiness_limits(engine) == limits


@pytest.mark.asyncio
@pytest.mark.parametrize("shape", ["wrong_key", "invalid_predicate", "other_table"])
async def test_readiness_migration_preserves_wrong_index_shapes(readiness_database, monkeypatch, shape):
    connection, schema, engine, migration = readiness_database
    name = migration.INDEXES[0][1]
    table = f'"{schema}".{migration.TABLE}'
    if shape == "other_table":
        await connection.execute(f'CREATE TABLE "{schema}".other_dataset (dataset_id text)')
        await connection.execute(f'CREATE INDEX "{name}" ON "{schema}".other_dataset (dataset_id)')
        assert await _readiness_capabilities(readiness_database) == (False, False)
    elif shape == "invalid_predicate":
        await _readiness_insert(readiness_database, "malformed", {"unselected": "\x00"})
        with pytest.raises(asyncpg.PostgresError):
            await connection.execute(
                f'CREATE INDEX CONCURRENTLY "{name}" ON {table} (dataset_id) '
                "WHERE publication_metadata_json::jsonb ? 'other'"
            )
    else:
        await connection.execute(f'CREATE INDEX "{name}" ON {table} (endpoint_id)')
    before = await _postgres_index_state(connection, schema, name)
    assert before is not None
    with pytest.raises(RuntimeError, match="existing_schema_index_mismatch"):
        await _run_upgrade_on_postgres(engine, migration, monkeypatch)
    assert await _postgres_index_state(connection, schema, name) == before
    assert await _postgres_index_state(connection, schema, migration.INDEXES[1][1]) is None


@pytest.mark.asyncio
async def test_readiness_catalog_lock_failure_restores_timeouts(readiness_database, monkeypatch):
    import asyncio

    from sqlalchemy import text
    from sqlalchemy.exc import DBAPIError

    connection, schema, engine, migration = readiness_database
    await _run_upgrade_on_postgres(engine, migration, monkeypatch)
    index_states = [await _postgres_index_state(connection, schema, name) for _key, name in migration.INDEXES]
    original_limits = await _readiness_limits(engine, ("100ms", "5s"))

    def check_locked_upgrade(sync_connection):
        context = MigrationContext.configure(sync_connection)
        monkeypatch.setattr(migration, "op", Operations(context))
        with context.begin_transaction():
            with pytest.raises(DBAPIError, match="lock timeout"):
                migration.upgrade()
            # The validation savepoint must leave this outer transaction usable.
            restored_limits = tuple(
                sync_connection.execute(
                    text(
                        "SELECT pg_backend_pid(), current_setting('lock_timeout'), current_setting('statement_timeout')"
                    )
                ).one()
            )
            assert restored_limits == original_limits

    async with connection.transaction():
        await connection.execute(f'LOCK TABLE "{schema}".{migration.TABLE} IN ACCESS EXCLUSIVE MODE')
        async with engine.connect() as migration_connection:
            await asyncio.wait_for(migration_connection.run_sync(check_locked_upgrade), timeout=3)
    assert await _readiness_limits(engine) == original_limits
    assert [await _postgres_index_state(connection, schema, name) for _key, name in migration.INDEXES] == index_states
