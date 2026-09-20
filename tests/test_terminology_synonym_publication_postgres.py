# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PostgreSQL identity and rollback proof for terminology publication."""

from __future__ import annotations

import importlib
import importlib.util
import os
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace

import asyncpg
import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine


terminology_synonyms = importlib.import_module("process.terminology_synonyms")
generation = importlib.import_module("process.reference_family_result_generation")
archive = importlib.import_module("process.reference_family_archive")
_ROOT = Path(__file__).resolve().parents[1]
LIVE_TABLE = "terminology_synonym"
OLD_TABLE = f"{LIVE_TABLE}_old"
STAGE_TABLE = f"{LIVE_TABLE}_stage"


class _AsyncpgDatabase:
    def __init__(self, connection):
        self.connection = connection

    async def status(self, statement):
        return await self.connection.execute(statement)

    async def all(self, statement, **params):
        statement = getattr(statement, "text", statement)
        arguments = []
        for position, (name, value) in enumerate(params.items(), start=1):
            statement = statement.replace(f":{name}", f"${position}")
            arguments.append(value)
        return await self.connection.fetch(statement, *arguments)

    async def first(self, statement, **params):
        rows = await self.all(statement, **params)
        return rows[0] if rows else None

    @asynccontextmanager
    async def transaction(self):
        async with self.connection.transaction():
            yield


def _qualified(schema, table):
    return f'"{schema}"."{table}"'


async def _relation_state(connection, schema, table):
    relation_name = f"{schema}.{table}"
    relation_oid = await connection.fetchval(
        "SELECT to_regclass($1)::oid::bigint",
        relation_name,
    )
    if relation_oid is None:
        return None
    markers = tuple(
        await connection.fetch(
            f"SELECT marker FROM {_qualified(schema, table)} ORDER BY marker"
        )
    )
    return relation_oid, tuple(row["marker"] for row in markers)


async def _prepare_relations(
    connection,
    schema,
    stage_markers=None,
    old_markers=None,
):
    default_stage_markers = ["stage-a", "stage-b", "stage-c"]
    marker_list_by_table = {
        LIVE_TABLE: ["live-a", "live-b"],
        OLD_TABLE: old_markers if old_markers is not None else ["older"],
        STAGE_TABLE: stage_markers if stage_markers is not None else default_stage_markers,
    }
    await connection.execute(f'CREATE SCHEMA "{schema}"')
    await connection.execute(
        f'CREATE TABLE "{schema}".reference_family_result_generation ('
        "importer_id text PRIMARY KEY, local_lineage_id uuid NOT NULL, local_generation bigint NOT NULL, "
        "origin_lineage_id uuid, origin_generation bigint, published_at timestamptz, relation_oids bigint[])"
    )
    await connection.execute(
        f'INSERT INTO "{schema}".reference_family_result_generation VALUES ($1,$2,0)',
        "terminology-synonyms", uuid.uuid4(),
    )
    for table, marker_list in marker_list_by_table.items():
        await connection.execute(f"CREATE TABLE {_qualified(schema, table)} (marker text NOT NULL)")
        await connection.executemany(
            f"INSERT INTO {_qualified(schema, table)} (marker) VALUES ($1)",
            [(marker,) for marker in marker_list],
        )
    return {
        table: await _relation_state(connection, schema, table)
        for table in marker_list_by_table
    }


@pytest.mark.asyncio
async def test_terminology_generation_migration_admits_one_exact_relation(monkeypatch):
    dsn = os.getenv("HLTHPRT_TERMINOLOGY_PUBLICATION_POSTGRES_DSN")
    if not dsn:
        pytest.skip("requires disposable PostgreSQL")
    schema = "terminology_migration_" + uuid.uuid4().hex
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    engine = create_async_engine(make_url(dsn).set(drivername="postgresql+asyncpg"))
    migrations = (
        "20260914110000_reference_family_result_generation.py",
        "20260914130000_mrf_result_generation.py",
        "20260920100000_cms_doctors_result_generation.py",
        "20260920110000_tiger_result_generation.py",
        "20260920120000_mrf_address_result_generation.py",
        "20260920130000_geo_result_generation.py",
        "20260920140000_pharmacy_economics_result_generation.py",
        "20260920150000_terminology_result_generation.py",
    )

    async def apply(connection, filename, action):
        spec = importlib.util.spec_from_file_location(filename[:-3], _ROOT / "alembic/versions" / filename)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        def run(sync_connection):
            module.op = Operations(MigrationContext.configure(sync_connection))
            getattr(module, action)()

        await connection.run_sync(run)

    try:
        async with engine.begin() as connection:
            await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
            await connection.execute(text(f'CREATE TABLE "{schema}".terminology_synonym (value text)'))
            for filename in migrations:
                await apply(connection, filename, "upgrade")
            assert await connection.scalar(text(
                f'SELECT count(*) FROM "{schema}".reference_family_result_generation'
            )) == 11
            published = await generation.publish_local_reference_family_generation(
                connection, importer_id="terminology-synonyms", schema_name=schema
            )
            assert published.relation_oids == (
                await connection.scalar(text(f"SELECT '{schema}.terminology_synonym'::regclass::oid::bigint")),
            )
            with pytest.raises(RuntimeError, match="prevents downgrade"):
                await apply(connection, migrations[-1], "downgrade")
    finally:
        async with engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await engine.dispose()


@pytest.mark.asyncio
async def test_terminology_reference_stage_has_complete_model_indexes():
    dsn = os.getenv("HLTHPRT_TERMINOLOGY_PUBLICATION_POSTGRES_DSN")
    if not dsn:
        pytest.skip("requires disposable PostgreSQL")
    engine = create_async_engine(make_url(dsn).set(drivername="postgresql+asyncpg"))
    dataset_id = uuid.uuid4()
    stage_schema = archive.reference_family_stage_schema(dataset_id)
    try:
        async with engine.begin() as connection:
            ownership = await archive.precreate_reference_family_restore(
                connection, importer_id="terminology-synonyms", dataset_id=dataset_id
            )
            assert ownership.relation_oids[0][0] == LIVE_TABLE
            await archive.complete_reference_family_restore(connection, ownership)
            indexes = (await connection.execute(text(
                "SELECT indexname FROM pg_indexes WHERE schemaname=:schema"
            ), {"schema": stage_schema})).scalars().all()
            assert len(indexes) >= 5
    finally:
        async with engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{stage_schema}" CASCADE'))
        await engine.dispose()


@asynccontextmanager
async def _database_case(monkeypatch):
    dsn = os.getenv("HLTHPRT_TERMINOLOGY_PUBLICATION_POSTGRES_DSN")
    if not dsn:
        pytest.skip("requires disposable PostgreSQL")

    connection = await asyncpg.connect(dsn)
    schema = f"terminology_publish_{uuid.uuid4().hex}"
    monkeypatch.setattr(terminology_synonyms, "db", _AsyncpgDatabase(connection))
    try:
        yield connection, schema
    finally:
        await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await connection.close()


@pytest.mark.asyncio
async def test_terminology_publication_preserves_predecessor_and_rolls_back_mismatch(monkeypatch):
    stage_cls = SimpleNamespace(__tablename__=STAGE_TABLE)
    async with _database_case(monkeypatch) as (connection, schema):
        original_state_by_table = await _prepare_relations(connection, schema)

        await terminology_synonyms._publish_stage(schema, stage_cls, 3)
        published = await generation.capture_reference_family_serving_generation(
            terminology_synonyms.db, importer_id="terminology-synonyms", schema_name=schema
        )
        assert published.origin_generation == 1

        assert await _relation_state(connection, schema, LIVE_TABLE) == original_state_by_table[STAGE_TABLE]
        assert await _relation_state(connection, schema, OLD_TABLE) == original_state_by_table[LIVE_TABLE]
        assert await _relation_state(connection, schema, STAGE_TABLE) is None
        assert not await connection.fetchval(
            "SELECT EXISTS (SELECT 1 FROM pg_class WHERE oid=$1)",
            original_state_by_table[OLD_TABLE][0],
        )

        await connection.execute(f'DROP SCHEMA "{schema}" CASCADE')
        original_state_by_table = await _prepare_relations(connection, schema)

        with pytest.raises(RuntimeError, match="promoted row count 3 does not match staged row count 4"):
            await terminology_synonyms._publish_stage(schema, stage_cls, 4)
        authority = await generation.read_reference_family_result_generation_authority(
            terminology_synonyms.db, importer_id="terminology-synonyms", schema_name=schema
        )
        assert authority.local_generation == 0

        for table, original_state in original_state_by_table.items():
            assert await _relation_state(connection, schema, table) == original_state

        await connection.execute(f'DROP SCHEMA "{schema}" CASCADE')
        original_state_by_table = await _prepare_relations(connection, schema, stage_markers=[])

        with pytest.raises(RuntimeError, match="refusing to publish an empty terminology snapshot"):
            await terminology_synonyms._publish_stage(schema, stage_cls, 0)

        for table, original_state in original_state_by_table.items():
            assert await _relation_state(connection, schema, table) == original_state


@pytest.mark.asyncio
async def test_terminology_rollback_reverses_relation_oids_and_content(monkeypatch):
    stage_cls = SimpleNamespace(__tablename__=STAGE_TABLE)
    async with _database_case(monkeypatch) as (connection, schema):
        original_state_by_table = await _prepare_relations(connection, schema)
        await terminology_synonyms._publish_stage(schema, stage_cls, 3)

        rollback_result = await terminology_synonyms._rollback_terminology_snapshot(
            schema,
            expected_live_oid=original_state_by_table[STAGE_TABLE][0],
            expected_old_oid=original_state_by_table[LIVE_TABLE][0],
        )

        assert rollback_result == {
            "live_oid": original_state_by_table[LIVE_TABLE][0],
            "predecessor_oid": original_state_by_table[STAGE_TABLE][0],
            "schema": schema,
        }
        assert await _relation_state(connection, schema, LIVE_TABLE) == original_state_by_table[LIVE_TABLE]
        assert await _relation_state(connection, schema, OLD_TABLE) == original_state_by_table[STAGE_TABLE]
        authority = await generation.read_reference_family_result_generation_authority(
            terminology_synonyms.db, importer_id="terminology-synonyms", schema_name=schema
        )
        assert authority.local_generation == 2
        assert authority.relation_oids == (original_state_by_table[LIVE_TABLE][0],)


@pytest.mark.asyncio
async def test_terminology_rollback_rejects_empty_predecessor_without_mutation(monkeypatch):
    async with _database_case(monkeypatch) as (connection, schema):
        original_state_by_table = await _prepare_relations(
            connection,
            schema,
            old_markers=[],
        )

        with pytest.raises(RuntimeError, match="predecessor is empty"):
            await terminology_synonyms._rollback_terminology_snapshot(
                schema,
                expected_live_oid=original_state_by_table[LIVE_TABLE][0],
                expected_old_oid=original_state_by_table[OLD_TABLE][0],
            )

        for table, original_state in original_state_by_table.items():
            assert await _relation_state(connection, schema, table) == original_state
