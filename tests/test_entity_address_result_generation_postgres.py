# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import datetime
import importlib
import importlib.util
import os
import re
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool


generation = importlib.import_module("process.entity_address_result_generation")
_DSN_ENV = "HLTHPRT_ENTITY_ADDRESS_GENERATION_TEST_DSN"
_DATABASE_PATTERN = re.compile(r"^hc_address_generation_[0-9a-f]{32}$")
_ROOT = Path(__file__).resolve().parents[1]
_MIGRATION_PATH = (
    _ROOT / "alembic/versions/20260914100000_entity_address_result_generation.py"
)


class _ConnectionDatabase:
    def __init__(self, connection):
        self.connection = connection

    async def first(self, statement, **parameters):
        return (await self.connection.execute(statement, parameters)).first()

    async def all(self, statement, **parameters):
        return (await self.connection.execute(statement, parameters)).all()


def _database_url():
    raw_dsn = os.getenv(_DSN_ENV, "")
    if not raw_dsn:
        pytest.skip(f"{_DSN_ENV} is not set")
    database_url = make_url(raw_dsn)
    if (
        database_url.drivername != "postgresql"
        or database_url.host not in {"127.0.0.1", "localhost"}
        or database_url.port != 5440
        or not _DATABASE_PATTERN.fullmatch(str(database_url.database or ""))
    ):
        pytest.fail(f"{_DSN_ENV} must identify a UUID-scoped local PostgreSQL test database")
    return database_url.set(drivername="postgresql+asyncpg")


def _migration_module():
    spec = importlib.util.spec_from_file_location("entity_address_result_generation_migration", _MIGRATION_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


async def _run_migration(connection, action: str) -> None:
    module = _migration_module()

    def apply(sync_connection) -> None:
        module.op = Operations(MigrationContext.configure(sync_connection))
        getattr(module, action)()

    await connection.run_sync(apply)


async def _authority(connection, schema: str = "mrf"):
    row = (
        await connection.execute(
            text(
                f'SELECT singleton, local_lineage_id, local_generation, origin_lineage_id, '
                f'origin_generation, published_at, relation_oids FROM "{schema}".'
                '"entity_address_result_generation" WHERE singleton IS TRUE'
            )
        )
    ).mappings().one()
    return generation.validate_entity_address_result_generation_authority(row)


async def _relation_oids(connection, schema: str = "mrf") -> tuple[int, ...]:
    values = []
    for table_name in generation.RELATION_NAMES:
        value = await connection.scalar(
            text("SELECT to_regclass(:name)::oid::bigint"),
            {"name": f"{schema}.{table_name}"},
        )
        values.append(int(value))
    return tuple(values)


@pytest.mark.parametrize("schema", ["unsafe-schema", "x" * 64])
def test_migration_rejects_unsafe_schema_before_sql(
    monkeypatch: pytest.MonkeyPatch,
    schema: str,
):
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.delenv("DB_SCHEMA", raising=False)

    with pytest.raises(RuntimeError, match="schema is invalid"):
        _migration_module()._schema()


@pytest.mark.asyncio
async def test_generation_migration_publication_adoption_and_transaction_rollback(
    monkeypatch: pytest.MonkeyPatch,
):
    database_url = _database_url()
    schema = "address_generation_" + uuid4().hex
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    engine = create_async_engine(database_url, poolclass=NullPool)
    source_lineage = uuid4()
    source_published_at = datetime.datetime(2026, 9, 14, 8, 30, tzinfo=datetime.UTC)
    try:
        async with engine.begin() as connection:
            await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
            await _run_migration(connection, "upgrade")
            initial = await _authority(connection, schema)
            assert UUID(initial.local_lineage_id)
            assert initial.local_generation == 0
            assert initial.serving_generation is None
            assert initial.relation_oids is None
            for table_name in generation.RELATION_NAMES:
                await connection.execute(text(f'CREATE TABLE "{schema}"."{table_name}" (value bigint)'))

        invalid_serving_updates = (
            (
                "origin_lineage_id=local_lineage_id, origin_generation=1, "
                "published_at=clock_timestamp(), "
                "relation_oids=ARRAY[10, 10, 12, 13, 14, 15, 16]::bigint[]"
            ),
            (
                "origin_lineage_id=local_lineage_id, origin_generation=NULL, "
                "published_at=clock_timestamp(), "
                "relation_oids=ARRAY[10, 11, 12, 13, 14, 15, 16]::bigint[]"
            ),
            (
                "origin_lineage_id=local_lineage_id, origin_generation=1, "
                "published_at=clock_timestamp(), "
                "relation_oids='[0:6]={10,11,12,13,14,15,16}'::bigint[]"
            ),
            (
                "origin_lineage_id=local_lineage_id, origin_generation=1, "
                "published_at=clock_timestamp(), "
                "relation_oids=ARRAY[[10, 11, 12, 13, 14, 15, 16]]::bigint[]"
            ),
        )
        for invalid_update in invalid_serving_updates:
            with pytest.raises(DBAPIError):
                async with engine.begin() as connection:
                    await connection.execute(
                        text(
                            f'UPDATE "{schema}"."entity_address_result_generation" '
                            f"SET {invalid_update} WHERE singleton IS TRUE"
                        )
                    )

        before_publication = datetime.datetime.now(datetime.UTC)
        async with engine.begin() as connection:
            first = await generation.publish_local_entity_address_generation(
                _ConnectionDatabase(connection),
                schema_name=schema,
            )
        after_publication = datetime.datetime.now(datetime.UTC)
        assert first.local_generation == 1
        assert first.serving_generation.origin_lineage_id == first.local_lineage_id
        assert first.serving_generation.origin_generation == 1
        assert before_publication <= first.serving_generation.published_at <= after_publication
        async with engine.connect() as connection:
            assert first.relation_oids == await _relation_oids(connection, schema)

        async with engine.begin() as connection:
            second = await generation.publish_local_entity_address_generation(
                _ConnectionDatabase(connection),
                schema_name=schema,
            )
            main_table = generation.RELATION_NAMES[0]
            await connection.execute(
                text(
                    f'ALTER TABLE "{schema}"."{main_table}" '
                    f'RENAME TO "{main_table}_source_old"'
                )
            )
            await connection.execute(
                text(f'CREATE TABLE "{schema}"."{main_table}" (value bigint)')
            )
            adopted = await generation.publish_adopted_entity_address_generation(
                _ConnectionDatabase(connection),
                schema_name=schema,
                source_generation={
                    "origin_lineage_id": str(source_lineage),
                    "origin_generation": 27,
                    "published_at": source_published_at,
                },
            )
        assert second.local_generation == 2
        assert adopted.local_generation == 2
        assert adopted.local_lineage_id == second.local_lineage_id
        assert adopted.serving_generation.origin_lineage_id == str(source_lineage)
        assert adopted.serving_generation.origin_generation == 27
        assert adopted.serving_generation.published_at == source_published_at
        assert adopted.relation_oids[0] != second.relation_oids[0]

        async with engine.begin() as connection:
            legacy = await generation.publish_adopted_entity_address_generation(
                _ConnectionDatabase(connection),
                schema_name=schema,
                source_generation=None,
            )
        assert legacy.local_generation == 2
        assert legacy.serving_generation is None
        assert legacy.relation_oids is None

        async with engine.connect() as connection:
            original_oids = await _relation_oids(connection, schema)
        with pytest.raises(RuntimeError, match="force rollback"):
            async with engine.begin() as connection:
                await connection.execute(
                    text(
                        f'ALTER TABLE "{schema}"."{main_table}" '
                        f'RENAME TO "{main_table}_rollback_old"'
                    )
                )
                await connection.execute(
                    text(f'CREATE TABLE "{schema}"."{main_table}" (value bigint)')
                )
                advanced = await generation.publish_local_entity_address_generation(
                    _ConnectionDatabase(connection),
                    schema_name=schema,
                )
                assert advanced.local_generation == 3
                assert advanced.relation_oids[0] != original_oids[0]
                raise RuntimeError("force rollback")

        async with engine.connect() as connection:
            rolled_back = await _authority(connection, schema)
            assert rolled_back.local_generation == 2
            assert rolled_back.serving_generation is None
            assert await _relation_oids(connection, schema) == original_oids
            assert await connection.scalar(
                text("SELECT to_regclass(:name) IS NULL"),
                {"name": f"{schema}.{main_table}_rollback_old"},
            )

        async with engine.begin() as connection:
            await _run_migration(connection, "downgrade")
            assert await connection.scalar(
                text("SELECT to_regclass(:name) IS NULL"),
                {"name": f"{schema}.entity_address_result_generation"},
            )
    finally:
        try:
            async with engine.begin() as connection:
                await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        finally:
            await engine.dispose()
