# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib
import os
import subprocess
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.schema import MetaData

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
