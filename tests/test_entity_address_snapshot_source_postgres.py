# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib
import os
import subprocess
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

source = importlib.import_module("process.entity_address_snapshot_source")


def _dsn() -> str:
    dsn = os.environ.get("HLTHPRT_ENTITY_ADDRESS_ARCHIVE_TEST_DSN", "")
    if not dsn:
        pytest.skip("HLTHPRT_ENTITY_ADDRESS_ARCHIVE_TEST_DSN is not set")
    return dsn


@pytest.mark.asyncio
async def test_native_capture_holds_seven_model_relations_until_rollback(tmp_path):
    engine = create_async_engine(_dsn())
    schema_name = "address_archive_" + uuid4().hex
    schema = f'"{schema_name}"'
    relations = source.entity_address_archive_relations()
    try:
        async with engine.begin() as connection:
            await connection.execute(text(f"CREATE SCHEMA {schema}"))
            for relation in relations:
                await connection.execute(text(f'CREATE TABLE {schema}."{relation.table_name}" (id bigint PRIMARY KEY)'))
                await connection.execute(text(f'INSERT INTO {schema}."{relation.table_name}" VALUES (1)'))
        sessions = async_sessionmaker(engine, expire_on_commit=False)

        async def archive_copy(capture):
            assert [relation.table_name for relation in capture.relations] == [
                relation.table_name for relation in relations
            ]
            dump = subprocess.run(
                [
                    os.environ["HLTHPRT_ENTITY_ADDRESS_ARCHIVE_TEST_PG_DUMP"],
                    "--format=custom",
                    "--file",
                    str(tmp_path / "source.dump"),
                    f"--snapshot={capture.postgres_snapshot}",
                    *[f"--table={schema_name}.{relation.table_name}" for relation in relations],
                ],
                capture_output=True,
                check=False,
                env=os.environ.copy(),
                text=True,
            )
            assert dump.returncode == 0, dump.stderr
            async with sessions() as contender:
                await contender.execute(text("SET lock_timeout = '100ms'"))
                with pytest.raises(Exception):
                    await contender.execute(text(f'ALTER TABLE {schema}."{relations[0].table_name}" RENAME TO blocked'))
            async with sessions() as contender:
                await contender.execute(text("SET lock_timeout = '100ms'"))
                with pytest.raises(Exception):
                    await contender.execute(text(f'DROP TABLE {schema}."{relations[1].table_name}"'))

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
