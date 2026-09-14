# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import datetime
import importlib
import importlib.util
import os
import re
from pathlib import Path
from uuid import uuid4

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool


generation = importlib.import_module("process.reference_family_result_generation")
_DSN_ENV = "HLTHPRT_REFERENCE_FAMILY_ARCHIVE_TEST_DSN"
_LOCAL_DATABASE = re.compile(r"^hc_reference_family_[0-9a-f]{32}$")
_MIGRATION_PATH = (
    Path(__file__).resolve().parents[1] / "alembic/versions/20260914110000_reference_family_result_generation.py"
)


def _database_url():
    raw = os.getenv(_DSN_ENV, "")
    if not raw:
        pytest.skip(f"{_DSN_ENV} is not set")
    url = make_url(raw)
    if (
        not url.drivername.startswith("postgresql")
        or url.host not in {"127.0.0.1", "localhost"}
        or url.port != 5440
        or not _LOCAL_DATABASE.fullmatch(str(url.database or ""))
    ):
        pytest.fail(f"{_DSN_ENV} must identify a UUID-owned PostgreSQL 18 test database on port 5440")
    return url.set(drivername="postgresql+asyncpg").render_as_string(hide_password=False)


def _migration_module():
    spec = importlib.util.spec_from_file_location("reference_family_generation_migration", _MIGRATION_PATH)
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


@pytest.mark.asyncio
async def test_four_family_generation_publication_adoption_and_rollback(monkeypatch):
    """Bind every family to exact OIDs and keep adoption transaction-local."""

    schema = "reference_generation_" + uuid4().hex
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    engine = create_async_engine(_database_url(), poolclass=NullPool)
    try:
        async with engine.begin() as connection:
            await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
            await _run_migration(connection, "upgrade")
            for relation_names in generation.RELATION_NAMES_BY_IMPORTER.values():
                for table_name in relation_names:
                    await connection.execute(text(f'CREATE TABLE "{schema}"."{table_name}" (value bigint)'))

        first_by_importer = {}
        for importer_id in generation.RELATION_NAMES_BY_IMPORTER:
            async with engine.begin() as connection:
                first = await generation.publish_local_reference_family_generation(
                    connection,
                    importer_id=importer_id,
                    schema_name=schema,
                )
                assert first.local_generation == 1
                assert first.serving_generation.origin_lineage_id == first.local_lineage_id
                assert first.serving_generation.origin_generation == 1
                assert first.relation_oids == await generation.current_reference_family_relation_oids(
                    connection,
                    importer_id=importer_id,
                    schema_name=schema,
                )
                first_by_importer[importer_id] = first

        importer_id = "places-zcta"
        source = first_by_importer[importer_id]
        source_generation = {
            "origin_lineage_id": source.local_lineage_id,
            "origin_generation": 2,
            "published_at": datetime.datetime(2026, 9, 14, 10, tzinfo=datetime.UTC),
        }
        with pytest.raises(RuntimeError, match="force rollback"):
            async with engine.begin() as connection:
                table_name = generation.RELATION_NAMES_BY_IMPORTER[importer_id][0]
                await connection.execute(text(f'ALTER TABLE "{schema}"."{table_name}" RENAME TO "{table_name}_old"'))
                await connection.execute(text(f'CREATE TABLE "{schema}"."{table_name}" (value bigint)'))
                adopted = await generation.publish_adopted_reference_family_generation(
                    connection,
                    importer_id=importer_id,
                    schema_name=schema,
                    source_generation=source_generation,
                )
                assert adopted.local_generation == 1
                assert adopted.serving_generation.origin_generation == 2
                assert adopted.relation_oids != source.relation_oids
                raise RuntimeError("force rollback")

        async with engine.connect() as connection:
            rolled_back = await generation.read_reference_family_result_generation_authority(
                connection,
                importer_id=importer_id,
                schema_name=schema,
            )
            assert rolled_back == source
            assert (
                await generation.capture_reference_family_serving_generation(
                    connection,
                    importer_id=importer_id,
                    schema_name=schema,
                )
            ) == source.serving_generation

        async with engine.begin() as connection:
            with pytest.raises(RuntimeError, match="prevents downgrade"):
                await _run_migration(connection, "downgrade")
    finally:
        try:
            async with engine.begin() as connection:
                await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        finally:
            await engine.dispose()
