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
_REFERENCE_MIGRATION_PATH = (
    Path(__file__).resolve().parents[1] / "alembic/versions/20260914110000_reference_family_result_generation.py"
)
_MRF_MIGRATION_PATH = Path(__file__).resolve().parents[1] / "alembic/versions/20260914130000_mrf_result_generation.py"
_NPI_MIGRATION_PATH = Path(__file__).resolve().parents[1] / "alembic/versions/20260914120000_npi_result_generation.py"
_NPI_TABLES = (
    "npi",
    "npi_address",
    "npi_taxonomy",
    "npi_taxonomy_group",
    "npi_other_identifier",
    "npi_phone_staffing",
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


def _migration_module(path: Path):
    spec = importlib.util.spec_from_file_location("reference_family_generation_migration", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


async def _run_migration(connection, path: Path, action: str) -> None:
    module = _migration_module(path)

    def apply(sync_connection) -> None:
        module.op = Operations(MigrationContext.configure(sync_connection))
        getattr(module, action)()

    await connection.run_sync(apply)


async def _npi_catalog_state(connection, schema: str):
    return (
        await connection.execute(
            text(
                "SELECT local_lineage_id, "
                "(SELECT array_agg(c.oid::bigint ORDER BY c.relname) "
                "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                "WHERE n.nspname=:schema AND c.relname = ANY(:tables)), "
                "(SELECT array_agg(t.tgname ORDER BY t.tgrelid) "
                "FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid "
                "JOIN pg_namespace n ON n.oid=c.relnamespace "
                "WHERE n.nspname=:schema AND c.relname = ANY(:tables) AND NOT t.tgisinternal) "
                f'FROM "{schema}".npi_result_generation WHERE singleton IS TRUE'
            ),
            {"schema": schema, "tables": list(_NPI_TABLES)},
        )
    ).one()


@pytest.mark.asyncio
async def test_mrf_upgrade_preserves_npi_catalog_and_reference_rows(monkeypatch):
    """The MRF branch extends reference authority without touching DEV's NPI family."""

    schema = "reference_mrf_upgrade_" + uuid4().hex
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    engine = create_async_engine(_database_url(), poolclass=NullPool)
    try:
        async with engine.begin() as connection:
            await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
            for table_name in _NPI_TABLES:
                await connection.execute(text(f'CREATE TABLE "{schema}"."{table_name}" (value bigint)'))
            for relation_names in generation.RELATION_NAMES_BY_IMPORTER.values():
                for table_name in relation_names:
                    await connection.execute(text(f'CREATE TABLE "{schema}"."{table_name}" (value bigint)'))
                    await connection.execute(text(f'INSERT INTO "{schema}"."{table_name}" VALUES (1)'))
            await _run_migration(connection, _NPI_MIGRATION_PATH, "upgrade")
            await _run_migration(connection, _REFERENCE_MIGRATION_PATH, "upgrade")
            npi_before = await _npi_catalog_state(connection, schema)
            reference_rows_before = await connection.scalar(
                text(f'SELECT count(*) FROM "{schema}".reference_family_result_generation')
            )
            await _run_migration(connection, _MRF_MIGRATION_PATH, "upgrade")
            npi_after = await _npi_catalog_state(connection, schema)
            assert npi_after == npi_before
            assert (
                await connection.scalar(text(f'SELECT count(*) FROM "{schema}".reference_family_result_generation'))
                == reference_rows_before + 1
            )
            assert (
                await connection.scalar(
                    text(
                        f"SELECT count(*) FROM \"{schema}\".reference_family_result_generation WHERE importer_id='mrf'"
                    )
                )
                == 1
            )
            for relation_names in generation.RELATION_NAMES_BY_IMPORTER.values():
                for table_name in relation_names:
                    assert await connection.scalar(text(f'SELECT count(*) FROM "{schema}"."{table_name}"')) == 1
    finally:
        try:
            async with engine.begin() as connection:
                await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        finally:
            await engine.dispose()


async def _publish_initial_generations(engine, schema):
    """Publish and verify generation one for every closed family."""

    authority_by_importer = {}
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
            authority_by_importer[importer_id] = first
    return authority_by_importer


async def _assert_adoption_rollback(engine, schema, incumbent_authority, source_generation_by_field):
    """Rotate one family and prove its authority rolls back with the tables."""

    importer_id = incumbent_authority.importer_id
    with pytest.raises(RuntimeError, match="force rollback"):
        async with engine.begin() as connection:
            table_name = generation.RELATION_NAMES_BY_IMPORTER[importer_id][0]
            await connection.execute(text(f'ALTER TABLE "{schema}"."{table_name}" RENAME TO "{table_name}_old"'))
            await connection.execute(text(f'CREATE TABLE "{schema}"."{table_name}" (value bigint)'))
            adopted = await generation.publish_adopted_reference_family_generation(
                connection,
                importer_id=importer_id,
                schema_name=schema,
                source_generation=source_generation_by_field,
            )
            assert adopted.local_generation == 1
            assert adopted.serving_generation.origin_generation == 2
            assert adopted.relation_oids != incumbent_authority.relation_oids
            raise RuntimeError("force rollback")
    async with engine.connect() as connection:
        rolled_back = await generation.read_reference_family_result_generation_authority(
            connection,
            importer_id=importer_id,
            schema_name=schema,
        )
        assert rolled_back == incumbent_authority
        assert (
            await generation.capture_reference_family_serving_generation(
                connection,
                importer_id=importer_id,
                schema_name=schema,
            )
        ) == incumbent_authority.serving_generation


@pytest.mark.asyncio
async def test_five_family_generation_publication_adoption_and_rollback(monkeypatch):
    """Bind every family to exact OIDs and keep adoption transaction-local."""

    schema = "reference_generation_" + uuid4().hex
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    engine = create_async_engine(_database_url(), poolclass=NullPool)
    try:
        async with engine.begin() as connection:
            await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
            await _run_migration(connection, _REFERENCE_MIGRATION_PATH, "upgrade")
            await _run_migration(connection, _MRF_MIGRATION_PATH, "upgrade")
            for relation_names in generation.RELATION_NAMES_BY_IMPORTER.values():
                for table_name in relation_names:
                    await connection.execute(text(f'CREATE TABLE "{schema}"."{table_name}" (value bigint)'))

        authority_by_importer = await _publish_initial_generations(engine, schema)
        incumbent_authority = authority_by_importer["places-zcta"]
        source_generation_by_field = {
            "origin_lineage_id": incumbent_authority.local_lineage_id,
            "origin_generation": 2,
            "published_at": datetime.datetime(2026, 9, 14, 10, tzinfo=datetime.UTC),
        }
        await _assert_adoption_rollback(engine, schema, incumbent_authority, source_generation_by_field)

        async with engine.begin() as connection:
            with pytest.raises(RuntimeError, match="prevents downgrade"):
                await _run_migration(connection, _MRF_MIGRATION_PATH, "downgrade")
    finally:
        try:
            async with engine.begin() as connection:
                await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        finally:
            await engine.dispose()
