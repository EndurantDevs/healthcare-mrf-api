# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""MS-DRG row receipts and authority commit with the same PostgreSQL publish."""

from __future__ import annotations

import asyncio
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
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.schema import MetaData

from db.models import CodeCatalog, CodeRelationship, CodeSynonym
from process import ms_drg_result_archive as archive
from process import ms_drg_result_generation as authority
from process.ms_drg_publication import SOURCE_ICD10PCS_INDEX, SOURCE_MS_DRG

_MIGRATION_PATH = Path(__file__).resolve().parents[1] / "alembic/versions/20260923021000_ms_drg_result_generation.py"


async def _upgrade_generation(connection) -> None:
    spec = importlib.util.spec_from_file_location("ms_drg_result_generation_migration", _MIGRATION_PATH)
    assert spec is not None and spec.loader is not None
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)

    def apply(sync_connection) -> None:
        migration.op = Operations(MigrationContext.configure(sync_connection))
        migration.upgrade()

    await connection.run_sync(apply)


def _dsn() -> str:
    raw = os.getenv("HLTHPRT_MS_DRG_AUTHORITY_TEST_DSN", "")
    if not raw:
        pytest.skip("HLTHPRT_MS_DRG_AUTHORITY_TEST_DSN is not set")
    url = make_url(raw)
    if (
        (url.drivername, url.username, url.host) != ("postgresql", "postgres", "127.0.0.1")
        or url.port not in {5432, 5440}
        or re.fullmatch(r"hc_ms_drg_authority_[0-9a-f]{32}", url.database or "") is None
    ):
        pytest.fail("MS-DRG authority test requires its dedicated local database")
    return url.set(drivername="postgresql+asyncpg").render_as_string(hide_password=False)


async def _create_source(engine, schema):
    metadata = MetaData(schema=schema)
    for model in (CodeCatalog, CodeSynonym, CodeRelationship):
        model.__table__.to_metadata(metadata, schema=schema)
    async with engine.begin() as connection:
        await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
        await connection.run_sync(metadata.create_all)
        await _upgrade_generation(connection)
        await connection.execute(
            text(f"INSERT INTO \"{schema}\".code_catalog (code_system,code,source) VALUES ('OTHER','x','foreign')")
        )


async def _source_variants(sessions, schema):
    async with sessions.begin() as session:
        await session.execute(
            text(f"INSERT INTO \"{schema}\".code_catalog (code_system,code,source) VALUES ('MS_DRG','001',:source)"),
            {"source": SOURCE_MS_DRG},
        )
        await session.execute(
            text(
                f'INSERT INTO "{schema}".code_synonym '
                "(code_system,code,synonym,term_type,source) "
                "VALUES ('MS_DRG','001','DRG 001','alias',:source)"
            ),
            {"source": SOURCE_MS_DRG},
        )
        first = await authority.publish_local_generation(session, schema, include_relationships=False)
        assert first["local_generation"] == 1
        assert first["receipt"]["tables"][2]["row_count"] == 0
    async with sessions.begin() as session:
        catalog_stage, catalog_manifest = await archive.prepare_source(session, schema, uuid4())
        assert catalog_manifest["include_relationships"] is False
        assert catalog_manifest["tables"][2]["row_count"] == 0
        await archive.cleanup_stage(session, catalog_stage, receiving=False)

    async with sessions.begin() as session:
        await session.execute(
            text(
                f'INSERT INTO "{schema}".code_relationship '
                "(from_system,from_code,relationship,to_system,to_code,source) "
                "VALUES ('ICD10PCS','a','belongs_to','MS_DRG','001',:source)"
            ),
            {"source": SOURCE_ICD10PCS_INDEX},
        )
        second = await authority.publish_local_generation(session, schema, include_relationships=True)
        assert second["local_generation"] == 2
        assert second["receipt"]["tables"][2]["row_count"] == 1

    with pytest.raises(RuntimeError, match="synthetic late failure"):
        async with sessions.begin() as session:
            await session.execute(
                text(f"UPDATE \"{schema}\".code_catalog SET code='002' WHERE source=:source"),
                {"source": SOURCE_MS_DRG},
            )
            await authority.publish_local_generation(session, schema, include_relationships=False)
            raise RuntimeError("synthetic late failure")
    async with sessions.begin() as session:
        current = await authority.read_current_generation(session, schema)
        assert current["local_generation"] == 2
        assert current["receipt"]["tables"][2]["row_count"] == 1
        assert await session.scalar(text(f"SELECT count(*) FROM \"{schema}\".code_catalog WHERE code='002'")) == 0
        assert await session.scalar(text(f"SELECT count(*) FROM \"{schema}\".code_catalog WHERE source='foreign'")) == 1


async def _source_stage_validation(sessions, schema):
    dataset_id = uuid4()
    async with sessions.begin() as session:
        stage, manifest = await archive.prepare_source(session, schema, dataset_id)
    async with sessions.begin() as session:
        await archive.verify_stage(session, stage, manifest, receiving=False)
        await session.execute(
            text(
                f'INSERT INTO "{stage["schema_name"]}".code_catalog '
                "(code_system,code,source) VALUES ('OTHER','extra',NULL)"
            )
        )
        with pytest.raises(archive.MsDrgArchiveError, match="foreign source"):
            await archive.verify_stage(session, stage, manifest, receiving=False)
        await session.execute(text(f"DELETE FROM \"{stage['schema_name']}\".code_catalog WHERE code='extra'"))
        await archive.cleanup_stage(session, stage, receiving=False)


async def _create_destination(engine):
    destination = "ms_drg_dest_" + uuid4().hex[:12]
    destination_tables = MetaData(schema=destination)
    for model in (CodeCatalog, CodeSynonym, CodeRelationship):
        model.__table__.to_metadata(destination_tables, schema=destination)
    async with engine.begin() as connection:
        await connection.execute(text(f'CREATE SCHEMA "{destination}"'))
        await connection.run_sync(destination_tables.create_all)
        await connection.execute(
            text(
                f'CREATE TABLE "{destination}".ms_drg_result_generation ('
                "id smallint primary key,local_lineage_id uuid not null,local_generation bigint not null,"
                "origin_lineage_id uuid,origin_generation bigint,published_at timestamptz,"
                "include_relationships boolean,receipt jsonb)"
            )
        )
        await connection.execute(
            text(
                f'INSERT INTO "{destination}".ms_drg_result_generation '
                "(id,local_lineage_id,local_generation) VALUES (1,:lineage,0)"
            ),
            {"lineage": str(uuid4())},
        )
        await connection.execute(
            text(
                f'INSERT INTO "{destination}".code_catalog (code_system,code,source) '
                "VALUES ('OTHER','retained','foreign'),('MS_DRG','001','foreign')"
            )
        )
    return destination


async def _activation_roundtrip(engine, sessions, schema, destination):
    async with sessions.begin() as session:
        source_stage, manifest = await archive.prepare_source(session, schema, uuid4())
        receiving = await archive.precreate_restore(session, destination, uuid4(), manifest)
        for name in archive.TABLES:
            await session.execute(
                text(
                    f'INSERT INTO "{receiving["schema_name"]}"."{name}" '
                    f'SELECT * FROM "{source_stage["schema_name"]}"."{name}"'
                )
            )
        await archive.verify_stage(session, receiving, manifest, receiving=True)
        prepared = await archive.prepare_predecessor(session, destination, receiving, manifest)
    with pytest.raises(archive.MsDrgArchiveError, match="key belongs to another source"):
        async with sessions.begin() as session:
            await archive.activate_stage(session, destination, receiving, manifest, prepared)
    async with sessions.begin() as session:
        await session.execute(text(f"DELETE FROM \"{destination}\".code_catalog WHERE code='001'"))
    with pytest.raises(RuntimeError, match="synthetic activation failure"):
        async with sessions.begin() as session:
            await archive.activate_stage(session, destination, receiving, manifest, prepared)
            raise RuntimeError("synthetic activation failure")
    async with sessions.begin() as session:
        assert await session.scalar(text(f'SELECT local_generation FROM "{destination}".ms_drg_result_generation')) == 0
        assert await session.scalar(text(f'SELECT count(*) FROM "{destination}".code_relationship')) == 0
    async with sessions.begin() as session:
        activation = await archive.activate_stage(session, destination, receiving, manifest, prepared)
        assert activation["current"]["local_generation"] == 1
        assert activation["current"]["include_relationships"] is True
    async with sessions.begin() as session:
        restored = await archive.rollback_activation(session, destination, activation)
        assert restored["local_generation"] == 2
        assert restored["origin_generation"] is None
        assert (
            await session.scalar(text(f"SELECT count(*) FROM \"{destination}\".code_catalog WHERE source='foreign'"))
            == 1
        )
        await archive.cleanup_stage(session, receiving, receiving=True)
        await archive.cleanup_stage(session, source_stage, receiving=False)
    async with engine.begin() as connection:
        await connection.execute(text(f'DROP SCHEMA "{destination}" CASCADE'))


async def _concurrent_publication(sessions, schema):
    entered = asyncio.Event()
    release = asyncio.Event()

    async def first_publisher():
        async with sessions.begin() as session:
            result = await authority.publish_local_generation(session, schema, include_relationships=False)
            entered.set()
            await release.wait()
            return result["local_generation"]

    async def second_publisher():
        await entered.wait()
        async with sessions.begin() as session:
            result = await authority.publish_local_generation(session, schema, include_relationships=True)
            return result["local_generation"]

    first_task = asyncio.create_task(first_publisher())
    second_task = asyncio.create_task(second_publisher())
    await asyncio.wait_for(entered.wait(), 5)
    await asyncio.sleep(0.05)
    assert not second_task.done()
    release.set()
    assert await asyncio.wait_for(first_task, 5) == 3
    assert await asyncio.wait_for(second_task, 5) == 4


async def _retained_and_drift(sessions, schema):
    async with sessions.begin() as session:
        retained = await authority.publish_local_generation(session, schema, include_relationships=False)
        assert retained["receipt"]["tables"][2]["row_count"] == 1
        retained_stage, retained_manifest = await archive.prepare_source(session, schema, uuid4())
        assert retained_manifest["include_relationships"] is False
        assert retained_manifest["tables"][2]["row_count"] == 1
        await archive.cleanup_stage(session, retained_stage, receiving=False)

    # A failed pre-publication DDL change must invalidate the old source receipt.
    async with sessions.begin() as session:
        await session.execute(text(f'ALTER TABLE "{schema}".code_catalog ALTER COLUMN source TYPE varchar(130)'))
    with pytest.raises(RuntimeError, match="MS-DRG serving result changed"):
        async with sessions.begin() as session:
            await authority.read_current_generation(session, schema)


@pytest.mark.asyncio
async def test_ms_drg_generation_variants_rollback_and_concurrent_publish(monkeypatch):
    """Exercise native publication, archive rollback, concurrency, and drift checks."""
    engine = create_async_engine(_dsn())
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    schema = "ms_drg_test_" + uuid4().hex[:12]
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    try:
        await _create_source(engine, schema)
        await _source_variants(sessions, schema)
        await _source_stage_validation(sessions, schema)
        destination = await _create_destination(engine)
        await _activation_roundtrip(engine, sessions, schema, destination)
        await _concurrent_publication(sessions, schema)
        await _retained_and_drift(sessions, schema)
    finally:
        async with engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await engine.dispose()
