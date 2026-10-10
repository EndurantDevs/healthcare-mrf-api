# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Opt-in native publication from the supported ALTER-appended catalog layout."""

from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from db.models import CodeCatalog
from process import code_sets_result_archive as archive
from process import ms_drg_result_generation as drg
from process.scoped_catalog_retention import cleanup_retained_catalog
from tests.scoped_catalog_native_fixture import catalog_actors, seal_catalog
from tests.test_code_sets_result_archive_postgres import (
    _copy,
    _dsn,
    _prepare_tracked_predecessor,
    _seed_code_generation,
    _setup,
)


async def _column_names(session, oid):
    return tuple(
        (
            await session.execute(
                text(
                    "SELECT attname FROM pg_attribute WHERE attrelid=:oid AND attnum>0 AND NOT attisdropped ORDER BY attnum"
                ),
                {"oid": oid},
            )
        ).scalars()
    )


async def _rows(session, schema):
    return list(
        (
            await session.execute(
                text(f'SELECT to_jsonb(row_value) FROM "{schema}".code_catalog row_value ORDER BY code_system,code')
            )
        ).scalars()
    )


async def _seed_catalogs(sessions, source, destination):
    for schema, code in ((source, "23"), (destination, "99")):
        await _seed_code_generation(sessions, schema, code)
        async with sessions.begin() as session:
            await session.execute(
                text(
                    f"INSERT INTO \"{schema}\".code_catalog (code_system,code,source) VALUES ('OTHER',:code,'unrelated')"
                ),
                {"code": code},
            )
            await session.execute(
                text(
                    f'UPDATE "{schema}".code_catalog SET source_attribution=source||:attribution,'
                    "updated_at=TIMESTAMP '2026-09-23 12:34:56'"
                ),
                {"attribution": ":synthetic attribution"},
            )
            await archive.publish_local_generation(session, schema)


async def _reject_source_drift(sessions, schema, dataset_id):
    for change in (
        "ALTER COLUMN source_attribution TYPE varchar(256)",
        "DROP CONSTRAINT code_catalog_pkey, ADD PRIMARY KEY (code,code_system)",
        "ADD COLUMN unexpected text",
    ):
        with pytest.raises(RuntimeError, match="columns differ|key differs"):
            async with sessions.begin() as session:
                await session.execute(text(f'ALTER TABLE "{schema}".code_catalog {change}'))
                await archive.prepare_source(session, schema, dataset_id, source_copy=_copy())
        async with sessions.begin() as session:
            assert (
                await session.scalar(
                    text("SELECT to_regnamespace(:schema)"), {"schema": archive.stage_schema(dataset_id)}
                )
                is None
            )


async def _reject_publication_drift(sessions, destination, prepared):
    statements = (
        f"ALTER TABLE \"{destination}\".code_catalog ADD CONSTRAINT attribution_guard CHECK (source_attribution <> '')",
        f'CREATE INDEX attribution_idx ON "{destination}".code_catalog (source_attribution) INCLUDE (updated_at)',
    )
    for statement in statements:
        with pytest.raises(RuntimeError, match="constraints differ|index is unavailable"):
            async with sessions.begin() as session:
                await session.execute(text(statement))
                await archive.activate_stage(session, destination=destination, prepared=prepared, source_copy=_copy())
        async with sessions.begin() as session:
            assert await archive.read_generation(session, destination) == prepared.expected


async def _assert_roundtrip(sessions, source_schema, destination, stage, expected, prepared):
    model_names = tuple(CodeCatalog.__table__.columns.keys())
    legacy_names = tuple(name for name in model_names if name != "source_attribution") + ("source_attribution",)
    async with sessions.begin() as session:
        prior_rows = await _rows(session, destination)
        source_rows = await _rows(session, source_schema)
        assert await _column_names(session, expected.code_catalog_oid) == legacy_names
        assert await _column_names(session, stage.catalog_oid) == model_names
        assert await archive._column_signature(session, expected.code_catalog_oid) == await archive._column_signature(
            session, stage.catalog_oid
        )
        assert await drg._table_shape(session, expected.code_catalog_oid, CodeCatalog) == await drg._table_shape(
            session, stage.catalog_oid, CodeCatalog
        )
        activation = await archive.activate_stage(
            session, destination=destination, prepared=prepared, source_copy=_copy()
        )
        assert activation.current.code_catalog_oid != expected.code_catalog_oid
        assert activation.current.origin_lineage_id == stage.source_generation.origin_lineage_id
        assert activation.current.row_sha256 == stage.source_generation.row_sha256
        combined_rows = [catalog_row for catalog_row in source_rows if catalog_row["source"] != "unrelated"]
        combined_rows.extend(catalog_row for catalog_row in prior_rows if catalog_row["source"] == "unrelated")
        assert await _rows(session, destination) == sorted(
            combined_rows, key=lambda catalog_row: (catalog_row["code_system"], catalog_row["code"])
        )
        assert await _rows(session, activation.retained_family["schema_name"]) == prior_rows
        assert await _column_names(session, expected.code_catalog_oid) == legacy_names
    async with sessions.begin() as session:
        restored = await archive.rollback_activation(
            session, destination=destination, activation=activation, source_copy=_copy()
        )
        assert restored.local_generation == expected.local_generation + 2
        assert restored.origin_lineage_id == expected.origin_lineage_id
        assert restored.origin_generation == expected.origin_generation
        assert (restored.row_count, restored.row_sha256) == (expected.row_count, expected.row_sha256)
        assert await _rows(session, destination) == prior_rows
        assert await _rows(session, source_schema) == source_rows
        await cleanup_retained_catalog(session, activation.retained_family)


@pytest.mark.asyncio
async def test_appended_attribution_copy_publication_rollback_and_schema_drift():
    """Real COPY and protected-principal publication accept order only, never schema drift."""
    engine = create_async_engine(_dsn())
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    token = uuid4().hex[:12]
    source, destination = f"cs_source_{token}", f"cs_target_{token}"
    dataset_id = uuid4()
    owned_schemas = [source, destination, archive.stage_schema(dataset_id)]
    try:
        async with catalog_actors(engine, owned_schemas) as actors:
            for schema in (source, destination):
                await _setup(engine, schema, appended_attribution=True)
            await _seed_catalogs(sessions, source, destination)
            for schema in (source, destination):
                await seal_catalog(actors, schema, (CodeCatalog,), archive.TABLE)
            await _reject_source_drift(actors.publisher, source, dataset_id)
            stage, expected, prepared = await _prepare_tracked_predecessor(
                actors.publisher, source, destination, dataset_id
            )
            await _reject_publication_drift(actors.publisher, destination, prepared)
            await _assert_roundtrip(actors.publisher, source, destination, stage, expected, prepared)
    finally:
        await engine.dispose()
