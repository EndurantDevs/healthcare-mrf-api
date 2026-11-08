# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Scoped code-set activation through complete native family swaps and exact rollback."""

from __future__ import annotations

import os
import re
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.schema import MetaData

from db.models import CodeCatalog
from process import code_sets_result_archive as archive
from process import reference_family_archive as native
from process.scoped_catalog_retention import cleanup_retained_catalog
from tests.scoped_catalog_native_fixture import catalog_actors, seal_catalog


def _copy():
    return native.ReferenceFamilySourceCopy(native.native_copy_projection, 1024**2, 30)


def test_manifest_rejects_non_string_identity_and_timestamp():
    payload = {
        "contract": archive.CONTRACT,
        "origin_lineage_id": str(uuid4()),
        "origin_generation": 1,
        "published_at": "2026-09-23T12:00:00+00:00",
        "row_count": len(archive.SOURCES),
        "row_sha256": "a" * 64,
        "schema_sha256": "b" * 64,
    }
    for field in ("origin_lineage_id", "published_at"):
        with pytest.raises(archive.CodeSetsArchiveError, match="manifest is invalid"):
            archive.validate_manifest({**payload, field: 7})


@pytest.mark.asyncio
async def test_cleanup_rejects_stage_namespace_not_derived_from_dataset():
    source = archive.validate_manifest(
        {
            "contract": archive.CONTRACT,
            "origin_lineage_id": str(uuid4()),
            "origin_generation": 1,
            "published_at": "2026-09-23T12:00:00+00:00",
            "row_count": len(archive.SOURCES),
            "row_sha256": "a" * 64,
            "schema_sha256": "b" * 64,
        }
    )
    stage = archive.CodeSetsStage(uuid4(), "unrelated_schema", 1, 2, source, len(archive.SOURCES), "a" * 64)
    session = SimpleNamespace(in_transaction=lambda: True, execute=AsyncMock())
    with pytest.raises(archive.CodeSetsArchiveError, match="stage identity is invalid"):
        await archive.cleanup_stage(session, stage)
    session.execute.assert_not_awaited()


def _dsn():
    raw = os.getenv("HLTHPRT_CODE_SETS_PUBLICATION_TEST_DSN", "")
    if not raw:
        pytest.skip("HLTHPRT_CODE_SETS_PUBLICATION_TEST_DSN is not set")
    url = make_url(raw)
    if (
        url.drivername != "postgresql"
        or url.username != "postgres"
        or url.host not in {"127.0.0.1", "localhost"}
        or url.port not in {5432, 5440}
    ):
        pytest.fail("code-set archive test requires a dedicated local PostgreSQL database")
    if not re.fullmatch(r"hc_scoped_reference_[0-9a-f]{32}", url.database or ""):
        pytest.fail("code-set archive test requires its dedicated local database")
    return url.set(drivername="postgresql+asyncpg").render_as_string(hide_password=False)


async def _setup(engine, schema, *, appended_attribution=False):
    metadata = MetaData(schema=schema)
    table = CodeCatalog.__table__.to_metadata(metadata, schema=schema)
    if appended_attribution:
        table._columns.remove(table.c.source_attribution)
    async with engine.begin() as connection:
        await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
        await connection.run_sync(metadata.create_all)
        if appended_attribution:
            await connection.execute(text(f'ALTER TABLE "{schema}".code_catalog ADD COLUMN source_attribution TEXT'))
        await connection.execute(
            text(
                f'CREATE TABLE "{schema}".code_sets_result_generation ('
                "id smallint primary key,local_lineage_id uuid not null,local_generation bigint not null,"
                "origin_lineage_id uuid,origin_generation bigint,published_at timestamptz,"
                "code_catalog_oid bigint,row_count bigint,row_sha256 text)"
            )
        )
        await connection.execute(
            text(
                f'INSERT INTO "{schema}".code_sets_result_generation VALUES (1,:lineage,0,NULL,NULL,NULL,NULL,NULL,NULL)'
            ),
            {"lineage": str(uuid4())},
        )


async def _catalog_entries(session, schema):
    return [
        tuple(entry)
        for entry in (
            await session.execute(
                text(f'SELECT code_system,code,source FROM "{schema}".code_catalog ORDER BY code_system,code')
            )
        ).all()
    ]


async def _seed_merge_catalogs(sessions, source, destination):
    async with sessions.begin() as session:
        await session.execute(
            text(
                f'INSERT INTO "{source}".code_catalog (code_system,code,source) VALUES '
                "('POS','23',:pos),('RC','0450',:rc),('MODIFIER','26',:modifier),"
                "('OTHER','1','source-unrelated')"
            ),
            {"pos": archive.SOURCES[0][0], "rc": archive.SOURCES[1][0], "modifier": archive.SOURCES[2][0]},
        )
        await archive.publish_local_generation(session, source)
        await session.execute(
            text(
                f'INSERT INTO "{destination}".code_catalog (code_system,code,source) '
                "VALUES ('OTHER','2','destination-unrelated'),('POS','99',:pos)"
            ),
            {"pos": archive.SOURCES[0][0]},
        )
        await session.execute(
            text(
                f'INSERT INTO "{destination}".code_catalog (code_system,code,source) '
                "VALUES ('RC','0450','foreign-writer')"
            )
        )


async def _assert_conflict_rejection(sessions, source, destination, dataset_id, source_sessions):
    async with source_sessions.begin() as session:
        stage = await archive.prepare_source(session, source, dataset_id, source_copy=_copy())
    async with sessions.begin() as session:
        expected = await archive.read_generation(session, destination)
        with pytest.raises(archive.CodeSetsArchiveError, match="untracked"):
            await archive.prepare_predecessor(
                session, destination=destination, stage=stage, expected=expected, source_copy=_copy()
            )
    async with sessions.begin() as session:
        await session.execute(
            text(f'DELETE FROM "{destination}".code_catalog WHERE source=:pos'), {"pos": archive.SOURCES[0][0]}
        )
        expected = await archive.read_generation(session, destination)
        prepared = await archive.prepare_predecessor(
            session, destination=destination, stage=stage, expected=expected, source_copy=_copy()
        )
        with pytest.raises(archive.CodeSetsArchiveError, match="another source"):
            await archive.activate_stage(session, destination=destination, prepared=prepared, source_copy=_copy())
        assert await _catalog_entries(session, destination) == [
            ("OTHER", "2", "destination-unrelated"),
            ("RC", "0450", "foreign-writer"),
        ]
    async with sessions.begin() as session:
        await session.execute(text(f"DELETE FROM \"{destination}\".code_catalog WHERE source='foreign-writer'"))
    return expected, prepared


async def _assert_insert_failure_rolls_back(sessions, destination, expected, prepared):
    async with sessions.begin() as session:
        await session.execute(
            text(
                f'CREATE FUNCTION "{destination}".reject_candidate() RETURNS trigger LANGUAGE plpgsql AS $$ '
                "BEGIN IF NEW.code_system='POS' AND NEW.code='23' THEN RAISE EXCEPTION 'synthetic insert failure'; "
                "END IF; RETURN NEW; END $$"
            )
        )
        await session.execute(
            text(
                f'CREATE TRIGGER reject_candidate BEFORE INSERT ON "{destination}".code_catalog '
                f'FOR EACH ROW EXECUTE FUNCTION "{destination}".reject_candidate()'
            )
        )
    with pytest.raises(RuntimeError, match="trigger|catalog|custody"):
        async with sessions.begin() as session:
            await archive.activate_stage(session, destination=destination, prepared=prepared, source_copy=_copy())
    async with sessions.begin() as session:
        assert await _catalog_entries(session, destination) == [("OTHER", "2", "destination-unrelated")]
        assert (await archive.read_generation(session, destination)) == expected
        await session.execute(text(f'DROP TRIGGER reject_candidate ON "{destination}".code_catalog'))
        await session.execute(text(f'DROP FUNCTION "{destination}".reject_candidate()'))
    with pytest.raises(RuntimeError, match="synthetic late failure"):
        async with sessions.begin() as session:
            await archive.activate_stage(session, destination=destination, prepared=prepared, source_copy=_copy())
            raise RuntimeError("synthetic late failure")
    async with sessions.begin() as session:
        assert await archive.read_generation(session, destination) == expected
        assert await _catalog_entries(session, destination) == [("OTHER", "2", "destination-unrelated")]


async def _assert_activation_and_rollback(sessions, destination, prepared):
    async with sessions.begin() as session:
        activation = await archive.activate_stage(
            session, destination=destination, prepared=prepared, source_copy=_copy()
        )
        assert await _catalog_entries(session, destination) == [
            ("MODIFIER", "26", archive.SOURCES[2][0]),
            ("OTHER", "2", "destination-unrelated"),
            ("POS", "23", archive.SOURCES[0][0]),
            ("RC", "0450", archive.SOURCES[1][0]),
        ]
    async with sessions.begin() as session:
        await session.execute(
            text(
                f'INSERT INTO "{destination}".code_catalog (code_system,code,source) '
                "VALUES ('OTHER','3','later-unrelated')"
            )
        )
        restored = await archive.rollback_activation(
            session, destination=destination, activation=activation, source_copy=_copy()
        )
        assert restored.local_generation == 2 and restored.origin_generation is None
        assert await _catalog_entries(session, destination) == [
            ("OTHER", "2", "destination-unrelated"),
            ("OTHER", "3", "later-unrelated"),
        ]
        await cleanup_retained_catalog(session, activation.retained_family)
    async with sessions.begin() as session:
        with pytest.raises(archive.CodeSetsArchiveError, match="generation changed"):
            await archive.rollback_activation(
                session, destination=destination, activation=activation, source_copy=_copy()
            )


@pytest.mark.asyncio
async def test_scoped_archive_merge_rollback_and_foreign_conflict():
    """Merge only owned rows, retain unrelated rows, and roll back atomically."""
    engine = create_async_engine(_dsn())
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    token = uuid4().hex[:12]
    source, destination = f"cs_source_{token}", f"cs_target_{token}"
    dataset_id = uuid4()
    owned_schemas = [source, destination, archive.stage_schema(dataset_id)]
    try:
        async with catalog_actors(engine, owned_schemas) as actors:
            await _setup(engine, source)
            await _setup(engine, destination)
            await _seed_merge_catalogs(sessions, source, destination)
            for schema in (source, destination):
                await seal_catalog(actors, schema, (CodeCatalog,), archive.TABLE)
            expected, prepared = await _assert_conflict_rejection(
                actors.publisher, source, destination, dataset_id, actors.builder
            )
            await _assert_insert_failure_rolls_back(actors.publisher, destination, expected, prepared)
            await _assert_activation_and_rollback(actors.publisher, destination, prepared)
    finally:
        await engine.dispose()


async def _seed_tracked_catalogs(sessions, source, destination):
    for schema, pos_code in ((source, "23"), (destination, "99")):
        await _seed_code_generation(sessions, schema, pos_code)


async def _seed_code_generation(sessions, schema, pos_code):
    async with sessions.begin() as session:
        await session.execute(
            text(
                f'INSERT INTO "{schema}".code_catalog (code_system,code,source) VALUES '
                "(:system,:pos,:pos_source),('RC','0450',:rc),('MODIFIER','26',:modifier)"
            ),
            {
                "system": "POS",
                "pos": pos_code,
                "pos_source": archive.SOURCES[0][0],
                "rc": archive.SOURCES[1][0],
                "modifier": archive.SOURCES[2][0],
            },
        )
        await archive.publish_local_generation(session, schema)


async def _prepare_tracked_predecessor(sessions, source, destination, dataset_id):
    async with sessions.begin() as session:
        stage = await archive.prepare_source(session, source, dataset_id, source_copy=_copy())
    async with sessions.begin() as session:
        expected = await archive.read_generation(session, destination)
        prepared = await archive.prepare_predecessor(
            session, destination=destination, stage=stage, expected=expected, source_copy=_copy()
        )
    return stage, expected, prepared


async def _assert_foreign_predecessors_rejected(sessions, destination, stage, prepared):
    async with sessions.begin() as session:
        await session.execute(
            text(
                f'INSERT INTO "{stage.schema_name}".code_catalog_predecessor '
                "(code_system,code,source) VALUES ('OTHER','7','foreign-writer')"
            )
        )
        with pytest.raises(archive.CodeSetsArchiveError, match="contains foreign rows"):
            await archive.activate_stage(session, destination=destination, prepared=prepared, source_copy=_copy())
        await session.execute(
            text(f"DELETE FROM \"{stage.schema_name}\".code_catalog_predecessor WHERE code_system='OTHER'")
        )
        await session.execute(
            text(
                f'INSERT INTO "{stage.schema_name}".code_catalog_predecessor '
                "(code_system,code,source) VALUES ('POS','null-source',NULL)"
            )
        )
        with pytest.raises(archive.CodeSetsArchiveError, match="contains foreign rows"):
            await archive.activate_stage(session, destination=destination, prepared=prepared, source_copy=_copy())
        await session.execute(
            text(f"DELETE FROM \"{stage.schema_name}\".code_catalog_predecessor WHERE code='null-source'")
        )


async def _assert_drift_rejected_and_restored(sessions, destination, stage, expected, prepared):
    async with sessions.begin() as session:
        await session.execute(
            text(f"UPDATE \"{destination}\".code_catalog SET display_name='untracked edit' WHERE code='99'")
        )
        with pytest.raises(archive.CodeSetsArchiveError, match="source rows changed"):
            await archive.activate_stage(session, destination=destination, prepared=prepared, source_copy=_copy())
    async with sessions.begin() as session:
        await session.execute(text(f"UPDATE \"{destination}\".code_catalog SET display_name=NULL WHERE code='99'"))
        activation = await archive.activate_stage(
            session, destination=destination, prepared=prepared, source_copy=_copy()
        )
        assert (
            await archive.read_generation(session, destination)
        ).origin_lineage_id == stage.source_generation.origin_lineage_id
    async with sessions.begin() as session:
        await session.execute(
            text(f"UPDATE \"{destination}\".code_catalog SET display_name='untracked edit' WHERE code='23'")
        )
        with pytest.raises(archive.CodeSetsArchiveError, match="source rows changed"):
            await archive.rollback_activation(
                session, destination=destination, activation=activation, source_copy=_copy()
            )
    async with sessions.begin() as session:
        await session.execute(text(f"UPDATE \"{destination}\".code_catalog SET display_name=NULL WHERE code='23'"))
        restored = await archive.rollback_activation(
            session, destination=destination, activation=activation, source_copy=_copy()
        )
        assert restored.origin_lineage_id == expected.origin_lineage_id
        assert restored.origin_generation == expected.origin_generation
        assert await _catalog_entries(session, destination) == [
            ("MODIFIER", "26", archive.SOURCES[2][0]),
            ("POS", "99", archive.SOURCES[0][0]),
            ("RC", "0450", archive.SOURCES[1][0]),
        ]
        await cleanup_retained_catalog(session, activation.retained_family)


@pytest.mark.asyncio
async def test_tracked_predecessor_and_drifted_source_fail_closed():
    """Reject foreign predecessors and drift before activation or rollback."""
    engine = create_async_engine(_dsn())
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    token = uuid4().hex[:12]
    source, destination = f"cs_source_{token}", f"cs_target_{token}"
    dataset_id = uuid4()
    owned_schemas = [source, destination, archive.stage_schema(dataset_id)]
    try:
        async with catalog_actors(engine, owned_schemas) as actors:
            await _setup(engine, source)
            await _setup(engine, destination)
            await _seed_tracked_catalogs(sessions, source, destination)
            for schema in (source, destination):
                await seal_catalog(actors, schema, (CodeCatalog,), archive.TABLE)
            stage, expected, prepared = await _prepare_tracked_predecessor(
                actors.publisher, source, destination, dataset_id
            )
            await _assert_foreign_predecessors_rejected(actors.publisher, destination, stage, prepared)
            await _assert_drift_rejected_and_restored(actors.publisher, destination, stage, expected, prepared)
    finally:
        await engine.dispose()


async def _restored_preparation(sessions, destination, original, receive_id):
    manifest = original.source_generation.as_dict()
    assert archive.validate_manifest(manifest) == original.source_generation
    async with sessions.begin() as session:
        received, predecessor_oid = await archive.precreate_restore(
            session, destination=destination, dataset_id=receive_id, manifest=manifest
        )
        await session.execute(
            text(
                f'INSERT INTO "{received.schema_name}".code_catalog SELECT * FROM "{original.schema_name}".code_catalog'
            )
        )
    async with sessions.begin() as session:
        await archive.complete_restore(session, received, predecessor_oid=predecessor_oid)
        expected = await archive.read_generation(session, destination)
        await archive.prepare_predecessor(
            session,
            destination=destination,
            stage=received,
            expected=expected,
            precreated_predecessor_oid=predecessor_oid,
            source_copy=_copy(),
        )
    async with sessions.begin() as session:
        prepared = await archive.validate_prepared_stage(
            session, destination=destination, stage=received, predecessor_oid=predecessor_oid
        )
        assert prepared.expected == expected
    return prepared


@pytest.mark.asyncio
async def test_precreated_two_relation_stage_validates_after_restore():
    """Authenticate the registered frozen restore before a genuine publisher swaps it."""
    engine = create_async_engine(_dsn())
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    token = uuid4().hex[:12]
    source_schema, destination = f"cs_source_{token}", f"cs_target_{token}"
    source_id, receive_id = uuid4(), uuid4()
    owned_schemas = [source_schema, destination, archive.stage_schema(source_id), archive.stage_schema(receive_id)]
    try:
        async with catalog_actors(engine, owned_schemas) as actors:
            await _setup(engine, source_schema)
            await _setup(engine, destination)
            await _seed_code_generation(sessions, source_schema, "23")
            for schema in (source_schema, destination):
                await seal_catalog(actors, schema, (CodeCatalog,), archive.TABLE)
            async with actors.builder.begin() as session:
                original = await archive.prepare_source(session, source_schema, source_id, source_copy=_copy())
            prepared = await _restored_preparation(actors.publisher, destination, original, receive_id)
            async with actors.publisher.begin() as session:
                activation = await archive.activate_stage(
                    session, destination=destination, prepared=prepared, source_copy=_copy()
                )
                assert activation.predecessor_catalog_oid == prepared.predecessor_catalog_oid
                assert await _catalog_entries(session, destination) == [
                    ("MODIFIER", "26", archive.SOURCES[2][0]),
                    ("POS", "23", archive.SOURCES[0][0]),
                    ("RC", "0450", archive.SOURCES[1][0]),
                ]
                await cleanup_retained_catalog(session, activation.retained_family)
    finally:
        await engine.dispose()
