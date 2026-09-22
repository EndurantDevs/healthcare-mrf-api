"""Optional paired Label runtime: actual dump/restore and atomic generation cutover."""

import asyncio
import importlib.util
import os
import re
import subprocess
import sys
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import MetaData, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from process import reference_family_archive as archive


def _url():
    raw = os.getenv("LABEL_ARCHIVE_TEST_DSN")
    if not raw:
        pytest.skip("LABEL_ARCHIVE_TEST_DSN is required")
    url = make_url(raw)
    if (
        url.host != "127.0.0.1"
        or url.port != 5440
        or not re.fullmatch(r"label_archive_test_[0-9a-f]{32}", url.database)
    ):
        pytest.fail("Label checks require a UUID-owned local test database on port 5440")
    return url


async def _install(session, schema, monkeypatch, publication, *, create_family=True):
    if create_family:
        await archive._create_model_family(session, archive.reference_family_spec("label"), schema)
    else:
        await session.execute(text(f'CREATE SCHEMA "{schema}"'))
    path = (
        Path(publication.__file__).resolve().parents[2]
        / "alembic/versions/202609210001_add_result_publication_authority.py"
    )
    spec = importlib.util.spec_from_file_location("label_test_migration", path)
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    monkeypatch.setenv("DB_SCHEMA", schema)

    def upgrade(connection):
        migration.op = Operations(MigrationContext.configure(connection))
        migration.upgrade()

    await (await session.connection()).run_sync(upgrade)


async def _ordinary_publish(session, source_schema, publication, monkeypatch):
    """Run the actual producer DDL against its canonical dated staging table."""
    from drug_snapshot_runtime.label import Label

    stage = Label.__table__.to_metadata(MetaData(), name="label_20260923", schema=source_schema)
    async with session.begin():
        await (await session.connection()).run_sync(stage.create)
        await session.execute(
            stage.insert().values(id="source", product_ndc=["00001-0001"], openfda={"brand_name": ["Example"]})
        )
        stage_oid = await session.scalar(text(f"SELECT '{source_schema}.label_20260923'::regclass::oid"))

    path = Path(publication.__file__).resolve().parents[2] / "process/label_publish.py"
    spec = importlib.util.spec_from_file_location("paired_label_publish", path)
    producer = importlib.util.module_from_spec(spec)
    # The paired repositories have distinct process packages. Bind only the
    # producer's existing compatibility import to its exact canonical module.
    with monkeypatch.context() as patch:
        patch.setitem(sys.modules, "process.result_publication_authority", publication)
        spec.loader.exec_module(producer)

    async def status(statement):
        return await session.execute(text(statement))

    database = SimpleNamespace(transaction=session.begin, status=status, execute=session.execute)
    await producer.publish_label_table(database, source_schema, "20260923")
    async with session.begin():
        authority = await publication.read_result_publication_authority(
            session, importer_id="label", schema=source_schema
        )
        assert authority.local_generation == 1
        assert authority.serving_generation.origin_lineage_id == authority.local_lineage_id
        assert authority.relation_oids == (stage_oid,)
        await _assert_label_indexes(session, source_schema)
        return authority


async def _assert_label_indexes(session, schema):
    """The ordinary producer and restored destination have the same four indexes."""
    indexes = await session.execute(
        text(
            "SELECT am.amname,pg_get_indexdef(i.indexrelid,1,true) FROM pg_index i "
            "JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_am am ON am.oid=c.relam "
            "WHERE i.indrelid=to_regclass(:relation) AND NOT i.indisprimary "
            "AND i.indisvalid AND i.indisready"
        ),
        {"relation": schema + ".label"},
    )
    assert sorted(indexes.tuples().all()) == [
        ("btree", "id"),
        ("btree", "set_id"),
        ("gin", "package_ndc"),
        ("gin", "product_ndc"),
    ]


async def _seed(sessions, source_schema, target_schema, monkeypatch, publication):
    """Install the real ledger and independent source/destination publications."""
    async with sessions() as session, session.begin():
        await _install(session, source_schema, monkeypatch, publication, create_family=False)
        await _install(session, target_schema, monkeypatch, publication)
        await session.execute(text(f"INSERT INTO {target_schema}.label (id) VALUES ('incumbent')"))
        original = await publication.publish_local_result_generation(
            session, importer_id="label", schema=target_schema, consumed_dependencies={}
        )

    async with sessions() as session:
        source_authority = await _ordinary_publish(session, source_schema, publication, monkeypatch)

    return source_authority, original


async def _copy_archive(sessions, url, prepared, tmp_path):
    """Dump a pinned clone, then data-restore into reviewed model tables."""
    source_stage = prepared.ownership.schema_name
    identity = prepared.ownership.dataset_id
    dump = tmp_path / "label.dump"
    pg_bin = Path(os.getenv("POSTGRES_BIN", "/usr/bin"))

    async def dump_archive(capture):
        await asyncio.to_thread(
            subprocess.run,
            [
                str(pg_bin / "pg_dump"),
                "--format=custom",
                "--no-owner",
                "--no-privileges",
                "--schema=" + source_stage,
                "--snapshot=" + capture.postgres_snapshot,
                "--file=" + str(dump),
                url.set(drivername="postgresql").render_as_string(hide_password=False),
            ],
            check=True,
            capture_output=True,
        )

    await archive.export_prepared_reference_family_archive(sessions, prepared=prepared, archive_copy=dump_archive)
    async with sessions() as session, session.begin():
        await archive.cleanup_reference_family_stage(session, prepared.ownership)
        ownership = await archive.precreate_reference_family_restore(session, importer_id="label", dataset_id=identity)
    await asyncio.to_thread(
        subprocess.run,
        [
            str(pg_bin / "pg_restore"),
            "--data-only",
            "--no-owner",
            "--no-privileges",
            "--exit-on-error",
            "--dbname=" + url.set(drivername="postgresql").render_as_string(hide_password=False),
            str(dump),
        ],
        check=True,
        capture_output=True,
    )
    return ownership


async def _cutover(sessions, ownership, prepared, target_schema, source_authority):
    """Reject unindexed data, then create the publisher's exact cutover receipt."""
    package = "b" * 64
    async with sessions() as session, session.begin():
        with pytest.raises(archive.ReferenceFamilyArchiveError):
            await archive.validate_reference_family_stage(session, ownership=ownership, manifest=prepared.manifest)
        await archive.complete_reference_family_restore(session, ownership)
        role_oid = await session.scalar(text("SELECT oid FROM pg_roles WHERE rolname=current_user"))
        incumbent = await archive.capture_reference_family_incumbent(
            session, importer_id="label", schema_name=target_schema
        )
        validation = await archive.prepare_reference_family_activation(
            session,
            ownership=ownership,
            manifest=prepared.manifest,
            package_id=package,
            profile_contract=archive.CONTRACT,
            sealed_owner_oid=role_oid,
        )
    cutover = archive.ReferenceFamilyCutoverAuthority(
        package, archive.CONTRACT, role_oid, role_oid, "manual", source_authority.serving_generation.as_dict()
    )

    async def activate(session, authority=cutover):
        return await archive.activate_validated_reference_family_stage(
            session,
            ownership=ownership,
            manifest=prepared.manifest,
            expected_incumbent=incumbent,
            validation_receipt=validation,
            cutover=authority,
        )

    return activate, cutover


async def _check_activation(
    sessions, activate, cutover, ownership, publication, original, target_schema, source_authority
):
    """Failed and committed cutovers retain exact native generation identity."""
    async with sessions() as session:
        transaction = await session.begin()
        await activate(session)
        adopted = await publication.read_result_publication_authority(
            session, importer_id="label", schema=target_schema
        )
        assert adopted.local_generation == original.local_generation + 1
        assert adopted.serving_generation == source_authority.serving_generation
        assert adopted.relation_oids == tuple(oid for _, oid in ownership.relation_oids)
        assert adopted.consumed_dependencies == {}
        await transaction.rollback()
    async with sessions() as session, session.begin():
        assert (
            await publication.read_result_publication_authority(session, importer_id="label", schema=target_schema)
            == original
        )
        automatic = replace(cutover, authority="automatic")
        with pytest.raises(archive.ReferenceFamilyArchiveError, match="stale or unrelated"):
            await activate(session, automatic)
        receipt = await activate(session)
        await _assert_label_indexes(session, target_schema)
        assert await session.scalar(text(f"SELECT id FROM {target_schema}.label")) == "source"
        assert await session.scalar(text(f'SELECT id FROM "{receipt.predecessor_schema_name}".label')) == "incumbent"
    with pytest.raises(DBAPIError):
        async with sessions() as session, session.begin():
            await activate(session)
    async with sessions() as session, session.begin():
        next_local = await publication.publish_local_result_generation(
            session, importer_id="label", schema=target_schema, consumed_dependencies={}
        )
        assert next_local.local_generation == original.local_generation + 2
        assert next_local.serving_generation.origin_lineage_id == original.local_lineage_id


@pytest.mark.asyncio
async def test_label_dump_restore_generation_cas_and_transaction_rollback(tmp_path, monkeypatch):
    """Roundtrip real PostgreSQL data while preserving CAS and rollback authority."""
    publication = pytest.importorskip("drug_snapshot_runtime.publication")
    url = _url()
    engine = create_async_engine(url.set(drivername="postgresql+asyncpg"))
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    identity = uuid4()
    source_schema, target_schema = "label_source", "label_target"
    source_stage = archive.reference_family_stage_schema(identity)
    try:
        source_authority, original = await _seed(sessions, source_schema, target_schema, monkeypatch, publication)

        async def retained(_session, _prepared):
            assert _prepared.manifest.importer_id == "label"

        prepared = await archive.prepare_reference_family_archive_source(
            sessions,
            importer_id="label",
            schema_name=source_schema,
            source_metadata={"release": "synthetic"},
            dataset_id=identity,
            on_prepared=retained,
        )
        assert prepared.manifest.dependencies == {}
        assert prepared.manifest.source_serving_generation.as_dict() == source_authority.serving_generation.as_dict()
        generationless = prepared.manifest.as_dict()
        generationless["publication_authority"] = "manual-only"
        generationless.pop("source_serving_generation")
        with pytest.raises(archive.ReferenceFamilyArchiveError, match="label source generation is required"):
            archive.validate_reference_family_manifest(generationless)

        ownership = await _copy_archive(sessions, url, prepared, tmp_path)
        activate, cutover = await _cutover(sessions, ownership, prepared, target_schema, source_authority)
        await _check_activation(
            sessions, activate, cutover, ownership, publication, original, target_schema, source_authority
        )
    finally:
        async with engine.begin() as connection:
            for schema in (
                source_stage,
                archive.reference_family_predecessor_schema(identity),
                source_schema,
                target_schema,
            ):
                await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await engine.dispose()
