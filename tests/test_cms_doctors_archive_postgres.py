# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native clinician-family publication authority and archive proof."""

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

from process import reference_family_archive as archive
from process import reference_family_result_generation as generation

_CMS_REVISION = "20260920100000_cms_doctors_result_generation"


def _database_url():
    raw = os.getenv("HLTHPRT_CMS_DOCTORS_ARCHIVE_TEST_DSN")
    if not raw:
        pytest.skip("set HLTHPRT_CMS_DOCTORS_ARCHIVE_TEST_DSN for the PostgreSQL proof")
    url = make_url(raw)
    if url.host not in {"localhost", "127.0.0.1", "postgres"} or not re.fullmatch(
        r"cms_archive_test_[0-9a-f]{32}", url.database or ""
    ):
        pytest.fail("clinician archive proof requires a UUID-owned local test database")
    return url.set(drivername="postgresql+asyncpg")


async def _migration(session, revision, action):
    path = Path(__file__).resolve().parents[1] / "alembic" / "versions" / f"{revision}.py"
    spec = importlib.util.spec_from_file_location("clinician_generation_migration", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    def apply(connection):
        module.op = Operations(MigrationContext.configure(connection))
        getattr(module, action)()

    connection = await session.connection()
    await connection.run_sync(apply)


async def _create_source(session, schema):
    await archive._create_model_family(session, archive.reference_family_spec("cms-doctors"), schema)
    await _migration(session, "20260914110000_reference_family_result_generation", "upgrade")
    await _migration(session, "20260914130000_mrf_result_generation", "upgrade")
    ledger = f'"{schema}".reference_family_result_generation'
    before = (await session.execute(text(f"SELECT * FROM {ledger} ORDER BY importer_id"))).all()
    await _migration(session, _CMS_REVISION, "upgrade")
    after = (
        await session.execute(text(f"SELECT * FROM {ledger} WHERE importer_id <> 'cms-doctors' ORDER BY importer_id"))
    ).all()
    assert before == after
    authority = await generation.read_reference_family_result_generation_authority(
        session,
        importer_id="cms-doctors",
        schema_name=schema,
    )
    assert authority.local_generation == 0 and authority.serving_generation is None
    with pytest.raises(RuntimeError, match="unavailable or drifted"):
        await generation.capture_reference_family_serving_generation(
            session, importer_id="cms-doctors", schema_name=schema
        )
    await _migration(session, _CMS_REVISION, "downgrade")
    await _migration(session, _CMS_REVISION, "upgrade")
    await session.execute(
        text(
            f"INSERT INTO \"{schema}\".doctor_clinician_address (npi, address_checksum, city) VALUES (1000000004, 1, 'Example')"
        )
    )
    await session.execute(
        text(
            f'INSERT INTO "{schema}".cms_doctor_education '
            "(npi, education_key, medical_school, generation_id, source_json, imported_at) "
            "VALUES (1000000004, 'assertion', 'Example School', 'source-digest', '{}', CURRENT_TIMESTAMP)"
        )
    )
    return await generation.publish_local_reference_family_generation(
        session, importer_id="cms-doctors", schema_name=schema
    )


async def _command(*args):
    process = await asyncio.create_subprocess_exec(
        *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=30)
    except BaseException:
        if process.returncode is None:
            process.kill()
        await process.wait()
        raise
    assert process.returncode == 0, stderr.decode()
    return stdout.decode()


async def _roundtrip(sessions, prepared, url, path):
    async def dump(capture):
        await _command(
            "pg_dump",
            "--dbname",
            url.set(drivername="postgresql").render_as_string(hide_password=False),
            "--format=custom",
            "--no-owner",
            "--no-acl",
            "--schema",
            capture.ownership.schema_name,
            "--snapshot",
            capture.postgres_snapshot,
            "--file",
            str(path),
        )

    await archive.export_prepared_reference_family_archive(sessions, prepared=prepared, archive_copy=dump)
    listing = await _command("pg_restore", "--list", str(path))
    assert "doctor_clinician_address" in listing and "cms_doctor_education" in listing
    assert "address_archive" not in listing
    async with sessions() as session, session.begin():
        await archive.cleanup_reference_family_stage(session, prepared.ownership)
        restored = await archive.precreate_reference_family_restore(
            session,
            importer_id="cms-doctors",
            dataset_id=prepared.ownership.dataset_id,
        )
    await _command(
        "pg_restore",
        "--dbname",
        url.set(drivername="postgresql").render_as_string(hide_password=False),
        "--data-only",
        "--no-owner",
        "--no-acl",
        "--exit-on-error",
        "--single-transaction",
        str(path),
    )
    async with sessions() as session, session.begin():
        await archive.complete_reference_family_restore(session, restored)
        await archive.validate_reference_family_stage(session, ownership=restored, manifest=prepared.manifest.as_dict())
        assert (
            await session.scalar(text(f'SELECT city FROM "{restored.schema_name}".doctor_clinician_address'))
            == "Example"
        )
        assert (
            await session.scalar(text(f'SELECT generation_id FROM "{restored.schema_name}".cms_doctor_education'))
            == "source-digest"
        )
    return restored


@pytest.mark.asyncio
async def test_clinician_migration_and_native_archive_are_one_closed_generation(monkeypatch, tmp_path):
    url = _database_url()
    schema = "cms_source_" + uuid4().hex
    dataset_id = uuid4()
    stage_schema = archive.reference_family_stage_schema(dataset_id)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    engine = create_async_engine(url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with sessions() as session, session.begin():
            authority = await _create_source(session, schema)
        async with sessions() as session, session.begin():
            with pytest.raises(RuntimeError, match="evidence prevents downgrade"):
                await _migration(session, _CMS_REVISION, "downgrade")

        async def metadata(session):
            serving = await generation.capture_reference_family_serving_generation(
                session,
                importer_id="cms-doctors",
                schema_name=schema,
            )
            assert serving == authority.serving_generation
            return {"serving_generation": serving.as_dict()}

        async def retain(_session, prepared):
            assert prepared.ownership.importer_id == "cms-doctors"

        prepared = await archive.prepare_reference_family_archive_source(
            sessions,
            importer_id="cms-doctors",
            schema_name=schema,
            source_metadata=None,
            dataset_id=dataset_id,
            source_metadata_factory=metadata,
            on_prepared=retain,
        )
        assert tuple(pair[0] for pair in prepared.ownership.relation_oids) == (
            "cms_doctor_education",
            "doctor_clinician_address",
        )
        await _roundtrip(sessions, prepared, url, tmp_path / "clinician.dump")
    finally:
        async with engine.begin() as connection:
            for owned_schema in (stage_schema, schema):
                await connection.execute(text(f'DROP SCHEMA IF EXISTS "{owned_schema}" CASCADE'))
                assert (
                    await connection.scalar(text("SELECT to_regnamespace(:schema)"), {"schema": owned_schema}) is None
                )
        await engine.dispose()
