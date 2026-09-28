# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native clinician-family publication authority and archive proof."""

import asyncio
import hashlib
import importlib.util
import json
import os
import re
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from db import models
from process import reference_family_archive as archive
from process import reference_family_result_generation as generation

_CMS_REVISION = "20260920100000_cms_doctors_result_generation"
_GROUP_REVISION = "20260929000000_cms_doctor_group_site"


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
    legacy_spec = archive.ReferenceFamilySpec(
        "cms-doctors",
        (models.DoctorClinicianAddress, models.CMSDoctorEducation),
    )
    await archive._create_model_family(session, legacy_spec, schema)
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
    with pytest.raises(RuntimeError, match="unavailable"):
        await generation.capture_reference_family_serving_generation(
            session, importer_id="cms-doctors", schema_name=schema
        )
    await _migration(session, _CMS_REVISION, "downgrade")
    await _migration(session, _CMS_REVISION, "upgrade")
    await _migration(session, _GROUP_REVISION, "upgrade")
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
    await session.execute(
        text(
            f'INSERT INTO "{schema}".cms_doctor_group_site '
            "(row_number,npi,org_pac_id,adrs_id,generation_id,source_json,observed_at) "
            "VALUES (1,1000000004,'0012345678','site-1','source-digest','{}',CURRENT_TIMESTAMP)"
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


async def _restore_native_dump(url, path):
    """Load only native archive data into the already owned stage, atomically."""
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


async def _roundtrip(sessions, prepared, url, path):
    """Export and restore the complete current family, then verify its rows."""

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
    assert all(
        name in listing
        for name in (
            "doctor_clinician_address",
            "cms_doctor_education",
            "cms_doctor_group_site",
        )
    )
    assert "address_archive" not in listing
    async with sessions() as session, session.begin():
        await archive.cleanup_reference_family_stage(session, prepared.ownership)
        restored = await archive.precreate_reference_family_restore(
            session,
            importer_id="cms-doctors",
            dataset_id=prepared.ownership.dataset_id,
        )
    await _restore_native_dump(url, path)
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
        assert (
            await session.scalar(text(f'SELECT org_pac_id FROM "{restored.schema_name}".cms_doctor_group_site'))
            == "0012345678"
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
            "cms_doctor_group_site",
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


@pytest.mark.asyncio
async def test_group_migration_advances_existing_two_relation_generation(monkeypatch):
    """An existing publication gains the new empty relation under a new local identity."""
    url = _database_url()
    schema = "cms_source_" + uuid4().hex
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    engine = create_async_engine(url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with sessions() as session, session.begin():
            legacy_spec = archive.ReferenceFamilySpec(
                "cms-doctors",
                (models.DoctorClinicianAddress, models.CMSDoctorEducation),
            )
            await archive._create_model_family(session, legacy_spec, schema)
            for revision in (
                "20260914110000_reference_family_result_generation",
                "20260914130000_mrf_result_generation",
                _CMS_REVISION,
            ):
                await _migration(session, revision, "upgrade")
            await session.execute(
                text(
                    f'UPDATE "{schema}".reference_family_result_generation '
                    "SET local_generation=1,origin_lineage_id=local_lineage_id,origin_generation=1,"
                    "published_at=transaction_timestamp(),relation_oids=ARRAY["
                    f"'\"{schema}\".doctor_clinician_address'::regclass::oid::bigint,"
                    f"'\"{schema}\".cms_doctor_education'::regclass::oid::bigint] "
                    "WHERE importer_id='cms-doctors'"
                )
            )
            await _migration(session, _GROUP_REVISION, "upgrade")
            authority = await generation.read_reference_family_result_generation_authority(
                session,
                importer_id="cms-doctors",
                schema_name=schema,
            )
            assert authority.local_generation == 2
            assert authority.serving_generation.origin_generation == 2
            assert authority.serving_generation.origin_lineage_id == authority.local_lineage_id
            assert len(authority.relation_oids) == 3
            assert authority.relation_oids[2] == await session.scalar(
                text(f"SELECT '\"{schema}\".cms_doctor_group_site'::regclass::oid::bigint")
            )
            with pytest.raises(RuntimeError, match="evidence prevents downgrade"):
                await _migration(session, _GROUP_REVISION, "downgrade")
    finally:
        async with engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
            assert await connection.scalar(text("SELECT to_regnamespace(:schema)"), {"schema": schema}) is None
        await engine.dispose()


async def _export_legacy_package(sessions, schema, dataset_id, url, tmp_path):
    """Build a synthetic historical package and retain its exact received bytes."""
    stage_schema = archive.reference_family_stage_schema(dataset_id)
    legacy_spec = archive.ReferenceFamilySpec("cms-doctors", (models.DoctorClinicianAddress, models.CMSDoctorEducation))
    async with sessions() as session, session.begin():
        authority = await _create_source(session, schema)
        await archive._create_model_family(session, legacy_spec, stage_schema)
        for table_name in legacy_spec.table_names:
            await session.execute(
                text(f'INSERT INTO "{stage_schema}".{table_name} SELECT * FROM "{schema}".{table_name}')
            )
        manifest = await archive._family_manifest(
            session,
            spec=legacy_spec,
            schema_name=stage_schema,
            source_metadata={"release": "synthetic-legacy"},
            source_serving_generation=authority.serving_generation,
        )
    manifest_bytes = json.dumps(manifest.as_dict(), indent=2).encode()
    manifest_path = tmp_path / "legacy.json"
    manifest_path.write_bytes(manifest_bytes)
    assert archive.validate_reference_family_manifest(json.loads(manifest_bytes)) == manifest
    dump_path = tmp_path / "legacy.dump"
    await _command(
        "pg_dump",
        "--dbname",
        url.set(drivername="postgresql").render_as_string(hide_password=False),
        "--format=custom",
        "--no-owner",
        "--no-acl",
        "--schema",
        stage_schema,
        "--file",
        str(dump_path),
    )
    listing = await _command("pg_restore", "--list", str(dump_path))
    assert "cms_doctor_group_site" not in listing
    original_bytes_by_path = {manifest_path: manifest_bytes, dump_path: dump_path.read_bytes()}
    return manifest, authority, original_bytes_by_path


async def _restore_legacy_stage(sessions, dataset_id, url, dump_path):
    """Replace the synthetic exported stage with the current three-table restore stage."""
    stage_schema = archive.reference_family_stage_schema(dataset_id)
    async with sessions() as session, session.begin():
        await session.execute(text(f'DROP SCHEMA "{stage_schema}" CASCADE'))
        ownership = await archive.precreate_reference_family_restore(
            session,
            importer_id="cms-doctors",
            dataset_id=dataset_id,
        )
    await _restore_native_dump(url, dump_path)
    async with sessions() as session, session.begin():
        await archive.complete_reference_family_restore(session, ownership)
    return ownership


async def _assert_legacy_tampering(session, ownership, manifest, schema):
    """Reject empty-schema drift and any attempt to blend group rows into an old package."""
    stage_schema = ownership.schema_name
    for statement, message in (
        (f'ALTER TABLE "{stage_schema}".cms_doctor_group_site ADD COLUMN unexpected text', "group schema differs"),
        (
            f'ALTER TABLE "{stage_schema}".cms_doctor_group_site ALTER COLUMN generation_id DROP NOT NULL',
            "group schema differs",
        ),
        (f'ALTER TABLE "{stage_schema}".cms_doctor_group_site ADD CHECK (npi > 0)', "group schema differs"),
        (
            f'CREATE INDEX unexpected_group_idx ON "{stage_schema}".cms_doctor_group_site (adrs_id)',
            "group schema differs",
        ),
        (
            f'INSERT INTO "{stage_schema}".cms_doctor_group_site SELECT * FROM "{schema}".cms_doctor_group_site',
            "restored stage differs",
        ),
    ):
        with pytest.raises(archive.ReferenceFamilyArchiveError, match=message):
            async with session.begin_nested():
                await session.execute(text(statement))
                await archive.validate_reference_family_stage(session, ownership=ownership, manifest=manifest)


async def _prepare_legacy_cutover(session, ownership, manifest, schema, authority):
    """Bind all three stage receipts to the original manifest and reject automatic activation."""
    owner_oid = await session.scalar(text("SELECT oid FROM pg_roles WHERE rolname=current_user"))
    package_id = hashlib.sha256(json.dumps(manifest.as_dict(), indent=2).encode()).hexdigest()
    validation = await archive.prepare_reference_family_activation(
        session,
        ownership=ownership,
        manifest=manifest,
        package_id=package_id,
        profile_contract=archive.CONTRACT,
        sealed_owner_oid=owner_oid,
    )
    assert len(validation.tables) == len(validation.relation_oids) == 3
    assert validation.tables[:2] == manifest.tables
    assert validation.tables[2].row_count == 0
    assert validation.manifest_sha256 == hashlib.sha256(archive._canonical_json(manifest.as_dict())).hexdigest()
    incumbent = await archive.capture_reference_family_incumbent(session, importer_id="cms-doctors", schema_name=schema)
    cutover = archive.ReferenceFamilyCutoverAuthority(
        package_id,
        archive.CONTRACT,
        owner_oid,
        owner_oid,
        "automatic",
        authority.serving_generation.as_dict(),
    )
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="requires manual activation"):
        await archive.activate_validated_reference_family_stage(
            session,
            ownership=ownership,
            manifest=manifest,
            expected_incumbent=incumbent,
            validation_receipt=validation,
            cutover=cutover,
        )
    return validation, incumbent, replace(cutover, authority="manual")


async def _assert_legacy_rollback(session, activate, monkeypatch, ownership, incumbent, authority):
    """Fail after rotation and prove both the family and its authority roll back."""

    async def fail_after_rotation(*args, **kwargs):
        raise RuntimeError("synthetic publication failure")

    with monkeypatch.context() as patch:
        patch.setattr(archive, "publish_adopted_reference_family_generation", fail_after_rotation)
        with pytest.raises(RuntimeError, match="synthetic publication failure"):
            async with session.begin_nested():
                await activate()
    assert (
        await archive.capture_reference_family_incumbent(
            session,
            importer_id="cms-doctors",
            schema_name=incumbent.schema_name,
        )
        == incumbent
    )
    await archive.verify_reference_family_stage_ownership(session, ownership)
    unchanged = await generation.read_reference_family_result_generation_authority(
        session,
        importer_id="cms-doctors",
        schema_name=incumbent.schema_name,
    )
    assert unchanged == authority


async def _assert_legacy_activation(session, activation_receipt, schema, stage_schema):
    """Verify the old group survives only in the predecessor and no generation was fabricated."""
    predecessor = activation_receipt.predecessor_schema_name
    assert len(activation_receipt.tables) == 3 and activation_receipt.tables[2].row_count == 0
    assert await session.scalar(text(f'SELECT count(*) FROM "{schema}".cms_doctor_group_site')) == 0
    assert await session.scalar(text(f'SELECT count(*) FROM "{predecessor}".cms_doctor_group_site')) == 1
    assert await session.scalar(text(f'SELECT city FROM "{schema}".doctor_clinician_address')) == "Example"
    adopted = await generation.read_reference_family_result_generation_authority(
        session,
        importer_id="cms-doctors",
        schema_name=schema,
    )
    assert adopted.serving_generation is None and adopted.relation_oids is None
    assert await session.scalar(text("SELECT to_regnamespace(:schema)"), {"schema": stage_schema}) is None


async def _exercise_legacy_activation(session, ownership, manifest, schema, authority, monkeypatch, protected):
    """Exercise schema guards, frozen-stage proof, rollback and the selected manual cutover path."""
    await archive.validate_reference_family_stage(session, ownership=ownership, manifest=manifest)
    await _assert_legacy_tampering(session, ownership, manifest, schema)
    validation, incumbent, cutover = await _prepare_legacy_cutover(session, ownership, manifest, schema, authority)

    async def activate():
        if protected:
            return await archive.activate_validated_reference_family_stage(
                session,
                ownership=ownership,
                manifest=manifest,
                expected_incumbent=incumbent,
                validation_receipt=validation,
                cutover=cutover,
            )
        return await archive.activate_reference_family_stage(
            session,
            ownership=ownership,
            manifest=manifest,
            expected_incumbent=incumbent,
            authority="manual",
        )

    await _assert_legacy_rollback(session, activate, monkeypatch, ownership, incumbent, authority)
    activation_receipt = await activate()
    await _assert_legacy_activation(session, activation_receipt, schema, ownership.schema_name)
    return activation_receipt.predecessor_schema_name


async def _drop_test_schemas(engine, schemas):
    """Remove only this test's named schemas and verify their absence."""
    async with engine.begin() as connection:
        for schema_name in schemas:
            if schema_name is None:
                continue
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))
            assert await connection.scalar(text("SELECT to_regnamespace(:schema)"), {"schema": schema_name}) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("protected", [False, True])
async def test_legacy_two_table_package_restores_manually_as_three_tables(monkeypatch, tmp_path, protected):
    """Restore an unchanged historical package manually with a verified empty third relation."""
    url = _database_url()
    schema = "cms_source_" + uuid4().hex
    dataset_id = uuid4()
    stage_schema = archive.reference_family_stage_schema(dataset_id)
    predecessor = None
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    engine = create_async_engine(url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        manifest, authority, original_bytes_by_path = await _export_legacy_package(
            sessions,
            schema,
            dataset_id,
            url,
            tmp_path,
        )
        ownership = await _restore_legacy_stage(sessions, dataset_id, url, tmp_path / "legacy.dump")
        async with sessions() as session, session.begin():
            predecessor = await _exercise_legacy_activation(
                session,
                ownership,
                manifest,
                schema,
                authority,
                monkeypatch,
                protected,
            )
        for archive_path, original_bytes in original_bytes_by_path.items():
            assert archive_path.read_bytes() == original_bytes
    finally:
        await _drop_test_schemas(engine, (stage_schema, schema, predecessor))
        await engine.dispose()
