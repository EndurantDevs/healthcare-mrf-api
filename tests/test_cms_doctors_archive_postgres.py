# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native clinician-family publication authority and archive proof."""

import asyncio
import hashlib
import importlib.util
import json
import os
import re
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from db import models
from db.connection import Database
from process import reference_family_archive as archive
from process import reference_family_result_generation as generation
from process.entity_address_cutover_contract import _ServingRelationLockTimeout, wait_for_publication_lock
from tests.cms_doctors_preparation_postgres_support import doctors_snapshot, pending_publisher_locks
from tests.provider_profile_snapshot_postgres_support import profile_reader
from tests.reference_family_generation_fixture import install_source_generation_guards, native_reference_source

_CMS_REVISION = "20260920100000_cms_doctors_result_generation"
_GROUP_REVISION = "20260929000000_cms_doctor_group_site"


def test_group_index_compatibility_keeps_constraint_order_pairs_separate():
    """Accept each exact index transition without mixing catalog orders or constraint versions."""
    hash_pairs = list(archive._cms_group_index_schema_hashes())
    all_hashes = {digest for pair in hash_pairs for digest in pair}
    assert len(hash_pairs) == 3 and len(all_hashes) == 6
    group = archive.ReferenceTableReceipt("CMSDoctorGroupSite", "cms_doctor_group_site", "a" * 64, 1)
    clinician = archive.ReferenceTableReceipt("DoctorClinicianAddress", "doctor_clinician_address", "b" * 64, 1)
    education = archive.ReferenceTableReceipt("CMSDoctorEducation", "cms_doctor_education", "c" * 64, 1)
    for source_hash, stage_hash in hash_pairs:
        manifest = archive.ReferenceFamilyManifest(
            "cms-doctors", (clinician, education, replace(group, schema_sha256=source_hash)), {}, "d" * 64, "e" * 64
        )
        observed = replace(
            manifest, tables=(clinician, education, replace(group, schema_sha256=stage_hash)), schema_sha256="f" * 64
        )
        assert archive._has_matching_manifest_stage_tables(manifest, observed.tables)
        assert archive._has_matching_family_manifest(manifest, observed)
        assert archive._has_matching_manifest_stage_tables(observed, observed.tables)
        assert archive._has_matching_family_manifest(observed, observed)
        assert not archive._has_matching_manifest_stage_tables(observed, manifest.tables)
        assert not archive._has_matching_family_manifest(observed, manifest)
        for other_hash in all_hashes - {source_hash, stage_hash}:
            assert not archive._has_matching_manifest_stage_tables(
                manifest, (clinician, education, replace(group, schema_sha256=other_hash))
            )


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


async def _create_source(session, schema, *, has_adrs_index=True):
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
    if has_adrs_index:
        await session.execute(
            text(f'CREATE INDEX cms_doctor_group_site_idx_adrs ON "{schema}".cms_doctor_group_site (adrs_id)')
        )
    await install_source_generation_guards(await session.connection(), schema)
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


async def _roundtrip(sessions, prepared, url, path, custody):
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

    original_manifest = archive._canonical_json(prepared.manifest.as_dict())
    await archive.export_prepared_reference_family_archive(
        sessions, prepared=prepared, archive_copy=dump, verify_custody=custody.verify
    )
    original_dump = path.read_bytes()
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
        await custody.retire(session, prepared.ownership)
        restored = await archive.precreate_reference_family_restore(
            session,
            importer_id="cms-doctors",
            dataset_id=prepared.ownership.dataset_id,
        )
    await _restore_native_dump(url, path)
    async with sessions() as session, session.begin():
        await archive.complete_reference_family_restore(session, restored)
        await archive.validate_reference_family_stage(session, ownership=restored, manifest=prepared.manifest.as_dict())
        await _assert_clinician_restored_rows(session, restored)
        await _assert_group_index_tampering(session, restored, prepared.manifest)
        await _assert_restored_activation(session, restored, prepared.manifest)
    assert archive._canonical_json(prepared.manifest.as_dict()) == original_manifest
    assert path.read_bytes() == original_dump
    return restored


async def _assert_clinician_restored_rows(session, restored):
    """Keep the original content checks separate from dump/restore resource ownership."""
    assert (
        await session.scalar(text(f'SELECT city FROM "{restored.schema_name}".doctor_clinician_address')) == "Example"
    )
    assert (
        await session.scalar(text(f'SELECT generation_id FROM "{restored.schema_name}".cms_doctor_education'))
        == "source-digest"
    )
    assert (
        await session.scalar(text(f'SELECT org_pac_id FROM "{restored.schema_name}".cms_doctor_group_site'))
        == "0012345678"
    )


async def _assert_group_index_tampering(session, ownership, manifest):
    """Reject unrelated index, column and other-table changes under either accepted shape."""
    group_table = f'"{ownership.schema_name}".cms_doctor_group_site'
    address_index = _group_address_index(ownership.schema_name)
    for statements in (
        (f"CREATE INDEX extra_lookup ON {group_table} (adrs_id)",),
        (f"ALTER TABLE {group_table} ALTER COLUMN generation_id DROP NOT NULL",),
        (f"ALTER TABLE {group_table} ADD CHECK (npi > 0)",),
        (f'CREATE INDEX extra_school ON "{ownership.schema_name}".cms_doctor_education (medical_school)',),
        (
            f"DROP INDEX {address_index}",
            f"CREATE INDEX partial_lookup ON {group_table} (adrs_id) WHERE adrs_id IS NOT NULL",
        ),
    ):
        await _assert_rejected_group_mutation(session, ownership, manifest, statements)


def _group_address_index(schema):
    """Name only the model-created address index in this owned schema."""
    name = archive._index_name_for_table("cms_doctor_group_site", f"{schema}_cms_doctor_group_site_idx_adrs")
    return f'"{schema}"."{name}"'


async def _assert_rejected_group_mutation(session, ownership, manifest, statements):
    """Roll back each structural mutation after proving it fails stage validation."""
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="restored stage differs"):
        async with session.begin_nested():
            for statement in statements:
                await session.execute(text(statement))
            await archive.validate_reference_family_stage(session, ownership=ownership, manifest=manifest)


async def _assert_restored_activation(session, ownership, manifest):
    """Exercise both cutover paths without retaining their disposable predecessors."""
    schema = os.environ["HLTHPRT_DB_SCHEMA"]
    incumbent = await archive.capture_reference_family_incumbent(session, importer_id="cms-doctors", schema_name=schema)
    owner_oid = await session.scalar(text("SELECT oid FROM pg_roles WHERE rolname=current_user"))
    package_id = hashlib.sha256(archive._canonical_json(manifest.as_dict())).hexdigest()
    validation = await archive.prepare_reference_family_activation(
        session,
        ownership=ownership,
        manifest=manifest,
        package_id=package_id,
        profile_contract=archive.CONTRACT,
        sealed_owner_oid=owner_oid,
    )
    assert validation.manifest_sha256 == package_id
    assert validation.tables[2].schema_sha256 == await archive.catalog_identity._schema_identity(
        session, dict(ownership.relation_oids)["cms_doctor_group_site"], ownership.schema_name, "cms_doctor_group_site"
    )
    cutover = archive.ReferenceFamilyCutoverAuthority(
        package_id,
        archive.CONTRACT,
        owner_oid,
        owner_oid,
        "manual",
        manifest.source_serving_generation.as_dict(),
    )
    for protected in (False, True):
        savepoint = await session.begin_nested()
        try:
            if protected:
                receipt = await archive.activate_validated_reference_family_stage(
                    session,
                    ownership=ownership,
                    manifest=manifest,
                    expected_incumbent=incumbent,
                    validation_receipt=validation,
                    cutover=cutover,
                )
            else:
                receipt = await archive.activate_reference_family_stage(
                    session,
                    ownership=ownership,
                    manifest=manifest,
                    expected_incumbent=incumbent,
                    authority="manual",
                )
            assert receipt.tables == validation.tables
            assert (
                await session.scalar(text(f'SELECT org_pac_id FROM "{schema}".cms_doctor_group_site')) == "0012345678"
            )
        finally:
            await savepoint.rollback()


@pytest.mark.asyncio
@pytest.mark.parametrize("has_adrs_index", [False, True])
async def test_clinician_migration_and_native_archive_are_one_closed_generation(monkeypatch, tmp_path, has_adrs_index):
    url = _database_url()
    schema = "cms_source_" + uuid4().hex
    dataset_id = uuid4()
    stage_schema = archive.reference_family_stage_schema(dataset_id)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    engine = create_async_engine(url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    cleanup = AsyncExitStack()
    cleanup.push_async_callback(engine.dispose)
    try:
        custody = await cleanup.enter_async_context(native_reference_source(sessions))
        cleanup.push_async_callback(_drop_test_schemas, engine, (stage_schema, schema))
        async with sessions() as session, session.begin():
            authority = await _create_source(session, schema, has_adrs_index=has_adrs_index)
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

        prepared = await archive.prepare_reference_family_archive_source(
            sessions,
            importer_id="cms-doctors",
            schema_name=schema,
            source_metadata=None,
            dataset_id=dataset_id,
            source_metadata_factory=metadata,
            on_prepared=custody.retain,
            source_copy=custody.source_copy,
            on_precreated=custody.precreate,
        )
        assert prepared.ownership.importer_id == "cms-doctors"
        assert tuple(pair[0] for pair in prepared.ownership.relation_oids) == (
            "cms_doctor_education",
            "cms_doctor_group_site",
            "doctor_clinician_address",
        )
        await _roundtrip(sessions, prepared, url, tmp_path / "clinician.dump", custody)
    finally:
        await cleanup.aclose()


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


async def _restore_legacy_stage(sessions, dataset_id, url, dump_path, *, has_adrs_index=True):
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
        if not has_adrs_index:
            await session.execute(text(f"DROP INDEX {_group_address_index(stage_schema)}"))
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
            f'CREATE INDEX unexpected_group_idx ON "{stage_schema}".cms_doctor_group_site (adrs_id, npi)',
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
        patch.setattr(archive, "_adopt_validated_family_generation", fail_after_rotation)
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
@pytest.mark.parametrize("has_adrs_index", [False, True])
async def test_legacy_two_table_package_restores_manually_as_three_tables(
    monkeypatch, tmp_path, protected, has_adrs_index
):
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
        ownership = await _restore_legacy_stage(
            sessions, dataset_id, url, tmp_path / "legacy.dump", has_adrs_index=has_adrs_index
        )
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


async def _prepare_readable_archive(fixture, dataset_id, custody):
    """Clone and validate a complete real family before any reader starts."""
    sessions = fixture.database.session_factory

    async def retain(session, prepared):
        assert prepared.ownership.dataset_id == dataset_id
        await custody.retain(session, prepared)

    async def metadata(session):
        serving = await generation.capture_reference_family_serving_generation(
            session, importer_id="cms-doctors", schema_name=fixture.schema
        )
        return {"serving_generation": serving.as_dict()}

    prepared = await archive.prepare_reference_family_archive_source(
        sessions,
        importer_id="cms-doctors",
        schema_name=fixture.schema,
        source_metadata=None,
        source_metadata_factory=metadata,
        dataset_id=dataset_id,
        on_prepared=retain,
        source_copy=custody.source_copy,
        on_precreated=custody.precreate,
    )
    async with sessions() as session, session.begin():
        await custody.verify(session, prepared)
        incumbent = await archive.capture_reference_family_incumbent(
            session,
            importer_id="cms-doctors",
            schema_name=fixture.schema,
        )
        owner_oid = await session.scalar(text("SELECT CAST(:owner AS regrole)::oid"), {"owner": custody.owner})
        package_id = hashlib.sha256(archive._canonical_json(prepared.manifest.as_dict())).hexdigest()
        validation = await archive.prepare_reference_family_activation(
            session,
            ownership=prepared.ownership,
            manifest=prepared.manifest,
            package_id=package_id,
            profile_contract=archive.CONTRACT,
            sealed_owner_oid=owner_oid,
        )
    return {
        "ownership": prepared.ownership,
        "manifest": prepared.manifest,
        "expected_incumbent": incumbent,
        "validation_receipt": validation,
        "cutover": archive.ReferenceFamilyCutoverAuthority(
            package_id,
            archive.CONTRACT,
            owner_oid,
            owner_oid,
            "manual",
            prepared.manifest.source_serving_generation.as_dict(),
        ),
    }


@asynccontextmanager
async def _readable_archive(fixture, dataset_id, monkeypatch):
    """Create the source before Reader admission, then retire every Reader-granted heap first."""
    async with AsyncExitStack() as cleanup:
        cleanup.push_async_callback(_drop_test_schemas, fixture.engine, (fixture.schema,))
        async with fixture.database.session_factory() as session, session.begin():
            await _create_source(session, fixture.schema)
        await cleanup.enter_async_context(profile_reader(fixture.database, fixture.schema, monkeypatch))
        reader_role = fixture.database._reader_database._reader_login[0]
        custody = await cleanup.enter_async_context(
            native_reference_source(fixture.database.session_factory, readers=(reader_role,))
        )
        cleanup.push_async_callback(_drop_readable_serving_tables, fixture.engine, fixture.schema)
        cleanup.push_async_callback(
            _drop_test_schemas,
            fixture.engine,
            (
                archive.reference_family_stage_schema(dataset_id),
                archive.reference_family_predecessor_schema(dataset_id),
            ),
        )
        yield await _prepare_readable_archive(fixture, dataset_id, custody)


async def _drop_readable_serving_tables(engine, schema):
    """Retire promoted clone-owned payloads before Owner removal, keeping Reader grant cleanup usable."""
    tables = ", ".join(f'"{schema}"."{name}"' for name in archive.reference_family_spec("cms-doctors").table_names)
    async with engine.begin() as connection:
        await connection.execute(text(f"DROP TABLE IF EXISTS {tables} RESTRICT"))


async def _activate_readable_archive(fixture, activation_by_field, is_validated):
    """Retry ownership belongs to the caller of either archive activation API."""
    async with fixture.database.transaction() as session:
        if is_validated:
            return await archive.activate_validated_reference_family_stage(session, **activation_by_field)
        return await archive.activate_reference_family_stage(
            session,
            authority="manual",
            **{name: activation_by_field[name] for name in ("ownership", "manifest", "expected_incumbent")},
        )


async def test_validated_archive_completes_with_continuous_snapshot_readers(monkeypatch):
    """Prepared metadata-only adoption drains real readers without changing caller ownership."""
    schema, dataset_id = "cms_reader_" + uuid4().hex, uuid4()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    engine = create_async_engine(_database_url())
    fixture = SimpleNamespace(
        engine=engine,
        schema=schema,
        database=Database(engine=engine, session_factory=async_sessionmaker(engine, expire_on_commit=False)),
    )
    entered, finished = asyncio.Event(), asyncio.Event()
    observed_snapshots = []
    tasks = []
    cleanup = AsyncExitStack()
    cleanup.push_async_callback(engine.dispose)

    async def read():
        after_count = 0
        while after_count < 3:
            after_count += finished.is_set()
            observed_snapshots.append(await doctors_snapshot(fixture))
            entered.set()

    try:
        activation_by_field = await cleanup.enter_async_context(_readable_archive(fixture, dataset_id, monkeypatch))
        incumbent = await doctors_snapshot(fixture)
        tasks = [asyncio.create_task(read()) for _ in range(3)]
        await entered.wait()
        for attempt in range(1, 5):
            try:
                receipt = await _activate_readable_archive(fixture, activation_by_field, True)
                break
            except _ServingRelationLockTimeout as error:
                await wait_for_publication_lock(error, attempt)
        finished.set()
        await asyncio.wait_for(asyncio.gather(*tasks), 3)
        adopted = await doctors_snapshot(fixture)
        assert adopted[0] == incumbent[0]
        assert adopted[1].relation_oids == tuple(oid for _, oid in receipt.relation_oids)
        assert adopted[1].serving_generation == incumbent[1].serving_generation
        assert {observed[1].relation_oids for observed in observed_snapshots} == {
            incumbent[1].relation_oids,
            adopted[1].relation_oids,
        }
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await cleanup.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("is_validated", [False, True])
async def test_archive_activation_preserves_late_readers(monkeypatch, is_validated):
    """Both entry points bound contention and preserve the caller's rollback and retry."""
    schema, dataset_id = "cms_reader_" + uuid4().hex, uuid4()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    engine = create_async_engine(_database_url())
    fixture = SimpleNamespace(
        engine=engine,
        schema=schema,
        database=Database(engine=engine, session_factory=async_sessionmaker(engine, expire_on_commit=False)),
    )
    reader = publisher = None
    entered, release = asyncio.Event(), asyncio.Event()
    cleanup = AsyncExitStack()
    cleanup.push_async_callback(engine.dispose)
    try:
        activation_by_field = await cleanup.enter_async_context(_readable_archive(fixture, dataset_id, monkeypatch))
        incumbent = await doctors_snapshot(fixture)
        reader = asyncio.create_task(doctors_snapshot(fixture, entered=entered, release=release))
        await asyncio.wait_for(entered.wait(), 3)
        publisher = asyncio.create_task(_activate_readable_archive(fixture, activation_by_field, is_validated))
        pending = await pending_publisher_locks(fixture, publisher)
        assert await doctors_snapshot(fixture) == incumbent and bool(pending) is is_validated
        with pytest.raises(_ServingRelationLockTimeout if is_validated else DBAPIError):
            await publisher
        async with fixture.database.transaction() as session:
            await archive.verify_reference_family_stage_ownership(session, activation_by_field["ownership"])
            await archive._verify_incumbent(session, activation_by_field["expected_incumbent"])
        assert await doctors_snapshot(fixture) == incumbent
        release.set()
        assert await reader == incumbent
        receipt = await _activate_readable_archive(fixture, activation_by_field, is_validated)
        markers, adopted = await doctors_snapshot(fixture)
        assert markers == incumbent[0] and adopted.local_generation == incumbent[1].local_generation
        assert adopted.serving_generation == (incumbent[1].serving_generation if is_validated else None)
        assert adopted.relation_oids == (tuple(oid for _, oid in receipt.relation_oids) if is_validated else None)
        assert tuple(sorted(receipt.relation_oids)) == activation_by_field["ownership"].relation_oids
    finally:
        release.set()
        tasks = [task for task in (reader, publisher) if task is not None]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await cleanup.aclose()
