# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real PostGIS archive and protected static-family publication regression."""

import os
import re
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from process import reference_family_archive as archive
from process import reference_family_result_generation as generation
from process.tiger_result_generation import publish_tiger_generation
from tests.test_cms_doctors_archive_postgres import _command, _migration


def _database_url():
    raw = os.getenv("HLTHPRT_TIGER_ARCHIVE_TEST_DSN")
    if not raw:
        pytest.skip("set HLTHPRT_TIGER_ARCHIVE_TEST_DSN for the native PostGIS proof")
    url = make_url(raw)
    if url.host not in {"localhost", "127.0.0.1", "postgres"} or not re.fullmatch(
        r"tiger_archive_test_[0-9a-f]{32}", url.database or ""
    ):
        pytest.fail("TIGER archive proof requires a UUID-owned local test database")
    return url.set(drivername="postgresql+asyncpg")


async def _installed_tables(session):
    await session.execute(text("CREATE EXTENSION postgis"))
    await session.execute(text("CREATE SCHEMA tiger"))
    await session.execute(
        text(
            "CREATE TABLE tiger.zip_state (zip varchar(5) NOT NULL, stusps varchar(2) NOT NULL, "
            "statefp varchar(2), PRIMARY KEY (zip,stusps))"
        )
    )
    await session.execute(
        text(
            "CREATE TABLE tiger.zcta5 (gid serial NOT NULL, statefp varchar(2) NOT NULL, "
            "zcta5ce varchar(5) NOT NULL, classfp varchar(2), mtfcc varchar(5), funcstat varchar(1), "
            "aland double precision, awater double precision, intptlat varchar(11), intptlon varchar(12), "
            "partflg varchar(1), the_geom geometry(MultiPolygon,4269), PRIMARY KEY(zcta5ce,statefp))"
        )
    )
    await session.execute(text("CREATE INDEX zcta5_geom_gist ON tiger.zcta5 USING gist(the_geom)"))
    await session.execute(text("INSERT INTO tiger.zip_state VALUES ('00001','AA','01')"))
    await session.execute(
        text(
            "INSERT INTO tiger.zcta5 (gid,statefp,zcta5ce,the_geom) VALUES (42,'01','00001', "
            "ST_GeomFromText('MULTIPOLYGON(((0 0,0 1,1 1,1 0,0 0)))',4269))"
        )
    )
    await session.execute(text("SELECT setval('tiger.zcta5_gid_seq',1000,true)"))


async def _install_authority_as_app(session, schema, role):
    await session.execute(text(f'CREATE ROLE "{role}" NOLOGIN'))
    await session.execute(text(f'CREATE SCHEMA "{schema}" AUTHORIZATION "{role}"'))
    await session.execute(text(f'GRANT USAGE ON SCHEMA tiger TO "{role}"'))
    await session.execute(text(f'GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA tiger TO "{role}"'))
    await session.execute(text(f'SET LOCAL ROLE "{role}"'))
    assert not await session.scalar(text("SELECT has_schema_privilege(current_user,'tiger','CREATE')"))
    for revision in (
        "20260914110000_reference_family_result_generation",
        "20260914130000_mrf_result_generation",
        "20260920100000_cms_doctors_result_generation",
    ):
        await _migration(session, revision, "upgrade")
    ledger = f'"{schema}".reference_family_result_generation'
    before = (await session.execute(text(f"SELECT * FROM {ledger} ORDER BY importer_id"))).all()
    await _migration(session, "20260920110000_tiger_result_generation", "upgrade")
    assert (
        await session.execute(text(f"SELECT * FROM {ledger} WHERE importer_id <> 'tiger' ORDER BY importer_id"))
    ).all() == before
    authority = await generation.read_reference_family_result_generation_authority(
        session,
        importer_id="tiger",
        schema_name="tiger",
    )
    assert authority.local_generation == 0 and authority.serving_generation is None
    assert await session.scalar(text("SELECT to_regclass('tiger.reference_family_result_generation')")) is None
    with pytest.raises(RuntimeError, match="unavailable or drifted"):
        await generation.capture_reference_family_serving_generation(session, importer_id="tiger", schema_name="tiger")
    await _migration(session, "20260920110000_tiger_result_generation", "downgrade")
    await _migration(session, "20260920110000_tiger_result_generation", "upgrade")
    await session.execute(text("RESET ROLE"))


async def _archive_restore(sessions, url, dataset_id, path):
    async def retain(_session, _prepared):
        return None

    prepared = await archive.prepare_reference_family_archive_source(
        sessions,
        importer_id="tiger",
        schema_name="tiger",
        source_metadata={"source_release": "reviewed-static-fixture"},
        dataset_id=dataset_id,
        on_prepared=retain,
    )

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
    assert "SEQUENCE SET" in listing and "zcta5_gid_seq" in listing
    async with sessions() as session, session.begin():
        await archive.cleanup_reference_family_stage(session, prepared.ownership)
        restored = await archive.precreate_reference_family_restore(session, importer_id="tiger", dataset_id=dataset_id)
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
    restored_source = archive.ReferenceFamilyPreparedSource(prepared.manifest, restored)
    await _assert_restored_source(sessions, restored_source)
    return restored_source


async def _assert_restored_source(sessions, prepared):
    ownership = prepared.ownership
    async with sessions() as session, session.begin():
        await archive.validate_reference_family_stage(session, ownership=ownership, manifest=prepared.manifest)
        assert ownership.sequence_oids[0][0] == "zcta5_gid_seq"
        assert ownership.sequence_oids[0][2:] == ("zcta5", "gid")
        assert await session.scalar(text(f'SELECT last_value FROM "{ownership.schema_name}".zcta5_gid_seq')) == 42
        assert (
            await session.scalar(
                text(
                    f'SELECT ST_Equals(copy.the_geom,live.the_geom) FROM "{ownership.schema_name}".zcta5 copy '
                    "CROSS JOIN tiger.zcta5 live"
                )
            )
            is True
        )


async def _activation_bindings(session, prepared):
    incumbent = await archive.capture_reference_family_incumbent(session, importer_id="tiger", schema_name="tiger")
    owner = await session.scalar(text("SELECT oid FROM pg_roles WHERE rolname=current_user"))
    validation = await archive.prepare_reference_family_activation(
        session,
        ownership=prepared.ownership,
        manifest=prepared.manifest,
        package_id="a" * 64,
        profile_contract=archive.CONTRACT,
        sealed_owner_oid=owner,
    )
    cutover = archive.ReferenceFamilyCutoverAuthority("a" * 64, archive.CONTRACT, owner, owner, "manual")
    return dict(
        ownership=prepared.ownership,
        manifest=prepared.manifest,
        expected_incumbent=incumbent,
        validation_receipt=validation,
        cutover=cutover,
    )


async def _reject_stage_drift(session, prepared):
    stage = prepared.ownership.schema_name
    for statement, message in (
        (
            f'ALTER TABLE "{stage}".zcta5 ALTER COLUMN the_geom TYPE geometry(MultiPolygon,4326) '
            "USING ST_SetSRID(the_geom,4326)",
            "restored stage differs",
        ),
        (f'ALTER SEQUENCE "{stage}".zcta5_gid_seq OWNED BY NONE', "owned sequence set"),
    ):
        savepoint = await session.begin_nested()
        try:
            await session.execute(text(statement))
            with pytest.raises(archive.ReferenceFamilyArchiveError, match=message):
                await archive.validate_reference_family_stage(
                    session,
                    ownership=prepared.ownership,
                    manifest=prepared.manifest,
                )
        finally:
            await savepoint.rollback()


async def _assert_rollback_and_publish(sessions, prepared, role):
    async with sessions() as session, session.begin():
        await _reject_stage_drift(session, prepared)
        await session.execute(text(f'GRANT USAGE ON SCHEMA "{prepared.ownership.schema_name}" TO "{role}"'))
        await session.execute(
            text(f'GRANT SELECT ON ALL TABLES IN SCHEMA "{prepared.ownership.schema_name}" TO "{role}"')
        )
        bindings = await _activation_bindings(session, prepared)
        before = await generation.read_reference_family_result_generation_authority(
            session, importer_id="tiger", schema_name="tiger"
        )
    with pytest.raises(RuntimeError, match="late publication failure"):
        async with sessions() as session, session.begin():
            await publish_tiger_generation(session, **bindings)
            raise RuntimeError("late publication failure")
    async with sessions() as session, session.begin():
        assert (
            await archive.capture_reference_family_incumbent(session, importer_id="tiger", schema_name="tiger")
            == bindings["expected_incumbent"]
        )
        await archive.verify_reference_family_stage_ownership(session, prepared.ownership)
        assert (
            await generation.read_reference_family_result_generation_authority(
                session, importer_id="tiger", schema_name="tiger"
            )
            == before
        )
        receipt = await publish_tiger_generation(session, **bindings)
        assert tuple(sorted(receipt["activation"]["relation_oids"])) == prepared.ownership.relation_oids
        assert receipt["generation"]["local_generation"] == 1
        assert receipt["dataset_id"] == str(prepared.ownership.dataset_id)
        assert await session.scalar(text("SELECT pg_get_serial_sequence('tiger.zcta5','gid')")) == "tiger.zcta5_gid_seq"
        assert await session.scalar(text("SELECT nextval('tiger.zcta5_gid_seq')")) == 43
    async with sessions() as session, session.begin():
        await session.execute(text(f'SET LOCAL ROLE "{role}"'))
        assert await session.scalar(text("SELECT count(*) FROM tiger.zcta5")) == 1
        assert not await session.scalar(text("SELECT has_schema_privilege(current_user,'tiger','CREATE')"))
        authority = await generation.read_reference_family_result_generation_authority(
            session, importer_id="tiger", schema_name="tiger"
        )
        assert authority.local_generation == 1
        assert authority.relation_oids == tuple(
            dict(prepared.ownership.relation_oids)[name] for name in ("zip_state", "zcta5")
        )
        assert (
            await generation.capture_reference_family_serving_generation(
                session, importer_id="tiger", schema_name="tiger"
            )
            == authority.serving_generation
        )
        with pytest.raises(RuntimeError, match="evidence prevents downgrade"):
            await _migration(session, "20260920110000_tiger_result_generation", "downgrade")


@pytest.mark.asyncio
async def test_tiger_native_postgis_archive_app_ledger_and_protected_publication(monkeypatch, tmp_path):
    url = _database_url()
    schema, role, dataset_id = "tiger_app_" + uuid4().hex, "tiger_role_" + uuid4().hex, uuid4()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    engine = create_async_engine(url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with sessions() as session, session.begin():
            assert await session.scalar(text("SELECT to_regnamespace('tiger')")) is None
            await _installed_tables(session)
            await _install_authority_as_app(session, schema, role)
        prepared = await _archive_restore(sessions, url, dataset_id, tmp_path / "tiger.dump")
        await _assert_rollback_and_publish(sessions, prepared, role)
    finally:
        async with engine.begin() as connection:
            for owned_schema in (
                archive.reference_family_stage_schema(dataset_id),
                archive.reference_family_predecessor_schema(dataset_id),
                "tiger",
                schema,
            ):
                await connection.execute(text(f'DROP SCHEMA IF EXISTS "{owned_schema}" CASCADE'))
                assert (
                    await connection.scalar(text("SELECT to_regnamespace(:schema)"), {"schema": owned_schema}) is None
                )
            await connection.execute(text(f'DROP ROLE IF EXISTS "{role}"'))
        await engine.dispose()
