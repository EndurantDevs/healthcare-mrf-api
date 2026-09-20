# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native census archive, retained replacement, and generation rollback proof."""

import subprocess
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy import MetaData, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from db import models
from db.connection import Database
from process import geo_census_import
from process import reference_family_archive as archive
from process import reference_family_result_generation as generation
from tests.test_reference_family_archive_postgres import _create_live_family, _database_url, _manifest
from tests.test_reference_family_result_generation_postgres import (
    _CENSUS_MIGRATION_PATH,
    _GEO_MIGRATION_PATH,
    _PHARMACY_MIGRATION_PATH,
    _TERMINOLOGY_MIGRATION_PATH,
    _run_migration,
    _upgrade_reference_generation_chain,
)


@pytest.mark.asyncio
async def test_census_source_clone_keeps_captured_dependency_identity():
    engine = create_async_engine(_database_url())
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    live = "census_capture_" + uuid4().hex
    identity = uuid4()
    stage = archive.reference_family_stage_schema(identity)
    dependency_by_dataset = {"geo": "a" * 64}
    captures = []

    async def dependencies(session):
        assert session.in_transaction()
        return dict(dependency_by_dataset)

    async def persist(_session, prepared):
        assert prepared.manifest.dependencies == {"geo": "a" * 64}

    async def copy(capture):
        captures.append(capture.manifest.as_dict())

    try:
        async with sessions.begin() as session:
            await _create_live_family(session, "geo-census", live)
            initial = await generation.publish_local_reference_family_generation(
                session, importer_id="geo-census", schema_name=live
            )
        prepared = await archive.prepare_reference_family_archive_source(
            sessions,
            importer_id="geo-census",
            schema_name=live,
            source_metadata={"release": "synthetic"},
            dataset_id=identity,
            on_prepared=persist,
            dependency_factory=dependencies,
        )
        dependency_by_dataset["geo"] = "b" * 64
        await archive.export_prepared_reference_family_archive(sessions, prepared=prepared, archive_copy=copy)
        assert captures[0]["dependencies"] == {"geo": "a" * 64}
        async with sessions.begin() as session:
            assert (
                await generation.read_reference_family_result_generation_authority(
                    session, importer_id="geo-census", schema_name=live
                )
                == initial
            )
            await archive.cleanup_reference_family_stage(session, prepared.ownership)
    finally:
        async with engine.begin() as connection:
            for schema in (stage, live):
                await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await engine.dispose()


@pytest.mark.asyncio
async def test_census_native_archive_atomic_retained_swap_and_rollback(tmp_path):
    engine = create_async_engine(_database_url())
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    live = "census_test_" + uuid4().hex
    identity = uuid4()
    stage = archive.reference_family_stage_schema(identity)
    previous = archive.reference_family_predecessor_schema(identity)
    try:
        async with sessions.begin() as session:
            await _create_live_family(session, "geo-census", live)
            await session.execute(
                text(
                    f'INSERT INTO "{live}".geo_zip_census_profile '
                    "(zip_code,total_population,median_household_income) VALUES ('10001',123,45678)"
                )
            )
        manifest = await _manifest(sessions, "geo-census", live, dependencies={"geo": "b" * 64})
        assert [(row.table_name, row.row_count) for row in manifest.tables] == [("geo_zip_census_profile", 1)]
        async with sessions.begin() as session:
            ownership = await archive.precreate_reference_family_restore(
                session, importer_id="geo-census", dataset_id=identity
            )
            await session.execute(
                text(f'INSERT INTO "{stage}".geo_zip_census_profile SELECT * FROM "{live}".geo_zip_census_profile')
            )
        archive_path = tmp_path / "census.dump"
        dsn = _database_url().replace("+asyncpg", "")
        subprocess.run(
            [
                "pg_dump",
                "-Fc",
                "--data-only",
                "--table",
                stage + ".geo_zip_census_profile",
                "--file",
                str(archive_path),
                dsn,
            ],
            check=True,
            capture_output=True,
        )
        async with sessions.begin() as session:
            await session.execute(text(f'TRUNCATE "{stage}".geo_zip_census_profile'))
        subprocess.run(
            ["pg_restore", "--data-only", "--dbname", dsn, str(archive_path)],
            check=True,
            capture_output=True,
        )
        async with sessions.begin() as session:
            await archive.complete_reference_family_restore(session, ownership)
            assert (
                await session.scalar(
                    text(
                        "SELECT count(*) FROM pg_index WHERE indrelid=to_regclass(:relation) "
                        "AND indisprimary AND indisvalid AND indisready"
                    ),
                    {"relation": stage + ".geo_zip_census_profile"},
                )
                == 1
            )
            await session.execute(text(f'UPDATE "{live}".geo_zip_census_profile SET total_population=1'))
            initial = await generation.publish_local_reference_family_generation(
                session, importer_id="geo-census", schema_name=live
            )
            incumbent = await archive.capture_reference_family_incumbent(
                session, importer_id="geo-census", schema_name=live
            )
            owner = await session.scalar(text("SELECT oid FROM pg_roles WHERE rolname=current_user"))
            validation = await archive.prepare_reference_family_activation(
                session,
                ownership=ownership,
                manifest=manifest,
                package_id="a" * 64,
                profile_contract=archive.CONTRACT,
                sealed_owner_oid=owner,
            )
        args = dict(
            ownership=ownership,
            manifest=manifest,
            expected_incumbent=incumbent,
            validation_receipt=validation,
            cutover=archive.ReferenceFamilyCutoverAuthority(
                "a" * 64,
                archive.CONTRACT,
                owner,
                owner,
                "manual",
            ),
        )
        async with sessions() as session:
            transaction = await session.begin()
            await archive.activate_validated_reference_family_stage(session, **args)
            assert await session.scalar(text(f'SELECT total_population FROM "{live}".geo_zip_census_profile')) == 123
            await transaction.rollback()
        async with sessions.begin() as session:
            assert await session.scalar(text(f'SELECT total_population FROM "{live}".geo_zip_census_profile')) == 1
            assert (
                await generation.read_reference_family_result_generation_authority(
                    session, importer_id="geo-census", schema_name=live
                )
                == initial
            )
            await archive.activate_validated_reference_family_stage(session, **args)
        async with sessions.begin() as session:
            assert await session.scalar(text(f'SELECT total_population FROM "{live}".geo_zip_census_profile')) == 123
            assert await session.scalar(text(f'SELECT total_population FROM "{previous}".geo_zip_census_profile')) == 1
            assert (
                await session.scalar(
                    text("SELECT to_regclass(:relation)::oid"), {"relation": live + ".geo_zip_census_profile"}
                )
                == dict(ownership.relation_oids)["geo_zip_census_profile"]
            )
    finally:
        async with engine.begin() as connection:
            for schema in (stage, previous, live):
                await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await engine.dispose()


@pytest.mark.asyncio
async def test_census_import_rolls_back_rows_when_generation_publication_fails(monkeypatch):
    schema = "census_import_" + uuid4().hex
    engine = create_async_engine(
        _database_url(),
        execution_options={"schema_translate_map": {"mrf": schema}},
    )
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    database = Database(engine=engine, session_factory=sessions)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    try:
        async with engine.begin() as connection:
            await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
            await _upgrade_reference_generation_chain(connection)
            for path in (
                _GEO_MIGRATION_PATH,
                _PHARMACY_MIGRATION_PATH,
                _TERMINOLOGY_MIGRATION_PATH,
                _CENSUS_MIGRATION_PATH,
            ):
                await _run_migration(connection, path, "upgrade")
            metadata = MetaData(schema=schema)
            models.GeoZipCensusProfile.__table__.to_metadata(metadata, schema=schema)
            await connection.run_sync(metadata.create_all)
            await connection.execute(
                text(f"INSERT INTO \"{schema}\".geo_zip_census_profile (zip_code,total_population) VALUES ('10001',1)")
            )
        async with sessions.begin() as session:
            initial = await generation.publish_local_reference_family_generation(
                session,
                importer_id="geo-census",
                schema_name=schema,
            )

        async def collect_profiles(*_args, **_kwargs):
            return {"10002": {"zip_code": "10002", "total_population": 2}}

        async def ready(_test_mode):
            return None

        monkeypatch.setattr(geo_census_import, "db", database)
        monkeypatch.setattr(geo_census_import, "_collect_profile_map", collect_profiles)
        monkeypatch.setattr(geo_census_import, "ensure_database", ready)
        monkeypatch.setattr(database, "create_table", AsyncMock())
        monkeypatch.setattr(
            generation,
            "publish_local_reference_family_generation",
            AsyncMock(side_effect=RuntimeError("synthetic publication failure")),
        )

        with pytest.raises(RuntimeError, match="synthetic publication failure"):
            await geo_census_import.load_geo_census_lookup()

        async with sessions.begin() as session:
            assert (
                await session.execute(
                    text(f'SELECT zip_code,total_population FROM "{schema}".geo_zip_census_profile ORDER BY zip_code')
                )
            ).all() == [("10001", 1)]
            assert (
                await generation.read_reference_family_result_generation_authority(
                    session,
                    importer_id="geo-census",
                    schema_name=schema,
                )
                == initial
            )
    finally:
        async with engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await engine.dispose()


@pytest.mark.asyncio
async def test_census_migration_preserves_other_results_and_rejects_evidence_downgrade(monkeypatch):
    engine = create_async_engine(_database_url())
    schema = "census_migration_" + uuid4().hex
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    table = f'"{schema}".reference_family_result_generation'
    try:
        async with engine.begin() as connection:
            await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
            await _upgrade_reference_generation_chain(connection)
            for path in (_GEO_MIGRATION_PATH, _PHARMACY_MIGRATION_PATH, _TERMINOLOGY_MIGRATION_PATH):
                await _run_migration(connection, path, "upgrade")
            before = (await connection.execute(text(f"SELECT * FROM {table} ORDER BY importer_id"))).all()
            await _run_migration(connection, _CENSUS_MIGRATION_PATH, "upgrade")
            assert (
                await connection.execute(
                    text(f"SELECT * FROM {table} WHERE importer_id <> 'geo-census' ORDER BY importer_id")
                )
            ).all() == before
            await connection.execute(text(f"UPDATE {table} SET local_generation=1 WHERE importer_id='geo-census'"))
            with pytest.raises(RuntimeError, match="evidence prevents downgrade"):
                await _run_migration(connection, _CENSUS_MIGRATION_PATH, "downgrade")
            await connection.execute(text(f"UPDATE {table} SET local_generation=0 WHERE importer_id='geo-census'"))
            await _run_migration(connection, _CENSUS_MIGRATION_PATH, "downgrade")
            assert (await connection.execute(text(f"SELECT * FROM {table} ORDER BY importer_id"))).all() == before
    finally:
        async with engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await engine.dispose()
