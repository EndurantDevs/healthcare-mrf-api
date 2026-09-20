# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""One native geo swap proves exact schema, ordering, and rollback."""

import subprocess
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from process import reference_family_archive as archive
from process import reference_family_result_generation as generation
from tests.test_reference_family_archive_postgres import _create_live_family, _database_url, _manifest

_EXPECTED_INDEX_NAMES = {
    "geo_zip_lookup_pkey",
    "geo_zip_lookup_idx_geo_zip_lookup_state_idx",
    "geo_zip_lookup_idx_geo_zip_lookup_city_state_idx",
    "geo_zip_lookup_idx_geo_zip_lookup_city_idx",
    "geo_zip_lookup_idx_geo_zip_lookup_lat_lng_idx",
}


async def _index_names(session, schema_name):
    rows = await session.execute(
        text("SELECT indexname FROM pg_indexes WHERE schemaname=:schema AND tablename='geo_zip_lookup'"),
        {"schema": schema_name},
    )
    return {row[0] for row in rows.all()}


async def _prepare_geo_candidate(sessions, live_schema, dataset_id):
    stage_schema = archive.reference_family_stage_schema(dataset_id)
    async with sessions() as session, session.begin():
        await _create_live_family(session, "geo", live_schema)
        await session.execute(
            text(
                f'INSERT INTO "{live_schema}".geo_zip_lookup '
                "(zip_code, city, city_lower, state, latitude, longitude) "
                "VALUES ('10001', 'New York', 'new york', 'NY', 40.7, -73.9)"
            )
        )
    manifest = await _manifest(sessions, "geo", live_schema)
    assert tuple(table.table_name for table in manifest.tables) == ("geo_zip_lookup",)
    async with sessions() as session, session.begin():
        stage_ownership = await archive.precreate_reference_family_restore(
            session, importer_id="geo", dataset_id=dataset_id
        )
        await session.execute(
            text(f'INSERT INTO "{stage_schema}".geo_zip_lookup SELECT * FROM "{live_schema}".geo_zip_lookup')
        )
        await archive.complete_reference_family_restore(session, stage_ownership)
        stage_index_names = await _index_names(session, stage_schema)
        assert stage_index_names == _EXPECTED_INDEX_NAMES
        await session.execute(text(f"UPDATE \"{live_schema}\".geo_zip_lookup SET city = 'Old City'"))
        initial_authority = await generation.publish_local_reference_family_generation(
            session, importer_id="geo", schema_name=live_schema
        )
    return stage_ownership, manifest, stage_index_names, initial_authority


async def _roundtrip_native_archive(sessions, database_url, stage_schema, archive_path):
    postgres_url = database_url.replace("+asyncpg", "")
    subprocess.run(
        [
            "pg_dump",
            "-Fc",
            "--data-only",
            "--table",
            f"{stage_schema}.geo_zip_lookup",
            "--file",
            str(archive_path),
            postgres_url,
        ],
        check=True,
        capture_output=True,
    )
    async with sessions() as session, session.begin():
        await session.execute(text(f'TRUNCATE "{stage_schema}".geo_zip_lookup'))
    subprocess.run(
        ["pg_restore", "--data-only", "--dbname", postgres_url, str(archive_path)],
        check=True,
        capture_output=True,
    )
    async with sessions() as session, session.begin():
        assert await session.scalar(text(f'SELECT city FROM "{stage_schema}".geo_zip_lookup')) == "New York"


async def _activation_binding(sessions, stage_ownership, manifest, live_schema, package_id):
    async with sessions() as session, session.begin():
        incumbent = await archive.capture_reference_family_incumbent(
            session, importer_id="geo", schema_name=live_schema
        )
        sealed_owner_oid = await session.scalar(text("SELECT oid FROM pg_roles WHERE rolname=current_user"))
        validation_receipt = await archive.prepare_reference_family_activation(
            session,
            ownership=stage_ownership,
            manifest=manifest,
            package_id=package_id,
            profile_contract=archive.CONTRACT,
            sealed_owner_oid=sealed_owner_oid,
        )
    return {
        "ownership": stage_ownership,
        "manifest": manifest,
        "expected_incumbent": incumbent,
        "validation_receipt": validation_receipt,
        "package_id": package_id,
        "sealed_owner_oid": sealed_owner_oid,
    }


async def _activate(session, activation_by_field, source_generation_by_field):
    return await archive.activate_validated_reference_family_stage(
        session,
        ownership=activation_by_field["ownership"],
        manifest=activation_by_field["manifest"],
        expected_incumbent=activation_by_field["expected_incumbent"],
        validation_receipt=activation_by_field["validation_receipt"],
        cutover=archive.ReferenceFamilyCutoverAuthority(
            activation_by_field["package_id"],
            archive.CONTRACT,
            activation_by_field["sealed_owner_oid"],
            activation_by_field["sealed_owner_oid"],
            "automatic",
            source_generation_by_field,
        ),
    )


async def _assert_atomic_activation(
    sessions,
    activation_by_field,
    initial_authority,
    source_generation_by_field,
    live_schema,
    predecessor_schema,
    stage_index_names,
):
    async with sessions() as session, session.begin():
        with pytest.raises(archive.ReferenceFamilyArchiveError, match="stale or unrelated"):
            await _activate(session, activation_by_field, initial_authority.serving_generation.as_dict())
    async with sessions() as session:
        transaction = await session.begin()
        await _activate(session, activation_by_field, source_generation_by_field)
        assert await session.scalar(text(f'SELECT city FROM "{live_schema}".geo_zip_lookup')) == "New York"
        await transaction.rollback()
    async with sessions() as session, session.begin():
        assert await session.scalar(text(f'SELECT city FROM "{live_schema}".geo_zip_lookup')) == "Old City"
        assert await session.scalar(
            text("SELECT to_regnamespace(:schema)"),
            {"schema": activation_by_field["ownership"].schema_name},
        )
        authority = await generation.read_reference_family_result_generation_authority(
            session, importer_id="geo", schema_name=live_schema
        )
        assert authority == initial_authority
        await _activate(session, activation_by_field, source_generation_by_field)
    async with sessions() as session, session.begin():
        assert await session.scalar(text(f'SELECT city FROM "{live_schema}".geo_zip_lookup')) == "New York"
        authority = await generation.read_reference_family_result_generation_authority(
            session, importer_id="geo", schema_name=live_schema
        )
        assert authority.serving_generation.as_dict() == source_generation_by_field
        assert await _index_names(session, live_schema) == stage_index_names
        assert await session.scalar(text(f'SELECT city FROM "{predecessor_schema}".geo_zip_lookup')) == "Old City"


@pytest.mark.asyncio
async def test_geo_replacement_keeps_indexes_and_fences_generation_and_failure(tmp_path):
    """Restore native data, then prove guarded rollback and committed replacement."""

    database_url = _database_url()
    engine = create_async_engine(database_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    live_schema = f"rf_geo_{uuid4().hex[:10]}"
    dataset_id = uuid4()
    stage_schema = archive.reference_family_stage_schema(dataset_id)
    predecessor_schema = archive.reference_family_predecessor_schema(dataset_id)
    package_id = "a" * 64
    try:
        stage_ownership, manifest, stage_index_names, initial_authority = await _prepare_geo_candidate(
            sessions, live_schema, dataset_id
        )
        archive_path = tmp_path / "geo.dump"
        await _roundtrip_native_archive(sessions, database_url, stage_schema, archive_path)
        activation_by_field = await _activation_binding(sessions, stage_ownership, manifest, live_schema, package_id)
        source_generation_by_field = {
            "origin_lineage_id": initial_authority.local_lineage_id,
            "origin_generation": 2,
            "published_at": "2026-09-20T10:00:00Z",
        }
        await _assert_atomic_activation(
            sessions,
            activation_by_field,
            initial_authority,
            source_generation_by_field,
            live_schema,
            predecessor_schema,
            stage_index_names,
        )
    finally:
        async with engine.begin() as connection:
            for schema_name in (stage_schema, predecessor_schema, live_schema):
                await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))
        await engine.dispose()
