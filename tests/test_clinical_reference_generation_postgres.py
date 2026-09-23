# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Synthetic transaction proof for the clinical reference generation boundary."""

import importlib.util
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from process import reference_family_result_generation as generation
from process.reference_family_archive import reference_family_spec
from tests.test_reference_family_archive_postgres import _database_url
from tests.test_reference_family_result_generation_postgres import _run_migration

_MIGRATION_PATH = (
    Path(__file__).resolve().parents[1] / "alembic/versions/20260923022000_clinical_reference_result_generation.py"
)


def test_clinical_migration_check_matches_model_and_generation_order():
    spec = importlib.util.spec_from_file_location("clinical_result_generation_migration", _MIGRATION_PATH)
    assert spec is not None and spec.loader is not None
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    previous, counts = migration._generation_shape()
    relation_names = generation.RELATION_NAMES_BY_IMPORTER["clinical-reference"]
    assert relation_names == reference_family_spec("clinical-reference").table_names
    assert "clinical-reference" not in counts
    assert "importer_id = 'clinical-reference' AND cardinality(relation_oids) = 7" in previous._shape(
        {**counts, "facility-anchors": 2, "clinical-reference": len(relation_names)}
    )


@pytest.mark.asyncio
async def test_clinical_migration_enforces_seven_relation_oids(monkeypatch):
    schema = "clinical_migration_" + uuid4().hex
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    spec = importlib.util.spec_from_file_location("clinical_result_generation_migration", _MIGRATION_PATH)
    assert spec is not None and spec.loader is not None
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    previous, counts = migration._generation_shape()
    prior_check = previous._shape({**counts, "facility-anchors": 2})
    engine = create_async_engine(_database_url())
    try:
        async with engine.begin() as connection:
            await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
            await connection.execute(
                text(
                    f'CREATE TABLE "{schema}".reference_family_result_generation ('
                    "importer_id text PRIMARY KEY, local_lineage_id uuid NOT NULL, local_generation bigint NOT NULL, "
                    "origin_lineage_id uuid, origin_generation bigint, published_at timestamptz, relation_oids bigint[], "
                    f"CONSTRAINT reference_family_result_generation_shape_check CHECK ({prior_check}))"
                )
            )
            await _run_migration(connection, _MIGRATION_PATH, "upgrade")
            await connection.execute(
                text(
                    f'UPDATE "{schema}".reference_family_result_generation '
                    "SET local_generation=1, origin_lineage_id=local_lineage_id, origin_generation=1, "
                    "published_at=now(), relation_oids=ARRAY[1,2,3,4,5,6,7]::bigint[] "
                    "WHERE importer_id='clinical-reference'"
                )
            )
            with pytest.raises(IntegrityError):
                async with connection.begin_nested():
                    await connection.execute(
                        text(
                            f'UPDATE "{schema}".reference_family_result_generation '
                            "SET relation_oids=ARRAY[1,2,3,4,5,6]::bigint[] WHERE importer_id='clinical-reference'"
                        )
                    )
    finally:
        async with engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await engine.dispose()


@pytest.mark.asyncio
async def test_clinical_generation_is_atomic_with_publication():
    engine = create_async_engine(_database_url())
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    schema = "clinical_generation_" + uuid4().hex
    names = generation.RELATION_NAMES_BY_IMPORTER["clinical-reference"]
    try:
        async with sessions.begin() as session:
            await session.execute(text(f'CREATE SCHEMA "{schema}"'))
            for name in names:
                await session.execute(text(f'CREATE TABLE "{schema}"."{name}" (id integer PRIMARY KEY)'))
            await session.execute(
                text(
                    f'CREATE TABLE "{schema}".reference_family_result_generation ('
                    "importer_id text PRIMARY KEY, local_lineage_id uuid NOT NULL, "
                    "local_generation bigint NOT NULL, origin_lineage_id uuid, "
                    "origin_generation bigint, published_at timestamptz, relation_oids bigint[])"
                )
            )
            await session.execute(
                text(
                    f'INSERT INTO "{schema}".reference_family_result_generation '
                    "VALUES ('clinical-reference',:lineage,0,NULL,NULL,NULL,NULL)"
                ),
                {"lineage": uuid4()},
            )
        async with sessions.begin() as session:
            first = await generation.publish_local_reference_family_generation(
                session, importer_id="clinical-reference", schema_name=schema
            )
            assert len(first.relation_oids) == 7
        with pytest.raises(RuntimeError, match="abort"):
            async with sessions.begin() as session:
                await generation.publish_local_reference_family_generation(
                    session, importer_id="clinical-reference", schema_name=schema
                )
                raise RuntimeError("abort")
        async with sessions.begin() as session:
            observed = await generation.read_reference_family_result_generation_authority(
                session, importer_id="clinical-reference", schema_name=schema
            )
            assert observed == first
    finally:
        async with engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await engine.dispose()
