# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Migrated companion tables support restricted imports and preserve prior storage."""

import json

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import MetaData, text
from sqlalchemy.exc import DBAPIError

from db.models import ImportRun
from process import scoped_catalog_binding as binding
from process import scoped_catalog_handoff as handoff
from process import scoped_catalog_named_storage as storage
from process.scoped_catalog_named_cutover import rollback
from process.scoped_catalog_publisher import run_publisher
from tests import test_named_scoped_catalog_postgres as native
from tests.test_ms_drg_companion_migration import migration


def _upgrade(connection, revision=None):
    module = migration(revision) if revision else migration()
    with Operations.context(MigrationContext.configure(connection)):
        module.upgrade()


async def _seed_migrated_catalog(engine, schema):
    metadata = MetaData()
    ImportRun.__table__.to_metadata(metadata, schema=schema)
    async with engine.begin() as connection:
        await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
        await connection.run_sync(metadata.create_all)
        await connection.run_sync(_upgrade, "20260215103000_add_code_catalog_and_crosswalk")
        with_model = migration("20260712120000_ptg2_v3_shared_schema")

        def adopt_catalog(sync_connection):
            with Operations.context(MigrationContext.configure(sync_connection)):
                with_model._adopt_code_catalog(schema)

        await connection.run_sync(adopt_catalog)
        await connection.run_sync(native._migrations)
        assert await storage.current_models(connection, schema) == storage.MODELS[:1]
        await connection.run_sync(_upgrade)
        assert await storage.current_models(connection, schema) == storage.MODELS
        await connection.execute(
            text(
                f'INSERT INTO "{schema}".code_catalog (code_system,code,source,effective_year) '
                "VALUES ('OTHER','1','synthetic-reference',2024)"
            )
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("importer", ["code-sets", "ms-drg"])
async def test_migrated_companions_allow_managed_publication_without_importer_ddl(monkeypatch, importer):
    monkeypatch.setattr(native, "_seed", _seed_migrated_catalog)
    monkeypatch.setattr(native, "_copy_archive_tables", _copy_archive_tables)
    async with native._resources(monkeypatch) as (actors, settings):
        async with actors.builder.begin() as session:
            with pytest.raises(DBAPIError):
                async with session.begin_nested():
                    await session.execute(text(f'CREATE TABLE "{settings.schema}".forbidden (id integer)'))
            with pytest.raises(DBAPIError):
                async with session.begin_nested():
                    await session.execute(text(f'DELETE FROM "{settings.schema}".code_synonym'))
        prepared = await native._prepare(actors, settings.schema, importer)
        candidate = prepared[handoff.METRIC]
        assert (await run_publisher(settings, native._attempt(candidate)))["status"] == "succeeded"
        async with actors.builder.begin() as session:
            assert await session.scalar(text(f'SELECT count(*) FROM "{settings.schema}".code_catalog')) == (
                1 + len(native._payload(importer)["code_catalog"])
            )
            assert (
                await session.scalar(
                    text(f"SELECT effective_year FROM \"{settings.schema}\".code_catalog WHERE code_system='OTHER'")
                )
                == 2024
            )
        await native._assert_archive_roundtrip(monkeypatch, actors, settings, importer)
        async with actors.publisher.begin() as session:
            receipt = await handoff.read_catalog_handoff_outcome(session, candidate)
            await rollback(session, receipt, settings)
        async with actors.builder.begin() as session:
            assert await session.scalar(text(f'SELECT count(*) FROM "{settings.schema}".code_catalog')) == 1
            assert await session.scalar(text(f'SELECT effective_year FROM "{settings.schema}".code_catalog')) == 2024


async def _copy_archive_tables(session, source, destination, models):
    for model in models:
        projected = await binding.catalog_projection_model(session, source, model)
        columns = tuple(projected.__table__.columns.keys())
        projection = "SELECT " + ",".join(native.native._quoted(name) for name in columns)
        projection += " FROM " + storage.qualified(source, model.__tablename__)
        await native.native.native_copy_projection(
            session,
            projection,
            schema_name=destination,
            table_name=model.__tablename__,
            columns=columns,
            max_bytes=1024**2,
            timeout=30,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("old_input", [False, True])
async def test_legacy_year_survives_owned_upsert_old_input_and_archive(monkeypatch, old_input):
    monkeypatch.setattr(native, "_seed", _seed_migrated_catalog)
    monkeypatch.setattr(native, "_copy_archive_tables", _copy_archive_tables)
    async with native._resources(monkeypatch) as (actors, settings):
        async with actors.engine.begin() as connection:
            await connection.execute(
                text(
                    f'INSERT INTO "{settings.schema}".code_catalog '
                    "(code_system,code,source,display_name,effective_year) "
                    "VALUES ('POS','23','cms_place_of_service_code_set','Previous label',2022)"
                )
            )
        prepared = await native._prepare(actors, settings.schema, "code-sets")
        candidate = prepared[handoff.METRIC]
        if old_input:
            async with actors.engine.begin() as connection:
                await connection.execute(
                    text(f'ALTER TABLE "{candidate["input"]["schema_name"]}".code_catalog DROP COLUMN effective_year')
                )
            async with actors.publisher.begin() as session:
                candidate["tables"] = await storage.table_receipts(
                    session, candidate["input"]["schema_name"], storage.MODELS[:1]
                )
            candidate = await native._old_handoff(actors, candidate)
        assert (await run_publisher(settings, native._attempt(candidate)))["status"] == "succeeded"
        async with actors.builder.begin() as session:
            assert (
                await session.execute(
                    text(
                        f'SELECT display_name,effective_year FROM "{settings.schema}".code_catalog '
                        "WHERE code_system='POS' AND code='23'"
                    )
                )
            ).one() == ("Emergency", 2022)
        await native._assert_archive_roundtrip(monkeypatch, actors, settings, "code-sets")
        async with actors.publisher.begin() as session:
            receipt = await handoff.read_catalog_handoff_outcome(session, candidate)
            await rollback(session, receipt, settings)
        async with actors.builder.begin() as session:
            assert (
                await session.execute(
                    text(
                        f'SELECT display_name,effective_year FROM "{settings.schema}".code_catalog '
                        "WHERE code_system='POS' AND code='23'"
                    )
                )
            ).one() == ("Previous label", 2022)


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["bigint", "not_null", "default", "extra"])
async def test_legacy_field_validation_refuses_any_unreviewed_shape(monkeypatch, damage):
    monkeypatch.setattr(native, "_seed", _seed_migrated_catalog)
    async with native._resources(monkeypatch) as (actors, settings):
        alteration = {
            "bigint": "ALTER COLUMN effective_year TYPE bigint",
            "not_null": "ALTER COLUMN effective_year SET NOT NULL",
            "default": "ALTER COLUMN effective_year SET DEFAULT 2024",
            "extra": "ADD COLUMN unreviewed integer",
        }[damage]
        async with actors.engine.begin() as connection:
            await connection.execute(text(f'ALTER TABLE "{settings.schema}".code_catalog {alteration}'))
        async with actors.builder.begin() as session:
            with pytest.raises(RuntimeError, match="model columns differ"):
                await storage.require_tables(session, settings.schema, storage.MODELS)


@pytest.mark.asyncio
async def test_companion_migration_replay_preserves_rows_and_native_grants(monkeypatch):
    async with native._resources(monkeypatch) as (actors, settings):
        schema = settings.schema
        async with actors.engine.begin() as connection:
            await connection.execute(
                text(
                    f'INSERT INTO "{schema}".code_synonym (code_system,code,synonym,term_type,source) '
                    "VALUES ('OTHER','1','Synthetic synonym','alias','synthetic-reference')"
                )
            )
            await connection.execute(
                text(
                    f'INSERT INTO "{schema}".code_relationship '
                    "(from_system,from_code,relationship,to_system,to_code,source) "
                    "VALUES ('OTHER','1','related','OTHER','2','synthetic-reference')"
                )
            )
            before = await _table_state(connection, schema)
            await connection.run_sync(_upgrade)
            assert await _table_state(connection, schema) == before
        async with actors.builder.begin() as session:
            for name in ("code_synonym", "code_relationship"):
                assert await session.scalar(text(f'SELECT count(*) FROM "{schema}".{name}')) == 1
                await _require_delete_denied(session, schema, name)


async def _table_state(connection, schema):
    """Compare data and existing grants without using them as migration authority."""
    state_by_table = {}
    for name in ("code_synonym", "code_relationship"):
        rows = list((await connection.execute(text(f'SELECT to_jsonb(t) FROM "{schema}".{name} t'))).scalars())
        grants = list(
            (
                await connection.execute(
                    text(
                        "SELECT grantee,privilege_type,is_grantable FROM information_schema.table_privileges "
                        "WHERE table_schema=:schema AND table_name=:name ORDER BY 1,2,3"
                    ),
                    {"schema": schema, "name": name},
                )
            ).all()
        )
        state_by_table[name] = (json.dumps(rows, sort_keys=True), grants)
    return state_by_table


async def _require_delete_denied(session, schema, name):
    with pytest.raises(DBAPIError):
        async with session.begin_nested():
            await session.execute(text(f'DELETE FROM "{schema}".{name}'))
