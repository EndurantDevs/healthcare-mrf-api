# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""The additive pin migration installs guards and refuses to erase active authority."""

import importlib
import importlib.util
from pathlib import Path
from uuid import uuid4

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.schema import CreateIndex, CreateTable

from db import models
from db.connection import Database
from process import provider_profile_source_store as shared
from process import source_profile_result_archive as archive
from tests.source_profile_archive_support import _database_url, _seed
from tests.test_source_profile_result_archive_postgres import _prepared_case

florida = importlib.import_module("process.florida_mqa_profile")


def _migration_module():
    path = Path(__file__).resolve().parents[1] / "alembic/versions/20260922010000_source_profile_archive_pins.py"
    spec = importlib.util.spec_from_file_location("profile_pin_migration", path)
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    return migration


async def _assert_pin_policies(session, schema):
    assert await session.scalar(
        text("SELECT relrowsecurity AND relforcerowsecurity FROM pg_class WHERE oid=to_regclass(:name)"),
        {"name": f'"{schema}".provider_profile_source_pin'},
    )
    assert (
        await session.scalar(
            text("SELECT count(*) FROM pg_policy WHERE polrelid=to_regclass(:name)"),
            {"name": f'"{schema}".provider_profile_source_pin'},
        )
        == 3
    )


@pytest.mark.asyncio
async def test_pin_migration_adopts_runtime_created_table_and_index(monkeypatch):
    """Both importer-first and migration-first startup install the same guards."""
    engine = create_async_engine(_database_url())
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    schema = "profile_migration_" + uuid4().hex
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    migration = _migration_module()

    def upgrade(connection):
        with Operations.context(MigrationContext.configure(connection)):
            migration.upgrade()

    try:
        async with sessions() as session:
            transaction = await session.begin()
            try:
                await archive.native._create_model_family(
                    session,
                    archive.native.ReferenceFamilySpec(
                        "profile-migration", (*archive.MODELS, models.ProviderProfileSourcePublication)
                    ),
                    schema,
                )
                metadata = archive.native.MetaData(schema=schema)
                pin = models.ProviderProfileSourcePin.__table__.to_metadata(metadata, schema=schema)
                await session.execute(CreateTable(pin))
                for index in pin.indexes:
                    await session.execute(CreateIndex(index))
                await (await session.connection()).run_sync(upgrade)
                await archive.require_pin_guards(session, schema)
                await _assert_pin_policies(session, schema)
            finally:
                await transaction.rollback()
                assert await session.scalar(text("SELECT to_regnamespace(:schema) IS NULL"), {"schema": schema})
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_pin_migration_guards_and_retained_downgrade_refusal(monkeypatch):
    """Preserve live pin authority across an attempted downgrade."""
    engine = create_async_engine(_database_url())
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    schema = "profile_migration_" + uuid4().hex
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    migration = _migration_module()

    def run(connection, action):
        with Operations.context(MigrationContext.configure(connection)):
            getattr(migration, action)()

    try:
        async with sessions() as session, session.begin():
            await archive.native._create_model_family(
                session,
                archive.native.ReferenceFamilySpec(
                    "profile-migration", (*archive.MODELS, models.ProviderProfileSourcePublication)
                ),
                schema,
            )
            await (await session.connection()).run_sync(run, "upgrade")
            await archive.require_pin_guards(session, schema)
            await _assert_pin_policies(session, schema)
            await archive.pins.record_pin(
                session,
                schema=schema,
                source_key="massachusetts-borim",
                run_id="a" * 32,
                pin_id=uuid4(),
                purpose="export",
                authority={},
            )
        with pytest.raises(RuntimeError, match="pins remain"):
            async with sessions() as session, session.begin():
                await (await session.connection()).run_sync(run, "downgrade")
        async with sessions() as session, session.begin():
            await session.execute(text(f'DELETE FROM "{schema}".provider_profile_source_pin'))
            await (await session.connection()).run_sync(run, "downgrade")
            assert (
                await session.scalar(
                    text("SELECT to_regclass(:name)"), {"name": f"{schema}.provider_profile_source_pin"}
                )
                is None
            )
            await session.execute(
                text(
                    "DROP TABLE "
                    + ", ".join(
                        archive._table(schema, name)
                        for name in (*archive.TABLES, "provider_profile_source_publication")
                    )
                    + " RESTRICT"
                )
            )
            await session.execute(text(f'DROP SCHEMA "{schema}" RESTRICT'))
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_runtime_verifies_but_does_not_replace_migration_owned_guards(monkeypatch):
    async with _prepared_case("massachusetts-borim-profile") as case:
        role = "profile_runtime_" + uuid4().hex
        runtime_engine = None
        async with case.sessions() as session, session.begin():
            await session.execute(text(f'CREATE ROLE "{role}" NOLOGIN'))
            metadata = archive.native.MetaData(schema=case.source_schema)
            projection = models.ProviderProfileProjection.__table__.to_metadata(
                metadata,
                schema=case.source_schema,
            )
            await session.execute(archive.native.CreateTable(projection))
        try:
            async with case.sessions() as session, session.begin():
                await session.execute(text(f'GRANT USAGE ON SCHEMA "{case.source_schema}" TO "{role}"'))
                await session.execute(
                    text(
                        f'GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA "{case.source_schema}" TO "{role}"'
                    )
                )
            runtime_engine = create_async_engine(_database_url(), connect_args={"server_settings": {"role": role}})
            sessions = async_sessionmaker(runtime_engine, expire_on_commit=False)
            with monkeypatch.context() as patch:
                runtime_db = Database(engine=runtime_engine, session_factory=sessions)
                patch.setattr(shared, "db", runtime_db)
                patch.setattr(florida, "db", runtime_db)
                for model in (
                    *archive.MODELS,
                    models.ProviderProfileSourcePublication,
                    models.ProviderProfileSourcePin,
                    models.ProviderProfileProjection,
                ):
                    patch.setattr(model.__table__, "schema", case.source_schema)
                await shared.ensure_tables()
                await florida._ensure_tables()
            async with sessions() as session, session.begin():
                assert await session.scalar(text("SELECT current_user")) == role
                assert not await session.scalar(
                    text("SELECT has_database_privilege(current_user,current_database(),'CREATE')")
                )
                assert await session.scalar(
                    text(
                        "SELECT pg_get_userbyid(proowner)<>current_user FROM pg_proc WHERE oid=to_regprocedure(:name)"
                    ),
                    {"name": f'"{case.source_schema}".provider_profile_pinned_run_guard()'},
                )
                await archive.require_pin_guards(session, case.source_schema)
            with pytest.raises(DBAPIError, match="must be owner|permission denied"):
                async with sessions() as session, session.begin():
                    await session.execute(text(next(archive.pins.pin_guard_statements(case.source_schema))))
        finally:
            if runtime_engine is not None:
                await runtime_engine.dispose()
            async with case.sessions() as session, session.begin():
                await session.execute(text(f'DROP TABLE "{case.source_schema}".provider_profile_projection'))
                await session.execute(text(f'REVOKE ALL ON ALL TABLES IN SCHEMA "{case.source_schema}" FROM "{role}"'))
                await session.execute(text(f'REVOKE ALL ON SCHEMA "{case.source_schema}" FROM "{role}"'))
                await session.execute(text(f'DROP ROLE "{role}"'))


async def _migrate_as_ordinary_owner(session, schema, ordinary_role):
    """Run the real migration as the existing native family's ordinary owner."""
    await archive.native._create_model_family(
        session,
        archive.native.ReferenceFamilySpec(
            "profile-migration", (*archive.MODELS, models.ProviderProfileSourcePublication)
        ),
        schema,
    )
    await session.execute(text(f'ALTER SCHEMA "{schema}" OWNER TO "{ordinary_role}"'))
    for name in (*archive.TABLES, "provider_profile_source_publication"):
        await session.execute(text(f'ALTER TABLE "{schema}"."{name}" OWNER TO "{ordinary_role}"'))
    await session.execute(text(f'SET LOCAL ROLE "{ordinary_role}"'))

    def upgrade(connection):
        with Operations.context(MigrationContext.configure(connection)):
            _migration_module().upgrade()

    await (await session.connection()).run_sync(upgrade)
    table_owner = (
        await session.execute(
            text("SELECT pg_get_userbyid(relowner),relacl FROM pg_class WHERE oid=to_regclass(:name)"),
            {"name": f'"{schema}".provider_profile_source_pin'},
        )
    ).one()
    assert tuple(table_owner) == (ordinary_role, None)
    for function in ("provider_profile_pinned_run_guard", "provider_profile_pinned_truncate_guard"):
        function_owner = (
            await session.execute(
                text("SELECT pg_get_userbyid(proowner),proacl FROM pg_proc WHERE oid=to_regprocedure(:name)"),
                {"name": f'"{schema}".{function}()'},
            )
        ).one()
        assert tuple(function_owner) == (ordinary_role, None)


async def _downgrade_pins(session):
    def downgrade(connection):
        with Operations.context(MigrationContext.configure(connection)):
            _migration_module().downgrade()

    await (await session.connection()).run_sync(downgrade)


async def _assert_inherited_pin_access(session, schema, ordinary_role, publisher_role):
    """Exercise inherited native authority, not per-object publisher grants."""
    await session.execute(text(f'SET LOCAL ROLE "{publisher_role}"'))
    assert await session.scalar(text("SELECT current_user")) == publisher_role
    assert await session.scalar(text("SELECT pg_has_role(current_user,:ordinary,'USAGE')"), {"ordinary": ordinary_role})
    await archive.require_pin_guards(session, schema)
    for function in ("provider_profile_pinned_run_guard", "provider_profile_pinned_truncate_guard"):
        assert await session.scalar(
            text("SELECT has_function_privilege(current_user,:name,'EXECUTE')"), {"name": f'"{schema}".{function}()'}
        )
    run_id = await _seed(session, schema, "massachusetts-borim-profile")
    await _assert_export_pin_access(session, schema, run_id)
    await _assert_adoption_pin_access(session, schema, run_id, ordinary_role, publisher_role)


async def _assert_export_pin_access(session, schema, run_id):
    """The ordinary native pin path remains available through inheritance."""
    pin_id = uuid4()
    await archive.pins.record_pin(
        session,
        schema=schema,
        source_key="massachusetts-borim",
        run_id=run_id,
        pin_id=pin_id,
        purpose="export",
        authority={"root_run_id": run_id, "run_ids": [run_id]},
    )
    assert [pin["run_id"] for pin in await archive._pin_group(session, schema, pin_id)] == [run_id]
    with pytest.raises(DBAPIError, match="retained run is pinned"):
        async with session.begin_nested():
            await session.execute(text(f"UPDATE \"{schema}\".provider_profile_fact SET display='blocked'"))
    assert (
        await archive.release_source_pin(
            session, schema=schema, importer_id="massachusetts-borim-profile", run_id=run_id, pin_id=pin_id
        )
        == "released"
    )
    assert await session.scalar(text(f'SELECT count(*) FROM "{schema}".provider_profile_source_pin')) == 0
    await session.execute(text(f"UPDATE \"{schema}\".provider_profile_fact SET display='unpinned'"))


async def _assert_adoption_pin_access(session, schema, run_id, ordinary_role, publisher_role):
    """Only the publisher can change adoption rows; ordinary readers still see seals."""
    adoption_id = uuid4()
    await archive.pins.record_pin(
        session,
        schema=schema,
        source_key="massachusetts-borim",
        run_id=run_id,
        pin_id=adoption_id,
        purpose="adoption",
        authority={"root_run_id": run_id, "run_ids": [run_id]},
    )
    assert [pin["run_id"] for pin in await archive._pin_group(session, schema, adoption_id)] == [run_id]
    await session.execute(text(f'SET LOCAL ROLE "{ordinary_role}"'))
    await _assert_ordinary_adoption_denial(session, schema, run_id, adoption_id)
    await session.execute(text(f'SET LOCAL ROLE "{publisher_role}"'))
    assert (
        await archive.release_source_pin(
            session, schema=schema, importer_id="massachusetts-borim-profile", run_id=run_id, pin_id=adoption_id
        )
        == "released"
    )


async def _assert_ordinary_adoption_denial(session, schema, run_id, adoption_id):
    """An ordinary owner reads the seal but cannot forge, alter or release it."""
    assert [
        pin_row[0]
        for pin_row in await session.execute(text(f'SELECT run_id FROM "{schema}".provider_profile_source_pin'))
    ] == [run_id]
    with pytest.raises(DBAPIError):
        async with session.begin_nested():
            await archive.pins.record_pin(
                session,
                schema=schema,
                source_key="massachusetts-borim",
                run_id=run_id,
                pin_id=uuid4(),
                purpose="adoption",
                authority={},
            )
    assert (
        await session.execute(
            text(f"UPDATE \"{schema}\".provider_profile_source_pin SET purpose='export' WHERE pin_id=:pin"),
            {"pin": str(adoption_id)},
        )
    ).rowcount == 0
    assert (
        await session.execute(
            text(f'DELETE FROM "{schema}".provider_profile_source_pin WHERE pin_id=:pin'),
            {"pin": str(adoption_id)},
        )
    ).rowcount == 0
    with pytest.raises(archive.SourceProfileArchiveError, match="pin authority is unavailable"):
        await archive.release_source_pin(
            session, schema=schema, importer_id="massachusetts-borim-profile", run_id=run_id, pin_id=adoption_id
        )
    await _assert_mixed_pin_group_denied(session, schema, adoption_id)
    with pytest.raises(RuntimeError, match="pins remain"):
        async with session.begin_nested():
            await _downgrade_pins(session)
    with pytest.raises(DBAPIError, match="retained run is pinned"):
        async with session.begin_nested():
            await session.execute(text(f"UPDATE \"{schema}\".provider_profile_fact SET display='blocked'"))


async def _assert_mixed_pin_group_denied(session, schema, adoption_id):
    """A lockable export row cannot conceal another visible adoption row."""
    sibling_run_id = "b" * 32
    await archive.pins.record_pin(
        session,
        schema=schema,
        source_key="massachusetts-borim",
        run_id=sibling_run_id,
        pin_id=adoption_id,
        purpose="export",
        authority={"root_run_id": sibling_run_id, "run_ids": [sibling_run_id]},
    )
    with pytest.raises(archive.SourceProfileArchiveError, match="pin authority is unavailable"):
        await archive.release_source_pin(
            session,
            schema=schema,
            importer_id="massachusetts-borim-profile",
            run_id=sibling_run_id,
            pin_id=adoption_id,
        )
    await session.execute(
        text(f'DELETE FROM "{schema}".provider_profile_source_pin WHERE pin_id=:pin AND run_id=:run'),
        {"pin": str(adoption_id), "run": sibling_run_id},
    )


async def _assert_limited_replacement_refused(session, schema, limited_role):
    await session.execute(text(f'SET LOCAL ROLE "{limited_role}"'))
    assert await session.scalar(text(f'SELECT count(*) FROM "{schema}".provider_profile_source_pin')) == 0
    with pytest.raises(DBAPIError, match="permission denied"):
        async with session.begin_nested():
            await session.execute(text(f'DELETE FROM "{schema}".provider_profile_source_pin'))
    with pytest.raises(DBAPIError, match="must be owner|permission denied"):
        async with session.begin_nested():
            await session.execute(text(next(archive.pins.pin_guard_statements(schema))))


@pytest.mark.asyncio
async def test_publisher_inherits_real_migration_owned_pin_authority_without_object_grants(monkeypatch):
    """Roll back every uniquely named fixture role/object, including on failure."""
    engine = create_async_engine(_database_url())
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    token = uuid4().hex
    schema = "profile_inheritance_" + token
    ordinary_role = "profile_owner_" + token
    publisher_role = "profile_publisher_" + token
    limited_role = "profile_limited_" + token
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    try:
        async with sessions() as session:
            transaction = await session.begin()
            try:
                for role in (ordinary_role, publisher_role, limited_role):
                    await session.execute(
                        text(f'CREATE ROLE "{role}" NOLOGIN INHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE')
                    )
                await session.execute(text(f'GRANT "{ordinary_role}" TO "{publisher_role}"'))
                await _migrate_as_ordinary_owner(session, schema, ordinary_role)
                await session.execute(text(f'GRANT USAGE ON SCHEMA "{schema}" TO "{limited_role}"'))
                await session.execute(
                    text(f'GRANT SELECT ON "{schema}".provider_profile_source_pin TO "{limited_role}"')
                )
                await _assert_inherited_pin_access(session, schema, ordinary_role, publisher_role)
                await _assert_limited_replacement_refused(session, schema, limited_role)
                await session.execute(text(f'SET LOCAL ROLE "{ordinary_role}"'))
                await _downgrade_pins(session)
                assert (
                    await session.scalar(
                        text("SELECT to_regclass(:name)"), {"name": f'"{schema}".provider_profile_source_pin'}
                    )
                    is None
                )
            finally:
                await transaction.rollback()
                assert await session.scalar(text("SELECT to_regnamespace(:schema) IS NULL"), {"schema": schema})
                assert await session.scalar(
                    text("SELECT NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=ANY(CAST(:roles AS text[])))"),
                    {"roles": [ordinary_role, publisher_role, limited_role]},
                )
    finally:
        await engine.dispose()
