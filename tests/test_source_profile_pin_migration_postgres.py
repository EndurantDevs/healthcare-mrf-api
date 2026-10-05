# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""The additive pin migration installs guards and refuses to erase active authority."""

import asyncio
import importlib
import importlib.util
from contextlib import asynccontextmanager
from pathlib import Path
from uuid import uuid4

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from asyncpg import ObjectNotInPrerequisiteStateError, UniqueViolationError
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


def _migration_module(*, statement=False):
    filename = (
        "20261005030000_source_profile_statement_pins.py"
        if statement
        else "20260922010000_source_profile_archive_pins.py"
    )
    path = Path(__file__).resolve().parents[1] / "alembic/versions" / filename
    spec = importlib.util.spec_from_file_location("profile_pin_migration", path)
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    return migration


def _apply_pin_revision(connection, action, *, statement=False):
    with Operations.context(MigrationContext.configure(connection)):
        getattr(_migration_module(statement=statement), action)()


def _run_pin_migrations(connection, action):
    for statement in (False, True) if action == "upgrade" else (True, False):
        _apply_pin_revision(connection, action, statement=statement)


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
                await (await session.connection()).run_sync(_run_pin_migrations, "upgrade")
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

    try:
        async with sessions() as session, session.begin():
            await archive.native._create_model_family(
                session,
                archive.native.ReferenceFamilySpec(
                    "profile-migration", (*archive.MODELS, models.ProviderProfileSourcePublication)
                ),
                schema,
            )
            await (await session.connection()).run_sync(_run_pin_migrations, "upgrade")
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
                await (await session.connection()).run_sync(_run_pin_migrations, "downgrade")
        async with sessions() as session, session.begin():
            await session.execute(text(f'DELETE FROM "{schema}".provider_profile_source_pin'))
            await (await session.connection()).run_sync(_run_pin_migrations, "downgrade")
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


async def _migrate_as_ordinary_owner(session, schema, ordinary_role, *, statement_as_administrator=False):
    """Run the real migration as the existing native family's ordinary owner."""
    administrator = await session.scalar(text("SELECT current_user"))
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

    await (await session.connection()).run_sync(_apply_pin_revision, "upgrade")
    if statement_as_administrator:
        await session.execute(text("RESET ROLE"))
        assert await session.scalar(text("SELECT current_user")) == administrator != ordinary_role
    await (await session.connection()).run_sync(_apply_pin_revision, "upgrade", statement=True)
    await session.execute(text(f'SET LOCAL ROLE "{ordinary_role}"'))
    table_owner = (
        await session.execute(
            text("SELECT pg_get_userbyid(relowner),relacl FROM pg_class WHERE oid=to_regclass(:name)"),
            {"name": f'"{schema}".provider_profile_source_pin'},
        )
    ).one()
    assert tuple(table_owner) == (ordinary_role, None)
    for function in (
        "provider_profile_pinned_run_guard",
        "provider_profile_pinned_truncate_guard",
        "provider_profile_attached_pin_guard",
    ):
        function_owner = (
            await session.execute(
                text("SELECT pg_get_userbyid(proowner),proacl FROM pg_proc WHERE oid=to_regprocedure(:name)"),
                {"name": f'"{schema}".{function}()'},
            )
        ).one()
        assert tuple(function_owner) == (ordinary_role, None)


async def _downgrade_pins(session):
    await (await session.connection()).run_sync(_run_pin_migrations, "downgrade")


async def _assert_inherited_pin_access(session, schema, ordinary_role, publisher_role):
    """Exercise inherited native authority, not per-object publisher grants."""
    await session.execute(text(f'SET LOCAL ROLE "{publisher_role}"'))
    assert await session.scalar(text("SELECT current_user")) == publisher_role
    assert await session.scalar(text("SELECT pg_has_role(current_user,:ordinary,'USAGE')"), {"ordinary": ordinary_role})
    await archive.require_pin_guards(session, schema)
    for function in (
        "provider_profile_pinned_run_guard",
        "provider_profile_pinned_truncate_guard",
        "provider_profile_attached_pin_guard",
    ):
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
@pytest.mark.parametrize("statement_as_administrator", (False, True), ids=("owner-upgrade", "administrator-upgrade"))
async def test_publisher_inherits_real_migration_owned_pin_authority_without_object_grants(
    monkeypatch, statement_as_administrator
):
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
                await _migrate_as_ordinary_owner(
                    session, schema, ordinary_role, statement_as_administrator=statement_as_administrator
                )
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


@pytest.mark.asyncio
async def test_statement_pin_downgrade_refuses_attached_tables_then_restores_cleanly(monkeypatch):
    async with _attached_child("provider_profile_artifact") as (case, session, parent, child, _name):
        monkeypatch.setenv("HLTHPRT_DB_SCHEMA", case.destination_schema)
        monkeypatch.delenv("DB_SCHEMA", raising=False)
        with pytest.raises(DBAPIError, match="attached tables remain") as refused:
            async with session.begin_nested():
                await (await session.connection()).run_sync(_apply_pin_revision, "downgrade", statement=True)
        assert refused.value.orig.sqlstate == "55000"
        await archive.require_pin_guards(session, case.destination_schema)
        await session.execute(text(f"ALTER TABLE {child} NO INHERIT {parent}"))
        await (await session.connection()).run_sync(_apply_pin_revision, "downgrade", statement=True)
        await (await session.connection()).run_sync(_apply_pin_revision, "upgrade", statement=True)
        await archive.require_pin_guards(session, case.destination_schema)


async def _other_run(session, case):
    return await session.scalar(
        text(f'SELECT run_id FROM "{case.source_schema}".provider_profile_artifact WHERE run_id<>:run'),
        {"run": case.incoming},
    )


async def _artifact_records(session, table):
    return [tuple(artifact) for artifact in await session.execute(text(f"SELECT * FROM {table} ORDER BY artifact_id"))]


async def _copy_artifacts(session, case, other):
    raw = (await (await session.connection()).get_raw_connection()).driver_connection
    await raw.copy_records_to_table(
        "provider_profile_artifact",
        schema_name=case.source_schema,
        columns=(
            "artifact_id",
            "run_id",
            "source_key",
            "file_name",
            "source_url",
            "category",
            "content_sha256",
            "content_bytes",
        ),
        records=[
            (uuid4().hex * 2, run, "batch", "manifest", "https://example.org/copy", "profile", "a" * 64, 1)
            for run in (other, case.incoming)
        ],
    )


async def _mutate_artifacts(session, case, operation, other):
    table = f'"{case.source_schema}".provider_profile_artifact'
    parameter_by_field = {
        "first": uuid4().hex * 2,
        "second": uuid4().hex * 2,
        "other": other,
        "pinned": case.incoming,
        "sha": "a" * 64,
    }
    if operation == "copy":
        await _copy_artifacts(session, case, other)
        return
    statement_by_operation = {
        "insert_select": f"INSERT INTO {table}(artifact_id,run_id,source_key,file_name,source_url,category,content_sha256,content_bytes) "
        "SELECT id,run,'batch','manifest','https://example.org/copy','profile',:sha,1 "
        "FROM (VALUES (:first,:other),(:second,:pinned)) AS incoming(id,run)",
        "delete": f"DELETE FROM {table}",
        "update_old": f"UPDATE {table} SET file_name='changed'",
        "update_new": f"UPDATE {table} SET run_id=:pinned WHERE run_id=:other",
        "upsert": f"INSERT INTO {table} SELECT * FROM {table} WHERE run_id=:pinned "
        "ON CONFLICT(artifact_id) DO UPDATE SET file_name='changed'",
    }
    await session.execute(text(statement_by_operation[operation]), parameter_by_field)


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["copy", "insert_select", "delete", "update_old", "update_new", "upsert"])
async def test_mixed_batch_rolls_back_every_changed_row(operation):
    async with _prepared_case("massachusetts-borim-profile") as case:
        table = f'"{case.source_schema}".provider_profile_artifact'
        async with case.sessions() as session, session.begin():
            before_artifacts = await _artifact_records(session, table)
            other = await _other_run(session, case)
            with pytest.raises((DBAPIError, ObjectNotInPrerequisiteStateError), match="retained run is pinned"):
                async with session.begin_nested():
                    await _mutate_artifacts(session, case, operation, other)
            assert await _artifact_records(session, table) == before_artifacts
            assert (await session.execute(text(f"UPDATE {table} SET file_name='unchanged' WHERE false"))).rowcount == 0
            assert (
                await session.execute(
                    text(f"INSERT INTO {table} SELECT * FROM {table} WHERE run_id=:run ON CONFLICT DO NOTHING"),
                    {"run": case.incoming},
                )
            ).rowcount == 0


async def _wait_for_advisory(session, pid):
    async with asyncio.timeout(3):
        while not await session.scalar(
            text("SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=:pid AND locktype='advisory' AND NOT granted)"),
            {"pid": pid},
        ):
            await asyncio.sleep(0.01)


async def _pin_writer_order(case, writer, pinner, control, pin_first, pin_id):
    other = await _other_run(control, case)
    writer_transaction = await writer.begin()
    pin_transaction = await pinner.begin()
    writer_pid = await writer.scalar(text("SELECT pg_backend_pid()"))
    pin_pid = await pinner.scalar(text("SELECT pg_backend_pid()"))
    update = text(
        f"UPDATE \"{case.source_schema}\".provider_profile_artifact SET file_name='written' WHERE run_id=:run"
    )
    task = None

    async def pin():
        await archive.pins.record_pin(
            pinner,
            schema=case.source_schema,
            source_key="synthetic",
            run_id=other,
            pin_id=pin_id,
            purpose="export",
            authority={},
        )

    try:
        if pin_first:
            await pin()
            task = asyncio.create_task(writer.execute(update, {"run": other}))
            await _wait_for_advisory(control, writer_pid)
            await pin_transaction.commit()
            with pytest.raises(DBAPIError, match="retained run is pinned"):
                await task
            await writer_transaction.rollback()
        else:
            await writer.execute(update, {"run": other})
            task = asyncio.create_task(pin())
            await _wait_for_advisory(control, pin_pid)
            await writer_transaction.commit()
            await task
            await pin_transaction.commit()
        await control.rollback()
        actual = await control.scalar(
            text(f'SELECT file_name FROM "{case.source_schema}".provider_profile_artifact WHERE run_id=:run'),
            {"run": other},
        )
        assert actual == ("manifest.json" if pin_first else "written")
    finally:
        if task is not None:
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        if writer_transaction.is_active:
            await writer_transaction.rollback()
        if pin_transaction.is_active:
            await pin_transaction.rollback()


@pytest.mark.asyncio
@pytest.mark.parametrize("pin_first", [True, False])
async def test_pin_and_writer_serialize_with_fresh_post_wait_visibility(pin_first):
    async with _prepared_case("massachusetts-borim-profile") as case:
        pin_id = uuid4()
        try:
            async with case.sessions() as control, case.sessions() as writer, case.sessions() as pinner:
                await _pin_writer_order(case, writer, pinner, control, pin_first, pin_id)
        finally:
            async with case.sessions() as session, session.begin():
                await session.execute(
                    text(f'DELETE FROM "{case.source_schema}".provider_profile_source_pin WHERE pin_id=:pin'),
                    {"pin": str(pin_id)},
                )


async def _assert_isolation_refusals(session, case):
    statements = (
        f"UPDATE \"{case.source_schema}\".provider_profile_artifact SET file_name='changed'",
        f'TRUNCATE "{case.source_schema}".provider_profile_artifact',
    )
    for statement in statements:
        with pytest.raises(DBAPIError, match="require read committed"):
            async with session.begin_nested():
                await session.execute(text(statement))
    with pytest.raises(ValueError, match="pin acquisition requires read committed"):
        await archive.pins.record_pin(
            session,
            schema=case.source_schema,
            source_key="synthetic",
            run_id=case.incoming,
            pin_id=uuid4(),
            purpose="export",
            authority={},
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("isolation", ["REPEATABLE READ", "SERIALIZABLE"])
async def test_snapshot_isolation_refuses_payload_writes_and_pin_acquisition(isolation):
    async with _prepared_case("massachusetts-borim-profile") as case:
        async with case.sessions() as session, session.begin():
            await session.execute(text(f"SET TRANSACTION ISOLATION LEVEL {isolation}"))
            await archive.require_pin_guards(session, case.source_schema)
            await _assert_isolation_refusals(session, case)


async def _tamper_guard(session, schema, fault):
    statements_by_fault = {
        "stable": (f'ALTER FUNCTION "{schema}".provider_profile_pinned_run_guard() STABLE',),
        "search_path": (f'ALTER FUNCTION "{schema}".provider_profile_pinned_run_guard() RESET search_path',),
        "disabled": (
            f'ALTER TABLE "{schema}".provider_profile_fact DISABLE TRIGGER provider_profile_pinned_run_guard_update',
        ),
        "transition": (
            f'DROP TRIGGER provider_profile_pinned_run_guard_update ON "{schema}".provider_profile_fact',
            f'CREATE TRIGGER provider_profile_pinned_run_guard_update AFTER UPDATE ON "{schema}".provider_profile_fact '
            f'REFERENCING OLD TABLE AS wrong_old NEW TABLE AS profile_guard_new FOR EACH STATEMENT EXECUTE FUNCTION "{schema}".provider_profile_pinned_run_guard()',
        ),
        "legacy": (
            f'CREATE TRIGGER provider_profile_pinned_run_guard BEFORE UPDATE ON "{schema}".provider_profile_fact '
            f'FOR EACH ROW EXECUTE FUNCTION "{schema}".provider_profile_pinned_run_guard()',
        ),
    }
    for statement in statements_by_fault[fault]:
        await session.execute(text(statement))


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["stable", "disabled", "transition", "legacy", "search_path"])
async def test_catalog_attestation_rejects_changed_statement_semantics(fault):
    async with _prepared_case("massachusetts-borim-profile") as case:
        async with case.sessions() as session, session.begin():
            async with session.begin_nested() as savepoint:
                await _tamper_guard(session, case.source_schema, fault)
                with pytest.raises(archive.SourceProfileArchiveError, match="statement seal is unavailable"):
                    await archive.require_pin_guards(session, case.source_schema)
                await savepoint.rollback()
            await archive.require_pin_guards(session, case.source_schema)


@pytest.mark.asyncio
@pytest.mark.parametrize("shadow", ["cardinality", "operator"])
async def test_hostile_search_path_cannot_bypass_retained_run_seal(shadow):
    async with _prepared_case("massachusetts-borim-profile") as case:
        async with case.sessions() as session, session.begin():
            trap_schema = "pin_shadow_" + uuid4().hex
            async with session.begin_nested() as savepoint:
                await _assert_shadowed_guard_refuses(session, case, trap_schema, shadow)
                await savepoint.rollback()


async def _assert_shadowed_guard_refuses(session, case, trap_schema, shadow):
    await session.execute(text(f'CREATE SCHEMA "{trap_schema}"'))
    if shadow == "cardinality":
        await session.execute(
            text(f"CREATE FUNCTION \"{trap_schema}\".cardinality(text[]) RETURNS integer LANGUAGE sql AS 'SELECT 0'")
        )
    else:
        await session.execute(
            text(
                f"CREATE FUNCTION \"{trap_schema}\".always_equal(integer,integer) RETURNS boolean LANGUAGE sql AS 'SELECT true'"
            )
        )
        await session.execute(
            text(
                f'CREATE OPERATOR "{trap_schema}".= (LEFTARG=integer,RIGHTARG=integer,FUNCTION="{trap_schema}".always_equal)'
            )
        )
    await session.execute(text(f'SET LOCAL search_path="{trap_schema}",pg_catalog'))
    with pytest.raises(DBAPIError, match="retained run is pinned"):
        async with session.begin_nested():
            await session.execute(
                text(f"UPDATE \"{case.source_schema}\".provider_profile_artifact SET file_name='changed'")
            )


@asynccontextmanager
async def _attached_child(table_name):
    """Make a transaction-local inherited relation; rollback owns every added object."""
    async with _prepared_case("massachusetts-borim-profile") as case:
        async with case.sessions() as session, session.begin() as transaction:
            try:
                child_name = "pin_child_" + uuid4().hex
                child = f'"{case.prepared.ownership.schema_name}"."{child_name}"'
                original = f'"{case.prepared.ownership.schema_name}"."{table_name}"'
                parent = f'"{case.destination_schema}"."{table_name}"'
                await session.execute(text(f"CREATE TABLE {child} (LIKE {original} INCLUDING ALL)"))
                await session.execute(text(f"INSERT INTO {child} SELECT * FROM {original}"))
                await session.execute(text(f"ALTER TABLE {child} INHERIT {parent}"))
                yield case, session, parent, child, child_name
            finally:
                await transaction.rollback()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("table_name", "changed_column"),
    [
        ("provider_profile_import_run", None),
        ("provider_profile_artifact", None),
        ("provider_profile_artifact", "artifact_id"),
        ("provider_profile_source_record", None),
        ("provider_profile_source_record", "record_id"),
        ("provider_profile_fact", None),
    ],
)
async def test_parent_inserts_preserve_primary_and_unique_keys_across_children(table_name, changed_column):
    async with _attached_child(table_name) as (_case, session, parent, child, _name):
        model = next(model for model in archive.MODELS if model.__tablename__ == table_name)
        columns = ",".join(f'"{column.name}"' for column in model.__table__.columns)
        values = ",".join(
            ":replacement" if column.name == changed_column else f'"{column.name}"'
            for column in model.__table__.columns
        )
        with pytest.raises(DBAPIError, match="attached key conflicts"):
            async with session.begin_nested():
                await session.execute(
                    text(f"INSERT INTO {parent} ({columns}) SELECT {values} FROM {child}"),
                    {"replacement": uuid4().hex * 2},
                )


async def _write_attached_key_collision(operation, case, session, parent, child):
    if operation == "copy":
        raw = (await (await session.connection()).get_raw_connection()).driver_connection
        rows = [tuple(row) for row in await session.execute(text(f"SELECT * FROM {child}"))]
        await raw.copy_records_to_table("provider_profile_artifact", schema_name=case.destination_schema, records=rows)
    elif operation == "upsert":
        await session.execute(
            text(
                f"INSERT INTO {parent} SELECT * FROM {child} ON CONFLICT(artifact_id) DO UPDATE SET file_name='changed'"
            )
        )
    else:
        await session.execute(
            text(
                f"UPDATE ONLY {parent} SET artifact_id=(SELECT artifact_id FROM {child}) WHERE artifact_id=(SELECT artifact_id FROM ONLY {parent} LIMIT 1)"
            )
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ("copy", "upsert", "update"))
async def test_parent_copy_upsert_and_update_cannot_duplicate_attached_keys(operation):
    async with _attached_child("provider_profile_artifact") as (case, session, parent, child, _name):
        with pytest.raises((DBAPIError, UniqueViolationError), match="attached key conflicts"):
            async with session.begin_nested():
                await _write_attached_key_collision(operation, case, session, parent, child)
        assert (
            await session.execute(
                text(
                    f"INSERT INTO {parent} SELECT * FROM ONLY {parent} ON CONFLICT(artifact_id) DO UPDATE SET file_name='ordinary'"
                )
            )
        ).rowcount > 0


async def _record_attachment_seal(session, case, parent, child, child_name):
    parent_oid = await session.scalar(text("SELECT CAST(:relation AS regclass)::oid"), {"relation": parent})
    child_oid = await session.scalar(text("SELECT CAST(:relation AS regclass)::oid"), {"relation": child})
    pin_id = uuid4()
    publication_by_field = {
        "contract": "source-profile-attachment.v2",
        "created_run_ids": [case.incoming],
        "reused_run_ids": [],
        "parents": [["provider_profile_artifact", parent_oid]],
        "children": [["provider_profile_artifact", child_name, child_oid]],
        "ancestor_relations": [],
    }
    await archive.pins.record_pin(
        session,
        schema=case.destination_schema,
        source_key="massachusetts-borim",
        run_id=case.incoming,
        pin_id=pin_id,
        purpose="adoption",
        authority={"created_here": True, "validation": {"publication": publication_by_field}},
    )
    return pin_id


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ("delete", "purpose", "authority", "truncate"))
async def test_attachment_owner_seal_requires_detachment_before_mutation(operation):
    async with _attached_child("provider_profile_artifact") as (case, session, parent, child, child_name):
        pin_id = await _record_attachment_seal(session, case, parent, child, child_name)
        pin_table = f'"{case.destination_schema}".provider_profile_source_pin'
        statement_by_operation = {
            "delete": f"DELETE FROM {pin_table} WHERE pin_id=:pin",
            "purpose": f"UPDATE {pin_table} SET purpose='export' WHERE pin_id=:pin",
            "authority": f"UPDATE {pin_table} SET authority_json='{{}}'::json WHERE pin_id=:pin",
            "truncate": f"TRUNCATE {pin_table}",
        }
        with pytest.raises(DBAPIError, match="attached adoption seal is retained"):
            async with session.begin_nested():
                await session.execute(text(statement_by_operation[operation]), {"pin": str(pin_id)})
        for statement in (
            f"UPDATE {parent} SET file_name='changed' WHERE run_id=:run",
            f"DELETE FROM {parent} WHERE run_id=:run",
        ):
            with pytest.raises(DBAPIError, match="retained run is pinned"):
                async with session.begin_nested():
                    await session.execute(text(statement), {"run": case.incoming})
        await session.execute(text(f"ALTER TABLE {child} NO INHERIT {parent}"))
        assert (
            await session.execute(text(f"DELETE FROM {pin_table} WHERE pin_id=:pin"), {"pin": str(pin_id)})
        ).rowcount == 1
