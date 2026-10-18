# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native distinct-owner component proof; not an enrolled-runtime or serving-composite claim."""

import asyncio
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from process import cms_doctors_immutable as immutable
from process import cms_doctors_preparation as preparation
from process import provider_directory_generic_source_policy as source
from process import reference_family_archive as archive
from tests.cms_doctors_component_postgres_support import ordinary_doctors_reader
from tests.cms_doctors_preparation_postgres_support import doctors_database, stage_family


@pytest.mark.asyncio
async def test_distinct_source_and_retention_owners_keep_source_writer_and_read_contracts(monkeypatch):
    async with doctors_database(monkeypatch, cms_active=False) as fixture:
        component = fixture.component
        owner, retained = component.owner, component.retention_owner
        async with component.admin_engine.connect() as admin:
            assert owner != retained
            assert (
                await admin.scalar(
                    text(
                        "SELECT pg_has_role(:publisher,:retained,'SET') AND pg_has_role(:publisher,:physical,'USAGE') "
                        "AND NOT pg_has_role(:publisher,:physical,'SET')"
                    ),
                    {"publisher": component.retained_publisher, "retained": retained, "physical": owner},
                )
                is True
            )
            assert (
                await admin.scalar(
                    text(
                        "SELECT nspowner=CAST(:owner AS regrole) FROM pg_namespace WHERE nspname='hp_snapshot_retention'"
                    ).bindparams(owner=retained)
                )
                is True
            )
        context = await stage_family(component.database, component.policy.schema)
        async with preparation.prepare_cms_doctors_generation(context) as prepared:
            async with component.database.transaction():
                authority = await preparation.apply_prepared_cms_doctors_generation(prepared)
            async with ordinary_doctors_reader(component.database) as reader:
                assert (
                    await immutable.capture_doctors_serving_generation(reader, component.policy.schema)
                    == authority.serving_generation
                )
        await _assert_retained_read_only(component, authority)
        assert component.authority.local_generation + 1 == authority.local_generation


async def _assert_retained_read_only(component, authority):
    retained_engine = create_async_engine(
        component.admin_engine.url.set(username=component.retained_publisher, password=None)
    )
    try:
        async with async_sessionmaker(retained_engine)() as session, session.begin():
            assert (
                await immutable.capture_doctors_serving_generation(session, component.policy.schema)
                == authority.serving_generation
            )
            with pytest.raises(ValueError, match="publisher_connection_required"):
                await immutable.capture_doctors_predecessor(session, component.policy.schema)
            with pytest.raises(DBAPIError) as refused:
                async with session.begin_nested():
                    await session.execute(text(f'SET LOCAL ROLE "{component.owner}"'))
            assert refused.value.orig.sqlstate == "42501"
    finally:
        await retained_engine.dispose()


@pytest.mark.asyncio
async def test_archive_schema_uses_original_publisher_then_physical_owner_on_same_transaction(monkeypatch):
    async with doctors_database(monkeypatch, cms_active=False) as fixture:
        component = fixture.component
        async with component.database.transaction() as session:
            original = source.source_copy_session()
            native = source.bound_publisher().connection
            backend = await native.fetchval("SELECT pg_backend_pid()")
            assert not await native.fetchval("SELECT has_database_privilege(current_user,current_database(),'CREATE')")
            ownership = await archive.precreate_reference_family_restore(
                session,
                importer_id="cms-doctors",
                dataset_id=uuid4(),
            )
            assert session is original and await native.fetchval("SELECT pg_backend_pid()") == backend
            assert await native.fetchval("SELECT current_user::text") == component.owner
            assert await native.fetchval(
                "SELECT nspowner=$2::regrole FROM pg_namespace WHERE nspname=$1", ownership.schema_name, component.owner
            )
            assert await native.fetchval(
                "SELECT bool_and(relowner=$2::regrole) FROM pg_class WHERE oid=ANY($1::oid[])",
                [oid for _, oid in ownership.relation_oids],
                component.owner,
            )
            for model in immutable._models():
                name = str(model.__tablename__)
                assert name in dict(ownership.relation_oids)
                columns = tuple(str(column.name) for column in model.__table__.columns)
                await archive.native_copy_projection(
                    session,
                    "SELECT "
                    + ",".join('"' + column + '"' for column in columns)
                    + f' FROM "{component.policy.schema}"."{name}"',
                    schema_name=ownership.schema_name,
                    table_name=name,
                    columns=columns,
                    max_bytes=1024 * 1024,
                    timeout=30,
                )
            await archive.complete_reference_family_restore(session, ownership)
            owner_oid = await immutable._writer(session)
            await archive.seal_model_family_storage(session, ownership, owner_oid)
            assert not await native.fetchval(
                "SELECT EXISTS(SELECT FROM pg_trigger WHERE tgrelid=ANY($1::oid[]))",
                [oid for _, oid in ownership.relation_oids],
            )
            await archive.cleanup_reference_family_stage(session, ownership)


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel", [False, True])
async def test_publisher_body_failure_preserves_dual_owners_and_original_native_transaction(monkeypatch, cancel):
    async with doctors_database(monkeypatch, cms_active=False) as fixture:
        component = fixture.component
        marker = "doctors_body_" + uuid4().hex
        schema = "doctors_body_" + uuid4().hex
        entered = asyncio.Event()
        state_by_key = {}

        task = asyncio.create_task(_publisher_body_failure(component, schema, marker, cancel, entered, state_by_key))
        try:
            await asyncio.wait_for(entered.wait(), 30)
            if cancel:
                await _cancel_active_backend(component, task, marker, state_by_key["pid"])
            await asyncio.wait_for(task, 30)
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        async with component.admin_engine.connect() as admin:
            assert await admin.scalar(text("SELECT to_regnamespace(:schema)"), {"schema": schema}) is None
            assert not await admin.scalar(
                text(
                    "SELECT EXISTS(SELECT FROM pg_stat_activity WHERE pid=:pid AND state='active' AND query LIKE :query)"
                ),
                {"pid": state_by_key["pid"], "query": "%" + marker + "%"},
            )


async def _publisher_body_failure(component, schema, marker, cancel, entered, state_by_key):
    async with component.database.transaction() as session:
        native = source.bound_publisher().connection
        state_by_key["pid"] = await native.fetchval("SELECT pg_backend_pid()")
        saved_owner = source._OWNER.get()
        try:
            async with source.source_publisher_body(session):
                assert await native.fetchval("SELECT current_user=session_user")
                await native.execute(f'CREATE SCHEMA "{schema}" AUTHORIZATION "{component.owner}"')
                entered.set()
                if cancel:
                    await native.execute("SELECT pg_sleep(60) /* " + marker + " */")
                raise RuntimeError("body rejected")
        except RuntimeError, asyncio.CancelledError:
            assert source._OWNER.get() is saved_owner
            assert await native.fetchval("SELECT current_user::text") == component.owner
            assert await native.fetchval("SELECT to_regnamespace($1)", schema) is None
            assert await native.fetchval("SELECT pg_backend_pid()") == state_by_key["pid"]
            assert await native.fetchval("SELECT 17") == 17


async def _cancel_active_backend(component, task, marker, pid):
    async with component.admin_engine.connect() as admin:
        async with asyncio.timeout(10):
            while not await admin.scalar(
                text(
                    "SELECT EXISTS(SELECT FROM pg_stat_activity WHERE pid=:pid AND state='active' AND query LIKE :query)"
                ),
                {"pid": pid, "query": "%" + marker + "%"},
            ):
                await asyncio.sleep(0.01)
        task.cancel()


@pytest.mark.asyncio
async def test_retained_publisher_captures_archive_with_distinct_physical_owner(monkeypatch):
    async with doctors_database(monkeypatch, cms_active=False) as fixture:
        component = fixture.component
        engine = create_async_engine(
            component.admin_engine.url.set(username=component.retained_publisher, password=None)
        )
        try:
            async with async_sessionmaker(engine)() as session, session.begin():
                capture = await archive.capture_reference_family_source(
                    session,
                    importer_id="cms-doctors",
                    schema_name=component.policy.schema,
                    source_metadata={"release": "synthetic-distinct-owner"},
                )
                assert capture.manifest.source_serving_generation == component.authority.serving_generation
                assert capture.manifest.source_capture_contract == archive.IMMUTABLE_NUCC_SOURCE_CAPTURE_CONTRACT
                assert len(capture.manifest.tables) == 3
                assert all(table.row_count == 1 for table in capture.manifest.tables)
                assert await session.scalar(text("SELECT current_user=session_user")) is True
                assert (
                    await session.scalar(
                        text("SELECT NOT pg_has_role(current_user,:owner,'SET')"), {"owner": component.owner}
                    )
                    is True
                )
        finally:
            await engine.dispose()
