# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retained export under real distinct roles; not enrolled-runtime qualification."""

import asyncio
from contextlib import asynccontextmanager
from uuid import uuid4

import pytest
from sqlalchemy import event, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from process import cms_doctors_immutable as immutable
from process import provider_directory_generic_source_policy as source
from process import reference_family_archive as archive
from process.provider_directory_projection_stage import _copy_driver
from tests.cms_doctors_preparation_postgres_support import doctors_database
from tests.reference_family_generation_fixture import native_reference_source


@asynccontextmanager
async def _retained_sessions(component):
    engine = create_async_engine(
        component.admin_engine.url.set(
            username=component.retained_publisher,
            password=None,
        ),
        pool_size=2,
        max_overflow=0,
    )
    try:
        yield async_sessionmaker(engine, expire_on_commit=False)
    finally:
        await engine.dispose()


def _options(custody, dataset):
    return dict(
        importer_id="cms-doctors",
        schema_name=None,
        source_metadata={"release": "synthetic-retained"},
        dataset_id=dataset,
        on_prepared=custody.retain,
        on_precreated=custody.precreate,
        source_copy=custody.source_copy,
    )


async def _assert_retained_actor(session, component):
    driver = await _copy_driver(session)
    assert source._PUBLISHER.get() is None
    assert await driver.fetchval(
        "SELECT current_user=session_user AND current_user=$1::name", component.retained_publisher
    )
    assert await driver.fetchval(
        "SELECT pg_has_role(current_user,$1,'SET') AND pg_has_role(current_user,$2,'USAGE') "
        "AND NOT pg_has_role(current_user,$2,'SET') AND NOT has_database_privilege($2,current_database(),'CREATE')",
        component.retention_owner,
        component.owner,
    )
    return driver


async def _assert_clone_owner(session, component, ownership):
    driver = await _assert_retained_actor(session, component)
    oids = [oid for _, oid in ownership.relation_oids]
    assert len(oids) == len(set(oids)) == 3
    assert await driver.fetchval(
        "SELECT nspowner=$2::regrole FROM pg_namespace WHERE oid=$1", ownership.schema_oid, component.retention_owner
    )
    assert await driver.fetchval(
        "SELECT bool_and(relowner=$2::regrole) FROM pg_class WHERE oid=ANY($1::oid[])", oids, component.retention_owner
    )
    assert not await driver.fetchval(
        "SELECT EXISTS(SELECT FROM pg_trigger WHERE tgrelid=ANY($1::oid[])) "
        "OR EXISTS(SELECT FROM pg_constraint WHERE conrelid=ANY($1::oid[]) AND contype='f')",
        oids,
    )


@pytest.mark.asyncio
async def test_retained_export_imports_snapshot_before_auth_and_keeps_distinct_clone_owner(monkeypatch):
    async with doctors_database(monkeypatch, cms_active=False) as fixture:
        component = fixture.component
        async with (
            _retained_sessions(component) as sessions,
            native_reference_source(
                sessions,
                owner=component.retention_owner,
                readers=(component.reader,),
            ) as custody,
        ):
            await _complete_export(component, sessions, custody)


async def _complete_export(component, sessions, custody):
    statements, copies, precreated = [], [], []
    engine = sessions.kw["bind"]
    event.listen(
        engine.sync_engine,
        "before_cursor_execute",
        lambda _c, _cursor, statement, _params, _context, _many: statements.append(statement),
    )

    async def precreate(session, ownership):
        await _assert_clone_owner(session, component, ownership)
        await custody.precreate(session, ownership)
        precreated.append(ownership)

    async def copy(session, query, *args, **kwargs):
        assert precreated
        await _assert_clone_owner(session, component, precreated[0])
        driver = await _copy_driver(session)
        copies.append((id(session), await driver.fetchval("SELECT pg_backend_pid()")))
        return await archive.native_copy_projection(session, query, *args, **kwargs)

    options = _options(custody, uuid4())
    options.update(
        schema_name=component.policy.schema,
        on_precreated=precreate,
        source_copy=archive.ReferenceFamilySourceCopy(copy, 1_000_000, 30),
    )
    prepared = await archive.prepare_reference_family_archive_source(sessions, **options)
    assert len(copies) == 3 and len(set(copies)) == 1
    snapshot_index = next(
        i for i, statement in enumerate(statements) if statement.startswith("SET TRANSACTION SNAPSHOT")
    )
    assert statements[snapshot_index - 1] == "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"
    owner_index = next(
        i for i, statement in enumerate(statements) if i > snapshot_index and "AS caller_safe" in statement
    )
    create_index = next(i for i, statement in enumerate(statements) if statement.startswith("CREATE SCHEMA"))
    assert snapshot_index < owner_index < create_index
    assert prepared.manifest.source_serving_generation == component.authority.serving_generation
    await _capture_export_and_cleanup(component, sessions, custody, prepared)


async def _capture_export_and_cleanup(component, sessions, custody, prepared):
    captures = []

    async def capture(selected):
        captures.append(selected)
        async with sessions() as session, session.begin():
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
            await session.execute(text(f"SET TRANSACTION SNAPSHOT '{selected.postgres_snapshot}'"))
            await _assert_clone_owner(session, component, selected.ownership)
            for name, _oid in selected.ownership.relation_oids:
                assert (
                    await session.scalar(text(f'SELECT count(*) FROM "{selected.ownership.schema_name}"."{name}"')) == 1
                )

    assert (
        await archive.export_prepared_reference_family_archive(
            sessions,
            prepared=prepared,
            archive_copy=capture,
            verify_custody=custody.verify,
        )
        == prepared.manifest
    )
    assert len(captures) == 1
    async with sessions() as session, session.begin():
        await custody.retire(session, prepared.ownership)
        assert (
            await session.scalar(text("SELECT to_regnamespace(:schema)"), {"schema": prepared.ownership.schema_name})
            is None
        )
        assert (
            await immutable.capture_doctors_serving_generation(session, component.policy.schema)
            == component.authority.serving_generation
        )
        with pytest.raises(DBAPIError) as refusal:
            await _attempt_physical_role(session, component.owner)
        assert refusal.value.orig.sqlstate == "42501"


async def _attempt_physical_role(session, owner):
    async with session.begin_nested():
        await session.execute(text(f'SET LOCAL ROLE "{owner}"'))


@pytest.mark.asyncio
async def test_retained_export_refuses_foreign_owner_and_assumed_actor_before_create(monkeypatch):
    async with doctors_database(monkeypatch, cms_active=False) as fixture:
        component = fixture.component
        async with _retained_sessions(component) as sessions, sessions() as session, session.begin():
            schema = archive.reference_family_stage_schema(uuid4())
            owner = await session.scalar(text("SELECT CAST(:owner AS regrole)::oid"), {"owner": component.owner})
            with pytest.raises(archive.ReferenceFamilyArchiveError, match="publisher owner differs"):
                await archive._create_model_family(
                    session, archive.reference_family_spec("cms-doctors"), schema, protected_owner_oid=owner
                )
            assert await session.scalar(text("SELECT to_regnamespace(:schema)"), {"schema": schema}) is None
            await session.execute(text(f'SET LOCAL ROLE "{component.retention_owner}"'))
            with pytest.raises(archive.ReferenceFamilyArchiveError, match="protected publisher is unavailable"):
                await archive.protected_publisher_owner(session)
            await session.execute(text("RESET ROLE"))
            await _assert_retained_actor(session, component)


@pytest.mark.asyncio
async def test_source_login_cannot_select_retained_export_destination(monkeypatch):
    async with doctors_database(monkeypatch, cms_active=False) as fixture:
        component, dataset = fixture.component, uuid4()
        calls = []

        async def forbidden(*_args, **_kwargs):
            calls.append(True)
            raise AssertionError("refusal must precede creation and payload")

        with pytest.raises(archive.ReferenceFamilyArchiveError, match="protected publisher is unavailable"):
            await archive.prepare_reference_family_archive_source(
                component.database.session_factory,
                importer_id="cms-doctors",
                schema_name=component.policy.schema,
                source_metadata={"release": "synthetic-refusal"},
                dataset_id=dataset,
                on_prepared=forbidden,
                on_precreated=forbidden,
                source_copy=archive.ReferenceFamilySourceCopy(forbidden, 4096, 30),
            )
        assert not calls
        await _assert_absent(component, dataset)


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["precreated", "prepared"])
async def test_retained_export_callback_failure_rolls_back_closed_clone(monkeypatch, phase):
    async with doctors_database(monkeypatch, cms_active=False) as fixture:
        component, dataset = fixture.component, uuid4()
        async with (
            _retained_sessions(component) as sessions,
            native_reference_source(
                sessions,
                owner=component.retention_owner,
            ) as custody,
        ):
            options = _options(custody, dataset)
            options["schema_name"] = component.policy.schema

            async def reject(session, value):
                ownership = value if phase == "precreated" else value.ownership
                await _assert_clone_owner(session, component, ownership)
                raise RuntimeError("reject retained clone")

            options["on_precreated" if phase == "precreated" else "on_prepared"] = reject
            with pytest.raises(RuntimeError, match="reject retained clone"):
                await archive.prepare_reference_family_archive_source(sessions, **options)
            await _assert_absent(component, dataset)


async def _assert_absent(component, dataset):
    async with component.admin_engine.connect() as admin:
        assert (
            await admin.scalar(
                text("SELECT to_regnamespace(:schema)"), {"schema": archive.reference_family_stage_schema(dataset)}
            )
            is None
        )
        authority = (
            await admin.execute(
                text(
                    f'SELECT local_generation,relation_oids FROM "{component.policy.schema}".reference_family_result_generation '
                    "WHERE importer_id='cms-doctors'"
                )
            )
        ).one()
        assert authority.local_generation == component.authority.local_generation
        assert tuple(authority.relation_oids) == component.authority.relation_oids
        for name in archive.reference_family_spec("cms-doctors").table_names:
            assert await admin.scalar(text(f'SELECT count(*) FROM "{component.policy.schema}"."{name}"')) == 1


@pytest.mark.asyncio
async def test_actual_retained_export_copy_cancellation_removes_owned_clone(monkeypatch):
    async with doctors_database(monkeypatch, cms_active=False) as fixture:
        async with (
            _retained_sessions(fixture.component) as sessions,
            native_reference_source(
                sessions,
                owner=fixture.component.retention_owner,
            ) as custody,
        ):
            await _cancel_copy(fixture.component, sessions, custody)


async def _cancel_copy(component, sessions, custody):
    dataset, marker, state = uuid4(), "retained_copy_" + uuid4().hex, {}

    async def copy(session, query, *args, **kwargs):
        state["calls"] = state.get("calls", 0) + 1
        if state["calls"] == 2:
            state["pid"] = await (await _copy_driver(session)).fetchval("SELECT pg_backend_pid()")
            query = f"SELECT selected.* FROM ({query}) selected CROSS JOIN (SELECT pg_sleep(60) /* {marker} */) pause"
        return await archive.native_copy_projection(session, query, *args, **kwargs)

    options = _options(custody, dataset)
    options.update(
        schema_name=component.policy.schema, source_copy=archive.ReferenceFamilySourceCopy(copy, 1_000_000, 120)
    )
    task = asyncio.create_task(archive.prepare_reference_family_archive_source(sessions, **options))
    try:
        await _cancel_backend(component, task, state, marker)
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 20)
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    assert state["calls"] == 2
    await _assert_absent(component, dataset)


async def _cancel_backend(component, task, state, marker):
    async with component.admin_engine.connect() as admin, asyncio.timeout(20):
        while not state.get("pid") or not await admin.scalar(
            text(
                "SELECT EXISTS(SELECT FROM pg_stat_activity WHERE pid=:pid AND state='active' AND query LIKE :marker)"
            ),
            {"pid": state.get("pid"), "marker": "%" + marker + "%"},
        ):
            if task.done():
                await task
                raise AssertionError("COPY completed before cancellation observation")
            await asyncio.sleep(0.01)
        task.cancel()
