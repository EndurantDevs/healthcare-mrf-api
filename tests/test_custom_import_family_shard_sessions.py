# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Actual SQLAlchemy session-instance events; no PostgreSQL pool proof is implied."""

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession

from process.custom_import.family_shard_sessions import _MAX_MEMORY_MB, import_session_factory


class _Connection:
    def __init__(self, failure=None):
        self.statements, self.failure = [], failure

    def exec_driver_sql(self, statement):
        self.statements.append(statement)
        if self.failure is not None:
            raise self.failure


class _Factory:
    def __init__(self):
        self.sessions, self.events = [], []

    @asynccontextmanager
    async def __call__(self):
        session = AsyncSession()
        self.sessions.append(session)
        self.events.append("open")
        try:
            yield session
        finally:
            self.events.append("close")
            await session.close()


def _begin(session, connection, *, nested=False):
    session.sync_session.dispatch.after_begin(session.sync_session, SimpleNamespace(nested=nested), connection)


@pytest.mark.parametrize("memory_mb", [256, 512, 1024, 2048, _MAX_MEMORY_MB])
async def test_owner_instance_utility_setting_precedes_caller_isolation_without_touching_others(memory_mb):
    base, ordinary, connection = _Factory(), AsyncSession(), _Connection()
    factory = import_session_factory(base, work_mem_mb=memory_mb)
    try:
        async with factory() as session:
            assert session is base.sessions[-1] and not session.in_transaction()
            assert list(ordinary.sync_session.dispatch.after_begin) == []
            async with session.begin():
                _begin(session, connection)
                connection.exec_driver_sql("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            assert connection.statements == [
                f"SET LOCAL work_mem = '{memory_mb}MB'",
                "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY",
            ]
        assert list(session.sync_session.dispatch.after_begin) == []
        _begin(session, connection)
        _begin(ordinary, connection)
        assert len(connection.statements) == 2 and base.events == ["open", "close"]
    finally:
        await ordinary.close()


async def test_savepoint_callbacks_are_skipped_and_each_root_applies_fresh_settings():
    base, connection = _Factory(), _Connection()
    factory = import_session_factory(base, work_mem_mb=512)
    async with factory() as session:
        for _transaction in range(2):
            async with session.begin():
                _begin(session, connection)
                async with session.begin_nested():
                    _begin(session, connection, nested=True)
        assert connection.statements == ["SET LOCAL work_mem = '512MB'"] * 2
    assert list(session.sync_session.dispatch.after_begin) == []


async def test_maintenance_memory_is_explicit_and_separate_for_index_phase():
    base, connection = _Factory(), _Connection()
    index_factory = import_session_factory(base, work_mem_mb=256, maintenance_work_mem_mb=1024)
    async with index_factory() as session:
        async with session.begin():
            _begin(session, connection)
    assert connection.statements == ["SET LOCAL work_mem = '256MB'", "SET LOCAL maintenance_work_mem = '1024MB'"]
    default_factory = import_session_factory(base, work_mem_mb=512)
    async with default_factory() as session:
        async with session.begin():
            _begin(session, connection)
    assert connection.statements[-1] == "SET LOCAL work_mem = '512MB'"
    assert sum("maintenance_work_mem" in statement for statement in connection.statements) == 1


@pytest.mark.parametrize("field", ["work_mem_mb", "maintenance_work_mem_mb"])
@pytest.mark.parametrize("value", [True, False, 0, -1, 256.0, "512", None, _MAX_MEMORY_MB + 1])
def test_invalid_memory_values_refuse_before_opening_a_session(field, value):
    base = _Factory()
    arguments_by_name = dict(work_mem_mb=512, **({} if field == "work_mem_mb" else {field: value}))
    if field == "work_mem_mb":
        arguments_by_name[field] = value
    if field == "maintenance_work_mem_mb" and value is None:
        assert callable(import_session_factory(base, **arguments_by_name))
        return
    with pytest.raises(ValueError, match="integer MB"):
        import_session_factory(base, **arguments_by_name)
    assert not base.events


def test_existing_factory_is_required():
    with pytest.raises(ValueError, match="session factory"):
        import_session_factory(None, work_mem_mb=512)


@pytest.mark.parametrize("failure", [RuntimeError("SQL setting refused"), asyncio.CancelledError("setting cancelled")])
async def test_setting_failure_removes_the_listener_and_preserves_original_failure(failure):
    base, connection = _Factory(), _Connection(failure)
    factory = import_session_factory(base, work_mem_mb=512)
    with pytest.raises(type(failure)) as raised:
        async with factory() as session, session.begin():
            _begin(session, connection)
    assert raised.value is failure and base.events == ["open", "close"]
    assert list(session.sync_session.dispatch.after_begin) == [] and not session.in_transaction()


async def test_actual_task_cancellation_removes_the_instance_listener_after_factory_cleanup():
    base, entered, release = _Factory(), asyncio.Event(), asyncio.Event()
    factory = import_session_factory(base, work_mem_mb=1024)

    async def operation():
        async with factory() as session, session.begin():
            _begin(session, _Connection())
            entered.set()
            await release.wait()

    pending = asyncio.create_task(operation())
    await entered.wait()
    pending.cancel("owner SQL operation cancelled")
    with pytest.raises(asyncio.CancelledError, match="owner SQL operation cancelled"):
        await pending
    assert base.events == ["open", "close"]
    assert list(base.sessions[0].sync_session.dispatch.after_begin) == []


@pytest.mark.parametrize("fails", [False, True])
async def test_listener_survives_the_underlying_factory_final_flush_and_is_always_removed(fails):
    session, connection, observations = AsyncSession(), _Connection(), []

    @asynccontextmanager
    async def final_flush():
        try:
            yield session
            _begin(session, connection)
            observations.append("factory-final-flush")
            if fails:
                raise RuntimeError("factory final flush failed")
        finally:
            await session.close()
            observations.append("factory-close")

    factory = import_session_factory(final_flush, work_mem_mb=512)
    if fails:
        with pytest.raises(RuntimeError, match="factory final flush"):
            async with factory():
                observations.append("body")
    else:
        async with factory():
            observations.append("body")
    assert connection.statements == ["SET LOCAL work_mem = '512MB'"]
    assert observations == ["body", "factory-final-flush", "factory-close"]
    assert list(session.sync_session.dispatch.after_begin) == []


async def test_borrowed_active_session_is_refused_without_touching_its_transaction():
    session = AsyncSession()

    @asynccontextmanager
    async def borrowed():
        yield session

    factory = import_session_factory(borrowed, work_mem_mb=512)
    try:
        async with session.begin():
            with pytest.raises(ValueError, match="clean session"):
                async with factory():
                    pytest.fail("borrowed transaction must refuse")
            assert session.in_transaction() and list(session.sync_session.dispatch.after_begin) == []
    finally:
        await session.close()


async def test_session_bound_to_an_active_external_connection_is_refused():
    session = AsyncSession()
    connection = Mock(spec=AsyncConnection)
    connection.in_transaction.return_value = True
    session.bind = connection

    @asynccontextmanager
    async def borrowed():
        yield session

    try:
        with pytest.raises(ValueError, match="borrowed transaction"):
            async with import_session_factory(borrowed, work_mem_mb=512)():
                pytest.fail("external root transaction must refuse")
        assert not session.in_transaction() and list(session.sync_session.dispatch.after_begin) == []
    finally:
        await session.close()


async def test_wrong_session_type_keeps_underlying_factory_cleanup():
    events = []

    @asynccontextmanager
    async def wrong():
        events.append("open")
        try:
            yield object()
        finally:
            events.append("close")

    with pytest.raises(ValueError, match="AsyncSession"):
        async with import_session_factory(wrong, work_mem_mb=512)():
            pytest.fail("an owned async session is required")
    assert events == ["open", "close"]
