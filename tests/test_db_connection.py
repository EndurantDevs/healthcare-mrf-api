# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import os
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import Column, Integer, MetaData, Table, select

from db.connection import (
    ConnectionProxy,
    Database,
    DeleteAdapter,
    InsertAdapter,
    SelectAdapter,
)


@pytest.mark.asyncio
async def test_database_connect_initializes_engine(monkeypatch):
    created_by_field = {}

    def fake_create_engine(url, **kwargs):  # pragma: no cover - executed in test
        created_by_field["url"] = url
        created_by_field["kwargs"] = kwargs
        return SimpleNamespace(connect=AsyncMock())

    def fake_sessionmaker(engine, **kwargs):
        created_by_field["session_kwargs"] = kwargs
        return lambda: "session"

    monkeypatch.setenv("HLTHPRT_DB_DRIVER", "psycopg")
    monkeypatch.setenv("HLTHPRT_DB_USER", "user")
    monkeypatch.setenv("HLTHPRT_DB_PASSWORD", "pass")
    monkeypatch.setenv("HLTHPRT_DB_HOST", "host")
    monkeypatch.setenv("HLTHPRT_DB_PORT", "5433")
    monkeypatch.setenv("HLTHPRT_DB_DATABASE", "db")
    monkeypatch.setenv("HLTHPRT_DB_POOL_MIN_SIZE", "2")
    monkeypatch.setenv("HLTHPRT_DB_POOL_MAX_SIZE", "4")

    monkeypatch.setattr("db.connection.create_async_engine", fake_create_engine)
    monkeypatch.setattr("db.connection.async_sessionmaker", fake_sessionmaker)

    db = Database()
    await db.connect()

    assert created_by_field["url"].drivername == "postgresql+psycopg"
    assert created_by_field["kwargs"]["pool_size"] == 2
    assert created_by_field["kwargs"]["max_overflow"] == 2
    assert db.engine is not None
    assert db.session_factory is not None


class _FakeResult:
    def __init__(self, value=None, rowcount=None):
        self._value = value
        self.rowcount = rowcount

    def all(self):
        return [self._value]

    def first(self):
        return self._value

    def scalar(self):
        return self._value


class _FakeSession:
    def __init__(self):
        self._in_tx = False
        self.executed = []
        self.committed = False
        self.rolled_back = False
        self.closed = False
        self.nested_transactions = 0

    async def execute(self, stmt, params=None):
        self.executed.append((stmt, params))
        return _FakeResult(value=42, rowcount=1)

    def in_transaction(self):
        return self._in_tx

    async def commit(self):
        self.committed = True

    async def rollback(self):
        self.rolled_back = True

    async def close(self):
        self.closed = True

    @asynccontextmanager
    async def begin(self):
        self._in_tx = True
        try:
            yield self
        except Exception:
            self.rolled_back = True
            raise
        else:
            self.committed = True
        finally:
            self._in_tx = False

    @asynccontextmanager
    async def begin_nested(self):
        self.nested_transactions += 1
        try:
            yield self
        except Exception:
            self.rolled_back = True
            raise


@pytest.mark.asyncio
async def test_session_manager_commits_and_rolls_back():
    db = Database()
    session = _FakeSession()
    db.session_factory = lambda: session

    async with db.session() as acquired:
        assert acquired is session
        session._in_tx = True

    assert session.committed
    assert session.closed

    session2 = _FakeSession()
    db.session_factory = lambda: session2
    with pytest.raises(RuntimeError):
        async with db.session():
            session2._in_tx = True
            raise RuntimeError

    assert session2.rolled_back


@pytest.mark.asyncio
async def test_transaction_context(monkeypatch):
    db = Database()
    session = _FakeSession()
    db.session_factory = lambda: session

    async with db.transaction() as tx_session:
        assert tx_session is session

    assert not session._in_tx


@pytest.mark.asyncio
async def test_transaction_helpers_reuse_one_bound_session():
    db = Database()
    sessions = []

    def session_factory():
        session = _FakeSession()
        sessions.append(session)
        return session

    db.session_factory = session_factory

    async with db.transaction() as transaction_session:
        assert await db.status("UPDATE test_table SET value = 1") == 1
        assert await db.scalar("SELECT 42") == 42
        assert await db.all("SELECT 42") == [42]
        assert await db.select(1).scalar() == 42

    assert sessions == [transaction_session]
    assert len(transaction_session.executed) == 4
    assert transaction_session.committed
    assert transaction_session.closed


@pytest.mark.asyncio
async def test_helpers_keep_independent_sessions_outside_transaction():
    db = Database()
    sessions = []

    def session_factory():
        session = _FakeSession()
        sessions.append(session)
        return session

    db.session_factory = session_factory

    await db.status("UPDATE test_table SET value = 1")
    await db.scalar("SELECT 42")
    await db.all("SELECT 42")

    assert len(sessions) == 3
    assert all(session.closed for session in sessions)


@pytest.mark.asyncio
async def test_nested_transaction_uses_savepoint_on_bound_session():
    db = Database()
    sessions = []

    def session_factory():
        session = _FakeSession()
        sessions.append(session)
        return session

    db.session_factory = session_factory

    async with db.transaction() as outer_session:
        async with db.transaction() as nested_session:
            assert nested_session is outer_session
            assert await db.scalar("SELECT 42") == 42

    assert sessions == [outer_session]
    assert outer_session.nested_transactions == 1


@pytest.mark.asyncio
async def test_nested_database_transactions_keep_each_binding():
    first_db = Database()
    second_db = Database()
    first_session = _FakeSession()
    second_session = _FakeSession()
    first_db.session_factory = lambda: first_session
    second_db.session_factory = lambda: second_session

    async with first_db.transaction():
        async with second_db.transaction():
            await first_db.scalar("SELECT 1")
            await second_db.scalar("SELECT 2")

    assert len(first_session.executed) == 1
    assert len(second_session.executed) == 1


@pytest.mark.asyncio
async def test_transaction_session_is_not_shared_with_child_task():
    db = Database()
    session = _FakeSession()
    db.session_factory = lambda: session

    async with db.transaction():
        child_call = asyncio.create_task(db.scalar("SELECT 42"))
        with pytest.raises(RuntimeError, match="child asyncio task"):
            await child_call
        assert await db.scalar("SELECT 42") == 42


@pytest.mark.asyncio
async def test_parallel_top_level_transactions_use_distinct_sessions():
    db = Database()
    sessions = []

    def session_factory():
        session = _FakeSession()
        sessions.append(session)
        return session

    db.session_factory = session_factory

    async def run_transaction():
        async with db.transaction() as session:
            await asyncio.sleep(0)
            await db.scalar("SELECT 42")
            return session

    first_session, second_session = await asyncio.gather(
        run_transaction(),
        run_transaction(),
    )

    assert first_session is not second_session
    assert sessions == [first_session, second_session]


@pytest.mark.asyncio
async def test_transaction_helpers_share_real_postgres_transaction():
    if "test" not in os.getenv("HLTHPRT_DB_DATABASE", "").lower():
        pytest.skip("real transaction test requires a disposable test database")

    database = Database()
    marker = "healthporta-db-transaction-test"
    await database.connect()
    try:
        async with database.transaction():
            await database.status(
                "SELECT set_config('application_name', :marker, true);",
                marker=marker,
            )
            backend_pid = await database.scalar("SELECT pg_backend_pid();")
            rows = await database.all(
                "SELECT pg_backend_pid(), current_setting('application_name');"
            )

            assert rows[0][0] == backend_pid
            assert rows[0][1] == marker

        assert await database.scalar(
            "SELECT current_setting('application_name');"
        ) != marker
    finally:
        await database.disconnect()


@pytest.mark.asyncio
async def test_statement_adapter_methods(monkeypatch):
    db = Database()
    session = _FakeSession()

    @asynccontextmanager
    async def session_ctx():
        yield session

    monkeypatch.setattr(db, "session", session_ctx)

    adapter = db.select(1)
    assert isinstance(adapter, SelectAdapter)
    assert await adapter.scalar() == 42
    assert await adapter.first() == 42
    assert await adapter.all() == [42]

    insert_adapter = db.insert(Table("t", MetaData(), Column("id", Integer)))
    assert isinstance(insert_adapter, InsertAdapter)
    await insert_adapter.status()
    stmt, _ = session.executed[-1]
    assert "INSERT" in str(stmt)

    delete_adapter = db.delete(Table("d", MetaData(), Column("id", Integer)))
    assert isinstance(delete_adapter, DeleteAdapter)
    await delete_adapter.status()
    stmt, _ = session.executed[-1]
    assert "DELETE" in str(stmt)


@pytest.mark.asyncio
async def test_connection_proxy_helpers(monkeypatch):
    executed_calls = []

    class FakeConnection:
        async def execute(self, stmt, params=None):
            executed_calls.append((stmt, params))
            return _FakeResult(value=99, rowcount=3)

    proxy = ConnectionProxy(SimpleNamespace(), FakeConnection(), SimpleNamespace())
    assert await proxy.all("SELECT 1") == [99]
    assert await proxy.first("SELECT 1") == 99
    assert await proxy.scalar("SELECT 1") == 99
    assert await proxy.status("DELETE") == 3
    assert executed_calls


@pytest.mark.asyncio
async def test_acquire_driver_avoids_managed_transaction_and_invalidates_on_error():
    driver_connection = SimpleNamespace()

    class FakeConnection:
        def __init__(self):
            self.invalidated = False

        async def get_raw_connection(self):
            return SimpleNamespace(driver_connection=driver_connection)

        async def invalidate(self):
            self.invalidated = True

    connection = FakeConnection()

    class ConnectContext:
        async def __aenter__(self):
            return connection

        async def __aexit__(self, exc_type, exc, tb):
            return False

    engine = SimpleNamespace(connect=lambda: ConnectContext())
    database = Database(engine=engine)

    with pytest.raises(RuntimeError, match="copy failed"):
        async with database.acquire_driver() as acquired:
            assert acquired is driver_connection
            raise RuntimeError("copy failed")

    assert connection.invalidated


@pytest.mark.asyncio
async def test_create_table_and_execute_ddl(monkeypatch):
    table = Table("things", MetaData(), Column("id", Integer))

    run_calls_by_name = {}

    class FakeBeginCtx:
        async def __aenter__(self):
            class Runner:
                async def run_sync(self_inner, fn, **kw):
                    run_calls_by_name["run"] = run_calls_by_name.get("run", 0) + 1

                
            return Runner()

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class FakeEngine:
        def __init__(self):
            self.begin = lambda: FakeBeginCtx()

        def connect(self):
            class _Conn:
                async def __aenter__(self_inner):
                    return self_inner

                async def __aexit__(self_inner, exc_type, exc, tb):
                    return False

                async def execution_options(self_inner, **kwargs):
                    self_inner.kw = kwargs
                    return self_inner

                async def exec_driver_sql(self_inner, statement):
                    run_calls_by_name["ddl"] = statement

            return _Conn()

    db = Database(engine=FakeEngine())
    await db.create_table(table)
    assert run_calls_by_name.get("run") == 1

    await db.execute_ddl("VACUUM")
    assert run_calls_by_name["ddl"] == "VACUUM"
