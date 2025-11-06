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
    created = {}

    def fake_create_engine(url, **kwargs):  # pragma: no cover - executed in test
        created["url"] = url
        created["kwargs"] = kwargs
        return SimpleNamespace(connect=AsyncMock())

    def fake_sessionmaker(engine, **kwargs):
        created["session_kwargs"] = kwargs
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

    assert created["url"].drivername == "postgresql+psycopg"
    assert created["kwargs"]["pool_size"] == 2
    assert created["kwargs"]["max_overflow"] == 2
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
        finally:
            self._in_tx = False


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
    executed = []

    class FakeConnection:
        async def execute(self, stmt, params=None):
            executed.append((stmt, params))
            return _FakeResult(value=99, rowcount=3)

    proxy = ConnectionProxy(SimpleNamespace(), FakeConnection(), SimpleNamespace())
    assert await proxy.all("SELECT 1") == [99]
    assert await proxy.first("SELECT 1") == 99
    assert await proxy.scalar("SELECT 1") == 99
    assert await proxy.status("DELETE") == 3
    assert executed


@pytest.mark.asyncio
async def test_create_table_and_execute_ddl(monkeypatch):
    table = Table("things", MetaData(), Column("id", Integer))

    run_calls = {}

    class FakeBeginCtx:
        async def __aenter__(self):
            class Runner:
                async def run_sync(self_inner, fn, **kw):
                    run_calls["run"] = run_calls.get("run", 0) + 1

                
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
                    run_calls["ddl"] = statement

            return _Conn()

    db = Database(engine=FakeEngine())
    await db.create_table(table)
    assert run_calls.get("run") == 1

    await db.execute_ddl("VACUUM")
    assert run_calls["ddl"] == "VACUUM"
