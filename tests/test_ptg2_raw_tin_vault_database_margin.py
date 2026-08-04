# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Database-helper coverage required by the vault migration foundation."""

from __future__ import annotations

import contextvars
from types import SimpleNamespace

import pytest
import sqlalchemy as sa
from sqlalchemy import Column, Computed, Integer, MetaData, Table
from sqlalchemy.dialects import postgresql
from sqlalchemy.exc import ArgumentError

from db import connection
from db import migration_expression_adoption as expression_adoption


class _StreamRows:
    def __init__(self) -> None:
        self._rows = iter(("first", "second"))

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self._rows)
        except StopIteration:
            raise StopAsyncIteration from None


class _StreamSession:
    async def stream(self, _statement, _params):
        return _StreamRows()

    def in_transaction(self) -> bool:
        return False

    async def close(self) -> None:
        return None


@pytest.mark.asyncio
async def test_statement_wrapping_and_streaming_boundaries() -> None:
    """Cover typed statement wrapping, column coercion, and stream iteration."""

    database = connection.Database(session_factory=_StreamSession)
    table = Table("vault_margin", MetaData(), Column("id", Integer))

    assert isinstance(
        database.update(table).where(table.c.id == 1),
        connection.UpdateAdapter,
    )
    assert isinstance(
        database.delete(table).where(table.c.id == 1),
        connection.DeleteAdapter,
    )
    assert database.select([table.c.id])._stmt is not None
    assert connection._wrap_statement(database, object()).__class__ is object
    assert [row async for row in database.select(table.c.id).iterate()] == [
        "first",
        "second",
    ]


def test_unbound_session_environment_and_insert_boundaries() -> None:
    """Cover fail-closed session lookup and all insert target shapes."""

    with pytest.raises(RuntimeError, match="No SQLAlchemy session"):
        contextvars.Context().run(connection.current_session)
    assert connection._is_env_enabled(None, default=True) is True

    database = connection.Database()
    table = Table("vault_margin", MetaData(), Column("id", Integer))
    model = SimpleNamespace(__table__=table)
    assert isinstance(database.insert(model), connection.InsertAdapter)
    with pytest.raises(ArgumentError):
        database.insert(object())
    with pytest.raises(TypeError):
        database.insert()


@pytest.mark.asyncio
async def test_async_dependency_guards_fail_closed(monkeypatch) -> None:
    """Cover every entry point protected by the async dependency guard."""

    unavailable = ImportError("async support unavailable")
    monkeypatch.setattr(connection, "_ASYNC_IMPORT_ERROR", unavailable)
    database = connection.Database()

    with pytest.raises(RuntimeError, match="SQLAlchemy async support"):
        await database.connect()
    with pytest.raises(RuntimeError, match="SQLAlchemy async support"):
        async with database.session():
            pytest.fail("session guard did not reject unavailable async support")
    with pytest.raises(RuntimeError, match="SQLAlchemy async support"):
        database.init_app(object())


@pytest.mark.asyncio
async def test_reconnect_uses_an_explicit_driver_without_aliasing(monkeypatch) -> None:
    """Cover database replacement and the already-qualified driver path."""

    created_by_field: dict[str, object] = {}
    database = connection.Database(engine=object())
    database._database_name = "old_database"

    async def _disconnect() -> None:
        database.engine = None
        database.session_factory = None
        database._database_name = None

    def _create_engine(url, **_options):
        created_by_field["url"] = url
        return SimpleNamespace()

    monkeypatch.setattr(database, "disconnect", _disconnect)
    monkeypatch.setattr(connection, "create_async_engine", _create_engine)
    monkeypatch.setattr(connection, "async_sessionmaker", lambda *_args, **_kwargs: object())
    monkeypatch.setenv("HLTHPRT_DB_DATABASE", "new_database")
    monkeypatch.setenv("HLTHPRT_DB_DRIVER", "postgresql+asyncpg")

    await database.connect()

    assert created_by_field["url"].drivername == "postgresql+asyncpg"
    assert database._database_name == "new_database"


class _ConnectionContext:
    def __init__(self, connected) -> None:
        self._connected = connected

    async def __aenter__(self):
        return self._connected

    async def __aexit__(self, _error_type, _error, _traceback) -> bool:
        return False


class _EngineConnection:
    def __init__(self) -> None:
        self.executed_ddl: list[str] = []
        self.driver_connection = object()

    def execution_options(self, **_options):
        return self

    async def exec_driver_sql(self, statement: str) -> None:
        self.executed_ddl.append(statement)

    async def get_raw_connection(self):
        return SimpleNamespace(driver_connection=self.driver_connection)


class _Engine:
    def __init__(self, connected: _EngineConnection) -> None:
        self._connected = connected

    def begin(self):
        return _ConnectionContext(self._connected)

    def connect(self):
        return _ConnectionContext(self._connected)


@pytest.mark.asyncio
async def test_lazy_ddl_acquire_and_driver_connections(monkeypatch) -> None:
    """Cover lazy connection setup for all migration-facing helpers."""

    connected = _EngineConnection()
    engine = _Engine(connected)
    database = connection.Database()

    async def _connect() -> None:
        database.engine = engine

    monkeypatch.setattr(database, "connect", _connect)
    await database.execute_ddl("VACUUM vault_margin")
    assert connected.executed_ddl == ["VACUUM vault_margin"]

    database.engine = None
    async with database.acquire() as proxy:
        assert proxy.raw_connection.driver_connection is connected.driver_connection

    database.engine = None
    async with database.acquire_driver() as driver_connection:
        assert driver_connection is connected.driver_connection


class _MiddlewareSession:
    def __init__(self, *, in_transaction: bool) -> None:
        self._in_transaction = in_transaction
        self.rolled_back = False
        self.closed = False

    def in_transaction(self) -> bool:
        return self._in_transaction

    async def commit(self) -> None:
        return None

    async def rollback(self) -> None:
        self.rolled_back = True

    async def close(self) -> None:
        self.closed = True


class _App:
    def __init__(self) -> None:
        self.listeners_by_name = {}
        self.middleware_by_name = {}

    def listener(self, name: str):
        def _register(function):
            self.listeners_by_name[name] = function
            return function

        return _register

    def middleware(self, name: str):
        def _register(function):
            self.middleware_by_name[name] = function
            return function

        return _register


@pytest.mark.asyncio
async def test_request_session_middleware_failure_and_empty_paths(monkeypatch) -> None:
    """Cover lazy request binding, rollback, no-session, and no-token cleanup."""

    app = _App()
    database = connection.Database()
    transaction_session = _MiddlewareSession(in_transaction=True)

    async def _connect() -> None:
        database.session_factory = lambda: transaction_session

    monkeypatch.setattr(database, "connect", _connect)
    database.init_app(app)
    bind_session = app.middleware_by_name["request"]
    cleanup_session = app.middleware_by_name["response"]

    request = SimpleNamespace(ctx=SimpleNamespace())
    await bind_session(request)
    await cleanup_session(request, SimpleNamespace(status=500))
    assert transaction_session.rolled_back is True
    assert transaction_session.closed is True

    empty_request = SimpleNamespace(ctx=SimpleNamespace())
    assert await cleanup_session(empty_request, SimpleNamespace(status=200)) is None

    tokenless_session = _MiddlewareSession(in_transaction=False)
    tokenless_request = SimpleNamespace(
        ctx=SimpleNamespace(sa_session=tokenless_session),
    )
    await cleanup_session(tokenless_request, SimpleNamespace(status=200))
    assert tokenless_session.closed is True


class _ExpressionResult:
    def scalar_one_or_none(self):
        return None


class _ExpressionConnection:
    def __init__(self) -> None:
        self.dialect = postgresql.dialect()
        self.statements: list[str] = []

    def execute(self, statement, _params=None):
        self.statements.append(str(statement))
        return _ExpressionResult()


def test_migration_expression_fallback_generated_and_missing_paths(monkeypatch) -> None:
    """Cover defensive expression adoption paths used by migration helpers."""

    assert expression_adoption._fallback_column_expression(
        sa.Column("plain_value", sa.Integer)
    ) == ""

    database_connection = _ExpressionConnection()
    generated_column = Column(
        "derived_value",
        Integer,
        Computed("base_value + 1"),
    )
    expression_adoption._install_expected_column_expression(
        database_connection,
        '"temporary_table"',
        generated_column,
    )
    assert any("GENERATED ALWAYS" in statement for statement in database_connection.statements)

    missing_connection = _ExpressionConnection()
    monkeypatch.setattr(
        expression_adoption,
        "_database_connection",
        lambda _operations: missing_connection,
    )
    with pytest.raises(RuntimeError, match="migration_expected_expression_missing"):
        expression_adoption._canonical_column_expression(
            object(),
            "mrf",
            "vault_table",
            Column("created_at", Integer, server_default=sa.text("1")),
        )
    assert missing_connection.statements[-1].startswith("DROP TABLE IF EXISTS")
