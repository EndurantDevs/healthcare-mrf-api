# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""SQLAlchemy 2 async infrastructure shared by API and workers."""

from __future__ import annotations

import asyncio
import contextvars
import inspect
import os
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Optional, Tuple

from sqlalchemy import delete as sa_delete
from sqlalchemy import func as sa_func
from sqlalchemy import insert as sa_insert
from sqlalchemy import select as sa_select
from sqlalchemy import text as sa_text
from sqlalchemy import update as sa_update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine.url import URL
from sqlalchemy.schema import Table as SATable
from sqlalchemy.sql import Executable, Select
from sqlalchemy.sql.dml import Delete, Insert, Update

try:
    from sqlalchemy.ext.asyncio import (AsyncEngine, AsyncSession,
                                        async_sessionmaker,
                                        create_async_engine)
except ImportError as exc:  # pragma: no cover - triggered only before dependency upgrade
    AsyncEngine = AsyncSession = None
    async_sessionmaker = None
    create_async_engine = None
    _ASYNC_IMPORT_ERROR = exc
else:
    _ASYNC_IMPORT_ERROR = None
try:
    from sqlalchemy.orm import DeclarativeBase
except ImportError:  # pragma: no cover - SQLAlchemy < 1.4 fallback
    from sqlalchemy.ext.declarative import declarative_base

    Base = declarative_base()
else:

    class Base(DeclarativeBase):
        __abstract__ = True


def _wrap_statement(db: "Database", stmt: Any) -> Any:
    if isinstance(stmt, Select):
        return SelectAdapter(db, stmt)
    if isinstance(stmt, Insert):
        return InsertAdapter(db, stmt)
    if isinstance(stmt, Update):
        return UpdateAdapter(db, stmt)
    if isinstance(stmt, Delete):
        return DeleteAdapter(db, stmt)
    return stmt


def _coerce_columns(columns: Tuple[Any, ...]) -> Tuple[Any, ...]:
    if len(columns) == 1 and isinstance(columns[0], (list, tuple, set)):
        columns = tuple(columns[0])
    return columns


class StatementAdapter:
    def __init__(self, db: "Database", stmt: Executable):
        self._db = db
        self._stmt = stmt
    def _wrap(self, stmt: Executable):
        return _wrap_statement(self._db, stmt)
    def __getattr__(self, item: str):
        attr = getattr(self._stmt, item)
        if callable(attr):
            def _wrapped(*args: Any, **kwargs: Any):
                result = attr(*args, **kwargs)
                return self._wrap(result)
            return _wrapped
        return attr
    async def execute(self, **params: Any):
        """Execute the wrapped statement with bound parameters."""
        async with self._db._execution_session() as session:
            return await session.execute(self._stmt, params)
    async def all(self, **params: Any):
        """Return every row produced by the wrapped statement."""
        result = await self.execute(**params)
        return result.all()
    async def first(self, **params: Any):
        """Return the first row produced by the wrapped statement."""
        result = await self.execute(**params)
        return result.first()
    async def scalar(self, **params: Any):
        """Return the first scalar produced by the wrapped statement."""
        result = await self.execute(**params)
        return result.scalar()
    async def status(self, **params: Any):
        """Return the affected-row count for the wrapped statement."""
        result = await self.execute(**params)
        return getattr(result, "rowcount", None)
    async def iterate(self, **params: Any):
        """Yield rows from the wrapped statement without buffering them."""
        async with self._db._execution_session() as session:
            async_result = await session.stream(self._stmt, params)
            async for row in async_result:
                yield row


class SelectAdapter(StatementAdapter):
    pass


class InsertAdapter(StatementAdapter):
    pass


class UpdateAdapter(StatementAdapter):
    pass


class DeleteAdapter(StatementAdapter):
    pass


class FuncProxy:
    def __init__(self, db: "Database"):
        self._db = db
    def __getattr__(self, item: str):
        attr = getattr(sa_func, item)

        def _call(*args: Any, **kwargs: Any):
            return attr(*args, **kwargs)

        return _call


class ConnectionProxy:
    def __init__(self, db: "Database", connection, raw_connection):
        self._db = db
        self._connection = connection
        self.raw_connection = raw_connection
    async def all(self, stmt: Any, **params: Any):
        """Execute a statement and return all rows on this connection."""
        stmt = sa_text(stmt) if isinstance(stmt, str) else stmt
        result = await self._connection.execute(stmt, params)
        return result.all()
    async def first(self, stmt: Any, **params: Any):
        """Execute a statement and return its first row."""
        stmt = sa_text(stmt) if isinstance(stmt, str) else stmt
        result = await self._connection.execute(stmt, params)
        return result.first()
    async def scalar(self, stmt: Any, **params: Any):
        """Execute a statement and return its first scalar value."""
        stmt = sa_text(stmt) if isinstance(stmt, str) else stmt
        result = await self._connection.execute(stmt, params)
        return result.scalar()
    async def status(self, stmt: Any, **params: Any):
        """Execute a statement and return its affected-row count."""
        stmt = sa_text(stmt) if isinstance(stmt, str) else stmt
        result = await self._connection.execute(stmt, params)
        return getattr(result, "rowcount", None)

    @asynccontextmanager
    async def transaction(self):
        """Expose the transaction already owned by this connection proxy."""
        yield self
    async def close(self):
        """Preserve the proxy close contract without closing its owner."""
        return None


_SESSION: contextvars.ContextVar[AsyncSession] = contextvars.ContextVar("db_session")


@dataclass(frozen=True)
class _TransactionBinding:
    database_id: int
    session: AsyncSession
    owner_task: Optional[asyncio.Task[Any]]


_TRANSACTION: contextvars.ContextVar[Tuple[_TransactionBinding, ...]] = (
    contextvars.ContextVar("db_transaction", default=())
)


def current_session() -> AsyncSession:
    """Return the SQLAlchemy session bound to the current context."""
    try:
        return _SESSION.get()
    except LookupError as exc:
        raise RuntimeError("No SQLAlchemy session bound to the current context") from exc


def _env_bool(value: Optional[str], default: bool = False) -> bool:
    if value is None:
        return default
    return value.lower() in {"1", "true", "on", "yes"}


@dataclass
class Database:
    engine: Optional[Any] = None
    session_factory: Optional[Any] = None
    func: FuncProxy = field(init=False, repr=False)
    _database_name: Optional[str] = field(init=False, default=None, repr=False)
    _database_override: Optional[str] = field(init=False, default=None, repr=False)

    text = staticmethod(sa_text)
    metadata = Base.metadata
    def __post_init__(self) -> None:
        self.func = FuncProxy(self)
    async def connect(self) -> None:
        """Create the configured async engine and session factory."""
        if _ASYNC_IMPORT_ERROR is not None:
            raise RuntimeError(
                "SQLAlchemy async support requires SQLAlchemy >= 1.4"
            ) from _ASYNC_IMPORT_ERROR

        requested_db = (
            self._database_override
            or os.getenv("HLTHPRT_DB_DATABASE_OVERRIDE")
            or os.getenv("HLTHPRT_DB_DATABASE", "postgres")
        )

        if self.engine is not None:
            if requested_db == self._database_name:
                return
            await self.disconnect()

        driver = os.getenv("HLTHPRT_DB_DRIVER", "postgresql+asyncpg")
        if driver == "asyncpg":
            driver = "postgresql+asyncpg"
        elif driver == "psycopg":
            driver = "postgresql+psycopg"

        url = URL.create(
            drivername=driver,
            username=os.getenv("HLTHPRT_DB_USER", "postgres"),
            password=os.getenv("HLTHPRT_DB_PASSWORD", ""),
            host=os.getenv("HLTHPRT_DB_HOST", "127.0.0.1"),
            port=int(os.getenv("HLTHPRT_DB_PORT", "5432")),
            database=requested_db,
        )

        pool_min = int(os.getenv("HLTHPRT_DB_POOL_MIN_SIZE", "1"))
        pool_max = int(os.getenv("HLTHPRT_DB_POOL_MAX_SIZE", "5"))
        pool_size = max(pool_min, 1)
        max_overflow = max(pool_max - pool_size, 0)

        self.engine = create_async_engine(
            url,
            pool_size=pool_size,
            max_overflow=max_overflow,
            echo=_env_bool(os.getenv("HLTHPRT_DB_ECHO")),
        )
        self.session_factory = async_sessionmaker(
            self.engine,
            expire_on_commit=False,
            autoflush=False,
        )
        self._database_name = requested_db
    def select(self, *columns: Any):
        """Build a select statement bound to this database helper."""
        columns = _coerce_columns(columns)
        stmt = sa_select(*columns)
        return SelectAdapter(self, stmt)
    def insert(self, *args: Any, **kwargs: Any):
        """Build a PostgreSQL-aware insert statement for a table or model."""
        target = args[0] if args else None
        table = None
        remaining_args = args
        if target is not None:
            if isinstance(target, SATable):
                table = target
            elif hasattr(target, "__table__"):
                table = target.__table__
                remaining_args = (table,) + args[1:]
        if table is not None:
            stmt = pg_insert(*remaining_args, **kwargs)
        else:
            stmt = sa_insert(*args, **kwargs)
        return InsertAdapter(self, stmt)
    def update(self, *args: Any, **kwargs: Any):
        """Build an update statement bound to this database helper."""
        stmt = sa_update(*args, **kwargs)
        return UpdateAdapter(self, stmt)
    def delete(self, *args: Any, **kwargs: Any):
        """Build a delete statement bound to this database helper."""
        stmt = sa_delete(*args, **kwargs)
        return DeleteAdapter(self, stmt)
    async def status(self, stmt: Any, **params: Any):
        """Execute a statement and return its affected-row count."""
        stmt = sa_text(stmt) if isinstance(stmt, str) else stmt
        async with self._execution_session() as session:
            result = await session.execute(stmt, params)
            return getattr(result, "rowcount", None)
    async def execute(self, stmt: Any, **params: Any):
        """Execute a statement in the current or a short-lived session."""
        stmt = sa_text(stmt) if isinstance(stmt, str) else stmt
        async with self._execution_session() as session:
            return await session.execute(stmt, params)
    async def all(self, stmt: Any, **params: Any):
        """Execute a statement and return all rows."""
        result = await self.execute(stmt, **params)
        return result.all()
    async def first(self, stmt: Any, **params: Any):
        """Execute a statement and return its first row."""
        result = await self.execute(stmt, **params)
        return result.first()
    async def scalar(self, stmt: Any, **params: Any):
        """Execute a statement and return its first scalar value."""
        result = await self.execute(stmt, **params)
        return result.scalar()
    async def stream(self, stmt: Any, **params: Any):
        """Execute a statement and return its streaming result."""
        stmt = sa_text(stmt) if isinstance(stmt, str) else stmt
        async with self._execution_session() as session:
            return await session.stream(stmt, params)
    async def create_table(self, table: SATable, **kwargs: Any) -> None:
        """Create a table and its schema when either is absent."""
        if self.engine is None:
            await self.connect()
        assert self.engine is not None
        async with self.engine.begin() as connection:
            if table.schema:
                preparer = connection.dialect.identifier_preparer
                schema_name = preparer.quote_schema(table.schema)
                await connection.exec_driver_sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
            await connection.run_sync(table.create, **kwargs)
    async def disconnect(self) -> None:
        """Dispose the active engine and clear connection state."""
        if self.engine is None:
            return
        await self.engine.dispose()
        self.engine = None
        self.session_factory = None
        self._database_name = None
    async def execute_ddl(self, statement: str) -> None:
        """Execute DDL through an autocommit connection."""
        if self.engine is None:
            await self.connect()
        assert self.engine is not None
        async with self.engine.connect() as connection:
            autocommit_conn = connection.execution_options(isolation_level="AUTOCOMMIT")
            if inspect.isawaitable(autocommit_conn):
                autocommit_conn = await autocommit_conn
            await autocommit_conn.exec_driver_sql(statement)

    @asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
        """Yield a context-bound session with commit and rollback handling."""
        if _ASYNC_IMPORT_ERROR is not None:
            raise RuntimeError(
                "SQLAlchemy async support requires SQLAlchemy >= 1.4"
            ) from _ASYNC_IMPORT_ERROR
        if self.session_factory is None:
            await self.connect()
        assert self.session_factory is not None
        session = self.session_factory()
        token = _SESSION.set(session)
        try:
            yield session
            if session.in_transaction():
                await session.commit()
        except Exception:
            if session.in_transaction():
                await session.rollback()
            raise
        finally:
            await session.close()
            _SESSION.reset(token)
    def _transaction_binding(self) -> Optional[_TransactionBinding]:
        binding = next(
            (
                candidate
                for candidate in reversed(_TRANSACTION.get())
                if candidate.database_id == id(self)
            ),
            None,
        )
        if binding is None:
            return None
        if binding.owner_task is not asyncio.current_task():
            raise RuntimeError(
                "Transaction-bound database helpers cannot run in a child asyncio task"
            )
        return binding

    @asynccontextmanager
    async def _execution_session(self) -> AsyncIterator[AsyncSession]:
        binding = self._transaction_binding()
        if binding is not None:
            yield binding.session
            return
        async with self.session() as session:
            yield session

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[AsyncSession]:
        """Yield an owned transaction or a nested transaction when reentered."""
        binding = self._transaction_binding()
        if binding is not None:
            async with binding.session.begin_nested():
                yield binding.session
            return
        async with self.session() as session:
            token = _TRANSACTION.set(
                _TRANSACTION.get()
                + (
                    _TransactionBinding(
                        database_id=id(self),
                        session=session,
                        owner_task=asyncio.current_task(),
                    ),
                )
            )
            try:
                async with session.begin():
                    yield session
            finally:
                _TRANSACTION.reset(token)

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[ConnectionProxy]:
        """Yield a compatibility proxy around an engine-owned connection."""
        if self.engine is None:
            await self.connect()
        assert self.engine is not None
        async with self.engine.begin() as connection:
            raw_connection = await connection.get_raw_connection()
            proxy = ConnectionProxy(self, connection, raw_connection)
            yield proxy

    @asynccontextmanager
    async def acquire_driver(self) -> AsyncIterator[Any]:
        """Yield a raw driver connection without a SQLAlchemy-owned transaction."""

        if self.engine is None:
            await self.connect()
        assert self.engine is not None
        async with self.engine.connect() as connection:
            raw_connection = await connection.get_raw_connection()
            driver_connection = getattr(
                raw_connection,
                "driver_connection",
                raw_connection,
            )
            try:
                yield driver_connection
            except BaseException:
                invalidate_task = asyncio.create_task(connection.invalidate())
                try:
                    await asyncio.shield(invalidate_task)
                except asyncio.CancelledError:
                    await invalidate_task
                raise
    def init_app(self, app) -> None:
        """Register database lifecycle and request-session hooks on an app."""
        if _ASYNC_IMPORT_ERROR is not None:
            raise RuntimeError(
                "SQLAlchemy async support requires SQLAlchemy >= 1.4"
            ) from _ASYNC_IMPORT_ERROR

        @app.listener("after_server_start")
        async def _on_start(_, __):
            await self.connect()

        @app.listener("before_server_stop")
        async def _on_stop(_, __):
            await self.disconnect()

        @app.middleware("request")
        async def _bind_session(request):
            if self.session_factory is None:
                await self.connect()
            assert self.session_factory is not None
            session = self.session_factory()
            token = _SESSION.set(session)
            request.ctx.sa_session = session
            request.ctx.session = session
            request.ctx._sa_session_token = token

        @app.middleware("response")
        async def _cleanup_session(request, response):
            session = getattr(request.ctx, "sa_session", None)
            token = getattr(request.ctx, "_sa_session_token", None)
            if session is None:
                return
            try:
                if session.in_transaction():
                    status = getattr(response, "status", 500) if response is not None else 500
                    if status < 400:
                        await session.commit()
                    else:
                        await session.rollback()
            finally:
                await session.close()
                if token is not None:
                    _SESSION.reset(token)


db = Database()


async def init_db(_: Any = None, loop: Any = None) -> None:
    """Backward-compatible helper retained for legacy importers."""
    await db.connect()


__all__ = [
    "Base",
    "Database",
    "ConnectionProxy",
    "SelectAdapter",
    "InsertAdapter",
    "UpdateAdapter",
    "DeleteAdapter",
    "FuncProxy",
    "current_session",
    "db",
    "init_db",
]
