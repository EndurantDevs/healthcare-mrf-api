# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""SQLAlchemy 2 async infrastructure shared by API and workers."""

from __future__ import annotations

import asyncio
import contextvars
import inspect
import os
import sys
from contextlib import AsyncExitStack, asynccontextmanager
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
    from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
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

    DatabaseModelBase = declarative_base()
else:

    class DatabaseModelBase(DeclarativeBase):
        __abstract__ = True


Base = DatabaseModelBase


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


_SESSION: contextvars.ContextVar[Optional[AsyncSession]] = contextvars.ContextVar("db_session")


@dataclass(frozen=True)
class _TransactionBinding:
    database_id: int
    session: AsyncSession
    owner_task: Optional[asyncio.Task[Any]]
    borrowed: bool = False


_TRANSACTION: contextvars.ContextVar[Tuple[_TransactionBinding, ...]] = contextvars.ContextVar(
    "db_transaction", default=()
)
_READER: contextvars.ContextVar[Optional[_TransactionBinding]] = contextvars.ContextVar("db_reader", default=None)


def current_session() -> AsyncSession:
    """Return the SQLAlchemy session bound to the current context."""
    reader = _READER.get()
    if reader is not None and reader.owner_task is not asyncio.current_task():
        raise RuntimeError("Reader-bound helpers require a separate child Reader session")
    try:
        session = _SESSION.get()
    except LookupError as exc:
        raise RuntimeError("No SQLAlchemy session bound to the current context") from exc
    if session is None:
        raise RuntimeError("No SQLAlchemy session bound to the current context")
    return session


def _is_env_enabled(value: Optional[str], default: bool = False) -> bool:
    if value is None:
        return default
    return value.lower() in {"1", "true", "on", "yes"}


def has_reader_session(database) -> bool:
    """Check explicit Reader ownership without consulting request or manifest fields."""
    binding = _READER.get()
    if binding is None or binding.database_id != id(database):
        return False
    if binding.owner_task is not asyncio.current_task():
        raise RuntimeError("Reader-bound helpers require a separate child Reader session")
    return True


async def run_as_writer(operation, *args, **kwargs):
    """Create explicit background work only after leaving an inherited Reader."""
    reader_token = _READER.set(None)
    session_token = _SESSION.set(None)
    try:
        return await operation(*args, **kwargs)
    finally:
        _SESSION.reset(session_token)
        _READER.reset(reader_token)


async def gather_reader_calls(database, *operations):
    """Keep pinned reads on their owner; retain ordinary independent fan-out."""
    if not has_reader_session(database):
        return await asyncio.gather(*operations)
    try:
        if not all(
            inspect.iscoroutine(operation) and inspect.getcoroutinestate(operation) == inspect.CORO_CREATED
            for operation in operations
        ):
            raise TypeError("Pinned Reader calls require unopened coroutines")
        return [await operation for operation in operations]
    finally:
        for operation in operations:
            if inspect.iscoroutine(operation):
                operation.close()


@dataclass
class Database:
    engine: Optional[Any] = None
    session_factory: Optional[Any] = None
    func: FuncProxy = field(init=False, repr=False)
    _database_name: Optional[str] = field(init=False, default=None, repr=False)
    _database_override: Optional[str] = field(init=False, default=None, repr=False)
    _reader_database: Optional[Database] = field(init=False, default=None, repr=False)
    _reader_login: Optional[Tuple[str, str]] = field(init=False, default=None, repr=False)

    text = staticmethod(sa_text)
    metadata = Base.metadata

    def __post_init__(self) -> None:
        self.func = FuncProxy(self)

    async def connect(self) -> None:
        """Create the configured async engine and session factory."""
        if _ASYNC_IMPORT_ERROR is not None:
            raise RuntimeError("SQLAlchemy async support requires SQLAlchemy >= 1.4") from _ASYNC_IMPORT_ERROR

        requested_db = self._requested_database_name()

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
            username=self._reader_login[0] if self._reader_login else os.getenv("HLTHPRT_DB_USER", "postgres"),
            password=self._reader_login[1] if self._reader_login else os.getenv("HLTHPRT_DB_PASSWORD", ""),
            host=os.getenv("HLTHPRT_DB_HOST", "127.0.0.1"),
            port=int(os.getenv("HLTHPRT_DB_PORT", "5432")),
            database=requested_db,
        )

        prefix = "HLTHPRT_DB_READER" if self._reader_login else "HLTHPRT_DB"
        pool_min = int(os.getenv(f"{prefix}_POOL_MIN_SIZE", "1"))
        pool_max = int(os.getenv(f"{prefix}_POOL_MAX_SIZE", "5"))
        pool_size = max(pool_min, 1)
        max_overflow = max(pool_max - pool_size, 0)

        self.engine = create_async_engine(
            url,
            pool_size=pool_size,
            max_overflow=max_overflow,
            echo=_is_env_enabled(os.getenv("HLTHPRT_DB_ECHO")),
            hide_parameters=True,
            **(
                {"isolation_level": "REPEATABLE READ", "execution_options": {"postgresql_readonly": True}}
                if self._reader_login
                else {}
            ),
        )
        self.session_factory = async_sessionmaker(
            self.engine,
            expire_on_commit=False,
            autoflush=False,
        )
        self._database_name = requested_db

    def _requested_database_name(self) -> str:
        """Return the configured database identity without opening a connection."""

        return (
            self._database_override
            or os.getenv("HLTHPRT_DB_DATABASE_OVERRIDE")
            or os.getenv("HLTHPRT_DB_DATABASE", "postgres")
        )

    @staticmethod
    def _session_database_name(session: AsyncSession) -> str | None:
        """Read a session bind's database name when SQLAlchemy exposes one."""

        bind = getattr(session, "bind", None)
        if bind is None:
            get_bind = getattr(session, "get_bind", None)
            if callable(get_bind):
                bind = get_bind()
        bind_url = getattr(bind, "url", None)
        if bind_url is None:
            bind_url = getattr(getattr(bind, "engine", None), "url", None)
        database_name = getattr(bind_url, "database", None)
        return None if database_name is None else str(database_name)

    @staticmethod
    def _has_bound_request_session() -> bool:
        """Return whether this task already owns a request-scoped session."""

        try:
            current_session()
        except RuntimeError:
            return False
        return True

    def _validate_existing_session_binding(self, session: AsyncSession) -> None:
        """Fail closed before exposing a caller-owned transaction to helpers."""

        if not isinstance(session, AsyncSession):
            raise TypeError("bind_existing_session requires an AsyncSession")
        if _TRANSACTION.get():
            raise RuntimeError("cannot bind an existing session while a database transaction is already bound")
        if self._has_bound_request_session():
            raise RuntimeError("cannot bind an existing session while a request session is already bound")
        if not session.in_transaction():
            raise RuntimeError("bind_existing_session requires an active caller transaction")
        if session.in_nested_transaction():
            raise RuntimeError("bind_existing_session rejects a nested caller transaction")
        self._validate_existing_session_database(session)

    def _validate_existing_session_database(self, session: AsyncSession) -> None:
        """Reject a mismatched database when the SQLAlchemy bind names one."""

        expected_database_name = self._requested_database_name()
        bound_database_name = self._session_database_name(session)
        if bound_database_name is not None and expected_database_name and bound_database_name != expected_database_name:
            raise RuntimeError("bind_existing_session database identity does not match")

    @staticmethod
    def _require_active_borrowed_transaction(session: AsyncSession) -> None:
        """Prevent a callback from silently ending or leaking the caller transaction."""

        if not session.in_transaction():
            raise RuntimeError("borrowed caller transaction ended before bridge exit")
        if session.in_nested_transaction():
            raise RuntimeError("borrowed caller transaction left a nested transaction active")

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
        if self.has_reader_session():
            raise RuntimeError("Reader scopes cannot create tables")
        if self.engine is None:
            await self.connect()
        assert self.engine is not None
        async with self.engine.begin() as connection:
            if table.schema:
                schema_exists = await connection.scalar(
                    sa_text("SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = :schema)"),
                    {"schema": table.schema},
                )
                if not schema_exists:
                    preparer = connection.dialect.identifier_preparer
                    schema_name = preparer.quote_schema(table.schema)
                    await connection.exec_driver_sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
            await connection.run_sync(table.create, **kwargs)

    async def disconnect(self) -> None:
        """Dispose the active engine and clear connection state."""
        if self._reader_database is not None:
            await self._reader_database.disconnect()
            self._reader_database = None
        if self.engine is None:
            return
        await self.engine.dispose()
        self.engine = None
        self.session_factory = None
        self._database_name = None

    async def execute_ddl(self, statement: str) -> None:
        """Execute DDL through an autocommit connection."""
        if self.has_reader_session():
            raise RuntimeError("Reader scopes cannot execute DDL")
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
        reader = self._reader_binding()
        if reader is not None:
            yield reader.session
            return
        binding = self._transaction_binding(borrowed_only=True)
        if binding is not None:
            yield binding.session
            return
        if _ASYNC_IMPORT_ERROR is not None:
            raise RuntimeError("SQLAlchemy async support requires SQLAlchemy >= 1.4") from _ASYNC_IMPORT_ERROR
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

    def _transaction_binding(self, *, borrowed_only: bool = False) -> Optional[_TransactionBinding]:
        binding = next(
            (candidate for candidate in reversed(_TRANSACTION.get()) if candidate.database_id == id(self)),
            None,
        )
        if binding is None or (borrowed_only and not binding.borrowed):
            return None
        if binding.owner_task is not asyncio.current_task():
            raise RuntimeError("Transaction-bound database helpers cannot run in a child asyncio task")
        return binding

    def _reader_binding(self) -> Optional[_TransactionBinding]:
        binding = _READER.get()
        if binding is None or binding.database_id != id(self):
            return None
        if binding.owner_task is not asyncio.current_task():
            raise RuntimeError("Reader-bound helpers require a separate child Reader session")
        return binding

    def has_reader_session(self) -> bool:
        """Report an explicitly authenticated Reader owned by this task."""
        return has_reader_session(self)

    @staticmethod
    def is_api_reader_enabled() -> bool:
        """Select an explicit Reader rollout, never a connection-error fallback."""
        return _is_env_enabled(os.getenv("HLTHPRT_API_READER_ENABLED"))

    @staticmethod
    def _is_pinned_read_path(path) -> bool:
        """Preserve account, control and unrelated ordinary route ownership."""
        route, _, parameter = path.rstrip("/").rpartition("/")
        if route == "/api/v1/npi/id" and parameter != "batch":
            return False  # Detail owns a bounded Reader that closes before geocoding.
        roots = ("/api/v1/pricing", "/api/v1/codes", "/api/v1/npi")
        return (
            path in roots
            or path.startswith(tuple(root + "/" for root in roots))
            or path == "/api/v1/coverage/statistics"
        )

    async def _connect_reader(self) -> Database:
        """Create a separate login pool without falling back to Writer credentials."""
        user = os.getenv("HLTHPRT_DB_READER_USER", "").strip()
        if not user or user == os.getenv("HLTHPRT_DB_USER", "postgres"):
            raise RuntimeError("A distinct Reader login is required")
        if self._reader_database is None:
            self._reader_database = Database()
            self._reader_database._reader_login = (user, os.getenv("HLTHPRT_DB_READER_PASSWORD", ""))
        reader = self._reader_database
        if reader._reader_login[0] != user:
            raise RuntimeError("Reader login changed while its pool was active")
        reader._database_override = self._requested_database_name()
        await reader.connect()
        return reader

    @asynccontextmanager
    async def reader_session(self, *, independent: bool = False) -> AsyncIterator[AsyncSession]:
        """Own a genuine Reader transaction, or reuse this task's exact Reader."""
        binding = _READER.get()
        if (
            binding
            and binding.database_id == id(self)
            and binding.owner_task is asyncio.current_task()
            and not independent
        ):
            yield binding.session
            return
        if self._transaction_binding() is not None:
            raise RuntimeError("Cannot open a Reader inside a Writer transaction")
        reader = await self._connect_reader()
        async with reader.session() as session:
            reader_context_token = None
            try:
                identity_result = await session.execute(sa_text("SELECT session_user, current_user"))
                if tuple(identity_result.first()) != (reader._reader_login[0], reader._reader_login[0]):
                    raise RuntimeError("Reader session login identity differs")
                session.info["api_reader_verified"] = True
                reader_context_token = _READER.set(_TransactionBinding(id(self), session, asyncio.current_task()))
                yield session
            except asyncio.CancelledError:
                if session.in_transaction():
                    await session.rollback()
                raise
            finally:
                if reader_context_token is not None:
                    _READER.reset(reader_context_token)
                session.info.pop("api_reader_verified", None)

    async def _close_request_session(self, request, *, success):
        """Release the request owner even when pinning or response handling fails."""
        session = getattr(request.ctx, "sa_session", None)
        if session is None:
            return
        scope = getattr(request.ctx, "_sa_reader_scope", None)
        token = getattr(request.ctx, "_sa_session_token", None)
        try:
            if session.in_transaction():
                if success:
                    await session.commit()
                else:
                    await session.rollback()
        finally:
            try:
                if scope is not None:
                    await scope.__aexit__(*sys.exc_info())
                else:
                    await session.close()
                    if token is not None:
                        _SESSION.reset(token)
            finally:
                request.ctx.sa_session = None
                request.ctx.session = None
                request.ctx._sa_reader_scope = None

    @asynccontextmanager
    async def bind_existing_session(self, session: AsyncSession) -> AsyncIterator[AsyncSession]:
        """Temporarily bind one trusted caller-owned SQLAlchemy transaction.

        This narrow in-process bridge is only for a prepared native publication
        callback sharing a coordinator's final transaction.  The caller owns
        transport, heavy validation, the outer transaction, and session
        lifecycle.  Peer-provided callbacks, network work, and arbitrary SQL
        are outside this bridge's scope.

        The binding is task-local.  It neither creates an engine nor commits,
        rolls back, closes the caller session, or alters the module-level
        database object.
        """

        if _ASYNC_IMPORT_ERROR is not None:
            raise RuntimeError("SQLAlchemy async support requires SQLAlchemy >= 1.4") from _ASYNC_IMPORT_ERROR
        self._validate_existing_session_binding(session)
        owner_task = asyncio.current_task()
        if owner_task is None:
            raise RuntimeError("bind_existing_session requires an asyncio task")
        token = _TRANSACTION.set(
            _TRANSACTION.get()
            + (
                _TransactionBinding(
                    database_id=id(self),
                    session=session,
                    owner_task=owner_task,
                    borrowed=True,
                ),
            )
        )
        try:
            yield session
            self._require_active_borrowed_transaction(session)
        finally:
            _TRANSACTION.reset(token)

    @asynccontextmanager
    async def _execution_session(self) -> AsyncIterator[AsyncSession]:
        reader = self._reader_binding()
        if reader is not None:
            yield reader.session
            return
        binding = self._transaction_binding()
        if binding is not None:
            yield binding.session
            return
        async with self.session() as session:
            yield session

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[AsyncSession]:
        """Yield an owned transaction or a nested transaction when reentered."""
        reader = self._reader_binding()
        if reader is not None:
            async with reader.session.begin_nested():
                yield reader.session
            return
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
        reader = self._reader_binding()
        if reader is not None:
            yield ConnectionProxy(self, reader.session, None)
            return
        binding = self._transaction_binding(borrowed_only=True)
        if binding is not None:
            yield ConnectionProxy(self, binding.session, None)
            return
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
        if self.has_reader_session():
            raise RuntimeError("Reader scopes cannot acquire a Writer driver")
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
            raise RuntimeError("SQLAlchemy async support requires SQLAlchemy >= 1.4") from _ASYNC_IMPORT_ERROR

        @app.listener("before_server_start")
        async def _on_start(_, __):
            await self.connect()
            if self.is_api_reader_enabled():
                await self._connect_reader()
            assert self.engine is not None
            async with AsyncExitStack() as connections:
                for _ in range(self.engine.pool.size()):
                    await connections.enter_async_context(self.engine.connect())

        @app.listener("before_server_stop")
        async def _on_stop(_, __):
            await self.disconnect()

        @app.middleware("request")
        async def _bind_session(request):
            is_pinned_reader = self.is_api_reader_enabled() and self._is_pinned_read_path(request.path)
            if is_pinned_reader:
                scope = self.reader_session()
                session = await scope.__aenter__()
                request.ctx._sa_reader_scope = scope
            else:
                if self.session_factory is None:
                    await self.connect()
                assert self.session_factory is not None
                session = self.session_factory()
                request.ctx._sa_session_token = _SESSION.set(session)
            request.ctx.sa_session = session
            request.ctx.session = session
            if is_pinned_reader:
                from sanic.exceptions import ServiceUnavailable
                from sqlalchemy.exc import SQLAlchemyError

                from api.reference_family_reads import pin_claims_reader

                try:
                    await pin_claims_reader(session)
                except RuntimeError, SQLAlchemyError:
                    await self._close_request_session(request, success=False)
                    raise ServiceUnavailable("Pricing snapshot is temporarily unavailable") from None
                except BaseException:
                    await self._close_request_session(request, success=False)
                    raise

        @app.middleware("response")
        async def _cleanup_session(request, response):
            status = getattr(response, "status", 500) if response is not None else 500
            await self._close_request_session(request, success=status < 400)


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
