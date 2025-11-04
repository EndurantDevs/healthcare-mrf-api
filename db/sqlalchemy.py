"""SQLAlchemy 2 async infrastructure shared by API and workers."""

from __future__ import annotations

import contextvars
import os
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Iterable, Optional, Tuple

from sqlalchemy import delete as sa_delete
from sqlalchemy import func as sa_func
from sqlalchemy import insert as sa_insert
from sqlalchemy import select as sa_select
from sqlalchemy import text as sa_text
from sqlalchemy import update as sa_update
from sqlalchemy.engine.url import URL
from sqlalchemy.schema import Table as SATable
from sqlalchemy.sql import Executable, Select
from sqlalchemy.sql.dml import Delete, Insert, Update
from sqlalchemy.sql.elements import ClauseElement

try:
    from sqlalchemy.ext.asyncio import (
        AsyncEngine,
        AsyncSession,
        async_sessionmaker,
        create_async_engine,
    )
except ImportError as exc:  # pragma: no cover - triggered only before dependency upgrade
    AsyncEngine = AsyncSession = None  # type: ignore[assignment]
    async_sessionmaker = None  # type: ignore[assignment]
    create_async_engine = None  # type: ignore[assignment]
    _ASYNC_IMPORT_ERROR = exc
else:
    _ASYNC_IMPORT_ERROR = None
try:
    from sqlalchemy.orm import DeclarativeBase
except ImportError:  # pragma: no cover - SQLAlchemy < 1.4 fallback
    from sqlalchemy.ext.declarative import declarative_base

    Base = declarative_base()  # type: ignore[assignment]
else:

    class Base(DeclarativeBase):
        __abstract__ = True

        def __init_subclass__(cls, **kwargs):
            super().__init_subclass__(**kwargs)
            database = globals().get("db")
            if database is not None and hasattr(cls, "__table__"):
                attach_gino(cls.__table__, database)

        @classmethod
        def query(cls):
            return globals()["db"].select(cls)

        @classmethod
        def insert(cls):
            return globals()["db"].insert(cls)


def _wrap_statement(db: "Database", stmt: Any) -> Any:
    if isinstance(stmt, Select):
        return SelectAdapter(db, stmt)
    if isinstance(stmt, Insert):
        return InsertAdapter(db, stmt)
    if isinstance(stmt, Update):
        return UpdateAdapter(db, stmt)
    if isinstance(stmt, Delete):
        return DeleteAdapter(db, stmt)
    if isinstance(stmt, ClauseElement):
        return attach_gino(stmt, db)
    return stmt


def _coerce_columns(columns: Tuple[Any, ...]) -> Tuple[Any, ...]:
    if len(columns) == 1 and isinstance(columns[0], (list, tuple, set)):
        columns = tuple(columns[0])
    return columns


class StatementExecutor:
    def __init__(self, db: "Database", stmt: Executable):
        self._db = db
        self._stmt = stmt

    def model(self, _model: Any) -> "StatementExecutor":
        # Compatibility shim: GINO allows chaining .model(cls) before execution.
        return self

    async def all(self, **params: Any):
        async with self._db.session() as session:
            result = await session.execute(self._stmt, params)
            return result.all()

    async def first(self, **params: Any):
        async with self._db.session() as session:
            result = await session.execute(self._stmt, params)
            return result.first()

    async def scalar(self, **params: Any):
        async with self._db.session() as session:
            result = await session.execute(self._stmt, params)
            return result.scalar()

    async def status(self, **params: Any):
        async with self._db.session() as session:
            result = await session.execute(self._stmt, params)
            return getattr(result, "rowcount", None)

    async def iterate(self, **params: Any):
        async with self._db.session() as session:
            result = await session.execute(self._stmt, params)
            for row in result:
                yield row

    async def load(self, _models: Any, **params: Any):
        async with self._db.session() as session:
            result = await session.execute(self._stmt, params)
            return result.all()


class TableExecutor:
    def __init__(self, db: "Database", table: SATable):
        self._db = db
        self._table = table

    async def create(self, **kwargs: Any):
        if self._db.engine is None:
            await self._db.connect()
        assert self._db.engine is not None
        async with self._db.engine.begin() as connection:
            await connection.run_sync(self._table.create, **kwargs)


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

    @property
    def gino(self) -> StatementExecutor:
        return StatementExecutor(self._db, self._stmt)


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
            expr = attr(*args, **kwargs)
            return attach_gino(expr, self._db)

        return _call


def attach_gino(expression: Any, db: "Database"):
    if hasattr(expression, "gino"):
        return expression
    if isinstance(expression, SATable):
        executor = TableExecutor(db, expression)
    elif isinstance(expression, Executable):
        executor = StatementExecutor(db, expression)
    else:
        executor = StatementExecutor(db, sa_select(expression))
    setattr(expression, "gino", executor)
    return expression


class ConnectionProxy:
    def __init__(self, db: "Database", connection, raw_connection):
        self._db = db
        self._connection = connection
        self.raw_connection = raw_connection

    async def all(self, stmt: Any, **params: Any):
        stmt = sa_text(stmt) if isinstance(stmt, str) else stmt
        result = await self._connection.execute(stmt, params)
        return result.all()

    async def first(self, stmt: Any, **params: Any):
        stmt = sa_text(stmt) if isinstance(stmt, str) else stmt
        result = await self._connection.execute(stmt, params)
        return result.first()

    async def scalar(self, stmt: Any, **params: Any):
        stmt = sa_text(stmt) if isinstance(stmt, str) else stmt
        result = await self._connection.execute(stmt, params)
        return result.scalar()

    async def status(self, stmt: Any, **params: Any):
        stmt = sa_text(stmt) if isinstance(stmt, str) else stmt
        result = await self._connection.execute(stmt, params)
        return getattr(result, "rowcount", None)

    async def close(self):
        return None


_SESSION: contextvars.ContextVar[AsyncSession] = contextvars.ContextVar("db_session")


def current_session() -> AsyncSession:
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

    text = staticmethod(sa_text)
    metadata = Base.metadata

    def __post_init__(self) -> None:
        self.func = FuncProxy(self)

    async def connect(self) -> None:
        if self.engine is not None:
            return

        if _ASYNC_IMPORT_ERROR is not None:
            raise RuntimeError(
                "SQLAlchemy async support requires SQLAlchemy >= 1.4"
            ) from _ASYNC_IMPORT_ERROR

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
            database=os.getenv("HLTHPRT_DB_DATABASE", "postgres"),
        )

        pool_min = int(os.getenv("HLTHPRT_DB_POOL_MIN_SIZE", "1"))
        pool_max = int(os.getenv("HLTHPRT_DB_POOL_MAX_SIZE", "5"))
        pool_size = max(pool_min, 1)
        max_overflow = max(pool_max - pool_size, 0)

        self.engine = create_async_engine(  # type: ignore[operator]
            url,
            pool_size=pool_size,
            max_overflow=max_overflow,
            echo=_env_bool(os.getenv("HLTHPRT_DB_ECHO")),
        )
        self.session_factory = async_sessionmaker(  # type: ignore[operator]
            self.engine,
            expire_on_commit=False,
            autoflush=False,
        )

    def select(self, *columns: Any):
        columns = _coerce_columns(columns)
        stmt = sa_select(*columns)
        return SelectAdapter(self, stmt)

    def insert(self, *args: Any, **kwargs: Any):
        stmt = sa_insert(*args, **kwargs)
        return InsertAdapter(self, stmt)

    def update(self, *args: Any, **kwargs: Any):
        stmt = sa_update(*args, **kwargs)
        return UpdateAdapter(self, stmt)

    def delete(self, *args: Any, **kwargs: Any):
        stmt = sa_delete(*args, **kwargs)
        return DeleteAdapter(self, stmt)

    async def status(self, stmt: Any, **params: Any):
        stmt = sa_text(stmt) if isinstance(stmt, str) else stmt
        async with self.session() as session:
            result = await session.execute(stmt, params)
            return getattr(result, "rowcount", None)

    async def disconnect(self) -> None:
        if self.engine is None:
            return
        await self.engine.dispose()
        self.engine = None
        self.session_factory = None

    @asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
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

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[AsyncSession]:
        async with self.session() as session:
            async with session.begin():
                yield session

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[ConnectionProxy]:
        if self.engine is None:
            await self.connect()
        assert self.engine is not None
        async with self.engine.begin() as connection:
            raw_connection = await connection.get_raw_connection()
            proxy = ConnectionProxy(self, connection, raw_connection)
            yield proxy

    def init_app(self, app) -> None:
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
            request.ctx._sa_session_token = token  # type: ignore[attr-defined]

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
