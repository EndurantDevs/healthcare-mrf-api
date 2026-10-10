# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Own the current-company fence before a scope approval's repeatable snapshot."""

from __future__ import annotations

import asyncio
import hashlib
from contextlib import asynccontextmanager
from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker
from sqlalchemy.pool import AsyncAdaptedQueuePool

from process.network_address_projection import _identifier
from process.uhc_flex_practitioner_async_safety import drain_operation

_FENCE_INFO = "registry_company_approval_fence"


def _fence_key(control_schema):
    _identifier(control_schema)
    return int.from_bytes(
        hashlib.sha256(("registry_company_approval:v1:" + control_schema).encode()).digest()[:8], "big", signed=True
    )


@dataclass
class _ScopeFence:
    session: object
    transaction: object
    control_schema: str
    key: int


def _source_engine(sessions):
    if type(sessions) is not async_sessionmaker:
        raise ValueError("registry_ptg_scope_company_fence_unavailable")
    engine = sessions.kw.get("bind")
    if (
        type(engine) is not AsyncEngine
        or sessions.class_ is not AsyncSession
        or sessions.kw.get("binds")
        or engine.dialect.name != "postgresql"
        or engine.dialect.driver != "asyncpg"
        or type(engine.pool) is not AsyncAdaptedQueuePool
        or engine.pool.size() != 1
        or engine.pool._max_overflow != 0
        or engine.pool.checkedout()
    ):
        raise ValueError("registry_ptg_scope_company_fence_unavailable")
    return engine


async def lock_registry_company_approval_writer(connection, control_schema):
    """Serialize the actual pointer writer before its existing native row lock."""
    if not connection.is_in_transaction():
        raise ValueError("registry_approval_requires_caller_transaction")
    await connection.execute("SELECT pg_catalog.pg_advisory_xact_lock($1)", _fence_key(control_schema))


async def require_registry_company_approval_fence(session, control_schema, *, is_exclusive=False):
    """Require the server-owned pre-snapshot context and its actual native lock."""
    if type(is_exclusive) is not bool:
        raise ValueError("registry_ptg_scope_company_fence_unavailable")
    fence = session.info.get(_FENCE_INFO)
    if (
        type(fence) is not _ScopeFence
        or fence.session is not session
        or fence.transaction is not session.get_transaction()
        or not session.in_transaction()
        or fence.control_schema != control_schema
    ):
        raise ValueError("registry_ptg_scope_company_fence_unavailable")
    mode = "ExclusiveLock" if is_exclusive else "ShareLock"
    held = await session.scalar(
        text(
            """SELECT EXISTS(SELECT FROM pg_catalog.pg_locks
      WHERE locktype='advisory' AND pid=pg_catalog.pg_backend_pid() AND granted AND mode='ShareLock'
        AND database=(SELECT oid FROM pg_catalog.pg_database WHERE datname=pg_catalog.current_database())
        AND classid=CAST(:high AS pg_catalog.oid) AND objid=CAST(:low AS pg_catalog.oid) AND objsubid=1)""".replace(
                "mode='ShareLock'", "mode='" + mode + "'"
            )
        ),
        {"high": (fence.key >> 32) & 0xFFFFFFFF, "low": fence.key & 0xFFFFFFFF},
    )
    if held is not True:
        raise ValueError("registry_ptg_scope_company_fence_unavailable")


async def _release_connection_fence(connection, key, *, is_exclusive=False):
    try:
        if connection.in_transaction():
            raise ValueError("registry_ptg_scope_company_fence_unavailable")
        await connection.execution_options(isolation_level="AUTOCOMMIT")
        unlock_function = "pg_advisory_unlock" if is_exclusive else "pg_advisory_unlock_shared"
        released = await connection.scalar(text(f"SELECT pg_catalog.{unlock_function}(:key)"), {"key": key})
        await connection.commit()
        if released is not True:
            raise ValueError("registry_ptg_scope_company_fence_unavailable")
    except BaseException:
        await drain_operation(connection.invalidate(), preserve_cancellation=False)
        raise


async def _finish_connection(connection, has_failed):
    if connection.sync_connection is None:
        return
    try:
        if has_failed:
            await drain_operation(connection.invalidate(), preserve_cancellation=False)
    finally:
        try:
            await connection.close()
        except BaseException:
            await drain_operation(connection.invalidate(), preserve_cancellation=False)
            await drain_operation(connection.close(), preserve_cancellation=False)
            raise


@asynccontextmanager
async def _company_approval_connection(sessions):
    """Retain only the genuine single-factory connection through native phases."""
    engine = _source_engine(sessions)
    connection, has_failed, original_error = engine.connect(), True, None
    try:
        async with asyncio.timeout(5):
            await drain_operation(connection.start(), preserve_cancellation=True)
        yield connection
        has_failed = False
    except BaseException as error:
        original_error = error
        raise
    finally:
        try:
            await _finish_connection(connection, has_failed)
        except BaseException:
            if original_error is None:
                raise


@asynccontextmanager
async def _company_approval_snapshot(sessions, connection, *, control_schema, is_exclusive=False):
    """Take the native session lock before RR and release after its actual commit."""
    if type(is_exclusive) is not bool:
        raise ValueError("registry_ptg_scope_company_fence_unavailable")
    key = _fence_key(control_schema)
    await connection.execution_options(isolation_level="AUTOCOMMIT")
    lock_function = "pg_try_advisory_lock" if is_exclusive else "pg_try_advisory_lock_shared"
    acquired = await connection.scalar(text(f"SELECT pg_catalog.{lock_function}(:key)"), {"key": key})
    await connection.commit()
    if acquired is not True:
        raise ValueError("registry_ptg_scope_company_busy")
    await connection.execution_options(isolation_level="REPEATABLE READ")
    async with sessions(bind=connection) as session, session.begin():
        session.info[_FENCE_INFO] = _ScopeFence(session, session.get_transaction(), control_schema, key)
        try:
            yield session
        finally:
            session.info.pop(_FENCE_INFO, None)
    await _release_connection_fence(connection, key, is_exclusive=is_exclusive)


@asynccontextmanager
async def registry_company_approval_transaction(sessions, *, control_schema, is_exclusive=False):
    """Take a session fence in AUTOCOMMIT, then own a fresh RR approval session.

    Failed/cancelled owners retire the retained connection, preventing pooled
    lock leakage. The preparation lock is released before long native COPY.
    """
    if type(is_exclusive) is not bool:
        raise ValueError("registry_ptg_scope_company_fence_unavailable")
    async with _company_approval_connection(sessions) as connection:
        async with _company_approval_snapshot(
            sessions, connection, control_schema=control_schema, is_exclusive=is_exclusive
        ) as session:
            yield session
