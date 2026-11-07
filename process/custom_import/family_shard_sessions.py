# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Explicit owner-import SQL memory, isolated to each physical transaction.

The trusted job supplies this factory only to selected import phases. The
shared database/API factory and role defaults are unchanged. The caller budgets
concurrent query operations/workers; a valid PostgreSQL setting is not admission
for that much aggregate memory. Native pool-reset qualification is separate.
"""

from contextlib import asynccontextmanager

from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession

# PostgreSQL's 64-bit memory GUC maximum is INT_MAX kilobytes (MAX_KILOBYTES).
# This integer-MB interface admits only whole MB within that native domain;
# smaller-platform/server rejections propagate without falling back silently.
_MAX_MEMORY_MB = (2**31 - 1) // 1024


def _setting(name, memory_mb):
    if type(memory_mb) is not int or not 1 <= memory_mb <= _MAX_MEMORY_MB:
        raise ValueError(f"import {name} requires positive integer MB in the PostgreSQL range")
    return f"SET LOCAL {name} = '{memory_mb}MB'"


def _clean_owner_session(session):
    if not isinstance(session, AsyncSession):
        raise ValueError("import SQL settings require an owned AsyncSession")
    if (
        session.in_transaction()
        or session.new
        or session.dirty
        or session.deleted
        or (isinstance(session.bind, AsyncConnection) and session.bind.in_transaction())
    ):
        raise ValueError("import SQL settings require a clean session without a borrowed transaction")


def import_session_factory(base_factory, *, work_mem_mb, maintenance_work_mem_mb=None):
    """Wrap an owner factory with transaction-local settings, never a global hook.

    The existing factory retains its commit/rollback/close semantics. Maintenance
    memory is opt-in for the existing CREATE INDEX phase; ordinary work memory
    does not tune it. Session ownership, isolation, authority and query budgets
    remain the import callers' responsibility.
    """
    if not callable(base_factory):
        raise ValueError("import SQL settings require the existing session factory")
    statements = (_setting("work_mem", work_mem_mb),)
    if maintenance_work_mem_mb is not None:
        statements += (_setting("maintenance_work_mem", maintenance_work_mem_mb),)

    @asynccontextmanager
    async def open_import_session():
        """Scope the listener to one fresh session before any transaction starts."""
        owned, listener = None, None
        try:
            async with base_factory() as session:
                _clean_owner_session(session)

                def begin_settings(_session, transaction, connection):
                    """Use utility SET so the caller may still select RO/RR isolation."""
                    if not transaction.nested:
                        for statement in statements:
                            connection.exec_driver_sql(statement)

                event.listen(session.sync_session, "after_begin", begin_settings)
                owned, listener = session, begin_settings
                yield session
        finally:
            if owned is not None:
                event.remove(owned.sync_session, "after_begin", listener)

    return open_import_session
