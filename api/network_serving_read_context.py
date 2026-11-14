# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Keep serving reads in an ordinary read-only repeatable-read transaction."""

from contextlib import asynccontextmanager, nullcontext

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from db.connection import _READER, current_session, db
from process.network_serving_read import NetworkServingReadUnavailable

_SNAPSHOT_SQL = "SELECT current_setting('transaction_isolation'),current_setting('transaction_read_only')"


async def bind_network_request_reader(request):
    """Bind an authenticated request to its configured SELECT-only Reader."""
    try:
        return await db.bind_request_reader(request)
    except (RuntimeError, SQLAlchemyError, OSError) as error:
        raise NetworkServingReadUnavailable("Network serving request Reader is unavailable") from error


async def _read_driver(session):
    """Use the caller's connection without treating its identity as authority."""
    connection = await session.connection()
    return (await connection.get_raw_connection()).driver_connection


def _entry_snapshot(session):
    """Keep the existing caller tuple without collecting connection identities."""
    is_active = session.in_transaction()
    if type(is_active) is not bool or session.get_nested_transaction() is not None:
        raise NetworkServingReadUnavailable("Network serving read transaction is unavailable")
    reader = _READER.get() if is_active else None
    _require_reader_owner(session, reader)
    return is_active, session.get_transaction(), reader


def _require_reader_owner(session, reader):
    """Keep existing session lifecycle ownership without native identity metadata."""
    if reader is None:
        return
    try:
        if (
            _READER.get() is not reader
            or current_session() is not session
            or session.info.get("api_reader_verified") is not True
        ):
            raise RuntimeError("configured Reader is unavailable")
    except RuntimeError as error:
        raise NetworkServingReadUnavailable("Network serving Reader is unavailable") from error


async def _require_read_modes(session):
    """Preserve the unavailable response for mode-query and connection failures."""
    try:
        if not session.in_transaction() or session.get_nested_transaction() is not None:
            raise NetworkServingReadUnavailable("Network serving read transaction ended")
        driver = await _read_driver(session)
        modes = await driver.fetchrow(_SNAPSHOT_SQL)
        if modes is None or tuple(modes) not in (("repeatable read", "on"), ("serializable", "on")):
            raise NetworkServingReadUnavailable("Network serving read snapshot is unavailable")
        return driver
    except Exception as error:
        raise NetworkServingReadUnavailable("Network serving read snapshot is unavailable") from error


@asynccontextmanager
async def network_serving_read_context(session):
    """Borrow a read snapshot or own a fresh one; PostgreSQL enforces access."""
    is_active = session.in_transaction()
    if type(is_active) is not bool or session.get_nested_transaction() is not None:
        raise NetworkServingReadUnavailable("Network serving read transaction is unavailable")
    async with nullcontext() if is_active else session.begin():
        try:
            if not is_active:
                await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            await session.execute(text("SET LOCAL lock_timeout='1s'"))
            driver = await _require_read_modes(session)
        except Exception as error:
            raise NetworkServingReadUnavailable("Network serving read snapshot is unavailable") from error
        yield driver
        await _require_read_modes(session)
