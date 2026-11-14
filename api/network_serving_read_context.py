# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Keep serving reads inside the caller's read-only snapshot."""

from contextlib import asynccontextmanager, nullcontext

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from db.connection import _READER, current_session, db
from process.network_serving_read import NetworkServingReadUnavailable

_SNAPSHOT_SQL = "SELECT current_setting('transaction_isolation'),current_setting('transaction_read_only')"


async def bind_network_request_reader(request):
    """Bind an authenticated request to its existing configured Reader."""
    try:
        return await db.bind_request_reader(request)
    except (RuntimeError, SQLAlchemyError, OSError) as error:
        raise NetworkServingReadUnavailable("Network serving request Reader is unavailable") from error


def _require_reader_owner(session, reader):
    if reader is None:
        if _READER.get() is not None:
            raise NetworkServingReadUnavailable("Network serving reader differs")
        return
    if (
        _READER.get() is not reader
        or current_session() is not session
        or reader.session is not session
        or session.info.get("api_reader_verified") is not True
    ):
        raise NetworkServingReadUnavailable("Network serving read snapshot is unavailable")


def _entry_snapshot(session):
    try:
        active, transaction, binding = session.in_transaction(), session.get_transaction(), _READER.get()
        if (
            type(active) is not bool
            or active != (transaction is not None)
            or session.get_nested_transaction() is not None
            or active
            and binding is None
        ):
            raise ValueError("Network serving read transaction differs")
        reader = binding if active else None
        _require_reader_owner(session, reader)
        return active, transaction, reader
    except Exception as error:
        raise NetworkServingReadUnavailable("Network serving read snapshot is unavailable") from error


async def _verify_snapshot(session, transaction, reader):
    try:
        _require_reader_owner(session, reader)
        if (
            session.get_transaction() is not transaction
            or transaction is None
            or session.get_nested_transaction() is not None
        ):
            raise ValueError("Network serving read transaction differs")
        modes = (await session.execute(text(_SNAPSHOT_SQL))).first()
        _require_reader_owner(session, reader)
        if (
            session.get_transaction() is not transaction
            or session.get_nested_transaction() is not None
            or modes is None
            or tuple(modes) not in (("repeatable read", "on"), ("serializable", "on"))
        ):
            raise ValueError("Network serving read transaction differs")
    except Exception as error:
        raise NetworkServingReadUnavailable("Network serving read snapshot is unavailable") from error


@asynccontextmanager
async def network_serving_read_context(session):
    """Borrow an existing read-only snapshot or own one fresh transaction.

    Borrowed transactions, including failed or cancelled reads, remain caller-owned.
    """
    active, transaction, reader = _entry_snapshot(session)
    async with nullcontext() if active else session.begin():
        transaction = transaction if active else session.get_transaction()
        try:
            if not active:
                await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            driver = (await (await session.connection()).get_raw_connection()).driver_connection
            await _verify_snapshot(session, transaction, reader)
            await session.execute(text("SET LOCAL lock_timeout='1s'"))
        except Exception as error:
            raise NetworkServingReadUnavailable("Network serving read snapshot is unavailable") from error
        yield driver
        await _verify_snapshot(session, transaction, reader)
