# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Keep serving reads inside the caller's exact native read-only snapshot."""

from contextlib import asynccontextmanager, nullcontext

from sqlalchemy import text

from api.network_read_execution import _original_read_driver
from db.connection import _READER, current_session
from process.network_serving_read import NetworkServingReadUnavailable

_IDENTITY_FIELDS = ("role_oid", "database_oid", "server_address", "server_port", "backend_pid")
_SNAPSHOT_SQL = (
    "SELECT current_setting('transaction_isolation'),current_setting('transaction_read_only'),"
    "current_user=session_user,(SELECT oid FROM pg_catalog.pg_roles WHERE rolname=current_user),"
    "(SELECT oid FROM pg_catalog.pg_database WHERE datname=current_database()),"
    "host(inet_server_addr()),inet_server_port(),pg_backend_pid()"
)


def _require_reader_owner(session, reader):
    if reader is None:
        if _READER.get() is not None:
            raise NetworkServingReadUnavailable("Network serving reader differs")
        return
    binding, metadata, identity = reader
    if (
        _READER.get() is not binding
        or current_session() is not session
        or binding.session is not session
        or session.info.get("api_reader_verified") is not True
        or session.info.get("api_reader_identity") is not metadata
        or set(metadata) != set(_IDENTITY_FIELDS)
        or tuple(metadata[field] for field in _IDENTITY_FIELDS) != identity
    ):
        raise NetworkServingReadUnavailable("Network serving read snapshot is unavailable")


def _entry_snapshot(session):
    try:
        active, transaction, binding = session.in_transaction(), session.get_transaction(), _READER.get()
        if (
            type(active) is not bool
            or active != (transaction is not None)
            or session.get_nested_transaction() is not None
        ):
            raise ValueError("Network serving read transaction differs")
        reader = None
        if active:
            metadata = session.info.get("api_reader_identity")
            if binding is None or type(metadata) is not dict or set(metadata) != set(_IDENTITY_FIELDS):
                raise ValueError("Network serving reader is unavailable")
            reader = binding, metadata, tuple(metadata[field] for field in _IDENTITY_FIELDS)
        _require_reader_owner(session, reader)
        return active, transaction, reader
    except Exception as error:
        raise NetworkServingReadUnavailable("Network serving read snapshot is unavailable") from error


async def _verify_snapshot(session, transaction, reader, expected_driver=None):
    try:
        _require_reader_owner(session, reader)
        if (
            session.get_transaction() is not transaction
            or transaction is None
            or session.get_nested_transaction() is not None
        ):
            raise ValueError("Network serving read transaction differs")
        driver, backend_pid = await _original_read_driver(session)
        if expected_driver is not None and (driver is not expected_driver[0] or backend_pid != expected_driver[1]):
            raise ValueError("Network serving read backend differs")
        snapshot = await driver.fetchrow(_SNAPSHOT_SQL)
        actual_driver, actual_pid = await _original_read_driver(session)
        _require_reader_owner(session, reader)
        if (
            session.get_transaction() is not transaction
            or session.get_nested_transaction() is not None
            or actual_driver is not driver
            or actual_pid != backend_pid
            or snapshot is None
            or len(snapshot) != 8
            or tuple(snapshot[:2]) not in (("repeatable read", "on"), ("serializable", "on"))
            or snapshot[7] != backend_pid
            or reader is not None
            and (snapshot[2] is not True or tuple(snapshot[3:]) != reader[2])
        ):
            raise ValueError("Network serving read snapshot differs")
        return driver, backend_pid
    except Exception as error:
        raise NetworkServingReadUnavailable("Network serving read snapshot is unavailable") from error


@asynccontextmanager
async def network_serving_read_context(session):
    """Borrow an existing read-only snapshot or own one fresh transaction.

    Native serving and Source authority remain the callers' responsibility.
    Borrowed transactions, including failed or cancelled reads, remain caller-owned.
    """
    active, transaction, reader = _entry_snapshot(session)
    async with nullcontext() if active else session.begin():
        transaction = transaction if active else session.get_transaction()
        try:
            if not active:
                await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            driver, backend_pid = await _verify_snapshot(session, transaction, reader)
            await session.execute(text("SET LOCAL lock_timeout='1s'"))
            await _verify_snapshot(session, transaction, reader, (driver, backend_pid))
        except Exception as error:
            raise NetworkServingReadUnavailable("Network serving read snapshot is unavailable") from error
        yield driver
        await _verify_snapshot(session, transaction, reader, (driver, backend_pid))
