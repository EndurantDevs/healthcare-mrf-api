# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Run transport fakes through the real Reader factory/ownership context."""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

from api import signed_request_reader as boundary
from db.connection import Database
from tests.test_api_reader_sessions import ReaderSession


def cold_caller():
    return SimpleNamespace(in_transaction=lambda: False)


def _transport_session(session):
    original_execute = session.execute
    original_in_transaction = session.in_transaction
    state_by_field = {"scope": None, "active": False}
    session.info = {}
    session.reader_setup = ReaderSession()
    session.begin_nested = session.reader_setup.begin_nested

    original_connection = getattr(session, "connection", None)

    async def start():
        if not state_by_field["active"]:
            scope = session.begin()
            await scope.__aenter__()
            state_by_field.update(scope=scope, active=True)
            await original_execute(
                getattr(session, "snapshot_sql", "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            )

    async def connection():
        await start()
        return await original_connection() if original_connection is not None else None

    async def execute(statement, *arguments, **options):
        if str(statement) in {"SELECT 1", "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"}:
            # Start the configured Reader without taking a data snapshot.
            await start()
            return None
        sql = str(statement)
        if sql in {
            "SET LOCAL lock_timeout = '250ms'",
            "SET LOCAL statement_timeout = '5s'",
            "SELECT current_setting('transaction_isolation'), current_setting('transaction_read_only')",
        } or (sql.startswith("LOCK TABLE ") and sql.endswith(" IN ACCESS SHARE MODE")):
            await start()
            return await session.reader_setup.execute(statement, *arguments, **options)
        return await original_execute(statement, *arguments, **options)

    async def finish(failure=None):
        if state_by_field["active"]:
            await state_by_field["scope"].__aexit__(type(failure) if failure else None, failure, None)
            state_by_field["active"] = False

    async def commit():
        await finish()

    async def rollback():
        await finish(RuntimeError("synthetic Reader rollback"))

    async def close():
        await finish()

    session.connection, session.execute = connection, execute
    session.in_transaction = lambda: state_by_field["active"] or original_in_transaction()
    session.commit, session.rollback, session.close = commit, rollback, close
    return session


def install_reader_factory(monkeypatch, session=None):
    """Authenticate unit transport payloads; native tests qualify real SQL separately."""
    session = (
        ReaderSession()
        if session is None
        else session
        if isinstance(session, ReaderSession)
        else _transport_session(session)
    )
    reader = Database(session_factory=lambda: session)
    reader._reader_login = ("reader_test", "")
    database = Database()

    async def connect_reader():
        return reader

    @asynccontextmanager
    async def empty_workspace(_reader):
        yield None

    monkeypatch.setattr(database, "_connect_reader", connect_reader)
    monkeypatch.setattr(boundary, "db", database)
    monkeypatch.setattr("api.reference_family_reads.pin_claims_reader", AsyncMock(return_value=None))
    monkeypatch.setattr("db.reader_plan_workspace.plan_reader_connection", empty_workspace)
    return database
