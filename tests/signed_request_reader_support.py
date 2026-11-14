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

    original_connection = session.connection

    async def connection():
        if not state_by_field["active"]:
            scope = session.begin()
            await scope.__aenter__()
            state_by_field.update(scope=scope, active=True)
            await original_execute(
                getattr(session, "snapshot_sql", "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            )
        return await original_connection()

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

    session.connection = connection
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
