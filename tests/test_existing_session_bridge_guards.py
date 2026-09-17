# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Borrowed publication transactions remain caller-owned on every rejection."""

from types import SimpleNamespace

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from db import connection


def test_session_identity_uses_fallback_bind_without_opening_a_connection():
    session = SimpleNamespace(bind=None, get_bind=lambda: SimpleNamespace(url=SimpleNamespace(database="synthetic")))
    assert connection.Database._session_database_name(session) == "synthetic"
    assert connection.Database._session_database_name(SimpleNamespace()) is None


@pytest.mark.asyncio
async def test_bridge_rejects_non_session_without_installing_binding():
    database = connection.Database()
    with pytest.raises(TypeError, match="requires an AsyncSession"):
        async with database.bind_existing_session(object()):
            pytest.fail("invalid session admitted")
    assert database._transaction_binding() is None


@pytest.mark.asyncio
async def test_bridge_preserves_active_request_session(monkeypatch):
    request_session = object()
    monkeypatch.setattr(connection, "current_session", lambda: request_session)
    database = connection.Database()
    async with AsyncSession() as session, session.begin():
        with pytest.raises(RuntimeError, match="request session is already bound"):
            async with database.bind_existing_session(session):
                pytest.fail("request session replaced")
        assert session.in_transaction()
        assert connection.current_session() is request_session
        assert database._transaction_binding() is None


@pytest.mark.asyncio
async def test_bridge_does_not_accept_or_end_nested_caller_transaction():
    database = connection.Database()
    async with AsyncSession() as session, session.begin(), session.begin_nested():
        with pytest.raises(RuntimeError, match="rejects a nested caller transaction"):
            async with database.bind_existing_session(session):
                pytest.fail("nested transaction admitted")
        assert session.in_transaction() and session.in_nested_transaction()
        assert database._transaction_binding() is None


async def _violate_caller_transaction(database, session, violation):
    assert database._transaction_binding().session is session
    if violation == "ended":
        await session.rollback()
    else:
        await session.begin_nested()


@pytest.mark.asyncio
@pytest.mark.parametrize("violation", ["ended", "nested"])
async def test_bridge_detects_callback_transaction_violation_and_unbinds(violation):
    database = connection.Database()
    database._database_override = "synthetic"
    engine = create_async_engine("postgresql+asyncpg://tester@localhost/synthetic")
    try:
        async with AsyncSession(bind=engine) as session:
            await session.begin()
            with pytest.raises(RuntimeError, match="borrowed caller transaction"):
                async with database.bind_existing_session(session):
                    await _violate_caller_transaction(database, session, violation)
            assert database._transaction_binding() is None
            if violation == "nested":
                assert session.in_transaction() and session.in_nested_transaction()
            else:
                assert not session.in_transaction()
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_bridge_reports_missing_async_dependency_before_borrowing(monkeypatch):
    failure = ImportError("synthetic missing async dependency")
    monkeypatch.setattr(connection, "_ASYNC_IMPORT_ERROR", failure)
    database = connection.Database()
    with pytest.raises(RuntimeError, match="SQLAlchemy async support") as observed:
        async with database.bind_existing_session(object()):
            pytest.fail("missing dependency admitted")
    assert observed.value.__cause__ is failure
    assert database._transaction_binding() is None
