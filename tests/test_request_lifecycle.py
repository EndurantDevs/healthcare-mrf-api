# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Request finalizers retain exception identity and release task-local ownership."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sanic import Sanic

from api import request_lifecycle
from api.request_lifecycle import RequestLifecycleSanic
from db.connection import Database, current_session
from tests.test_api_reader_sessions import ReaderSession, reader_database
from tests.test_db_connection import _FakeSession
from tests.test_request_reader_ownership import _request_callbacks


def _request():
    return SimpleNamespace(path="/api/v1/pricing/synthetic", ctx=SimpleNamespace())


def _assert_released(request, database):
    assert request.ctx.sa_session is None
    assert request.ctx.session is None
    assert request.ctx._sa_reader_scope is None
    assert request.ctx._sa_session_token is None
    assert not database.has_reader_session()
    with pytest.raises(RuntimeError, match="No SQLAlchemy"):
        current_session()


@pytest.mark.parametrize("failure_type", [RuntimeError, asyncio.CancelledError])
@pytest.mark.parametrize("cleanup_failure", [None, "rollback", "close"])
async def test_request_finalizer_preserves_primary_failure_and_resets_tokens(
    monkeypatch, failure_type, cleanup_failure
):
    monkeypatch.setenv("HLTHPRT_API_READER_ENABLED", "false")
    session = _FakeSession()
    database = Database(session_factory=lambda: session)
    monkeypatch.setattr(request_lifecycle, "db", database)
    callbacks = _request_callbacks(database, monkeypatch)
    failure = failure_type("synthetic request failure")
    if cleanup_failure is not None:
        monkeypatch.setattr(session, cleanup_failure, AsyncMock(side_effect=ValueError("private cleanup detail")))

    async def handle_request(_app, request):
        await callbacks["request"](request)
        session._in_tx = True
        raise failure

    monkeypatch.setattr(Sanic, "handle_request", handle_request)
    app = RequestLifecycleSanic("request-failure-" + uuid4().hex)
    request = _request()
    with pytest.raises(failure_type) as caught:
        await app.handle_request(request)
    assert caught.value is failure
    assert getattr(failure, "__notes__", []) == (
        ["Request session cleanup failed: ValueError"] if cleanup_failure is not None else []
    )
    assert not session.committed
    _assert_released(request, database)


@pytest.mark.parametrize("status", [200, 500])
async def test_normal_response_middleware_owns_commit_and_finalizer_is_idempotent(monkeypatch, status):
    monkeypatch.setenv("HLTHPRT_API_READER_ENABLED", "false")
    session = _FakeSession()
    session.close = AsyncMock(wraps=session.close)
    database = Database(session_factory=lambda: session)
    monkeypatch.setattr(request_lifecycle, "db", database)
    callbacks = _request_callbacks(database, monkeypatch)

    async def handle_request(_app, request):
        await callbacks["request"](request)
        session._in_tx = True
        await callbacks["response"](request, SimpleNamespace(status=status))

    monkeypatch.setattr(Sanic, "handle_request", handle_request)
    app = RequestLifecycleSanic("request-response-" + uuid4().hex)
    request = _request()
    await app.handle_request(request)
    assert session.committed is (status < 400)
    assert session.rolled_back is (status >= 400)
    session.close.assert_awaited_once()
    _assert_released(request, database)


@pytest.mark.parametrize("close_failure", [False, True])
async def test_finalizer_rolls_back_orphan_and_reports_cleanup_failure(monkeypatch, close_failure):
    monkeypatch.setenv("HLTHPRT_API_READER_ENABLED", "false")
    session = _FakeSession()
    database = Database(session_factory=lambda: session)
    monkeypatch.setattr(request_lifecycle, "db", database)
    callbacks = _request_callbacks(database, monkeypatch)
    failure = ValueError("synthetic close failure")
    if close_failure:
        session.close = AsyncMock(side_effect=failure)

    async def handle_request(_app, request):
        await callbacks["request"](request)
        session._in_tx = True

    monkeypatch.setattr(Sanic, "handle_request", handle_request)
    app = RequestLifecycleSanic("request-orphan-" + uuid4().hex)
    request = _request()
    if close_failure:
        with pytest.raises(ValueError) as caught:
            await app.handle_request(request)
        assert caught.value is failure
    else:
        await app.handle_request(request)
        assert session.closed
    assert session.rolled_back and not session.committed
    _assert_released(request, database)


@pytest.mark.parametrize("close_failure", [False, True])
async def test_direct_exception_handler_cancellation_releases_newly_bound_reader(monkeypatch, close_failure):
    monkeypatch.setenv("HLTHPRT_API_READER_ENABLED", "true")
    session = ReaderSession()
    if close_failure:
        session.close = AsyncMock(side_effect=ValueError("private cleanup detail"))
    database = reader_database(monkeypatch, [session])
    monkeypatch.setattr(request_lifecycle, "db", database)
    callbacks = _request_callbacks(database, monkeypatch)
    failure = asyncio.CancelledError("synthetic handler cancellation")
    original = ValueError("synthetic transport error")

    async def handle_exception(_app, request, exception, run_middleware):
        assert exception is original and run_middleware is True
        await callbacks["request"](request)
        assert database.has_reader_session()
        raise failure

    monkeypatch.setattr(Sanic, "handle_exception", handle_exception)
    app = RequestLifecycleSanic("request-exception-" + uuid4().hex)
    request = _request()
    with pytest.raises(asyncio.CancelledError) as caught:
        await app.handle_exception(request, original)
    assert caught.value is failure
    assert session.rolled_back and not session.committed
    assert getattr(failure, "__notes__", []) == (
        ["Request session cleanup failed: ValueError"] if close_failure else []
    )
    if not close_failure:
        assert session.closed
    _assert_released(request, database)


async def test_session_close_failure_still_resets_context_token():
    session = _FakeSession()
    session.close = AsyncMock(side_effect=ValueError("synthetic close failure"))
    database = Database(session_factory=lambda: session)
    with pytest.raises(ValueError, match="synthetic close failure"):
        async with database.session():
            assert current_session() is session
    with pytest.raises(RuntimeError, match="No SQLAlchemy"):
        current_session()
