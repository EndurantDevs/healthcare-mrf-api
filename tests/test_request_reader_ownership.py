# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Cold request replacement and active-writer refusal before routed admission."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import reference_family_reads
from db.connection import current_session
from tests.test_api_reader_sessions import ReaderSession, reader_database
from tests.test_db_connection import _FakeSession


def _request_callbacks(database, monkeypatch):
    monkeypatch.setattr("api.reference_family_reads.pin_claims_reader", AsyncMock(return_value=None))
    callbacks_by_kind = {}

    class App:
        def listener(self, _name):
            return lambda callback: callback

        def middleware(self, kind):
            def register(callback):
                callbacks_by_kind[kind] = callback
                return callback

            return register

    database.init_app(App())
    return callbacks_by_kind


@pytest.mark.asyncio
async def test_cold_request_reader_replacement_uses_existing_cleanup_lifecycle(monkeypatch):
    monkeypatch.setenv("HLTHPRT_API_READER_ENABLED", "false")
    ordinary, reader = _FakeSession(), ReaderSession()
    database = reader_database(monkeypatch, [reader])
    database.session_factory = lambda: ordinary
    callbacks = _request_callbacks(database, monkeypatch)
    request = SimpleNamespace(path="/api/v1/pricing/synthetic", ctx=SimpleNamespace())
    await callbacks["request"](request)
    assert current_session() is ordinary
    assert await database.bind_request_reader(request) is reader
    reference_family_reads.pin_claims_reader.assert_awaited_once_with(reader, api_read=True)
    assert ordinary.closed and not ordinary.committed and not ordinary.rolled_back
    assert request.ctx.sa_session is reader and database.has_reader_session()
    assert await database.bind_request_reader(request) is reader
    await callbacks["response"](request, SimpleNamespace(status=200))
    assert reader.closed and reader.committed and request.ctx.sa_session is None
    assert not database.has_reader_session()
    with pytest.raises(RuntimeError, match="No SQLAlchemy"):
        current_session()


@pytest.mark.asyncio
@pytest.mark.parametrize("refusal", ["active", "missing_owner", "detached"])
async def test_reader_replacement_does_not_touch_active_or_foreign_writer(monkeypatch, refusal):
    monkeypatch.setenv("HLTHPRT_API_READER_ENABLED", "false")
    ordinary, reader = _FakeSession(), ReaderSession()
    database = reader_database(monkeypatch, [reader])
    database.session_factory = lambda: ordinary
    callbacks = _request_callbacks(database, monkeypatch)
    request = SimpleNamespace(path="/api/v1/pricing/synthetic", ctx=SimpleNamespace())
    await callbacks["request"](request)
    if refusal == "active":
        ordinary._in_tx = True
    elif refusal == "missing_owner":
        token = request.ctx._sa_session_token
        request.ctx._sa_session_token = None
    else:
        request.ctx.sa_session = _FakeSession()
    with pytest.raises(RuntimeError, match="unused request-owned"):
        await database.bind_request_reader(request)
    assert not ordinary.closed and not ordinary.committed and not ordinary.rolled_back
    assert current_session() is ordinary and not database.has_reader_session()
    database._connect_reader.assert_not_awaited()
    request.ctx.sa_session = ordinary
    if refusal == "missing_owner":
        request.ctx._sa_session_token = token
    await callbacks["response"](request, SimpleNamespace(status=503))
    assert ordinary.closed
