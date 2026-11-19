# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Cold request replacement and active-writer refusal before routed admission."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import network_serving_read_context, reference_family_reads
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
@pytest.mark.parametrize(
    "path,should_lock",
    [
        ("/api/v1/codes/POS/23", True),
        ("/api/v1/clinical/concepts/MS_DRG/001", True),
        ("/api/v1/codes/POS/23/related", False),
        ("/api/v1/npi/geo", False),
        ("/api/v1/pricing/providers", False),
    ],
)
async def test_catalog_reader_locks_follow_the_requested_data(monkeypatch, path, should_lock):
    monkeypatch.setenv("HLTHPRT_SCOPED_CATALOG_DATA_SCHEMA", "mrf_scoped_catalog")
    session = ReaderSession()
    database = reader_database(monkeypatch, [session])
    monkeypatch.setattr(reference_family_reads, "pin_claims_reader", AsyncMock())
    lock = AsyncMock()
    monkeypatch.setattr(reference_family_reads, "lock_scoped_catalog_reader", lock)
    await database._pin_request_reader(SimpleNamespace(path=path, args={}), session)
    assert lock.await_count == int(should_lock)


@pytest.mark.parametrize(
    "flag,expected", [("y", True), ("true", True), ("ON", True), ("n", False), ("mismatch", False)]
)
def test_pricing_expand_codes_reuses_pricing_parser_for_lock(monkeypatch, flag, expected):
    monkeypatch.setenv("HLTHPRT_SCOPED_CATALOG_DATA_SCHEMA", "mrf_scoped_catalog")
    request = SimpleNamespace(path="/api/v1/pricing/providers/123/procedures/45", args={"expand_codes": flag})
    assert reference_family_reads.uses_scoped_catalog(request) is expected


@pytest.mark.parametrize(
    ("path", "args", "should_lock"),
    [
        ("/api/v1/pricing/providers/123/procedures", {"source_key": "fixture"}, True),
        ("/api/v1/pricing/providers/123/procedures", {"snapshot_id": "fixture"}, True),
        ("/api/v1/pricing/physicians/123/services", {"plan_id": "fixture"}, True),
        ("/api/v1/pricing/physicians/123/services/", {"plan_release_id": "fixture"}, True),
        ("/api/v1/pricing/providers/123/procedures", {}, False),
        ("/api/v1/pricing/physicians/123/services", {"source_key": " "}, False),
        ("/api/v1/pricing/providers/123", {"snapshot_id": "fixture"}, False),
    ],
)
def test_source_scoped_procedure_list_locks_even_without_flags(monkeypatch, path, args, should_lock):
    monkeypatch.setenv("HLTHPRT_SCOPED_CATALOG_DATA_SCHEMA", "mrf_scoped_catalog")
    assert reference_family_reads.uses_scoped_catalog(SimpleNamespace(path=path, args=args)) is should_lock


async def test_explicit_unprepared_reader_pins_providers_after_catalog_before_yield(monkeypatch):
    session = ReaderSession()
    database = reader_database(monkeypatch, [session])
    events = []

    async def lock(active):
        assert active is session and session.transaction_starts
        events.append("catalog")

    async def pin(active):
        assert active is session
        events.append("provider")

    monkeypatch.setattr(reference_family_reads, "lock_scoped_catalog_reader", lock)
    monkeypatch.setattr(database, "_pin_provider_reader", pin)
    async with database.reader_session(scoped_catalog=True) as active:
        assert active is session and events == ["catalog", "provider"]


@pytest.mark.asyncio
@pytest.mark.parametrize("network_read", [False, True])
async def test_cold_request_reader_replacement_uses_existing_cleanup_lifecycle(monkeypatch, network_read):
    monkeypatch.setenv("HLTHPRT_API_READER_ENABLED", "false")
    ordinary, reader = _FakeSession(), ReaderSession()
    database = reader_database(monkeypatch, [reader])
    database.session_factory = lambda: ordinary
    callbacks = _request_callbacks(database, monkeypatch)
    monkeypatch.setattr(network_serving_read_context, "db", database)
    bind_reader = (
        network_serving_read_context.bind_network_request_reader if network_read else database.bind_request_reader
    )
    request = SimpleNamespace(path="/api/v1/pricing/synthetic", ctx=SimpleNamespace())
    await callbacks["request"](request)
    assert current_session() is ordinary
    assert await bind_reader(request) is reader
    reference_family_reads.pin_claims_reader.assert_awaited_once_with(reader)
    assert ordinary.closed and not ordinary.committed and not ordinary.rolled_back
    assert request.ctx.sa_session is reader and database.has_reader_session()
    assert await bind_reader(request) is reader
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


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [ConnectionRefusedError("reader refused"), TimeoutError("reader timed out")])
async def test_network_reader_connection_failure_uses_unavailable_response(monkeypatch, failure):
    """Reader connection failures keep the existing unavailable boundary and cause."""
    from process.network_serving_read import NetworkServingReadUnavailable

    bind_reader = AsyncMock(side_effect=failure)
    database = SimpleNamespace(bind_request_reader=bind_reader)
    monkeypatch.setattr(network_serving_read_context, "db", database)
    request = SimpleNamespace(ctx=SimpleNamespace())
    with pytest.raises(NetworkServingReadUnavailable, match="Reader is unavailable") as caught:
        await network_serving_read_context.bind_network_request_reader(request)
    assert caught.value.__cause__ is failure
    bind_reader.assert_awaited_once_with(request)
