# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Request-local ceilings never renew authority or change retained build bounds."""

import asyncio
from dataclasses import fields
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from api import control_admission_batch as service
from process.custom_import import build_source as staging
from process.custom_import import continuation_projection, source_batch
from process.custom_import import query_projection_build as projection
from process.custom_import.continuation_runtime import BuildContinuationContext
from process.custom_import.runner_types import LeaseAuthorityLost
from tests import test_custom_import_admission_service as admission_fixture
from tests import test_custom_import_continuation_projection as continuation_fixture
from tests import test_custom_import_source_service as source_fixture
from tests.test_custom_import_build_source import _request
from tests.test_custom_import_source_batch import _CURSOR, _PERMIT, _context_case


@pytest.mark.parametrize("global_timeout,expected", [(60, 399.0), (600, 699.0)])
def test_http_ceiling_uses_actual_protocol_anchor_not_extension_time(monkeypatch, global_timeout, expected):
    now = datetime(2030, 1, 1, tzinfo=timezone.utc)
    protocol = SimpleNamespace(_time=100.0, response_timeout=global_timeout)
    request = SimpleNamespace(
        transport=object(),
        protocol=protocol,
        app=SimpleNamespace(config=SimpleNamespace(RESPONSE_TIMEOUT=global_timeout)),
    )
    monkeypatch.setattr(service.time, "monotonic", lambda: 130.0)
    deadline = service.extend_batch_response_deadline(request, expires_at=now + timedelta(hours=1), trusted_now=now)
    assert deadline == expected
    assert request.app.config.RESPONSE_TIMEOUT == global_timeout
    monkeypatch.setattr(service.time, "monotonic", lambda: 150.0)
    assert (
        service.extend_batch_response_deadline(request, expires_at=now + timedelta(hours=1), trusted_now=now)
        == deadline
    )


@pytest.mark.parametrize("value", [True, "300", float("nan"), float("inf"), -float("inf")])
def test_operation_deadline_rejects_nonfinite_or_untyped_values(value):
    with pytest.raises(ValueError, match="operation deadline"):
        _request(operation_deadline=value)


@pytest.mark.parametrize("operation_deadline,expected", [(None, 3700.0), (399.0, 399.0), (5000.0, 3700.0)])
@pytest.mark.parametrize("continued", [False, True])
async def test_initial_and_renewed_windows_keep_fixed_operation_ceiling(
    monkeypatch, operation_deadline, expected, continued
):
    now = datetime(2029, 1, 1, tzinfo=timezone.utc)
    continuation = (
        dict(continuation=BuildContinuationContext(41, 19, b"p" * 32), build_deadline_at=now - timedelta(days=1))
        if continued
        else {}
    )
    request = _request(statement_timeout_ms=900_000, operation_deadline=operation_deadline, **continuation)
    lease = SimpleNamespace(state="running", expires_at=now + timedelta(hours=1))
    result = SimpleNamespace(one=lambda: (lease.expires_at, now, "read committed"), scalar_one=lambda: now)
    session = SimpleNamespace(info={}, execute=AsyncMock(return_value=result))
    timeout = AsyncMock()
    monkeypatch.setattr(staging.time, "monotonic", lambda: 100.0)
    monkeypatch.setattr(staging, "_set_timeout", timeout)
    monkeypatch.setattr(staging, "heartbeat_execution", AsyncMock(return_value=lease))
    await staging._initial_page_window(session, request)
    assert session.info[staging._WINDOW].deadline == expected
    assert timeout.await_args_list[0].args[1] == (149_500 if operation_deadline == 399.0 else 900_000)
    # Lease renewal can move the authority ceiling, never the HTTP operation ceiling.
    monkeypatch.setattr(staging.time, "monotonic", lambda: 120.0)
    with staging.batch_operation_deadline(9000.0):
        await staging._renew_page_window(session, request)
    assert session.info[staging._WINDOW].deadline == (399.0 if operation_deadline == 399.0 else 3720.0)
    assert request.statement_timeout_ms == 900_000
    assert staging.heartbeat_execution.await_args.kwargs["continuation_id"] == (41 if continued else None)


async def test_expired_operation_stops_before_initial_database_read(monkeypatch):
    session = SimpleNamespace(info={}, execute=AsyncMock())
    timeout = AsyncMock()
    monkeypatch.setattr(staging.time, "monotonic", lambda: 300.0)
    monkeypatch.setattr(staging, "_set_timeout", timeout)
    with pytest.raises(LeaseAuthorityLost, match="deadline elapsed"):
        await staging._initial_page_window(session, _request(operation_deadline=299.0))
    session.execute.assert_not_awaited()
    timeout.assert_not_awaited()


async def test_source_context_retains_operation_ceiling_without_changing_permit(monkeypatch):
    case = _context_case(monkeypatch)
    with staging.batch_operation_deadline(299.0):
        context, *_ = await source_batch._load_context(None, 4, 11, 1, 1, _CURSOR, case.request.lease_token, _PERMIT)
    assert context.request.operation_deadline == 299.0
    assert context.request.authorization_expires_at == _PERMIT.expires_at
    assert context.request.statement_timeout_ms == case.request.statement_timeout_ms


async def test_projection_context_keeps_the_same_ceiling_and_permit_through_retained_loading(monkeypatch):
    case = _context_case(monkeypatch)
    pins = SimpleNamespace(execution_id=4, build_id=11, fence=1)
    verified = SimpleNamespace(pins=pins, permit=_PERMIT, token=SimpleNamespace(value=case.request.lease_token))
    step = AsyncMock(return_value=object())
    monkeypatch.setattr(projection, "projection_step", step)
    with staging.batch_operation_deadline(299.0):
        assert await projection.serve_projection_batch(None, verified) is step.return_value
    request = step.await_args.args[1]
    assert request.operation_deadline == 299.0 and request.authorization_expires_at == _PERMIT.expires_at
    assert request.statement_timeout_ms == case.request.statement_timeout_ms
    assert step.await_args.kwargs == {"expected": pins, "permit": _PERMIT}
    assert case.events == ["read-closed"]
    assert _request().operation_deadline is None


@pytest.mark.parametrize("expires", [False, True])
async def test_admission_connection_retry_reuses_retained_deadline_and_one_outer_timeout(monkeypatch, expires):
    wire, verified, ring = admission_fixture._wire()
    retained = _request(authorization_expires_at=verified.permit.expires_at)
    deadline = asyncio.get_running_loop().time() + (1 if expires else 30)
    load = AsyncMock(return_value=retained)
    calls = []

    async def admit(_factory, request, *_args, **_kwargs):
        calls.append(request)
        if len(calls) == 1:
            failure = source_fixture._disconnected()
            failure._custom_import_retry_blocked = False
            raise failure
        if expires:
            await asyncio.Event().wait()
        return service.admission_sql.AdmissionResult("graph", 1, 1, 0)

    timeout = Mock(wraps=asyncio.timeout_at)
    monkeypatch.setattr(service, "extend_batch_response_deadline", Mock(return_value=deadline))
    monkeypatch.setattr(service, "_retained_request", load)
    monkeypatch.setattr(service.admission_sql, "admit_source_batch", admit)
    monkeypatch.setattr(asyncio, "timeout_at", timeout)
    reply = await service.serve_admission_batch(
        wire, None, keyring=ring, expected_origin=admission_fixture.ORIGIN, trusted_now=admission_fixture.NOW
    )
    assert reply.status == (503 if expires else 200) and len(calls) == 2
    assert calls[0] is calls[1] and calls[0].operation_deadline == deadline
    assert calls[0].authorization_expires_at == retained.authorization_expires_at
    assert retained.operation_deadline is None
    load.assert_awaited_once()
    assert [call.args for call in timeout.call_args_list] == [(deadline,), (deadline,)]


async def test_admission_preload_deadline_cancels_retained_read_before_owner_batch(monkeypatch):
    wire, verified, ring = admission_fixture._wire()
    retained = _request(authorization_expires_at=verified.permit.expires_at)
    events = []

    async def load(*_args):
        try:
            await asyncio.sleep(0)
        except asyncio.CancelledError:
            events.append("cancelled")
            raise
        return retained

    deadline = asyncio.get_running_loop().time()
    extend = Mock(return_value=deadline)
    owner = AsyncMock(return_value=service.admission_sql.AdmissionResult("graph", 1, 1, 0))
    monkeypatch.setattr(service, "extend_batch_response_deadline", extend)
    monkeypatch.setattr(service, "_retained_request", load)
    monkeypatch.setattr(service.admission_sql, "admit_source_batch", owner)
    reply = await service.serve_admission_batch(
        wire, None, keyring=ring, expected_origin=admission_fixture.ORIGIN, trusted_now=admission_fixture.NOW
    )
    assert reply.status == 503 and reply.headers["Cache-Control"] == "no-store"
    assert events == ["cancelled"] and retained.operation_deadline is None
    extend.assert_called_once()
    owner.assert_not_awaited()


@pytest.mark.parametrize("expires", [False, True])
async def test_source_connection_retry_reconstructs_with_same_deadline_and_one_outer_timeout(monkeypatch, expires):
    wire, _ = source_fixture._wire()
    deadline = asyncio.get_running_loop().time() + (1 if expires else 30)
    extend = Mock(return_value=deadline)
    calls = []

    async def step(_factory, **operation):
        calls.append((_request(), operation))
        if len(calls) == 1:
            failure = source_fixture._disconnected()
            failure._custom_import_retry_blocked = False
            raise failure
        if expires:
            await asyncio.Event().wait()
        return source_fixture._committed()

    timeout = Mock(wraps=asyncio.timeout_at)
    monkeypatch.setattr(service, "extend_batch_response_deadline", extend)
    monkeypatch.setattr(source_batch, "serve_source_batch", step)
    monkeypatch.setattr(asyncio, "timeout_at", timeout)
    reply = await source_fixture._serve(wire)
    assert reply.status == (503 if expires else 200) and len(calls) == 2
    assert [request.operation_deadline for request, _ in calls] == [deadline, deadline]
    assert calls[0][1] == calls[1][1]
    assert calls[0][1]["source_permit"] is calls[1][1]["source_permit"]
    assert _request().operation_deadline is None
    extend.assert_called_once()
    timeout.assert_called_once_with(deadline)


@pytest.mark.parametrize("global_timeout", [None, 60, 600])
async def test_continuation_keeps_http_ceiling_and_existing_operation_cap(monkeypatch, global_timeout):
    retained = continuation_fixture._build_request()
    wire = continuation_fixture._wire()
    anchor = asyncio.get_running_loop().time() - 2
    if global_timeout is not None:
        wire.transport = object()
        wire.protocol = SimpleNamespace(_time=anchor, response_timeout=global_timeout)
        wire.app = SimpleNamespace(config=SimpleNamespace(RESPONSE_TIMEOUT=global_timeout))
    load = AsyncMock(return_value=(retained, 20, 8, datetime.now(timezone.utc) + timedelta(hours=1)))
    step = AsyncMock(return_value=continuation_fixture.RECEIPT)
    relative, absolute = Mock(wraps=asyncio.timeout), Mock(wraps=asyncio.timeout_at)
    monkeypatch.setattr(continuation_projection, "_retained_request", load)
    monkeypatch.setattr(continuation_projection, "projection_step", step)
    monkeypatch.setattr(asyncio, "timeout", relative)
    monkeypatch.setattr(asyncio, "timeout_at", absolute)
    reply = await continuation_projection.serve_continuation_projection(wire, "owner-pool")
    assert reply.status == 200 and reply.headers["Cache-Control"] == "no-store"
    request = step.await_args.args[1]
    assert request.continuation is retained.continuation
    assert request.build_deadline_at == retained.build_deadline_at
    assert retained.operation_deadline is None
    if global_timeout is not None:
        assert request is not retained
        if global_timeout == 600:
            assert anchor + 302 <= request.operation_deadline <= asyncio.get_running_loop().time() + 300
        else:
            assert request.operation_deadline == anchor + 299
        absolute.assert_called_once_with(request.operation_deadline)
        relative.assert_not_called()
    else:
        assert request is retained and request.operation_deadline is None
        relative.assert_called_once_with(300)
        absolute.assert_not_called()


async def test_continuation_exhausted_http_deadline_stops_after_authentication_before_owner_step(monkeypatch):
    wire = continuation_fixture._wire()
    wire.transport = object()
    wire.protocol = SimpleNamespace(_time=asyncio.get_running_loop().time() - 301, response_timeout=60)
    wire.app = SimpleNamespace(config=SimpleNamespace(RESPONSE_TIMEOUT=60))
    load = AsyncMock(
        return_value=(continuation_fixture._build_request(), 20, 8, datetime.now(timezone.utc) + timedelta(hours=1))
    )
    step = AsyncMock()
    monkeypatch.setattr(continuation_projection, "_retained_request", load)
    monkeypatch.setattr(continuation_projection, "projection_step", step)
    reply = await continuation_projection.serve_continuation_projection(wire, "owner-pool")
    assert reply.status == 409
    load.assert_awaited_once()
    step.assert_not_awaited()


def test_non_http_request_keeps_default_and_immutable_equality():
    assert _request().operation_deadline is None
    assert _request() == _request(operation_deadline=299.0)


def test_non_http_positional_continuation_keeps_existing_field_order():
    request = continuation_fixture._build_request()
    original_fields = (getattr(request, item.name) for item in fields(request) if item.name != "operation_deadline")
    rebuilt = staging.SourceBuildRequest(*original_fields)
    assert rebuilt.continuation is request.continuation and rebuilt.operation_deadline is None
    assert rebuilt == request


async def test_concurrent_request_deadlines_are_captured_once_and_restored():
    first_ready, second_ready = asyncio.Event(), asyncio.Event()

    async def capture(deadline, ready, peer):
        with staging.batch_operation_deadline(deadline):
            ready.set()
            await peer.wait()
            request = _request()
            with staging.batch_operation_deadline(deadline + 100):
                assert _request().operation_deadline == deadline + 100
                assert request.operation_deadline == deadline
            assert _request().operation_deadline == deadline
        assert _request().operation_deadline is None
        return request.operation_deadline

    assert await asyncio.gather(
        capture(299.0, first_ready, second_ready), capture(199.0, second_ready, first_ready)
    ) == [
        299.0,
        199.0,
    ]
    assert _request().operation_deadline is None


@pytest.mark.parametrize("error", [RuntimeError, asyncio.CancelledError])
def test_nested_deadline_scope_restores_on_error(error):
    with staging.batch_operation_deadline(299.0):
        with pytest.raises(error), staging.batch_operation_deadline(199.0):
            assert _request().operation_deadline == 199.0
            raise error()
        assert _request().operation_deadline == 299.0
    assert _request().operation_deadline is None
