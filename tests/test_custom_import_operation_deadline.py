# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Request-local ceilings never renew authority or change retained build bounds."""

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import control_admission_batch as service
from process.custom_import import build_source as staging
from process.custom_import import source_batch
from process.custom_import.runner_types import LeaseAuthorityLost
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
async def test_initial_and_renewed_windows_keep_fixed_operation_ceiling(monkeypatch, operation_deadline, expected):
    now = datetime(2029, 1, 1, tzinfo=timezone.utc)
    request = _request(statement_timeout_ms=900_000, operation_deadline=operation_deadline)
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


def test_non_http_request_keeps_default_and_immutable_equality():
    assert _request().operation_deadline is None
    assert _request() == _request(operation_deadline=299.0)


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
