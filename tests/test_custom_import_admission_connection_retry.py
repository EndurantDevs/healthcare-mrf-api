# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Portable admission reconnect checks through the real fenced page cleanup."""

import asyncio
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, call

import pytest
from sqlalchemy.exc import DataError, InterfaceError, OperationalError, StatementError

from api import control_admission_batch as service
from api import control_batch_diagnostics as diagnostics
from process.custom_import import admission_sql as admission
from process.custom_import import build_source as staging
from process.custom_import.runner_types import CancellationRequested, LeaseAuthorityLost
from tests import test_custom_import_admission_service as boundary
from tests import test_custom_import_admission_sql as sql_cases


def _disconnect(kind=InterfaceError):
    return kind(
        "synthetic-sql", {"value": "synthetic-payload"}, OSError("synthetic-driver"), connection_invalidated=True
    )


def _install_boundary(monkeypatch, *, cursor=7):
    wire, verified, ring = boundary._wire(cursor=cursor)
    retained = boundary._request(page_row_limit=256, authorization_expires_at=verified.permit.expires_at)
    state = SimpleNamespace(wire=wire, verified=verified, ring=ring, retained=retained)
    state.authorize = Mock(return_value=verified)
    state.preload = AsyncMock(return_value=retained)
    state.deadline = Mock(wraps=service.extend_batch_response_deadline)
    state.log = Mock(wraps=service.log_batch_failure)
    monkeypatch.setattr(service, "verify_request", state.authorize)
    monkeypatch.setattr(service, "_retained_request", state.preload)
    monkeypatch.setattr(service, "extend_batch_response_deadline", state.deadline)
    monkeypatch.setattr(service, "log_batch_failure", state.log)
    return state


def _install_pages(monkeypatch, state, outcomes, *, cleanup_phase=None, cleanup_error=None):
    """Reuse SQL fixtures, retaining real _page_session and _drain_cleanup."""

    _, build = sql_cases._page(monkeypatch)
    build.build_id, build.admission_after_occurrence_id = 51, state.verified.pins.expected_after_occurrence_id
    state.build, state.sessions = build, []
    state.permit = AsyncMock()
    monkeypatch.setattr(admission, "_verify_admission_permit", state.permit)

    @asynccontextmanager
    async def transaction(session, begin):
        try:
            async with begin():
                yield
        except BaseException:
            if len(state.sessions) == 1 and cleanup_phase == "rollback":
                raise cleanup_error
            raise
        else:
            if len(state.sessions) == 1 and cleanup_phase == "commit":
                raise cleanup_error

    @asynccontextmanager
    async def factory():
        if state.sessions:
            assert state.sessions[-1].events[-1] == "close"
        rows = sql_cases._rows()
        outcome = outcomes[len(state.sessions)]
        if isinstance(outcome, BaseException):
            rows[1] = outcome
        elif outcome is not None:
            rows = outcome
        session = sql_cases._Session(rows)
        begin = session.begin
        session.begin = lambda: transaction(session, begin)
        state.sessions.append(session)
        try:
            async with session:
                yield session
        finally:
            if len(state.sessions) == 1 and cleanup_phase == "close":
                raise cleanup_error

    state.factory = factory
    state.operation = AsyncMock(wraps=admission.admit_source_batch)
    monkeypatch.setattr(admission, "admit_source_batch", state.operation)


async def _serve(state):
    return await service.serve_admission_batch(
        state.wire,
        state.factory,
        keyring=state.ring,
        expected_origin=boundary.ORIGIN,
        trusted_now=boundary.NOW,
    )


def _assert_scope(state, *, operation_count, page_count):
    expected = call(
        state.factory,
        state.retained,
        51,
        state.verified.pins.expected_after_occurrence_id,
        admission_permit=state.verified.permit,
    )
    assert state.operation.await_args_list == [expected] * operation_count
    assert len(state.sessions) == page_count
    assert all(page.events[-1] == "close" for page in state.sessions)
    assert state.permit.await_count == page_count
    for actual in state.permit.await_args_list:
        assert actual.args[1] is state.retained and actual.args[2] is state.verified.permit
    assert state.retained.lease_token is boundary.TOKEN
    assert state.retained.fence == state.verified.pins.fence == 7
    state.authorize.assert_called_once()
    state.preload.assert_awaited_once_with(state.factory, state.verified)
    assert state.deadline.call_count == 2
    assert state.deadline.call_args.kwargs["expires_at"] == boundary.NOW + boundary.timedelta(seconds=120)
    assert state.retained.statement_timeout_ms == 1_000
    assert state.retained.build_deadline_at == boundary.NOW + boundary.timedelta(hours=1)


@pytest.mark.parametrize("kind", [InterfaceError, OperationalError])
async def test_invalidated_admission_retries_only_after_rollback_close_with_same_scope(monkeypatch, caplog, kind):
    state = _install_boundary(monkeypatch)
    failure = _disconnect(kind)
    _install_pages(monkeypatch, state, [failure, None])

    reply = await _serve(state)

    assert reply.status == 200 and reply.headers["Cache-Control"] == "no-store"
    assert json.loads(reply.body) == {
        "execution_id": 40,
        "build_id": 51,
        "fence": 7,
        "phase": "rejected",
        "after_occurrence_id": 999,
        "rows_processed": 512,
        "candidate_error_count": 2,
    }
    _assert_scope(state, operation_count=2, page_count=2)
    first, second = state.sessions
    assert first is not second and first.events[-2:] == ["rollback", "close"]
    assert not first.pending and not first.durable
    assert second.events[-3:] == ["verify_before_commit", "commit", "close"]
    assert second.durable == ["apply", "advance"]
    assert len(caplog.records) == 1
    assert caplog.records[0].getMessage().startswith("custom_import_batch_retry_attempted stage=admission_execute ")
    assert "synthetic-" not in caplog.text and not caplog.records[0].exc_info


@pytest.mark.parametrize("phase", ["commit", "rollback", "close"])
async def test_cleanup_failure_blocks_retry_and_preserves_original_error(monkeypatch, caplog, phase):
    state = _install_boundary(monkeypatch)
    failure = _disconnect()
    cleanup = RuntimeError("synthetic-cleanup") if phase == "rollback" else failure
    _install_pages(
        monkeypatch,
        state,
        [failure if phase == "rollback" else None, None],
        cleanup_phase=phase,
        cleanup_error=cleanup,
    )

    reply = await _serve(state)

    assert reply.status == 503 and json.loads(reply.body) == {"error": "admission_unavailable"}
    _assert_scope(state, operation_count=1, page_count=1)
    assert failure._custom_import_retry_blocked is True
    state.log.assert_called_once_with(diagnostics.BatchFailureStage.ADMISSION_EXECUTE, failure)
    if phase == "rollback":
        assert failure.__cause__ is cleanup
    assert "retry_blocked=True" in caplog.text and "retry_attempted" not in caplog.text
    assert "synthetic-" not in caplog.text


async def test_second_disconnect_is_terminal_with_two_closed_payload_free_logs(monkeypatch, caplog):
    state = _install_boundary(monkeypatch)
    _install_pages(monkeypatch, state, [_disconnect(), _disconnect(OperationalError), None])

    reply = await _serve(state)

    assert reply.status == 503 and json.loads(reply.body) == {"error": "admission_unavailable"}
    _assert_scope(state, operation_count=2, page_count=2)
    assert all(page.events[-2:] == ["rollback", "close"] for page in state.sessions)
    assert len(caplog.records) == 2
    first_message = caplog.records[0].getMessage()
    second_message = caplog.records[1].getMessage()
    assert first_message.startswith("custom_import_batch_retry_attempted stage=admission_execute ")
    assert second_message.startswith("custom_import_batch_unavailable stage=admission_execute ")
    assert "database_subtype=sqlalchemy_interface" in first_message
    assert "database_subtype=sqlalchemy_operational" in second_message
    assert "synthetic-" not in caplog.text and all(not item.exc_info for item in caplog.records)


@pytest.mark.parametrize(
    "attribute,value",
    [
        ("_custom_import_retry_blocked", True),
        ("_custom_import_retry_blocked", None),
        ("_custom_import_retry_blocked", 0),
        ("_custom_import_retry_blocked", "unknown"),
        ("connection_invalidated", False),
        ("connection_invalidated", None),
        ("connection_invalidated", 1),
        ("connection_invalidated", "true"),
    ],
)
async def test_retry_flags_must_be_literal_safe_booleans(monkeypatch, attribute, value):
    _, verified, _ = boundary._wire()
    failure = _disconnect()
    setattr(failure, attribute, value)
    operation = AsyncMock(side_effect=failure)
    monkeypatch.setattr(admission, "admit_source_batch", operation)
    with pytest.raises(InterfaceError) as caught:
        await service._admission_operation(None, boundary._request(), verified)
    assert caught.value is failure
    operation.assert_awaited_once()


@pytest.mark.parametrize("attribute", ["connection_invalidated", "_custom_import_retry_blocked"])
async def test_retry_inspection_does_not_invoke_exception_descriptors(monkeypatch, attribute):
    class ObservedInterfaceError(InterfaceError):
        pass

    _, verified, _ = boundary._wire()
    failure = _disconnect(ObservedInterfaceError)
    forbidden = Mock(side_effect=AssertionError("descriptor must remain unread"))
    monkeypatch.setattr(ObservedInterfaceError, attribute, property(forbidden), raising=False)
    operation = AsyncMock(side_effect=failure)
    monkeypatch.setattr(admission, "admit_source_batch", operation)
    with pytest.raises(InterfaceError) as caught:
        await service._admission_operation(None, boundary._request(), verified)
    assert caught.value is failure
    operation.assert_awaited_once()
    forbidden.assert_not_called()


async def test_retry_inspection_failure_keeps_original_error(monkeypatch):
    _, verified, _ = boundary._wire()
    failure = _disconnect()
    inspect_attribute = Mock(side_effect=RuntimeError("synthetic-inspection"))
    monkeypatch.setattr(diagnostics, "getattr_static", inspect_attribute)
    operation = AsyncMock(side_effect=failure)
    monkeypatch.setattr(admission, "admit_source_batch", operation)
    with pytest.raises(InterfaceError) as caught:
        await service._admission_operation(None, boundary._request(), verified)
    assert caught.value is failure
    operation.assert_awaited_once()
    inspect_attribute.assert_called_once_with(failure, "connection_invalidated", False)


async def test_pending_task_cancellation_prevents_admission_retry(monkeypatch):
    _, verified, _ = boundary._wire()
    failure = _disconnect()
    operation = AsyncMock(side_effect=failure)
    monkeypatch.setattr(admission, "admit_source_batch", operation)
    ready = asyncio.Event()

    async def cancelled_operation():
        ready.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            assert asyncio.current_task().cancelling()
        return await service._admission_operation(None, boundary._request(), verified)

    task = asyncio.create_task(cancelled_operation())
    await ready.wait()
    task.cancel()
    with pytest.raises(InterfaceError) as caught:
        await task
    assert caught.value is failure
    operation.assert_awaited_once()


@pytest.mark.parametrize(
    "failure",
    [
        DataError(None, None, OSError(), connection_invalidated=True),
        StatementError("synthetic", None, None, None),
        RuntimeError("synthetic"),
        asyncio.CancelledError(),
    ],
)
async def test_unknown_or_unrelated_failure_never_retries(monkeypatch, failure):
    _, verified, _ = boundary._wire()
    operation = AsyncMock(side_effect=failure)
    monkeypatch.setattr(admission, "admit_source_batch", operation)
    with pytest.raises(type(failure)) as caught:
        await service._admission_operation(None, boundary._request(), verified)
    assert caught.value is failure
    operation.assert_awaited_once()


@pytest.mark.parametrize("change", ["cursor", "fence", "expiry", "cancel"])
async def test_reconnect_preserves_fresh_cursor_and_authority_refusals(monkeypatch, change):
    state = _install_boundary(monkeypatch)
    rows = sql_cases._rows()
    if change == "cursor":
        rows[0]["problem"] = "custom_import_build_progress_conflict"
    _install_pages(monkeypatch, state, [_disconnect(), rows])
    if change in {"fence", "cancel"}:
        error = LeaseAuthorityLost("synthetic-fence") if change == "fence" else CancellationRequested("synthetic")
        staging._lock_page.side_effect = [state.build, error]
    elif change == "expiry":
        monkeypatch.setattr(staging, "verify_live_attempt", AsyncMock(return_value=state.verified.permit.expires_at))

    reply = await _serve(state)

    expected = "admission_progress_conflict" if change == "cursor" else "admission_authority_lost"
    assert reply.status == 409 and json.loads(reply.body) == {"error": expected}
    assert state.operation.await_count == len(state.sessions) == 2
    assert all(page.events[-2:] == ["rollback", "close"] for page in state.sessions)
    assert state.operation.await_args_list[0] == state.operation.await_args_list[1]
    state.authorize.assert_called_once()
    state.preload.assert_awaited_once()


@pytest.mark.parametrize("terminal", [False, True])
async def test_reconnect_and_confirmed_timeout_fallback_have_independent_finite_bounds(monkeypatch, terminal):
    state = _install_boundary(monkeypatch)
    final = sql_cases._database_error() if terminal else sql_cases._rows()
    if not terminal:
        final[1]["rows_processed"] = final[3]["rows_processed"] = 256
    _install_pages(monkeypatch, state, [sql_cases._database_error(), _disconnect(), sql_cases._database_error(), final])

    reply = await _serve(state)

    assert reply.status == (503 if terminal else 200)
    _assert_scope(state, operation_count=2, page_count=4)
    assert [page.executed[1][1]["physical_row_cap"] for page in state.sessions] == [100_000, 256, 100_000, 256]
    assert [page.executed[1][1]["expected_after_id"] for page in state.sessions] == [7] * 4
    assert all(page.events[-2:] == ["rollback", "close"] for page in state.sessions[:3])
    assert state.sessions[-1].events[-2:] == (["rollback", "close"] if terminal else ["commit", "close"])
