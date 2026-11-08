# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Thin SOURCE HTTP boundary; engine transaction behavior is tested separately."""

import asyncio
import json
import sys
from contextlib import asynccontextmanager
from dataclasses import asdict, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from sanic.compat import Header
from sqlalchemy.exc import DataError, InterfaceError, OperationalError, StatementError

from api import control_admission_batch as admission
from api import control_batch_diagnostics as diagnostics
from api import control_source_batch as service
from process.custom_import import source_authorization as source
from process.custom_import import source_worker
from process.custom_import.runner_types import CancellationRequested, LeaseAuthorityLost
from tests import test_custom_import_source_authorization as envelope
from tests.test_custom_import_admission_service import BodyStream


def _wire():
    request = SimpleNamespace(
        headers=Header(envelope.headers()),
        body=envelope.legacy.canonical(envelope.BODY),
        method="POST",
        path=source.SOURCE_PATH,
        query_string="",
    )
    return request, envelope.verify()


def _committed():
    return source_worker.SourceBatchReceipt(
        40,
        51,
        7,
        2,
        60,
        source.SourceCursor(**envelope.CURSOR),
        source.SourceCursor(1, 3, 3, 1),
        "source",
        3,
        False,
    )


async def _serve(request, session_factory=None):
    return await service.serve_source_batch(
        request,
        session_factory,
        keyring=envelope.legacy.keyring(),
        expected_origin=envelope.legacy.ORIGIN,
        trusted_now=envelope.legacy.NOW,
    )


async def test_handler_forwards_only_verified_closed_inputs_and_returns_committed_receipt(monkeypatch):
    request, verified = _wire()
    operation = AsyncMock(return_value=_committed())
    monkeypatch.setattr(service, "_source_operation", operation)
    reply = await _serve(request, "existing-pool")
    operation.assert_awaited_once_with("existing-pool", verified)
    assert reply.status == 200 and reply.headers["Cache-Control"] == "no-store"
    assert json.loads(reply.body) == asdict(_committed()) and len(reply.body) <= 1_024


async def test_fixed_step_seam_converts_cursor_and_preserves_exact_token_and_permit(monkeypatch):
    _, verified = _wire()
    step = AsyncMock(return_value=_committed())
    module = SimpleNamespace(
        SourceCursor=source.SourceCursor, SourceCursorConflict=LookupError, serve_source_batch=step
    )
    monkeypatch.setitem(sys.modules, "process.custom_import.source_batch", module)
    assert await service._source_operation("existing-pool", verified) == _committed()
    assert step.await_args.args == ("existing-pool",)
    assert step.await_args.kwargs == {
        "execution_id": 40,
        "build_id": 51,
        "fence": 7,
        "stream_slot": 2,
        "expected_cursor": verified.pins.expected_cursor,
        "lease_token": verified.token.value,
        "source_permit": verified.permit,
    }
    assert step.await_args.kwargs["source_permit"] is verified.permit


async def test_engine_cursor_conflict_cannot_leak_cursors_or_grant_retry(monkeypatch):
    _, verified = _wire()
    step = AsyncMock(side_effect=LookupError("do-not-log"))
    module = SimpleNamespace(
        SourceCursor=source.SourceCursor, SourceCursorConflict=LookupError, serve_source_batch=step
    )
    monkeypatch.setitem(sys.modules, "process.custom_import.source_batch", module)
    with pytest.raises(service.SourceProgressConflict, match="^custom_import_source_progress_conflict$"):
        await service._source_operation(None, verified)


def _disconnected(kind=InterfaceError):
    return kind(
        "synthetic-sql", {"value": "synthetic-payload"}, OSError("synthetic-driver"), connection_invalidated=True
    )


def _install_retry_step(monkeypatch, outcomes, *, commit_error=None, close_error=None):
    from process.custom_import import source_batch as engine

    state = SimpleNamespace(calls=[], events=[], sessions=[])

    @asynccontextmanager
    async def transaction(index):
        state.events.append((index, "begin"))
        try:
            yield
        except BaseException:
            state.events.append((index, "rollback"))
            raise
        else:
            state.events.append((index, "commit"))
            if index == 0 and commit_error is not None:
                raise commit_error

    @asynccontextmanager
    async def factory():
        index = len(state.sessions)
        session = SimpleNamespace(begin=lambda: transaction(index))
        state.sessions.append(session)
        state.events.append((index, "open"))
        try:
            yield session
        finally:
            state.events.append((index, "close"))
            if index == 0 and close_error is not None:
                raise close_error

    async def step(used_factory, **operation):
        assert used_factory is factory
        state.calls.append(operation)
        async with engine._read_session(used_factory):
            outcome = outcomes[len(state.calls) - 1]
            if isinstance(outcome, BaseException):
                raise outcome
            return outcome

    monkeypatch.setitem(
        sys.modules,
        "process.custom_import.source_batch",
        SimpleNamespace(
            SourceCursor=engine.SourceCursor, SourceCursorConflict=engine.SourceCursorConflict, serve_source_batch=step
        ),
    )
    state.factory = factory
    state.engine = engine
    return state


@pytest.mark.parametrize("kind", [InterfaceError, OperationalError])
async def test_invalidated_source_connection_retries_after_close_with_identical_authority(monkeypatch, caplog, kind):
    request, verified = _wire()
    authorize = Mock(return_value=verified)
    deadline = Mock(wraps=admission.extend_batch_response_deadline)
    monkeypatch.setattr(service.authorization, "verify_request", authorize)
    monkeypatch.setattr(admission, "extend_batch_response_deadline", deadline)
    failure = _disconnected(kind)
    if kind is OperationalError:
        failure._custom_import_retry_blocked = False
    state = _install_retry_step(monkeypatch, [failure, _committed()])

    reply = await _serve(request, state.factory)

    assert reply.status == 200 and json.loads(reply.body) == asdict(_committed())
    assert state.events == [
        (0, "open"),
        (0, "begin"),
        (0, "rollback"),
        (0, "close"),
        (1, "open"),
        (1, "begin"),
        (1, "commit"),
        (1, "close"),
    ]
    assert len(state.sessions) == 2 and state.sessions[0] is not state.sessions[1]
    first, second = state.calls
    assert first == second
    assert asdict(first["expected_cursor"]) == asdict(verified.pins.expected_cursor)
    for name, expected in (("source_permit", verified.permit), ("lease_token", verified.token.value)):
        assert first[name] is second[name] is expected
    assert first["expected_cursor"] is second["expected_cursor"]
    authorize.assert_called_once()
    deadline.assert_called_once_with(request, expires_at=verified.permit.expires_at, trusted_now=envelope.legacy.NOW)
    assert len(caplog.records) == 1 and "connection_invalidated=True" in caplog.text
    assert caplog.records[0].getMessage().startswith("custom_import_batch_retry_attempted ")
    assert "custom_import_batch_unavailable" not in caplog.text
    assert "synthetic-" not in caplog.text and not caplog.records[0].exc_info


@pytest.mark.parametrize("kind", [InterfaceError, OperationalError])
@pytest.mark.parametrize("failure_phase", ["commit", "close"])
async def test_uncertain_commit_or_session_cleanup_failure_is_terminal(monkeypatch, caplog, kind, failure_phase):
    request, _ = _wire()
    failure = _disconnected(kind)
    state = _install_retry_step(
        monkeypatch,
        [_committed(), _committed()],
        commit_error=failure if failure_phase == "commit" else None,
        close_error=failure if failure_phase == "close" else None,
    )

    reply = await _serve(request, state.factory)

    assert reply.status == 503 and json.loads(reply.body) == {"error": "source_unavailable"}
    assert state.events == [(0, "open"), (0, "begin"), (0, "commit"), (0, "close")]
    assert len(state.calls) == len(state.sessions) == 1
    assert failure._custom_import_retry_blocked is True
    assert len(caplog.records) == 1
    assert caplog.records[0].getMessage().startswith("custom_import_batch_unavailable ")
    assert "retry_blocked=True" in caplog.text
    assert "custom_import_batch_retry_attempted" not in caplog.text and "synthetic-" not in caplog.text


async def test_retried_source_operation_preserves_existing_cursor_conflict(monkeypatch, caplog):
    from process.custom_import import source_batch as engine

    request, _ = _wire()
    conflict = engine.SourceCursorConflict(engine.SourceCursor(1, 3, 3, 1), False, "source")
    state = _install_retry_step(monkeypatch, [_disconnected(), conflict])

    reply = await _serve(request, state.factory)

    assert reply.status == 409 and json.loads(reply.body) == {"error": "source_progress_conflict"}
    assert state.events == [
        (0, "open"),
        (0, "begin"),
        (0, "rollback"),
        (0, "close"),
        (1, "open"),
        (1, "begin"),
        (1, "rollback"),
        (1, "close"),
    ]
    assert len(state.calls) == 2
    assert state.calls[0]["expected_cursor"] is state.calls[1]["expected_cursor"]
    assert len(caplog.records) == 1 and "connection_invalidated=True" in caplog.text
    assert caplog.records[0].getMessage().startswith("custom_import_batch_retry_attempted ")
    assert "custom_import_batch_unavailable" not in caplog.text
    assert b"after" not in reply.body and b"rollback" not in reply.body


async def test_second_source_disconnect_is_terminal_and_preserves_both_closed_logs(monkeypatch, caplog):
    request, _ = _wire()
    state = _install_retry_step(monkeypatch, [_disconnected(), _disconnected(OperationalError), _committed()])

    reply = await _serve(request, state.factory)

    assert reply.status == 503 and json.loads(reply.body) == {"error": "source_unavailable"}
    assert len(state.calls) == len(state.sessions) == 2
    assert state.events[-2:] == [(1, "rollback"), (1, "close")]
    assert len(caplog.records) == 2
    assert caplog.records[0].getMessage().startswith("custom_import_batch_retry_attempted ")
    assert caplog.records[1].getMessage().startswith("custom_import_batch_unavailable ")
    assert "database_subtype=sqlalchemy_interface" in caplog.records[0].getMessage()
    assert "database_subtype=sqlalchemy_operational" in caplog.records[1].getMessage()
    assert "synthetic-" not in caplog.text


@pytest.mark.parametrize("blocked", [True, None, 0, "unknown", object()])
async def test_unknown_or_blocked_source_cleanup_marker_prevents_retry(monkeypatch, blocked):
    _, verified = _wire()
    failure = _disconnected()
    failure._custom_import_retry_blocked = blocked
    state = _install_retry_step(monkeypatch, [failure, _committed()])
    with pytest.raises(InterfaceError) as caught:
        await service._source_operation(state.factory, verified)
    assert caught.value is failure and len(state.calls) == 1


@pytest.mark.parametrize("invalidated", [None, 1, "true", object()])
async def test_source_retry_requires_a_literal_invalidated_boolean(monkeypatch, invalidated):
    _, verified = _wire()
    failure = _disconnected()
    failure.connection_invalidated = invalidated
    state = _install_retry_step(monkeypatch, [failure, _committed()])
    with pytest.raises(InterfaceError) as caught:
        await service._source_operation(state.factory, verified)
    assert caught.value is failure and len(state.calls) == 1


@pytest.mark.parametrize("attribute", ["connection_invalidated", "_custom_import_retry_blocked"])
async def test_source_retry_does_not_invoke_exception_descriptors(monkeypatch, attribute):
    class ObservedInterfaceError(InterfaceError):
        pass

    _, verified = _wire()
    failure = _disconnected(ObservedInterfaceError)
    forbidden = Mock(side_effect=AssertionError("exception descriptor must remain unread"))
    monkeypatch.setattr(ObservedInterfaceError, attribute, property(forbidden), raising=False)
    state = _install_retry_step(monkeypatch, [failure, _committed()])
    with pytest.raises(InterfaceError) as caught:
        await service._source_operation(state.factory, verified)
    assert caught.value is failure and len(state.calls) == 1
    forbidden.assert_not_called()


async def test_source_retry_inspection_failure_preserves_original_error(monkeypatch):
    _, verified = _wire()
    failure = _disconnected()
    state = _install_retry_step(monkeypatch, [failure, _committed()])
    inspect_attribute = Mock(side_effect=RuntimeError("synthetic-inspection-failure"))
    monkeypatch.setattr(diagnostics, "getattr_static", inspect_attribute)

    with pytest.raises(InterfaceError) as caught:
        await service._source_operation(state.factory, verified)

    assert caught.value is failure and len(state.calls) == len(state.sessions) == 1
    assert state.events == [(0, "open"), (0, "begin"), (0, "rollback"), (0, "close")]
    inspect_attribute.assert_called_once_with(failure, "connection_invalidated", False)


async def test_failed_cleanup_blocks_retry_of_preserved_source_disconnect(monkeypatch):
    _, verified = _wire()
    failure, cleanup = _disconnected(), RuntimeError("synthetic-close")
    state = _install_retry_step(monkeypatch, [failure, _committed()], close_error=cleanup)
    with pytest.raises(InterfaceError) as caught:
        await service._source_operation(state.factory, verified)
    assert caught.value is failure and failure.__cause__ is cleanup
    assert failure._custom_import_retry_blocked is True and len(state.calls) == 1


@pytest.mark.parametrize(
    "failure",
    [
        InterfaceError(None, None, OSError(), connection_invalidated=False),
        DataError(None, None, OSError(), connection_invalidated=True),
        StatementError("synthetic", None, None, None),
        LeaseAuthorityLost("synthetic"),
        CancellationRequested("synthetic"),
        RuntimeError("synthetic"),
        asyncio.CancelledError(),
    ],
)
async def test_other_source_failures_never_retry(monkeypatch, failure):
    _, verified = _wire()
    state = _install_retry_step(monkeypatch, [failure, _committed()])
    with pytest.raises(type(failure)) as caught:
        await service._source_operation(state.factory, verified)
    assert caught.value is failure and len(state.calls) == 1


async def test_source_retry_refuses_a_current_task_with_pending_cancellation(monkeypatch):
    _, verified = _wire()
    failure = _disconnected()
    state = _install_retry_step(monkeypatch, [failure, _committed()])
    waiting = asyncio.Event()

    async def cancel_before_operation():
        waiting.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            assert asyncio.current_task().cancelling()
        return await service._source_operation(state.factory, verified)

    task = asyncio.create_task(cancel_before_operation())
    await waiting.wait()
    task.cancel()
    with pytest.raises(InterfaceError) as caught:
        await task
    assert caught.value is failure and len(state.calls) == 1


@pytest.mark.parametrize(
    "mutate",
    [
        lambda request: request.headers.add(source.KEY_ID_HEADER.lower(), "current"),
        lambda request: request.headers.add("Authorization", "Bearer duplicate"),
        lambda request: request.headers.add("X-Custom-Import-Source-Extra", "1"),
        lambda request: request.headers.add("X-Custom-Import-Admission-Context", "cross-purpose"),
        lambda request: setattr(request, "body", request.body + b"\n"),
        lambda request: setattr(request, "query_string", "more=1"),
        lambda request: setattr(request, "body", envelope.legacy.canonical(envelope.BODY | {"row": {}})),
    ],
)
async def test_denial_precedes_database_or_source_work(monkeypatch, mutate):
    request, _ = _wire()
    mutate(request)
    operation = AsyncMock()
    monkeypatch.setattr(service, "_source_operation", operation)
    reply = await _serve(request)
    assert reply.status == 403 and reply.headers["Cache-Control"] == "no-store"
    operation.assert_not_awaited()


@pytest.mark.parametrize(
    "error,status,code",
    [
        (LeaseAuthorityLost("do-not-log"), 409, "source_authority_lost"),
        (CancellationRequested("do-not-log"), 409, "source_authority_lost"),
        (service.SourceProgressConflict("do-not-log"), 409, "source_progress_conflict"),
        (RuntimeError("uncertain COMMIT do-not-log"), 503, "source_unavailable"),
        (OSError("do-not-log"), 503, "source_unavailable"),
    ],
)
async def test_failure_is_metadata_only_no_retry_or_rollback_claim(monkeypatch, caplog, error, status, code):
    request, _ = _wire()
    operation = AsyncMock(side_effect=error)
    monkeypatch.setattr(service, "_source_operation", operation)
    reply = await _serve(request)
    assert reply.status == status and json.loads(reply.body) == {"error": code}
    assert reply.headers["Cache-Control"] == "no-store"
    if status == 503:
        assert len(caplog.records) == 1
        assert (
            caplog.records[0].getMessage().startswith("custom_import_batch_unavailable stage=source_execute category=")
        )
    else:
        assert not caplog.records
    assert "do-not-log" not in caplog.text
    assert b"rollback" not in reply.body and b"do-not-log" not in reply.body
    operation.assert_awaited_once()


@pytest.mark.parametrize("stage", ["receive", "authorize", "execute", "receipt"])
async def test_unavailable_diagnostics_keep_private_values(monkeypatch, caplog, stage):
    request, _ = _wire()
    error = StatementError("synthetic-message", "SELECT synthetic_sql", {"field": "synthetic-payload"}, None)
    request.headers.add("X-Synthetic-Diagnostic", "synthetic-header")
    monkeypatch.setattr(service, "_source_operation", AsyncMock(return_value=_committed()))
    if stage == "receive":
        request = _streaming(b"synthetic-body")
        monkeypatch.setattr(admission, "_receive_admission_body", AsyncMock(side_effect=error))
        reply = await service.source_batch(request)
    else:
        patched_module, patched_attribute = {
            "authorize": (service.authorization, "verify_request"),
            "execute": (service, "_source_operation"),
            "receipt": (service, "_receipt"),
        }[stage]
        replacement = AsyncMock(side_effect=error) if stage == "execute" else Mock(side_effect=error)
        monkeypatch.setattr(patched_module, patched_attribute, replacement)
        reply = await _serve(request)
    assert reply.status == 503 and json.loads(reply.body) == {"error": "source_unavailable"}
    assert reply.headers["Cache-Control"] == "no-store"
    assert len(caplog.records) == 1
    log_entry = caplog.records[0]
    assert log_entry.getMessage() == (
        f"custom_import_batch_unavailable stage=source_{stage} category=database database_reason=unknown "
        "database_subtype=sqlalchemy_statement connection_invalidated=False retry_blocked=False"
    )
    assert log_entry.args == (f"source_{stage}", "database", "unknown", "sqlalchemy_statement", False, False)
    assert not log_entry.exc_info and log_entry.exc_text is None and log_entry.stack_info is None
    assert not any(sentinel in caplog.text for sentinel in ("synthetic-", "synthetic_sql"))
    assert request.body == b"" or request.body.decode() not in caplog.text


async def test_cancelled_handler_does_not_synthesize_receipt(monkeypatch):
    request, _ = _wire()
    monkeypatch.setattr(service, "_source_operation", AsyncMock(side_effect=asyncio.CancelledError))
    with pytest.raises(asyncio.CancelledError):
        await _serve(request)


@pytest.mark.parametrize(
    "changes",
    [
        {"execution_id": 99},
        {"before": source.SourceCursor(2, 0, 0, 0)},
        {"rows_processed": 0},
        {"phase": "admission"},
        {"capture_bundle_id": True},
    ],
)
async def test_malformed_service_result_is_unavailable_not_a_false_committed_ack(monkeypatch, changes):
    request, _ = _wire()
    monkeypatch.setattr(service, "_source_operation", AsyncMock(return_value=replace(_committed(), **changes)))
    assert (await _serve(request)).status == 503


def _streaming(*chunks):
    request, _ = _wire()
    request.app = SimpleNamespace(
        ctx=SimpleNamespace(
            custom_import_admission_authority=(
                envelope.legacy.keyring(),
                envelope.legacy.ORIGIN,
            )
        )
    )
    request.stream = BodyStream(*chunks)
    request.body = b""
    return request


async def test_route_default_off_without_shared_dedicated_authority():
    reply = await service.source_batch(SimpleNamespace(app=SimpleNamespace(ctx=SimpleNamespace())))
    assert reply.status == 503 and reply.headers["Cache-Control"] == "no-store"


@pytest.mark.parametrize("chunks", [(b"x" * 513,), (b"x" * 256, b"x" * 257)])
async def test_stream_is_bounded_before_service(monkeypatch, chunks):
    operation = AsyncMock()
    monkeypatch.setattr(service, "serve_source_batch", operation)
    reply = await service.source_batch(_streaming(*chunks))
    assert reply.status == 413
    operation.assert_not_awaited()


async def test_wrong_purpose_header_is_rejected_before_body_accumulation(monkeypatch):
    request = _streaming(b"never-read")
    request.headers.add("X-Custom-Import-Admission-Key-Id", "current")
    assert (await service.source_batch(request)).status == 403
    assert request.stream.request_max_size is None and request.body == b""


async def test_wrapper_uses_existing_pool_and_independent_origin(monkeypatch):
    request = _streaming(b"x" * 512)
    request.headers.extend([("Host", "forged.invalid"), ("X-Forwarded-Host", "forged.invalid")])
    operation = AsyncMock(return_value=admission._reply({"synthetic": True}))
    monkeypatch.setattr(service, "serve_source_batch", operation)
    monkeypatch.setattr(service, "db", SimpleNamespace(session_factory="existing-pool"))
    assert (await service.source_batch(request)).status == 200
    assert operation.await_args.args == (request, "existing-pool")
    assert operation.await_args.kwargs["expected_origin"] == envelope.legacy.ORIGIN
    assert request.body == b"x" * 512
