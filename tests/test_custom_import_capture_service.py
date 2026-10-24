# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Authentication precedes bounded receipt and the existing atomic registrar."""

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import control_capture_handoff as api
from process.custom_import import capture_wire
from process.custom_import.runner_types import LeaseAuthorityLost
from tests.capture_handoff_test_support import NOW, ORIGIN, fixture


class Stream:
    def __init__(self, chunks=()):
        self.chunks, self.reads, self.request_max_size = chunks, 0, None

    async def __aiter__(self):
        for chunk in self.chunks:
            self.reads += 1
            yield chunk


def _wire(state, operation="bundle", chunks=()):
    return SimpleNamespace(
        headers=state.headers,
        stream=Stream(chunks),
        method="POST",
        path="/control/v1/custom-import/capture-" + operation,
        query_string="",
    )


async def _serve(request, state):
    return await api.serve_capture_handoff(
        request,
        lambda: None,
        keyring=state.ring,
        expected_origin=ORIGIN,
        trusted_now=NOW,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["signature", "duplicate", "purpose", "compression", "plain_mapping"])
async def test_forbidden_headers_never_read_body_or_touch_store(monkeypatch, fault):
    state = fixture()
    if fault == "signature":
        state.headers["X-Custom-Import-Capture-Signature"] = "x" * 43
    elif fault == "duplicate":
        state.headers.add("Authorization", "Bearer duplicate")
    elif fault == "purpose":
        state.headers["X-Custom-Import-Source-Context"] = "other-purpose"
    elif fault == "compression":
        state.headers["Content-Encoding"] = "gzip"
    else:
        state.headers = dict(state.headers.items())
    preflight = AsyncMock()
    monkeypatch.setattr(api.service, "preflight_capture", preflight)
    request = _wire(state, chunks=[b"untrusted-body"])
    result = await _serve(request, state)
    assert result.status == 403 and result.body == b'{"error":"capture_forbidden"}'
    assert result.headers["Cache-Control"] == "no-store"
    assert request.stream.reads == 0
    preflight.assert_not_called()


@pytest.mark.asyncio
async def test_stale_or_canceled_authority_refuses_before_payload(monkeypatch):
    state = fixture()
    monkeypatch.setattr(api.service, "preflight_capture", AsyncMock(side_effect=LeaseAuthorityLost()))
    request = _wire(state, chunks=[b"not-read"])
    result = await _serve(request, state)
    assert result.status == 409 and request.stream.reads == 0


@pytest.mark.asyncio
async def test_complete_binary_bundle_reaches_existing_service_once(monkeypatch):
    state = fixture()
    chunks = [chunk async for chunk in capture_wire.iter_capture_body(state.captures, limits=state.request.limits)]
    monkeypatch.setattr(api.service, "preflight_capture", AsyncMock(return_value=NOW + timedelta(seconds=4)))
    register = AsyncMock(return_value=71)
    monkeypatch.setattr(api.service, "register_capture", register)
    request = _wire(state, chunks=chunks)
    result = await _serve(request, state)
    assert result.status == 200 and result.body == b'{"capture_bundle_id":71}'
    assert register.await_args.args[2] == state.captures
    assert request.stream.request_max_size == state.request.limits.maximum_compressed_bytes + 65_548


@pytest.mark.asyncio
@pytest.mark.parametrize("chunks", [(), (b"", b"")])
async def test_preflight_accepts_only_empty_body_without_registration(monkeypatch, chunks):
    state = fixture()
    deadline = NOW + timedelta(seconds=4)
    preflight = AsyncMock(return_value=deadline)
    register, retained = AsyncMock(), AsyncMock()
    monkeypatch.setattr(api.service, "preflight_capture", preflight)
    monkeypatch.setattr(api.service, "register_capture", register)
    monkeypatch.setattr(api.service, "retained_capture", retained)
    request = _wire(state, operation="preflight", chunks=chunks)

    result = await _serve(request, state)

    assert result.status == 200 and result.body == b'{"acquisition_deadline_at":"2030-01-01T00:00:04Z"}'
    assert result.headers["Cache-Control"] == "no-store"
    assert request.stream.request_max_size == 1 and request.stream.reads == len(chunks)
    preflight.assert_awaited_once()
    register.assert_not_called()
    retained.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["preflight", "retained"])
async def test_body_on_empty_body_route_is_rejected_before_store_access(monkeypatch, operation):
    state = fixture()
    preflight = AsyncMock(return_value=NOW + timedelta(seconds=4))
    register, retained = AsyncMock(), AsyncMock()
    monkeypatch.setattr(api.service, "preflight_capture", preflight)
    monkeypatch.setattr(api.service, "register_capture", register)
    monkeypatch.setattr(api.service, "retained_capture", retained)
    request = _wire(state, operation=operation, chunks=[b"", b"unexpected-body", b"not-read"])

    result = await _serve(request, state)

    assert result.status == 400 and result.body == b'{"error":"capture_body_invalid"}'
    assert result.headers["Cache-Control"] == "no-store"
    assert request.stream.request_max_size == 1 and request.stream.reads == 2
    assert preflight.await_count == (1 if operation == "preflight" else 0)
    register.assert_not_called()
    retained.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["malformed", "too_large"])
async def test_failed_body_receipt_never_registers_a_bundle(monkeypatch, fault):
    state = fixture()
    monkeypatch.setattr(api.service, "preflight_capture", AsyncMock(return_value=NOW + timedelta(seconds=4)))
    register = AsyncMock()
    monkeypatch.setattr(api.service, "register_capture", register)
    request = _wire(state, chunks=[b"invalid-capture-body"])
    if fault == "too_large":
        reader = AsyncMock(side_effect=api.PayloadTooLarge("synthetic-sensitive-detail"))
        monkeypatch.setattr(api.capture_wire, "read_capture_body", reader)

    result = await _serve(request, state)

    expected_status, expected_body = (
        (413, b'{"error":"capture_body_too_large"}')
        if fault == "too_large"
        else (400, b'{"error":"capture_body_invalid"}')
    )
    assert (result.status, result.body) == (expected_status, expected_body)
    assert result.headers["Cache-Control"] == "no-store"
    assert request.stream.request_max_size == state.request.limits.maximum_compressed_bytes + 65_548
    if fault == "too_large":
        reader.assert_awaited_once_with(request.stream, limits=state.request.limits)
    else:
        assert request.stream.reads == 1
    register.assert_not_called()


@pytest.mark.asyncio
async def test_uncertain_registration_returns_no_receipt_and_redacts_error(monkeypatch, caplog):
    state = fixture()
    chunks = [chunk async for chunk in capture_wire.iter_capture_body(state.captures, limits=state.request.limits)]
    monkeypatch.setattr(api.service, "preflight_capture", AsyncMock(return_value=NOW + timedelta(seconds=4)))
    register = AsyncMock(side_effect=RuntimeError(f"synthetic-sensitive-detail {state.signed.context} {chunks!r}"))
    monkeypatch.setattr(api.service, "register_capture", register)
    request = _wire(state, chunks=chunks)

    result = await _serve(request, state)

    assert result.status == 503 and result.body == b'{"error":"capture_unavailable"}'
    assert result.headers["Cache-Control"] == "no-store"
    register.assert_awaited_once()
    assert register.await_args.args[2] == state.captures
    assert request.stream.reads == len(chunks)
    assert [record.getMessage() for record in caplog.records] == ["custom_import_capture_unavailable (RuntimeError)"]
    assert all(record.exc_info is None and record.exc_text is None for record in caplog.records)


@pytest.mark.asyncio
async def test_canceled_preflight_propagates_without_reading_or_registering(monkeypatch, caplog):
    state = fixture()
    monkeypatch.setattr(api.service, "preflight_capture", AsyncMock(side_effect=asyncio.CancelledError))
    register = AsyncMock()
    monkeypatch.setattr(api.service, "register_capture", register)
    request = _wire(state, chunks=[b"not-read"])

    with pytest.raises(asyncio.CancelledError):
        await _serve(request, state)

    assert request.stream.reads == 0
    register.assert_not_called()
    assert not caplog.records


@pytest.mark.asyncio
async def test_retained_reply_streams_exact_originals_without_acquisition_or_registration(monkeypatch):
    state = fixture()
    preflight, register = AsyncMock(), AsyncMock()
    retained = AsyncMock(return_value=(71, state.captures))
    monkeypatch.setattr(api.service, "preflight_capture", preflight)
    monkeypatch.setattr(api.service, "register_capture", register)
    monkeypatch.setattr(api.service, "retained_capture", retained)
    request = _wire(state, operation="retained", chunks=[b""])

    result = await _serve(request, state)
    writer = SimpleNamespace(write=AsyncMock())
    await result.streaming_fn(writer)

    expected_chunks = [
        chunk async for chunk in capture_wire.iter_capture_body(state.captures, limits=state.request.limits)
    ]
    assert [call.args[0] for call in writer.write.await_args_list] == expected_chunks
    assert result.status == 200 and result.content_type == "application/octet-stream"
    assert result.headers["X-Custom-Import-Capture-Bundle-Id"] == "71"
    assert result.headers["Cache-Control"] == "no-store" and result.headers["Pragma"] == "no-cache"
    assert request.stream.request_max_size == 1 and request.stream.reads == 1
    retained.assert_awaited_once()
    preflight.assert_not_called()
    register.assert_not_called()


@pytest.mark.asyncio
async def test_disabled_capability_does_not_consume_body(monkeypatch):
    state = fixture()
    request = _wire(state, chunks=[b"not-read"])
    request.app = SimpleNamespace(ctx=SimpleNamespace())
    assert (await api._capture_route(request)).status == 503
    assert request.stream.reads == 0


@pytest.mark.asyncio
async def test_enabled_route_forwards_only_pinned_authority_and_current_time(monkeypatch):
    state = fixture()
    request = _wire(state)
    request.app = SimpleNamespace(ctx=SimpleNamespace(custom_import_capture_authority=(state.ring, ORIGIN)))
    serve = AsyncMock(return_value=object())
    monkeypatch.setattr(api, "serve_capture_handoff", serve)
    before = datetime.now(timezone.utc)

    result = await api._capture_route(request)

    assert result is serve.return_value
    serve.assert_awaited_once()
    assert serve.await_args.args == (request, api.db.session_factory)
    assert serve.await_args.kwargs["keyring"] is state.ring
    assert serve.await_args.kwargs["expected_origin"] == ORIGIN
    assert before <= serve.await_args.kwargs["trusted_now"] <= datetime.now(timezone.utc)
    assert request.stream.reads == 0


@pytest.mark.asyncio
async def test_startup_requires_explicit_enable_and_existing_pinned_authority(monkeypatch):
    state = object()
    app = SimpleNamespace(ctx=SimpleNamespace(custom_import_admission_authority=state))
    monkeypatch.delenv(api.ENABLED_ENV, raising=False)
    await api.initialize_capture_authority(app, None)
    assert app.ctx.custom_import_capture_authority is None
    monkeypatch.setenv(api.ENABLED_ENV, "1")
    await api.initialize_capture_authority(app, None)
    assert app.ctx.custom_import_capture_authority is state
    app.ctx.custom_import_admission_authority = None
    with pytest.raises(api.service.CaptureHandoffError):
        await api.initialize_capture_authority(app, None)


@pytest.mark.asyncio
async def test_retained_stream_cannot_outlive_its_original_operation_deadline():
    state = fixture()
    writer = SimpleNamespace(write=AsyncMock())
    reply = api._binary_reply(71, state.captures, state.request.limits, asyncio.get_running_loop().time() - 1)
    with pytest.raises(TimeoutError):
        await reply.streaming_fn(writer)
    writer.write.assert_not_called()


@pytest.mark.asyncio
async def test_canceled_retained_write_propagates_without_sending_more_chunks():
    state = fixture()
    writer = SimpleNamespace(write=AsyncMock(side_effect=asyncio.CancelledError))
    reply = api._binary_reply(71, state.captures, state.request.limits, asyncio.get_running_loop().time() + 60)

    with pytest.raises(asyncio.CancelledError):
        await reply.streaming_fn(writer)

    writer.write.assert_awaited_once()
