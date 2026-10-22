# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Authentication precedes bounded receipt and the existing atomic registrar."""

import asyncio
from datetime import timedelta
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
@pytest.mark.parametrize("fault", ["signature", "duplicate", "purpose", "compression"])
async def test_forbidden_headers_never_read_body_or_touch_store(monkeypatch, fault):
    state = fixture()
    if fault == "signature":
        state.headers["X-Custom-Import-Capture-Signature"] = "x" * 43
    elif fault == "duplicate":
        state.headers.add("Authorization", "Bearer duplicate")
    elif fault == "purpose":
        state.headers["X-Custom-Import-Source-Context"] = "other-purpose"
    else:
        state.headers["Content-Encoding"] = "gzip"
    preflight = AsyncMock()
    monkeypatch.setattr(api.service, "preflight_capture", preflight)
    request = _wire(state, chunks=[b"untrusted-body"])
    result = await _serve(request, state)
    assert result.status == 403 and result.headers["Cache-Control"] == "no-store"
    assert request.stream.reads == 0
    preflight.assert_not_awaited()


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
async def test_disabled_capability_does_not_consume_body(monkeypatch):
    state = fixture()
    request = _wire(state, chunks=[b"not-read"])
    request.app = SimpleNamespace(ctx=SimpleNamespace())
    assert (await api._capture_route(request)).status == 503
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
    writer.write.assert_not_awaited()
