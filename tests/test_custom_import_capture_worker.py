# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""One-shot TLS framing and uncertain-acknowledgement behavior without network I/O."""

import asyncio
import json
import ssl
from dataclasses import replace
from datetime import timedelta, timezone
from io import BytesIO

import httpx
import pytest

from process.custom_import import capture_wire, capture_worker
from process.custom_import.capture import capture_stream
from tests.capture_handoff_test_support import NOW, ORIGIN, fixture
from tests.test_custom_import_admission_worker import _Stream


def _client(state, handler, monkeypatch, **overrides_by_name):
    monkeypatch.setattr(capture_worker, "_utcnow", lambda: NOW)
    return capture_worker.CaptureHandoffTransport(
        **{
            "expected_origin": ORIGIN,
            "signed_permit": state.signed,
            "launch_expires_at": NOW + timedelta(hours=1),
            "tls_context": ssl.create_default_context(),
            "transport": httpx.MockTransport(handler),
            **overrides_by_name,
        }
    )


def _json_reply(document):
    return httpx.Response(
        200,
        stream=_Stream([json.dumps(document, separators=(",", ":")).encode("ascii")]),
        headers={"Content-Type": "application/json", "Cache-Control": "no-store"},
    )


def test_transport_clock_uses_trusted_utc():
    now = capture_worker._utcnow()
    assert now.tzinfo is timezone.utc
    assert capture_worker.common._trusted_utc(now) is now


@pytest.mark.parametrize("failure", ["tls-type", "tls-hostname", "tls-verification", "permit-type", "transport-type"])
def test_untrusted_transport_configuration_is_refused_before_client_creation(monkeypatch, failure):
    state = fixture()
    calls = []
    tls = ssl.create_default_context()
    overrides_by_name = {}
    if failure == "tls-type":
        overrides_by_name["tls_context"] = None
    elif failure == "tls-hostname":
        tls.check_hostname = False
        overrides_by_name["tls_context"] = tls
    elif failure == "tls-verification":
        tls.verify_mode = ssl.CERT_OPTIONAL
        overrides_by_name["tls_context"] = tls
    elif failure == "permit-type":
        overrides_by_name["signed_permit"] = state.signed.context
    else:
        overrides_by_name["transport"] = object()
    with pytest.raises(capture_worker.CaptureTransportError, match="^custom_import_capture_transport_unavailable$"):
        _client(state, calls.append, monkeypatch, **overrides_by_name)
    assert calls == []


@pytest.mark.parametrize("failure", ["launch-expiry", "key-type", "key-format", "signature-length", "context"])
def test_invalid_launch_envelope_is_refused_before_client_creation(monkeypatch, failure):
    state = fixture()
    calls = []
    overrides_by_name = {}
    if failure == "launch-expiry":
        overrides_by_name["launch_expires_at"] = NOW + timedelta(minutes=14)
    elif failure == "key-type":
        overrides_by_name["signed_permit"] = replace(state.signed, key_id=None)
    elif failure == "key-format":
        overrides_by_name["signed_permit"] = replace(state.signed, key_id="invalid key")
    elif failure == "signature-length":
        overrides_by_name["signed_permit"] = replace(
            state.signed, signature=capture_worker.common._base64url_encode(b"s" * 31)
        )
    else:
        overrides_by_name["signed_permit"] = replace(state.signed, context="!")
    with pytest.raises(capture_worker.CaptureTransportError, match="^custom_import_capture_transport_unavailable$"):
        _client(state, calls.append, monkeypatch, **overrides_by_name)
    assert calls == []


@pytest.mark.asyncio
async def test_preflight_register_and_retained_use_exact_authenticated_paths(monkeypatch):
    state = fixture()
    calls = []

    async def respond(request):
        calls.append(request)
        assert "cookie" not in request.headers
        assert request.headers["Authorization"].startswith("Bearer ")
        assert request.url.host == "engine.example.invalid"
        if request.url.path.endswith("-preflight"):
            assert await request.aread() == b""
            return _json_reply({"acquisition_deadline_at": "2030-01-01T00:00:04Z"})
        if request.url.path.endswith("-bundle"):
            assert await capture_wire.read_capture_body(request.stream, limits=state.request.limits) == state.captures
            return _json_reply({"capture_bundle_id": 71})
        payload = b"".join(
            [
                chunk
                async for chunk in capture_wire.iter_capture_body(
                    state.captures,
                    limits=state.request.limits,
                )
            ]
        )
        return httpx.Response(
            200,
            stream=_Stream([payload]),
            headers={
                "Content-Type": "application/octet-stream",
                "Cache-Control": "no-store",
                "X-Custom-Import-Capture-Bundle-Id": "71",
            },
        )

    async with _client(state, respond, monkeypatch) as client:
        assert await client.preflight(state.request, state.grant) > 0
        assert await client.register(state.request, state.grant, state.captures) == 71
        assert await client.retained(state.request, state.grant, 71) == state.captures
    assert [request.url.path for request in calls] == [
        "/control/v1/custom-import/capture-preflight",
        "/control/v1/custom-import/capture-bundle",
        "/control/v1/custom-import/capture-retained",
    ]


@pytest.mark.asyncio
async def test_wrong_content_policy_is_refused_before_exchange(monkeypatch):
    state = fixture()
    calls = []
    async with _client(state, lambda request: calls.append(request), monkeypatch) as client:
        with pytest.raises(capture_worker.CaptureTransportError):
            await client.preflight(replace(state.request, maximum_dataset_retained_bytes=1), state.grant)
    assert calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("overrides_by_name", [{"definition": None}, {"objects": None}])
async def test_malformed_candidate_is_refused_before_exchange(monkeypatch, overrides_by_name):
    state = fixture()
    calls = []
    async with _client(state, calls.append, monkeypatch) as client:
        with pytest.raises(capture_worker.CaptureTransportError, match="^custom_import_capture_transport_unavailable$"):
            await client.preflight(replace(state.request, **overrides_by_name), state.grant)
    assert calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("preflight", [False, True])
async def test_registration_requires_live_original_acquisition_deadline(monkeypatch, preflight):
    state = fixture()
    calls = []

    def respond(request):
        calls.append(request.url.path)
        return _json_reply({"acquisition_deadline_at": "2030-01-01T00:00:04Z"})

    async with _client(state, respond, monkeypatch) as client:
        if preflight:
            await client.preflight(state.request, state.grant)
            monkeypatch.setattr(capture_worker, "_utcnow", lambda: NOW + timedelta(seconds=4))
        with pytest.raises(capture_worker.CaptureTransportError, match="^custom_import_capture_transport_unavailable$"):
            await client.register(state.request, state.grant, state.captures)
    assert calls == (["/control/v1/custom-import/capture-preflight"] if preflight else [])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "document",
    [
        {},
        {"acquisition_deadline_at": "2030-01-01T00:00:04Z", "extra": 1},
        {"acquisition_deadline_at": None},
        {"acquisition_deadline_at": "not-a-time"},
        {"acquisition_deadline_at": "2030-01-01T00:00:04"},
        {"acquisition_deadline_at": "2030-01-01T00:00:00Z"},
        {"acquisition_deadline_at": "2030-01-01T00:16:00Z"},
    ],
)
async def test_invalid_preflight_reply_never_opens_acquisition(monkeypatch, document):
    state = fixture()
    calls = []
    reply = _json_reply(document)

    def respond(request):
        calls.append(request.url.path)
        return reply

    async with _client(state, respond, monkeypatch) as client:
        with pytest.raises(capture_worker.CaptureTransportError, match="^custom_import_capture_transport_unavailable$"):
            await client.preflight(state.request, state.grant)
        assert client._acquisition_deadline is None
        with pytest.raises(capture_worker.CaptureTransportError):
            await client.register(state.request, state.grant, state.captures)
        assert not client._client.cookies
    assert calls == ["/control/v1/custom-import/capture-preflight"]
    assert reply.is_closed


@pytest.mark.asyncio
async def test_repeated_preflight_can_only_shorten_original_deadline(monkeypatch):
    state = fixture()
    deadlines = iter(["2030-01-01T00:00:04Z", "2030-01-01T00:00:10Z", "2030-01-01T00:00:02Z"])
    async with _client(
        state, lambda _request: _json_reply({"acquisition_deadline_at": next(deadlines)}), monkeypatch
    ) as client:
        await client.preflight(state.request, state.grant)
        await client.preflight(state.request, state.grant)
        assert client._acquisition_deadline == NOW + timedelta(seconds=4)
        await client.preflight(state.request, state.grant)
        assert client._acquisition_deadline == NOW + timedelta(seconds=2)


@pytest.mark.asyncio
@pytest.mark.parametrize("document", [{}, {"capture_bundle_id": 71, "extra": 1}, {"capture_bundle_id": 0}])
async def test_invalid_registration_acknowledgement_is_closed_without_retry(monkeypatch, document):
    state = fixture()
    calls = []
    reply = _json_reply(document)

    def respond(request):
        calls.append(request.url.path)
        if request.url.path.endswith("-preflight"):
            return _json_reply({"acquisition_deadline_at": "2030-01-01T00:00:04Z"})
        return reply

    async with _client(state, respond, monkeypatch) as client:
        await client.preflight(state.request, state.grant)
        with pytest.raises(capture_worker.CaptureTransportError, match="^custom_import_capture_transport_unavailable$"):
            await client.register(state.request, state.grant, state.captures)
        assert not client._client.cookies
    assert calls == ["/control/v1/custom-import/capture-preflight", "/control/v1/custom-import/capture-bundle"]
    assert reply.is_closed


@pytest.mark.asyncio
async def test_uncertain_registration_is_not_retried_or_reinterpreted(monkeypatch):
    state = fixture()
    calls = []

    async def respond(request):
        calls.append(request.url.path)
        if request.url.path.endswith("-preflight"):
            return _json_reply({"acquisition_deadline_at": "2030-01-01T00:00:04Z"})
        raise httpx.ReadTimeout("synthetic-lost-ack")

    async with _client(state, respond, monkeypatch) as client:
        await client.preflight(state.request, state.grant)
        with pytest.raises(capture_worker.CaptureTransportError):
            await client.register(state.request, state.grant, state.captures)
    assert len(calls) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["redirect", "encoding", "cookie", "bundle"])
async def test_retained_read_refuses_transport_or_binding_substitution(monkeypatch, change):
    state = fixture()
    headers_by_name = {
        "Content-Type": "application/octet-stream",
        "Cache-Control": "no-store",
        "X-Custom-Import-Capture-Bundle-Id": "71",
    }
    status = 200
    if change == "redirect":
        status, headers_by_name["Location"] = 302, "https://other.example.invalid"
    elif change == "encoding":
        headers_by_name["Content-Encoding"] = "gzip"
    elif change == "cookie":
        headers_by_name["Set-Cookie"] = "synthetic=ignored"
    else:
        headers_by_name["X-Custom-Import-Capture-Bundle-Id"] = "72"
    async with _client(state, lambda _request: httpx.Response(status, headers=headers_by_name), monkeypatch) as client:
        with pytest.raises(capture_worker.CaptureTransportError):
            await client.retained(state.request, state.grant, 71)
        assert not client._client.cookies


@pytest.mark.asyncio
async def test_retained_read_refuses_valid_originals_from_different_snapshot(monkeypatch):
    state = fixture()
    substituted_captures = tuple(
        capture_stream(BytesIO(capture.payload), stream, source_snapshot_token="different-snapshot")
        for capture, stream in zip(state.captures, state.request.definition.source_streams, strict=True)
    )
    framed_originals = b"".join(
        [chunk async for chunk in capture_wire.iter_capture_body(substituted_captures, limits=state.request.limits)]
    )
    reply = httpx.Response(
        200,
        stream=_Stream([framed_originals]),
        headers={
            "Content-Type": "application/octet-stream",
            "Cache-Control": "no-store",
            "X-Custom-Import-Capture-Bundle-Id": "71",
        },
    )
    calls = []

    def respond(request):
        calls.append(request.url.path)
        return reply

    async with _client(state, respond, monkeypatch) as client:
        with pytest.raises(capture_worker.CaptureTransportError, match="^custom_import_capture_transport_unavailable$"):
            await client.retained(state.request, state.grant, 71)
        assert not client._client.cookies
    assert calls == ["/control/v1/custom-import/capture-retained"]
    assert reply.is_closed


@pytest.mark.asyncio
async def test_canceling_retained_transfer_closes_reply_and_clears_cookies(monkeypatch):
    state = fixture()
    started = asyncio.Event()
    release = asyncio.Event()

    class WaitingStream(_Stream):
        async def __aiter__(self):
            started.set()
            await release.wait()
            yield b"unreachable"

    stream = WaitingStream([])
    calls = []

    def respond(request):
        calls.append(request.url.path)
        assert "cookie" not in request.headers
        return httpx.Response(
            200,
            stream=stream,
            headers={
                "Content-Type": "application/octet-stream",
                "Cache-Control": "no-store",
                "X-Custom-Import-Capture-Bundle-Id": "71",
            },
        )

    async with _client(state, respond, monkeypatch) as client:
        client._client.cookies.set("synthetic", "discard")
        task = asyncio.create_task(client.retained(state.request, state.grant, 71))
        try:
            await asyncio.wait_for(started.wait(), timeout=1)
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        assert not client._client.cookies
    assert calls == ["/control/v1/custom-import/capture-retained"]
    assert stream.closed
