# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""One-shot TLS framing and uncertain-acknowledgement behavior without network I/O."""

import json
import ssl
from dataclasses import replace
from datetime import timedelta

import httpx
import pytest

from process.custom_import import capture_wire, capture_worker
from tests.capture_handoff_test_support import NOW, ORIGIN, fixture
from tests.test_custom_import_admission_worker import _Stream


def _client(state, handler, monkeypatch):
    monkeypatch.setattr(capture_worker, "_utcnow", lambda: NOW)
    return capture_worker.CaptureHandoffTransport(
        expected_origin=ORIGIN,
        signed_permit=state.signed,
        launch_expires_at=NOW + timedelta(hours=1),
        tls_context=ssl.create_default_context(),
        transport=httpx.MockTransport(handler),
    )


def _json_reply(document):
    return httpx.Response(
        200,
        stream=_Stream([json.dumps(document, separators=(",", ":")).encode("ascii")]),
        headers={"Content-Type": "application/json", "Cache-Control": "no-store"},
    )


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
