# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Real HTTP deadlines for authenticated batches and neighboring public reads."""

import asyncio
import socket
from dataclasses import replace
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest
from sanic import Sanic, response
from sanic.compat import Header
from sanic.models.asgi import MockTransport
from sanic.request import Request

from api import control_admission_batch as admission
from api import control_source_batch as source
from process.custom_import import admission_sql
from process.custom_import.runner_types import LeaseAuthorityLost
from tests import test_custom_import_admission_service as admission_fixture
from tests import test_custom_import_source_service as source_fixture

_DEFAULT_TIMEOUT = 0.1
_WORK_SECONDS = 0.4


def _batch_state(kind):
    if kind == "admission":
        wire, verified, keyring = admission_fixture._wire()
        receipt = admission_sql.AdmissionResult("graph", 1, 1, 0)
    else:
        wire, verified = source_fixture._wire()
        keyring = source_fixture.envelope.legacy.keyring()
        receipt = source_fixture._committed()
    return SimpleNamespace(
        kind=kind,
        wire=wire,
        verified=verified,
        keyring=keyring,
        receipt=receipt,
        now=admission_fixture.NOW,
        entered=asyncio.Event(),
        attempted=0,
        committed=False,
        cancelled=False,
        failure=None,
        observations=[],
    )


def _install_writers(state, monkeypatch):
    clock = SimpleNamespace(now=lambda _timezone: state.now)
    monkeypatch.setattr(admission, "datetime", clock)
    monkeypatch.setattr(source, "datetime", clock)
    retained = admission_fixture._request(
        lease_seconds=2,
        build_deadline_at=state.now + timedelta(seconds=2),
        authorization_expires_at=state.verified.permit.expires_at,
    )
    monkeypatch.setattr(admission, "_retained_request", AsyncMock(return_value=retained))

    async def delayed_batch(*_args, **_kwargs):
        state.attempted += 1
        state.entered.set()
        try:
            await asyncio.sleep(_WORK_SECONDS)
        except asyncio.CancelledError:
            state.cancelled = True
            raise
        if state.failure is not None:
            raise state.failure
        state.committed = True
        return state.receipt

    monkeypatch.setattr(admission_sql, "admit_source_batch", delayed_batch)
    monkeypatch.setattr(source, "_source_operation", delayed_batch)


async def _close_server(server, state):
    protocols = set(server.connections) | {row[1] for row in state.observations}
    pending_tasks = [protocol._task for protocol in protocols if protocol._task is not None]
    for protocol in protocols:
        protocol.close()
    await server.close()
    await asyncio.wait_for(asyncio.gather(*pending_tasks, return_exceptions=True), timeout=1)
    assert not server.is_serving()
    assert all(
        protocol._callback_check_timeouts is None or protocol._callback_check_timeouts.cancelled()
        for protocol in protocols
    )


@pytest.fixture(params=("admission", "source"))
async def batch_server(request, monkeypatch):
    state = _batch_state(request.param)
    _install_writers(state, monkeypatch)
    app = Sanic(f"batch_deadline_{state.kind}_{uuid4().hex}", configure_logging=False)
    app.config.RESPONSE_TIMEOUT = _DEFAULT_TIMEOUT
    monkeypatch.setattr(Sanic, "test_mode", True)
    app.ctx.custom_import_admission_authority = (state.keyring, admission_fixture.ORIGIN)
    app.blueprint(admission.blueprint)
    app.blueprint(source.blueprint)
    admission.register_batch_response_deadline(app)

    @app.on_response
    async def observe(inbound, reply):
        state.observations.append((inbound.path, inbound.protocol, inbound.protocol.response_timeout, reply.status))

    @app.get("/public-slow")
    async def public_slow(_request):
        await asyncio.sleep(_WORK_SECONDS)
        return response.json({"ok": True})

    listener, server = socket.socket(), None
    try:
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        listener.setblocking(False)
        state.origin = f"http://127.0.0.1:{listener.getsockname()[1]}"
        server = await app.create_server(sock=listener, access_log=False)
        await server.startup()
        await server.start_serving()
        async with httpx.AsyncClient(base_url=state.origin, trust_env=False, timeout=2) as client:
            state.client, state.app = client, app
            yield state
    finally:
        try:
            if server is not None:
                await _close_server(server, state)
        finally:
            listener.close()
            Sanic.unregister_app(app)


async def _post(state):
    return await state.client.post(state.wire.path, headers=list(state.wire.headers.items()), content=state.wire.body)


async def test_batch_deadline_does_not_escape_to_concurrent_or_keepalive_public_request(batch_server):
    state = batch_server
    pending = asyncio.create_task(_post(state))
    try:
        await asyncio.wait_for(state.entered.wait(), timeout=1)
        async with httpx.AsyncClient(base_url=state.origin, trust_env=False, timeout=2) as public:
            assert (await public.get("/public-slow")).status_code == 503
        reply = await pending
        assert reply.status_code == 200 and reply.headers["cache-control"] == "no-store"
        assert state.attempted == 1 and state.committed and not state.cancelled
        assert state.app.config.RESPONSE_TIMEOUT == _DEFAULT_TIMEOUT
        batch_observation = next(row for row in state.observations if row[0] == state.wire.path)
        assert batch_observation[2] == (2 if state.kind == "admission" else 300)
        assert batch_observation[1].request_timeout == state.app.config.REQUEST_TIMEOUT == 60
        assert (await state.client.get("/public-slow")).status_code == 503
        public_observation = state.observations[-1]
        assert public_observation[1] is batch_observation[1]
        assert public_observation[2] == _DEFAULT_TIMEOUT
    finally:
        if not pending.done():
            pending.cancel()
        await asyncio.gather(pending, return_exceptions=True)


@pytest.mark.parametrize("failure", ("invalid_signature", "expired"))
async def test_denied_authority_never_extends_response_deadline(batch_server, failure):
    state = batch_server
    if failure == "expired":
        state.now = state.verified.permit.expires_at
    else:
        header = (
            "X-Custom-Import-Admission-Signature" if state.kind == "admission" else "X-Custom-Import-Source-Signature"
        )
        state.wire.headers[header] = "invalid"
    reply = await _post(state)
    assert reply.status_code == 403 and state.attempted == 0
    assert state.observations[-1][2] == _DEFAULT_TIMEOUT
    assert state.app.config.RESPONSE_TIMEOUT == _DEFAULT_TIMEOUT


@pytest.mark.parametrize("batch_server", ("admission",), indirect=True)
async def test_verified_admission_deadline_covers_the_retained_read(batch_server, monkeypatch):
    state = batch_server
    retained_read = admission._retained_request

    async def delayed_read(*args, **kwargs):
        await asyncio.sleep(_WORK_SECONDS)
        return await retained_read(*args, **kwargs)

    monkeypatch.setattr(admission, "_retained_request", delayed_read)
    reply = await _post(state)
    assert reply.status_code == 200
    assert state.attempted == 1 and state.committed and not state.cancelled
    assert state.observations[-1][2] == 2
    assert state.app.config.RESPONSE_TIMEOUT == _DEFAULT_TIMEOUT


@pytest.mark.parametrize("batch_server", ("admission",), indirect=True)
async def test_shorter_retained_deadline_rearms_the_provisional_timer(batch_server, monkeypatch):
    state = batch_server
    retained = replace(
        admission._retained_request.return_value,
        build_deadline_at=state.now + timedelta(seconds=0.6),
    )

    async def delayed_read(*_args, **_kwargs):
        await asyncio.sleep(_WORK_SECONDS)
        return retained

    monkeypatch.setattr(admission, "_retained_request", delayed_read)
    reply = await _post(state)
    assert reply.status_code == 503
    assert state.attempted == 1 and state.cancelled and not state.committed
    assert state.observations[-1][2] == 0.6
    assert state.app.config.RESPONSE_TIMEOUT == _DEFAULT_TIMEOUT


async def test_long_batch_preserves_lease_authority_failure_without_receipt(batch_server):
    state = batch_server
    state.failure = LeaseAuthorityLost("synthetic lost authority")
    reply = await _post(state)
    assert reply.status_code == 409
    assert reply.json() == {"error": f"{state.kind}_authority_lost"}
    assert state.attempted == 1 and not state.committed and not state.cancelled


@pytest.mark.parametrize("asgi", (False, True))
async def test_no_native_protocol_keeps_existing_transport_behavior(asgi):
    app = Sanic(f"non_native_batch_{uuid4().hex}", configure_logging=False)
    try:
        transport = None
        if asgi:
            transport = MockTransport({"type": "http", "path": "/"}, AsyncMock(), AsyncMock())
            transport.loop = asyncio.get_running_loop()
        request = Request(b"/", Header(), "1.1", "GET", transport, app)
        admission.extend_batch_response_deadline(
            request,
            expires_at=admission_fixture.NOW + timedelta(minutes=15),
            trusted_now=admission_fixture.NOW,
        )
        admission.register_batch_response_deadline(app)
        app.signalize(allow_fail_builtin=False)
        await app.dispatch("http.lifecycle.handle", inline=True, context={"request": request})
        if asgi:
            assert not hasattr(request.protocol, "response_timeout")
        assert app.config.RESPONSE_TIMEOUT == 60
    finally:
        Sanic.unregister_app(app)
