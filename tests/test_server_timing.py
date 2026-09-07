"""Exercise timing against Sanic's real routing and middleware lifecycle."""

from types import SimpleNamespace

import pytest
from sanic import Sanic, response
from sanic.compat import Header
from sanic.http.constants import Stage

from api import server_timing


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [200, 422])
async def test_server_timing_includes_response_cleanup(monkeypatch, status):
    events = []

    def clock():
        """Use deterministic endpoints while recording lifecycle order."""
        events.append("clock")
        return 1_000 if len(events) == 1 else 75_001_000

    monkeypatch.setattr(server_timing.time, "perf_counter_ns", clock)
    app = Sanic(f"server_timing_{status}")
    server_timing.register_server_timing(app)

    @app.middleware("request")
    async def bind(_request):
        """Represent the normal request-session binding stage."""
        events.append("bind")

    @app.get("/")
    async def handler(_request):
        """Return a fully serialized response with a forged timing value."""
        events.append("handler")
        result = response.json({"status": status}, status=status)
        result.headers.add(server_timing.SERVER_DURATION_HEADER, "forged")
        result.headers.add(server_timing.SERVER_DURATION_HEADER, "duplicate")
        return result

    @app.middleware("response")
    async def cleanup(_request, _response):
        """Represent commit or rollback and completed session close."""
        events.append("cleanup")

    _request, observed = await app.asgi_client.get("/")
    assert events == ["clock", "bind", "handler", "cleanup", "clock"]
    assert observed.status_code == status
    assert observed.json == {"status": status}
    assert observed.headers[server_timing.SERVER_DURATION_HEADER] == "75000000"


@pytest.mark.asyncio
@pytest.mark.parametrize("body,content_type,started,stage", [
    (b"", "application/json", 1, Stage.HANDLER),
    (b"streamed", "text/plain", 1, Stage.HANDLER),
    (b"{}", "application/json", None, Stage.HANDLER),
    (b"{}", "application/json", "invalid", Stage.HANDLER),
    ("{}", "application/json", 1, Stage.HANDLER),
    (b"{}", "application/json", 1, Stage.RESPONSE),
    (b"{}", "application/json", 1, Stage.IDLE),
    (b"{}", "application/json", 1, None),
])
async def test_server_timing_omits_incomplete_or_unmeasured_responses(
    body, content_type, started, stage,
):
    handlers_by_event = {}
    app = SimpleNamespace(signal=lambda name: lambda fn: handlers_by_event.setdefault(name, fn))
    server_timing.register_server_timing(app)
    request = SimpleNamespace(ctx=SimpleNamespace(_healthporta_server_started_ns=started))
    headers = Header()
    if stage is Stage.HANDLER:
        headers.add(server_timing.SERVER_DURATION_HEADER, "stale")
        headers.add(server_timing.SERVER_DURATION_HEADER, "duplicate")
    observed = SimpleNamespace(
        body=body, content_type=content_type,
        stream=SimpleNamespace(stage=stage), headers=headers,
    )
    await handlers_by_event["http.lifecycle.response"](request, observed)
    assert observed.headers == {}
    assert not vars(request.ctx)


@pytest.mark.asyncio
async def test_server_timing_keeps_interleaved_requests_independent(monkeypatch):
    handlers_by_event = {}
    app = SimpleNamespace(signal=lambda name: lambda fn: handlers_by_event.setdefault(name, fn))
    server_timing.register_server_timing(app)
    ticks = iter([10, 20, 70, 90])
    monkeypatch.setattr(server_timing.time, "perf_counter_ns", lambda: next(ticks))
    requests = [SimpleNamespace(ctx=SimpleNamespace()) for _ in range(2)]
    for request in requests:
        await handlers_by_event["http.lifecycle.handle"](request)
    durations = []
    for request in requests:
        observed = response.json({"ok": True})
        observed.stream = SimpleNamespace(stage=Stage.HANDLER)
        await handlers_by_event["http.lifecycle.response"](request, observed)
        durations.append(observed.headers[server_timing.SERVER_DURATION_HEADER])
    assert durations == ["60", "70"]
