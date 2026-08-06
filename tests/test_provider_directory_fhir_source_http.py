# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import importlib
from unittest.mock import Mock

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

importer = importlib.import_module("process.provider_directory_fhir")


async def _start_test_server(app: web.Application) -> TestServer:
    server = TestServer(app)
    await server.start_server()
    return server


@pytest.mark.asyncio
async def test_source_http_session_reuses_anonymous_connections_and_redirects(
    monkeypatch,
):
    monkeypatch.setenv(importer.SOURCE_HTTP_KEEPALIVE_ENV, "true")
    transports: list[object] = []
    observed_headers: list[dict[str, str]] = []

    def record_request(request: web.Request) -> None:
        transports.append(request.transport)
        observed_headers.append(dict(request.headers))

    async def page(request: web.Request) -> web.Response:
        record_request(request)
        return web.json_response(
            {"resourceType": "Bundle", "type": "searchset", "entry": []}
        )

    async def redirect(request: web.Request) -> web.Response:
        record_request(request)
        redirect_response = web.HTTPFound("/page/redirected")
        redirect_response.set_cookie("ignored", "synthetic")
        raise redirect_response

    app = web.Application()
    app.router.add_get("/page/{page}", page)
    app.router.add_get("/redirect", redirect)
    server = await _start_test_server(app)
    try:
        async with importer._source_http_session_scope():
            paths = ["/redirect", *(f"/page/{number}" for number in range(20))]
            for path in paths:
                fetch_result = await importer._fetch_json(
                    str(server.make_url(path)),
                    timeout=2,
                )
                assert fetch_result[:3] == (
                    200,
                    {
                        "resourceType": "Bundle",
                        "type": "searchset",
                        "entry": [],
                    },
                    None,
                )
    finally:
        await server.close()

    assert len(transports) == 22
    assert all(transport is transports[0] for transport in transports)
    assert all(
        headers.get("Accept-Encoding") == "identity"
        for headers in observed_headers
    )
    assert all("Cookie" not in headers for headers in observed_headers)
    assert importer._SOURCE_HTTP_SESSION.get() is None


@pytest.mark.asyncio
async def test_source_http_session_preserves_body_caps_and_retry_after(monkeypatch):
    monkeypatch.setenv(importer.SOURCE_HTTP_KEEPALIVE_ENV, "true")
    observed_body_caps: list[int] = []
    read_response_body = importer._read_source_http_response_body

    async def capture_body_cap(response, *, timeout, max_bytes):
        observed_body_caps.append(max_bytes)
        return await read_response_body(
            response,
            timeout=timeout,
            max_bytes=max_bytes,
        )

    monkeypatch.setattr(importer, "_read_source_http_response_body", capture_body_cap)

    async def page(_request: web.Request) -> web.Response:
        return web.json_response({"resourceType": "Bundle", "entry": []})

    async def limited(_request: web.Request) -> web.Response:
        return web.json_response(
            {"resourceType": "OperationOutcome"},
            status=429,
            headers={"Retry-After": "30"},
        )

    app = web.Application()
    app.router.add_get("/page", page)
    app.router.add_get("/limited", limited)
    server = await _start_test_server(app)
    try:
        async with importer._source_http_session_scope():
            success = await importer._fetch_json(
                str(server.make_url("/page")),
                timeout=2,
            )
            failure = await importer._fetch_json(
                str(server.make_url("/limited")),
                timeout=2,
            )
    finally:
        await server.close()

    assert success[:3] == (200, {"resourceType": "Bundle", "entry": []}, None)
    assert failure[:3] == (
        429,
        {
            "resourceType": "OperationOutcome",
            importer.SOURCE_RETRY_AFTER_FIELD: "30",
        },
        None,
    )
    assert observed_body_caps == [importer.MAX_FHIR_JSON_BODY_BYTES, 1024 * 1024]


@pytest.mark.asyncio
async def test_source_http_body_reader_stops_at_configured_bound():
    async def page(_request: web.Request) -> web.Response:
        return web.Response(body=b"x" * 32)

    app = web.Application()
    app.router.add_get("/page", page)
    server = await _start_test_server(app)
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(server.make_url("/page")) as response:
                body = await importer._read_source_http_response_body(
                    response,
                    timeout=2,
                    max_bytes=7,
                )
    finally:
        await server.close()

    assert body == b"x" * 7


@pytest.mark.asyncio
async def test_source_http_session_sanitizes_redirect_loop_errors(monkeypatch):
    monkeypatch.setenv(importer.SOURCE_HTTP_KEEPALIVE_ENV, "true")
    synthetic_cursor = "opaque-cursor-must-not-leak"

    async def redirect_loop(_request: web.Request) -> web.Response:
        raise web.HTTPFound(f"/loop?cursor={synthetic_cursor}")

    app = web.Application()
    app.router.add_get("/loop", redirect_loop)
    server = await _start_test_server(app)
    try:
        async with importer._source_http_session_scope():
            fetch_result = await importer._fetch_json(
                str(server.make_url("/loop")),
                timeout=2,
            )
    finally:
        await server.close()

    assert fetch_result[:3] == (302, None, None)
    assert synthetic_cursor not in repr(fetch_result)


@pytest.mark.asyncio
async def test_source_http_session_retains_slow_error_status_and_retry_after(
    monkeypatch,
):
    monkeypatch.setenv(importer.SOURCE_HTTP_KEEPALIVE_ENV, "true")

    async def slow_limited(request: web.Request) -> web.StreamResponse:
        response = web.StreamResponse(
            status=429,
            headers={"Retry-After": "30"},
        )
        await response.prepare(request)
        await asyncio.sleep(0.1)
        return response

    app = web.Application()
    app.router.add_get("/limited", slow_limited)
    server = await _start_test_server(app)
    try:
        async with importer._source_http_session_scope():
            fetch_result = await importer._fetch_json(
                str(server.make_url("/limited")),
                timeout=0.01,
            )
    finally:
        await server.close()

    assert fetch_result[:3] == (
        429,
        {importer.SOURCE_RETRY_AFTER_FIELD: "30"},
        None,
    )


@pytest.mark.asyncio
async def test_source_http_session_keeps_credentialed_fetches_on_urllib(monkeypatch):
    monkeypatch.setenv(importer.SOURCE_HTTP_KEEPALIVE_ENV, "true")
    fetch_json_sync = Mock(
        return_value=(200, {"resourceType": "Bundle"}, None, 1)
    )
    monkeypatch.setattr(importer, "_fetch_json_sync", fetch_json_sync)

    async with importer._source_http_session_scope():
        header_auth_result = await importer._fetch_json_with_options(
            "https://payer.example/fhir/Practitioner",
            timeout=2,
            extra_headers={"Authorization": "Bearer private"},
        )
        url_auth_result = await importer._fetch_json(
            "https://synthetic:secret@payer.example/fhir/Practitioner",
            timeout=2,
        )

    expected_result = (200, {"resourceType": "Bundle"}, None, 1)
    assert header_auth_result == expected_result
    assert url_auth_result == expected_result
    assert fetch_json_sync.call_count == 2


@pytest.mark.asyncio
async def test_source_http_session_is_disabled_by_default(monkeypatch):
    monkeypatch.delenv(importer.SOURCE_HTTP_KEEPALIVE_ENV, raising=False)
    fetch_json_sync = Mock(
        return_value=(200, {"resourceType": "Bundle"}, None, 1)
    )
    monkeypatch.setattr(importer, "_fetch_json_sync", fetch_json_sync)

    async with importer._source_http_session_scope():
        assert importer._SOURCE_HTTP_SESSION.get() is None
        result = await importer._fetch_json(
            "https://payer.example/fhir/Practitioner",
            timeout=2,
        )

    assert result == (200, {"resourceType": "Bundle"}, None, 1)
    fetch_json_sync.assert_called_once()


@pytest.mark.asyncio
async def test_resource_import_wrapper_sets_and_clears_source_session(monkeypatch):
    monkeypatch.setenv(importer.SOURCE_HTTP_KEEPALIVE_ENV, "true")

    async def import_resources(source_records, **import_options):
        assert source_records == [{"source_id": "source_a"}]
        assert import_options == {"page_count": 100}
        assert importer._SOURCE_HTTP_SESSION.get() is not None
        child_session = await asyncio.create_task(
            asyncio.sleep(0, result=importer._SOURCE_HTTP_SESSION.get())
        )
        assert child_session is importer._SOURCE_HTTP_SESSION.get()
        return {"Practitioner": 2}

    monkeypatch.setattr(importer, "_import_resources", import_resources)

    result = await importer._import_resources_with_source_http_session(
        [{"source_id": "source_a"}],
        page_count=100,
    )

    assert result == {"Practitioner": 2}
    assert importer._SOURCE_HTTP_SESSION.get() is None


@pytest.mark.asyncio
async def test_source_http_session_closes_on_error_and_cancellation(monkeypatch):
    monkeypatch.setenv(importer.SOURCE_HTTP_KEEPALIVE_ENV, "true")
    created_sessions: list[aiohttp.ClientSession] = []
    create_session = importer._source_http_client_session

    def capture_session() -> aiohttp.ClientSession:
        session = create_session()
        created_sessions.append(session)
        return session

    monkeypatch.setattr(importer, "_source_http_client_session", capture_session)

    with pytest.raises(RuntimeError, match="synthetic failure"):
        async with importer._source_http_session_scope():
            raise RuntimeError("synthetic failure")

    request_started = asyncio.Event()
    release_response = asyncio.Event()

    async def slow_page(request: web.Request) -> web.StreamResponse:
        response = web.StreamResponse(status=200)
        await response.prepare(request)
        request_started.set()
        await release_response.wait()
        return response

    app = web.Application()
    app.router.add_get("/slow", slow_page)
    server = await _start_test_server(app)

    async def fetch_in_scope() -> None:
        async with importer._source_http_session_scope():
            await importer._fetch_json(str(server.make_url("/slow")), timeout=10)

    task = asyncio.create_task(fetch_in_scope())
    await request_started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    release_response.set()
    await server.close()

    assert len(created_sessions) == 2
    assert all(session.closed for session in created_sessions)
    assert importer._SOURCE_HTTP_SESSION.get() is None


@pytest.mark.asyncio
async def test_source_http_session_normalizes_read_timeout(monkeypatch):
    monkeypatch.setenv(importer.SOURCE_HTTP_KEEPALIVE_ENV, "true")

    async def slow_page(_request: web.Request) -> web.Response:
        await asyncio.sleep(0.1)
        return web.json_response({"resourceType": "Bundle"})

    app = web.Application()
    app.router.add_get("/slow", slow_page)
    server = await _start_test_server(app)
    try:
        async with importer._source_http_session_scope():
            result = await importer._fetch_json(
                str(server.make_url("/slow")),
                timeout=0.01,
            )
    finally:
        await server.close()

    assert result[0] is None
    assert result[1] is None
    assert "timeout" in (result[2] or "").lower()
