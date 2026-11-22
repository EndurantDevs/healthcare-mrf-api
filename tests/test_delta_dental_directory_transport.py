# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exercise the finder request boundary with synthetic routes and responses."""

import asyncio
import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from multidict import CIMultiDict

from process import delta_dental_directory_transport as finder
from process.control_cancel import ImportCancelledError
from process.ext.source_http_proxy import POLICY_ENV, POOL_ENV, SourceProxyConfig, SourceProxyError


def _binding(monkeypatch, scheme="https"):
    policy_rule_map = {"source_id": "finder", "use_proxy": True, "endpoint_scope": finder.SOURCE_ORIGIN}
    monkeypatch.setenv(POLICY_ENV, json.dumps({"version": 1, "revision": "policy", "sources": [policy_rule_map]}))
    routes = [{"id": str(i), "url": f"{scheme}://user:example@proxy{i}.example.test"} for i in range(2)]
    monkeypatch.setenv(POOL_ENV, json.dumps({"version": 1, "revision": "pool", "proxies": routes}))
    return SourceProxyConfig.from_environment().bind("finder", finder.SOURCE_ORIGIN)


class Reply:
    def __init__(self, status=200, headers=None, body=b"sample"):
        self.status = status
        self.headers = CIMultiDict(headers or {})
        self.body = body
        self.content_length = len(body)
        self.content = self

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def iter_chunked(self, size):
        yield self.body


def _route(url=None, resource="document", headers=None):
    request = SimpleNamespace(
        url=url or finder.FINDER_URL,
        resource_type=resource,
        method="GET",
        post_data_buffer=None,
        all_headers=AsyncMock(return_value=headers or {}),
    )
    return SimpleNamespace(request=request, abort=AsyncMock(), fulfill=AsyncMock())


def _transport(monkeypatch, replies):
    binding = _binding(monkeypatch)
    selected = finder._selection(binding, finder.FINDER_URL)
    calls = []

    def exchange(method, url, **options):
        assert options["proxy"] == selected.request_options["proxy"]
        assert options["proxy_auth"] == selected.request_options["proxy_auth"]
        assert options["ssl"].check_hostname and not options["allow_redirects"]
        assert "proxy-authorization" not in options["headers"]
        calls.append(url)
        return replies[len(calls) - 1]

    transport = finder._FinderTransport(
        SimpleNamespace(request=exchange), binding, selected, AsyncMock(), AsyncMock(), {}, 3
    )
    return transport, calls


async def test_source_scope_and_public_asset_reuse(monkeypatch):
    reply = Reply(headers={"Cache-Control": "public, max-age=60", "Vary": "Accept-Language"})
    transport, calls = _transport(monkeypatch, [reply, reply, Reply(200, {"Cache-Control": "private"})])
    external = _route("https://external.example.test/")
    await transport.route(external)
    external.abort.assert_awaited_once()
    for language in ("en", "en", "es"):
        route = _route(resource="script", headers={"Accept-Language": language})
        await transport.route(route)
        route.fulfill.assert_awaited_once()
    await transport.route(_route(resource="script", headers={"cookie": "session=synthetic"}))
    assert transport.stats == {"origin_requests": 3, "suppressed_requests": 1, "asset_cache_hits": 1}
    assert calls == [finder.FINDER_URL] * 3


@pytest.mark.parametrize("status", [401, 403, 429, 503])
async def test_stop_and_persist_cooldown_across_the_entire_pool(monkeypatch, status):
    transport, calls = _transport(monkeypatch, [Reply(status, {"Retry-After": "120"})])
    before = datetime.now(UTC)
    for _ in range(2):
        route = _route()
        await transport.route(route)
        route.abort.assert_awaited_once()
    with pytest.raises(SourceProxyError):
        transport.check()
    assert calls == [finder.FINDER_URL]
    transport.pause.assert_awaited_once()
    boundary, snapshot = transport.pause.await_args.args
    if status in {429, 503}:
        assert datetime.fromisoformat(boundary) >= before + timedelta(seconds=120)
    else:
        assert boundary is None
    rebound = _binding(monkeypatch)
    rebound.restore_cooldowns(snapshot)
    with pytest.raises(SourceProxyError, match="source_proxy_unavailable"):
        finder._selection(rebound, finder.FINDER_URL)


async def test_reject_redirect_and_preserve_cancellation(monkeypatch):
    transport, calls = _transport(monkeypatch, [Reply(302, {"Location": "https://external.example.test/"})])
    await transport.route(_route())
    with pytest.raises(SourceProxyError, match="external_redirect"):
        transport.check()
    assert calls == [finder.FINDER_URL]
    cancelled, calls = _transport(monkeypatch, [])
    cancelled.cancel_check.side_effect = ImportCancelledError("cancelled")
    await cancelled.route(_route())
    with pytest.raises(ImportCancelledError):
        cancelled.check()
    assert calls == []
    with pytest.raises(SourceProxyError, match="proxy_required"):
        finder._selection(None, finder.FINDER_URL)
    with pytest.raises(SourceProxyError, match="proxy_required"):
        finder._selection(_binding(monkeypatch, "http"), finder.FINDER_URL)


async def test_unrouted_browser_socket_is_denied():
    async with finder._deny_browser_egress() as port:
        preferences_by_field = finder._firefox_preferences(port)
        assert preferences_by_field["network.proxy.http"] == preferences_by_field["network.proxy.ssl"] == "127.0.0.1"
        assert preferences_by_field["network.proxy.http_port"] == preferences_by_field["network.proxy.ssl_port"] == port
        assert preferences_by_field["network.proxy.failover_direct"] is False
        assert preferences_by_field["network.proxy.no_proxies_on"] == ""
        assert preferences_by_field["network.proxy.allow_hijacking_localhost"] is True
        assert preferences_by_field["fission.autostart"] is True
        assert preferences_by_field["media.peerconnection.enabled"] is False
        client_reader, client_writer = await asyncio.open_connection("127.0.0.1", port)
        try:
            denial = await asyncio.wait_for(client_reader.read(), timeout=2)
            assert denial.startswith(b"HTTP/1.1 403 Forbidden\r\n")
        finally:
            client_writer.close()
            await client_writer.wait_closed()
    with pytest.raises(OSError):
        await asyncio.open_connection("127.0.0.1", port)


@pytest.mark.parametrize("failure", [RuntimeError, asyncio.CancelledError])
async def test_denial_listener_closes_after_failure(failure):
    with pytest.raises(failure):
        async with finder._deny_browser_egress() as port:
            raise failure("synthetic launch failure")
    with pytest.raises(OSError):
        await asyncio.open_connection("127.0.0.1", port)
