# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Render the finder with one frozen, TLS-verified Source proxy route."""

import asyncio
import re
import ssl
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from time import monotonic
from urllib.parse import urljoin, urlsplit

import aiohttp

from process.control_cancel import ImportCancelledError
from process.delta_dental_directory_browser import FINDER_URL, SOURCE_ORIGIN
from process.ext.source_http_proxy import SourceProxyError, SourceProxyUnavailable

_IGNORED_TYPES = {"image", "media", "font", "ping", "eventsource"}
_HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "host",
    "content-length",
}
_MAX_BODY = 8 * 1024 * 1024
_ASSET_CACHE_BYTES = 16 * 1024 * 1024
_CURSOR = "finder-browser"


def _remaining_cooldown(binding):
    now = datetime.now(UTC)
    for item in binding.cooldown_snapshot()["cooldowns"]:
        until = item["retry_not_before"]
        if until is None or datetime.fromisoformat(until) > now:
            raise SourceProxyUnavailable(until)


def _selection(binding, url):
    if binding is None:
        raise SourceProxyError("delta_dental_finder_proxy_required")
    _remaining_cooldown(binding)
    route = binding.select(url, _CURSOR)
    options = route.request_options
    trust = options.get("ssl")
    if not (
        urlsplit(options.get("proxy", "")).scheme == "https"
        and options.get("proxy_auth")
        and isinstance(trust, ssl.SSLContext)
        and trust.check_hostname
        and trust.verify_mode == ssl.CERT_REQUIRED
    ):
        raise SourceProxyError("delta_dental_finder_proxy_required")
    return route


def _retry_boundary(value):
    now = datetime.now(UTC)
    if value is None:
        return (now + timedelta(seconds=60)).isoformat()
    try:
        if re.fullmatch(r"[0-9]+", value.strip()):
            return (now + timedelta(seconds=max(1, int(value)))).isoformat()
        date = parsedate_to_datetime(value)
        if date.tzinfo is not None:
            return max(now + timedelta(seconds=1), date.astimezone(UTC)).isoformat()
    except ValueError, TypeError, OverflowError:
        return None
    return None


def _headers(values):
    return {key.lower(): value for key, value in values.items() if key.lower() not in _HOP_HEADERS}


class _FinderTransport:
    def __init__(self, http, binding, selection, cancel_check, pause, stats, max_requests):
        self.http = http
        self.binding = binding
        self.selection = selection
        self.cancel_check = cancel_check
        self.pause = pause
        self.stats = stats
        self.stats.update(origin_requests=0, suppressed_requests=0, asset_cache_hits=0)
        self.max_requests = max_requests
        self.lock = asyncio.Lock()
        self.failure = None
        self.assets = {}
        self.asset_bytes = 0

    def check(self):
        """Raise the first transport failure before accepting a rendered page."""
        if self.failure is not None:
            raise self.failure

    async def route(self, browser_route):
        """Forward eligible browser requests serially on the prepared connection."""
        request = browser_route.request
        if not request.url.startswith(SOURCE_ORIGIN + "/") or request.resource_type in _IGNORED_TYPES:
            self.stats["suppressed_requests"] += 1
            await browser_route.abort()
            return
        async with self.lock:
            try:
                self.check()
                await self.cancel_check()
                status, headers, body = await self._response(request)
            except Exception as error:
                self.failure = (
                    error
                    if isinstance(error, (SourceProxyError, ImportCancelledError))
                    else SourceProxyError("delta_dental_finder_transport_failed")
                )
                await browser_route.abort()
                return
        await browser_route.fulfill(status=status, headers=headers, body=body)

    async def _check_status(self, response):
        """Persist publisher access holds before stopping the route."""
        if response.status in {401, 403, 429, 503}:
            is_transient = response.status in {429, 503}
            until = _retry_boundary(response.headers.get("Retry-After")) if is_transient else None
            self.binding.defer(self.selection.proxy_id, until)
            await self.pause(until, self.binding.cooldown_snapshot())
            if is_transient:
                raise SourceProxyUnavailable(until)
            raise SourceProxyError("delta_dental_finder_access_denied")

    async def _response(self, request):
        """Reuse eligible assets or run a bounded same-origin proxy request."""
        method, url = request.method, request.url
        if method not in {"GET", "HEAD", "POST"}:
            raise SourceProxyError("delta_dental_finder_request_unsupported")
        headers = _headers(await request.all_headers())
        if "authorization" in headers:
            raise SourceProxyError("delta_dental_finder_origin_auth_unsupported")
        headers["accept-encoding"] = "identity"
        is_asset = request.resource_type in {"script", "stylesheet"} or (
            request.resource_type == "document" and url == FINDER_URL
        )
        cacheable = (
            method == "GET" and is_asset and not any(key in headers for key in ("cookie", "cache-control", "pragma"))
        )
        cached = self.assets.get(url) if cacheable else None
        if (
            cached
            and cached[0] > monotonic()
            and all(headers.get(name) == variant_header for name, variant_header in cached[1])
        ):
            self.stats["asset_cache_hits"] += 1
            return cached[2]
        selection = self.binding.select(url, _CURSOR)
        if selection.proxy_id != self.selection.proxy_id:
            raise SourceProxyError("delta_dental_finder_proxy_route_changed")
        if self.stats["origin_requests"] >= self.max_requests:
            raise SourceProxyError("delta_dental_finder_request_budget_exhausted")
        request_body = request.post_data_buffer
        if request_body is not None and len(request_body) > _MAX_BODY:
            raise SourceProxyError("delta_dental_finder_request_too_large")
        self.stats["origin_requests"] += 1
        async with self.http.request(
            method, url, data=request_body, headers=headers, allow_redirects=False, **self.selection.request_options
        ) as response:
            await self._check_status(response)
            if 300 <= response.status < 400:
                destination = urljoin(url, response.headers.get("Location", ""))
                if not destination.startswith(SOURCE_ORIGIN + "/"):
                    raise SourceProxyError("delta_dental_finder_external_redirect")
                self.binding.select(destination, _CURSOR)
            if response.content_length is not None and response.content_length > _MAX_BODY:
                raise SourceProxyError("delta_dental_finder_response_too_large")
            raw = bytearray()
            async for part in response.content.iter_chunked(65536):
                raw.extend(part)
                if len(raw) > _MAX_BODY:
                    raise SourceProxyError("delta_dental_finder_response_too_large")
            outgoing = _headers(response.headers)
            cookies = response.headers.getall("Set-Cookie", [])
            if cookies:
                outgoing["set-cookie"] = "\n".join(cookies)
            reply = response.status, outgoing, bytes(raw)
        if cacheable and response.status == 200 and not cookies:
            self._remember_asset(url, headers, reply)
        return reply

    def _remember_asset(self, url, request_headers, result):
        _, headers, body = result
        directives = {part.strip().lower() for part in headers.get("cache-control", "").split(",")}
        vary_headers = [item.strip().lower() for item in headers.get("vary", "").split(",") if item.strip()]
        if "public" not in directives or directives & {"private", "no-store", "no-cache"} or "*" in vary_headers:
            return
        ttl = next(
            (directive.removeprefix("max-age=") for directive in directives if directive.startswith("max-age=")), ""
        )
        age = headers.get("age", "0")
        if not re.fullmatch(r"[0-9]{1,10}", ttl) or not re.fullmatch(r"[0-9]{1,10}", age):
            return
        previous = self.assets.pop(url, None)
        if previous:
            self.asset_bytes -= len(previous[2][2])
        if self.asset_bytes + len(body) <= _ASSET_CACHE_BYTES and len(self.assets) < 512 and int(ttl) > int(age):
            self.assets[url] = (
                monotonic() + int(ttl) - int(age),
                [(name, request_headers.get(name)) for name in vary_headers],
                result,
            )
            self.asset_bytes += len(body)


class _DenyBrowserProtocol(asyncio.Protocol):
    """Reject native browser HTTP and CONNECT without reading or forwarding either."""

    def connection_made(self, transport):
        """Close the accepted socket after reporting a fixed denial."""
        transport.write(b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
        transport.close()


@asynccontextmanager
async def _deny_browser_egress():
    """Reserve a private loopback endpoint for all unintercepted browser traffic."""
    loop = asyncio.get_running_loop()
    listener = await loop.create_server(_DenyBrowserProtocol, "127.0.0.1", 0)
    try:
        yield listener.sockets[0].getsockname()[1]
    finally:
        listener.close()
        listener.abort_clients()
        await listener.wait_closed()


def _firefox_preferences(port):
    """Retain Firefox isolation and direct native traffic only to the deny socket."""
    return {
        "network.proxy.type": 1,
        "network.proxy.http": "127.0.0.1",
        "network.proxy.http_port": port,
        "network.proxy.ssl": "127.0.0.1",
        "network.proxy.ssl_port": port,
        "network.proxy.no_proxies_on": "",
        "network.proxy.allow_hijacking_localhost": True,
        "network.proxy.failover_direct": False,
        "media.peerconnection.enabled": False,
        "fission.autostart": True,
        "fission.webContentIsolationStrategy": 1,
        "dom.file.createInChild": False,
        "dom.filesystem.pathcheck.disabled": False,
        "dom.permissions.testing.enabled": False,
    }


class FinderBrowserSession:
    def __init__(self, page, transport):
        self.page = page
        self.check = transport.check


@asynccontextmanager
async def open_finder_page(binding, *, cancel_check, pause, stats=None, max_requests=100000, executable_path=None):
    """Send browser HTTP through the selected Source session, including subresources."""
    selection = _selection(binding, FINDER_URL)
    if type(max_requests) is not int or max_requests < 1:
        raise SourceProxyError("delta_dental_finder_request_budget_invalid")
    from playwright.async_api import async_playwright

    async with (
        aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit_per_host=1),
            trust_env=False,
            auto_decompress=False,
            cookie_jar=aiohttp.DummyCookieJar(),
            timeout=aiohttp.ClientTimeout(total=60),
        ) as http,
        async_playwright() as playwright,
        _deny_browser_egress() as deny_port,
    ):
        transport = _FinderTransport(
            http, binding, selection, cancel_check, pause, stats if stats is not None else {}, max_requests
        )
        # Unhandled browser traffic cannot go directly to an origin. Only the
        # route handler above has the authenticated egress and its TLS trust.
        browser = await playwright.firefox.launch(
            executable_path=executable_path,
            firefox_user_prefs=_firefox_preferences(deny_port),
        )
        async with browser:
            context = await browser.new_context(service_workers="block", accept_downloads=False)
            await context.route("**/*", transport.route)
            await context.route_web_socket("**/*", lambda socket: socket.close())
            try:
                yield FinderBrowserSession(await context.new_page(), transport)
            finally:
                await context.close()
                transport.check()
