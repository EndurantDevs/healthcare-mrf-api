# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Acquire one national finder response through an exact authenticated Source proxy."""

import re
import ssl
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from urllib.parse import urlsplit

import aiohttp

from process.delta_dental_directory_capture import MAX_PAGE_BYTES, FinderPaginationUnresolved
from process.delta_dental_directory_json import SOURCE_ENDPOINT, finder_payload
from process.ext.source_http_proxy import SourceProxyError, SourceProxyUnavailable

_MAX_REQUESTS = 100_000
_CURSOR = "finder-http"


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


async def _check_status(response, binding, route, pause):
    """Stop the source on access denials or rate limits before reading the body."""
    if response.status in {401, 403, 429, 503}:
        is_transient = response.status in {429, 503}
        until = _retry_boundary(response.headers.get("Retry-After")) if is_transient else None
        binding.defer(route.proxy_id, until)
        await pause(until, binding.cooldown_snapshot())
        if is_transient:
            raise SourceProxyUnavailable(until)
        raise SourceProxyError("delta_dental_finder_access_denied")
    if response.status in {400, 404, 422}:
        raise FinderPaginationUnresolved("finder_search_rejected")
    if response.status != 200:
        raise SourceProxyError("delta_dental_finder_response_status_invalid")


async def _json_body(response):
    """Bound the wire response before giving retained bytes to the parser."""
    if (
        response.content_type != "application/json"
        or response.headers.get("Content-Encoding", "identity") != "identity"
    ):
        raise SourceProxyError("delta_dental_finder_response_content_invalid")
    if response.content_length is not None and response.content_length > MAX_PAGE_BYTES:
        raise SourceProxyError("delta_dental_finder_response_too_large")
    raw = bytearray()
    async for part in response.content.iter_chunked(65536):
        raw.extend(part)
        if len(raw) > MAX_PAGE_BYTES:
            raise SourceProxyError("delta_dental_finder_response_too_large")
    return bytes(raw)


@asynccontextmanager
async def open_finder_http(binding, *, cancel_check, pause, stats):
    """Keep transport closed until the first authorized cache miss."""
    route = _selection(binding, SOURCE_ENDPOINT)
    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(limit_per_host=1),
        cookie_jar=aiohttp.DummyCookieJar(),
        trust_env=False,
        auto_decompress=False,
        timeout=aiohttp.ClientTimeout(total=60),
    ) as http:

        async def fetch_page(search, number, previous_page):
            """POST once per exact search and keep an interrupted HTML window partial."""
            if number != 1 or previous_page is not None:
                raise FinderPaginationUnresolved("legacy_window_remaining_page_unavailable")
            await cancel_check()
            selected = _selection(binding, SOURCE_ENDPOINT)
            if selected.proxy_id != route.proxy_id:
                raise SourceProxyError("delta_dental_finder_proxy_route_changed")
            if stats["origin_requests"] >= _MAX_REQUESTS:
                raise SourceProxyError("delta_dental_finder_request_budget_exhausted")
            stats["origin_requests"] += 1
            try:
                async with http.post(
                    SOURCE_ENDPOINT,
                    json=finder_payload(search),
                    headers={"Accept": "application/json", "Accept-Encoding": "identity"},
                    allow_redirects=False,
                    **route.request_options,
                ) as response:
                    await _check_status(response, binding, route, pause)
                    return await _json_body(response)
            except aiohttp.ClientHttpProxyError:
                raise SourceProxyError("delta_dental_finder_proxy_connect_failed") from None
            except aiohttp.ClientError, TimeoutError:
                raise SourceProxyError("delta_dental_finder_request_failed") from None

        yield fetch_page
