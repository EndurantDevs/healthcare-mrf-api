# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Acquire one national finder response through an exact authenticated Source proxy."""

import re
import ssl
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from urllib.parse import urlsplit

import aiohttp

from process.delta_dental_directory_capture import DEFAULT_REQUEST_BUDGET, MAX_PAGE_BYTES, FinderPaginationUnresolved
from process.delta_dental_directory_json import SOURCE_ENDPOINT, finder_payload
from process.ext.source_http_proxy import SourceProxyError, SourceProxyUnavailable

_CURSOR = "finder-http"


def _remaining_cooldown(binding):
    latest = datetime.now(UTC)
    retry_not_before = None
    for entry in binding.cooldown_snapshot()["cooldowns"]:
        until = entry["retry_not_before"]
        if until is None:
            raise SourceProxyUnavailable(None)
        if (boundary := datetime.fromisoformat(until)) > latest:
            latest, retry_not_before = boundary, until
    if retry_not_before is not None:
        raise SourceProxyUnavailable(retry_not_before)


def _selection(binding, url, cursor=_CURSOR, exclude_ids=frozenset()):
    if binding is None:
        raise SourceProxyError("delta_dental_finder_proxy_required")
    _remaining_cooldown(binding)
    route = binding.select(url, cursor, exclude_ids)
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


async def _check_status(response, binding, route, pause, on_hold, allow_retry):
    """Stop the source on access denials or rate limits before reading the body."""
    if response.status in {401, 403, 429, 503}:
        is_transient = response.status in {429, 503}
        until = _retry_boundary(response.headers.get("Retry-After")) if is_transient else None
        if on_hold is not None:
            on_hold(until, is_transient)
        binding.defer(route.proxy_id, until)
        if response.status == 429 and until is not None and allow_retry is not None:
            allow_retry()
        if on_hold is None:
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


def _pin_worker_routes(binding, concurrency):
    """Require a distinct allowed route for every worker before origin access."""
    routes = [_selection(binding, SOURCE_ENDPOINT)]
    for worker_id in range(1, concurrency):
        selected_ids = {route.proxy_id for route in routes}
        try:
            route = _selection(binding, SOURCE_ENDPOINT, f"{_CURSOR}-{worker_id}", selected_ids)
        except SourceProxyUnavailable as exc:
            raise SourceProxyError("delta_dental_finder_distinct_proxy_routes_required") from exc
        if route.proxy_id in selected_ids or any(route.request_options == pinned.request_options for pinned in routes):
            raise SourceProxyError("delta_dental_finder_distinct_proxy_routes_required")
        routes.append(route)
    return routes


def _validate_worker_route(binding, routes, worker_id):
    """Revalidate availability and fail closed if a worker would change routes."""
    route = routes[worker_id]
    if worker_id == 0:
        selected = _selection(binding, SOURCE_ENDPOINT)
    else:
        selected = _selection(
            binding,
            SOURCE_ENDPOINT,
            f"{_CURSOR}-{worker_id}",
            {other.proxy_id for other in routes if other is not route},
        )
    if selected.proxy_id != route.proxy_id:
        raise SourceProxyError("delta_dental_finder_proxy_route_changed")
    return route


@asynccontextmanager
async def open_finder_http(
    binding,
    *,
    cancel_check,
    pause,
    stats,
    concurrency=1,
    max_origin_requests=DEFAULT_REQUEST_BUDGET,
    before_start=None,
    on_hold=None,
):
    """Keep transport closed until the first authorized cache miss."""
    routes = _pin_worker_routes(binding, concurrency)
    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(limit=concurrency, limit_per_host=1),
        cookie_jar=aiohttp.DummyCookieJar(),
        trust_env=False,
        auto_decompress=False,
        timeout=aiohttp.ClientTimeout(total=60),
    ) as http:

        async def fetch_page(search, number, previous_page, *, worker_id=0, start_attempt=None, allow_retry=None):
            """POST once per exact search and keep an interrupted HTML window partial."""
            if number != 1 or previous_page is not None:
                raise FinderPaginationUnresolved("legacy_window_remaining_page_unavailable")
            await cancel_check()
            route = _validate_worker_route(binding, routes, worker_id)

            def reserve_request():
                """Enforce the request budget at final admission."""
                if stats["origin_requests"] >= max_origin_requests:
                    raise SourceProxyError("delta_dental_finder_request_budget_exhausted")
                if start_attempt is not None:
                    start_attempt()

            if before_start is None:
                reserve_request()
            else:
                await before_start(reserve_request)
            stats["origin_requests"] += 1
            try:
                async with http.post(
                    SOURCE_ENDPOINT,
                    json=finder_payload(search),
                    headers={"Accept": "application/json", "Accept-Encoding": "identity"},
                    allow_redirects=False,
                    **route.request_options,
                ) as response:
                    await _check_status(response, binding, route, pause, on_hold, allow_retry)
                    return await _json_body(response)
            except aiohttp.ClientHttpProxyError:
                raise SourceProxyError("delta_dental_finder_proxy_connect_failed") from None
            except aiohttp.ClientError, TimeoutError:
                raise SourceProxyError("delta_dental_finder_request_failed") from None

        yield fetch_page
