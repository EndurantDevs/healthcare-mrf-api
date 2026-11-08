# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded directory HTTP with configured transport and one aggregate request budget."""

from __future__ import annotations

import asyncio
import hashlib
import json
import ssl
import time
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime

import aiohttp

from process.vsp_directory_contract import (
    MAX_RESPONSE_BYTES,
    MIN_REQUEST_INTERVAL,
    VSPDirectoryError,
    canonical_json,
    request_key,
    require,
    validated_url,
)


def _unique_object(pairs):
    object_by_key = {}
    for key, item in pairs:
        require(key not in object_by_key, "vsp_response_duplicate_key")
        object_by_key[key] = item
    return object_by_key


def _reject_constant(_value):
    raise VSPDirectoryError("vsp_response_nonfinite_json")


def strict_json(raw):
    """Do not accept truncated, duplicate-key or nonstandard JSON source bytes."""
    try:
        value = json.loads(raw, object_pairs_hook=_unique_object, parse_constant=_reject_constant)
    except UnicodeError, ValueError, RecursionError:
        raise VSPDirectoryError("vsp_response_json_invalid") from None
    require(type(value) in (dict, list), "vsp_response_json_invalid")
    return value


def retry_delay(header, *, now=None):
    """Honor publisher delays, including HTTP dates, without shortening a cooldown."""
    if header is None:
        return 60.0
    now = now or datetime.now(UTC)
    try:
        seconds = float(header)
        require(0 <= seconds <= 86400, "vsp_retry_after_invalid")
        return max(1.0, seconds)
    except TypeError, ValueError:
        try:
            boundary = parsedate_to_datetime(header)
            require(boundary.tzinfo is not None, "vsp_retry_after_invalid")
            return max(1.0, (boundary - now).total_seconds())
        except TypeError, ValueError, OverflowError:
            raise VSPDirectoryError("vsp_retry_after_invalid") from None


async def _noop():
    return None


class VSPHTTP:
    """Serialize directory requests using the configured direct or proxy transport."""

    def __init__(
        self,
        session,
        proxy_binding,
        *,
        pause,
        max_requests=100000,
        clock=time.monotonic,
        sleep=asyncio.sleep,
        cancel_check=_noop,
    ):
        require(proxy_binding is None or callable(getattr(proxy_binding, "select", None)), "vsp_source_proxy_required")
        require(callable(pause) and type(max_requests) is int and max_requests > 0, "vsp_http_configuration_invalid")
        self.session = session
        self.binding = proxy_binding
        self._direct_tls = ssl.create_default_context() if proxy_binding is None else None
        self.pause = pause
        self.max_requests = max_requests
        self.clock, self.sleep = clock, sleep
        self.cancel_check = cancel_check
        self.requests = 0
        self._last_request = -float("inf")
        self._lock = asyncio.Lock()

    async def wait(self, seconds):
        """Honor the entire origin delay while polling cancellation every fifteen seconds."""
        while seconds > 0:
            await self.cancel_check()
            interval = min(15.0, seconds)
            await self.sleep(interval)
            seconds -= interval
        await self.cancel_check()

    async def _body(self, response):
        length = response.headers.get("Content-Length")
        if length is not None:
            require(
                length.isascii() and length.isdigit() and int(length) <= MAX_RESPONSE_BYTES,
                "vsp_response_length_invalid",
            )
        require(
            response.headers.get("Content-Encoding", "identity").lower() in {"", "identity"},
            "vsp_response_encoding_invalid",
        )
        content_type = response.headers.get("Content-Type", "").split(";", 1)[0].strip().lower()
        require(
            content_type == "application/json" or content_type.startswith("application/vnd.vsp."),
            "vsp_response_media_type_invalid",
        )
        raw = bytearray()
        async for part in response.content.iter_chunked(65536):
            raw.extend(part)
            require(len(raw) <= MAX_RESPONSE_BYTES, "vsp_response_length_invalid")
        require(length is None or len(raw) == int(length), "vsp_response_truncated")
        return bytes(raw)

    def _request_options(self, selection):
        if self.binding is None:
            return {"ssl": self._direct_tls}
        require(selection is not None, "vsp_source_proxy_required")
        options_by_name = dict(selection.request_options)
        context = options_by_name.get("ssl")
        require(
            bool(options_by_name.get("proxy"))
            and isinstance(context, ssl.SSLContext)
            and context.check_hostname
            and context.verify_mode == ssl.CERT_REQUIRED,
            "vsp_source_proxy_required",
        )
        return options_by_name

    def _cooldowns(self):
        return self.binding.cooldown_snapshot() if self.binding is not None else None

    async def _exchange(self, method, url, body, selection):
        headers_by_name = {
            "Accept": "application/json",
            "Accept-Encoding": "identity",
            "User-Agent": "HealthPorta-Provider-Directory/1.0",
        }
        if body is not None:
            headers_by_name["Content-Type"] = "application/json"
        async with self.session.request(
            method,
            url,
            data=canonical_json(body).encode() if body is not None else None,
            headers=headers_by_name,
            timeout=aiohttp.ClientTimeout(total=60),
            allow_redirects=False,
            **self._request_options(selection),
        ) as response:
            require(str(response.url) == url, "vsp_response_url_changed")
            if response.status in {408, 423, 425, 429, 500, 502, 503, 504}:
                return None, retry_delay(response.headers.get("Retry-After"))
            require(response.status == 200, "vsp_http_terminal_status")
            raw = await self._body(response)
            decoded = strict_json(raw)
            require(not isinstance(decoded, dict) or not decoded.get("errors"), "vsp_search_source_error")
            return (
                decoded,
                raw,
                {
                    "request_key": request_key(method, url, body),
                    "method": method,
                    "url": url,
                    "payload": body,
                    "observed_at": datetime.now(UTC).isoformat(),
                    "sha256": hashlib.sha256(raw).hexdigest(),
                    "byte_count": len(raw),
                    "proxy_id": selection.proxy_id if selection is not None else None,
                    "policy_revision": getattr(self.binding, "policy_revision", None),
                    "pool_revision": getattr(self.binding, "pool_revision", None),
                    "etag": response.headers.get("ETag"),
                    "last_modified": response.headers.get("Last-Modified"),
                },
            ), None

    async def request(self, method, url, body=None):
        """Return exact source bytes plus nonsecret provenance; never follow redirects."""
        validated_url(url, method=method)
        require(
            (method == "POST" and type(body) is dict) or (method == "GET" and body is None),
            "vsp_request_payload_invalid",
        )
        key = request_key(method, url, body)
        async with self._lock:
            for attempt in range(3):
                require(self.requests < self.max_requests, "vsp_request_budget_exhausted")
                await self.wait(max(0.0, self._last_request + MIN_REQUEST_INTERVAL - self.clock()))
                selection = self.binding.select(url, key) if self.binding is not None else None
                self._request_options(selection)
                self._last_request = self.clock()
                self.requests += 1
                await self.pause(
                    (datetime.now(UTC) + timedelta(seconds=MIN_REQUEST_INTERVAL)).isoformat(),
                    self._cooldowns(),
                )
                try:
                    reply, delay = await self._exchange(method, url, body, selection)
                    if reply is not None:
                        return reply
                    # The same origin delay applies across the entire proxy pool.
                    boundary = datetime.now(UTC) + timedelta(seconds=delay)
                    await self.pause(boundary.isoformat(), self._cooldowns())
                    require(attempt < 2, "vsp_http_retry_exhausted")
                    await self.wait(delay)
                except aiohttp.ClientError, TimeoutError:
                    require(attempt < 2, "vsp_http_transport_failed")
                    delay = 5.0 * (attempt + 1)
                    boundary = datetime.now(UTC) + timedelta(seconds=delay)
                    if self.binding is not None:
                        self.binding.defer(selection.proxy_id, boundary.isoformat())
                    await self.pause(boundary.isoformat(), self._cooldowns())
                    await self.wait(delay)
        raise VSPDirectoryError("vsp_http_retry_exhausted")
