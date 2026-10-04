# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded single-object HTTPS acquisition into the existing capture contract.

Callers supply expected raw content identity and an opaque snapshot token.
This module has no ambient credentials, hosted authority, persistence or retries.
Capture/decoding limits remain authoritative; a sealed payload is held in memory.
"""

from __future__ import annotations

import asyncio
import hashlib
import ipaddress
import math
import re
from contextlib import AsyncExitStack
from tempfile import SpooledTemporaryFile
from typing import BinaryIO
from urllib.parse import urlsplit

import aiohttp
from aiohttp.abc import AbstractResolver, ResolveResult
from yarl import URL

from process.custom_import._source_text import validate_snapshot_token
from process.custom_import.capture import SealedCapture, capture_stream
from process.custom_import.capture_limits import CaptureLimits
from process.custom_import.definition import _COMPRESSIONS, _FORMATS, SourceStream
from process.url_security import assert_public_ip

_REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})
_MAX_REDIRECTS = 3


class HttpsAcquisitionError(ValueError):
    """A redacted acquisition failure; source locations and bytes are not exposed."""


class _StrictResolver(AbstractResolver):
    """Validate every address that aiohttp will use for the actual connection."""

    def __init__(self) -> None:
        self._resolver = aiohttp.DefaultResolver()

    async def resolve(self, host: str, port: int = 0, family: int = 0) -> list[ResolveResult]:
        """Deny empty or mixed-public/private answers without an environment bypass."""
        addresses = await self._resolver.resolve(host, port, family)
        if not addresses:
            raise HttpsAcquisitionError("https_destination_denied")
        for address in addresses:
            assert_public_ip(ipaddress.ip_address(address["host"]), strict=True)
        return addresses

    async def close(self) -> None:
        """Release the delegated resolver even when the connector does not own it."""
        await self._resolver.close()


def _https_url(url_text: str, *, relative_to: URL | None = None) -> URL:
    """Validate raw authority before client normalization and redirect resolution."""
    if (
        not isinstance(url_text, str)
        or "#" in url_text
        or any(ord(char) <= 32 or ord(char) == 127 for char in url_text)
        or "@" in urlsplit(url_text).netloc
    ):
        raise HttpsAcquisitionError("https_url_denied")
    url = URL(url_text)
    if relative_to is not None:
        url = relative_to.join(url)
    hostname = url.raw_host
    if url.scheme != "https" or not hostname or url.raw_user is not None or url.raw_password is not None:
        raise HttpsAcquisitionError("https_url_denied")
    if not url.port or "%" in hostname:
        raise HttpsAcquisitionError("https_url_denied")
    # aiohttp treats numeric hosts as literals, including noncanonical IPv4.
    if ":" in hostname or hostname.replace(".", "").isdigit():
        assert_public_ip(ipaddress.ip_address(hostname), strict=True)
    return url


def _validate_identity(
    expected_sha256: str, expected_bytes: int, limits: CaptureLimits, timeout_seconds: float
) -> None:
    """Fail before I/O when expected identity or resource bounds are invalid."""
    if not isinstance(expected_sha256, str) or not re.fullmatch(r"[0-9a-f]{64}", expected_sha256):
        raise HttpsAcquisitionError("https_request_invalid")
    if (
        not isinstance(limits, CaptureLimits)
        or isinstance(expected_bytes, bool)
        or not isinstance(expected_bytes, int)
        or not 0 <= expected_bytes <= limits.maximum_compressed_bytes
        or isinstance(timeout_seconds, bool)
        or not isinstance(timeout_seconds, (int, float))
        or not math.isfinite(timeout_seconds)
        or timeout_seconds <= 0
    ):
        raise HttpsAcquisitionError("https_request_invalid")


def _check_response(response: aiohttp.ClientResponse, expected_bytes: int) -> None:
    """Require a complete identity-encoded representation, never a partial object."""
    if response.status != 200 or "Content-Range" in response.headers:
        raise HttpsAcquisitionError("https_response_denied")
    for name, allowed in (("Content-Encoding", "identity"), ("Transfer-Encoding", "chunked")):
        headers = response.headers.getall(name, [])
        if headers and (len(headers) != 1 or headers[0].strip().lower() != allowed):
            raise HttpsAcquisitionError("https_response_denied")
    lengths = response.headers.getall("Content-Length", [])
    if lengths and "Transfer-Encoding" in response.headers:
        raise HttpsAcquisitionError("https_response_denied")
    if lengths and (len(lengths) != 1 or not lengths[0].isascii() or not lengths[0].isdigit()):
        raise HttpsAcquisitionError("https_response_denied")
    if lengths and int(lengths[0]) != expected_bytes:
        raise HttpsAcquisitionError("https_length_mismatch")


async def _copy_response(
    response: aiohttp.ClientResponse,
    spool: BinaryIO,
    expected_sha256: str,
    expected_bytes: int,
    limits: CaptureLimits,
) -> None:
    """Count/hash raw bytes independently of headers before admitting a capture."""
    _check_response(response, expected_bytes)
    digest = hashlib.sha256()
    received_bytes = 0
    async for chunk in response.content.iter_chunked(limits.read_chunk_bytes):
        received_bytes += len(chunk)
        if received_bytes > expected_bytes:
            raise HttpsAcquisitionError("https_length_mismatch")
        digest.update(chunk)
        spool.write(chunk)
    if received_bytes != expected_bytes:
        raise HttpsAcquisitionError("https_length_mismatch")
    if digest.hexdigest() != expected_sha256:
        raise HttpsAcquisitionError("https_digest_mismatch")
    spool.seek(0)


async def _fetch_object(
    url: URL, spool: BinaryIO, expected_sha256: str, expected_bytes: int, limits: CaptureLimits, timeout_seconds: float
) -> None:
    """Own one session and at most three same-origin redirects, without retries."""
    async with AsyncExitStack() as owned:
        resolver = _StrictResolver()
        owned.push_async_callback(resolver.close)
        connector = await owned.enter_async_context(
            aiohttp.TCPConnector(resolver=resolver, use_dns_cache=False, force_close=True, ssl=True)
        )
        session = await owned.enter_async_context(
            aiohttp.ClientSession(
                connector=connector,
                connector_owner=False,
                timeout=aiohttp.ClientTimeout(total=timeout_seconds),
                trust_env=False,
                auth=None,
                cookie_jar=aiohttp.DummyCookieJar(),
                auto_decompress=False,
                headers={"Accept-Encoding": "identity"},
            )
        )
        # aiohttp otherwise transparently retries disconnected idempotent requests.
        session._retry_connection = False
        original_origin = (url.scheme, url.raw_host, url.port)
        for redirect_count in range(_MAX_REDIRECTS + 1):
            async with session.get(url, allow_redirects=False, proxy=None, ssl=True) as response:
                if response.status not in _REDIRECT_STATUSES:
                    await _copy_response(response, spool, expected_sha256, expected_bytes, limits)
                    return
                locations = response.headers.getall("Location", [])
                if redirect_count == _MAX_REDIRECTS or len(locations) != 1 or not locations[0]:
                    raise HttpsAcquisitionError("https_redirect_denied")
                location = locations[0]
                if "#" in location or any(ord(char) <= 32 or ord(char) == 127 for char in location):
                    raise HttpsAcquisitionError("https_redirect_denied")
                url = _https_url(location, relative_to=url)
                if (url.scheme, url.raw_host, url.port) != original_origin:
                    raise HttpsAcquisitionError("https_redirect_denied")


async def acquire_https(
    url: str,
    source_stream: SourceStream,
    *,
    source_snapshot_token: str,
    expected_sha256: str,
    expected_bytes: int,
    limits: CaptureLimits,
    timeout_seconds: float,
) -> SealedCapture:
    """Acquire one expected HTTPS object and return its existing bounded capture.

    The deadline covers all redirects and body reads. Synchronous, byte-bounded
    sealing is checked against that same deadline before a result can escape.
    Caller cancellation propagates after owned resources close. Failures expose
    stable codes only, suppressing URL, query, body and underlying driver errors.
    """
    try:
        _validate_identity(expected_sha256, expected_bytes, limits, timeout_seconds)
        if (
            not isinstance(source_stream, SourceStream)
            or source_stream.format not in _FORMATS
            or source_stream.compression not in _COMPRESSIONS
        ):
            raise HttpsAcquisitionError("https_request_invalid")
        validate_snapshot_token(source_snapshot_token)
        checked_url = _https_url(url)
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout_seconds
        async with asyncio.timeout_at(deadline):
            with SpooledTemporaryFile(max_size=min(limits.maximum_compressed_bytes, 1024 * 1024), mode="w+b") as spool:
                await _fetch_object(checked_url, spool, expected_sha256, expected_bytes, limits, timeout_seconds)
                capture = capture_stream(
                    spool, source_stream, source_snapshot_token=source_snapshot_token, limits=limits
                )
                await asyncio.sleep(0)
                if loop.time() >= deadline:
                    raise TimeoutError
                return capture
    except asyncio.CancelledError:
        raise asyncio.CancelledError from None
    except TimeoutError:
        raise HttpsAcquisitionError("https_acquisition_timeout") from None
    except HttpsAcquisitionError:
        raise
    except Exception:
        raise HttpsAcquisitionError("https_acquisition_failed") from None
