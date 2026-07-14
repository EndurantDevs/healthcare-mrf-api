# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Shared SSRF / local-file / unbounded-fetch guards for control-plane fetches.

The engine's HTTP stacks (aiohttp for PTG, urllib for MS-DRG) share scheme allowlisting,
resolved-IP blocklisting (rejecting any non-global address), per-redirect-hop
re-validation, and a streaming size cap counted on bytes actually read.
"""

from __future__ import annotations

import asyncio
import ipaddress
import os
import socket
import urllib.request
from typing import Any
from urllib.parse import urlsplit

# Streaming cap counted on bytes actually read (never on Content-Length, which a
# malicious origin controls). ``HLTHPRT_FETCH_MAX_BYTES`` overrides every caller's
# default when set; otherwise each call site supplies a path-appropriate default
# (TOC/preview fetches use a tighter cap than bulk MRF downloads).
FETCH_MAX_BYTES_ENV = "HLTHPRT_FETCH_MAX_BYTES"
FETCH_ALLOW_LOCAL_ENV = "HLTHPRT_FETCH_ALLOW_LOCAL"


class UnsafeUrlError(ValueError):
    """Raised when a caller-supplied URL is not safe to fetch server-side."""


def _allow_local() -> bool:
    return str(os.getenv(FETCH_ALLOW_LOCAL_ENV, "")).strip().lower() in {"1", "true", "yes", "on"}


def fetch_max_bytes(default: int) -> int:
    """Return the configured maximum safe response size in bytes."""
    raw = os.getenv(FETCH_MAX_BYTES_ENV)
    if raw:
        try:
            value = int(raw)
        except ValueError:
            value = 0
        if value > 0:
            return value
    return default


def assert_public_ip(ip: ipaddress._BaseAddress) -> None:
    """Reject loopback and private destination addresses."""
    if ip.is_loopback or ip.is_private:
        if _allow_local():
            return
        raise UnsafeUrlError(f"non-public IP address is not allowed: {ip}")
    # not is_global also catches ranges the flag checks miss, e.g. CGNAT 100.64.0.0/10
    if ip.is_link_local or ip.is_multicast or ip.is_reserved or ip.is_unspecified or not ip.is_global:
        raise UnsafeUrlError(f"non-public IP address is not allowed: {ip}")


def _validate_scheme_and_host(url: str) -> tuple[str, int]:
    parsed = urlsplit(str(url or "").strip())
    if parsed.scheme not in {"http", "https"}:
        raise UnsafeUrlError("only http(s) URLs are allowed")
    if not parsed.hostname:
        raise UnsafeUrlError("URL host is required")
    hostname = parsed.hostname.strip().lower()
    if not _allow_local() and (hostname in {"localhost", "0.0.0.0"} or hostname.endswith(".local")):
        raise UnsafeUrlError("local hosts are not allowed")
    return hostname, parsed.port or (443 if parsed.scheme == "https" else 80)


def _resolve_and_check(hostname: str, port: int) -> None:
    try:
        literal = ipaddress.ip_address(hostname)
    except ValueError:
        literal = None
    if literal is not None:
        assert_public_ip(literal)
        return
    infos = socket.getaddrinfo(hostname, port, type=socket.SOCK_STREAM)
    if not infos:
        raise UnsafeUrlError("URL host cannot be resolved")
    for info in infos:
        assert_public_ip(ipaddress.ip_address(info[4][0]))


def assert_safe_url_sync(url: str) -> None:
    """Validate URL scheme, host, and destination synchronously."""
    hostname, port = _validate_scheme_and_host(url)
    _resolve_and_check(hostname, port)


async def assert_safe_url(url: str) -> None:
    """Validate URL scheme, host, and destination asynchronously."""
    hostname, port = _validate_scheme_and_host(url)
    await asyncio.to_thread(_resolve_and_check, hostname, port)


class _ValidatingRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Re-validate every redirect target so a public host cannot bounce to a private one."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        """Validate a redirect target before constructing its request."""
        assert_safe_url_sync(newurl)
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def urlopen_safe(
    request: Any,
    *,
    timeout: float,
    max_bytes: int,
) -> tuple[bytes, str | None]:
    """urllib fetch with SSRF validation, per-redirect re-validation, and a byte cap."""
    url = request.full_url if isinstance(request, urllib.request.Request) else str(request)
    assert_safe_url_sync(url)
    opener = urllib.request.build_opener(_ValidatingRedirectHandler())
    with opener.open(request, timeout=timeout) as response:
        assert_safe_url_sync(response.geturl())
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = response.read(64 * 1024)
            if not chunk:
                break
            total += len(chunk)
            if max_bytes is not None and total > max_bytes:
                raise UnsafeUrlError(f"response exceeds {max_bytes} byte fetch limit")
            chunks.append(chunk)
        charset = response.headers.get_content_charset()
    return b"".join(chunks), charset
