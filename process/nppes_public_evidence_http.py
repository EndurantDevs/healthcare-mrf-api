# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded same-origin HTTP streaming for official NPPES artifacts."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import hashlib
import inspect
import os
from pathlib import Path
from urllib.parse import urljoin

import aiohttp

from process.nppes_public_evidence_archive import archive_error
from process.nppes_public_evidence_artifacts import canonical_cms_url
from process.control_cancel import ImportCancelledError


_CHUNK_BYTES = 1024 * 1024
_MAX_REDIRECTS = 4
_USER_AGENT = (
    "Healthporta NPPES public-evidence importer/1.0 "
    "(+https://github.com/EndurantDevs/healthcare-mrf-api)"
)
_REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})


@dataclass(frozen=True, slots=True, repr=False)
class HttpStreamResult:
    """Bounded identity and HTTP observations for one streamed response."""

    status: int
    final_url: str
    sha256: str | None
    byte_count: int | None
    etag: str | None
    last_modified: str | None


async def _invoke_cancel(callback) -> None:
    if callback is None:
        return
    callback_result = callback()
    if inspect.isawaitable(callback_result):
        await callback_result


def _request_headers(etag: str | None) -> dict[str, str]:
    headers_by_name = {
        "Accept-Encoding": "identity",
        "User-Agent": _USER_AGENT,
    }
    if etag and not etag.startswith("W/"):
        headers_by_name["If-None-Match"] = etag
    return headers_by_name


def _declared_content_length(response: object, max_bytes: int) -> int | None:
    raw_length = response.headers.get("Content-Length")
    if raw_length is None:
        return None
    if not raw_length.isascii() or not raw_length.isdigit():
        raise archive_error()
    declared_length = int(raw_length)
    if not 1 <= declared_length <= max_bytes:
        raise archive_error()
    return declared_length


async def _stream_identity_body(
    response: object,
    temporary_path: Path,
    max_bytes: int,
    cancel_check=None,
) -> tuple[str, int]:
    digest = hashlib.sha256()
    byte_count = 0
    descriptor = os.open(
        temporary_path,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL,
        0o600,
    )
    with os.fdopen(descriptor, "wb", closefd=True) as output:
        async for chunk in response.content.iter_chunked(_CHUNK_BYTES):
            await _invoke_cancel(cancel_check)
            if type(chunk) is not bytes or not chunk:
                raise archive_error()
            byte_count += len(chunk)
            if byte_count > max_bytes:
                raise archive_error()
            digest.update(chunk)
            output.write(chunk)
        output.flush()
        os.fsync(output.fileno())
    return digest.hexdigest(), byte_count


def _redirect_target(
    response: object,
    current_url: str,
    redirect_count: int,
) -> str | None:
    if response.status not in _REDIRECT_STATUSES:
        return None
    if redirect_count == _MAX_REDIRECTS:
        raise archive_error()
    location = response.headers.get("Location")
    return canonical_cms_url(urljoin(current_url, location or ""))


async def _terminal_response_result(
    response: object,
    current_url: str,
    temporary_path: Path,
    max_bytes: int,
    etag: str | None,
    cancel_check=None,
) -> HttpStreamResult:
    if response.status == 304:
        return HttpStreamResult(
            status=304,
            final_url=current_url,
            sha256=None,
            byte_count=None,
            etag=response.headers.get("ETag") or etag,
            last_modified=response.headers.get("Last-Modified"),
        )
    if response.status != 200:
        raise archive_error()
    encoding = (response.headers.get("Content-Encoding") or "identity").lower()
    if encoding != "identity":
        raise archive_error()
    declared_length = _declared_content_length(response, max_bytes)
    body_sha256, byte_count = await _stream_identity_body(
        response,
        temporary_path,
        max_bytes,
        cancel_check,
    )
    if byte_count == 0 or (
        declared_length is not None and byte_count != declared_length
    ):
        raise archive_error()
    return HttpStreamResult(
        status=200,
        final_url=current_url,
        sha256=body_sha256,
        byte_count=byte_count,
        etag=response.headers.get("ETag"),
        last_modified=response.headers.get("Last-Modified"),
    )


async def _stream_official_url(
    source_url: str,
    temporary_path: Path,
    *,
    max_bytes: int,
    etag: str | None,
    cancel_check=None,
) -> HttpStreamResult:
    """Stream one canonical CMS response into a newly created bounded file."""

    timeout = aiohttp.ClientTimeout(total=4 * 60 * 60, connect=120, sock_read=600)
    current_url = canonical_cms_url(source_url)
    await _invoke_cancel(cancel_check)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        for redirect_count in range(_MAX_REDIRECTS + 1):
            await _invoke_cancel(cancel_check)
            async with session.get(
                current_url,
                headers=_request_headers(etag),
                allow_redirects=False,
            ) as response:
                redirect_target = _redirect_target(
                    response,
                    current_url,
                    redirect_count,
                )
                if redirect_target is not None:
                    current_url = redirect_target
                    continue
                return await _terminal_response_result(
                    response,
                    current_url,
                    temporary_path,
                    max_bytes,
                    etag,
                    cancel_check,
                )
    raise archive_error()


async def stream_official_url(
    source_url: str,
    temporary_path: Path,
    *,
    max_bytes: int,
    etag: str | None,
    cancel_check=None,
) -> HttpStreamResult:
    """Normalize transport and filesystem failures at the HTTP boundary."""

    try:
        stream_result = await _stream_official_url(
            source_url,
            temporary_path,
            max_bytes=max_bytes,
            etag=etag,
            cancel_check=cancel_check,
        )
    except (asyncio.CancelledError, ImportCancelledError, KeyboardInterrupt):
        raise
    except Exception:
        normalized_error = archive_error()
    else:
        return stream_result
    raise normalized_error


__all__ = ("HttpStreamResult", "stream_official_url")
