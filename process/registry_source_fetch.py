# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fetch one catalogued source artifact without changing accepted source data.

The caller owns catalog verification, parsing and generation admission. A receipt
proves raw bytes and explicit edition provenance; it is not parser acceptance.
"""

from __future__ import annotations

import asyncio
import hashlib
import ipaddress
import os
import re
import tempfile
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import urlsplit

import aiohttp

from process.ptg_parts.artifacts import sha256_file
from process.ptg_parts.source_download import _PublicResolver
from process.uhc_flex_practitioner_async_safety import drain_operation
from process.url_security import assert_public_ip

MAX_ARTIFACT_BYTES = 128 * 1024 * 1024
PLAN_FINDER_MAX_ARTIFACT_BYTES = 512 * 1024 * 1024
FETCH_DEADLINE_SECONDS = 30
PLAN_FINDER_FETCH_DEADLINE_SECONDS = 120
_CHUNK_BYTES = 64 * 1024
_HOSTS_BY_SYSTEM = {"cms": {"www.cms.gov", "download.cms.gov", "downloads.cms.gov"}, "naic": {"content.naic.org"}}
_PLAN_FINDER_SOURCE = ("cms", "plan-finder")
_DIGEST = re.compile(r"[0-9a-f]{64}\Z")


class RegistrySourceFetchError(ValueError):
    """A sanitized failure; no URL, response body or credentials are retained."""

    def __init__(self):
        super().__init__("Registry source artifact is unavailable or invalid")


def _text(value, limit):
    return type(value) is str and value == value.strip() and 0 < len(value) <= limit and value.isprintable()


def _source_url(value, source_system):
    if not _text(value, 2048):
        raise RegistrySourceFetchError()
    try:
        parsed = urlsplit(value)
    except ValueError:
        raise RegistrySourceFetchError() from None
    if (
        parsed.scheme != "https"
        or parsed.netloc not in _HOSTS_BY_SYSTEM.get(source_system, set())
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or not parsed.path.startswith("/")
        or "\\" in value
        or any(part in {".", ".."} for part in parsed.path.split("/"))
    ):
        raise RegistrySourceFetchError()


@dataclass(frozen=True)
class RegistrySourceFetchSpec:
    source_system: str
    source_id: str
    edition_id: str
    source_url: str
    parser_version: str
    reporting_year: int | None
    published_at: datetime | None
    max_bytes: int
    expected_sha256: str | None = None

    def __post_init__(self):
        for name, limit in (("source_system", 64), ("source_id", 128), ("edition_id", 128), ("parser_version", 128)):
            if not _text(getattr(self, name), limit):
                raise RegistrySourceFetchError()
        _source_url(self.source_url, self.source_system)
        if self.reporting_year is not None and (
            type(self.reporting_year) is not int or not 1900 <= self.reporting_year <= 2100
        ):
            raise RegistrySourceFetchError()
        if self.published_at is not None and (
            type(self.published_at) is not datetime or self.published_at.utcoffset() is None
        ):
            raise RegistrySourceFetchError()
        maximum = (
            PLAN_FINDER_MAX_ARTIFACT_BYTES
            if (self.source_system, self.source_id) == _PLAN_FINDER_SOURCE
            else MAX_ARTIFACT_BYTES
        )
        if type(self.max_bytes) is not int or not 1 <= self.max_bytes <= maximum:
            raise RegistrySourceFetchError()
        if self.expected_sha256 is not None and (
            type(self.expected_sha256) is not str or not _DIGEST.fullmatch(self.expected_sha256)
        ):
            raise RegistrySourceFetchError()


def _validator(value, *, date=False):
    if value is None:
        return None
    if not _text(value, 1024):
        raise RegistrySourceFetchError()
    if date:
        try:
            if parsedate_to_datetime(value).utcoffset() is None:
                raise RegistrySourceFetchError()
        except ValueError, TypeError:
            raise RegistrySourceFetchError() from None
    return value


@dataclass(frozen=True)
class RegistrySourceFetchReceipt:
    spec: RegistrySourceFetchSpec
    artifact_sha256: str
    artifact_bytes: int
    retrieved_at: datetime
    etag: str | None = None
    last_modified: str | None = None

    def __post_init__(self):
        if (
            type(self.spec) is not RegistrySourceFetchSpec
            or type(self.artifact_sha256) is not str
            or not _DIGEST.fullmatch(self.artifact_sha256)
        ):
            raise RegistrySourceFetchError()
        if type(self.artifact_bytes) is not int or not 1 <= self.artifact_bytes <= self.spec.max_bytes:
            raise RegistrySourceFetchError()
        if type(self.retrieved_at) is not datetime or self.retrieved_at.utcoffset() is None:
            raise RegistrySourceFetchError()
        _validator(self.etag)
        _validator(self.last_modified, date=True)


@dataclass(frozen=True)
class RegistrySourceFetchResult:
    receipt: RegistrySourceFetchReceipt
    artifact_path: Path
    unchanged: bool


@dataclass(frozen=True)
class RegistrySourceLoopbackTransport:
    """Explicit native-test seam; never register this through runtime config."""

    url: str

    def __post_init__(self):
        try:
            parsed = urlsplit(self.url)
            port = parsed.port
        except ValueError, TypeError:
            raise RegistrySourceFetchError() from None
        if (
            parsed.scheme != "http"
            or parsed.hostname not in {"127.0.0.1", "::1"}
            or port is None
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
            or not _text(self.url, 2048)
        ):
            raise RegistrySourceFetchError()


class _StrictResolver(_PublicResolver):
    async def resolve(self, host, port=0, family=0):
        """Reject private answers even when legacy local fetching is enabled."""
        addresses = await super().resolve(host, port, family)
        for address in addresses:
            assert_public_ip(ipaddress.ip_address(address["host"]), strict=True)
        return addresses


def _artifact_path(artifact_dir, digest):
    return Path(artifact_dir) / (digest + ".artifact")


def _verified_previous(spec, artifact_dir, previous):
    if previous is None:
        return None
    if type(previous) is not RegistrySourceFetchReceipt:
        raise RegistrySourceFetchError()
    if previous.spec != spec:
        return None
    path = _artifact_path(artifact_dir, previous.artifact_sha256)
    if path.is_symlink() or not path.is_file() or path.stat().st_size > spec.max_bytes:
        return None
    if sha256_file(path) != (previous.artifact_sha256, previous.artifact_bytes):
        return None
    if spec.expected_sha256 is not None and previous.artifact_sha256 != spec.expected_sha256:
        return None
    return previous


def _request_headers(previous):
    headers_by_name = {"Accept-Encoding": "identity", "User-Agent": "Registry-Source-Fetch/1.0"}
    if previous is not None:
        if previous.etag is not None:
            headers_by_name["If-None-Match"] = previous.etag
        elif previous.last_modified is not None:
            headers_by_name["If-Modified-Since"] = previous.last_modified
    return headers_by_name


def _not_modified(response, previous, artifact_dir):
    if previous is None or not (previous.etag or previous.last_modified):
        raise RegistrySourceFetchError()
    if _verified_previous(previous.spec, artifact_dir, previous) is None:
        raise RegistrySourceFetchError()
    for name, expected in (("ETag", previous.etag), ("Last-Modified", previous.last_modified)):
        actual = response.headers.get(name)
        if actual is not None and actual != expected:
            raise RegistrySourceFetchError()
    receipt = replace(previous, retrieved_at=datetime.now(timezone.utc))
    return RegistrySourceFetchResult(receipt, _artifact_path(artifact_dir, receipt.artifact_sha256), True)


async def _stage_response(response, spec, artifact_dir):
    if response.status != 200 or response.headers.get("Content-Encoding", "identity").lower() != "identity":
        raise RegistrySourceFetchError()
    if response.content_length is not None and response.content_length > spec.max_bytes:
        raise RegistrySourceFetchError()
    digest = hashlib.sha256()
    byte_count = 0
    descriptor, name = tempfile.mkstemp(prefix=".registry-fetch-", dir=artifact_dir)
    stage = Path(name)
    try:
        with os.fdopen(descriptor, "wb") as output:
            async for chunk in response.content.iter_chunked(_CHUNK_BYTES):
                byte_count += len(chunk)
                if byte_count > spec.max_bytes:
                    raise RegistrySourceFetchError()
                output.write(chunk)
                digest.update(chunk)
            output.flush()
            os.fsync(output.fileno())
        artifact_sha256 = digest.hexdigest()
        if not byte_count or (spec.expected_sha256 is not None and artifact_sha256 != spec.expected_sha256):
            raise RegistrySourceFetchError()
        receipt = RegistrySourceFetchReceipt(
            spec,
            artifact_sha256,
            byte_count,
            datetime.now(timezone.utc),
            _validator(response.headers.get("ETag")),
            _validator(response.headers.get("Last-Modified"), date=True),
        )
        final_path = _artifact_path(artifact_dir, artifact_sha256)
        if final_path.is_symlink():
            raise RegistrySourceFetchError()
        if (
            final_path.is_file()
            and final_path.stat().st_size == byte_count
            and await drain_operation(asyncio.to_thread(sha256_file, final_path), preserve_cancellation=True)
            == (artifact_sha256, byte_count)
        ):
            return receipt, final_path
        os.replace(stage, final_path)
        return receipt, final_path
    finally:
        stage.unlink(missing_ok=True)


async def _fetch(spec, artifact_dir, previous, test_transport):
    Path(artifact_dir).mkdir(parents=True, exist_ok=True)
    verified = await drain_operation(
        asyncio.to_thread(_verified_previous, spec, artifact_dir, previous), preserve_cancellation=True
    )
    connector = aiohttp.TCPConnector(resolver=_StrictResolver()) if test_transport is None else aiohttp.TCPConnector()
    timeout = aiohttp.ClientTimeout(total=_fetch_deadline(spec), connect=5, sock_read=10)
    url = spec.source_url if test_transport is None else test_transport.url
    async with aiohttp.ClientSession(
        connector=connector, timeout=timeout, trust_env=False, auto_decompress=False
    ) as session:
        session._retry_connection = False
        async with session.get(url, headers=_request_headers(verified), allow_redirects=False, ssl=True) as response:
            if response.status == 304:
                return await drain_operation(
                    asyncio.to_thread(_not_modified, response, verified, artifact_dir), preserve_cancellation=True
                )
            receipt, path = await _stage_response(response, spec, artifact_dir)
    is_unchanged = verified is not None and receipt.artifact_sha256 == verified.artifact_sha256
    return RegistrySourceFetchResult(receipt, path, is_unchanged)


def _fetch_deadline(spec):
    return (
        PLAN_FINDER_FETCH_DEADLINE_SECONDS
        if (spec.source_system, spec.source_id) == _PLAN_FINDER_SOURCE
        else FETCH_DEADLINE_SECONDS
    )


async def fetch_registry_source(
    spec: RegistrySourceFetchSpec,
    artifact_dir: str | Path,
    *,
    previous: RegistrySourceFetchReceipt | None = None,
    _test_transport: RegistrySourceLoopbackTransport | None = None,
) -> RegistrySourceFetchResult:
    """Perform one bounded GET; only the caller can accept or publish a generation.

    Rehash a prior local artifact before sending a conditional validator. An
    unchanged result is safe to skip only for the same complete spec, including
    parser identity. No receipt is persisted until the caller accepts parsing.
    """
    try:
        if type(spec) is not RegistrySourceFetchSpec:
            raise RegistrySourceFetchError()
        if _test_transport is not None and type(_test_transport) is not RegistrySourceLoopbackTransport:
            raise RegistrySourceFetchError()
        async with asyncio.timeout(_fetch_deadline(spec)):
            return await _fetch(spec, artifact_dir, previous, _test_transport)
    except asyncio.CancelledError:
        raise
    except Exception:
        raise RegistrySourceFetchError() from None
