# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Raw artifact download and JSON materialization helpers for PTG2."""

from __future__ import annotations

import asyncio
import concurrent.futures
import datetime
import gzip
import hashlib
import logging
import math
import os
import re
import sys
import tempfile
import time
import zipfile
import zlib
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlsplit

import aiohttp

from process.ptg_parts.artifact_streams import logical_artifact_identity, stream_logical_artifact
from process.ptg_parts.artifacts import (
    PTG2ArtifactStore,
    _hash_existing_file_into,
    _is_strong_etag,
    _load_completed_ranges,
    _range_sidecar_path,
    _safe_url_suffix,
    _write_completed_ranges,
    choose_reusable_raw_artifact,
    ptg2_temp_parent,
    sha256_file,
)
from process.ptg_parts.canonical import canonicalize_url, semantic_hash
from process.ptg_parts.config import (
    PTG2_DEFAULT_DOWNLOAD_TASKS,
    PTG2_DOWNLOAD_TASKS_ENV,
    PTG2_KEEP_PARTIAL_ENV,
    PTG2_RANGE_DOWNLOADS_ENV,
    _download_progress_interval_bytes,
    _download_retry_count,
    _download_retry_delay_seconds,
    _env_bool,
    _env_int,
    _range_download_chunk_bytes,
    _range_download_min_bytes,
    _range_download_tasks,
)
from process.ptg_parts.domain import (
    PTG2_ARTIFACT_RAW,
    PTG2DownloadedJob,
    PTG2HeadMetadata,
    PTG2LogicalArtifact,
    PTG2RawArtifact,
)
from process.ptg_parts.live_progress import (
    current_live_progress_context,
    reset_live_progress_context,
    set_live_progress_context,
    write_live_progress,
)
from process.ptg_parts.input_artifact_retention import (
    async_named_artifact_lock,
    bind_artifact_lease,
    current_artifact_lease_id,
    protect_artifact_path,
    protect_artifact_prefix,
    protect_existing_artifact,
    publish_artifact_file,
)
from process.ptg_parts.progress import _scale_stage_progress_pct
from process.ptg_parts.screen import _emit_screen_line
from process.url_security import UnsafeUrlError, assert_safe_url

logger = logging.getLogger(__name__)

# Default streaming cap for control-triggered PTG downloads (bulk MRF artifacts can be
# very large, so this is generous; HLTHPRT_FETCH_MAX_BYTES tightens it per deployment).
PTG2_DEFAULT_MAX_BYTES = 64 * 1024 * 1024 * 1024
_REDIRECT_STATUSES = {301, 302, 303, 307, 308}
_MAX_REDIRECTS = 10
_GZIP_INTEGRITY_CHUNK_BYTES = 8 * 1024 * 1024
_GZIP_REUSE_VALIDATE_MAX_BYTES_ENV = "HLTHPRT_PTG2_REUSE_GZIP_VALIDATE_MAX_BYTES"
_GZIP_VALIDATE_FRESH_ENV = "HLTHPRT_PTG2_VALIDATE_FRESH_GZIP"
INCOMPLETE_TLS_CHAIN_HOSTS_ENV = "HLTHPRT_INCOMPLETE_TLS_CHAIN_HOSTS"
DEFAULT_INCOMPLETE_TLS_CHAIN_HOSTS = frozenset({"api.midlandschoice.com"})
_CONTENT_RANGE_PATTERN = re.compile(r"bytes\s+(\d+)-(\d+)/(\d+)$", re.IGNORECASE)


@dataclass
class RangeDownloadProgress:
    completed_bytes: int
    next_progress_bytes: int


class _UnsafeRangeResponseError(RuntimeError):
    pass


def _validate_content_range(
    response: aiohttp.ClientResponse,
    *,
    url: str,
    expected_start: int,
    expected_end: int,
    expected_total: int,
) -> None:
    content_range = (response.headers.get("Content-Range") or "").strip()
    match = _CONTENT_RANGE_PATTERN.fullmatch(content_range)
    actual = tuple(int(value) for value in match.groups()) if match else None
    expected = (expected_start, expected_end, expected_total)
    if actual != expected:
        raise _UnsafeRangeResponseError(
            f"Range download for {url} returned Content-Range {content_range!r}, "
            f"expected 'bytes {expected_start}-{expected_end}/{expected_total}'"
        )


def _validate_range_response(
    response: aiohttp.ClientResponse,
    *,
    url: str,
    expected_start: int,
    expected_end: int,
    expected_total: int,
    expected_etag: str,
) -> None:
    if response.status != 206:
        raise _UnsafeRangeResponseError(
            f"Range download not supported for {url}: status {response.status}"
        )
    _validate_content_range(
        response,
        url=url,
        expected_start=expected_start,
        expected_end=expected_end,
        expected_total=expected_total,
    )
    response_etag = response.headers.get("ETag")
    if response_etag and response_etag != expected_etag:
        raise _UnsafeRangeResponseError(
            f"Range download for {url} changed ETag from {expected_etag!r} to {response_etag!r}"
        )


def _reset_partial_download(path: Path) -> None:
    path.unlink(missing_ok=True)
    _range_sidecar_path(path).unlink(missing_ok=True)


def _validated_resume_offset(path: Path, *, total_bytes: int | None, etag: str | None) -> int:
    if not path.exists() or not total_bytes or not _is_strong_etag(etag):
        return 0
    completed = _load_completed_ranges(
        _range_sidecar_path(path),
        total_bytes=total_bytes,
        etag=etag,
    )
    size = path.stat().st_size
    if 0 < size < total_bytes and completed == {(0, size - 1)}:
        return size
    return 0


def _incomplete_tls_chain_hosts() -> set[str]:
    raw = os.getenv(INCOMPLETE_TLS_CHAIN_HOSTS_ENV)
    if raw is None:
        return set(DEFAULT_INCOMPLETE_TLS_CHAIN_HOSTS)
    return {
        value.strip().lower()
        for value in re.split(r"[, ]+", raw)
        if value.strip()
    }


def _request_ssl_kwargs(url: str | None) -> dict[str, object]:
    parsed = urlsplit(str(url or "").strip())
    host = (parsed.hostname or "").lower()
    if parsed.scheme == "https" and host in _incomplete_tls_chain_hosts():
        return {"ssl": False}
    return {}


def _expected_gzip_artifact(url: str, path: str | Path) -> bool:
    url_suffixes = {suffix.lower() for suffix in Path(urlsplit(url).path).suffixes}
    return ".gz" in url_suffixes or Path(path).suffix.lower() == ".gz"


def _gzip_magic_error(url: str, path: str | Path) -> str | None:
    if not _expected_gzip_artifact(url, path):
        return None
    artifact_path = Path(path)
    try:
        with artifact_path.open("rb") as fp:
            magic = fp.read(2)
    except OSError as exc:
        return f"raw gzip artifact is not readable: {exc}"
    if magic != b"\x1f\x8b":
        return f"raw artifact for gzip URL does not have a gzip header: {artifact_path}"
    return None


def _gzip_reuse_validate_max_bytes() -> int:
    return _env_int(_GZIP_REUSE_VALIDATE_MAX_BYTES_ENV, 2 * 1024 * 1024 * 1024)


def _gzip_integrity_error(url: str, path: str | Path, *, max_bytes: int | None = None) -> str | None:
    magic_error = _gzip_magic_error(url, path)
    if magic_error:
        return magic_error
    if not _expected_gzip_artifact(url, path):
        return None
    artifact_path = Path(path)
    if max_bytes is not None and max_bytes >= 0:
        try:
            if artifact_path.stat().st_size > max_bytes:
                return None
        except OSError as exc:
            return f"raw gzip artifact is not readable: {exc}"
    try:
        with gzip.open(artifact_path, "rb") as fp:
            for chunk in iter(lambda: fp.read(_GZIP_INTEGRITY_CHUNK_BYTES), b""):
                if not chunk:
                    break
    except (EOFError, OSError, zlib.error) as exc:
        return f"raw gzip artifact failed integrity check: {exc}"
    return None


async def _open_validated_request(
    session: aiohttp.ClientSession,
    method: str,
    url: str,
    **kwargs,
) -> aiohttp.ClientResponse:
    current_url = str(url)
    current_method = method.upper()
    for _ in range(_MAX_REDIRECTS + 1):
        await assert_safe_url(current_url)
        request_kwargs = dict(kwargs)
        request_kwargs.update(_request_ssl_kwargs(current_url))
        response = await session.request(
            current_method,
            current_url,
            allow_redirects=False,
            **request_kwargs,
        )
        if response.status not in _REDIRECT_STATUSES:
            await assert_safe_url(str(response.url))
            return response
        location = response.headers.get("Location")
        response.release()
        if not location:
            raise RuntimeError(f"redirect target is missing for {current_url}")
        next_url = urljoin(str(response.url), location)
        await assert_safe_url(next_url)
        current_url = next_url
        if response.status == 303 and current_method != "HEAD":
            current_method = "GET"
            kwargs.pop("data", None)
            kwargs.pop("json", None)
    raise RuntimeError(f"too many redirects for {url}")


@asynccontextmanager
async def _validated_request(
    session: aiohttp.ClientSession,
    method: str,
    url: str,
    **kwargs,
):
    response = await _open_validated_request(session, method, url, **kwargs)
    try:
        yield response
    finally:
        response.release()


def _format_eta_seconds(seconds: float | None) -> str:
    if seconds is None or seconds < 0 or not math.isfinite(seconds):
        return "unknown"
    return f"{seconds:.0f}"


def _emit_download_progress(
    *,
    url: str,
    bytes_read: int,
    total_bytes: int | None,
    started_at: float,
    done: bool,
) -> None:
    elapsed = max(time.monotonic() - started_at, 0.0)
    mib_s = (bytes_read / (1024 * 1024)) / elapsed if elapsed > 0 else 0.0
    if total_bytes and total_bytes > 0:
        percent = min((bytes_read / total_bytes) * 100, 100.0)
        eta = ((total_bytes - bytes_read) / (1024 * 1024)) / mib_s if mib_s > 0 and total_bytes > bytes_read else 0.0
        total_text = str(total_bytes)
        eta_text = _format_eta_seconds(eta)
    else:
        percent = 0.0
        total_text = "unknown"
        eta_text = "unknown"
    line = (
        "PTG2_DOWNLOAD_PROGRESS"
        f"\turl={url}"
        f"\tbytes={bytes_read}"
        f"\ttotal_bytes={total_text}"
        f"\tpercent={percent:.2f}"
        f"\tmib_s={mib_s:.2f}"
        f"\telapsed_seconds={elapsed:.0f}"
        f"\teta_seconds={eta_text}"
        f"\tdone={'true' if done else 'false'}"
    )
    _emit_screen_line(line, stderr=True)
    logger.info(line)
    live_context = current_live_progress_context()
    overall_pct = _scale_stage_progress_pct(
        percent,
        live_context.get("overall_progress_start_pct"),
        live_context.get("overall_progress_end_pct"),
    )
    pct = overall_pct if overall_pct is not None else percent
    phase_detail = (
        f"download {percent:.2f}% "
        f"({bytes_read / (1024 ** 2):.1f} MiB/{(total_bytes or 0) / (1024 ** 2):.1f} MiB)"
        if total_bytes and total_bytes > 0
        else f"downloaded {bytes_read / (1024 ** 2):.1f} MiB"
    )
    write_live_progress(
        phase="download",
        unit="bytes",
        done=bytes_read,
        total=total_bytes,
        pct=pct,
        phase_pct=percent,
        rate={"mib_s": mib_s},
        eta_seconds=eta if total_bytes and total_bytes > 0 else None,
        message=f"downloading {_safe_download_label(url)}",
        detail=phase_detail,
        label=url,
    )


def _safe_download_label(url: str) -> str:
    parsed = urlsplit(str(url))
    if parsed.scheme and parsed.netloc:
        tail = parsed.path.rsplit("/", 1)[-1]
        return f"{parsed.netloc}/{tail}" if tail else parsed.netloc
    return str(url)[:128]


def _progress_job_index(job: dict[str, Any]) -> int:
    try:
        return max(int(job.get("_ptg_progress_index") or 0), 0)
    except (TypeError, ValueError):
        return 0


def _progress_job_total(job: dict[str, Any], default: int) -> int:
    try:
        return max(int(job.get("_ptg_progress_total") or default), 1)
    except (TypeError, ValueError):
        return max(default, 1)


async def fetch_head_metadata(url: str, timeout_seconds: int = 30) -> PTG2HeadMetadata:
    """Return safe HTTP HEAD metadata, tolerating unavailable HEAD requests."""
    await assert_safe_url(url)
    timeout = aiohttp.ClientTimeout(total=timeout_seconds, connect=min(timeout_seconds, 10))
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with _validated_request(session, "HEAD", url) as response:
                headers = response.headers
                length = headers.get("Content-Length")
                return PTG2HeadMetadata(
                    url=str(response.url),
                    status=response.status,
                    etag=headers.get("ETag"),
                    content_length=int(length) if length and length.isdigit() else None,
                    last_modified=headers.get("Last-Modified"),
                    content_encoding=headers.get("Content-Encoding"),
                    content_type=headers.get("Content-Type"),
                    supports_head=response.status < 400,
                )
    except (aiohttp.ClientError, asyncio.TimeoutError):
        return PTG2HeadMetadata(url=url, supports_head=False)


async def _probe_http_range_support(
    url: str,
    etag: str | None = None,
) -> tuple[bool, int | None, str | None, str | None]:
    await assert_safe_url(url)
    timeout = aiohttp.ClientTimeout(total=60, connect=30, sock_read=30)
    validator = etag if _is_strong_etag(etag) else None
    headers_by_name = {"Range": "bytes=0-0"}
    if validator:
        headers_by_name["If-Match"] = validator
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with _validated_request(session, "GET", url, headers=headers_by_name) as response:
                if response.status != 206:
                    return False, None, None, None
                content_range = response.headers.get("Content-Range") or ""
                match = _CONTENT_RANGE_PATTERN.fullmatch(content_range.strip())
                if not match:
                    return False, None, None, None
                range_start = int(match.group(1))
                range_end = int(match.group(2))
                range_total = int(match.group(3))
                if (range_start, range_end) != (0, 0) or len(await response.content.read()) != 1:
                    return False, None, None, None
                response_etag = response.headers.get("ETag")
                if validator and response_etag and response_etag != validator:
                    return False, None, None, None
                strong_validator = validator or (response_etag if _is_strong_etag(response_etag) else None)
                return True, range_total, strong_validator, str(response.url)
    except Exception:
        return False, None, None, None


async def _download_raw_artifact_ranges(
    *,
    url: str,
    partial_path: Path,
    total_bytes: int,
    etag: str | None,
    max_bytes: int | None,
    started_at: float,
) -> bool:
    if not _is_strong_etag(etag):
        raise RuntimeError(f"Range download for {url} requires a strong ETag")
    if max_bytes is not None and total_bytes > max_bytes:
        raise RuntimeError(f"PTG2 max-bytes guard exceeded for {url}")
    chunk_size = _range_download_chunk_bytes()
    ranges = [
        (start, min(start + chunk_size - 1, total_bytes - 1))
        for start in range(0, total_bytes, chunk_size)
    ]
    sidecar_path = _range_sidecar_path(partial_path)
    completed = _load_completed_ranges(sidecar_path, total_bytes=total_bytes, etag=etag)
    if partial_path.exists() and partial_path.stat().st_size > total_bytes:
        partial_path.unlink(missing_ok=True)
        sidecar_path.unlink(missing_ok=True)
        completed = set()
    partial_path.parent.mkdir(parents=True, exist_ok=True)
    with open(partial_path, "ab") as fp:
        fp.truncate(total_bytes)
    completed_bytes = sum(end - start + 1 for start, end in completed)
    next_progress_bytes = _download_progress_interval_bytes()
    while completed_bytes >= next_progress_bytes:
        next_progress_bytes += _download_progress_interval_bytes()
    progress = RangeDownloadProgress(completed_bytes, next_progress_bytes)
    lock = asyncio.Lock()
    timeout = aiohttp.ClientTimeout(total=None, connect=60, sock_read=600)
    pending_ranges = [item for item in ranges if item not in completed]
    async def fetch_range(session: aiohttp.ClientSession, item: tuple[int, int]) -> None:
        start, end = item
        headers = {"Range": f"bytes={start}-{end}"}
        if etag:
            headers["If-Match"] = etag
        expected_length = end - start + 1
        received = 0
        counted = 0
        completed_ok = False
        try:
            async with _validated_request(session, "GET", url, headers=headers) as response:
                _validate_range_response(
                    response,
                    url=url,
                    expected_start=start,
                    expected_end=end,
                    expected_total=total_bytes,
                    expected_etag=str(etag),
                )
                offset = start
                fd = os.open(partial_path, os.O_WRONLY)
                try:
                    async for chunk in response.content.iter_chunked(1024 * 1024):
                        if not chunk:
                            continue
                        os.pwrite(fd, chunk, offset)
                        offset += len(chunk)
                        received += len(chunk)
                        async with lock:
                            progress.completed_bytes += len(chunk)
                            counted += len(chunk)
                            if progress.completed_bytes >= progress.next_progress_bytes:
                                _emit_download_progress(
                                    url=url,
                                    bytes_read=min(progress.completed_bytes, total_bytes),
                                    total_bytes=total_bytes,
                                    started_at=started_at,
                                    done=False,
                                )
                                interval = _download_progress_interval_bytes()
                                while progress.completed_bytes >= progress.next_progress_bytes:
                                    progress.next_progress_bytes += interval
                finally:
                    os.close(fd)
                if received != expected_length:
                    raise RuntimeError(
                        f"Range download for {url} returned {received} bytes, expected {expected_length}"
                    )
                completed_ok = True
                async with lock:
                    completed.add(item)
                    _write_completed_ranges(sidecar_path, total_bytes=total_bytes, etag=etag, completed=completed)
        finally:
            if not completed_ok and counted:
                async with lock:
                    progress.completed_bytes = max(0, progress.completed_bytes - counted)
                    while progress.next_progress_bytes > _download_progress_interval_bytes() and (
                        progress.completed_bytes < progress.next_progress_bytes - _download_progress_interval_bytes()
                    ):
                        progress.next_progress_bytes -= _download_progress_interval_bytes()

    semaphore = asyncio.Semaphore(_range_download_tasks())

    async def bounded_fetch(session: aiohttp.ClientSession, item: tuple[int, int]) -> None:
        """Fetch one byte range while applying retry and concurrency limits."""
        async with semaphore:
            retries = _download_retry_count()
            for attempt in range(retries + 1):
                try:
                    await fetch_range(session, item)
                    return
                except Exception as exc:
                    if attempt >= retries:
                        raise
                    delay = _download_retry_delay_seconds() * (2 ** attempt)
                    message = (
                        f"PTG2_DOWNLOAD_RETRY url={url} range={item[0]}-{item[1]} "
                        f"attempt={attempt + 1} next_attempt={attempt + 2} delay_seconds={delay:.2f} error={exc}"
                    )
                    _emit_screen_line(message, stderr=True)
                    logger.debug(message)
                    if delay > 0:
                        await asyncio.sleep(delay)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        range_tasks = [asyncio.create_task(bounded_fetch(session, item)) for item in pending_ranges]
        try:
            await asyncio.gather(*range_tasks)
        except BaseException:
            for range_task in range_tasks:
                range_task.cancel()
            await asyncio.gather(*range_tasks, return_exceptions=True)
            raise
    sidecar_path.unlink(missing_ok=True)
    _emit_download_progress(
        url=url,
        bytes_read=total_bytes,
        total_bytes=total_bytes,
        started_at=started_at,
        done=False,
    )
    return True


@dataclass
class _SingleGetDownloadState:
    path: Path
    digest: Any
    byte_count: int
    total_bytes: int | None
    validator: str | None
    last_modified: str | None
    next_progress_bytes: int


class _DownloadSizeLimitError(RuntimeError):
    pass


def _single_get_download_state(path: Path, head: PTG2HeadMetadata) -> _SingleGetDownloadState:
    validator = head.etag if _is_strong_etag(head.etag) else None
    resume_offset = _validated_resume_offset(
        path,
        total_bytes=head.content_length,
        etag=validator,
    )
    if resume_offset == 0:
        _reset_partial_download(path)
    digest = hashlib.sha256()
    byte_count = _hash_existing_file_into(path, digest) if resume_offset else 0
    next_progress_bytes = _download_progress_interval_bytes()
    while byte_count >= next_progress_bytes:
        next_progress_bytes += _download_progress_interval_bytes()
    return _SingleGetDownloadState(
        path=path,
        digest=digest,
        byte_count=byte_count,
        total_bytes=head.content_length,
        validator=validator,
        last_modified=head.last_modified if resume_offset else None,
        next_progress_bytes=next_progress_bytes,
    )


def _can_resume_single_get_download(state: _SingleGetDownloadState) -> bool:
    return bool(
        state.byte_count
        and state.total_bytes
        and state.byte_count < state.total_bytes
        and _is_strong_etag(state.validator)
    )


def _prepare_single_get_response(
    response: aiohttp.ClientResponse,
    *,
    state: _SingleGetDownloadState,
    url: str,
    is_resume_request: bool,
) -> str:
    if is_resume_request:
        if response.status != 206:
            raise _UnsafeRangeResponseError(
                f"Resume download not supported for {url}: status {response.status}"
            )
        _validate_content_range(
            response,
            url=url,
            expected_start=state.byte_count,
            expected_end=int(state.total_bytes) - 1,
            expected_total=int(state.total_bytes),
        )
        returned_etag = response.headers.get("ETag")
        if returned_etag and returned_etag != state.validator:
            raise _UnsafeRangeResponseError(
                f"Resume download for {url} changed ETag from {state.validator!r} to {returned_etag!r}"
            )
        return "ab"

    if response.status == 206:
        raise _UnsafeRangeResponseError(f"Full download for {url} unexpectedly returned partial content")
    content_length = response.headers.get("Content-Length")
    state.total_bytes = int(content_length) if content_length and content_length.isdigit() else None
    returned_etag = response.headers.get("ETag")
    state.validator = returned_etag if _is_strong_etag(returned_etag) else None
    state.last_modified = response.headers.get("Last-Modified")
    state.digest = hashlib.sha256()
    state.byte_count = 0
    state.next_progress_bytes = _download_progress_interval_bytes()
    return "wb"


async def _stream_single_get_response(
    response: aiohttp.ClientResponse,
    *,
    state: _SingleGetDownloadState,
    url: str,
    file_mode: str,
    max_bytes: int | None,
    started_at: float,
) -> None:
    if max_bytes is not None and state.total_bytes is not None and state.total_bytes > max_bytes:
        raise _DownloadSizeLimitError(f"PTG2 max-bytes guard exceeded for {url}")
    with open(state.path, file_mode) as output_fp:
        async for chunk in response.content.iter_chunked(1024 * 1024):
            if not chunk:
                continue
            state.byte_count += len(chunk)
            if max_bytes is not None and state.byte_count > max_bytes:
                raise _DownloadSizeLimitError(f"PTG2 max-bytes guard exceeded for {url}")
            state.digest.update(chunk)
            output_fp.write(chunk)
            if state.byte_count >= state.next_progress_bytes:
                _emit_download_progress(
                    url=url,
                    bytes_read=state.byte_count,
                    total_bytes=state.total_bytes,
                    started_at=started_at,
                    done=False,
                )
                while state.byte_count >= state.next_progress_bytes:
                    state.next_progress_bytes += _download_progress_interval_bytes()


async def _run_single_get_attempt(
    *,
    url: str,
    state: _SingleGetDownloadState,
    max_bytes: int | None,
    started_at: float,
    timeout: aiohttp.ClientTimeout,
) -> None:
    is_resume_request = _can_resume_single_get_download(state)
    request_headers_by_name = None
    if is_resume_request:
        request_headers_by_name = {
            "Range": f"bytes={state.byte_count}-",
            "If-Match": str(state.validator),
        }
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with _validated_request(session, "GET", url, headers=request_headers_by_name) as response:
            response.raise_for_status()
            file_mode = _prepare_single_get_response(
                response,
                state=state,
                url=url,
                is_resume_request=is_resume_request,
            )
            await _stream_single_get_response(
                response,
                state=state,
                url=url,
                file_mode=file_mode,
                max_bytes=max_bytes,
                started_at=started_at,
            )
    if state.total_bytes is not None and state.byte_count != state.total_bytes:
        raise RuntimeError(
            f"Download for {url} ended at {state.byte_count} bytes, expected {state.total_bytes}"
        )
    _range_sidecar_path(state.path).unlink(missing_ok=True)


def _preserve_single_get_partial(state: _SingleGetDownloadState, error: Exception) -> None:
    invalid_range = isinstance(error, _UnsafeRangeResponseError)
    rejected_validator = isinstance(error, aiohttp.ClientResponseError) and error.status in {412, 416}
    has_complete_prefix = bool(
        state.byte_count
        and state.total_bytes
        and state.byte_count < state.total_bytes
        and _is_strong_etag(state.validator)
        and state.path.exists()
        and state.path.stat().st_size == state.byte_count
    )
    if has_complete_prefix and not invalid_range and not rejected_validator and not isinstance(error, _DownloadSizeLimitError):
        _write_completed_ranges(
            _range_sidecar_path(state.path),
            total_bytes=int(state.total_bytes),
            etag=state.validator,
            completed={(0, state.byte_count - 1)},
        )
        return
    _reset_partial_download(state.path)
    state.digest = hashlib.sha256()
    state.byte_count = 0
    state.total_bytes = None
    state.validator = None
    state.last_modified = None
    state.next_progress_bytes = _download_progress_interval_bytes()


async def _download_raw_artifact_single_get(
    *,
    url: str,
    path: Path,
    head: PTG2HeadMetadata,
    max_bytes: int | None,
    started_at: float,
) -> tuple[Any, int, str | None, int | None, str | None]:
    state = _single_get_download_state(path, head)
    timeout = aiohttp.ClientTimeout(total=None, connect=60, sock_read=600)
    retries = _download_retry_count()
    for attempt in range(retries + 1):
        try:
            await _run_single_get_attempt(
                url=url,
                state=state,
                max_bytes=max_bytes,
                started_at=started_at,
                timeout=timeout,
            )
            return state.digest, state.byte_count, state.validator, state.total_bytes, state.last_modified
        except UnsafeUrlError:
            raise
        except _DownloadSizeLimitError as exc:
            _preserve_single_get_partial(state, exc)
            raise
        except Exception as exc:
            _preserve_single_get_partial(state, exc)
            if attempt >= retries:
                raise
            delay = _download_retry_delay_seconds() * (2 ** attempt)
            message = (
                f"PTG2_DOWNLOAD_RETRY url={url} bytes={state.byte_count} "
                f"attempt={attempt + 1} next_attempt={attempt + 2} "
                f"delay_seconds={delay:.2f} error={exc}"
            )
            _emit_screen_line(message, stderr=True)
            logger.debug(message)
            if delay > 0:
                await asyncio.sleep(delay)
    raise RuntimeError(f"Download retries exhausted for {url}")


async def download_raw_artifact(
    url: str,
    store: PTG2ArtifactStore | None = None,
    reuse_raw_artifacts: bool = True,
    max_bytes: int | None = None,
    keep_partial_artifacts: bool | None = None,
) -> PTG2RawArtifact:
    """Download or reuse one raw artifact under a URL-scoped cross-process lock."""

    store = store or PTG2ArtifactStore()
    await assert_safe_url(url)
    canonical_url = canonicalize_url(url)
    download_key = semantic_hash(canonical_url, domain="ptg2_retained_download_lock")
    async with async_named_artifact_lock(store, "download", download_key):
        return await _download_raw_artifact_locked(
            url,
            store=store,
            canonical_url=canonical_url,
            reuse_raw_artifacts=reuse_raw_artifacts,
            max_bytes=max_bytes,
            keep_partial_artifacts=keep_partial_artifacts,
        )


async def _download_raw_artifact_locked(
    url: str,
    *,
    store: PTG2ArtifactStore,
    canonical_url: str,
    reuse_raw_artifacts: bool,
    max_bytes: int | None,
    keep_partial_artifacts: bool | None,
) -> PTG2RawArtifact:
    """Run one raw download after this canonical URL has been serialized."""

    keep_partials = _env_bool(PTG2_KEEP_PARTIAL_ENV, True) if keep_partial_artifacts is None else keep_partial_artifacts
    head = await fetch_head_metadata(url)
    progress_started_at = time.monotonic()
    progress_interval_bytes = _download_progress_interval_bytes()
    next_progress_bytes = progress_interval_bytes
    validate_downloaded_gzip = _env_bool(_GZIP_VALIDATE_FRESH_ENV, False)
    if reuse_raw_artifacts:
        candidates = store.find_candidates(canonical_url)
        for reuse_candidate in candidates:
            reuse_uri = reuse_candidate.get("raw_storage_uri") or reuse_candidate.get("storage_uri")
            if not reuse_uri:
                continue
            reuse_path = store.path_from_uri(str(reuse_uri))
            try:
                protect_existing_artifact(store, reuse_path)
            except ValueError:
                # A migrated external path is outside this collector's authority.
                continue
        candidate, mode = choose_reusable_raw_artifact(candidates, head, store=store)
        if candidate is not None and mode is not None:
            candidate_uri = candidate.get("raw_storage_uri") or candidate.get("storage_uri")
            candidate_path = store.path_from_uri(candidate_uri)
            try:
                protected = protect_existing_artifact(store, candidate_path)
            except ValueError:
                # Older manifests can point outside the current managed root. The
                # collector cannot delete those paths, so normal reuse remains safe.
                protected = candidate_path.exists()
            if not protected:
                candidate = None
                mode = None
        if candidate is not None and mode is not None:
            raw_uri = candidate.get("raw_storage_uri") or candidate.get("storage_uri")
            raw_path = store.path_from_uri(raw_uri)
            expected = str(candidate["raw_sha256"]).lower()
            actual, byte_count = sha256_file(raw_path)
            gzip_error = _gzip_integrity_error(
                url,
                raw_path,
                max_bytes=_gzip_reuse_validate_max_bytes(),
            )
            if expected and actual != expected:
                validate_downloaded_gzip = True
                store.record_manifest(
                    {
                        "artifact_kind": PTG2_ARTIFACT_RAW,
                        "canonical_url": canonical_url,
                        "raw_storage_uri": raw_uri,
                        "raw_sha256": expected,
                        "status": "corrupt",
                        "actual_sha256": actual,
                    }
                )
            elif gzip_error:
                validate_downloaded_gzip = True
                store.record_manifest(
                    {
                        "artifact_kind": PTG2_ARTIFACT_RAW,
                        "canonical_url": canonical_url,
                        "raw_storage_uri": raw_uri,
                        "raw_sha256": expected or actual,
                        "status": "corrupt",
                        "actual_sha256": actual,
                        "error": gzip_error,
                    }
                )
            else:
                _emit_download_progress(
                    url=url,
                    bytes_read=byte_count,
                    total_bytes=head.content_length if head and head.content_length else byte_count,
                    started_at=progress_started_at,
                    done=True,
                )
                return PTG2RawArtifact(
                    original_url=url,
                    canonical_url=canonical_url,
                    raw_path=str(raw_path),
                    raw_storage_uri=raw_uri,
                    raw_sha256=actual,
                    byte_count=byte_count,
                    head=head,
                    reused=True,
                    verification_mode=mode,
                    reused_from_source_file_version_id=candidate.get("source_file_version_id"),
                )

    partial_path = store.partial_path(canonical_url, suffix=_safe_url_suffix(url))
    tmp_path = partial_path if keep_partials else store.tmp_dir / f"ptg2-{os.getpid()}-{datetime.datetime.utcnow().timestamp()}.part"
    if keep_partials:
        protect_artifact_path(store, partial_path)
        protect_artifact_path(store, _range_sidecar_path(partial_path))
    digest = hashlib.sha256()
    byte_count = 0
    download_etag = head.etag
    download_content_length = head.content_length
    download_last_modified = head.last_modified
    _emit_download_progress(
        url=url,
        bytes_read=0,
        total_bytes=head.content_length if head and head.content_length else None,
        started_at=progress_started_at,
        done=False,
    )
    try:
        if str(url).startswith("file://") or (not str(url).lower().startswith(("http://", "https://")) and Path(url).exists()):
            source_path = Path(urlsplit(url).path if str(url).startswith("file://") else url)
            total_bytes = source_path.stat().st_size if source_path.exists() else None
            download_content_length = total_bytes
            with open(source_path, "rb") as src, open(tmp_path, "wb") as dst:
                for chunk in iter(lambda: src.read(1024 * 1024), b""):
                    byte_count += len(chunk)
                    if max_bytes is not None and byte_count > max_bytes:
                        raise RuntimeError(f"PTG2 max-bytes guard exceeded for {url}")
                    digest.update(chunk)
                    dst.write(chunk)
                    if byte_count >= next_progress_bytes:
                        _emit_download_progress(
                            url=url,
                            bytes_read=byte_count,
                            total_bytes=total_bytes,
                            started_at=progress_started_at,
                            done=False,
                        )
                        while byte_count >= next_progress_bytes:
                            next_progress_bytes += progress_interval_bytes
        else:
            used_range_download = False
            range_total = head.content_length if head and head.content_length else None
            if (
                _env_bool(PTG2_RANGE_DOWNLOADS_ENV, True)
                and range_total
                and range_total >= _range_download_min_bytes()
            ):
                head_etag = head.etag if _is_strong_etag(head.etag) else None
                range_supported, probed_total, probed_etag, _range_url = await _probe_http_range_support(
                    url,
                    head_etag,
                )
                if range_supported and probed_total and _is_strong_etag(probed_etag):
                    if max_bytes is not None and probed_total > max_bytes:
                        raise RuntimeError(f"PTG2 max-bytes guard exceeded for {url}")
                    try:
                        await _download_raw_artifact_ranges(
                            url=url,
                            partial_path=tmp_path,
                            total_bytes=probed_total,
                            etag=probed_etag,
                            max_bytes=max_bytes,
                            started_at=progress_started_at,
                        )
                    except UnsafeUrlError:
                        raise
                    except Exception as exc:
                        logger.warning(
                            "Falling back to a full PTG2 download after unsafe or failed ranges for %s: %s",
                            url,
                            exc,
                        )
                        _reset_partial_download(tmp_path)
                    else:
                        digest = hashlib.sha256()
                        byte_count = _hash_existing_file_into(tmp_path, digest)
                        download_etag = probed_etag
                        download_content_length = probed_total
                        download_last_modified = head.last_modified if probed_etag == head.etag else None
                        used_range_download = True
            if not used_range_download:
                (
                    digest,
                    byte_count,
                    download_etag,
                    download_content_length,
                    download_last_modified,
                ) = await _download_raw_artifact_single_get(
                    url=url,
                    path=tmp_path,
                    head=head,
                    max_bytes=max_bytes,
                    started_at=progress_started_at,
                )
        raw_sha = digest.hexdigest()
        # The digest is the complete physical identity. Omitting URL-derived
        # suffixes lets different source URLs with identical bytes share one
        # retained raw container.
        final_path = store.artifact_path(raw_sha, kind=PTG2_ARTIFACT_RAW)
        publish_artifact_file(
            store,
            tmp_path,
            final_path,
            expected_sha256=raw_sha,
        )
        actual_sha, actual_size = sha256_file(final_path)
        if actual_sha != raw_sha:
            raise RuntimeError(f"Checksum verification failed for {final_path}")
        gzip_error = (
            _gzip_integrity_error(url, final_path, max_bytes=None)
            if validate_downloaded_gzip
            else _gzip_magic_error(url, final_path)
        )
        if gzip_error:
            raw_uri = store.storage_uri(final_path)
            store.record_manifest(
                {
                    "artifact_kind": PTG2_ARTIFACT_RAW,
                    "canonical_url": canonical_url,
                    "raw_storage_uri": raw_uri,
                    "raw_sha256": actual_sha,
                    "sha256": actual_sha,
                    "status": "corrupt",
                    "actual_sha256": actual_sha,
                    "error": gzip_error,
                }
            )
            # This digest path may already be leased by another URL/import.
            # The failed observation is non-reusable; reference-aware GC owns
            # physical deletion after every valid lease has ended.
            raise RuntimeError(gzip_error)
        _emit_download_progress(
            url=url,
            bytes_read=actual_size,
            total_bytes=download_content_length or actual_size,
            started_at=progress_started_at,
            done=True,
        )
        raw_uri = store.storage_uri(final_path)
        manifest_payload = {
            "artifact_kind": PTG2_ARTIFACT_RAW,
            "canonical_url": canonical_url,
            "original_url": url,
            "raw_storage_uri": raw_uri,
            "raw_sha256": actual_sha,
            "sha256": actual_sha,
            "content_length": download_content_length or actual_size,
            "byte_count": actual_size,
            "etag": download_etag,
            "last_modified": download_last_modified,
            "status": "available",
        }
        store.record_manifest(manifest_payload)
        verified_head = PTG2HeadMetadata(
            url=head.url,
            status=head.status,
            etag=download_etag,
            content_length=download_content_length or actual_size,
            last_modified=download_last_modified,
            content_encoding=head.content_encoding,
            content_type=head.content_type,
            supports_head=head.supports_head,
        )
        return PTG2RawArtifact(
            original_url=url,
            canonical_url=canonical_url,
            raw_path=str(final_path),
            raw_storage_uri=raw_uri,
            raw_sha256=actual_sha,
            byte_count=actual_size,
            head=verified_head,
            reused=False,
            verification_mode="downloaded",
        )
    except BaseException as exc:
        if tmp_path.exists() and keep_partials:
            try:
                store.record_manifest(
                    {
                        "artifact_kind": "partial_raw",
                        "canonical_url": canonical_url,
                        "original_url": url,
                        "raw_storage_uri": store.storage_uri(partial_path),
                        "partial_sha256": digest.hexdigest(),
                        "byte_count": partial_path.stat().st_size,
                        "status": "partial",
                        "error": str(exc),
                    }
                )
            except Exception as preserve_exc:
                logger.warning("Failed to preserve partial PTG2 download %s: %s", tmp_path, preserve_exc)
        elif tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
        raise


async def materialize_json_source(
    url: str,
    output_dir: str | Path,
    reuse_raw_artifacts: bool = True,
    max_bytes: int | None = None,
    materialize_logical: bool = True,
    keep_partial_artifacts: bool | None = None,
) -> tuple[PTG2RawArtifact, PTG2LogicalArtifact]:
    """Download a source and return its raw and logical artifact identities."""
    raw_artifact = await download_raw_artifact(
        url,
        reuse_raw_artifacts=reuse_raw_artifacts,
        max_bytes=max_bytes,
        keep_partial_artifacts=keep_partial_artifacts,
    )
    logical_artifact = (
        stream_logical_artifact(raw_artifact.raw_path, output_dir=output_dir)
        if materialize_logical or _should_materialize_logical_artifact(raw_artifact.raw_path)
        else logical_artifact_identity(
            raw_artifact.raw_path,
            raw_sha256=raw_artifact.raw_sha256,
            raw_byte_count=raw_artifact.byte_count,
            allow_deferred=True,
        )
    )
    return raw_artifact, logical_artifact


def _should_materialize_logical_artifact(raw_path: str | Path) -> bool:
    try:
        return zipfile.is_zipfile(raw_path)
    except OSError:
        return False


def _retained_logical_artifact_dir(store: PTG2ArtifactStore, raw_artifact: PTG2RawArtifact) -> Path:
    digest = raw_artifact.raw_sha256
    path = store.root / "logical" / digest[:2] / digest[2:4] / digest
    path.mkdir(parents=True, exist_ok=True)
    return path


async def _retained_logical_artifact(
    store: PTG2ArtifactStore,
    raw_artifact: PTG2RawArtifact,
) -> PTG2LogicalArtifact:
    logical_dir = _retained_logical_artifact_dir(store, raw_artifact)
    protect_artifact_prefix(store, logical_dir)
    async with async_named_artifact_lock(
        store,
        "logical",
        raw_artifact.raw_sha256,
    ):
        for candidate in reversed(store.find_logical_candidates(raw_artifact.raw_sha256)):
            logical_uri = candidate.get("logical_storage_uri") or candidate.get("storage_uri")
            logical_sha256 = str(candidate.get("logical_sha256") or "")
            if not logical_uri or re.fullmatch(r"[0-9a-f]{64}", logical_sha256) is None:
                continue
            logical_path = store.path_from_uri(str(logical_uri))
            try:
                expected_size = int(candidate.get("byte_count"))
                size_matches = logical_path.stat().st_size == expected_size
            except (OSError, TypeError, ValueError):
                size_matches = False
            try:
                protected = size_matches and protect_existing_artifact(store, logical_path)
            except ValueError:
                # Ignore migrated logical records outside this managed store.
                protected = False
            if protected:
                actual_sha256, _actual_size = sha256_file(logical_path)
                if actual_sha256 != logical_sha256:
                    store.record_manifest(
                        {
                            "artifact_kind": "logical_json",
                            "raw_sha256": raw_artifact.raw_sha256,
                            "logical_sha256": logical_sha256,
                            "logical_storage_uri": store.storage_uri(logical_path),
                            "status": "corrupt",
                            "actual_sha256": actual_sha256,
                        }
                    )
                    continue
                return PTG2LogicalArtifact(
                    logical_path=str(logical_path),
                    logical_sha256=logical_sha256,
                    byte_count=expected_size,
                    compression=candidate.get("compression"),
                    member_name=candidate.get("member_name"),
                )

        with tempfile.TemporaryDirectory(
            prefix=f"logical-{raw_artifact.raw_sha256[:12]}-",
            dir=store.tmp_dir,
        ) as temporary_dir:
            staged = stream_logical_artifact(
                raw_artifact.raw_path,
                output_dir=temporary_dir,
            )
            final_path = logical_dir / f"{staged.logical_sha256}.json"
            publish_artifact_file(
                store,
                staged.logical_path,
                final_path,
                expected_sha256=staged.logical_sha256,
            )
        retained = replace(staged, logical_path=str(final_path))
        store.record_manifest(
            {
                "artifact_kind": "logical_json",
                "raw_sha256": raw_artifact.raw_sha256,
                "logical_sha256": retained.logical_sha256,
                "logical_storage_uri": store.storage_uri(final_path),
                "byte_count": retained.byte_count,
                "compression": retained.compression,
                "member_name": retained.member_name,
                "status": "available",
            }
        )
        return retained


def _materialize_json_source_from_facade():
    ptg_module = sys.modules.get("process.ptg")
    return getattr(ptg_module, "materialize_json_source", materialize_json_source)


def _download_ptg_job_artifact_sync_from_facade(job: dict[str, object], **kwargs) -> PTG2DownloadedJob:
    live_progress_context = kwargs.pop("live_progress_context", None)
    artifact_lease_id = kwargs.pop("artifact_lease_id", None)
    ptg_module = sys.modules.get("process.ptg")
    downloader = getattr(ptg_module, "_download_ptg_job_artifact_sync", _download_ptg_job_artifact_sync)
    with bind_artifact_lease(str(artifact_lease_id) if artifact_lease_id else None):
        if isinstance(live_progress_context, dict) and live_progress_context:
            token = set_live_progress_context(**live_progress_context)
            try:
                return downloader(job, **kwargs)
            finally:
                reset_live_progress_context(token)
        return downloader(job, **kwargs)


async def _download_ptg_job_artifact(
    job: dict[str, Any],
    *,
    reuse_raw_artifacts: bool,
    max_bytes: int | None,
    keep_partial_artifacts: bool | None,
) -> PTG2DownloadedJob:
    try:
        store = PTG2ArtifactStore()
        raw_artifact = await download_raw_artifact(
            job["url"],
            store=store,
            reuse_raw_artifacts=reuse_raw_artifacts,
            max_bytes=max_bytes,
            keep_partial_artifacts=keep_partial_artifacts,
        )
        logical_artifact = (
            await _retained_logical_artifact(store, raw_artifact)
            if _should_materialize_logical_artifact(raw_artifact.raw_path)
            else logical_artifact_identity(
                raw_artifact.raw_path,
                raw_sha256=raw_artifact.raw_sha256,
                raw_byte_count=raw_artifact.byte_count,
                allow_deferred=True,
            )
        )
        return PTG2DownloadedJob(job=job, raw_artifact=raw_artifact, logical_artifact=logical_artifact)
    except Exception as exc:
        return PTG2DownloadedJob(job=job, error=str(exc))


def _download_ptg_job_artifact_sync(
    job: dict[str, Any],
    *,
    reuse_raw_artifacts: bool,
    max_bytes: int | None,
    keep_partial_artifacts: bool | None,
) -> PTG2DownloadedJob:
    return asyncio.run(
        _download_ptg_job_artifact(
            job,
            reuse_raw_artifacts=reuse_raw_artifacts,
            max_bytes=max_bytes,
            keep_partial_artifacts=keep_partial_artifacts,
        )
    )


async def _iter_downloaded_ptg_jobs(
    jobs: list[dict[str, Any]],
    *,
    reuse_raw_artifacts: bool,
    max_bytes: int | None,
    keep_partial_artifacts: bool | None,
):
    download_tasks = max(_env_int(PTG2_DOWNLOAD_TASKS_ENV, PTG2_DEFAULT_DOWNLOAD_TASKS), 1)
    executor = concurrent.futures.ThreadPoolExecutor(
        max_workers=download_tasks,
        thread_name_prefix="ptg2-download",
    )
    pending: set[asyncio.Future[PTG2DownloadedJob]] = set()
    job_iter = iter(jobs)
    base_live_progress_context = current_live_progress_context()
    artifact_lease_id = current_artifact_lease_id()
    job_count = max(len(jobs), 1)

    def schedule_more() -> None:
        """Start downloads until the configured in-flight task limit is reached."""
        while len(pending) < download_tasks:
            try:
                job = next(job_iter)
            except StopIteration:
                return
            job_index = _progress_job_index(job)
            job_total = max(_progress_job_total(job, job_count), 1)
            progress_start = 5.0 + (job_index / job_total) * 15.0
            progress_end = 5.0 + ((job_index + 1) / job_total) * 15.0
            live_progress_context = {
                **base_live_progress_context,
                "overall_progress_start_pct": progress_start,
                "overall_progress_end_pct": progress_end,
            }
            pending.add(
                asyncio.wrap_future(
                    executor.submit(
                        _download_ptg_job_artifact_sync_from_facade,
                        job,
                        reuse_raw_artifacts=reuse_raw_artifacts,
                        max_bytes=max_bytes,
                        keep_partial_artifacts=keep_partial_artifacts,
                        live_progress_context=live_progress_context,
                        artifact_lease_id=artifact_lease_id,
                    )
                )
            )

    schedule_more()
    try:
        while pending:
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            schedule_more()
            for task in done:
                yield task.result()
    finally:
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        # Running thread-pool calls cannot be cancelled by their asyncio
        # wrappers. Wait for them before the import-level artifact lease is
        # released, otherwise GC could reclaim a file a worker still uses.
        await asyncio.to_thread(
            executor.shutdown,
            wait=True,
            cancel_futures=True,
        )
