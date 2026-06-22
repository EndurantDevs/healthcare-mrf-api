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
import zlib
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import urljoin, urlsplit

import aiohttp

from process.ptg_parts.artifact_streams import logical_artifact_identity, stream_logical_artifact
from process.ptg_parts.artifacts import (
    PTG2ArtifactStore,
    _hash_existing_file_into,
    _load_completed_ranges,
    _range_sidecar_path,
    _safe_url_suffix,
    _write_completed_ranges,
    choose_reusable_raw_artifact,
    ptg2_temp_parent,
    sha256_file,
)
from process.ptg_parts.canonical import canonicalize_url
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
from process.ptg_parts.live_progress import write_live_progress
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
        response = await session.request(current_method, current_url, allow_redirects=False, **kwargs)
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
    write_live_progress(
        phase="download",
        unit="bytes",
        done=bytes_read,
        total=total_bytes,
        pct=percent,
        rate={"mib_s": mib_s},
        eta_seconds=eta if total_bytes and total_bytes > 0 else None,
        message=f"downloading {_safe_download_label(url)}",
        label=url,
    )


def _safe_download_label(url: str) -> str:
    parsed = urlsplit(str(url))
    if parsed.scheme and parsed.netloc:
        tail = parsed.path.rsplit("/", 1)[-1]
        return f"{parsed.netloc}/{tail}" if tail else parsed.netloc
    return str(url)[:128]


async def fetch_head_metadata(url: str, timeout_seconds: int = 30) -> PTG2HeadMetadata:
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


async def _probe_http_range_support(url: str) -> tuple[bool, int | None, str | None, str | None]:
    await assert_safe_url(url)
    timeout = aiohttp.ClientTimeout(total=60, connect=30, sock_read=30)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with _validated_request(session, "GET", url, headers={"Range": "bytes=0-0"}) as response:
                if response.status != 206:
                    return False, None, None, None
                content_range = response.headers.get("Content-Range") or ""
                match = re.match(r"bytes\s+0-0/(\d+)$", content_range.strip())
                if not match:
                    return False, None, None, None
                await response.content.read()
                return True, int(match.group(1)), response.headers.get("ETag"), str(response.url)
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
    lock = asyncio.Lock()
    timeout = aiohttp.ClientTimeout(total=None, connect=60, sock_read=600)
    pending_ranges = [item for item in ranges if item not in completed]

    async def fetch_range(session: aiohttp.ClientSession, item: tuple[int, int]) -> None:
        nonlocal completed_bytes, next_progress_bytes
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
                if response.status != 206:
                    raise RuntimeError(f"Range download not supported for {url}: status {response.status}")
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
                            completed_bytes += len(chunk)
                            counted += len(chunk)
                            if completed_bytes >= next_progress_bytes:
                                _emit_download_progress(
                                    url=url,
                                    bytes_read=min(completed_bytes, total_bytes),
                                    total_bytes=total_bytes,
                                    started_at=started_at,
                                    done=False,
                                )
                                interval = _download_progress_interval_bytes()
                                while completed_bytes >= next_progress_bytes:
                                    next_progress_bytes += interval
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
                    completed_bytes = max(0, completed_bytes - counted)
                    while next_progress_bytes > _download_progress_interval_bytes() and (
                        completed_bytes < next_progress_bytes - _download_progress_interval_bytes()
                    ):
                        next_progress_bytes -= _download_progress_interval_bytes()

    semaphore = asyncio.Semaphore(_range_download_tasks())

    async def bounded_fetch(session: aiohttp.ClientSession, item: tuple[int, int]) -> None:
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
        await asyncio.gather(*(bounded_fetch(session, item) for item in pending_ranges))
    sidecar_path.unlink(missing_ok=True)
    _emit_download_progress(
        url=url,
        bytes_read=total_bytes,
        total_bytes=total_bytes,
        started_at=started_at,
        done=False,
    )
    return True


async def download_raw_artifact(
    url: str,
    store: PTG2ArtifactStore | None = None,
    reuse_raw_artifacts: bool = True,
    max_bytes: int | None = None,
    keep_partial_artifacts: bool | None = None,
) -> PTG2RawArtifact:
    store = store or PTG2ArtifactStore()
    await assert_safe_url(url)
    keep_partials = _env_bool(PTG2_KEEP_PARTIAL_ENV, True) if keep_partial_artifacts is None else keep_partial_artifacts
    canonical_url = canonicalize_url(url)
    head = await fetch_head_metadata(url)
    progress_started_at = time.monotonic()
    progress_interval_bytes = _download_progress_interval_bytes()
    next_progress_bytes = progress_interval_bytes
    validate_downloaded_gzip = _env_bool(_GZIP_VALIDATE_FRESH_ENV, False)
    if reuse_raw_artifacts:
        candidate, mode = choose_reusable_raw_artifact(store.find_candidates(canonical_url), head, store=store)
        if candidate is not None and mode is not None:
            raw_uri = candidate.get("raw_storage_uri") or candidate.get("storage_uri")
            raw_path = store.path_from_uri(raw_uri)
            expected = candidate.get("raw_sha256") or candidate.get("sha256")
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
    digest = hashlib.sha256()
    byte_count = 0
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
            timeout = aiohttp.ClientTimeout(total=None, connect=60, sock_read=600)
            used_range_download = False
            range_total = head.content_length if head and head.content_length else None
            if (
                _env_bool(PTG2_RANGE_DOWNLOADS_ENV, True)
                and range_total
                and range_total >= _range_download_min_bytes()
            ):
                range_supported, probed_total, probed_etag, _range_url = await _probe_http_range_support(url)
                if range_supported and probed_total:
                    await _download_raw_artifact_ranges(
                        url=url,
                        partial_path=partial_path,
                        total_bytes=probed_total,
                        etag=probed_etag or (head.etag if head else None),
                        max_bytes=max_bytes,
                        started_at=progress_started_at,
                    )
                    byte_count = _hash_existing_file_into(partial_path, digest)
                    used_range_download = True
            if not used_range_download:
                resume_from = partial_path.stat().st_size if partial_path.exists() else 0
                if head.content_length and resume_from == head.content_length:
                    byte_count = _hash_existing_file_into(partial_path, digest)
                elif head.content_length and resume_from > head.content_length:
                    partial_path.unlink(missing_ok=True)
                    resume_from = 0
                if byte_count == 0 and resume_from > 0:
                    byte_count = _hash_existing_file_into(partial_path, digest)
                    while byte_count >= next_progress_bytes:
                        next_progress_bytes += progress_interval_bytes
            if not used_range_download and not (head.content_length and byte_count == head.content_length):
                retries = _download_retry_count()
                attempt = 0
                while not (head.content_length and byte_count == head.content_length):
                    try:
                        async with aiohttp.ClientSession(timeout=timeout) as session:
                            resume_from = partial_path.stat().st_size if partial_path.exists() else 0
                            if resume_from != byte_count:
                                digest = hashlib.sha256()
                                byte_count = _hash_existing_file_into(partial_path, digest) if resume_from > 0 else 0
                            headers = {"Range": f"bytes={resume_from}-"} if resume_from > 0 and byte_count == resume_from else None
                            async with _validated_request(session, "GET", url, headers=headers) as response:
                                response.raise_for_status()
                                length = response.headers.get("Content-Length")
                                if resume_from > 0 and response.status != 206:
                                    digest = hashlib.sha256()
                                    byte_count = 0
                                    resume_from = 0
                                    while next_progress_bytes > progress_interval_bytes:
                                        next_progress_bytes -= progress_interval_bytes
                                response_total = (
                                    head.content_length
                                    if head and head.content_length
                                    else (resume_from + int(length) if length and length.isdigit() else None)
                                )
                                mode = "ab" if resume_from > 0 and response.status == 206 else "wb"
                                with open(tmp_path, mode) as dst:
                                    async for chunk in response.content.iter_chunked(1024 * 1024):
                                        byte_count += len(chunk)
                                        if max_bytes is not None and byte_count > max_bytes:
                                            raise RuntimeError(f"PTG2 max-bytes guard exceeded for {url}")
                                        digest.update(chunk)
                                        dst.write(chunk)
                                        if byte_count >= next_progress_bytes:
                                            _emit_download_progress(
                                                url=url,
                                                bytes_read=byte_count,
                                                total_bytes=response_total,
                                                started_at=progress_started_at,
                                                done=False,
                                            )
                                            while byte_count >= next_progress_bytes:
                                                next_progress_bytes += progress_interval_bytes
                        if head.content_length and byte_count != head.content_length:
                            raise RuntimeError(
                                f"Download for {url} ended at {byte_count} bytes, expected {head.content_length}"
                            )
                        break
                    except UnsafeUrlError:
                        raise
                    except Exception as exc:
                        if attempt >= retries:
                            raise
                        delay = _download_retry_delay_seconds() * (2 ** attempt)
                        message = (
                            f"PTG2_DOWNLOAD_RETRY url={url} bytes={byte_count} "
                            f"attempt={attempt + 1} next_attempt={attempt + 2} "
                            f"delay_seconds={delay:.2f} error={exc}"
                        )
                        _emit_screen_line(message, stderr=True)
                        logger.debug(message)
                        attempt += 1
                        if delay > 0:
                            await asyncio.sleep(delay)
        raw_sha = digest.hexdigest()
        final_path = store.artifact_path(raw_sha, kind=PTG2_ARTIFACT_RAW, suffix=_safe_url_suffix(url))
        final_path.parent.mkdir(parents=True, exist_ok=True)
        if final_path.exists():
            tmp_path.unlink(missing_ok=True)
        else:
            os.replace(tmp_path, final_path)
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
            final_path.unlink(missing_ok=True)
            raise RuntimeError(gzip_error)
        _emit_download_progress(
            url=url,
            bytes_read=actual_size,
            total_bytes=head.content_length if head and head.content_length else actual_size,
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
            "content_length": head.content_length if head else actual_size,
            "byte_count": actual_size,
            "etag": head.etag if head else None,
            "last_modified": head.last_modified if head else None,
            "status": "available",
        }
        store.record_manifest(manifest_payload)
        return PTG2RawArtifact(
            original_url=url,
            canonical_url=canonical_url,
            raw_path=str(final_path),
            raw_storage_uri=raw_uri,
            raw_sha256=actual_sha,
            byte_count=actual_size,
            head=head,
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
    raw_artifact = await download_raw_artifact(
        url,
        reuse_raw_artifacts=reuse_raw_artifacts,
        max_bytes=max_bytes,
        keep_partial_artifacts=keep_partial_artifacts,
    )
    logical_artifact = (
        stream_logical_artifact(raw_artifact.raw_path, output_dir=output_dir)
        if materialize_logical
        else logical_artifact_identity(
            raw_artifact.raw_path,
            raw_sha256=raw_artifact.raw_sha256,
            raw_byte_count=raw_artifact.byte_count,
            allow_deferred=True,
        )
    )
    return raw_artifact, logical_artifact

def _materialize_json_source_from_facade():
    ptg_module = sys.modules.get("process.ptg")
    return getattr(ptg_module, "materialize_json_source", materialize_json_source)


def _download_ptg_job_artifact_sync_from_facade(job: dict[str, object], **kwargs) -> PTG2DownloadedJob:
    ptg_module = sys.modules.get("process.ptg")
    downloader = getattr(ptg_module, "_download_ptg_job_artifact_sync", _download_ptg_job_artifact_sync)
    return downloader(job, **kwargs)


async def _download_ptg_job_artifact(
    job: dict[str, Any],
    *,
    reuse_raw_artifacts: bool,
    max_bytes: int | None,
    keep_partial_artifacts: bool | None,
) -> PTG2DownloadedJob:
    try:
        with tempfile.TemporaryDirectory(dir=ptg2_temp_parent()) as tmpdir:
            raw_artifact, logical_artifact = await _materialize_json_source_from_facade()(
                job["url"],
                tmpdir,
                reuse_raw_artifacts=reuse_raw_artifacts,
                max_bytes=max_bytes,
                materialize_logical=False,
                keep_partial_artifacts=keep_partial_artifacts,
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

    def schedule_more() -> None:
        while len(pending) < download_tasks:
            try:
                job = next(job_iter)
            except StopIteration:
                return
            pending.add(
                asyncio.wrap_future(
                    executor.submit(
                        _download_ptg_job_artifact_sync_from_facade,
                        job,
                        reuse_raw_artifacts=reuse_raw_artifacts,
                        max_bytes=max_bytes,
                        keep_partial_artifacts=keep_partial_artifacts,
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
        executor.shutdown(wait=False, cancel_futures=True)
