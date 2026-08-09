# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Verified CMS acquisition into durable content-addressed storage."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
import hashlib
import inspect
import os
from pathlib import Path
import re
import tempfile

from process.control_cancel import ImportCancelledError
from process.nppes_public_evidence_artifacts import (
    RetainedHttpArtifact as _RetainedHttpArtifact,
    atomic_write_index as _atomic_write_index,
    canonical_cms_url as _canonical_cms_url,
    index_path as _index_path,
    nppes_artifact_store,
    retained_path as _retained_path,
    resolve_nppes_artifact_root,
    retain_verified_inode as _retain_verified_inode,
    verified_cached_artifact as _verified_cached_artifact,
)
from process.nppes_public_evidence_archive import (
    NPPES_LISTING_URL,
    NppesArchiveCandidate,
    RetainedNppesArchive,
    archive_error,
    parse_official_nppes_listing,
    validate_nppes_archive_candidate,
)
from process.nppes_public_evidence_http import (
    HttpStreamResult as _HttpStreamResult,
    stream_official_url as _stream_official_url,
)
from process.ptg_parts.artifacts import PTG2ArtifactStore, sha256_file
from process.ptg_parts.input_artifact_retention import (
    async_named_artifact_lock,
)


_SHA256_RE = re.compile(r"[0-9a-f]{64}", flags=re.ASCII)
_DEFAULT_MAX_ARCHIVE_BYTES = 4 * 1024**3
_MAX_LISTING_BYTES = 4 * 1024 * 1024


async def _invoke_cancel(callback) -> None:
    """Run one optional cooperative cancellation boundary."""

    if callback is None:
        return
    result = callback()
    if inspect.isawaitable(result):
        await result


@dataclass(frozen=True, slots=True, repr=False)
class NppesListingSnapshot:
    """One retained official listing and its deterministic candidate vector."""

    path: Path
    listing_sha256: str
    byte_count: int
    candidates: tuple[NppesArchiveCandidate, ...]
    etag: str | None
    last_modified: str | None
    acquired_at: str

    def __repr__(self) -> str:
        return "<nppes-listing-snapshot>"


def _utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def _positive_limit(raw_value: str | None, default: int) -> int:
    if raw_value is None or not raw_value.strip():
        return default
    if not raw_value.isascii() or not raw_value.isdigit():
        raise archive_error()
    parsed = int(raw_value)
    if not 1 <= parsed < 2**63:
        raise archive_error()
    return parsed


def _listing_metadata_value(value: object) -> str | None:
    if value is None:
        return None
    if (
        type(value) is not str
        or len(value.encode("utf-8")) > 4096
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
    ):
        raise archive_error()
    return value


def validate_nppes_listing_snapshot(candidate: object) -> NppesListingSnapshot:
    """Rehash and rebuild one exact retained official listing observation."""

    try:
        if (
            type(candidate) is not NppesListingSnapshot
            or not isinstance(candidate.path, Path)
            or candidate.path.is_symlink()
            or type(candidate.listing_sha256) is not str
            or _SHA256_RE.fullmatch(candidate.listing_sha256) is None
            or type(candidate.byte_count) is not int
            or not 1 <= candidate.byte_count <= _MAX_LISTING_BYTES
            or type(candidate.candidates) is not tuple
            or not candidate.candidates
            or any(
                type(archive_candidate) is not NppesArchiveCandidate
                for archive_candidate in candidate.candidates
            )
            or type(candidate.acquired_at) is not str
        ):
            raise archive_error()
        datetime.strptime(candidate.acquired_at, "%Y-%m-%dT%H:%M:%SZ")
        raw_listing = candidate.path.read_bytes()
        if (
            len(raw_listing) != candidate.byte_count
            or hashlib.sha256(raw_listing).hexdigest() != candidate.listing_sha256
        ):
            raise archive_error()
        fixed_candidates = tuple(
            validate_nppes_archive_candidate(archive_candidate)
            for archive_candidate in candidate.candidates
        )
        parsed_candidates = parse_official_nppes_listing(raw_listing)
        if fixed_candidates != parsed_candidates:
            raise archive_error()
        rebuilt = NppesListingSnapshot(
            path=candidate.path,
            listing_sha256=candidate.listing_sha256,
            byte_count=candidate.byte_count,
            candidates=parsed_candidates,
            etag=_listing_metadata_value(candidate.etag),
            last_modified=_listing_metadata_value(candidate.last_modified),
            acquired_at=candidate.acquired_at,
        )
    except Exception:
        normalized_error = archive_error()
    else:
        return rebuilt
    raise normalized_error


async def _acquire_url(
    store: PTG2ArtifactStore,
    source_url: str,
    *,
    suffix: str,
    max_bytes: int,
    cancel_check=None,
) -> _RetainedHttpArtifact:
    """Acquire, durably retain, and index one canonical CMS URL."""

    _canonical_cms_url(source_url)
    lock_key = hashlib.sha256(source_url.encode("ascii")).hexdigest()
    async with async_named_artifact_lock(store, "nppes-acquisition", lock_key):
        cached = await _verified_cached_artifact(
            store,
            source_url,
            suffix,
            max_bytes,
            cancel_check,
        )
        with tempfile.TemporaryDirectory(dir=store.tmp_dir) as temporary_dir:
            os.chmod(temporary_dir, 0o700)
            staged_path = Path(temporary_dir) / f"artifact{suffix}"
            stream_result = await _stream_official_url(
                source_url,
                staged_path,
                max_bytes=max_bytes,
                etag=cached.etag if cached else None,
                cancel_check=cancel_check,
            )
            if stream_result.final_url != source_url:
                raise archive_error()
            acquired_at = _utc_now()
            if stream_result.status == 304:
                retained = _retained_not_modified_artifact(
                    cached,
                    source_url,
                    stream_result,
                    acquired_at,
                )
            else:
                retained = await _published_retained_artifact(
                    store,
                    source_url,
                    suffix,
                    staged_path,
                    stream_result,
                    acquired_at,
                    cancel_check,
                )
        await _persist_retained_url_index(
            store,
            source_url,
            suffix,
            retained,
            cancel_check,
        )
        return retained


def _retained_not_modified_artifact(
    cached: _RetainedHttpArtifact | None,
    source_url: str,
    stream_result: _HttpStreamResult,
    acquired_at: str,
) -> _RetainedHttpArtifact:
    if cached is None:
        raise archive_error()
    return _RetainedHttpArtifact(
        path=cached.path,
        source_url=source_url,
        final_url=stream_result.final_url,
        sha256=cached.sha256,
        byte_count=cached.byte_count,
        etag=stream_result.etag or cached.etag,
        last_modified=stream_result.last_modified or cached.last_modified,
        acquired_at=acquired_at,
    )


async def _published_retained_artifact(
    store: PTG2ArtifactStore,
    source_url: str,
    suffix: str,
    staged_path: Path,
    stream_result: _HttpStreamResult,
    acquired_at: str,
    cancel_check=None,
) -> _RetainedHttpArtifact:
    if stream_result.sha256 is None or stream_result.byte_count is None:
        raise archive_error()
    staged_digest, staged_bytes = await asyncio.to_thread(
        sha256_file,
        staged_path,
    )
    await _invoke_cancel(cancel_check)
    if (
        staged_digest != stream_result.sha256
        or staged_bytes != stream_result.byte_count
    ):
        raise archive_error()
    durable_path = await asyncio.to_thread(
        _retain_verified_inode,
        store,
        staged_path,
        stream_result.sha256,
        suffix,
    )
    return _RetainedHttpArtifact(
        path=durable_path,
        source_url=source_url,
        final_url=stream_result.final_url,
        sha256=stream_result.sha256,
        byte_count=stream_result.byte_count,
        etag=stream_result.etag,
        last_modified=stream_result.last_modified,
        acquired_at=acquired_at,
    )


async def _persist_retained_url_index(
    store: PTG2ArtifactStore,
    source_url: str,
    suffix: str,
    retained: _RetainedHttpArtifact,
    cancel_check=None,
) -> None:
    index_values_by_name = {
        "contract": "healthporta.nppes-retained-url-index.v1",
        "source_url": retained.source_url,
        "final_url": retained.final_url,
        "suffix": suffix,
        "sha256": retained.sha256,
        "byte_count": retained.byte_count,
        "etag": retained.etag,
        "last_modified": retained.last_modified,
        "acquired_at": retained.acquired_at,
    }
    await asyncio.to_thread(
        _atomic_write_index,
        _index_path(store, source_url),
        index_values_by_name,
    )
    await _invoke_cancel(cancel_check)


async def acquire_nppes_listing(
    store: PTG2ArtifactStore,
    *,
    cancel_check=None,
) -> NppesListingSnapshot:
    """Retain and parse the exact current official NPPES listing."""

    try:
        retained = await _acquire_url(
            store,
            NPPES_LISTING_URL,
            suffix=".html",
            max_bytes=_MAX_LISTING_BYTES,
            cancel_check=cancel_check,
        )
        raw_html = await asyncio.to_thread(retained.path.read_bytes)
        if len(raw_html) != retained.byte_count:
            raise archive_error()
        candidates = parse_official_nppes_listing(raw_html)
        snapshot = validate_nppes_listing_snapshot(NppesListingSnapshot(
            path=retained.path,
            listing_sha256=retained.sha256,
            byte_count=retained.byte_count,
            candidates=candidates,
            etag=retained.etag,
            last_modified=retained.last_modified,
            acquired_at=retained.acquired_at,
        ))
    except (asyncio.CancelledError, ImportCancelledError):
        raise
    except Exception:
        normalized_error = archive_error()
    else:
        return snapshot
    raise normalized_error


async def acquire_nppes_archive(
    store: PTG2ArtifactStore,
    candidate: object,
    listing_sha256: object,
    *,
    cancel_check=None,
) -> RetainedNppesArchive:
    """Retain one exact candidate and bind it to the discovered listing."""

    try:
        fixed_candidate = validate_nppes_archive_candidate(candidate)
        if type(listing_sha256) is not str or _SHA256_RE.fullmatch(listing_sha256) is None:
            raise archive_error()
        max_bytes = _positive_limit(
            os.getenv("HLTHPRT_NPPES_MAX_ARCHIVE_BYTES"),
            _DEFAULT_MAX_ARCHIVE_BYTES,
        )
        retained = await _acquire_url(
            store,
            fixed_candidate.source_url,
            suffix=".zip",
            max_bytes=max_bytes,
            cancel_check=cancel_check,
        )
        archive = RetainedNppesArchive(
            candidate=fixed_candidate,
            path=retained.path,
            artifact_sha256=retained.sha256,
            artifact_byte_count=retained.byte_count,
            listing_sha256=listing_sha256,
            etag=retained.etag,
            last_modified=retained.last_modified,
            acquired_at=retained.acquired_at,
        )
    except (asyncio.CancelledError, ImportCancelledError):
        raise
    except Exception:
        normalized_error = archive_error()
    else:
        return archive
    raise normalized_error


__all__ = (
    "NppesListingSnapshot",
    "acquire_nppes_archive",
    "acquire_nppes_listing",
    "nppes_artifact_store",
    "resolve_nppes_artifact_root",
    "validate_nppes_listing_snapshot",
)
