# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded logical uploads from a trusted, operator-provisioned staging directory."""

from __future__ import annotations

import asyncio
import hashlib
import math
import os
import re
import stat
from contextlib import ExitStack, closing, contextmanager
from dataclasses import asdict, dataclass, field, replace
from tempfile import SpooledTemporaryFile

from process.custom_import._source_text import validate_snapshot_token
from process.custom_import.capture import SealedCapture, capture_stream, iter_records
from process.custom_import.capture_limits import CaptureLimits
from process.custom_import.definition import _COMPRESSIONS, _FORMATS, SourceStream

_OBJECT_ID = re.compile(r"[a-z0-9][a-z0-9_-]{0,127}")
_STREAM_ID = re.compile(r"[a-z][a-z0-9_]{0,62}")
_SHA256 = re.compile(r"[0-9a-f]{64}")


class UploadAcquisitionError(ValueError):
    """A fixed failure code without staged locations, object IDs or source bytes."""


@dataclass(frozen=True)
class UploadObject:
    """A flat logical object name and exact original-byte identity."""

    stream_id: str
    object_id: str = field(repr=False)
    expected_sha256: str
    expected_bytes: int

    def __post_init__(self) -> None:
        if (
            not isinstance(self.stream_id, str)
            or not _STREAM_ID.fullmatch(self.stream_id)
            or not isinstance(self.object_id, str)
            or not _OBJECT_ID.fullmatch(self.object_id)
            or not isinstance(self.expected_sha256, str)
            or not _SHA256.fullmatch(self.expected_sha256)
            or isinstance(self.expected_bytes, bool)
            or not isinstance(self.expected_bytes, int)
            or not 0 <= self.expected_bytes <= CaptureLimits().maximum_compressed_bytes
        ):
            raise UploadAcquisitionError("upload_request_invalid")


def upload_object_identity(source: UploadObject) -> dict[str, str | int]:
    """Return stable logical/content fields, independent of physical path or inode."""
    if type(source) is not UploadObject:
        raise UploadAcquisitionError("upload_request_invalid")
    return asdict(replace(source))


def _directory_identity(metadata):
    if not stat.S_ISDIR(metadata.st_mode):
        raise UploadAcquisitionError("upload_source_denied")
    return metadata.st_dev, metadata.st_ino


def _file_identity(metadata):
    if not stat.S_ISREG(metadata.st_mode):
        raise UploadAcquisitionError("upload_source_denied")
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_mode,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
        metadata.st_nlink,
    )


@dataclass(frozen=True)
class StagedUploadProvider:
    """Trusted composition pins one directory; source requests cannot choose it."""

    root: str | os.PathLike[str] = field(repr=False)
    _identity: tuple[int, int] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        try:
            root = os.fspath(self.root)
            if not isinstance(root, str) or not os.path.isabs(root):
                raise ValueError
            root = os.path.normpath(root)
            object.__setattr__(self, "root", root)
            object.__setattr__(self, "_identity", _directory_identity(os.stat(root, follow_symlinks=False)))
        except Exception:
            raise UploadAcquisitionError("upload_provider_invalid") from None

    @contextmanager
    def _open(self, source: UploadObject):
        with ExitStack() as owned:
            directory_fd = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            owned.callback(os.close, directory_fd)
            if _directory_identity(os.fstat(directory_fd)) != self._identity:
                raise UploadAcquisitionError("upload_source_changed")
            named = os.stat(source.object_id, dir_fd=directory_fd, follow_symlinks=False)
            before = _file_identity(named)
            if named.st_size != source.expected_bytes:
                raise UploadAcquisitionError("upload_identity_mismatch")
            file_fd = os.open(source.object_id, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory_fd)
            owned.callback(os.close, file_fd)
            if _file_identity(os.fstat(file_fd)) != before:
                raise UploadAcquisitionError("upload_source_changed")
            yield file_fd
            if (
                _file_identity(os.fstat(file_fd)) != before
                or _file_identity(os.stat(source.object_id, dir_fd=directory_fd, follow_symlinks=False)) != before
                or _directory_identity(os.stat(self.root, follow_symlinks=False)) != self._identity
            ):
                raise UploadAcquisitionError("upload_source_changed")


def _checked_limits(source, source_stream, source_snapshot_token, provider, limits, timeout_seconds):
    upload_object_identity(source)
    if not isinstance(limits, CaptureLimits):
        raise UploadAcquisitionError("upload_request_invalid")
    limits = CaptureLimits(**asdict(limits))
    if (
        not isinstance(provider, StagedUploadProvider)
        or not isinstance(source_stream, SourceStream)
        or source_stream.stream_id != source.stream_id
        or source_stream.format not in _FORMATS
        or source_stream.compression not in _COMPRESSIONS
        or any(value > getattr(CaptureLimits(), name) for name, value in asdict(limits).items())
        or source.expected_bytes > limits.maximum_compressed_bytes
        or isinstance(timeout_seconds, bool)
        or not isinstance(timeout_seconds, (int, float))
        or not math.isfinite(timeout_seconds)
        or timeout_seconds <= 0
    ):
        raise UploadAcquisitionError("upload_request_invalid")
    validate_snapshot_token(source_snapshot_token)
    return limits


async def _checkpoint(deadline):
    await asyncio.sleep(0)
    if asyncio.get_running_loop().time() >= deadline:
        raise TimeoutError


async def _copy_upload(file_fd, spool, source, limits, deadline):
    digest = hashlib.sha256()
    received_bytes = 0
    while True:
        await _checkpoint(deadline)
        chunk = os.read(file_fd, min(limits.read_chunk_bytes, source.expected_bytes - received_bytes + 1))
        await _checkpoint(deadline)
        if not chunk:
            break
        received_bytes += len(chunk)
        if received_bytes > source.expected_bytes:
            raise UploadAcquisitionError("upload_identity_mismatch")
        digest.update(chunk)
        spool.write(chunk)
    if received_bytes != source.expected_bytes or digest.hexdigest() != source.expected_sha256:
        raise UploadAcquisitionError("upload_identity_mismatch")
    spool.seek(0)


async def _validate_records(sealed, source_stream, limits, deadline):
    with closing(iter_records(sealed, source_stream, limits=limits)) as decoded_records:
        for _record in decoded_records:
            await _checkpoint(deadline)


async def acquire_upload(
    upload_object: UploadObject,
    source_stream: SourceStream,
    *,
    source_snapshot_token: str,
    provider: StagedUploadProvider,
    limits: CaptureLimits,
    timeout_seconds: float,
) -> SealedCapture:
    """Acquire and validate one original; deadline checks surround bounded local reads.

    Regular-file reads run synchronously between cancellation points, so no worker
    can outlive or reuse an owned descriptor. Kernel reads themselves are not preempted.
    Staged originals remain reusable and are never removed by acquisition.
    """
    try:
        limits = _checked_limits(upload_object, source_stream, source_snapshot_token, provider, limits, timeout_seconds)
        deadline = asyncio.get_running_loop().time() + timeout_seconds
        async with asyncio.timeout_at(deadline):
            await _checkpoint(deadline)
            with (
                provider._open(upload_object) as file_fd,
                SpooledTemporaryFile(max_size=min(limits.maximum_compressed_bytes, 1024 * 1024), mode="w+b") as spool,
            ):
                await _copy_upload(file_fd, spool, upload_object, limits, deadline)
                sealed = capture_stream(
                    spool, source_stream, source_snapshot_token=source_snapshot_token, limits=limits
                )
                await _checkpoint(deadline)
                await _validate_records(sealed, source_stream, limits, deadline)
            await _checkpoint(deadline)
            return sealed
    except asyncio.CancelledError:
        raise asyncio.CancelledError from None
    except TimeoutError:
        raise UploadAcquisitionError("upload_acquisition_timeout") from None
    except UploadAcquisitionError:
        raise
    except Exception:
        raise UploadAcquisitionError("upload_acquisition_failed") from None
