# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Own and authenticate one anonymous source-local projection COPY stream."""

from __future__ import annotations

from contextlib import contextmanager, suppress
import hashlib
import hmac
import os
import stat
import tempfile
import threading
from collections.abc import Iterator
from pathlib import Path
from typing import BinaryIO

from process.ptg_parts.ptg2_tax_identity_source_projection import (
    PreparedTaxIdentitySourceProjection,
    TaxIdentitySourceProjectionError,
    _fail,
    _strict_sha256,
)

_COPY_READ_BYTES = 1024 * 1024
_LEASE_TOKEN = object()
_CopyIdentity = tuple[int, int, int, int, int, int, int, int, int]
_ScratchIdentity = tuple[int, int, int, int, int]


def _anonymous_copy_identity(metadata: os.stat_result) -> _CopyIdentity:
    """Return the complete identity of one private anonymous regular file."""

    if (
        os.name != "posix"
        or not stat.S_ISREG(metadata.st_mode)
        or metadata.st_nlink != 0
        or metadata.st_uid != os.geteuid()
        or stat.S_IMODE(metadata.st_mode) & 0o077
    ):
        raise _fail()
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_mode,
        metadata.st_nlink,
        metadata.st_uid,
        metadata.st_gid,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
    )


def _scratch_identity(metadata: os.stat_result) -> _ScratchIdentity:
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != os.geteuid()
        or stat.S_IMODE(metadata.st_mode) & 0o022
    ):
        raise _fail()
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_mode,
        metadata.st_uid,
        metadata.st_gid,
    )


def _open_scratch_parent(scratch_parent: Path) -> tuple[int, _ScratchIdentity]:
    scratch_descriptor: int | None = None
    try:
        if os.name != "posix" or not all(
            hasattr(os, flag_name) for flag_name in ("O_DIRECTORY", "O_NOFOLLOW")
        ):
            raise _fail()
        path_identity = _scratch_identity(os.lstat(scratch_parent))
        open_flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
        open_flags |= getattr(os, "O_CLOEXEC", 0)
        scratch_descriptor = os.open(scratch_parent, open_flags)
        descriptor_identity = _scratch_identity(os.fstat(scratch_descriptor))
        if descriptor_identity != path_identity:
            raise _fail()
        opened_descriptor = scratch_descriptor
        scratch_descriptor = None
        return opened_descriptor, descriptor_identity
    except TaxIdentitySourceProjectionError:
        raise
    except Exception:
        raise _fail() from None
    finally:
        if scratch_descriptor is not None:
            with suppress(OSError):
                os.close(scratch_descriptor)


def _is_scratch_parent_unchanged(
    scratch_parent: Path,
    scratch_descriptor: int,
    expected_identity: _ScratchIdentity,
) -> bool:
    try:
        return (
            _scratch_identity(os.fstat(scratch_descriptor)) == expected_identity
            and _scratch_identity(os.lstat(scratch_parent)) == expected_identity
        )
    except Exception:
        return False


def _open_anonymous_projection_copy(scratch_parent: Path) -> BinaryIO:
    """Create an anonymous private file under one validated scratch parent."""

    scratch_descriptor: int | None = None
    copy_file: BinaryIO | None = None
    try:
        scratch_descriptor, scratch_identity = _open_scratch_parent(scratch_parent)
        copy_file = tempfile.TemporaryFile(
            mode="w+b",
            prefix=".ptg2-tax-source-",
            dir=scratch_parent,
        )
        if (
            not _is_scratch_parent_unchanged(
                scratch_parent,
                scratch_descriptor,
                scratch_identity,
            )
            or _anonymous_copy_identity(os.fstat(copy_file.fileno()))[6] != 0
        ):
            raise _fail()
        opened_file = copy_file
        copy_file = None
        return opened_file
    except TaxIdentitySourceProjectionError:
        raise
    except Exception:
        raise _fail() from None
    finally:
        if copy_file is not None:
            with suppress(BaseException):
                copy_file.close()
        if scratch_descriptor is not None:
            with suppress(OSError):
                os.close(scratch_descriptor)


class _ProjectionCopyLease:
    """One-shot ownership of one anonymous COPY descriptor."""

    __slots__ = (
        "_copy_file",
        "_copy_identity",
        "_copy_sha256",
        "_copy_byte_count",
        "_lock",
        "_state",
    )

    def __init__(
        self,
        copy_file: BinaryIO,
        *,
        copy_identity: _CopyIdentity,
        copy_sha256: str,
        copy_byte_count: int,
        token: object,
    ) -> None:
        if token is not _LEASE_TOKEN:
            raise _fail()
        self._copy_file: BinaryIO | None = copy_file
        self._copy_identity = copy_identity
        self._copy_sha256 = copy_sha256
        self._copy_byte_count = copy_byte_count
        self._lock = threading.Lock()
        self._state = "ready"

    @property
    def copy_sha256(self) -> str:
        """Return the authenticated complete-file digest."""

        return self._copy_sha256

    @property
    def copy_byte_count(self) -> int:
        """Return the authenticated complete-file byte count."""

        return self._copy_byte_count

    @contextmanager
    def claim(self) -> Iterator[tuple[BinaryIO, _CopyIdentity]]:
        """Yield the exact descriptor once and close it after terminal use."""

        with self._lock:
            if self._state != "ready" or self._copy_file is None:
                raise _fail()
            self._state = "active"
            copy_file = self._copy_file
        try:
            yield copy_file, self._copy_identity
        finally:
            with self._lock:
                self._state = "closed"
                self._copy_file = None
            with suppress(BaseException):
                copy_file.close()

    def cleanup(self) -> None:
        """Close only this descriptor, deferring closure while it is active."""

        copy_file: BinaryIO | None = None
        with self._lock:
            if self._state == "ready":
                self._state = "closed"
                copy_file = self._copy_file
                self._copy_file = None
            elif self._state == "active":
                self._state = "close_pending"
        if copy_file is not None:
            with suppress(BaseException):
                copy_file.close()


def _seal_projection_copy_lease(
    copy_file: BinaryIO,
    *,
    copy_metadata: os.stat_result,
    copy_sha256: str,
    copy_byte_count: int,
) -> _ProjectionCopyLease:
    """Transfer one completely authenticated anonymous file into a lease."""

    try:
        normalized_sha256 = _strict_sha256(copy_sha256)
        if type(copy_byte_count) is not int or copy_byte_count <= 0:
            raise _fail()
        expected_identity = _anonymous_copy_identity(copy_metadata)
        current_identity = _anonymous_copy_identity(os.fstat(copy_file.fileno()))
        if (
            current_identity != expected_identity
            or copy_metadata.st_size != copy_byte_count
        ):
            raise _fail()
        return _ProjectionCopyLease(
            copy_file,
            copy_identity=expected_identity,
            copy_sha256=normalized_sha256,
            copy_byte_count=copy_byte_count,
            token=_LEASE_TOKEN,
        )
    except TaxIdentitySourceProjectionError:
        raise
    except Exception:
        raise _fail() from None


def _authenticate_and_seal_projection_copy(
    copy_file: BinaryIO,
) -> tuple[_ProjectionCopyLease, str, int]:
    """Hash the creation descriptor and transfer its exact identity to a lease."""

    try:
        copy_file.flush()
        initial_metadata = os.fstat(copy_file.fileno())
        initial_identity = _anonymous_copy_identity(initial_metadata)
        copy_file.seek(0)
        copy_digest = hashlib.sha256()
        observed_byte_count = 0
        while copy_chunk := copy_file.read(_COPY_READ_BYTES):
            if type(copy_chunk) is not bytes:
                raise _fail()
            copy_digest.update(copy_chunk)
            observed_byte_count += len(copy_chunk)
        final_metadata = os.fstat(copy_file.fileno())
        if (
            observed_byte_count != initial_metadata.st_size
            or _anonymous_copy_identity(final_metadata) != initial_identity
        ):
            raise _fail()
        copy_sha256 = copy_digest.hexdigest()
        owner = _seal_projection_copy_lease(
            copy_file,
            copy_metadata=final_metadata,
            copy_sha256=copy_sha256,
            copy_byte_count=observed_byte_count,
        )
        return owner, copy_sha256, observed_byte_count
    except TaxIdentitySourceProjectionError:
        raise
    except Exception:
        raise _fail() from None


class _AuthenticatedCopyReader:
    """Hash and count exactly the immutable bytes returned to asyncpg."""

    __slots__ = (
        "_copy_file",
        "_expected_byte_count",
        "_expected_sha256",
        "_finished",
        "_observed_byte_count",
        "_observed_sha256",
    )

    def __init__(
        self,
        copy_file: BinaryIO,
        *,
        expected_sha256: str,
        expected_byte_count: int,
    ) -> None:
        self._copy_file = copy_file
        self._expected_sha256 = expected_sha256
        self._expected_byte_count = expected_byte_count
        self._observed_sha256 = hashlib.sha256()
        self._observed_byte_count = 0
        self._finished = False

    def read(self, size: int = -1) -> bytes:
        """Return one bounded chunk and bind its exact bytes into the proof."""

        try:
            if type(size) is not int or size < -1:
                raise _fail()
            if size == 0:
                return b""
            if self._finished:
                return b""
            read_size = _COPY_READ_BYTES if size == -1 else min(size, _COPY_READ_BYTES)
            copy_chunk = self._copy_file.read(read_size)
            if type(copy_chunk) is not bytes or len(copy_chunk) > read_size:
                raise _fail()
            if not copy_chunk:
                self._finished = True
                return b""
            next_byte_count = self._observed_byte_count + len(copy_chunk)
            if next_byte_count > self._expected_byte_count:
                raise _fail()
            self._observed_sha256.update(copy_chunk)
            self._observed_byte_count = next_byte_count
            return copy_chunk
        except TaxIdentitySourceProjectionError:
            raise
        except Exception:
            raise _fail() from None

    def finish(self, expected_identity: _CopyIdentity) -> None:
        """Require EOF, exact consumed content, and unchanged descriptor state."""

        try:
            if not self._finished and self.read(1):
                raise _fail()
            current_identity = _anonymous_copy_identity(
                os.fstat(self._copy_file.fileno())
            )
            if (
                not self._finished
                or self._observed_byte_count != self._expected_byte_count
                or not hmac.compare_digest(
                    self._observed_sha256.hexdigest(),
                    self._expected_sha256,
                )
                or current_identity != expected_identity
            ):
                raise _fail()
        except TaxIdentitySourceProjectionError:
            raise
        except Exception:
            raise _fail() from None


@contextmanager
def _authenticated_projection_copy_stream(
    prepared: PreparedTaxIdentitySourceProjection,
) -> Iterator[_AuthenticatedCopyReader]:
    """Yield one bounded reader and validate exactly what its caller consumed."""

    try:
        if type(prepared) is not PreparedTaxIdentitySourceProjection:
            raise _fail()
        owner = prepared._copy_owner
        public_sha256 = _strict_sha256(prepared.copy_sha256)
        if (
            type(owner) is not _ProjectionCopyLease
            or type(prepared.copy_byte_count) is not int
            or prepared.copy_byte_count != owner.copy_byte_count
            or not hmac.compare_digest(public_sha256, owner.copy_sha256)
        ):
            raise _fail()
    except TaxIdentitySourceProjectionError:
        raise
    except Exception:
        raise _fail() from None
    with owner.claim() as (copy_file, expected_identity):
        try:
            if _anonymous_copy_identity(os.fstat(copy_file.fileno())) != expected_identity:
                raise _fail()
            copy_file.seek(0)
        except TaxIdentitySourceProjectionError:
            raise
        except Exception:
            raise _fail() from None
        copy_reader = _AuthenticatedCopyReader(
            copy_file,
            expected_sha256=owner.copy_sha256,
            expected_byte_count=owner.copy_byte_count,
        )
        try:
            yield copy_reader
        except BaseException:
            raise
        else:
            copy_reader.finish(expected_identity)


def _cleanup_projection_copy_owner(
    prepared: PreparedTaxIdentitySourceProjection,
) -> None:
    """Best-effort close only a genuine projection lease."""

    owner = getattr(prepared, "_copy_owner", None)
    if type(owner) is _ProjectionCopyLease:
        owner.cleanup()


__all__: list[str] = []
