# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Private held-FD authentication for paired tax-identity sidecars."""

from __future__ import annotations

import hashlib
import hmac
import os
import stat
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO, Iterator, Protocol


_HASH_CHUNK_BYTES = 1024 * 1024
_HEADER_BYTES = 13
_V1_MAGIC = b"PTG2TAX1"
_V2_MAGIC = b"PTG2TAX2"
_SCRATCH_INVALID = "ptg2_tax_identity_shadow_scratch_invalid"
_ARTIFACT_INVALID = "ptg2_tax_identity_shadow_artifact_invalid"
_ARTIFACT_CHANGED = "ptg2_tax_identity_shadow_artifact_changed"
_PAIR_INVALID = "ptg2_tax_identity_shadow_pair_invalid"


class TaxIdentityShadowAdmissionError(RuntimeError):
    """One redacted shadow-bundle admission failure."""


class _ArtifactSpec(Protocol):
    sidecar_version: int
    path: Path
    byte_count: int
    row_count: int
    record_bytes: int
    token_policy_id: str
    sha256: str


@dataclass(frozen=True, slots=True)
class _ScratchRoot:
    requested_path: Path
    resolved_path: Path
    descriptor: int
    metadata: os.stat_result


@dataclass(frozen=True, slots=True)
class _ArtifactPreflight:
    artifact: _ArtifactSpec
    name: str
    metadata: os.stat_result


@dataclass(frozen=True, slots=True)
class _HeldArtifact:
    preflight: _ArtifactPreflight
    stream: BinaryIO
    metadata: os.stat_result


def _fail(code: str) -> TaxIdentityShadowAdmissionError:
    return TaxIdentityShadowAdmissionError(code)


def _node_identity(metadata: os.stat_result) -> tuple[int, int, int, int]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        stat.S_IFMT(metadata.st_mode),
        metadata.st_uid,
    )


def _file_identity(metadata: os.stat_result) -> tuple[int, ...]:
    return (
        *_node_identity(metadata),
        metadata.st_nlink,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
    )


def _nofollow_flag() -> int:
    value = getattr(os, "O_NOFOLLOW", None)
    if value is None:
        raise _fail(_SCRATCH_INVALID)
    return int(value)


def _nonblock_flag() -> int:
    value = getattr(os, "O_NONBLOCK", None)
    if value is None:
        raise _fail(_SCRATCH_INVALID)
    return int(value)


def _validate_root_metadata(metadata: os.stat_result) -> None:
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != os.geteuid()
        or stat.S_IMODE(metadata.st_mode) & 0o077
    ):
        raise _fail(_SCRATCH_INVALID)


@contextmanager
def _open_scratch_root(raw_root: str | Path) -> Iterator[_ScratchRoot]:
    requested = Path(raw_root)
    if not requested.is_absolute():
        raise _fail(_SCRATCH_INVALID)
    try:
        named_metadata = requested.lstat()
        if stat.S_ISLNK(named_metadata.st_mode):
            raise _fail(_SCRATCH_INVALID)
        _validate_root_metadata(named_metadata)
        resolved = requested.resolve(strict=True)
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
        flags |= getattr(os, "O_DIRECTORY", 0) | _nofollow_flag()
        descriptor = os.open(resolved, flags)
    except TaxIdentityShadowAdmissionError:
        raise
    except (OSError, RuntimeError, ValueError):
        raise _fail(_SCRATCH_INVALID) from None
    try:
        opened_metadata = os.fstat(descriptor)
        _validate_root_metadata(opened_metadata)
        if _node_identity(named_metadata) != _node_identity(opened_metadata):
            raise _fail(_SCRATCH_INVALID)
        yield _ScratchRoot(requested, resolved, descriptor, opened_metadata)
    finally:
        os.close(descriptor)


def _recheck_root(root: _ScratchRoot) -> None:
    try:
        named_metadata = root.requested_path.lstat()
        opened_metadata = os.fstat(root.descriptor)
        _validate_root_metadata(named_metadata)
        _validate_root_metadata(opened_metadata)
    except (OSError, TaxIdentityShadowAdmissionError):
        raise _fail(_ARTIFACT_CHANGED) from None
    expected = _node_identity(root.metadata)
    if _node_identity(named_metadata) != expected or _node_identity(opened_metadata) != expected:
        raise _fail(_ARTIFACT_CHANGED)


def _preflight_artifact(
    root: _ScratchRoot,
    artifact: _ArtifactSpec,
    max_artifact_bytes: int,
) -> _ArtifactPreflight:
    try:
        parent = artifact.path.parent.resolve(strict=True)
        name = artifact.path.name
        if parent != root.resolved_path or name in {"", ".", ".."}:
            raise _fail(_SCRATCH_INVALID)
        metadata = os.stat(name, dir_fd=root.descriptor, follow_symlinks=False)
    except TaxIdentityShadowAdmissionError:
        raise
    except (OSError, RuntimeError, ValueError):
        raise _fail(_ARTIFACT_INVALID) from None
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != os.geteuid()
        or metadata.st_nlink != 1
        or metadata.st_size != artifact.byte_count
        or metadata.st_size > max_artifact_bytes
    ):
        raise _fail(_ARTIFACT_INVALID)
    return _ArtifactPreflight(artifact, name, metadata)


@contextmanager
def _open_artifact(root: _ScratchRoot, preflight: _ArtifactPreflight) -> Iterator[_HeldArtifact]:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | _nofollow_flag()
    flags |= _nonblock_flag()
    try:
        descriptor = os.open(preflight.name, flags, dir_fd=root.descriptor)
    except OSError:
        raise _fail(_ARTIFACT_CHANGED) from None
    try:
        opened_metadata = os.fstat(descriptor)
        if _file_identity(opened_metadata) != _file_identity(preflight.metadata):
            raise _fail(_ARTIFACT_CHANGED)
        with os.fdopen(descriptor, "rb", closefd=False) as stream:
            yield _HeldArtifact(preflight, stream, opened_metadata)
    finally:
        os.close(descriptor)


def _read_exact(stream: BinaryIO, byte_count: int, digest: Any) -> bytes:
    chunks: list[bytes] = []
    remaining = byte_count
    while remaining:
        chunk = stream.read(remaining)
        if not chunk:
            raise _fail(_ARTIFACT_INVALID)
        digest.update(chunk)
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _authenticate_held_artifact(held: _HeldArtifact) -> None:
    artifact = held.preflight.artifact
    digest = hashlib.sha256()
    fixed_header = _read_exact(held.stream, _HEADER_BYTES, digest)
    expected_magic = _V1_MAGIC if artifact.sidecar_version == 1 else _V2_MAGIC
    if (
        fixed_header[:8] != expected_magic
        or int.from_bytes(fixed_header[8:10], "little") != artifact.sidecar_version
        or int.from_bytes(fixed_header[10:12], "little") != artifact.record_bytes
    ):
        raise _fail(_ARTIFACT_INVALID)
    policy_bytes = _read_exact(held.stream, fixed_header[12], digest)
    try:
        header_policy_id = policy_bytes.decode("ascii")
    except UnicodeDecodeError:
        raise _fail(_ARTIFACT_INVALID) from None
    if not hmac.compare_digest(header_policy_id, artifact.token_policy_id):
        raise _fail(_ARTIFACT_INVALID)
    observed_bytes = _HEADER_BYTES + len(policy_bytes)
    remaining_bytes = artifact.byte_count - observed_bytes
    if remaining_bytes < 0:
        raise _fail(_ARTIFACT_INVALID)
    while remaining_bytes:
        requested_bytes = min(remaining_bytes, _HASH_CHUNK_BYTES)
        chunk = held.stream.read(requested_bytes)
        if not chunk or len(chunk) > requested_bytes:
            raise _fail(_ARTIFACT_INVALID)
        digest.update(chunk)
        observed_bytes += len(chunk)
        remaining_bytes -= len(chunk)
    has_extra_bytes = bool(held.stream.read(1))
    payload_bytes = observed_bytes - _HEADER_BYTES - len(policy_bytes)
    if (
        has_extra_bytes
        or observed_bytes != artifact.byte_count
        or payload_bytes < 0
        or payload_bytes % artifact.record_bytes
        or payload_bytes // artifact.record_bytes != artifact.row_count
        or not hmac.compare_digest(digest.hexdigest(), artifact.sha256)
    ):
        raise _fail(_ARTIFACT_INVALID)


def _recheck_artifact(root: _ScratchRoot, held: _HeldArtifact) -> None:
    try:
        opened_metadata = os.fstat(held.stream.fileno())
        named_metadata = os.stat(
            held.preflight.name,
            dir_fd=root.descriptor,
            follow_symlinks=False,
        )
    except OSError:
        raise _fail(_ARTIFACT_CHANGED) from None
    expected = _file_identity(held.metadata)
    if _file_identity(opened_metadata) != expected or _file_identity(named_metadata) != expected:
        raise _fail(_ARTIFACT_CHANGED)


def _is_held_artifact_pair_distinct(v1: _HeldArtifact, v2: _HeldArtifact) -> bool:
    return (v1.metadata.st_dev, v1.metadata.st_ino) != (
        v2.metadata.st_dev,
        v2.metadata.st_ino,
    )


def authenticate_shadow_artifact_pair(
    *,
    scratch_root: str | Path,
    v1: _ArtifactSpec,
    v2: _ArtifactSpec,
    max_artifact_bytes: int,
) -> None:
    """Authenticate both artifacts through simultaneously held file descriptors."""

    with _open_scratch_root(scratch_root) as root:
        first = _preflight_artifact(root, v1, max_artifact_bytes)
        second = _preflight_artifact(root, v2, max_artifact_bytes)
        _recheck_root(root)
        with ExitStack() as stack:
            v1_held = stack.enter_context(_open_artifact(root, first))
            v2_held = stack.enter_context(_open_artifact(root, second))
            if not _is_held_artifact_pair_distinct(v1_held, v2_held):
                raise _fail(_PAIR_INVALID)
            _authenticate_held_artifact(v1_held)
            _authenticate_held_artifact(v2_held)
            _recheck_artifact(root, v1_held)
            _recheck_artifact(root, v2_held)
            _recheck_root(root)
