# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Safe content-addressed layout and reads for retained directory artifacts."""

from __future__ import annotations

import os
import stat
import tempfile
from pathlib import Path

from process.provider_directory_retained_artifact_base import (
    RetainedArtifactError,
    require_digest,
    require_positive_int,
)


ARTIFACT_ROOT_ENV = "HLTHPRT_PROVIDER_DIRECTORY_ARTIFACT_ROOT"
_BLOB_DIRECTORY = "blobs"
_TEMP_DIRECTORY = "tmp"
_CLOSE_FLAGS = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
_DIRECTORY_FLAG = getattr(os, "O_DIRECTORY", None)
_NOFOLLOW_FLAG = getattr(os, "O_NOFOLLOW", None)


def retained_artifact_blob_components(artifact_sha256: str) -> tuple[str, ...]:
    """Return the sole canonical relative layout shared with artifact writers."""

    artifact_digest = require_digest(artifact_sha256, "artifact_sha256")
    return (
        _BLOB_DIRECTORY,
        artifact_digest[:2],
        artifact_digest[2:4],
        artifact_digest,
    )


def _configured_root_components() -> tuple[str, ...]:
    configured_root = os.getenv(ARTIFACT_ROOT_ENV)
    if configured_root is None or not configured_root.strip():
        raise RetainedArtifactError("retained_artifact_root_required")
    raw_root = Path(configured_root)
    if not raw_root.is_absolute() or ".." in raw_root.parts:
        raise RetainedArtifactError("retained_artifact_root_invalid")
    artifact_root = Path(os.path.abspath(raw_root))
    if artifact_root == Path(artifact_root.anchor):
        raise RetainedArtifactError("retained_artifact_root_invalid")
    _reject_temporary_artifact_root(artifact_root)
    return tuple(artifact_root.parts[1:])


def _reject_temporary_artifact_root(artifact_root: Path) -> None:
    temporary_roots = {
        Path(os.path.realpath(tempfile.gettempdir())),
        Path("/tmp"),
        Path("/var/tmp"),
        Path("/private/tmp"),
    }
    normalized_root = Path(os.path.realpath(artifact_root))
    if any(
        temporary_root == normalized_root or temporary_root in normalized_root.parents
        for temporary_root in temporary_roots
    ):
        raise RetainedArtifactError("retained_artifact_root_temporary")


def _directory_identity(directory_state: os.stat_result) -> tuple[int, int, int]:
    if stat.S_ISLNK(directory_state.st_mode) or not stat.S_ISDIR(
        directory_state.st_mode
    ):
        raise RetainedArtifactError("retained_artifact_path_unsafe")
    return (
        directory_state.st_dev,
        directory_state.st_ino,
        directory_state.st_mode,
    )


def _file_identity(file_state: os.stat_result) -> tuple[int, int, int, int, int, int]:
    if stat.S_ISLNK(file_state.st_mode) or not stat.S_ISREG(file_state.st_mode):
        raise RetainedArtifactError("retained_blob_path_unsafe")
    return (
        file_state.st_dev,
        file_state.st_ino,
        file_state.st_mode,
        file_state.st_size,
        file_state.st_mtime_ns,
        file_state.st_ctime_ns,
    )


def _require_descriptor_relative_platform() -> None:
    if _DIRECTORY_FLAG is None or _NOFOLLOW_FLAG is None:
        raise RetainedArtifactError("retained_blob_openat_unavailable")
    if os.open not in os.supports_dir_fd or os.stat not in os.supports_dir_fd:
        raise RetainedArtifactError("retained_blob_openat_unavailable")


def _open_directory_at(parent_descriptor: int, component: str) -> int:
    try:
        return os.open(
            component,
            _CLOSE_FLAGS | _DIRECTORY_FLAG | _NOFOLLOW_FLAG,
            dir_fd=parent_descriptor,
        )
    except OSError as error:
        raise RetainedArtifactError("retained_artifact_path_unsafe") from error


def _close_descriptors(descriptors: list[int] | tuple[int, ...]) -> None:
    for descriptor in reversed(descriptors):
        os.close(descriptor)


def _open_directory_chain(
    directory_components: tuple[str, ...],
) -> tuple[list[int], tuple[tuple[int, int, int], ...]]:
    _require_descriptor_relative_platform()
    descriptors = [os.open("/", _CLOSE_FLAGS | _DIRECTORY_FLAG)]
    try:
        for component in directory_components:
            descriptors.append(_open_directory_at(descriptors[-1], component))
        identities = tuple(
            _directory_identity(os.fstat(descriptor)) for descriptor in descriptors
        )
        return descriptors, identities
    except BaseException:
        _close_descriptors(descriptors)
        raise


def _reject_optional_root_entry(root_descriptor: int, entry_name: str) -> None:
    try:
        entry_state = os.stat(
            entry_name,
            dir_fd=root_descriptor,
            follow_symlinks=False,
        )
    except FileNotFoundError:
        return
    except OSError as error:
        raise RetainedArtifactError("retained_artifact_path_unsafe") from error
    _directory_identity(entry_state)


def _open_blob_directory_chain(
    artifact_sha256: str,
) -> tuple[
    list[int],
    tuple[tuple[int, int, int], ...],
    tuple[str, ...],
    str,
]:
    root_components = _configured_root_components()
    blob_components = retained_artifact_blob_components(artifact_sha256)
    root_descriptors, _root_identities = _open_directory_chain(root_components)
    try:
        root_descriptor = root_descriptors[-1]
        _reject_optional_root_entry(root_descriptor, _TEMP_DIRECTORY)
        descriptors = root_descriptors
        for component in blob_components[:-1]:
            descriptors.append(_open_directory_at(descriptors[-1], component))
        identities = tuple(
            _directory_identity(os.fstat(descriptor)) for descriptor in descriptors
        )
        return (
            descriptors,
            identities,
            root_components + blob_components[:-1],
            blob_components[-1],
        )
    except BaseException:
        _close_descriptors(root_descriptors)
        raise


def _named_file_identity(parent_descriptor: int, leaf_name: str):
    try:
        file_state = os.stat(
            leaf_name,
            dir_fd=parent_descriptor,
            follow_symlinks=False,
        )
    except OSError as error:
        raise RetainedArtifactError("retained_blob_identity_changed") from error
    return _file_identity(file_state)


def _open_blob_leaf(parent_descriptor: int, leaf_name: str) -> int:
    try:
        return os.open(
            leaf_name,
            _CLOSE_FLAGS | _NOFOLLOW_FLAG,
            dir_fd=parent_descriptor,
        )
    except FileNotFoundError as error:
        raise RetainedArtifactError("retained_blob_unavailable") from error
    except OSError as error:
        raise RetainedArtifactError("retained_blob_path_unsafe") from error


class _OpenedArtifactBlob:
    """Private openat descriptor chain that never leaves the retained reader."""

    __slots__ = (
        "_descriptor",
        "_directory_components",
        "_directory_descriptors",
        "_directory_identities",
        "_file_identity",
        "_leaf_name",
    )

    def __init__(
        self,
        descriptor: int,
        directory_components: tuple[str, ...],
        directory_descriptors: list[int],
        directory_identities: tuple[tuple[int, int, int], ...],
        leaf_name: str,
        file_identity: tuple[int, int, int, int, int, int],
    ):
        self._descriptor = descriptor
        self._directory_components = directory_components
        self._directory_descriptors = directory_descriptors
        self._directory_identities = directory_identities
        self._leaf_name = leaf_name
        self._file_identity = file_identity

    def read_at(self, byte_count: int, offset: int) -> bytes:
        """Read one bounded chunk without sharing the descriptor or root."""

        try:
            return os.pread(self._descriptor, byte_count, offset)
        except OSError as error:
            raise RetainedArtifactError("retained_blob_read_failed") from error

    def verify_and_close(self) -> None:
        """Reopen the directory chain, verify every inode, and close."""

        if self._descriptor < 0:
            raise RetainedArtifactError("retained_blob_reader_closed")
        fresh_descriptors: list[int] = []
        try:
            fresh_descriptors, fresh_identities = _open_directory_chain(
                self._directory_components
            )
            original_identities = tuple(
                _directory_identity(os.fstat(descriptor))
                for descriptor in self._directory_descriptors
            )
            current_file_identity = _file_identity(os.fstat(self._descriptor))
            original_name_identity = _named_file_identity(
                self._directory_descriptors[-1], self._leaf_name
            )
            fresh_name_identity = _named_file_identity(
                fresh_descriptors[-1], self._leaf_name
            )
            if not (
                fresh_identities == original_identities == self._directory_identities
                and current_file_identity
                == original_name_identity
                == fresh_name_identity
                == self._file_identity
            ):
                raise RetainedArtifactError("retained_blob_identity_changed")
        except OSError as error:
            raise RetainedArtifactError("retained_blob_identity_changed") from error
        finally:
            _close_descriptors(fresh_descriptors)
            self.abort()

    def abort(self) -> None:
        """Close an unfinished reader and every retained directory descriptor."""

        if self._descriptor >= 0:
            os.close(self._descriptor)
            self._descriptor = -1
        if self._directory_descriptors:
            _close_descriptors(self._directory_descriptors)
            self._directory_descriptors = []


def _open_retained_artifact_blob(
    artifact_sha256: str,
    artifact_byte_count: int,
) -> _OpenedArtifactBlob:
    require_positive_int(artifact_byte_count, "artifact_byte_count")
    (
        directory_descriptors,
        directory_identities,
        directory_components,
        leaf_name,
    ) = _open_blob_directory_chain(artifact_sha256)
    descriptor = -1
    try:
        descriptor = _open_blob_leaf(directory_descriptors[-1], leaf_name)
        file_identity = _file_identity(os.fstat(descriptor))
        named_identity = _named_file_identity(directory_descriptors[-1], leaf_name)
        if file_identity != named_identity or file_identity[3] != artifact_byte_count:
            raise RetainedArtifactError("retained_blob_identity_mismatch")
    except BaseException:
        if descriptor >= 0:
            os.close(descriptor)
        _close_descriptors(directory_descriptors)
        raise
    return _OpenedArtifactBlob(
        descriptor,
        directory_components,
        directory_descriptors,
        directory_identities,
        leaf_name,
        file_identity,
    )


__all__ = ("ARTIFACT_ROOT_ENV", "retained_artifact_blob_components")
