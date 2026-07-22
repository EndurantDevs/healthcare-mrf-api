# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Descriptor and durability primitives for retained blob installation."""

from __future__ import annotations

import os
import stat
import sys
from dataclasses import dataclass
from pathlib import Path

from process.provider_directory_retained_artifact_base import RetainedArtifactError
from process.provider_directory_retained_blob_store import (
    _CLOSE_FLAGS,
    _DIRECTORY_FLAG,
    _NOFOLLOW_FLAG,
    _TEMP_DIRECTORY,
    _configured_root_components,
    _directory_identity,
    _file_identity,
    _open_directory_at,
    _reject_optional_root_entry,
    retained_artifact_blob_components,
)


_NONBLOCK_FLAG = getattr(os, "O_NONBLOCK", 0)
_TMPFILE_FLAG = getattr(os, "O_TMPFILE", None)
_DIR_FD_OPERATIONS = (os.link, os.mkdir, os.open, os.stat, os.unlink)
_PROC_SELF_FD_DIRECTORY = "/proc/self/fd"


def _combine_secondary_errors(
    primary_error: BaseException,
    secondary_errors: list[BaseException],
) -> None:
    for secondary_error in secondary_errors:
        primary_error.add_note(f"cleanup failure: {secondary_error}")
        for nested_note in getattr(secondary_error, "__notes__", ()):
            primary_error.add_note(nested_note)


def _raise_combined_errors(errors: list[RetainedArtifactError]) -> None:
    if not errors:
        return
    primary_error = errors[0]
    _combine_secondary_errors(primary_error, errors[1:])
    raise primary_error


def _close_descriptor(
    descriptor: int,
    error_code: str,
) -> RetainedArtifactError | None:
    try:
        os.close(descriptor)
    except BaseException as error:
        close_error = RetainedArtifactError(error_code)
        close_error.__cause__ = error
        return close_error
    return None


def _close_owned_descriptor(descriptor: int, error_code: str) -> None:
    close_error = _close_descriptor(descriptor, error_code)
    if close_error is not None:
        raise close_error


def _close_descriptor_sequence(
    descriptors: list[int],
    error_code: str,
) -> None:
    owned_descriptors = tuple(reversed(descriptors))
    descriptors.clear()
    errors = [
        error
        for descriptor in owned_descriptors
        if (error := _close_descriptor(descriptor, error_code)) is not None
    ]
    _raise_combined_errors(errors)


def _close_after_error(
    primary_error: BaseException,
    cleanup_action,
) -> None:
    try:
        cleanup_action()
    except BaseException as cleanup_error:
        _combine_secondary_errors(primary_error, [cleanup_error])


@dataclass
class _OpenedDirectoryChain:
    descriptors: list[int]
    identities: tuple[tuple[int, int, int], ...]
    components: tuple[str, ...]

    @property
    def parent_descriptor(self) -> int:
        """Return the descriptor that owns this chain's leaf entry."""
        return self.descriptors[-1]

    def close(self) -> None:
        """Close every descriptor retained to pin the directory chain."""
        _close_descriptor_sequence(
            self.descriptors,
            "retained_blob_directory_close_failed",
        )


@dataclass
class _OpenedFile:
    descriptor: int
    directory_chain: _OpenedDirectoryChain
    leaf_name: str

    def close(self) -> None:
        """Close the file and its pinned parent chain once."""
        close_errors: list[RetainedArtifactError] = []
        if self.descriptor >= 0:
            descriptor = self.descriptor
            self.descriptor = -1
            close_error = _close_descriptor(
                descriptor,
                "retained_blob_source_close_failed",
            )
            if close_error is not None:
                close_errors.append(close_error)
        try:
            self.directory_chain.close()
        except RetainedArtifactError as close_error:
            close_errors.append(close_error)
        _raise_combined_errors(close_errors)


@dataclass
class _BlobInstallTarget:
    directory_chain: _OpenedDirectoryChain
    leaf_name: str
    durability_start_index: int

    def sync_directories(self) -> None:
        """Fsync the blob directory chain through the configured root."""
        for descriptor in reversed(
            self.directory_chain.descriptors[self.durability_start_index :]
        ):
            _sync_directory(descriptor)

    def close(self) -> None:
        """Close the pinned canonical target directory chain."""
        self.directory_chain.close()


@dataclass
class _TemporaryBlob:
    descriptor: int

    def close(self) -> None:
        """Close the temporary write descriptor once."""
        if self.descriptor >= 0:
            descriptor = self.descriptor
            self.descriptor = -1
            close_error = _close_descriptor(
                descriptor,
                "retained_blob_temporary_close_failed",
            )
            if close_error is not None:
                raise close_error


def _require_descriptor_install_platform() -> None:
    if _DIRECTORY_FLAG is None or _NOFOLLOW_FLAG is None:
        raise RetainedArtifactError("retained_blob_install_unavailable")
    if any(operation not in os.supports_dir_fd for operation in _DIR_FD_OPERATIONS):
        raise RetainedArtifactError("retained_blob_install_unavailable")


def _require_install_platform() -> None:
    _require_descriptor_install_platform()
    if not sys.platform.startswith("linux") or _TMPFILE_FLAG is None:
        raise RetainedArtifactError("retained_blob_install_unavailable")
    try:
        proc_fd_state = os.stat(_PROC_SELF_FD_DIRECTORY)
    except OSError as error:
        raise RetainedArtifactError("retained_blob_install_unavailable") from error
    if not stat.S_ISDIR(proc_fd_state.st_mode):
        raise RetainedArtifactError("retained_blob_install_unavailable")


def _link_open_descriptor(
    source_descriptor: int,
    target_directory_descriptor: int,
    target_leaf_name: str,
) -> None:
    """Atomically name the already-open inode through Linux procfs linkat."""
    try:
        _file_identity(os.fstat(source_descriptor))
    except (OSError, RetainedArtifactError) as error:
        raise RetainedArtifactError(
            "retained_blob_publish_identity_unavailable"
        ) from error
    source_descriptor_path = f"{_PROC_SELF_FD_DIRECTORY}/{source_descriptor}"
    try:
        os.link(
            source_descriptor_path,
            target_leaf_name,
            dst_dir_fd=target_directory_descriptor,
        )
    except FileExistsError:
        raise
    except (NotImplementedError, OSError) as error:
        raise RetainedArtifactError("retained_blob_publish_failed") from error


def _sync_directory(directory_descriptor: int) -> None:
    try:
        os.fsync(directory_descriptor)
    except OSError as error:
        raise RetainedArtifactError("retained_blob_durability_failed") from error


def _sync_file(file_descriptor: int) -> None:
    try:
        os.fsync(file_descriptor)
    except OSError as error:
        raise RetainedArtifactError("retained_blob_durability_failed") from error


def _open_or_create_directory(parent_descriptor: int, component: str) -> int:
    try:
        os.mkdir(component, mode=0o700, dir_fd=parent_descriptor)
    except FileExistsError:
        _sync_directory(parent_descriptor)
        return _open_directory_at(parent_descriptor, component)
    except OSError as error:
        raise RetainedArtifactError("retained_artifact_path_unsafe") from error
    _sync_directory(parent_descriptor)
    return _open_directory_at(parent_descriptor, component)


def _open_directory_chain(
    directory_components: tuple[str, ...],
) -> tuple[list[int], tuple[tuple[int, int, int], ...]]:
    descriptors: list[int] = []
    try:
        descriptors.append(os.open("/", _CLOSE_FLAGS | _DIRECTORY_FLAG))
        for component in directory_components:
            descriptors.append(_open_directory_at(descriptors[-1], component))
        identities = tuple(
            _directory_identity(os.fstat(descriptor)) for descriptor in descriptors
        )
        return descriptors, identities
    except OSError as error:
        primary_error = RetainedArtifactError("retained_artifact_path_unsafe")
        _close_after_error(
            primary_error,
            lambda: _close_descriptor_sequence(
                descriptors,
                "retained_blob_directory_close_failed",
            ),
        )
        raise primary_error from error
    except BaseException as primary_error:
        _close_after_error(
            primary_error,
            lambda: _close_descriptor_sequence(
                descriptors,
                "retained_blob_directory_close_failed",
            ),
        )
        raise


def _open_install_target(artifact_sha256: str) -> _BlobInstallTarget:
    _require_descriptor_install_platform()
    root_components = _configured_root_components()
    blob_components = retained_artifact_blob_components(artifact_sha256)
    descriptors, _root_identities = _open_directory_chain(root_components)
    try:
        _reject_optional_root_entry(descriptors[-1], _TEMP_DIRECTORY)
        for component in blob_components[:-1]:
            descriptors.append(_open_or_create_directory(descriptors[-1], component))
        identities = tuple(
            _directory_identity(os.fstat(descriptor)) for descriptor in descriptors
        )
    except OSError as error:
        primary_error = RetainedArtifactError("retained_artifact_path_unsafe")
        _close_after_error(
            primary_error,
            lambda: _close_descriptor_sequence(
                descriptors,
                "retained_blob_directory_close_failed",
            ),
        )
        raise primary_error from error
    except BaseException as primary_error:
        _close_after_error(
            primary_error,
            lambda: _close_descriptor_sequence(
                descriptors,
                "retained_blob_directory_close_failed",
            ),
        )
        raise
    return _BlobInstallTarget(
        directory_chain=_OpenedDirectoryChain(
            descriptors=descriptors,
            identities=identities,
            components=root_components + blob_components[:-1],
        ),
        leaf_name=blob_components[-1],
        durability_start_index=len(root_components),
    )


def _absolute_file_components(
    source_path: str | os.PathLike[str],
) -> tuple[tuple[str, ...], str]:
    try:
        raw_source_path = os.fspath(source_path)
    except TypeError as error:
        raise RetainedArtifactError("retained_blob_source_path_invalid") from error
    except (OSError, ValueError) as error:
        raise RetainedArtifactError("retained_blob_source_path_unsafe") from error
    if not isinstance(raw_source_path, str):
        raise RetainedArtifactError("retained_blob_source_path_invalid")
    if "\x00" in raw_source_path:
        raise RetainedArtifactError("retained_blob_source_path_unsafe")
    parsed_source_path = Path(raw_source_path)
    if (
        not parsed_source_path.is_absolute()
        or ".." in parsed_source_path.parts
        or not parsed_source_path.name
    ):
        raise RetainedArtifactError("retained_blob_source_path_invalid")
    return tuple(parsed_source_path.parts[1:-1]), parsed_source_path.name


def _open_source(source_path: str | os.PathLike[str]) -> _OpenedFile:
    _require_descriptor_install_platform()
    directory_components, leaf_name = _absolute_file_components(source_path)
    descriptors, identities = _open_directory_chain(directory_components)
    descriptor = -1
    try:
        descriptor = os.open(
            leaf_name,
            _CLOSE_FLAGS | _NOFOLLOW_FLAG | _NONBLOCK_FLAG,
            dir_fd=descriptors[-1],
        )
        _file_identity(os.fstat(descriptor))
    except (OSError, ValueError, RetainedArtifactError) as error:
        primary_error = RetainedArtifactError("retained_blob_source_path_unsafe")
        if descriptor >= 0:
            _close_after_error(
                primary_error,
                lambda: _close_owned_descriptor(
                    descriptor,
                    "retained_blob_source_close_failed",
                ),
            )
        _close_after_error(
            primary_error,
            lambda: _close_descriptor_sequence(
                descriptors,
                "retained_blob_directory_close_failed",
            ),
        )
        raise primary_error from error
    except BaseException as primary_error:
        if descriptor >= 0:
            _close_after_error(
                primary_error,
                lambda: _close_owned_descriptor(
                    descriptor,
                    "retained_blob_source_close_failed",
                ),
            )
        _close_after_error(
            primary_error,
            lambda: _close_descriptor_sequence(
                descriptors,
                "retained_blob_directory_close_failed",
            ),
        )
        raise
    return _OpenedFile(
        descriptor=descriptor,
        directory_chain=_OpenedDirectoryChain(
            descriptors=descriptors,
            identities=identities,
            components=directory_components,
        ),
        leaf_name=leaf_name,
    )


def _open_optional_target(target: _BlobInstallTarget) -> int:
    try:
        return os.open(
            target.leaf_name,
            _CLOSE_FLAGS | _NOFOLLOW_FLAG,
            dir_fd=target.directory_chain.parent_descriptor,
        )
    except FileNotFoundError:
        return -1
    except OSError as error:
        raise RetainedArtifactError("retained_blob_path_unsafe") from error


def _inode_identity(entry_state: os.stat_result) -> tuple[int, int]:
    return (entry_state.st_dev, entry_state.st_ino)


def _named_inode_identity(
    parent_descriptor: int,
    leaf_name: str,
) -> tuple[int, int] | None:
    try:
        entry_state = os.stat(
            leaf_name,
            dir_fd=parent_descriptor,
            follow_symlinks=False,
        )
    except FileNotFoundError:
        return None
    except OSError as error:
        raise RetainedArtifactError("retained_blob_identity_unavailable") from error
    return _inode_identity(entry_state)


def _create_temporary_blob(
    target: _BlobInstallTarget,
    artifact_sha256: str,
) -> _TemporaryBlob:
    del artifact_sha256
    if _TMPFILE_FLAG is None:
        raise RetainedArtifactError("retained_blob_install_unavailable")
    try:
        descriptor = os.open(
            ".",
            os.O_RDWR
            | _TMPFILE_FLAG
            | getattr(os, "O_CLOEXEC", 0)
            | _NOFOLLOW_FLAG,
            0o600,
            dir_fd=target.directory_chain.parent_descriptor,
        )
    except OSError as error:
        raise RetainedArtifactError("retained_blob_install_unavailable") from error
    return _TemporaryBlob(descriptor=descriptor)


from process.provider_directory_retained_blob_verification import (
    _copy_verified_source,
    _verify_descriptor,
    _verify_path_identity,
    _verify_source,
    _verify_target,
)
