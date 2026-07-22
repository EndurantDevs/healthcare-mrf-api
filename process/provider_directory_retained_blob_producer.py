# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Recoverable installation for canonical retained artifact blobs."""

from __future__ import annotations

import os
from collections.abc import Callable

from process import provider_directory_retained_blob_install_support as install_io
from process.provider_directory_retained_artifact_base import (
    RetainedArtifactError,
    require_digest,
    require_positive_int,
)


_require_install_platform = install_io._require_install_platform
_link_open_descriptor = install_io._link_open_descriptor
_create_temporary_blob = install_io._create_temporary_blob


def _publish_temporary_blob(
    temporary: install_io._TemporaryBlob,
    target: install_io._BlobInstallTarget,
) -> None:
    try:
        _link_open_descriptor(
            temporary.descriptor,
            target.directory_chain.parent_descriptor,
            target.leaf_name,
        )
    except FileExistsError:
        return
    except RetainedArtifactError:
        raise
    except OSError as error:
        raise RetainedArtifactError("retained_blob_publish_failed") from error


def _combine_cleanup_errors(
    primary_error: BaseException,
    cleanup_errors: list[BaseException],
) -> None:
    for cleanup_error in cleanup_errors:
        primary_error.add_note(f"cleanup failure: {cleanup_error}")
        for nested_note in getattr(cleanup_error, "__notes__", ()):
            primary_error.add_note(nested_note)


def _remove_temporary_blob(temporary: install_io._TemporaryBlob) -> None:
    temporary.close()


def _cleanup_failed_install(
    primary_error: BaseException,
    temporary: install_io._TemporaryBlob,
) -> None:
    try:
        _remove_temporary_blob(temporary)
    except BaseException as cleanup_error:
        _combine_cleanup_errors(primary_error, [cleanup_error])


def _install_missing_blob(
    source_file: install_io._OpenedFile,
    install_target: install_io._BlobInstallTarget,
    artifact_sha256: str,
    artifact_byte_count: int,
) -> None:
    temporary = _create_temporary_blob(install_target, artifact_sha256)
    try:
        source_identity = install_io._copy_verified_source(
            source_file,
            temporary,
            artifact_sha256,
            artifact_byte_count,
        )
        install_io._verify_path_identity(
            source_file.directory_chain,
            source_file.leaf_name,
            source_identity,
            "retained_blob_source_identity_changed",
        )
        _publish_temporary_blob(temporary, install_target)
        install_io._sync_directory(
            install_target.directory_chain.parent_descriptor
        )
        _remove_temporary_blob(temporary)
        install_io._verify_target(
            install_target,
            artifact_sha256,
            artifact_byte_count,
        )
    except BaseException as primary_error:
        _cleanup_failed_install(
            primary_error,
            temporary,
        )
        raise


def _install_or_verify_blob(
    source_file: install_io._OpenedFile,
    target: install_io._BlobInstallTarget,
    artifact_sha256: str,
    artifact_byte_count: int,
) -> None:
    existing_descriptor = install_io._open_optional_target(target)
    if existing_descriptor < 0:
        _install_missing_blob(
            source_file,
            target,
            artifact_sha256,
            artifact_byte_count,
        )
        return
    install_io._close_owned_descriptor(
        existing_descriptor,
        "retained_blob_target_close_failed",
    )
    install_io._verify_source(source_file, artifact_sha256, artifact_byte_count)
    install_io._verify_target(target, artifact_sha256, artifact_byte_count)


def _run_with_cleanup(
    operation: Callable[[], None],
    cleanup: Callable[[], None],
) -> None:
    try:
        operation()
    except BaseException as primary_error:
        try:
            cleanup()
        except BaseException as cleanup_error:
            _combine_cleanup_errors(primary_error, [cleanup_error])
        raise
    cleanup()


def _install_with_target(
    source_file: install_io._OpenedFile,
    artifact_sha256: str,
    artifact_byte_count: int,
) -> None:
    _require_install_platform()
    target = install_io._open_install_target(artifact_sha256)
    _run_with_cleanup(
        lambda: _install_or_verify_blob(
            source_file,
            target,
            artifact_sha256,
            artifact_byte_count,
        ),
        target.close,
    )


def install_retained_artifact_blob(
    source_path: str | os.PathLike[str],
    *,
    artifact_sha256: str,
    artifact_byte_count: int,
) -> None:
    """Install one inode-bound blob; failed visible publications stay retryable."""
    artifact_digest = require_digest(artifact_sha256, "artifact_sha256")
    expected_byte_count = require_positive_int(
        artifact_byte_count, "artifact_byte_count"
    )
    source_file = install_io._open_source(source_path)
    _run_with_cleanup(
        lambda: _install_with_target(
            source_file,
            artifact_digest,
            expected_byte_count,
        ),
        source_file.close,
    )


__all__ = ("install_retained_artifact_blob",)
