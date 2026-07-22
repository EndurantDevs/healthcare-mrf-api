# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Content and descriptor-chain verification for retained blob installation."""

from __future__ import annotations

import hashlib
import os
from typing import TYPE_CHECKING

from process import provider_directory_retained_blob_install_support as install_io
from process.provider_directory_retained_artifact_base import RetainedArtifactError
from process.provider_directory_retained_blob_store import (
    _directory_identity,
    _file_identity,
    _named_file_identity,
)


if TYPE_CHECKING:
    from process.provider_directory_retained_blob_install_support import (
        _BlobInstallTarget,
        _OpenedDirectoryChain,
        _OpenedFile,
        _TemporaryBlob,
    )


_READ_CHUNK_BYTES = 4 * 1024 * 1024


def _verify_descriptor(
    descriptor: int,
    artifact_sha256: str,
    artifact_byte_count: int,
    mismatch_code: str,
) -> tuple[int, int, int, int, int, int]:
    try:
        before_identity = _file_identity(os.fstat(descriptor))
        if before_identity[3] != artifact_byte_count:
            raise RetainedArtifactError(mismatch_code)
        artifact_digest = hashlib.sha256()
        read_offset = 0
        while read_offset < artifact_byte_count:
            byte_chunk = os.pread(
                descriptor,
                min(_READ_CHUNK_BYTES, artifact_byte_count - read_offset),
                read_offset,
            )
            if not byte_chunk:
                raise RetainedArtifactError(mismatch_code)
            artifact_digest.update(byte_chunk)
            read_offset += len(byte_chunk)
        after_identity = _file_identity(os.fstat(descriptor))
    except OSError as error:
        raise RetainedArtifactError(mismatch_code) from error
    if (
        after_identity != before_identity
        or read_offset != artifact_byte_count
        or artifact_digest.hexdigest() != artifact_sha256
    ):
        raise RetainedArtifactError(mismatch_code)
    return before_identity


def _verify_path_identity(
    directory_chain: _OpenedDirectoryChain,
    leaf_name: str,
    file_identity: tuple[int, int, int, int, int, int],
    mismatch_code: str,
) -> None:
    fresh_descriptors: list[int] = []
    try:
        fresh_descriptors, fresh_identities = install_io._open_directory_chain(
            directory_chain.components
        )
        current_identities = tuple(
            _directory_identity(os.fstat(descriptor))
            for descriptor in directory_chain.descriptors
        )
        original_name_identity = _named_file_identity(
            directory_chain.parent_descriptor, leaf_name
        )
        fresh_name_identity = _named_file_identity(
            fresh_descriptors[-1], leaf_name
        )
        if not (
            fresh_identities == current_identities == directory_chain.identities
            and original_name_identity == fresh_name_identity == file_identity
        ):
            raise RetainedArtifactError(mismatch_code)
    except (OSError, RetainedArtifactError) as error:
        if isinstance(error, RetainedArtifactError) and str(error) == mismatch_code:
            primary_error = error
        else:
            primary_error = RetainedArtifactError(mismatch_code)
        install_io._close_after_error(
            primary_error,
            lambda: install_io._close_descriptor_sequence(
                fresh_descriptors,
                "retained_blob_directory_close_failed",
            ),
        )
        if primary_error is error:
            raise
        raise primary_error from error
    except BaseException as primary_error:
        install_io._close_after_error(
            primary_error,
            lambda: install_io._close_descriptor_sequence(
                fresh_descriptors,
                "retained_blob_directory_close_failed",
            ),
        )
        raise
    install_io._close_descriptor_sequence(
        fresh_descriptors,
        "retained_blob_directory_close_failed",
    )


def _verify_source(
    source_file: _OpenedFile,
    artifact_sha256: str,
    artifact_byte_count: int,
) -> None:
    file_identity = _verify_descriptor(
        source_file.descriptor,
        artifact_sha256,
        artifact_byte_count,
        "retained_blob_source_mismatch",
    )
    _verify_path_identity(
        source_file.directory_chain,
        source_file.leaf_name,
        file_identity,
        "retained_blob_source_identity_changed",
    )


def _verify_target(
    install_target: _BlobInstallTarget,
    artifact_sha256: str,
    artifact_byte_count: int,
) -> None:
    descriptor = install_io._open_optional_target(install_target)
    if descriptor < 0:
        raise RetainedArtifactError("retained_blob_unavailable")
    try:
        file_identity = _verify_descriptor(
            descriptor,
            artifact_sha256,
            artifact_byte_count,
            "retained_blob_identity_mismatch",
        )
        install_io._sync_file(descriptor)
        install_target.sync_directories()
        _verify_path_identity(
            install_target.directory_chain,
            install_target.leaf_name,
            file_identity,
            "retained_blob_identity_changed",
        )
    except BaseException as primary_error:
        install_io._close_after_error(
            primary_error,
            lambda: install_io._close_owned_descriptor(
                descriptor,
                "retained_blob_target_close_failed",
            ),
        )
        raise
    install_io._close_owned_descriptor(
        descriptor,
        "retained_blob_target_close_failed",
    )


def _write_all(descriptor: int, byte_chunk: bytes) -> None:
    unwritten_bytes = memoryview(byte_chunk)
    while unwritten_bytes:
        written_byte_count = os.write(descriptor, unwritten_bytes)
        if written_byte_count <= 0:
            raise RetainedArtifactError("retained_blob_install_failed")
        unwritten_bytes = unwritten_bytes[written_byte_count:]


def _copy_verified_source(
    source_file: _OpenedFile,
    temporary_blob: _TemporaryBlob,
    artifact_sha256: str,
    artifact_byte_count: int,
) -> tuple[int, int, int, int, int, int]:
    try:
        source_identity = _file_identity(os.fstat(source_file.descriptor))
        if source_identity[3] != artifact_byte_count:
            raise RetainedArtifactError("retained_blob_source_mismatch")
        artifact_digest = hashlib.sha256()
        copied_byte_count = 0
        while copied_byte_count < artifact_byte_count:
            byte_chunk = os.pread(
                source_file.descriptor,
                min(_READ_CHUNK_BYTES, artifact_byte_count - copied_byte_count),
                copied_byte_count,
            )
            if not byte_chunk:
                raise RetainedArtifactError("retained_blob_source_mismatch")
            artifact_digest.update(byte_chunk)
            _write_all(temporary_blob.descriptor, byte_chunk)
            copied_byte_count += len(byte_chunk)
        install_io._sync_file(temporary_blob.descriptor)
        current_source_identity = _file_identity(os.fstat(source_file.descriptor))
    except OSError as error:
        raise RetainedArtifactError("retained_blob_install_failed") from error
    if (
        current_source_identity != source_identity
        or copied_byte_count != artifact_byte_count
        or artifact_digest.hexdigest() != artifact_sha256
    ):
        raise RetainedArtifactError("retained_blob_source_mismatch")
    return source_identity
