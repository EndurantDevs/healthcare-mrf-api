# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import hashlib
import os
import stat
from pathlib import Path

import pytest

from process import provider_directory_retained_blob_install_support as install_io
from process import provider_directory_retained_blob_producer as blob_producer
from process.provider_directory_retained_artifact_contract import RetainedArtifactError
from process.provider_directory_retained_blob_producer import (
    install_retained_artifact_blob,
)


pytest_plugins = ("tests.provider_directory_retained_reader_fixtures",)


def test_existing_target_file_sync_failure_is_wrapped(
    monkeypatch,
    retained_artifact_test_root: Path,
) -> None:
    artifact_bytes = b"existing file durability\n"
    artifact_sha256 = hashlib.sha256(artifact_bytes).hexdigest()
    source_path = retained_artifact_test_root / "existing-sync-source"
    source_path.write_bytes(artifact_bytes)
    install_retained_artifact_blob(
        source_path,
        artifact_sha256=artifact_sha256,
        artifact_byte_count=len(artifact_bytes),
    )
    original_fsync = install_io.os.fsync

    def fail_regular_file_sync(descriptor: int) -> None:
        if stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise OSError("fixture file sync failure")
        original_fsync(descriptor)

    monkeypatch.setattr(install_io.os, "fsync", fail_regular_file_sync)
    with pytest.raises(RetainedArtifactError, match="durability_failed"):
        install_retained_artifact_blob(
            source_path,
            artifact_sha256=artifact_sha256,
            artifact_byte_count=len(artifact_bytes),
        )


def test_verified_descriptor_rejects_wrong_size(
    retained_artifact_test_root: Path,
) -> None:
    source_path = retained_artifact_test_root / "wrong-size-source"
    source_path.write_bytes(b"size")
    descriptor = os.open(source_path, os.O_RDONLY)
    try:
        with pytest.raises(RetainedArtifactError, match="fixture_mismatch"):
            install_io._verify_descriptor(
                descriptor,
                hashlib.sha256(b"size").hexdigest(),
                5,
                "fixture_mismatch",
            )
    finally:
        os.close(descriptor)


def test_verified_descriptor_initial_fstat_failure_is_typed(
    monkeypatch,
    retained_artifact_test_root: Path,
) -> None:
    source_path = retained_artifact_test_root / "verify-fstat-source"
    source_path.write_bytes(b"fstat")
    descriptor = os.open(source_path, os.O_RDONLY)
    original_fstat = install_io.os.fstat

    def fail_descriptor_fstat(current_descriptor: int):
        if current_descriptor == descriptor:
            raise OSError("fixture verify fstat failure")
        return original_fstat(current_descriptor)

    monkeypatch.setattr(install_io.os, "fstat", fail_descriptor_fstat)
    try:
        with pytest.raises(RetainedArtifactError, match="fixture_mismatch"):
            install_io._verify_descriptor(
                descriptor,
                hashlib.sha256(b"fstat").hexdigest(),
                5,
                "fixture_mismatch",
            )
    finally:
        monkeypatch.setattr(install_io.os, "fstat", original_fstat)
        os.close(descriptor)


def test_copy_initial_fstat_failure_is_typed(
    monkeypatch,
    retained_artifact_test_root: Path,
) -> None:
    artifact_bytes = b"copy-fstat"
    artifact_sha256 = hashlib.sha256(artifact_bytes).hexdigest()
    source_path = retained_artifact_test_root / "copy-fstat-source"
    source_path.write_bytes(artifact_bytes)
    source_file = install_io._open_source(source_path)
    install_target = install_io._open_install_target(artifact_sha256)
    temporary_blob = blob_producer._create_temporary_blob(
        install_target,
        artifact_sha256,
    )
    original_fstat = install_io.os.fstat

    def fail_source_fstat(descriptor: int):
        if descriptor == source_file.descriptor:
            raise OSError("fixture copy fstat failure")
        return original_fstat(descriptor)

    monkeypatch.setattr(install_io.os, "fstat", fail_source_fstat)
    with pytest.raises(RetainedArtifactError, match="install_failed"):
        install_io._copy_verified_source(
            source_file,
            temporary_blob,
            artifact_sha256,
            len(artifact_bytes),
        )
    monkeypatch.setattr(install_io.os, "fstat", original_fstat)
    temporary_blob.close()
    install_target.close()
    source_file.close()


def test_descriptor_and_copy_short_reads_are_typed(
    monkeypatch,
    retained_artifact_test_root: Path,
) -> None:
    artifact_bytes = b"short-read"
    artifact_sha256 = hashlib.sha256(artifact_bytes).hexdigest()
    source_path = retained_artifact_test_root / "short-read-source"
    source_path.write_bytes(artifact_bytes)
    source_file = install_io._open_source(source_path)
    install_target = install_io._open_install_target(artifact_sha256)
    temporary_blob = blob_producer._create_temporary_blob(
        install_target,
        artifact_sha256,
    )
    original_pread = install_io.os.pread
    monkeypatch.setattr(install_io.os, "pread", lambda *args, **kwargs: b"")
    with pytest.raises(RetainedArtifactError, match="fixture_mismatch"):
        install_io._verify_descriptor(
            source_file.descriptor,
            artifact_sha256,
            len(artifact_bytes),
            "fixture_mismatch",
        )
    with pytest.raises(RetainedArtifactError, match="source_mismatch"):
        install_io._copy_verified_source(
            source_file,
            temporary_blob,
            artifact_sha256,
            len(artifact_bytes),
        )
    monkeypatch.setattr(install_io.os, "pread", original_pread)
    temporary_blob.close()
    install_target.close()
    source_file.close()


def test_target_close_failure_is_note_under_verification_primary(
    monkeypatch,
    retained_artifact_test_root: Path,
) -> None:
    artifact_bytes = b"expected-target"
    artifact_sha256 = hashlib.sha256(artifact_bytes).hexdigest()
    install_target = install_io._open_install_target(artifact_sha256)
    target_descriptor = os.open(
        install_target.leaf_name,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL,
        0o600,
        dir_fd=install_target.directory_chain.parent_descriptor,
    )
    os.write(target_descriptor, b"wrong-target")
    os.close(target_descriptor)
    original_open_target = install_io._open_optional_target
    original_close = install_io.os.close
    opened_descriptor_by_field = {}

    def open_and_record(current_install_target):
        descriptor = original_open_target(current_install_target)
        opened_descriptor_by_field["value"] = descriptor
        return descriptor

    def fail_target_close(descriptor: int) -> None:
        if descriptor == opened_descriptor_by_field.get("value"):
            raise OSError("fixture target close failure")
        original_close(descriptor)

    monkeypatch.setattr(install_io, "_open_optional_target", open_and_record)
    monkeypatch.setattr(install_io.os, "close", fail_target_close)
    with pytest.raises(RetainedArtifactError, match="identity_mismatch") as raised:
        install_io._verify_target(
            install_target,
            artifact_sha256,
            len(artifact_bytes),
        )
    assert any(
        "target_close_failed" in note
        for note in getattr(raised.value, "__notes__", ())
    )
    monkeypatch.setattr(install_io.os, "close", original_close)
    original_close(opened_descriptor_by_field["value"])
    os.unlink(
        install_target.leaf_name,
        dir_fd=install_target.directory_chain.parent_descriptor,
    )
    install_target.close()
