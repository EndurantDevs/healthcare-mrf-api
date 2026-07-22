# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import hashlib
import os
import stat
import subprocess
import sys
from pathlib import Path

import pytest

from process import provider_directory_retained_blob_install_support as install_io
from process import provider_directory_retained_blob_producer as blob_producer
from process.provider_directory_retained_artifact_contract import RetainedArtifactError
from process.provider_directory_retained_blob_producer import (
    install_retained_artifact_blob,
)
from process.provider_directory_retained_blob_store import (
    retained_artifact_blob_components,
)


pytest_plugins = ("tests.provider_directory_retained_reader_fixtures",)


def _producer_input(artifact_root: Path, label: str) -> tuple[Path, bytes, str]:
    artifact_bytes = f"durability-{label}\n".encode()
    artifact_sha256 = hashlib.sha256(artifact_bytes).hexdigest()
    source_path = artifact_root / f"source-{label}"
    source_path.write_bytes(artifact_bytes)
    return source_path, artifact_bytes, artifact_sha256


def _canonical_path(artifact_root: Path, artifact_sha256: str) -> Path:
    return artifact_root.joinpath(*retained_artifact_blob_components(artifact_sha256))


def test_existing_canonical_success_fsyncs_file_and_directory(
    monkeypatch,
    retained_artifact_test_root: Path,
) -> None:
    source_path, artifact_bytes, artifact_sha256 = _producer_input(
        retained_artifact_test_root,
        "existing-barriers",
    )
    install_retained_artifact_blob(
        source_path,
        artifact_sha256=artifact_sha256,
        artifact_byte_count=len(artifact_bytes),
    )
    blob_path = _canonical_path(retained_artifact_test_root, artifact_sha256)
    blob_inode = os.stat(blob_path).st_ino
    parent_inode = os.stat(blob_path.parent).st_ino
    original_fsync = install_io.os.fsync
    synced_entries: list[tuple[int, int]] = []

    def record_fsync(descriptor: int) -> None:
        descriptor_state = os.fstat(descriptor)
        synced_entries.append((descriptor_state.st_ino, descriptor_state.st_mode))
        original_fsync(descriptor)

    monkeypatch.setattr(install_io.os, "fsync", record_fsync)
    install_retained_artifact_blob(
        source_path,
        artifact_sha256=artifact_sha256,
        artifact_byte_count=len(artifact_bytes),
    )

    assert any(
        inode == blob_inode and stat.S_ISREG(mode)
        for inode, mode in synced_entries
    )
    assert any(
        inode == parent_inode and stat.S_ISDIR(mode)
        for inode, mode in synced_entries
    )


def test_concurrent_shard_creation_has_parent_fsync_before_open(
    monkeypatch,
    retained_artifact_test_root: Path,
) -> None:
    source_path, artifact_bytes, artifact_sha256 = _producer_input(
        retained_artifact_test_root,
        "concurrent-shard",
    )
    root_inode = os.stat(retained_artifact_test_root).st_ino
    original_mkdir = install_io.os.mkdir
    original_sync = install_io._sync_directory
    original_open_directory = install_io._open_directory_at
    events: list[tuple[str, str | int]] = []
    conflict_state_by_field = {"has_injected_conflict": False}

    def create_then_report_existing(component, *args, **kwargs):
        if (
            component == "blobs"
            and not conflict_state_by_field["has_injected_conflict"]
        ):
            original_mkdir(component, *args, **kwargs)
            conflict_state_by_field["has_injected_conflict"] = True
            events.append(("mkdir_file_exists", component))
            raise FileExistsError("fixture concurrent shard creation")
        return original_mkdir(component, *args, **kwargs)

    def record_sync(descriptor: int) -> None:
        events.append(("sync", os.fstat(descriptor).st_ino))
        original_sync(descriptor)

    def record_open(parent_descriptor: int, component: str) -> int:
        events.append(("open", component))
        return original_open_directory(parent_descriptor, component)

    monkeypatch.setattr(install_io.os, "mkdir", create_then_report_existing)
    monkeypatch.setattr(install_io, "_sync_directory", record_sync)
    monkeypatch.setattr(install_io, "_open_directory_at", record_open)
    install_retained_artifact_blob(
        source_path,
        artifact_sha256=artifact_sha256,
        artifact_byte_count=len(artifact_bytes),
    )

    conflict_index = events.index(("mkdir_file_exists", "blobs"))
    root_sync_index = events.index(("sync", root_inode), conflict_index + 1)
    blob_open_index = events.index(("open", "blobs"), conflict_index + 1)
    assert conflict_index < root_sync_index < blob_open_index


def test_post_link_fsync_failure_leaves_valid_blob_for_verified_retry(
    monkeypatch,
    retained_artifact_test_root: Path,
) -> None:
    source_path, artifact_bytes, artifact_sha256 = _producer_input(
        retained_artifact_test_root,
        "post-link-retry",
    )
    blob_path = _canonical_path(retained_artifact_test_root, artifact_sha256)
    original_publish = blob_producer._publish_temporary_blob
    original_sync = install_io._sync_directory
    sync_state_by_field = {
        "should_fail_next_sync": False,
        "cleanup_barrier_count": 0,
    }

    def publish_then_arm_failure(temporary, install_target):
        original_publish(temporary, install_target)
        sync_state_by_field["should_fail_next_sync"] = True

    def fail_post_link_sync_once(descriptor: int) -> None:
        if sync_state_by_field["should_fail_next_sync"]:
            sync_state_by_field["should_fail_next_sync"] = False
            raise RetainedArtifactError("retained_blob_durability_failed")
        sync_state_by_field["cleanup_barrier_count"] += 1
        original_sync(descriptor)

    monkeypatch.setattr(
        blob_producer,
        "_publish_temporary_blob",
        publish_then_arm_failure,
    )
    monkeypatch.setattr(install_io, "_sync_directory", fail_post_link_sync_once)
    with pytest.raises(RetainedArtifactError, match="durability_failed"):
        install_retained_artifact_blob(
            source_path,
            artifact_sha256=artifact_sha256,
            artifact_byte_count=len(artifact_bytes),
        )
    assert blob_path.read_bytes() == artifact_bytes
    assert sync_state_by_field["cleanup_barrier_count"] > 0

    monkeypatch.setattr(blob_producer, "_publish_temporary_blob", original_publish)
    monkeypatch.setattr(install_io, "_sync_directory", original_sync)
    install_retained_artifact_blob(
        source_path,
        artifact_sha256=artifact_sha256,
        artifact_byte_count=len(artifact_bytes),
    )
    assert blob_path.read_bytes() == artifact_bytes


def test_replacement_after_publication_before_failure_cleanup_is_preserved(
    monkeypatch,
    retained_artifact_test_root: Path,
) -> None:
    source_path, artifact_bytes, artifact_sha256 = _producer_input(
        retained_artifact_test_root,
        "replacement-before-cleanup",
    )
    blob_path = _canonical_path(retained_artifact_test_root, artifact_sha256)
    original_publish = blob_producer._publish_temporary_blob
    original_sync = install_io._sync_directory
    sync_state_by_field = {"should_fail_next_sync": False}

    def publish_then_arm_failure(temporary, install_target):
        original_publish(temporary, install_target)
        sync_state_by_field["should_fail_next_sync"] = True

    def fail_post_link_sync_once(descriptor: int) -> None:
        if sync_state_by_field["should_fail_next_sync"]:
            sync_state_by_field["should_fail_next_sync"] = False
            replacement_path = blob_path.with_suffix(".replacement")
            replacement_path.write_bytes(b"R" * len(artifact_bytes))
            os.replace(replacement_path, blob_path)
            raise RetainedArtifactError("retained_blob_durability_failed")
        original_sync(descriptor)

    monkeypatch.setattr(
        blob_producer,
        "_publish_temporary_blob",
        publish_then_arm_failure,
    )
    monkeypatch.setattr(install_io, "_sync_directory", fail_post_link_sync_once)
    with pytest.raises(RetainedArtifactError, match="durability_failed"):
        install_retained_artifact_blob(
            source_path,
            artifact_sha256=artifact_sha256,
            artifact_byte_count=len(artifact_bytes),
        )
    replacement_bytes = b"R" * len(artifact_bytes)
    assert blob_path.read_bytes() == replacement_bytes

    monkeypatch.setattr(blob_producer, "_publish_temporary_blob", original_publish)
    monkeypatch.setattr(install_io, "_sync_directory", original_sync)
    with pytest.raises(RetainedArtifactError, match="identity_mismatch"):
        install_retained_artifact_blob(
            source_path,
            artifact_sha256=artifact_sha256,
            artifact_byte_count=len(artifact_bytes),
        )
    assert blob_path.read_bytes() == replacement_bytes


def test_procfd_publication_ignores_and_preserves_decoy_temp_name(
    monkeypatch,
    retained_artifact_test_root: Path,
) -> None:
    source_path, artifact_bytes, artifact_sha256 = _producer_input(
        retained_artifact_test_root,
        "temp-replacement",
    )
    blob_path = _canonical_path(retained_artifact_test_root, artifact_sha256)
    original_create = install_io._create_temporary_blob
    if not sys.platform.startswith("linux"):
        original_create = blob_producer._create_temporary_blob
    original_link_descriptor = blob_producer._link_open_descriptor
    decoy_name = f".{artifact_sha256}.replacement.partial"
    decoy_bytes = b"X" * len(artifact_bytes)

    def create_and_record(*args, **kwargs):
        temporary = original_create(*args, **kwargs)
        assert os.fstat(temporary.descriptor).st_nlink == 0
        return temporary

    def publish_while_decoy_exists(source_descriptor, target_descriptor, target_name):
        decoy_descriptor = os.open(
            decoy_name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o600,
            dir_fd=target_descriptor,
        )
        try:
            os.write(decoy_descriptor, decoy_bytes)
            os.fsync(decoy_descriptor)
        finally:
            os.close(decoy_descriptor)
        original_link_descriptor(
            source_descriptor,
            target_descriptor,
            target_name,
        )

    monkeypatch.setattr(blob_producer, "_create_temporary_blob", create_and_record)
    monkeypatch.setattr(
        blob_producer,
        "_link_open_descriptor",
        publish_while_decoy_exists,
    )
    install_retained_artifact_blob(
        source_path,
        artifact_sha256=artifact_sha256,
        artifact_byte_count=len(artifact_bytes),
    )
    assert blob_path.read_bytes() == artifact_bytes
    decoy_path = blob_path.parent / decoy_name
    assert decoy_path.read_bytes() == decoy_bytes
    decoy_path.unlink()


def test_replacement_after_link_before_publish_return_is_preserved(
    monkeypatch,
    retained_artifact_test_root: Path,
) -> None:
    source_path, artifact_bytes, artifact_sha256 = _producer_input(
        retained_artifact_test_root,
        "replacement-before-return",
    )
    blob_path = _canonical_path(retained_artifact_test_root, artifact_sha256)
    original_link_descriptor = blob_producer._link_open_descriptor
    replacement_bytes = b"W" * len(artifact_bytes)

    def publish_then_replace(source_descriptor, target_descriptor, target_name):
        original_link_descriptor(
            source_descriptor,
            target_descriptor,
            target_name,
        )
        replacement_path = blob_path.with_suffix(".replacement")
        replacement_path.write_bytes(replacement_bytes)
        os.replace(replacement_path, blob_path)

    monkeypatch.setattr(
        blob_producer,
        "_link_open_descriptor",
        publish_then_replace,
    )
    with pytest.raises(RetainedArtifactError, match="identity_mismatch"):
        install_retained_artifact_blob(
            source_path,
            artifact_sha256=artifact_sha256,
            artifact_byte_count=len(artifact_bytes),
        )
    assert blob_path.read_bytes() == replacement_bytes

    monkeypatch.setattr(
        blob_producer,
        "_link_open_descriptor",
        original_link_descriptor,
    )
    with pytest.raises(RetainedArtifactError, match="identity_mismatch"):
        install_retained_artifact_blob(
            source_path,
            artifact_sha256=artifact_sha256,
            artifact_byte_count=len(artifact_bytes),
        )
    assert blob_path.read_bytes() == replacement_bytes


def test_fifo_source_fails_without_blocking(
    retained_artifact_test_root: Path,
) -> None:
    fifo_path = retained_artifact_test_root / "producer-input.fifo"
    os.mkfifo(fifo_path)
    artifact_sha256 = hashlib.sha256(b"fifo").hexdigest()
    test_script = """
import sys
from process.provider_directory_retained_artifact_contract import RetainedArtifactError
from process.provider_directory_retained_blob_producer import install_retained_artifact_blob
try:
    install_retained_artifact_blob(
        sys.argv[1], artifact_sha256=sys.argv[2], artifact_byte_count=4
    )
except RetainedArtifactError as error:
    raise SystemExit(0 if 'source_path_unsafe' in str(error) else 3)
raise SystemExit(4)
"""
    completed_process = subprocess.run(
        [sys.executable, "-c", test_script, str(fifo_path), artifact_sha256],
        cwd=Path(__file__).resolve().parents[1],
        env=os.environ.copy(),
        capture_output=True,
        text=True,
        timeout=3,
        check=False,
    )
    assert completed_process.returncode == 0, completed_process.stderr
