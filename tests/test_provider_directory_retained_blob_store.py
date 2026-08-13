# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import hashlib
import os
import shutil
import uuid
from pathlib import Path

import pytest

from process import provider_directory_retained_blob_producer as blob_producer
from process import provider_directory_retained_blob_store as blob_store
from process.provider_directory_retained_artifact_contract import RetainedArtifactError
from process.provider_directory_retained_blob_producer import (
    install_retained_artifact_blob,
)
from process.provider_directory_retained_blob_store import (
    ARTIFACT_ROOT_ENV,
    _open_retained_artifact_blob,
    retained_artifact_blob_components,
)
from tests.provider_directory_retained_reader_support import (
    write_retained_artifact_blob,
)


pytest_plugins = ("tests.provider_directory_retained_reader_fixtures",)


def test_blob_path_is_canonical_and_does_not_create_read_directories(
    retained_artifact_test_root: Path,
) -> None:
    artifact_sha256 = hashlib.sha256(b"canonical-path").hexdigest()
    blob_components = retained_artifact_blob_components(artifact_sha256)
    assert blob_components == (
        "blobs",
        artifact_sha256[:2],
        artifact_sha256[2:4],
        artifact_sha256,
    )
    blob_path = retained_artifact_test_root.joinpath(*blob_components)
    assert not blob_path.parent.exists()
    with pytest.raises(RetainedArtifactError, match="path_unsafe"):
        _open_retained_artifact_blob(artifact_sha256, 1)
    assert not blob_path.parent.exists()


def test_producer_installs_canonical_blob_durably_and_idempotently(
    retained_artifact_test_root: Path,
) -> None:
    artifact_bytes = b"canonical producer bytes\n"
    artifact_sha256 = hashlib.sha256(artifact_bytes).hexdigest()
    source_path = retained_artifact_test_root / "producer-source"
    second_source_path = retained_artifact_test_root / "producer-source-second"
    source_path.write_bytes(artifact_bytes)
    second_source_path.write_bytes(artifact_bytes)

    install_retained_artifact_blob(
        source_path,
        artifact_sha256=artifact_sha256,
        artifact_byte_count=len(artifact_bytes),
    )
    install_retained_artifact_blob(
        second_source_path,
        artifact_sha256=artifact_sha256,
        artifact_byte_count=len(artifact_bytes),
    )

    blob_path = retained_artifact_test_root.joinpath(
        *retained_artifact_blob_components(artifact_sha256)
    )
    assert blob_path.read_bytes() == artifact_bytes
    assert list(blob_path.parent.glob("*.partial")) == []


@pytest.mark.parametrize("mismatch_kind", ("digest", "size"))
def test_producer_rejects_mismatched_source_without_installing_blob(
    retained_artifact_test_root: Path,
    mismatch_kind: str,
) -> None:
    expected_bytes = b"expected producer bytes\n"
    source_bytes = (
        b"X" * len(expected_bytes) if mismatch_kind == "digest" else expected_bytes
    )
    artifact_sha256 = hashlib.sha256(expected_bytes).hexdigest()
    expected_byte_count = len(expected_bytes) + (mismatch_kind == "size")
    source_path = retained_artifact_test_root / f"producer-{mismatch_kind}"
    source_path.write_bytes(source_bytes)

    with pytest.raises(RetainedArtifactError, match="source_mismatch"):
        install_retained_artifact_blob(
            source_path,
            artifact_sha256=artifact_sha256,
            artifact_byte_count=expected_byte_count,
        )

    blob_path = retained_artifact_test_root.joinpath(
        *retained_artifact_blob_components(artifact_sha256)
    )
    assert not blob_path.exists()


@pytest.mark.parametrize("symlink_kind", ("parent", "leaf"))
def test_producer_does_not_follow_source_symlinks(
    retained_artifact_test_root: Path,
    symlink_kind: str,
) -> None:
    artifact_bytes = b"producer source safety\n"
    artifact_sha256 = hashlib.sha256(artifact_bytes).hexdigest()
    actual_directory = retained_artifact_test_root / "actual-source"
    actual_directory.mkdir()
    actual_source = actual_directory / "artifact"
    actual_source.write_bytes(artifact_bytes)
    if symlink_kind == "parent":
        linked_directory = retained_artifact_test_root / "linked-source"
        linked_directory.symlink_to(actual_directory, target_is_directory=True)
        source_path = linked_directory / actual_source.name
    else:
        source_path = retained_artifact_test_root / "linked-artifact"
        source_path.symlink_to(actual_source)

    with pytest.raises(RetainedArtifactError, match="path_unsafe"):
        install_retained_artifact_blob(
            source_path,
            artifact_sha256=artifact_sha256,
            artifact_byte_count=len(artifact_bytes),
        )


def test_producer_rejects_unsafe_target_shard_and_corrupt_existing_blob(
    retained_artifact_test_root: Path,
) -> None:
    artifact_bytes = b"producer target safety\n"
    artifact_sha256 = hashlib.sha256(artifact_bytes).hexdigest()
    source_path = retained_artifact_test_root / "producer-target-source"
    source_path.write_bytes(artifact_bytes)
    blob_components = retained_artifact_blob_components(artifact_sha256)
    blob_root = retained_artifact_test_root / blob_components[0]
    outside_path = retained_artifact_test_root.parent / f"outside-{uuid.uuid4().hex}"
    outside_path.mkdir()
    blob_root.mkdir()
    (blob_root / blob_components[1]).symlink_to(
        outside_path, target_is_directory=True
    )
    try:
        with pytest.raises(RetainedArtifactError, match="path_unsafe"):
            install_retained_artifact_blob(
                source_path,
                artifact_sha256=artifact_sha256,
                artifact_byte_count=len(artifact_bytes),
            )
    finally:
        shutil.rmtree(blob_root)
        shutil.rmtree(outside_path)

    corrupt_blob_path = retained_artifact_test_root.joinpath(*blob_components)
    corrupt_blob_path.parent.mkdir(parents=True)
    corrupt_bytes = b"X" * len(artifact_bytes)
    corrupt_blob_path.write_bytes(corrupt_bytes)
    with pytest.raises(RetainedArtifactError, match="identity_mismatch"):
        install_retained_artifact_blob(
            source_path,
            artifact_sha256=artifact_sha256,
            artifact_byte_count=len(artifact_bytes),
        )
    assert corrupt_blob_path.read_bytes() == corrupt_bytes


def test_producer_detects_canonical_ancestor_replacement_during_install(
    retained_artifact_test_root: Path,
    monkeypatch,
) -> None:
    artifact_bytes = b"producer ancestor identity\n"
    artifact_sha256 = hashlib.sha256(artifact_bytes).hexdigest()
    source_path = retained_artifact_test_root / "producer-ancestor-source"
    source_path.write_bytes(artifact_bytes)
    blob_root = retained_artifact_test_root / "blobs"
    parked_blob_root = retained_artifact_test_root / "blobs.parked"
    original_publish = blob_producer._publish_temporary_blob

    def publish_then_replace(temporary, target):
        publication = original_publish(temporary, target)
        blob_root.rename(parked_blob_root)
        blob_root.mkdir()
        return publication

    monkeypatch.setattr(
        blob_producer,
        "_publish_temporary_blob",
        publish_then_replace,
    )
    try:
        with pytest.raises(RetainedArtifactError, match="identity_changed"):
            install_retained_artifact_blob(
                source_path,
                artifact_sha256=artifact_sha256,
                artifact_byte_count=len(artifact_bytes),
            )
    finally:
        shutil.rmtree(blob_root)
        parked_blob_root.rename(blob_root)


@pytest.mark.parametrize("configured_root", (None, "", "relative/root", "/"))
def test_blob_root_configuration_is_mandatory_and_absolute(
    monkeypatch,
    configured_root: str | None,
) -> None:
    if configured_root is None:
        monkeypatch.delenv(ARTIFACT_ROOT_ENV, raising=False)
    else:
        monkeypatch.setenv(ARTIFACT_ROOT_ENV, configured_root)
    with pytest.raises(RetainedArtifactError, match="artifact_root"):
        _open_retained_artifact_blob(hashlib.sha256(b"root").hexdigest(), 1)


def test_blob_root_rejects_system_temporary_descendants(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv(ARTIFACT_ROOT_ENV, str(tmp_path))
    with pytest.raises(RetainedArtifactError, match="root_temporary"):
        _open_retained_artifact_blob(hashlib.sha256(b"temporary").hexdigest(), 1)


@pytest.mark.parametrize("unsafe_component", ("blobs", "tmp"))
def test_blob_root_rejects_fixed_directory_symlinks(
    retained_artifact_test_root: Path,
    unsafe_component: str,
) -> None:
    outside_path = retained_artifact_test_root.parent / f"outside-{uuid.uuid4().hex}"
    outside_path.mkdir()
    (retained_artifact_test_root / unsafe_component).symlink_to(
        outside_path, target_is_directory=True
    )
    try:
        with pytest.raises(RetainedArtifactError, match="path_unsafe"):
            _open_retained_artifact_blob(hashlib.sha256(b"symlink").hexdigest(), 1)
    finally:
        shutil.rmtree(outside_path)


def test_blob_root_rejects_parent_chain_symlink(
    monkeypatch, retained_artifact_test_root
) -> None:
    actual_parent = retained_artifact_test_root.parent / f"actual-{uuid.uuid4().hex}"
    linked_parent = retained_artifact_test_root.parent / f"linked-{uuid.uuid4().hex}"
    nested_root = actual_parent / "nested"
    nested_root.mkdir(parents=True)
    linked_parent.symlink_to(actual_parent, target_is_directory=True)
    monkeypatch.setenv(ARTIFACT_ROOT_ENV, str(linked_parent / "nested"))
    try:
        with pytest.raises(RetainedArtifactError, match="path_unsafe"):
            _open_retained_artifact_blob(hashlib.sha256(b"parent-link").hexdigest(), 1)
    finally:
        linked_parent.unlink()
        shutil.rmtree(actual_parent)


@pytest.mark.parametrize("symlink_kind", ("shard", "leaf"))
def test_blob_path_rejects_shard_and_leaf_symlinks(
    retained_artifact_test_root,
    symlink_kind: str,
) -> None:
    artifact_sha256 = hashlib.sha256(b"shard-link").hexdigest()
    blob_directory = retained_artifact_test_root / "blobs"
    outside_path = retained_artifact_test_root.parent / f"outside-{uuid.uuid4().hex}"
    outside_path.mkdir()
    blob_directory.mkdir()
    shard_path = blob_directory / artifact_sha256[:2]
    if symlink_kind == "shard":
        shard_path.symlink_to(outside_path, target_is_directory=True)
    else:
        leaf_parent = shard_path / artifact_sha256[2:4]
        leaf_parent.mkdir(parents=True)
        (leaf_parent / artifact_sha256).symlink_to(outside_path)
    try:
        with pytest.raises(RetainedArtifactError, match="path_unsafe"):
            _open_retained_artifact_blob(artifact_sha256, 1)
    finally:
        shutil.rmtree(outside_path)


def test_opened_blob_reads_once_and_verifies_unchanged_path(
    retained_artifact_test_root: Path,
) -> None:
    artifact_bytes = b"verified retained bytes\n"
    artifact_sha256, _blob_path = write_retained_artifact_blob(
        retained_artifact_test_root, artifact_bytes
    )
    opened_blob = _open_retained_artifact_blob(
        artifact_sha256,
        len(artifact_bytes),
    )
    assert opened_blob.read_at(len(artifact_bytes), 0) == artifact_bytes
    opened_blob.verify_and_close()
    with pytest.raises(RetainedArtifactError, match="reader_closed"):
        opened_blob.verify_and_close()


@pytest.mark.parametrize(
    "mutation_kind",
    ("rewrite", "truncate", "replace", "unlink"),
)
def test_opened_blob_detects_mutation_and_truncation(
    retained_artifact_test_root: Path,
    mutation_kind: str,
) -> None:
    artifact_bytes = b"immutable retained bytes\n"
    artifact_sha256, blob_path = write_retained_artifact_blob(
        retained_artifact_test_root, artifact_bytes
    )
    opened_blob = _open_retained_artifact_blob(
        artifact_sha256,
        len(artifact_bytes),
    )
    if mutation_kind == "rewrite":
        blob_path.write_bytes(b"X" * len(artifact_bytes))
    elif mutation_kind == "truncate":
        blob_path.write_bytes(artifact_bytes[:3])
    elif mutation_kind == "replace":
        replacement_path = blob_path.with_name(f"{blob_path.name}.replacement")
        replacement_path.write_bytes(artifact_bytes)
        os.replace(replacement_path, blob_path)
    else:
        blob_path.unlink()
    with pytest.raises(RetainedArtifactError, match="identity_changed"):
        opened_blob.verify_and_close()


def test_opened_blob_detects_ancestor_swap_to_symlink(
    retained_artifact_test_root: Path,
) -> None:
    artifact_bytes = b"ancestor-bound\n"
    artifact_sha256, _blob_path = write_retained_artifact_blob(
        retained_artifact_test_root, artifact_bytes
    )
    opened_blob = _open_retained_artifact_blob(
        artifact_sha256,
        len(artifact_bytes),
    )
    blob_directory = retained_artifact_test_root / "blobs"
    parked_directory = retained_artifact_test_root / "blobs.parked"
    outside_directory = (
        retained_artifact_test_root.parent / f"outside-{uuid.uuid4().hex}"
    )
    outside_directory.mkdir()
    blob_directory.rename(parked_directory)
    blob_directory.symlink_to(outside_directory, target_is_directory=True)
    try:
        with pytest.raises(RetainedArtifactError, match="path_unsafe|identity_changed"):
            opened_blob.verify_and_close()
    finally:
        blob_directory.unlink()
        parked_directory.rename(blob_directory)
        shutil.rmtree(outside_directory)


def test_opened_blob_detects_ancestor_replacement_with_real_directory(
    retained_artifact_test_root: Path,
) -> None:
    artifact_bytes = b"real-directory-ancestor-bound\n"
    artifact_sha256, _blob_path = write_retained_artifact_blob(
        retained_artifact_test_root, artifact_bytes
    )
    opened_blob = _open_retained_artifact_blob(
        artifact_sha256,
        len(artifact_bytes),
    )
    blob_directory = retained_artifact_test_root / "blobs"
    parked_directory = retained_artifact_test_root / "blobs.parked"
    blob_directory.rename(parked_directory)
    shutil.copytree(parked_directory, blob_directory)
    try:
        with pytest.raises(RetainedArtifactError, match="identity_changed"):
            opened_blob.verify_and_close()
    finally:
        shutil.rmtree(blob_directory)
        parked_directory.rename(blob_directory)


def test_open_blob_rejects_wrong_size_missing_file_and_platform_guard(
    retained_artifact_test_root: Path,
    monkeypatch,
) -> None:
    artifact_bytes = b"size-bound\n"
    artifact_sha256, blob_path = write_retained_artifact_blob(
        retained_artifact_test_root, artifact_bytes
    )
    with pytest.raises(RetainedArtifactError, match="identity_mismatch"):
        _open_retained_artifact_blob(artifact_sha256, len(artifact_bytes) + 1)
    blob_path.unlink()
    with pytest.raises(RetainedArtifactError, match="blob_unavailable"):
        _open_retained_artifact_blob(artifact_sha256, len(artifact_bytes))
    monkeypatch.setattr(blob_store, "_NOFOLLOW_FLAG", None)
    with pytest.raises(RetainedArtifactError, match="openat_unavailable"):
        _open_retained_artifact_blob(artifact_sha256, len(artifact_bytes))


def test_blob_low_level_guards_wrap_unsafe_modes_and_io_errors(
    retained_artifact_test_root: Path,
    monkeypatch,
) -> None:
    artifact_bytes = b"low-level-guard\n"
    artifact_sha256, blob_path = write_retained_artifact_blob(
        retained_artifact_test_root,
        artifact_bytes,
    )
    with pytest.raises(RetainedArtifactError, match="blob_path_unsafe"):
        blob_store._file_identity(os.lstat(blob_path.parent))
    original_supports_dir_fd = blob_store.os.supports_dir_fd
    monkeypatch.setattr(blob_store.os, "supports_dir_fd", set())
    with pytest.raises(RetainedArtifactError, match="openat_unavailable"):
        _open_retained_artifact_blob(artifact_sha256, len(artifact_bytes))
    monkeypatch.setattr(blob_store.os, "supports_dir_fd", original_supports_dir_fd)
    root_descriptor = os.open(retained_artifact_test_root, os.O_RDONLY)
    os.close(root_descriptor)
    with pytest.raises(RetainedArtifactError, match="path_unsafe"):
        blob_store._reject_optional_root_entry(root_descriptor, "tmp")


def test_blob_read_and_verify_wrap_descriptor_io_failures(
    retained_artifact_test_root: Path,
    monkeypatch,
) -> None:
    artifact_bytes = b"descriptor-errors\n"
    artifact_sha256, _blob_path = write_retained_artifact_blob(
        retained_artifact_test_root,
        artifact_bytes,
    )
    aborted_blob = _open_retained_artifact_blob(
        artifact_sha256,
        len(artifact_bytes),
    )
    aborted_blob.abort()
    aborted_blob.abort()
    with pytest.raises(RetainedArtifactError, match="blob_read_failed"):
        aborted_blob.read_at(1, 0)
    opened_blob = _open_retained_artifact_blob(
        artifact_sha256,
        len(artifact_bytes),
    )
    original_fstat = blob_store.os.fstat

    def fail_blob_fstat(descriptor: int):
        if descriptor == opened_blob._descriptor:
            raise OSError("fixture descriptor failure")
        return original_fstat(descriptor)

    monkeypatch.setattr(blob_store.os, "fstat", fail_blob_fstat)
    with pytest.raises(RetainedArtifactError, match="identity_changed"):
        opened_blob.verify_and_close()


def test_blob_path_rejects_invalid_digest(retained_artifact_test_root: Path) -> None:
    with pytest.raises(RetainedArtifactError, match="artifact_sha256_invalid"):
        retained_artifact_blob_components("not-a-digest")
