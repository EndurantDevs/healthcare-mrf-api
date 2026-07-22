# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Isolated filesystem fixture for retained-reader proofs."""

from __future__ import annotations

import os
import shutil
import sys
import uuid
from contextlib import suppress
from pathlib import Path

import pytest

from process import provider_directory_retained_blob_install_support as install_io
from process import provider_directory_retained_blob_producer as blob_producer
from process.provider_directory_retained_blob_store import ARTIFACT_ROOT_ENV


def _link_open_descriptor_for_test(
    source_descriptor: int,
    target_directory_descriptor: int,
    target_leaf_name: str,
) -> None:
    """Emulate atomic fd publication where Darwin lacks Linux procfd linkat."""
    shadow_name = f".fd-link-test-{uuid.uuid4().hex}"
    shadow_descriptor = os.open(
        shadow_name,
        os.O_RDWR | os.O_CREAT | os.O_EXCL,
        0o600,
        dir_fd=target_directory_descriptor,
    )
    try:
        read_offset = 0
        while byte_chunk := os.pread(source_descriptor, 1024 * 1024, read_offset):
            os.write(shadow_descriptor, byte_chunk)
            read_offset += len(byte_chunk)
        os.fsync(shadow_descriptor)
    finally:
        os.close(shadow_descriptor)
    try:
        os.link(
            shadow_name,
            target_leaf_name,
            src_dir_fd=target_directory_descriptor,
            dst_dir_fd=target_directory_descriptor,
            follow_symlinks=False,
        )
    finally:
        os.unlink(shadow_name, dir_fd=target_directory_descriptor)


def _create_unnamed_blob_for_test(
    target: install_io._BlobInstallTarget,
    artifact_sha256: str,
) -> install_io._TemporaryBlob:
    leaf_name = f".{artifact_sha256}.{uuid.uuid4().hex}.test-partial"
    descriptor = os.open(
        leaf_name,
        os.O_RDWR | os.O_CREAT | os.O_EXCL,
        0o600,
        dir_fd=target.directory_chain.parent_descriptor,
    )
    os.unlink(leaf_name, dir_fd=target.directory_chain.parent_descriptor)
    return install_io._TemporaryBlob(descriptor=descriptor)


@pytest.fixture
def retained_artifact_test_root(monkeypatch) -> Path:
    """Create an isolated non-system-temporary artifact root."""

    base_path = Path.home() / ".healthporta-retained-reader-tests"
    root_path = base_path / uuid.uuid4().hex
    root_path.mkdir(parents=True)
    monkeypatch.setenv(ARTIFACT_ROOT_ENV, str(root_path))
    if not sys.platform.startswith("linux"):
        monkeypatch.setattr(blob_producer, "_require_install_platform", lambda: None)
        monkeypatch.setattr(
            blob_producer,
            "_link_open_descriptor",
            _link_open_descriptor_for_test,
        )
        monkeypatch.setattr(
            blob_producer,
            "_create_temporary_blob",
            _create_unnamed_blob_for_test,
        )
    try:
        yield root_path
    finally:
        shutil.rmtree(root_path)
        with suppress(OSError):
            base_path.rmdir()
