# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import hashlib
import os
from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from process.ptg_parts import input_artifact_retention as retention
from process.ptg_parts.artifacts import PTG2ArtifactStore


def _sealed_stage(store: PTG2ArtifactStore, payload: bytes = b"payload"):
    stage_directory = store.tmp_dir / "private-stage"
    stage_directory.mkdir(mode=0o700)
    staged = stage_directory / "artifact"
    staged.write_bytes(payload)
    stage = retention._capture_streamed_artifact_stage(
        store,
        staged,
        streamed_sha256=hashlib.sha256(payload).hexdigest(),
        streamed_byte_count=len(payload),
    )
    return staged, stage


def test_verified_stage_is_opaque_and_close_is_idempotent(tmp_path):
    store = PTG2ArtifactStore(tmp_path / "opaque-stage")
    with pytest.raises(TypeError, match="must come from the stream sealer"):
        retention.PTG2VerifiedArtifactStage(
            token=object(),
            stage_state=object(),
        )

    _staged, stage = _sealed_stage(store)
    assert stage.byte_count == len(b"payload")
    stage.close()
    stage.close()
    with pytest.raises(RuntimeError, match="stage is closed"):
        stage._assert_unchanged()


def test_verified_stage_rejects_missing_name_and_invalid_byte_counts(tmp_path):
    store = PTG2ArtifactStore(tmp_path / "missing-name")
    staged, stage = _sealed_stage(store)
    staged.unlink()
    try:
        with pytest.raises(RuntimeError, match="staging identity changed"):
            stage._assert_unchanged()
    finally:
        stage.close()

    private_directory = store.tmp_dir / "invalid-count"
    private_directory.mkdir(mode=0o700)
    candidate = private_directory / "artifact"
    candidate.write_bytes(b"")
    for invalid_count in (True, -1, 1.5):
        with pytest.raises(ValueError, match="non-negative integer"):
            retention._capture_streamed_artifact_stage(
                store,
                candidate,
                streamed_sha256=hashlib.sha256(b"").hexdigest(),
                streamed_byte_count=invalid_count,
            )


def test_verified_stage_directory_failures_are_explicit(tmp_path, monkeypatch):
    store = PTG2ArtifactStore(tmp_path / "directory-guards")
    missing = store.tmp_dir / "missing" / "artifact"
    with pytest.raises(RuntimeError, match="directory is unavailable"):
        retention._open_private_stage_directory(store, missing)

    outside = tmp_path / "outside"
    outside.mkdir(mode=0o700)
    with pytest.raises(RuntimeError, match="outside the private root"):
        retention._open_private_stage_directory(store, outside / "artifact")

    private_directory = store.tmp_dir / "private"
    private_directory.mkdir(mode=0o700)
    monkeypatch.setattr(
        retention.os,
        "open",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(PermissionError()),
    )
    with pytest.raises(RuntimeError, match="directory is unsafe"):
        retention._open_private_stage_directory(store, private_directory / "artifact")


def test_verified_stage_rejects_directory_identity_change(tmp_path, monkeypatch):
    store = PTG2ArtifactStore(tmp_path / "directory-identity")
    private_directory = store.tmp_dir / "private"
    private_directory.mkdir(mode=0o700)
    real_fstat = os.fstat

    def changed_inode(file_descriptor):
        values = list(real_fstat(file_descriptor))
        values[1] += 1
        return os.stat_result(values)

    monkeypatch.setattr(retention.os, "fstat", changed_inode)
    with pytest.raises(RuntimeError, match="directory identity changed"):
        retention._open_private_stage_directory(store, private_directory / "artifact")


def test_verified_destination_and_store_binding_fail_closed(tmp_path):
    store = PTG2ArtifactStore(tmp_path / "destination")
    other_store = PTG2ArtifactStore(tmp_path / "other-destination")
    _staged, stage = _sealed_stage(store)
    try:
        with pytest.raises(ValueError, match="destination is invalid"):
            retention._verified_artifact_destination(
                store,
                stage,
                artifact_kind="unknown",
                suffix="",
                namespace_sha256=None,
            )
        with pytest.raises(ValueError, match="do not accept a namespace"):
            retention._verified_artifact_destination(
                store,
                stage,
                artifact_kind="raw",
                suffix="",
                namespace_sha256="b" * 64,
            )
        with pytest.raises(TypeError, match="must be a sealed"):
            retention.publish_verified_artifact_stage(
                store,
                object(),
                artifact_kind="raw",
            )
        with pytest.raises(ValueError, match="belongs to another store"):
            retention.publish_verified_artifact_stage(
                other_store,
                stage,
                artifact_kind="raw",
            )
    finally:
        stage.close()


def test_verified_publish_requires_one_filesystem(tmp_path, monkeypatch):
    store = PTG2ArtifactStore(tmp_path / "filesystem")
    _staged, stage = _sealed_stage(store)

    @contextmanager
    def fake_parent_fd(*_args, **_kwargs):
        yield 123

    monkeypatch.setattr(
        retention.PTG2VerifiedArtifactStage,
        "_assert_unchanged",
        lambda _stage: None,
    )
    monkeypatch.setattr(retention, "_managed_artifact_parent_fd", fake_parent_fd)
    monkeypatch.setattr(
        retention.os,
        "fstat",
        lambda _file_descriptor: SimpleNamespace(
            st_dev=stage._state.sealed_stat.st_dev + 1
        ),
    )
    try:
        with pytest.raises(RuntimeError, match="share the artifact filesystem"):
            retention.publish_verified_artifact_stage(
                store,
                stage,
                artifact_kind="raw",
            )
    finally:
        stage.close()


def test_verified_stage_closes_directory_when_file_open_fails(tmp_path):
    store = PTG2ArtifactStore(tmp_path / "missing-file")
    private_directory = store.tmp_dir / "private"
    private_directory.mkdir(mode=0o700)

    with pytest.raises(FileNotFoundError):
        retention._capture_streamed_artifact_stage(
            store,
            private_directory / "missing",
            streamed_sha256=hashlib.sha256(b"").hexdigest(),
            streamed_byte_count=0,
        )
