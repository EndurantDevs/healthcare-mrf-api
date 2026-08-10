import asyncio

import datetime

import gzip

import hashlib

import importlib

import json

import os

import runpy

import threading

import zipfile

from concurrent.futures import ThreadPoolExecutor

from pathlib import Path

import pytest

from process.ptg_parts import input_artifact_retention as retention

from process.ptg_parts import source_download

from process.ptg_parts.artifacts import PTG2ArtifactStore, sha256_file

from process.ptg_parts.domain import PTG2HeadMetadata, PTG2RawArtifact

from process.ptg_parts.input_artifact_retention import (
    PTG2ArtifactLeaseLostError,
    PTG2ArtifactLease,
    artifact_lease_context,
    bind_artifact_lease,
    _capture_streamed_artifact_stage,
    collect_ptg2_input_artifacts,
    guard_artifact_lease,
    protect_artifact_path,
    protect_artifact_prefix,
    protect_existing_artifact,
    publish_artifact_file,
    publish_verified_artifact_stage,
)

from tests.test_ptg2_input_artifact_retention import (
    NOW,
    _assert_shared_artifact_survives,
    _collect,
    _download_test_artifact,
    _make_file,
    _manifest_compaction_fixture,
    _private_verified_stage,
    _stored_zip_artifact,
)

def test_verified_publish_requires_canonical_digest_and_exact_size(tmp_path):
    """Reject an unverified staging contract before replacing retained bytes."""

    store = PTG2ArtifactStore(tmp_path / "verified-contract")
    stage_dir = store.tmp_dir / "private-stage"
    stage_dir.mkdir(mode=0o700)
    staged = stage_dir / "stage"
    staged.write_bytes(b"payload")

    with pytest.raises(ValueError, match="canonical SHA-256"):
        _capture_streamed_artifact_stage(
            store,
            staged,
            streamed_sha256="A" * 64,
            streamed_byte_count=7,
        )
    with pytest.raises(RuntimeError, match="does not match"):
        _capture_streamed_artifact_stage(
            store,
            staged,
            streamed_sha256="a" * 64,
            streamed_byte_count=8,
        )

def test_verified_stage_rejects_non_private_directory_and_fifo(tmp_path):
    """Reject unsafe writer namespaces and non-regular stage nodes promptly."""

    store = PTG2ArtifactStore(tmp_path / "verified-node-contract")
    unsafe_dir = store.tmp_dir / "shared-stage"
    unsafe_dir.mkdir(mode=0o755)
    unsafe_stage = unsafe_dir / "artifact"
    unsafe_stage.write_bytes(b"payload")
    with pytest.raises(RuntimeError, match="directory is not private"):
        _capture_streamed_artifact_stage(
            store,
            unsafe_stage,
            streamed_sha256=hashlib.sha256(b"payload").hexdigest(),
            streamed_byte_count=7,
        )

    if not hasattr(os, "mkfifo"):
        return
    private_dir = store.tmp_dir / "private-fifo-stage"
    private_dir.mkdir(mode=0o700)
    fifo_stage = private_dir / "artifact"
    os.mkfifo(fifo_stage)
    with pytest.raises(RuntimeError, match="does not match its stream"):
        _capture_streamed_artifact_stage(
            store,
            fifo_stage,
            streamed_sha256=hashlib.sha256(b"").hexdigest(),
            streamed_byte_count=0,
        )

def test_verified_publish_rejects_same_size_stage_mutation(tmp_path):
    """Do not publish a held inode changed after its stream was sealed."""

    store = PTG2ArtifactStore(tmp_path / "mutated-stage")
    original = b"streamed-original"
    staged, digest, stage = _private_verified_stage(store, original)
    final = store.artifact_path(digest)
    final.parent.mkdir(parents=True, exist_ok=True)
    final.write_bytes(b"retained-old")
    old_bytes = final.read_bytes()
    try:
        staged.write_bytes(b"mutated--payload!")
        changed = staged.stat()
        os.utime(
            staged,
            ns=(changed.st_atime_ns, changed.st_mtime_ns + 1_000_000_000),
        )
        with pytest.raises(RuntimeError, match="staging identity changed"):
            publish_verified_artifact_stage(
                store,
                stage,
                artifact_kind="raw",
            )
    finally:
        stage.close()

    assert final.read_bytes() == old_bytes

def test_verified_publish_rejects_stage_name_inode_swap(tmp_path):
    """Keep a replacement staging name from substituting same-size bytes."""

    store = PTG2ArtifactStore(tmp_path / "swapped-stage")
    streamed_bytes = b"streamed-original"
    staged, digest, stage = _private_verified_stage(store, streamed_bytes)
    held_name = staged.with_suffix(".held")
    try:
        staged.rename(held_name)
        staged.write_bytes(b"substitute-bytes-")
        with pytest.raises(RuntimeError, match="staging identity changed"):
            publish_verified_artifact_stage(
                store,
                stage,
                artifact_kind="raw",
            )
    finally:
        stage.close()

    assert not store.artifact_path(digest).exists()

def test_verified_publish_rejects_source_swap_during_atomic_replace(
    tmp_path,
    monkeypatch,
):
    """Validate a destination candidate before it can replace retained bytes."""

    store = PTG2ArtifactStore(tmp_path / "replace-race")
    streamed_bytes = b"streamed-original"
    staged, digest, stage = _private_verified_stage(store, streamed_bytes)
    final = store.artifact_path(digest)
    final.parent.mkdir(parents=True, exist_ok=True)
    final.write_bytes(b"retained-old")
    held_name = staged.with_suffix(".held")
    real_replace = os.replace
    swap_state_by_name = {"has_swapped_source": False}

    def swap_before_replace(source_name, destination_name, *args, **kwargs):
        if (
            not swap_state_by_name["has_swapped_source"]
            and source_name == staged.name
            and kwargs.get("src_dir_fd") is not None
        ):
            swap_state_by_name["has_swapped_source"] = True
            staged.rename(held_name)
            staged.write_bytes(b"substitute-bytes-")
        return real_replace(source_name, destination_name, *args, **kwargs)

    monkeypatch.setattr(retention.os, "replace", swap_before_replace)
    try:
        with pytest.raises(RuntimeError, match="publication changed inode"):
            publish_verified_artifact_stage(
                store,
                stage,
                artifact_kind="raw",
            )
    finally:
        stage.close()

    assert swap_state_by_name["has_swapped_source"] is True
    assert final.read_bytes() == b"retained-old"
    assert not list(final.parent.glob(f".{final.name}.publish-*"))

def test_verified_publish_rejects_final_symlink_without_changing_lease(tmp_path):
    """Reject a managed final symlink before recording or replacing its target."""

    store = PTG2ArtifactStore(tmp_path / "final-symlink")
    staged, digest, stage = _private_verified_stage(store, b"streamed")
    final = store.artifact_path(digest)
    target_file = store.root / "raw" / "safe-target"
    target_file.parent.mkdir(parents=True, exist_ok=True)
    target_file.write_bytes(b"target")
    final.parent.mkdir(parents=True, exist_ok=True)
    final.symlink_to(target_file)
    lease = PTG2ArtifactLease(
        store=store,
        owner="verified-symlink",
        heartbeat_seconds=0,
    ).start()
    try:
        with bind_artifact_lease(lease.lease_id):
            with pytest.raises(RuntimeError, match="target is not a regular file"):
                publish_verified_artifact_stage(
                    store,
                    stage,
                    artifact_kind="raw",
                )
        marker = json.loads(lease.marker_path.read_text(encoding="utf-8"))
        assert marker["paths"] == []
    finally:
        stage.close()
        lease.release()

    assert final.is_symlink()
    assert target_file.read_bytes() == b"target"
    assert staged.exists()

def test_verified_publish_rejects_symlinked_destination_parent(tmp_path):
    """Walk managed destination components without following a parent symlink."""

    store = PTG2ArtifactStore(tmp_path / "parent-symlink")
    _staged, _digest, stage = _private_verified_stage(store, b"streamed")
    outside = tmp_path / "outside"
    outside.mkdir()
    (store.root / "raw").symlink_to(outside, target_is_directory=True)
    try:
        with pytest.raises(RuntimeError, match="unsafe path component"):
            publish_verified_artifact_stage(
                store,
                stage,
                artifact_kind="raw",
            )
    finally:
        stage.close()

    assert list(outside.iterdir()) == []

def test_verified_publish_records_exact_derived_path_in_active_lease(tmp_path):
    """Protect the exact inode path before a collector can observe publication."""

    store = PTG2ArtifactStore(tmp_path / "verified-lease")
    _staged, digest, stage = _private_verified_stage(store, b"streamed")
    try:
        with artifact_lease_context(
            store=store,
            owner="verified-publication",
            heartbeat_seconds=0,
        ) as lease:
            final = publish_verified_artifact_stage(
                store,
                stage,
                artifact_kind="raw",
            )
            marker = json.loads(lease.marker_path.read_text(encoding="utf-8"))
            assert marker["paths"] == [final.relative_to(store.root).as_posix()]
            assert final == store.artifact_path(digest)
    finally:
        stage.close()

def test_retention_stale_lease_candidate_edges(tmp_path):
    """Reject invalid expirations and retain the latest stale timestamp."""
    store = PTG2ArtifactStore(tmp_path / "stale")
    artifact = store.root / "raw" / "item"
    artifact.parent.mkdir(parents=True, exist_ok=True)
    artifact.write_text("x", encoding="utf-8")
    bad = retention._StaleLease(
        tmp_path / "bad.json",
        {"expires_at": "bad", "paths": [], "prefixes": []},
    )
    with pytest.raises(RuntimeError, match="expiration"):
        retention._stale_unleased_candidates_locked(
            store,
            (bad,),
            active_exact_paths=set(),
            active_prefixes=set(),
        )

    def stale(name, expires_at, path):
        return retention._StaleLease(
            tmp_path / name,
            {"expires_at": expires_at, "paths": [path], "prefixes": []},
        )

    late = "2026-07-19T10:00:00Z"
    early = "2026-07-19T09:00:00Z"
    assert (
        retention._stale_unleased_candidates_locked(
            store,
            (
                stale("protected", late, "raw/item"),
                stale("missing", late, "raw/missing"),
            ),
            active_exact_paths={"raw/item"},
            active_prefixes=set(),
        )
        == {}
    )
    candidates = retention._stale_unleased_candidates_locked(
        store,
        (
            stale("late", late, "raw/item"),
            stale("early", early, "raw/item"),
        ),
        active_exact_paths=set(),
        active_prefixes=set(),
    )
    assert candidates["raw/item"] == datetime.datetime(
        2026, 7, 19, 10, tzinfo=datetime.UTC
    )

def test_retention_selection_honors_byte_cap(tmp_path):
    """Stop selection before the next artifact exceeds the byte cap."""
    now = datetime.datetime(2026, 7, 19, tzinfo=datetime.UTC)
    stored_artifacts = [
        retention._StoredArtifact(
            tmp_path / "a",
            "raw/a",
            6,
            now.timestamp() - 7200,
            False,
            now - datetime.timedelta(hours=2),
        ),
        retention._StoredArtifact(
            tmp_path / "b",
            "raw/b",
            6,
            now.timestamp() - 7200,
            False,
            now - datetime.timedelta(hours=2),
        ),
    ]
    _eligible, selected = retention._select_artifacts(
        stored_artifacts,
        now_timestamp=now.timestamp(),
        retention_seconds=0,
        min_age_seconds=0,
        target_bytes=None,
        max_delete_bytes=10,
        max_delete_files=None,
    )
    assert [selected_artifact.relative_path for selected_artifact in selected] == [
        "raw/a"
    ]

def test_retention_manifest_and_metadata_cleanup_edges(tmp_path):
    store = PTG2ArtifactStore(tmp_path / "manifest")
    assert (
        retention._is_record_pointing_to_missing_file(
            store, {"storage_uri": None}
        )
        is False
    )
    assert (
        retention._is_record_pointing_to_missing_file(
            store, {"storage_uri": str(tmp_path.parent / "outside")}
        )
        is False
    )
    store.manifest_path.write_text("[]\n", encoding="utf-8")
    with pytest.raises(RuntimeError, match="manifest record"):
        retention._validate_manifest_locked(store)
    store.manifest_path.write_text("not-json\n[]\n", encoding="utf-8")
    with pytest.raises(RuntimeError, match="before compaction"):
        retention._compact_manifest_locked(store)

    invalid_store = PTG2ArtifactStore(tmp_path / "meta-bad")
    invalid_dir = retention._unleased_dir(invalid_store)
    invalid_dir.mkdir(parents=True)
    (invalid_dir / "bad.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "relative_path": None,
                "unleased_since": "2026-07-19T10:00:00Z",
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(RuntimeError, match="fails closed"):
        retention._prune_unleased_metadata_locked(invalid_store)

    missing_store = PTG2ArtifactStore(tmp_path / "meta-missing")
    relative_path = "raw/missing"
    marker_path = retention._unleased_marker_path(missing_store, relative_path)
    marker_path.parent.mkdir(parents=True)
    marker_path.write_text(
        json.dumps(
            retention._unleased_payload(
                relative_path,
                datetime.datetime(2026, 7, 19, tzinfo=datetime.UTC),
            )
        ),
        encoding="utf-8",
    )
    retention._prune_unleased_metadata_locked(missing_store)
    assert not marker_path.exists()

    atomic_store = PTG2ArtifactStore(tmp_path / "atomic")
    directory_candidate = atomic_store.root / ".manifest.jsonl.directory.tmp"
    directory_candidate.mkdir()
    retention._prune_atomic_metadata_temps_locked(atomic_store)
    assert directory_candidate.is_dir()

def test_retention_collector_honors_all_environment_caps(tmp_path, monkeypatch):
    monkeypatch.setenv(retention.PTG2_INPUT_ARTIFACT_TARGET_BYTES_ENV, "123")
    monkeypatch.setenv(retention.PTG2_INPUT_ARTIFACT_MAX_DELETE_BYTES_ENV, "456")
    monkeypatch.setenv(retention.PTG2_INPUT_ARTIFACT_MAX_DELETE_FILES_ENV, "7")
    captured_caps_by_name = {}

    def capture_selection(_stored_artifacts, **selection_options):
        captured_caps_by_name.update(selection_options)
        return [], []

    monkeypatch.setattr(retention, "_select_artifacts", capture_selection)
    result = collect_ptg2_input_artifacts(
        root=tmp_path / "collect",
        target_bytes=retention.DEFAULT_TARGET_BYTES,
        max_delete_bytes=retention.DEFAULT_MAX_DELETE_BYTES,
        max_delete_files=retention.DEFAULT_MAX_DELETE_FILES,
    )
    assert result.target_bytes == 123
    assert result.executed is False
    assert captured_caps_by_name["target_bytes"] == 123
    assert captured_caps_by_name["max_delete_bytes"] == 456
    assert captured_caps_by_name["max_delete_files"] == 7

def test_input_artifact_gc_module_entrypoint_invokes_retention_main(
    monkeypatch,
):
    entrypoint_path = Path(retention.__file__).with_name(
        "ptg2_input_artifact_gc.py"
    )
    calls = []
    monkeypatch.setattr(retention, "main", lambda: calls.append("main"))

    runpy.run_path(
        str(entrypoint_path),
        run_name="ptg2_input_artifact_gc_import",
    )
    assert calls == []

    runpy.run_path(str(entrypoint_path), run_name="__main__")
    assert calls == ["main"]
