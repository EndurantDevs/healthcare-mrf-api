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

def test_crashed_expired_lease_is_removed_and_artifact_collected(tmp_path):
    root = tmp_path / "artifacts"
    store = PTG2ArtifactStore(root)
    raw_path = _make_file(store.artifact_path("c" * 64, suffix=".json"), b"expired")
    lease = PTG2ArtifactLease(
        store=store,
        owner="crashed-import",
        ttl_seconds=3600,
        heartbeat_seconds=0,
    ).start()
    with bind_artifact_lease(lease.lease_id):
        assert protect_existing_artifact(store, raw_path)
    marker = json.loads(lease.marker_path.read_text(encoding="utf-8"))
    marker["expires_at"] = (NOW - datetime.timedelta(hours=2)).isoformat()
    lease.marker_path.write_text(json.dumps(marker), encoding="utf-8")

    result = _collect(root)

    assert result.active_lease_ids == ()
    assert result.stale_lease_files == (lease.marker_path,)
    assert result.deleted_files == (raw_path,)
    assert not lease.marker_path.exists()
    assert not raw_path.exists()
    lease.release()

def test_dry_run_reports_expired_lease_artifact_without_mutating_it(tmp_path):
    root = tmp_path / "artifacts"
    store = PTG2ArtifactStore(root)
    raw_path = _make_file(store.artifact_path("ca" * 32), b"expired")
    lease = PTG2ArtifactLease(
        store=store,
        owner="crashed-import-dry-run",
        ttl_seconds=3600,
        heartbeat_seconds=0,
    ).start()
    with bind_artifact_lease(lease.lease_id):
        assert protect_existing_artifact(store, raw_path)
    marker = json.loads(lease.marker_path.read_text(encoding="utf-8"))
    marker["expires_at"] = (NOW - datetime.timedelta(hours=2)).isoformat()
    lease.marker_path.write_text(json.dumps(marker), encoding="utf-8")

    dry_run = collect_ptg2_input_artifacts(
        root=root,
        execute=False,
        now=NOW,
        retention_hours=1,
        min_age_hours=1,
        target_bytes=0,
        max_delete_bytes=None,
        max_delete_files=None,
    )

    assert dry_run.selected_files == (raw_path,)
    assert dry_run.deleted_files == ()
    assert lease.marker_path.exists()
    assert raw_path.exists()

    executed = _collect(root, retention_hours=1)

    assert executed.deleted_files == (raw_path,)
    assert not lease.marker_path.exists()
    lease.release()

def test_invalid_lease_marker_fails_closed_without_deleting(tmp_path):
    root = tmp_path / "artifacts"
    store = PTG2ArtifactStore(root)
    raw_path = _make_file(store.artifact_path("a" * 64), b"must-stay")
    store.leases_dir.mkdir(parents=True, exist_ok=True)
    (store.leases_dir / "broken.json").write_text("{partial", encoding="utf-8")

    with pytest.raises(RuntimeError, match="cleanup fails closed"):
        _collect(root)

    assert raw_path.exists()

def test_shared_artifact_uses_latest_crashed_lease_expiry_for_grace(tmp_path):
    root = tmp_path / "artifacts"
    store = PTG2ArtifactStore(root)
    raw_path = _make_file(store.artifact_path("8" * 64), b"shared")
    leases = [
        PTG2ArtifactLease(
            store=store,
            owner=f"crashed-import-{index}",
            heartbeat_seconds=0,
        ).start()
        for index in range(2)
    ]
    expirations = [
        NOW - datetime.timedelta(hours=2),
        NOW - datetime.timedelta(minutes=30),
    ]
    for lease, expires_at in zip(leases, expirations, strict=True):
        with bind_artifact_lease(lease.lease_id):
            assert protect_existing_artifact(store, raw_path)
        marker = json.loads(lease.marker_path.read_text(encoding="utf-8"))
        marker["expires_at"] = expires_at.isoformat()
        lease.marker_path.write_text(json.dumps(marker), encoding="utf-8")

    first = _collect(root)

    assert set(first.stale_lease_files) == {lease.marker_path for lease in leases}
    assert first.deleted_files == ()
    assert raw_path.exists()

    second = _collect(root, now=NOW + datetime.timedelta(minutes=31))

    assert second.deleted_files == (raw_path,)
    assert not raw_path.exists()
    for lease in leases:
        lease.release()

def test_gc_deletes_stale_unreferenced_raw_and_logical_files(tmp_path):
    root = tmp_path / "artifacts"
    store = PTG2ArtifactStore(root)
    raw_path = _make_file(store.artifact_path("d" * 64, suffix=".json.gz"), b"raw")
    logical_path = _make_file(
        root / "logical" / "dd" / "dd" / ("d" * 64) / f"{'e' * 64}.json",
        b"expanded",
    )

    observed = _collect(root)

    assert set(observed.newly_unleased_files) == {raw_path, logical_path}
    assert observed.deleted_files == ()
    assert raw_path.exists()
    assert logical_path.exists()

    result = _collect(root, now=NOW + datetime.timedelta(hours=25))

    assert set(result.selected_files) == {raw_path, logical_path}
    assert set(result.deleted_files) == {raw_path, logical_path}
    assert result.deleted_bytes == len(b"rawexpanded")
    assert not raw_path.exists()
    assert not logical_path.exists()

def test_gc_compacts_manifest_and_drops_missing_artifact_records(tmp_path):
    """Verify gc compacts manifest and drops missing artifact records."""
    root = tmp_path / "artifacts"
    store = PTG2ArtifactStore(root)
    (
        raw_path,
        base_record_dict,
        partial_record_dict,
        orphan_temps,
    ) = _manifest_compaction_fixture(root, store)

    retention_result = _collect(
        root,
        retention_hours=24,
        min_age_hours=1,
        target_bytes=1024,
    )
    compacted_records = [
        json.loads(line)
        for line in store.manifest_path.read_text(encoding="utf-8").splitlines()
    ]

    assert retention_result.manifest_entries_before == 5
    assert retention_result.manifest_entries_after == 2
    assert retention_result.manifest_invalid_lines == 0
    assert compacted_records[0] == {
        **base_record_dict,
        "etag": '"new"',
        "recorded_at": compacted_records[0]["recorded_at"],
    }
    assert compacted_records[1] == {
        **partial_record_dict,
        "partial_sha256": "3" * 64,
        "recorded_at": compacted_records[1]["recorded_at"],
    }
    assert raw_path.exists()
    assert not any(path.exists() for path in orphan_temps)

def test_torn_manifest_tail_is_isolated_and_gc_fails_closed(tmp_path):
    root = tmp_path / "artifacts"
    store = PTG2ArtifactStore(root)
    raw_path = _make_file(store.artifact_path("7" * 64), b"retained")
    store.manifest_path.write_text('{"artifact_kind":"raw"', encoding="utf-8")
    valid_record_dict = {
        "artifact_kind": "raw",
        "canonical_url": "https://example.test/retained.json",
        "raw_storage_uri": store.storage_uri(raw_path),
        "raw_sha256": "7" * 64,
        "status": "available",
    }

    store.record_manifest(valid_record_dict)

    lines = store.manifest_path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2
    assert json.loads(lines[1]) == {
        **valid_record_dict,
        "recorded_at": json.loads(lines[1])["recorded_at"],
    }
    assert store.find_candidates(valid_record_dict["canonical_url"])
    manifest_before = store.manifest_path.read_bytes()

    with pytest.raises(RuntimeError, match="manifest; cleanup fails closed"):
        _collect(root)

    assert store.manifest_path.read_bytes() == manifest_before
    assert raw_path.exists()

def test_active_partial_and_range_sidecar_are_leased_then_collected(tmp_path):
    root = tmp_path / "artifacts"
    store = PTG2ArtifactStore(root)
    partial_path = _make_file(
        store.partial_path("https://example.test/large.json.gz"),
        b"partial",
    )
    sidecar_path = _make_file(
        source_download._range_sidecar_path(partial_path),
        b"{}",
    )
    lease = PTG2ArtifactLease(
        store=store,
        owner="active-download",
        heartbeat_seconds=0,
    ).start()
    with bind_artifact_lease(lease.lease_id):
        protect_artifact_path(store, partial_path)
        protect_artifact_path(store, sidecar_path)

    active = _collect(root)

    assert set(active.protected_files) == {partial_path, sidecar_path}
    assert active.deleted_files == ()
    lease.release()

    collected = _collect(root, now=NOW + datetime.timedelta(hours=25))

    assert set(collected.deleted_files) == {partial_path, sidecar_path}
    assert collected.total_bytes_after == 0

def test_publish_repairs_corrupt_content_addressed_target(tmp_path):
    root = tmp_path / "artifacts"
    store = PTG2ArtifactStore(root)
    expected_payload = b"correct-content"
    expected_sha256 = hashlib.sha256(expected_payload).hexdigest()
    final_path = _make_file(
        store.artifact_path(expected_sha256),
        b"corrupt-content",
        age_hours=0,
    )
    staged_path = root / "tmp" / "replacement"
    staged_path.write_bytes(expected_payload)

    with artifact_lease_context(store=store, owner="repair", heartbeat_seconds=0):
        publish_artifact_file(
            store,
            staged_path,
            final_path,
            expected_sha256=expected_sha256,
        )

    assert final_path.read_bytes() == expected_payload
    assert not staged_path.exists()


def test_retention_publish_rejects_symlink_stage(tmp_path):
    """Never publish a staged symlink at a content-addressed path."""

    store = PTG2ArtifactStore(tmp_path / "publish-symlink")
    symlink_target = tmp_path / "symlink-target"
    symlink_target.write_bytes(b"matching payload")
    staged_symlink = tmp_path / "staged-symlink"
    staged_symlink.symlink_to(symlink_target)
    expected_sha256 = hashlib.sha256(symlink_target.read_bytes()).hexdigest()
    final = store.root / "raw" / expected_sha256

    with pytest.raises(RuntimeError, match="staging is not a regular file"):
        publish_artifact_file(
            store,
            staged_symlink,
            final,
            expected_sha256=expected_sha256,
        )

    assert staged_symlink.is_symlink()
    assert not final.exists()


def test_logical_reuse_rejects_same_size_corruption_and_rebuilds(tmp_path):
    root = tmp_path / "artifacts"
    store = PTG2ArtifactStore(root)
    source_payload = b'{"in_network":[]}'
    raw_path = tmp_path / "rates.json"
    raw_path.write_bytes(source_payload)
    raw_sha256, raw_size = sha256_file(raw_path)
    logical_sha256 = hashlib.sha256(source_payload).hexdigest()
    logical_path = (
        root
        / "logical"
        / raw_sha256[:2]
        / raw_sha256[2:4]
        / raw_sha256
        / f"{logical_sha256}.json"
    )
    _make_file(logical_path, b"x" * len(source_payload), age_hours=0)
    store.record_manifest(
        {
            "artifact_kind": "logical_json",
            "raw_sha256": raw_sha256,
            "logical_sha256": logical_sha256,
            "logical_storage_uri": store.storage_uri(logical_path),
            "byte_count": len(source_payload),
            "status": "available",
        }
    )
    raw_artifact = PTG2RawArtifact(
        original_url=raw_path.as_uri(),
        canonical_url=raw_path.as_uri(),
        raw_path=str(raw_path),
        raw_storage_uri=store.storage_uri(raw_path),
        raw_sha256=raw_sha256,
        byte_count=raw_size,
    )

    with artifact_lease_context(store=store, owner="logical-repair", heartbeat_seconds=0):
        retained = asyncio.run(
            source_download._retained_logical_artifact(store, raw_artifact)
        )

    assert retained.logical_sha256 == logical_sha256
    assert Path(retained.logical_path).read_bytes() == source_payload

def test_capacity_target_deletes_oldest_unleased_file_before_retention_age(tmp_path):
    root = tmp_path / "artifacts"
    store = PTG2ArtifactStore(root)
    oldest = _make_file(store.artifact_path("2" * 64), b"12345", age_hours=3)
    newer = _make_file(store.artifact_path("3" * 64), b"67890", age_hours=2)

    observed = _collect(
        root,
        retention_hours=24,
        min_age_hours=1,
        target_bytes=5,
    )
    assert observed.deleted_files == ()

    result = _collect(
        root,
        now=NOW + datetime.timedelta(hours=2),
        retention_hours=24,
        min_age_hours=1,
        target_bytes=5,
    )

    assert result.deleted_files == (oldest,)
    assert result.total_bytes_after == 5
    assert result.over_target_bytes == 0
    assert not oldest.exists()
    assert newer.exists()

def test_cleanup_cycle_honors_file_and_byte_deletion_caps(tmp_path):
    root = tmp_path / "artifacts"
    store = PTG2ArtifactStore(root)
    paths = [
        _make_file(store.artifact_path(character * 64), b"12345")
        for character in ("b", "c", "d")
    ]
    _collect(root)

    result = _collect(
        root,
        now=NOW + datetime.timedelta(hours=25),
        max_delete_bytes=5,
        max_delete_files=1,
    )

    assert len(result.deleted_files) == 1
    assert result.deleted_bytes == 5
    assert sum(path.exists() for path in paths) == 2

def test_lease_release_removes_marker_and_starts_unleased_grace(tmp_path):
    root = tmp_path / "artifacts"
    store = PTG2ArtifactStore(root)
    raw_path = _make_file(store.artifact_path("9" * 64), b"published")
    lease = PTG2ArtifactLease(
        store=store,
        owner="published-import",
        heartbeat_seconds=0,
    ).start()
    marker_path = lease.marker_path
    with bind_artifact_lease(lease.lease_id):
        assert protect_existing_artifact(store, raw_path)

    lease.release()

    assert not marker_path.exists()
    unleased_markers = list((root / ".retention" / "unleased").glob("*.json"))
    assert len(unleased_markers) == 1
    marker = json.loads(unleased_markers[0].read_text(encoding="utf-8"))
    released_at = datetime.datetime.fromisoformat(marker["unleased_since"].replace("Z", "+00:00"))

    immediate = _collect(root, now=released_at + datetime.timedelta(minutes=30))

    assert immediate.deleted_files == ()
    assert raw_path.exists()

    after_grace = _collect(root, now=released_at + datetime.timedelta(hours=2))

    assert after_grace.deleted_files == (raw_path,)
    assert not raw_path.exists()
    assert not list((root / ".retention" / "unleased").glob("*.json"))

def test_live_shared_lease_prevents_grace(tmp_path):
    root = tmp_path / "artifacts"
    store = PTG2ArtifactStore(root)
    raw_path = _make_file(store.artifact_path("a1" * 32), b"shared")
    leases = [
        PTG2ArtifactLease(store=store, owner=f"import-{index}", heartbeat_seconds=0).start()
        for index in range(2)
    ]
    for lease in leases:
        with bind_artifact_lease(lease.lease_id):
            assert protect_existing_artifact(store, raw_path)

    leases[0].release()

    assert not list((root / ".retention" / "unleased").glob("*.json"))
    while_shared = _collect(root)
    assert while_shared.deleted_files == ()
    assert raw_path.exists()

    leases[1].release()

    assert len(list((root / ".retention" / "unleased").glob("*.json"))) == 1
