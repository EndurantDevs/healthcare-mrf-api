import asyncio
import datetime
import gzip
import hashlib
import importlib
import json
import os
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


NOW = datetime.datetime.now(datetime.UTC)


def _make_file(path: Path, payload: bytes, *, age_hours: float = 48) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    modified_at = (NOW - datetime.timedelta(hours=age_hours)).timestamp()
    os.utime(path, (modified_at, modified_at))
    return path


def _collect(root: Path, **overrides):
    collection_options_dict = {
        "root": root,
        "execute": True,
        "now": NOW,
        "retention_hours": 24,
        "min_age_hours": 1,
        "target_bytes": 0,
        "max_delete_bytes": None,
        "max_delete_files": None,
    }
    collection_options_dict.update(overrides)
    return collect_ptg2_input_artifacts(**collection_options_dict)


def test_concurrent_identical_artifact_has_one_logical_file_and_two_safe_leases(
    tmp_path,
    monkeypatch,
):
    """Verify concurrent identical artifact has one logical file and two safe leases."""
    root = tmp_path / "artifacts"
    store = PTG2ArtifactStore(root)
    staged_zip = tmp_path / "rates.zip"
    with zipfile.ZipFile(staged_zip, "w") as archive:
        archive.writestr("nested/rates.json", json.dumps({"in_network": [{"code": "99213"}]}))
    raw_sha256, raw_size = sha256_file(staged_zip)
    raw_path = store.artifact_path(raw_sha256, suffix=".zip")
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    os.replace(staged_zip, raw_path)
    raw_artifact = PTG2RawArtifact(
        original_url="https://example.test/rates.zip",
        canonical_url="https://example.test/rates.zip",
        raw_path=str(raw_path),
        raw_storage_uri=store.storage_uri(raw_path),
        raw_sha256=raw_sha256,
        byte_count=raw_size,
    )
    monkeypatch.setenv("HLTHPRT_PTG2_ARTIFACT_DIR", str(root))

    async def fake_download(*_args, **_kwargs):
        return raw_artifact

    monkeypatch.setattr(source_download, "download_raw_artifact", fake_download)
    barrier = threading.Barrier(3)
    download_results = []

    def run_import(owner: str) -> None:
        with artifact_lease_context(
            store=PTG2ArtifactStore(root),
            owner=owner,
            heartbeat_seconds=0,
        ):
            assert protect_existing_artifact(PTG2ArtifactStore(root), raw_path)
            downloaded = asyncio.run(
                source_download._download_ptg_job_artifact(
                    {"type": "in_network", "url": raw_artifact.original_url},
                    reuse_raw_artifacts=True,
                    max_bytes=None,
                    keep_partial_artifacts=True,
                )
            )
            assert downloaded.error is None
            download_results.append(downloaded)
            barrier.wait(timeout=10)
            barrier.wait(timeout=10)

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(run_import, f"import-{index}") for index in range(2)]
        barrier.wait(timeout=10)
        logical_paths = {
            Path(download_result.logical_artifact.logical_path)
            for download_result in download_results
        }
        assert len(logical_paths) == 1
        logical_path = logical_paths.pop()
        assert logical_path.read_text(encoding="utf-8") == json.dumps(
            {"in_network": [{"code": "99213"}]}
        )
        assert len(list((root / "logical").rglob("*.json"))) == 1
        for path in (raw_path, logical_path):
            old = (NOW - datetime.timedelta(hours=48)).timestamp()
            os.utime(path, (old, old))
        retention_result = _collect(root)
        assert set(retention_result.protected_files) == {raw_path, logical_path}
        assert retention_result.deleted_files == ()
        barrier.wait(timeout=10)
        for future in futures:
            future.result(timeout=10)


def test_url_validation_failure_does_not_unlink_shared_raw_artifact(
    tmp_path,
    monkeypatch,
):
    """Verify url validation failure does not unlink shared raw artifact."""
    root = tmp_path / "artifacts"
    store = PTG2ArtifactStore(root)
    source_bytes = b'{"in_network":[]}'
    plain_source = _make_file(tmp_path / "rates.json", source_bytes, age_hours=0)
    gzip_named_source = _make_file(
        tmp_path / "rates.json.gz", source_bytes, age_hours=0
    )

    async def fake_head(url: str):
        return source_download.PTG2HeadMetadata(
            url=url,
            status=200,
            content_length=len(source_bytes),
            supports_head=False,
        )

    monkeypatch.setattr(source_download, "fetch_head_metadata", fake_head)
    plain_lease = PTG2ArtifactLease(
        store=store,
        owner="plain-url-import",
        heartbeat_seconds=0,
    ).start()
    gzip_lease = PTG2ArtifactLease(
        store=store,
        owner="gzip-url-import",
        heartbeat_seconds=0,
    ).start()
    try:
        with bind_artifact_lease(plain_lease.lease_id):
            raw_artifact = asyncio.run(
                source_download._download_raw_artifact_locked(
                    plain_source.as_uri(),
                    store=store,
                    canonical_url=plain_source.as_uri(),
                    reuse_raw_artifacts=False,
                    max_bytes=None,
                    keep_partial_artifacts=False,
                )
            )
        raw_path = Path(raw_artifact.raw_path)

        with bind_artifact_lease(gzip_lease.lease_id):
            with pytest.raises(RuntimeError, match="gzip header"):
                asyncio.run(
                    source_download._download_raw_artifact_locked(
                        gzip_named_source.as_uri(),
                        store=store,
                        canonical_url=gzip_named_source.as_uri(),
                        reuse_raw_artifacts=False,
                        max_bytes=None,
                        keep_partial_artifacts=False,
                    )
                )

        assert raw_path.read_bytes() == source_bytes
        retention_result = _collect(root)
        assert retention_result.protected_files == (raw_path,)
        assert retention_result.deleted_files == ()
    finally:
        gzip_lease.release()
        plain_lease.release()


def test_active_lease_prevents_raw_and_logical_deletion(tmp_path):
    root = tmp_path / "artifacts"
    store = PTG2ArtifactStore(root)
    raw_path = _make_file(store.artifact_path("a" * 64, suffix=".json.gz"), b"raw")
    logical_dir = root / "logical" / "aa" / "aa" / ("a" * 64)
    logical_path = _make_file(logical_dir / f"{'b' * 64}.json", b"logical")

    lease = PTG2ArtifactLease(
        store=store,
        owner="active-import",
        ttl_seconds=3600,
        heartbeat_seconds=0,
    ).start()
    try:
        with bind_artifact_lease(lease.lease_id):
            assert protect_existing_artifact(store, raw_path)
            protect_artifact_prefix(store, logical_dir)
        result = _collect(root)
        assert set(result.protected_files) == {raw_path, logical_path}
        assert result.selected_files == ()
        assert raw_path.exists()
        assert logical_path.exists()
    finally:
        lease.release()


def test_heartbeats_keep_a_72_hour_import_active(tmp_path, monkeypatch):
    root = tmp_path / "artifacts"
    store = PTG2ArtifactStore(root)
    raw_path = _make_file(store.artifact_path("0" * 64), b"long-running")
    mutable_clock_dict = {"now": NOW}
    monkeypatch.setattr(
        retention,
        "_utcnow",
        lambda: mutable_clock_dict["now"],
    )
    lease = PTG2ArtifactLease(
        store=store,
        owner="72-hour-import",
        ttl_seconds=6 * 3600,
        heartbeat_seconds=0,
    ).start()
    try:
        with bind_artifact_lease(lease.lease_id):
            assert protect_existing_artifact(store, raw_path)
        for elapsed_hours in range(5, 71, 5):
            mutable_clock_dict["now"] = NOW + datetime.timedelta(hours=elapsed_hours)
            lease.heartbeat()
        mutable_clock_dict["now"] = NOW + datetime.timedelta(hours=72)

        retention_result = _collect(root, now=mutable_clock_dict["now"])

        assert retention_result.active_lease_ids == (lease.lease_id,)
        assert retention_result.protected_files == (raw_path,)
        assert retention_result.deleted_files == ()
        assert raw_path.exists()
    finally:
        lease.release()


def test_async_lease_guard_cancels_import_when_marker_is_lost(tmp_path):
    store = PTG2ArtifactStore(tmp_path / "artifacts")
    lease = PTG2ArtifactLease(
        store=store,
        owner="lost-marker-import",
        ttl_seconds=60,
        heartbeat_seconds=1,
    ).start()

    async def scenario() -> None:
        started = asyncio.Event()
        stopped = asyncio.Event()

        async def import_operation() -> None:
            started.set()
            try:
                await asyncio.sleep(30)
            finally:
                stopped.set()

        guarded = asyncio.create_task(
            guard_artifact_lease(lease, import_operation())
        )
        await started.wait()
        with store.retention_lock():
            lease.marker_path.unlink()
        with pytest.raises(PTG2ArtifactLeaseLostError, match="marker was lost"):
            await asyncio.wait_for(guarded, timeout=3)
        assert stopped.is_set()

    try:
        asyncio.run(scenario())
    finally:
        lease.release()


def test_heartbeat_queued_before_normal_release_is_benign(tmp_path, monkeypatch):
    store = PTG2ArtifactStore(tmp_path / "artifacts")
    lease = PTG2ArtifactLease(
        store=store,
        owner="normal-release",
        heartbeat_seconds=0,
    ).start()
    heartbeat_started = threading.Event()
    heartbeat_errors: list[BaseException] = []
    real_utcnow = retention._utcnow

    def observed_utcnow():
        heartbeat_started.set()
        return real_utcnow()

    def heartbeat() -> None:
        try:
            lease.heartbeat()
        except BaseException as exc:  # pragma: no cover - asserted below
            heartbeat_errors.append(exc)

    monkeypatch.setattr(retention, "_utcnow", observed_utcnow)
    thread = threading.Thread(target=heartbeat)
    with store.retention_lock():
        thread.start()
        assert heartbeat_started.wait(timeout=2)
        lease._released = True
    thread.join(timeout=2)

    assert not thread.is_alive()
    assert heartbeat_errors == []
    lease._released = False
    lease.release()


def test_async_lease_guard_stops_monitor_after_normal_completion(tmp_path):
    store = PTG2ArtifactStore(tmp_path / "artifacts")
    lease = PTG2ArtifactLease(
        store=store,
        owner="completed-import",
        ttl_seconds=60,
        heartbeat_seconds=1,
    ).start()

    async def scenario() -> None:
        current_task = asyncio.current_task()
        assert await guard_artifact_lease(lease, asyncio.sleep(0, result="done")) == "done"
        guard_tasks = {
            task
            for task in asyncio.all_tasks()
            if task is not current_task
            and task.get_name().startswith("ptg2-artifact-lease-guard-")
        }
        assert guard_tasks == set()

    try:
        asyncio.run(scenario())
    finally:
        lease.release()


def test_async_lease_guard_can_be_disabled(tmp_path, monkeypatch):
    store = PTG2ArtifactStore(tmp_path / "artifacts")
    lease = PTG2ArtifactLease(
        store=store,
        owner="unmonitored-import",
        ttl_seconds=60,
        heartbeat_seconds=0,
    ).start()
    monkeypatch.setattr(
        lease,
        "heartbeat",
        lambda: (_ for _ in ()).throw(AssertionError("heartbeat must stay disabled")),
    )

    try:
        result = asyncio.run(guard_artifact_lease(lease, asyncio.sleep(0, result=17)))
        assert result == 17
    finally:
        lease.release()


def test_concurrent_updates_to_one_lease_are_atomic_and_preserve_all_references(tmp_path):
    root = tmp_path / "artifacts"
    store = PTG2ArtifactStore(root)
    paths = [
        _make_file(store.artifact_path(character * 64), character.encode("ascii"))
        for character in ("4", "5", "6", "7")
    ]
    lease = PTG2ArtifactLease(
        store=store,
        owner="multi-download-import",
        heartbeat_seconds=0,
    ).start()

    def protect(path: Path) -> None:
        with bind_artifact_lease(lease.lease_id):
            assert protect_existing_artifact(PTG2ArtifactStore(root), path)

    try:
        with ThreadPoolExecutor(max_workers=len(paths)) as executor:
            list(executor.map(protect, paths))
        marker = json.loads(lease.marker_path.read_text(encoding="utf-8"))
        assert set(marker["paths"]) == {
            path.relative_to(root).as_posix()
            for path in paths
        }
        assert not list(store.leases_dir.glob("*.tmp"))
    finally:
        lease.release()


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
    raw_path = _make_file(
        store.artifact_path("f" * 64, suffix=".json"),
        b"keep",
        age_hours=0,
    )
    base_record_dict = {
        "artifact_kind": "raw",
        "canonical_url": "https://example.test/file.json",
        "raw_storage_uri": store.storage_uri(raw_path),
        "raw_sha256": "f" * 64,
        "status": "available",
    }
    store.record_manifest({**base_record_dict, "etag": '"old"'})
    store.record_manifest({**base_record_dict, "etag": '"new"'})
    missing_path = store.artifact_path("1" * 64, suffix=".json")
    store.record_manifest(
        {
            "artifact_kind": "raw",
            "canonical_url": "https://example.test/missing.json",
            "raw_storage_uri": store.storage_uri(missing_path),
            "raw_sha256": "1" * 64,
            "status": "available",
        }
    )
    partial_path = store.partial_path("https://example.test/partial.json")
    partial_path.write_bytes(b"partial")
    partial_record_dict = {
        "artifact_kind": "partial_raw",
        "canonical_url": "https://example.test/partial.json",
        "raw_storage_uri": store.storage_uri(partial_path),
        "status": "partial",
    }
    store.record_manifest({**partial_record_dict, "partial_sha256": "2" * 64})
    store.record_manifest({**partial_record_dict, "partial_sha256": "3" * 64})
    orphan_temps = [
        root / ".manifest.jsonl.crashed.tmp",
        store.leases_dir / ".lease.json.crashed.tmp",
        root / ".retention" / "unleased" / ".artifact.json.crashed.tmp",
    ]
    for orphan_temp in orphan_temps:
        orphan_temp.parent.mkdir(parents=True, exist_ok=True)
        orphan_temp.write_text("partial", encoding="utf-8")

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


def test_heartbeat_keeps_a_72_hour_import_live(tmp_path, monkeypatch):
    root = tmp_path / "artifacts"
    store = PTG2ArtifactStore(root)
    raw_path = _make_file(store.artifact_path("b1" * 32), b"long-running")
    clock_values = [NOW]
    monkeypatch.setattr(retention, "_utcnow", lambda: clock_values[0])
    lease = PTG2ArtifactLease(
        store=store,
        owner="long-import",
        ttl_seconds=6 * 60 * 60,
        heartbeat_seconds=0,
    ).start()
    try:
        with bind_artifact_lease(lease.lease_id):
            assert protect_existing_artifact(store, raw_path)
        for elapsed_hours in (24, 48, 71):
            clock_values[0] = NOW + datetime.timedelta(hours=elapsed_hours)
            lease.heartbeat()

        result = _collect(root, now=NOW + datetime.timedelta(hours=72))

        assert result.active_lease_ids == (lease.lease_id,)
        assert result.deleted_files == ()
        assert raw_path.exists()
    finally:
        clock_values[0] = NOW + datetime.timedelta(hours=72)
        lease.release()


@pytest.mark.parametrize("error", [RuntimeError("failed"), asyncio.CancelledError()])
def test_lease_context_releases_references_on_failure_or_cancel(tmp_path, error):
    root = tmp_path / "artifacts"
    store = PTG2ArtifactStore(root)
    raw_path = _make_file(store.artifact_path("c1" * 32), b"interrupted")

    with pytest.raises(type(error)):
        with artifact_lease_context(
            store=store,
            owner="interrupted-import",
            heartbeat_seconds=0,
        ):
            assert protect_existing_artifact(store, raw_path)
            raise error

    assert not list(store.leases_dir.glob("*.json"))
    assert len(list((root / ".retention" / "unleased").glob("*.json"))) == 1


def test_malformed_live_lease_fails_cleanup_closed(tmp_path):
    root = tmp_path / "artifacts"
    store = PTG2ArtifactStore(root)
    raw_path = _make_file(store.artifact_path("d1" * 32), b"protected")
    lease = PTG2ArtifactLease(store=store, owner="active", heartbeat_seconds=0).start()
    with bind_artifact_lease(lease.lease_id):
        assert protect_existing_artifact(store, raw_path)
    lease.marker_path.write_text("not-json", encoding="utf-8")

    with pytest.raises(RuntimeError, match="fails closed"):
        _collect(root)

    assert raw_path.exists()
    lease.release()


def test_gc_is_idempotent_after_explicit_unleased_grace(tmp_path):
    root = tmp_path / "artifacts"
    store = PTG2ArtifactStore(root)
    raw_path = _make_file(store.artifact_path("e1" * 32), b"old")

    observed = _collect(root)
    deleted = _collect(root, now=NOW + datetime.timedelta(hours=25))
    repeated = _collect(root, now=NOW + datetime.timedelta(hours=26))

    assert observed.deleted_files == ()
    assert deleted.deleted_files == (raw_path,)
    assert repeated.deleted_files == ()
    assert repeated.total_bytes_before == 0
    assert not list((root / ".retention" / "unleased").glob("*.json"))


def test_identical_raw_bytes_from_different_suffixes_share_one_file(tmp_path, monkeypatch):
    root = tmp_path / "artifacts"
    store = PTG2ArtifactStore(root)
    compressed_source = gzip.compress(b'{"in_network":[]}', mtime=0)
    source_paths = [tmp_path / "first.json.gz", tmp_path / "second.gz"]
    for path in source_paths:
        path.write_bytes(compressed_source)

    async def safe_url(_url):
        return None

    async def local_head(url):
        return PTG2HeadMetadata(
            url=str(url),
            content_length=Path(url).stat().st_size,
            supports_head=False,
        )

    monkeypatch.setattr(source_download, "assert_safe_url", safe_url)
    monkeypatch.setattr(source_download, "fetch_head_metadata", local_head)
    monkeypatch.setenv("HLTHPRT_PTG2_RANGE_DOWNLOADS", "false")

    with artifact_lease_context(store=store, owner="content-sharing", heartbeat_seconds=0):
        artifacts = [
            asyncio.run(source_download.download_raw_artifact(str(path), store=store))
            for path in source_paths
        ]

    assert artifacts[0].raw_sha256 == artifacts[1].raw_sha256
    assert artifacts[0].raw_path == artifacts[1].raw_path
    assert len([path for path in (root / "raw").rglob("*") if path.is_file()]) == 1


def test_cancel_waits_for_running_download_worker_before_releasing_lease(
    tmp_path,
    monkeypatch,
):
    ptg = importlib.import_module("process.ptg")
    root = tmp_path / "artifacts"
    store = PTG2ArtifactStore(root)
    raw_path = _make_file(store.artifact_path("fb" * 32), b"worker-input", age_hours=0)
    worker_started = threading.Event()
    worker_release = threading.Event()
    lease_paths = []
    monkeypatch.setenv("HLTHPRT_PTG2_ARTIFACT_DIR", str(root))
    monkeypatch.setenv(source_download.PTG2_DOWNLOAD_TASKS_ENV, "1")

    def blocking_download(job, **_kwargs):
        assert retention.current_artifact_lease_id()
        assert protect_existing_artifact(PTG2ArtifactStore(root), raw_path)
        worker_started.set()
        assert worker_release.wait(timeout=10)
        return source_download.PTG2DownloadedJob(job=job, error="worker stopped")

    monkeypatch.setattr(ptg, "_download_ptg_job_artifact_sync", blocking_download)

    async def run_import():
        with artifact_lease_context(
            store=store,
            owner="cancelled-download-import",
            heartbeat_seconds=0,
        ) as lease:
            lease_paths.append(lease.marker_path)
            async for _result in source_download._iter_downloaded_ptg_jobs(
                [{"type": "in_network", "url": "https://example.test/rates.json"}],
                reuse_raw_artifacts=True,
                max_bytes=None,
                keep_partial_artifacts=True,
            ):
                continue

    async def exercise_cancel():
        task = asyncio.create_task(run_import())
        assert await asyncio.to_thread(worker_started.wait, 10)
        task.cancel()
        await asyncio.sleep(0.05)
        assert not task.done()
        assert lease_paths[0].exists()
        worker_release.set()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(exercise_cancel())

    assert not lease_paths[0].exists()
    assert len(list((root / ".retention" / "unleased").glob("*.json"))) == 1


@pytest.mark.parametrize("outcome", ["success", "failure", "cancel"])
def test_ptg_main_releases_input_lease_for_every_terminal_outcome(
    tmp_path,
    monkeypatch,
    outcome,
):
    ptg = importlib.import_module("process.ptg")

    root = tmp_path / "artifacts"
    store = PTG2ArtifactStore(root)
    raw_path = _make_file(store.artifact_path("fa" * 32), b"input", age_hours=0)
    observed_lease_ids = []
    monkeypatch.setenv("HLTHPRT_PTG2_ARTIFACT_DIR", str(root))

    async def fake_import(**_kwargs):
        lease_id = retention.current_artifact_lease_id()
        assert lease_id
        observed_lease_ids.append(lease_id)
        assert protect_existing_artifact(PTG2ArtifactStore(root), raw_path)
        if outcome == "failure":
            raise RuntimeError("import failed")
        if outcome == "cancel":
            raise asyncio.CancelledError()
        return {"status": "succeeded"}

    monkeypatch.setattr(ptg, "_main_with_artifact_lease", fake_import)

    if outcome == "success":
        assert asyncio.run(ptg.main(import_id="lifecycle-test")) == {
            "status": "succeeded"
        }
    elif outcome == "failure":
        with pytest.raises(RuntimeError, match="import failed"):
            asyncio.run(ptg.main(import_id="lifecycle-test"))
    else:
        with pytest.raises(asyncio.CancelledError):
            asyncio.run(ptg.main(import_id="lifecycle-test"))

    assert len(observed_lease_ids) == 1
    assert not list(store.leases_dir.glob("*.json"))
    assert len(list((root / ".retention" / "unleased").glob("*.json"))) == 1


def test_retention_parsing_and_unleased_marker_edges(tmp_path):
    assert retention._parse_timestamp(None) is None
    assert retention._parse_timestamp("not-a-timestamp") is None
    assert retention._parse_timestamp("2026-07-19T10:00:00") == datetime.datetime(
        2026, 7, 19, 10, tzinfo=datetime.UTC
    )
    with pytest.raises(ValueError):
        retention._normalized_managed_relative_path(None)
    with pytest.raises(ValueError):
        retention._normalized_managed_relative_path("raw/../escape")

    store = PTG2ArtifactStore(tmp_path / "markers")
    relative_path = "raw/aa/artifact"
    marker_path = retention._unleased_marker_path(store, relative_path)
    marker_path.parent.mkdir(parents=True, exist_ok=True)
    marker_path.write_text(
        json.dumps(
            {
                "schema_version": 999,
                "relative_path": relative_path,
                "unleased_since": "2026-07-19T10:00:00Z",
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(RuntimeError, match="retention marker"):
        retention._read_unleased_since_locked(store, relative_path)
    marker_path.write_text(
        json.dumps(
            {
                "schema_version": retention.UNLEASED_SCHEMA_VERSION,
                "relative_path": relative_path,
                "unleased_since": "bad",
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(RuntimeError, match="retention timestamp"):
        retention._read_unleased_since_locked(store, relative_path)
    marker_path.write_text(
        json.dumps(
            retention._unleased_payload(
                relative_path,
                datetime.datetime(2026, 7, 19, tzinfo=datetime.UTC),
            )
        ),
        encoding="utf-8",
    )
    assert (
        retention._is_newly_marked_unleased_locked(
            store,
            relative_path,
            now=datetime.datetime(2026, 7, 20, tzinfo=datetime.UTC),
        )
        is False
    )


def test_retention_prefix_reference_edges(tmp_path):
    """Resolve only regular files beneath a protected prefix."""
    store = PTG2ArtifactStore(tmp_path / "refs")
    assert retention._lease_referenced_paths_locked(
        store, {"paths": [], "prefixes": ["raw/missing"]}
    ) == set()
    prefix = store.root / "raw" / "prefix"
    prefix.mkdir(parents=True)
    target_path = prefix / "target"
    target_path.write_text("x", encoding="utf-8")
    (prefix / "link").symlink_to(target_path)
    assert retention._lease_referenced_paths_locked(
        store, {"paths": [], "prefixes": ["raw/prefix"]}
    ) == {"raw/prefix/target"}


def test_retention_lease_lifecycle_edges(tmp_path):
    """Exercise idempotent starts, duplicate markers, and lease binding."""
    store = PTG2ArtifactStore(tmp_path / "lease")
    lease = PTG2ArtifactLease(
        store=store, owner="first", heartbeat_seconds=0
    ).start()
    assert lease.start() is lease
    lease.release()
    duplicate = PTG2ArtifactLease(
        store=store,
        owner="duplicate",
        heartbeat_seconds=0,
        lease_id="fixed",
    )
    duplicate.marker_path.parent.mkdir(parents=True, exist_ok=True)
    duplicate.marker_path.write_text("{}", encoding="utf-8")
    with pytest.raises(RuntimeError, match="already exists"):
        duplicate.start()
    duplicate.marker_path.unlink()

    assert retention.has_released_current_artifact_lease() is False
    with bind_artifact_lease("not-registered"):
        assert retention.has_released_current_artifact_lease() is False
    assert (
        retention.has_protected_existing_artifact(
            store, store.root / "raw" / "missing"
        )
        is False
    )
    protect_artifact_prefix(store, store.root / "raw" / "unused")
    assert not list(store.leases_dir.glob("*.json"))
    prefix_lease = PTG2ArtifactLease(
        store=store, owner="prefix", heartbeat_seconds=0
    ).start()
    try:
        with bind_artifact_lease(prefix_lease.lease_id):
            protect_artifact_prefix(store, store.root / "raw" / "not-created")
        lease_payload_by_field = json.loads(
            prefix_lease.marker_path.read_text(encoding="utf-8")
        )
        assert lease_payload_by_field["prefixes"] == ["raw/not-created"]
    finally:
        prefix_lease.release()


def test_retention_gc_result_reports_remaining_capacity(tmp_path):
    """Report bytes remaining above the configured retained-input target."""
    store = PTG2ArtifactStore(tmp_path / "result")
    gc_result = retention.PTG2InputArtifactGCResult(
        executed=False,
        root=store.root,
        active_lease_ids=(),
        stale_lease_files=(),
        protected_files=(),
        newly_unleased_files=(),
        eligible_files=(),
        selected_files=(),
        deleted_files=(),
        total_bytes_before=20,
        total_bytes_after=15,
        selected_bytes=0,
        deleted_bytes=0,
        target_bytes=10,
        manifest_entries_before=0,
        manifest_entries_after=0,
        manifest_invalid_lines=0,
    )
    assert gc_result.over_target_bytes == 5


def test_retention_publish_conflict_edges(tmp_path):
    """Exercise directory, reuse, discard, and checksum publish conflicts."""
    store = PTG2ArtifactStore(tmp_path / "publish")
    staged = tmp_path / "staged"
    staged.write_text("new", encoding="utf-8")
    directory_target = store.root / "raw" / "directory"
    directory_target.mkdir(parents=True)
    with pytest.raises(RuntimeError, match="not a regular file"):
        publish_artifact_file(store, staged, directory_target)

    same = store.root / "raw" / "same"
    same.parent.mkdir(parents=True, exist_ok=True)
    same.write_text("same", encoding="utf-8")
    assert publish_artifact_file(store, same, same) == same

    existing = store.root / "raw" / "existing"
    existing.write_text("old", encoding="utf-8")
    discarded = tmp_path / "discarded"
    discarded.write_text("discard", encoding="utf-8")
    assert publish_artifact_file(store, discarded, existing) == existing
    assert not discarded.exists()

    checksum_target = store.root / "raw" / "checksum"
    checksum_target.write_text("old", encoding="utf-8")
    mismatched = tmp_path / "mismatched"
    mismatched.write_text("new", encoding="utf-8")
    with pytest.raises(RuntimeError, match="staging checksum"):
        publish_artifact_file(
            store,
            mismatched,
            checksum_target,
            expected_sha256="0" * 64,
        )


def test_verified_publish_replaces_old_target_without_hashing_it(
    tmp_path,
    monkeypatch,
):
    """Publish verified fresh bytes atomically without selecting old content."""

    store = PTG2ArtifactStore(tmp_path / "verified-publish")
    stage_dir = store.tmp_dir / "private-stage"
    stage_dir.mkdir(mode=0o700)
    staged = stage_dir / "fresh-stage"
    staged_payload = b"fresh verified artifact"
    staged.write_bytes(staged_payload)
    verified_sha256 = hashlib.sha256(staged_payload).hexdigest()
    final = store.artifact_path(verified_sha256)
    final.parent.mkdir(parents=True, exist_ok=True)
    final.write_bytes(b"stale retained artifact")
    old_inode = final.stat().st_ino

    def reject_old_target_hash(*_args, **_kwargs):
        raise AssertionError("fresh publication must not hash the old target")

    monkeypatch.setattr(retention, "sha256_file", reject_old_target_hash)

    with _capture_streamed_artifact_stage(
        store,
        staged,
        streamed_sha256=verified_sha256,
        streamed_byte_count=len(staged_payload),
    ) as verified_stage:
        published = publish_verified_artifact_stage(
            store,
            verified_stage,
            artifact_kind="raw",
        )

    assert published == final
    assert final.read_bytes() == staged_payload
    assert final.stat().st_ino != old_inode
    assert not staged.exists()


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


def _private_verified_stage(
    store: PTG2ArtifactStore,
    payload: bytes,
    *,
    name: str = "artifact.stage",
):
    stage_dir = store.tmp_dir / f"private-{name}"
    stage_dir.mkdir(mode=0o700)
    staged = stage_dir / name
    staged.write_bytes(payload)
    streamed_sha256 = hashlib.sha256(payload).hexdigest()
    stage = _capture_streamed_artifact_stage(
        store,
        staged,
        streamed_sha256=streamed_sha256,
        streamed_byte_count=len(payload),
    )
    return staged, streamed_sha256, stage


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
