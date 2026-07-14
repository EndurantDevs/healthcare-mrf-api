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
    collect_ptg2_input_artifacts,
    guard_artifact_lease,
    protect_artifact_path,
    protect_artifact_prefix,
    protect_existing_artifact,
    publish_artifact_file,
)


NOW = datetime.datetime.now(datetime.UTC)


def _make_file(path: Path, payload: bytes, *, age_hours: float = 48) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    modified_at = (NOW - datetime.timedelta(hours=age_hours)).timestamp()
    os.utime(path, (modified_at, modified_at))
    return path


def _collect(root: Path, **overrides):
    options = {
        "root": root,
        "execute": True,
        "now": NOW,
        "retention_hours": 24,
        "min_age_hours": 1,
        "target_bytes": 0,
        "max_delete_bytes": None,
        "max_delete_files": None,
    }
    options.update(overrides)
    return collect_ptg2_input_artifacts(**options)


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
    results = []

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
            results.append(downloaded)
            barrier.wait(timeout=10)
            barrier.wait(timeout=10)

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(run_import, f"import-{index}") for index in range(2)]
        barrier.wait(timeout=10)
        logical_paths = {Path(result.logical_artifact.logical_path) for result in results}
        assert len(logical_paths) == 1
        logical_path = logical_paths.pop()
        assert logical_path.read_text(encoding="utf-8") == json.dumps(
            {"in_network": [{"code": "99213"}]}
        )
        assert len(list((root / "logical").rglob("*.json"))) == 1
        for path in (raw_path, logical_path):
            old = (NOW - datetime.timedelta(hours=48)).timestamp()
            os.utime(path, (old, old))
        result = _collect(root)
        assert set(result.protected_files) == {raw_path, logical_path}
        assert result.deleted_files == ()
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
    payload = b'{"in_network":[]}'
    plain_source = _make_file(tmp_path / "rates.json", payload, age_hours=0)
    gzip_named_source = _make_file(tmp_path / "rates.json.gz", payload, age_hours=0)

    async def fake_head(url: str):
        return source_download.PTG2HeadMetadata(
            url=url,
            status=200,
            content_length=len(payload),
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

        assert raw_path.read_bytes() == payload
        result = _collect(root)
        assert result.protected_files == (raw_path,)
        assert result.deleted_files == ()
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
    clock = {"now": NOW}
    monkeypatch.setattr(retention, "_utcnow", lambda: clock["now"])
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
            clock["now"] = NOW + datetime.timedelta(hours=elapsed_hours)
            lease.heartbeat()
        clock["now"] = NOW + datetime.timedelta(hours=72)

        result = _collect(root, now=clock["now"])

        assert result.active_lease_ids == (lease.lease_id,)
        assert result.protected_files == (raw_path,)
        assert result.deleted_files == ()
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
    base_record = {
        "artifact_kind": "raw",
        "canonical_url": "https://example.test/file.json",
        "raw_storage_uri": store.storage_uri(raw_path),
        "raw_sha256": "f" * 64,
        "status": "available",
    }
    store.record_manifest({**base_record, "etag": '"old"'})
    store.record_manifest({**base_record, "etag": '"new"'})
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
    partial_record = {
        "artifact_kind": "partial_raw",
        "canonical_url": "https://example.test/partial.json",
        "raw_storage_uri": store.storage_uri(partial_path),
        "status": "partial",
    }
    store.record_manifest({**partial_record, "partial_sha256": "2" * 64})
    store.record_manifest({**partial_record, "partial_sha256": "3" * 64})
    orphan_temps = [
        root / ".manifest.jsonl.crashed.tmp",
        store.leases_dir / ".lease.json.crashed.tmp",
        root / ".retention" / "unleased" / ".artifact.json.crashed.tmp",
    ]
    for orphan_temp in orphan_temps:
        orphan_temp.parent.mkdir(parents=True, exist_ok=True)
        orphan_temp.write_text("partial", encoding="utf-8")

    result = _collect(
        root,
        retention_hours=24,
        min_age_hours=1,
        target_bytes=1024,
    )
    compacted = [
        json.loads(line)
        for line in store.manifest_path.read_text(encoding="utf-8").splitlines()
    ]

    assert result.manifest_entries_before == 5
    assert result.manifest_entries_after == 2
    assert result.manifest_invalid_lines == 0
    assert compacted[0] == {
        **base_record,
        "etag": '"new"',
        "recorded_at": compacted[0]["recorded_at"],
    }
    assert compacted[1] == {
        **partial_record,
        "partial_sha256": "3" * 64,
        "recorded_at": compacted[1]["recorded_at"],
    }
    assert raw_path.exists()
    assert not any(path.exists() for path in orphan_temps)


def test_torn_manifest_tail_is_isolated_and_gc_fails_closed(tmp_path):
    root = tmp_path / "artifacts"
    store = PTG2ArtifactStore(root)
    raw_path = _make_file(store.artifact_path("7" * 64), b"retained")
    store.manifest_path.write_text('{"artifact_kind":"raw"', encoding="utf-8")
    valid_record = {
        "artifact_kind": "raw",
        "canonical_url": "https://example.test/retained.json",
        "raw_storage_uri": store.storage_uri(raw_path),
        "raw_sha256": "7" * 64,
        "status": "available",
    }

    store.record_manifest(valid_record)

    lines = store.manifest_path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2
    assert json.loads(lines[1]) == {
        **valid_record,
        "recorded_at": json.loads(lines[1])["recorded_at"],
    }
    assert store.find_candidates(valid_record["canonical_url"])
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


def test_one_shared_lease_release_does_not_start_grace_while_another_is_live(tmp_path):
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
    clock = [NOW]
    monkeypatch.setattr(retention, "_utcnow", lambda: clock[0])
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
            clock[0] = NOW + datetime.timedelta(hours=elapsed_hours)
            lease.heartbeat()

        result = _collect(root, now=NOW + datetime.timedelta(hours=72))

        assert result.active_lease_ids == (lease.lease_id,)
        assert result.deleted_files == ()
        assert raw_path.exists()
    finally:
        clock[0] = NOW + datetime.timedelta(hours=72)
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
    payload = gzip.compress(b'{"in_network":[]}', mtime=0)
    source_paths = [tmp_path / "first.json.gz", tmp_path / "second.gz"]
    for path in source_paths:
        path.write_bytes(payload)

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
