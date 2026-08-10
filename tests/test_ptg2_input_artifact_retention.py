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


def _stored_zip_artifact(tmp_path):
    root = tmp_path / "artifacts"
    store = PTG2ArtifactStore(root)
    staged_zip = tmp_path / "rates.zip"
    with zipfile.ZipFile(staged_zip, "w") as archive:
        archive.writestr(
            "nested/rates.json",
            json.dumps({"in_network": [{"code": "99213"}]}),
        )
    raw_sha256, raw_size = sha256_file(staged_zip)
    raw_path = store.artifact_path(raw_sha256, suffix=".zip")
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    os.replace(staged_zip, raw_path)
    return root, raw_path, PTG2RawArtifact(
        original_url="https://example.test/rates.zip",
        canonical_url="https://example.test/rates.zip",
        raw_path=str(raw_path),
        raw_storage_uri=store.storage_uri(raw_path),
        raw_sha256=raw_sha256,
        byte_count=raw_size,
    )


def test_concurrent_identical_artifact_has_one_logical_file_and_two_safe_leases(
    tmp_path,
    monkeypatch,
):
    """Verify concurrent identical artifact has one logical file and two safe leases."""
    root, raw_path, raw_artifact = _stored_zip_artifact(tmp_path)
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


def _manifest_compaction_fixture(root, store):
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
    return raw_path, base_record_dict, partial_record_dict, orphan_temps


def _download_test_artifact(source_path, store):
    return asyncio.run(
        source_download._download_raw_artifact_locked(
            source_path.as_uri(),
            store=store,
            canonical_url=source_path.as_uri(),
            reuse_raw_artifacts=False,
            max_bytes=None,
            keep_partial_artifacts=False,
        )
    )



def _assert_shared_artifact_survives(root, store, plain_source, gzip_source):
    plain_lease = PTG2ArtifactLease(store=store, owner="plain-url-import", heartbeat_seconds=0).start()
    gzip_lease = PTG2ArtifactLease(store=store, owner="gzip-url-import", heartbeat_seconds=0).start()
    try:
        with bind_artifact_lease(plain_lease.lease_id):
            raw_artifact = _download_test_artifact(plain_source, store)
        raw_path = Path(raw_artifact.raw_path)

        with bind_artifact_lease(gzip_lease.lease_id):
            with pytest.raises(RuntimeError, match="gzip header"):
                _download_test_artifact(gzip_source, store)
        retention_result = _collect(root)
        assert retention_result.protected_files == (raw_path,)
        assert retention_result.deleted_files == ()
        return raw_path
    finally:
        gzip_lease.release()
        plain_lease.release()


def test_url_validation_failure_does_not_unlink_shared_raw_artifact(
    tmp_path,
    monkeypatch,
):
    """Verify url validation failure does not unlink shared raw artifact."""
    root = tmp_path / "artifacts"
    store = PTG2ArtifactStore(root)
    source_bytes = b'{"in_network":[]}'
    plain_source = _make_file(tmp_path / "rates.json", source_bytes, age_hours=0)
    gzip_source = _make_file(tmp_path / "rates.json.gz", source_bytes, age_hours=0)

    async def fake_head(url: str):
        return source_download.PTG2HeadMetadata(
            url=url,
            status=200,
            content_length=len(source_bytes),
            supports_head=False,
        )

    monkeypatch.setattr(source_download, "fetch_head_metadata", fake_head)
    raw_path = _assert_shared_artifact_survives(
        root, store, plain_source, gzip_source
    )
    assert raw_path.read_bytes() == source_bytes


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
