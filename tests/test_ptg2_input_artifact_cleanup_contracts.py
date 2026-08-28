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

    unpublished = tmp_path / "unpublished"
    unpublished.write_text("wrong", encoding="utf-8")
    new_checksum_target = store.root / "raw" / "new-checksum"
    with pytest.raises(RuntimeError, match="staging checksum"):
        publish_artifact_file(
            store,
            unpublished,
            new_checksum_target,
            expected_sha256="0" * 64,
        )
    assert unpublished.exists()
    assert not new_checksum_target.exists()


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
