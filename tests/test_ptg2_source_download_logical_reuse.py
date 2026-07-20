# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import hashlib
import json
import zipfile
from pathlib import Path

import pytest

from process.ptg_parts import source_download
from process.ptg_parts.artifacts import PTG2ArtifactStore, sha256_file
from process.ptg_parts.domain import PTG2HeadMetadata, PTG2RawArtifact
from process.ptg_parts.input_artifact_retention import artifact_lease_context


class _ImmediateExecutor:
    """Complete submitted callbacks inline for deterministic scheduling tests."""

    def __init__(self, **_kwargs):
        self.is_shutdown = False

    def submit(self, callback, *args, **kwargs):
        future = source_download.concurrent.futures.Future()
        try:
            future.set_result(callback(*args, **kwargs))
        except BaseException as exc:
            future.set_exception(exc)
        return future

    def shutdown(self, **_kwargs):
        self.is_shutdown = True


def _raw_zip_artifact(tmp_path: Path, store: PTG2ArtifactStore) -> PTG2RawArtifact:
    raw_path = tmp_path / "test-import.zip"
    logical_content_by_field = {"in_network": []}
    with zipfile.ZipFile(raw_path, "w") as archive:
        archive.writestr("nested/rates.json", json.dumps(logical_content_by_field))
    raw_sha256, raw_byte_count = sha256_file(raw_path)
    source_url = "https://example.invalid/test-import.zip"
    return PTG2RawArtifact(
        original_url=source_url,
        canonical_url=source_url,
        raw_path=str(raw_path),
        raw_storage_uri=store.storage_uri(raw_path),
        raw_sha256=raw_sha256,
        byte_count=raw_byte_count,
    )


def test_retained_zip_logical_artifact_marks_default_reuse(tmp_path):
    """Keep retained logical reuse enabled by default and expose each reuse hit."""

    store = PTG2ArtifactStore(tmp_path / "artifacts")
    raw_artifact = _raw_zip_artifact(tmp_path, store)

    async def materialize_twice():
        fresh_logical_artifact = await source_download._retained_logical_artifact(
            store,
            raw_artifact,
        )
        reused_logical_artifact = await source_download._retained_logical_artifact(
            store,
            raw_artifact,
        )
        return fresh_logical_artifact, reused_logical_artifact

    with artifact_lease_context(
        store=store,
        owner="logical-default-reuse-test",
        heartbeat_seconds=0,
    ):
        fresh_logical_artifact, reused_logical_artifact = asyncio.run(
            materialize_twice()
        )

    assert fresh_logical_artifact.reused is False
    assert reused_logical_artifact.reused is True
    assert reused_logical_artifact.logical_path == fresh_logical_artifact.logical_path
    assert reused_logical_artifact.logical_sha256 == fresh_logical_artifact.logical_sha256


def _install_fresh_zip_spies(monkeypatch, raw_artifact):
    expansion_paths = []
    real_stream_logical_artifact = source_download.stream_logical_artifact

    async def fake_download_raw_artifact(*_args, **kwargs):
        assert kwargs["reuse_raw_artifacts"] is False
        return raw_artifact

    def record_logical_expansion(raw_path, output_dir=None):
        expansion_paths.append(Path(raw_path))
        return real_stream_logical_artifact(raw_path, output_dir=output_dir)

    monkeypatch.setattr(
        source_download,
        "download_raw_artifact",
        fake_download_raw_artifact,
    )
    monkeypatch.setattr(
        source_download,
        "stream_logical_artifact",
        record_logical_expansion,
    )
    return expansion_paths


def test_disabled_raw_reuse_forces_fresh_zip_logical_expansion(
    tmp_path,
    monkeypatch,
):
    """Bypass a valid retained ZIP expansion when raw reuse is explicitly disabled."""

    store = PTG2ArtifactStore(tmp_path / "artifacts")
    raw_artifact = _raw_zip_artifact(tmp_path, store)
    monkeypatch.setenv("HLTHPRT_PTG2_ARTIFACT_DIR", str(store.root))
    expansion_paths = _install_fresh_zip_spies(monkeypatch, raw_artifact)
    with artifact_lease_context(
        store=store,
        owner="logical-forced-fresh-test",
        heartbeat_seconds=0,
    ):
        seeded_logical_artifact = asyncio.run(
            source_download._retained_logical_artifact(store, raw_artifact)
        )
        old_inode = Path(seeded_logical_artifact.logical_path).stat().st_ino
        expansion_paths.clear()
        downloaded_job = asyncio.run(
            source_download._download_ptg_job_artifact(
                {"type": "in_network", "url": raw_artifact.original_url},
                reuse_raw_artifacts=False,
                max_bytes=None,
                keep_partial_artifacts=False,
            )
        )

    assert downloaded_job.error is None
    assert downloaded_job.logical_artifact is not None
    assert downloaded_job.logical_artifact.reused is False
    assert expansion_paths == [Path(raw_artifact.raw_path)]
    assert Path(downloaded_job.logical_artifact.logical_path).stat().st_ino != (
        old_inode
    )


def test_fresh_raw_publish_replaces_retained_target_without_post_hash(
    tmp_path,
    monkeypatch,
):
    """Use streamed verification once and replace an old content-addressed inode."""

    source_path = tmp_path / "source.json"
    source_payload = b'{"in_network": []}'
    source_path.write_bytes(source_payload)
    raw_sha256 = hashlib.sha256(source_payload).hexdigest()
    store = PTG2ArtifactStore(tmp_path / "raw-artifacts")
    final_path = store.artifact_path(raw_sha256)
    final_path.parent.mkdir(parents=True, exist_ok=True)
    final_path.write_bytes(b"corrupt old retained bytes")
    old_inode = final_path.stat().st_ino

    async def fake_head(_url):
        return PTG2HeadMetadata(
            url=str(source_path),
            content_length=len(source_payload),
        )

    def reject_post_publish_hash(*_args, **_kwargs):
        raise AssertionError("fresh raw publication must not rehash final bytes")

    monkeypatch.setattr(source_download, "fetch_head_metadata", fake_head)
    monkeypatch.setattr(source_download, "sha256_file", reject_post_publish_hash)

    raw_artifact = asyncio.run(
        source_download._download_raw_artifact_locked(
            str(source_path),
            store=store,
            canonical_url=str(source_path),
            reuse_raw_artifacts=False,
            max_bytes=None,
            keep_partial_artifacts=False,
        )
    )

    assert raw_artifact.raw_sha256 == raw_sha256
    assert raw_artifact.byte_count == len(source_payload)
    assert raw_artifact.reused is False
    assert final_path.read_bytes() == source_payload
    assert final_path.stat().st_ino != old_inode


def _stage_tracker_raw_artifact(tmp_path, *, reused=False):
    raw_path = tmp_path / "rates.json"
    raw_payload = b'{"in_network": []}'
    raw_path.write_bytes(raw_payload)
    raw_sha256 = hashlib.sha256(raw_payload).hexdigest()
    return PTG2RawArtifact(
        original_url="https://example.invalid/rates.json",
        canonical_url="https://example.invalid/rates.json",
        raw_path=str(raw_path),
        raw_storage_uri=str(raw_path),
        raw_sha256=raw_sha256,
        byte_count=len(raw_payload),
        reused=reused,
    )


def test_stage_tracker_retains_raw_count_when_logical_stage_fails(
    tmp_path,
    monkeypatch,
):
    """Record completed raw work even when logical materialization errors."""

    raw_artifact = _stage_tracker_raw_artifact(tmp_path)
    tracker = source_download.PTG2FreshArtifactStageTracker()

    async def fake_download(*_args, **_kwargs):
        return raw_artifact

    def fail_logical(*_args, **_kwargs):
        raise RuntimeError("logical stage failed")

    monkeypatch.setattr(source_download, "download_raw_artifact", fake_download)
    monkeypatch.setattr(source_download, "logical_artifact_identity", fail_logical)

    downloaded = asyncio.run(
        source_download._download_ptg_job_artifact(
            {"type": "in_network", "url": raw_artifact.original_url},
            reuse_raw_artifacts=False,
            max_bytes=None,
            keep_partial_artifacts=False,
            artifact_stage_observer=tracker.observe,
        )
    )

    counts = tracker.snapshot()
    assert downloaded.error == "logical stage failed"
    assert counts.artifacts_observed == counts.raw_artifacts_total == 1
    assert counts.logical_artifacts_total == 0
    assert counts.is_reuse_detected is False


def test_stage_tracker_retains_raw_count_when_logical_stage_is_cancelled(
    tmp_path,
    monkeypatch,
):
    """Keep raw proof when cancellation interrupts retained ZIP expansion."""

    store = PTG2ArtifactStore(tmp_path / "cancel-artifacts")
    raw_artifact = _raw_zip_artifact(tmp_path, store)
    tracker = source_download.PTG2FreshArtifactStageTracker()

    async def fake_download(*_args, **_kwargs):
        return raw_artifact

    async def cancel_logical(*_args, **_kwargs):
        raise asyncio.CancelledError

    monkeypatch.setattr(source_download, "download_raw_artifact", fake_download)
    monkeypatch.setattr(
        source_download,
        "_retained_logical_artifact",
        cancel_logical,
    )

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(
            source_download._download_ptg_job_artifact(
                {"type": "in_network", "url": raw_artifact.original_url},
                reuse_raw_artifacts=False,
                max_bytes=None,
                keep_partial_artifacts=False,
                artifact_stage_observer=tracker.observe,
            )
        )

    counts = tracker.snapshot()
    assert counts.artifacts_observed == counts.raw_artifacts_total == 1
    assert counts.logical_artifacts_total == 0


def test_stage_tracker_fails_immediately_when_raw_stage_reports_reuse(
    tmp_path,
    monkeypatch,
):
    """Fail closed before logical work when a fresh raw stage reports reuse."""

    raw_artifact = _stage_tracker_raw_artifact(tmp_path, reused=True)
    tracker = source_download.PTG2FreshArtifactStageTracker()

    async def fake_download(*_args, **_kwargs):
        return raw_artifact

    monkeypatch.setattr(source_download, "download_raw_artifact", fake_download)

    with pytest.raises(
        source_download.PTG2ArtifactStageFreshnessError
    ) as exc_info:
        asyncio.run(
            source_download._download_ptg_job_artifact(
                {"type": "in_network", "url": raw_artifact.original_url},
                reuse_raw_artifacts=False,
                max_bytes=None,
                keep_partial_artifacts=False,
                artifact_stage_observer=tracker.observe,
            )
        )

    assert exc_info.value.counts.raw_artifacts_reused == 1
    assert exc_info.value.reason == "reused"
    assert tracker.snapshot().is_reuse_detected is True


def test_stage_tracker_fails_on_duplicate_identity_without_exposing_it(tmp_path):
    """Reject a second same-kind identity after recording duplicate proof."""

    raw_artifact = _stage_tracker_raw_artifact(tmp_path)
    tracker = source_download.PTG2FreshArtifactStageTracker()
    observation = source_download.PTG2ArtifactStageObservation(
        artifact_kind=source_download.PTG2_ARTIFACT_RAW,
        identity_sha256=raw_artifact.raw_sha256,
        byte_count=raw_artifact.byte_count,
    )
    tracker.observe(observation)

    with pytest.raises(
        source_download.PTG2ArtifactStageFreshnessError
    ) as exc_info:
        tracker.observe(observation)

    counts = exc_info.value.counts
    assert exc_info.value.reason == "duplicate_identity"
    assert raw_artifact.raw_sha256 not in str(exc_info.value)
    assert counts.raw_artifacts_total == 2
    assert counts.raw_artifacts_unique == 1
    assert counts.raw_artifacts_duplicate_identities == 1


def test_download_prefetch_does_not_refill_after_freshness_error(
    tmp_path,
    monkeypatch,
):
    """Surface a completed freshness failure before scheduling another job."""

    raw_artifact = _stage_tracker_raw_artifact(tmp_path)
    tracker = source_download.PTG2FreshArtifactStageTracker()
    observation = source_download.PTG2ArtifactStageObservation(
        artifact_kind=source_download.PTG2_ARTIFACT_RAW,
        identity_sha256=raw_artifact.raw_sha256,
        byte_count=raw_artifact.byte_count,
    )
    tracker.observe(observation)
    started_urls = []

    def fail_freshness(job, **_kwargs):
        started_urls.append(job["url"])
        tracker.observe(observation)

    async def consume_downloads():
        async for _downloaded in source_download._iter_downloaded_ptg_jobs(
            [
                {"type": "in_network", "url": "https://example.invalid/first"},
                {"type": "in_network", "url": "https://example.invalid/second"},
            ],
            reuse_raw_artifacts=False,
            max_bytes=None,
            keep_partial_artifacts=False,
            artifact_stage_observer=tracker.observe,
        ):
            raise AssertionError("freshness failure must precede yielded downloads")

    monkeypatch.setenv(source_download.PTG2_DOWNLOAD_TASKS_ENV, "1")
    monkeypatch.setattr(
        source_download.concurrent.futures,
        "ThreadPoolExecutor",
        _ImmediateExecutor,
    )
    monkeypatch.setattr(
        source_download,
        "_download_ptg_job_artifact_sync_from_facade",
        fail_freshness,
    )

    with pytest.raises(source_download.PTG2ArtifactStageFreshnessError):
        asyncio.run(consume_downloads())

    assert started_urls == ["https://example.invalid/first"]
