# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Cancellation and lease proof for hospital-price source orchestration."""

from __future__ import annotations

import asyncio
import contextlib
import threading
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from tests.hospital_price_orchestration_support import (
    ArtifactStore as _ArtifactStore,
    Attempt as _Attempt,
    DownloadedSource as _DownloadedSource,
    orchestrator_module as _orchestrator_module,
)

@pytest.mark.asyncio
async def test_queued_source_acknowledgements_are_released_on_pipeline_cancel():
    orchestrator = _orchestrator_module()
    downloads: asyncio.Queue[Any] = asyncio.Queue()
    acknowledgement = asyncio.get_running_loop().create_future()
    downloads.put_nowait((SimpleNamespace(raw=None), acknowledgement))
    downloads.put_nowait(None)

    orchestrator._acknowledge_queued_downloads(downloads)

    assert acknowledgement.done()
    assert downloads.empty()


@pytest.mark.asyncio
async def test_cancellation_during_source_cleanup_blocks_publication(
    tmp_path, monkeypatch
):
    orchestrator = _orchestrator_module()
    attempt = _Attempt("one", "a", "A", "https://a/source.json", 0)
    cleanup_started, allow_cleanup = threading.Event(), threading.Event()
    source_roots: list[Path] = []
    raw_paths: list[Path] = []
    publish = AsyncMock(side_effect=AssertionError("publication must not run"))
    unlink = orchestrator._unlink_transient_source

    async def download_source(item: Any, source_store: Any, _max_bytes: int):
        source_root = Path(source_store.root)
        raw_path = source_root / "raw" / "source.json"
        raw_path.parent.mkdir(parents=True)
        raw_path.write_bytes(b"{}")
        source_roots.append(source_root)
        raw_paths.append(raw_path)
        return _DownloadedSource(
            item[0],
            SimpleNamespace(
                raw_sha256="a" * 64,
                raw_path=str(raw_path),
                byte_count=2,
            ),
            item[1],
        )

    def blocked_unlink(store: Any, raw: Any) -> None:
        cleanup_started.set()
        assert allow_cleanup.wait(timeout=2)
        unlink(store, raw)

    monkeypatch.setattr(orchestrator, "download_source", download_source)
    monkeypatch.setattr(orchestrator, "_ensure_content", AsyncMock(return_value="version"))
    monkeypatch.setattr(orchestrator, "_publish_download", publish)
    monkeypatch.setattr(orchestrator, "_unlink_transient_source", blocked_unlink)
    monkeypatch.setattr(orchestrator, "_require_disk_capacity", lambda *_args: None)
    monkeypatch.setattr(orchestrator, "_progress", lambda *_args: None)

    pipeline = asyncio.create_task(orchestrator._stream_sources(
        {}, {}, _ArtifactStore(tmp_path), {attempt.source_url: [attempt]},
        (1, 1, 1, 1), (None, 0, 1),
        ("hospital-prices:test", 1024, 4096, 2048, 1),
    ))
    assert await asyncio.to_thread(cleanup_started.wait, 1)
    pipeline.cancel()
    await asyncio.sleep(0)
    pipeline.cancel()
    allow_cleanup.set()

    with pytest.raises(asyncio.CancelledError):
        await pipeline

    publish.assert_not_awaited()
    assert not raw_paths[0].exists()
    assert not source_roots[0].exists()


def _source_consumption_state(tmp_path):
    raw_path = tmp_path / "hospital-mrf-source-shared" / "raw" / "source.json"
    raw_path.parent.mkdir(parents=True)
    raw_path.write_bytes(b"{}")
    return SimpleNamespace(
        parse_started=asyncio.Event(),
        allow_parse=asyncio.Event(),
        started_urls=[],
        lease_events=[],
        slot_events=[],
        slot_entered=asyncio.Event(),
        raw=SimpleNamespace(
            raw_sha256="a" * 64, raw_path=str(raw_path), byte_count=2
        ),
        attempts_by_url={
            "https://a/first.json": [
                _Attempt("one", "a", "A", "https://a/first.json", 0)
            ],
            "https://b/second.json": [
                _Attempt("two", "b", "B", "https://b/second.json", 0)
            ],
        },
    )


@pytest.mark.asyncio
async def test_source_worker_holds_slot_and_lease_until_raw_cleanup(
    tmp_path, monkeypatch
):
    """Retain fetch capacity and artifact lease through raw consumption."""

    orchestrator = _orchestrator_module()
    state = _source_consumption_state(tmp_path)
    @contextlib.asynccontextmanager
    async def resource_slot(_store, resource, slot_count):
        assert slot_count == 1
        if resource == "fetch":
            state.slot_events.append("enter")
            state.slot_entered.set()
        try:
            yield
        finally:
            if resource == "fetch":
                state.slot_events.append("exit")
    @contextlib.contextmanager
    def lease_context(**_kwargs: Any):
        state.lease_events.append("start")
        try:
            yield object()
        finally:
            state.lease_events.append("release")
    async def download_source(item: Any, _store: Any, _max_bytes: int):
        url, attempts = item
        state.started_urls.append(url)
        return _DownloadedSource(url, state.raw, attempts)
    async def ensure_content(*_args: Any) -> str:
        if len(state.started_urls) == 1:
            state.parse_started.set()
            await state.allow_parse.wait()
        raise ValueError("invalid content")
    async def fail_attempts(attempts: Any, *_args: Any) -> int:
        return len(attempts)
    for name, collaborator in (
        ("artifact_lease_context", lease_context),
        ("_hospital_resource_slot", resource_slot), ("download_source", download_source),
        ("_ensure_content", ensure_content), ("_fail_attempts", fail_attempts),
        ("_require_disk_capacity", lambda *_args: None),
        ("_progress", lambda *_args: None),
    ):
        monkeypatch.setattr(orchestrator, name, collaborator)
    pipeline = asyncio.create_task(orchestrator._stream_sources(
        {}, {}, _ArtifactStore(tmp_path), state.attempts_by_url, (1, 1, 1, 1),
        (None, 0, 2), ("hospital-prices:test", 1024, 4096, 2048, 1),
    ))
    await asyncio.wait_for(state.slot_entered.wait(), timeout=1)
    await asyncio.wait_for(state.parse_started.wait(), timeout=1)
    await asyncio.sleep(0)
    assert state.started_urls == ["https://a/first.json"]
    assert state.lease_events == ["start"]
    assert state.slot_events == ["enter"]
    state.allow_parse.set()
    metrics = await asyncio.wait_for(pipeline, timeout=2)
    assert metrics["failed"] == 2
    assert state.started_urls == ["https://a/first.json", "https://b/second.json"]
    assert state.lease_events == ["start", "release", "start", "release"]
    assert state.slot_events == ["enter", "exit", "enter", "exit"]


def _install_waiting_producer_pipeline(orchestrator, monkeypatch, tmp_path):
    state = SimpleNamespace(
        parse_started=asyncio.Event(),
        both_downloaded=asyncio.Event(),
        cleanup_started=asyncio.Event(),
        first_release=asyncio.Event(),
        allow_cleanup=asyncio.Event(),
        lease_events=[],
        source_roots=[],
        raw_paths=[],
        attempts=(
            _Attempt("one", "a", "A", "https://a/source.json", 0),
            _Attempt("two", "b", "B", "https://b/source.json", 0),
        ),
    )

    @contextlib.contextmanager
    def lease_context(**_kwargs: Any):
        state.lease_events.append("start")
        try:
            yield object()
        finally:
            state.lease_events.append("release")
            if state.lease_events.count("release") == 1:
                state.first_release.set()

    async def download_source(
        item: Any, source_store: Any, _max_bytes: int
    ) -> _DownloadedSource:
        source_root = Path(source_store.root)
        raw_path = source_root / "raw" / "source.json"
        raw_path.parent.mkdir(parents=True)
        raw_path.write_bytes(b"{}")
        state.source_roots.append(source_root)
        state.raw_paths.append(raw_path)
        if len(state.raw_paths) == 2:
            state.both_downloaded.set()
        raw = SimpleNamespace(
            raw_sha256="a" * 64, raw_path=str(raw_path), byte_count=2
        )
        return _DownloadedSource(item[0], raw, item[1])

    async def ensure_content(*_args: Any) -> str:
        assert state.raw_paths[-1].is_file()
        state.parse_started.set()
        try:
            await asyncio.Future()
        finally:
            state.cleanup_started.set()
            await state.allow_cleanup.wait()

    monkeypatch.setattr(orchestrator, "artifact_lease_context", lease_context)
    monkeypatch.setattr(orchestrator, "download_source", download_source)
    monkeypatch.setattr(orchestrator, "_ensure_content", ensure_content)
    monkeypatch.setattr(orchestrator, "_require_disk_capacity", lambda *_args: None)
    monkeypatch.setattr(orchestrator, "_progress", lambda *_args: None)
    return state


@pytest.mark.asyncio
async def test_source_pipeline_cancellation_releases_waiting_producer_lease(
    tmp_path, monkeypatch
):
    orchestrator = _orchestrator_module()
    state = _install_waiting_producer_pipeline(orchestrator, monkeypatch, tmp_path)

    pipeline = asyncio.create_task(orchestrator._stream_sources(
        {}, {}, _ArtifactStore(tmp_path),
        {attempt.source_url: [attempt] for attempt in state.attempts},
        (2, 1, 2, 1), (None, 0, 2),
        ("hospital-prices:test", 1024, 4096, 2048, 1),
    ))
    await asyncio.wait_for(state.parse_started.wait(), timeout=1)
    await asyncio.wait_for(state.both_downloaded.wait(), timeout=1)
    pipeline.cancel()
    await asyncio.wait_for(state.cleanup_started.wait(), timeout=1)
    await asyncio.wait_for(state.first_release.wait(), timeout=1)
    assert state.lease_events == ["start", "start", "release"]
    assert sum(raw_path.exists() for raw_path in state.raw_paths) == 1
    assert sum(source_root.exists() for source_root in state.source_roots) == 1
    pipeline.cancel()
    state.allow_cleanup.set()
    with pytest.raises(asyncio.CancelledError):
        await pipeline

    assert state.lease_events == ["start", "start", "release", "release"]
    assert all(not raw_path.exists() for raw_path in state.raw_paths)
    assert all(not source_root.exists() for source_root in state.source_roots)
