# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded source-pipeline proof for hospital-price orchestration."""

from __future__ import annotations

import asyncio
from collections import Counter
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
async def test_source_pipeline_overlaps_download_and_deduplicates_invalid_content(
    tmp_path, monkeypatch
):
    orchestrator = _orchestrator_module()
    first_parse_started = asyncio.Event()
    calls_by_digest: Counter[str] = Counter()
    raw_path = tmp_path / "hospital-mrf-source-shared" / "raw" / "source.json"
    raw_path.parent.mkdir(parents=True)
    raw_path.write_bytes(b"{}")
    raw = SimpleNamespace(
        raw_sha256="a" * 64, raw_path=str(raw_path), byte_count=2
    )
    attempts_by_url = {
        "https://a/first.json": [_Attempt("one", "a", "A", "https://a/first.json", 0)],
        "https://b/second.json": [_Attempt("two", "b", "B", "https://b/second.json", 0)],
    }

    async def download_source(
        item: Any, _store: Any, _max_bytes: int
    ) -> _DownloadedSource:
        url, attempts = item
        if url.endswith("second.json"):
            await asyncio.wait_for(first_parse_started.wait(), timeout=1)
        return _DownloadedSource(url, raw, attempts)

    async def ensure_content(
        _ctx: Any, _task: Any, _store: Any, downloaded_raw: Any,
        _max_decompressed_bytes: int,
        _max_output_bytes: int,
        _required_free_bytes: int,
    ) -> str:
        digest = downloaded_raw.raw_sha256
        calls_by_digest[digest] += 1
        first_parse_started.set()
        await asyncio.sleep(0)
        raise ValueError("invalid duplicate content")

    async def fail_attempts(attempts: Any, *_args: Any) -> int:
        return len(attempts)

    monkeypatch.setattr(orchestrator, "download_source", download_source)
    monkeypatch.setattr(orchestrator, "_ensure_content", ensure_content)
    monkeypatch.setattr(orchestrator, "_fail_attempts", fail_attempts)
    monkeypatch.setattr(orchestrator, "_progress", lambda *_args: None)

    pipeline_metrics = await asyncio.wait_for(
        orchestrator._stream_sources(
            {}, {}, _ArtifactStore(tmp_path), attempts_by_url,
            (1, 2, 1, 2), (None, 0, 2),
            ("hospital-prices:test", 1024, 4096, 2048, 1),
        ),
        timeout=2,
    )

    assert pipeline_metrics == {
        "processed": 2, "published": 0, "superseded": 0, "unchanged": 0,
        "failed": 2, "contents": 1,
    }
    assert calls_by_digest[raw.raw_sha256] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("content_error", (None, ValueError("invalid content")))
async def test_source_pipeline_deletes_private_raw_before_terminal_outcome(
    tmp_path, monkeypatch, content_error
):
    orchestrator = _orchestrator_module()
    attempt = _Attempt("one", "a", "A", "https://a/source.json", 0)
    source_roots: list[Path] = []
    raw_paths: list[Path] = []
    sentinel = tmp_path / "unrelated.raw"
    sentinel.write_bytes(b"preserve")

    async def download_source(
        item: Any, source_store: Any, _max_bytes: int
    ) -> _DownloadedSource:
        source_root = Path(source_store.root)
        raw_path = source_root / "raw" / "source.json"
        raw_path.parent.mkdir(parents=True)
        raw_path.write_bytes(b"{}")
        source_roots.append(source_root)
        raw_paths.append(raw_path)
        return _DownloadedSource(
            item[0],
            SimpleNamespace(raw_sha256="a" * 64, raw_path=str(raw_path), byte_count=2),
            item[1],
        )

    async def ensure_content(*_args: Any) -> str:
        assert raw_paths[-1].is_file()
        if content_error is not None:
            raise content_error
        return "version"

    async def publish(*_args: Any) -> tuple[int, int, int, int]:
        assert not raw_paths[-1].exists()
        return 1, 0, 0, 0

    async def fail_attempts(attempts: Any, *_args: Any) -> int:
        assert not raw_paths[-1].exists()
        return len(attempts)

    monkeypatch.setattr(orchestrator, "download_source", download_source)
    monkeypatch.setattr(orchestrator, "_ensure_content", ensure_content)
    monkeypatch.setattr(orchestrator, "_publish_download", publish)
    monkeypatch.setattr(orchestrator, "_fail_attempts", fail_attempts)
    monkeypatch.setattr(orchestrator, "_require_disk_capacity", lambda *_args: None)
    monkeypatch.setattr(orchestrator, "_progress", lambda *_args: None)

    metrics = await orchestrator._stream_sources(
        {}, {}, _ArtifactStore(tmp_path), {attempt.source_url: [attempt]},
        (1, 1, 1, 1), (None, 0, 1),
        ("hospital-prices:test", 1024, 4096, 2048, 1),
    )

    assert metrics["published"] == (content_error is None)
    assert metrics["failed"] == (content_error is not None)
    assert len(source_roots) == 1
    assert source_roots[0].parent == tmp_path
    assert not source_roots[0].exists()
    assert sentinel.read_bytes() == b"preserve"


@pytest.mark.asyncio
async def test_source_cleanup_failure_blocks_publication(tmp_path, monkeypatch):
    orchestrator = _orchestrator_module()
    attempt = _Attempt("one", "a", "A", "https://a/source.json", 0)
    unrelated_raw = tmp_path / "unrelated-source.json"
    unrelated_raw.write_bytes(b"{}")
    publish = AsyncMock(side_effect=AssertionError("publication must not run"))

    async def download_source(item: Any, _store: Any, _max_bytes: int):
        return _DownloadedSource(
            item[0],
            SimpleNamespace(
                raw_sha256="a" * 64,
                raw_path=str(unrelated_raw),
                byte_count=2,
            ),
            item[1],
        )

    monkeypatch.setattr(orchestrator, "download_source", download_source)
    monkeypatch.setattr(orchestrator, "_ensure_content", AsyncMock(return_value="version"))
    monkeypatch.setattr(orchestrator, "_publish_download", publish)
    monkeypatch.setattr(orchestrator, "_require_disk_capacity", lambda *_args: None)
    monkeypatch.setattr(orchestrator, "_progress", lambda *_args: None)

    with pytest.raises(RuntimeError, match="outside task-owned raw scratch"):
        await orchestrator._stream_sources(
            {}, {}, _ArtifactStore(tmp_path), {attempt.source_url: [attempt]},
            (1, 1, 1, 1), (None, 0, 1),
            ("hospital-prices:test", 1024, 4096, 2048, 1),
        )
    assert unrelated_raw.read_bytes() == b"{}"
    publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_mixed_403_and_500_publishes_only_the_rebound_sibling(
    tmp_path, monkeypatch
):
    orchestrator = _orchestrator_module()
    expired_url = "https://files.example/prices.json?sig=expired"
    fresh_url = "https://files.example/prices.json?sig=fresh"
    attempts = tuple(
        _Attempt(f"attempt-{key}", key, f"Hospital {key.upper()}", expired_url,
                 0, source_http_status=status)
        for key, status in (("a", 403), ("b", 500))
    )
    published_attempts: list[tuple[_Attempt, ...]] = []
    failed_attempts: list[tuple[_Attempt, ...]] = []

    async def download(source_job, source_store, _max_bytes, **kwargs):
        url, grouped_attempts = source_job
        if url == expired_url:
            assert [attempt.source_http_status for attempt in grouped_attempts] == [403, 500]
            return _DownloadedSource(
                url, None, grouped_attempts, "permission", "expired", True
            )
        assert kwargs == {"exact_url_only": True}
        raw_path = Path(source_store.root) / "raw" / "source.json"
        raw_path.parent.mkdir(parents=True)
        raw_path.write_bytes(b"{}")
        raw = SimpleNamespace(raw_sha256="a" * 64, raw_path=str(raw_path), byte_count=2)
        return _DownloadedSource(url, raw, grouped_attempts)

    async def fail(grouped_attempts, *_args):
        failed_attempts.append(grouped_attempts)
        return len(grouped_attempts)

    async def publish(_ctx, _task, downloaded, _version):
        published_attempts.append(downloaded.attempts)
        return 1, 0, 0, 0

    for name, collaborator in (
        ("download_source", download),
        ("_refreshed_source_job", AsyncMock(return_value=(fresh_url, (attempts[0],)))),
        ("_ensure_content", AsyncMock(return_value=None)),
        ("_fail_attempts", fail), ("_publish_download", publish),
    ):
        monkeypatch.setattr(orchestrator, name, collaborator)
    monkeypatch.setattr(orchestrator, "_require_disk_capacity", lambda *_args: None)
    monkeypatch.setattr(orchestrator, "_progress", lambda *_args: None)

    metrics = await orchestrator._stream_sources(
        {}, {}, _ArtifactStore(tmp_path), {expired_url: list(attempts)},
        (1, 1, 1, 1), (None, 0, 2),
        ("hospital-prices:test", 1024, 4096, 2048, 1),
    )

    assert metrics == {
        "processed": 2, "published": 1, "superseded": 0,
        "unchanged": 0, "failed": 1, "contents": 1,
    }
    assert published_attempts == [(attempts[0],)]
    assert failed_attempts == [(attempts[1],)]
