# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Focused pipeline and lifecycle proof for hospital-price orchestration."""

from __future__ import annotations

import asyncio
import contextlib
from collections import Counter
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from tests.hospital_price_orchestration_support import (
    ROOT,
    ArtifactStore,
    Attempt,
    DownloadedSource,
    configure_incomplete_import,
    orchestrator_module,
)


_ArtifactStore = ArtifactStore
_Attempt = Attempt
_DownloadedSource = DownloadedSource
_orchestrator_module = orchestrator_module


def test_locator_groups_duplicate_urls_once():
    orchestrator = _orchestrator_module()
    hospitals = (
        {"hospital_id": "a", "name": "A", "cms_hpt_url": "https://a/locator"},
        {"hospital_id": "b", "name": "B", "cms_hpt_url": "https://a/locator"},
        {"hospital_id": "c", "name": "C", "cms_hpt_url": "https://c/locator"},
    )

    groups = orchestrator._locator_groups(hospitals)

    assert [group[0] for group in groups] == ["https://a/locator", "https://c/locator"]
    assert [hospital["hospital_id"] for hospital in groups[0][1]] == ["a", "b"]


@pytest.mark.asyncio
async def test_parser_failure_cleans_private_directory(
    tmp_path, monkeypatch
):
    orchestrator = _orchestrator_module()
    output_paths: list[Path] = []

    async def has_version(*_args: Any) -> bool:
        return False

    async def failed_parser(_source: Path, output: Path, *_args: Any) -> None:
        output_paths.append(output)
        (output / "partial.copy").write_bytes(b"partial")
        raise ValueError("invalid MRF")

    monkeypatch.setattr(orchestrator, "has_existing_version", has_version)
    monkeypatch.setattr(orchestrator, "run_native_parser", failed_parser)
    source_path = tmp_path / "source.json"
    source_path.write_text("{}")
    raw = SimpleNamespace(
        raw_sha256="a" * 64, raw_path=str(source_path), byte_count=2
    )
    with pytest.raises(ValueError, match="invalid MRF"):
        await orchestrator._ensure_content(
            {}, {}, _ArtifactStore(tmp_path), raw, 2048, 1024
        )

    assert len(output_paths) == 1
    assert not output_paths[0].exists()


@pytest.mark.asyncio
async def test_existing_content_skips_native_parser(tmp_path, monkeypatch):
    orchestrator = _orchestrator_module()
    monkeypatch.setattr(
        orchestrator, "has_existing_version", AsyncMock(return_value=True)
    )
    parser = AsyncMock(side_effect=AssertionError("parser must not run"))
    monkeypatch.setattr(orchestrator, "run_native_parser", parser)
    raw = SimpleNamespace(
        raw_sha256="a" * 64, raw_path=str(tmp_path / "source.json"), byte_count=2
    )

    assert await orchestrator._ensure_content(
        {}, {}, _ArtifactStore(tmp_path), raw, 2048, 1024
    ) == orchestrator.hospital_price_version_id(raw.raw_sha256)
    parser.assert_not_awaited()


def test_control_plane_wiring_names_one_dedicated_queue():
    process_source = (ROOT / "process/__init__.py").read_text()
    worker_source = (ROOT / "api/control_workers.py").read_text()
    imports_source = (ROOT / "api/control_imports.py").read_text()

    assert "class HospitalPrices:" in process_source
    assert 'queue_name = "arq:HospitalPrices"' in process_source
    assert 'process_group.add_command(hospital_prices, name="hospital-prices")' in process_source
    assert 'WorkerSpec("arq:HospitalPrices", "process.HospitalPrices"' in worker_source
    assert '"hospital-prices": {' in imports_source
    assert '"queue": "arq:HospitalPrices"' in imports_source


def test_resource_limits_derive_workers_from_explicit_byte_budgets(
    tmp_path, monkeypatch
):
    orchestrator = _orchestrator_module()
    configured_env_by_name = {
        "HLTHPRT_HOSPITAL_MRF_MAX_BYTES": "100",
        "HLTHPRT_HOSPITAL_MRF_MAX_DECOMPRESSED_BYTES": "120",
        "HLTHPRT_HOSPITAL_MRF_MAX_OUTPUT_BYTES": "80",
        "HLTHPRT_HOSPITAL_PRICE_ACTIVE_RAW_BYTES": "250",
        "HLTHPRT_HOSPITAL_PRICE_ACTIVE_SCRATCH_BYTES": "170",
        "HLTHPRT_HOSPITAL_PRICE_MIN_FREE_BYTES": "20",
    }
    for env_name, env_value in configured_env_by_name.items():
        monkeypatch.setenv(env_name, env_value)
    monkeypatch.setattr(
        orchestrator._runtime.shutil,
        "disk_usage",
        lambda _path: SimpleNamespace(free=500),
    )

    assert orchestrator._resource_limits(_ArtifactStore(tmp_path), 8, 5, 0) == (
        2, 2, 100, 120, 80, 440,
    )

    required_byte_counts: list[int] = []
    monkeypatch.setattr(
        orchestrator._runtime, "require_disk_capacity",
        lambda _store, byte_count: required_byte_counts.append(byte_count),
    )
    orchestrator._resource_limits(_ArtifactStore(tmp_path), 8, 5, 3)
    assert required_byte_counts == [3_000_440]

    monkeypatch.setenv("HLTHPRT_HOSPITAL_PRICE_ACTIVE_RAW_BYTES", "99")
    with pytest.raises(RuntimeError, match="cannot admit one source"):
        orchestrator._resource_limits(_ArtifactStore(tmp_path), 8, 5, 0)


def test_resource_limits_fail_before_work_when_capacity_is_unconfigured_or_low(
    tmp_path, monkeypatch
):
    orchestrator = _orchestrator_module()
    for name in (
        "HLTHPRT_HOSPITAL_MRF_MAX_OUTPUT_BYTES",
        "HLTHPRT_HOSPITAL_MRF_MAX_DECOMPRESSED_BYTES",
        "HLTHPRT_HOSPITAL_PRICE_ACTIVE_RAW_BYTES",
        "HLTHPRT_HOSPITAL_PRICE_ACTIVE_SCRATCH_BYTES",
        "HLTHPRT_HOSPITAL_PRICE_MIN_FREE_BYTES",
    ):
        monkeypatch.delenv(name, raising=False)
    with pytest.raises(RuntimeError, match="MAX_OUTPUT_BYTES"):
        orchestrator._resource_limits(_ArtifactStore(tmp_path), 1, 1, 0)

    for env_name, env_value in {
        "HLTHPRT_HOSPITAL_MRF_MAX_BYTES": "100",
        "HLTHPRT_HOSPITAL_MRF_MAX_DECOMPRESSED_BYTES": "100",
        "HLTHPRT_HOSPITAL_MRF_MAX_OUTPUT_BYTES": "100",
        "HLTHPRT_HOSPITAL_PRICE_ACTIVE_RAW_BYTES": "100",
        "HLTHPRT_HOSPITAL_PRICE_ACTIVE_SCRATCH_BYTES": "100",
        "HLTHPRT_HOSPITAL_PRICE_MIN_FREE_BYTES": "1",
    }.items():
        monkeypatch.setenv(env_name, env_value)
    monkeypatch.setattr(
        orchestrator._runtime.shutil,
        "disk_usage",
        lambda _path: SimpleNamespace(free=200),
    )
    with pytest.raises(RuntimeError, match="storage capacity is insufficient"):
        orchestrator._resource_limits(_ArtifactStore(tmp_path), 1, 1, 0)


@pytest.mark.asyncio
async def test_source_pipeline_overlaps_download_and_deduplicates_invalid_content(
    tmp_path, monkeypatch
):
    orchestrator = _orchestrator_module()
    first_parse_started = asyncio.Event()
    calls_by_digest: Counter[str] = Counter()
    raw = SimpleNamespace(
        raw_sha256="a" * 64, raw_path=str(tmp_path / "source.json"), byte_count=2
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
            {}, {}, _ArtifactStore(tmp_path), attempts_by_url, (1, 2), (None, 0, 2),
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
async def test_source_worker_holds_one_lease_until_raw_content_is_consumed(tmp_path, monkeypatch):
    orchestrator = _orchestrator_module()
    parse_started, allow_parse = asyncio.Event(), asyncio.Event()
    started_download_urls: list[str] = []
    lease_events: list[str] = []
    raw = SimpleNamespace(
        raw_sha256="a" * 64, raw_path=str(tmp_path / "source.json"), byte_count=2
    )
    attempts_by_url = {
        "https://a/first.json": [_Attempt("one", "a", "A", "https://a/first.json", 0)],
        "https://b/second.json": [_Attempt("two", "b", "B", "https://b/second.json", 0)],
    }

    @contextlib.contextmanager
    def lease_context(**_kwargs: Any):
        lease_events.append("start")
        try:
            yield object()
        finally:
            lease_events.append("release")

    async def download_source(
        item: Any, _store: Any, _max_bytes: int
    ) -> _DownloadedSource:
        url, attempts = item
        started_download_urls.append(url)
        return _DownloadedSource(url, raw, attempts)

    async def ensure_content(*_args: Any) -> str:
        if len(started_download_urls) == 1:
            parse_started.set()
            await allow_parse.wait()
        raise ValueError("invalid content")

    async def fail_attempts(attempts: Any, *_args: Any) -> int:
        return len(attempts)

    monkeypatch.setattr(orchestrator, "artifact_lease_context", lease_context)
    monkeypatch.setattr(orchestrator, "download_source", download_source)
    monkeypatch.setattr(orchestrator, "_ensure_content", ensure_content)
    monkeypatch.setattr(orchestrator, "_fail_attempts", fail_attempts)
    monkeypatch.setattr(orchestrator, "_require_disk_capacity", lambda *_args: None)
    monkeypatch.setattr(orchestrator, "_progress", lambda *_args: None)

    pipeline = asyncio.create_task(orchestrator._stream_sources(
        {}, {}, _ArtifactStore(tmp_path), attempts_by_url, (1, 1), (None, 0, 2),
        ("hospital-prices:test", 1024, 4096, 2048, 1),
    ))
    await asyncio.wait_for(parse_started.wait(), timeout=1)
    await asyncio.sleep(0)
    assert started_download_urls == ["https://a/first.json"]
    assert lease_events == ["start"]

    allow_parse.set()
    metrics = await asyncio.wait_for(pipeline, timeout=2)

    assert metrics["failed"] == 2
    assert started_download_urls == ["https://a/first.json", "https://b/second.json"]
    assert lease_events == ["start", "release", "start", "release"]


@pytest.mark.asyncio
async def test_source_pipeline_cancellation_releases_waiting_producer_lease(
    tmp_path, monkeypatch
):
    orchestrator = _orchestrator_module()
    parse_started = asyncio.Event()
    cleanup_started = asyncio.Event()
    allow_cleanup = asyncio.Event()
    lease_events: list[str] = []
    attempt = _Attempt("one", "a", "A", "https://a/source.json", 0)
    raw = SimpleNamespace(
        raw_sha256="a" * 64, raw_path=str(tmp_path / "source.json"), byte_count=2
    )

    @contextlib.contextmanager
    def lease_context(**_kwargs: Any):
        lease_events.append("start")
        try:
            yield object()
        finally:
            lease_events.append("release")

    async def download_source(
        item: Any, _store: Any, _max_bytes: int
    ) -> _DownloadedSource:
        return _DownloadedSource(item[0], raw, item[1])

    async def ensure_content(*_args: Any) -> str:
        parse_started.set()
        try:
            await asyncio.Future()
        finally:
            cleanup_started.set()
            await allow_cleanup.wait()

    monkeypatch.setattr(orchestrator, "artifact_lease_context", lease_context)
    monkeypatch.setattr(orchestrator, "download_source", download_source)
    monkeypatch.setattr(orchestrator, "_ensure_content", ensure_content)
    monkeypatch.setattr(orchestrator, "_require_disk_capacity", lambda *_args: None)
    monkeypatch.setattr(orchestrator, "_progress", lambda *_args: None)

    pipeline = asyncio.create_task(orchestrator._stream_sources(
        {}, {}, _ArtifactStore(tmp_path), {attempt.source_url: [attempt]},
        (1, 1), (None, 0, 1),
        ("hospital-prices:test", 1024, 4096, 2048, 1),
    ))
    await asyncio.wait_for(parse_started.wait(), timeout=1)
    pipeline.cancel()
    await asyncio.wait_for(cleanup_started.wait(), timeout=1)
    await asyncio.sleep(0)
    pipeline.cancel()
    await asyncio.sleep(0)
    assert lease_events == ["start"]
    allow_cleanup.set()
    with pytest.raises(asyncio.CancelledError):
        await pipeline

    assert lease_events == ["start", "release"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("resolved_failures", "pipeline_metrics", "expected_metrics"),
    (
        (1, {"processed": 1, "published": 1, "superseded": 0, "unchanged": 0,
             "failed": 0, "contents": 1}, {"published": 1, "failed": 1}),
        (1, {"processed": 1, "published": 0, "superseded": 0, "unchanged": 0,
             "failed": 1, "contents": 0}, {"published": 0, "failed": 2}),
        (0, {"processed": 2, "published": 1, "superseded": 1, "unchanged": 0,
             "failed": 0, "contents": 1}, {"published": 1, "superseded": 1}),
    ),
)
async def test_bulk_import_rejects_every_incomplete_selected_cohort(
    tmp_path, monkeypatch, resolved_failures, pipeline_metrics, expected_metrics
):
    orchestrator = _orchestrator_module()
    hospitals = (
        {"hospital_id": "a", "name": "A", "cms_hpt_url": "https://a/locator"},
        {"hospital_id": "b", "name": "B", "cms_hpt_url": "https://b/locator"},
    )

    configure_incomplete_import(
        orchestrator, monkeypatch, resolved_failures, pipeline_metrics
    )
    context_by_field: dict[str, Any] = {}
    with pytest.raises(RuntimeError, match="did not complete every selected hospital"):
        await orchestrator._run_import(
            context_by_field, {}, hospitals, _ArtifactStore(tmp_path), [],
            "hospital-prices:test", 300,
        )
    actual_metrics = context_by_field["context"]["hospital_price_metrics"]
    assert {name: actual_metrics[name] for name in expected_metrics} == expected_metrics
