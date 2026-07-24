# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import importlib
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from tests.claims_pricing_contract_fakes import RecordingRedis


claims_pricing = importlib.import_module("process.claims_pricing")


@pytest.mark.asyncio
async def test_run_state_tracks_totals_members_and_lock():
    redis = RecordingRedis()
    await claims_pricing._init_run_state(redis, "run-a", 2)
    await claims_pricing._increment_total_chunks(redis, "run-a", 0)
    await claims_pricing._increment_total_chunks(redis, "run-a", 3)
    await claims_pricing._mark_chunk_done(redis, "run-a", "provider:0")
    total_chunks, done_chunks = await claims_pricing._get_run_progress(redis, "run-a", 99)
    assert (total_chunks, done_chunks) == (5, 1)
    assert await claims_pricing._is_finalize_lock_claimed(
        redis,
        "run-a",
        "owner-one",
    ) is True
    assert await claims_pricing._is_finalize_lock_claimed(
        redis,
        "run-a",
        "owner-two",
    ) is False
    assert not await claims_pricing._is_claims_finalize_lock_released(
        redis,
        "run-a",
        "owner-two",
    )
    assert await claims_pricing._is_claims_finalize_lock_released(
        redis,
        "run-a",
        "owner-one",
    )
    assert set(redis.deleted_keys) == {
        "claims_pricing:run-a:total_chunks",
        "claims_pricing:run-a:done_chunks",
        "claims_pricing:run-a:finalize_lock",
        "claims_pricing:run-a:finalized",
    }


@pytest.mark.asyncio
async def test_progress_falls_back_for_invalid_redis_values():
    redis = RecordingRedis()
    redis.values_by_key["claims_pricing:run-a:total_chunks"] = b"bad"
    redis.members_by_key["claims_pricing:run-a:done_chunks"] = {"a", "b"}
    assert await claims_pricing._get_run_progress(redis, "run-a", 4) == (4, 2)


@pytest.mark.asyncio
async def test_mark_done_retries_with_bounded_backoff(monkeypatch):
    mark_done = AsyncMock(side_effect=[OSError("one"), OSError("two"), None])
    sleep = AsyncMock()
    monkeypatch.setattr(claims_pricing, "_mark_chunk_done", mark_done)
    monkeypatch.setattr(claims_pricing.asyncio, "sleep", sleep)
    monkeypatch.setattr(claims_pricing, "CLAIMS_MARK_DONE_RETRIES", 3)
    monkeypatch.setattr(claims_pricing, "CLAIMS_MARK_DONE_RETRY_BASE_SECONDS", 2)
    monkeypatch.setattr(claims_pricing, "CLAIMS_MARK_DONE_RETRY_MAX_SECONDS", 3)
    await claims_pricing._mark_chunk_done_with_retry(object(), "run-a", "chunk-a")
    assert [sleep_call.args[0] for sleep_call in sleep.await_args_list] == [2, 3]


@pytest.mark.asyncio
async def test_mark_done_propagates_terminal_failure(monkeypatch):
    terminal_error = OSError("redis unavailable")
    monkeypatch.setattr(claims_pricing, "_mark_chunk_done", AsyncMock(side_effect=terminal_error))
    monkeypatch.setattr(claims_pricing.asyncio, "sleep", AsyncMock())
    monkeypatch.setattr(claims_pricing, "CLAIMS_MARK_DONE_RETRIES", 1)
    with pytest.raises(OSError, match="redis unavailable"):
        await claims_pricing._mark_chunk_done_with_retry(object(), "run-a", "chunk-a")


@pytest.mark.asyncio
async def test_timed_value_reports_failure_without_logging_completion(
    monkeypatch,
):
    step_end = Mock()
    step_failed = Mock()
    monkeypatch.setattr(claims_pricing, "_step_start", lambda _label: 10.0)
    monkeypatch.setattr(claims_pricing, "_step_end", step_end)
    monkeypatch.setattr(claims_pricing, "_step_failed", step_failed)

    async def fail_step():
        raise RuntimeError("failed step")

    with pytest.raises(RuntimeError, match="failed step") as raised:
        await claims_pricing._timed_value("synthetic", fail_step())
    step_end.assert_not_called()
    step_failed.assert_called_once_with(
        "synthetic",
        10.0,
        raised.value,
    )


@pytest.mark.asyncio
async def test_download_split_source_annotates_chunk_contract(monkeypatch, tmp_path):
    run_identity = claims_pricing._ClaimsRunIdentity(
        True,
        "import-a",
        "run-a",
        "stage-a",
        tmp_path,
    )
    source_path = tmp_path / "provider.csv"
    download_source = AsyncMock(return_value=str(source_path))
    split_source = AsyncMock(
        return_value=[
            {
                "dataset_key": "provider",
                "chunk_index": 0,
                "chunk_path": str(tmp_path / "chunk.csv"),
            }
        ]
    )
    step_end = Mock()
    monkeypatch.setattr(claims_pricing, "_step_start", lambda _label: 2.0)
    monkeypatch.setattr(claims_pricing, "_step_end", step_end)
    monkeypatch.setattr(claims_pricing, "_download_source_file", download_source)
    monkeypatch.setattr(claims_pricing, "_split_source_into_chunks", split_source)
    downloaded = await claims_pricing._download_split_claims_source(
        run_identity,
        "provider",
        {"url": "https://example.test/provider.csv", "reporting_year": "2023"},
        2,
        asyncio.Semaphore(1),
    )
    assert downloaded.chunk_entries[0]["reporting_year"] == 2023
    assert downloaded.chunk_entries[0]["source_index"] == 2
    assert download_source.await_args.kwargs["reporting_year"] == 2023
    assert split_source.await_args.args[2] == (
        tmp_path / "chunks" / "provider" / "2023_0002"
    )
    step_end.assert_called_once_with(
        "download+split provider year=2023",
        2.0,
    )


@pytest.mark.asyncio
async def test_download_split_source_closes_timing_on_failure(monkeypatch, tmp_path):
    run_identity = claims_pricing._ClaimsRunIdentity(False, "i", "r", "s", tmp_path)
    step_end = Mock()
    step_failed = Mock()
    monkeypatch.setattr(claims_pricing, "_step_start", lambda _label: 3.0)
    monkeypatch.setattr(claims_pricing, "_step_end", step_end)
    monkeypatch.setattr(claims_pricing, "_step_failed", step_failed)
    monkeypatch.setattr(claims_pricing, "_download_source_file", AsyncMock(side_effect=OSError("offline")))
    with pytest.raises(OSError, match="offline") as raised:
        await claims_pricing._download_split_claims_source(
            run_identity,
            "provider",
            {"url": "https://example.test/provider.csv"},
            0,
            asyncio.Semaphore(1),
        )
    step_end.assert_not_called()
    step_failed.assert_called_once_with(
        "download+split provider year=2013",
        3.0,
        raised.value,
    )


@pytest.mark.asyncio
async def test_enqueue_chunk_uses_stable_identity(tmp_path):
    redis = RecordingRedis()
    run_identity = claims_pricing._ClaimsRunIdentity(False, "import-a", "run-a", "stage-a", tmp_path)
    chunk_by_field = {
        "dataset_key": "provider",
        "chunk_index": "2",
        "source_index": "1",
        "reporting_year": "2023",
        "chunk_path": "/tmp/chunk.csv",
    }
    await claims_pricing._enqueue_claim_chunk(redis, run_identity, "mrf", chunk_by_field)
    queued_job = redis.jobs[0]
    assert queued_job["task"]["chunk_id"] == "provider:2023:1:2"
    assert queued_job["options"]["_job_id"] == "claims_chunk_run-a_provider_2023_1_2"
    assert queued_job["options"]["_queue_name"] == claims_pricing.CLAIMS_QUEUE_NAME


@pytest.mark.asyncio
async def test_stream_chunks_updates_total_after_enqueue(monkeypatch, tmp_path):
    redis = RecordingRedis()
    run_identity = claims_pricing._ClaimsRunIdentity(False, "i", "r", "s", tmp_path)
    monkeypatch.setattr(
        claims_pricing,
        "DATASETS",
        (claims_pricing.DatasetConfig("provider", "landing", 10),),
    )
    downloaded = claims_pricing._DownloadedClaimsSource(
        [{"dataset_key": "provider", "chunk_index": 0, "chunk_path": "/tmp/chunk.csv"}]
    )
    monkeypatch.setattr(claims_pricing, "_download_split_claims_source", AsyncMock(return_value=downloaded))
    chunk_entries = await claims_pricing._stream_claim_chunks(
        redis,
        run_identity,
        "mrf",
        {"provider": [{"url": "https://example.test/provider.csv"}]},
    )
    assert chunk_entries == downloaded.chunk_entries
    assert redis.values_by_key["claims_pricing:r:total_chunks"] == "1"
    assert redis.jobs[0]["function"] == "claims_pricing_process_chunk"


@pytest.mark.asyncio
async def test_stream_chunks_accepts_empty_source_result(monkeypatch, tmp_path):
    redis = RecordingRedis()
    run_identity = claims_pricing._ClaimsRunIdentity(False, "i", "r", "s", tmp_path)
    monkeypatch.setattr(
        claims_pricing,
        "DATASETS",
        (claims_pricing.DatasetConfig("provider", "landing", 10),),
    )
    monkeypatch.setattr(
        claims_pricing,
        "_download_split_claims_source",
        AsyncMock(return_value=claims_pricing._DownloadedClaimsSource([])),
    )
    assert await claims_pricing._stream_claim_chunks(
        redis,
        run_identity,
        "mrf",
        {"provider": [{"url": "https://example.test/provider.csv"}]},
    ) == []
    assert redis.jobs == []


@pytest.mark.asyncio
async def test_stream_chunks_cancels_sibling_on_failure(monkeypatch, tmp_path):
    run_identity = claims_pricing._ClaimsRunIdentity(False, "i", "r", "s", tmp_path)
    monkeypatch.setattr(
        claims_pricing,
        "DATASETS",
        (
            claims_pricing.DatasetConfig("bad", "bad", 1),
            claims_pricing.DatasetConfig("slow", "slow", 1),
        ),
    )
    slow_cancelled = asyncio.Event()

    async def download_source(_identity, dataset_key, *_args):
        if dataset_key == "bad":
            raise OSError("split failed")
        try:
            await asyncio.Event().wait()
        finally:
            slow_cancelled.set()

    monkeypatch.setattr(claims_pricing, "_download_split_claims_source", download_source)
    with pytest.raises(OSError, match="split failed"):
        await claims_pricing._stream_claim_chunks(
            RecordingRedis(),
            run_identity,
            "mrf",
            {"bad": [{}], "slow": [{}]},
        )
    await asyncio.sleep(0)
    assert slow_cancelled.is_set()


@pytest.mark.asyncio
async def test_start_persists_manifest_before_finalize(monkeypatch, tmp_path):
    redis = RecordingRedis()
    chunk_entries = [
        {
            "dataset_key": "provider",
            "chunk_index": 0,
            "chunk_path": str(tmp_path / "chunk.csv"),
            "source_index": 0,
        }
    ]
    monkeypatch.setattr(
        claims_pricing,
        "DATASETS",
        (claims_pricing.DatasetConfig("provider", "landing", 10),),
    )
    monkeypatch.setattr(claims_pricing, "CLAIMS_WORKDIR", str(tmp_path))
    monkeypatch.setattr(claims_pricing, "ensure_database", AsyncMock())
    monkeypatch.setattr(claims_pricing, "_prepare_tables", AsyncMock(return_value=({}, "mrf")))
    monkeypatch.setattr(claims_pricing, "_fetch_catalog", AsyncMock(return_value={"dataset": []}))
    monkeypatch.setattr(
        claims_pricing,
        "_resolve_sources_async",
        AsyncMock(
            return_value={
                "provider": [{"url": "https://example.test/provider.csv"}]
            }
        ),
    )
    monkeypatch.setattr(claims_pricing, "_stream_claim_chunks", AsyncMock(return_value=chunk_entries))
    monkeypatch.setattr(claims_pricing, "mark_control_run", AsyncMock())
    response_by_field = await claims_pricing.claims_pricing_start(
        {"redis": redis},
        {"test_mode": True, "import_id": "import-a", "run_id": "run-a"},
    )
    manifest_by_field = json.loads(Path(response_by_field["manifest_path"]).read_text())
    assert response_by_field["total_chunks"] == 1
    assert manifest_by_field["chunks"] == chunk_entries
    assert manifest_by_field["sources"] == {
        "provider": [{"url": "https://example.test/provider.csv"}]
    }
    assert manifest_by_field["work_dir"].endswith("import_a/run-a")
    assert redis.jobs[-1]["function"] == "claims_pricing_finalize"
    assert redis.jobs[-1]["task"]["manifest_path"] == response_by_field["manifest_path"]


def _claims_source_gap_contract(tmp_path):
    chunk_entries = [
        {
            "dataset_key": "provider",
            "chunk_index": 0,
            "chunk_path": str(tmp_path / "provider-chunk.csv"),
            "source_index": 0,
        }
    ]
    sources_by_dataset = {
        "provider": [{"url": "https://example.test/provider.csv"}],
        "geo_service": [{"url": "https://example.test/geo.csv"}],
    }
    dataset_configs = (
        claims_pricing.DatasetConfig("provider", "provider", 10),
        claims_pricing.DatasetConfig("geo_service", "geo", 10),
    )
    return dataset_configs, chunk_entries, sources_by_dataset


def _configure_claims_source_gap(
    monkeypatch,
    tmp_path,
    dataset_configs,
    chunk_entries,
    sources_by_dataset,
    mark_control,
):
    monkeypatch.setattr(
        claims_pricing,
        "DATASETS",
        dataset_configs,
    )
    monkeypatch.setattr(claims_pricing, "CLAIMS_WORKDIR", str(tmp_path))
    monkeypatch.setattr(claims_pricing, "ensure_database", AsyncMock())
    monkeypatch.setattr(
        claims_pricing,
        "_prepare_tables",
        AsyncMock(return_value=({}, "mrf")),
    )
    monkeypatch.setattr(
        claims_pricing,
        "_fetch_catalog",
        AsyncMock(return_value={"dataset": []}),
    )
    monkeypatch.setattr(
        claims_pricing,
        "_resolve_sources_async",
        AsyncMock(return_value=sources_by_dataset),
    )
    monkeypatch.setattr(
        claims_pricing,
        "_stream_claim_chunks",
        AsyncMock(return_value=chunk_entries),
    )
    monkeypatch.setattr(claims_pricing, "mark_control_run", mark_control)


@pytest.mark.asyncio
async def test_start_refuses_finalize_when_a_required_source_has_no_rows(
    monkeypatch,
    tmp_path,
):
    redis = RecordingRedis()
    mark_control = AsyncMock()
    contract_fields = _claims_source_gap_contract(tmp_path)
    _configure_claims_source_gap(
        monkeypatch,
        tmp_path,
        *contract_fields,
        mark_control,
    )

    with pytest.raises(RuntimeError, match="geo_service:0"):
        await claims_pricing.claims_pricing_start(
            {"redis": redis},
            {
                "test_mode": True,
                "import_id": "import-a",
                "run_id": "run-a",
            },
        )

    assert all(
        queued_job["function"] != "claims_pricing_finalize"
        for queued_job in redis.jobs
    )
    assert mark_control.await_args.kwargs["status"] == "failed"
    assert mark_control.await_args.kwargs["metrics"] == {
        "missing_sources": ["geo_service:0"]
    }
