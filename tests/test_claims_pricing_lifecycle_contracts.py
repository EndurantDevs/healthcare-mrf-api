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
    assert await claims_pricing._is_finalize_lock_claimed(redis, "run-a") is True
    assert await claims_pricing._is_finalize_lock_claimed(redis, "run-a") is False
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
async def test_timed_value_closes_step_on_failure(monkeypatch):
    step_end = Mock()
    monkeypatch.setattr(claims_pricing, "_step_start", lambda _label: 10.0)
    monkeypatch.setattr(claims_pricing, "_step_end", step_end)

    async def fail_step():
        raise RuntimeError("failed step")

    with pytest.raises(RuntimeError, match="failed step"):
        await claims_pricing._timed_value("synthetic", fail_step())
    step_end.assert_called_once_with("synthetic", 10.0)


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


@pytest.mark.asyncio
async def test_download_split_source_closes_timing_on_failure(monkeypatch, tmp_path):
    run_identity = claims_pricing._ClaimsRunIdentity(False, "i", "r", "s", tmp_path)
    step_end = Mock()
    monkeypatch.setattr(claims_pricing, "_step_start", lambda _label: 3.0)
    monkeypatch.setattr(claims_pricing, "_step_end", step_end)
    monkeypatch.setattr(claims_pricing, "_download_source_file", AsyncMock(side_effect=OSError("offline")))
    with pytest.raises(OSError, match="offline"):
        await claims_pricing._download_split_claims_source(
            run_identity,
            "provider",
            {"url": "https://example.test/provider.csv"},
            0,
            asyncio.Semaphore(1),
        )
    step_end.assert_called_once()


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
        }
    ]
    monkeypatch.setattr(claims_pricing, "CLAIMS_WORKDIR", str(tmp_path))
    monkeypatch.setattr(claims_pricing, "ensure_database", AsyncMock())
    monkeypatch.setattr(claims_pricing, "_prepare_tables", AsyncMock(return_value=({}, "mrf")))
    monkeypatch.setattr(claims_pricing, "_fetch_catalog", AsyncMock(return_value={"dataset": []}))
    monkeypatch.setattr(claims_pricing, "_resolve_sources_async", AsyncMock(return_value={"provider": []}))
    monkeypatch.setattr(claims_pricing, "_stream_claim_chunks", AsyncMock(return_value=chunk_entries))
    monkeypatch.setattr(claims_pricing, "mark_control_run", AsyncMock())
    response_by_field = await claims_pricing.claims_pricing_start(
        {"redis": redis},
        {"test_mode": True, "import_id": "import-a", "run_id": "run-a"},
    )
    manifest_by_field = json.loads(Path(response_by_field["manifest_path"]).read_text())
    assert response_by_field["total_chunks"] == 1
    assert manifest_by_field["chunks"] == chunk_entries
    assert manifest_by_field["sources"] == {"provider": []}
    assert manifest_by_field["work_dir"].endswith("import_a/run-a")
    assert redis.jobs[-1]["function"] == "claims_pricing_finalize"
    assert redis.jobs[-1]["task"]["manifest_path"] == response_by_field["manifest_path"]


@pytest.mark.asyncio
async def test_start_requires_arq_redis_context():
    with pytest.raises(RuntimeError, match="redis context"):
        await claims_pricing.claims_pricing_start({}, {"run_id": "run-a"})


def test_chunk_spec_applies_defaults(monkeypatch):
    monkeypatch.setattr(claims_pricing, "get_import_schema", lambda *_args: "mrf_test")
    chunk_spec = claims_pricing._claims_chunk_spec(
        {
            "dataset_key": "provider",
            "chunk_id": "chunk-a",
            "chunk_path": "/tmp/chunk.csv",
            "run_id": "run-a",
            "reporting_year": "bad",
            "test_mode": True,
        }
    )
    assert chunk_spec.schema == "mrf_test"
    assert chunk_spec.reporting_year == 2013
    assert chunk_spec.stage_suffix


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("dataset_key", "loader_name"),
    [
        ("provider", "_load_provider_rows"),
        ("provider_service", "_load_provider_service_rows"),
        ("provider_drug", "_load_provider_service_rows"),
        ("geo_service", "_load_geo_service_rows"),
        ("drug_spending", "_load_geo_service_rows"),
    ],
)
async def test_chunk_dispatch_supports_registry_aliases(monkeypatch, dataset_key, loader_name):
    class_by_name = {
        "PricingProvider": object(),
        "PricingProviderProcedure": object(),
        "PricingProviderProcedureLocation": object(),
        "PricingProcedure": object(),
        "PricingProcedureGeoBenchmark": object(),
    }
    loader = AsyncMock()
    monkeypatch.setattr(claims_pricing, "ensure_database", AsyncMock())
    monkeypatch.setattr(claims_pricing, "_staging_classes", lambda *_args: class_by_name)
    monkeypatch.setattr(claims_pricing, loader_name, loader)
    chunk_spec = claims_pricing._ClaimsChunkSpec(
        dataset_key,
        "chunk-a",
        "/tmp/chunk.csv",
        "run-a",
        "stage-a",
        "mrf",
        2023,
        True,
    )
    await claims_pricing._load_claims_chunk(chunk_spec)
    assert loader.await_args.kwargs["test_mode"] is False


@pytest.mark.asyncio
async def test_chunk_dispatch_rejects_unknown_dataset(monkeypatch):
    monkeypatch.setattr(claims_pricing, "ensure_database", AsyncMock())
    monkeypatch.setattr(claims_pricing, "_staging_classes", lambda *_args: {})
    chunk_spec = claims_pricing._ClaimsChunkSpec("unknown", "c", "/tmp/c", "", "s", "mrf", 2023, False)
    with pytest.raises(RuntimeError, match="Unsupported dataset_key"):
        await claims_pricing._load_claims_chunk(chunk_spec)


@pytest.mark.asyncio
async def test_process_chunk_validates_payload_and_file(tmp_path):
    with pytest.raises(RuntimeError, match="missing required fields"):
        await claims_pricing.claims_pricing_process_chunk({}, {})
    missing_path = tmp_path / "missing.csv"
    with pytest.raises(RuntimeError, match="does not exist"):
        await claims_pricing.claims_pricing_process_chunk(
            {},
            {
                "dataset_key": "provider",
                "chunk_id": "chunk-a",
                "chunk_path": str(missing_path),
            },
        )


@pytest.mark.asyncio
async def test_process_chunk_returns_manifest_and_progress(monkeypatch, tmp_path):
    chunk_path = tmp_path / "chunk.csv"
    chunk_path.write_text("header\n")
    redis = RecordingRedis()
    monkeypatch.setattr(claims_pricing, "_load_claims_chunk", AsyncMock())
    record_complete = AsyncMock()
    monkeypatch.setattr(claims_pricing, "_record_claims_chunk_complete", record_complete)
    response_by_field = await claims_pricing.claims_pricing_process_chunk(
        {"redis": redis},
        {
            "dataset_key": "provider",
            "chunk_id": "chunk-a",
            "chunk_path": str(chunk_path),
            "run_id": "run-a",
        },
    )
    assert response_by_field == {"ok": True, "chunk_id": "chunk-a", "dataset_key": "provider"}
    record_complete.assert_awaited_once()


@pytest.mark.asyncio
async def test_process_chunk_skips_progress_without_run(monkeypatch, tmp_path):
    chunk_path = tmp_path / "chunk.csv"
    chunk_path.write_text("header\n")
    monkeypatch.setattr(claims_pricing, "_load_claims_chunk", AsyncMock())
    record_complete = AsyncMock()
    monkeypatch.setattr(claims_pricing, "_record_claims_chunk_complete", record_complete)
    await claims_pricing.claims_pricing_process_chunk(
        {"redis": RecordingRedis()},
        {"dataset_key": "provider", "chunk_id": "chunk-a", "chunk_path": str(chunk_path)},
    )
    record_complete.assert_not_awaited()


@pytest.mark.asyncio
async def test_record_chunk_complete_emits_exact_progress(monkeypatch):
    redis = object()
    live_progress = Mock()
    monkeypatch.setattr(claims_pricing, "_mark_chunk_done_with_retry", AsyncMock())
    monkeypatch.setattr(claims_pricing, "_get_run_progress", AsyncMock(return_value=(5, 3)))
    monkeypatch.setattr(claims_pricing, "enqueue_live_progress", live_progress)
    chunk_spec = claims_pricing._ClaimsChunkSpec("provider", "c", "/tmp/c", "r", "s", "mrf", 2023, False)
    await claims_pricing._record_claims_chunk_complete(redis, chunk_spec)
    assert live_progress.call_args.kwargs["message"] == "processed 3/5 chunks"


def test_finalize_spec_uses_manifest_fallback(monkeypatch):
    monkeypatch.setattr(claims_pricing, "get_import_schema", lambda *_args: "mrf")
    finalize_spec = claims_pricing._claims_finalize_spec(
        {"import_id": "import-a", "total_chunks": 2},
        {"run_id": "run-a", "stage_suffix": "stage-a", "total_chunks": "4"},
    )
    assert finalize_spec == claims_pricing._ClaimsFinalizeSpec(
        "import_a", "run-a", False, "mrf", "stage-a", 4
    )


@pytest.mark.asyncio
async def test_finalize_wait_accepts_runs_without_redis():
    finalize_spec = claims_pricing._ClaimsFinalizeSpec("i", "", False, "mrf", "s", 0)
    assert await claims_pricing._wait_for_claims_finalize_turn(None, finalize_spec) is None
    assert await claims_pricing._wait_for_claims_finalize_turn(RecordingRedis(), finalize_spec) is None


@pytest.mark.asyncio
async def test_finalize_wait_returns_idempotent_manifest():
    redis = RecordingRedis()
    redis.values_by_key["claims_pricing:run-a:finalized"] = "1"
    finalize_spec = claims_pricing._ClaimsFinalizeSpec("import-a", "run-a", False, "mrf", "s", 1)
    assert await claims_pricing._wait_for_claims_finalize_turn(redis, finalize_spec) == {
        "ok": True,
        "already_finalized": True,
        "run_id": "run-a",
        "import_id": "import-a",
    }


@pytest.mark.asyncio
async def test_finalize_wait_retries_incomplete_chunks(monkeypatch):
    finalize_spec = claims_pricing._ClaimsFinalizeSpec("i", "r", False, "mrf", "s", 2)
    monkeypatch.setattr(claims_pricing, "_get_run_progress", AsyncMock(return_value=(2, 1)))
    monkeypatch.setattr(claims_pricing, "enqueue_live_progress", Mock())
    with pytest.raises(claims_pricing.Retry):
        await claims_pricing._wait_for_claims_finalize_turn(RecordingRedis(), finalize_spec)


@pytest.mark.asyncio
async def test_finalize_wait_retries_when_lock_is_owned(monkeypatch):
    finalize_spec = claims_pricing._ClaimsFinalizeSpec("i", "r", False, "mrf", "s", 1)
    monkeypatch.setattr(claims_pricing, "_get_run_progress", AsyncMock(return_value=(1, 1)))
    monkeypatch.setattr(claims_pricing, "_claim_finalize_lock", AsyncMock(return_value=False))
    with pytest.raises(claims_pricing.Retry):
        await claims_pricing._wait_for_claims_finalize_turn(RecordingRedis(), finalize_spec)


@pytest.mark.asyncio
async def test_finalize_wait_claims_lock_and_marks_phase(monkeypatch):
    finalize_spec = claims_pricing._ClaimsFinalizeSpec("i", "r", False, "mrf", "s", 1)
    mark_control = AsyncMock()
    monkeypatch.setattr(claims_pricing, "_get_run_progress", AsyncMock(return_value=(1, 1)))
    monkeypatch.setattr(claims_pricing, "_claim_finalize_lock", AsyncMock(return_value=True))
    monkeypatch.setattr(claims_pricing, "mark_control_run", mark_control)
    assert await claims_pricing._wait_for_claims_finalize_turn(RecordingRedis(), finalize_spec) is None
    assert mark_control.await_args.kwargs["status"] == "finalizing"
    assert mark_control.await_args.kwargs["progress"]["pct"] == 99


@pytest.mark.asyncio
@pytest.mark.parametrize("defer_indexes", [False, True])
async def test_materialize_publish_orders_staged_contract(monkeypatch, defer_indexes):
    calls = []

    async def record_call(name, return_value=None):
        calls.append(name)
        return return_value

    monkeypatch.setattr(claims_pricing, "CLAIMS_DEFER_STAGE_INDEXES", defer_indexes)
    monkeypatch.setattr(claims_pricing, "_ensure_live_code_tables", lambda _schema: record_call("codes"))
    monkeypatch.setattr(
        claims_pricing,
        "_materialize_code_and_crosswalk_rows",
        lambda *_args: record_call("crosswalk"),
    )
    monkeypatch.setattr(claims_pricing, "_materialize_cost_level_rows", lambda *_args: record_call("cost"))
    monkeypatch.setattr(
        claims_pricing,
        "_collect_cost_level_diagnostics",
        lambda *_args: record_call("diagnostics", {"key_coverage": []}),
    )
    monkeypatch.setattr(claims_pricing, "_build_staging_indexes", lambda *_args: record_call("indexes"))
    monkeypatch.setattr(claims_pricing, "_publish_by_table_rename", lambda *_args: record_call("publish"))
    diagnostics = await claims_pricing._materialize_and_publish_claims({}, "mrf")
    expected_calls = ["codes", "crosswalk", "cost", "diagnostics"]
    if defer_indexes:
        expected_calls.append("indexes")
    assert calls == [*expected_calls, "publish"]
    assert diagnostics == {"key_coverage": []}
