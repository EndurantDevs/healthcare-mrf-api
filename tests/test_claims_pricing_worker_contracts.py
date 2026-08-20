# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import importlib
from unittest.mock import AsyncMock, Mock

import pytest

from tests.claims_pricing_contract_fakes import RecordingRedis


claims_pricing = importlib.import_module("process.claims_pricing")


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
async def test_chunk_dispatch_supports_registry_aliases(
    monkeypatch,
    dataset_key,
    loader_name,
):
    class_by_name = {
        "PricingProvider": object(),
        "PricingProviderProcedure": object(),
        "PricingProviderProcedureLocation": object(),
        "PricingProcedure": object(),
        "PricingProcedureGeoBenchmark": object(),
    }
    loader = AsyncMock()
    monkeypatch.setattr(claims_pricing, "ensure_database", AsyncMock())
    monkeypatch.setattr(
        claims_pricing,
        "_staging_classes",
        lambda *_args: class_by_name,
    )
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
    chunk_spec = claims_pricing._ClaimsChunkSpec(
        "unknown",
        "c",
        "/tmp/c",
        "",
        "s",
        "mrf",
        2023,
        False,
    )
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
    monkeypatch.setattr(
        claims_pricing,
        "_record_claims_chunk_complete",
        record_complete,
    )
    response_by_field = await claims_pricing.claims_pricing_process_chunk(
        {"redis": redis},
        {
            "dataset_key": "provider",
            "chunk_id": "chunk-a",
            "chunk_path": str(chunk_path),
            "run_id": "run-a",
        },
    )
    assert response_by_field == {
        "ok": True,
        "chunk_id": "chunk-a",
        "dataset_key": "provider",
    }
    record_complete.assert_awaited_once()


@pytest.mark.asyncio
async def test_process_chunk_skips_progress_without_run(monkeypatch, tmp_path):
    chunk_path = tmp_path / "chunk.csv"
    chunk_path.write_text("header\n")
    monkeypatch.setattr(claims_pricing, "_load_claims_chunk", AsyncMock())
    record_complete = AsyncMock()
    monkeypatch.setattr(
        claims_pricing,
        "_record_claims_chunk_complete",
        record_complete,
    )
    await claims_pricing.claims_pricing_process_chunk(
        {"redis": RecordingRedis()},
        {
            "dataset_key": "provider",
            "chunk_id": "chunk-a",
            "chunk_path": str(chunk_path),
        },
    )
    record_complete.assert_not_awaited()


@pytest.mark.asyncio
async def test_record_chunk_complete_emits_exact_progress(monkeypatch):
    redis = object()
    live_progress = Mock()
    monkeypatch.setattr(
        claims_pricing,
        "_mark_chunk_done_with_retry",
        AsyncMock(),
    )
    monkeypatch.setattr(
        claims_pricing,
        "_get_run_progress",
        AsyncMock(return_value=(5, 3)),
    )
    monkeypatch.setattr(claims_pricing, "enqueue_live_progress", live_progress)
    chunk_spec = claims_pricing._ClaimsChunkSpec(
        "provider",
        "c",
        "/tmp/c",
        "r",
        "s",
        "mrf",
        2023,
        False,
    )
    await claims_pricing._record_claims_chunk_complete(redis, chunk_spec)
    assert live_progress.call_args.kwargs["message"] == "processed 3/5 chunks"


def test_finalize_spec_uses_manifest_fallback(monkeypatch):
    monkeypatch.setattr(claims_pricing, "get_import_schema", lambda *_args: "mrf")
    finalize_spec = claims_pricing._claims_finalize_spec(
        {"import_id": "import-a", "total_chunks": 2},
        {"run_id": "run-a", "stage_suffix": "stage-a", "total_chunks": "4"},
    )
    assert finalize_spec == claims_pricing._ClaimsFinalizeSpec(
        "import_a",
        "run-a",
        False,
        "mrf",
        "stage-a",
        4,
    )


@pytest.mark.asyncio
async def test_finalize_wait_requires_redis_and_run_identity():
    finalize_spec = claims_pricing._ClaimsFinalizeSpec(
        "i",
        "",
        False,
        "mrf",
        "s",
        0,
    )
    with pytest.raises(RuntimeError, match="redis context"):
        await claims_pricing._wait_for_claims_finalize_turn(
            None,
            finalize_spec,
        )
    with pytest.raises(RuntimeError, match="run_id"):
        await claims_pricing._wait_for_claims_finalize_turn(
            RecordingRedis(),
            finalize_spec,
        )


@pytest.mark.asyncio
async def test_finalize_wait_returns_idempotent_manifest():
    redis = RecordingRedis()
    redis.values_by_key["claims_pricing:run-a:finalized"] = "1"
    finalize_spec = claims_pricing._ClaimsFinalizeSpec(
        "import-a",
        "run-a",
        False,
        "mrf",
        "s",
        1,
    )
    assert await claims_pricing._wait_for_claims_finalize_turn(
        redis,
        finalize_spec,
    ) == {
        "ok": True,
        "already_finalized": True,
        "run_id": "run-a",
        "import_id": "import-a",
    }


@pytest.mark.asyncio
async def test_finalize_wait_retries_incomplete_chunks(monkeypatch):
    finalize_spec = claims_pricing._ClaimsFinalizeSpec(
        "i",
        "r",
        False,
        "mrf",
        "s",
        2,
    )
    monkeypatch.setattr(
        claims_pricing,
        "_get_run_progress",
        AsyncMock(return_value=(2, 1)),
    )
    monkeypatch.setattr(claims_pricing, "enqueue_live_progress", Mock())
    with pytest.raises(claims_pricing.Retry):
        await claims_pricing._wait_for_claims_finalize_turn(
            RecordingRedis(),
            finalize_spec,
        )


@pytest.mark.asyncio
async def test_finalize_wait_retries_when_lock_is_owned(monkeypatch):
    finalize_spec = claims_pricing._ClaimsFinalizeSpec(
        "i",
        "r",
        False,
        "mrf",
        "s",
        1,
    )
    monkeypatch.setattr(
        claims_pricing,
        "_get_run_progress",
        AsyncMock(return_value=(1, 1)),
    )
    monkeypatch.setattr(
        claims_pricing,
        "_claim_finalize_lock",
        AsyncMock(return_value=False),
    )
    with pytest.raises(claims_pricing.Retry):
        await claims_pricing._wait_for_claims_finalize_turn(
            RecordingRedis(),
            finalize_spec,
        )


@pytest.mark.asyncio
async def test_finalize_wait_claims_lock_and_marks_phase(monkeypatch):
    finalize_spec = claims_pricing._ClaimsFinalizeSpec(
        "i",
        "r",
        False,
        "mrf",
        "s",
        1,
    )
    mark_control = AsyncMock()
    monkeypatch.setattr(
        claims_pricing,
        "_get_run_progress",
        AsyncMock(return_value=(1, 1)),
    )
    monkeypatch.setattr(
        claims_pricing,
        "_claim_finalize_lock",
        AsyncMock(return_value=True),
    )
    monkeypatch.setattr(claims_pricing, "mark_control_run", mark_control)
    assert await claims_pricing._wait_for_claims_finalize_turn(
        RecordingRedis(),
        finalize_spec,
    ) is None
    assert mark_control.await_args.kwargs["status"] == "finalizing"
    assert mark_control.await_args.kwargs["progress"]["pct"] == 99


@pytest.mark.asyncio
async def test_finalize_wait_releases_lock_when_status_update_fails(
    monkeypatch,
):
    redis = RecordingRedis()
    finalize_spec = claims_pricing._ClaimsFinalizeSpec(
        "i",
        "r",
        False,
        "mrf",
        "s",
        1,
    )
    release_lock = AsyncMock()
    monkeypatch.setattr(
        claims_pricing,
        "_get_run_progress",
        AsyncMock(return_value=(1, 1)),
    )
    monkeypatch.setattr(
        claims_pricing,
        "_claim_finalize_lock",
        AsyncMock(return_value=True),
    )
    monkeypatch.setattr(
        claims_pricing,
        "mark_control_run",
        AsyncMock(side_effect=RuntimeError("status unavailable")),
    )
    monkeypatch.setattr(
        claims_pricing,
        "_release_claims_finalize_lock_safely",
        release_lock,
    )

    with pytest.raises(RuntimeError, match="status unavailable"):
        await claims_pricing._wait_for_claims_finalize_turn(
            redis,
            finalize_spec,
        )

    release_lock.assert_awaited_once_with(redis, finalize_spec)


@pytest.mark.asyncio
@pytest.mark.parametrize("defer_indexes", [False, True])
async def test_materialize_publish_orders_staged_contract(
    monkeypatch,
    defer_indexes,
):
    """Preserve every materialization step and its publication order."""

    calls = []

    async def record_call(name, return_value=None):
        calls.append(name)
        return return_value

    monkeypatch.setattr(
        claims_pricing,
        "CLAIMS_DEFER_STAGE_INDEXES",
        defer_indexes,
    )
    for function_name, call_name in (
        ("_ensure_live_code_tables", "codes"),
        ("_materialize_code_and_crosswalk_rows", "crosswalk"),
        ("_materialize_procedure_provider_counts", "provider_counts"),
        ("_materialize_procedure_taxonomy_signals", "taxonomy_signals"),
        ("_materialize_cost_level_rows", "cost"),
        ("_build_staging_indexes", "indexes"),
        ("_publish_by_table_rename", "publish"),
    ):
        monkeypatch.setattr(
            claims_pricing,
            function_name,
            lambda *_args, name=call_name: record_call(name),
        )
    monkeypatch.setattr(
        claims_pricing,
        "_collect_cost_level_diagnostics",
        lambda *_args: record_call("diagnostics", {"key_coverage": []}),
    )
    diagnostics = await claims_pricing._materialize_and_publish_claims({}, "mrf")
    expected_calls = [
        "codes",
        "crosswalk",
        "provider_counts",
        "taxonomy_signals",
        "cost",
        "diagnostics",
    ]
    if defer_indexes:
        expected_calls.append("indexes")
    assert calls == [*expected_calls, "publish"]
    assert diagnostics == {"key_coverage": []}
