# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import pytest

from tests.test_process_provider_quality_unit import (
    _FakeRedis,
    provider_quality,
)

@pytest.mark.asyncio
async def test_enqueue_materialize_phase_shards_sets_progress_and_jobs(monkeypatch):
    redis = _FakeRedis()
    monkeypatch.setattr(provider_quality, "PROVIDER_QUALITY_MATERIALIZE_SHARD_QUEUE_NAME", "arq:test_finish")
    queued = await provider_quality._enqueue_materialize_phase_shards(
        redis,
        run_id="run_1",
        phase=provider_quality.MAT_PHASE_2_BUILD_LSH_SHARDED,
        years=(2023, 2024),
        shard_count=2,
        stage_suffix="stage_1",
        schema="mrf",
        test_mode=True,
        job_name="provider_quality_materialize_lsh_shard",
    )

    assert queued == 4
    assert redis.store[provider_quality._mat_phase_key("run_1")] == provider_quality.MAT_PHASE_2_BUILD_LSH_SHARDED
    assert redis.store[provider_quality._mat_total_key("run_1")] == "4"
    assert redis.store[provider_quality._mat_done_key("run_1")] == "0"
    assert redis.store[provider_quality._mat_failed_key("run_1")] == "0"
    assert len(redis.jobs) == 4
    assert {job["payload"]["year"] for job in redis.jobs} == {2023, 2024}
    assert {job["payload"]["shard_id"] for job in redis.jobs} == {0, 1}
    assert {job["queue_name"] for job in redis.jobs} == {"arq:test_finish"}
    assert {job["max_tries"] for job in redis.jobs} == {None}


@pytest.mark.asyncio
async def test_wait_for_materialize_phase_completion_retry_and_failure():
    redis = _FakeRedis()
    await provider_quality._set_materialize_phase(
        redis,
        "run_wait",
        provider_quality.MAT_PHASE_5_BUILD_MEASURE_SHARDED,
        total=3,
    )

    redis.store[provider_quality._mat_done_key("run_wait")] = "1"
    with pytest.raises(provider_quality.Retry):
        await provider_quality._wait_for_materialize_phase_completion(
            redis,
            "run_wait",
            provider_quality.MAT_PHASE_5_BUILD_MEASURE_SHARDED,
        )

    redis.store[provider_quality._mat_done_key("run_wait")] = "3"
    redis.store[provider_quality._mat_failed_key("run_wait")] = "1"
    with pytest.raises(RuntimeError, match="failed_shards=1"):
        await provider_quality._wait_for_materialize_phase_completion(
            redis,
            "run_wait",
            provider_quality.MAT_PHASE_5_BUILD_MEASURE_SHARDED,
        )


@pytest.mark.asyncio
async def test_wait_for_materialize_phase_completion_logs_duration_summary(monkeypatch):
    redis = _FakeRedis()
    await provider_quality._set_materialize_phase(
        redis,
        "run_done",
        provider_quality.MAT_PHASE_7_BUILD_SCORE_SHARDED,
        total=2,
    )
    redis.store[provider_quality._mat_done_key("run_done")] = "2"
    redis.store[provider_quality._mat_failed_key("run_done")] = "0"
    await redis.rpush(
        provider_quality._mat_phase_duration_key("run_done", provider_quality.MAT_PHASE_7_BUILD_SCORE_SHARDED),
        "10.0",
    )
    await redis.rpush(
        provider_quality._mat_phase_duration_key("run_done", provider_quality.MAT_PHASE_7_BUILD_SCORE_SHARDED),
        "30.0",
    )

    observed_log_entries: list[tuple] = []

    def _capture_info(msg, *args, **kwargs):
        observed_log_entries.append((msg, args))

    monkeypatch.setattr(provider_quality.logger, "info", _capture_info)
    await provider_quality._wait_for_materialize_phase_completion(
        redis,
        "run_done",
        provider_quality.MAT_PHASE_7_BUILD_SCORE_SHARDED,
    )

    assert observed_log_entries
    logged = " ".join(str(part) for part in observed_log_entries[-1][1])
    assert "run_done" in logged
    assert provider_quality.MAT_PHASE_7_BUILD_SCORE_SHARDED in logged


@pytest.mark.asyncio
async def test_provider_quality_finalize_marks_control_run_failed_on_terminal_error(monkeypatch, tmp_path):
    redis = _FakeRedis()
    calls: list[dict[str, object]] = []

    async def _noop(*_args, **_kwargs):
        return None

    async def _raise_materialize(*_args, **_kwargs):
        raise RuntimeError("materialization failed")

    async def _capture_mark(run_id, **kwargs):
        calls.append({"run_id": run_id, **kwargs})

    monkeypatch.setattr(provider_quality, "ensure_database", _noop)
    monkeypatch.setattr(
        provider_quality,
        "_read_manifest",
        lambda _path: {
            "total_chunks": 0,
            "run_id": "run_failed",
            "stage_suffix": "stage_failed",
            "work_dir": str(tmp_path),
        },
    )
    monkeypatch.setattr(provider_quality, "_staging_classes", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(provider_quality, "_materialize_quality_rows", _raise_materialize)
    monkeypatch.setattr(provider_quality, "mark_control_run", _capture_mark)

    with pytest.raises(RuntimeError, match="materialization failed"):
        await provider_quality.provider_quality_finalize(
            {"redis": redis},
            {
                "run_id": "run_failed",
                "import_id": "20260609",
                "manifest_path": str(tmp_path / "manifest.json"),
                "stage_suffix": "stage_failed",
                "schema": "mrf",
            },
        )

    assert calls[-1]["run_id"] == "run_failed"
    assert calls[-1]["status"] == "failed"
    assert calls[-1]["error"] == {
        "code": "provider_quality_finalize_failed",
        "message": "materialization failed",
    }


@pytest.mark.asyncio
async def test_ensure_materialize_indexes_analyzes_available_models(monkeypatch):
    class _Model:
        __tablename__ = "stage_table"

    indexed_model_schema_pairs: list[tuple[type, str]] = []
    statements: list[str] = []

    async def _capture_indexes(model, schema):
        indexed_model_schema_pairs.append((model, schema))

    async def _capture_status(statement, **_kwargs):
        statements.append(statement)

    monkeypatch.setattr(provider_quality, "_ensure_indexes", _capture_indexes)
    monkeypatch.setattr(provider_quality.db, "status", _capture_status)

    await provider_quality._ensure_materialize_indexes(
        {"PricingProviderQualityFeature": _Model},
        "mrf",
        "PricingProviderQualityFeature",
        "MissingModel",
    )

    assert indexed_model_schema_pairs == [(_Model, "mrf")]
    assert statements == ["ANALYZE mrf.stage_table;"]


@pytest.mark.asyncio
async def test_ensure_provider_quality_rx_agg_table_builds_once(monkeypatch):
    class _Qpp:
        __tablename__ = "pricing_qpp_provider_stage_test"

    statements: list[str] = []
    scalar_calls_by_metric = {"total": 0}

    async def is_table_existing(_schema: str, table: str) -> bool:
        return table == "pricing_provider_prescription"

    async def _capture_status(statement, **_kwargs):
        statements.append(statement)

    async def _capture_scalar(_statement, **_kwargs):
        scalar_calls_by_metric["total"] += 1
        return 0

    monkeypatch.setattr(provider_quality, "_is_table_available", is_table_existing)
    monkeypatch.setattr(provider_quality.db, "status", _capture_status)
    monkeypatch.setattr(provider_quality.db, "scalar", _capture_scalar)

    await provider_quality._ensure_provider_quality_rx_agg_table(
        {"PricingQppProvider": _Qpp},
        "mrf",
        (2023,),
    )

    assert scalar_calls_by_metric["total"] == 1
    assert any("CREATE TABLE IF NOT EXISTS mrf.pricing_provider_quality_rx_agg_stage_test" in statement_sql for statement_sql in statements)
    assert any("FROM mrf.pricing_provider_prescription r" in statement_sql for statement_sql in statements)
    assert any("WHERE r.year IN (2023)" in statement_sql for statement_sql in statements)
    assert any("CREATE UNIQUE INDEX IF NOT EXISTS" in statement_sql for statement_sql in statements)
    assert statements[-1] == "ANALYZE mrf.pricing_provider_quality_rx_agg_stage_test;"
