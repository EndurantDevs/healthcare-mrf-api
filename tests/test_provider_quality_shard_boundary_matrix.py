# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Behavior boundaries for resumable provider-quality shard execution."""

from __future__ import annotations

import importlib
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import ANY, AsyncMock

import pytest

from process.provider_quality_parts import execution_helpers, materialize_shards

provider_quality = importlib.import_module("process.provider_quality")


class _RedisMarker:
    pass


def _shard_task(**replacements: object) -> dict[str, object]:
    task_by_field: dict[str, object] = {
        "run_id": "run-a",
        "stage_suffix": "stage-a",
        "schema": "mrf",
        "test_mode": True,
        "year": 2024,
        "shard_id": 1,
        "shard_count": 3,
    }
    task_by_field.update(replacements)
    return task_by_field


@pytest.mark.parametrize(
    "task",
    (
        _shard_task(shard_id=-1),
        _shard_task(shard_id=3),
    ),
)
def test_shard_payload_rejects_out_of_range_partition(task) -> None:
    with pytest.raises(RuntimeError, match="Invalid shard payload"):
        materialize_shards._materialize_shard_task_values(task)


@pytest.mark.parametrize(
    "task",
    (
        _shard_task(run_id=""),
        _shard_task(stage_suffix=""),
    ),
)
def test_shard_payload_requires_run_and_stage_identity(task) -> None:
    with pytest.raises(RuntimeError, match="missing required fields"):
        materialize_shards._materialize_shard_task_values(task)


def test_shard_payload_applies_safe_numeric_defaults(monkeypatch) -> None:
    monkeypatch.setattr(materialize_shards, "get_import_schema", lambda *_args: "safe")
    assert materialize_shards._materialize_shard_task_values(
        {
            "run_id": "run",
            "stage_suffix": "stage",
            "year": "bad",
            "shard_count": 0,
            "shard_id": "bad",
        }
    ) == (
        "run",
        "stage",
        "safe",
        False,
        materialize_shards.PROVIDER_QUALITY_MIN_YEAR,
        0,
        1,
    )


def _patch_shard_job_dependencies(
    monkeypatch,
    *,
    count_results: tuple[object, object] = (3, 4),
    execution_error: Exception | None = None,
) -> dict[str, AsyncMock]:
    calls_by_name = {
        "ensure": AsyncMock(),
        "done": AsyncMock(),
        "failed": AsyncMock(),
        "duration": AsyncMock(),
        "execute": AsyncMock(side_effect=execution_error),
    }
    count_results_list = list(count_results)

    async def count_rows(*_args, **_kwargs):
        result = count_results_list.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    monkeypatch.setattr(materialize_shards, "ensure_database", calls_by_name["ensure"])
    monkeypatch.setattr(
        materialize_shards,
        "_staging_classes",
        lambda *_args: {"Stage": object},
    )
    monkeypatch.setattr(
        materialize_shards,
        "_build_cohort_materialization_context",
        AsyncMock(return_value={"score_table": "score_stage"}),
    )
    monkeypatch.setattr(materialize_shards, "_count_shard_rows", count_rows)
    monkeypatch.setattr(materialize_shards, "_execute_shard_sql", calls_by_name["execute"])
    monkeypatch.setattr(materialize_shards, "_mark_materialize_done", calls_by_name["done"])
    monkeypatch.setattr(materialize_shards, "_mark_materialize_failed", calls_by_name["failed"])
    monkeypatch.setattr(
        materialize_shards,
        "_record_materialize_phase_duration",
        calls_by_name["duration"],
    )
    return calls_by_name


@pytest.mark.asyncio
async def test_shard_job_preserves_run_parameter_and_records_completion(monkeypatch) -> None:
    calls_by_name = _patch_shard_job_dependencies(monkeypatch)
    redis = _RedisMarker()

    shard_result = await materialize_shards._run_materialize_shard_job(
        {"redis": redis},
        _shard_task(),
        sql_builder=lambda context: f"INSERT INTO {context['score_table']}",
        phase="score",
        target_table_key="score_table",
        include_run_id=True,
    )

    assert shard_result == {
        "ok": True,
        "run_id": "run-a",
        "phase": "score",
        "year": 2024,
        "shard_id": 1,
        "shard_count": 3,
    }
    calls_by_name["execute"].assert_awaited_once_with(
        "INSERT INTO score_stage",
        year=2024,
        shard_id=1,
        shard_count=3,
        run_id="run-a",
    )
    calls_by_name["done"].assert_awaited_once_with(redis, "run-a")
    calls_by_name["duration"].assert_awaited_once()
    calls_by_name["failed"].assert_not_awaited()


@pytest.mark.asyncio
async def test_shard_job_tolerates_observability_count_failures(monkeypatch) -> None:
    calls_by_name = _patch_shard_job_dependencies(
        monkeypatch,
        count_results=(RuntimeError("before"), RuntimeError("after")),
    )

    result = await materialize_shards._run_materialize_shard_job(
        {},
        _shard_task(),
        sql_builder=lambda _context: "SELECT 1",
        phase="measure",
        target_table_key="score_table",
    )

    assert result["ok"] is True
    calls_by_name["execute"].assert_awaited_once_with(
        "SELECT 1",
        year=2024,
        shard_id=1,
        shard_count=3,
    )
    calls_by_name["done"].assert_not_awaited()
    calls_by_name["duration"].assert_not_awaited()


@pytest.mark.asyncio
async def test_shard_job_marks_terminal_execution_failure(monkeypatch) -> None:
    calls_by_name = _patch_shard_job_dependencies(
        monkeypatch,
        execution_error=RuntimeError("sql failed"),
    )
    redis = _RedisMarker()

    with pytest.raises(RuntimeError, match="sql failed"):
        await materialize_shards._run_materialize_shard_job(
            {"redis": redis},
            _shard_task(),
            sql_builder=lambda _context: "SELECT broken",
            phase="domain",
            target_table_key="score_table",
        )

    calls_by_name["failed"].assert_awaited_once_with(redis, "run-a")
    calls_by_name["done"].assert_not_awaited()


@pytest.mark.asyncio
async def test_shard_job_without_redis_still_propagates_failure(monkeypatch) -> None:
    calls_by_name = _patch_shard_job_dependencies(
        monkeypatch,
        execution_error=RuntimeError("sql failed"),
    )
    with pytest.raises(RuntimeError, match="sql failed"):
        await materialize_shards._run_materialize_shard_job(
            {},
            _shard_task(),
            sql_builder=lambda _context: "SELECT broken",
            phase="domain",
            target_table_key="score_table",
        )
    calls_by_name["failed"].assert_not_awaited()


class _DatabaseProbe:
    def __init__(self, scalar_value: object = 0) -> None:
        self.scalar_value = scalar_value
        self.statements: list[tuple[object, dict[str, object]]] = []

    @asynccontextmanager
    async def transaction(self):
        yield

    async def status(self, statement, **parameters):
        self.statements.append((statement, parameters))

    def text(self, statement: str) -> str:
        return statement

    @asynccontextmanager
    async def session(self):
        yield self

    async def execute(self, statement, parameters):
        self.statements.append((statement, parameters))
        return SimpleNamespace(scalar=lambda: self.scalar_value)


@pytest.mark.asyncio
async def test_execution_helpers_apply_transaction_and_count_contract(monkeypatch) -> None:
    database = _DatabaseProbe(scalar_value="7")
    monkeypatch.setattr(execution_helpers, "db", database)

    await execution_helpers._execute_shard_sql("UPDATE target", shard_id=2)
    count = await execution_helpers._count_shard_rows(
        "mrf",
        "target",
        year=2024,
        shard_id=2,
        shard_count=8,
    )

    assert count == 7
    assert database.statements[-1][1] == {
        "year": 2024,
        "shard_id": 2,
        "shard_count": 8,
    }
    assert any(statement == "UPDATE target" for statement, _ in database.statements)


@pytest.mark.asyncio
async def test_retry_helper_handles_empty_success_and_deadlock(monkeypatch) -> None:
    attempts: list[int] = []
    sleeps: list[float] = []

    async def push_rows(*_args, **_kwargs):
        attempts.append(1)
        if len(attempts) == 1:
            raise RuntimeError("deadlock detected")

    async def record_sleep(delay: float):
        sleeps.append(delay)

    monkeypatch.setattr(execution_helpers, "push_objects", push_rows)
    monkeypatch.setattr(execution_helpers.asyncio, "sleep", record_sleep)
    await execution_helpers._push_objects_with_retry([], object)
    await execution_helpers._push_objects_with_retry(
        [{"npi": 1}],
        type("Stage", (), {"__tablename__": "stage"}),
    )

    assert len(attempts) == 2
    assert sleeps == [execution_helpers.PROVIDER_QUALITY_DB_DEADLOCK_BASE_DELAY_SECONDS]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error",
    (
        RuntimeError("not retryable"),
        RuntimeError("deadlock detected"),
    ),
)
async def test_retry_helper_propagates_terminal_failure(monkeypatch, error) -> None:
    async def push_rows(*_args, **_kwargs):
        raise error

    monkeypatch.setattr(execution_helpers, "push_objects", push_rows)
    if "deadlock" in str(error):
        monkeypatch.setattr(execution_helpers, "PROVIDER_QUALITY_DB_DEADLOCK_RETRIES", 1)

    with pytest.raises(RuntimeError, match=str(error)):
        await execution_helpers._push_objects_with_retry(
            [{"npi": 1}],
            type("Stage", (), {"__tablename__": "stage"}),
        )


def _patch_materialize_state_machine(monkeypatch, phase: str) -> dict[str, AsyncMock]:
    calls_by_name = {
        "ensure_rx": AsyncMock(),
        "reset": AsyncMock(),
        "set_phase": AsyncMock(),
        "indexes": AsyncMock(),
        "enqueue": AsyncMock(),
        "wait": AsyncMock(),
    }
    monkeypatch.setattr(provider_quality, "_materialize_reporting_years", lambda _manifest: (2024,))
    monkeypatch.setattr(
        provider_quality,
        "_build_cohort_materialization_context",
        AsyncMock(
            return_value={
                "feature_table": "feature",
                "lsh_table": "lsh",
                "peer_target_table": "peer",
                "measure_table": "measure",
                "domain_table": "domain",
                "score_table": "score",
            }
        ),
    )
    monkeypatch.setattr(provider_quality, "_ensure_provider_quality_rx_agg_table", calls_by_name["ensure_rx"])
    monkeypatch.setattr(provider_quality, "_get_materialize_phase", AsyncMock(return_value=phase))
    monkeypatch.setattr(provider_quality, "_reset_materialize_state", calls_by_name["reset"])
    monkeypatch.setattr(provider_quality, "_set_materialize_phase", calls_by_name["set_phase"])
    monkeypatch.setattr(provider_quality, "_ensure_materialize_indexes", calls_by_name["indexes"])
    monkeypatch.setattr(provider_quality, "_enqueue_materialize_phase_shards", calls_by_name["enqueue"])
    monkeypatch.setattr(provider_quality, "_wait_for_materialize_phase_completion", calls_by_name["wait"])
    monkeypatch.setattr(provider_quality.db, "status", AsyncMock())
    monkeypatch.setattr(provider_quality, "_cohort_sql_phase_1_build_features", lambda _context: "phase-1")
    monkeypatch.setattr(provider_quality, "_cohort_sql_phase_3_procedure_bucket", lambda _context: "phase-3")
    monkeypatch.setattr(provider_quality, "_cohort_sql_phase_4_peer_targets", lambda _context: "phase-4")
    return calls_by_name


async def _run_materialize_state_machine(monkeypatch, phase: str) -> dict[str, AsyncMock]:
    calls_by_name = _patch_materialize_state_machine(monkeypatch, phase)
    await provider_quality._materialize_quality_rows_sharded(
        object(),
        classes={},
        schema="mrf",
        run_id="run",
        stage_suffix="stage",
        test_mode=True,
        manifest={"year": 2024},
    )
    return calls_by_name


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "phase",
    (
        "",
        "unknown",
        provider_quality.MAT_PHASE_1_BUILD_FEATURES,
        provider_quality.MAT_PHASE_2_BUILD_LSH_SHARDED,
        provider_quality.MAT_PHASE_3_UPDATE_PROCEDURE_BUCKET,
        provider_quality.MAT_PHASE_4_BUILD_PEER_TARGETS,
        provider_quality.MAT_PHASE_5_BUILD_MEASURE_SHARDED,
        provider_quality.MAT_PHASE_6_BUILD_DOMAIN_SHARDED,
    ),
)
async def test_materialize_state_machine_retries_after_scheduling(monkeypatch, phase) -> None:
    with pytest.raises(provider_quality.Retry):
        await _run_materialize_state_machine(monkeypatch, phase)


@pytest.mark.asyncio
async def test_materialize_state_machine_finishes_score_phase(monkeypatch) -> None:
    calls_by_name = await _run_materialize_state_machine(
        monkeypatch,
        provider_quality.MAT_PHASE_7_BUILD_SCORE_SHARDED,
    )
    calls_by_name["wait"].assert_awaited_once()
    calls_by_name["set_phase"].assert_awaited_once_with(
        ANY,
        "run",
        provider_quality.MAT_PHASE_DONE,
        total=0,
    )


@pytest.mark.asyncio
async def test_materialize_state_machine_accepts_completed_run(monkeypatch) -> None:
    calls_by_name = await _run_materialize_state_machine(
        monkeypatch,
        provider_quality.MAT_PHASE_DONE,
    )
    calls_by_name["enqueue"].assert_not_awaited()


@pytest.mark.asyncio
async def test_materialize_state_machine_accepts_empty_procedure_bucket(
    monkeypatch,
) -> None:
    _patch_materialize_state_machine(
        monkeypatch,
        provider_quality.MAT_PHASE_3_UPDATE_PROCEDURE_BUCKET,
    )
    monkeypatch.setattr(
        provider_quality,
        "_cohort_sql_phase_3_procedure_bucket",
        lambda _context: "",
    )
    with pytest.raises(provider_quality.Retry):
        await provider_quality._materialize_quality_rows_sharded(
            object(),
            classes={},
            schema="mrf",
            run_id="run",
            stage_suffix="stage",
            test_mode=True,
            manifest={"year": 2024},
        )
