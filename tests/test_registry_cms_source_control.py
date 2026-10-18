# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact control/ARQ adapter, trusted target configuration and cancellation drain."""

import asyncio
import json
import os
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

os.environ.setdefault("HLTHPRT_REDIS_ADDRESS", "redis://localhost")

import process
from api import control_imports, control_workers
from process import control_lifecycle
from process import registry_cms_source_worker as worker
from tests.test_registry_cms_source_worker import configuration

IMPORTER = "registry-cms-source-worker"
_PRIVATE = "synthetic-sensitive-marker"


def event_params():
    configured = configuration()
    return {name: configured[name] for name in worker.REGISTRY_CMS_EVENT_FIELDS}


def control_payload():
    adapter = control_imports._SINGLE_JOB_ADAPTERS[IMPORTER]
    return control_imports._adapter_payload(adapter, {"run_id": "synthetic-run", "importer": IMPORTER}, event_params())


def test_real_registry_schema_adapter_worker_and_arq_function_agree():
    item = next(item for item in control_imports.importer_registry() if item["name"] == IMPORTER)
    assert item["schedulable"] and item["cancelable"] and item["enqueue_adapter"] == "arq_single_job"
    assert item["queue"] == "arq:RegistryCMSSource"
    assert item["function"] == "control_single_job_start"
    assert {param["name"] for param in item["params_schema"]} == worker.REGISTRY_CMS_EVENT_FIELDS
    assert all(param["required"] for param in item["params_schema"])
    spec = control_workers._BY_QUEUE[item["queue"]]
    assert spec.worker_class == "process.RegistryCMSSource" and spec.importers == (IMPORTER,)
    settings = process.RegistryCMSSource
    assert settings.queue_name == spec.queue
    assert settings.max_jobs == settings.queue_read_limit == settings.max_tries == 1
    assert settings.job_timeout == 4200
    assert settings.functions == [process.control_single_job_start]
    assert settings.functions[0].name == "control_single_job_start"
    assert settings.functions[0].max_tries == 1


@pytest.mark.asyncio
async def test_enqueue_uses_actual_fixed_adapter_and_only_event_identity(monkeypatch):
    redis = SimpleNamespace(enqueue_job=AsyncMock(return_value=SimpleNamespace(job_id="synthetic-job")))
    create_pool = AsyncMock(return_value=redis)
    monkeypatch.setattr(control_imports, "create_pool", create_pool)
    result = await control_imports._enqueue_import_start(
        {"run_id": "synthetic-run", "importer": IMPORTER, "params": event_params()}
    )
    assert result["status"] == "queued" and result["phase_detail"] == "enqueued"
    args, kwargs = redis.enqueue_job.await_args
    assert args[0] == "control_single_job_start"
    payload = args[1]
    assert payload["target_module"] == "process.registry_cms_source_worker"
    assert payload["target_function"] == "control_start" and payload["call_style"] == "ctx_task"
    assert payload["task"] == event_params() and payload["run_shutdown"] is False
    assert kwargs == {"_queue_name": "arq:RegistryCMSSource", "_job_id": "registry_cms_source_synthetic-run"}
    run_by_name = {
        "run_id": "synthetic-run",
        "importer": IMPORTER,
        "params": event_params(),
        "metrics": result["metrics"],
    }
    assert control_imports._arq_cleanup_identity(run_by_name)[0:2] == ("synthetic-run", IMPORTER)
    assert control_imports._supports_active_cancel(IMPORTER)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "params",
    [
        None,
        [],
        {},
        {"source_schema": "source_origin"},
        {"source_schema": "source_origin", "receipt_id": "a" * 64, "notification_sha256": True},
        {"source_schema": "bad;schema", "receipt_id": "a" * 64, "notification_sha256": "b" * 64},
    ],
)
async def test_malformed_params_refuse_before_admission_and_queue(monkeypatch, params):
    database = SimpleNamespace(execute=AsyncMock(side_effect=AssertionError("Unexpected database write")))
    pool = AsyncMock(side_effect=AssertionError("Unexpected queue connection"))
    monkeypatch.setattr(control_imports, "db", database)
    monkeypatch.setattr(control_imports, "create_pool", pool)
    with pytest.raises(ValueError):
        await control_imports.create_import_run({"importer": IMPORTER, "params": params})
    result = await control_imports._enqueue_import_start(
        {"importer": IMPORTER, "run_id": "synthetic", "params": params}
    )
    assert result["status"] == "failed" and result["error"]["code"] == "invalid_enqueue_adapter"
    database.execute.assert_not_called()
    pool.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field",
    [
        "execution_dsn",
        "notification_dsn",
        "owner_role",
        "approval",
        "coordinates",
        "custom_data",
        "sql",
        "env",
        "path",
        "test_mode",
        "target_module",
    ],
)
async def test_configuration_and_target_overrides_never_reach_queue(monkeypatch, field):
    pool = AsyncMock(side_effect=AssertionError("Unexpected queue connection"))
    monkeypatch.setattr(control_imports, "create_pool", pool)
    params = event_params() | {field: _PRIVATE}
    with pytest.raises(ValueError):
        await control_imports.create_import_run({"importer": IMPORTER, "params": params})
    result = await control_imports._enqueue_import_start(
        {"importer": IMPORTER, "run_id": "synthetic", "params": params}
    )
    assert result["status"] == "failed" and _PRIVATE not in str(result)
    pool.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["source_schema"])
async def test_target_refuses_event_mismatch_without_opening_worker(monkeypatch, field):
    monkeypatch.setenv(worker.CONFIG_ENV, json.dumps(configuration()))
    run = AsyncMock(side_effect=AssertionError("Unexpected worker resource"))
    monkeypatch.setattr(worker, "run_registry_cms_source_worker", run)
    task = event_params() | {"run_id": "synthetic-run", field: "other_schema" if field == "source_schema" else "c" * 64}
    with pytest.raises(ValueError):
        await worker.control_start({}, task)
    run.assert_not_awaited()


@pytest.mark.asyncio
async def test_actual_control_wrapper_invokes_real_target_and_projects_safe_result(monkeypatch):
    monkeypatch.setenv(worker.CONFIG_ENV, json.dumps(configuration()))
    monkeypatch.setenv("HLTHPRT_IMPORT_LIVE_PROGRESS_HEARTBEAT_SECONDS", "0")
    marks = AsyncMock(return_value=True)
    monkeypatch.setattr(control_lifecycle, "mark_control_run", marks)
    monkeypatch.setattr(control_lifecycle, "_flush_terminal_status_events", AsyncMock())
    run = AsyncMock(return_value={"state": "replayed"})
    monkeypatch.setattr(worker, "run_registry_cms_source_worker", run)
    result = await control_lifecycle.control_single_job_start({}, control_payload())
    assert result["status"] == "succeeded" and result["result"] == {"state": "replayed"}
    assert run.await_count == 1
    assert [call.kwargs["status"] for call in marks.await_args_list] == ["running", "succeeded"]


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["control_flag", "external_cancel", "worker_failure"])
async def test_control_terminal_projection_follows_consumer_drain(monkeypatch, mode):
    monkeypatch.setenv(worker.CONFIG_ENV, json.dumps(configuration()))
    monkeypatch.setenv("HLTHPRT_IMPORT_LIVE_PROGRESS_HEARTBEAT_SECONDS", "0")
    events = []
    started = asyncio.Event()

    async def run(_config, **_kwargs):
        try:
            started.set()
            if mode == "worker_failure":
                raise RuntimeError(_PRIVATE)
            await asyncio.Event().wait()
        finally:
            await asyncio.sleep(0)
            events.append("drained")

    async def is_attempt_current(*args, **kwargs):
        events.append(kwargs["status"])
        return True

    monkeypatch.setattr(worker, "run_registry_cms_source_worker", run)
    monkeypatch.setattr(control_lifecycle, "mark_control_run", is_attempt_current)
    monkeypatch.setattr(control_lifecycle, "_flush_terminal_status_events", AsyncMock())
    redis = SimpleNamespace(get=AsyncMock(side_effect=[None, b"1"] if mode == "control_flag" else None))
    task = asyncio.create_task(control_lifecycle.control_single_job_start({"redis": redis}, control_payload()))
    await started.wait()
    if mode == "external_cancel":
        task.cancel(_PRIVATE)
    if mode == "worker_failure":
        with pytest.raises(RuntimeError, match="^registry_cms_source_worker_failed$"):
            await task
        terminal = "failed"
    else:
        outcome = await task
        terminal = "canceled" if mode == "control_flag" else "failed"
        assert outcome["status"] == terminal and _PRIVATE not in str(outcome)
    assert events.index("drained") < events.index(terminal)


@pytest.mark.asyncio
async def test_control_future_event_uses_only_source_owned_receipt_facts(monkeypatch):
    monkeypatch.setenv(worker.CONFIG_ENV, json.dumps(configuration()))
    run = AsyncMock(return_value={"state": "review_required"})
    monkeypatch.setattr(worker, "run_registry_cms_source_worker", run)
    event = event_params() | {"receipt_id": "c" * 64, "notification_sha256": "d" * 64}
    assert await worker.control_start({}, event | {"run_id": "future-run"}) == {"state": "review_required"}
    assert run.await_args.kwargs == {"event": event}
    assert run.await_args.args[0].consumer.coordinates.edition_id == "edition-new"


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["succeeded", "failed", "canceled", "dead_letter", "running"])
async def test_all_status_event_replay_compares_exact_parameters(monkeypatch, status):
    existing_by_name = dict(run_id="event-run", importer=IMPORTER, status=status, params=event_params())
    find = AsyncMock(return_value=existing_by_name)
    active = AsyncMock(side_effect=AssertionError("Must use all-status event lookup"))
    monkeypatch.setattr(control_imports, "find_importer_run_by_idempotency_key", find)
    monkeypatch.setattr(control_imports, "find_active_run_by_idempotency_key", active)
    assert (
        await control_imports._idempotent_import_run(IMPORTER, "event-key", params=event_params()) == existing_by_name
    )
    with pytest.raises(ValueError, match="event_identity_mismatch"):
        await control_imports._idempotent_import_run(
            IMPORTER, "event-key", params=event_params() | {"receipt_id": "e" * 64}
        )
    active.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("same_event", [True, False])
async def test_completed_event_api_replay_never_creates_another_run(monkeypatch, same_event):
    existing_by_name = dict(run_id="event-run", importer=IMPORTER, status="succeeded", params=event_params())
    monkeypatch.setattr(
        control_imports, "find_importer_run_by_idempotency_key", AsyncMock(return_value=existing_by_name)
    )
    enqueue = AsyncMock(side_effect=AssertionError("Completed event must not enqueue"))
    monkeypatch.setattr(control_imports, "_enqueue_import_start", enqueue)
    params = event_params() if same_event else event_params() | {"notification_sha256": "e" * 64}
    if same_event:
        result, created = await control_imports.create_import_run(
            dict(importer=IMPORTER, idempotency_key="event-key", params=params)
        )
        assert not created and result["run_id"] == "event-run"
    else:
        with pytest.raises(ValueError, match="event_identity_mismatch"):
            await control_imports.create_import_run(dict(importer=IMPORTER, idempotency_key="event-key", params=params))
    enqueue.assert_not_awaited()


@pytest.mark.asyncio
async def test_active_other_event_cannot_replace_queued_event_identity(monkeypatch):
    monkeypatch.setattr(control_imports, "find_importer_run_by_idempotency_key", AsyncMock(return_value=None))
    monkeypatch.setattr(
        control_imports,
        "find_earliest_active_run_by_importer",
        AsyncMock(
            return_value=dict(
                run_id="other-event",
                importer=IMPORTER,
                status="running",
                params=event_params() | {"receipt_id": "e" * 64},
            )
        ),
    )
    with pytest.raises(ValueError, match="event_identity_mismatch"):
        await control_imports.create_import_run(
            dict(importer=IMPORTER, idempotency_key="event-key", params=event_params())
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["completed", "replayed", "stale_source", "review_required"])
async def test_control_result_state_reaches_genuine_terminal_metrics(monkeypatch, state):
    monkeypatch.setenv(worker.CONFIG_ENV, json.dumps(configuration()))
    monkeypatch.setenv("HLTHPRT_IMPORT_LIVE_PROGRESS_HEARTBEAT_SECONDS", "0")
    marks = AsyncMock(return_value=True)
    monkeypatch.setattr(control_lifecycle, "mark_control_run", marks)
    monkeypatch.setattr(control_lifecycle, "_flush_terminal_status_events", AsyncMock())
    monkeypatch.setattr(worker, "run_registry_cms_source_worker", AsyncMock(return_value={"state": state}))
    await control_lifecycle.control_single_job_start({}, control_payload())
    terminal = marks.await_args_list[-1]
    assert terminal.kwargs["status"] == "succeeded"
    assert terminal.kwargs["metrics"] == {"state": state}


@pytest.mark.asyncio
@pytest.mark.parametrize("present", [False, True])
async def test_registry_stale_reconciliation_keeps_exact_attempt_and_worker_absence(monkeypatch, present):
    from tests import test_control_missing_worker_reconciliation as support

    run = support._run(IMPORTER)
    run["params"] = event_params()
    run["metrics"] = dict(
        queue="arq:RegistryCMSSource",
        worker_class="process.RegistryCMSSource",
        job_id="registry_cms_source_" + support.RUN_ID,
    )
    connection = support._install_reconciliation_dependencies(
        monkeypatch, run, kubernetes={"enabled": True, "job_count": int(present), "pod_count": int(present)}
    )
    request = support.EXPECTED_BODY | {"expected_importer": IMPORTER}
    if present:
        with pytest.raises(control_imports.StaleWorkerReconciliationConflict):
            await control_imports.reconcile_stale_worker_failure(support.RUN_ID, request)
        assert not connection.updates
    else:
        result = await control_imports.reconcile_stale_worker_failure(support.RUN_ID, request)
        assert result["status"] == "failed" and result["attempt_id"] == support.ATTEMPT_ID
        assert len(connection.updates) == 1
