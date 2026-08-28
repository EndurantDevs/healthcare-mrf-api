from __future__ import annotations

import asyncio
import threading
import urllib.parse
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest

from api import control_workers
from process.control_lifecycle import acquire_control_run_worker_action_lock


RUN_ID = "run_missing_worker"


def test_exact_worker_presence_queries_job_and_pod_for_same_run(monkeypatch):
    payload = {"run_id": RUN_ID, "importer": "plan-pricing-projection"}
    spec = control_workers._exact_worker_spec(payload)
    selector = control_workers._kubernetes_label_selector(spec, payload)
    query = urllib.parse.urlencode({"labelSelector": selector})
    requests: list[tuple[str, str]] = []

    def request(method, path, **_kwargs):
        requests.append((method, path))
        return {"items": [{"metadata": {"name": "present"}}]}

    monkeypatch.setenv("HLTHPRT_WORKER_LAUNCHER", "kubernetes")
    monkeypatch.setattr(control_workers, "_is_kubernetes_configured", lambda: True)
    monkeypatch.setattr(control_workers, "_kubernetes_namespace", lambda: "dev")
    monkeypatch.setattr(control_workers, "_kubernetes_request", request)

    assert control_workers.exact_worker_presence(payload) == {
        "enabled": True,
        "job_count": 1,
        "pod_count": 1,
    }
    assert requests == [
        ("GET", f"/apis/batch/v1/namespaces/dev/jobs?{query}"),
        ("GET", f"/api/v1/namespaces/dev/pods?{query}"),
    ]


@pytest.mark.asyncio
async def test_plan_pricing_ensure_holds_exact_run_lock_through_launch(
    monkeypatch,
):
    event_list: list[str] = []

    @asynccontextmanager
    async def acquire():
        event_list.append("transaction-enter")
        yield object()
        event_list.append("transaction-exit")

    async def lock(_connection, _run_id):
        event_list.append("run-lock")

    async def admit(*_args, **_kwargs):
        event_list.append("admit")
        return None

    monkeypatch.setattr(control_workers.db, "acquire", acquire)
    monkeypatch.setattr(control_workers, "acquire_ptg_admission_lock", AsyncMock())
    monkeypatch.setattr(
        control_workers,
        "acquire_control_run_worker_action_lock",
        lock,
    )
    monkeypatch.setattr(control_workers, "require_not_wave_owned_run", AsyncMock())
    monkeypatch.setattr(control_workers, "require_no_capacity_owning_wave", AsyncMock())
    monkeypatch.setattr(control_workers, "_admit_worker_ensure", admit)
    monkeypatch.setattr(
        control_workers,
        "ensure_worker",
        lambda _payload: event_list.append("launch") or {"status": "started"},
    )

    result_by_field = await control_workers.guarded_ensure_worker(
        {"run_id": RUN_ID, "importer": "plan-pricing-projection"}
    )

    assert result_by_field == {"status": "started"}
    assert event_list == [
        "transaction-enter",
        "run-lock",
        "admit",
        "launch",
        "transaction-exit",
    ]


@pytest.mark.asyncio
async def test_plan_pricing_ensure_keeps_lock_until_canceled_launch_settles(
    monkeypatch,
):
    events: list[str] = []
    launch_started = threading.Event()
    release_launch = threading.Event()

    @asynccontextmanager
    async def acquire():
        events.append("transaction-enter")
        try:
            yield object()
        finally:
            events.append("transaction-exit")

    def launch(_payload):
        launch_started.set()
        release_launch.wait(timeout=2)
        events.append("launch-finished")
        return {"status": "started"}

    monkeypatch.setattr(control_workers.db, "acquire", acquire)
    monkeypatch.setattr(control_workers, "acquire_ptg_admission_lock", AsyncMock())
    monkeypatch.setattr(
        control_workers,
        "acquire_control_run_worker_action_lock",
        AsyncMock(),
    )
    monkeypatch.setattr(control_workers, "require_not_wave_owned_run", AsyncMock())
    monkeypatch.setattr(control_workers, "require_no_capacity_owning_wave", AsyncMock())
    monkeypatch.setattr(
        control_workers,
        "_admit_worker_ensure",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(control_workers, "ensure_worker", launch)

    ensure_task = asyncio.create_task(
        control_workers.guarded_ensure_worker(
            {"run_id": RUN_ID, "importer": "plan-pricing-projection"}
        )
    )
    assert await asyncio.to_thread(launch_started.wait, 1)
    ensure_task.cancel()
    await asyncio.sleep(0)
    assert "transaction-exit" not in events

    release_launch.set()
    with pytest.raises(asyncio.CancelledError):
        await ensure_task

    assert events == ["transaction-enter", "launch-finished", "transaction-exit"]


@pytest.mark.asyncio
async def test_worker_action_lock_uses_exact_run_key():
    calls = []

    class _Executor:
        async def scalar(self, statement, **params):
            calls.append((str(statement), params))

    await acquire_control_run_worker_action_lock(_Executor(), RUN_ID)

    assert "pg_advisory_xact_lock" in calls[0][0]
    assert calls[0][1] == {
        "lock_name": f"control-run-worker-action:v1:{RUN_ID}"
    }


@pytest.mark.asyncio
async def test_worker_action_lock_requires_run_id():
    with pytest.raises(ValueError, match="run_id is required"):
        await acquire_control_run_worker_action_lock(object(), " ")
