# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Phase checkpoints preserve the original launch admission and active Job."""

import pytest

from api import control_workers as workers
from api import provider_directory_source_worker as source
from process import control_lifecycle as lifecycle
from process import provider_directory_rooted_graph_worker as worker
from tests.test_provider_directory_rooted_graph_worker import operation, task
from tests.test_provider_directory_source_worker_launch import (
    IMAGE,
    SPEC,
    _mock_original_worker_control,
)
from tests.test_provider_directory_source_worker_launch import secret_selection as secret_selection
from tests.test_provider_directory_source_worker_launch import source_runtime_mount as source_runtime_mount


def _install_phase_persistence(monkeypatch, recorded_by_field):
    async def update(statement):
        assert "metrics" not in {column.name for column in statement._values}
        parameters = statement.compile().params
        assert all(
            value in parameters.values()
            for value in (recorded_by_field["run_id"], recorded_by_field["importer"], "running")
        )
        for field in ("status", "phase_detail", "progress", "finished_at"):
            recorded_by_field[field] = parameters[field]
        return 1

    attempt_by_field = {
        "run_id": recorded_by_field["run_id"],
        "attempt_id": "synthetic-attempt",
        "attempt_started_at": "2026-01-01T00:00:00Z",
    }
    monkeypatch.setattr(lifecycle, "current_live_progress_context", lambda: attempt_by_field)
    monkeypatch.setattr(lifecycle, "read_live_progress", lambda _run: None)
    monkeypatch.setattr(lifecycle, "write_live_progress", lambda **_payload: True)
    monkeypatch.setattr(lifecycle, "_execute_control_run_update", update)


@pytest.mark.asyncio
async def test_running_worker_repeat_ensure_after_phase_updates(monkeypatch, secret_selection, source_runtime_mount):
    request = task()
    run_id = request["run_id"]
    recorded_by_field = {
        "run_id": run_id,
        "importer": "provider-directory-fhir",
        "status": "running",
        "params": request,
        "metrics": {
            "queue": SPEC.queue,
            "function": "control_single_job_start",
            "job_id": "provider_directory_start_" + run_id,
        },
    }
    original_metrics_by_field = dict(recorded_by_field["metrics"])
    monkeypatch.setenv(source.IMAGE_ENV, IMAGE)
    admitted = workers._AdmittedWorkerRequest(run_id=run_id, importer=recorded_by_field["importer"])
    workers._bind_admitted_job(admitted, recorded_by_field)
    assert await workers._admit_source_worker_launches(admitted, recorded_by_field) is None
    job = workers._worker_job_manifest(SPEC, admitted, IMAGE)
    job["status"] = {"active": 1}
    _, calls = _mock_original_worker_control(monkeypatch, recorded_by_field, [recorded_by_field], job)
    _install_phase_persistence(monkeypatch, recorded_by_field)
    context_by_field = {}
    for phase in ("acquiring", "publishing"):
        await worker._record_phase(context_by_field, run_id, operation(), phase)
        assert recorded_by_field["metrics"] == original_metrics_by_field
        assert recorded_by_field["phase_detail"] == "rooted graph " + phase
        assert recorded_by_field["progress"]["attempt_id"] == "synthetic-attempt"
        outcome_by_field = await workers.guarded_ensure_worker(
            {"run_id": run_id, "importer": recorded_by_field["importer"]}
        )
        assert outcome_by_field["status"] == "already_running"
        assert context_by_field["context"]["audit"]["rooted_graph"]["state"] == phase
    assert calls and all(call[0] == "GET" for call in calls)
