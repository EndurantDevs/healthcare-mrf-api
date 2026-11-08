# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from unittest.mock import AsyncMock, Mock, call

import pytest

from api import control_imports, control_workers
from tests.test_control_imports_api import _install_queued_cancel_stubs
from tests.test_control_terminal_queue_residue import _install_dependencies

RUN_ID = "run_cancel_residue"
JOB_ID = f"ptg_start_{RUN_ID}"
QUEUE = "arq:PTGLarge"


def _cancel_run(status="queued"):
    return {
        "run_id": RUN_ID,
        "importer": "ptg",
        "status": status,
        "params": {"_expected_queue": QUEUE},
        "progress": {"pct": 0},
        "metrics": {
            "enqueue_adapter": "arq_single_job",
            "queue": QUEUE,
            "resource_class": "large",
            "worker_class": "process.PTGLarge",
            "job_id": JOB_ID,
            "cancel_signal": {"cancel_flag": {"redis": True}},
        },
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ("queued", "canceling"))
@pytest.mark.parametrize("queue_score", (1.0, None))
async def test_cancel_expired_payload_finishes(monkeypatch, status, queue_score):
    run = _cancel_run(status)
    _, _, pipeline = _install_dependencies(monkeypatch, run, queue_score=queue_score)
    pipeline.get = AsyncMock(return_value=None)
    remove_queued_job = control_imports._remove_queued_job
    _install_queued_cancel_stubs(monkeypatch, run, {}, {"enabled": True, "deleted": 0, "items": []})
    monkeypatch.setattr(control_imports, "_remove_queued_job", remove_queued_job)
    presence = Mock(return_value={"enabled": True, "job_count": 0, "pod_count": 0, "stopped": True})
    monkeypatch.setattr(control_imports, "exact_worker_presence", presence)

    canceled = await control_imports.request_cancel(RUN_ID)

    assert canceled["status"] == "canceled"
    assert canceled["finished_at"] is not None
    assert canceled["progress"]["pct"] == 100
    assert canceled["metrics"]["cancel_signal"]["payload_absent"] is True
    presence.assert_called_once_with(
        {"run_id": RUN_ID, "importer": "ptg", "queue": QUEUE, "worker_class": "process.PTGLarge"}
    )
    if queue_score is None:
        pipeline.zrem.assert_not_called()
    else:
        pipeline.zrem.assert_called_once_with(QUEUE, JOB_ID)
    pipeline.delete.assert_not_called()
    assert pipeline.watch.await_args_list == [
        call(f"arq:job:{JOB_ID}"),
        call(QUEUE, *(key for _, key in control_imports._arq_evidence_keys(JOB_ID))),
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("present_index", range(4))
async def test_cancel_residue_preserves_arq_state(monkeypatch, present_index):
    run = _cancel_run()
    _, _, pipeline = _install_dependencies(
        monkeypatch, run, key_presence=tuple(index == present_index for index in range(4))
    )
    pipeline.get = AsyncMock(return_value=None)

    signal = await control_imports._remove_queued_job(run)

    assert signal["identity_unavailable"] is True
    pipeline.multi.assert_not_called()
    pipeline.zrem.assert_not_called()
    pipeline.delete.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("mismatch", ("job", "lane", "adapter", "status", "params"))
async def test_cancel_residue_requires_durable_identity(monkeypatch, mismatch):
    run = _cancel_run()
    if mismatch == "job":
        run["metrics"]["job_id"] = "unrelated_job"
    elif mismatch == "lane":
        run["metrics"]["resource_class"] = "small"
    elif mismatch == "adapter":
        run["metrics"]["enqueue_adapter"] = "pending"
    elif mismatch == "status":
        run["status"] = "running"
    else:
        run["params"]["_expected_queue"] = "arq:PTGSmall"
    _, _, pipeline = _install_dependencies(monkeypatch, run)
    pipeline.get = AsyncMock(return_value=None)

    signal = await control_imports._remove_queued_job(run)

    assert signal["identity_unavailable"] is True
    pipeline.multi.assert_not_called()
    pipeline.zrem.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("importer", ("npi", "provider-directory-fhir"))
@pytest.mark.parametrize("queue_mismatch", (False, True))
async def test_other_adapters_require_matching_queue(monkeypatch, importer, queue_mismatch):
    run = _cancel_run()
    adapter = control_imports._SINGLE_JOB_ADAPTERS[importer]
    run["importer"] = importer
    run["params"] = {}
    run["metrics"]["queue"] = "arq:Unrelated" if queue_mismatch else adapter["queue"]
    run["metrics"]["job_id"] = f"{adapter['job_prefix']}_{RUN_ID}"
    _, _, pipeline = _install_dependencies(monkeypatch, run)
    pipeline.get = AsyncMock(return_value=None)

    signal = await control_imports._remove_queued_job(run)

    if queue_mismatch:
        assert signal["identity_unavailable"] is True
        pipeline.multi.assert_not_called()
    else:
        assert signal["payload_absent"] is True
        pipeline.zrem.assert_called_once_with(adapter["queue"], run["metrics"]["job_id"])


@pytest.mark.asyncio
async def test_cancel_residue_refuses_concurrent_enqueue(monkeypatch):
    run = _cancel_run()
    _, _, pipeline = _install_dependencies(monkeypatch, run)
    pipeline.get = AsyncMock(return_value=None)
    pipeline.execute.side_effect = control_imports.WatchError("changed")

    signal = await control_imports._remove_queued_job(run)

    assert signal["redis"] is False
    assert signal["removed"] is False
    assert "payload_absent" not in signal


@pytest.mark.asyncio
@pytest.mark.parametrize("error", (control_imports.WatchError("changed"), RuntimeError("unavailable")))
@pytest.mark.parametrize("failed_operation", ("watch", "get", "execute"))
async def test_failed_absence_proof_cannot_complete(monkeypatch, error, failed_operation):
    run = _cancel_run("canceling")
    _, _, pipeline = _install_dependencies(monkeypatch, run)
    pipeline.get = AsyncMock(return_value=None)
    getattr(pipeline, failed_operation).side_effect = error
    remove_queued_job = control_imports._remove_queued_job
    _install_queued_cancel_stubs(monkeypatch, run, {}, {"enabled": True, "deleted": 0, "items": []})
    monkeypatch.setattr(control_imports, "_remove_queued_job", remove_queued_job)

    result = await control_imports.request_cancel(RUN_ID)

    assert result["status"] == "canceling"
    assert result["metrics"]["cancel_signal"]["identity_unavailable"] is True
    assert result["finished_at"] is None


@pytest.mark.asyncio
async def test_cancel_waits_for_deleted_worker_pod(monkeypatch):
    run = _cancel_run()
    _, _, pipeline = _install_dependencies(monkeypatch, run, key_presence=(False,) * 8)
    pipeline.get = AsyncMock(return_value=None)
    pipeline.zscore.side_effect = [1.0, None]
    remove_queued_job = control_imports._remove_queued_job
    _, _, delete_workers = _install_queued_cancel_stubs(monkeypatch, run, {}, {})
    delete_workers.side_effect = [
        {"enabled": True, "deleted": 1, "items": [{"deleted": True}]},
        {"enabled": True, "deleted": 0, "items": []},
    ]
    monkeypatch.setattr(control_imports, "_remove_queued_job", remove_queued_job)
    presence = Mock(
        side_effect=[
            {"enabled": True, "job_count": 0, "pod_count": 1, "stopped": False},
            {"enabled": True, "job_count": 0, "pod_count": 0, "stopped": True},
        ]
    )
    monkeypatch.setattr(control_imports, "exact_worker_presence", presence)

    pending = await control_imports.request_cancel(RUN_ID)
    canceled = await control_imports.request_cancel(RUN_ID)

    assert pending["status"] == "canceling" and pending["finished_at"] is None
    assert canceled["status"] == "canceled" and canceled["finished_at"] is not None
    assert presence.call_count == 2
    pipeline.zrem.assert_called_once_with(QUEUE, JOB_ID)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "presence",
    (
        {"enabled": False},
        {"enabled": True},
        {"enabled": True, "job_count": 1, "pod_count": 0},
        {"enabled": True, "job_count": 0, "pod_count": 0},
        {"enabled": True, "job_count": 0, "pod_count": 0, "stopped": False},
    ),
)
async def test_cancel_rejects_incomplete_worker_absence(monkeypatch, presence):
    monkeypatch.setattr(control_imports, "exact_worker_presence", Mock(return_value=presence))
    assert not await control_imports._is_canceled_worker_stopped(_cancel_run())


@pytest.mark.asyncio
async def test_cancel_rejects_worker_lookup_failure(monkeypatch):
    monkeypatch.setattr(control_imports, "exact_worker_presence", Mock(side_effect=RuntimeError("unavailable")))
    assert not await control_imports._is_canceled_worker_stopped(_cancel_run())


@pytest.mark.asyncio
@pytest.mark.parametrize("condition,phase", (("Complete", "Succeeded"), ("Failed", "Failed")))
async def test_cancel_preserves_terminal_worker_history(monkeypatch, condition, phase):
    run = _cancel_run("canceling")
    _, _, pipeline = _install_dependencies(monkeypatch, run)
    pipeline.get = AsyncMock(return_value=None)
    remove_queued_job = control_imports._remove_queued_job
    _install_queued_cancel_stubs(
        monkeypatch, run, {}, {"enabled": True, "deleted": 0, "items": [{"reason": "terminal"}]}
    )
    monkeypatch.setattr(control_imports, "_remove_queued_job", remove_queued_job)
    requests = Mock(
        side_effect=[
            {"items": [{"status": {"conditions": [{"type": condition, "status": "True"}]}}]},
            {"items": [{"status": {"phase": phase}}]},
        ]
    )
    monkeypatch.setattr(control_workers, "_launcher_mode", lambda: "kubernetes")
    monkeypatch.setattr(control_workers, "_is_kubernetes_configured", lambda: True)
    monkeypatch.setattr(control_workers, "_kubernetes_namespace", lambda: "test-workers")
    monkeypatch.setattr(control_workers, "_kubernetes_request", requests)

    canceled = await control_imports.request_cancel(RUN_ID)

    assert canceled["status"] == "canceled"
    assert canceled["metrics"]["cancel_signal"]["worker_stopped"] is True
    assert len(requests.call_args_list) == 2
    assert all(request.args[0] == "GET" for request in requests.call_args_list)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "job_status,pod_records",
    (
        ({"failed": 1}, [{"status": {"phase": "Failed"}}]),
        ({"active": 1, "conditions": [{"type": "Failed", "status": "True"}]}, []),
        ({"conditions": [{"type": "Failed", "status": "True"}]}, [{"status": {"phase": "Running"}}]),
        ({"conditions": [{"type": "Complete", "status": "True"}]}, [{}]),
        ({"active": "0", "conditions": [{"type": "Complete", "status": "True"}]}, []),
        ({"conditions": [{"type": "Complete", "status": "True"}, "unknown"]}, []),
    ),
)
async def test_cancel_requires_all_workers_terminal(monkeypatch, job_status, pod_records):
    requests = Mock(side_effect=[{"items": [{"status": job_status}]}, {"items": pod_records}])
    monkeypatch.setattr(control_workers, "_launcher_mode", lambda: "kubernetes")
    monkeypatch.setattr(control_workers, "_is_kubernetes_configured", lambda: True)
    monkeypatch.setattr(control_workers, "_kubernetes_namespace", lambda: "test-workers")
    monkeypatch.setattr(control_workers, "_kubernetes_request", requests)

    assert not await control_imports._is_canceled_worker_stopped(_cancel_run())


@pytest.mark.asyncio
@pytest.mark.parametrize("response", (None, {}, {"items": None}, {"items": [None]}))
async def test_cancel_rejects_malformed_worker_inventory(monkeypatch, response):
    monkeypatch.setattr(control_workers, "_launcher_mode", lambda: "kubernetes")
    monkeypatch.setattr(control_workers, "_is_kubernetes_configured", lambda: True)
    monkeypatch.setattr(control_workers, "_kubernetes_namespace", lambda: "test-workers")
    monkeypatch.setattr(control_workers, "_kubernetes_request", Mock(return_value=response))

    assert not await control_imports._is_canceled_worker_stopped(_cancel_run())


@pytest.mark.parametrize("lane", ("Small", "Normal", "Large", "Huge"))
def test_cancel_presence_uses_exact_ptg_lane(monkeypatch, lane):
    requests = Mock(return_value={"items": []})
    monkeypatch.setattr(control_workers, "_launcher_mode", lambda: "kubernetes")
    monkeypatch.setattr(control_workers, "_is_kubernetes_configured", lambda: True)
    monkeypatch.setattr(control_workers, "_kubernetes_namespace", lambda: "test-workers")
    monkeypatch.setattr(control_workers, "_kubernetes_request", requests)
    payload = {"run_id": RUN_ID, "importer": "ptg", "queue": f"arq:PTG{lane}", "worker_class": f"process.PTG{lane}"}

    assert control_workers.exact_worker_presence(payload) == {
        "enabled": True,
        "job_count": 0,
        "pod_count": 0,
        "stopped": True,
    }
    assert "/jobs?" in requests.call_args_list[0].args[1]
    assert "/pods?" in requests.call_args_list[1].args[1]
    assert requests.call_args_list[0].args[1].split("?", 1)[1] == requests.call_args_list[1].args[1].split("?", 1)[1]


@pytest.mark.parametrize("queue", ("arq:NPI", "arq:PTGCandidateAudit", "arq:Missing"))
def test_cancel_presence_rejects_other_worker_specs(queue):
    with pytest.raises(RuntimeError, match="identity"):
        control_workers._exact_worker_spec({"run_id": RUN_ID, "importer": "ptg", "queue": queue})


@pytest.mark.parametrize(
    "kubernetes",
    (
        {"enabled": False},
        {"enabled": True, "deleted": 0, "items": [], "errors": [{"status": 503}]},
        {"enabled": True, "deleted": 0, "items": [{"reason": "active"}]},
    ),
)
def test_cancel_residue_requires_worker_cleanup(kubernetes):
    assert not control_imports._is_queued_arq_cancel_completed(
        {
            "payload_absent": True,
            "removed": True,
            "cancel_flag": {"redis": True},
            "kubernetes": kubernetes,
        }
    )


@pytest.mark.parametrize("cancel_flag", (None, {"redis": False}))
def test_cancel_residue_requires_cancel_fence(cancel_flag):
    assert not control_imports._is_queued_arq_cancel_completed(
        {
            "payload_absent": True,
            "removed": True,
            "cancel_flag": cancel_flag,
            "kubernetes": {"enabled": True, "deleted": 1},
        }
    )
