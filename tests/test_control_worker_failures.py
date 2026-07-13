# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from api import control_workers


def test_worker_state_reports_failed_kubernetes_pod_termination(monkeypatch):
    request_calls: list[tuple[str, str, dict[str, object] | None]] = []

    def fake_request(method, path, body=None):
        request_calls.append((method, path, body))
        if method == "GET" and "/apis/batch/v1" in path:
            return {"items": [{"metadata": {"name": "failed-job"}, "status": {"failed": 1}}]}
        if method == "GET" and "/api/v1/namespaces/" in path:
            terminated_state_map = {"reason": "OOMKilled", "exitCode": 137}
            container_status_map = {"name": "worker", "state": {"terminated": terminated_state_map}}
            pod_status_map = {"containerStatuses": [container_status_map]}
            return {"items": [{"metadata": {"name": "failed-job-pod"}, "status": pod_status_map}]}
        return {}

    monkeypatch.setenv("HLTHPRT_WORKER_LAUNCHER", "kubernetes")
    monkeypatch.setattr(control_workers, "_is_kubernetes_configured", lambda: True)
    monkeypatch.setattr(control_workers, "_kubernetes_namespace", lambda: "healthporta-dev")
    monkeypatch.setattr(control_workers, "_kubernetes_request", fake_request)

    worker_state_map = control_workers.worker_state(
        {"importer": "ptg", "queue": "arq:PTGNormal", "worker_class": "process.PTGNormal", "run_id": "run_ptg"}
    )

    assert worker_state_map["status"] == "failed"
    worker_state_item = worker_state_map["items"][0]
    assert worker_state_item["job_status"] == "failed"
    assert worker_state_item["failure"]["reason"] == "OOMKilled"
    assert worker_state_item["failure"]["exitCode"] == 137
    assert any("/api/v1/namespaces/healthporta-dev/pods" in call[1] for call in request_calls)
