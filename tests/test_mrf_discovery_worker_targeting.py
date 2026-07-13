from __future__ import annotations

from api import control_workers


def _discovery_launch_payload() -> dict[str, str]:
    return {
        "importer": "mrf-source-discovery",
        "run_id": "run_discovery",
        "job_id": "job_discovery",
    }


def test_process_worker_runs_targeted_mrf_discovery_job_once(monkeypatch, tmp_path):
    class FakeProcess:
        pid = 790

    captured_by_key: dict[str, object] = {}

    monkeypatch.setenv("HLTHPRT_WORKER_STATE_DIR", str(tmp_path / "state"))
    monkeypatch.setenv("HLTHPRT_WORKER_LOG_DIR", str(tmp_path / "logs"))

    def fake_popen(cmd, *, env, **_kwargs):
        captured_by_key["cmd"] = cmd
        captured_by_key["env"] = env
        return FakeProcess()

    monkeypatch.setattr(control_workers.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(control_workers, "_pid_running", lambda pid: pid == FakeProcess.pid)
    monkeypatch.setattr(control_workers, "_pid_matches_spec", lambda pid, spec: True)

    result = control_workers.ensure_worker(_discovery_launch_payload())

    assert result["status"] == "started"
    assert captured_by_key["cmd"][-2:] == ["worker-once", "process.MRFSourceDiscovery"]
    assert captured_by_key["env"]["HLTHPRT_WORKER_ONCE_TARGET_JOB_ID"] == "job_discovery"


def test_kubernetes_worker_runs_targeted_mrf_discovery_job_once(monkeypatch):
    request_calls: list[tuple[str, str, dict[str, object] | None]] = []

    def fake_request(method, path, body=None):
        request_calls.append((method, path, body))
        if method == "GET" and any(item[0] == "POST" for item in request_calls):
            return {"items": [{"metadata": {"name": "worker-job"}, "status": {"active": 1}}]}
        return {"items": []}

    monkeypatch.setenv("HLTHPRT_WORKER_LAUNCHER", "kubernetes")
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_IMAGE", "ghcr.io/endurantdevs/healthcare-mrf-api:dev")
    monkeypatch.setattr(control_workers, "_is_kubernetes_configured", lambda: True)
    monkeypatch.setattr(control_workers, "_kubernetes_namespace", lambda: "healthporta-dev")
    monkeypatch.setattr(control_workers, "_kubernetes_request", fake_request)

    result = control_workers.ensure_worker(_discovery_launch_payload())

    assert result["status"] == "started"
    job = next(call[2] for call in request_calls if call[0] == "POST")
    container = job["spec"]["template"]["spec"]["containers"][0]
    assert container["command"][-2:] == ["worker-once", "process.MRFSourceDiscovery"]
    environment_by_name = {item["name"]: item["value"] for item in container["env"]}
    assert environment_by_name["HLTHPRT_WORKER_ONCE_TARGET_JOB_ID"] == "job_discovery"
