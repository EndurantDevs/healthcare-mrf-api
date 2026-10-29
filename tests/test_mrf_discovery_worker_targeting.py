from __future__ import annotations

from types import SimpleNamespace

import pytest

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
    monkeypatch.setattr(control_workers, "_is_pid_running", lambda pid: pid == FakeProcess.pid)
    monkeypatch.setattr(control_workers, "_is_pid_spec_match", lambda pid, spec: True)

    launch_result = control_workers.ensure_worker(_discovery_launch_payload())

    assert launch_result["status"] == "started"
    assert captured_by_key["cmd"][-2:] == ["worker-once", "process.MRFSourceDiscovery"]
    assert captured_by_key["env"]["HLTHPRT_WORKER_ONCE_TARGET_JOB_ID"] == "job_discovery"


@pytest.mark.parametrize("discovery_image", [None, "", "  ", "example/discovery@sha256:" + "a" * 64])
def test_kubernetes_worker_runs_targeted_mrf_discovery_job_once(monkeypatch, discovery_image):
    request_calls: list[tuple[str, str, dict[str, object] | None]] = []

    def fake_request(method, path, body=None):
        request_calls.append((method, path, body))
        if method == "GET" and any(request_call[0] == "POST" for request_call in request_calls):
            return {"items": [{"metadata": {"name": "worker-job"}, "status": {"active": 1}}]}
        return {"items": []}

    monkeypatch.setenv("HLTHPRT_WORKER_LAUNCHER", "kubernetes")
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_IMAGE", "ghcr.io/example/healthcare-mrf-api:test")
    if discovery_image is None:
        monkeypatch.delenv("HLTHPRT_MRF_SOURCE_DISCOVERY_WORKER_IMAGE", raising=False)
    else:
        monkeypatch.setenv("HLTHPRT_MRF_SOURCE_DISCOVERY_WORKER_IMAGE", discovery_image)
    monkeypatch.setattr(control_workers, "_is_kubernetes_configured", lambda: True)
    monkeypatch.setattr(control_workers, "_kubernetes_namespace", lambda: "healthporta-dev")
    monkeypatch.setattr(control_workers, "_kubernetes_request", fake_request)

    launch_result = control_workers.ensure_worker(_discovery_launch_payload())

    assert launch_result["status"] == "started"
    job = next(call[2] for call in request_calls if call[0] == "POST")
    container = job["spec"]["template"]["spec"]["containers"][0]
    assert container["image"] == (
        discovery_image.strip()
        if discovery_image and discovery_image.strip()
        else "ghcr.io/example/healthcare-mrf-api:test"
    )
    assert container["command"][-2:] == ["worker-once", "process.MRFSourceDiscovery"]
    environment_by_name = {entry["name"]: entry["value"] for entry in container["env"]}
    assert environment_by_name["HLTHPRT_WORKER_ONCE_TARGET_JOB_ID"] == "job_discovery"


@pytest.mark.parametrize("importer,specialized", [("mrf", False), ("mrf-source-discovery", True)])
def test_discovery_image_scope_preserves_specialized_launch_priority(monkeypatch, importer, specialized):
    generic_image = "example/worker@sha256:" + "b" * 64
    specialized_image = "example/source@sha256:" + "c" * 64
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_IMAGE", generic_image)
    monkeypatch.setenv("HLTHPRT_MRF_SOURCE_DISCOVERY_WORKER_IMAGE", "example/discovery@sha256:" + "a" * 64)
    spec = control_workers._BY_IMPORTER_ROLE[(importer, "start")]
    manifests = []

    def capture_manifest(selected_spec, launch_request, image):
        manifests.append(image)
        return {}

    monkeypatch.setattr(control_workers, "_worker_job_manifest", capture_manifest)
    monkeypatch.setattr(control_workers, "_kubernetes_request", lambda *args: {})
    monkeypatch.setattr(control_workers, "_worker_state", lambda *args: {})
    monkeypatch.setattr(control_workers, "_kubernetes_namespace", lambda: "synthetic-workers")
    monkeypatch.setattr(
        control_workers.registry_aca_source_worker,
        "authorized_aca_source_worker_launch",
        lambda *args: SimpleNamespace(image=specialized_image) if specialized else None,
    )
    launch_result = control_workers._ensure_kubernetes_job(spec, {"importer": importer}, {})
    assert launch_result["status"] == "started"
    assert manifests == [specialized_image if specialized else generic_image]
