from __future__ import annotations

from api import control_workers


def test_worker_registry_exposes_shared_and_finish_workers():
    items = control_workers.worker_registry()
    by_importer = {
        importer: item
        for item in items
        for importer in item["importers"]
        if item["role"] == "start"
    }
    by_queue = {item["queue"]: item for item in items}

    assert by_importer["claims-procedures"]["worker_class"] == "process.ClaimsPricing"
    assert by_importer["ms-drg"]["worker_class"] == "process.MSDRG"
    assert by_queue["arq:PartDFormularyNetwork_finish"]["role"] == "finish"


def test_ensure_worker_starts_registered_burst_worker(monkeypatch, tmp_path):
    captured: dict[str, object] = {}

    class FakeProcess:
        pid = 12345

    def fake_popen(cmd, *, cwd, env, stdout, stderr, start_new_session):
        captured.update(
            {
                "cmd": cmd,
                "cwd": cwd,
                "env": env,
                "stdout": stdout,
                "stderr": stderr,
                "start_new_session": start_new_session,
            }
        )
        return FakeProcess()

    monkeypatch.setenv("HLTHPRT_WORKER_STATE_DIR", str(tmp_path / "state"))
    monkeypatch.setenv("HLTHPRT_WORKER_LOG_DIR", str(tmp_path / "logs"))
    monkeypatch.setattr(control_workers.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(control_workers, "_pid_running", lambda pid: pid == FakeProcess.pid)
    monkeypatch.setattr(control_workers, "_pid_matches_spec", lambda pid, spec: True)

    result = control_workers.ensure_worker({"importer": "claims-pricing", "run_id": "run_1"})

    assert result["status"] == "started"
    assert result["items"][0]["worker_class"] == "process.ClaimsPricing"
    assert captured["cmd"][-2:] == ["process.ClaimsPricing", "--burst"]
    assert captured["start_new_session"] is True


def test_ensure_worker_uses_finish_role_for_finalizing_run(monkeypatch, tmp_path):
    class FakeProcess:
        pid = 456

    captured: dict[str, object] = {}

    monkeypatch.setenv("HLTHPRT_WORKER_STATE_DIR", str(tmp_path / "state"))
    monkeypatch.setenv("HLTHPRT_WORKER_LOG_DIR", str(tmp_path / "logs"))

    def fake_popen(cmd, **_kwargs):
        captured["cmd"] = cmd
        return FakeProcess()

    monkeypatch.setattr(control_workers.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(control_workers, "_pid_running", lambda pid: pid == FakeProcess.pid)
    monkeypatch.setattr(control_workers, "_pid_matches_spec", lambda pid, spec: True)

    result = control_workers.ensure_worker({"importer": "partd-formulary-network", "status": "finalizing"})

    assert result["status"] == "started"
    assert result["items"][0]["role"] == "finish"
    assert captured["cmd"][-2:] == ["process.PartDFormularyNetwork_finish", "--burst"]


def test_ensure_worker_can_create_kubernetes_job(monkeypatch):
    calls: list[tuple[str, str, dict[str, object] | None]] = []

    def fake_request(method, path, body=None):
        calls.append((method, path, body))
        if method == "GET" and any(item[0] == "POST" for item in calls):
            return {
                "items": [
                    {
                        "metadata": {"name": "worker-job"},
                        "status": {"active": 1},
                    }
                ]
            }
        return {"items": []}

    monkeypatch.setenv("HLTHPRT_WORKER_LAUNCHER", "kubernetes")
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_IMAGE", "ghcr.io/endurantdevs/healthcare-mrf-api:dev")
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_ENV_FROM_CONFIGMAP", "mrf-api-config")
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_ENV_FROM_SECRET", "mrf-api-secret")
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_PVC_NAME", "import-workdir")
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_PVC_MOUNT_PATH", "/work")
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_ACTIVE_DEADLINE_SECONDS", "43200")
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "local_mrf")
    monkeypatch.setattr(control_workers, "_kubernetes_configured", lambda: True)
    monkeypatch.setattr(control_workers, "_kubernetes_namespace", lambda: "healthporta-dev")
    monkeypatch.setattr(control_workers, "_kubernetes_request", fake_request)

    result = control_workers.ensure_worker(
        {"importer": "claims-pricing", "run_id": "run_123", "import_id": "import_123"}
    )

    assert result["status"] == "started"
    post = next(call for call in calls if call[0] == "POST")
    job = post[2]
    assert post[1] == "/apis/batch/v1/namespaces/healthporta-dev/jobs"
    assert job["kind"] == "Job"
    container = job["spec"]["template"]["spec"]["containers"][0]
    assert container["image"] == "ghcr.io/endurantdevs/healthcare-mrf-api:dev"
    assert container["command"][-2:] == ["process.ClaimsPricing", "--burst"]
    assert {"configMapRef": {"name": "mrf-api-config"}} in container["envFrom"]
    assert {"secretRef": {"name": "mrf-api-secret"}} in container["envFrom"]
    assert container["volumeMounts"] == [{"name": "import-workdir", "mountPath": "/work"}]
    assert job["spec"]["template"]["spec"]["volumes"] == [
        {"name": "import-workdir", "persistentVolumeClaim": {"claimName": "import-workdir"}}
    ]
    assert "parallelism" not in job["spec"]
    assert "completions" not in job["spec"]
    assert job["spec"]["activeDeadlineSeconds"] == 43200


def test_kubernetes_start_worker_replicas_use_parallel_job(monkeypatch):
    calls: list[tuple[str, str, dict[str, object] | None]] = []

    def fake_request(method, path, body=None):
        calls.append((method, path, body))
        if method == "GET" and any(item[0] == "POST" for item in calls):
            return {
                "items": [
                    {
                        "metadata": {"name": "worker-job"},
                        "status": {"active": 8},
                    }
                ]
            }
        return {"items": []}

    monkeypatch.setenv("HLTHPRT_WORKER_LAUNCHER", "kubernetes")
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_IMAGE", "ghcr.io/endurantdevs/healthcare-mrf-api:dev")
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_START_REPLICAS", "process.MRF=8")
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "local_mrf")
    monkeypatch.setattr(control_workers, "_kubernetes_configured", lambda: True)
    monkeypatch.setattr(control_workers, "_kubernetes_namespace", lambda: "healthporta-dev")
    monkeypatch.setattr(control_workers, "_kubernetes_request", fake_request)

    result = control_workers.ensure_worker({"importer": "mrf", "run_id": "run_mrf"})

    assert result["status"] == "started"
    post = next(call for call in calls if call[0] == "POST")
    job = post[2]
    assert job["spec"]["parallelism"] == 8
    assert job["spec"]["completions"] == 8
    container = job["spec"]["template"]["spec"]["containers"][0]
    assert container["command"][-2:] == ["process.MRF", "--burst"]


def test_kubernetes_start_worker_replicas_do_not_apply_to_finish(monkeypatch):
    calls: list[tuple[str, str, dict[str, object] | None]] = []

    def fake_request(method, path, body=None):
        calls.append((method, path, body))
        if method == "GET" and any(item[0] == "POST" for item in calls):
            return {
                "items": [
                    {
                        "metadata": {"name": "worker-job"},
                        "status": {"active": 1},
                    }
                ]
            }
        return {"items": []}

    monkeypatch.setenv("HLTHPRT_WORKER_LAUNCHER", "kubernetes")
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_IMAGE", "ghcr.io/endurantdevs/healthcare-mrf-api:dev")
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_START_REPLICAS", "process.MRF=8")
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "local_mrf")
    monkeypatch.setattr(control_workers, "_kubernetes_configured", lambda: True)
    monkeypatch.setattr(control_workers, "_kubernetes_namespace", lambda: "healthporta-dev")
    monkeypatch.setattr(control_workers, "_kubernetes_request", fake_request)

    result = control_workers.ensure_worker({"importer": "mrf", "run_id": "run_mrf", "status": "finalizing"})

    assert result["status"] == "started"
    post = next(call for call in calls if call[0] == "POST")
    job = post[2]
    assert "parallelism" not in job["spec"]
    assert "completions" not in job["spec"]
    container = job["spec"]["template"]["spec"]["containers"][0]
    assert container["command"][-2:] == ["process.MRF_finish", "--burst"]


def test_find_running_pid_ignores_other_node_worker(monkeypatch):
    output = """
111 /opt/python main.py worker process.PTG HLTHPRT_IMPORT_NODE_ID=mrf-local-smoke-b
222 /opt/python main.py worker process.PTG HLTHPRT_IMPORT_NODE_ID=local_mrf
"""

    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "local_mrf")
    monkeypatch.setattr(control_workers.subprocess, "check_output", lambda *_args, **_kwargs: output)

    spec = control_workers._BY_QUEUE["arq:PTG"]  # pylint: disable=protected-access

    assert control_workers._find_running_pid(spec) == 222  # pylint: disable=protected-access


def test_find_running_pid_requires_exact_worker_class(monkeypatch):
    output = """
111 /opt/python main.py worker process.ProviderQuality_finish --burst HLTHPRT_IMPORT_NODE_ID=local_mrf
222 /opt/python main.py worker process.ProviderQuality --burst HLTHPRT_IMPORT_NODE_ID=local_mrf
"""

    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "local_mrf")
    monkeypatch.setattr(control_workers.subprocess, "check_output", lambda *_args, **_kwargs: output)

    start_spec = control_workers._BY_QUEUE["arq:ProviderQuality"]  # pylint: disable=protected-access
    finish_spec = control_workers._BY_QUEUE["arq:ProviderQuality_finish"]  # pylint: disable=protected-access

    assert control_workers._find_running_pid(start_spec) == 222  # pylint: disable=protected-access
    assert control_workers._find_running_pid(finish_spec) == 111  # pylint: disable=protected-access
