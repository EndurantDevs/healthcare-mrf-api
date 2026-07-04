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
    assert by_importer["entity-address-unified"]["worker_class"] == "process.EntityAddressUnified"
    assert by_importer["provider-directory-fhir"]["worker_class"] == "process.ProviderDirectoryFHIR"
    assert by_importer["ms-drg"]["worker_class"] == "process.MSDRG"
    assert by_importer["terminology-synonyms"]["worker_class"] == "process.TerminologySynonyms"
    assert by_importer["openaddresses"]["worker_class"] == "process.OpenAddresses"
    assert by_queue["arq:OpenAddresses"]["role"] == "start"
    assert by_queue["arq:ProviderDirectoryFHIR"]["role"] == "start"
    assert by_queue["arq:PTGSmall"]["worker_class"] == "process.PTGSmall"
    assert by_queue["arq:PTGNormal"]["worker_class"] == "process.PTGNormal"
    assert by_queue["arq:PTGLarge"]["worker_class"] == "process.PTGLarge"
    assert by_queue["arq:PTGHuge"]["worker_class"] == "process.PTGHuge"
    assert "entity-address-unified" in by_queue["arq:EntityAddressUnified"]["importers"]
    assert by_queue["arq:PartDFormularyNetwork_finish"]["role"] == "finish"


def test_resolve_specs_prefers_finish_role_over_stale_start_queue():
    specs = control_workers._resolve_specs(
        {"importer": "mrf", "role": "finish", "queue": "arq:MRF"}
    )

    assert [spec.queue for spec in specs] == ["arq:MRF_finish"]


def test_resolve_specs_prefers_finalizing_status_over_stale_start_queue():
    specs = control_workers._resolve_specs(
        {"importer": "mrf", "status": "finalizing", "queue": "arq:MRF"}
    )

    assert [spec.queue for spec in specs] == ["arq:MRF_finish"]


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


def test_ensure_worker_uses_explicit_ptg_lane(monkeypatch, tmp_path):
    class FakeProcess:
        pid = 789

    captured: dict[str, object] = {}

    monkeypatch.setenv("HLTHPRT_WORKER_STATE_DIR", str(tmp_path / "state"))
    monkeypatch.setenv("HLTHPRT_WORKER_LOG_DIR", str(tmp_path / "logs"))

    def fake_popen(cmd, *, env, **_kwargs):
        captured["cmd"] = cmd
        captured["env"] = env
        return FakeProcess()

    monkeypatch.setattr(control_workers.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(control_workers, "_pid_running", lambda pid: pid == FakeProcess.pid)
    monkeypatch.setattr(control_workers, "_pid_matches_spec", lambda pid, spec: True)

    result = control_workers.ensure_worker(
        {"importer": "ptg", "queue": "arq:PTGSmall", "worker_class": "process.PTGSmall", "run_id": "run_ptg"}
    )

    assert result["status"] == "started"
    assert result["items"][0]["worker_class"] == "process.PTGSmall"
    assert captured["cmd"][-2:] == ["process.PTGSmall", "--burst"]
    assert captured["env"]["HLTHPRT_ACTIVE_WORKER_QUEUE"] == "arq:PTGSmall"
    assert captured["env"]["HLTHPRT_ACTIVE_WORKER_CLASS"] == "process.PTGSmall"


def test_ensure_worker_starts_entity_address_unified_shared_worker(monkeypatch, tmp_path):
    class FakeProcess:
        pid = 2468

    captured: dict[str, object] = {}

    monkeypatch.setenv("HLTHPRT_WORKER_STATE_DIR", str(tmp_path / "state"))
    monkeypatch.setenv("HLTHPRT_WORKER_LOG_DIR", str(tmp_path / "logs"))

    def fake_popen(cmd, *, env, **_kwargs):
        captured["cmd"] = cmd
        captured["env"] = env
        return FakeProcess()

    monkeypatch.setattr(control_workers.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(control_workers, "_pid_running", lambda pid: pid == FakeProcess.pid)
    monkeypatch.setattr(control_workers, "_pid_matches_spec", lambda pid, spec: True)

    result = control_workers.ensure_worker({"importer": "entity-address-unified", "run_id": "run_refresh"})

    assert result["status"] == "started"
    assert result["items"][0]["worker_class"] == "process.EntityAddressUnified"
    assert captured["cmd"][-2:] == ["process.EntityAddressUnified", "--burst"]
    assert captured["env"]["HLTHPRT_ACTIVE_WORKER_QUEUE"] == "arq:EntityAddressUnified"


def test_ensure_worker_rejects_mismatched_explicit_ptg_lane():
    result = control_workers.ensure_worker(
        {"importer": "ptg", "queue": "arq:PTGSmall", "worker_class": "process.PTGLarge", "run_id": "run_ptg"}
    )

    assert result["status"] == "unsupported"


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
    monkeypatch.setenv(
        "HLTHPRT_WORKER_JOB_SECRET_VOLUME_MOUNTS_JSON",
        '[{"name":"provider-directory-credentials","secretName":"provider-directory-credentials","mountPath":"/var/run/healthporta/provider-directory","optional":true}]',
    )
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
    assert container["volumeMounts"] == [
        {"name": "import-workdir", "mountPath": "/work"},
        {
            "name": "provider-directory-credentials",
            "mountPath": "/var/run/healthporta/provider-directory",
            "readOnly": True,
        },
    ]
    assert job["spec"]["template"]["spec"]["volumes"] == [
        {"name": "import-workdir", "persistentVolumeClaim": {"claimName": "import-workdir"}},
        {
            "name": "provider-directory-credentials",
            "secret": {"secretName": "provider-directory-credentials", "optional": True},
        },
    ]
    assert "parallelism" not in job["spec"]
    assert "completions" not in job["spec"]
    assert job["spec"]["activeDeadlineSeconds"] == 43200


def test_kubernetes_worker_job_uses_resource_profile(monkeypatch):
    calls: list[tuple[str, str, dict[str, object] | None]] = []

    def fake_request(method, path, body=None):
        calls.append((method, path, body))
        if method == "GET" and any(item[0] == "POST" for item in calls):
            return {"items": [{"metadata": {"name": "worker-job"}, "status": {"active": 1}}]}
        return {"items": []}

    monkeypatch.setenv("HLTHPRT_WORKER_LAUNCHER", "kubernetes")
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_IMAGE", "ghcr.io/endurantdevs/healthcare-mrf-api:dev")
    monkeypatch.setenv(
        "HLTHPRT_WORKER_JOB_RESOURCE_PROFILES_JSON",
        '{"process.PTGSmall":{"requests":{"cpu":"2","memory":"4Gi"},"limits":{"cpu":"4","memory":"8Gi"}}}',
    )
    monkeypatch.setattr(control_workers, "_kubernetes_configured", lambda: True)
    monkeypatch.setattr(control_workers, "_kubernetes_namespace", lambda: "healthporta-dev")
    monkeypatch.setattr(control_workers, "_kubernetes_request", fake_request)

    result = control_workers.ensure_worker(
        {"importer": "ptg", "queue": "arq:PTGSmall", "worker_class": "process.PTGSmall", "run_id": "run_ptg"}
    )

    assert result["status"] == "started"
    job = next(call[2] for call in calls if call[0] == "POST")
    container = job["spec"]["template"]["spec"]["containers"][0]
    assert container["resources"] == {
        "requests": {"cpu": "2", "memory": "4Gi"},
        "limits": {"cpu": "4", "memory": "8Gi"},
    }
    env = {item["name"]: item["value"] for item in container["env"]}
    assert env["HLTHPRT_ACTIVE_WORKER_QUEUE"] == "arq:PTGSmall"
    assert env["HLTHPRT_ACTIVE_WORKER_CLASS"] == "process.PTGSmall"


def test_kubernetes_start_worker_replicas_use_parallel_job(monkeypatch):
    calls: list[tuple[str, str, dict[str, object] | None]] = []

    def fake_request(method, path, body=None):
        calls.append((method, path, body))
        if method == "GET" and any(item[0] == "POST" for item in calls):
            return {
                "items": [
                    {
                        "metadata": {"name": "worker-job"},
                        "status": {"active": 16},
                    }
                ]
            }
        return {"items": []}

    monkeypatch.setenv("HLTHPRT_WORKER_LAUNCHER", "kubernetes")
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_IMAGE", "ghcr.io/endurantdevs/healthcare-mrf-api:dev")
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_START_REPLICAS", "process.MRF=16")
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "local_mrf")
    monkeypatch.setattr(control_workers, "_kubernetes_configured", lambda: True)
    monkeypatch.setattr(control_workers, "_kubernetes_namespace", lambda: "healthporta-dev")
    monkeypatch.setattr(control_workers, "_kubernetes_request", fake_request)

    result = control_workers.ensure_worker({"importer": "mrf", "run_id": "run_mrf"})

    assert result["status"] == "started"
    post = next(call for call in calls if call[0] == "POST")
    job = post[2]
    assert job["spec"]["parallelism"] == 16
    assert job["spec"]["completions"] == 16
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
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_START_REPLICAS", "process.MRF=16")
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


def test_kubernetes_completed_start_job_promotes_running_import_to_finish(monkeypatch):
    calls: list[tuple[str, str, dict[str, object] | None]] = []

    def fake_request(method, path, body=None):
        calls.append((method, path, body))
        if method == "GET":
            if any(item[0] == "POST" for item in calls):
                return {
                    "items": [
                        {
                            "metadata": {"name": "worker-job"},
                            "status": {"active": 1},
                        }
                    ]
                }
            if len([item for item in calls if item[0] == "GET"]) == 1:
                return {
                    "items": [
                        {
                            "metadata": {"name": "start-worker-job"},
                            "status": {"succeeded": 16},
                        }
                    ]
                }
            return {"items": []}
        return {}

    monkeypatch.setenv("HLTHPRT_WORKER_LAUNCHER", "kubernetes")
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_IMAGE", "ghcr.io/endurantdevs/healthcare-mrf-api:dev")
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_START_REPLICAS", "process.MRF=16")
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "local_mrf")
    monkeypatch.setattr(control_workers, "_kubernetes_configured", lambda: True)
    monkeypatch.setattr(control_workers, "_kubernetes_namespace", lambda: "healthporta-dev")
    monkeypatch.setattr(control_workers, "_kubernetes_request", fake_request)

    result = control_workers.ensure_worker({"importer": "mrf", "run_id": "run_mrf", "status": "running"})

    assert result["status"] == "started"
    post = next(call for call in calls if call[0] == "POST")
    job = post[2]
    assert "parallelism" not in job["spec"]
    assert "completions" not in job["spec"]
    container = job["spec"]["template"]["spec"]["containers"][0]
    assert container["command"][-2:] == ["process.MRF_finish", "--burst"]


def test_kubernetes_completed_worker_job_is_recreated(monkeypatch):
    calls: list[tuple[str, str, dict[str, object] | None]] = []

    def fake_request(method, path, body=None):
        calls.append((method, path, body))
        if method == "GET":
            if any(item[0] == "POST" for item in calls):
                return {"items": [{"metadata": {"name": "worker-job"}, "status": {"active": 1}}]}
            return {"items": [{"metadata": {"name": "worker-job"}, "status": {"succeeded": 1}}]}
        return {}

    monkeypatch.setenv("HLTHPRT_WORKER_LAUNCHER", "kubernetes")
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_IMAGE", "ghcr.io/endurantdevs/healthcare-mrf-api:dev")
    monkeypatch.setattr(control_workers, "_kubernetes_configured", lambda: True)
    monkeypatch.setattr(control_workers, "_kubernetes_namespace", lambda: "healthporta-dev")
    monkeypatch.setattr(control_workers, "_kubernetes_request", fake_request)

    result = control_workers.ensure_worker(
        {"importer": "claims-procedures", "run_id": "run_claims", "status": "finalizing"}
    )

    assert result["status"] == "started"
    assert any(call[0] == "DELETE" and call[1].endswith("/jobs/worker-job") for call in calls)
    assert any(call[0] == "POST" and call[1] == "/apis/batch/v1/namespaces/healthporta-dev/jobs" for call in calls)


def test_delete_kubernetes_worker_jobs_deletes_active_matching_run(monkeypatch):
    request_calls: list[tuple[str, str, dict[str, object] | None]] = []

    def fake_request(method, path, body=None):
        request_calls.append((method, path, body))
        if method == "GET":
            return {
                "items": [
                    {"metadata": {"name": "active-job"}, "status": {"active": 1}},
                    {"metadata": {"name": "done-job"}, "status": {"succeeded": 1}},
                ]
            }
        return {}

    monkeypatch.setenv("HLTHPRT_WORKER_LAUNCHER", "kubernetes")
    monkeypatch.setattr(control_workers, "_kubernetes_configured", lambda: True)
    monkeypatch.setattr(control_workers, "_kubernetes_namespace", lambda: "healthporta-dev")
    monkeypatch.setattr(control_workers, "_kubernetes_request", fake_request)

    delete_response = control_workers.delete_kubernetes_worker_jobs(
        {"importer": "ptg", "queue": "arq:PTGLarge", "worker_class": "process.PTGLarge", "run_id": "run_ptg"}
    )

    assert delete_response["deleted"] == 1
    assert delete_response["items"] == [
        {"job_name": "active-job", "worker_class": "process.PTGLarge", "deleted": True},
        {"job_name": "done-job", "worker_class": "process.PTGLarge", "deleted": False, "reason": "terminal"},
    ]
    get_call = request_calls[0]
    assert get_call[0] == "GET"
    assert "healthporta.com%2Frun-id-hash%3D" in get_call[1]
    assert "healthporta.com%2Fworker-class-hash%3D" in get_call[1]
    assert any(call[0] == "DELETE" and call[1].endswith("/jobs/active-job") for call in request_calls)
    assert not any(call[0] == "DELETE" and call[1].endswith("/jobs/done-job") for call in request_calls)


def test_find_running_pid_ignores_other_node_worker(monkeypatch):
    output = """
111 /opt/python main.py worker process.PTG HLTHPRT_IMPORT_NODE_ID=mrf-local-smoke-b
222 /opt/python main.py worker process.PTG HLTHPRT_IMPORT_NODE_ID=local_mrf
"""

    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "local_mrf")
    monkeypatch.setattr(control_workers.subprocess, "check_output", lambda *_args, **_kwargs: output)

    spec = control_workers._BY_QUEUE["arq:PTG"]

    assert control_workers._find_running_pid(spec) == 222


def test_find_running_pid_requires_exact_worker_class(monkeypatch):
    output = """
111 /opt/python main.py worker process.ProviderQuality_finish --burst HLTHPRT_IMPORT_NODE_ID=local_mrf
222 /opt/python main.py worker process.ProviderQuality --burst HLTHPRT_IMPORT_NODE_ID=local_mrf
"""

    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "local_mrf")
    monkeypatch.setattr(control_workers.subprocess, "check_output", lambda *_args, **_kwargs: output)

    start_spec = control_workers._BY_QUEUE["arq:ProviderQuality"]
    finish_spec = control_workers._BY_QUEUE["arq:ProviderQuality_finish"]

    assert control_workers._find_running_pid(start_spec) == 222
    assert control_workers._find_running_pid(finish_spec) == 111
