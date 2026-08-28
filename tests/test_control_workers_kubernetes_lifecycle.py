from __future__ import annotations

from api import control_workers


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
    monkeypatch.setattr(control_workers, "_is_kubernetes_configured", lambda: True)
    monkeypatch.setattr(control_workers, "_kubernetes_namespace", lambda: "healthporta-dev")
    monkeypatch.setattr(control_workers, "_kubernetes_request", fake_request)

    worker_response = control_workers.ensure_worker(
        {"importer": "ptg", "queue": "arq:PTGSmall", "worker_class": "process.PTGSmall", "run_id": "run_ptg"}
    )

    assert worker_response["status"] == "started"
    job = next(call[2] for call in calls if call[0] == "POST")
    container = job["spec"]["template"]["spec"]["containers"][0]
    assert container["resources"] == {
        "requests": {"cpu": "2", "memory": "4Gi"},
        "limits": {"cpu": "4", "memory": "8Gi"},
    }
    assert container["command"][-2:] == ["worker-once", "process.PTGSmall"]
    env_by_name = {
        environment_entry["name"]: environment_entry["value"]
        for environment_entry in container["env"]
    }
    assert env_by_name["HLTHPRT_ACTIVE_WORKER_QUEUE"] == "arq:PTGSmall"
    assert env_by_name["HLTHPRT_ACTIVE_WORKER_CLASS"] == "process.PTGSmall"
    assert env_by_name["HLTHPRT_WORKER_ONCE_TARGET_JOB_ID"] == "ptg_start_run_ptg"


def test_kubernetes_hospital_worker_targets_exact_job(monkeypatch):
    monkeypatch.setenv(
        "HLTHPRT_HOSPITAL_PRICE_WORKER_JOB_PRIORITY_CLASS",
        "ci-nonpreempting",
    )
    spec = control_workers._BY_QUEUE["arq:HospitalPrices"]
    job = control_workers._worker_job_manifest(
        spec,
        {
            "run_id": "run_hospital",
            "job_id": "hospital_prices_start_run_hospital",
        },
        "healthcare-mrf-api:test",
    )
    container = job["spec"]["template"]["spec"]["containers"][0]
    assert job["spec"]["template"]["spec"]["priorityClassName"] == (
        "ci-nonpreempting"
    )
    env_by_name = {entry["name"]: entry["value"] for entry in container["env"]}

    assert container["command"][-2:] == ["worker-once", "process.HospitalPrices"]
    assert env_by_name["HLTHPRT_WORKER_ONCE_TARGET_JOB_ID"] == (
        "hospital_prices_start_run_hospital"
    )


def test_kubernetes_worker_job_sets_finalizer_identity_capacity_only_for_ptg_huge():
    capacity_env_by_worker = {}

    for queue in (
        "arq:PTGSmall",
        "arq:PTG",
        "arq:PTGNormal",
        "arq:PTGLarge",
        "arq:PTGHuge",
    ):
        spec = control_workers._BY_QUEUE[queue]
        job = control_workers._worker_job_manifest(
            spec,
            {"run_id": f"run_{queue.removeprefix('arq:').lower()}"},
            "healthcare-mrf-api:test",
        )
        container = job["spec"]["template"]["spec"]["containers"][0]
        capacity_env_by_worker[spec.worker_class] = [
            entry
            for entry in container["env"]
            if entry["name"] == "HLTHPRT_PTG2_V3_FINALIZER_IDENTITY_MAP_MAX_BYTES"
        ]

    assert capacity_env_by_worker == {
        "process.PTGSmall": [],
        "process.PTG": [],
        "process.PTGNormal": [],
        "process.PTGLarge": [],
        "process.PTGHuge": [
            {
                "name": "HLTHPRT_PTG2_V3_FINALIZER_IDENTITY_MAP_MAX_BYTES",
                "value": "68719476736",
            }
        ],
    }


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
    monkeypatch.setattr(control_workers, "_is_kubernetes_configured", lambda: True)
    monkeypatch.setattr(control_workers, "_kubernetes_namespace", lambda: "healthporta-dev")
    monkeypatch.setattr(control_workers, "_kubernetes_request", fake_request)

    worker_response = control_workers.ensure_worker(
        {"importer": "mrf", "run_id": "run_mrf"}
    )

    assert worker_response["status"] == "started"
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
    monkeypatch.setattr(control_workers, "_is_kubernetes_configured", lambda: True)
    monkeypatch.setattr(control_workers, "_kubernetes_namespace", lambda: "healthporta-dev")
    monkeypatch.setattr(control_workers, "_kubernetes_request", fake_request)

    worker_response = control_workers.ensure_worker(
        {"importer": "mrf", "run_id": "run_mrf", "status": "finalizing"}
    )

    assert worker_response["status"] == "started"
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
    monkeypatch.setattr(control_workers, "_is_kubernetes_configured", lambda: True)
    monkeypatch.setattr(control_workers, "_kubernetes_namespace", lambda: "healthporta-dev")
    monkeypatch.setattr(control_workers, "_kubernetes_request", fake_request)

    worker_response = control_workers.ensure_worker(
        {"importer": "mrf", "run_id": "run_mrf", "status": "running"}
    )

    assert worker_response["status"] == "started"
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
    monkeypatch.setattr(control_workers, "_is_kubernetes_configured", lambda: True)
    monkeypatch.setattr(control_workers, "_kubernetes_namespace", lambda: "healthporta-dev")
    monkeypatch.setattr(control_workers, "_kubernetes_request", fake_request)

    result = control_workers.ensure_worker(
        {"importer": "claims-procedures", "run_id": "run_claims", "status": "finalizing"}
    )

    assert result["status"] == "started"
    assert any(call[0] == "DELETE" and call[1].endswith("/jobs/worker-job") for call in calls)
    assert any(call[0] == "POST" and call[1] == "/apis/batch/v1/namespaces/healthporta-dev/jobs" for call in calls)


def test_kubernetes_completed_worker_jobs_are_all_removed_before_recreate(monkeypatch):
    calls: list[tuple[str, str, dict[str, object] | None]] = []

    def fake_request(method, path, body=None):
        calls.append((method, path, body))
        if method == "GET":
            if any(item[0] == "POST" for item in calls):
                return {"items": [{"metadata": {"name": "worker-job-new"}, "status": {"active": 1}}]}
            return {
                "items": [
                    {"metadata": {"name": "worker-job-old-a"}, "status": {"succeeded": 1}},
                    {"metadata": {"name": "worker-job-old-b"}, "status": {"succeeded": 1}},
                ]
            }
        return {}

    monkeypatch.setenv("HLTHPRT_WORKER_LAUNCHER", "kubernetes")
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_IMAGE", "ghcr.io/endurantdevs/healthcare-mrf-api:dev")
    monkeypatch.setattr(control_workers, "_is_kubernetes_configured", lambda: True)
    monkeypatch.setattr(control_workers, "_kubernetes_namespace", lambda: "healthporta-dev")
    monkeypatch.setattr(control_workers, "_kubernetes_request", fake_request)

    result = control_workers.ensure_worker({"worker_class": "process.PTGNormal"})

    assert result["status"] == "started"
    assert any(call[0] == "DELETE" and call[1].endswith("/jobs/worker-job-old-a") for call in calls)
    assert any(call[0] == "DELETE" and call[1].endswith("/jobs/worker-job-old-b") for call in calls)
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
    monkeypatch.setattr(control_workers, "_is_kubernetes_configured", lambda: True)
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
    output = "111 /opt/python main.py worker process.PTG HLTHPRT_IMPORT_NODE_ID=mrf-local-smoke-b\n222 /opt/python main.py worker process.PTG HLTHPRT_IMPORT_NODE_ID=local_mrf"
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "local_mrf")
    monkeypatch.setattr(control_workers.subprocess, "check_output", lambda *_args, **_kwargs: output)
    assert control_workers._find_running_pid(control_workers._BY_QUEUE["arq:PTG"]) == 222


def test_find_running_pid_requires_exact_worker_class(monkeypatch):
    output = "111 /opt/python main.py worker process.ProviderQuality_finish --burst HLTHPRT_IMPORT_NODE_ID=local_mrf\n222 /opt/python main.py worker process.ProviderQuality --burst HLTHPRT_IMPORT_NODE_ID=local_mrf"
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "local_mrf")
    monkeypatch.setattr(control_workers.subprocess, "check_output", lambda *_args, **_kwargs: output)
    assert control_workers._find_running_pid(control_workers._BY_QUEUE["arq:ProviderQuality"]) == 222
    assert control_workers._find_running_pid(control_workers._BY_QUEUE["arq:ProviderQuality_finish"]) == 111


def test_find_running_pid_matches_ptg_worker_once(monkeypatch):
    output = "111 /opt/python main.py worker-once process.PTGSmall HLTHPRT_IMPORT_NODE_ID=local_mrf\n222 /opt/python main.py worker process.PTGNormal --burst HLTHPRT_IMPORT_NODE_ID=local_mrf"
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "local_mrf")
    monkeypatch.setattr(control_workers.subprocess, "check_output", lambda *_args, **_kwargs: output)
    assert control_workers._find_running_pid(control_workers._BY_QUEUE["arq:PTGSmall"]) == 111
    assert control_workers._find_running_pid(control_workers._BY_QUEUE["arq:PTGNormal"]) == 222
