from __future__ import annotations

from api import control_workers


def test_ensure_worker_rejects_mismatched_explicit_ptg_lane():
    result = control_workers.ensure_worker(
        {"importer": "ptg", "queue": "arq:PTGSmall", "worker_class": "process.PTGLarge", "run_id": "run_ptg"}
    )

    assert result["status"] == "unsupported"
    assert result["contract_id"] == (
        control_workers.WORKER_ENSURE_RUN_IDENTITY_CONTRACT
    )
    assert result["run_id"] == "run_ptg"
    assert result["items"] == []


def test_ensure_worker_without_run_id_does_not_fabricate_run_identity(
    monkeypatch,
):
    monkeypatch.setattr(control_workers, "_resolve_specs", lambda _payload: [])

    result = control_workers.ensure_worker({"importer": "unknown"})

    assert result == {
        "status": "unsupported",
        "items": [],
        "message": "no worker is registered for unknown",
    }


def _configure_kubernetes_worker_environment(monkeypatch) -> None:
    """Configure the complete Kubernetes worker-job environment contract."""
    monkeypatch.setenv("HLTHPRT_WORKER_LAUNCHER", "kubernetes")
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_IMAGE", "ghcr.io/endurantdevs/healthcare-mrf-api:dev")
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_ENV_FROM_CONFIGMAP", "mrf-api-config")
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_ENV_FROM_SECRET", "mrf-api-secret")
    monkeypatch.setenv(
        "HLTHPRT_WORKER_JOB_SECRET_ENV_JSON",
        '[{"name":"EXAMPLE_STATUS_TOKEN","secretName":"runtime-secret","key":"status-token"}]',
    )
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_PVC_NAME", "import-workdir")
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_PVC_MOUNT_PATH", "/work")
    monkeypatch.setenv(
        "HLTHPRT_WORKER_JOB_SECRET_VOLUME_MOUNTS_JSON",
        '[{"name":"provider-directory-credentials","secretName":"provider-directory-credentials","mountPath":"/var/run/healthporta/provider-directory","optional":true}]',
    )
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_ACTIVE_DEADLINE_SECONDS", "43200")
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "local_mrf")


def _assert_kubernetes_worker_job(job: dict[str, object]) -> None:
    """Verify the generated job preserves secret and volume contracts."""
    assert job["kind"] == "Job"
    container = job["spec"]["template"]["spec"]["containers"][0]
    assert container["image"] == "ghcr.io/endurantdevs/healthcare-mrf-api:dev"
    assert {"name": "HLTHPRT_CONTROL_RUN_ID", "value": "run_123"} in container["env"]
    assert {
        "name": "EXAMPLE_STATUS_TOKEN",
        "valueFrom": {
            "secretKeyRef": {
                "name": "runtime-secret",
                "key": "status-token",
            }
        },
    } in container["env"]
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


def test_ensure_worker_can_create_kubernetes_job(monkeypatch):
    """Verify ensure worker can create kubernetes job."""
    calls: list[tuple[str, str, dict[str, object] | None]] = []

    def fake_request(method, path, body=None):
        calls.append((method, path, body))
        if method == "GET" and any(item[0] == "POST" for item in calls):
            return {"items": [{"metadata": {"name": "worker-job"}, "status": {"active": 1}}]}
        return {"items": []}

    _configure_kubernetes_worker_environment(monkeypatch)
    monkeypatch.setattr(control_workers, "_is_kubernetes_configured", lambda: True)
    monkeypatch.setattr(control_workers, "_kubernetes_namespace", lambda: "healthporta-dev")
    monkeypatch.setattr(control_workers, "_kubernetes_request", fake_request)

    worker_response = control_workers.ensure_worker(
        {"importer": "claims-pricing", "run_id": "run_123", "import_id": "import_123"}
    )

    assert worker_response["status"] == "started"
    assert worker_response["contract_id"] == (
        control_workers.WORKER_ENSURE_RUN_IDENTITY_CONTRACT
    )
    assert worker_response["run_id"] == "run_123"
    assert worker_response["items"][0]["run_id"] == "run_123"
    post = next(call for call in calls if call[0] == "POST")
    job = post[2]
    assert post[1] == "/apis/batch/v1/namespaces/healthporta-dev/jobs"
    assert job is not None
    _assert_kubernetes_worker_job(job)


def test_worker_secret_env_rejects_invalid_specs_and_supports_optional_keys(
    monkeypatch,
):
    monkeypatch.delenv("HLTHPRT_WORKER_JOB_SECRET_ENV_JSON", raising=False)
    assert control_workers._worker_job_secret_env() == []

    monkeypatch.setenv("HLTHPRT_WORKER_JOB_SECRET_ENV_JSON", "not-json")
    assert control_workers._worker_job_secret_env() == []

    monkeypatch.setenv("HLTHPRT_WORKER_JOB_SECRET_ENV_JSON", "{}")
    assert control_workers._worker_job_secret_env() == []

    monkeypatch.setenv(
        "HLTHPRT_WORKER_JOB_SECRET_ENV_JSON",
        (
            '[null,{},{"name":"MISSING_KEY","secretName":"runtime-secret"},'
            '{"name":"OPTIONAL_TOKEN","secret_name":"runtime-secret",'
            '"key":"optional-token","optional":true}]'
        ),
    )
    assert control_workers._worker_job_secret_env() == [
        {
            "name": "OPTIONAL_TOKEN",
            "valueFrom": {
                "secretKeyRef": {
                    "name": "runtime-secret",
                    "key": "optional-token",
                    "optional": True,
                }
            },
        }
    ]


def test_ensure_kubernetes_job_requires_worker_image(monkeypatch):
    monkeypatch.delenv("HLTHPRT_WORKER_JOB_IMAGE", raising=False)
    spec = control_workers._BY_QUEUE["arq:ClaimsPricing"]

    result = control_workers._ensure_kubernetes_job(
        spec,
        {"run_id": "run_without_image"},
        {"running": False},
    )

    assert result == {
        "running": False,
        "status": "failed",
        "message": "HLTHPRT_WORKER_JOB_IMAGE is not configured",
    }


def test_ensure_kubernetes_job_fails_closed_on_terminal_cleanup_error(
    monkeypatch,
):
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_IMAGE", "healthcare-mrf-api:test")
    monkeypatch.setattr(
        control_workers,
        "_kubernetes_namespace",
        lambda: "healthporta-dev",
    )

    def fail_cleanup(*_args):
        raise control_workers._KubernetesApiError(500, "cleanup failed")

    monkeypatch.setattr(
        control_workers,
        "_delete_terminal_kubernetes_worker_jobs",
        fail_cleanup,
    )
    spec = control_workers._BY_QUEUE["arq:ClaimsPricing"]

    result = control_workers._ensure_kubernetes_job(
        spec,
        {"run_id": "run_cleanup_failure"},
        {"running": False, "job_status": "failed"},
    )

    assert result["status"] == "failed"
    assert result["message"] == "cleanup failed"


def test_ensure_kubernetes_job_tolerates_missing_terminal_job(monkeypatch):
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_IMAGE", "healthcare-mrf-api:test")
    monkeypatch.setattr(
        control_workers,
        "_kubernetes_namespace",
        lambda: "healthporta-dev",
    )

    def missing_cleanup(*_args):
        raise control_workers._KubernetesApiError(404, "already deleted")

    monkeypatch.setattr(
        control_workers,
        "_delete_terminal_kubernetes_worker_jobs",
        missing_cleanup,
    )
    monkeypatch.setattr(
        control_workers,
        "_kubernetes_request",
        lambda *_args: {},
    )
    monkeypatch.setattr(
        control_workers,
        "_worker_state",
        lambda *_args: {"running": True, "job_status": "active"},
    )
    spec = control_workers._BY_QUEUE["arq:ClaimsPricing"]

    ensure_response = control_workers._ensure_kubernetes_job(
        spec,
        {"run_id": "run_missing_terminal"},
        {"running": False, "job_status": "succeeded"},
    )

    assert ensure_response["status"] == "started"
    assert ensure_response["running"] is True


def test_ensure_kubernetes_job_reports_post_conflict(monkeypatch):
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_IMAGE", "healthcare-mrf-api:test")
    monkeypatch.setattr(
        control_workers,
        "_kubernetes_namespace",
        lambda: "healthporta-dev",
    )
    monkeypatch.setattr(
        control_workers,
        "_kubernetes_request",
        lambda *_args: (_ for _ in ()).throw(
            control_workers._KubernetesApiError(409, "already exists")
        ),
    )
    monkeypatch.setattr(
        control_workers,
        "_worker_state",
        lambda *_args: {"running": False, "job_status": "pending"},
    )
    spec = control_workers._BY_QUEUE["arq:ClaimsPricing"]

    result = control_workers._ensure_kubernetes_job(
        spec,
        {"run_id": "run_conflict"},
        {"running": False},
    )

    assert result["status"] == "exists"
    assert result["running"] is False


def test_ensure_kubernetes_job_reports_post_failure(monkeypatch):
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_IMAGE", "healthcare-mrf-api:test")
    monkeypatch.setattr(
        control_workers,
        "_kubernetes_namespace",
        lambda: "healthporta-dev",
    )
    monkeypatch.setattr(
        control_workers,
        "_kubernetes_request",
        lambda *_args: (_ for _ in ()).throw(
            control_workers._KubernetesApiError(503, "api unavailable")
        ),
    )
    spec = control_workers._BY_QUEUE["arq:ClaimsPricing"]

    result = control_workers._ensure_kubernetes_job(
        spec,
        {"run_id": "run_post_failure"},
        {"running": False},
    )

    assert result["status"] == "failed"
    assert result["message"] == "api unavailable"


def test_provider_directory_kubernetes_job_has_six_day_deadline_floor(
    monkeypatch,
):
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_ACTIVE_DEADLINE_SECONDS", "259200")
    provider_spec = next(
        spec
        for spec in control_workers._START_WORKERS
        if spec.worker_class == "process.ProviderDirectoryFHIR"
    )

    job = control_workers._worker_job_manifest(
        provider_spec,
        {"run_id": "run-provider-directory"},
        "healthcare-mrf-api:test",
    )

    assert job["spec"]["activeDeadlineSeconds"] == 518400

