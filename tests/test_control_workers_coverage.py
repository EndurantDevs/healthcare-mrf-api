from __future__ import annotations

import asyncio
import importlib
from unittest.mock import AsyncMock

import pytest

from api import control_workers
from process.ptg_parts import ptg2_partitioned_candidate_audit as partitioned_audit
from process.ptg_parts.ptg2_partitioned_candidate_audit_report import (
    PartitionedAuditHttpMetrics,
)

ptg_candidate_audit = importlib.import_module("process.ptg_candidate_audit")


def _spec() -> control_workers.WorkerSpec:
    return control_workers.WorkerSpec(
        "arq:Example",
        "process.Example",
        ("example",),
    )


def test_process_start_and_pid_file_errors_are_reported(monkeypatch, tmp_path):
    spec = _spec()
    worker_state_map = {"running": False, "worker_class": spec.worker_class}
    monkeypatch.setenv("HLTHPRT_WORKER_LAUNCHER", "process")
    monkeypatch.setattr(
        control_workers,
        "_worker_state",
        lambda *_args: worker_state_map,
    )
    monkeypatch.setattr(
        control_workers,
        "_start_process",
        lambda *_args: (_ for _ in ()).throw(RuntimeError("start failed")),
    )

    assert control_workers._ensure_spec(spec, {}) == {
        **worker_state_map,
        "status": "failed",
        "message": "start failed",
    }

    monkeypatch.setenv("HLTHPRT_WORKER_STATE_DIR", str(tmp_path))
    control_workers._write_pid_file(spec, 41)
    assert control_workers._pid_path(spec).read_text(encoding="utf-8") == "41"

    control_workers._remove_stale_pid(spec)
    assert not control_workers._pid_path(spec).exists()
    control_workers._remove_stale_pid(spec)

    class UnwritablePath:
        def mkdir(self, **_kwargs):
            raise OSError("read only")

    monkeypatch.setattr(control_workers, "_state_dir", UnwritablePath)
    control_workers._write_pid_file(spec, 42)


def test_terminal_job_cleanup_skips_ineligible_jobs(monkeypatch):
    spec = _spec()
    job_records = [
        {"metadata": {}, "status": {"succeeded": 1}},
        {"metadata": {"name": "active"}, "status": {"active": 1}},
        {"metadata": {"name": "pending"}, "status": {}},
        {"metadata": {"name": "gone"}, "status": {"succeeded": 1}},
    ]
    monkeypatch.setattr(
        control_workers,
        "_kubernetes_request",
        lambda *_args, **_kwargs: {"items": job_records},
    )

    def delete_job(_namespace, job_name):
        if job_name == "gone":
            raise control_workers._KubernetesApiError(404, "gone")
        if job_name == "fatal":
            raise control_workers._KubernetesApiError(503, "unavailable")

    monkeypatch.setattr(control_workers, "_delete_kubernetes_job", delete_job)
    control_workers._delete_terminal_kubernetes_worker_jobs(
        "namespace-test",
        spec,
        {},
    )

    job_records[:] = [
        {"metadata": {"name": "fatal"}, "status": {"failed": 1}},
    ]
    with pytest.raises(control_workers._KubernetesApiError, match="unavailable"):
        control_workers._delete_terminal_kubernetes_worker_jobs(
            "namespace-test",
            spec,
            {},
        )


def test_delete_worker_jobs_reports_disabled_and_error_states(monkeypatch):
    spec = _spec()
    monkeypatch.setenv("HLTHPRT_WORKER_LAUNCHER", "kubernetes")
    monkeypatch.setattr(control_workers, "_resolve_specs", lambda _payload: [])

    assert control_workers.delete_kubernetes_worker_jobs({}) == {
        "enabled": True,
        "deleted": 0,
        "reason": "no matching worker spec",
    }

    monkeypatch.setattr(control_workers, "_resolve_specs", lambda _payload: [spec])
    monkeypatch.setattr(
        control_workers,
        "_kubernetes_namespace",
        lambda: "namespace-test",
    )
    monkeypatch.setattr(control_workers, "_is_kubernetes_configured", lambda: False)
    assert control_workers.delete_kubernetes_worker_jobs({})["error"] == (
        "kubernetes worker launcher is not configured"
    )

    monkeypatch.setattr(control_workers, "_is_kubernetes_configured", lambda: True)
    monkeypatch.setattr(
        control_workers,
        "_delete_kubernetes_worker_jobs_for_spec",
        lambda *_args: (
            [{"job_name": "kept", "deleted": False}],
            [{"job_name": "failed", "error": "unavailable"}],
        ),
    )
    summary = control_workers.delete_kubernetes_worker_jobs({})
    assert summary["deleted"] == 0
    assert summary["errors"] == [
        {"job_name": "failed", "error": "unavailable"},
    ]


def test_delete_jobs_for_spec_preserves_query_and_record_errors(monkeypatch):
    spec = _spec()

    def fail_query(*_args, **_kwargs):
        raise control_workers._KubernetesApiError(503, "query failed")

    monkeypatch.setattr(control_workers, "_kubernetes_request", fail_query)
    deletion_record_list, deletion_error_list = (
        control_workers._delete_kubernetes_worker_jobs_for_spec(
            "namespace-test",
            spec,
            {},
        )
    )
    assert deletion_record_list == []
    assert deletion_error_list[0]["status"] == 503

    monkeypatch.setattr(
        control_workers,
        "_kubernetes_request",
        lambda *_args, **_kwargs: {"items": [{}]},
    )
    monkeypatch.setattr(
        control_workers,
        "_delete_kubernetes_worker_job_record",
        lambda *_args: (None, {"job_name": "failed", "error": "delete failed"}),
    )
    deletion_record_list, deletion_error_list = (
        control_workers._delete_kubernetes_worker_jobs_for_spec(
            "namespace-test",
            spec,
            {},
        )
    )
    assert deletion_record_list == []
    assert deletion_error_list == [
        {"job_name": "failed", "error": "delete failed"},
    ]


def test_kubernetes_worker_state_handles_configuration_edges(monkeypatch):
    spec = _spec()
    monkeypatch.setattr(control_workers, "_is_kubernetes_configured", lambda: False)
    assert control_workers._kubernetes_worker_state(spec)["job_status"] == (
        "unconfigured"
    )

    monkeypatch.setattr(control_workers, "_is_kubernetes_configured", lambda: True)
    monkeypatch.setattr(
        control_workers,
        "_kubernetes_namespace",
        lambda: "namespace-test",
    )

    def fail_request(*_args, **_kwargs):
        raise control_workers._KubernetesApiError(503, "lookup failed")

    monkeypatch.setattr(control_workers, "_kubernetes_request", fail_request)
    error_state = control_workers._kubernetes_worker_state(spec)
    assert error_state["job_status"] == "error"
    assert error_state["message"] == "lookup failed"

    monkeypatch.setattr(
        control_workers,
        "_kubernetes_request",
        lambda *_args, **_kwargs: {
            "items": [
                {
                    "metadata": {"name": "failed-job"},
                    "status": {"failed": 1},
                }
            ]
        },
    )
    monkeypatch.setattr(
        control_workers,
        "_kubernetes_worker_failure",
        lambda *_args: None,
    )
    failed_state = control_workers._kubernetes_worker_state(spec)
    assert failed_state["job_status"] == "failed"
    assert "failure" not in failed_state


def test_worker_failure_lookup_handles_error_and_empty_result(monkeypatch):
    """Keep lookup transport failures distinct from a valid empty pod list."""

    def fail_request(*_args, **_kwargs):
        raise control_workers._KubernetesApiError(503, "pod lookup failed")

    monkeypatch.setattr(control_workers, "_kubernetes_request", fail_request)
    assert control_workers._kubernetes_worker_failure(
        "namespace-test",
        "selector=test",
    ) == {
        "lookup_status": "error",
        "message": "pod lookup failed",
    }

    monkeypatch.setattr(
        control_workers,
        "_kubernetes_request",
        lambda *_args, **_kwargs: {"items": []},
    )
    assert (
        control_workers._kubernetes_worker_failure(
            "namespace-test",
            "selector=test",
        )
        is None
    )


def test_pod_failure_helpers_skip_non_failures():
    """Ignore absent, malformed, and successful container termination states."""

    assert control_workers._pod_container_failure({}) is None
    assert (
        control_workers._pod_container_failure(
            {
                "status": {
                    "containerStatuses": [
                        {"state": "invalid", "lastState": "invalid"},
                    ]
                }
            }
        )
        is None
    )
    assert (
        control_workers._pod_container_failure(
            {
                "status": {
                    "containerStatuses": [
                        {
                            "state": {
                                "terminated": {
                                    "reason": "Completed",
                                    "exitCode": 0,
                                }
                            }
                        }
                    ]
                }
            }
        )
        is None
    )
    assert (
        control_workers._container_termination(
            {"state": {}, "lastState": {}},
        )
        is None
    )


def test_worker_manifest_sets_pod_identity_options(monkeypatch):
    spec = _spec()
    monkeypatch.setenv(
        "HLTHPRT_WORKER_JOB_SERVICE_ACCOUNT",
        "worker-service-account",
    )
    monkeypatch.setenv(
        "HLTHPRT_WORKER_JOB_IMAGE_PULL_SECRET",
        "secret-one, secret-two",
    )
    monkeypatch.delenv("HLTHPRT_WORKER_JOB_ACTIVE_DEADLINE_SECONDS", raising=False)
    monkeypatch.setattr(control_workers, "_worker_job_secret_env", lambda: [])
    monkeypatch.setattr(control_workers, "_worker_job_env_from", lambda: [])
    monkeypatch.setattr(
        control_workers,
        "_worker_job_resources",
        lambda *_args: {},
    )
    monkeypatch.setattr(control_workers, "_worker_job_pvc_volumes", lambda: [])
    monkeypatch.setattr(control_workers, "_worker_job_secret_volumes", lambda: [])
    monkeypatch.setattr(
        control_workers,
        "_worker_job_container_security_context",
        lambda: {},
    )
    monkeypatch.setattr(
        control_workers,
        "_worker_job_pod_security_context",
        lambda *, has_pvc: {"hasPvc": has_pvc},
    )
    monkeypatch.setattr(control_workers, "_worker_start_replicas", lambda _spec: 1)

    manifest = control_workers._worker_job_manifest(
        spec,
        {},
        "example.invalid/worker:test",
    )
    pod_spec = manifest["spec"]["template"]["spec"]
    assert pod_spec["serviceAccountName"] == "worker-service-account"
    assert pod_spec["imagePullSecrets"] == [
        {"name": "secret-one"},
        {"name": "secret-two"},
    ]


def test_resource_profile_skips_empty_normalized_section():
    assert control_workers._normalize_resource_profile(
        {
            "requests": {"unsupported": "ignored"},
            "limits": {"memory": "2Gi"},
        }
    ) == {"limits": {"memory": "2Gi"}}


def test_namespace_and_kubernetes_configuration_edges(monkeypatch, tmp_path):
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_NAMESPACE", "namespace-override")
    assert control_workers._kubernetes_namespace() == "namespace-override"

    namespace_file = tmp_path / "namespace"
    namespace_file.write_text("namespace-file\n", encoding="utf-8")
    monkeypatch.delenv("HLTHPRT_WORKER_JOB_NAMESPACE")
    monkeypatch.setattr(control_workers, "_K8S_API_NAMESPACE", namespace_file)
    assert control_workers._kubernetes_namespace() == "namespace-file"

    monkeypatch.setattr(
        control_workers,
        "_K8S_API_NAMESPACE",
        tmp_path / "missing-namespace",
    )
    assert control_workers._kubernetes_namespace() == "default"

    token_file = tmp_path / "token"
    token_file.write_text("token", encoding="utf-8")
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "kubernetes.invalid")
    monkeypatch.setattr(control_workers, "_K8S_API_TOKEN", token_file)
    assert control_workers._is_kubernetes_configured() is True


@pytest.mark.asyncio
async def test_partition_progress_without_run_is_a_no_op():
    await ptg_candidate_audit._partition_progress(
        None,
        snapshot_id=None,
        completed=1,
        total=1,
    )


@pytest.mark.asyncio
async def test_partition_result_collection_skips_repeated_counter():
    metrics = PartitionedAuditHttpMetrics(planned_request_count=1)

    async def complete_without_metric():
        await asyncio.sleep(0)
        return "result"

    callback = AsyncMock()
    results = await partitioned_audit._collect_partition_results(
        tasks=[asyncio.create_task(complete_without_metric())],
        metrics=metrics,
        progress_callback=callback,
        total=1,
    )

    assert results == ["result"]
    callback.assert_awaited_once_with(0, 1)
