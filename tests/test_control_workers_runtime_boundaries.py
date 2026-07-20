# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import io
import json
import os
import urllib.error

import pytest

from api import control_workers


class _FakeHttpResponse:
    def __init__(self, payload: bytes):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self) -> bytes:
        return self.payload


def test_kubernetes_request_rejects_missing_host_and_token(monkeypatch, tmp_path):
    monkeypatch.delenv("KUBERNETES_SERVICE_HOST", raising=False)
    with pytest.raises(control_workers._KubernetesApiError) as host_error:
        control_workers._kubernetes_request("GET", "/version")
    assert host_error.value.status == 0

    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "kubernetes.internal")
    monkeypatch.setattr(control_workers, "_K8S_API_TOKEN", tmp_path / "missing-token")
    with pytest.raises(control_workers._KubernetesApiError, match="cannot read") as token_error:
        control_workers._kubernetes_request("GET", "/version")
    assert token_error.value.status == 0


@pytest.mark.parametrize(
    ("response_bytes", "body", "with_ca", "expected"),
    [
        (b'{"version":"v1"}', {"probe": True}, True, {"version": "v1"}),
        (b"", None, False, {}),
    ],
)
def test_kubernetes_request_success_variants(
    monkeypatch,
    tmp_path,
    response_bytes,
    body,
    with_ca,
    expected,
):
    token_path = tmp_path / "token"
    token_path.write_text("secret-token", encoding="utf-8")
    ca_path = tmp_path / "ca.crt"
    if with_ca:
        ca_path.write_text("test-ca", encoding="utf-8")
    contexts: list[str | None] = []
    requests = []

    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "kubernetes.internal")
    monkeypatch.setenv("KUBERNETES_SERVICE_PORT", "6443")
    monkeypatch.setattr(control_workers, "_K8S_API_TOKEN", token_path)
    monkeypatch.setattr(control_workers, "_K8S_API_CA", ca_path)
    monkeypatch.setattr(
        control_workers.ssl,
        "create_default_context",
        lambda cafile=None: contexts.append(cafile) or object(),
    )

    def fake_urlopen(request, **kwargs):
        requests.append((request, kwargs))
        return _FakeHttpResponse(response_bytes)

    monkeypatch.setattr(control_workers.urllib.request, "urlopen", fake_urlopen)

    assert control_workers._kubernetes_request("POST", "/api", body) == expected
    request, kwargs = requests[0]
    assert request.full_url == "https://kubernetes.internal:6443/api"
    assert request.data == (None if body is None else json.dumps(body).encode("utf-8"))
    assert request.headers["Authorization"] == "Bearer secret-token"
    assert kwargs["timeout"] == 10
    assert contexts == ([str(ca_path)] if with_ca else [None])


@pytest.mark.parametrize(
    ("raised_error", "status", "message"),
    [
        (
            urllib.error.HTTPError(
                "https://kubernetes.internal/api",
                403,
                "Forbidden",
                {},
                io.BytesIO(b"denied"),
            ),
            403,
            "denied",
        ),
        (
            urllib.error.HTTPError(
                "https://kubernetes.internal/api",
                409,
                "Conflict",
                {},
                io.BytesIO(b""),
            ),
            409,
            "Conflict",
        ),
        (urllib.error.URLError("offline"), 0, "offline"),
    ],
)
def test_kubernetes_request_translates_transport_errors(
    monkeypatch,
    tmp_path,
    raised_error,
    status,
    message,
):
    token_path = tmp_path / "token"
    token_path.write_text("token", encoding="utf-8")
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "kubernetes.internal")
    monkeypatch.setattr(control_workers, "_K8S_API_TOKEN", token_path)
    monkeypatch.setattr(control_workers, "_K8S_API_CA", tmp_path / "missing-ca")
    monkeypatch.setattr(control_workers.ssl, "create_default_context", lambda **_kwargs: object())

    def raise_url_error(*_args, **_kwargs):
        raise raised_error

    monkeypatch.setattr(control_workers.urllib.request, "urlopen", raise_url_error)
    with pytest.raises(control_workers._KubernetesApiError, match=message) as translated:
        control_workers._kubernetes_request("GET", "/api")
    assert translated.value.status == status


@pytest.mark.parametrize(
    ("role", "setting", "expected"),
    [
        ("finish", "process.PTG=9", 1),
        ("start", "missing-separator,other.Queue=4,process.PTG=bad", 1),
        ("start", "arq:PTG=0", 1),
        ("start", "process.PTG=6", 6),
    ],
)
def test_worker_start_replicas_handles_all_config_shapes(monkeypatch, role, setting, expected):
    spec = control_workers.WorkerSpec("arq:PTG", "process.PTG", ("ptg",), role=role)
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_START_REPLICAS", setting)
    assert control_workers._worker_start_replicas(spec) == expected


def test_pid_helpers_handle_missing_invalid_and_live_processes(monkeypatch, tmp_path):
    pid_path = tmp_path / "worker.pid"
    assert control_workers._read_pid(pid_path) is None
    pid_path.write_text("not-a-pid", encoding="utf-8")
    assert control_workers._read_pid(pid_path) is None
    pid_path.write_text("123", encoding="utf-8")
    assert control_workers._read_pid(pid_path) == 123

    assert control_workers._is_pid_running(None) is False
    assert control_workers._is_pid_running(-1) is False

    def missing_process(_pid, _signal):
        raise ProcessLookupError

    monkeypatch.setattr(control_workers.os, "kill", missing_process)
    assert control_workers._is_pid_running(123) is False

    def protected_process(_pid, _signal):
        raise PermissionError

    monkeypatch.setattr(control_workers.os, "kill", protected_process)
    assert control_workers._is_pid_running(123) is True
    monkeypatch.setattr(control_workers.os, "kill", lambda *_args: None)
    assert control_workers._is_pid_running(123) is True


def test_pid_spec_and_node_checks_use_fallbacks(monkeypatch):
    spec = control_workers.WorkerSpec("arq:PTG", "process.PTG", ("ptg",))
    matching = "/opt/python /repo/main.py worker process.PTG"
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "node-a")

    assert control_workers._is_pid_spec_match(None, spec) is False
    assert control_workers._is_pid_on_current_node(None) is False

    def fallback_once_factory(result):
        calls = iter([RuntimeError("eww unavailable"), result])

        def fallback_once(*_args, **_kwargs):
            value = next(calls)
            if isinstance(value, BaseException):
                raise value
            return value

        return fallback_once

    monkeypatch.setattr(
        control_workers.subprocess,
        "check_output",
        fallback_once_factory(matching),
    )
    assert control_workers._is_pid_spec_match(12, spec) is True
    monkeypatch.setattr(
        control_workers.subprocess,
        "check_output",
        fallback_once_factory("HLTHPRT_IMPORT_NODE_ID=node-b worker"),
    )
    assert control_workers._is_pid_on_current_node(12) is False

    def always_fail(*_args, **_kwargs):
        raise RuntimeError("ps unavailable")

    monkeypatch.setattr(control_workers.subprocess, "check_output", always_fail)
    assert control_workers._is_pid_spec_match(12, spec) is True
    assert control_workers._is_pid_on_current_node(12) is True


def test_find_running_pid_handles_ps_failures_and_noisy_rows(monkeypatch):
    spec = control_workers.WorkerSpec("arq:PTG", "process.PTG", ("ptg",))

    def always_fail(*_args, **_kwargs):
        raise RuntimeError("ps unavailable")

    monkeypatch.setattr(control_workers.subprocess, "check_output", always_fail)
    assert control_workers._find_running_pid(spec) is None

    own_pid = 777
    process_rows = "\n".join(
        (
            "50 rg main.py worker process.PTG",
            "60 /opt/python /repo/main.py worker process.Other",
            "bad /opt/python /repo/main.py worker process.PTG",
            f"{own_pid} /opt/python /repo/main.py worker process.PTG",
            "888 /opt/python /repo/main.py worker-once process.PTG",
        )
    )
    calls = iter([RuntimeError("eww unavailable"), process_rows])

    def fallback_ps(*_args, **_kwargs):
        value = next(calls)
        if isinstance(value, BaseException):
            raise value
        return value

    monkeypatch.setattr(control_workers.subprocess, "check_output", fallback_ps)
    monkeypatch.setattr(control_workers.os, "getpid", lambda: own_pid)
    assert control_workers._find_running_pid(spec) == 888


def test_process_match_and_node_match_branch_matrix(monkeypatch):
    spec = control_workers.WorkerSpec("arq:PTG", "process.PTG", ("ptg",))
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "node-a")
    assert control_workers._is_current_node_match("worker") is True
    assert control_workers._is_current_node_match("HLTHPRT_IMPORT_NODE_ID=node-b") is False
    assert control_workers._is_current_node_match("HLTHPRT_IMPORT_NODE_ID=node-a") is True
    assert control_workers._is_process_worker_spec_match("HLTHPRT_IMPORT_NODE_ID=node-b", spec) is False
    assert control_workers._is_process_worker_spec_match("/repo/main.py other process.PTG", spec) is False
    assert control_workers._is_process_worker_spec_match("/repo/main.py worker process.Other", spec) is False
    assert control_workers._is_process_worker_spec_match("/repo/main.py worker process.PTG", spec) is True
    monkeypatch.delenv("HLTHPRT_IMPORT_NODE_ID")
    assert control_workers._is_current_node_match("anything") is True


@pytest.mark.parametrize("raw", ["not-json", "{}", '[1, {"secretName":"only"}]'])
def test_secret_volumes_reject_invalid_config(monkeypatch, raw):
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_SECRET_VOLUME_MOUNTS_JSON", raw)
    assert control_workers._worker_job_secret_volumes() == []


def test_secret_volumes_normalize_aliases_duplicates_and_options(monkeypatch):
    monkeypatch.setenv(
        "HLTHPRT_WORKER_JOB_SECRET_VOLUME_MOUNTS_JSON",
        json.dumps(
            [
                None,
                {"mountPath": "/missing-secret"},
                {"secretName": "missing-path"},
                {
                    "name": "provider-secret",
                    "secretName": "provider-a",
                    "mountPath": "/secrets/a",
                    "optional": True,
                    "readOnly": False,
                    "subPath": "token",
                },
                {
                    "name": "provider-secret",
                    "secret_name": "provider-b",
                    "mount_path": "/secrets/b",
                    "read_only": True,
                },
            ]
        ),
    )
    volumes = control_workers._worker_job_secret_volumes()
    assert volumes[0] == {
        "volume": {
            "name": "provider-secret",
            "secret": {"secretName": "provider-a", "optional": True},
        },
        "volumeMount": {
            "name": "provider-secret",
            "mountPath": "/secrets/a",
            "readOnly": False,
            "subPath": "token",
        },
    }
    assert volumes[1]["volume"]["name"] == "provider-secret-4"
    assert volumes[1]["volumeMount"]["readOnly"] is True


def test_worker_resource_profiles_cover_invalid_and_fallback_shapes(monkeypatch):
    spec = control_workers.WorkerSpec("arq:PTG", "process.PTG", ("ptg",))
    monkeypatch.delenv("HLTHPRT_WORKER_JOB_RESOURCE_PROFILES_JSON", raising=False)
    assert control_workers._worker_job_resource_profile(spec, {}) == {}
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_RESOURCE_PROFILES_JSON", "not-json")
    assert control_workers._worker_job_resource_profile(spec, {}) == {}
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_RESOURCE_PROFILES_JSON", "[]")
    assert control_workers._worker_job_resource_profile(spec, {}) == {}

    monkeypatch.setenv(
        "HLTHPRT_WORKER_JOB_RESOURCE_PROFILES_JSON",
        json.dumps(
            {
                "process.PTG": {"requests": "invalid"},
                "arq:PTG": {
                    "requests": {"cpu": " 2 ", "unsupported": "ignored"},
                    "limits": {"memory": "8Gi", "cpu": ""},
                },
            }
        ),
    )
    assert control_workers._worker_job_resource_profile(spec, {}) == {
        "requests": {"cpu": "2"},
        "limits": {"memory": "8Gi"},
    }

    monkeypatch.setenv("HLTHPRT_WORKER_JOB_RESOURCE_PROFILES_JSON", "{}")
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_CPU_REQUEST", "1")
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_MEMORY_LIMIT", "4Gi")
    assert control_workers._worker_job_resources(spec) == {
        "requests": {"cpu": "1"},
        "limits": {"memory": "4Gi"},
    }


def test_worker_pvc_volumes_require_both_values_and_normalize_name(monkeypatch):
    monkeypatch.delenv("HLTHPRT_WORKER_JOB_PVC_NAME", raising=False)
    monkeypatch.delenv("HLTHPRT_WORKER_JOB_PVC_MOUNT_PATH", raising=False)
    assert control_workers._worker_job_pvc_volumes() == []
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_PVC_NAME", "provider-data")
    assert control_workers._worker_job_pvc_volumes() == []
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_PVC_MOUNT_PATH", "/work")
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_PVC_VOLUME_NAME", "")
    assert control_workers._worker_job_pvc_volumes()[0]["volume"]["name"] == "import-workdir"


def test_ensure_worker_applies_started_and_failed_status_precedence(monkeypatch):
    worker_specs = [
        control_workers.WorkerSpec("arq:One", "process.One", ("one",)),
        control_workers.WorkerSpec("arq:Two", "process.Two", ("two",)),
    ]
    monkeypatch.setattr(control_workers, "_resolve_specs", lambda _request: worker_specs)
    worker_responses = iter(({"status": "started"}, {"status": "failed"}))
    monkeypatch.setattr(
        control_workers,
        "_ensure_spec",
        lambda *_args: next(worker_responses),
    )
    assert control_workers.ensure_worker({})["status"] == "failed"

    monkeypatch.setattr(
        control_workers,
        "_ensure_spec",
        lambda *_args: {"status": "already_running"},
    )
    assert control_workers.ensure_worker({})["status"] == "already_running"


@pytest.mark.parametrize(
    ("job_status", "is_running", "expected_status"),
    [
        ("failed", False, "failed"),
        ("succeeded", False, "succeeded"),
        ("missing", False, "missing"),
        (None, False, "inactive"),
        (None, True, "running"),
    ],
)
def test_worker_state_aggregates_runtime_status(
    monkeypatch,
    job_status,
    is_running,
    expected_status,
):
    worker_spec = control_workers.WorkerSpec("arq:One", "process.One", ("one",))
    monkeypatch.setattr(control_workers, "_resolve_specs", lambda _request: [worker_spec])
    monkeypatch.setattr(
        control_workers,
        "_worker_state",
        lambda *_args: {"job_status": job_status, "running": is_running},
    )
    assert control_workers.worker_state({})["status"] == expected_status


def test_worker_resolution_rejects_unknown_or_conflicting_identity():
    assert control_workers.worker_state({"worker_class": "process.Unknown"})["status"] == "unsupported"
    assert control_workers._resolve_specs(
        {"worker_class": "process.PTG", "queue": "arq:Different"}
    ) == []
    assert control_workers._resolve_specs({"queue": "arq:Unknown"}) == []
    assert control_workers._resolve_specs(
        {"importer": "unknown", "role": "finish"}
    ) == []
    assert control_workers._resolve_specs({"importer": "unknown"}) == []


def test_worker_state_replaces_a_stale_pid_with_the_discovered_process(monkeypatch):
    worker_spec = control_workers.WorkerSpec("arq:PTG", "process.PTG", ("ptg",))
    removed_specs = []
    written_pids = []
    monkeypatch.setattr(control_workers, "_launcher_mode", lambda: "process")
    monkeypatch.setattr(control_workers, "_read_pid", lambda _path: 101)
    monkeypatch.setattr(control_workers, "_is_pid_running", lambda pid: pid == 202)
    monkeypatch.setattr(control_workers, "_remove_stale_pid", removed_specs.append)
    monkeypatch.setattr(control_workers, "_find_running_pid", lambda _spec: 202)
    monkeypatch.setattr(
        control_workers,
        "_write_pid_file",
        lambda spec, pid: written_pids.append((spec, pid)),
    )

    state = control_workers._worker_state(worker_spec)

    assert state["running"] is True
    assert state["pid"] == 202
    assert removed_specs == [worker_spec]
    assert written_pids == [(worker_spec, 202)]


def test_ensure_spec_returns_an_existing_running_worker(monkeypatch):
    worker_spec = control_workers.WorkerSpec("arq:PTG", "process.PTG", ("ptg",))
    monkeypatch.setattr(
        control_workers,
        "_worker_state",
        lambda *_args: {"running": True, "pid": 101},
    )

    assert control_workers._ensure_spec(worker_spec, {}) == {
        "running": True,
        "pid": 101,
        "status": "already_running",
    }


def test_delete_worker_record_distinguishes_missing_and_failed_deletes(monkeypatch):
    worker_spec = control_workers.WorkerSpec("arq:PTG", "process.PTG", ("ptg",))
    assert control_workers._delete_kubernetes_worker_job_record(
        "healthporta-dev", worker_spec, {}
    ) == (None, None)

    active_job_record_by_field = {
        "metadata": {"name": "active-job"},
        "status": {"active": 1},
    }
    for status_code, expected_reason in ((404, "not_found"), (403, None)):
        def reject_delete(*_args, **_kwargs):
            raise control_workers._KubernetesApiError(status_code, "rejected")

        monkeypatch.setattr(control_workers, "_delete_kubernetes_job", reject_delete)
        deletion_record, deletion_error = (
            control_workers._delete_kubernetes_worker_job_record(
                "healthporta-dev", worker_spec, active_job_record_by_field
            )
        )
        if expected_reason:
            assert deletion_record["reason"] == expected_reason
            assert deletion_error is None
        else:
            assert deletion_record is None
            assert deletion_error["status"] == status_code
