# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Keep registered NPI jobs on the fixed, admitted Source bootstrap."""

import json
from contextlib import asynccontextmanager
from dataclasses import asdict, replace
from unittest.mock import AsyncMock

import pytest

from api import control_workers as workers
from api import npi_source_worker as source
from tests.test_provider_directory_source_worker_launch import (
    _change_isolation,
    _change_job_bindings,
    _change_target,
)

RUN_ID = "run_" + "a" * 32
JOB_ID = "npi_start_" + RUN_ID
IMAGE = "example/source-worker@sha256:" + "c" * 64
SPEC = workers._BY_WORKER_CLASS["process.NPI"]


def _recorded():
    return {
        "run_id": RUN_ID,
        "importer": "npi",
        "status": "queued",
        "metrics": {"queue": SPEC.queue, "function": "control_single_job_start", "job_id": JOB_ID},
    }


@pytest.fixture
def admitted(monkeypatch):
    monkeypatch.setenv(source.IMAGE_ENV, IMAGE)
    request = workers._AdmittedWorkerRequest(run_id=RUN_ID, importer="npi")
    workers._bind_admitted_job(request, _recorded())
    request.npi_source_launch = source.admit_npi_source_worker_launch(_recorded())
    return request


@pytest.fixture
def mounted_job(admitted, monkeypatch):
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_PVC_NAME", "synthetic-work")
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_PVC_MOUNT_PATH", "/work")
    monkeypatch.setenv(
        "HLTHPRT_WORKER_JOB_SECRET_ENV_JSON",
        json.dumps(
            [
                {
                    "name": "SOURCE_USER",
                    "secretName": "synthetic-publisher",
                    "key": "username",
                    "workerClasses": [SPEC.worker_class],
                    "importers": ["npi"],
                    "purposes": [source.PURPOSE],
                }
            ]
        ),
    )
    monkeypatch.setenv(
        "HLTHPRT_WORKER_JOB_SECRET_VOLUME_MOUNTS_JSON",
        json.dumps(
            [
                {
                    "name": "source-policy",
                    "secretName": "synthetic-source-policy",
                    "mountPath": "/etc/source-policy",
                    "subPath": "roles.json",
                    "defaultMode": 0o440,
                    "workerClasses": [SPEC.worker_class],
                }
            ]
        ),
    )
    return workers._worker_job_manifest(SPEC, admitted, "example/ordinary:latest")


def test_fixed_manifest_keeps_observed_job_image_actor_and_secrets(admitted, mounted_job, monkeypatch):
    launch = admitted.npi_source_launch
    assert launch == source.NPISourceWorkerLaunch(RUN_ID, JOB_ID, IMAGE)
    assert source.authorized_npi_source_worker_launch(SPEC, admitted) is launch
    assert workers._single_job_worker_target(SPEC, admitted) == JOB_ID
    pod = mounted_job["spec"]["template"]["spec"]
    container = pod["containers"][0]
    assert container["command"] == ["/usr/local/bin/npi-source-worker"]
    assert container["image"] == IMAGE and container["workingDir"] == "/app"
    assert pod["securityContext"]["runAsUser"] == pod["securityContext"]["runAsGroup"] == 10001
    environment_by_name = {entry["name"]: entry for entry in container["env"]}
    assert environment_by_name["HLTHPRT_CONTROL_RUN_ID"]["value"] == RUN_ID
    assert environment_by_name["HLTHPRT_WORKER_ONCE_TARGET_JOB_ID"]["value"] == JOB_ID
    assert environment_by_name["SOURCE_USER"]["valueFrom"]["secretKeyRef"]["name"] == "synthetic-publisher"
    assert source.is_npi_source_job_matching(mounted_job, launch)
    setting = "HLTHPRT_SOURCE_PROFILE_ROLE_POLICY_FILE"
    monkeypatch.setenv(setting, "/synthetic-readonly/roles.json")
    admitted[setting] = "/untrusted/roles.json"
    environment, _ = workers._worker_job_environment(SPEC, admitted)
    assert [entry for entry in environment if entry["name"] == setting] == [
        {"name": setting, "value": "/synthetic-readonly/roles.json"}
    ]


@pytest.mark.parametrize("image", ["", "example/source:latest", IMAGE[:-1] + "z"])
def test_admission_requires_immutable_image(monkeypatch, image):
    monkeypatch.setenv(source.IMAGE_ENV, image)
    with pytest.raises(ValueError, match="npi_source_worker_admission_unavailable"):
        source.admit_npi_source_worker_launch(_recorded())
    assert source.admit_npi_source_worker_launch({"importer": "nucc"}) is None


@pytest.mark.parametrize("index", range(5))
def test_admitted_native_coordinates_cannot_change(admitted, index):
    identity_values = list(admitted.admitted_job)
    identity_values[index] = "other"
    admitted.admitted_job = tuple(identity_values)
    with pytest.raises(ValueError, match="admission_unavailable"):
        source.authorized_npi_source_worker_launch(SPEC, admitted)


@pytest.mark.parametrize("field", ["run_id", "importer", "job_id"])
def test_request_cannot_redirect_admitted_job(admitted, field):
    admitted[field] = "other"
    with pytest.raises(ValueError, match="admission_unavailable"):
        workers._single_job_worker_target(SPEC, admitted)


@pytest.mark.parametrize("field", ["worker_class", "queue", "role", "importers"])
def test_capability_is_bound_to_registered_worker(admitted, field):
    spec = replace(SPEC, **{field: ("other",) if field == "importers" else "other"})
    with pytest.raises(ValueError, match="admission_unavailable"):
        source.authorized_npi_source_worker_launch(spec, admitted)


@pytest.mark.parametrize("field", ["target_module", "target_function", "job_prefix", "call_style", "run_shutdown"])
def test_changed_adapter_refuses_launch(admitted, monkeypatch, field):
    from api.control_imports import _SINGLE_JOB_ADAPTERS

    monkeypatch.setitem(_SINGLE_JOB_ADAPTERS, "npi", {**_SINGLE_JOB_ADAPTERS["npi"], field: "other"})
    with pytest.raises(ValueError, match="admission_unavailable"):
        source.authorized_npi_source_worker_launch(SPEC, admitted)


def test_serialized_capability_never_authorizes_launcher_or_secrets(admitted, mounted_job):
    raw_payload_by_field = {
        **admitted,
        "admitted_job": admitted.admitted_job,
        "npi_source_launch": asdict(admitted.npi_source_launch),
    }
    assert source.authorized_npi_source_worker_launch(SPEC, raw_payload_by_field) is None
    assert workers._worker_job_secret_env(SPEC.worker_class, launch_request=raw_payload_by_field) == []
    assert workers._ensure_spec(SPEC, raw_payload_by_field)["status"] == "failed"
    admitted.npi_source_launch = raw_payload_by_field["npi_source_launch"]
    with pytest.raises(ValueError, match="admission_unavailable"):
        source.authorized_npi_source_worker_launch(SPEC, admitted)


@pytest.mark.parametrize("field", ["purposes", "importers", "workerClasses"])
def test_source_secret_selector_cannot_broaden(admitted, mounted_job, monkeypatch, field):
    selection = json.loads(workers.os.environ["HLTHPRT_WORKER_JOB_SECRET_ENV_JSON"])[0]
    selection[field].append("other")
    with pytest.raises(ValueError, match="selector is invalid"):
        workers._is_admitted_importer_selected(selection, SPEC.worker_class, admitted)


@pytest.mark.parametrize("with_job_id", [False, True], ids=["api", "scheduled"])
@pytest.mark.asyncio
async def test_locked_ensure_admits_the_actual_registered_job(monkeypatch, with_job_id):
    monkeypatch.setenv(source.IMAGE_ENV, IMAGE)

    @asynccontextmanager
    async def connection():
        yield object()

    monkeypatch.setattr(workers.db, "acquire", connection)
    monkeypatch.setattr(workers, "acquire_control_run_worker_action_lock", AsyncMock())
    observed = AsyncMock(return_value=_recorded())
    monkeypatch.setattr(workers, "admit_existing_outer_run_action", observed)
    launched_requests = []
    monkeypatch.setattr(
        workers, "ensure_worker", lambda request: launched_requests.append(request) or {"status": "started"}
    )
    request_by_field = {"run_id": RUN_ID, "importer": "npi"}
    if with_job_id:
        request_by_field.update(queue=SPEC.queue, job_id=JOB_ID)
    assert await workers.guarded_ensure_worker(request_by_field) == {"status": "started"}
    observed.assert_awaited_once()
    assert len(launched_requests) == 1
    assert source.authorized_npi_source_worker_launch(SPEC, launched_requests[0]).job_id == JOB_ID


@pytest.mark.asyncio
async def test_missing_image_refuses_before_launch(monkeypatch):
    monkeypatch.delenv(source.IMAGE_ENV, raising=False)
    request = workers._AdmittedWorkerRequest(run_id=RUN_ID, importer="npi")
    workers._bind_admitted_job(request, _recorded())
    assert (await workers._admit_source_worker_launches(request, _recorded()))["status"] == "failed"
    assert request.npi_source_launch is None


def test_source_launch_has_no_local_process_fallback(admitted, monkeypatch):
    monkeypatch.setenv("HLTHPRT_WORKER_LAUNCHER", "process")
    monkeypatch.setattr(workers, "_start_process", lambda *args: pytest.fail("ordinary NPI process started"))
    assert workers._ensure_spec(SPEC, admitted)["status"] == "failed"


def test_kubernetes_create_uses_fixed_launch(admitted, mounted_job, monkeypatch):
    monkeypatch.setattr(workers, "_worker_state", lambda *args: {"running": True, "job_status": "active"})
    calls = []
    monkeypatch.setattr(workers, "_kubernetes_request", lambda *args: calls.append(args) or {})
    assert workers._ensure_kubernetes_job(SPEC, admitted, {"job_status": "missing"})["status"] == "started"
    assert len(calls) == 1 and calls[0][0] == "POST"
    assert source.is_npi_source_job_matching(calls[0][2], admitted.npi_source_launch)


@pytest.mark.parametrize(
    "drift",
    [
        "image",
        "command",
        "actor",
        "purpose",
        "target",
        "duplicate_target",
        "container_actor",
        "container_nonroot",
        "escalation",
        "capabilities",
        "seccomp",
        "supplemental_groups",
        "service_account_token",
        "hostPID",
        "hostIPC",
        "hostNetwork",
        "postStart",
        "preStop",
        "/app",
        "/opt",
        "/usr/local/bin",
        "mount-path",
        "pvc-source",
        "secret-source",
        "secret-subpath",
        "secret-writable",
        "secret-mode",
        "volume-device",
        "extra-volume",
        "missing-volume",
        "missing-mount",
        "duplicate-volume",
        "duplicate-mount",
        "swapped-name",
    ],
)
def test_existing_job_refuses_changed_code_identity_or_mounts(admitted, mounted_job, monkeypatch, drift):
    _change_npi_job(mounted_job, drift)
    assert not source.is_npi_source_job_matching(mounted_job, admitted.npi_source_launch)
    monkeypatch.setattr(workers, "_is_kubernetes_configured", lambda: True)
    monkeypatch.setattr(workers, "_launcher_mode", lambda: "kubernetes")
    calls = []
    monkeypatch.setattr(workers, "_kubernetes_request", lambda *args: calls.append(args) or {"items": [mounted_job]})
    worker_state = workers._ensure_spec(SPEC, admitted)
    assert worker_state["status"] == "failed" and worker_state["job_status"] == "purpose_mismatch"
    assert len(calls) == 1 and calls[0][0] == "GET"


def _change_npi_job(mounted_job, drift):
    pod = mounted_job["spec"]["template"]["spec"]
    container = pod["containers"][0]
    if drift in {"image", "command"}:
        container[drift] = "other" if drift == "image" else ["other"]
        return
    if drift == "actor":
        pod["securityContext"]["runAsUser"] = 0
        return
    if drift == "purpose":
        mounted_job["metadata"]["labels"]["healthporta.com/worker-purpose"] = "other"
        return
    if drift in {"target", "duplicate_target"}:
        _change_target(container, drift)
        return
    if drift in {
        "container_actor",
        "container_nonroot",
        "escalation",
        "capabilities",
        "seccomp",
        "supplemental_groups",
        "service_account_token",
        "hostPID",
        "hostIPC",
        "hostNetwork",
    }:
        _change_isolation(pod, container, drift)
        return
    _change_job_bindings(mounted_job, drift)
