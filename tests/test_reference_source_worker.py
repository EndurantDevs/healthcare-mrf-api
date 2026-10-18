# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exercise fixed worker routing without claiming native admission or deployment."""

import json
from copy import deepcopy
from dataclasses import asdict, replace
from unittest.mock import AsyncMock

import pytest

from api import control_workers as workers
from api import reference_source_worker as reference

RUN_ID = "run_" + "a" * 32
JOB_ID = "b" * 32
IMAGE = "example/reference-worker@sha256:" + "c" * 64
ORDINARY_IMAGE = "example/ordinary-worker@sha256:" + "d" * 64
SPEC = workers._BY_WORKER_CLASS["process.Geo"]


def _recorded():
    return {
        "run_id": RUN_ID,
        "importer": "geo",
        "status": "queued",
        "metrics": {"queue": SPEC.queue, "function": "control_single_job_start", "job_id": JOB_ID},
    }


@pytest.fixture
def admitted(monkeypatch):
    monkeypatch.setenv(reference.IMAGE_ENV, IMAGE)
    request = workers._AdmittedWorkerRequest(run_id=RUN_ID, importer="geo")
    recorded = _recorded()
    workers._bind_admitted_job(request, recorded)
    request.reference_source_launch = reference.admit_reference_source_worker_launch(recorded)
    return request


@pytest.fixture
def secret_selection(monkeypatch):
    selection_by_field = {
        "name": "SOURCE_USER",
        "secretName": "synthetic-publisher",
        "key": "username",
        "workerClasses": [SPEC.worker_class],
        "importers": ["geo"],
        "purposes": [reference.PURPOSE],
    }
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_SECRET_ENV_JSON", json.dumps([selection_by_field]))
    return selection_by_field


def test_admission_keeps_actual_random_queue_id(admitted):
    assert admitted.reference_source_launch == reference.ReferenceSourceWorkerLaunch(RUN_ID, JOB_ID, IMAGE)
    assert reference.authorized_reference_source_worker_launch(SPEC, admitted) == admitted.reference_source_launch
    assert workers._single_job_worker_target(SPEC, admitted) == JOB_ID
    assert workers._worker_command("python", SPEC)[-2:] == ["worker-once", "process.Geo"]


@pytest.mark.parametrize("image", ["", "example/reference-worker:latest", IMAGE[:-1] + "z"])
def test_reference_admission_requires_immutable_image(monkeypatch, image):
    monkeypatch.setenv(reference.IMAGE_ENV, image)
    with pytest.raises(ValueError, match="reference_source_worker_admission_unavailable"):
        reference.admit_reference_source_worker_launch(_recorded())
    assert reference.admit_reference_source_worker_launch({"importer": "nucc"}) is None


@pytest.mark.parametrize("field", ["queue", "function", "job_id", "run_id", "metrics"])
def test_missing_or_foreign_observed_coordinates_refuse(monkeypatch, field):
    monkeypatch.setenv(reference.IMAGE_ENV, IMAGE)
    recorded = _recorded()
    if field in {"run_id", "metrics"}:
        recorded[field] = None
    else:
        recorded["metrics"][field] = "other" if field != "job_id" else " "
    with pytest.raises(ValueError, match="admission_unavailable"):
        reference.admit_reference_source_worker_launch(recorded)


@pytest.mark.parametrize("index", range(5))
def test_each_observed_coordinate_remains_required(admitted, index):
    identity_values = list(admitted.admitted_job)
    identity_values[index] = "different"
    admitted.admitted_job = tuple(identity_values)
    with pytest.raises(ValueError, match="admission_unavailable"):
        reference.authorized_reference_source_worker_launch(SPEC, admitted)


@pytest.mark.parametrize("field", ["run_id", "importer", "job_id"])
def test_conflicting_request_cannot_redirect_native_job(admitted, field):
    admitted[field] = "different"
    with pytest.raises(ValueError, match="admission_unavailable"):
        workers._single_job_worker_target(SPEC, admitted)


@pytest.mark.parametrize("field", ["worker_class", "queue", "role", "importers"])
def test_capability_cannot_select_another_worker(admitted, field):
    spec = replace(SPEC, **{field: ("other",) if field == "importers" else "other"})
    with pytest.raises(ValueError, match="admission_unavailable"):
        reference.authorized_reference_source_worker_launch(spec, admitted)


def test_serialized_capability_cannot_select_source_secrets_or_launcher(admitted, secret_selection):
    raw = json.loads(
        json.dumps(
            {
                **admitted,
                "admitted_job": admitted.admitted_job,
                "reference_source_launch": asdict(admitted.reference_source_launch),
            }
        )
    )
    assert reference.authorized_reference_source_worker_launch(SPEC, raw) is None
    assert workers._worker_job_secret_env(SPEC.worker_class, launch_request=raw) == []
    assert workers._ensure_spec(SPEC, raw)["status"] == "failed"
    admitted.reference_source_launch = asdict(admitted.reference_source_launch)
    with pytest.raises(ValueError, match="admission_unavailable"):
        reference.authorized_reference_source_worker_launch(SPEC, admitted)


@pytest.mark.parametrize("field", ["purposes", "importers", "workerClasses"])
def test_secret_selector_cannot_broaden_source_purpose(admitted, secret_selection, field):
    secret_selection[field].append("other")
    with pytest.raises(ValueError, match="selector is invalid"):
        workers._is_admitted_importer_selected(secret_selection, SPEC.worker_class, admitted)


def test_manifest_keeps_fixed_image_command_actor_and_native_target(admitted, secret_selection, monkeypatch):
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_PVC_NAME", "synthetic-work")
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_PVC_MOUNT_PATH", "/work")
    manifest = workers._worker_job_manifest(SPEC, admitted, ORDINARY_IMAGE)
    pod = manifest["spec"]["template"]["spec"]
    container = pod["containers"][0]
    assert container["image"] == IMAGE
    assert container["command"] == reference.COMMAND
    assert container["workingDir"] == "/app"
    assert pod["securityContext"]["runAsUser"] == pod["securityContext"]["runAsGroup"] == 10001
    assert pod["securityContext"]["fsGroup"] == 10001
    assert pod["automountServiceAccountToken"] is False
    env_by_name = {entry["name"]: entry for entry in container["env"]}
    assert env_by_name["HLTHPRT_WORKER_ONCE_TARGET_JOB_ID"]["value"] == JOB_ID
    assert env_by_name["SOURCE_USER"]["valueFrom"]["secretKeyRef"]["name"] == "synthetic-publisher"
    assert reference.is_reference_source_job_matching(manifest, admitted.reference_source_launch)


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
    ],
)
def test_existing_job_cannot_substitute_source_identity(admitted, secret_selection, drift):
    manifest = workers._worker_job_manifest(SPEC, admitted, ORDINARY_IMAGE)
    pod = manifest["spec"]["template"]["spec"]
    container = pod["containers"][0]
    if drift in {"image", "command"}:
        container[drift] = ORDINARY_IMAGE if drift == "image" else ["other"]
    elif drift == "actor":
        pod["securityContext"]["runAsUser"] = 65534
    elif drift == "purpose":
        manifest["metadata"]["labels"]["healthporta.com/worker-purpose"] = "other"
    elif drift in {"target", "duplicate_target"}:
        _change_target(container, drift)
    else:
        _change_isolation(pod, container, drift)
    assert not reference.is_reference_source_job_matching(manifest, admitted.reference_source_launch)


def _change_target(container, drift):
    target = next(entry for entry in container["env"] if entry["name"] == "HLTHPRT_WORKER_ONCE_TARGET_JOB_ID")
    if drift == "target":
        target["value"] = "other"
    else:
        container["env"].append(deepcopy(target))


def _change_isolation(pod, container, drift):
    if drift in {"hostPID", "hostIPC", "hostNetwork"}:
        pod[drift] = True
    elif drift == "service_account_token":
        pod["automountServiceAccountToken"] = True
    elif drift in {"seccomp", "supplemental_groups"}:
        key = "seccompProfile" if drift == "seccomp" else "supplementalGroups"
        pod["securityContext"][key] = {"type": "Unconfined"} if drift == "seccomp" else [0]
    else:
        key, override = {
            "container_actor": ("runAsUser", 0),
            "container_nonroot": ("runAsNonRoot", False),
            "escalation": ("allowPrivilegeEscalation", True),
            "capabilities": ("capabilities", {"add": ["SYS_ADMIN"], "drop": ["ALL"]}),
        }[drift]
        container["securityContext"][key] = override


@pytest.mark.asyncio
async def test_actual_ensure_path_binds_only_the_observed_native_job(monkeypatch):
    monkeypatch.setenv(reference.IMAGE_ENV, IMAGE)
    observed = AsyncMock(return_value=_recorded())
    monkeypatch.setattr(workers, "admit_existing_outer_run_action", observed)
    launched_requests = []
    monkeypatch.setattr(
        workers, "ensure_worker", lambda request: launched_requests.append(request) or {"status": "started"}
    )
    assert await workers.guarded_ensure_worker({"run_id": RUN_ID, "importer": "geo"}) == {"status": "started"}
    observed.assert_awaited_once()
    assert len(launched_requests) == 1
    assert reference.authorized_reference_source_worker_launch(SPEC, launched_requests[0]).job_id == JOB_ID


def test_source_launch_never_falls_back_to_local_process(admitted, monkeypatch):
    monkeypatch.setenv("HLTHPRT_WORKER_LAUNCHER", "process")
    monkeypatch.setattr(workers, "_start_process", lambda *args: pytest.fail("ordinary source process started"))
    assert workers._ensure_spec(SPEC, admitted)["status"] == "failed"


def test_kubernetes_create_uses_fixed_source_launch(admitted, secret_selection, monkeypatch):
    monkeypatch.setattr(workers, "_worker_state", lambda *args: {"running": True, "job_status": "active"})
    calls = []
    monkeypatch.setattr(workers, "_kubernetes_request", lambda *args: calls.append(args) or {})
    result = workers._ensure_kubernetes_job(SPEC, admitted, {"job_status": "missing"})
    assert result["status"] == "started"
    assert len(calls) == 1 and calls[0][0] == "POST"
    assert reference.is_reference_source_job_matching(calls[0][2], admitted.reference_source_launch)


def test_existing_kubernetes_job_mismatch_refuses_launch(admitted, secret_selection, monkeypatch, tmp_path):
    token = tmp_path / "synthetic-token"
    token.write_text("synthetic")
    monkeypatch.setattr(workers, "_K8S_API_TOKEN", token)
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "synthetic")
    monkeypatch.setenv("HLTHPRT_WORKER_LAUNCHER", "kubernetes")
    manifest = workers._worker_job_manifest(SPEC, admitted, ORDINARY_IMAGE)
    manifest["spec"]["template"]["spec"]["containers"][0]["command"] = ["other"]
    calls = []
    monkeypatch.setattr(workers, "_kubernetes_request", lambda *args: calls.append(args) or {"items": [manifest]})
    result = workers._ensure_spec(SPEC, admitted)
    assert result["status"] == "failed" and result["job_status"] == "purpose_mismatch"
    assert all(call[0] == "GET" for call in calls)
