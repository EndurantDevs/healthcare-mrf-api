# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exercise fixed worker routing without claiming native admission or deployment."""

import json
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import asdict, replace
from unittest.mock import AsyncMock

import pytest

from api import control_workers as workers
from api import provider_directory_source_worker as source

RUN_ID = "run_" + "a" * 32
JOB_ID = "provider_directory_start_" + RUN_ID
IMAGE = "example/source-worker@sha256:" + "c" * 64
ORDINARY_IMAGE = "example/ordinary-worker@sha256:" + "d" * 64
SPEC = workers._BY_WORKER_CLASS["process.ProviderDirectoryFHIR"]


def _recorded():
    return {
        "run_id": RUN_ID,
        "importer": "provider-directory-fhir",
        "status": "queued",
        "metrics": {"queue": SPEC.queue, "function": "control_single_job_start", "job_id": JOB_ID},
    }


@pytest.fixture
def admitted(monkeypatch):
    monkeypatch.setenv(source.IMAGE_ENV, IMAGE)
    request = workers._AdmittedWorkerRequest(run_id=RUN_ID, importer="provider-directory-fhir")
    recorded = _recorded()
    workers._bind_admitted_job(request, recorded)
    request.fhir_source_launch = source.admit_fhir_source_worker_launch(recorded)
    return request


@pytest.fixture
def secret_selection(monkeypatch):
    selection_by_field = {
        "name": "SOURCE_USER",
        "secretName": "synthetic-publisher",
        "key": "username",
        "workerClasses": [SPEC.worker_class],
        "importers": ["provider-directory-fhir"],
        "purposes": [source.PURPOSE],
    }
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_SECRET_ENV_JSON", json.dumps([selection_by_field]))
    return selection_by_field


def test_admission_keeps_actual_observed_queue_id(admitted):
    assert admitted.fhir_source_launch == source.FHIRSourceWorkerLaunch(RUN_ID, JOB_ID, IMAGE)
    assert source.authorized_fhir_source_worker_launch(SPEC, admitted) == admitted.fhir_source_launch
    assert workers._single_job_worker_target(SPEC, admitted) == JOB_ID
    assert workers._worker_command("python", SPEC)[-2:] == ["worker-once", "process.ProviderDirectoryFHIR"]


@pytest.mark.parametrize("image", ["", "example/source-worker:latest", IMAGE[:-1] + "z"])
def test_fhir_admission_requires_immutable_image(monkeypatch, image):
    monkeypatch.setenv(source.IMAGE_ENV, image)
    monkeypatch.setenv(source.REFERENCE_IMAGE_ENV, ORDINARY_IMAGE)
    with pytest.raises(ValueError, match="fhir_source_worker_admission_unavailable"):
        source.admit_fhir_source_worker_launch(_recorded())
    assert source.admit_fhir_source_worker_launch({"importer": "nucc"}) is None


def test_fhir_image_setting_takes_precedence_without_changing_reference(monkeypatch):
    from api import reference_source_worker as reference

    monkeypatch.setenv(source.IMAGE_ENV, IMAGE)
    monkeypatch.setenv(reference.IMAGE_ENV, ORDINARY_IMAGE)
    assert source.admit_fhir_source_worker_launch(_recorded()).image == IMAGE
    reference_run_by_field = {
        "run_id": RUN_ID,
        "importer": "geo",
        "metrics": {"queue": "arq:Geo", "function": "control_single_job_start", "job_id": "geo-job"},
    }
    assert reference.admit_reference_source_worker_launch(reference_run_by_field).image == ORDINARY_IMAGE


def test_absent_fhir_image_setting_retains_reference_fallback(monkeypatch):
    monkeypatch.delenv(source.IMAGE_ENV, raising=False)
    monkeypatch.setenv(source.REFERENCE_IMAGE_ENV, IMAGE)
    assert source.admit_fhir_source_worker_launch(_recorded()).image == IMAGE


@pytest.mark.parametrize("image", ["", "example/source-worker:latest", IMAGE[:-1] + "z"])
def test_absent_fhir_image_setting_rejects_invalid_reference_fallback(monkeypatch, image):
    monkeypatch.delenv(source.IMAGE_ENV, raising=False)
    monkeypatch.setenv(source.REFERENCE_IMAGE_ENV, image)
    with pytest.raises(ValueError, match="fhir_source_worker_admission_unavailable"):
        source.admit_fhir_source_worker_launch(_recorded())


@pytest.mark.parametrize("field", ["queue", "function", "job_id", "run_id", "metrics"])
def test_missing_or_foreign_observed_coordinates_refuse(monkeypatch, field):
    monkeypatch.setenv(source.IMAGE_ENV, IMAGE)
    recorded = _recorded()
    if field in {"run_id", "metrics"}:
        recorded[field] = None
    else:
        recorded["metrics"][field] = "other" if field != "job_id" else " "
    with pytest.raises(ValueError, match="admission_unavailable"):
        source.admit_fhir_source_worker_launch(recorded)


@pytest.mark.parametrize("index", range(5))
def test_each_observed_coordinate_remains_required(admitted, index):
    identity_values = list(admitted.admitted_job)
    identity_values[index] = "different"
    admitted.admitted_job = tuple(identity_values)
    with pytest.raises(ValueError, match="admission_unavailable"):
        source.authorized_fhir_source_worker_launch(SPEC, admitted)


@pytest.mark.parametrize("field", ["run_id", "importer", "job_id"])
def test_conflicting_request_cannot_redirect_native_job(admitted, field):
    admitted[field] = "different"
    with pytest.raises(ValueError, match="admission_unavailable"):
        workers._single_job_worker_target(SPEC, admitted)


@pytest.mark.parametrize("field", ["worker_class", "queue", "role", "importers"])
def test_capability_cannot_select_another_worker(admitted, field):
    spec = replace(SPEC, **{field: ("other",) if field == "importers" else "other"})
    with pytest.raises(ValueError, match="admission_unavailable"):
        source.authorized_fhir_source_worker_launch(spec, admitted)


def test_serialized_capability_cannot_select_source_secrets_or_launcher(admitted, secret_selection):
    raw = json.loads(
        json.dumps(
            {
                **admitted,
                "admitted_job": admitted.admitted_job,
                "fhir_source_launch": asdict(admitted.fhir_source_launch),
            }
        )
    )
    assert source.authorized_fhir_source_worker_launch(SPEC, raw) is None
    assert workers._worker_job_secret_env(SPEC.worker_class, launch_request=raw) == []
    assert workers._ensure_spec(SPEC, raw)["status"] == "failed"
    admitted.fhir_source_launch = asdict(admitted.fhir_source_launch)
    with pytest.raises(ValueError, match="admission_unavailable"):
        source.authorized_fhir_source_worker_launch(SPEC, admitted)


@pytest.mark.parametrize("field", ["purposes", "importers", "workerClasses"])
def test_secret_selector_cannot_broaden_source_purpose(admitted, secret_selection, field):
    secret_selection[field].append("other")
    with pytest.raises(ValueError, match="selector is invalid"):
        workers._is_admitted_importer_selected(secret_selection, SPEC.worker_class, admitted)


@pytest.mark.parametrize("has_volume", [False, True])
def test_manifest_keeps_fixed_image_command_actor_and_native_target(admitted, secret_selection, monkeypatch, has_volume):
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_PVC_NAME", "synthetic-work" if has_volume else "")
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_PVC_MOUNT_PATH", "/work" if has_volume else "")
    monkeypatch.delenv("HLTHPRT_WORKER_JOB_SECRET_VOLUME_MOUNTS_JSON", raising=False)
    manifest = workers._worker_job_manifest(SPEC, admitted, ORDINARY_IMAGE)
    pod = manifest["spec"]["template"]["spec"]
    container = pod["containers"][0]
    assert container["image"] == IMAGE
    assert container["command"] == source.COMMAND
    assert container["workingDir"] == "/app"
    assert pod["securityContext"]["runAsUser"] == pod["securityContext"]["runAsGroup"] == 10001
    assert pod["securityContext"].get("fsGroup") == (65534 if has_volume else None)
    assert pod["securityContext"].get("fsGroupChangePolicy") == ("OnRootMismatch" if has_volume else None)
    assert pod["automountServiceAccountToken"] is False
    env_by_name = {entry["name"]: entry for entry in container["env"]}
    assert env_by_name["HLTHPRT_WORKER_ONCE_TARGET_JOB_ID"]["value"] == JOB_ID
    assert env_by_name["SOURCE_USER"]["valueFrom"]["secretKeyRef"]["name"] == "synthetic-publisher"
    assert source.is_fhir_source_job_matching(manifest, admitted.fhir_source_launch)
    if has_volume:
        pod["securityContext"]["fsGroup"] = 10001
        assert not source.is_fhir_source_job_matching(manifest, admitted.fhir_source_launch)


@pytest.mark.parametrize("has_volume", [False, True])
def test_unadmitted_spec_keeps_shared_identity(has_volume):
    """The source-specific actor override never changes ordinary worker security."""
    pod_spec_dict = {"securityContext": workers._worker_job_pod_security_context(has_group_read_volume=has_volume)}
    original = deepcopy(pod_spec_dict)
    labels_by_key = {}
    source.apply_fhir_source_worker_spec(SPEC, {"run_id": RUN_ID}, labels_by_key, pod_spec_dict)
    assert pod_spec_dict == original and labels_by_key == {}
    assert pod_spec_dict["securityContext"]["runAsUser"] == pod_spec_dict["securityContext"]["runAsGroup"] == 65534


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
    assert not source.is_fhir_source_job_matching(manifest, admitted.fhir_source_launch)


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
    monkeypatch.setenv(source.IMAGE_ENV, IMAGE)
    observed = AsyncMock(return_value=_recorded())

    @asynccontextmanager
    async def connection():
        yield object()

    monkeypatch.setattr(workers.db, "acquire", connection)
    monkeypatch.setattr(workers, "acquire_control_run_worker_action_lock", AsyncMock())
    monkeypatch.setattr(workers, "admit_existing_outer_run_action", observed)
    launched_requests = []
    monkeypatch.setattr(
        workers, "ensure_worker", lambda request: launched_requests.append(request) or {"status": "started"}
    )
    assert await workers.guarded_ensure_worker({"run_id": RUN_ID, "importer": "provider-directory-fhir"}) == {
        "status": "started"
    }
    observed.assert_awaited_once()
    assert len(launched_requests) == 1
    assert source.authorized_fhir_source_worker_launch(SPEC, launched_requests[0]).job_id == JOB_ID


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
    assert source.is_fhir_source_job_matching(calls[0][2], admitted.fhir_source_launch)


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


@pytest.mark.parametrize("with_job_id", [False, True], ids=["api", "scheduled"])
@pytest.mark.asyncio
async def test_real_ensure_shapes_select_only_the_stored_fhir_job(monkeypatch, with_job_id):
    """API and scheduled-client request shapes cross the same locked admission path."""
    monkeypatch.setenv(source.IMAGE_ENV, IMAGE)

    @asynccontextmanager
    async def connection():
        yield object()

    monkeypatch.setattr(workers.db, "acquire", connection)
    monkeypatch.setattr(workers, "acquire_control_run_worker_action_lock", AsyncMock())
    observed = AsyncMock(return_value=_recorded())
    monkeypatch.setattr(workers, "admit_existing_outer_run_action", observed)
    admitted_requests = []
    monkeypatch.setattr(
        workers, "ensure_worker", lambda request: admitted_requests.append(request) or {"status": "started"}
    )
    request_by_field = {"run_id": RUN_ID, "importer": "provider-directory-fhir"}
    if with_job_id:
        request_by_field.update(queue=SPEC.queue, job_id=JOB_ID)
    assert await workers.guarded_ensure_worker(request_by_field) == {"status": "started"}
    assert len(admitted_requests) == 1
    admitted = admitted_requests[0]
    assert source.authorized_fhir_source_worker_launch(SPEC, admitted).job_id == JOB_ID
    assert admitted.cms_registry_launch is None
    assert observed.await_args.kwargs["run_id"] == RUN_ID


def test_signed_cms_purpose_has_precedence_without_ordinary_credentials(admitted, monkeypatch):
    """An ordinary capability cannot replace an admitted signed CMS process."""
    from api import provider_directory_cms_registry_worker as cms

    cms_image = "example/cms-worker@sha256:" + "e" * 64
    admitted.cms_registry_launch = cms.CMSRegistryWorkerLaunch(RUN_ID, cms_image, "f" * 64)
    admitted["job_id"] = JOB_ID
    assert source.authorized_fhir_source_worker_launch(SPEC, admitted) is None
    manifest = workers._worker_job_manifest(SPEC, admitted, ORDINARY_IMAGE)
    pod = manifest["spec"]["template"]["spec"]
    assert pod["containers"][0]["image"] == cms_image
    assert pod["containers"][0]["command"] == cms.COMMAND
    assert pod["containers"][0]["workingDir"] == "/app"
    assert manifest["metadata"]["labels"]["healthporta.com/worker-purpose"] == cms.PURPOSE
    assert not source.is_fhir_source_secret_selected(
        {"purposes": [source.PURPOSE], "importers": ["provider-directory-fhir"], "workerClasses": [SPEC.worker_class]},
        SPEC.worker_class,
        admitted,
    )


def test_source_policy_path_uses_deployment_value_only(admitted, monkeypatch):
    setting = "HLTHPRT_SOURCE_PROFILE_ROLE_POLICY_FILE"
    monkeypatch.setenv(setting, "/synthetic-readonly/source-roles.json")
    admitted[setting] = "/untrusted/roles.json"
    environment, _ = workers._worker_job_environment(SPEC, admitted)
    assert [entry for entry in environment if entry["name"] == setting] == [
        {"name": setting, "value": "/synthetic-readonly/source-roles.json"}
    ]


@pytest.mark.asyncio
async def test_missing_paired_image_refuses_before_worker_launch(monkeypatch):
    monkeypatch.delenv(source.IMAGE_ENV, raising=False)
    monkeypatch.delenv(source.REFERENCE_IMAGE_ENV, raising=False)
    request = workers._AdmittedWorkerRequest(run_id=RUN_ID, importer="provider-directory-fhir")
    workers._bind_admitted_job(request, _recorded())
    result = await workers._admit_source_worker_launches(request, _recorded())
    assert result["status"] == "failed" and request.fhir_source_launch is None


@pytest.mark.asyncio
async def test_cms_admission_never_constructs_an_ordinary_source_capability(monkeypatch):
    from api import provider_directory_cms_registry_worker as cms

    cms_launch = cms.CMSRegistryWorkerLaunch(RUN_ID, IMAGE, "f" * 64)
    monkeypatch.setattr(cms, "admit_cms_registry_worker_launch", AsyncMock(return_value=cms_launch))
    ordinary = AsyncMock(side_effect=AssertionError("ordinary admission replaced signed CMS"))
    monkeypatch.setattr(source, "admit_fhir_source_worker_launch", ordinary)
    request = workers._AdmittedWorkerRequest(run_id=RUN_ID, importer="provider-directory-fhir")
    workers._bind_admitted_job(request, _recorded())
    assert await workers._admit_source_worker_launches(request, _recorded()) is None
    assert request.cms_registry_launch is cms_launch and request.fhir_source_launch is None
    ordinary.assert_not_called()


@pytest.fixture
def mounted_job(admitted, secret_selection, monkeypatch):
    """Keep the real generator's trusted work PVC and readonly Source policy mount."""
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_PVC_NAME", "synthetic-work")
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_PVC_MOUNT_PATH", "/work")
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
    return workers._worker_job_manifest(SPEC, admitted, ORDINARY_IMAGE)


def _change_job_bindings(manifest, drift):
    pod = manifest["spec"]["template"]["spec"]
    container = pod["containers"][0]
    if drift in {"postStart", "preStop"}:
        container["lifecycle"] = {drift: {"exec": {"command": ["/synthetic-hook"]}}}
        return
    if drift in {"/app", "/opt", "/usr/local/bin"}:
        pod["volumes"].append({"name": "unreviewed", "emptyDir": {}})
        container["volumeMounts"].append({"name": "unreviewed", "mountPath": drift})
        return
    if drift == "mount-path":
        container["volumeMounts"][0]["mountPath"] = "/app"
        return
    if drift == "pvc-source":
        pod["volumes"][0]["persistentVolumeClaim"]["claimName"] = "foreign-work"
        return
    if drift == "secret-source":
        pod["volumes"][1]["secret"]["secretName"] = "foreign-policy"
        return
    if drift == "secret-subpath":
        container["volumeMounts"][1]["subPath"] = "foreign.json"
        return
    if drift == "secret-writable":
        container["volumeMounts"][1]["readOnly"] = False
        return
    if drift == "secret-mode":
        pod["volumes"][1]["secret"]["defaultMode"] = 0o644
        return
    if drift == "volume-device":
        container["volumeDevices"] = [{"name": "source-policy", "devicePath": "/usr/local/bin/fhir-source-worker"}]
        return
    _change_binding_members(pod, container, drift)


def _change_binding_members(pod, container, drift):
    if drift == "extra-volume":
        pod["volumes"].append({"name": "unreviewed", "emptyDir": {}})
        return
    if drift == "missing-volume":
        pod["volumes"].pop()
        return
    if drift == "missing-mount":
        container["volumeMounts"].pop()
        return
    if drift == "duplicate-volume":
        pod["volumes"].append(deepcopy(pod["volumes"][0]))
        return
    if drift == "duplicate-mount":
        container["volumeMounts"].append(deepcopy(container["volumeMounts"][0]))
        return
    if drift == "swapped-name":
        container["volumeMounts"][0]["name"] = "source-policy"
        return
    raise AssertionError("unknown regression drift")


@pytest.mark.parametrize(
    "drift",
    [
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
def test_existing_job_refuses_unreviewed_process_or_mounts(admitted, mounted_job, drift):
    assert source.is_fhir_source_job_matching(mounted_job, admitted.fhir_source_launch)
    _change_job_bindings(mounted_job, drift)
    assert not source.is_fhir_source_job_matching(mounted_job, admitted.fhir_source_launch)


def test_existing_job_preserves_trusted_pvc_and_secret_bindings(admitted, mounted_job):
    pod = mounted_job["spec"]["template"]["spec"]
    assert pod["volumes"][0]["persistentVolumeClaim"]["claimName"] == "synthetic-work"
    assert pod["volumes"][1]["secret"]["defaultMode"] == 0o440
    assert pod["containers"][0]["volumeMounts"][1] == {
        "name": "source-policy",
        "mountPath": "/etc/source-policy",
        "readOnly": True,
        "subPath": "roles.json",
    }
    assert source.is_fhir_source_job_matching(mounted_job, admitted.fhir_source_launch)


def test_existing_job_accepts_only_the_secret_api_default(admitted, mounted_job, monkeypatch):
    """Kubernetes defaults an omitted Secret volume defaultMode to 0644."""
    setting = "HLTHPRT_WORKER_JOB_SECRET_VOLUME_MOUNTS_JSON"
    mounts = json.loads(workers.os.environ[setting])
    del mounts[0]["defaultMode"]
    monkeypatch.setenv(setting, json.dumps(mounts))
    manifest = workers._worker_job_manifest(SPEC, admitted, ORDINARY_IMAGE)
    secret = manifest["spec"]["template"]["spec"]["volumes"][1]["secret"]
    assert "defaultMode" not in secret
    assert source.is_fhir_source_job_matching(manifest, admitted.fhir_source_launch)
    secret["defaultMode"] = 0o644
    assert source.is_fhir_source_job_matching(manifest, admitted.fhir_source_launch)


@pytest.mark.parametrize("drift", ["postStart", "/app"])
def test_ensure_refuses_existing_job_code_changes(admitted, mounted_job, monkeypatch, drift):
    """The real state/ensure path cannot reuse an active Job with unreviewed code."""
    _change_job_bindings(mounted_job, drift)
    mounted_job["status"] = {"active": 1}
    monkeypatch.setattr(workers, "_is_kubernetes_configured", lambda: True)
    monkeypatch.setattr(workers, "_launcher_mode", lambda: "kubernetes")
    calls = []
    monkeypatch.setattr(workers, "_kubernetes_request", lambda *args: calls.append(args) or {"items": [mounted_job]})
    result = workers._ensure_spec(SPEC, admitted)
    assert result["status"] == "failed" and result["job_status"] == "purpose_mismatch"
    assert len(calls) == 1 and calls[0][0] == "GET"
