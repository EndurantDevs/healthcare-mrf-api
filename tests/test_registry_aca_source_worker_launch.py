# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fixed ACA bootstrap capability and native job matching without cluster calls."""

import json
from copy import deepcopy
from dataclasses import asdict, replace

import pytest

from api import control_imports, control_workers
from api import registry_aca_source_worker as aca

RUN_ID = "run_" + "a" * 32
JOB_ID = "registry_aca_source_" + RUN_ID
IMAGE = "example/source-worker@sha256:" + "c" * 64
SPEC = control_workers.WorkerSpec("arq:RegistryACASource", "process.RegistryACASource", ("registry-aca-source-worker",))
ADAPTER = {
    "queue": SPEC.queue,
    "function": "control_single_job_start",
    "payload": "control_wrapped",
    "target_module": "process.registry_aca_source_worker",
    "target_function": "control_start",
    "job_prefix": "registry_aca_source",
}


def _observed_run():
    return {
        "run_id": RUN_ID,
        "importer": "registry-aca-source-worker",
        "metrics": {"queue": SPEC.queue, "function": "control_single_job_start", "job_id": JOB_ID},
    }


@pytest.fixture
def admitted(monkeypatch):
    monkeypatch.setenv(aca.IMAGE_ENV, IMAGE)
    monkeypatch.setitem(control_imports._SINGLE_JOB_ADAPTERS, "registry-aca-source-worker", dict(ADAPTER))
    monkeypatch.setitem(control_workers._BY_WORKER_CLASS, SPEC.worker_class, SPEC)
    request = control_workers._AdmittedWorkerRequest(run_id=RUN_ID, importer="registry-aca-source-worker")
    observed = _observed_run()
    control_workers._bind_admitted_job(request, observed)
    request.aca_source_launch = aca.admit_aca_source_worker_launch(observed)
    return request


def _selection():
    return {
        "purposes": [aca.PURPOSE],
        "importers": ["registry-aca-source-worker"],
        "workerClasses": [SPEC.worker_class],
    }


def _job(admitted, *, has_group_read_volume=False):
    labels_by_key = {}
    pod_by_field = {
        "securityContext": control_workers._worker_job_pod_security_context(
            has_group_read_volume=has_group_read_volume
        ),
        "automountServiceAccountToken": False,
    }
    aca.apply_aca_source_worker_spec(SPEC, admitted, labels_by_key, pod_by_field)
    launch = aca.authorized_aca_source_worker_launch(SPEC, admitted)
    pod_by_field["containers"] = [
        {
            "image": launch.image,
            "command": list(aca.COMMAND),
            "workingDir": "/app",
            "securityContext": control_workers._worker_job_container_security_context(),
            "env": [
                {"name": "HLTHPRT_CONTROL_RUN_ID", "value": launch.run_id},
                {"name": "HLTHPRT_WORKER_ONCE_TARGET_JOB_ID", "value": launch.job_id},
            ],
        }
    ]
    return {
        "metadata": {"labels": dict(labels_by_key)},
        "spec": {"template": {"metadata": {"labels": dict(labels_by_key)}, "spec": pod_by_field}},
    }


def test_fixed_bootstrap_uses_server_image_and_actual_native_job(admitted, monkeypatch):
    admitted.update(image="example/ordinary:latest", argv=["other"], command=["other"], engine_source="/tmp")
    monkeypatch.setenv(aca.IMAGE_ENV, "example/later@sha256:" + "d" * 64)
    launch = aca.authorized_aca_source_worker_launch(SPEC, admitted)
    assert launch == aca.ACASourceWorkerLaunch(RUN_ID, JOB_ID, IMAGE)
    assert aca.COMMAND == ["/usr/local/bin/aca-registry-source-worker"]
    assert aca.IMAGE_ENV == "HLTHPRT_REFERENCE_SOURCE_WORKER_IMAGE"
    assert aca.is_aca_source_secret_selected(_selection(), SPEC.worker_class, admitted)
    assert not aca.is_aca_source_secret_selected(_selection(), "process.Geo", admitted)


@pytest.mark.parametrize("image", ["", "example/owner:latest", IMAGE[:-1] + "z", " " + IMAGE])
def test_missing_or_unqualified_owner_image_refuses_without_fallback(monkeypatch, image):
    monkeypatch.setenv(aca.IMAGE_ENV, image)
    with pytest.raises(ValueError, match="aca_source_worker_admission_unavailable"):
        aca.admit_aca_source_worker_launch(_observed_run())
    assert aca.admit_aca_source_worker_launch({"importer": "geo"}) is None


@pytest.mark.parametrize("field", ["queue", "function", "job_id", "run_id", "metrics"])
def test_observed_native_coordinates_are_required(monkeypatch, field):
    monkeypatch.setenv(aca.IMAGE_ENV, IMAGE)
    observed = _observed_run()
    if field in {"run_id", "metrics"}:
        observed[field] = None
    else:
        observed["metrics"][field] = "other" if field != "job_id" else " "
    with pytest.raises(ValueError, match="admission_unavailable"):
        aca.admit_aca_source_worker_launch(observed)


@pytest.mark.parametrize("index", range(5))
def test_every_admitted_native_coordinate_is_checked(admitted, index):
    coordinates = list(admitted.admitted_job)
    coordinates[index] = "different"
    admitted.admitted_job = tuple(coordinates)
    with pytest.raises(ValueError, match="admission_unavailable"):
        aca.authorized_aca_source_worker_launch(SPEC, admitted)


@pytest.mark.parametrize("field", ["run_id", "importer", "job_id"])
def test_request_cannot_redirect_observed_job(admitted, field):
    admitted[field] = "different"
    with pytest.raises(ValueError, match="admission_unavailable"):
        aca.authorized_aca_source_worker_launch(SPEC, admitted)


@pytest.mark.parametrize("field", ["worker_class", "queue", "role", "importers"])
def test_capability_refuses_foreign_actual_worker_spec(admitted, field):
    changed = replace(SPEC, **{field: ("other",) if field == "importers" else "other"})
    with pytest.raises(ValueError, match="admission_unavailable"):
        aca.authorized_aca_source_worker_launch(changed, admitted)


@pytest.mark.parametrize("field", tuple(ADAPTER))
def test_exact_engine_adapter_cannot_change(admitted, monkeypatch, field):
    monkeypatch.setitem(control_imports._SINGLE_JOB_ADAPTERS["registry-aca-source-worker"], field, "other")
    with pytest.raises(ValueError, match="admission_unavailable"):
        aca.authorized_aca_source_worker_launch(SPEC, admitted)


def test_serialized_capability_cannot_select_owner_image_or_source_credentials(admitted):
    raw = json.loads(
        json.dumps(
            {**admitted, "admitted_job": admitted.admitted_job, "aca_source_launch": asdict(admitted.aca_source_launch)}
        )
    )
    assert aca.authorized_aca_source_worker_launch(SPEC, raw) is None
    assert not aca.is_aca_source_secret_selected(_selection(), SPEC.worker_class, raw)
    admitted.aca_source_launch = asdict(admitted.aca_source_launch)
    with pytest.raises(ValueError, match="admission_unavailable"):
        aca.authorized_aca_source_worker_launch(SPEC, admitted)


@pytest.mark.parametrize("field", ["purposes", "importers", "workerClasses"])
def test_secret_selection_cannot_expand_owner_purpose(admitted, field):
    selection = _selection()
    selection[field].append("other")
    with pytest.raises(ValueError, match="selector is invalid"):
        aca.is_aca_source_secret_selected(selection, SPEC.worker_class, admitted)


def test_secret_selection_rejects_ambiguous_class_keys_or_missing_registered_worker(admitted, monkeypatch):
    selection = _selection() | {"worker_classes": [SPEC.worker_class]}
    with pytest.raises(ValueError, match="selector is invalid"):
        aca.is_aca_source_secret_selected(selection, SPEC.worker_class, admitted)
    monkeypatch.delitem(control_workers._BY_WORKER_CLASS, SPEC.worker_class)
    assert not aca.is_aca_source_secret_selected(_selection(), SPEC.worker_class, admitted)


@pytest.mark.parametrize("has_group_read_volume", [False, True])
def test_matching_job_uses_exact_source_actor_and_hardened_process(admitted, has_group_read_volume):
    job = _job(admitted, has_group_read_volume=has_group_read_volume)
    assert aca.is_aca_source_job_matching(job, admitted.aca_source_launch)
    security = job["spec"]["template"]["spec"]["securityContext"]
    assert security["runAsUser"] == security["runAsGroup"] == 10001
    assert security.get("fsGroup", 10001) == 10001


@pytest.mark.parametrize("field", ["initContainers", "ephemeralContainers"])
def test_extra_pod_processes_cannot_reuse_source_authority(admitted, field):
    job = _job(admitted)
    job["spec"]["template"]["spec"][field] = [{"name": "extra", "image": IMAGE}]
    assert not aca.is_aca_source_job_matching(job, admitted.aca_source_launch)


@pytest.mark.parametrize(
    "drift",
    [
        "image",
        "command",
        "args",
        "workingDir",
        "purpose",
        "run",
        "target",
        "duplicate_target",
        "actor",
        "hostNetwork",
        "service_account",
        "capabilities",
    ],
)
def test_existing_job_drift_cannot_reuse_admitted_source_authority(admitted, drift):
    job = _job(admitted)
    _damage_job(job, drift)
    assert not aca.is_aca_source_job_matching(job, admitted.aca_source_launch)


def _damage_job(job, drift):
    pod = job["spec"]["template"]["spec"]
    container = pod["containers"][0]
    if drift in {"image", "command", "args", "workingDir"}:
        container[drift] = ["other"] if drift in {"command", "args"} else "other"
        return
    if drift == "purpose":
        job["metadata"]["labels"]["healthporta.com/worker-purpose"] = "other"
        return
    if drift in {"run", "target", "duplicate_target"}:
        _change_job_identity(container, drift)
        return
    if drift == "actor":
        pod["securityContext"]["runAsUser"] = 65534
        return
    if drift == "hostNetwork":
        pod[drift] = True
        return
    if drift == "service_account":
        pod["automountServiceAccountToken"] = True
        return
    container["securityContext"]["capabilities"] = {"add": ["SYS_ADMIN"], "drop": ["ALL"]}


def _change_job_identity(container, drift):
    entry = container["env"][0 if drift == "run" else 1]
    if drift == "duplicate_target":
        container["env"].append(deepcopy(entry))
    else:
        entry["value"] = "different"


@pytest.mark.parametrize("job", [{}, {"spec": {}}, None])
def test_malformed_existing_job_refuses(job, admitted):
    assert not aca.is_aca_source_job_matching(job, admitted.aca_source_launch)
