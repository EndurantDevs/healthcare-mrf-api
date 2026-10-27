# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Select one fixed FHIR source bootstrap from an observed native queue job."""

import os
from dataclasses import dataclass
from importlib import import_module

from api.reference_source_worker import _IMAGE
from api.reference_source_worker import IMAGE_ENV as REFERENCE_IMAGE_ENV

PURPOSE = "provider-directory-source"
COMMAND = ["/usr/local/bin/fhir-source-worker"]
IMAGE_ENV = "HLTHPRT_PROVIDER_DIRECTORY_SOURCE_WORKER_IMAGE"
_WORKER_CLASS = "process.ProviderDirectoryFHIR"
_IMPORTER = "provider-directory-fhir"
_QUEUE = "arq:ProviderDirectoryFHIR"
_FUNCTION = "control_single_job_start"


@dataclass(frozen=True)
class FHIRSourceWorkerLaunch:
    """Keep the owner image and observed native job outside serialized input."""

    run_id: str
    job_id: str
    image: str


def admit_fhir_source_worker_launch(admitted_run):
    """Require the configured immutable source image and original queued identity."""
    if admitted_run.get("importer") != _IMPORTER:
        return None
    metrics = admitted_run.get("metrics")
    image = os.getenv(IMAGE_ENV, os.getenv(REFERENCE_IMAGE_ENV, ""))
    if (
        not isinstance(metrics, dict)
        or metrics.get("queue") != _QUEUE
        or metrics.get("function") != _FUNCTION
        or any(
            type(identity) is not str or not identity or identity != identity.strip()
            for identity in (admitted_run.get("run_id"), metrics.get("job_id"))
        )
    ):
        raise ValueError("fhir_source_worker_admission_unavailable")
    params_by_name = admitted_run.get("params")
    if isinstance(params_by_name, dict) and params_by_name.get("provider_directory_dataset_followup_stage") == "address_overlay":
        from process.provider_directory_generic_source_artifacts import ordinary_overlay_followup

        directory = import_module("process.provider_directory_fhir")
        if ordinary_overlay_followup(directory, params_by_name) is not None:
            if {"test_mode", "run_id"}.intersection(params_by_name):
                raise ValueError("fhir_source_worker_admission_unavailable")
            return None
    if _IMAGE.fullmatch(image) is None:
        raise ValueError("fhir_source_worker_admission_unavailable")
    return FHIRSourceWorkerLaunch(admitted_run["run_id"], metrics["job_id"], image)


def authorized_fhir_source_worker_launch(spec, launch_request):
    """Accept only the exact server-held capability and closed engine adapter."""
    from api.control_workers import _AdmittedWorkerRequest

    if type(launch_request) is not _AdmittedWorkerRequest:
        return None
    if getattr(launch_request, "cms_registry_launch", None) is not None:
        return None
    launch = getattr(launch_request, "fhir_source_launch", None)
    if launch is None:
        return None
    if type(launch) is not FHIRSourceWorkerLaunch:
        raise ValueError("fhir_source_worker_admission_unavailable")
    _require_fhir_job_identity(spec, launch_request, launch.run_id, launch.job_id)
    return launch


def admitted_overlay_job(spec, launch_request):
    """Select ordinary execution only for the overlay classified from the native run."""
    from api.control_workers import _AdmittedWorkerRequest

    if type(launch_request) is not _AdmittedWorkerRequest:
        return None
    identity = launch_request.ordinary_fhir_overlay_job
    if identity is None:
        return None
    if (
        type(identity) is not tuple or len(identity) != 5
        or identity != launch_request.admitted_job
        or launch_request.cms_registry_launch is not None or launch_request.fhir_source_launch is not None
    ):
        raise ValueError("fhir_source_worker_admission_unavailable")
    _require_fhir_job_identity(spec, launch_request, identity[0], identity[4])
    return identity[4]


def _require_fhir_job_identity(spec, launch_request, run_id, job_id):
    """Keep both runtimes on the same exact native adapter and queue identity."""
    from api.control_imports import _SINGLE_JOB_ADAPTERS

    adapter = _SINGLE_JOB_ADAPTERS.get(_IMPORTER, {})
    if (
        (spec.worker_class, spec.queue, spec.importers, spec.role) != (_WORKER_CLASS, _QUEUE, (_IMPORTER,), "start")
        or launch_request.admitted_job != (run_id, _IMPORTER, _QUEUE, _FUNCTION, job_id)
        or launch_request.get("run_id") != run_id
        or launch_request.get("importer", _IMPORTER) != _IMPORTER
        or launch_request.get("job_id") not in (None, "", job_id)
        or tuple(
            adapter.get(name)
            for name in ("queue", "function", "payload", "target_module", "target_function", "job_prefix")
        )
        != (
            _QUEUE,
            _FUNCTION,
            "control_wrapped",
            "process.provider_directory_fhir",
            "process_data",
            "provider_directory_start",
        )
        or adapter.get("run_shutdown") is not True
        or job_id != "provider_directory_start_" + run_id
    ):
        raise ValueError("fhir_source_worker_admission_unavailable")


def apply_fhir_source_worker_spec(spec, launch_request, labels, pod_spec):
    """Use the fixed non-root source runtime only after native purpose admission."""
    if authorized_fhir_source_worker_launch(spec, launch_request) is not None:
        labels["healthporta.com/worker-purpose"] = PURPOSE
        security = pod_spec["securityContext"]
        # Preserve the shared volume group to avoid rewriting existing file ownership.
        security.update(runAsUser=10001, runAsGroup=10001)


def _is_fhir_secret_scope(selection, worker_class):
    """Keep environment and mount selectors on the same exact Source purpose."""
    classes = selection.get("workerClasses", selection.get("worker_classes"))
    if (
        selection.get("purposes") != [PURPOSE]
        or selection.get("importers") != [_IMPORTER]
        or classes != [_WORKER_CLASS]
        or ("workerClasses" in selection and "worker_classes" in selection)
    ):
        raise ValueError("worker purpose secret selector is invalid")
    return worker_class == _WORKER_CLASS


def is_fhir_source_secret_selected(selection, worker_class, launch_request):
    """Select source credentials only for the exact admitted FHIR queue purpose."""
    if not _is_fhir_secret_scope(selection, worker_class):
        return False
    from api.control_workers import _BY_WORKER_CLASS

    spec = _BY_WORKER_CLASS.get(_WORKER_CLASS)
    return spec is not None and authorized_fhir_source_worker_launch(spec, launch_request) is not None


def is_fhir_source_volume_selected(selection, worker_class, launch):
    """Use the existing Source capability for explicitly purpose-scoped mounts."""
    return _is_fhir_secret_scope(selection, worker_class) and type(launch) is FHIRSourceWorkerLaunch


def is_fhir_source_job_matching(job, launch):
    """Refuse changed bootstrap code, native target or process isolation on reuse."""
    from api.control_workers import (
        _BY_WORKER_CLASS,
        _worker_job_container_security_context,
        _worker_job_pod_security_context,
        _worker_job_pvc_volumes,
        _worker_job_secret_volumes,
    )

    if type(launch) is not FHIRSourceWorkerLaunch:
        return False
    try:
        template = job["spec"]["template"]
        pod = template["spec"]
        containers = pod["containers"]
        security = pod["securityContext"]
        volumes = [*_worker_job_pvc_volumes(), *_worker_job_secret_volumes(_BY_WORKER_CLASS[_WORKER_CLASS], launch)]
        expected_security = _worker_job_pod_security_context(has_group_read_volume="fsGroup" in security)
        expected_security.update(runAsUser=10001, runAsGroup=10001)
        return (
            job["metadata"]["labels"]["healthporta.com/worker-purpose"] == PURPOSE
            and template["metadata"]["labels"]["healthporta.com/worker-purpose"] == PURPOSE
            and security == expected_security
            and pod["automountServiceAccountToken"] is False
            and all(not pod.get(key, False) for key in ("hostPID", "hostIPC", "hostNetwork"))
            and not pod.get("initContainers")
            and not pod.get("ephemeralContainers")
            and len(containers) == 1
            and containers[0]["securityContext"] == _worker_job_container_security_context()
            and containers[0]["image"] == launch.image
            and containers[0]["command"] == COMMAND
            and containers[0]["workingDir"] == "/app"
            and not containers[0].get("args")
            and not containers[0].get("lifecycle")
            and not containers[0].get("volumeDevices")
            and containers[0].get("volumeMounts", []) == [volume["volumeMount"] for volume in volumes]
            and _secret_volume_defaults(pod.get("volumes", []))
            == _secret_volume_defaults([volume["volume"] for volume in volumes])
            and all(
                [entry.get("value") for entry in containers[0]["env"] if entry.get("name") == name] == [expected]
                for name, expected in (
                    ("HLTHPRT_CONTROL_RUN_ID", launch.run_id),
                    ("HLTHPRT_WORKER_ONCE_TARGET_JOB_ID", launch.job_id),
                )
            )
        )
    except KeyError, TypeError, IndexError:
        return False


def _secret_volume_defaults(volumes):
    """Retain every binding field while allowing the API's Secret default 0644."""
    return [
        {**volume, "secret": {"defaultMode": 0o644, **volume["secret"]}} if "secret" in volume else volume
        for volume in volumes
    ]


def is_overlay_job_matching(job, expected_job):
    """Refuse reuse of a different runtime or native target for an ordinary overlay."""
    try:
        template_by_field = job["spec"]["template"]
        pod_by_field = template_by_field["spec"]
        expected_pod_by_field = expected_job["spec"]["template"]["spec"]
        containers = pod_by_field["containers"]
        expected_container_by_field = expected_pod_by_field["containers"][0]
        return (
            len(containers) == 1
            and all(containers[0].get(name) == expected_container_by_field.get(name) for name in (
                "image", "imagePullPolicy", "workingDir", "command", "env", "envFrom", "securityContext", "volumeMounts",
            ))
            and not any(containers[0].get(name) for name in ("args", "lifecycle", "volumeDevices"))
            and not any(pod_by_field.get(name) for name in (
                "initContainers", "ephemeralContainers", "hostPID", "hostIPC", "hostNetwork",
            ))
            and pod_by_field["automountServiceAccountToken"] is False
            and pod_by_field.get("serviceAccountName", "default") == expected_pod_by_field.get("serviceAccountName", "default")
            and pod_by_field["securityContext"] == expected_pod_by_field["securityContext"]
            and _secret_volume_defaults(pod_by_field.get("volumes", []))
            == _secret_volume_defaults(expected_pod_by_field.get("volumes", []))
            and all("healthporta.com/worker-purpose" not in metadata.get("labels", {})
                    for metadata in (job["metadata"], template_by_field["metadata"]))
            and job["metadata"]["annotations"] == expected_job["metadata"]["annotations"]
        )
    except KeyError, TypeError, IndexError:
        return False


def is_fhir_job_set_matching(spec, launch_request, jobs):
    """Check existing FHIR Jobs against the runtime selected by native admission."""
    launch = authorized_fhir_source_worker_launch(spec, launch_request)
    if launch is not None:
        return all(is_fhir_source_job_matching(job, launch) for job in jobs)
    if admitted_overlay_job(spec, launch_request) is None:
        return True
    from api.control_workers import _worker_job_manifest

    expected_job = _worker_job_manifest(spec, launch_request, os.getenv("HLTHPRT_WORKER_JOB_IMAGE", "").strip())
    return all(is_overlay_job_matching(job, expected_job) for job in jobs)
