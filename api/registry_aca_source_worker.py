# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Select one fixed ACA source bootstrap from an observed native queue job."""

import os
from dataclasses import dataclass

from api.reference_source_worker import _IMAGE, IMAGE_ENV

PURPOSE = "registry-aca-source"
COMMAND = ["/usr/local/bin/aca-registry-source-worker"]
_WORKER_CLASS = "process.RegistryACASource"
_IMPORTER = "registry-aca-source-worker"
_QUEUE = "arq:RegistryACASource"
_FUNCTION = "control_single_job_start"


@dataclass(frozen=True)
class ACASourceWorkerLaunch:
    """Keep the owner image and observed native job outside serialized input."""

    run_id: str
    job_id: str
    image: str


def admit_aca_source_worker_launch(admitted_run):
    """Require the configured immutable source image and original queued identity."""
    if admitted_run.get("importer") != _IMPORTER:
        return None
    metrics = admitted_run.get("metrics")
    image = os.getenv(IMAGE_ENV, "")
    if (
        not isinstance(metrics, dict)
        or metrics.get("queue") != _QUEUE
        or metrics.get("function") != _FUNCTION
        or any(
            type(identity) is not str or not identity or identity != identity.strip()
            for identity in (admitted_run.get("run_id"), metrics.get("job_id"))
        )
        or _IMAGE.fullmatch(image) is None
    ):
        raise ValueError("aca_source_worker_admission_unavailable")
    return ACASourceWorkerLaunch(admitted_run["run_id"], metrics["job_id"], image)


def authorized_aca_source_worker_launch(spec, launch_request):
    """Accept only the exact server-held capability and closed engine adapter."""
    from api.control_imports import _SINGLE_JOB_ADAPTERS
    from api.control_workers import _AdmittedWorkerRequest

    if type(launch_request) is not _AdmittedWorkerRequest:
        return None
    launch = getattr(launch_request, "aca_source_launch", None)
    if launch is None:
        return None
    adapter = _SINGLE_JOB_ADAPTERS.get(_IMPORTER, {})
    if (
        type(launch) is not ACASourceWorkerLaunch
        or (spec.worker_class, spec.queue, spec.importers, spec.role) != (_WORKER_CLASS, _QUEUE, (_IMPORTER,), "start")
        or launch_request.admitted_job != (launch.run_id, _IMPORTER, _QUEUE, _FUNCTION, launch.job_id)
        or launch_request.get("run_id") != launch.run_id
        or launch_request.get("importer", _IMPORTER) != _IMPORTER
        or launch_request.get("job_id") not in (None, "", launch.job_id)
        or tuple(
            adapter.get(name)
            for name in ("queue", "function", "payload", "target_module", "target_function", "job_prefix")
        )
        != (
            _QUEUE,
            _FUNCTION,
            "control_wrapped",
            "process.registry_aca_source_worker",
            "control_start",
            "registry_aca_source",
        )
        or launch.job_id != "registry_aca_source_" + launch.run_id
    ):
        raise ValueError("aca_source_worker_admission_unavailable")
    return launch


def apply_aca_source_worker_spec(spec, launch_request, labels, pod_spec):
    """Use the fixed non-root source runtime only after native purpose admission."""
    if authorized_aca_source_worker_launch(spec, launch_request) is not None:
        labels["healthporta.com/worker-purpose"] = PURPOSE
        security = pod_spec["securityContext"]
        security.update(runAsUser=10001, runAsGroup=10001)
        if "fsGroup" in security:
            security["fsGroup"] = 10001


def is_aca_source_secret_selected(selection, worker_class, launch_request):
    """Select source credentials only for the exact admitted ACA queue purpose."""
    classes = selection.get("workerClasses", selection.get("worker_classes"))
    if (
        selection.get("purposes") != [PURPOSE]
        or selection.get("importers") != [_IMPORTER]
        or classes != [_WORKER_CLASS]
        or ("workerClasses" in selection and "worker_classes" in selection)
    ):
        raise ValueError("worker purpose secret selector is invalid")
    if worker_class != _WORKER_CLASS:
        return False
    from api.control_workers import _BY_WORKER_CLASS

    spec = _BY_WORKER_CLASS.get(_WORKER_CLASS)
    return spec is not None and authorized_aca_source_worker_launch(spec, launch_request) is not None


def is_aca_source_job_matching(job, launch):
    """Refuse changed bootstrap code, native target or process isolation on reuse."""
    from api.control_workers import _worker_job_container_security_context, _worker_job_pod_security_context

    if type(launch) is not ACASourceWorkerLaunch:
        return False
    try:
        template = job["spec"]["template"]
        pod = template["spec"]
        containers = pod["containers"]
        security = pod["securityContext"]
        expected_security = _worker_job_pod_security_context(has_group_read_volume="fsGroup" in security)
        expected_security.update(runAsUser=10001, runAsGroup=10001)
        if "fsGroup" in expected_security:
            expected_security["fsGroup"] = 10001
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
