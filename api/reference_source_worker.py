# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Select a fixed reference snapshot worker from an observed queued job."""

import os
import re
from dataclasses import dataclass

PURPOSE = "reference-source"
COMMAND = ["/usr/local/bin/reference-source-worker"]
IMAGE_ENV = "HLTHPRT_REFERENCE_SOURCE_WORKER_IMAGE"
_IMAGE = re.compile(r"[a-z0-9][a-z0-9./:_-]+@sha256:[0-9a-f]{64}\Z")
_WORKER_CLASS = "process.Geo"
_QUEUE = "arq:Geo"
_FUNCTION = "control_single_job_start"


@dataclass(frozen=True)
class ReferenceSourceWorkerLaunch:
    """Keep native admission outside serialized request fields."""

    run_id: str
    job_id: str
    image: str


def admit_reference_source_worker_launch(admitted_run):
    """Use the existing stored random enqueue ID, without inventing a target."""
    if admitted_run.get("importer") != "geo":
        return None
    metrics = admitted_run.get("metrics")
    image = os.getenv(IMAGE_ENV, "")
    if (
        not isinstance(metrics, dict)
        or metrics.get("queue") != _QUEUE
        or metrics.get("function") != _FUNCTION
        or any(
            not isinstance(identity, str) or not identity or identity != identity.strip()
            for identity in (admitted_run.get("run_id"), metrics.get("job_id"))
        )
        or _IMAGE.fullmatch(image) is None
    ):
        raise ValueError("reference_source_worker_admission_unavailable")
    return ReferenceSourceWorkerLaunch(admitted_run["run_id"], metrics["job_id"], image)


def authorized_reference_source_worker_launch(spec, launch_request):
    """Require the exact server-held capability and actual enqueue coordinates."""
    from api.control_imports import _SINGLE_JOB_ADAPTERS
    from api.control_workers import _AdmittedWorkerRequest

    if not isinstance(launch_request, _AdmittedWorkerRequest):
        return None
    launch = launch_request.reference_source_launch
    if launch is None:
        return None
    adapter = _SINGLE_JOB_ADAPTERS["geo"]
    if (
        type(launch) is not ReferenceSourceWorkerLaunch
        or (spec.worker_class, spec.queue, spec.importers, spec.role) != (_WORKER_CLASS, _QUEUE, ("geo",), "start")
        or launch_request.admitted_job != (launch.run_id, "geo", _QUEUE, _FUNCTION, launch.job_id)
        or launch_request.get("run_id") != launch.run_id
        or launch_request.get("importer", "geo") != "geo"
        or launch_request.get("job_id") not in (None, "", launch.job_id)
        or (adapter["queue"], adapter["function"], adapter["target_module"], adapter["target_function"])
        != (_QUEUE, _FUNCTION, "process.geo_import", "main")
    ):
        raise ValueError("reference_source_worker_admission_unavailable")
    return launch


def apply_reference_source_worker_spec(spec, launch_request, labels, pod_spec):
    """Keep the fixed source process isolated from ordinary workers."""
    if authorized_reference_source_worker_launch(spec, launch_request) is not None:
        labels["healthporta.com/worker-purpose"] = PURPOSE
        security = pod_spec["securityContext"]
        security.update(runAsUser=10001, runAsGroup=10001)
        if "fsGroup" in security:
            security["fsGroup"] = 10001


def is_reference_source_secret_selected(selection, worker_class, launch_request):
    """Select source credentials only for the exact admitted reference job."""
    classes = selection.get("workerClasses", selection.get("worker_classes"))
    if (
        selection.get("purposes") != [PURPOSE]
        or selection.get("importers") != ["geo"]
        or classes != [_WORKER_CLASS]
        or ("workerClasses" in selection and "worker_classes" in selection)
    ):
        raise ValueError("worker purpose secret selector is invalid")
    if worker_class != _WORKER_CLASS:
        return False
    from api.control_workers import _BY_WORKER_CLASS

    return authorized_reference_source_worker_launch(_BY_WORKER_CLASS[worker_class], launch_request) is not None


def is_reference_source_job_matching(job, launch):
    """Refuse a replacement image, command, identity or target on an existing job."""
    from api.control_workers import _worker_job_container_security_context, _worker_job_pod_security_context

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
            and len(containers) == 1
            and containers[0]["securityContext"] == _worker_job_container_security_context()
            and containers[0]["image"] == launch.image
            and containers[0]["command"] == COMMAND
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
