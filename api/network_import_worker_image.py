# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Select the shared retained-import runtime only for an admitted network job."""

import os
import re

IMAGE_ENV = "HLTHPRT_REGISTRY_NETWORK_WORKER_IMAGE"


def is_network_import_job(spec, launch_request):
    """A request cannot borrow another single-job admission."""
    from api.control_workers import _AdmittedWorkerRequest

    if type(launch_request) is not _AdmittedWorkerRequest or launch_request.admitted_job is None:
        return False
    run_id = launch_request.get("run_id")
    if launch_request.admitted_job[1] != "registry-publication-worker":
        return False
    expected = (
        run_id,
        "registry-publication-worker",
        "arq:PTGCandidateAudit",
        "control_single_job_start",
        "registry_publication_" + str(run_id),
    )
    if (
        launch_request.admitted_job != expected
        or launch_request.get("importer") != expected[1]
        or (spec.worker_class, spec.queue, spec.role) != ("process.PTGCandidateAudit", expected[2], "start")
    ):
        raise ValueError("registry_network_worker_admission_changed")
    return True


def network_import_job_environment(spec, launch_request):
    """A full network rebuild runs for many hours; the shared queue deadline is sized for audits."""
    if not is_network_import_job(spec, launch_request):
        return []
    return [{"name": "HLTHPRT_PTG_CANDIDATE_AUDIT_JOB_TIMEOUT", "value": "172800"}]


def configured_network_import_image(spec, launch_request):
    """A request cannot select an image; only the server configuration names it."""
    if not is_network_import_job(spec, launch_request):
        return None
    image = os.environ.get(IMAGE_ENV, "").strip()
    if not image:
        return None
    if re.fullmatch(r"[^\s@]+@sha256:[0-9a-f]{64}", image) is None:
        raise ValueError("registry_network_worker_image_invalid")
    return image
