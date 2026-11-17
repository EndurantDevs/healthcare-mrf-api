# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Select the shared retained-import runtime only for an admitted network job."""

import os
import re

IMAGE_ENV = "HLTHPRT_REGISTRY_NETWORK_WORKER_IMAGE"


def configured_network_import_image(spec, launch_request):
    """A request cannot select an image or borrow another single-job admission."""
    from api.control_workers import _AdmittedWorkerRequest

    if type(launch_request) is not _AdmittedWorkerRequest or launch_request.admitted_job is None:
        return None
    run_id = launch_request.get("run_id")
    if launch_request.admitted_job[1] != "registry-publication-worker":
        return None
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
    image = os.environ.get(IMAGE_ENV, "").strip()
    if not image:
        return None
    if re.fullmatch(r"[^\s@]+@sha256:[0-9a-f]{64}", image) is None:
        raise ValueError("registry_network_worker_image_invalid")
    return image
