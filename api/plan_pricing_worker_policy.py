# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Select the release reader image and refuse reserved pricing worker launches."""

import os

from db.plan_release_storage import plan_release_schema, storage_relation
from process.plan_pricing_admission import require, require_unreserved_pricing_run

RELEASE_IMAGE_ENV = "HLTHPRT_PLAN_RELEASE_WORKER_IMAGE"
_RELEASE_READER_IMPORTERS = frozenset(
    {
        "plan-pricing-prewarm",
        "plan-pricing-code-projection",
        "plan-pricing-em-distance",
        "plan-pricing-em-attribution",
    }
)


def _is_release_reader_job(spec, request):
    from api import control_workers as workers
    from api.control_imports import _SINGLE_JOB_ADAPTERS, _enqueue_job_options

    if type(request) is not workers._AdmittedWorkerRequest:
        return False
    identity = request.admitted_job
    importer = identity[1] if type(identity) is tuple and len(identity) == 5 else None
    if importer not in _RELEASE_READER_IMPORTERS and request.get("importer") not in _RELEASE_READER_IMPORTERS:
        return False
    require(importer in _RELEASE_READER_IMPORTERS, "worker_image_admission_required")
    require(
        all(type(coordinate) is str and coordinate and coordinate == coordinate.strip() for coordinate in identity),
        "worker_image_admission_required",
    )
    adapter = _SINGLE_JOB_ADAPTERS[importer]
    require(
        spec == workers._BY_IMPORTER_ROLE[(importer, "start")]
        and spec.queue == "arq:PTGCandidateAudit"
        and spec.worker_class == "process.PTGCandidateAudit"
        and spec.role == "start"
        and identity
        == (
            request.get("run_id"),
            importer,
            adapter["queue"],
            adapter["function"],
            _enqueue_job_options(adapter, {"run_id": request.get("run_id")})["_job_id"],
        ),
        "worker_image_selection_changed",
    )
    require(
        request.get("importer", importer) == importer
        and all(
            request.get(field) in (None, "", expected)
            for field, expected in (
                ("queue", identity[2]),
                ("job_id", identity[4]),
                ("worker_class", spec.worker_class),
                ("role", "start"),
            )
        )
        and request.get("image") in (None, ""),
        "worker_image_request_changed",
    )
    return not workers._is_code_calibration_job(spec, request)


def configured_release_worker_image(spec, request):
    """Use a server-configured image only for an exact admitted release reader."""
    from api.reference_source_worker import _IMAGE

    image = os.getenv(RELEASE_IMAGE_ENV, "").strip()
    if not image or not _is_release_reader_job(spec, request):
        return None
    require(_IMAGE.fullmatch(image) is not None, "worker_image_unavailable")
    return image


def release_worker_job_set_matches(spec, request, jobs):
    """Refuse old images or changed execution settings before reusing a Job."""
    image = configured_release_worker_image(spec, request)
    if image is None:
        return True
    from api.control_workers import _worker_job_manifest
    from api.provider_directory_source_worker import is_overlay_job_matching

    expected = _worker_job_manifest(spec, request, image)
    return all(
        is_overlay_job_matching(job, expected)
        and job["spec"].get("activeDeadlineSeconds") == expected["spec"].get("activeDeadlineSeconds")
        and job["spec"]["template"]["spec"].get("activeDeadlineSeconds")
        == expected["spec"]["template"]["spec"].get("activeDeadlineSeconds")
        for job in jobs
    )


async def admit_pricing_worker(connection, run_id):
    """Refuse a reserved persisted run on the existing worker-action transaction."""
    native_run = await connection.fetchrow(
        f"SELECT * FROM {storage_relation(plan_release_schema(), 'import_run')} WHERE run_id=$1", run_id
    )
    require(native_run is not None, "worker_run_missing")
    require_unreserved_pricing_run(dict(native_run))
