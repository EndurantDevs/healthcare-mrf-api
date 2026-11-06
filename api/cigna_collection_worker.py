# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Select an optional collection image only for its admitted PTG source attempt."""

import os
from dataclasses import dataclass

from api.reference_source_worker import _IMAGE
from process.ptg_parts.canonical import canonical_json_dumps, sha256_bytes
from process.ptg_parts.cigna_toc_collection import (
    COLLECTION_PROOF_PARAM,
    validate_collection_proof,
)
from process.ptg_parts.cigna_toc_dispatch import validated_collection_dispatch
from process.ptg_parts.ptg_source_attempt_guard import (
    source_file_import_id_from_payload,
)

IMAGE_ENV = "HLTHPRT_CIGNA_COLLECTION_WORKER_IMAGE"


@dataclass(frozen=True)
class CollectionWorkerJob:
    """Keep persisted queue and source coordinates outside serialized request fields."""

    identity: tuple[str, str, str, str, str]
    source_file_import_id: str
    source_key: str
    proof_sha256: str


def _proof_digest(proof):
    return sha256_bytes(canonical_json_dumps(proof).encode("utf-8"))


def _require_request(job, request):
    """A caller may identify the admitted target, never replace its inputs or image."""
    if (
        request.get("run_id") != job.identity[0]
        or request.get("importer", "ptg") != "ptg"
        or request.get("job_id") not in (None, "", job.identity[4])
        or request.get("queue") not in (None, "", job.identity[2])
        or request.get("source_key") not in (None, "", job.source_key)
        or request.get("image") not in (None, "")
    ):
        raise ValueError("collection_worker_request_changed")
    source_id = source_file_import_id_from_payload(request, required=False)
    if source_id is not None and source_id != job.source_file_import_id:
        raise ValueError("collection_worker_source_changed")
    for view in (request, request.get("params") or {}):
        for field in (COLLECTION_PROOF_PARAM, "cigna_toc_collection_proof"):
            if field in view:
                supplied = validate_collection_proof(view[field])
                if _proof_digest(supplied) != job.proof_sha256:
                    raise ValueError("collection_worker_proof_changed")


def _admitted_job(admitted_run, request):
    from api.control_imports import (
        _PTG_CONTROL_QUEUES,
        _adapter_for_import_row,
        _enqueue_job_options,
    )
    from api.control_workers import _BY_QUEUE, _AdmittedWorkerRequest

    if admitted_run.get("importer") != "ptg":
        return None
    params = admitted_run.get("params") or {}
    proof = validated_collection_dispatch("ptg", params)
    if proof is None:
        return None
    if type(request) is not _AdmittedWorkerRequest:
        raise ValueError("collection_worker_admission_required")
    source_id = source_file_import_id_from_payload(admitted_run, required=True)
    adapter = _adapter_for_import_row(admitted_run)
    metrics = admitted_run.get("metrics") or {}
    queue = adapter["queue"]
    spec = _BY_QUEUE.get(queue)
    job_id = _enqueue_job_options(adapter, admitted_run).get("_job_id")
    identity = (admitted_run.get("run_id"), "ptg", queue, "ptg_control_start", job_id)
    if (
        queue not in _PTG_CONTROL_QUEUES
        or spec is None
        or (spec.worker_class, spec.importers, spec.role)
        != ("process." + queue.removeprefix("arq:"), ("ptg",), "start")
        or (adapter.get("function"), adapter.get("payload"), adapter.get("job_prefix"))
        != ("ptg_control_start", "ptg_control", "ptg_start")
        or any(
            type(coordinate) is not str or not coordinate or coordinate != coordinate.strip() for coordinate in identity
        )
        or tuple(metrics.get(field) for field in ("queue", "function", "job_id")) != identity[2:]
        or request.admitted_job != identity
        or params.get("_expected_worker_class") not in (None, "", spec.worker_class)
        or metrics.get("worker_class") not in (None, "", spec.worker_class)
    ):
        raise ValueError("collection_worker_admission_changed")
    job = CollectionWorkerJob(identity, source_id, params["source_key"], _proof_digest(proof))
    _require_request(job, request)
    return job


def admit_collection_worker(admitted_run, request):
    """Capture only after the caller's existing persisted source-attempt admission."""
    from api.control_workers import _failed_worker_admission

    try:
        request.cigna_collection_job = None
        request.cigna_collection_job = _admitted_job(admitted_run, request)
    except ValueError, TypeError, KeyError:
        return _failed_worker_admission(request, "Collection worker admission is unavailable")
    return None


def authorized_collection_worker_job(spec, request):
    """Require the exact server-held admission and unchanged worker request."""
    from api.control_imports import _PTG_CONTROL_QUEUES
    from api.control_workers import _BY_QUEUE, _AdmittedWorkerRequest

    if type(request) is not _AdmittedWorkerRequest:
        return None
    job = request.cigna_collection_job
    if job is None:
        return None
    if (
        type(job) is not CollectionWorkerJob
        or request.admitted_job != job.identity
        or job.identity[2] not in _PTG_CONTROL_QUEUES
        or spec != _BY_QUEUE.get(job.identity[2])
        or request.get("worker_class") not in (None, "", spec.worker_class)
        or request.get("role") not in (None, "", "start")
    ):
        raise ValueError("collection_worker_selection_changed")
    _require_request(job, request)
    return job


def configured_collection_worker_image(spec, request):
    """Select only the server-configured immutable image for an admitted job."""
    if authorized_collection_worker_job(spec, request) is None:
        return None
    image = os.getenv(IMAGE_ENV, "").strip()
    if not image:
        return None
    if _IMAGE.fullmatch(image) is None:
        raise ValueError("collection_worker_image_unavailable")
    return image


def collection_job_set_matches(spec, request, jobs):
    """Refuse reuse of another runtime or target for the admitted collection job."""
    image = configured_collection_worker_image(spec, request)
    if image is None:
        return True
    from api.control_workers import _worker_job_manifest
    from api.provider_directory_source_worker import is_overlay_job_matching

    expected_job = _worker_job_manifest(spec, request, image)
    return all(is_overlay_job_matching(job, expected_job) for job in jobs)
