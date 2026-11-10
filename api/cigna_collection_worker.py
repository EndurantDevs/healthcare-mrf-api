# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Select an optional collection image only for its admitted PTG source attempt."""

import os
from contextlib import nullcontext
from dataclasses import dataclass
from typing import Any

from sanic.exceptions import SanicException
from sqlalchemy import select

from api.reference_source_worker import _IMAGE
from process.ptg_parts.canonical import canonical_json_dumps, sha256_bytes
from process.ptg_parts.cigna_toc_collection import (
    COLLECTION_PROOF_PARAM,
    validate_collection_proof,
)
from process.ptg_parts.cigna_toc_dispatch import _has_collection_toc, validated_collection_dispatch
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
    source_request_by_field = dict(request)
    if source_request_by_field.get("source_file_import_id") is None:
        source_request_by_field["source_file_import_id"] = job.source_file_import_id
    source_id = source_file_import_id_from_payload(source_request_by_field, required=True)
    if source_id != job.source_file_import_id:
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
    return all(
        is_overlay_job_matching(job, expected_job)
        and job["spec"].get("activeDeadlineSeconds") == expected_job["spec"].get("activeDeadlineSeconds")
        and job["spec"]["template"]["spec"].get("activeDeadlineSeconds")
        == expected_job["spec"]["template"]["spec"].get("activeDeadlineSeconds")
        for job in jobs
    )


def require_collection_request_match(current_params, requested_params):
    """Reject replay or retry changes to admitted collection inputs."""
    from api.control_imports import _string_list

    current_proof = validated_collection_dispatch("ptg", current_params)
    requested_proof = validated_collection_dispatch("ptg", requested_params)
    if current_proof is None and requested_proof is None:
        return
    scope_fields = (
        "source_file_import_id",
        "import_id",
        "source_key",
        "import_month",
        "max_files",
        "max_items",
        "test",
        "test_mode",
        "plan_ids",
        "file_url_contains",
    )
    if (
        current_proof != requested_proof
        or any(current_params.get(field) != requested_params.get(field) for field in scope_fields)
        or any(
            _string_list(current_params.get(plural) or current_params.get(singular))
            != _string_list(requested_params.get(plural) or requested_params.get(singular))
            for plural, singular in (
                ("plan_market_types", "plan_market_type"),
                ("source_network_names", "source_network_name"),
            )
        )
    ):
        raise SanicException("Collection request conflicts with its admitted proof or scope", status_code=409)


async def recheck_collection_replay(requested_params, replay, *, connection=None):
    """Read the private proof under the existing narrow admission transaction."""
    from api.control_imports import acquire_ptg_admission_lock, db
    from db.models import ImportRun

    if replay.get("importer") != "ptg":
        return
    if COLLECTION_PROOF_PARAM not in requested_params and not _has_collection_toc(replay.get("params") or {}):
        return
    context = nullcontext(connection) if connection is not None else db.acquire()
    async with context as locked:
        if connection is None:
            await acquire_ptg_admission_lock(locked)
        current_params = await locked.scalar(
            select(ImportRun.params).where(ImportRun.run_id == replay["run_id"]).limit(1)
        )
        if not isinstance(current_params, dict):
            raise ValueError("Cigna collection replay lacks retained archive proof")
        source_file_import_id_from_payload({**replay, "params": current_params}, required=True)
        require_collection_request_match(current_params, requested_params)


async def recheck_collection_attempt(connection, requested_params, source_file_import_id):
    """Keep terminal control runs bound to their retained source-attempt inputs."""
    from db.models import ImportRun

    if not source_file_import_id:
        return
    retained_rows = await connection.all(
        select(ImportRun.params, ImportRun.source_file_import_id, ImportRun.import_id, ImportRun.metrics)
        .where(ImportRun.importer == "ptg")
        .where(ImportRun.source_file_import_id == str(source_file_import_id))
    )
    for row in retained_rows:
        retained_by_field = dict(getattr(row, "_mapping", row))
        current_params = retained_by_field.get("params")
        if not isinstance(current_params, dict):
            if COLLECTION_PROOF_PARAM in requested_params:
                raise ValueError("Cigna collection replay lacks retained archive proof")
            continue
        if (
            COLLECTION_PROOF_PARAM not in requested_params
            and COLLECTION_PROOF_PARAM not in current_params
            and not _has_collection_toc(current_params)
        ):
            continue
        source_file_import_id_from_payload(retained_by_field, required=True)
        require_collection_request_match(current_params, requested_params)


async def _locked_ptg_source_replay(
    connection: Any,
    import_row: dict[str, Any],
    *,
    source_file_import_id: str,
) -> dict[str, Any] | None:
    """Reuse active source admissions only after checking their retained inputs."""
    from api.control_imports import (
        _active_idempotency_run,
        _active_ptg_source_file_replay,
        is_ptg_wave_owned_run,
        recheck_collection_replay,
        recheck_frozen_binding_on_connection,
    )

    replay = None
    idempotency_key = import_row.get("idempotency_key")
    if idempotency_key:
        replay = await _active_idempotency_run(connection, "ptg", str(idempotency_key))
        if replay and await is_ptg_wave_owned_run(connection, str(replay.get("run_id") or "")):
            replay = None
    if not replay:
        replay = await _active_ptg_source_file_replay(connection, source_file_import_id)
    if replay:
        await recheck_collection_replay(import_row["params"], replay, connection=connection)
        await recheck_frozen_binding_on_connection(connection, import_row["params"])
    return replay
