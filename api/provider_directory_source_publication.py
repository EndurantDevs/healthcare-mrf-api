# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Admit publication of one completed native acquisition without queue replay."""

import hashlib
import json
import re
from dataclasses import replace
from datetime import UTC, datetime

from api import provider_directory_source_worker as source

ROLE = "source-publication"
ROUTES_ENV = "HLTHPRT_PROVIDER_DIRECTORY_SOURCE_PUBLICATION_ROUTES_JSON"
HANDOFF_ENV = "HLTHPRT_PROVIDER_DIRECTORY_SOURCE_PUBLICATION_HANDOFF_SHA256"
ATTEMPT_ENV = "HLTHPRT_PROVIDER_DIRECTORY_SOURCE_PUBLICATION_ATTEMPT_ID"


def selected_spec(spec, payload):
    """Keep publication out of the global class and queue registries."""
    if payload.get("role") != ROLE:
        return spec
    if (
        payload.get("importer") != source._IMPORTER
        or payload.get("queue") not in (None, "", spec.queue)
        or payload.get("worker_class") not in (None, "", spec.worker_class)
        or set(payload)
        - {"role", "importer", "run_id", "job_id", "queue", "worker_class", "resource_class", "status", "reason"}
    ):
        raise ValueError("fhir_source_publication_selector_invalid")
    return replace(spec, role=ROLE)


async def read_completed_run(connection, run_id):
    """Observe bounded native proof under the caller's exact run action fence."""
    from sqlalchemy import text

    from api.control_imports import ImportRun
    from db.plan_release_storage import storage_relation

    if type(run_id) is not str or re.fullmatch(r"run_[0-9a-f]{32}", run_id) is None:
        raise ValueError("fhir_source_publication_run_invalid")
    rows = await connection.all(
        text(
            "SELECT run_id,importer,status,finished_at,retry_of_run_id,"
            "COALESCE(error::jsonb,'null'::jsonb)='null'::jsonb AS no_error,"
            "CASE WHEN octet_length(params::text)<=262144 THEN params END AS params,"
            "CASE WHEN octet_length(progress::text)<=262144 THEN progress END AS progress,"
            "CASE WHEN octet_length(metrics::text)<=262144 THEN metrics END AS metrics "
            "FROM " + storage_relation(str(ImportRun.__table__.schema), "import_run") + " WHERE run_id=:run_id"
        ),
        run_id=run_id,
    )
    if len(rows) != 1:
        raise ValueError("fhir_source_publication_run_unavailable")
    row = rows[0]
    run = dict(row._mapping) if hasattr(row, "_mapping") else dict(row)
    for name in ("params", "progress", "metrics"):
        if type(run.get(name)) is str:
            run[name] = json.loads(run[name])
    return run


def _completed_proof(run):
    """The native task and its canonical handoff, never the request, grant scope."""
    if (
        run.get("importer") != source._IMPORTER
        or run.get("status") != "succeeded"
        or run.get("no_error") is not True
        or run.get("finished_at") is None
        or any(type(run.get(name)) is not dict for name in ("params", "progress", "metrics"))
    ):
        raise ValueError("fhir_source_publication_completed_acquisition_required")
    params, progress, metrics = (run[name] for name in ("params", "progress", "metrics"))
    job_identity = original_job_identity(run)
    publication = metrics.get("source_publication")
    if (
        type(publication) is not dict
        or publication.get("state") != "incomplete"
        or params.get("run_id", run["run_id"]) != run["run_id"]
        or type(params.get("source_ids")) is not list
        or len(params["source_ids"]) != 1
        or params["source_ids"] == ["cms-npd"]
        or params.get("import_resources") is not True
        or params.get("defer_typed_materialization") is not True
        or params.get("publish_artifacts") is not False
        or params.get("publish_after_acquisition") is not False
    ):
        raise ValueError("fhir_source_publication_scope_invalid")
    attempt = progress.get("attempt_id")
    if type(attempt) is not str or re.fullmatch(re.escape(run["run_id"]) + r":[0-9a-f]{32}", attempt) is None:
        raise ValueError("fhir_source_publication_attempt_invalid")
    started = datetime.fromisoformat(progress["attempt_started_at"])
    finished = run["finished_at"]
    finished = datetime.fromisoformat(finished) if type(finished) is str else finished
    started = started.replace(tzinfo=UTC) if started.tzinfo is None else started.astimezone(UTC)
    finished = finished.replace(tzinfo=UTC) if finished.tzinfo is None else finished.astimezone(UTC)
    if finished < started:
        raise ValueError("fhir_source_publication_attempt_invalid")
    handoffs = metrics.get("source_candidate_handoffs")
    if type(handoffs) is not dict or len(handoffs) != 1:
        raise ValueError("fhir_source_publication_handoff_required")
    dataset_id, receipt = next(iter(handoffs.items()))
    if type(receipt) is not dict or set(receipt) != {"document", "sha256"}:
        raise ValueError("fhir_source_publication_handoff_invalid")
    document = receipt["document"]
    encoded = json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()
    if len(encoded) > 262144 or hashlib.sha256(encoded).hexdigest() != receipt["sha256"]:
        raise ValueError("fhir_source_publication_handoff_changed")
    _require_handoff_scope(document, dataset_id, params, run["run_id"])
    return attempt, receipt["sha256"], job_identity


def original_job_identity(run):
    """Terminal projection may omit all job fields; partial coordinates are invalid."""
    from api.control_imports import _SINGLE_JOB_ADAPTERS, _enqueue_job_options

    adapter = _SINGLE_JOB_ADAPTERS[source._IMPORTER]
    canonical_by_field = {
        "queue": adapter["queue"],
        "function": adapter["function"],
        "job_id": _enqueue_job_options(adapter, {"run_id": run["run_id"]})["_job_id"],
    }
    metrics = run["metrics"]
    present = set(metrics).intersection(canonical_by_field)
    if present and (
        present != set(canonical_by_field)
        or any(metrics[name] != expected for name, expected in canonical_by_field.items())
    ):
        raise ValueError("fhir_source_publication_native_job_changed")
    return canonical_by_field


def _require_handoff_scope(document, dataset_id, params, run_id):
    from api.control_imports import ImportRun
    from process.provider_directory_generic_source_storage import validate_handoff_document
    from process.provider_directory_source_storage_binding import FHIRSourceStorageBinding

    storage = document["storage"]
    binding = FHIRSourceStorageBinding(**{name: value for name, value in storage.items() if name != "producer"})
    producer = storage["producer"]
    validate_handoff_document(document, storage)
    identity = document["identity"]
    if (
        binding.catalog_schema != str(ImportRun.__table__.schema)
        or identity["dataset_id"] != dataset_id
        or identity["acquisition_root_run_id"] != (params.get("provider_directory_pagination_root_run_id") or run_id)
        or identity["metadata"].get("source_ids") != params["source_ids"]
        or type(identity["endpoint_id"]) is not str
        or not identity["endpoint_id"]
        or type(producer) is not list
        or len(producer) != 2
        or type(producer[0]) is not str
        or re.fullmatch(r"[0-9a-f]{40}", producer[0]) is None
        or type(producer[1]) is not str
        or re.fullmatch(r"sha256:[0-9a-f]{64}", producer[1]) is None
    ):
        raise ValueError("fhir_source_publication_handoff_scope_changed")


async def admit_launch(connection, request):
    """Capture a separately configured executor while preserving the retained producer."""
    from api import control_workers as workers

    run = await read_completed_run(connection, request.get("run_id"))
    attempt, handoff_sha256, job_identity = _completed_proof(run)
    routes = source._configured_routes(setting=ROUTES_ENV)
    if any(route["root_run_id"] is None for route in routes):
        raise ValueError("fhir_source_publication_route_invalid")
    route = source._source_route(run, routes=routes)
    if route is None:
        raise ValueError("fhir_source_publication_disabled")
    request.admitted_job = (
        run["run_id"],
        source._IMPORTER,
        job_identity["queue"],
        job_identity["function"],
        job_identity["job_id"],
    )
    launch = source.FHIRSourcePublicationLaunch(
        run["run_id"],
        job_identity["job_id"],
        route["image"],
        root_run_id=route["root_run_id"],
        secret_name=route["secret_name"],
        source_id=route["source_id"],
        endpoint_scope=route["endpoint_scope"],
        retry_of_run_id=run["params"].get("retry_of_run_id") or "",
        attempt_id=attempt,
        handoff_sha256=handoff_sha256,
    )
    request.fhir_source_launch = launch


def require_original_stopped(launch):
    """Use existing exact Job and Pod inventory; never mutate the original worker."""
    from api import control_workers as workers

    if type(launch) is not source.FHIRSourcePublicationLaunch:
        return
    presence = workers.exact_worker_presence(
        {
            "importer": source._IMPORTER,
            "run_id": launch.run_id,
            "worker_class": source._WORKER_CLASS,
            "queue": source._QUEUE,
        }
    )
    if presence.get("enabled") is not True or presence.get("stopped") is not True:
        raise ValueError("fhir_source_publication_original_worker_active")


def environment(launch):
    """Emit the exact native proof expected by the fixed publication bootstrap."""
    if type(launch) is not source.FHIRSourcePublicationLaunch:
        return []
    return [{"name": HANDOFF_ENV, "value": launch.handoff_sha256}, {"name": ATTEMPT_ENV, "value": launch.attempt_id}]
