# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Select admitted pricing workers and propagate their independently qualified policy."""

import os
import re
from dataclasses import dataclass

from db.plan_release_storage import plan_release_schema, storage_relation
from process.plan_pricing_admission import (
    HISTORICAL_REVISIONS,
    PricingAdmission,
    is_reserved_pricing_key,
    pricing_control_layout,
    require,
    require_managed_pricing_admission,
)
from process.plan_pricing_control_policy import is_qualified_historical_revision, pricing_control_schema
from process.plan_release_storage_custody import _document

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


@dataclass(frozen=True)
class HistoricalPricingWorkerPolicy:
    schema: str
    revision: str
    run_id: str


async def admit_pricing_worker(connection, run_id, worker_payload):
    """Keep native admission on the existing worker-action transaction."""
    native_run = await connection.fetchrow(
        f"SELECT * FROM {storage_relation(plan_release_schema(), 'import_run')} WHERE run_id=$1", run_id
    )
    require(native_run is not None, "worker_run_missing")
    if native_run["importer"] != "plan-pricing-projection":
        return None
    run_by_field = {**native_run, "params": _document(native_run["params"])}
    admission = await require_managed_pricing_admission(connection, run_by_field, action="launch")
    if admission is not None:
        worker_payload["importer"] = run_by_field["importer"]
    elif is_reserved_pricing_key(run_by_field.get("idempotency_key")):
        schema, revision, is_managed = await pricing_control_layout(connection)
        require(not is_managed, "worker_control_layout_changed")
        worker_payload.historical_pricing_policy = HistoricalPricingWorkerPolicy(schema, revision, run_id)
        worker_payload["importer"] = run_by_field["importer"]
    return admission


def _require_pricing_job(launch_request):
    identity = getattr(launch_request, "admitted_job", None)
    require(
        identity
        == (
            launch_request.get("run_id"),
            "plan-pricing-projection",
            "arq:PTGCandidateAudit",
            "control_single_job_start",
            "plan_pricing_projection_" + launch_request["run_id"],
        )
        and launch_request.get("importer") == "plan-pricing-projection",
        "worker_policy_selection_changed",
    )


def _historical_worker_policy(launch_request):
    policy = getattr(launch_request, "historical_pricing_policy", None)
    if policy is None:
        return None
    require(type(policy) is HistoricalPricingWorkerPolicy, "worker_historical_policy_invalid")
    _require_pricing_job(launch_request)
    require(
        policy.run_id == launch_request.get("run_id") and policy.schema == pricing_control_schema(),
        "worker_policy_selection_changed",
    )
    require(
        policy.revision in HISTORICAL_REVISIONS or is_qualified_historical_revision(policy.schema, policy.revision),
        "worker_historical_policy_changed",
    )
    return policy


def _policy_volumes(settings):
    configmap_sources = []
    for setting, key in settings:
        name = os.getenv(setting, "")
        require(
            type(name) is str
            and len(name) <= 253
            and re.fullmatch(r"[a-z0-9](?:[-a-z0-9.]*[a-z0-9])?", name) is not None,
            "worker_policy_reference_missing",
        )
        configmap_sources.append({"configMap": {"name": name, "optional": False, "items": [{"key": key, "path": key}]}})
    return [
        {
            "volume": {
                "name": "pricing-control-policy",
                "projected": {"defaultMode": 292, "sources": configmap_sources},
            },
            "volumeMount": {"name": "pricing-control-policy", "mountPath": "/etc/pricing-control", "readOnly": True},
        }
    ]


def pricing_worker_policy_volumes(spec, launch_request):
    """Mount fixed independent policy only for the exact authenticated pricing job."""
    admission = getattr(launch_request, "pricing_control_admission", None)
    historical = _historical_worker_policy(launch_request)
    if admission is None and historical is None:
        return []
    _require_pricing_job(launch_request)
    require(
        spec.worker_class == "process.PTGCandidateAudit" and spec.queue == "arq:PTGCandidateAudit",
        "worker_policy_selection_changed",
    )
    if historical is not None:
        require(admission is None, "worker_control_layout_changed")
        if historical.revision in HISTORICAL_REVISIONS:
            return []
        return _policy_volumes((("HLTHPRT_PLAN_PRICING_HISTORICAL_CONFIGMAP", "historical.json"),))
    require(type(admission) is PricingAdmission, "worker_admission_invalid")
    return _policy_volumes(
        (
            ("HLTHPRT_PLAN_PRICING_NAMESPACE_POLICY_CONFIGMAP", "namespace-role-policy.json"),
            ("HLTHPRT_PLAN_PRICING_PREPARATION_CONFIGMAP", "preparation.json"),
        )
    )


def pricing_worker_policy_environment(launch_request):
    """Propagate the independently configured schema to an admitted worker."""
    admission = getattr(launch_request, "pricing_control_admission", None)
    historical = _historical_worker_policy(launch_request)
    if historical is not None:
        require(admission is None, "worker_control_layout_changed")
        return [{"name": "HLTHPRT_PLAN_PRICING_CONTROL_SCHEMA", "value": historical.schema}]
    if admission is None:
        return []
    require(type(admission) is PricingAdmission, "worker_admission_invalid")
    return [{"name": "HLTHPRT_PLAN_PRICING_CONTROL_SCHEMA", "value": pricing_control_schema()}]
