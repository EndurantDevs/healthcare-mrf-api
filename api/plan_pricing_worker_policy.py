# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Propagate the exact independently qualified policy to an admitted pricing worker."""

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
