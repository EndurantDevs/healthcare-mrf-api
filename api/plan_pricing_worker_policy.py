# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Propagate only the two existing trusted documents to an admitted worker."""

import os
import re

from db.plan_release_storage import plan_release_schema, storage_relation
from process.plan_pricing_admission import PricingAdmission, require, require_managed_pricing_admission
from process.plan_pricing_control_policy import pricing_control_schema
from process.plan_release_storage_custody import _document


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
    return admission


def pricing_worker_policy_volumes(spec, launch_request):
    """Mount fixed independent policy only for the exact admitted native job."""
    admission = getattr(launch_request, "pricing_control_admission", None)
    if admission is None:
        return []
    require(type(admission) is PricingAdmission, "worker_admission_invalid")
    identity = getattr(launch_request, "admitted_job", None)
    require(
        spec.worker_class == "process.PTGCandidateAudit"
        and spec.queue == "arq:PTGCandidateAudit"
        and identity
        == (
            launch_request.get("run_id"),
            "plan-pricing-projection",
            spec.queue,
            "control_single_job_start",
            "plan_pricing_projection_" + launch_request["run_id"],
        )
        and launch_request.get("importer") == "plan-pricing-projection",
        "worker_policy_selection_changed",
    )
    configmap_sources = []
    for setting, key in (
        ("HLTHPRT_PLAN_PRICING_NAMESPACE_POLICY_CONFIGMAP", "namespace-role-policy.json"),
        ("HLTHPRT_PLAN_PRICING_PREPARATION_CONFIGMAP", "preparation.json"),
    ):
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


def pricing_worker_policy_environment(launch_request):
    """Propagate the independently configured schema to an admitted worker."""
    admission = getattr(launch_request, "pricing_control_admission", None)
    if admission is None:
        return []
    require(type(admission) is PricingAdmission, "worker_admission_invalid")
    return [{"name": "HLTHPRT_PLAN_PRICING_CONTROL_SCHEMA", "value": pricing_control_schema()}]
