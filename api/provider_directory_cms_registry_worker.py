# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Derive one fixed owner-worker purpose from a stored signed CMS job."""

import hashlib
import json
import os
import re
from dataclasses import dataclass
from importlib import import_module

PURPOSE = "cms-registry-source"
COMMAND = ["/usr/local/bin/cms-registry-source-worker"]
IMAGE_ENV = "HLTHPRT_PROVIDER_DIRECTORY_CMS_REGISTRY_WORKER_IMAGE"
_IMAGE = re.compile(r"[a-z0-9][a-z0-9./:_-]+@sha256:[0-9a-f]{64}\Z")
_WORKER_CLASS = "process.ProviderDirectoryFHIR"
_QUEUE = "arq:ProviderDirectoryFHIR"


@dataclass(frozen=True)
class CMSRegistryWorkerLaunch:
    """Server-created metadata; no serialized request can select this purpose."""

    run_id: str
    image: str
    policy_sha256: str


async def admit_cms_registry_worker_launch(admitted_run):
    """Verify both original capacity signatures before selecting an owner image."""
    if admitted_run.get("importer") != "provider-directory-fhir":
        return None
    params = admitted_run.get("params")
    if not isinstance(params, dict) or not params.get("provider_directory_profile_contract_id"):
        return None
    from process.provider_directory_cms_serving import _signed_plan, _verified_capture_limits
    from process.provider_directory_profile_selection_contract import validated_profile_execution

    fhir = import_module("process.provider_directory_fhir")
    execution = validated_profile_execution(params)
    if execution.attestation.operation != "publish" or execution.attestation.desired_cms_dataset is None:
        return None
    envelope, plan = _signed_plan(execution)
    admission = envelope["lease"]["signing_preflight_guard"]["healthcare_request"]["cms_nonprofile_admission"]
    image = os.getenv(IMAGE_ENV, "")
    if not image and "registry_source_retention" not in admission:
        return None
    if _IMAGE.fullmatch(image) is None or "registry_source_retention" not in admission:
        raise ValueError("cms_registry_worker_admission_unavailable")
    limits = await _verified_capture_limits(fhir, execution, plan)
    policy = limits.lease.signing_preflight_guard["healthcare_request"]["cms_nonprofile_admission"][
        "registry_source_retention"
    ]
    if policy["source_pin"]["schema_name"] != fhir._schema():
        raise ValueError("cms_registry_worker_admission_unavailable")
    encoded = json.dumps(policy, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    return CMSRegistryWorkerLaunch(admitted_run["run_id"], image, hashlib.sha256(encoded).hexdigest())


def authorized_cms_registry_worker_launch(spec, launch_request):
    """Use only the capability retained by exact native single-job admission."""
    from api.control_imports import _SINGLE_JOB_ADAPTERS, _enqueue_job_options
    from api.control_workers import _AdmittedWorkerRequest, _single_job_worker_target

    if not isinstance(launch_request, _AdmittedWorkerRequest):
        return None
    launch = launch_request.cms_registry_launch
    if launch is None:
        return None
    identity = launch_request.admitted_job
    adapter = _SINGLE_JOB_ADAPTERS["provider-directory-fhir"]
    if (
        type(launch) is not CMSRegistryWorkerLaunch
        or spec.worker_class != _WORKER_CLASS
        or spec.queue != _QUEUE
        or spec.role != "start"
        or not isinstance(identity, tuple)
        or len(identity) != 5
        or identity[:4] != (launch.run_id, "provider-directory-fhir", _QUEUE, "control_single_job_start")
        or launch_request.get("run_id") != launch.run_id
        or launch_request.get("importer", "provider-directory-fhir") != "provider-directory-fhir"
        or identity[4] != _single_job_worker_target(spec, launch_request)
        or identity[4] != _enqueue_job_options(adapter, {"run_id": launch.run_id})["_job_id"]
    ):
        raise ValueError("cms_registry_worker_admission_unavailable")
    return launch


def apply_cms_registry_worker_spec(spec, launch_request, labels, pod_spec):
    """Add the fixed publisher process identity only for native purpose admission."""
    launch = authorized_cms_registry_worker_launch(spec, launch_request)
    if launch is not None:
        labels["healthporta.com/worker-purpose"] = PURPOSE
        labels["healthporta.com/cms-registry-policy-hash"] = launch.policy_sha256[:32]
        security = pod_spec["securityContext"]
        security.update(runAsUser=10001, runAsGroup=10001)
        if "fsGroup" in security:
            security["fsGroup"] = 10001


def is_cms_registry_secret_selected(selection, worker_class, launch_request):
    """Keep publisher credentials restricted to the exact admitted worker purpose."""
    if "purposes" not in selection:
        return True
    if (
        selection["purposes"] != [PURPOSE]
        or selection.get("importers") != ["provider-directory-fhir"]
        or not ("workerClasses" in selection or "worker_classes" in selection)
    ):
        raise ValueError("worker purpose secret selector is invalid")
    if worker_class != _WORKER_CLASS:
        return False
    from api.control_workers import _BY_WORKER_CLASS

    return authorized_cms_registry_worker_launch(_BY_WORKER_CLASS[worker_class], launch_request) is not None


def is_cms_registry_job_matching(job, launch):
    """Reject an existing worker with a different image, command or signed policy."""
    try:
        labels = job["metadata"]["labels"]
        template = job["spec"]["template"]
        pod = template["spec"]
        containers = pod["containers"]
        security = pod["securityContext"]
        return (
            labels["healthporta.com/worker-purpose"] == PURPOSE
            and labels["healthporta.com/cms-registry-policy-hash"] == launch.policy_sha256[:32]
            and template["metadata"]["labels"]["healthporta.com/worker-purpose"] == PURPOSE
            and template["metadata"]["labels"]["healthporta.com/cms-registry-policy-hash"] == launch.policy_sha256[:32]
            and security["runAsNonRoot"] is True
            and security["runAsUser"] == 10001
            and security["runAsGroup"] == 10001
            and security.get("fsGroup", 10001) == 10001
            and len(containers) == 1
            and containers[0]["image"] == launch.image
            and containers[0]["command"] == COMMAND
        )
    except KeyError, TypeError, IndexError:
        return False
