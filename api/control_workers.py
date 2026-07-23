# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import ssl
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class WorkerSpec:
    queue: str
    worker_class: str
    importers: tuple[str, ...]
    role: str = "start"


_PROVIDER_DIRECTORY_WORKER_CLASS = "process.ProviderDirectoryFHIR"
_PROVIDER_DIRECTORY_MIN_ACTIVE_DEADLINE_SECONDS = 144 * 60 * 60


_START_WORKERS: tuple[WorkerSpec, ...] = (
    WorkerSpec("arq:PTG", "process.PTG", ("ptg",)),
    WorkerSpec("arq:PTGSmall", "process.PTGSmall", ("ptg",)),
    WorkerSpec("arq:PTGNormal", "process.PTGNormal", ("ptg",)),
    WorkerSpec("arq:PTGLarge", "process.PTGLarge", ("ptg",)),
    WorkerSpec("arq:PTGHuge", "process.PTGHuge", ("ptg",)),
    WorkerSpec(
        "arq:PTGCandidateAudit",
        "process.PTGCandidateAudit",
        ("ptg-candidate-audit",),
    ),
    WorkerSpec("arq:MRF", "process.MRF", ("mrf",)),
    WorkerSpec("arq:NPI", "process.NPI", ("npi",)),
    WorkerSpec("arq:NUCC", "process.NUCC", ("nucc",)),
    WorkerSpec("arq:CodeSets", "process.CodeSets", ("code-sets",)),
    WorkerSpec("arq:MSDRG", "process.MSDRG", ("ms-drg",)),
    WorkerSpec("arq:ClinicalReference", "process.ClinicalReference", ("clinical-reference",)),
    WorkerSpec("arq:TerminologySynonyms", "process.TerminologySynonyms", ("terminology-synonyms",)),
    WorkerSpec("arq:Geo", "process.Geo", ("geo",)),
    WorkerSpec("arq:GeoCensus", "process.GeoCensus", ("geo-census",)),
    WorkerSpec("arq:Attributes", "process.Attributes", ("plan-attributes",)),
    WorkerSpec("arq:MRFSourceDiscovery", "process.MRFSourceDiscovery", ("mrf-source-discovery",)),
    WorkerSpec("arq:ClaimsPricing", "process.ClaimsPricing", ("claims-pricing", "claims-procedures")),
    WorkerSpec("arq:DrugClaims", "process.DrugClaims", ("drug-claims",)),
    WorkerSpec("arq:ProviderQuality", "process.ProviderQuality", ("provider-quality",)),
    WorkerSpec("arq:ProviderEnrichment", "process.ProviderEnrichment", ("provider-enrichment",)),
    WorkerSpec("arq:ProviderDirectoryFHIR", "process.ProviderDirectoryFHIR", ("provider-directory-fhir",)),
    WorkerSpec("arq:PartDFormularyNetwork", "process.PartDFormularyNetwork", ("partd-formulary-network",)),
    WorkerSpec("arq:PharmacyLicense", "process.PharmacyLicense", ("pharmacy-license",)),
    WorkerSpec("arq:PlacesZcta", "process.PlacesZcta", ("places-zcta",)),
    WorkerSpec("arq:LODES", "process.LODES", ("lodes",)),
    WorkerSpec("arq:MedicareEnrollment", "process.MedicareEnrollment", ("medicare-enrollment",)),
    WorkerSpec("arq:CMSDoctors", "process.CMSDoctors", ("cms-doctors",)),
    WorkerSpec("arq:FacilityAnchors", "process.FacilityAnchors", ("facility-anchors",)),
    WorkerSpec("arq:PharmacyEconomics", "process.PharmacyEconomics", ("pharmacy-economics",)),
    WorkerSpec("arq:EntityAddressUnified", "process.EntityAddressUnified", ("entity-address-unified",)),
    WorkerSpec("arq:AddressArchive", "process.AddressArchive", ("address-archive-v2-migrate",)),
    WorkerSpec("arq:OpenAddresses", "process.OpenAddresses", ("openaddresses",)),
)

_FINISH_WORKERS: tuple[WorkerSpec, ...] = (
    WorkerSpec("arq:MRF_finish", "process.MRF_finish", ("mrf",), role="finish"),
    WorkerSpec("arq:ClaimsPricing_finish", "process.ClaimsPricing_finish", ("claims-pricing", "claims-procedures"), role="finish"),
    WorkerSpec("arq:DrugClaims_finish", "process.DrugClaims_finish", ("drug-claims",), role="finish"),
    WorkerSpec("arq:ProviderQuality_finish", "process.ProviderQuality_finish", ("provider-quality",), role="finish"),
    WorkerSpec("arq:PartDFormularyNetwork_finish", "process.PartDFormularyNetwork_finish", ("partd-formulary-network",), role="finish"),
    WorkerSpec("arq:PharmacyLicense_finish", "process.PharmacyLicense_finish", ("pharmacy-license",), role="finish"),
)

_WORKERS = (*_START_WORKERS, *_FINISH_WORKERS)
_BY_QUEUE = {spec.queue: spec for spec in _WORKERS}
_BY_WORKER_CLASS = {spec.worker_class: spec for spec in _WORKERS}
_BY_IMPORTER_ROLE: dict[tuple[str, str], WorkerSpec] = {}
for _spec in _WORKERS:
    for _importer in _spec.importers:
        _BY_IMPORTER_ROLE.setdefault((_importer, _spec.role), _spec)
_ENGINE_LABEL = "mrf"
_K8S_API_TOKEN = Path("/var/run/secrets/kubernetes.io/serviceaccount/token")
_K8S_API_CA = Path("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
_K8S_API_NAMESPACE = Path("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
WORKER_ENSURE_RUN_IDENTITY_CONTRACT = (
    "healthporta.worker-ensure-run-identity.v1"
)


def worker_registry() -> list[dict[str, Any]]:
    """Return the current state of every registered import worker."""

    return [_worker_state(spec) for spec in _WORKERS]


def ensure_worker(payload: dict[str, Any]) -> dict[str, Any]:
    """Start or locate workers selected by one launch payload."""

    specs = _resolve_specs(payload)
    if not specs:
        importer = str(payload.get("importer") or "").strip()
        queue = str(payload.get("queue") or "").strip()
        return _worker_ensure_response(
            payload,
            status="unsupported",
            items=[],
            message=(
                "no worker is registered for "
                f"{queue or importer or 'request'}"
            ),
        )

    items = [_ensure_spec(spec, payload) for spec in specs]
    status = "already_running"
    if any(item["status"] == "started" for item in items):
        status = "started"
    if any(item["status"] in {"failed", "blocked", "unsupported"} for item in items):
        status = "failed"
    return _worker_ensure_response(
        payload,
        status=status,
        items=items,
    )


def _worker_ensure_response(
    payload: dict[str, Any],
    *,
    status: str,
    items: list[dict[str, Any]],
    message: str | None = None,
) -> dict[str, Any]:
    """Bind successful worker evidence to the exact requested control run."""

    response_by_field: dict[str, Any] = {
        "status": status,
        "items": items,
    }
    if message is not None:
        response_by_field["message"] = message
    run_id = str(payload.get("run_id") or "").strip()
    if not run_id:
        return response_by_field
    response_by_field.update(
        {
            "contract_id": WORKER_ENSURE_RUN_IDENTITY_CONTRACT,
            "run_id": run_id,
            "items": [{**item, "run_id": run_id} for item in items],
        }
    )
    return response_by_field


def worker_state(payload: dict[str, Any]) -> dict[str, Any]:
    """Return current process or Kubernetes worker state for a launch payload."""
    specs = _resolve_specs(payload)
    if not specs:
        importer = str(payload.get("importer") or "").strip()
        queue = str(payload.get("queue") or "").strip()
        return {
            "status": "unsupported",
            "items": [],
            "message": f"no worker is registered for {queue or importer or 'request'}",
        }

    items = [_worker_state(spec, payload) for spec in specs]
    status = "running" if any(item.get("running") for item in items) else "inactive"
    if any(item.get("job_status") == "failed" for item in items):
        status = "failed"
    elif items and all(item.get("job_status") == "succeeded" for item in items):
        status = "succeeded"
    elif items and all(item.get("job_status") == "missing" for item in items):
        status = "missing"
    return {"status": status, "items": items}


def _resolve_specs(payload: dict[str, Any]) -> list[WorkerSpec]:
    worker_class = str(payload.get("worker_class") or "").strip()
    queue = str(payload.get("queue") or "").strip()
    if worker_class:
        spec = _BY_WORKER_CLASS.get(worker_class)
        if spec is None:
            return []
        if queue and spec.queue != queue:
            return []
        return [spec]

    importer = str(payload.get("importer") or "").strip()
    role = str(payload.get("role") or "").strip().lower()
    explicit_role = bool(role)
    status = str(payload.get("status") or "").strip().lower()
    if not role:
        role = "finish" if status == "finalizing" else "start"
    role_overrides_queue = bool(importer and (explicit_role or status == "finalizing"))
    if queue and not role_overrides_queue:
        spec = _BY_QUEUE.get(queue)
        return [spec] if spec is not None else []
    if importer and role == "start" and not explicit_role:
        finished_start_spec = _finish_spec_after_completed_start(importer, payload)
        if finished_start_spec is not None:
            return [finished_start_spec]
    spec = _BY_IMPORTER_ROLE.get((importer, role))
    if spec is not None:
        return [spec]
    if role == "finish":
        return []
    return [_BY_IMPORTER_ROLE[(importer, "start")]] if (importer, "start") in _BY_IMPORTER_ROLE else []


def _finish_spec_after_completed_start(importer: str, payload: dict[str, Any]) -> WorkerSpec | None:
    if _launcher_mode() != "kubernetes":
        return None
    start_spec = _BY_IMPORTER_ROLE.get((importer, "start"))
    finish_spec = _BY_IMPORTER_ROLE.get((importer, "finish"))
    if start_spec is None or finish_spec is None:
        return None
    state = _worker_state(start_spec, payload)
    if state.get("job_status") == "succeeded":
        return finish_spec
    return None


def _ensure_spec(spec: WorkerSpec, payload: dict[str, Any]) -> dict[str, Any]:
    state = _worker_state(spec, payload)
    if state["running"]:
        return {**state, "status": "already_running"}
    if _launcher_mode() == "kubernetes":
        return _ensure_kubernetes_job(spec, payload, state)

    if spec.worker_class == "process.DrugClaims_finish" and _worker_state(_BY_QUEUE["arq:ClaimsPricing_finish"])["running"]:
        return {**state, "status": "blocked", "message": "ClaimsPricing_finish is already running"}
    if spec.worker_class == "process.ClaimsPricing_finish" and _worker_state(_BY_QUEUE["arq:DrugClaims_finish"])["running"]:
        return {**state, "status": "blocked", "message": "DrugClaims_finish is already running"}

    try:
        pid = _start_process(spec, payload)
    except Exception as exc:
        return {**state, "status": "failed", "message": str(exc)}
    return {**_worker_state(spec), "status": "started", "pid": pid}


def _start_process(spec: WorkerSpec, payload: dict[str, Any]) -> int:
    for directory in (_state_dir(), _log_dir()):
        directory.mkdir(parents=True, exist_ok=True)
    cmd = _worker_command(sys.executable, spec)
    env = os.environ.copy()
    env["HLTHPRT_ACTIVE_WORKER_CLASS"] = spec.worker_class
    env["HLTHPRT_ACTIVE_WORKER_QUEUE"] = spec.queue
    import_id = str(payload.get("import_id") or "").strip()
    if import_id:
        env["HLTHPRT_IMPORT_ID_OVERRIDE"] = import_id
    run_id = str(payload.get("run_id") or "").strip()
    if run_id:
        env["HLTHPRT_CONTROL_RUN_ID"] = run_id
    if _uses_single_job_worker(spec):
        env["HLTHPRT_WORKER_ONCE_TARGET_JOB_ID"] = _single_job_worker_target(spec, payload) or ""
    with _log_path(spec).open("ab") as log_handle:
        process = subprocess.Popen(
            cmd,
            cwd=str(_repo_root()),
            env=env,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    _pid_path(spec).write_text(str(process.pid), encoding="utf-8")
    return process.pid


def _worker_state(spec: WorkerSpec, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    if _launcher_mode() == "kubernetes":
        return _kubernetes_worker_state(spec, payload)

    pid = _read_pid(_pid_path(spec))
    running = _is_pid_running(pid) and _is_pid_spec_match(pid, spec)
    if pid and not running:
        _remove_stale_pid(spec)
    if not running:
        pid = _find_running_pid(spec)
        running = _is_pid_running(pid)
        if running and pid:
            _write_pid_file(spec, pid)
    return {
        "queue": spec.queue,
        "worker_class": spec.worker_class,
        "importers": list(spec.importers),
        "role": spec.role,
        "running": running,
        "pid": pid if running else None,
        "pid_path": str(_pid_path(spec)),
        "log_path": str(_log_path(spec)),
        "command": " ".join(_worker_command(sys.executable, spec)),
    }


def _write_pid_file(spec: WorkerSpec, pid: int) -> None:
    try:
        _state_dir().mkdir(parents=True, exist_ok=True)
        _pid_path(spec).write_text(str(pid), encoding="utf-8")
    except OSError:
        return


def _launcher_mode() -> str:
    return os.getenv("HLTHPRT_WORKER_LAUNCHER", "process").strip().lower()


def _ensure_kubernetes_job(
    spec: WorkerSpec,
    payload: dict[str, Any],
    state: dict[str, Any],
) -> dict[str, Any]:
    image = os.getenv("HLTHPRT_WORKER_JOB_IMAGE", "").strip()
    if not image:
        return {**state, "status": "failed", "message": "HLTHPRT_WORKER_JOB_IMAGE is not configured"}

    namespace = _kubernetes_namespace()
    if state.get("job_status") in {"succeeded", "failed"}:
        try:
            _delete_terminal_kubernetes_worker_jobs(namespace, spec, payload)
        except _KubernetesApiError as exc:
            if exc.status != 404:
                return {**state, "status": "failed", "message": str(exc)}

    job = _worker_job_manifest(spec, payload, image)
    try:
        _kubernetes_request("POST", f"/apis/batch/v1/namespaces/{namespace}/jobs", job)
    except _KubernetesApiError as exc:
        if exc.status == 409:
            refreshed = _worker_state(spec, payload)
            return {**refreshed, "status": "already_running" if refreshed.get("running") else "exists"}
        return {**state, "status": "failed", "message": str(exc)}
    return {**_worker_state(spec, payload), "status": "started"}


def _delete_terminal_kubernetes_worker_jobs(namespace: str, spec: WorkerSpec, payload: dict[str, Any]) -> None:
    selector = _kubernetes_label_selector(spec, payload)
    path = f"/apis/batch/v1/namespaces/{namespace}/jobs?{urllib.parse.urlencode({'labelSelector': selector})}"
    body = _kubernetes_request("GET", path)
    for job_record in _kubernetes_job_records(body):
        metadata = job_record.get("metadata") if isinstance(job_record.get("metadata"), dict) else {}
        job_name = str(metadata.get("name") or "").strip()
        if not job_name:
            continue
        status = job_record.get("status") if isinstance(job_record.get("status"), dict) else {}
        active_count = int(status.get("active") or 0)
        succeeded_count = int(status.get("succeeded") or 0)
        failed_count = int(status.get("failed") or 0)
        if active_count or not (succeeded_count or failed_count):
            continue
        try:
            _delete_kubernetes_job(namespace, job_name)
        except _KubernetesApiError as exc:
            if exc.status != 404:
                raise


def _delete_kubernetes_job(namespace: str, job_name: str) -> None:
    encoded = urllib.parse.quote(job_name, safe="")
    delete_options_dict = {
        "apiVersion": "v1",
        "kind": "DeleteOptions",
        "propagationPolicy": "Background",
    }
    _kubernetes_request(
        "DELETE",
        f"/apis/batch/v1/namespaces/{namespace}/jobs/{encoded}",
        delete_options_dict,
    )


def delete_kubernetes_worker_jobs(cancel_request: dict[str, Any]) -> dict[str, Any]:
    """Delete active Kubernetes worker jobs matching a cancel request."""
    if _launcher_mode() != "kubernetes":
        return {"enabled": False, "launcher": _launcher_mode(), "deleted": 0}

    worker_specs = _resolve_specs(cancel_request)
    if not worker_specs:
        return {"enabled": True, "deleted": 0, "reason": "no matching worker spec"}

    namespace = _kubernetes_namespace()
    if not _is_kubernetes_configured():
        return {
            "enabled": True,
            "namespace": namespace,
            "deleted": 0,
            "error": "kubernetes worker launcher is not configured",
        }

    deletion_records: list[dict[str, Any]] = []
    deletion_errors: list[dict[str, Any]] = []
    for worker_spec in worker_specs:
        worker_records, worker_errors = _delete_kubernetes_worker_jobs_for_spec(namespace, worker_spec, cancel_request)
        deletion_records.extend(worker_records)
        deletion_errors.extend(worker_errors)

    delete_summary_dict: dict[str, Any] = {
        "enabled": True,
        "namespace": namespace,
        "deleted": sum(1 for deletion_record in deletion_records if deletion_record.get("deleted")),
        "items": deletion_records,
    }
    if deletion_errors:
        delete_summary_dict["errors"] = deletion_errors
    return delete_summary_dict


def _delete_kubernetes_worker_jobs_for_spec(
    namespace: str,
    worker_spec: WorkerSpec,
    payload: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Delete cancelable jobs for one worker spec and collect API errors."""
    selector = _kubernetes_label_selector(worker_spec, payload)
    path = f"/apis/batch/v1/namespaces/{namespace}/jobs?{urllib.parse.urlencode({'labelSelector': selector})}"
    try:
        body = _kubernetes_request("GET", path)
    except _KubernetesApiError as exc:
        return [], [{"worker_class": worker_spec.worker_class, "status": exc.status, "error": str(exc)}]

    deletion_records: list[dict[str, Any]] = []
    deletion_errors: list[dict[str, Any]] = []
    for job_record in _kubernetes_job_records(body):
        deletion_record, deletion_error = _delete_kubernetes_worker_job_record(namespace, worker_spec, job_record)
        if deletion_record:
            deletion_records.append(deletion_record)
        if deletion_error:
            deletion_errors.append(deletion_error)
    return deletion_records, deletion_errors


def _kubernetes_job_records(body: dict[str, Any]) -> list[dict[str, Any]]:
    """Return Kubernetes Job records from a list response body."""
    raw_records = body.get("items") if isinstance(body, dict) else []
    return [record for record in raw_records if isinstance(record, dict)]


def _delete_kubernetes_worker_job_record(
    namespace: str,
    worker_spec: WorkerSpec,
    job_record: dict[str, Any],
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """Delete one active job or report why it was left alone."""
    metadata = job_record.get("metadata") if isinstance(job_record.get("metadata"), dict) else {}
    job_name = str(metadata.get("name") or "").strip()
    if not job_name:
        return None, None
    status = job_record.get("status") if isinstance(job_record.get("status"), dict) else {}
    active_count = int(status.get("active") or 0)
    succeeded_count = int(status.get("succeeded") or 0)
    failed_count = int(status.get("failed") or 0)
    if not active_count and (succeeded_count or failed_count):
        return _kubernetes_delete_record(job_name, worker_spec, deleted=False, reason="terminal"), None
    try:
        _delete_kubernetes_job(namespace, job_name)
    except _KubernetesApiError as exc:
        if exc.status == 404:
            return _kubernetes_delete_record(job_name, worker_spec, deleted=False, reason="not_found"), None
        return None, {"job_name": job_name, "worker_class": worker_spec.worker_class, "status": exc.status, "error": str(exc)}
    return _kubernetes_delete_record(job_name, worker_spec, deleted=True), None


def _kubernetes_delete_record(
    job_name: str,
    worker_spec: WorkerSpec,
    *,
    deleted: bool,
    reason: str | None = None,
) -> dict[str, Any]:
    """Build the public delete summary for one Kubernetes worker job."""
    deletion_summary_dict: dict[str, Any] = {
        "job_name": job_name,
        "worker_class": worker_spec.worker_class,
        "deleted": deleted,
    }
    if reason:
        deletion_summary_dict["reason"] = reason
    return deletion_summary_dict


def _kubernetes_worker_state(spec: WorkerSpec, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    """Summarize Kubernetes jobs matching a worker spec and optional run."""
    state_base_dict = {
        "queue": spec.queue,
        "worker_class": spec.worker_class,
        "importers": list(spec.importers),
        "role": spec.role,
        "running": False,
        "pid": None,
        "launcher": "kubernetes",
        "command": " ".join(_worker_command(_worker_python(), spec)),
    }
    if not _is_kubernetes_configured():
        return {**state_base_dict, "job_name": _worker_job_name(spec, payload or {}), "job_status": "unconfigured"}

    selector = _kubernetes_label_selector(spec, payload or {})
    namespace = _kubernetes_namespace()
    path = f"/apis/batch/v1/namespaces/{namespace}/jobs?{urllib.parse.urlencode({'labelSelector': selector})}"
    try:
        body = _kubernetes_request("GET", path)
    except _KubernetesApiError as exc:
        return {**state_base_dict, "job_name": _worker_job_name(spec, payload or {}), "job_status": "error", "message": str(exc)}

    raw_job_records = body.get("items") if isinstance(body, dict) else []
    jobs = [job_record for job_record in raw_job_records if isinstance(job_record, dict)]
    active = sum(int((job.get("status") or {}).get("active") or 0) for job in jobs)
    succeeded = sum(int((job.get("status") or {}).get("succeeded") or 0) for job in jobs)
    failed = sum(int((job.get("status") or {}).get("failed") or 0) for job in jobs)
    latest = jobs[-1] if jobs else {}
    latest_name = ((latest.get("metadata") or {}).get("name") if isinstance(latest, dict) else None) or _worker_job_name(spec, payload or {})
    if active:
        job_status = "active"
    elif failed:
        job_status = "failed"
    elif succeeded:
        job_status = "succeeded"
    else:
        job_status = "missing"
    state_map = {
        **state_base_dict,
        "running": active > 0,
        "job_name": latest_name,
        "job_status": job_status,
        "active_jobs": active,
        "succeeded_jobs": succeeded,
        "failed_jobs": failed,
    }
    if job_status == "failed":
        failure = _kubernetes_worker_failure(namespace, selector)
        if failure:
            state_map["failure"] = failure
    return state_map


def _kubernetes_worker_failure(namespace: str, selector: str) -> dict[str, Any] | None:
    path = f"/api/v1/namespaces/{namespace}/pods?{urllib.parse.urlencode({'labelSelector': selector})}"
    try:
        body = _kubernetes_request("GET", path)
    except _KubernetesApiError as exc:
        return {"lookup_status": "error", "message": str(exc)}

    pods = _kubernetes_pod_records(body)
    failures = [_pod_container_failure(pod) for pod in pods]
    failures = [failure for failure in failures if failure is not None]
    if not failures:
        return None
    return failures[-1]


def _kubernetes_pod_records(body: dict[str, Any]) -> list[dict[str, Any]]:
    raw_records = body.get("items") if isinstance(body, dict) else []
    return [record for record in raw_records if isinstance(record, dict)]


def _pod_container_failure(pod: dict[str, Any]) -> dict[str, Any] | None:
    metadata = pod.get("metadata") if isinstance(pod.get("metadata"), dict) else {}
    status = pod.get("status") if isinstance(pod.get("status"), dict) else {}
    for container_status in _pod_container_statuses(status):
        terminated = _container_termination(container_status)
        if not terminated:
            continue
        reason = str(terminated.get("reason") or "").strip()
        exit_code = terminated.get("exitCode")
        if reason == "Completed" and int(exit_code or 0) == 0:
            continue
        return {
            "pod_name": metadata.get("name"),
            "container": container_status.get("name"),
            "reason": reason or None,
            "exitCode": exit_code,
            "message": terminated.get("message"),
            "startedAt": terminated.get("startedAt"),
            "finishedAt": terminated.get("finishedAt"),
        }
    return None


def _pod_container_statuses(status: dict[str, Any]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for key in ("initContainerStatuses", "containerStatuses"):
        values = status.get(key)
        if isinstance(values, list):
            records.extend(value for value in values if isinstance(value, dict))
    return records


def _container_termination(container_status: dict[str, Any]) -> dict[str, Any] | None:
    for state_key in ("state", "lastState"):
        state = container_status.get(state_key)
        if not isinstance(state, dict):
            continue
        terminated = state.get("terminated")
        if isinstance(terminated, dict):
            return terminated
    return None


def _kubernetes_label_selector(spec: WorkerSpec, payload: dict[str, Any]) -> str:
    selector_label_map = {
        "app.kubernetes.io/managed-by": "healthporta-worker-launcher",
        "healthporta.com/engine": _ENGINE_LABEL,
        "healthporta.com/worker-class-hash": _label_hash(spec.worker_class),
        "healthporta.com/role": spec.role,
    }
    run_id = str(payload.get("run_id") or "").strip()
    if run_id:
        selector_label_map["healthporta.com/run-id-hash"] = _label_hash(run_id)
    return ",".join(f"{key}={value}" for key, value in selector_label_map.items())


def _worker_job_manifest(spec: WorkerSpec, payload: dict[str, Any], image: str) -> dict[str, Any]:
    """Build the Kubernetes Job manifest for one import worker."""

    job_name = _worker_job_name(spec, payload)
    env_list = [
        {"name": "HLTHPRT_WORKER_LAUNCHER", "value": "process"},
        {"name": "HLTHPRT_IMPORT_NODE_ID", "value": os.getenv("HLTHPRT_IMPORT_NODE_ID", "")},
        {"name": "HLTHPRT_ACTIVE_WORKER_CLASS", "value": spec.worker_class},
        {"name": "HLTHPRT_ACTIVE_WORKER_QUEUE", "value": spec.queue},
    ]
    import_id = str(payload.get("import_id") or "").strip()
    if import_id:
        env_list.append({"name": "HLTHPRT_IMPORT_ID_OVERRIDE", "value": import_id})
    run_id = str(payload.get("run_id") or "").strip()
    if run_id:
        env_list.append({"name": "HLTHPRT_CONTROL_RUN_ID", "value": run_id})
    target_job_id = _single_job_worker_target(spec, payload)
    if target_job_id:
        env_list.append(
            {
                "name": "HLTHPRT_WORKER_ONCE_TARGET_JOB_ID",
                "value": target_job_id,
            }
        )
    env_list.extend(_worker_job_secret_env())

    container_dict: dict[str, Any] = {
        "name": "worker",
        "image": image,
        "imagePullPolicy": os.getenv("HLTHPRT_WORKER_JOB_IMAGE_PULL_POLICY", "IfNotPresent"),
        "workingDir": str(_repo_root()),
        "command": _worker_command(_worker_python(), spec),
        "env": env_list,
        "securityContext": _worker_job_container_security_context(),
    }
    env_from_list = _worker_job_env_from()
    if env_from_list:
        container_dict["envFrom"] = env_from_list
    resource_dict = _worker_job_resources(spec, payload)
    if resource_dict:
        container_dict["resources"] = resource_dict
    pvc_volumes = _worker_job_pvc_volumes()
    volumes = [*pvc_volumes, *_worker_job_secret_volumes()]
    if volumes:
        container_dict["volumeMounts"] = [volume_spec["volumeMount"] for volume_spec in volumes]

    pod_spec_dict: dict[str, Any] = {
        "restartPolicy": "Never",
        "automountServiceAccountToken": False,
        "securityContext": _worker_job_pod_security_context(has_pvc=bool(pvc_volumes)),
        "containers": [container_dict],
    }
    if volumes:
        pod_spec_dict["volumes"] = [volume_spec["volume"] for volume_spec in volumes]
    service_account = os.getenv("HLTHPRT_WORKER_JOB_SERVICE_ACCOUNT", "").strip()
    if service_account:
        pod_spec_dict["serviceAccountName"] = service_account
    pull_secret = os.getenv("HLTHPRT_WORKER_JOB_IMAGE_PULL_SECRET", "").strip()
    if pull_secret:
        pod_spec_dict["imagePullSecrets"] = [{"name": secret_name} for secret_name in _csv(pull_secret)]

    labels_by_key = {
        "app.kubernetes.io/name": "healthporta-import-worker",
        "app.kubernetes.io/managed-by": "healthporta-worker-launcher",
        "healthporta.com/engine": _ENGINE_LABEL,
        "healthporta.com/worker-class-hash": _label_hash(spec.worker_class),
        "healthporta.com/role": spec.role,
    }
    if run_id:
        labels_by_key["healthporta.com/run-id-hash"] = _label_hash(run_id)
    job_spec_dict: dict[str, Any] = {
        "backoffLimit": int(os.getenv("HLTHPRT_WORKER_JOB_BACKOFF_LIMIT", "0")),
        "ttlSecondsAfterFinished": int(os.getenv("HLTHPRT_WORKER_JOB_TTL_SECONDS", "86400")),
        "template": {
            "metadata": {"labels": labels_by_key},
            "spec": pod_spec_dict,
        },
    }
    active_deadline_seconds = int(os.getenv("HLTHPRT_WORKER_JOB_ACTIVE_DEADLINE_SECONDS", "0") or "0")
    if (
        active_deadline_seconds > 0
        and spec.worker_class == _PROVIDER_DIRECTORY_WORKER_CLASS
    ):
        active_deadline_seconds = max(
            active_deadline_seconds,
            _PROVIDER_DIRECTORY_MIN_ACTIVE_DEADLINE_SECONDS,
        )
    if active_deadline_seconds > 0:
        job_spec_dict["activeDeadlineSeconds"] = active_deadline_seconds
    replicas = _worker_start_replicas(spec)
    if replicas > 1:
        job_spec_dict["parallelism"] = replicas
        job_spec_dict["completions"] = replicas

    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": job_name,
            "labels": labels_by_key,
            "annotations": {
                "healthporta.com/queue": spec.queue,
                "healthporta.com/worker-class": spec.worker_class,
                "healthporta.com/importers": ",".join(spec.importers),
                "healthporta.com/run-id": run_id,
            },
        },
        "spec": job_spec_dict,
    }


def _worker_job_env_from() -> list[dict[str, Any]]:
    env_from_list: list[dict[str, Any]] = []
    for name in _csv(os.getenv("HLTHPRT_WORKER_JOB_ENV_FROM_CONFIGMAP", "")):
        env_from_list.append({"configMapRef": {"name": name}})
    for name in _csv(os.getenv("HLTHPRT_WORKER_JOB_ENV_FROM_SECRET", "")):
        env_from_list.append({"secretRef": {"name": name}})
    return env_from_list


def _worker_job_secret_env() -> list[dict[str, Any]]:
    """Build explicitly named worker environment values from secret keys."""

    raw = os.getenv("HLTHPRT_WORKER_JOB_SECRET_ENV_JSON", "").strip()
    if not raw:
        return []
    try:
        secret_env_spec_list = json.loads(raw)
    except json.JSONDecodeError:
        return []
    if not isinstance(secret_env_spec_list, list):
        return []

    environment_by_name: dict[str, dict[str, Any]] = {}
    for secret_env_spec in secret_env_spec_list:
        if not isinstance(secret_env_spec, dict):
            continue
        environment_name = str(secret_env_spec.get("name") or "").strip()
        secret_name = str(
            secret_env_spec.get("secretName")
            or secret_env_spec.get("secret_name")
            or ""
        ).strip()
        secret_key = str(secret_env_spec.get("key") or "").strip()
        if not environment_name or not secret_name or not secret_key:
            continue
        secret_key_reference_by_field: dict[str, Any] = {
            "name": secret_name,
            "key": secret_key,
        }
        if bool(secret_env_spec.get("optional", False)):
            secret_key_reference_by_field["optional"] = True
        environment_by_name[environment_name] = {
            "name": environment_name,
            "valueFrom": {
                "secretKeyRef": secret_key_reference_by_field
            },
        }
    return list(environment_by_name.values())


def _worker_job_container_security_context() -> dict[str, Any]:
    return {
        "allowPrivilegeEscalation": False,
        "capabilities": {"drop": ["ALL"]},
    }


def _worker_job_pod_security_context(*, has_pvc: bool) -> dict[str, Any]:
    security_context_dict: dict[str, Any] = {
        "runAsNonRoot": True,
        "runAsUser": 65534,
        "runAsGroup": 65534,
        "seccompProfile": {"type": "RuntimeDefault"},
    }
    if has_pvc:
        security_context_dict["fsGroup"] = 65534
        security_context_dict["fsGroupChangePolicy"] = "OnRootMismatch"
    return security_context_dict


def _worker_job_resources(spec: WorkerSpec, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    profile = _worker_job_resource_profile(spec, payload or {})
    if profile:
        return profile
    requests_dict = {
        key: value
        for key, value in {
            "cpu": os.getenv("HLTHPRT_WORKER_JOB_CPU_REQUEST", "").strip(),
            "memory": os.getenv("HLTHPRT_WORKER_JOB_MEMORY_REQUEST", "").strip(),
        }.items()
        if value
    }
    limits_dict = {
        key: value
        for key, value in {
            "cpu": os.getenv("HLTHPRT_WORKER_JOB_CPU_LIMIT", "").strip(),
            "memory": os.getenv("HLTHPRT_WORKER_JOB_MEMORY_LIMIT", "").strip(),
        }.items()
        if value
    }
    resource_dict: dict[str, Any] = {}
    if requests_dict:
        resource_dict["requests"] = requests_dict
    if limits_dict:
        resource_dict["limits"] = limits_dict
    return resource_dict


def _worker_job_resource_profile(spec: WorkerSpec, payload: dict[str, Any]) -> dict[str, Any]:
    raw = os.getenv("HLTHPRT_WORKER_JOB_RESOURCE_PROFILES_JSON", "").strip()
    if not raw:
        return {}
    try:
        profiles = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    if not isinstance(profiles, dict):
        return {}
    resource_class = str(payload.get("resource_class") or "").strip()
    candidates = [
        spec.worker_class,
        spec.queue,
        resource_class,
        f"ptg:{resource_class}" if resource_class else "",
    ]
    for key in candidates:
        if not key:
            continue
        profile = profiles.get(key)
        if isinstance(profile, dict):
            normalized_resource_dict = _normalize_resource_profile(profile)
            if normalized_resource_dict:
                return normalized_resource_dict
    return {}


def _normalize_resource_profile(profile: dict[str, Any]) -> dict[str, Any]:
    resources_by_section: dict[str, Any] = {}
    for section in ("requests", "limits"):
        values = profile.get(section)
        if not isinstance(values, dict):
            continue
        normalized_resource_dict = {
            key: str(value).strip()
            for key, value in values.items()
            if key in {"cpu", "memory"} and str(value).strip()
        }
        if normalized_resource_dict:
            resources_by_section[section] = normalized_resource_dict
    return resources_by_section


def _worker_job_pvc_volumes() -> list[dict[str, Any]]:
    claim_name = os.getenv("HLTHPRT_WORKER_JOB_PVC_NAME", "").strip()
    mount_path = os.getenv("HLTHPRT_WORKER_JOB_PVC_MOUNT_PATH", "").strip()
    if not claim_name or not mount_path:
        return []

    volume_name = os.getenv("HLTHPRT_WORKER_JOB_PVC_VOLUME_NAME", "import-workdir").strip() or "import-workdir"
    return [
        {
            "volume": {
                "name": volume_name,
                "persistentVolumeClaim": {"claimName": claim_name},
            },
            "volumeMount": {
                "name": volume_name,
                "mountPath": mount_path,
            },
        }
    ]


def _worker_job_secret_volumes() -> list[dict[str, Any]]:
    raw = os.getenv("HLTHPRT_WORKER_JOB_SECRET_VOLUME_MOUNTS_JSON", "").strip()
    if not raw:
        return []
    try:
        mount_specs = json.loads(raw)
    except json.JSONDecodeError:
        return []
    if not isinstance(mount_specs, list):
        return []

    volumes: list[dict[str, Any]] = []
    used_names: set[str] = set()
    for index, mount_spec in enumerate(mount_specs):
        if not isinstance(mount_spec, dict):
            continue
        secret_name = str(mount_spec.get("secretName") or mount_spec.get("secret_name") or "").strip()
        mount_path = str(mount_spec.get("mountPath") or mount_spec.get("mount_path") or "").strip()
        if not secret_name or not mount_path:
            continue
        volume_name = _dns_safe(str(mount_spec.get("name") or secret_name))[:54].strip("-") or f"secret-{index}"
        if volume_name in used_names:
            volume_name = f"{volume_name[:50].rstrip('-')}-{index}"
        used_names.add(volume_name)
        secret_dict: dict[str, Any] = {"secretName": secret_name}
        if bool(mount_spec.get("optional", False)):
            secret_dict["optional"] = True
        volume_mount_dict: dict[str, Any] = {
            "name": volume_name,
            "mountPath": mount_path,
            "readOnly": mount_spec.get("readOnly", mount_spec.get("read_only", True)) is not False,
        }
        sub_path = str(mount_spec.get("subPath") or mount_spec.get("sub_path") or "").strip()
        if sub_path:
            volume_mount_dict["subPath"] = sub_path
        volumes.append(
            {
                "volume": {"name": volume_name, "secret": secret_dict},
                "volumeMount": volume_mount_dict,
            }
        )
    return volumes


def _worker_job_name(spec: WorkerSpec, payload: dict[str, Any]) -> str:
    run_id = str(payload.get("run_id") or payload.get("import_id") or "adhoc").strip()
    seed = f"{_ENGINE_LABEL}:{spec.worker_class}:{spec.role}:{run_id}"
    suffix = hashlib.sha1(seed.encode("utf-8")).hexdigest()[:10]
    base = _dns_safe(f"hpw-{_ENGINE_LABEL}-{spec.worker_class}-{spec.role}")
    return f"{base[:52]}-{suffix}"[:63].rstrip("-")


def _worker_python() -> str:
    return os.getenv("HLTHPRT_WORKER_JOB_PYTHON", "/opt/venv/bin/python")


def _uses_single_job_worker(spec: WorkerSpec) -> bool:
    return spec.role == "start" and (
        spec.worker_class.startswith("process.PTG")
        or spec.worker_class
        in {"process.MRFSourceDiscovery", "process.ProviderDirectoryFHIR"}
    )


def _single_job_worker_target(
    spec: WorkerSpec,
    payload: dict[str, Any],
) -> str | None:
    if not _uses_single_job_worker(spec):
        return None
    queued_job_id = str(payload.get("job_id") or "").strip()
    if queued_job_id:
        return queued_job_id
    run_id = str(payload.get("run_id") or "").strip()
    if spec.worker_class.startswith("process.PTG") and run_id:
        return f"ptg_start_{run_id}"
    return None


def _worker_command(python: str, spec: WorkerSpec) -> list[str]:
    if _uses_single_job_worker(spec):
        return [python, str(_main_path()), "worker-once", spec.worker_class]
    return [python, str(_main_path()), "worker", spec.worker_class, "--burst"]


def _kubernetes_namespace() -> str:
    override = os.getenv("HLTHPRT_WORKER_JOB_NAMESPACE", "").strip()
    if override:
        return override
    try:
        return _K8S_API_NAMESPACE.read_text(encoding="utf-8").strip()
    except OSError:
        return "default"


def _is_kubernetes_configured() -> bool:
    return bool(os.getenv("KUBERNETES_SERVICE_HOST")) and _K8S_API_TOKEN.exists()


class _KubernetesApiError(RuntimeError):
    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status


def _kubernetes_request(method: str, path: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
    host = os.getenv("KUBERNETES_SERVICE_HOST", "").strip()
    port = os.getenv("KUBERNETES_SERVICE_PORT", "443").strip()
    if not host:
        raise _KubernetesApiError(0, "KUBERNETES_SERVICE_HOST is not configured")
    try:
        token = _K8S_API_TOKEN.read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise _KubernetesApiError(0, f"cannot read Kubernetes service account token: {exc}") from exc
    data = None if body is None else json.dumps(body).encode("utf-8")
    request = urllib.request.Request(
        f"https://{host}:{port}{path}",
        data=data,
        method=method,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        },
    )
    context = ssl.create_default_context(cafile=str(_K8S_API_CA)) if _K8S_API_CA.exists() else ssl.create_default_context()
    try:
        with urllib.request.urlopen(request, context=context, timeout=10) as response:  # nosec B310 - in-cluster API URL
            raw = response.read()
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise _KubernetesApiError(exc.code, detail or exc.reason) from exc
    except urllib.error.URLError as exc:
        raise _KubernetesApiError(0, str(exc.reason)) from exc
    return json.loads(raw.decode("utf-8")) if raw else {}


def _label_hash(value: str) -> str:
    return hashlib.sha1(value.encode("utf-8")).hexdigest()[:16]


def _dns_safe(value: str) -> str:
    chars = [char.lower() if char.isalnum() else "-" for char in value]
    return "-".join(part for part in "".join(chars).split("-") if part) or "worker"


def _csv(raw: str) -> list[str]:
    return [item.strip() for item in raw.split(",") if item.strip()]


def _worker_start_replicas(spec: WorkerSpec) -> int:
    if spec.role != "start":
        return 1
    for item in _csv(os.getenv("HLTHPRT_WORKER_JOB_START_REPLICAS", "")):
        name, separator, raw_count = item.partition("=")
        if not separator or name.strip() not in {spec.worker_class, spec.queue}:
            continue
        try:
            return max(1, int(raw_count.strip()))
        except ValueError:
            return 1
    return 1


def _read_pid(path: Path) -> int | None:
    try:
        raw = path.read_text(encoding="utf-8").strip()
    except OSError:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def _is_pid_running(pid: int | None) -> bool:
    if not pid or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _is_pid_spec_match(pid: int | None, spec: WorkerSpec) -> bool:
    if not pid:
        return False
    try:
        output = subprocess.check_output(["ps", "eww", "-p", str(pid), "-o", "command="], text=True)
    except Exception:
        try:
            output = subprocess.check_output(["ps", "-p", str(pid), "-o", "command="], text=True)
        except Exception:
            return True
    return _is_process_worker_spec_match(output, spec)


def _is_pid_on_current_node(pid: int | None) -> bool:
    if not pid:
        return False
    try:
        output = subprocess.check_output(["ps", "eww", "-p", str(pid), "-o", "command="], text=True)
    except Exception:
        try:
            output = subprocess.check_output(["ps", "-p", str(pid), "-o", "command="], text=True)
        except Exception:
            return True
    return _is_current_node_match(output)


def _find_running_pid(spec: WorkerSpec) -> int | None:
    try:
        output = subprocess.check_output(["ps", "eww", "-axo", "pid=,command="], text=True)
    except Exception:
        try:
            output = subprocess.check_output(["ps", "-axo", "pid=,command="], text=True)
        except Exception:
            return None
    for line in output.splitlines():
        text = line.strip()
        if " rg " in text or not _is_process_worker_spec_match(text, spec):
            continue
        raw_pid = text.split(None, 1)[0]
        try:
            pid = int(raw_pid)
        except ValueError:
            continue
        if pid != os.getpid():
            return pid
    return None


def _is_process_worker_spec_match(process_text: str, spec: WorkerSpec) -> bool:
    if not _is_current_node_match(process_text):
        return False
    parts = process_text.split()
    for index, part in enumerate(parts[:-2]):
        if part.endswith("main.py") and parts[index + 1] in {"worker", "worker-once"} and parts[index + 2] == spec.worker_class:
            return True
    return False


def _is_current_node_match(process_text: str) -> bool:
    node_id = os.getenv("HLTHPRT_IMPORT_NODE_ID", "").strip()
    if not node_id:
        return True
    key = "HLTHPRT_IMPORT_NODE_ID="
    if key not in process_text:
        return True
    return f"{key}{node_id}" in process_text


def _remove_stale_pid(spec: WorkerSpec) -> None:
    try:
        _pid_path(spec).unlink()
    except OSError:
        return


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _main_path() -> Path:
    return _repo_root() / "main.py"


def _state_dir() -> Path:
    return Path(os.getenv("HLTHPRT_WORKER_STATE_DIR") or "/tmp/healthporta-workers/mrf").resolve()


def _log_dir() -> Path:
    return Path(os.getenv("HLTHPRT_WORKER_LOG_DIR") or "/tmp/healthporta-workers/mrf/logs").resolve()


def _safe_name(value: str) -> str:
    return value.replace(":", "_").replace(".", "_").replace("/", "_")


def _pid_path(spec: WorkerSpec) -> Path:
    return _state_dir() / f"{_safe_name(spec.worker_class)}.pid"


def _log_path(spec: WorkerSpec) -> Path:
    return _log_dir() / f"{_safe_name(spec.worker_class)}.log"


__all__ = ["ensure_worker", "worker_registry"]
