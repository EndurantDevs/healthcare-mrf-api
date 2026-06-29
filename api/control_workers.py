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


_START_WORKERS: tuple[WorkerSpec, ...] = (
    WorkerSpec("arq:PTG", "process.PTG", ("ptg",)),
    WorkerSpec("arq:PTGSmall", "process.PTGSmall", ("ptg",)),
    WorkerSpec("arq:PTGNormal", "process.PTGNormal", ("ptg",)),
    WorkerSpec("arq:PTGLarge", "process.PTGLarge", ("ptg",)),
    WorkerSpec("arq:PTGHuge", "process.PTGHuge", ("ptg",)),
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


def worker_registry() -> list[dict[str, Any]]:
    return [_worker_state(spec) for spec in _WORKERS]


def ensure_worker(payload: dict[str, Any]) -> dict[str, Any]:
    specs = _resolve_specs(payload)
    if not specs:
        importer = str(payload.get("importer") or "").strip()
        queue = str(payload.get("queue") or "").strip()
        return {
            "status": "unsupported",
            "items": [],
            "message": f"no worker is registered for {queue or importer or 'request'}",
        }

    items = [_ensure_spec(spec, payload) for spec in specs]
    status = "already_running"
    if any(item["status"] == "started" for item in items):
        status = "started"
    if any(item["status"] in {"failed", "blocked", "unsupported"} for item in items):
        status = "failed"
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
    except Exception as exc:  # pylint: disable=broad-exception-caught
        return {**state, "status": "failed", "message": str(exc)}
    return {**_worker_state(spec), "status": "started", "pid": pid}


def _start_process(spec: WorkerSpec, payload: dict[str, Any]) -> int:
    state_dir = _state_dir()
    log_dir = _log_dir()
    state_dir.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = _log_path(spec)
    cmd = [sys.executable, str(_main_path()), "worker", spec.worker_class, "--burst"]
    env = os.environ.copy()
    env["HLTHPRT_ACTIVE_WORKER_CLASS"] = spec.worker_class
    env["HLTHPRT_ACTIVE_WORKER_QUEUE"] = spec.queue
    import_id = str(payload.get("import_id") or "").strip()
    if import_id:
        env["HLTHPRT_IMPORT_ID_OVERRIDE"] = import_id
    with log_path.open("ab") as log_handle:
        process = subprocess.Popen(  # pylint: disable=consider-using-with
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
    running = _pid_running(pid) and _pid_matches_spec(pid, spec)
    if pid and not running:
        _remove_stale_pid(spec)
    if not running:
        pid = _find_running_pid(spec)
        running = _pid_running(pid)
        if running and pid:
            try:
                _state_dir().mkdir(parents=True, exist_ok=True)
                _pid_path(spec).write_text(str(pid), encoding="utf-8")
            except OSError:
                pass
    return {
        "queue": spec.queue,
        "worker_class": spec.worker_class,
        "importers": list(spec.importers),
        "role": spec.role,
        "running": running,
        "pid": pid if running else None,
        "pid_path": str(_pid_path(spec)),
        "log_path": str(_log_path(spec)),
        "command": " ".join([sys.executable, str(_main_path()), "worker", spec.worker_class, "--burst"]),
    }


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
    if state.get("job_status") in {"succeeded", "failed"} and state.get("job_name"):
        try:
            _delete_kubernetes_job(namespace, str(state["job_name"]))
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


def _delete_kubernetes_job(namespace: str, job_name: str) -> None:
    encoded = urllib.parse.quote(job_name, safe="")
    body = {
        "apiVersion": "v1",
        "kind": "DeleteOptions",
        "propagationPolicy": "Background",
    }
    _kubernetes_request("DELETE", f"/apis/batch/v1/namespaces/{namespace}/jobs/{encoded}", body)


def _kubernetes_worker_state(spec: WorkerSpec, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    base = {
        "queue": spec.queue,
        "worker_class": spec.worker_class,
        "importers": list(spec.importers),
        "role": spec.role,
        "running": False,
        "pid": None,
        "launcher": "kubernetes",
        "command": " ".join([_worker_python(), str(_main_path()), "worker", spec.worker_class, "--burst"]),
    }
    if not _kubernetes_configured():
        return {**base, "job_name": _worker_job_name(spec, payload or {}), "job_status": "unconfigured"}

    labels = {
        "app.kubernetes.io/managed-by": "healthporta-worker-launcher",
        "healthporta.com/engine": _ENGINE_LABEL,
        "healthporta.com/worker-class-hash": _label_hash(spec.worker_class),
        "healthporta.com/role": spec.role,
    }
    run_id = str((payload or {}).get("run_id") or "").strip()
    if run_id:
        labels["healthporta.com/run-id-hash"] = _label_hash(run_id)
    selector = ",".join(f"{key}={value}" for key, value in labels.items())
    namespace = _kubernetes_namespace()
    path = f"/apis/batch/v1/namespaces/{namespace}/jobs?{urllib.parse.urlencode({'labelSelector': selector})}"
    try:
        body = _kubernetes_request("GET", path)
    except _KubernetesApiError as exc:
        return {**base, "job_name": _worker_job_name(spec, payload or {}), "job_status": "error", "message": str(exc)}

    items = body.get("items") if isinstance(body, dict) else []
    jobs = [item for item in items if isinstance(item, dict)]
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
    return {
        **base,
        "running": active > 0,
        "job_name": latest_name,
        "job_status": job_status,
        "active_jobs": active,
        "succeeded_jobs": succeeded,
        "failed_jobs": failed,
    }


def _worker_job_manifest(spec: WorkerSpec, payload: dict[str, Any], image: str) -> dict[str, Any]:
    job_name = _worker_job_name(spec, payload)
    env = [
        {"name": "HLTHPRT_WORKER_LAUNCHER", "value": "process"},
        {"name": "HLTHPRT_IMPORT_NODE_ID", "value": os.getenv("HLTHPRT_IMPORT_NODE_ID", "")},
        {"name": "HLTHPRT_ACTIVE_WORKER_CLASS", "value": spec.worker_class},
        {"name": "HLTHPRT_ACTIVE_WORKER_QUEUE", "value": spec.queue},
    ]
    import_id = str(payload.get("import_id") or "").strip()
    if import_id:
        env.append({"name": "HLTHPRT_IMPORT_ID_OVERRIDE", "value": import_id})

    container: dict[str, Any] = {
        "name": "worker",
        "image": image,
        "imagePullPolicy": os.getenv("HLTHPRT_WORKER_JOB_IMAGE_PULL_POLICY", "IfNotPresent"),
        "workingDir": str(_repo_root()),
        "command": [_worker_python(), str(_main_path()), "worker", spec.worker_class, "--burst"],
        "env": env,
        "securityContext": _worker_job_container_security_context(),
    }
    env_from = _worker_job_env_from()
    if env_from:
        container["envFrom"] = env_from
    resources = _worker_job_resources(spec, payload)
    if resources:
        container["resources"] = resources
    volumes = _worker_job_pvc_volumes()
    if volumes:
        container["volumeMounts"] = [item["volumeMount"] for item in volumes]

    pod_spec: dict[str, Any] = {
        "restartPolicy": "Never",
        "automountServiceAccountToken": False,
        "securityContext": _worker_job_pod_security_context(has_pvc=bool(volumes)),
        "containers": [container],
    }
    if volumes:
        pod_spec["volumes"] = [item["volume"] for item in volumes]
    service_account = os.getenv("HLTHPRT_WORKER_JOB_SERVICE_ACCOUNT", "").strip()
    if service_account:
        pod_spec["serviceAccountName"] = service_account
    pull_secret = os.getenv("HLTHPRT_WORKER_JOB_IMAGE_PULL_SECRET", "").strip()
    if pull_secret:
        pod_spec["imagePullSecrets"] = [{"name": item} for item in _csv(pull_secret)]

    labels = {
        "app.kubernetes.io/name": "healthporta-import-worker",
        "app.kubernetes.io/managed-by": "healthporta-worker-launcher",
        "healthporta.com/engine": _ENGINE_LABEL,
        "healthporta.com/worker-class-hash": _label_hash(spec.worker_class),
        "healthporta.com/role": spec.role,
    }
    run_id = str(payload.get("run_id") or "").strip()
    if run_id:
        labels["healthporta.com/run-id-hash"] = _label_hash(run_id)
    job_spec: dict[str, Any] = {
        "backoffLimit": int(os.getenv("HLTHPRT_WORKER_JOB_BACKOFF_LIMIT", "0")),
        "ttlSecondsAfterFinished": int(os.getenv("HLTHPRT_WORKER_JOB_TTL_SECONDS", "86400")),
        "template": {
            "metadata": {"labels": labels},
            "spec": pod_spec,
        },
    }
    active_deadline_seconds = int(os.getenv("HLTHPRT_WORKER_JOB_ACTIVE_DEADLINE_SECONDS", "0") or "0")
    if active_deadline_seconds > 0:
        job_spec["activeDeadlineSeconds"] = active_deadline_seconds
    replicas = _worker_start_replicas(spec)
    if replicas > 1:
        job_spec["parallelism"] = replicas
        job_spec["completions"] = replicas

    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": job_name,
            "labels": labels,
            "annotations": {
                "healthporta.com/queue": spec.queue,
                "healthporta.com/worker-class": spec.worker_class,
                "healthporta.com/importers": ",".join(spec.importers),
                "healthporta.com/run-id": run_id,
            },
        },
        "spec": job_spec,
    }


def _worker_job_env_from() -> list[dict[str, Any]]:
    env_from: list[dict[str, Any]] = []
    for name in _csv(os.getenv("HLTHPRT_WORKER_JOB_ENV_FROM_CONFIGMAP", "")):
        env_from.append({"configMapRef": {"name": name}})
    for name in _csv(os.getenv("HLTHPRT_WORKER_JOB_ENV_FROM_SECRET", "")):
        env_from.append({"secretRef": {"name": name}})
    return env_from


def _worker_job_container_security_context() -> dict[str, Any]:
    return {
        "allowPrivilegeEscalation": False,
        "capabilities": {"drop": ["ALL"]},
    }


def _worker_job_pod_security_context(*, has_pvc: bool) -> dict[str, Any]:
    security_context: dict[str, Any] = {
        "runAsNonRoot": True,
        "runAsUser": 65534,
        "runAsGroup": 65534,
        "seccompProfile": {"type": "RuntimeDefault"},
    }
    if has_pvc:
        security_context["fsGroup"] = 65534
        security_context["fsGroupChangePolicy"] = "OnRootMismatch"
    return security_context


def _worker_job_resources(spec: WorkerSpec, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    profile = _worker_job_resource_profile(spec, payload or {})
    if profile:
        return profile
    requests = {
        key: value
        for key, value in {
            "cpu": os.getenv("HLTHPRT_WORKER_JOB_CPU_REQUEST", "").strip(),
            "memory": os.getenv("HLTHPRT_WORKER_JOB_MEMORY_REQUEST", "").strip(),
        }.items()
        if value
    }
    limits = {
        key: value
        for key, value in {
            "cpu": os.getenv("HLTHPRT_WORKER_JOB_CPU_LIMIT", "").strip(),
            "memory": os.getenv("HLTHPRT_WORKER_JOB_MEMORY_LIMIT", "").strip(),
        }.items()
        if value
    }
    resources: dict[str, Any] = {}
    if requests:
        resources["requests"] = requests
    if limits:
        resources["limits"] = limits
    return resources


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
            normalized = _normalize_resource_profile(profile)
            if normalized:
                return normalized
    return {}


def _normalize_resource_profile(profile: dict[str, Any]) -> dict[str, Any]:
    resources: dict[str, Any] = {}
    for section in ("requests", "limits"):
        values = profile.get(section)
        if not isinstance(values, dict):
            continue
        normalized = {
            key: str(value).strip()
            for key, value in values.items()
            if key in {"cpu", "memory"} and str(value).strip()
        }
        if normalized:
            resources[section] = normalized
    return resources


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


def _worker_job_name(spec: WorkerSpec, payload: dict[str, Any]) -> str:
    run_id = str(payload.get("run_id") or payload.get("import_id") or "adhoc").strip()
    seed = f"{_ENGINE_LABEL}:{spec.worker_class}:{spec.role}:{run_id}"
    suffix = hashlib.sha1(seed.encode("utf-8")).hexdigest()[:10]
    base = _dns_safe(f"hpw-{_ENGINE_LABEL}-{spec.worker_class}-{spec.role}")
    return f"{base[:52]}-{suffix}"[:63].rstrip("-")


def _worker_python() -> str:
    return os.getenv("HLTHPRT_WORKER_JOB_PYTHON", "/opt/venv/bin/python")


def _kubernetes_namespace() -> str:
    override = os.getenv("HLTHPRT_WORKER_JOB_NAMESPACE", "").strip()
    if override:
        return override
    try:
        return _K8S_API_NAMESPACE.read_text(encoding="utf-8").strip()
    except OSError:
        return "default"


def _kubernetes_configured() -> bool:
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


def _pid_running(pid: int | None) -> bool:
    if not pid or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _pid_matches_spec(pid: int | None, spec: WorkerSpec) -> bool:
    if not pid:
        return False
    try:
        output = subprocess.check_output(["ps", "eww", "-p", str(pid), "-o", "command="], text=True)
    except Exception:  # pylint: disable=broad-exception-caught
        try:
            output = subprocess.check_output(["ps", "-p", str(pid), "-o", "command="], text=True)
        except Exception:  # pylint: disable=broad-exception-caught
            return True
    return _process_matches_worker_spec(output, spec)


def _pid_matches_current_node(pid: int | None) -> bool:
    if not pid:
        return False
    try:
        output = subprocess.check_output(["ps", "eww", "-p", str(pid), "-o", "command="], text=True)
    except Exception:  # pylint: disable=broad-exception-caught
        try:
            output = subprocess.check_output(["ps", "-p", str(pid), "-o", "command="], text=True)
        except Exception:  # pylint: disable=broad-exception-caught
            return True
    return _matches_current_node(output)


def _find_running_pid(spec: WorkerSpec) -> int | None:
    try:
        output = subprocess.check_output(["ps", "eww", "-axo", "pid=,command="], text=True)
    except Exception:  # pylint: disable=broad-exception-caught
        try:
            output = subprocess.check_output(["ps", "-axo", "pid=,command="], text=True)
        except Exception:  # pylint: disable=broad-exception-caught
            return None
    for line in output.splitlines():
        text = line.strip()
        if " rg " in text or not _process_matches_worker_spec(text, spec):
            continue
        raw_pid = text.split(None, 1)[0]
        try:
            pid = int(raw_pid)
        except ValueError:
            continue
        if pid != os.getpid():
            return pid
    return None


def _process_matches_worker_spec(process_text: str, spec: WorkerSpec) -> bool:
    if not _matches_current_node(process_text):
        return False
    parts = process_text.split()
    for index, part in enumerate(parts[:-2]):
        if part.endswith("main.py") and parts[index + 1] == "worker" and parts[index + 2] == spec.worker_class:
            return True
    return False


def _matches_current_node(process_text: str) -> bool:
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
        pass


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
