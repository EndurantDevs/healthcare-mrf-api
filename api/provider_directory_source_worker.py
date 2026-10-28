# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Select one fixed FHIR source bootstrap from an observed native queue job."""

import json
import os
import re
from dataclasses import dataclass
from importlib import import_module

from api.reference_source_worker import _IMAGE
from api.reference_source_worker import IMAGE_ENV as REFERENCE_IMAGE_ENV

PURPOSE = "provider-directory-source"
COMMAND = ["/usr/local/bin/fhir-source-worker"]
IMAGE_ENV = "HLTHPRT_PROVIDER_DIRECTORY_SOURCE_WORKER_IMAGE"
ROUTES_ENV = "HLTHPRT_PROVIDER_DIRECTORY_SOURCE_WORKER_ROUTES_JSON"
_WORKER_CLASS = "process.ProviderDirectoryFHIR"
_IMPORTER = "provider-directory-fhir"
_QUEUE = "arq:ProviderDirectoryFHIR"
_FUNCTION = "control_single_job_start"


@dataclass(frozen=True)
class FHIRSourceWorkerLaunch:
    """Keep the owner image and observed native job outside serialized input."""

    run_id: str
    job_id: str
    image: str
    root_run_id: str = ""
    secret_name: str = ""
    source_id: str = ""
    endpoint_scope: str = ""
    retry_of_run_id: str = ""


def _configured_routes():
    """Validate the complete server-owned route inventory before selection."""
    raw = os.getenv(ROUTES_ENV, "").strip()
    if not raw:
        return []
    try:
        routes = json.loads(raw)
    except json.JSONDecodeError as error:
        raise ValueError("fhir_source_worker_route_invalid") from error
    if type(routes) is not list or len(routes) > 32:
        raise ValueError("fhir_source_worker_route_invalid")
    identities = set()
    for route in routes:
        if (
            type(route) is not dict
            or set(route) != {"root_run_id", "source_id", "endpoint_scope", "image", "secret_name"}
            or not (
                type(route["root_run_id"]) is str
                and re.fullmatch(r"run_[0-9a-f]{32}", route["root_run_id"])
                and type(route["source_id"]) is str
                and re.fullmatch(r"[A-Za-z0-9_-]{1,96}", route["source_id"])
                and type(route["endpoint_scope"]) is str
                and route["endpoint_scope"] == route["endpoint_scope"].strip()
                and route["endpoint_scope"]
                and type(route["image"]) is str
                and _IMAGE.fullmatch(route["image"])
                and type(route["secret_name"]) is str
                and re.fullmatch(r"[a-z0-9]([-a-z0-9]*[a-z0-9])?", route["secret_name"])
            )
        ):
            raise ValueError("fhir_source_worker_route_invalid")
        route_identity_fields = tuple(route[key] for key in ("root_run_id", "source_id", "endpoint_scope"))
        if route_identity_fields in identities:
            raise ValueError("fhir_source_worker_route_invalid")
        identities.add(route_identity_fields)
    return routes


def _source_route(admitted_run):
    """Resolve a private exact retry route from persisted native run coordinates."""
    params = admitted_run.get("params")
    params = params if type(params) is dict else {}
    root = params.get("provider_directory_pagination_root_run_id")
    root_routes = [route for route in _configured_routes() if route["root_run_id"] == root]
    if not root_routes:
        return None
    matches = [
        route
        for route in root_routes
        if params.get("source_ids") == [route["source_id"]]
        and params.get("provider_directory_endpoint_scope") == route["endpoint_scope"]
        and type(params.get("retry_of_run_id")) is str
        and re.fullmatch(r"run_[0-9a-f]{32}", params["retry_of_run_id"])
    ]
    if len(matches) != 1:
        raise ValueError("fhir_source_worker_route_changed")
    return matches[0]


def _is_launch_secret_selected(selection, launch, *, allow_shared=False):
    """Keep routed credentials isolated while allowing explicitly shared env."""
    secret_name = str(selection.get("secretName") or selection.get("secret_name") or "").strip()
    route_secret_names = {route["secret_name"] for route in _configured_routes()}
    shared_for_routes = selection.get("sharedForRoutes", False)
    if type(shared_for_routes) is not bool or (shared_for_routes and not allow_shared):
        raise ValueError("worker purpose shared secret selector is invalid")
    if shared_for_routes:
        if secret_name in route_secret_names:
            raise ValueError("worker purpose shared secret selector is invalid")
        return True
    if launch.secret_name:
        return secret_name == launch.secret_name
    return secret_name not in route_secret_names


def admit_fhir_source_worker_launch(admitted_run):
    """Require the configured immutable source image and original queued identity."""
    if admitted_run.get("importer") != _IMPORTER:
        return None
    metrics = admitted_run.get("metrics")
    image = os.getenv(IMAGE_ENV, os.getenv(REFERENCE_IMAGE_ENV, ""))
    if (
        not isinstance(metrics, dict)
        or metrics.get("queue") != _QUEUE
        or metrics.get("function") != _FUNCTION
        or any(
            type(identity) is not str or not identity or identity != identity.strip()
            for identity in (admitted_run.get("run_id"), metrics.get("job_id"))
        )
    ):
        raise ValueError("fhir_source_worker_admission_unavailable")
    params_by_name = admitted_run.get("params")
    if (
        isinstance(params_by_name, dict)
        and params_by_name.get("provider_directory_dataset_followup_stage") == "address_overlay"
    ):
        from process.provider_directory_generic_source_artifacts import ordinary_overlay_followup

        directory = import_module("process.provider_directory_fhir")
        if ordinary_overlay_followup(directory, params_by_name) is not None:
            if {"test_mode", "run_id"}.intersection(params_by_name):
                raise ValueError("fhir_source_worker_admission_unavailable")
            return None
    route = _source_route(admitted_run)
    if route is not None:
        image = route["image"]
    if _IMAGE.fullmatch(image) is None:
        raise ValueError("fhir_source_worker_admission_unavailable")
    return FHIRSourceWorkerLaunch(
        admitted_run["run_id"],
        metrics["job_id"],
        image,
        route["root_run_id"] if route else "",
        route["secret_name"] if route else "",
        route["source_id"] if route else "",
        route["endpoint_scope"] if route else "",
        params_by_name["retry_of_run_id"] if route else "",
    )


def _lineage_by_id(lineage_rows):
    """Normalize the bounded native rows before checking the retry chain."""
    return {
        lineage_record["run_id"]: lineage_record
        for lineage_record in (
            dict(raw_record._mapping) if hasattr(raw_record, "_mapping") else dict(raw_record)
            for raw_record in lineage_rows
        )
    }


async def require_fhir_source_route_ancestry(connection, launch):
    """Accept a configured route only for a genuine failed-parent retry chain."""
    if type(launch) is not FHIRSourceWorkerLaunch or not launch.root_run_id:
        return
    from sqlalchemy import text

    from api.control_imports import ImportRun
    from db.plan_release_storage import storage_relation

    run_relation = storage_relation(str(ImportRun.__table__.schema), "import_run")
    lineage_rows = await connection.all(
        text(
            "SELECT run_id,importer,status,retry_of_run_id,params FROM "
            + run_relation
            + " WHERE importer='provider-directory-fhir' AND "
            "(run_id=:root OR params::jsonb->>'provider_directory_pagination_root_run_id'=:root) LIMIT 65"
        ),
        root=launch.root_run_id,
    )
    runs_by_id = _lineage_by_id(lineage_rows)
    if len(lineage_rows) > 64 or len(runs_by_id) != len(lineage_rows):
        raise ValueError("fhir_source_worker_route_changed")
    current_id = launch.run_id
    seen_run_ids = set()
    while current_id not in seen_run_ids and len(seen_run_ids) <= 64:
        seen_run_ids.add(current_id)
        lineage_record = runs_by_id.get(current_id)
        if lineage_record is None:
            break
        params = lineage_record["params"]
        if not (
            type(params) is dict
            and params.get("source_ids") == [launch.source_id]
            and params.get("provider_directory_endpoint_scope") == launch.endpoint_scope
            and (
                current_id == launch.root_run_id
                or params.get("provider_directory_pagination_root_run_id") == launch.root_run_id
            )
        ):
            break
        if current_id == launch.root_run_id:
            if lineage_record["retry_of_run_id"] is None and lineage_record["status"] == "failed":
                return
            break
        parent_id = lineage_record["retry_of_run_id"]
        if not (
            type(parent_id) is str
            and params.get("retry_of_run_id") == parent_id
            and lineage_record["status"]
            in ({"queued", "starting", "running", "finalizing"} if current_id == launch.run_id else {"failed"})
            and (current_id != launch.run_id or parent_id == launch.retry_of_run_id)
        ):
            break
        current_id = parent_id
    raise ValueError("fhir_source_worker_route_changed")


def authorized_fhir_source_worker_launch(spec, launch_request):
    """Accept only the exact server-held capability and closed engine adapter."""
    from api.control_workers import _AdmittedWorkerRequest

    if type(launch_request) is not _AdmittedWorkerRequest:
        return None
    if getattr(launch_request, "cms_registry_launch", None) is not None:
        return None
    launch = getattr(launch_request, "fhir_source_launch", None)
    if launch is None:
        return None
    if type(launch) is not FHIRSourceWorkerLaunch:
        raise ValueError("fhir_source_worker_admission_unavailable")
    _require_fhir_job_identity(spec, launch_request, launch.run_id, launch.job_id)
    return launch


def admitted_overlay_job(spec, launch_request):
    """Select ordinary execution only for the overlay classified from the native run."""
    from api.control_workers import _AdmittedWorkerRequest

    if type(launch_request) is not _AdmittedWorkerRequest:
        return None
    identity = launch_request.ordinary_fhir_overlay_job
    if identity is None:
        return None
    if (
        type(identity) is not tuple
        or len(identity) != 5
        or identity != launch_request.admitted_job
        or launch_request.cms_registry_launch is not None
        or launch_request.fhir_source_launch is not None
    ):
        raise ValueError("fhir_source_worker_admission_unavailable")
    _require_fhir_job_identity(spec, launch_request, identity[0], identity[4])
    return identity[4]


def _require_fhir_job_identity(spec, launch_request, run_id, job_id):
    """Keep both runtimes on the same exact native adapter and queue identity."""
    from api.control_imports import _SINGLE_JOB_ADAPTERS

    adapter = _SINGLE_JOB_ADAPTERS.get(_IMPORTER, {})
    if (
        (spec.worker_class, spec.queue, spec.importers, spec.role) != (_WORKER_CLASS, _QUEUE, (_IMPORTER,), "start")
        or launch_request.admitted_job != (run_id, _IMPORTER, _QUEUE, _FUNCTION, job_id)
        or launch_request.get("run_id") != run_id
        or launch_request.get("importer", _IMPORTER) != _IMPORTER
        or launch_request.get("job_id") not in (None, "", job_id)
        or tuple(
            adapter.get(name)
            for name in ("queue", "function", "payload", "target_module", "target_function", "job_prefix")
        )
        != (
            _QUEUE,
            _FUNCTION,
            "control_wrapped",
            "process.provider_directory_fhir",
            "process_data",
            "provider_directory_start",
        )
        or adapter.get("run_shutdown") is not True
        or job_id != "provider_directory_start_" + run_id
    ):
        raise ValueError("fhir_source_worker_admission_unavailable")


def apply_fhir_source_worker_spec(spec, launch_request, labels, pod_spec):
    """Use the fixed non-root source runtime only after native purpose admission."""
    if authorized_fhir_source_worker_launch(spec, launch_request) is not None:
        labels["healthporta.com/worker-purpose"] = PURPOSE
        security = pod_spec["securityContext"]
        # Preserve the shared volume group to avoid rewriting existing file ownership.
        security.update(runAsUser=10001, runAsGroup=10001)


def _is_fhir_secret_scope(selection, worker_class):
    """Keep environment and mount selectors on the same exact Source purpose."""
    classes = selection.get("workerClasses", selection.get("worker_classes"))
    if (
        selection.get("purposes") != [PURPOSE]
        or selection.get("importers") != [_IMPORTER]
        or classes != [_WORKER_CLASS]
        or ("workerClasses" in selection and "worker_classes" in selection)
    ):
        raise ValueError("worker purpose secret selector is invalid")
    return worker_class == _WORKER_CLASS


def is_fhir_source_secret_selected(selection, worker_class, launch_request):
    """Select source credentials only for the exact admitted FHIR queue purpose."""
    if not _is_fhir_secret_scope(selection, worker_class):
        return False
    from api.control_workers import _BY_WORKER_CLASS

    spec = _BY_WORKER_CLASS.get(_WORKER_CLASS)
    launch = authorized_fhir_source_worker_launch(spec, launch_request) if spec is not None else None
    return launch is not None and _is_launch_secret_selected(selection, launch, allow_shared=True)


def is_fhir_source_volume_selected(selection, worker_class, launch):
    """Use the existing Source capability for explicitly purpose-scoped mounts."""
    return (
        _is_fhir_secret_scope(selection, worker_class)
        and type(launch) is FHIRSourceWorkerLaunch
        and _is_launch_secret_selected(selection, launch)
    )


def is_fhir_source_job_matching(job, launch, launch_request=None):
    """Refuse changed bootstrap code, native target or process isolation on reuse."""
    from api.control_workers import (
        _BY_WORKER_CLASS,
        _worker_job_container_security_context,
        _worker_job_pod_security_context,
        _worker_job_pvc_volumes,
        _worker_job_secret_volumes,
    )

    if type(launch) is not FHIRSourceWorkerLaunch:
        return False
    try:
        template = job["spec"]["template"]
        pod = template["spec"]
        containers = pod["containers"]
        security = pod["securityContext"]
        volumes = [*_worker_job_pvc_volumes(), *_worker_job_secret_volumes(_BY_WORKER_CLASS[_WORKER_CLASS], launch)]
        expected_security = _worker_job_pod_security_context(has_group_read_volume="fsGroup" in security)
        expected_security.update(runAsUser=10001, runAsGroup=10001)
        expected_env = None
        if launch_request is not None:
            from api.control_workers import _worker_job_manifest

            expected_env = _worker_job_manifest(_BY_WORKER_CLASS[_WORKER_CLASS], launch_request, launch.image)["spec"][
                "template"
            ]["spec"]["containers"][0]["env"]
        return (
            job["metadata"]["labels"]["healthporta.com/worker-purpose"] == PURPOSE
            and template["metadata"]["labels"]["healthporta.com/worker-purpose"] == PURPOSE
            and security == expected_security
            and pod["automountServiceAccountToken"] is False
            and all(not pod.get(key, False) for key in ("hostPID", "hostIPC", "hostNetwork"))
            and not pod.get("initContainers")
            and not pod.get("ephemeralContainers")
            and len(containers) == 1
            and containers[0]["securityContext"] == _worker_job_container_security_context()
            and containers[0]["image"] == launch.image
            and (expected_env is None or containers[0]["env"] == expected_env)
            and job["metadata"]["annotations"].get("healthporta.com/source-root-run-id", "") == launch.root_run_id
            and containers[0]["command"] == COMMAND
            and containers[0]["workingDir"] == "/app"
            and not containers[0].get("args")
            and not containers[0].get("lifecycle")
            and not containers[0].get("volumeDevices")
            and containers[0].get("volumeMounts", []) == [volume["volumeMount"] for volume in volumes]
            and _secret_volume_defaults(pod.get("volumes", []))
            == _secret_volume_defaults([volume["volume"] for volume in volumes])
            and all(
                [entry.get("value") for entry in containers[0]["env"] if entry.get("name") == name] == [expected]
                for name, expected in (
                    ("HLTHPRT_CONTROL_RUN_ID", launch.run_id),
                    ("HLTHPRT_WORKER_ONCE_TARGET_JOB_ID", launch.job_id),
                )
            )
        )
    except KeyError, TypeError, IndexError:
        return False


def _secret_volume_defaults(volumes):
    """Retain every binding field while allowing the API's Secret default 0644."""
    return [
        {**volume, "secret": {"defaultMode": 0o644, **volume["secret"]}} if "secret" in volume else volume
        for volume in volumes
    ]


def is_overlay_job_matching(job, expected_job):
    """Refuse reuse of a different runtime or native target for an ordinary overlay."""
    try:
        template_by_field = job["spec"]["template"]
        pod_by_field = template_by_field["spec"]
        expected_pod_by_field = expected_job["spec"]["template"]["spec"]
        containers = pod_by_field["containers"]
        expected_container_by_field = expected_pod_by_field["containers"][0]
        return (
            len(containers) == 1
            and all(
                containers[0].get(name) == expected_container_by_field.get(name)
                for name in (
                    "image",
                    "imagePullPolicy",
                    "workingDir",
                    "command",
                    "env",
                    "envFrom",
                    "securityContext",
                    "volumeMounts",
                )
            )
            and not any(containers[0].get(name) for name in ("args", "lifecycle", "volumeDevices"))
            and not any(
                pod_by_field.get(name)
                for name in (
                    "initContainers",
                    "ephemeralContainers",
                    "hostPID",
                    "hostIPC",
                    "hostNetwork",
                )
            )
            and pod_by_field["automountServiceAccountToken"] is False
            and pod_by_field.get("serviceAccountName", "default")
            == expected_pod_by_field.get("serviceAccountName", "default")
            and pod_by_field["securityContext"] == expected_pod_by_field["securityContext"]
            and _secret_volume_defaults(pod_by_field.get("volumes", []))
            == _secret_volume_defaults(expected_pod_by_field.get("volumes", []))
            and all(
                "healthporta.com/worker-purpose" not in metadata.get("labels", {})
                for metadata in (job["metadata"], template_by_field["metadata"])
            )
            and job["metadata"]["annotations"] == expected_job["metadata"]["annotations"]
        )
    except KeyError, TypeError, IndexError:
        return False


def is_fhir_job_set_matching(spec, launch_request, jobs):
    """Check existing FHIR Jobs against the runtime selected by native admission."""
    launch = authorized_fhir_source_worker_launch(spec, launch_request)
    if launch is not None:
        return all(is_fhir_source_job_matching(job, launch, launch_request) for job in jobs)
    if admitted_overlay_job(spec, launch_request) is None:
        return True
    from api.control_workers import _worker_job_manifest

    expected_job = _worker_job_manifest(spec, launch_request, os.getenv("HLTHPRT_WORKER_JOB_IMAGE", "").strip())
    return all(is_overlay_job_matching(job, expected_job) for job in jobs)
