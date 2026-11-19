# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Launch one protected catalog continuation from its actual managed handoff."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
from dataclasses import replace
from decimal import Decimal, DecimalException, localcontext

from sqlalchemy import text

from process import scoped_catalog_named_generation as generations
from process import scoped_catalog_named_publication as publication
from process.control_lifecycle import acquire_control_run_worker_action_lock
from process.scoped_catalog_layout import CONTROL_SCHEMA, DATA_SCHEMA, DATA_SCHEMA_ENV, catalog_schema
from process.scoped_catalog_publisher import PREFIX
from process.scoped_catalog_retention import canonical_metadata

ROLE = "scoped-catalog-publisher"
SECRET_NAME = "scoped-catalog-publisher"
SERVICE_ACCOUNT = "scoped-catalog-publisher"
SELECTORS = ("run_id", "attempt_id", "attempt_started_at", "node_id", "expected_lineage_id", "expected_generation")
_FIELDS = frozenset((*SELECTORS, "role", "importer", "status", "reason"))
_IMAGE = re.compile(r"[^\s@]+@sha256:[0-9a-f]{64}\Z")


_WORKER_SPECS = {
    ("arq:CodeSets", "process.CodeSets", ("code-sets",)),
    ("arq:MSDRG", "process.MSDRG", ("ms-drg",)),
}


def configured_code_sets_image(spec):
    """Use the compatible acquisition and continuation image for the selected catalog family."""
    if (spec.queue, spec.worker_class, spec.importers) not in _WORKER_SPECS or spec.role not in {"start", ROLE}:
        return None
    is_isolated = catalog_schema(CONTROL_SCHEMA) == DATA_SCHEMA
    if spec.importers == ("ms-drg",) and not is_isolated:
        return None
    image = os.getenv("HLTHPRT_CODE_SETS_WORKER_IMAGE", "").strip()
    if (is_isolated or image) and not _IMAGE.fullmatch(image):
        raise ValueError("code_sets_worker_image_invalid")
    return image or None


def is_publisher_request(request):
    """Route pending catalog repairs away from ordinary acquisition workers."""
    return request.get("role") == ROLE or (
        request.get("importer") in publication.IMPORTERS
        and (request.get("role") == "finish" or request.get("status") in {"finalizing", "canceling"})
    )


def selected_spec(spec, request):
    """Validate the fixed role and logical attempt selectors before choosing a worker."""
    if (
        request.get("role") != ROLE
        or request.get("importer") not in publication.IMPORTERS
        or request.get("status") not in {"finalizing", "canceling"}
        or set(request) - _FIELDS
        or any(type(request.get(key)) is not str or not 0 < len(request[key]) <= 128 for key in SELECTORS[:-1])
        or type(request.get("expected_generation")) is not int
        or not 0 <= request["expected_generation"] < 2**63
    ):
        raise ValueError("catalog publisher selector is invalid")
    return replace(spec, role=ROLE)


def _bind_repair_request(request, run, handoff, expected):
    """Derive the publisher coordinates from the locked run, never from repair hints."""
    from api import control_workers as workers

    spec = workers._BY_IMPORTER_ROLE[(run["importer"], "start")]
    hints_by_field = {
        "queue": spec.queue,
        "worker_class": spec.worker_class,
        "job_id": run["metrics"].get("job_id"),
        "import_id": run["params"].get("import_id"),
        "resource_class": run["params"].get("resource_class") or run["params"].get("_resource_class"),
    }
    selected_by_field = {
        "importer": run["importer"],
        **{key: handoff[key] for key in SELECTORS[:4]},
        "expected_lineage_id": expected["local_lineage_id"],
        "expected_generation": expected["local_generation"],
    }
    if (
        request.get("role") not in {None, "finish"}
        or request.get("status") not in {"finalizing", "canceling"}
        or set(request) - _FIELDS - hints_by_field.keys()
        or any(
            type(request[key]) is not type(expected_selector) or request[key] != expected_selector
            for key, expected_selector in selected_by_field.items()
            if key in request
        )
        or any(request[key] != expected_hint for key, expected_hint in hints_by_field.items() if key in request)
    ):
        raise ValueError("catalog repair selector differs from the managed run")
    selected_by_field.update(role=ROLE, status=request["status"])
    if "reason" in request:
        selected_by_field["reason"] = request["reason"]
    request.clear()
    request.update(selected_by_field)


async def _admit(connection, request, schema, *, is_repair=False):
    """Lock and validate the persisted managed attempt before its publisher can start."""
    from process.scoped_catalog_named_storage import qualified

    run_records = await connection.all(
        text(
            f"SELECT run_id,engine,importer,node_id,status,progress,metrics,params,finished_at,error "
            f"FROM {qualified(schema, 'import_run')} WHERE run_id=:run_id FOR UPDATE"
        ),
        run_id=request["run_id"],
    )
    if len(run_records) != 1:
        raise ValueError("catalog publisher run is unavailable")
    run_record = run_records[0]
    run = dict(run_record._mapping) if hasattr(run_record, "_mapping") else dict(run_record)
    for name in ("progress", "metrics", "params"):
        if type(run.get(name)) is str and len(run[name]) <= 262144:
            run[name] = json.loads(run[name])
        if type(run.get(name)) is not dict:
            raise ValueError("catalog publisher run content is unavailable")
    handoff = publication.validate_handoff((run["metrics"] or {}).get(publication.METRIC))
    expected = generations.logical_generation(handoff["expected_generations"][run["importer"]])
    if is_repair:
        _bind_repair_request(request, run, handoff, expected)
    if (
        run["engine"] != "healthcare-mrf-api"
        or run["error"] is not None
        or run["importer"] != request["importer"]
        or run["node_id"] != request["node_id"]
        or handoff["schema_name"] != catalog_schema(schema)
        or any(handoff[key] != request[key] for key in SELECTORS[:4])
        or any((run["progress"] or {}).get(key) != request[key] for key in ("attempt_id", "attempt_started_at"))
        or (expected["local_lineage_id"], expected["local_generation"])
        != (request["expected_lineage_id"], request["expected_generation"])
        or publication.digest(
            {
                "importer": run["importer"],
                "params": run["params"],
                "options": {"include_relationships": handoff["include_relationships"], "test_mode": False},
                "metrics": handoff["metrics"],
            }
        )
        != handoff["source_contract_sha256"]
    ):
        raise ValueError("catalog publisher attempt changed")
    if run["status"] in {"succeeded", "canceled"} and run["finished_at"] is not None:
        metric = "scoped_catalog_publication" if run["status"] == "succeeded" else "scoped_catalog_cancellation"
        receipt = run["metrics"].get(metric)
        if type(receipt) is not dict or receipt.get("handoff") != handoff:
            raise ValueError("catalog publisher terminal result differs")
        return run["status"]
    if (
        run["status"] != request["status"]
        or run["status"] not in {"finalizing", "canceling"}
        or run["finished_at"] is not None
    ):
        raise ValueError("catalog publisher run is not pending")
    return None


def acquisition_environment(spec):
    """Pass the target's split selection to its two catalog acquisition workers only."""
    if (spec.queue, spec.worker_class, spec.importers) not in _WORKER_SPECS:
        return []
    schema = catalog_schema(CONTROL_SCHEMA)
    return [] if schema == CONTROL_SCHEMA else [{"name": DATA_SCHEMA_ENV, "value": schema}]


def manifest(spec, request, image):
    """Build one isolated Job with immutable code and only dedicated publisher Secret references."""
    from api import control_workers as workers

    if not _IMAGE.fullmatch(image):
        raise ValueError("catalog publisher requires an immutable configured image")
    arguments = []
    for key in SELECTORS:
        arguments.extend(("--" + key.replace("_", "-"), str(request[key])))
    keys = ("DSN", "SCHEMA", "OWNER_ROLE", "READER_ROLES", "COPY_MAX_BYTES", "TIMEOUT_SECONDS")
    environment_list = [
        {
            "name": PREFIX + key,
            "valueFrom": {
                "secretKeyRef": {
                    "name": SECRET_NAME,
                    "key": key.lower().replace("_", "-"),
                    "optional": False,
                }
            },
        }
        for key in keys
    ]
    container_by_field = {
        "name": "worker",
        "image": image,
        "imagePullPolicy": "IfNotPresent",
        "workingDir": "/opt",
        "command": [workers._worker_python(), "-m", "process.scoped_catalog_publisher"],
        "args": arguments,
        "env": environment_list,
        "securityContext": workers._worker_job_container_security_context(),
    }
    if resource_profile_map := workers._worker_job_resources(spec, request):
        container_by_field["resources"] = resource_profile_map
    labels, body = workers._worker_job_spec(spec, request, container_by_field, [], request["run_id"])
    body.update(backoffLimit=0, activeDeadlineSeconds=660, parallelism=1, completions=1)
    body.pop("ttlSecondsAfterFinished", None)
    body["template"]["spec"].update(serviceAccountName=SERVICE_ACCOUNT, automountServiceAccountToken=False)
    suffix = hashlib.sha256(
        canonical_metadata({key: request[key] for key in (*SELECTORS, "status")}).encode()
    ).hexdigest()[:20]
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": "catalog-publisher-" + suffix,
            "labels": labels,
            "annotations": {
                "healthporta.com/worker-class": spec.worker_class,
                "healthporta.com/queue": spec.queue,
                "healthporta.com/importers": ",".join(spec.importers),
                "healthporta.com/run-id": request["run_id"],
            },
        },
        "spec": body,
    }


def _resource_quantity(value):
    """Compare exact quantities despite Kubernetes canonical suffix changes."""
    if type(value) not in {str, int}:
        raise ValueError("invalid resource quantity")
    value = str(value)
    match = re.fullmatch(r"([+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+))([eE][+-]?[0-9]+|[numkMGTPE]|[KMGTPE]i)?", value)
    if match is None:
        raise ValueError("invalid resource quantity")
    number, suffix = match.groups()
    if suffix and len(suffix) > 1 and suffix[0] in "eE" and suffix[-1] != "i":
        return Decimal(number + suffix)
    with localcontext() as context:
        context.prec = len(value) + 20
        if suffix and suffix.endswith("i"):
            return Decimal(number) * (1024 ** ("KMGTPE".index(suffix[0]) + 1))
        exponent_by_suffix = {"n": -9, "u": -6, "m": -3, "": 0, "k": 3, "M": 6, "G": 9, "T": 12, "P": 15, "E": 18}
        return Decimal(number).scaleb(exponent_by_suffix[suffix or ""])


def _normalized_resources(resources):
    """Keep every resource key while normalizing only its numeric quantity."""
    if not isinstance(resources, dict) or set(resources) - {"requests", "limits"}:
        raise ValueError("invalid resource map")
    if any(not isinstance(values, dict) for values in resources.values()):
        raise ValueError("invalid resource section")
    normalized_by_section = {
        section: {name: _resource_quantity(value) for name, value in values.items()}
        for section, values in resources.items()
        if values
    }
    if normalized_by_section.get("limits"):
        normalized_by_section["requests"] = {
            **normalized_by_section["limits"],
            **normalized_by_section.get("requests", {}),
        }
    return normalized_by_section


def _matches(actual, expected):
    try:
        pod = actual["spec"]["template"]["spec"]
        wanted = expected["spec"]["template"]["spec"]
        if (
            actual["metadata"]["name"] != expected["metadata"]["name"]
            or actual["metadata"].get("annotations") != expected["metadata"]["annotations"]
            or pod.get("serviceAccountName") != SERVICE_ACCOUNT
            or pod.get("automountServiceAccountToken") is not False
            or any(
                actual["spec"].get(key) != expected["spec"][key]
                for key in ("backoffLimit", "activeDeadlineSeconds", "parallelism", "completions")
            )
            or actual["spec"].get("ttlSecondsAfterFinished") is not None
            or pod.get("restartPolicy") != "Never"
            or pod.get("securityContext") != wanted["securityContext"]
        ):
            return False
        if (
            any(
                pod.get(key)
                for key in ("initContainers", "ephemeralContainers", "volumes", "hostNetwork", "hostPID", "hostIPC")
            )
            or len(pod["containers"]) != 1
        ):
            return False
        container = pod["containers"][0]
        return (
            not container.get("envFrom")
            and _normalized_resources(container.get("resources", {}))
            == _normalized_resources(wanted["containers"][0].get("resources", {}))
            and all(
                container.get(key) == wanted["containers"][0][key]
                for key in ("image", "command", "args", "env", "workingDir", "securityContext")
            )
        )
    except KeyError, TypeError, ValueError, DecimalException:
        return False


def ensure(spec, request):
    """Create or observe the exact admitted Job without retrying any terminal execution."""
    from api import control_workers as workers

    if type(request) is not workers._AdmittedWorkerRequest or request.scoped_catalog_admission is not True:
        return {"status": "failed", "role": ROLE, "message": "catalog publisher requires native admission"}
    if workers._launcher_mode() != "kubernetes":
        return {"status": "blocked", "role": ROLE, "message": "catalog publisher requires the isolated launcher"}
    try:
        expected = manifest(spec, request, workers._configured_worker_job_image(spec, request))
        path = f"/apis/batch/v1/namespaces/{workers._kubernetes_namespace()}/jobs"
        name = expected["metadata"]["name"]
        try:
            actual = workers._kubernetes_request("GET", path + "/" + name)
        except workers._KubernetesApiError as error:
            if error.status != 404:
                raise
            try:
                workers._kubernetes_request("POST", path, expected)
                return {"status": "started", "role": ROLE, "job_name": name, "job_status": "pending"}
            except workers._KubernetesApiError as collision:
                if collision.status != 409:
                    raise
                actual = workers._kubernetes_request("GET", path + "/" + name)
        if not _matches(actual, expected):
            raise ValueError("catalog publisher Job differs")
        is_terminal = workers._is_terminal_worker_job(actual)
        return {
            "status": "failed" if is_terminal else "already_running",
            "role": ROLE,
            "job_name": name,
            "job_status": "failed" if is_terminal else "active",
            "message": "publication has not committed" if is_terminal else "publisher exists",
        }
    except ValueError, workers._KubernetesApiError:
        return {"status": "failed", "role": ROLE, "message": "catalog publisher launch or configuration failed"}


def state(request):
    """Report the exact one-shot Job; the native run remains the terminal outcome source."""
    from api import control_workers as workers

    try:
        spec = selected_spec(workers._BY_IMPORTER_ROLE[(request.get("importer"), "start")], request)
        expected = manifest(spec, request, workers._configured_worker_job_image(spec, request))
        path = f"/apis/batch/v1/namespaces/{workers._kubernetes_namespace()}/jobs/" + expected["metadata"]["name"]
        actual = workers._kubernetes_request("GET", path)
        if not _matches(actual, expected):
            raise ValueError("catalog publisher Job differs")
        status = actual.get("status") or {}
        is_terminal = workers._is_terminal_worker_job(actual)
        phase = "active"
        if is_terminal:
            is_failed = any(
                condition.get("type") == "Failed" and condition.get("status") == "True"
                for condition in status["conditions"]
            )
            phase = "failed" if is_failed else "succeeded"
        item_by_field = {
            "role": ROLE,
            "job_name": expected["metadata"]["name"],
            "job_status": phase,
            "running": not is_terminal,
        }
        return workers._worker_ensure_response(request, status="ok", items=[item_by_field])
    except ValueError, KeyError, TypeError, workers._KubernetesApiError:
        return workers._worker_ensure_response(
            request, status="failed", items=[], message="catalog publisher status unavailable"
        )


def stop_publisher(run):
    """Stop only the exact pending publication Job before a cancellation row-lock wait."""
    from api import control_workers as workers

    if run.get("status") != "finalizing" or workers._launcher_mode() != "kubernetes":
        return {"deleted": 0}
    try:
        handoff = publication.validate_handoff((run["metrics"] or {}).get(publication.METRIC))
        generation = generations.logical_generation(handoff["expected_generations"][run["importer"]])
        request_by_field = {
            "role": ROLE,
            "status": "finalizing",
            "importer": run["importer"],
            **{key: handoff[key] for key in SELECTORS[:4]},
            "expected_lineage_id": generation["local_lineage_id"],
            "expected_generation": generation["local_generation"],
        }
        if (
            run["run_id"] != request_by_field["run_id"]
            or run["node_id"] != request_by_field["node_id"]
            or any(
                (run["progress"] or {}).get(key) != request_by_field[key]
                for key in ("attempt_id", "attempt_started_at")
            )
        ):
            raise ValueError("catalog publisher stop attempt differs")
        spec = selected_spec(workers._BY_IMPORTER_ROLE[(run["importer"], "start")], request_by_field)
        expected = manifest(spec, request_by_field, workers._configured_worker_job_image(spec, request_by_field))
        namespace = workers._kubernetes_namespace()
        actual = workers._kubernetes_request(
            "GET", f"/apis/batch/v1/namespaces/{namespace}/jobs/" + expected["metadata"]["name"]
        )
        if not _matches(actual, expected):
            raise ValueError("catalog publisher stop Job differs")
        if workers._is_terminal_worker_job(actual):
            return {"deleted": 0}
        uid = actual["metadata"].get("uid")
        if type(uid) is not str or not uid:
            raise ValueError("catalog publisher stop Job identifier is unavailable")
        workers._delete_kubernetes_job(namespace, expected["metadata"]["name"], uid=uid)
        return {"deleted": 1}
    except workers._KubernetesApiError as error:
        return {"deleted": 0} if error.status == 404 else {"deleted": 0, "error": "publisher stop failed"}
    except ValueError, KeyError, TypeError:
        return {"deleted": 0, "error": "publisher stop admission failed"}


async def guarded_ensure(request):
    """Hold the native run action lock through admission and Job creation."""
    from api import control_workers as workers
    from db.models import ImportRun

    is_repair = request.get("role") != ROLE
    try:
        if not is_repair:
            selected_spec(workers._BY_IMPORTER_ROLE[(request.get("importer"), "start")], request)
        async with workers.db.acquire() as connection:
            await acquire_control_run_worker_action_lock(connection, request["run_id"])
            terminal = await _admit(connection, request, str(ImportRun.__table__.schema), is_repair=is_repair)
            spec = selected_spec(workers._BY_IMPORTER_ROLE[(request.get("importer"), "start")], request)
            if terminal is not None:
                item_by_field = {
                    "status": "blocked",
                    "role": ROLE,
                    "job_status": terminal,
                    "message": "managed publication is already terminal",
                }
            else:
                request.scoped_catalog_admission = True
                item_by_field = await workers._await_worker_launch(
                    asyncio.create_task(asyncio.to_thread(ensure, spec, request))
                )
        return workers._worker_ensure_response(request, status=item_by_field["status"], items=[item_by_field])
    except ValueError, KeyError, TypeError, RuntimeError:
        return workers._worker_ensure_response(
            request, status="failed", items=[], message="catalog publisher admission failed"
        )


async def request_cancel(run_id, observed):
    """Stop the exact publisher before waiting; native committed success takes precedence."""
    from api import control_imports as imports
    from process.scoped_catalog_handoff import request_catalog_cancel

    db = imports.db

    await asyncio.to_thread(stop_publisher, observed)
    async with asyncio.timeout(8), db.transaction() as session:
        row = await db.first(
            imports.select(imports.ImportRun.__table__).where(imports.ImportRun.run_id == run_id).with_for_update()
        )
        if row is None:
            return None
        current_by_field = dict(row._mapping)
        if current_by_field["status"] in imports.TERMINAL_STATUSES:
            return imports.normalize_run(current_by_field)
        if (current_by_field["metrics"] or {}).get("scoped_catalog_handoff") != (observed["metrics"] or {}).get(
            "scoped_catalog_handoff"
        ):
            raise RuntimeError("catalog cancellation attempt changed")
        await request_catalog_cancel(session, current_by_field)
    updated = await imports.get_import_run(run_id)
    if updated is not None:
        imports._write_run_live_progress(updated, publish_event=False)
        imports.enqueue_status_event(updated)
    return updated
