# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import ssl
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from api import (
    npi_source_worker,
    provider_directory_cms_registry_stop,
    provider_directory_source_worker,
    reference_source_worker,
    registry_aca_source_worker,
)
from db.models import db
from process.control_lifecycle import _SOURCE_PROFILE_IMPORTERS_BY_MODULE, acquire_control_run_worker_action_lock
from process.ptg_parts.ptg_source_attempt_actions import (
    PTGSourceAttemptIdentityError,
    PTGWorkerActionSelection,
    admit_existing_outer_run_action,
)
from process.ptg_parts.ptg_source_attempt_guard import (
    PTGSourceAttemptTerminalError,
    source_file_import_id_from_payload,
)
from process.ptg_parts.ptg_wave_admission_fence import (
    PTG_WAVE_FENCED_IMPORTERS,
    PTGWaveCapacityConflict,
    PTGWaveOwnershipConflict,
    acquire_ptg_admission_lock,
    require_no_capacity_owning_wave,
    require_not_wave_owned_run,
)


@dataclass(frozen=True)
class WorkerSpec:
    queue: str
    worker_class: str
    importers: tuple[str, ...]
    role: str = "start"


class _AdmittedWorkerRequest(dict[str, Any]):
    """Carry an observed queue identity outside the serialized launch request."""

    admitted_job: tuple[str, str, str, str, str] | None = None
    cms_registry_launch: object | None = None
    reference_source_launch: object | None = None
    aca_source_launch: object | None = None
    fhir_source_launch: object | None = None
    ordinary_fhir_overlay_job: tuple[str, str, str, str, str] | None = None
    npi_source_launch: object | None = None
    pricing_control_admission: object | None = None


_PROVIDER_DIRECTORY_WORKER_CLASS = "process.ProviderDirectoryFHIR"
_PROVIDER_DIRECTORY_MIN_ACTIVE_DEADLINE_SECONDS = 144 * 60 * 60


_START_WORKERS: tuple[WorkerSpec, ...] = (
    WorkerSpec("arq:RegistrySourceCheck", "process.RegistrySourceCheck", ("registry-source-check",)),
    WorkerSpec("arq:RegistryACASource", "process.RegistryACASource", ("registry-aca-source-worker",)),
    WorkerSpec("arq:RegistryPTGSource", "process.RegistryPTGSource", ("registry-ptg-source-worker",)),
    WorkerSpec("arq:RegistryCMSSource", "process.RegistryCMSSource", ("registry-cms-source-worker",)),
    WorkerSpec("arq:PTG", "process.PTG", ("ptg",)),
    WorkerSpec("arq:PTGSmall", "process.PTGSmall", ("ptg",)),
    WorkerSpec("arq:PTGNormal", "process.PTGNormal", ("ptg",)),
    WorkerSpec("arq:PTGLarge", "process.PTGLarge", ("ptg",)),
    WorkerSpec("arq:PTGHuge", "process.PTGHuge", ("ptg",)),
    WorkerSpec(
        "arq:PTGCandidateAudit",
        "process.PTGCandidateAudit",
        (
            "ptg-candidate-audit",
            "plan-pricing-projection",
            "plan-pricing-prewarm",
            "plan-pricing-em-distance",
            "ptg-provider-membership-projection",
            "plan-pricing-code-projection",
            "plan-pricing-em-attribution",
        ),
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
    WorkerSpec("arq:HospitalPrices", "process.HospitalPrices", ("hospital-prices",)),
    WorkerSpec("arq:ClaimsPricing", "process.ClaimsPricing", ("claims-pricing", "claims-procedures")),
    WorkerSpec("arq:DrugClaims", "process.DrugClaims", ("drug-claims",)),
    WorkerSpec("arq:ProviderQuality", "process.ProviderQuality", ("provider-quality",)),
    WorkerSpec("arq:ProviderEnrichment", "process.ProviderEnrichment", ("provider-enrichment",)),
    WorkerSpec("arq:ProviderDirectoryFHIR", "process.ProviderDirectoryFHIR", ("provider-directory-fhir",)),
    WorkerSpec("arq:FloridaMQAProfile", "process.FloridaMQAProfile", ("florida-mqa-profile",)),
    WorkerSpec("arq:MassachusettsBORIMProfile", "process.MassachusettsBORIMProfile", ("massachusetts-borim-profile",)),
    WorkerSpec("arq:KentuckyKBMLProfile", "process.KentuckyKBMLProfile", ("kentucky-kbml-profile",)),
    WorkerSpec("arq:TennesseeTDHProfile", "process.TennesseeTDHProfile", ("tennessee-tdh-profile",)),
    WorkerSpec("arq:RhodeIslandDOHProfile", "process.RhodeIslandDOHProfile", ("rhode-island-doh-profile",)),
    WorkerSpec("arq:NewYorkNYPPProfile", "process.NewYorkNYPPProfile", ("new-york-nypp-profile",)),
    WorkerSpec("arq:PartDFormularyNetwork", "process.PartDFormularyNetwork", ("partd-formulary-network",)),
    WorkerSpec("arq:PharmacyLicense", "process.PharmacyLicense", ("pharmacy-license",)),
    WorkerSpec("arq:PlacesZcta", "process.PlacesZcta", ("places-zcta",)),
    WorkerSpec("arq:LODES", "process.LODES", ("lodes",)),
    WorkerSpec("arq:MedicareEnrollment", "process.MedicareEnrollment", ("medicare-enrollment",)),
    WorkerSpec("arq:CMSDoctors", "process.CMSDoctors", ("cms-doctors",)),
    WorkerSpec("arq:FacilityAnchors", "process.FacilityAnchors", ("facility-anchors",)),
    WorkerSpec("arq:PharmacyEconomics", "process.PharmacyEconomics", ("pharmacy-economics",)),
    WorkerSpec("arq:EntityAddressUnified", "process.EntityAddressUnified", ("entity-address-unified",)),
    WorkerSpec(
        "arq:AddressArchive",
        "process.AddressArchive",
        (
            "address-archive-v2-migrate",
            "address-formatted-address",
            "address-numeric-grid-alias",
            "address-numeric-grid-alias-revoke",
            "address-strict-source-backfill",
        ),
    ),
    WorkerSpec("arq:OpenAddresses", "process.OpenAddresses", ("openaddresses",)),
)

_FINISH_WORKERS: tuple[WorkerSpec, ...] = (
    WorkerSpec(
        "arq:SourceProfile_finish",
        "process.SourceProfile_finish",
        tuple(_SOURCE_PROFILE_IMPORTERS_BY_MODULE.values()),
        role="finish",
    ),
    WorkerSpec("arq:MRF_finish", "process.MRF_finish", ("mrf",), role="finish"),
    WorkerSpec(
        "arq:ClaimsPricing_finish",
        "process.ClaimsPricing_finish",
        ("claims-pricing", "claims-procedures"),
        role="finish",
    ),
    WorkerSpec("arq:DrugClaims_finish", "process.DrugClaims_finish", ("drug-claims",), role="finish"),
    WorkerSpec("arq:ProviderQuality_finish", "process.ProviderQuality_finish", ("provider-quality",), role="finish"),
    WorkerSpec(
        "arq:PartDFormularyNetwork_finish",
        "process.PartDFormularyNetwork_finish",
        ("partd-formulary-network",),
        role="finish",
    ),
    WorkerSpec("arq:PharmacyLicense_finish", "process.PharmacyLicense_finish", ("pharmacy-license",), role="finish"),
)

_WORKERS = (*_START_WORKERS, *_FINISH_WORKERS)
_BY_QUEUE = {spec.queue: spec for spec in _WORKERS}
_BY_WORKER_CLASS = {spec.worker_class: spec for spec in _WORKERS}
_BY_IMPORTER_ROLE: dict[tuple[str, str], WorkerSpec] = {}
for _spec in _WORKERS:
    for _importer in _spec.importers:
        _BY_IMPORTER_ROLE.setdefault((_importer, _spec.role), _spec)
_NUCC_ACTION_DEADLINE: ContextVar[float | None] = ContextVar("nucc_action_deadline", default=None)
_ENGINE_LABEL = "mrf"
_K8S_API_TOKEN = Path("/var/run/secrets/kubernetes.io/serviceaccount/token")
_K8S_API_CA = Path("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
_K8S_API_NAMESPACE = Path("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
_NON_LAUNCHABLE_CONTROL_RUN_STATUSES = frozenset({"canceling", "succeeded", "failed", "canceled", "dead_letter"})
WORKER_ENSURE_RUN_IDENTITY_CONTRACT = "healthporta.worker-ensure-run-identity.v1"


async def _await_worker_launch(task: asyncio.Task[dict[str, Any]]) -> dict[str, Any]:
    """Keep the transaction fence until the threaded launch has settled."""

    pending_cancellation: asyncio.CancelledError | None = None
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError as exc:
            pending_cancellation = exc
    result = task.result()
    if pending_cancellation is not None:
        raise pending_cancellation
    return result


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
            message=(f"no worker is registered for {queue or importer or 'request'}"),
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


def _worker_action_selection(
    importer: str,
    selected_specs: list[WorkerSpec],
) -> PTGWorkerActionSelection:
    return PTGWorkerActionSelection(
        request_importer=importer or None,
        allowed_importers=frozenset(
            selected_importer for worker_spec in selected_specs for selected_importer in worker_spec.importers
        ),
        allowed_roles=frozenset(worker_spec.role for worker_spec in selected_specs),
    )


def _failed_worker_admission(
    worker_payload: dict[str, Any],
    message: str,
) -> dict[str, Any]:
    return _worker_ensure_response(
        worker_payload,
        status="failed",
        items=[],
        message=message,
    )


async def _admit_worker_ensure(
    worker_payload: dict[str, Any],
    *,
    run_id: str,
    importer: str,
    selected_specs: list[WorkerSpec],
) -> dict[str, Any] | None:
    try:
        requested_source_id = source_file_import_id_from_payload(
            worker_payload,
            required=False,
        )
        admitted_run = await admit_existing_outer_run_action(
            run_id=run_id,
            event_kind="ensure_admitted",
            expected_source_file_import_id=requested_source_id,
            worker_selection=_worker_action_selection(
                importer,
                selected_specs,
            ),
        )
    except PTGSourceAttemptIdentityError, ValueError:
        return _failed_worker_admission(
            worker_payload,
            "PTG source-attempt identity is invalid or changed",
        )
    except PTGSourceAttemptTerminalError as error:
        return _failed_worker_admission(worker_payload, str(error))
    if admitted_run is None:
        return _failed_worker_admission(
            worker_payload,
            "control run was not found",
        )
    admitted_status = str(admitted_run.get("status") or "").strip().lower()
    if admitted_status in _NON_LAUNCHABLE_CONTROL_RUN_STATUSES:
        return _failed_worker_admission(
            worker_payload,
            f"control run is not launchable: {admitted_status}",
        )
    if str(admitted_run.get("importer") or "") != "ptg" and requested_source_id is not None:
        return _failed_worker_admission(
            worker_payload,
            "source-attempt identity requires a PTG import",
        )
    _bind_admitted_job(worker_payload, admitted_run)
    return await _admit_source_worker_launches(worker_payload, admitted_run)


async def _admit_source_worker_launches(worker_payload, admitted_run):
    """Capture exact source-image capabilities only after native job admission."""
    if isinstance(worker_payload, _AdmittedWorkerRequest):
        from api.provider_directory_cms_registry_worker import admit_cms_registry_worker_launch

        try:
            worker_payload.cms_registry_launch = await admit_cms_registry_worker_launch(admitted_run)
        except ValueError, RuntimeError:
            return _failed_worker_admission(worker_payload, "CMS registry worker admission is unavailable")
        if worker_payload.cms_registry_launch is None:
            try:
                worker_payload.fhir_source_launch = provider_directory_source_worker.admit_fhir_source_worker_launch(
                    admitted_run
                )
                worker_payload.ordinary_fhir_overlay_job = (
                    worker_payload.admitted_job
                    if admitted_run.get("importer") == "provider-directory-fhir"
                    and worker_payload.fhir_source_launch is None
                    else None
                )
            except ValueError, RuntimeError:
                return _failed_worker_admission(worker_payload, "FHIR source worker admission is unavailable")
        try:
            worker_payload.npi_source_launch = npi_source_worker.admit_npi_source_worker_launch(admitted_run)
        except ValueError, RuntimeError:
            return _failed_worker_admission(worker_payload, "NPI source worker admission is unavailable")
        try:
            worker_payload.reference_source_launch = reference_source_worker.admit_reference_source_worker_launch(
                admitted_run
            )
        except ValueError, RuntimeError:
            return _failed_worker_admission(worker_payload, "Reference source worker admission is unavailable")
        try:
            worker_payload.aca_source_launch = registry_aca_source_worker.admit_aca_source_worker_launch(admitted_run)
        except ValueError, RuntimeError:
            return _failed_worker_admission(worker_payload, "ACA source worker admission is unavailable")
    return None


def _bind_admitted_job(payload: dict[str, Any], admitted_run: dict[str, Any]) -> None:
    """Retain only real admission coordinates for scoped secret selection."""

    if not isinstance(payload, _AdmittedWorkerRequest):
        return
    metrics = admitted_run.get("metrics")
    if not isinstance(metrics, dict):
        return
    identity = tuple(admitted_run.get(name) for name in ("run_id", "importer")) + tuple(
        metrics.get(name) for name in ("queue", "function", "job_id")
    )
    if all(isinstance(value, str) and value and value == value.strip() for value in identity):
        payload.admitted_job = identity
        params = admitted_run.get("params")
        if (
            identity[1] == "plan-pricing-code-projection"
            and isinstance(params, dict)
            and params.get("operation") == "calibrate"
        ):
            from api.plan_pricing_code_contract import validate_params

            validate_params(params)
            payload.code_calibration_job = identity


async def guarded_ensure_worker(
    worker_payload: dict[str, Any],
) -> dict[str, Any]:
    """Persist run/selector admission before any worker launch."""

    worker_payload = _AdmittedWorkerRequest(worker_payload)
    importer = str(worker_payload.get("importer") or "").strip()
    run_id = str(worker_payload.get("run_id") or "").strip()
    selected_specs = _resolve_specs(worker_payload)
    selects_ptg = any("ptg" in spec.importers for spec in selected_specs)
    if failure := _nucc_worker_selection_failure(worker_payload, selected_specs, importer, run_id):
        return failure
    selects_ptg_family = any(PTG_WAVE_FENCED_IMPORTERS.intersection(spec.importers) for spec in selected_specs)
    if (selects_ptg and importer and importer != "ptg") or (importer == "ptg" and selected_specs and not selects_ptg):
        return _failed_worker_admission(worker_payload, "PTG worker selector conflicts with importer")
    if selects_ptg_family and not run_id:
        return _failed_worker_admission(worker_payload, "PTG-family worker launch requires run_id")
    if importer == "nucc" or any("nucc" in spec.importers for spec in selected_specs):
        return await _guarded_nucc_ensure(
            worker_payload,
            run_id=run_id,
            importer=importer,
            selected_specs=selected_specs,
        )
    if run_id:
        if selects_ptg_family:
            return await _guarded_ptg_family_ensure(
                worker_payload,
                run_id=run_id,
                importer=importer,
                selected_specs=selected_specs,
            )
        if any("provider-directory-fhir" in spec.importers for spec in selected_specs):
            return await provider_directory_cms_registry_stop.guarded_ensure(
                worker_payload,
                run_id=run_id,
                importer=importer,
                selected_specs=selected_specs,
            )
        admission_failure = await _admit_importer_worker_ensure(
            worker_payload, run_id=run_id, importer=importer, selected_specs=selected_specs
        )
        if admission_failure is not None:
            return admission_failure
    else:
        try:
            requested_source_id = source_file_import_id_from_payload(
                worker_payload,
                required=False,
            )
        except ValueError:
            requested_source_id = "invalid"
        if requested_source_id is not None:
            return _failed_worker_admission(
                worker_payload,
                "source-attempt worker launch requires run_id",
            )
    return await asyncio.to_thread(ensure_worker, worker_payload)


async def _guarded_nucc_ensure(worker_payload, *, run_id, selected_specs, importer=""):
    """Hold the authenticated action fence until the existing launcher settles."""
    from api.nucc_source_control import require_unpublished_source

    if (
        importer not in {"", "nucc"}
        or selected_specs != [_BY_IMPORTER_ROLE[("nucc", "start")]]
        or str(worker_payload.get("importer") or "") not in {"", "nucc"}
        or source_file_import_id_from_payload(worker_payload, required=False) is not None
    ):
        return _failed_worker_admission(worker_payload, "NUCC worker selector conflicts with importer")
    async with _nucc_action_transaction(run_id) as (current, receipt, protected):
        if current is None:
            launch_result = _failed_worker_admission(worker_payload, "control run was not found")
        elif (
            receipt is not None
            or current.get("status") not in {"queued", "starting", "running"}
            or any(
                (current.get("metrics") or {}).get(key) is not None
                for key in ("nucc_handoff", "nucc_native_publication")
            )
        ):
            launch_result = _failed_worker_admission(worker_payload, "NUCC control run is not launchable")
        else:
            if protected:
                require_unpublished_source(current)
            _bind_admitted_job(worker_payload, current)
            launch_task = asyncio.create_task(asyncio.to_thread(ensure_worker, worker_payload))
            launch_result = await _await_worker_launch(launch_task)
    if receipt is not None:
        from api.control_imports import get_import_run

        _publish_nucc_run(await get_import_run(run_id))
    return launch_result


async def _guarded_ptg_family_ensure(
    worker_payload: dict[str, Any],
    *,
    run_id: str,
    importer: str,
    selected_specs: list[WorkerSpec],
) -> dict[str, Any]:
    """Hold the shared capacity lock through PTG admission and worker launch."""

    worker_payload = _AdmittedWorkerRequest(worker_payload)
    async with db.acquire() as connection:
        await acquire_ptg_admission_lock(connection)
        await acquire_control_run_worker_action_lock(connection, run_id)
        try:
            await require_not_wave_owned_run(connection, run_id)
            await require_no_capacity_owning_wave(connection)
        except (PTGWaveCapacityConflict, PTGWaveOwnershipConflict) as exc:
            return _failed_worker_admission(worker_payload, str(exc))
        admission_failure = await _admit_worker_ensure(
            worker_payload,
            run_id=run_id,
            importer=importer,
            selected_specs=selected_specs,
        )
        if admission_failure is not None:
            return admission_failure
        if any("plan-pricing-projection" in spec.importers for spec in selected_specs):
            from api.plan_pricing_worker_policy import admit_pricing_worker

            worker_payload.pricing_control_admission = await admit_pricing_worker(
                connection.raw_connection.driver_connection, run_id, worker_payload
            )
        launch_task = asyncio.create_task(asyncio.to_thread(ensure_worker, worker_payload))
        return await _await_worker_launch(launch_task)


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


def exact_worker_presence(payload: dict[str, Any]) -> dict[str, Any]:
    """Return exact-run Job and Pod counts and terminal-stop proof without mutation."""

    spec = _exact_worker_spec(payload)
    if _launcher_mode() != "kubernetes" or not _is_kubernetes_configured():
        raise RuntimeError("Kubernetes worker lookup is unavailable")
    namespace = _kubernetes_namespace()
    selector = _kubernetes_label_selector(spec, payload)
    query = urllib.parse.urlencode({"labelSelector": selector})
    jobs = _exact_worker_records(f"/apis/batch/v1/namespaces/{namespace}/jobs?{query}")
    pods = _exact_worker_records(f"/api/v1/namespaces/{namespace}/pods?{query}")
    return {
        "enabled": True,
        "job_count": len(jobs),
        "pod_count": len(pods),
        "stopped": all(_is_terminal_worker_job(job) for job in jobs)
        and all((pod.get("status") or {}).get("phase") in {"Succeeded", "Failed"} for pod in pods),
    }


def _exact_worker_records(path: str) -> list[dict[str, Any]]:
    """Keep malformed API responses from being mistaken for worker absence."""
    response = _kubernetes_request("GET", path)
    records = response.get("items") if isinstance(response, dict) else None
    if not isinstance(records, list) or any(not isinstance(record, dict) for record in records):
        raise RuntimeError("exact worker inventory is unavailable")
    return records


def _is_terminal_worker_job(job: dict[str, Any]) -> bool:
    """Require a terminal Job condition, including after failed retries."""
    status = job.get("status") or {}
    active = status.get("active", 0)
    conditions = status.get("conditions")
    if type(active) is not int or active != 0 or not isinstance(conditions, list):
        return False
    return all(isinstance(condition, dict) for condition in conditions) and any(
        condition.get("type") in {"Complete", "Failed"} and condition.get("status") == "True"
        for condition in conditions
    )


def _exact_worker_spec(payload: dict[str, Any]) -> WorkerSpec:
    run_id = str(payload.get("run_id") or "").strip()
    importer = str(payload.get("importer") or "").strip()
    queue = str(payload.get("queue") or "").strip()
    spec = _BY_QUEUE.get(queue) if importer == "ptg" and queue else _BY_IMPORTER_ROLE.get((importer, "start"))
    if not run_id or spec is None or importer not in spec.importers or spec.role != "start":
        raise RuntimeError("exact worker identity is unavailable")
    worker_class = str(payload.get("worker_class") or "").strip()
    if (queue and queue != spec.queue) or (worker_class and worker_class != spec.worker_class):
        raise ValueError("exact worker selector conflicts with importer")
    return spec


def _resolve_specs(launch_request: dict[str, Any]) -> list[WorkerSpec]:
    worker_class = str(launch_request.get("worker_class") or "").strip()
    queue = str(launch_request.get("queue") or "").strip()
    importer = str(launch_request.get("importer") or "").strip()
    if importer in _SOURCE_PROFILE_IMPORTERS_BY_MODULE.values() and (
        launch_request.get("role") == "finish" or launch_request.get("status") == "finalizing"
    ):
        selected = _BY_WORKER_CLASS.get(worker_class) if worker_class else None
        if worker_class and (selected is None or importer not in selected.importers):
            return []
        return [_BY_IMPORTER_ROLE[(importer, "finish")]]
    if worker_class:
        spec = _BY_WORKER_CLASS.get(worker_class)
        if spec is None:
            return []
        if queue and spec.queue != queue:
            return []
        return [spec]

    role = str(launch_request.get("role") or "").strip().lower()
    explicit_role = bool(role)
    status = str(launch_request.get("status") or "").strip().lower()
    if not role:
        role = "finish" if status == "finalizing" else "start"
    role_overrides_queue = bool(importer and (explicit_role or status == "finalizing"))
    if queue and not role_overrides_queue:
        spec = _BY_QUEUE.get(queue)
        return [spec] if spec is not None else []
    if importer and role == "start" and not explicit_role:
        finished_start_spec = _finish_spec_after_completed_start(
            importer,
            launch_request,
        )
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


def _ensure_spec(spec: WorkerSpec, launch_request: dict[str, Any]) -> dict[str, Any]:
    from api.provider_directory_cms_registry_worker import authorized_cms_registry_worker_launch

    reference_launch = reference_source_worker.authorized_reference_source_worker_launch(spec, launch_request)
    aca_launch = registry_aca_source_worker.authorized_aca_source_worker_launch(spec, launch_request)
    cms_launch = authorized_cms_registry_worker_launch(spec, launch_request)
    fhir_launch = provider_directory_source_worker.authorized_fhir_source_worker_launch(spec, launch_request)
    overlay_job_id = provider_directory_source_worker.admitted_overlay_job(spec, launch_request)
    npi_launch = npi_source_worker.authorized_npi_source_worker_launch(spec, launch_request)
    if spec.worker_class == "process.NPI" and npi_launch is None:
        return {"status": "failed", "message": "NPI source worker requires exact admitted job"}
    if (
        spec.worker_class == "process.ProviderDirectoryFHIR"
        and cms_launch is None
        and fhir_launch is None
        and overlay_job_id is None
    ):
        return {"status": "failed", "message": "FHIR source worker requires exact admitted job"}
    if spec.worker_class == "process.RegistryACASource" and aca_launch is None:
        return {"status": "failed", "message": "ACA source worker requires exact admitted job"}
    if spec.worker_class == "process.Geo" and reference_launch is None:
        return {"status": "failed", "message": "Reference source worker requires exact admitted job"}
    if (
        cms_launch is not None
        or fhir_launch is not None
        or overlay_job_id is not None
        or npi_launch is not None
        or reference_launch is not None
        or aca_launch is not None
    ) and _launcher_mode() != "kubernetes":
        return {"status": "failed", "message": "Source worker requires the configured isolated launcher"}
    state = _worker_state(spec, launch_request)
    if state.get("job_status") == "purpose_mismatch":
        return {**state, "status": "failed", "message": "Existing worker purpose does not match admission"}
    if state["running"]:
        return {**state, "status": "already_running"}
    if _launcher_mode() == "kubernetes":
        return _ensure_kubernetes_job(spec, launch_request, state)

    if (
        spec.worker_class == "process.DrugClaims_finish"
        and _worker_state(_BY_QUEUE["arq:ClaimsPricing_finish"])["running"]
    ):
        return {**state, "status": "blocked", "message": "ClaimsPricing_finish is already running"}
    if (
        spec.worker_class == "process.ClaimsPricing_finish"
        and _worker_state(_BY_QUEUE["arq:DrugClaims_finish"])["running"]
    ):
        return {**state, "status": "blocked", "message": "DrugClaims_finish is already running"}

    try:
        pid = _start_process(spec, launch_request)
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
    _nucc_remaining_seconds()
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
    is_running = _is_pid_running(pid) and _is_pid_spec_match(pid, spec)
    if pid and not is_running:
        _remove_stale_pid(spec)
    if not is_running:
        pid = _find_running_pid(spec)
        is_running = _is_pid_running(pid)
        if is_running and pid:
            _write_pid_file(spec, pid)
    return {
        "queue": spec.queue,
        "worker_class": spec.worker_class,
        "importers": list(spec.importers),
        "role": spec.role,
        "running": is_running,
        "pid": pid if is_running else None,
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
    launch_request: dict[str, Any],
    state: dict[str, Any],
) -> dict[str, Any]:
    from api.provider_directory_cms_registry_worker import authorized_cms_registry_worker_launch

    launch = authorized_cms_registry_worker_launch(spec, launch_request)
    fhir_launch = provider_directory_source_worker.authorized_fhir_source_worker_launch(spec, launch_request)
    launch = fhir_launch if fhir_launch is not None else launch
    npi_launch = npi_source_worker.authorized_npi_source_worker_launch(spec, launch_request)
    launch = npi_launch if npi_launch is not None else launch
    reference_launch = reference_source_worker.authorized_reference_source_worker_launch(spec, launch_request)
    launch = reference_launch if reference_launch is not None else launch
    aca_launch = registry_aca_source_worker.authorized_aca_source_worker_launch(spec, launch_request)
    launch = aca_launch if aca_launch is not None else launch
    image = launch.image if launch is not None else os.getenv("HLTHPRT_WORKER_JOB_IMAGE", "").strip()
    if not image:
        return {**state, "status": "failed", "message": "HLTHPRT_WORKER_JOB_IMAGE is not configured"}

    job = _worker_job_manifest(spec, launch_request, image)
    namespace = _kubernetes_namespace()
    if state.get("job_status") in {"succeeded", "failed"}:
        try:
            _delete_terminal_kubernetes_worker_jobs(namespace, spec, launch_request)
        except _KubernetesApiError as exc:
            if exc.status != 404:
                return {**state, "status": "failed", "message": str(exc)}

    try:
        _kubernetes_request("POST", f"/apis/batch/v1/namespaces/{namespace}/jobs", job)
    except _KubernetesApiError as exc:
        if exc.status == 409:
            refreshed = _worker_state(spec, launch_request)
            if refreshed.get("job_status") == "purpose_mismatch":
                return {**refreshed, "status": "failed", "message": "Existing worker purpose does not match admission"}
            return {**refreshed, "status": "already_running" if refreshed.get("running") else "exists"}
        return {**state, "status": "failed", "message": str(exc)}
    return {**_worker_state(spec, launch_request), "status": "started"}


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
        return None, {
            "job_name": job_name,
            "worker_class": worker_spec.worker_class,
            "status": exc.status,
            "error": str(exc),
        }
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


def _kubernetes_worker_base(spec: WorkerSpec) -> dict[str, Any]:
    """Build the common state fields before querying Kubernetes jobs."""
    return {
        "queue": spec.queue,
        "worker_class": spec.worker_class,
        "importers": list(spec.importers),
        "role": spec.role,
        "running": False,
        "pid": None,
        "launcher": "kubernetes",
        "command": " ".join(_worker_command(_worker_python(), spec)),
    }


def _kubernetes_worker_state(spec: WorkerSpec, launch_request: dict[str, Any] | None = None) -> dict[str, Any]:
    """Summarize Kubernetes jobs matching a worker spec and optional run."""
    state_base_dict = _kubernetes_worker_base(spec)
    if not _is_kubernetes_configured():
        return {
            **state_base_dict,
            "job_name": _worker_job_name(spec, launch_request or {}),
            "job_status": "unconfigured",
        }

    selector = _kubernetes_label_selector(spec, launch_request or {})
    namespace = _kubernetes_namespace()
    path = f"/apis/batch/v1/namespaces/{namespace}/jobs?{urllib.parse.urlencode({'labelSelector': selector})}"
    try:
        body = _kubernetes_request("GET", path)
    except _KubernetesApiError as exc:
        return {
            **state_base_dict,
            "job_name": _worker_job_name(spec, launch_request or {}),
            "job_status": "error",
            "message": str(exc),
        }

    raw_job_records = body.get("items") if isinstance(body, dict) else []
    jobs = [job_record for job_record in raw_job_records if isinstance(job_record, dict)]
    from api.provider_directory_cms_registry_worker import (
        authorized_cms_registry_worker_launch,
        is_cms_registry_job_matching,
    )

    launch = authorized_cms_registry_worker_launch(spec, launch_request)
    if launch is not None and any(not is_cms_registry_job_matching(job, launch) for job in jobs):
        return {**state_base_dict, "job_status": "purpose_mismatch"}
    reference_launch = reference_source_worker.authorized_reference_source_worker_launch(spec, launch_request)
    if reference_launch is not None and any(
        not reference_source_worker.is_reference_source_job_matching(job, reference_launch) for job in jobs
    ):
        return {**state_base_dict, "job_status": "purpose_mismatch"}
    aca_launch = registry_aca_source_worker.authorized_aca_source_worker_launch(spec, launch_request)
    if aca_launch is not None and any(
        not registry_aca_source_worker.is_aca_source_job_matching(job, aca_launch) for job in jobs
    ):
        return {**state_base_dict, "job_status": "purpose_mismatch"}
    if not provider_directory_source_worker.is_fhir_job_set_matching(spec, launch_request, jobs):
        return {**state_base_dict, "job_status": "purpose_mismatch"}
    npi_launch = npi_source_worker.authorized_npi_source_worker_launch(spec, launch_request)
    if npi_launch is not None and any(
        not npi_source_worker.is_npi_source_job_matching(job, npi_launch) for job in jobs
    ):
        return {**state_base_dict, "job_status": "purpose_mismatch"}
    state_map = _kubernetes_job_state(state_base_dict, jobs, spec, launch_request or {})
    if state_map["job_status"] == "failed":
        failure = _kubernetes_worker_failure(namespace, selector)
        if failure:
            state_map["failure"] = failure
    return state_map


def _kubernetes_job_state(
    state_base_dict: dict[str, Any],
    jobs: list[dict[str, Any]],
    spec: WorkerSpec,
    launch_request: dict[str, Any],
) -> dict[str, Any]:
    """Summarize counts and the latest matching Kubernetes job."""
    active = sum(int((job.get("status") or {}).get("active") or 0) for job in jobs)
    succeeded = sum(int((job.get("status") or {}).get("succeeded") or 0) for job in jobs)
    failed = sum(int((job.get("status") or {}).get("failed") or 0) for job in jobs)
    latest = jobs[-1] if jobs else {}
    latest_name = (
        (latest.get("metadata") or {}).get("name") if isinstance(latest, dict) else None
    ) or _worker_job_name(spec, launch_request or {})
    if active:
        job_status = "active"
    elif failed:
        job_status = "failed"
    elif succeeded:
        job_status = "succeeded"
    else:
        job_status = "missing"
    return {
        **state_base_dict,
        "running": active > 0,
        "job_name": latest_name,
        "job_status": job_status,
        "active_jobs": active,
        "succeeded_jobs": succeeded,
        "failed_jobs": failed,
    }


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


def _is_code_calibration_job(spec, payload):
    """Only persisted exact admission can select the fixed diagnostic deadline."""
    identity = getattr(payload, "code_calibration_job", None)
    return (
        isinstance(payload, _AdmittedWorkerRequest)
        and identity is not None
        and identity == getattr(payload, "admitted_job", None)
        and identity
        == (
            payload.get("run_id"),
            "plan-pricing-code-projection",
            "arq:PTGCandidateAudit",
            "control_single_job_start",
            "plan_pricing_code_projection_" + payload.get("run_id", ""),
        )
        and spec.worker_class == "process.PTGCandidateAudit"
        and spec.queue == "arq:PTGCandidateAudit"
    )


def _worker_job_environment(
    spec: WorkerSpec,
    launch_request: dict[str, Any],
) -> tuple[list[dict[str, Any]], str]:
    """Build the explicit environment for one worker Job."""

    env_list = [
        {"name": "HLTHPRT_WORKER_LAUNCHER", "value": "process"},
        {"name": "HLTHPRT_IMPORT_NODE_ID", "value": os.getenv("HLTHPRT_IMPORT_NODE_ID", "")},
        {"name": "HLTHPRT_ACTIVE_WORKER_CLASS", "value": spec.worker_class},
        {"name": "HLTHPRT_ACTIVE_WORKER_QUEUE", "value": spec.queue},
    ]
    import_id = str(launch_request.get("import_id") or "").strip()
    if import_id:
        env_list.append({"name": "HLTHPRT_IMPORT_ID_OVERRIDE", "value": import_id})
    run_id = str(launch_request.get("run_id") or "").strip()
    if run_id:
        env_list.append({"name": "HLTHPRT_CONTROL_RUN_ID", "value": run_id})
    if set(spec.importers).intersection(_SOURCE_PROFILE_IMPORTERS_BY_MODULE.values()) or (
        provider_directory_source_worker.authorized_fhir_source_worker_launch(spec, launch_request) is not None
        or npi_source_worker.authorized_npi_source_worker_launch(spec, launch_request) is not None
    ):
        policy_setting = "HLTHPRT_SOURCE_PROFILE_ROLE_POLICY_FILE"
        if policy_path := os.environ.get(policy_setting):
            env_list.append({"name": policy_setting, "value": policy_path})
    if _is_code_calibration_job(spec, launch_request):
        env_list.append({"name": "HLTHPRT_PTG_CANDIDATE_AUDIT_JOB_TIMEOUT", "value": "385"})
    target_job_id = _single_job_worker_target(spec, launch_request)
    if target_job_id:
        env_list.append(
            {
                "name": "HLTHPRT_WORKER_ONCE_TARGET_JOB_ID",
                "value": target_job_id,
            }
        )
    if spec.worker_class == "process.PTGHuge":
        env_list.append(
            {
                "name": "HLTHPRT_PTG2_V3_FINALIZER_IDENTITY_MAP_MAX_BYTES",
                "value": "68719476736",
            }
        )
    if spec.worker_class == "process.HospitalPrices":
        env_list.append(
            {
                "name": "HLTHPRT_HOSPITAL_PRICE_US_EGRESS_HOSTS",
                "value": (
                    "cdn.hs.uab.edu,d2cg6hcwj0g0z0.cloudfront.net,"
                    "shelteringarmsinstitute.com,bilh.org,ajh.org,bidmilton.org,"
                    "bidneedham.org,bidplymouth.org,bidmc.org,exeterhospital.org,"
                    "mountauburnhospital.org,nebh.org,winchesterhospital.org,"
                    "www.northwell.edu"
                ),
            }
        )
    env_list.extend(_worker_job_secret_env(spec.worker_class, launch_request=launch_request))
    from api.plan_pricing_worker_policy import pricing_worker_policy_environment

    env_list.extend(pricing_worker_policy_environment(launch_request))
    return env_list, run_id


def _worker_job_container(
    spec: WorkerSpec,
    launch_request: dict[str, Any],
    image: str,
) -> tuple[dict[str, Any], list[dict[str, Any]], str]:
    """Build the container and volume records for one worker Job."""

    env_list, run_id = _worker_job_environment(spec, launch_request)

    container_dict: dict[str, Any] = {
        "name": "worker",
        "image": image,
        "imagePullPolicy": os.getenv("HLTHPRT_WORKER_JOB_IMAGE_PULL_POLICY", "IfNotPresent"),
        "workingDir": str(_repo_root()),
        "command": _worker_command(_worker_python(), spec),
        "env": env_list,
        "securityContext": _worker_job_container_security_context(),
    }
    from api.provider_directory_cms_registry_worker import COMMAND, authorized_cms_registry_worker_launch

    launch = authorized_cms_registry_worker_launch(spec, launch_request)
    if launch is not None:
        container_dict.update(image=launch.image, command=list(COMMAND), workingDir="/app")
    fhir_launch = provider_directory_source_worker.authorized_fhir_source_worker_launch(spec, launch_request)
    if fhir_launch is not None:
        container_dict.update(
            image=fhir_launch.image, command=list(provider_directory_source_worker.COMMAND), workingDir="/app"
        )
    npi_launch = npi_source_worker.authorized_npi_source_worker_launch(spec, launch_request)
    if npi_launch is not None:
        container_dict.update(image=npi_launch.image, command=list(npi_source_worker.COMMAND), workingDir="/app")
    reference_launch = reference_source_worker.authorized_reference_source_worker_launch(spec, launch_request)
    if reference_launch is not None:
        container_dict.update(
            image=reference_launch.image, command=list(reference_source_worker.COMMAND), workingDir="/app"
        )
    aca_launch = registry_aca_source_worker.authorized_aca_source_worker_launch(spec, launch_request)
    if aca_launch is not None:
        container_dict.update(
            image=aca_launch.image, command=list(registry_aca_source_worker.COMMAND), workingDir="/app"
        )
    env_from_list = _worker_job_env_from()
    if env_from_list:
        container_dict["envFrom"] = env_from_list
    resource_dict = _worker_job_resources(spec, launch_request)
    if resource_dict:
        container_dict["resources"] = resource_dict
    pvc_volumes = _worker_job_pvc_volumes()
    from api.plan_pricing_worker_policy import pricing_worker_policy_volumes

    volumes = [
        *pvc_volumes,
        *_worker_job_secret_volumes(spec, fhir_launch),
        *pricing_worker_policy_volumes(spec, launch_request),
    ]
    if volumes:
        container_dict["volumeMounts"] = [volume_spec["volumeMount"] for volume_spec in volumes]
    return container_dict, volumes, run_id


def _worker_job_spec(
    spec: WorkerSpec,
    launch_request: dict[str, Any],
    container_dict: dict[str, Any],
    volumes: list[dict[str, Any]],
    run_id: str,
) -> tuple[dict[str, str], dict[str, Any]]:
    """Build labels and the Kubernetes Job spec around one container."""
    pod_spec_dict: dict[str, Any] = {
        "restartPolicy": "Never",
        "automountServiceAccountToken": False,
        "securityContext": _worker_job_pod_security_context(
            has_group_read_volume=bool(volumes),
        ),
        "containers": [container_dict],
    }
    if priority_class := _hospital_worker_priority_class(spec):
        pod_spec_dict["priorityClassName"] = priority_class
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
    if active_deadline_seconds > 0 and spec.worker_class == _PROVIDER_DIRECTORY_WORKER_CLASS:
        active_deadline_seconds = max(
            active_deadline_seconds,
            _PROVIDER_DIRECTORY_MIN_ACTIVE_DEADLINE_SECONDS,
        )
    if _is_code_calibration_job(spec, launch_request):
        active_deadline_seconds = min(active_deadline_seconds, 415) if active_deadline_seconds > 0 else 415
    if active_deadline_seconds > 0:
        job_spec_dict["activeDeadlineSeconds"] = active_deadline_seconds
    replicas = _worker_start_replicas(spec)
    if replicas > 1:
        job_spec_dict["parallelism"] = replicas
        job_spec_dict["completions"] = replicas
    return labels_by_key, job_spec_dict


def _hospital_worker_priority_class(spec: WorkerSpec) -> str:
    if spec.worker_class != "process.HospitalPrices":
        return ""
    return os.getenv("HLTHPRT_HOSPITAL_PRICE_WORKER_JOB_PRIORITY_CLASS", "").strip()


def _worker_job_manifest(
    spec: WorkerSpec,
    launch_request: dict[str, Any],
    image: str,
) -> dict[str, Any]:
    """Build the Kubernetes Job manifest for one import worker."""

    job_name = _worker_job_name(spec, launch_request)
    container_dict, volumes, run_id = _worker_job_container(
        spec,
        launch_request,
        image,
    )
    labels_by_key, job_spec_dict = _worker_job_spec(
        spec,
        launch_request,
        container_dict,
        volumes,
        run_id,
    )
    from api.provider_directory_cms_registry_worker import apply_cms_registry_worker_spec

    apply_cms_registry_worker_spec(spec, launch_request, labels_by_key, job_spec_dict["template"]["spec"])
    provider_directory_source_worker.apply_fhir_source_worker_spec(
        spec, launch_request, labels_by_key, job_spec_dict["template"]["spec"]
    )
    npi_source_worker.apply_npi_source_worker_spec(
        spec, launch_request, labels_by_key, job_spec_dict["template"]["spec"]
    )
    reference_source_worker.apply_reference_source_worker_spec(
        spec, launch_request, labels_by_key, job_spec_dict["template"]["spec"]
    )

    registry_aca_source_worker.apply_aca_source_worker_spec(
        spec, launch_request, labels_by_key, job_spec_dict["template"]["spec"]
    )

    fhir_launch = provider_directory_source_worker.authorized_fhir_source_worker_launch(spec, launch_request)
    annotations_by_key = {
        "healthporta.com/queue": spec.queue,
        "healthporta.com/worker-class": spec.worker_class,
        "healthporta.com/importers": ",".join(spec.importers),
        "healthporta.com/run-id": run_id,
    }
    if fhir_launch is not None and fhir_launch.root_run_id:
        annotations_by_key["healthporta.com/source-root-run-id"] = fhir_launch.root_run_id
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": job_name,
            "labels": labels_by_key,
            "annotations": annotations_by_key,
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


def _worker_job_secret_env(
    worker_class: str | None = None,
    *,
    launch_request: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
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
        if not _is_worker_class_selected(secret_env_spec, worker_class):
            continue
        if not _is_admitted_importer_selected(secret_env_spec, worker_class, launch_request):
            continue
        environment_name = str(secret_env_spec.get("name") or "").strip()
        secret_name = str(secret_env_spec.get("secretName") or secret_env_spec.get("secret_name") or "").strip()
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
            "valueFrom": {"secretKeyRef": secret_key_reference_by_field},
        }
    return list(environment_by_name.values())


def _is_admitted_importer_selected(selection_spec, worker_class, launch_request) -> bool:
    """Require exact single-job admission before selecting importer-scoped secrets."""

    from api.provider_directory_cms_registry_worker import is_cms_registry_secret_selected

    if selection_spec.get("purposes") == [provider_directory_source_worker.PURPOSE]:
        return provider_directory_source_worker.is_fhir_source_secret_selected(
            selection_spec, worker_class, launch_request
        )
    if selection_spec.get("purposes") == [npi_source_worker.PURPOSE]:
        return npi_source_worker.is_npi_source_secret_selected(selection_spec, worker_class, launch_request)
    if selection_spec.get("purposes") == [registry_aca_source_worker.PURPOSE]:
        return registry_aca_source_worker.is_aca_source_secret_selected(selection_spec, worker_class, launch_request)
    if selection_spec.get("purposes") == [reference_source_worker.PURPOSE]:
        return reference_source_worker.is_reference_source_secret_selected(selection_spec, worker_class, launch_request)
    if not is_cms_registry_secret_selected(selection_spec, worker_class, launch_request):
        return False

    if "importers" not in selection_spec:
        return True
    selected = selection_spec["importers"]
    if (
        not isinstance(selected, list)
        or not selected
        or any(not isinstance(name, str) or not name or name != name.strip() for name in selected)
        or not ("workerClasses" in selection_spec or "worker_classes" in selection_spec)
    ):
        raise ValueError("importer-scoped worker secret selector is invalid")
    if launch_request is None:
        return False
    admission = launch_request.admitted_job if isinstance(launch_request, _AdmittedWorkerRequest) else None
    importer = admission[1] if admission else launch_request.get("importer")
    if importer not in selected:
        return False
    from api.control_imports import _SINGLE_JOB_ADAPTERS, _enqueue_job_options

    spec = _BY_WORKER_CLASS.get(worker_class)
    adapter = _SINGLE_JOB_ADAPTERS.get(importer, {})
    if (
        admission is None
        or spec is None
        or spec.role != "start"
        or not _uses_single_job_worker(spec)
        or importer not in spec.importers
        or launch_request.get("importer", importer) != importer
        or admission[:4] != (launch_request.get("run_id"), importer, spec.queue, "control_single_job_start")
        or admission[4] != _single_job_worker_target(spec, launch_request)
        or adapter.get("queue") != spec.queue
        or not adapter.get("job_prefix")
        or admission[4] != _enqueue_job_options(adapter, {"run_id": admission[0]})["_job_id"]
    ):
        raise ValueError("importer-scoped worker secret requires exact admitted job")
    return True


def _worker_job_container_security_context() -> dict[str, Any]:
    return {
        "allowPrivilegeEscalation": False,
        "capabilities": {"drop": ["ALL"]},
    }


def _worker_job_pod_security_context(
    *,
    has_group_read_volume: bool,
) -> dict[str, Any]:
    security_context_dict: dict[str, Any] = {
        "runAsNonRoot": True,
        "runAsUser": 65534,
        "runAsGroup": 65534,
        "seccompProfile": {"type": "RuntimeDefault"},
    }
    if has_group_read_volume:
        security_context_dict["fsGroup"] = 65534
        security_context_dict["fsGroupChangePolicy"] = "OnRootMismatch"
    return security_context_dict


def _worker_job_resources(spec: WorkerSpec, payload_by_field: dict[str, Any] | None = None) -> dict[str, Any]:
    profile = _worker_job_resource_profile(spec, payload_by_field or {})
    if profile:
        return profile
    if spec.worker_class == "process.TennesseeTDHProfile":
        return {"requests": {"cpu": "1", "memory": "4Gi"}, "limits": {"cpu": "4", "memory": "8Gi"}}
    if spec.worker_class in {
        "process.MassachusettsBORIMProfile",
        "process.KentuckyKBMLProfile",
        "process.RhodeIslandDOHProfile",
    }:
        return {"requests": {"cpu": "500m", "memory": "512Mi"}, "limits": {"cpu": "4", "memory": "4Gi"}}
    requests_dict = {
        key: resource_value
        for key, resource_value in {
            "cpu": os.getenv("HLTHPRT_WORKER_JOB_CPU_REQUEST", "").strip(),
            "memory": os.getenv("HLTHPRT_WORKER_JOB_MEMORY_REQUEST", "").strip(),
        }.items()
        if resource_value
    }
    limits_dict = {
        key: resource_value
        for key, resource_value in {
            "cpu": os.getenv("HLTHPRT_WORKER_JOB_CPU_LIMIT", "").strip(),
            "memory": os.getenv("HLTHPRT_WORKER_JOB_MEMORY_LIMIT", "").strip(),
        }.items()
        if resource_value
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
            key: str(value).strip() for key, value in values.items() if key in {"cpu", "memory"} and str(value).strip()
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


def _is_worker_class_selected(
    selection_spec: dict[str, Any],
    worker_class: str | None,
) -> bool:
    has_worker_classes = "workerClasses" in selection_spec
    has_worker_classes_alias = "worker_classes" in selection_spec
    if has_worker_classes and has_worker_classes_alias:
        return False
    if not has_worker_classes and not has_worker_classes_alias:
        return True
    selected_worker_classes = selection_spec["workerClasses" if has_worker_classes else "worker_classes"]
    if (
        not isinstance(selected_worker_classes, list)
        or not selected_worker_classes
        or any(
            not isinstance(selected_class, str) or not selected_class.strip()
            for selected_class in selected_worker_classes
        )
    ):
        return False
    return bool(worker_class) and worker_class in {selected_class.strip() for selected_class in selected_worker_classes}


def _worker_job_secret_source(
    mount_spec: dict[str, Any],
    secret_name: str,
) -> dict[str, Any] | None:
    secret_source_by_field: dict[str, Any] = {"secretName": secret_name}
    if bool(mount_spec.get("optional", False)):
        secret_source_by_field["optional"] = True
    has_default_mode = "defaultMode" in mount_spec
    has_default_mode_alias = "default_mode" in mount_spec
    if has_default_mode and has_default_mode_alias:
        return None
    if not has_default_mode and not has_default_mode_alias:
        return secret_source_by_field
    default_mode = mount_spec["defaultMode" if has_default_mode else "default_mode"]
    if isinstance(default_mode, bool) or not isinstance(default_mode, int) or not 0 <= default_mode <= 0o777:
        return None
    secret_source_by_field["defaultMode"] = default_mode
    return secret_source_by_field


def _worker_job_secret_volumes(spec: WorkerSpec, fhir_source_launch=None) -> list[dict[str, Any]]:
    """Build validated secret mounts selected for one worker class."""

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
        if not _is_worker_class_selected(mount_spec, spec.worker_class):
            continue
        secret_name = str(mount_spec.get("secretName") or mount_spec.get("secret_name") or "").strip()
        if "purposes" in mount_spec and not provider_directory_source_worker.is_fhir_source_volume_selected(
            mount_spec, spec.worker_class, fhir_source_launch
        ):
            continue
        mount_path = str(mount_spec.get("mountPath") or mount_spec.get("mount_path") or "").strip()
        if not secret_name or not mount_path:
            continue
        secret_dict = _worker_job_secret_source(mount_spec, secret_name)
        if secret_dict is None:
            continue
        volume_name = _dns_safe(str(mount_spec.get("name") or secret_name))[:54].strip("-") or f"secret-{index}"
        if volume_name in used_names:
            volume_name = f"{volume_name[:50].rstrip('-')}-{index}"
        used_names.add(volume_name)
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
        in {
            "process.HospitalPrices",
            "process.MRFSourceDiscovery",
            "process.NPI",
            "process.RegistryACASource",
            "process.ProviderDirectoryFHIR",
            "process.Geo",
        }
    )


def _admitted_source_job_target(spec, payload):
    """Use only a native observed job for each fixed source-worker capability."""
    npi_launch = npi_source_worker.authorized_npi_source_worker_launch(spec, payload)
    if npi_launch is not None:
        return npi_launch.job_id
    reference_launch = reference_source_worker.authorized_reference_source_worker_launch(spec, payload)
    if reference_launch is not None:
        return reference_launch.job_id
    aca_launch = registry_aca_source_worker.authorized_aca_source_worker_launch(spec, payload)
    if aca_launch is not None:
        return aca_launch.job_id
    fhir_launch = provider_directory_source_worker.authorized_fhir_source_worker_launch(spec, payload)
    if fhir_launch is not None:
        return fhir_launch.job_id
    return None


def _single_job_worker_target(
    spec: WorkerSpec,
    launch_payload: dict[str, Any],
) -> str | None:
    if not _uses_single_job_worker(spec):
        return None
    source_job_id = _admitted_source_job_target(
        spec, launch_payload
    ) or provider_directory_source_worker.admitted_overlay_job(spec, launch_payload)
    if source_job_id is not None:
        return source_job_id
    queued_job_id = str(launch_payload.get("job_id") or "").strip()
    if queued_job_id:
        return queued_job_id
    run_id = str(launch_payload.get("run_id") or "").strip()
    projection_job_prefix_by_importer = {
        "ptg-provider-membership-projection": "ptg_provider_membership_projection",
        "plan-pricing-code-projection": "plan_pricing_code_projection",
        "plan-pricing-projection": "plan_pricing_projection",
        "plan-pricing-prewarm": "plan_pricing_prewarm",
        "plan-pricing-em-distance": "plan_pricing_em_distance",
        "plan-pricing-em-attribution": "plan_pricing_em_attribution",
    }
    importer = str(launch_payload.get("importer") or "").strip()
    if importer in projection_job_prefix_by_importer and run_id:
        return f"{projection_job_prefix_by_importer[importer]}_{run_id}"
    if spec.worker_class.startswith("process.PTG") and run_id:
        return f"ptg_start_{run_id}"
    if spec.worker_class == "process.NPI" and run_id:
        return f"npi_start_{run_id}"
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


def _kubernetes_request(
    method: str, path: str, body: dict[str, Any] | None = None, *, timeout_seconds: float = 10
) -> dict[str, Any]:
    remaining = _nucc_remaining_seconds()
    if remaining is not None:
        timeout_seconds = min(timeout_seconds, remaining)
    host = os.getenv("KUBERNETES_SERVICE_HOST", "").strip()
    port = os.getenv("KUBERNETES_SERVICE_PORT", "443").strip()
    if not host:
        raise _KubernetesApiError(0, "KUBERNETES_SERVICE_HOST is not configured")
    try:
        token = _K8S_API_TOKEN.read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise _KubernetesApiError(0, f"cannot read Kubernetes service account token: {exc}") from exc
    request_body_bytes = None if body is None else json.dumps(body).encode("utf-8")
    request = urllib.request.Request(
        f"https://{host}:{port}{path}",
        data=request_body_bytes,
        method=method,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        },
    )
    context = (
        ssl.create_default_context(cafile=str(_K8S_API_CA)) if _K8S_API_CA.exists() else ssl.create_default_context()
    )
    try:
        with urllib.request.urlopen(request, context=context, timeout=timeout_seconds) as response:  # nosec B310 - in-cluster API URL
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
        output = subprocess.check_output(
            ["ps", "eww", "-p", str(pid), "-o", "command="], text=True, **_nucc_subprocess_options()
        )
    except Exception:
        try:
            output = subprocess.check_output(
                ["ps", "-p", str(pid), "-o", "command="], text=True, **_nucc_subprocess_options()
            )
        except Exception:
            return True
    return _is_process_worker_spec_match(output, spec)


def _is_pid_on_current_node(pid: int | None) -> bool:
    if not pid:
        return False
    try:
        output = subprocess.check_output(
            ["ps", "eww", "-p", str(pid), "-o", "command="], text=True, **_nucc_subprocess_options()
        )
    except Exception:
        try:
            output = subprocess.check_output(
                ["ps", "-p", str(pid), "-o", "command="], text=True, **_nucc_subprocess_options()
            )
        except Exception:
            return True
    return _is_current_node_match(output)


def _find_running_pid(spec: WorkerSpec) -> int | None:
    try:
        output = subprocess.check_output(
            ["ps", "eww", "-axo", "pid=,command="], text=True, **_nucc_subprocess_options()
        )
    except Exception:
        try:
            output = subprocess.check_output(["ps", "-axo", "pid=,command="], text=True, **_nucc_subprocess_options())
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
        if (
            part.endswith("main.py")
            and parts[index + 1] in {"worker", "worker-once"}
            and parts[index + 2] == spec.worker_class
        ):
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


__all__ = ["ensure_worker", "exact_worker_presence", "worker_registry"]


@asynccontextmanager
async def _legacy_nucc_action(run_id):
    """Keep the existing plain-storage row fence outside the protected Source protocol."""
    from api import control_imports as imports

    async with imports.db.transaction():
        current_record = (
            await imports.db.execute(
                imports.select(imports.ImportRun.__table__).where(imports.ImportRun.run_id == run_id).with_for_update()
            )
        ).first()
        current = imports._raw_connection_run(current_record) if current_record is not None else None
        if current is not None and current.get("importer") != "nucc":
            raise RuntimeError("NUCC action run identity changed")
        yield current


@asynccontextmanager
async def _nucc_action_transaction(run_id: str):
    """Keep launch deadlines and all actions inside the same authenticated original Source transaction."""
    from api import control_imports as imports
    from api.nucc_source_control import source_action

    deadline_token = _NUCC_ACTION_DEADLINE.set(time.monotonic() + 45)
    try:
        async with asyncio.timeout(60), source_action(run_id) as protected:
            if protected is None:
                async with _legacy_nucc_action(run_id) as current:
                    yield current, None, False
            else:
                current, receipt = protected
                binding = imports.db._transaction_binding()
                if binding is None:
                    raise RuntimeError("NUCC original Source transaction is unavailable")
                budget = await _nucc_action_budget(binding.session)
                _NUCC_ACTION_DEADLINE.set(min(_NUCC_ACTION_DEADLINE.get(), time.monotonic() + budget))
                yield current, receipt, True
    finally:
        _NUCC_ACTION_DEADLINE.reset(deadline_token)


def _publish_nucc_run(current):
    """Project only the durable outcome after its transaction has committed."""
    from api import control_imports as imports

    public = imports.normalize_run(current) if current is not None else None
    if public is not None:
        imports.enqueue_status_event(public)
        imports._write_run_live_progress(public, publish_event=False)
    return public


async def _enqueue_nucc_run(run_values_by_field, enqueue_params):
    """Keep the action fence until Redis settles and acknowledge the exact queued current_record."""
    from api import control_imports as imports

    run_id = run_values_by_field["run_id"]
    async with _nucc_action_transaction(run_id) as (current, receipt, protected):
        if current is None:
            raise RuntimeError("NUCC run disappeared before enqueue")
        if receipt is None and _nucc_run_is_unclaimed(current):
            if current.get("params") != run_values_by_field.get("params"):
                raise RuntimeError("NUCC enqueue parameters changed")
            task = asyncio.create_task(
                asyncio.wait_for(
                    imports._enqueue_selected_import(
                        {**current, "params": enqueue_params, "family": imports._importer_family("nucc")}
                    ),
                    timeout=min(30, _nucc_remaining_seconds()),
                )
            )
            enqueue_result = await _await_worker_launch(task)
            acknowledged_by_field = {
                **run_values_by_field,
                **enqueue_result,
                "metrics": {**(current.get("metrics") or {}), **enqueue_result.get("metrics", {})},
            }
            await imports._write_enqueue_result(run_id, "nucc", acknowledged_by_field)
            if protected:
                from process.nucc_protected_control import _locked_source_run

                current = await _locked_source_run(imports.db._transaction_binding().session, run_id)
            else:
                current_record = (
                    await imports.db.execute(
                        imports.select(imports.ImportRun.__table__).where(imports.ImportRun.run_id == run_id)
                    )
                ).first()
                current = imports._raw_connection_run(current_record) if current_record is not None else None
    return _publish_nucc_run(await imports.get_import_run(run_id))


def _nucc_run_is_unclaimed(current):
    progress = current.get("progress") or {}
    metrics = current.get("metrics") or {}
    return (
        current.get("status") == "queued"
        and current.get("phase_detail") == "created"
        and current.get("finished_at") is None
        and all(progress.get(key) is None for key in ("attempt_id", "attempt_started_at"))
        and all(metrics.get(key) is None for key in ("nucc_handoff", "nucc_native_publication"))
        and metrics.get("job_id") in {None, f"nucc_start_{current['run_id']}"}
    )


async def _request_nucc_cancel(run_id):
    """Fence exact cancellation before signaling; a publication handoff stays Publisher-owned."""
    from api import control_imports as imports
    from api.nucc_source_control import require_source_attempt

    async with _nucc_action_transaction(run_id) as (current_by_field, receipt, protected):
        if (
            current_by_field is not None
            and receipt is None
            and current_by_field.get("status") not in imports.TERMINAL_STATUSES
        ):
            handoff = (current_by_field.get("metrics") or {}).get("nucc_handoff")
            if protected:
                require_source_attempt(current_by_field)
                if handoff is not None and current_by_field["status"] not in {"finalizing", "canceling"}:
                    raise RuntimeError("NUCC Source handoff state differs")
            canceled_at = imports.utc_now()
            statement = (
                imports.update(imports.ImportRun)
                .where(
                    imports.ImportRun.run_id == run_id,
                    imports.ImportRun.node_id == current_by_field["node_id"],
                    imports.ImportRun.engine == current_by_field["engine"],
                    imports.ImportRun.importer == "nucc",
                    imports.ImportRun.status == current_by_field["status"],
                    imports.ImportRun.progress
                    == imports.literal(current_by_field.get("progress") or {}, type_=imports.JSONB),
                    imports.ImportRun.metrics
                    == imports.literal(current_by_field.get("metrics") or {}, type_=imports.JSONB),
                )
                .values(status="canceling", phase_detail="cancel requested", heartbeat_at=canceled_at)
            )
            if await imports.db.status(statement) != 1:
                raise RuntimeError("NUCC cancellation attempt changed")
            if protected and handoff is not None:
                current_by_field = {
                    **current_by_field,
                    "status": "canceling",
                    "phase_detail": "cancel requested",
                    "heartbeat_at": canceled_at,
                }
            else:
                metrics = current_by_field.get("metrics") or {}
                signal_task = asyncio.create_task(
                    imports._cancel_signal_for_run(
                        current_by_field,
                        run_id=run_id,
                        is_pending_adapter=current_by_field["status"] == "queued"
                        and metrics.get("enqueue_adapter") == "pending",
                        is_queued_arq=imports._is_queued_arq_cancel(current_by_field, metrics),
                        nucc_redis_timeout=min(30, _nucc_remaining_seconds()),
                    )
                )
                signal = await _await_worker_launch(signal_task)
                current_by_field = await imports._request_worker_cancel(
                    current_by_field, cancel_signal=signal, publish=False
                )
    return _publish_nucc_run(await imports.get_import_run(run_id))


def _nucc_worker_selection_failure(payload, selected_specs, importer, run_id):
    selects_nucc = any("nucc" in spec.importers for spec in selected_specs)
    if (selects_nucc and importer not in {"", "nucc"}) or (importer == "nucc" and not selects_nucc):
        return _failed_worker_admission(payload, "NUCC worker selector conflicts with importer")
    if selects_nucc and not run_id:
        return _failed_worker_admission(payload, "NUCC worker launch requires run_id")
    return None


async def _admit_importer_worker_ensure(worker_payload, *, run_id, importer, selected_specs):
    if any("provider-directory-fhir" in spec.importers for spec in selected_specs):
        return await provider_directory_cms_registry_stop.guarded_ensure(
            worker_payload,
            run_id=run_id,
            importer=importer,
            selected_specs=selected_specs,
        )
    if any("nucc" in spec.importers for spec in selected_specs):
        return await _guarded_nucc_ensure(worker_payload, run_id=run_id, selected_specs=selected_specs)
    admission_failure = await _admit_worker_ensure(
        worker_payload,
        run_id=run_id,
        importer=importer,
        selected_specs=selected_specs,
    )
    return admission_failure


def _nucc_remaining_seconds():
    deadline = _NUCC_ACTION_DEADLINE.get()
    if deadline is None:
        return None
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise RuntimeError("NUCC worker action deadline expired")
    return remaining


def _nucc_subprocess_options():
    remaining = _nucc_remaining_seconds()
    return {} if remaining is None else {"timeout": remaining}


async def _nucc_action_budget(session):
    from sqlalchemy import text

    limits = await session.scalar(
        text(
            "SELECT jsonb_build_object('idle_ms',EXTRACT(EPOCH FROM "
            "current_setting('idle_in_transaction_session_timeout')::interval)*1000,"
            "'transaction_remaining_ms',CASE WHEN COALESCE(current_setting('transaction_timeout',true),'0')::interval "
            "= interval '0' THEN NULL ELSE EXTRACT(EPOCH FROM "
            "COALESCE(current_setting('transaction_timeout',true),'0')::interval "
            "- (clock_timestamp()-transaction_timestamp()))*1000 END)"
        )
    )
    return _bounded_nucc_action_budget(limits)


def _bounded_nucc_action_budget(limits):
    if not isinstance(limits, dict) or set(limits) != {"idle_ms", "transaction_remaining_ms"}:
        raise RuntimeError("NUCC transaction timeout limits are unavailable")
    budget = 45.0
    for key in ("idle_ms", "transaction_remaining_ms"):
        value = limits[key]
        if value is None and key == "transaction_remaining_ms":
            continue
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise RuntimeError("NUCC transaction timeout limit is invalid")
        if key == "idle_ms" and value == 0:
            continue
        budget = min(budget, value / 1000 - 2)
    if budget <= 0:
        raise RuntimeError("NUCC transaction timeout margin is exhausted")
    return budget
