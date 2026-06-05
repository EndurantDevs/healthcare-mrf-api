# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import datetime as dt
import os
import shutil
import uuid
from dataclasses import asdict
from pathlib import Path
from typing import Any

import click
from arq import create_pool
from sqlalchemy import insert, select, text, update
from sqlalchemy.exc import IntegrityError

from db.models import ImportRun, db
from process.import_status_events import enqueue_status_event
from process.live_progress import enqueue_live_progress, estimate_payload_from_live, progress_payload_from_live, read_live_progress
from process.redis_config import build_redis_settings
from process.serialization import deserialize_job, serialize_job

ENGINE_NAME = "healthcare-mrf-api"
ACTIVE_STATUSES = {"queued", "starting", "running", "finalizing", "canceling"}
TERMINAL_STATUSES = {"succeeded", "failed", "canceled", "dead_letter"}
CANCEL_FLAG_TTL_SECONDS = 7 * 24 * 60 * 60

_SINGLE_JOB_ADAPTERS: dict[str, dict[str, Any]] = {
    "ptg": {"queue": "arq:PTG", "function": "ptg_control_start", "payload": "ptg_control", "job_prefix": "ptg_start"},
    "mrf": {"queue": "arq:MRF", "function": "init_file", "payload": "test_mode"},
    "npi": {
        "queue": "arq:NPI",
        "function": "control_single_job_start",
        "payload": "control_wrapped",
        "target_module": "process.npi",
        "target_function": "process_data",
    },
    "nucc": {
        "queue": "arq:NUCC",
        "function": "control_single_job_start",
        "payload": "control_wrapped",
        "target_module": "process.nucc",
        "target_function": "process_data",
    },
    "code-sets": {
        "queue": "arq:CodeSets",
        "function": "control_single_job_start",
        "payload": "control_wrapped_kwargs",
        "target_module": "process.code_sets",
        "target_function": "main",
    },
    "ms-drg": {
        "queue": "arq:MSDRG",
        "function": "control_single_job_start",
        "payload": "control_wrapped_kwargs",
        "target_module": "process.ms_drg",
        "target_function": "main",
    },
    "clinical-reference": {
        "queue": "arq:ClinicalReference",
        "function": "control_single_job_start",
        "payload": "control_wrapped_kwargs",
        "target_module": "process.clinical_reference",
        "target_function": "main",
    },
    "geo": {
        "queue": "arq:Geo",
        "function": "control_single_job_start",
        "payload": "control_wrapped_kwargs",
        "target_module": "process.geo_import",
        "target_function": "main",
    },
    "geo-census": {
        "queue": "arq:GeoCensus",
        "function": "control_single_job_start",
        "payload": "control_wrapped_kwargs",
        "target_module": "process.geo_census_import",
        "target_function": "load_geo_census_lookup",
    },
    "plan-attributes": {
        "queue": "arq:Attributes",
        "function": "control_single_job_start",
        "payload": "control_wrapped",
        "target_module": "process.attributes",
        "target_function": "plan_attributes_control_start",
    },
    "mrf-source-discovery": {
        "queue": "arq:MRFSourceDiscovery",
        "function": "control_single_job_start",
        "payload": "control_wrapped_kwargs",
        "target_module": "process.mrf_source_discovery",
        "target_function": "main",
    },
    "claims-pricing": {"queue": "arq:ClaimsPricing", "function": "claims_pricing_start", "payload": "run_import", "job_prefix": "claims_start"},
    "claims-procedures": {"queue": "arq:ClaimsPricing", "function": "claims_pricing_start", "payload": "run_import", "job_prefix": "claims_procedures_start"},
    "drug-claims": {"queue": "arq:DrugClaims", "function": "drug_claims_start", "payload": "run_import", "job_prefix": "drug_claims_start"},
    "provider-quality": {"queue": "arq:ProviderQuality", "function": "provider_quality_start", "payload": "run_import", "job_prefix": "provider_quality_start"},
    "partd-formulary-network": {"queue": "arq:PartDFormularyNetwork", "function": "partd_formulary_network_start", "payload": "run_import"},
    "pharmacy-license": {"queue": "arq:PharmacyLicense", "function": "pharmacy_license_start", "payload": "run_import"},
    "places-zcta": {
        "queue": "arq:PlacesZcta",
        "function": "control_single_job_start",
        "payload": "control_wrapped",
        "target_module": "process.places_zcta",
        "target_function": "process_data",
    },
    "lodes": {
        "queue": "arq:LODES",
        "function": "control_single_job_start",
        "payload": "control_wrapped",
        "target_module": "process.lodes",
        "target_function": "process_data",
    },
    "medicare-enrollment": {
        "queue": "arq:MedicareEnrollment",
        "function": "control_single_job_start",
        "payload": "control_wrapped",
        "target_module": "process.medicare_enrollment",
        "target_function": "process_data",
    },
    "cms-doctors": {
        "queue": "arq:CMSDoctors",
        "function": "control_single_job_start",
        "payload": "control_wrapped",
        "target_module": "process.cms_doctors",
        "target_function": "process_data",
    },
    "facility-anchors": {
        "queue": "arq:FacilityAnchors",
        "function": "control_single_job_start",
        "payload": "control_wrapped",
        "target_module": "process.facility_anchors",
        "target_function": "process_data",
    },
    "pharmacy-economics": {
        "queue": "arq:PharmacyEconomics",
        "function": "control_single_job_start",
        "payload": "control_wrapped",
        "target_module": "process.pharmacy_economics",
        "target_function": "process_data",
    },
    "provider-enrichment": {
        "queue": "arq:ProviderEnrichment",
        "function": "control_single_job_start",
        "payload": "control_wrapped",
        "target_module": "process.provider_enrichment",
        "target_function": "process_data",
    },
    "entity-address-unified": {
        "queue": "arq:EntityAddressUnified",
        "function": "control_single_job_start",
        "payload": "control_wrapped",
        "target_module": "process.entity_address_unified",
        "target_function": "process_data",
    },
}

_CANCELABLE_IMPORTERS = {"ptg", "npi", "nucc", "places-zcta", "cms-doctors"}
_FINISH_IMPORTERS = {
    "mrf",
    "claims-pricing",
    "claims-procedures",
    "drug-claims",
    "provider-quality",
    "partd-formulary-network",
    "pharmacy-license",
}


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.UTC).replace(tzinfo=None)


def _option_type_name(option: click.Option) -> str:
    if isinstance(option.type, click.Choice):
        return "choice"
    name = getattr(option.type, "name", None)
    return str(name or option.type or "string")


def _param_schema(command: click.Command) -> list[dict[str, Any]]:
    params: list[dict[str, Any]] = []
    for param in command.params:
        if not isinstance(param, click.Option):
            continue
        params.append(
            {
                "name": param.name,
                "opts": list(param.opts),
                "required": bool(param.required),
                "multiple": bool(param.multiple),
                "is_flag": bool(param.is_flag),
                "type": _option_type_name(param),
                "default": _json_safe_default(param.default),
                "help": param.help,
            }
        )
    return params


def _json_safe_default(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (list, tuple)):
        return [_json_safe_default(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_safe_default(item) for key, item in value.items()}
    return None


def importer_registry() -> list[dict[str, Any]]:
    from process import process_group, process_group_end  # pylint: disable=import-outside-toplevel

    finish_commands = set(process_group_end.commands)
    importers: list[dict[str, Any]] = []
    for name, command in sorted(process_group.commands.items()):
        importers.append(
            {
                "name": name,
                "engine": ENGINE_NAME,
                "family": _importer_family(name),
                "kind": "discovered" if name == "ptg" else "scheduled",
                "lifecycle": "start_finish" if name in finish_commands else "single",
                "schedulable": True,
                "cancelable": name in _CANCELABLE_IMPORTERS,
                "retryable": True,
                "enqueue_adapter": "arq_single_job" if name in _SINGLE_JOB_ADAPTERS else "pending",
                "params_schema": _param_schema(command),
            }
        )
    return importers


def importer_names() -> set[str]:
    return {entry["name"] for entry in importer_registry()}


def _importer_family(importer: str) -> str:
    if importer in {"ptg", "mrf", "mrf-source-discovery"}:
        return "mrf"
    if importer in {"claims-pricing", "claims-procedures", "drug-claims"}:
        return "claims"
    if importer in {"npi", "nucc", "provider-quality", "provider-enrichment", "entity-address-unified", "cms-doctors"}:
        return "provider"
    if importer in {"partd-formulary-network", "pharmacy-license", "pharmacy-economics"}:
        return "pharmacy"
    if importer in {"geo", "geo-census", "places-zcta", "lodes"}:
        return "geo"
    if importer in {"code-sets", "ms-drg", "clinical-reference", "plan-attributes"}:
        return "reference"
    return "other"


def _new_run_id() -> str:
    return f"run_{uuid.uuid4().hex}"


def node_health() -> dict[str, Any]:
    artifact_root = Path(os.getenv("HLTHPRT_PTG2_ARTIFACT_ROOT") or "/tmp")
    try:
        usage = shutil.disk_usage(artifact_root)
        disk = {"path": str(artifact_root), "total": usage.total, "used": usage.used, "free": usage.free}
    except OSError:
        disk = {"path": str(artifact_root), "total": None, "used": None, "free": None}
    return {
        "engine": ENGINE_NAME,
        "node_id": os.getenv("HLTHPRT_IMPORT_NODE_ID"),
        "status": "ok",
        "time": utc_now().isoformat(),
        "features": {
            "control_api": True,
            "ptg_parse_preview": True,
            "enqueue_adapters": True,
            "enqueue_adapter_count": len(_SINGLE_JOB_ADAPTERS),
        },
        "disk": disk,
    }


async def ensure_import_run_table() -> None:
    if not hasattr(db, "connect") or not hasattr(db, "engine"):
        return
    await db.connect()
    if db.engine is None:
        return
    async with db.engine.begin() as conn:
        schema = ImportRun.__table__.schema or (os.getenv("HLTHPRT_DB_SCHEMA") or "mrf")
        await conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
        await conn.run_sync(ImportRun.__table__.create, checkfirst=True)
        for spec in getattr(ImportRun, "__my_additional_indexes__", []) or []:
            name = str(spec.get("name") or "").strip()
            columns = ", ".join(str(item).strip() for item in spec.get("index_elements", ()) if str(item).strip())
            if not name or not columns:
                continue
            unique = "UNIQUE " if spec.get("unique") else ""
            where = f" WHERE {spec['where']}" if spec.get("where") else ""
            await conn.execute(
                text(f'CREATE {unique}INDEX IF NOT EXISTS {name} ON "{schema}"."{ImportRun.__tablename__}" ({columns}){where}')
            )


def parse_ptg_toc_preview(payload: dict[str, Any]) -> dict[str, Any]:
    from process.ptg_parts.source_jobs import parse_toc_catalog_entries  # pylint: disable=import-outside-toplevel

    toc_content = payload.get("toc")
    if not isinstance(toc_content, dict):
        raise ValueError("toc must be an object")
    toc_url = str(payload.get("toc_url") or "inline://toc")
    entries = parse_toc_catalog_entries(
        toc_content,
        toc_url=toc_url,
        plan_ids=_string_list(payload.get("plan_ids")),
        plan_name_contains=_string_list(payload.get("plan_name_contains")),
        plan_market_types=_string_list(payload.get("plan_market_types")),
    )
    items = [asdict(entry) for entry in entries]
    by_domain: dict[str, int] = {}
    plans: dict[tuple[str, str | None], dict[str, Any]] = {}
    for item in items:
        domain = str(item.get("domain") or "unknown")
        by_domain[domain] = by_domain.get(domain, 0) + 1
        for plan in item.get("plan_info") or ():
            if not isinstance(plan, dict):
                continue
            plan_id = str(plan.get("plan_id") or "").strip()
            market_type = plan.get("plan_market_type")
            if plan_id:
                plans[(plan_id, market_type)] = plan
    return {
        "status": "parsed",
        "counts": {
            "entries": len(items),
            "plans": len(plans),
            "by_domain": by_domain,
        },
        "items": items,
    }


def _string_list(value: Any) -> list[str] | None:
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        return [text] if text else None
    if isinstance(value, (list, tuple)):
        result = [str(item).strip() for item in value if str(item).strip()]
        return result or None
    return None


def normalize_run(row: Any) -> dict[str, Any]:
    if row is None:
        return {}
    if hasattr(row, "to_json_dict"):
        data = row.to_json_dict()
    elif isinstance(row, dict):
        data = dict(row)
    else:
        data = {name: getattr(row, name) for name in ImportRun.__table__.columns.keys() if hasattr(row, name)}
    return _overlay_live_progress(data)


def _overlay_live_progress(data: dict[str, Any]) -> dict[str, Any]:
    if data.get("status") not in ACTIVE_STATUSES:
        return data
    live = read_live_progress(str(data.get("run_id") or ""))
    if not live:
        return data
    data = dict(data)
    data["progress"] = {**dict(data.get("progress") or {}), **progress_payload_from_live(live)}
    estimate = estimate_payload_from_live(live)
    if estimate:
        data["estimate"] = estimate
    phase = live.get("phase") or data.get("phase_detail")
    if phase:
        data["phase_detail"] = str(phase)[:128]
    return data


def _finish_params_for(importer: str, current: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    params = dict(current.get("params") or {})
    overrides = payload.get("params") if isinstance(payload.get("params"), dict) else {}
    params.update(overrides)
    test_mode = bool(payload.get("test_mode", params.get("test_mode", params.get("test", False))))
    import_id = (
        payload.get("import_id")
        or params.get("import_id")
        or current.get("import_id")
        or utc_now().strftime("%Y%m%d")
    )
    result = {
        "import_id": str(import_id),
        "test_mode": test_mode,
    }
    if importer != "mrf":
        result["run_id"] = current["run_id"]
    manifest_path = payload.get("manifest_path") or params.get("manifest_path")
    if manifest_path:
        result["manifest_path"] = manifest_path
    return result


def _finish_function(importer: str):
    if importer in {"claims-pricing", "claims-procedures"}:
        from process.claims_pricing import finish_main  # pylint: disable=import-outside-toplevel
    elif importer == "drug-claims":
        from process.drug_claims import finish_main  # pylint: disable=import-outside-toplevel
    elif importer == "provider-quality":
        from process.provider_quality import finish_main  # pylint: disable=import-outside-toplevel
    elif importer == "partd-formulary-network":
        from process.partd_formulary_network import finish_main  # pylint: disable=import-outside-toplevel
    elif importer == "pharmacy-license":
        from process.pharmacy_license import finish_main  # pylint: disable=import-outside-toplevel
    elif importer == "mrf":
        from process.initial import finish_main  # pylint: disable=import-outside-toplevel
    else:
        raise ValueError(f"importer does not support finalize: {importer}")
    return finish_main


async def list_import_runs(*, status: str | None = None, importer: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
    await ensure_import_run_table()
    bounded_limit = max(1, min(int(limit or 50), 200))
    stmt = select(ImportRun).order_by(ImportRun.created_at.desc()).limit(bounded_limit)
    if status:
        stmt = stmt.where(ImportRun.status == status)
    if importer:
        stmt = stmt.where(ImportRun.importer == importer)
    result = await db.execute(stmt)
    return [normalize_run(row) for row in result.scalars().all()]


async def get_import_run(run_id: str) -> dict[str, Any] | None:
    await ensure_import_run_table()
    result = await db.execute(select(ImportRun).where(ImportRun.run_id == run_id).limit(1))
    row = result.scalar_one_or_none()
    return normalize_run(row) if row else None


async def finalize_import_run(run_id: str, payload: dict[str, Any]) -> dict[str, Any] | None:
    current = await get_import_run(run_id)
    if not current:
        return None
    importer = str(current.get("importer") or "").strip()
    if importer not in _FINISH_IMPORTERS:
        raise ValueError(f"importer does not support finalize: {importer}")
    if current.get("status") in TERMINAL_STATUSES:
        return current

    finish_params = _finish_params_for(importer, current, payload)
    finish_fn = _finish_function(importer)
    result = await finish_fn(**finish_params)
    now = utc_now()
    metrics = dict(current.get("metrics") or {})
    metrics["finalize"] = result if isinstance(result, dict) else {"queued": True}
    await db.execute(
        update(ImportRun)
        .where(ImportRun.run_id == run_id)
        .values(
            status="finalizing",
            phase_detail="finalize enqueued",
            heartbeat_at=now,
            progress={"unit": "run", "total": 1, "done": 0, "pct": 0, "message": "finalizing"},
            metrics=metrics,
            import_id=finish_params.get("import_id"),
        )
    )
    return await get_import_run(run_id)


async def find_active_run_by_idempotency_key(idempotency_key: str) -> dict[str, Any] | None:
    await ensure_import_run_table()
    result = await db.execute(
        select(ImportRun)
        .where(ImportRun.idempotency_key == idempotency_key)
        .where(ImportRun.status.in_(ACTIVE_STATUSES))
        .limit(1)
    )
    row = result.scalar_one_or_none()
    return normalize_run(row) if row else None


async def create_import_run(payload: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    importer = str(payload.get("importer") or "").strip()
    if importer not in importer_names():
        raise ValueError(f"unknown importer: {importer}")
    await ensure_import_run_table()

    idempotency_key = str(payload.get("idempotency_key") or "").strip() or None
    if idempotency_key:
        active = await find_active_run_by_idempotency_key(idempotency_key)
        if active:
            return active, False

    now = utc_now()
    run_id = str(payload.get("run_id") or "").strip() or _new_run_id()
    row = {
        "run_id": run_id,
        "engine": ENGINE_NAME,
        "node_id": os.getenv("HLTHPRT_IMPORT_NODE_ID"),
        "importer": importer,
        "family": _importer_family(importer),
        "status": "queued",
        "phase_detail": "created",
        "params": payload.get("params") if isinstance(payload.get("params"), dict) else {},
        "idempotency_key": idempotency_key,
        "triggered_by": str(payload.get("triggered_by") or "api"),
        "schedule_id": payload.get("schedule_id"),
        "subscription_id": payload.get("subscription_id"),
        "source_file_import_id": payload.get("source_file_import_id"),
        "created_at": now,
        "heartbeat_at": now,
        "progress": {"unit": "run", "total": 1, "done": 0, "pct": 0, "message": "queued"},
        "metrics": {},
        "error": None,
        "snapshot_id": None,
        "import_id": payload.get("import_id"),
        "retry_of_run_id": payload.get("retry_of_run_id"),
    }
    try:
        await db.execute(insert(ImportRun).values(**row))
    except IntegrityError:
        if idempotency_key:
            active = await find_active_run_by_idempotency_key(idempotency_key)
            if active:
                return active, False
        raise
    enqueue_result = await _enqueue_import_start(row)
    row.update(enqueue_result)
    await db.execute(
        update(ImportRun)
        .where(ImportRun.run_id == run_id)
        .values(
            status=row["status"],
            phase_detail=row["phase_detail"],
            heartbeat_at=row["heartbeat_at"],
            progress=row["progress"],
            metrics=row["metrics"],
            error=row["error"],
        )
    )
    enqueue_status_event(row)
    _write_run_live_progress(row, publish_event=False)
    return row, True


async def _enqueue_import_start(row: dict[str, Any]) -> dict[str, Any]:
    importer = str(row.get("importer") or "")
    adapter = _SINGLE_JOB_ADAPTERS.get(importer)
    now = utc_now()
    if adapter is None:
        return {
            "status": "queued",
            "phase_detail": "created; enqueue adapter pending",
            "heartbeat_at": now,
            "progress": {"unit": "run", "total": 1, "done": 0, "pct": 0, "message": "queued; enqueue adapter pending"},
            "metrics": {"enqueue_adapter": "pending"},
            "error": None,
        }

    params = row.get("params") if isinstance(row.get("params"), dict) else {}
    job_payload = _adapter_payload(adapter, row, params)
    kwargs = {"_queue_name": adapter["queue"]}
    if adapter.get("job_prefix"):
        kwargs["_job_id"] = f"{adapter['job_prefix']}_{row['run_id']}"
    try:
        redis = await create_pool(
            build_redis_settings(),
            job_serializer=serialize_job,
            job_deserializer=deserialize_job,
        )
        job = await redis.enqueue_job(adapter["function"], job_payload, **kwargs)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        return {
            "status": "failed",
            "phase_detail": "enqueue failed",
            "heartbeat_at": utc_now(),
            "progress": {"unit": "run", "total": 1, "done": 0, "pct": 0, "message": "enqueue failed"},
            "metrics": {"enqueue_adapter": "arq_single_job", "queue": adapter["queue"], "function": adapter["function"]},
            "error": {"code": "enqueue_failed", "message": str(exc)},
        }
    job_id = getattr(job, "job_id", None) or str(job or "")
    return {
        "status": "queued",
        "phase_detail": "enqueued",
        "heartbeat_at": utc_now(),
        "progress": {"unit": "run", "total": 1, "done": 0, "pct": 0, "message": "queued"},
        "metrics": {
            "enqueue_adapter": "arq_single_job",
            "queue": adapter["queue"],
            "function": adapter["function"],
            "job_id": job_id,
        },
        "error": None,
    }


def _adapter_payload(adapter: dict[str, Any], row: dict[str, Any], params: dict[str, Any]) -> dict[str, Any]:
    test_mode = bool(params.get("test_mode", params.get("test", False)))
    if adapter["payload"] == "test_mode":
        return {"test_mode": test_mode, "run_id": row["run_id"]}
    if adapter["payload"] in {"control_wrapped", "control_wrapped_kwargs"}:
        return {
            "run_id": row["run_id"],
            "importer": row.get("importer"),
            "family": row.get("family"),
            "target_module": adapter["target_module"],
            "target_function": adapter["target_function"],
            "call_style": "kwargs" if adapter["payload"] == "control_wrapped_kwargs" else "ctx_task",
            "task": {"test_mode": test_mode, **params},
        }
    if adapter["payload"] == "run_import":
        payload = {
            "run_id": row["run_id"],
            "import_id": params.get("import_id") or row.get("import_id"),
            "test_mode": test_mode,
        }
        for key in ("artifacts", "source_urls", "max_records", "max_files"):
            if key in params:
                payload[key] = params[key]
        return payload
    if adapter["payload"] == "ptg_control":
        return {
            "run_id": row["run_id"],
            "source_file_import_id": row.get("source_file_import_id"),
            "params": dict(params),
        }
    return dict(params)


async def request_cancel(run_id: str) -> dict[str, Any] | None:
    current = await get_import_run(run_id)
    if not current:
        return None
    if current.get("status") in TERMINAL_STATUSES:
        return current
    if current.get("status") != "queued" and not _supports_active_cancel(str(current.get("importer") or "")):
        raise ValueError(f"importer does not support canceling active runs: {current.get('importer')}")
    now = utc_now()
    current_progress = current.get("progress") if isinstance(current.get("progress"), dict) else {}
    current_metrics = current.get("metrics") if isinstance(current.get("metrics"), dict) else {}
    metrics = dict(current_metrics)
    pending_adapter = current.get("status") == "queued" and metrics.get("enqueue_adapter") == "pending"
    queued_arq = current.get("status") == "queued" and metrics.get("enqueue_adapter") == "arq_single_job"
    if pending_adapter:
        cancel_signal = {"redis": False, "pending_adapter": True}
    elif queued_arq:
        cancel_signal = await _remove_queued_job(current)
    else:
        cancel_signal = await _set_cancel_flag(run_id)
    metrics["cancel_signal"] = cancel_signal
    canceled_before_start = pending_adapter or queued_arq
    status = "canceled" if canceled_before_start else "canceling"
    progress = {
        "unit": "run",
        "total": 1,
        "done": 1 if canceled_before_start else 0,
        "pct": 100 if canceled_before_start else current_progress.get("pct", 0),
        "message": "canceled" if canceled_before_start else "cancel requested",
    }
    await db.execute(
        update(ImportRun)
        .where(ImportRun.run_id == run_id)
        .values(
            status=status,
            phase_detail="canceled before start" if canceled_before_start else "cancel requested",
            heartbeat_at=now,
            finished_at=now if canceled_before_start else current.get("finished_at"),
            progress=progress,
            metrics=metrics,
        )
    )
    updated = await get_import_run(run_id)
    if updated:
        _write_run_live_progress({**updated, "progress": progress}, publish_event=False)
        enqueue_status_event({**updated, "progress": progress, "metrics": metrics})
    return updated


def _write_run_live_progress(run: dict[str, Any], *, publish_event: bool) -> None:
    progress = run.get("progress") if isinstance(run.get("progress"), dict) else {}
    payload = dict(progress)
    payload.update(
        run_id=run.get("run_id"),
        importer=run.get("importer"),
        status=run.get("status"),
        started_at=run.get("started_at"),
        finished_at=run.get("finished_at"),
        publish_event=publish_event,
    )
    payload.setdefault("phase", run.get("phase_detail"))
    payload.setdefault("message", run.get("phase_detail"))
    enqueue_live_progress(**payload)


async def _set_cancel_flag(run_id: str) -> dict[str, Any]:
    try:
        redis = await create_pool(
            build_redis_settings(),
            job_serializer=serialize_job,
            job_deserializer=deserialize_job,
        )
        await redis.set(f"cancel:{run_id}", "1", ex=CANCEL_FLAG_TTL_SECONDS)
        return {"redis": True, "key": f"cancel:{run_id}", "ttl_seconds": CANCEL_FLAG_TTL_SECONDS}
    except Exception as exc:  # pylint: disable=broad-exception-caught
        return {"redis": False, "error": str(exc)}


def _supports_active_cancel(importer: str) -> bool:
    return importer in _CANCELABLE_IMPORTERS


async def _remove_queued_job(run: dict[str, Any]) -> dict[str, Any]:
    metrics = run.get("metrics") if isinstance(run.get("metrics"), dict) else {}
    queue = str(metrics.get("queue") or "").strip()
    job_id = str(metrics.get("job_id") or "").strip()
    if not queue or not job_id:
        return {"redis": False, "removed": False, "reason": "missing queue or job_id"}
    try:
        redis = await create_pool(
            build_redis_settings(),
            job_serializer=serialize_job,
            job_deserializer=deserialize_job,
        )
        removed = int(await redis.zrem(queue, job_id) or 0)
        deleted = int(await redis.delete(f"arq:job:{job_id}") or 0)
        return {
            "redis": True,
            "queue": queue,
            "job_id": job_id,
            "removed": removed > 0,
            "deleted_job_key": deleted > 0,
        }
    except Exception as exc:  # pylint: disable=broad-exception-caught
        return {"redis": False, "removed": False, "error": str(exc), "queue": queue, "job_id": job_id}


async def retry_import_run(run_id: str, payload: dict[str, Any]) -> tuple[dict[str, Any], bool] | None:
    current = await get_import_run(run_id)
    if not current:
        return None
    retry_payload = {
        "importer": current["importer"],
        "params": current.get("params") or {},
        "triggered_by": payload.get("triggered_by") or "api",
        "idempotency_key": payload.get("idempotency_key"),
        "schedule_id": current.get("schedule_id"),
        "subscription_id": current.get("subscription_id"),
        "source_file_import_id": current.get("source_file_import_id"),
        "import_id": current.get("import_id"),
        "retry_of_run_id": run_id,
    }
    return await create_import_run(retry_payload)
