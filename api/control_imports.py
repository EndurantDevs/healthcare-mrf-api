# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import base64
import datetime as dt
import json
import os
import shutil
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import click
import redis
from arq import create_pool
from sqlalchemy import and_, insert, or_, select, text, update
from sqlalchemy.exc import IntegrityError

from db.models import ImportRun, db
from process.import_status_events import enqueue_status_event, isoformat_utc
from process.live_progress import enqueue_live_progress, estimate_payload_from_live, progress_payload_from_live, read_live_progress
from process.redis_config import build_redis_settings
from process.serialization import deserialize_job, serialize_job

ENGINE_NAME = "healthcare-mrf-api"
ACTIVE_STATUSES = {"queued", "starting", "running", "finalizing", "canceling"}
TERMINAL_STATUSES = {"succeeded", "failed", "canceled", "dead_letter"}
CANCEL_FLAG_TTL_SECONDS = 7 * 24 * 60 * 60
MAX_IMPORT_RUN_LIST_LIMIT = 200
MAX_TRIGGERED_BY_LENGTH = 32
_IMPORT_RUN_ENSURE_LOCK = asyncio.Lock()
_IMPORT_RUN_ADVISORY_LOCK_KEY = 44_706_101_200_001


@dataclass
class _ImportRunEnsureState:
    ensured: bool = False


_IMPORT_RUN_ENSURE_STATE = _ImportRunEnsureState()

_IMPORTER_DEPENDENCIES: dict[str, list[str]] = {
    "npi": ["nucc"],
    "terminology-synonyms": ["nucc", "code-sets", "clinical-reference", "claims-pricing", "drug-claims"],
}

_SINGLE_JOB_ADAPTERS: dict[str, dict[str, Any]] = {
    "ptg": {"queue": "arq:PTG", "function": "ptg_control_start", "payload": "ptg_control", "job_prefix": "ptg_start"},
    "ptg-candidate-audit": {
        "queue": "arq:PTGCandidateAudit",
        "function": "control_single_job_start",
        "payload": "control_wrapped_kwargs",
        "target_module": "process.ptg_candidate_audit",
        "target_function": "main",
        "job_prefix": "ptg_candidate_audit",
    },
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
    "terminology-synonyms": {
        "queue": "arq:TerminologySynonyms",
        "function": "control_single_job_start",
        "payload": "control_wrapped_kwargs",
        "target_module": "process.terminology_synonyms",
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
        "run_shutdown": True,
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
    "provider-directory-fhir": {
        "queue": "arq:ProviderDirectoryFHIR",
        "function": "control_single_job_start",
        "payload": "control_wrapped",
        "target_module": "process.provider_directory_fhir",
        "target_function": "process_data",
        "run_shutdown": True,
    },
    "entity-address-unified": {
        "queue": "arq:EntityAddressUnified",
        "function": "control_single_job_start",
        "payload": "control_wrapped",
        "target_module": "process.entity_address_unified",
        "target_function": "process_data",
        "run_shutdown": True,
    },
    "address-archive-v2-migrate": {
        "queue": "arq:AddressArchive",
        "function": "control_single_job_start",
        "payload": "control_wrapped",
        "target_module": "process.address_archive_migration",
        "target_function": "process_data",
    },
    "openaddresses": {
        "queue": "arq:OpenAddresses",
        "function": "control_single_job_start",
        "payload": "control_wrapped",
        "target_module": "process.openaddresses",
        "target_function": "process_data",
        "run_shutdown": True,
    },
}

_PTG_CONTROL_QUEUES = frozenset({"arq:PTG", "arq:PTGSmall", "arq:PTGNormal", "arq:PTGLarge", "arq:PTGHuge"})

_CANCELABLE_IMPORTERS = {
    "ptg",
    "ptg-candidate-audit",
    "npi",
    "nucc",
    "places-zcta",
    "cms-doctors",
    "mrf-source-discovery",
    "provider-directory-fhir",
    "address-archive-v2-migrate",
    "openaddresses",
}
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
    """Return a naive UTC timestamp for persisted control-plane records."""

    return dt.datetime.now(dt.UTC).replace(tzinfo=None)


def _option_type_name(option: click.Option) -> str:
    if isinstance(option.type, click.Choice):
        return "choice"
    name = getattr(option.type, "name", None)
    return str(name or option.type or "string")


def _param_schema(command: click.Command) -> list[dict[str, Any]]:
    parameter_schema_list: list[dict[str, Any]] = []
    for param in command.params:
        if not isinstance(param, click.Option):
            continue
        option_schema_by_name = {
            "name": param.name,
            "opts": list(param.opts),
            "required": bool(param.required),
            "multiple": bool(param.multiple),
            "is_flag": bool(param.is_flag),
            "type": _option_type_name(param),
            "default": _json_safe_default(param.default),
            "help": param.help,
        }
        if isinstance(param.type, click.Choice):
            option_schema_by_name["choices"] = list(param.type.choices)
        parameter_schema_list.append(option_schema_by_name)
    return parameter_schema_list


def _json_safe_default(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (list, tuple)):
        return [_json_safe_default(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_safe_default(item) for key, item in value.items()}
    return None


def importer_registry() -> list[dict[str, Any]]:
    """Describe the importer commands exposed by the public control API."""

    from process import process_group, process_group_end

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
                "queue": _SINGLE_JOB_ADAPTERS.get(name, {}).get("queue"),
                "depends_on": list(_IMPORTER_DEPENDENCIES.get(name, [])),
                "params_schema": _param_schema(command),
            }
        )
    return importers


def importer_names() -> set[str]:
    """Return every importer name accepted by the control API."""

    return {entry["name"] for entry in importer_registry()}


def _importer_family(importer: str) -> str:
    if importer in {"ptg", "ptg-candidate-audit", "mrf", "mrf-source-discovery"}:
        return "mrf"
    if importer in {"claims-pricing", "claims-procedures", "drug-claims"}:
        return "claims"
    if importer in {
        "npi",
        "nucc",
        "provider-quality",
        "provider-enrichment",
        "provider-directory-fhir",
        "entity-address-unified",
        "cms-doctors",
        "address-archive-v2-migrate",
    }:
        return "provider"
    if importer in {"partd-formulary-network", "pharmacy-license", "pharmacy-economics"}:
        return "pharmacy"
    if importer in {"geo", "geo-census", "places-zcta", "lodes", "openaddresses"}:
        return "geo"
    if importer in {"code-sets", "ms-drg", "clinical-reference", "terminology-synonyms", "plan-attributes"}:
        return "reference"
    return "other"


def _new_run_id() -> str:
    return f"run_{uuid.uuid4().hex}"


async def node_health() -> dict[str, Any]:
    """Collect bounded database, Redis, worker, disk, and memory health."""

    artifact_root = Path(os.getenv("HLTHPRT_PTG2_ARTIFACT_ROOT") or os.getenv("HLTHPRT_PTG2_ARTIFACT_DIR") or "/tmp")
    health_checks_by_name: dict[str, dict[str, Any]] = {
        "database": await _database_check(),
        "redis": _redis_check(),
    }
    worker_checks_by_name, worker_status_by_queue, queue_depth_by_name = (
        _worker_and_queue_health()
    )
    health_checks_by_name.update(worker_checks_by_name)
    failing_checks = sorted(
        name
        for name, health_check in health_checks_by_name.items()
        if not health_check.get("ok")
    )
    return {
        "engine": ENGINE_NAME,
        "node_id": os.getenv("HLTHPRT_IMPORT_NODE_ID"),
        "status": "degraded" if failing_checks else "ok",
        "checks": health_checks_by_name,
        "failing_checks": failing_checks,
        "time": dt.datetime.now(dt.UTC).isoformat(),
        "features": {
            "control_api": True,
            "ptg_parse_preview": True,
            "enqueue_adapters": True,
            "enqueue_adapter_count": len(_SINGLE_JOB_ADAPTERS),
        },
        "ram": _ram_status(),
        "disk": _disk_status_by_name(artifact_root),
        "queue_depth": queue_depth_by_name,
        "workers": worker_status_by_queue,
    }


def _disk_status_by_name(artifact_root: Path) -> dict[str, Any]:
    """Return bounded artifact-volume usage without failing node health."""

    try:
        usage = shutil.disk_usage(artifact_root)
        return {
            "path": str(artifact_root),
            "total": usage.total,
            "used": usage.used,
            "free": usage.free,
        }
    except OSError:
        return {
            "path": str(artifact_root),
            "total": None,
            "used": None,
            "free": None,
        }


def _worker_and_queue_health() -> tuple[
    dict[str, dict[str, Any]],
    dict[str, Any],
    dict[str, int],
]:
    """Return worker and queue snapshots with independent failure states."""

    health_checks_by_name: dict[str, dict[str, Any]] = {}
    worker_status_by_queue: dict[str, Any] = {}
    try:
        worker_status_by_queue = _worker_health()
        health_checks_by_name["workers"] = {
            "ok": True,
            "running": sum(
                1
                for worker_status in worker_status_by_queue.values()
                if worker_status.get("running")
            ),
        }
    except Exception as exc:
        health_checks_by_name["workers"] = {"ok": False, "error": str(exc)}
    queue_depth_by_name: dict[str, int] = {}
    try:
        queue_depth_by_name = _queue_depths()
        health_checks_by_name["queue_depth"] = {"ok": True}
    except Exception as exc:
        health_checks_by_name["queue_depth"] = {"ok": False, "error": str(exc)}
    return health_checks_by_name, worker_status_by_queue, queue_depth_by_name


def _ram_status() -> dict[str, int | None]:
    total = available = None
    memory_values_by_name: dict[str, int] = {}

    try:
        with open("/proc/meminfo", "r", encoding="utf-8") as handle:
            for line in handle:
                key, _sep, raw_value = line.partition(":")
                parts = raw_value.strip().split()
                if parts and parts[0].isdigit():
                    memory_values_by_name[key] = int(parts[0]) * 1024
            total = memory_values_by_name.get("MemTotal")
            available = memory_values_by_name.get("MemAvailable")
    except OSError:
        memory_values_by_name.clear()
    if total is None and hasattr(os, "sysconf"):
        try:
            total = int(os.sysconf("SC_PAGE_SIZE")) * int(os.sysconf("SC_PHYS_PAGES"))
        except (OSError, ValueError, TypeError):
            total = None
    return {
        "total": total,
        "available": available,
        "schedulable": (
            None
            if total is None
            else max(total - memory_values_by_name.get("Hugetlb", 0), 0)
        ),
    }


async def _database_check() -> dict[str, Any]:
    try:
        await db.execute(text("SELECT 1"))
        return {"ok": True}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def _redis_check() -> dict[str, Any]:
    try:
        _redis_client().ping()
        return {"ok": True}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def _worker_health() -> dict[str, Any]:
    from api.control_workers import worker_registry

    return {
        item["queue"]: {
            "worker_class": item["worker_class"],
            "role": item["role"],
            "running": item["running"],
            "pid": item.get("pid"),
        }
        for item in worker_registry()
    }


def _queue_depths() -> dict[str, int]:
    queues = {
        str(spec.get("queue"))
        for spec in _SINGLE_JOB_ADAPTERS.values()
        if str(spec.get("queue") or "").strip()
    }
    queues.update(_PTG_CONTROL_QUEUES)
    for importer in _FINISH_IMPORTERS:
        queue = str(_SINGLE_JOB_ADAPTERS.get(importer, {}).get("queue") or "").strip()
        if queue:
            queues.add(f"{queue}_finish")
    client = _redis_client()
    return {queue: int(client.zcard(queue) or 0) for queue in sorted(queues)}


def _redis_client() -> redis.Redis:
    dsn = os.getenv("HLTHPRT_REDIS_ADDRESS")
    if dsn:
        return redis.Redis.from_url(dsn, socket_connect_timeout=1.0, socket_timeout=1.0)
    settings = build_redis_settings()
    return redis.Redis(
        host=settings.host,
        port=settings.port,
        password=settings.password,
        db=settings.database,
        ssl=settings.ssl,
        socket_connect_timeout=1.0,
        socket_timeout=1.0,
    )


async def ensure_import_run_table() -> None:
    """Create the public import-run control table once per process."""

    if _IMPORT_RUN_ENSURE_STATE.ensured:
        return
    async with _IMPORT_RUN_ENSURE_LOCK:
        if _IMPORT_RUN_ENSURE_STATE.ensured:
            return
        await _ensure_import_run_table_once()
        _IMPORT_RUN_ENSURE_STATE.ensured = True


async def _ensure_import_run_table_once() -> None:
    if not hasattr(db, "connect") or not hasattr(db, "engine"):
        return
    await db.connect()
    if db.engine is None:
        return
    async with db.engine.begin() as conn:
        schema = ImportRun.__table__.schema or (os.getenv("HLTHPRT_DB_SCHEMA") or "mrf")
        quoted_schema = _quote_ident(schema)
        await conn.execute(text("SELECT pg_advisory_xact_lock(:lock_key)"), {"lock_key": _IMPORT_RUN_ADVISORY_LOCK_KEY})
        await conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {quoted_schema}"))
        await conn.run_sync(ImportRun.__table__.create, checkfirst=True)
        for spec in getattr(ImportRun, "__my_additional_indexes__", []) or []:
            name = str(spec.get("name") or "").strip()
            columns = ", ".join(str(item).strip() for item in spec.get("index_elements", ()) if str(item).strip())
            if not name or not columns:
                continue
            unique = "UNIQUE " if spec.get("unique") else ""
            where = f" WHERE {spec['where']}" if spec.get("where") else ""
            await conn.execute(
                text(
                    f"CREATE {unique}INDEX IF NOT EXISTS {_quote_ident(name)} "
                    f"ON {quoted_schema}.{_quote_ident(ImportRun.__tablename__)} ({columns}){where}"
                )
            )


def _quote_ident(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def parse_ptg_toc_preview(preview_payload_map: dict[str, Any]) -> dict[str, Any]:
    """Parse and summarize an inline PTG table of contents."""

    from process.ptg_parts.source_jobs import parse_toc_catalog_entries

    toc_content = preview_payload_map.get("toc")
    if not isinstance(toc_content, dict):
        raise ValueError("toc must be an object")
    toc_url = str(preview_payload_map.get("toc_url") or "inline://toc")
    entries = parse_toc_catalog_entries(
        toc_content,
        toc_url=toc_url,
        plan_ids=_string_list(preview_payload_map.get("plan_ids")),
        plan_name_contains=_string_list(
            preview_payload_map.get("plan_name_contains")
        ),
        plan_market_types=_string_list(
            preview_payload_map.get("plan_market_types")
        ),
    )
    catalog_entry_list = [asdict(entry) for entry in entries]
    by_domain: dict[str, int] = {}
    plan_by_identity: dict[tuple[str, str | None], dict[str, Any]] = {}
    for catalog_entry in catalog_entry_list:
        domain = str(catalog_entry.get("domain") or "unknown")
        by_domain[domain] = by_domain.get(domain, 0) + 1
        for plan in catalog_entry.get("plan_info") or ():
            if not isinstance(plan, dict):
                continue
            plan_id = str(plan.get("plan_id") or "").strip()
            market_type = plan.get("plan_market_type")
            if plan_id:
                plan_by_identity[(plan_id, market_type)] = plan
    return {
        "status": "parsed",
        "counts": {
            "entries": len(catalog_entry_list),
            "plans": len(plan_by_identity),
            "by_domain": by_domain,
        },
        "items": catalog_entry_list,
    }


def _string_list(value: Any) -> list[str] | None:
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        return [text] if text else None
    if isinstance(value, (list, tuple)):
        normalized_text_list = [
            str(item).strip() for item in value if str(item).strip()
        ]
        return normalized_text_list or None
    return None


_RUN_TIMESTAMP_KEYS = ("created_at", "started_at", "finished_at", "heartbeat_at")


def _serialize_run_timestamps(data: dict[str, Any]) -> dict[str, Any]:
    """Serialize naive-UTC run timestamps as timezone-aware UTC ISO-8601 strings."""
    data = dict(data)
    for key in _RUN_TIMESTAMP_KEYS:
        if data.get(key) is not None:
            data[key] = isoformat_utc(data[key])
    return data


def normalize_run(row: Any) -> dict[str, Any]:
    """Convert an import-run model or mapping to its API representation."""

    if row is None:
        return {}
    if hasattr(row, "to_json_dict"):
        data = row.to_json_dict()
    elif isinstance(row, dict):
        data = dict(row)
    else:
        data = {name: getattr(row, name) for name in ImportRun.__table__.columns.keys() if hasattr(row, name)}
    return _overlay_live_progress(_serialize_run_timestamps(data))


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


def _finish_params_for(
    importer: str,
    current: dict[str, Any],
    finish_payload_map: dict[str, Any],
) -> dict[str, Any]:
    current_params_by_name = dict(current.get("params") or {})
    overrides = (
        finish_payload_map.get("params")
        if isinstance(finish_payload_map.get("params"), dict)
        else {}
    )
    current_params_by_name.update(overrides)
    test_mode = bool(
        finish_payload_map.get(
            "test_mode",
            current_params_by_name.get(
                "test_mode",
                current_params_by_name.get("test", False),
            ),
        )
    )
    import_id = (
        finish_payload_map.get("import_id")
        or current_params_by_name.get("import_id")
        or current.get("import_id")
        or utc_now().strftime("%Y%m%d")
    )
    finish_params_by_name = {
        "import_id": str(import_id),
        "test_mode": test_mode,
    }
    if importer != "mrf":
        finish_params_by_name["run_id"] = current["run_id"]
    manifest_path = finish_payload_map.get(
        "manifest_path"
    ) or current_params_by_name.get("manifest_path")
    if manifest_path:
        finish_params_by_name["manifest_path"] = manifest_path
    return finish_params_by_name


def _finish_function(importer: str):
    if importer in {"claims-pricing", "claims-procedures"}:
        from process.claims_pricing import finish_main
    elif importer == "drug-claims":
        from process.drug_claims import finish_main
    elif importer == "provider-quality":
        from process.provider_quality import finish_main
    elif importer == "partd-formulary-network":
        from process.partd_formulary_network import finish_main
    elif importer == "pharmacy-license":
        from process.pharmacy_license import finish_main
    elif importer == "mrf":
        from process.initial import finish_main
    else:
        raise ValueError(f"importer does not support finalize: {importer}")
    return finish_main


async def list_import_runs(
    *,
    status: str | None = None,
    importer: str | None = None,
    retry_of_run_id: str | None = None,
    limit: int = 50,
    cursor: str | None = None,
) -> list[dict[str, Any]]:
    """Return the items from one filtered import-run page."""

    page = await list_import_runs_page(
        status=status,
        importer=importer,
        retry_of_run_id=retry_of_run_id,
        limit=limit,
        cursor=cursor,
    )
    return page["items"]


async def list_import_runs_page(
    *,
    status: str | None = None,
    importer: str | None = None,
    retry_of_run_id: str | None = None,
    limit: int = 50,
    cursor: str | None = None,
) -> dict[str, Any]:
    """Return a stable cursor page of filtered import runs."""

    bounded_limit = max(1, min(int(limit or 50), MAX_IMPORT_RUN_LIST_LIMIT))
    statement = select(ImportRun)
    if status:
        statement = statement.where(ImportRun.status == status)
    if importer:
        statement = statement.where(ImportRun.importer == importer)
    if retry_of_run_id:
        statement = statement.where(ImportRun.retry_of_run_id == retry_of_run_id)
    if cursor:
        created_at, run_id = _decode_import_run_cursor(cursor)
        statement = statement.where(
            or_(
                ImportRun.created_at < created_at,
                and_(ImportRun.created_at == created_at, ImportRun.run_id < run_id),
            )
        )
    statement = statement.order_by(
        ImportRun.created_at.desc(), ImportRun.run_id.desc()
    ).limit(bounded_limit + 1)
    query_result = await db.execute(statement)
    run_rows = list(query_result.scalars().all())
    next_cursor = None
    if len(run_rows) > bounded_limit:
        next_run_row = run_rows[bounded_limit - 1]
        next_cursor = _encode_import_run_cursor(
            next_run_row.created_at, next_run_row.run_id
        )
        run_rows = run_rows[:bounded_limit]
    return {
        "items": [normalize_run(run_row) for run_row in run_rows],
        "next_cursor": next_cursor,
    }


def _encode_import_run_cursor(created_at: dt.datetime | None, run_id: str) -> str | None:
    if created_at is None or not run_id:
        return None
    if created_at.tzinfo is not None:
        created_at = created_at.astimezone(dt.UTC).replace(tzinfo=None)
    payload = json.dumps({"created_at": created_at.isoformat(), "run_id": run_id}, separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(payload).decode().rstrip("=")


def _decode_import_run_cursor(cursor: str) -> tuple[dt.datetime, str]:
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        payload = json.loads(base64.urlsafe_b64decode(padded.encode()).decode())
        created_at = dt.datetime.fromisoformat(str(payload["created_at"]).replace("Z", "+00:00"))
        run_id = str(payload["run_id"]).strip()
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError("invalid cursor") from exc
    if not run_id:
        raise ValueError("invalid cursor")
    if created_at.tzinfo is not None:
        created_at = created_at.astimezone(dt.UTC).replace(tzinfo=None)
    return created_at, run_id


async def get_import_run(run_id: str) -> dict[str, Any] | None:
    """Return one import run after reconciling terminal worker failures."""

    result = await db.execute(select(ImportRun).where(ImportRun.run_id == run_id).limit(1))
    row = result.scalar_one_or_none()
    if not row:
        return None
    return await _sync_terminal_worker_failure(normalize_run(row))


async def _sync_terminal_worker_failure(run: dict[str, Any]) -> dict[str, Any]:
    if run.get("status") not in {"starting", "running", "finalizing"}:
        return run
    worker_status = await _active_worker_state(run)
    failed_item = _failed_worker_state_item(worker_status)
    if failed_item is None:
        return run

    now = utc_now()
    progress_dict = {"unit": "run", "total": 1, "done": 1, "pct": 100, "message": "worker job failed"}
    metrics_map = dict(run.get("metrics") or {})
    metrics_map["terminal_worker_state"] = worker_status
    error_dict = _worker_job_failure_error(failed_item)
    await db.execute(
        update(ImportRun)
        .where(ImportRun.run_id == run["run_id"])
        .values(
            status="failed",
            phase_detail="worker job failed",
            heartbeat_at=now,
            finished_at=now,
            progress=progress_dict,
            metrics=metrics_map,
            error=error_dict,
        )
    )
    return {
        **run,
        "status": "failed",
        "phase_detail": "worker job failed",
        "heartbeat_at": isoformat_utc(now),
        "finished_at": isoformat_utc(now),
        "progress": progress_dict,
        "metrics": metrics_map,
        "error": error_dict,
    }


async def _active_worker_state(run: dict[str, Any]) -> dict[str, Any]:
    payload = _active_worker_cancel_payload(run)
    if not payload:
        return {"status": "unsupported", "items": []}
    try:
        from api.control_workers import worker_state

        return await asyncio.to_thread(worker_state, payload)
    except Exception as exc:
        return {"status": "error", "items": [], "message": str(exc)}


def _failed_worker_state_item(worker_status: dict[str, Any]) -> dict[str, Any] | None:
    items = worker_status.get("items") if isinstance(worker_status, dict) else None
    if not isinstance(items, list):
        return None
    for item in items:
        if isinstance(item, dict) and item.get("job_status") == "failed":
            return item
    return None


def _worker_job_failure_error(worker_item: dict[str, Any]) -> dict[str, Any]:
    failure = worker_item.get("failure") if isinstance(worker_item.get("failure"), dict) else {}
    job_name = str(worker_item.get("job_name") or "worker job")
    reason = str(failure.get("reason") or worker_item.get("job_status") or "failed").strip()
    message = f"Kubernetes worker job {job_name} failed"
    if reason:
        message = f"{message}: {reason}"

    error_dict: dict[str, Any] = {
        "code": "worker_job_failed",
        "message": message,
        "reason": reason or "failed",
        "job_name": worker_item.get("job_name"),
        "worker_class": worker_item.get("worker_class"),
        "queue": worker_item.get("queue"),
        "job_status": worker_item.get("job_status"),
        "kubernetes_evidence": {"items": [worker_item]},
    }
    if "exitCode" in failure:
        error_dict["exitCode"] = failure.get("exitCode")
    return error_dict


async def finalize_import_run(run_id: str, finalize_payload: dict[str, Any]) -> dict[str, Any] | None:
    """Dispatch and record the importer-specific finalization phase."""

    current_run = await get_import_run(run_id)
    if not current_run:
        return None
    importer = str(current_run.get("importer") or "").strip()
    if importer not in _FINISH_IMPORTERS:
        raise ValueError(f"importer does not support finalize: {importer}")
    if current_run.get("status") in TERMINAL_STATUSES:
        return current_run

    finish_params = _finish_params_for(importer, current_run, finalize_payload)
    finish_fn = _finish_function(importer)
    finalize_result = await finish_fn(**finish_params)
    now = utc_now()
    run_metrics_by_name = dict(current_run.get("metrics") or {})
    run_metrics_by_name["finalize"] = (
        finalize_result if isinstance(finalize_result, dict) else {"queued": True}
    )
    await db.execute(
        update(ImportRun)
        .where(ImportRun.run_id == run_id)
        .values(
            status="finalizing",
            phase_detail="finalize enqueued",
            heartbeat_at=now,
            progress={"unit": "run", "total": 1, "done": 0, "pct": 0, "message": "finalizing"},
            metrics=run_metrics_by_name,
            import_id=finish_params.get("import_id"),
        )
    )
    return await get_import_run(run_id)


async def find_active_run_by_idempotency_key(idempotency_key: str) -> dict[str, Any] | None:
    """Find the active run that owns an idempotency key."""

    result = await db.execute(
        select(ImportRun)
        .where(ImportRun.idempotency_key == idempotency_key)
        .where(ImportRun.status.in_(ACTIVE_STATUSES))
        .limit(1)
    )
    row = result.scalar_one_or_none()
    return normalize_run(row) if row else None


async def find_active_run_by_importer(importer: str) -> dict[str, Any] | None:
    """Return the earliest active run for an importer."""

    result = await db.execute(
        select(ImportRun)
        .where(ImportRun.importer == importer)
        .where(ImportRun.status.in_(ACTIVE_STATUSES))
        .order_by(ImportRun.created_at.asc())
        .limit(1)
    )
    row = result.scalar_one_or_none()
    return normalize_run(row) if row else None


async def find_active_runs_by_importer(importer: str) -> list[dict[str, Any]]:
    """Return every active run for an importer in admission order."""
    result = await db.execute(
        select(ImportRun)
        .where(ImportRun.importer == importer)
        .where(ImportRun.status.in_(ACTIVE_STATUSES))
        .order_by(ImportRun.created_at.asc())
    )
    return [normalize_run(row) for row in result.scalars().all()]


_PROVIDER_DIRECTORY_ADMISSION_LOCK_KEY = "import-run-admission:provider-directory-fhir"
_PROVIDER_DIRECTORY_ACQUISITION = "acquisition"
_PROVIDER_DIRECTORY_SCOPED_ARTIFACT = "scoped_artifact"
_PROVIDER_DIRECTORY_EXCLUSIVE = "exclusive"


def _normalize_connection_run(connection_row: Any) -> dict[str, Any]:
    mapping = getattr(connection_row, "_mapping", None)
    return normalize_run(dict(mapping) if mapping is not None else connection_row)


async def _active_idempotency_run(connection: Any, idempotency_key: str) -> dict[str, Any] | None:
    statement = (
        select(ImportRun.__table__)
        .where(ImportRun.idempotency_key == idempotency_key)
        .where(ImportRun.status.in_(ACTIVE_STATUSES))
        .limit(1)
    )
    active_rows = await connection.all(statement)
    return _normalize_connection_run(active_rows[0]) if active_rows else None


async def _provider_directory_retry_child(connection: Any, retry_of_run_id: str) -> dict[str, Any] | None:
    statement = (
        select(ImportRun.__table__)
        .where(ImportRun.importer == "provider-directory-fhir")
        .where(ImportRun.retry_of_run_id == retry_of_run_id)
        .limit(1)
    )
    child_rows = await connection.all(statement)
    return _normalize_connection_run(child_rows[0]) if child_rows else None


async def _active_importer_runs(connection: Any, importer: str) -> list[dict[str, Any]]:
    statement = (
        select(ImportRun.__table__)
        .where(ImportRun.importer == importer)
        .where(ImportRun.status.in_(ACTIVE_STATUSES))
        .order_by(ImportRun.created_at.asc())
    )
    active_rows = await connection.all(statement)
    return [_normalize_connection_run(active_row) for active_row in active_rows]


def _canonical_provider_directory_endpoint_scope(value: Any) -> str | None:
    from urllib.parse import urlsplit

    raw_scope = str(value or "").strip().rstrip("/")
    if not raw_scope:
        return None
    parsed_scope = urlsplit(raw_scope)
    if (
        parsed_scope.scheme != "https"
        or not parsed_scope.netloc
        or parsed_scope.username
        or parsed_scope.password
        or parsed_scope.query
        or parsed_scope.fragment
    ):
        return None
    canonical_scope = f"https://{parsed_scope.netloc.lower()}{parsed_scope.path.rstrip('/')}"
    return canonical_scope if raw_scope == canonical_scope else None


def _provider_directory_acquisition_scope(
    params: dict[str, Any],
    metrics: dict[str, Any] | None = None,
) -> tuple[frozenset[str], str] | None:
    if params.get("import_resources") is not True:
        return None
    exclusive_flags = (
        "stale_cleanup",
        "publish_artifacts",
        "publish_after_acquisition",
        "publish_corroboration",
    )
    if any(params.get(flag_name) is not False for flag_name in exclusive_flags):
        return None
    incompatible_modes = (
        "canonical_backfill_only",
        "contact_backfill_only",
        "publish_artifacts_only",
        "seed_only",
    )
    if any(params.get(flag_name) for flag_name in incompatible_modes):
        return None
    try:
        source_concurrency = int(params.get("source_concurrency") or 1)
    except (TypeError, ValueError):
        return None
    if source_concurrency != 1:
        return None
    source_ids = _provider_directory_source_ids(params.get("source_ids"))
    if source_ids is None:
        return None
    raw_endpoint_scope = str(params.get("provider_directory_endpoint_scope") or "").strip()
    endpoint_scopes = set()
    if raw_endpoint_scope:
        endpoint_scope = _canonical_provider_directory_endpoint_scope(raw_endpoint_scope)
        if endpoint_scope is None:
            return None
        endpoint_scopes.add(endpoint_scope)
    else:
        active_groups = (metrics or {}).get("active_source_groups")
        if isinstance(active_groups, list):
            for active_group in active_groups:
                if not isinstance(active_group, dict) or not active_group.get("api_base"):
                    continue
                endpoint_scope = _canonical_provider_directory_endpoint_scope(active_group["api_base"])
                if endpoint_scope is None:
                    return None
                endpoint_scopes.add(endpoint_scope)
    if len(endpoint_scopes) != 1:
        return None
    return source_ids, endpoint_scopes.pop()


def _provider_directory_source_ids(source_values: Any) -> frozenset[str] | None:
    if not isinstance(source_values, list) or not source_values:
        return None
    if any(not isinstance(source_id, str) or not source_id.strip() for source_id in source_values):
        return None
    source_ids = frozenset(source_id.strip() for source_id in source_values)
    return source_ids if len(source_ids) == len(source_values) else None


def _provider_directory_artifact_scope(params: dict[str, Any]) -> frozenset[str] | None:
    if params.get("publish_artifacts_only") is not True:
        return None
    incompatible_flags = (
        "import_resources",
        "canonical_backfill_only",
        "contact_backfill_only",
        "refresh_preset",
        "seed_only",
        "stale_cleanup",
        "publish_after_acquisition",
        "publish_artifacts",
    )
    if any(params.get(flag_name) for flag_name in incompatible_flags):
        return None
    return _provider_directory_source_ids(params.get("source_ids"))


def _provider_directory_operation(
    params: dict[str, Any],
    metrics: dict[str, Any] | None = None,
) -> tuple[str, frozenset[str], str | None]:
    acquisition_scope = _provider_directory_acquisition_scope(params, metrics)
    if acquisition_scope is not None:
        source_ids, endpoint_scope = acquisition_scope
        return _PROVIDER_DIRECTORY_ACQUISITION, source_ids, endpoint_scope
    artifact_source_ids = _provider_directory_artifact_scope(params)
    if artifact_source_ids is not None:
        return _PROVIDER_DIRECTORY_SCOPED_ARTIFACT, artifact_source_ids, None
    return _PROVIDER_DIRECTORY_EXCLUSIVE, frozenset(), None


def _provider_directory_blocking_run(
    params: dict[str, Any],
    active_runs: list[dict[str, Any]],
) -> dict[str, Any] | None:
    if not active_runs:
        return None
    requested_kind, requested_source_ids, requested_endpoint = _provider_directory_operation(params)
    if requested_kind == _PROVIDER_DIRECTORY_EXCLUSIVE:
        return active_runs[0]
    classified_active_runs = []
    for active_run in active_runs:
        active_params = active_run.get("params")
        if not isinstance(active_params, dict):
            return active_run
        active_metrics = active_run.get("metrics")
        if not isinstance(active_metrics, dict):
            active_metrics = None
        active_operation = _provider_directory_operation(active_params, active_metrics)
        if active_operation[0] == _PROVIDER_DIRECTORY_EXCLUSIVE:
            return active_run
        classified_active_runs.append((active_run, active_operation))

    active_acquisitions = [
        (active_run, operation)
        for active_run, operation in classified_active_runs
        if operation[0] == _PROVIDER_DIRECTORY_ACQUISITION
    ]
    if (
        requested_kind == _PROVIDER_DIRECTORY_ACQUISITION
        and len(active_acquisitions) >= _provider_directory_max_active()
    ):
        return active_acquisitions[0][0]

    for active_run, (active_kind, active_source_ids, active_endpoint) in classified_active_runs:
        if requested_kind == _PROVIDER_DIRECTORY_SCOPED_ARTIFACT:
            if active_kind == _PROVIDER_DIRECTORY_SCOPED_ARTIFACT:
                return active_run
            if not requested_source_ids.isdisjoint(active_source_ids):
                return active_run
            continue
        if active_kind == _PROVIDER_DIRECTORY_SCOPED_ARTIFACT:
            if not requested_source_ids.isdisjoint(active_source_ids):
                return active_run
            continue
        if not requested_source_ids.isdisjoint(active_source_ids) or requested_endpoint == active_endpoint:
            return active_run
    return None


def _provider_directory_max_active() -> int:
    raw_limit = os.getenv("HLTHPRT_PROVIDER_DIRECTORY_MAX_ACTIVE", "").strip()
    try:
        configured_limit = int(raw_limit) if raw_limit else None
    except ValueError:
        configured_limit = None
    return configured_limit if configured_limit is not None and configured_limit > 0 else 2


def _allows_parallel_active_importer_runs(
    importer: str,
    payload: dict[str, Any],
    idempotency_key: str | None,
) -> bool:
    if importer != "ptg":
        return False
    source_file_import_id = str(payload.get("source_file_import_id") or "").strip()
    return bool(source_file_import_id and idempotency_key)


def _normalize_triggered_by(value: Any) -> str:
    triggered_by = str(value or "api").strip() or "api"
    return triggered_by[:MAX_TRIGGERED_BY_LENGTH].rstrip("-_:. ") or "api"


async def _admit_provider_directory_run(import_row: dict[str, Any]) -> dict[str, Any] | None:
    async with db.acquire() as connection:
        await connection.scalar(
            text("SELECT pg_advisory_xact_lock(hashtextextended(:lock_key, 0))"),
            lock_key=_PROVIDER_DIRECTORY_ADMISSION_LOCK_KEY,
        )
        retry_of_run_id = import_row.get("retry_of_run_id")
        if retry_of_run_id:
            retry_child = await _provider_directory_retry_child(connection, str(retry_of_run_id))
            if retry_child:
                return retry_child
        idempotency_key = import_row.get("idempotency_key")
        if idempotency_key:
            active_run = await _active_idempotency_run(connection, str(idempotency_key))
            if active_run:
                return active_run
        active_runs = await _active_importer_runs(connection, "provider-directory-fhir")
        blocking_run = _provider_directory_blocking_run(import_row["params"], active_runs)
        if blocking_run:
            return blocking_run
        await connection.status(insert(ImportRun).values(**import_row))
    return None


async def create_import_run(
    request_payload_map: dict[str, Any],
) -> tuple[dict[str, Any], bool]:
    """Create and enqueue an import run unless admission deduplicates it."""

    importer = str(request_payload_map.get("importer") or "").strip()
    if importer not in importer_names():
        raise ValueError(f"unknown importer: {importer}")

    idempotency_key = (
        str(request_payload_map.get("idempotency_key") or "").strip() or None
    )
    if idempotency_key and importer != "provider-directory-fhir":
        active = await find_active_run_by_idempotency_key(idempotency_key)
        if active:
            return active, False
    if importer != "provider-directory-fhir" and not _allows_parallel_active_importer_runs(
        importer,
        request_payload_map,
        idempotency_key,
    ):
        active_importer = await find_active_run_by_importer(importer)
        if active_importer:
            return active_importer, False

    now = utc_now()
    run_id = str(request_payload_map.get("run_id") or "").strip() or _new_run_id()
    retry_of_run_id = (
        str(request_payload_map.get("retry_of_run_id") or "").strip() or None
    )
    import_run_values_by_name = {
        "run_id": run_id,
        "engine": ENGINE_NAME,
        "node_id": os.getenv("HLTHPRT_IMPORT_NODE_ID"),
        "importer": importer,
        "family": _importer_family(importer),
        "status": "queued",
        "phase_detail": "created",
        "params": (
            request_payload_map.get("params")
            if isinstance(request_payload_map.get("params"), dict)
            else {}
        ),
        "idempotency_key": idempotency_key,
        "triggered_by": _normalize_triggered_by(
            request_payload_map.get("triggered_by")
        ),
        "schedule_id": request_payload_map.get("schedule_id"),
        "subscription_id": request_payload_map.get("subscription_id"),
        "source_file_import_id": request_payload_map.get(
            "source_file_import_id"
        ),
        "created_at": now,
        "heartbeat_at": now,
        "progress": {"unit": "run", "total": 1, "done": 0, "pct": 0, "message": "queued"},
        "metrics": {},
        "error": None,
        "snapshot_id": None,
        "import_id": request_payload_map.get("import_id"),
        "retry_of_run_id": retry_of_run_id,
    }
    try:
        if importer == "provider-directory-fhir":
            blocking_run = await _admit_provider_directory_run(
                import_run_values_by_name
            )
            if blocking_run:
                return blocking_run, False
        else:
            await db.execute(
                insert(ImportRun).values(**import_run_values_by_name)
            )
    except IntegrityError:
        if idempotency_key:
            active = await find_active_run_by_idempotency_key(idempotency_key)
            if active:
                return active, False
        raise
    enqueue_result = await _enqueue_import_start(import_run_values_by_name)
    import_run_values_by_name.update(enqueue_result)
    await db.execute(
        update(ImportRun)
        .where(ImportRun.run_id == run_id)
        .values(
            status=import_run_values_by_name["status"],
            phase_detail=import_run_values_by_name["phase_detail"],
            heartbeat_at=import_run_values_by_name["heartbeat_at"],
            progress=import_run_values_by_name["progress"],
            metrics=import_run_values_by_name["metrics"],
            error=import_run_values_by_name["error"],
        )
    )
    import_run_values_by_name = _serialize_run_timestamps(
        import_run_values_by_name
    )
    enqueue_status_event(import_run_values_by_name)
    _write_run_live_progress(import_run_values_by_name, publish_event=False)
    return import_run_values_by_name, True


async def _enqueue_import_start(
    import_run_values_by_name: dict[str, Any],
) -> dict[str, Any]:
    """Enqueue one importer start job and return its run-state update."""

    importer = str(import_run_values_by_name.get("importer") or "")
    now = utc_now()
    params = (
        import_run_values_by_name.get("params")
        if isinstance(import_run_values_by_name.get("params"), dict)
        else {}
    )
    try:
        adapter = _adapter_for_import_row(import_run_values_by_name)
    except ValueError as exc:
        return {
            "status": "failed",
            "phase_detail": "enqueue failed",
            "heartbeat_at": now,
            "progress": {"unit": "run", "total": 1, "done": 0, "pct": 0, "message": "enqueue failed"},
            "metrics": {"enqueue_adapter": "arq_single_job", **_ptg_lane_metrics(params)},
            "error": {"code": "invalid_enqueue_adapter", "message": str(exc)},
        }
    if adapter is None:
        return {
            "status": "queued",
            "phase_detail": "created; enqueue adapter pending",
            "heartbeat_at": now,
            "progress": {"unit": "run", "total": 1, "done": 0, "pct": 0, "message": "queued; enqueue adapter pending"},
            "metrics": {"enqueue_adapter": "pending"},
            "error": None,
        }

    job_payload = _adapter_payload(
        adapter,
        import_run_values_by_name,
        params,
    )
    enqueue_options_by_name = {"_queue_name": adapter["queue"]}
    if adapter.get("job_prefix"):
        enqueue_options_by_name["_job_id"] = (
            f"{adapter['job_prefix']}_{import_run_values_by_name['run_id']}"
        )
    try:
        redis = await create_pool(
            build_redis_settings(),
            job_serializer=serialize_job,
            job_deserializer=deserialize_job,
        )
        job = await redis.enqueue_job(
            adapter["function"],
            job_payload,
            **enqueue_options_by_name,
        )
    except Exception as exc:
        return {
            "status": "failed",
            "phase_detail": "enqueue failed",
            "heartbeat_at": utc_now(),
            "progress": {"unit": "run", "total": 1, "done": 0, "pct": 0, "message": "enqueue failed"},
            "metrics": {
                "enqueue_adapter": "arq_single_job",
                "queue": adapter["queue"],
                "function": adapter["function"],
                **_ptg_lane_metrics(params),
            },
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
            **_ptg_lane_metrics(params),
        },
        "error": None,
    }


def _adapter_for_import_row(row: dict[str, Any]) -> dict[str, Any] | None:
    importer = str(row.get("importer") or "")
    adapter = _SINGLE_JOB_ADAPTERS.get(importer)
    if adapter is None or importer != "ptg":
        return adapter
    params = row.get("params") if isinstance(row.get("params"), dict) else {}
    queue = str(params.get("_expected_queue") or "").strip()
    if not queue:
        return adapter
    if queue not in _PTG_CONTROL_QUEUES:
        raise ValueError(f"unsupported PTG queue: {queue}")
    return {**adapter, "queue": queue}


def _ptg_lane_metrics(params: dict[str, Any]) -> dict[str, Any]:
    queue = str(params.get("_expected_queue") or "").strip()
    worker_class = str(params.get("_expected_worker_class") or "").strip()
    resource_class = str(params.get("resource_class") or params.get("_resource_class") or "").strip()
    return {
        key: value
        for key, value in {
            "queue": queue,
            "worker_class": worker_class,
            "resource_class": resource_class,
        }.items()
        if value
    }


_TASK_LINEAGE_FIELDS_BY_IMPORTER = {
    "mrf-source-discovery": ("retry_of_run_id",),
    "provider-directory-fhir": ("retry_of_run_id",),
}


def _adapter_payload(
    adapter: dict[str, Any],
    import_run_values_by_name: dict[str, Any],
    params: dict[str, Any],
) -> dict[str, Any]:
    """Build the ARQ payload for one normalized import-run row."""

    test_mode = bool(params.get("test_mode", params.get("test", False)))
    payload_kind = adapter["payload"]
    if payload_kind == "test_mode":
        return _test_mode_adapter_payload(
            import_run_values_by_name,
            params,
            test_mode=test_mode,
        )
    if payload_kind in {"control_wrapped", "control_wrapped_kwargs"}:
        return _control_wrapped_adapter_payload(
            adapter,
            import_run_values_by_name,
            params,
            test_mode=test_mode,
        )
    if payload_kind == "run_import":
        return _run_import_adapter_payload(
            import_run_values_by_name,
            params,
            test_mode=test_mode,
        )
    if payload_kind == "ptg_control":
        return {
            "run_id": import_run_values_by_name["run_id"],
            "source_file_import_id": import_run_values_by_name.get(
                "source_file_import_id"
            ),
            "params": dict(params),
        }
    return dict(params)


def _test_mode_adapter_payload(
    import_run_values_by_name: dict[str, Any],
    params: dict[str, Any],
    *,
    test_mode: bool,
) -> dict[str, Any]:
    """Build a legacy test-mode payload with optional MRF chunk controls."""

    job_payload_map = {
        "test_mode": test_mode,
        "run_id": import_run_values_by_name["run_id"],
    }
    for key in (
        "mrf_file_chunking",
        "mrf_chunk_target_bytes",
        "mrf_chunk_target_mb",
        "mrf_chunk_min_bytes",
        "mrf_chunk_min_mb",
    ):
        if key in params:
            job_payload_map[key] = params[key]
    return job_payload_map


def _control_wrapped_adapter_payload(
    adapter: dict[str, Any],
    import_run_values_by_name: dict[str, Any],
    params: dict[str, Any],
    *,
    test_mode: bool,
) -> dict[str, Any]:
    """Build a control-wrapped task payload with retry lineage."""

    task_payload_map = {"test_mode": test_mode, **params}
    task_lineage_fields = _TASK_LINEAGE_FIELDS_BY_IMPORTER.get(
        str(import_run_values_by_name.get("importer") or ""),
        (),
    )
    for field in task_lineage_fields:
        if import_run_values_by_name.get(field):
            task_payload_map[field] = import_run_values_by_name[field]
    return {
        "run_id": import_run_values_by_name["run_id"],
        "importer": import_run_values_by_name.get("importer"),
        "family": import_run_values_by_name.get("family"),
        "target_module": adapter["target_module"],
        "target_function": adapter["target_function"],
        "call_style": (
            "kwargs"
            if adapter["payload"] == "control_wrapped_kwargs"
            else "ctx_task"
        ),
        "run_shutdown": bool(adapter.get("run_shutdown")),
        "task": task_payload_map,
    }


def _run_import_adapter_payload(
    import_run_values_by_name: dict[str, Any],
    params: dict[str, Any],
    *,
    test_mode: bool,
) -> dict[str, Any]:
    """Build an importer payload with optional artifact bounds."""

    job_payload_map = {
        "run_id": import_run_values_by_name["run_id"],
        "import_id": params.get("import_id")
        or import_run_values_by_name.get("import_id"),
        "test_mode": test_mode,
    }
    for key in ("artifacts", "source_urls", "max_records", "max_files"):
        if key in params:
            job_payload_map[key] = params[key]
    return job_payload_map


async def request_cancel(run_id: str) -> dict[str, Any] | None:
    """Mark an active run for cancellation and signal its worker."""

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
    run_metrics_by_name = dict(current_metrics)
    is_pending_adapter = (
        current.get("status") == "queued"
        and run_metrics_by_name.get("enqueue_adapter") == "pending"
    )
    is_queued_arq = (
        current.get("status") == "queued"
        and run_metrics_by_name.get("enqueue_adapter") == "arq_single_job"
    )
    worker_cancel_signal_map = await _cancel_signal_for_run(
        current,
        run_id=run_id,
        is_pending_adapter=is_pending_adapter,
        is_queued_arq=is_queued_arq,
    )
    run_metrics_by_name["cancel_signal"] = worker_cancel_signal_map
    has_terminalized_active_worker = (
        _has_terminalized_active_worker_cancel_signal(worker_cancel_signal_map)
    )
    canceled_before_start = is_pending_adapter or is_queued_arq
    cancel_state_by_name = _cancel_state_by_name(
        canceled_before_start=canceled_before_start,
        has_terminalized_active_worker=has_terminalized_active_worker,
        current_progress=current_progress,
    )
    return await _persist_cancel_request(
        run_id,
        current_run=current,
        requested_at=now,
        cancel_state_by_name=cancel_state_by_name,
        run_metrics_by_name=run_metrics_by_name,
    )


def _cancel_state_by_name(
    *,
    canceled_before_start: bool,
    has_terminalized_active_worker: bool,
    current_progress: dict[str, Any],
) -> dict[str, Any]:
    """Build persisted status and progress for a cancellation request."""

    canceled_now = canceled_before_start or has_terminalized_active_worker
    phase_detail = "cancel requested"
    if canceled_before_start:
        phase_detail = "canceled before start"
    elif has_terminalized_active_worker:
        phase_detail = "canceled active worker"
    return {
        "canceled_now": canceled_now,
        "status": "canceled" if canceled_now else "canceling",
        "phase_detail": phase_detail,
        "progress": _cancel_progress_by_name(
            canceled_now=canceled_now,
            current_progress=current_progress,
        ),
    }


async def _persist_cancel_request(
    run_id: str,
    *,
    current_run: dict[str, Any],
    requested_at: dt.datetime,
    cancel_state_by_name: dict[str, Any],
    run_metrics_by_name: dict[str, Any],
) -> dict[str, Any] | None:
    """Persist cancellation state and publish its live control event."""

    cancel_progress_by_name = cancel_state_by_name["progress"]
    canceled_now = bool(cancel_state_by_name["canceled_now"])
    await db.execute(
        update(ImportRun)
        .where(ImportRun.run_id == run_id)
        .values(
            status=cancel_state_by_name["status"],
            phase_detail=cancel_state_by_name["phase_detail"],
            heartbeat_at=requested_at,
            finished_at=(
                requested_at if canceled_now else current_run.get("finished_at")
            ),
            progress=cancel_progress_by_name,
            metrics=run_metrics_by_name,
        )
    )
    updated = await get_import_run(run_id)
    if updated:
        _write_run_live_progress(
            {**updated, "progress": cancel_progress_by_name},
            publish_event=False,
        )
        enqueue_status_event(
            {
                **updated,
                "progress": cancel_progress_by_name,
                "metrics": run_metrics_by_name,
            }
        )
    return updated


async def _cancel_signal_for_run(
    current_run: dict[str, Any],
    *,
    run_id: str,
    is_pending_adapter: bool,
    is_queued_arq: bool,
) -> dict[str, Any]:
    """Signal queued or active work through its configured control path."""

    if is_pending_adapter:
        return {"redis": False, "pending_adapter": True}
    if is_queued_arq:
        return await _remove_queued_job(current_run)
    worker_cancel_signal_map = await _set_cancel_flag(run_id)
    worker_cancel_signal_map["kubernetes"] = await _delete_active_worker_jobs(
        current_run
    )
    return worker_cancel_signal_map


def _cancel_progress_by_name(
    *,
    canceled_now: bool,
    current_progress: dict[str, Any],
) -> dict[str, Any]:
    """Return terminal or in-progress cancellation progress."""

    return {
        "unit": "run",
        "total": 1,
        "done": 1 if canceled_now else 0,
        "pct": 100 if canceled_now else current_progress.get("pct", 0),
        "message": "canceled" if canceled_now else "cancel requested",
    }


def _has_terminalized_active_worker_cancel_signal(cancel_signal: dict[str, Any]) -> bool:
    kubernetes = cancel_signal.get("kubernetes") if isinstance(cancel_signal, dict) else None
    if not isinstance(kubernetes, dict) or not kubernetes.get("enabled"):
        return False
    if kubernetes.get("errors"):
        return False
    try:
        deleted = int(kubernetes.get("deleted") or 0)
    except (TypeError, ValueError):
        deleted = 0
    if deleted > 0:
        return True
    items = kubernetes.get("items")
    if not isinstance(items, list) or not items:
        return False
    return all(
        isinstance(item, dict) and not item.get("deleted") and item.get("reason") == "terminal"
        for item in items
    )


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
    except Exception as exc:
        return {"redis": False, "error": str(exc)}


async def _delete_active_worker_jobs(run: dict[str, Any]) -> dict[str, Any]:
    payload = _active_worker_cancel_payload(run)
    try:
        from api.control_workers import delete_kubernetes_worker_jobs

        return await asyncio.to_thread(delete_kubernetes_worker_jobs, payload)
    except Exception as exc:
        return {"enabled": False, "deleted": 0, "error": str(exc)}


def _active_worker_cancel_payload(run: dict[str, Any]) -> dict[str, Any]:
    params = run.get("params") if isinstance(run.get("params"), dict) else {}
    metrics = run.get("metrics") if isinstance(run.get("metrics"), dict) else {}
    payload = {
        "run_id": run.get("run_id"),
        "importer": run.get("importer"),
        "status": run.get("status"),
        "import_id": run.get("import_id") or params.get("import_id"),
        "queue": metrics.get("queue") or params.get("_expected_queue"),
        "worker_class": metrics.get("worker_class") or params.get("_expected_worker_class"),
        "resource_class": params.get("resource_class") or params.get("_resource_class"),
    }
    return {key: value for key, value in payload.items() if value not in (None, "")}


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
        deleted = int(
            await redis.delete(
                f"arq:job:{job_id}",
                f"arq:retry:{job_id}",
                f"arq:result:{job_id}",
            )
            or 0
        )
        return {
            "redis": True,
            "queue": queue,
            "job_id": job_id,
            "removed": removed > 0,
            "deleted_job_key": deleted > 0,
            "deleted_keys": deleted,
        }
    except Exception as exc:
        return {"redis": False, "removed": False, "error": str(exc), "queue": queue, "job_id": job_id}


def _retry_child_params(
    current_run_map: dict[str, Any],
    run_id: str,
    retry_params_by_name: dict[str, Any],
) -> dict[str, Any]:
    current_params_by_name = (
        current_run_map.get("params")
        if isinstance(current_run_map.get("params"), dict)
        else {}
    )
    child_params_by_name = {**current_params_by_name, **retry_params_by_name}
    if current_run_map.get("importer") == "mrf-source-discovery":
        root_run_id = str(
            current_params_by_name.get("mrf_discovery_root_run_id")
            or retry_params_by_name.get("mrf_discovery_root_run_id")
            or run_id
        ).strip()
        child_params_by_name["retry_of_run_id"] = run_id
        child_params_by_name["mrf_discovery_root_run_id"] = root_run_id
        return child_params_by_name
    if current_run_map.get("importer") != "provider-directory-fhir":
        return child_params_by_name
    root_run_id = str(
        current_params_by_name.get("provider_directory_pagination_root_run_id")
        or retry_params_by_name.get("provider_directory_pagination_root_run_id")
        or run_id
    ).strip()
    child_params_by_name["retry_of_run_id"] = run_id
    child_params_by_name["provider_directory_pagination_root_run_id"] = root_run_id
    return child_params_by_name


async def retry_import_run(run_id: str, payload: dict[str, Any]) -> tuple[dict[str, Any], bool] | None:
    """Create a retry derived from an existing import run."""

    current = await get_import_run(run_id)
    if not current:
        return None
    retry_params = payload.get("retry_params") if isinstance(payload.get("retry_params"), dict) else {}
    child_run_payload_map = {
        "importer": current["importer"],
        "params": _retry_child_params(current, run_id, retry_params),
        "triggered_by": payload.get("triggered_by") or "api",
        "idempotency_key": payload.get("idempotency_key"),
        "schedule_id": current.get("schedule_id"),
        "subscription_id": current.get("subscription_id"),
        "source_file_import_id": current.get("source_file_import_id"),
        "import_id": current.get("import_id"),
        "retry_of_run_id": run_id,
    }
    return await create_import_run(child_run_payload_map)
