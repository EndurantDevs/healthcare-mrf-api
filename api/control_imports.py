# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import base64
import datetime as dt
import hashlib
import json
import logging
import os
import shutil
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import click
import redis
from arq import create_pool
from arq.constants import (
    in_progress_key_prefix,
    job_key_prefix,
    result_key_prefix,
    retry_key_prefix,
)
from arq.jobs import deserialize_job as arq_deserialize_job
from redis.exceptions import WatchError
from sqlalchemy import and_, insert, or_, select, text, update
from sqlalchemy.exc import IntegrityError

from process.provider_directory_profile_selection import (
    ProviderDirectoryProfileSelectionError,
    validated_profile_execution,
)
from api.control_workers import exact_worker_presence
from process.hospital_hpt_registry import selected_hospital_hpt_registry
from process.hospital_price_runtime import (
    configured_resource_limits,
    hospital_price_artifact_store,
    locator_groups,
)
from process.provider_directory_fhir_census_contract import (
    ProviderDirectoryFHIRAcquisitionStrategy,
)
from process.provider_directory_refresh_preset import (
    apply_provider_directory_refresh_preset,
)
from process.provider_directory_validated_publication_contract import (
    validated_publication_candidate_from_params,
)
from process.uhc_provider_file_admission import (
    validate_uhc_official_file_admission,
)
from process.ptg_parts.frozen_rate_binding import (
    FROZEN_RATE_FILE_PROTECTED_FIELDS,
    normalize_protected_frozen_rate_params,
    protected_frozen_tuple_presence,
)
from process.ptg_parts.frozen_rate_binding_store import (
    insert_or_compare_frozen_binding,
    recheck_frozen_binding_on_connection,
)
from process.ptg_parts.frozen_rate_privacy import (
    frozen_private_scalar_values,
    has_frozen_private_evidence,
    redact_frozen_public_values,
)
from process.ptg_parts.ptg_source_attempt_actions import (
    record_source_attempt_event,
)
from process.ptg_parts.ptg_source_attempt_guard import (
    guard_source_attempt,
    require_source_attempt_capabilities,
    source_file_import_id_from_payload,
)
from process.ptg_parts.ptg_wave_admission_fence import (
    PTG_WAVE_FENCED_IMPORTERS,
    acquire_ptg_admission_lock,
    is_ptg_wave_owned_run,
    require_no_capacity_owning_wave,
    require_not_wave_owned_run,
)
from process.ptg_singleton_direct_control import (
    DIRECT_RATE_FILE_INTENT_SHA256_FIELD,
    DIRECT_RATE_FILE_PROTECTED_FIELDS,
    DIRECT_RATE_FILE_PUBLIC_MARKER,
    protected_singleton_direct_presence,
)

from db.models import ImportRun, PTG2ImportRun, PTG2Snapshot, db
from process.import_status_events import enqueue_status_event, isoformat_utc
from process.control_lifecycle import acquire_control_run_worker_action_lock
from process.live_progress import enqueue_live_progress, estimate_payload_from_live, progress_payload_from_live, read_live_progress
from process.ptg_allowed_amount_blank import (
    ALLOWED_AMOUNT_BLANK_ERROR,
    allowed_amount_blank_metrics,
)
from process.redis_config import build_redis_settings
from process.serialization import deserialize_job, serialize_job

ENGINE_NAME = "healthcare-mrf-api"
ACTIVE_STATUSES = {"queued", "starting", "running", "finalizing", "canceling"}
TERMINAL_STATUSES = {"succeeded", "failed", "canceled", "dead_letter"}
ALL_STATUS_IDEMPOTENCY_IMPORTERS = frozenset(
    {"plan-pricing-projection", "plan-pricing-prewarm"}
)
STALE_WORKER_RECONCILIATION_IMPORTERS = ALL_STATUS_IDEMPOTENCY_IMPORTERS
STALE_WORKER_RECONCILIATION_MIN_AGE_SECONDS = 60
_STALE_WORKER_RECONCILIATION_FIELDS = frozenset(
    {
        "expected_importer",
        "expected_status",
        "expected_heartbeat_at",
        "expected_attempt_id",
        "expected_attempt_started_at",
    }
)
CANCEL_FLAG_TTL_SECONDS = 7 * 24 * 60 * 60
MAX_IMPORT_RUN_LIST_LIMIT = 200
MAX_TRIGGERED_BY_LENGTH = 32
_IMPORT_RUN_ENSURE_LOCK = asyncio.Lock()
_IMPORT_RUN_ADVISORY_LOCK_KEY = 44_706_101_200_001
logger = logging.getLogger(__name__)


class StaleWorkerReconciliationConflict(RuntimeError):
    """The exact active worker identity changed or still has live residue."""


class StaleWorkerReconciliationUnavailable(RuntimeError):
    """The worker absence proof could not be completed."""


@dataclass
class _ImportRunEnsureState:
    ensured: bool = False


_IMPORT_RUN_ENSURE_STATE = _ImportRunEnsureState()

_IMPORTER_DEPENDENCIES: dict[str, list[str]] = {
    "npi": ["nucc"],
    "florida-mqa-profile": ["npi"],
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
    "plan-pricing-projection": {
        "queue": "arq:PTGCandidateAudit",
        "function": "control_single_job_start",
        "payload": "control_wrapped_kwargs",
        "target_module": "api.plan_pricing_projection",
        "target_function": "build_plan_pricing_projection",
        "job_prefix": "plan_pricing_projection",
    },
    "plan-pricing-prewarm": {
        "queue": "arq:PTGCandidateAudit",
        "function": "control_single_job_start",
        "payload": "control_wrapped_kwargs",
        "target_module": "api.plan_pricing_prewarm",
        "target_function": "prewarm_plan_pricing",
        "job_prefix": "plan_pricing_prewarm",
    },
    "mrf": {"queue": "arq:MRF", "function": "init_file", "payload": "test_mode"},
    "npi": {
        "queue": "arq:NPI",
        "function": "control_single_job_start",
        "payload": "control_wrapped",
        "target_module": "process.npi",
        "target_function": "process_data",
        "run_shutdown": True,
        "job_prefix": "npi_start",
    },
    "nucc": {
        "queue": "arq:NUCC",
        "function": "control_single_job_start",
        "payload": "control_wrapped",
        "target_module": "process.nucc",
        "target_function": "process_data",
        "run_shutdown": True,
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
        "run_shutdown": True,
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
        "run_shutdown": True,
    },
    "provider-directory-fhir": {
        "queue": "arq:ProviderDirectoryFHIR",
        "function": "control_single_job_start",
        "payload": "control_wrapped",
        "target_module": "process.provider_directory_fhir",
        "target_function": "process_data",
        "run_shutdown": True,
        "job_prefix": "provider_directory_start",
    },
    "florida-mqa-profile": {
        "queue": "arq:FloridaMQAProfile",
        "function": "control_single_job_start",
        "payload": "control_wrapped_kwargs",
        "target_module": "process.florida_mqa_profile",
        "target_function": "process_data",
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
    "address-formatted-address": {
        "queue": "arq:AddressArchive",
        "function": "control_single_job_start",
        "payload": "control_wrapped",
        "target_module": "process.address_formatted_address",
        "target_function": "process_address_formatted_address",
    },
    "address-numeric-grid-alias": {
        "queue": "arq:AddressArchive",
        "function": "control_single_job_start",
        "payload": "control_wrapped",
        "target_module": "process.address_numeric_grid_alias_worker",
        "target_function": "process_data",
    },
    "address-strict-source-backfill": {
        "queue": "arq:AddressArchive",
        "function": "control_single_job_start",
        "payload": "control_wrapped",
        "target_module": "process.address_numeric_grid_alias_worker",
        "target_function": "process_address_strict_source_backfill",
    },
    "address-numeric-grid-alias-revoke": {
        "queue": "arq:AddressArchive",
        "function": "control_single_job_start",
        "payload": "control_wrapped",
        "target_module": "process.address_numeric_grid_alias_worker",
        "target_function": "process_address_numeric_grid_alias_revoke",
    },
    "openaddresses": {
        "queue": "arq:OpenAddresses",
        "function": "control_single_job_start",
        "payload": "control_wrapped",
        "target_module": "process.openaddresses",
        "target_function": "process_data",
        "run_shutdown": True,
    },
    "hospital-prices": {
        "queue": "arq:HospitalPrices",
        "function": "control_single_job_start",
        "payload": "control_wrapped",
        "target_module": "process.hospital_prices",
        "target_function": "process_data",
        "job_prefix": "hospital_prices_start",
    },
}

_PTG_CONTROL_QUEUES = frozenset({"arq:PTG", "arq:PTGSmall", "arq:PTGNormal", "arq:PTGLarge", "arq:PTGHuge"})
_PTG_FULL_REBUILD_TOKEN_PARAM = "_full_rebuild_token"
_PTG_FULL_REBUILD_SCOPE_PARAM = "_full_rebuild_scope_digest"
_PTG_FULL_REBUILD_MARKER_PARAM = "full_rebuild_requested"
_PTG_EXACT_WAVE_INTERNAL_PARAMS = frozenset({
    "_wave_id",
    "_wave_digest",
    "_wave_job_id",
})
_PTG_FULL_REBUILD_SCOPE_DIGEST_DOMAIN = b"PTG2V3FULLREBUILDSCOPE\x01"
_EPHEMERAL_PARAM_NAMES_BY_IMPORTER = {
    "ptg": frozenset(
        {
            _PTG_FULL_REBUILD_TOKEN_PARAM,
            _PTG_FULL_REBUILD_SCOPE_PARAM,
        }
    ),
}
_PROVIDER_DIRECTORY_CURRENT_VERSION_CENSUS_STRATEGY = (
    ProviderDirectoryFHIRAcquisitionStrategy.CUTOFF_BOUNDED_CURRENT_VERSION_CENSUS.value
)
_PROVIDER_DIRECTORY_SERVER_ISSUED_SUBSET_STRATEGY = (
    ProviderDirectoryFHIRAcquisitionStrategy.SERVER_ISSUED_TRAVERSAL_SUBSET.value
)
_CONTROL_HIDDEN_PARAM_NAMES_BY_IMPORTER = {
    "provider-directory-fhir": frozenset(
        {
            "provider_directory_acquisition_strategy",
            "provider_directory_census_cutoff",
            "provider_directory_pagination_root_run_id",
            "restart_expired_current_census_slice",
            "retry_of_run_id",
        }
    ),
}

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
    "address-formatted-address",
    "address-numeric-grid-alias",
    "address-strict-source-backfill",
    "address-numeric-grid-alias-revoke",
    "clinical-reference",
    "ms-drg",
    "openaddresses",
    "hospital-prices",
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


def _control_param_schema(
    importer: str,
    command: click.Command,
) -> list[dict[str, Any]]:
    """Hide CLI-only controls from the authenticated import API catalog."""

    hidden_names = _CONTROL_HIDDEN_PARAM_NAMES_BY_IMPORTER.get(
        importer,
        frozenset(),
    )
    return [
        parameter
        for parameter in _param_schema(command)
        if parameter["name"] not in hidden_names
    ]


def _json_safe_default(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (list, tuple)):
        return [_json_safe_default(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_safe_default(item) for key, item in value.items()}
    return None


def _plan_pricing_projection_registry_entry() -> dict[str, Any]:
    return {
        "name": "plan-pricing-projection",
        "engine": ENGINE_NAME,
        "family": "mrf",
        "kind": "control",
        "lifecycle": "single",
        "schedulable": False,
        "cancelable": False,
        "retryable": True,
        "enqueue_adapter": "arq_single_job",
        "queue": "arq:PTGCandidateAudit",
        "depends_on": [],
        "params_schema": [
            {
                "name": "binding_manifest_digest",
                "opts": ["--binding-manifest-digest"],
                "required": True,
                "multiple": False,
                "is_flag": False,
                "type": "string",
                "default": None,
                "help": "Exact release binding-manifest digest.",
            },
            {
                "name": "bindings",
                "opts": ["--bindings"],
                "required": True,
                "multiple": False,
                "is_flag": False,
                "type": "array",
                "default": None,
                "help": "Exact release binding array.",
            },
        ],
    }


_PLAN_PRICING_PREWARM_PARAM_NAMES = frozenset(
    {"plan_release_id", "serving_revision_id", "projection_id"}
)


def _validate_plan_pricing_prewarm_params(
    importer: str,
    params_by_name: dict[str, Any],
) -> None:
    if importer != "plan-pricing-prewarm":
        return
    if set(params_by_name) != _PLAN_PRICING_PREWARM_PARAM_NAMES:
        raise ValueError(
            "plan-pricing-prewarm params must be exactly plan_release_id, "
            "serving_revision_id, and projection_id"
        )
    if any(
        type(params_by_name[name]) is not str
        or not params_by_name[name]
        or params_by_name[name] != params_by_name[name].strip()
        for name in _PLAN_PRICING_PREWARM_PARAM_NAMES
    ):
        raise ValueError("plan-pricing-prewarm params must be non-empty strings")


def _plan_pricing_prewarm_registry_entry() -> dict[str, Any]:
    parameter_help_by_name = {
        "plan_release_id": "Exact current immutable plan release ID.",
        "serving_revision_id": "Exact current serving revision ID.",
        "projection_id": "Exact ready pricing projection ID.",
    }
    return {
        "name": "plan-pricing-prewarm",
        "engine": ENGINE_NAME,
        "family": "mrf",
        "kind": "control",
        "lifecycle": "single",
        "schedulable": False,
        "cancelable": False,
        "retryable": True,
        "enqueue_adapter": "arq_single_job",
        "queue": "arq:PTGCandidateAudit",
        "depends_on": [],
        "params_schema": [
            {
                "name": name,
                "opts": ["--" + name.replace("_", "-")],
                "required": True,
                "multiple": False,
                "is_flag": False,
                "type": "string",
                "default": None,
                "help": parameter_help_by_name[name],
            }
            for name in (
                "plan_release_id",
                "serving_revision_id",
                "projection_id",
            )
        ],
    }


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
                "params_schema": _control_param_schema(name, command),
            }
        )
    importers.extend(
        (
            _plan_pricing_projection_registry_entry(),
            _plan_pricing_prewarm_registry_entry(),
        )
    )
    return sorted(importers, key=lambda importer: importer["name"])


def importer_names() -> set[str]:
    """Return every importer name accepted by the control API."""

    return {entry["name"] for entry in importer_registry()}


def _importer_family(importer: str) -> str:
    if importer in {
        "ptg", "ptg-candidate-audit", "plan-pricing-projection",
        "plan-pricing-prewarm", "mrf", "mrf-source-discovery",
        "hospital-prices",
    }:
        return "mrf"
    if importer in {"claims-pricing", "claims-procedures", "drug-claims"}:
        return "claims"
    if importer in {
        "npi",
        "nucc",
        "provider-quality",
        "provider-enrichment",
        "provider-directory-fhir",
        "florida-mqa-profile",
        "entity-address-unified",
        "cms-doctors",
        "address-archive-v2-migrate",
        "address-formatted-address",
        "address-numeric-grid-alias",
        "address-strict-source-backfill",
        "address-numeric-grid-alias-revoke",
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
    plan_by_identity: dict[tuple[str, ...], dict[str, Any]] = {}
    for catalog_entry in catalog_entry_list:
        domain = str(catalog_entry.get("domain") or "unknown")
        by_domain[domain] = by_domain.get(domain, 0) + 1
        for plan_details_by_field in catalog_entry.get("plan_info") or ():
            if not isinstance(plan_details_by_field, dict):
                continue
            plan_id = str(plan_details_by_field.get("plan_id") or "").strip()
            market_type = plan_details_by_field.get("plan_market_type")
            engine_plan_hash = str(
                plan_details_by_field.get("engine_plan_hash") or ""
            ).strip()
            if plan_id:
                plan_identity = (
                    ("engine_plan_hash", engine_plan_hash)
                    if engine_plan_hash
                    else ("legacy", plan_id, str(market_type or ""))
                )
                plan_by_identity[plan_identity] = plan_details_by_field
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


def normalize_run(import_run: Any) -> dict[str, Any]:
    """Convert an import-run model or mapping to its API representation."""

    if import_run is None:
        return {}
    if hasattr(import_run, "to_json_dict"):
        run_by_field = import_run.to_json_dict()
    elif isinstance(import_run, dict):
        run_by_field = dict(import_run)
    else:
        run_by_field = {
            field_name: getattr(import_run, field_name)
            for field_name in ImportRun.__table__.columns.keys()
            if hasattr(import_run, field_name)
        }
    has_private_frozen_evidence = has_frozen_private_evidence(run_by_field)
    private_frozen_values: frozenset[str] = frozenset()
    if isinstance(run_by_field.get("params"), dict):
        if (
            str(run_by_field.get("importer") or "") == "ptg"
            and has_private_frozen_evidence
        ):
            private_frozen_values = frozen_private_scalar_values(
                run_by_field["params"]
            )
        run_by_field["params"] = _params_for_import_run_response(
            str(run_by_field.get("importer") or ""),
            run_by_field["params"],
        )
    normalized_data = _overlay_live_progress(
        _serialize_run_timestamps(run_by_field)
    )
    return redact_frozen_public_values(
        normalized_data,
        private_frozen_values,
        strip_evidence=has_private_frozen_evidence,
    )


def _params_for_import_run_storage(
    importer: str,
    params_by_name: dict[str, Any],
) -> dict[str, Any]:
    """Remove importer-specific ephemeral values from persisted API state."""

    ephemeral_param_names = _EPHEMERAL_PARAM_NAMES_BY_IMPORTER.get(
        importer,
        frozenset(),
    )
    return {
        name: param_value
        for name, param_value in params_by_name.items()
        if name not in ephemeral_param_names
    }


def _params_for_import_run_response(
    importer: str,
    params_by_name: dict[str, Any],
) -> dict[str, Any]:
    """Project private multipart coordinates to an opaque public run marker."""

    stored_params_by_name = _params_for_import_run_storage(
        importer,
        params_by_name,
    )
    if importer != "ptg":
        return stored_params_by_name
    if protected_singleton_direct_presence(stored_params_by_name):
        public_params_by_name = {
            name: param_value
            for name, param_value in stored_params_by_name.items()
            if name not in DIRECT_RATE_FILE_PROTECTED_FIELDS
            and name not in {"source_file_id", "source_key"}
        }
        public_params_by_name[DIRECT_RATE_FILE_PUBLIC_MARKER] = True
        public_params_by_name[DIRECT_RATE_FILE_INTENT_SHA256_FIELD] = (
            stored_params_by_name[DIRECT_RATE_FILE_INTENT_SHA256_FIELD]
        )
        public_params_by_name["max_files"] = 1
        return public_params_by_name
    if not protected_frozen_tuple_presence(stored_params_by_name):
        return stored_params_by_name
    public_params_by_name = {
        name: param_value
        for name, param_value in stored_params_by_name.items()
        if name not in FROZEN_RATE_FILE_PROTECTED_FIELDS
    }
    public_params_by_name["frozen_rate_file_set_protected"] = True
    public_params_by_name["frozen_rate_file_count"] = int(
        stored_params_by_name["frozen_rate_file_count"]
    )
    return public_params_by_name


@dataclass(frozen=True)
class _ImportParamViews:
    persisted_by_name: dict[str, Any]
    enqueue_by_name: dict[str, Any]


def _import_param_views(
    importer: str,
    params_by_name: dict[str, Any],
    *,
    run_id: str,
) -> _ImportParamViews:
    """Build separate persisted and one-shot enqueue parameter mappings."""

    if importer != "ptg":
        return _ImportParamViews(
            persisted_by_name=dict(params_by_name),
            enqueue_by_name=dict(params_by_name),
        )
    if _PTG_FULL_REBUILD_SCOPE_PARAM in params_by_name:
        raise ValueError(
            "PTG full rebuild scope is internal and cannot be supplied"
        )
    if _PTG_FULL_REBUILD_MARKER_PARAM in params_by_name:
        raise ValueError(
            "PTG full rebuild marker is internal and cannot be supplied"
        )
    if _PTG_EXACT_WAVE_INTERNAL_PARAMS.intersection(params_by_name):
        raise ValueError(
            "PTG exact-wave identity is internal and cannot be supplied"
        )
    ordinary_params_by_name = {
        name: param_value
        for name, param_value in params_by_name.items()
        if name != _PTG_FULL_REBUILD_TOKEN_PARAM
    }
    if _PTG_FULL_REBUILD_TOKEN_PARAM not in params_by_name:
        return _ImportParamViews(
            persisted_by_name=ordinary_params_by_name,
            enqueue_by_name=dict(ordinary_params_by_name),
        )
    scope_digest = _ptg_full_rebuild_scope_digest(
        params_by_name[_PTG_FULL_REBUILD_TOKEN_PARAM],
        run_id=run_id,
    )
    return _ImportParamViews(
        persisted_by_name={
            **ordinary_params_by_name,
            _PTG_FULL_REBUILD_MARKER_PARAM: True,
        },
        enqueue_by_name={
            **ordinary_params_by_name,
            _PTG_FULL_REBUILD_SCOPE_PARAM: scope_digest,
        },
    )


def _assert_ptg_rebuild_request_params(
    importer: str,
    params_by_name: dict[str, Any],
) -> None:
    """Reject internal rebuild fields at the authenticated request boundary."""

    if importer != "ptg":
        return
    if _PTG_FULL_REBUILD_SCOPE_PARAM in params_by_name:
        raise ValueError(
            "PTG full rebuild scope is internal and cannot be supplied"
        )
    if _PTG_FULL_REBUILD_MARKER_PARAM in params_by_name:
        raise ValueError(
            "PTG full rebuild marker is internal and cannot be supplied"
        )
    if _PTG_EXACT_WAVE_INTERNAL_PARAMS.intersection(params_by_name):
        raise ValueError(
            "PTG exact-wave identity is internal and cannot be supplied"
        )


def _ptg_full_rebuild_scope_digest(raw_token: Any, *, run_id: str) -> str:
    """Derive a domain-separated scope from one canonical token and run id."""

    if not isinstance(raw_token, str):
        raise ValueError("private PTG full rebuild token must be a valid UUID")
    try:
        normalized_token = uuid.UUID(raw_token)
    except (AttributeError, ValueError) as exc:
        raise ValueError("private PTG full rebuild token must be a valid UUID") from exc
    if raw_token != str(normalized_token):
        raise ValueError("private PTG full rebuild token must be a valid UUID")
    if not run_id:
        raise ValueError("private PTG full rebuild request requires a control run id")
    run_id_bytes = run_id.encode("utf-8")
    return hashlib.sha256(
        _PTG_FULL_REBUILD_SCOPE_DIGEST_DOMAIN
        + normalized_token.bytes
        + len(run_id_bytes).to_bytes(4, byteorder="big")
        + run_id_bytes
    ).hexdigest()


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

        return finish_main
    if importer == "drug-claims":
        from process.drug_claims import finish_main

        return finish_main
    if importer == "provider-quality":
        from process.provider_quality import finish_main

        return finish_main
    if importer == "partd-formulary-network":
        from process.partd_formulary_network import finish_main

        return finish_main
    if importer == "pharmacy-license":
        from process.pharmacy_license import finish_main

        return finish_main
    if importer == "mrf":
        from process.initial import finish_main

        return finish_main
    raise ValueError(f"importer does not support finalize: {importer}")


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
    """Return one import run; plan-pricing terminalization stays explicit."""

    query_result = await db.execute(
        select(ImportRun).where(ImportRun.run_id == run_id).limit(1)
    )
    durable_run = query_result.scalar_one_or_none()
    if not durable_run:
        return None
    public_run = normalize_run(durable_run)
    if public_run.get("importer") not in STALE_WORKER_RECONCILIATION_IMPORTERS:
        public_run = await _sync_terminal_worker_failure(public_run)
    blank_metrics_by_name = await _allowed_amount_blank_terminal_metrics(
        durable_run, public_run
    )
    if blank_metrics_by_name is not None:
        blank_metrics_by_name = {
            metric_name: metric_value
            for metric_name, metric_value in blank_metrics_by_name.items()
            if metric_name != "source_key"
        }
        public_run["metrics"] = {
            **dict(public_run.get("metrics") or {}),
            **blank_metrics_by_name,
        }
    return public_run


def _blank_projection_inputs(
    durable_run: Any,
    public_run: dict[str, Any],
) -> tuple[dict[str, Any], str, dict[str, Any]] | None:
    """Return protected coordinates only for the exact failed outer run."""

    # Public normalization removes the protected coordinates needed here.
    params = (
        durable_run.params
        if isinstance(getattr(durable_run, "params", None), dict)
        else {}
    )
    source_file_import_id = str(
        getattr(durable_run, "source_file_import_id", None) or ""
    ).strip()
    error = public_run.get("error")
    if (
        public_run.get("importer") != "ptg"
        or public_run.get("status") != "failed"
        or not isinstance(error, dict)
        or error.get("code") != "ptg_import_failed"
        or error.get("message") != ALLOWED_AMOUNT_BLANK_ERROR
        or not source_file_import_id
        or params.get("source_file_import_id") != source_file_import_id
        or params.get("import_id") != source_file_import_id
        or params.get("max_files") != 1
        or not params.get("allowed_url")
        or params.get("in_network_url") is not None
    ):
        return None
    return params, source_file_import_id, error


async def _allowed_amount_blank_terminal_metrics(
    durable_run: Any,
    public_run: dict[str, Any],
) -> dict[str, Any] | None:
    """Load the narrow durable proof for a failed singleton allowed file."""

    projection_inputs = _blank_projection_inputs(durable_run, public_run)
    if projection_inputs is None:
        return None
    params, source_file_import_id, error = projection_inputs
    engine_run_result = await db.execute(
        select(PTG2ImportRun)
        .where(
            PTG2ImportRun.import_run_id == f"ptg2:{source_file_import_id}"
        )
        .limit(1)
    )
    engine_run = engine_run_result.scalar_one_or_none()
    report = (
        engine_run.report
        if engine_run is not None and isinstance(engine_run.report, dict)
        else {}
    )
    snapshot_id = report.get("snapshot_id")
    if not isinstance(snapshot_id, str) or not snapshot_id:
        return None
    snapshot_result = await db.execute(
        select(PTG2Snapshot)
        .where(PTG2Snapshot.snapshot_id == snapshot_id)
        .limit(1)
    )
    return allowed_amount_blank_metrics(
        source_file_import_id=source_file_import_id,
        source_key=str(params.get("source_key") or ""),
        import_month=params.get("import_month"),
        plan_ids=params.get("plan_ids") or [],
        plan_market_types=params.get("plan_market_types") or [],
        outer_error=error,
        engine_run=engine_run,
        engine_snapshot=snapshot_result.scalar_one_or_none(),
    )


async def _sync_terminal_worker_failure(run: dict[str, Any]) -> dict[str, Any]:
    if run.get("status") not in {"starting", "running", "finalizing"}:
        return run
    if (
        str(run.get("importer") or "") == "ptg"
        and await is_ptg_wave_owned_run(db, str(run.get("run_id") or ""))
    ):
        return run
    worker_status = await _active_worker_state(run)
    failed_item = _failed_worker_state_item(worker_status)
    if failed_item is None:
        return run

    now = utc_now()
    progress_dict = {
        "unit": "run",
        "total": 1,
        "done": 1,
        "pct": 100,
        "message": "worker job failed",
    }
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
    failure = (
        worker_item.get("failure")
        if isinstance(worker_item.get("failure"), dict)
        else {}
    )
    job_name = str(worker_item.get("job_name") or "worker job")
    reason = str(
        failure.get("reason") or worker_item.get("job_status") or "failed"
    ).strip()
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


def _required_reconciliation_timestamp(value: Any, field_name: str) -> dt.datetime:
    if type(value) is not str or not value or value != value.strip():
        raise ValueError(f"{field_name} must be a non-empty UTC timestamp")
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field_name} must be a valid UTC timestamp") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{field_name} must include a UTC offset")
    return parsed.astimezone(dt.UTC).replace(tzinfo=None)


def _validated_stale_worker_reconciliation(
    payload: dict[str, Any],
) -> dict[str, Any]:
    if type(payload) is not dict or set(payload) != _STALE_WORKER_RECONCILIATION_FIELDS:
        raise ValueError(
            "request must contain exactly expected_importer, expected_status, "
            "expected_heartbeat_at, expected_attempt_id, and "
            "expected_attempt_started_at"
    )
    importer = payload.get("expected_importer")
    if type(importer) is not str or importer not in STALE_WORKER_RECONCILIATION_IMPORTERS:
        raise ValueError("expected_importer does not support worker reconciliation")
    if payload.get("expected_status") != "running":
        raise ValueError("expected_status must be running")
    attempt_id = payload.get("expected_attempt_id")
    attempt_started_at = payload.get("expected_attempt_started_at")
    if type(attempt_id) is not str or not attempt_id or attempt_id != attempt_id.strip():
        raise ValueError("expected_attempt_id must be a non-empty string")
    _ = _required_reconciliation_timestamp(
        attempt_started_at,
        "expected_attempt_started_at",
    )
    return {
        **payload,
        "expected_heartbeat": _required_reconciliation_timestamp(
            payload.get("expected_heartbeat_at"),
            "expected_heartbeat_at",
        ),
    }


def _raw_connection_run(connection_row: Any) -> dict[str, Any]:
    mapping = getattr(connection_row, "_mapping", None)
    if mapping is not None:
        return dict(mapping)
    return dict(connection_row) if isinstance(connection_row, dict) else {}


async def _locked_reconciliation_run(
    connection: Any,
    run_id: str,
) -> dict[str, Any] | None:
    rows = await connection.all(
        select(ImportRun.__table__)
        .where(ImportRun.run_id == run_id)
        .limit(1)
        .with_for_update()
    )
    return _raw_connection_run(rows[0]) if rows else None


def _reconciliation_attempt(run: dict[str, Any]) -> tuple[str, str]:
    progress = run.get("progress") if isinstance(run.get("progress"), dict) else {}
    attempt_pair = _cancel_attempt_pair(progress)
    if attempt_pair is None:
        raise StaleWorkerReconciliationConflict(
            "run does not have a complete worker attempt identity"
        )
    return attempt_pair


def _is_same_lifecycle_lost_result(
    run: dict[str, Any],
    expected: dict[str, Any],
) -> bool:
    error = run.get("error") if isinstance(run.get("error"), dict) else {}
    progress = run.get("progress") if isinstance(run.get("progress"), dict) else {}
    try:
        observed_heartbeat = _required_reconciliation_timestamp(
            error.get("observed_heartbeat_at"),
            "observed_heartbeat_at",
        )
    except ValueError:
        return False
    return (
        run.get("status") == "failed"
        and error.get("code") == "worker_lifecycle_lost"
        and run.get("importer") == expected["expected_importer"]
        and observed_heartbeat == expected["expected_heartbeat"]
        and error.get("attempt_id") == expected["expected_attempt_id"]
        and error.get("attempt_started_at")
        == expected["expected_attempt_started_at"]
        and progress.get("attempt_id") == expected["expected_attempt_id"]
        and progress.get("attempt_started_at")
        == expected["expected_attempt_started_at"]
    )


def _stale_worker_receipt(
    run: dict[str, Any],
    *,
    reconciled: bool,
) -> dict[str, Any]:
    progress = run.get("progress") if isinstance(run.get("progress"), dict) else {}
    error = run.get("error") if isinstance(run.get("error"), dict) else {}
    return {
        "run_id": run.get("run_id"),
        "importer": run.get("importer"),
        "status": run.get("status"),
        "reconciled": reconciled,
        "error_code": error.get("code"),
        "attempt_id": progress.get("attempt_id"),
        "attempt_started_at": progress.get("attempt_started_at"),
    }


async def _arq_worker_presence(run: dict[str, Any]) -> dict[str, Any]:
    _run_id, _importer, queue, job_id = _reconciliation_arq_identity(run)
    redis_pool = await create_pool(
        build_redis_settings(),
        job_serializer=serialize_job,
        job_deserializer=deserialize_job,
    )
    try:
        async with redis_pool.pipeline(transaction=True) as pipe:
            pipe.zscore(queue, job_id)
            pipe.exists(job_key_prefix + job_id)
            pipe.exists(retry_key_prefix + job_id)
            pipe.exists(in_progress_key_prefix + job_id)
            pipe.exists(result_key_prefix + job_id)
            queue_score, job, retry, in_progress, result = await pipe.execute()
    finally:
        await redis_pool.aclose(close_connection_pool=True)
    return {
        "queue_member": queue_score is not None,
        "job": bool(job),
        "retry": bool(retry),
        "in_progress": bool(in_progress),
        "result": bool(result),
    }


def _require_stale_worker_identity(
    run: dict[str, Any],
    expected: dict[str, Any],
    now: dt.datetime,
) -> tuple[str, str, dt.datetime]:
    if run.get("importer") != expected["expected_importer"]:
        raise StaleWorkerReconciliationConflict("importer changed during reconciliation")
    if run.get("status") != expected["expected_status"]:
        raise StaleWorkerReconciliationConflict("run status changed during reconciliation")
    attempt_id, attempt_started_at = _reconciliation_attempt(run)
    if (
        attempt_id != expected["expected_attempt_id"]
        or attempt_started_at != expected["expected_attempt_started_at"]
    ):
        raise StaleWorkerReconciliationConflict("worker attempt changed during reconciliation")
    heartbeat_at = run.get("heartbeat_at")
    if not isinstance(heartbeat_at, dt.datetime):
        raise StaleWorkerReconciliationConflict("run heartbeat is unavailable")
    if heartbeat_at.tzinfo is not None:
        heartbeat_at = heartbeat_at.astimezone(dt.UTC).replace(tzinfo=None)
    if heartbeat_at != expected["expected_heartbeat"]:
        raise StaleWorkerReconciliationConflict("run heartbeat changed during reconciliation")
    if now - heartbeat_at < dt.timedelta(
        seconds=STALE_WORKER_RECONCILIATION_MIN_AGE_SECONDS
    ):
        raise StaleWorkerReconciliationConflict("run heartbeat is not stale")
    return attempt_id, attempt_started_at, heartbeat_at


def _require_absent_worker_state(
    kubernetes_by_field: dict[str, Any],
    arq_by_field: dict[str, Any],
) -> None:
    if kubernetes_by_field.get("enabled") is not True:
        raise StaleWorkerReconciliationUnavailable(
            "Kubernetes worker lookup is unavailable"
        )
    if int(kubernetes_by_field.get("job_count") or 0) or int(
        kubernetes_by_field.get("pod_count") or 0
    ):
        raise StaleWorkerReconciliationConflict(
            "Kubernetes worker evidence is still present"
        )
    if any(
        arq_by_field.get(key)
        for key in ("queue_member", "job", "retry", "in_progress", "result")
    ):
        raise StaleWorkerReconciliationConflict("ARQ worker evidence is still present")


def _reconciliation_worker_payload(run: dict[str, Any]) -> dict[str, Any]:
    run_id, importer, queue, _job_id = _reconciliation_arq_identity(run)
    metrics = run.get("metrics") if isinstance(run.get("metrics"), dict) else {}
    persisted_queue = str(metrics.get("queue") or "").strip()
    if persisted_queue and persisted_queue != queue:
        raise StaleWorkerReconciliationConflict(
            "persisted worker queue conflicts with importer adapter"
        )
    payload = {
        "run_id": run_id,
        "importer": importer,
        "queue": queue,
        "worker_class": str(metrics.get("worker_class") or "").strip(),
    }
    return {key: value for key, value in payload.items() if value}


def _reconciliation_arq_identity(
    run: dict[str, Any],
) -> tuple[str, str, str, str]:
    run_id = str(run.get("run_id") or "").strip()
    importer = str(run.get("importer") or "").strip()
    adapter = _adapter_for_import_row(run)
    if not run_id or not importer or adapter is None:
        raise StaleWorkerReconciliationConflict("exact ARQ identity is unavailable")
    queue = str(adapter["queue"])
    canonical_job_id = _enqueue_job_options(adapter, {"run_id": run_id}).get(
        "_job_id"
    )
    if not canonical_job_id:
        raise StaleWorkerReconciliationConflict("exact ARQ job ID is unavailable")
    metrics = run.get("metrics") if isinstance(run.get("metrics"), dict) else {}
    persisted_job_id = str(metrics.get("job_id") or "").strip()
    if persisted_job_id and persisted_job_id != canonical_job_id:
        raise StaleWorkerReconciliationConflict(
            "persisted ARQ job ID conflicts with importer adapter"
        )
    return run_id, importer, queue, canonical_job_id


async def _verified_absent_worker(
    run_by_field: dict[str, Any],
    expected_by_field: dict[str, Any],
    now: dt.datetime,
) -> tuple[str, str, dt.datetime]:
    attempt_id, attempt_started_at, heartbeat_at = _require_stale_worker_identity(
        run_by_field,
        expected_by_field,
        now,
    )
    worker_payload_by_field = _reconciliation_worker_payload(run_by_field)
    try:
        kubernetes_by_field = await asyncio.to_thread(
            exact_worker_presence,
            worker_payload_by_field,
        )
        arq_by_field = await _arq_worker_presence(run_by_field)
    except ValueError as exc:
        raise StaleWorkerReconciliationConflict(str(exc)) from exc
    except Exception as exc:
        raise StaleWorkerReconciliationUnavailable(
            "worker absence proof is unavailable"
        ) from exc
    _require_absent_worker_state(kubernetes_by_field, arq_by_field)
    return attempt_id, attempt_started_at, heartbeat_at


def _stale_worker_terminal_values(
    run_by_field: dict[str, Any],
    expected_by_field: dict[str, Any],
    *,
    attempt_id: str,
    attempt_started_at: str,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    absence_by_field = {
        "kubernetes_job_count": 0,
        "kubernetes_pod_count": 0,
        "arq_queue_member": False,
        "arq_job": False,
        "arq_retry": False,
        "arq_in_progress": False,
        "arq_result": False,
    }
    progress_by_field = {
        "unit": "run",
        "total": 1,
        "done": 1,
        "pct": 100,
        "message": "worker lifecycle lost",
        "attempt_id": attempt_id,
        "attempt_started_at": attempt_started_at,
    }
    error_by_field = {
        "code": "worker_lifecycle_lost",
        "message": "Worker lifecycle state disappeared before terminal status",
        "retryable": False,
        "observed_heartbeat_at": expected_by_field["expected_heartbeat"].replace(
            tzinfo=dt.UTC
        ).isoformat(timespec="microseconds"),
        "attempt_id": attempt_id,
        "attempt_started_at": attempt_started_at,
        "absence": absence_by_field,
    }
    metrics_by_name = {
        **dict(run_by_field.get("metrics") or {}),
        "terminal_worker_reconciliation": absence_by_field,
    }
    return progress_by_field, error_by_field, metrics_by_name


async def _persist_stale_worker_failure(
    connection: Any,
    run_by_field: dict[str, Any],
    expected_by_field: dict[str, Any],
    *,
    now: dt.datetime,
    attempt_id: str,
    attempt_started_at: str,
    heartbeat_at: dt.datetime,
) -> dict[str, Any]:
    progress_by_field, error_by_field, metrics_by_name = _stale_worker_terminal_values(
        run_by_field,
        expected_by_field,
        attempt_id=attempt_id,
        attempt_started_at=attempt_started_at,
    )
    update_count = await connection.status(
        update(ImportRun)
        .where(
            ImportRun.run_id == run_by_field["run_id"],
            ImportRun.status == "running",
            ImportRun.heartbeat_at == heartbeat_at,
            ImportRun.progress["attempt_id"].as_string() == attempt_id,
            ImportRun.progress["attempt_started_at"].as_string()
            == attempt_started_at,
        )
        .values(
            status="failed",
            phase_detail="worker lifecycle lost",
            heartbeat_at=now,
            finished_at=now,
            progress=progress_by_field,
            metrics=metrics_by_name,
            error=error_by_field,
        )
    )
    if update_count != 1:
        raise StaleWorkerReconciliationConflict("run changed during reconciliation")
    return {
        **run_by_field,
        "status": "failed",
        "phase_detail": "worker lifecycle lost",
        "heartbeat_at": now,
        "finished_at": now,
        "progress": progress_by_field,
        "metrics": metrics_by_name,
        "error": error_by_field,
    }


async def reconcile_stale_worker_failure(
    run_id: str,
    request_by_field: dict[str, Any],
) -> dict[str, Any] | None:
    """Fail one exact stale run only after worker and queue absence proof."""

    normalized_run_id = str(run_id or "").strip()
    if not normalized_run_id:
        raise ValueError("run_id is required")
    expected_by_field = _validated_stale_worker_reconciliation(request_by_field)
    now = utc_now()
    async with db.acquire() as connection:
        await acquire_control_run_worker_action_lock(connection, normalized_run_id)
        run_by_field = await _locked_reconciliation_run(
            connection,
            normalized_run_id,
        )
        if run_by_field is None:
            return None
        if _is_same_lifecycle_lost_result(run_by_field, expected_by_field):
            return _stale_worker_receipt(run_by_field, reconciled=False)
        attempt_id, attempt_started_at, heartbeat_at = await _verified_absent_worker(
            run_by_field,
            expected_by_field,
            now,
        )
        updated_run_by_field = await _persist_stale_worker_failure(
            connection,
            run_by_field,
            expected_by_field,
            now=now,
            attempt_id=attempt_id,
            attempt_started_at=attempt_started_at,
            heartbeat_at=heartbeat_at,
        )

    public_run_by_field = normalize_run(updated_run_by_field)
    try:
        _write_run_live_progress(public_run_by_field, publish_event=False)
        enqueue_status_event(public_run_by_field)
    except Exception:
        logger.warning(
            "stale worker reconciliation event projection failed for %s",
            normalized_run_id,
            exc_info=True,
        )
    return _stale_worker_receipt(public_run_by_field, reconciled=True)


async def finalize_import_run(run_id: str, finalize_payload: dict[str, Any]) -> dict[str, Any] | None:
    """Dispatch and record the importer-specific finalization phase."""

    current_run = await get_import_run(run_id)
    if not current_run:
        return None
    if str(current_run.get("importer") or "") == "ptg":
        await require_not_wave_owned_run(db, run_id)
    importer = str(current_run.get("importer") or "").strip()
    if importer not in _FINISH_IMPORTERS:
        raise ValueError(f"importer does not support finalize: {importer}")
    if current_run.get("status") in TERMINAL_STATUSES:
        return current_run

    finish_params = _finish_params_for(importer, current_run, finalize_payload)
    finish_fn = _finish_function(importer)
    now = utc_now()
    finalizing_progress_by_field = {
        "unit": "run",
        "total": 1,
        "done": 0,
        "pct": 0,
        "message": "finalizing",
    }
    finalize_result = await finish_fn(**finish_params)
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
            progress=finalizing_progress_by_field,
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


async def find_importer_run_by_idempotency_key(
    importer: str,
    idempotency_key: str,
) -> dict[str, Any] | None:
    """Find the durable all-status owner for a scoped idempotency key."""

    result = await db.execute(
        select(ImportRun)
        .where(ImportRun.importer == importer)
        .where(ImportRun.idempotency_key == idempotency_key)
        .limit(1)
    )
    row = result.scalar_one_or_none()
    return normalize_run(row) if row else None


async def _idempotent_import_run(
    importer: str,
    idempotency_key: str,
) -> dict[str, Any] | None:
    if importer in ALL_STATUS_IDEMPOTENCY_IMPORTERS:
        return await find_importer_run_by_idempotency_key(
            importer,
            idempotency_key,
        )
    return await find_active_run_by_idempotency_key(idempotency_key)


async def find_earliest_active_run_by_importer(importer: str) -> dict[str, Any] | None:
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
_NPI_ADMISSION_LOCK_KEY = "import-run-admission:npi"
_PROVIDER_DIRECTORY_ACQUISITION = "acquisition"
_PROVIDER_DIRECTORY_SCOPED_ARTIFACT = "scoped_artifact"
_PROVIDER_DIRECTORY_SCOPED_RELATION_ARTIFACT = "scoped_relation_artifact"
_PROVIDER_DIRECTORY_SCOPED_SEED = "scoped_seed"
_PROVIDER_DIRECTORY_GLOBAL_PROFILE = "global_profile"
_PROVIDER_DIRECTORY_EXCLUSIVE = "exclusive"
_PROVIDER_DIRECTORY_RELATION_ARTIFACT_TARGETS = frozenset(
    {
        "dataset_network_plan",
        "dataset_affiliation_organization",
    }
)


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


async def _active_ptg_source_file_replay(
    connection: Any,
    source_file_import_id: str,
) -> dict[str, Any] | None:
    """Return an active ordinary PTG run for an immutable source replay."""

    statement = (
        select(ImportRun.__table__)
        .where(ImportRun.importer == "ptg")
        .where(ImportRun.source_file_import_id == source_file_import_id)
        .where(ImportRun.status.in_(ACTIVE_STATUSES))
        .order_by(ImportRun.created_at.asc())
        .limit(2)
    )
    active_rows = await connection.all(statement)
    for active_row in active_rows:
        active_run = _normalize_connection_run(active_row)
        if not await is_ptg_wave_owned_run(
            connection,
            str(active_run.get("run_id") or ""),
        ):
            return active_run
    return None


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
        "full_address_artifact_rebuild",
    )
    if any(params.get(flag_name) for flag_name in incompatible_flags):
        return None
    return _provider_directory_source_ids(params.get("source_ids"))


def _provider_directory_relation_artifact_targets(
    params: dict[str, Any],
) -> frozenset[str] | None:
    """Return exact source-local relation targets or fail closed."""

    publish_corroboration = params.get("publish_corroboration")
    if publish_corroboration is not None and publish_corroboration is not False:
        return None
    raw_targets = params.get("publish_artifacts_targets")
    if not isinstance(raw_targets, str):
        return None
    target_values = [target.strip() for target in raw_targets.split(",")]
    if (
        not target_values
        or any(not target for target in target_values)
        or len(target_values) != len(set(target_values))
    ):
        return None
    targets = frozenset(target_values)
    if not targets.issubset(_PROVIDER_DIRECTORY_RELATION_ARTIFACT_TARGETS):
        return None
    return targets


def _provider_directory_seed_scope(params: dict[str, Any]) -> frozenset[str] | None:
    """Return exact source IDs for a metadata-only, non-cleanup seed run."""
    if params.get("seed_only") is not True:
        return None
    incompatible_flags = (
        "import_resources",
        "canonical_backfill_only",
        "contact_backfill_only",
        "dataset_rehydrate_only",
        "publish_artifacts_only",
        "publish_artifacts",
        "publish_after_acquisition",
        "publish_corroboration",
        "full_refresh",
        "stale_cleanup",
        "refresh_preset",
        "preset",
    )
    if any(params.get(flag_name) for flag_name in incompatible_flags):
        return None
    return _provider_directory_source_ids(params.get("source_ids"))


def _provider_directory_operation(
    params: dict[str, Any],
    metrics: dict[str, Any] | None = None,
) -> tuple[str, frozenset[str], str | None]:
    if _is_current_version_census_control(params):
        return _PROVIDER_DIRECTORY_EXCLUSIVE, frozenset(), None
    if any(
        field_name in params
        for field_name in (
            "provider_directory_profile_contract_id",
            "provider_directory_profile_generation",
            "provider_directory_profile_selection_attestation",
        )
    ):
        try:
            validated_profile_execution(params)
        except ProviderDirectoryProfileSelectionError:
            return _PROVIDER_DIRECTORY_EXCLUSIVE, frozenset(), None
        return _PROVIDER_DIRECTORY_GLOBAL_PROFILE, frozenset(), None
    acquisition_scope = _provider_directory_acquisition_scope(params, metrics)
    if acquisition_scope is not None:
        source_ids, endpoint_scope = acquisition_scope
        return _PROVIDER_DIRECTORY_ACQUISITION, source_ids, endpoint_scope
    artifact_source_ids = _provider_directory_artifact_scope(params)
    if artifact_source_ids is not None:
        if _provider_directory_relation_artifact_targets(params) is not None:
            return (
                _PROVIDER_DIRECTORY_SCOPED_RELATION_ARTIFACT,
                artifact_source_ids,
                None,
            )
        return _PROVIDER_DIRECTORY_SCOPED_ARTIFACT, artifact_source_ids, None
    seed_source_ids = _provider_directory_seed_scope(params)
    if seed_source_ids is not None:
        return _PROVIDER_DIRECTORY_SCOPED_SEED, seed_source_ids, None
    return _PROVIDER_DIRECTORY_EXCLUSIVE, frozenset(), None


def _is_current_version_census_control(
    params: dict[str, Any],
) -> bool:
    raw_strategy = params.get("provider_directory_acquisition_strategy")
    return bool(
        raw_strategy
        in {
            ProviderDirectoryFHIRAcquisitionStrategy.CUTOFF_BOUNDED_CURRENT_VERSION_CENSUS,
            ProviderDirectoryFHIRAcquisitionStrategy.SERVER_ISSUED_TRAVERSAL_SUBSET,
        }
        or (
            isinstance(raw_strategy, str)
            and raw_strategy.strip()
            in {
                _PROVIDER_DIRECTORY_CURRENT_VERSION_CENSUS_STRATEGY,
                _PROVIDER_DIRECTORY_SERVER_ISSUED_SUBSET_STRATEGY,
            }
        )
    )


def _reject_control_current_version_census(
    importer: str,
    params: dict[str, Any],
) -> None:
    """Keep the current-version census on its reviewed CLI path."""

    raw_cutoff = params.get("provider_directory_census_cutoff")
    if (
        importer == "provider-directory-fhir"
        and (
            _is_current_version_census_control(params)
            or raw_cutoff not in (None, "")
        )
    ):
        raise ValueError(
            "provider_directory_current_version_census_control_api_disabled"
        )


def _classified_provider_directory_runs(
    active_runs: list[dict[str, Any]],
) -> tuple[
    dict[str, Any] | None,
    list[
        tuple[
            dict[str, Any],
            tuple[str, frozenset[str], str | None],
        ]
    ],
]:
    classified_runs = []
    for active_run in active_runs:
        active_params = active_run.get("params")
        if not isinstance(active_params, dict):
            return active_run, []
        active_metrics = active_run.get("metrics")
        if not isinstance(active_metrics, dict):
            active_metrics = None
        operation = _provider_directory_operation(
            active_params,
            active_metrics,
        )
        if operation[0] == _PROVIDER_DIRECTORY_EXCLUSIVE:
            return active_run, []
        classified_runs.append((active_run, operation))
    return None, classified_runs


def _has_provider_directory_operation_conflict(
    requested_operation: tuple[str, frozenset[str], str | None],
    active_operation: tuple[str, frozenset[str], str | None],
) -> bool:
    requested_kind, requested_source_ids, requested_endpoint = (
        requested_operation
    )
    active_kind, active_source_ids, active_endpoint = active_operation
    scoped_artifact_kinds = {
        _PROVIDER_DIRECTORY_SCOPED_ARTIFACT,
        _PROVIDER_DIRECTORY_SCOPED_RELATION_ARTIFACT,
    }
    profile_conflict_kinds = {
        _PROVIDER_DIRECTORY_GLOBAL_PROFILE,
        *scoped_artifact_kinds,
        _PROVIDER_DIRECTORY_SCOPED_SEED,
    }
    if requested_kind == _PROVIDER_DIRECTORY_GLOBAL_PROFILE:
        return active_kind in profile_conflict_kinds
    if active_kind == _PROVIDER_DIRECTORY_GLOBAL_PROFILE:
        return requested_kind in {
            *scoped_artifact_kinds,
            _PROVIDER_DIRECTORY_SCOPED_SEED,
        }
    if requested_kind == _PROVIDER_DIRECTORY_SCOPED_RELATION_ARTIFACT:
        return (
            active_kind == _PROVIDER_DIRECTORY_SCOPED_ARTIFACT
            or not requested_source_ids.isdisjoint(active_source_ids)
        )
    if requested_kind == _PROVIDER_DIRECTORY_SCOPED_ARTIFACT:
        return (
            active_kind in scoped_artifact_kinds
            or not requested_source_ids.isdisjoint(active_source_ids)
        )
    if requested_kind == _PROVIDER_DIRECTORY_SCOPED_SEED:
        return not requested_source_ids.isdisjoint(active_source_ids)
    if active_kind in {
        *scoped_artifact_kinds,
        _PROVIDER_DIRECTORY_SCOPED_SEED,
    }:
        return not requested_source_ids.isdisjoint(active_source_ids)
    return (
        not requested_source_ids.isdisjoint(active_source_ids)
        or requested_endpoint == active_endpoint
    )


def _provider_directory_blocking_run(
    params: dict[str, Any],
    active_runs: list[dict[str, Any]],
) -> dict[str, Any] | None:
    """Return the first active run that conflicts with the requested operation."""
    if not active_runs:
        return None
    requested_kind, requested_source_ids, requested_endpoint = _provider_directory_operation(params)
    if requested_kind == _PROVIDER_DIRECTORY_EXCLUSIVE:
        return active_runs[0]
    blocking_run, classified_active_runs = (
        _classified_provider_directory_runs(active_runs)
    )
    if blocking_run is not None:
        return blocking_run
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

    requested_operation = (
        requested_kind,
        requested_source_ids,
        requested_endpoint,
    )
    for active_run, active_operation in classified_active_runs:
        if _has_provider_directory_operation_conflict(
            requested_operation,
            active_operation,
        ):
            return active_run
    return None


def _provider_directory_max_active() -> int:
    raw_limit = os.getenv("HLTHPRT_PROVIDER_DIRECTORY_MAX_ACTIVE", "").strip()
    try:
        configured_limit = int(raw_limit) if raw_limit else None
    except ValueError:
        configured_limit = None
    return configured_limit if configured_limit is not None and configured_limit > 0 else 2


def _is_parallel_active_importer_run_allowed(
    importer: str,
    payload: dict[str, Any],
    idempotency_key: str | None,
) -> bool:
    if importer != "ptg":
        return False
    source_file_import_id = str(payload.get("source_file_import_id") or "").strip()
    return bool(source_file_import_id and idempotency_key)


def _validate_provider_directory_profile_execution_params(
    importer: str,
    params: dict[str, Any],
) -> None:
    """Reject malformed proof-bearing Profile work before durable admission."""

    if importer != "provider-directory-fhir" or not any(
        field_name in params
        for field_name in (
            "provider_directory_profile_contract_id",
            "provider_directory_profile_generation",
            "provider_directory_profile_selection_attestation",
        )
    ):
        return
    try:
        validated_profile_execution(params)
    except ProviderDirectoryProfileSelectionError as exc:
        raise ValueError(str(exc)) from exc


def _validate_hospital_price_admission(params: dict[str, Any]) -> None:
    hospitals = selected_hospital_hpt_registry(params)
    configured_resource_limits(
        hospital_price_artifact_store(), len(locator_groups(hospitals))
    )


async def _validate_hospital_price_params(
    importer: str, params: dict[str, Any]
) -> None:
    if importer != "hospital-prices":
        return
    try:
        await asyncio.to_thread(_validate_hospital_price_admission, params)
    except ValueError as exc:
        raise ValueError(str(exc)) from exc


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


async def _locked_ptg_source_replay(
    connection: Any,
    import_row: dict[str, Any],
    *,
    source_file_import_id: str,
) -> dict[str, Any] | None:
    idempotency_key = import_row.get("idempotency_key")
    if idempotency_key:
        active_run = await _active_idempotency_run(
            connection,
            str(idempotency_key),
        )
        if (
            active_run
            and not await is_ptg_wave_owned_run(
                connection,
                str(active_run.get("run_id") or ""),
            )
        ):
            await recheck_frozen_binding_on_connection(
                connection,
                import_row["params"],
            )
            return active_run
    source_replay = await _active_ptg_source_file_replay(
        connection,
        source_file_import_id,
    )
    if source_replay:
        await recheck_frozen_binding_on_connection(
            connection,
            import_row["params"],
        )
    return source_replay


async def _admit_ptg_source_file_run(
    import_row: dict[str, Any],
) -> dict[str, Any] | None:
    """Bind immutable input and insert control lifecycle state atomically."""
    async with db.acquire() as connection:
        source_file_import_id = source_file_import_id_from_payload(
            import_row,
            required=True,
        )
        await require_source_attempt_capabilities(
            connection,
            require_attempt_authority=False,
        )
        await guard_source_attempt(
            connection,
            source_file_import_id=source_file_import_id,
        )
        await acquire_ptg_admission_lock(connection)
        idempotency_key = import_row.get("idempotency_key")
        source_replay = await _locked_ptg_source_replay(
            connection,
            import_row,
            source_file_import_id=source_file_import_id,
        )
        if source_replay:
            return source_replay
        # A binding-verified ordinary replay above is read-only. All new work
        # must clear the exact-wave capacity fence before it can insert a
        # frozen binding.
        await require_no_capacity_owning_wave(connection)
        await insert_or_compare_frozen_binding(
            connection,
            import_row["params"],
        )
        if not _is_parallel_active_importer_run_allowed(
            "ptg",
            import_row,
            (
                str(idempotency_key)
                if idempotency_key is not None
                else None
            ),
        ):
            active_runs = await _active_importer_runs(connection, "ptg")
            if active_runs:
                return active_runs[0]
        await connection.status(insert(ImportRun).values(**import_row))
        await record_source_attempt_event(
            connection,
            source_file_import_id=source_file_import_id,
            event_kind=(
                "retry_admitted"
                if import_row.get("retry_of_run_id")
                else "start_admitted"
            ),
            outer_run=import_row,
        )
    return None


async def _admit_wave_fenced_import_run(
    import_row: dict[str, Any],
) -> dict[str, Any] | None:
    """Serialize ordinary PTG-family admission against an exact-wave owner."""

    async with db.acquire() as connection:
        await acquire_ptg_admission_lock(connection)
        await require_no_capacity_owning_wave(connection)
        idempotency_key = import_row.get("idempotency_key")
        if idempotency_key:
            # Preserve the established public deduplication contract while
            # holding the shared wave fence; the second in-transaction check
            # below still closes the ordinary-admission race.
            existing = await find_active_run_by_idempotency_key(str(idempotency_key))
            if existing:
                return existing
            active_run = await _active_idempotency_run(connection, str(idempotency_key))
            if active_run:
                return active_run
        existing_importer = await find_earliest_active_run_by_importer(
            str(import_row["importer"])
        )
        if existing_importer:
            return existing_importer
        active_runs = await _active_importer_runs(
            connection,
            str(import_row["importer"]),
        )
        if active_runs:
            return active_runs[0]
        await connection.status(insert(ImportRun).values(**import_row))
    return None


async def _admit_npi_import_run(
    import_row: dict[str, Any],
) -> dict[str, Any] | None:
    """Serialize NPI admission so only one active run can reach its worker."""

    async with db.acquire() as connection:
        await connection.scalar(
            text("SELECT pg_advisory_xact_lock(hashtextextended(:lock_key, 0))"),
            lock_key=_NPI_ADMISSION_LOCK_KEY,
        )
        idempotency_key = import_row.get("idempotency_key")
        if idempotency_key:
            active_run = await _active_idempotency_run(
                connection,
                str(idempotency_key),
            )
            if active_run:
                return active_run
        active_runs = await _active_importer_runs(connection, "npi")
        if active_runs:
            return active_runs[0]
        await connection.status(insert(ImportRun).values(**import_row))
    return None


async def _admit_import_row(
    importer: str,
    import_run_values_by_name: dict[str, Any],
    *,
    is_ptg_source_file_admission: bool,
) -> dict[str, Any] | None:
    if importer == "provider-directory-fhir":
        return await _admit_provider_directory_run(import_run_values_by_name)
    if is_ptg_source_file_admission:
        return await _admit_ptg_source_file_run(import_run_values_by_name)
    if importer in PTG_WAVE_FENCED_IMPORTERS:
        return await _admit_wave_fenced_import_run(import_run_values_by_name)
    if importer == "npi":
        return await _admit_npi_import_run(import_run_values_by_name)
    await db.execute(insert(ImportRun).values(**import_run_values_by_name))
    return None


async def create_import_run(
    request_payload_map: dict[str, Any],
) -> tuple[dict[str, Any], bool]:
    """Create and enqueue an import run unless admission deduplicates it."""

    importer = str(request_payload_map.get("importer") or "").strip()
    if importer not in importer_names():
        raise ValueError(f"unknown importer: {importer}")
    raw_params_by_name = (
        request_payload_map.get("params")
        if isinstance(request_payload_map.get("params"), dict)
        else {}
    )
    effective_params_by_name = (
        apply_provider_directory_refresh_preset(raw_params_by_name)
        if importer == "provider-directory-fhir"
        else raw_params_by_name
    )
    if importer == "npi" and any(
        bool(effective_params_by_name.get(parameter_name))
        for parameter_name in ("test", "test_mode")
    ):
        raise ValueError("NPI test mode requires an isolated database")
    _reject_control_current_version_census(
        importer,
        effective_params_by_name,
    )
    _assert_ptg_rebuild_request_params(
        importer,
        effective_params_by_name,
    )
    _validate_provider_directory_profile_execution_params(
        importer,
        effective_params_by_name,
    )
    _validate_plan_pricing_prewarm_params(
        importer,
        effective_params_by_name,
    )
    await _validate_hospital_price_params(importer, effective_params_by_name)
    if importer == "provider-directory-fhir":
        validated_publication_candidate_from_params(
            effective_params_by_name
        )
        validate_uhc_official_file_admission(effective_params_by_name)
    normalized_params_by_name = (
        normalize_protected_frozen_rate_params(effective_params_by_name)
        if importer == "ptg"
        else dict(effective_params_by_name)
    )
    if importer == "ptg" and protected_frozen_tuple_presence(
        normalized_params_by_name
    ):
        protected_id = normalized_params_by_name[
            "source_file_import_id"
        ]
        if (
            str(request_payload_map.get("source_file_import_id") or "").strip()
            != protected_id
            or str(request_payload_map.get("import_id") or "").strip()
            != protected_id
        ):
            raise ValueError(
                "protected outer and nested source_file_import_id and "
                "import_id must all match"
            )
    request_payload_map = {
        **request_payload_map,
        "params": normalized_params_by_name,
    }
    source_file_import_id = (
        source_file_import_id_from_payload(
            request_payload_map,
            required=False,
        )
        if importer == "ptg"
        else None
    )
    is_ptg_source_file_admission = source_file_import_id is not None
    if source_file_import_id is not None:
        request_payload_map = {
            **request_payload_map,
            "source_file_import_id": source_file_import_id,
            "import_id": source_file_import_id,
        }

    idempotency_key = (
        str(request_payload_map.get("idempotency_key") or "").strip() or None
    )
    if (
        idempotency_key
        and importer != "provider-directory-fhir"
        and not is_ptg_source_file_admission
    ):
        replayed_run = await _idempotent_import_run(importer, idempotency_key)
        if replayed_run:
            return normalize_run(replayed_run), False
    if (
        importer != "provider-directory-fhir"
        and not is_ptg_source_file_admission
        and not _is_parallel_active_importer_run_allowed(
            importer,
            request_payload_map,
            idempotency_key,
        )
    ):
        active_importer = await find_earliest_active_run_by_importer(importer)
        if active_importer:
            return normalize_run(active_importer), False

    now = utc_now()
    run_id = str(request_payload_map.get("run_id") or "").strip() or _new_run_id()
    retry_of_run_id = (
        str(request_payload_map.get("retry_of_run_id") or "").strip() or None
    )
    param_views = _import_param_views(
        importer,
        request_payload_map.get("params")
        if isinstance(request_payload_map.get("params"), dict)
        else {},
        run_id=run_id,
    )
    import_id = request_payload_map.get("import_id")
    if importer == "openaddresses":
        import_id = (
            import_id
            or normalized_params_by_name.get("import_id")
            or normalized_params_by_name.get("stage_suffix")
            or run_id
        )
    import_run_values_by_name = {
        "run_id": run_id,
        "engine": ENGINE_NAME,
        "node_id": os.getenv("HLTHPRT_IMPORT_NODE_ID"),
        "importer": importer,
        "family": _importer_family(importer),
        "status": "queued",
        "phase_detail": "created",
        "params": param_views.persisted_by_name,
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
        "import_id": import_id,
        "retry_of_run_id": retry_of_run_id,
    }
    try:
        blocking_run = await _admit_import_row(
            importer,
            import_run_values_by_name,
            is_ptg_source_file_admission=is_ptg_source_file_admission,
        )
        if blocking_run:
            return normalize_run(blocking_run), False
    except IntegrityError:
        if idempotency_key:
            replayed_run = await _idempotent_import_run(
                importer,
                idempotency_key,
            )
            if replayed_run:
                return normalize_run(replayed_run), False
        raise
    enqueue_result = await _enqueue_import_start(
        {
            **import_run_values_by_name,
            "params": param_views.enqueue_by_name,
        }
    )
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
    public_run_by_name = normalize_run(import_run_values_by_name)
    enqueue_status_event(public_run_by_name)
    _write_run_live_progress(public_run_by_name, publish_event=False)
    return public_run_by_name, True


def _enqueue_progress(message: str) -> dict[str, Any]:
    return {
        "unit": "run",
        "total": 1,
        "done": 0,
        "pct": 0,
        "message": message,
    }


def _invalid_enqueue_adapter_result(
    *,
    now: dt.datetime,
    params: dict[str, Any],
    error: ValueError,
) -> dict[str, Any]:
    return {
        "status": "failed",
        "phase_detail": "enqueue failed",
        "heartbeat_at": now,
        "progress": _enqueue_progress("enqueue failed"),
        "metrics": {
            "enqueue_adapter": "arq_single_job",
            **_ptg_lane_metrics(params),
        },
        "error": {
            "code": "invalid_enqueue_adapter",
            "message": str(error),
        },
    }


def _pending_enqueue_adapter_result(
    now: dt.datetime,
) -> dict[str, Any]:
    return {
        "status": "queued",
        "phase_detail": "created; enqueue adapter pending",
        "heartbeat_at": now,
        "progress": _enqueue_progress("queued; enqueue adapter pending"),
        "metrics": {"enqueue_adapter": "pending"},
        "error": None,
    }


def _failed_enqueue_result(
    *,
    importer: str,
    params: dict[str, Any],
    adapter: dict[str, Any],
    error: Exception,
) -> dict[str, Any]:
    return {
        "status": "failed",
        "phase_detail": "enqueue failed",
        "heartbeat_at": utc_now(),
        "progress": _enqueue_progress("enqueue failed"),
        "metrics": {
            "enqueue_adapter": "arq_single_job",
            "queue": adapter["queue"],
            "function": adapter["function"],
            **_ptg_lane_metrics(params),
        },
        "error": {
            "code": "enqueue_failed",
            "message": _safe_enqueue_error_message(
                importer,
                params,
                error,
            ),
        },
    }


def _successful_enqueue_result(
    *,
    params: dict[str, Any],
    adapter: dict[str, Any],
    job_id: str | None,
) -> dict[str, Any]:
    return {
        "status": "queued",
        "phase_detail": "enqueued",
        "heartbeat_at": utc_now(),
        "progress": _enqueue_progress("queued"),
        "metrics": {
            "enqueue_adapter": "arq_single_job",
            "queue": adapter["queue"],
            "function": adapter["function"],
            "job_id": job_id,
            **_ptg_lane_metrics(params),
        },
        "error": None,
    }


def _enqueue_job_options(
    adapter: dict[str, Any],
    import_run_values_by_name: dict[str, Any],
) -> dict[str, str]:
    options_by_name = {"_queue_name": str(adapter["queue"])}
    if adapter.get("job_prefix"):
        options_by_name["_job_id"] = (
            f"{adapter['job_prefix']}_{import_run_values_by_name['run_id']}"
        )
    return options_by_name


async def _enqueue_import_start(
    import_run_values_by_name: dict[str, Any],
) -> dict[str, Any]:
    """Enqueue one importer start job and return its run-state update."""

    importer = str(import_run_values_by_name.get("importer") or "")
    now = utc_now()
    raw_params = import_run_values_by_name.get("params")
    params = raw_params if isinstance(raw_params, dict) else {}
    try:
        adapter = _adapter_for_import_row(import_run_values_by_name)
    except ValueError as exc:
        return _invalid_enqueue_adapter_result(
            now=now,
            params=params,
            error=exc,
        )
    if adapter is None:
        return _pending_enqueue_adapter_result(now)

    job_payload = _adapter_payload(adapter, import_run_values_by_name, params)
    enqueue_options_by_name = _enqueue_job_options(adapter, import_run_values_by_name)
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
        return _failed_enqueue_result(
            importer=importer,
            params=params,
            adapter=adapter,
            error=exc,
        )
    job_id = _safe_enqueued_job_id(
        importer,
        params,
        job,
        requested_job_id=enqueue_options_by_name.get("_job_id"),
    )
    return _successful_enqueue_result(
        params=params,
        adapter=adapter,
        job_id=job_id,
    )


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


def _safe_enqueue_error_message(
    importer: str,
    params_by_name: dict[str, Any],
    exc: Exception,
) -> str:
    """Keep ephemeral importer parameters out of persisted enqueue errors."""

    ephemeral_param_names = _EPHEMERAL_PARAM_NAMES_BY_IMPORTER.get(
        importer,
        frozenset(),
    )
    if any(name in params_by_name for name in ephemeral_param_names):
        return "import job enqueue failed"
    return str(exc)


def _safe_enqueued_job_id(
    importer: str,
    params_by_name: dict[str, Any],
    job: Any,
    *,
    requested_job_id: Any,
) -> str:
    """Avoid reflecting an ephemeral payload through a job representation."""

    ephemeral_param_names = _EPHEMERAL_PARAM_NAMES_BY_IMPORTER.get(
        importer,
        frozenset(),
    )
    if any(name in params_by_name for name in ephemeral_param_names):
        return str(requested_job_id or "")
    explicit_job_id = getattr(job, "job_id", None)
    if explicit_job_id:
        return str(explicit_job_id)
    return str(job or "")


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
            "import_id": import_run_values_by_name.get("import_id"),
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
    if import_run_values_by_name.get("importer") == "openaddresses":
        task_payload_map["control_import_id"] = (
            import_run_values_by_name.get("import_id")
            or task_payload_map.get("import_id")
            or task_payload_map.get("stage_suffix")
            or import_run_values_by_name["run_id"]
        )
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


def _is_queued_arq_cancel(
    current_run: dict[str, Any],
    run_metrics_by_name: dict[str, Any],
) -> bool:
    """Preserve queued cancellation provenance across retries."""

    if run_metrics_by_name.get("enqueue_adapter") != "arq_single_job":
        return False
    if current_run.get("status") == "queued":
        return True
    prior_signal_by_name = run_metrics_by_name.get("cancel_signal")
    return (
        current_run.get("status") == "canceling"
        and isinstance(prior_signal_by_name, dict)
        and "cancel_flag" in prior_signal_by_name
    )


async def request_cancel(run_id: str) -> dict[str, Any] | None:
    """Mark an active run for cancellation and signal its worker."""

    current = await get_import_run(run_id)
    if not current:
        return None
    if str(current.get("importer") or "") == "ptg":
        await require_not_wave_owned_run(db, run_id)
    if current.get("status") in TERMINAL_STATUSES:
        return current
    current_metrics = current.get("metrics") if isinstance(current.get("metrics"), dict) else {}
    run_metrics_by_name = dict(current_metrics)
    is_queued_arq = _is_queued_arq_cancel(current, run_metrics_by_name)
    if (
        current.get("status") != "queued"
        and not is_queued_arq
        and not _supports_active_cancel(str(current.get("importer") or ""))
    ):
        raise ValueError(f"importer does not support canceling active runs: {current.get('importer')}")
    now = utc_now()
    current_progress = current.get("progress") if isinstance(current.get("progress"), dict) else {}
    is_pending_adapter = (
        current.get("status") == "queued"
        and run_metrics_by_name.get("enqueue_adapter") == "pending"
    )
    worker_cancel_signal_map = await _cancel_signal_for_run(
        current,
        run_id=run_id,
        is_pending_adapter=is_pending_adapter,
        is_queued_arq=is_queued_arq,
    )
    run_metrics_by_name["cancel_signal"] = worker_cancel_signal_map
    canceled_before_start = is_pending_adapter or (
        is_queued_arq
        and _is_queued_arq_cancel_completed(worker_cancel_signal_map)
    )
    has_terminalized_active_worker = (
        _has_terminalized_active_worker_cancel_signal(worker_cancel_signal_map)
        and (not is_queued_arq or canceled_before_start)
    )
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
    if has_terminalized_active_worker:
        phase_detail = "canceled active worker"
    elif canceled_before_start:
        phase_detail = "canceled before start"
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
    cancel_update = (
        update(ImportRun)
        .where(ImportRun.run_id == run_id)
        .where(ImportRun.status.not_in(TERMINAL_STATUSES))
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
    attempt_pair = _cancel_attempt_pair(cancel_progress_by_name)
    if attempt_pair is not None:
        attempt_id, attempt_started_at = attempt_pair
        cancel_update = cancel_update.where(
            ImportRun.progress["attempt_id"].as_string() == attempt_id,
            ImportRun.progress["attempt_started_at"].as_string()
            == attempt_started_at,
        )
    update_result = await db.execute(cancel_update)
    updated = await get_import_run(run_id)
    if getattr(update_result, "rowcount", 1) == 0:
        return updated
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
    cancel_flag_by_name = await _set_cancel_flag(run_id)
    queued_signal_by_name = await _remove_queued_job(current_run)
    kubernetes_signal_by_name = await _delete_active_worker_jobs(current_run)
    if is_queued_arq:
        queued_signal_by_name["cancel_flag"] = cancel_flag_by_name
        queued_signal_by_name["kubernetes"] = kubernetes_signal_by_name
        return queued_signal_by_name
    return {
        **cancel_flag_by_name,
        "arq_cleanup": queued_signal_by_name,
        "kubernetes": kubernetes_signal_by_name,
    }


def _cancel_progress_by_name(
    *,
    canceled_now: bool,
    current_progress: dict[str, Any],
) -> dict[str, Any]:
    """Return terminal or in-progress cancellation progress."""

    cancel_progress_by_name = {
        "unit": "run",
        "total": 1,
        "done": 1 if canceled_now else 0,
        "pct": 100 if canceled_now else current_progress.get("pct", 0),
        "message": "canceled" if canceled_now else "cancel requested",
    }
    attempt_pair = _cancel_attempt_pair(current_progress)
    if attempt_pair is not None:
        attempt_id, attempt_started_at = attempt_pair
        cancel_progress_by_name["attempt_id"] = attempt_id
        cancel_progress_by_name["attempt_started_at"] = attempt_started_at
    return cancel_progress_by_name


def _cancel_attempt_pair(
    progress_by_name: dict[str, Any],
) -> tuple[str, str] | None:
    """Return a complete exact-attempt fence or no attempt fence."""

    attempt_id = progress_by_name.get("attempt_id")
    attempt_started_at = progress_by_name.get("attempt_started_at")
    if (
        type(attempt_id) is str
        and bool(attempt_id)
        and attempt_id == attempt_id.strip()
        and type(attempt_started_at) is str
        and bool(attempt_started_at)
        and attempt_started_at == attempt_started_at.strip()
    ):
        return attempt_id, attempt_started_at
    return None


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


def _is_queued_arq_cancel_completed(cancel_signal: dict[str, Any]) -> bool:
    """Return whether queued work was removed or its launched worker was fenced."""

    if cancel_signal.get("identity_mismatch") or cancel_signal.get(
        "identity_unavailable"
    ):
        return False
    if cancel_signal.get("removed"):
        return True
    if _has_terminalized_active_worker_cancel_signal(cancel_signal):
        return True
    kubernetes = cancel_signal.get("kubernetes")
    if not isinstance(kubernetes, dict) or not kubernetes.get("enabled"):
        return False
    if kubernetes.get("error") or kubernetes.get("errors"):
        return False
    cancel_flag = cancel_signal.get("cancel_flag")
    if not isinstance(cancel_flag, dict) or not cancel_flag.get("redis"):
        return False
    items = kubernetes.get("items")
    return isinstance(items, list) and not items


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


def _arq_cleanup_identity(
    run: dict[str, Any],
) -> tuple[str, str, dict[str, Any], str, str]:
    """Resolve one run's adapter-bound queue and exact ARQ job ID."""

    run_id = str(run.get("run_id") or "").strip()
    importer = str(run.get("importer") or "").strip()
    adapter = _adapter_for_import_row(run)
    if not run_id or not importer or not adapter:
        raise ValueError("missing queue or job_id")
    queue = str(adapter["queue"])
    metrics_by_name = (
        run.get("metrics") if isinstance(run.get("metrics"), dict) else {}
    )
    job_id = str(metrics_by_name.get("job_id") or "").strip()
    if not job_id and adapter.get("job_prefix"):
        job_id = _enqueue_job_options(adapter, {"run_id": run_id})["_job_id"]
    if not job_id:
        raise ValueError("missing exact ARQ job ID")
    return run_id, importer, adapter, queue, job_id


def _is_arq_job_owned_by_run(
    raw_job_bytes: Any,
    *,
    adapter: dict[str, Any],
    run_id: str,
    importer: str,
) -> bool:
    """Validate the stored ARQ function and immutable run ownership."""

    try:
        job_definition = arq_deserialize_job(
            raw_job_bytes,
            deserializer=deserialize_job,
        )
        if not isinstance(job_definition.args, (list, tuple)) or len(
            job_definition.args
        ) != 1:
            return False
        job_payload_by_name = job_definition.args[0]
        return (
            job_definition.function == adapter["function"]
            and job_definition.kwargs == {}
            and isinstance(job_payload_by_name, dict)
            and job_payload_by_name.get("run_id") == run_id
            and (
                "importer" not in job_payload_by_name
                or job_payload_by_name.get("importer") == importer
            )
        )
    except Exception:
        return False


def _arq_cleanup_refusal(
    queue: str,
    job_id: str,
    code: str,
    reason: str,
) -> dict[str, Any]:
    return {
        "redis": True,
        "queue": queue,
        "job_id": job_id,
        "removed": False,
        code: True,
        "reason": reason,
    }


def _arq_cleanup_outcome(
    queue: str,
    job_id: str,
    transaction_results: list[Any],
) -> dict[str, Any]:
    removed_count = int(transaction_results[0] or 0)
    deleted_key_count = int(transaction_results[1] or 0)
    return {
        "redis": True,
        "queue": queue,
        "job_id": job_id,
        "removed": removed_count > 0,
        "deleted_job_key": deleted_key_count > 0,
        "deleted_keys": deleted_key_count,
    }


async def _remove_queued_job(run: dict[str, Any]) -> dict[str, Any]:
    """Atomically remove exact ARQ state after validating run ownership."""

    try:
        run_id, importer, adapter, queue, job_id = _arq_cleanup_identity(run)
    except ValueError as exc:
        return {"redis": False, "removed": False, "identity_unavailable": True, "reason": str(exc)}
    job_key = f"arq:job:{job_id}"
    try:
        redis = await create_pool(
            build_redis_settings(),
            job_serializer=serialize_job,
            job_deserializer=deserialize_job,
        )
        async with redis.pipeline(transaction=True) as pipe:
            await pipe.watch(job_key)
            raw_job_bytes = await pipe.get(job_key)
            if raw_job_bytes is None:
                return _arq_cleanup_refusal(
                    queue,
                    job_id,
                    "identity_unavailable",
                    "ARQ job payload is unavailable",
                )
            if not _is_arq_job_owned_by_run(
                raw_job_bytes,
                adapter=adapter,
                run_id=run_id,
                importer=importer,
            ):
                return _arq_cleanup_refusal(
                    queue,
                    job_id,
                    "identity_mismatch",
                    "ARQ job payload does not match import run",
                )
            pipe.multi()
            pipe.zrem(queue, job_id)
            pipe.delete(
                job_key,
                f"arq:retry:{job_id}",
                f"arq:in-progress:{job_id}",
                f"arq:result:{job_id}",
            )
            transaction_results = await pipe.execute()
        return _arq_cleanup_outcome(queue, job_id, transaction_results)
    except WatchError:
        return {
            "redis": False,
            "queue": queue,
            "job_id": job_id,
            "removed": False,
            "reason": "ARQ job changed during cleanup",
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
    if current_params_by_name.get("frozen_rate_file_set_protected") is True:
        raise ValueError(
            "protected frozen runs cannot be retried through the public API"
        )
    if current_params_by_name.get(DIRECT_RATE_FILE_PUBLIC_MARKER) is True:
        raise ValueError(
            "protected direct runs cannot be retried through the public API"
        )
    if current_run_map.get("importer") == "ptg" and (
        _has_ptg_full_rebuild_control(current_params_by_name)
        or _has_ptg_full_rebuild_control(retry_params_by_name)
    ):
        raise ValueError(
            "full rebuild runs cannot be retried; create a new controlled "
            "rebuild attempt"
        )
    child_params_by_name = {
        **current_params_by_name,
        **retry_params_by_name,
    }
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
        return _params_for_import_run_storage(
            str(current_run_map.get("importer") or ""),
            child_params_by_name,
        )
    root_run_id = str(
        current_params_by_name.get("provider_directory_pagination_root_run_id")
        or retry_params_by_name.get("provider_directory_pagination_root_run_id")
        or run_id
    ).strip()
    child_params_by_name["retry_of_run_id"] = run_id
    child_params_by_name["provider_directory_pagination_root_run_id"] = root_run_id
    return child_params_by_name


def _has_ptg_full_rebuild_control(params_by_name: dict[str, Any]) -> bool:
    return any(
        name in params_by_name
        for name in (
            _PTG_FULL_REBUILD_TOKEN_PARAM,
            _PTG_FULL_REBUILD_SCOPE_PARAM,
            _PTG_FULL_REBUILD_MARKER_PARAM,
        )
    )


async def retry_import_run(run_id: str, payload: dict[str, Any]) -> tuple[dict[str, Any], bool] | None:
    """Create a retry derived from an existing import run."""

    current = await get_import_run(run_id)
    if not current:
        return None
    if str(current.get("importer") or "") == "ptg":
        await require_not_wave_owned_run(db, run_id)
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
