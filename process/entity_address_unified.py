# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import datetime
import hashlib
import importlib
import logging
import os
import re
import time
from dataclasses import dataclass
from typing import Iterable, Mapping

from arq import create_pool
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateColumn

from api import ptg2_geo_projection as geo_projection
from api.ptg2_geo_policy import (
    provider_address_identity_coherence_sql,
    provider_address_identity_reference_joins_sql,
    provider_address_point_coherence_sql,
    provider_address_point_reference_join_sql,
)
from db.models import (
    EntityAddressEvidence,
    EntityAddressMedicationBridge,
    EntityAddressNetworkBridge,
    EntityAddressPlanBridge,
    EntityAddressProcedureBridge,
    EntityAddressUnified,
    FacilityAnchorNPICandidate,
    db,
)
from process.control_lifecycle import mark_control_run
from process.ext import address_alias_sql
from process.ext.address_format import (
    ADDRESS_FORMAT_FUNCTION,
    ADDRESS_FORMAT_SOURCE,
    ADDRESS_FORMAT_VERSION,
)
from process.ext.utils import ensure_database, make_class, my_init_db, print_time_info
from process.live_progress import enqueue_live_progress, write_live_progress
from process import provider_directory_profile as profile_artifact
from process.redis_config import build_redis_settings
from process.serialization import deserialize_job, serialize_job

logger = logging.getLogger(__name__)

ENTITY_ADDRESS_UNIFIED_QUEUE_NAME = "arq:EntityAddressUnified"
POSTGRES_IDENTIFIER_MAX_LENGTH = 63
DEFAULT_MIN_ROWS = 1_000_000
DEFAULT_TEST_LIMIT_PER_SOURCE = 20_000
DEFAULT_SOURCE_CONCURRENCY = 3
DEFAULT_AGGREGATE_SHARDS = 16
DEFAULT_AGGREGATE_CONCURRENCY = 4
DEFAULT_SOURCE_TABLE_SHARDS = 1
DEFAULT_ENRICH_SHARDS = 1
DEFAULT_ENRICH_CONCURRENCY = 1
DEFAULT_EVIDENCE_SHARDS = 16
DEFAULT_EVIDENCE_CONCURRENCY = 4
DEFAULT_INLINE_SOURCE_EVIDENCE = True
DEFAULT_SUPPORT_STAGE_CONCURRENCY = 4
DEFAULT_STAGE_INDEX_CONCURRENCY = 4
DEFAULT_SUPPORT_INDEX_CONCURRENCY = 2
DEFAULT_FACILITY_CANDIDATE_SHARDS = 4
DEFAULT_PROCEDURE_BRIDGE_SHARDS = 1
DEFAULT_MEDICATION_BRIDGE_SHARDS = 1
DEFAULT_SUPPORT_CODE_LOCATION_INDEXES = False
DEFAULT_SUPPORT_HEAP_LOAD = True
DEFAULT_BUILD_NETWORK_BRIDGE = False
DEFAULT_BUILD_CODE_BRIDGES = True
DEFAULT_BUILD_FACILITY_CANDIDATES = True
DEFAULT_SERVING_ONLY_REFRESH = False
DEFAULT_SPLIT_ARRAY_AGGREGATES = False
DEFAULT_REQUIRE_INLINE_SOURCE_EVIDENCE = False
DEFAULT_UNLOGGED_STAGE = False
DEFAULT_STAGE_INDEX_PROFILE = "all"
DEFAULT_POST_PUBLISH_INDEX_PROFILE = "none"
DEFAULT_POST_PUBLISH_INDEX_CONCURRENTLY = True
DEFAULT_DEFER_PUBLISH_VALIDATION = False
DEFAULT_RAW_GROUP_INDEX_PROFILE = "group"
DEFAULT_AGGREGATE_SOURCE_RECORD_IDS = True
DEFAULT_COMPACT_SOURCE_RECORD_IDS = True
DEFAULT_COMPACT_SOURCE_RECORD_IDS_BY_REWRITE = False
DEFAULT_FINAL_SUMMARY_COUNTS = True
DEFAULT_KEEP_RAW_STAGE = False
DEFAULT_TRUST_SOURCE_ADDRESS_KEY = True
DEFAULT_SQL_WORK_MEM = "256MB"
DEFAULT_SQL_MAINTENANCE_WORK_MEM = "2GB"
DEFAULT_SQL_TEMP_FILE_LIMIT = "128GB"
DEFAULT_SQL_LOCK_TIMEOUT = "30s"
DEFAULT_SQL_STATEMENT_TIMEOUT = "0"
DEFAULT_SQL_SYNCHRONOUS_COMMIT = "off"
DEFAULT_SQL_JIT = "off"
DEFAULT_CUTOVER_LOCK_TIMEOUT = "50ms"
DEFAULT_CUTOVER_RETRY_ATTEMPTS = 4
DEFAULT_CUTOVER_RETRY_BACKOFF_MS = 25
DEFAULT_CUTOVER_RETRY_MAX_BACKOFF_MS = 100
DEFAULT_PROVIDER_DIRECTORY_PARTIAL_SCOPE = "latest-run"
PROVIDER_DIRECTORY_PARTIAL_SCOPE_INDEX = (
    "provider_directory_address_overlay_source_run_resource_idx"
)
DEFAULT_PROVIDER_DIRECTORY_SOURCE_BATCH_SIZE = 100
ENTITY_ADDRESS_REFRESH_MODE_FULL = "full"
ENTITY_ADDRESS_REFRESH_MODE_PROVIDER_DIRECTORY_PARTIAL = "provider-directory-partial"
ENTITY_ADDRESS_REFRESH_MODES = {
    ENTITY_ADDRESS_REFRESH_MODE_FULL,
    ENTITY_ADDRESS_REFRESH_MODE_PROVIDER_DIRECTORY_PARTIAL,
}
ARCHIVE_IDENTITY_VERSION = "v2"
BASE_ADDRESS_VERSION = f"address_archive_v2:v2+fmt-v{ADDRESS_FORMAT_VERSION}"
ALIAS_BASE_ADDRESS_VERSION_PREFIX = f"{BASE_ADDRESS_VERSION}+alias-v1:g"
ARCHIVE_COORDINATE_EPSILON_DEGREES = "0.0000001"
SUPPORT_TABLE_MODELS = (
    EntityAddressEvidence,
    EntityAddressPlanBridge,
    EntityAddressNetworkBridge,
    EntityAddressProcedureBridge,
    EntityAddressMedicationBridge,
    FacilityAnchorNPICandidate,
)
PROVIDER_DIRECTORY_DATASET_FENCE_TABLES = (
    "provider_directory_source",
    "provider_directory_address_overlay",
    "provider_directory_endpoint_dataset",
    "provider_directory_dataset_resource",
)
PROVIDER_DIRECTORY_COMPATIBILITY_ADDRESS_TABLES = (
    "provider_directory_practitioner",
    "provider_directory_organization",
    "provider_directory_location",
    "provider_directory_practitioner_role",
    "provider_directory_organization_affiliation",
)

_PROVIDER_DIRECTORY_CURRENT_OVERLAY_CTES_TEMPLATE = """
WITH requested_sources AS MATERIALIZED (
    SELECT
        source.source_id::varchar AS source_id,
        source.endpoint_id::varchar AS endpoint_id
      FROM {source_ref} AS source
      {requested_source_filter}
), endpoint_aliases AS MATERIALIZED (
    SELECT
        sibling.source_id::varchar AS source_id,
        sibling.endpoint_id::varchar AS endpoint_id
      FROM {source_ref} AS sibling
      JOIN (
            SELECT DISTINCT endpoint_id
              FROM requested_sources
             WHERE endpoint_id IS NOT NULL
      ) AS selected_endpoint
        ON selected_endpoint.endpoint_id = sibling.endpoint_id
), current_endpoint_counts AS MATERIALIZED (
    SELECT dataset.endpoint_id
      FROM {dataset_ref} AS dataset
     WHERE dataset.is_current IS TRUE
  GROUP BY dataset.endpoint_id
    HAVING COUNT(*) = 1
), current_datasets AS MATERIALIZED (
    SELECT
        dataset.endpoint_id::varchar AS endpoint_id,
        dataset.dataset_id::varchar AS dataset_id,
        COALESCE(dataset.acquisition_root_run_id, dataset.import_run_id)::varchar AS run_id,
        dataset.published_at
      FROM {dataset_ref} AS dataset
      JOIN current_endpoint_counts AS current_endpoint
        ON current_endpoint.endpoint_id = dataset.endpoint_id
     WHERE dataset.is_current IS TRUE
       AND dataset.status = 'published'
       AND dataset.published_at IS NOT NULL
       AND dataset.superseded_at IS NULL
       AND COALESCE(dataset.acquisition_root_run_id, dataset.import_run_id) IS NOT NULL
), {affected_overlay_ctes}current_overlay AS MATERIALIZED (
    SELECT
        overlay.*,
        dataset.dataset_id,
        dataset.run_id AS dataset_run_id,
        dataset.published_at AS dataset_published_at
      FROM {current_overlay_ref} AS overlay
      JOIN endpoint_aliases AS alias
        ON alias.source_id = overlay.source_id
      JOIN current_datasets AS dataset
        ON dataset.endpoint_id = alias.endpoint_id
     WHERE overlay.last_seen_run_id = dataset.run_id
       {run_filter}
       AND EXISTS (
            SELECT 1
              FROM {dataset_resource_ref} AS dataset_resource
             WHERE dataset_resource.dataset_id = dataset.dataset_id
               AND dataset_resource.resource_type = overlay.resource_type
               AND dataset_resource.resource_id = overlay.resource_id
       )
)
"""

_PROVIDER_DIRECTORY_PARTIAL_OVERLAY_SOURCE_TEMPLATE = """
{current_overlay_ctes}
SELECT
    'npi'::varchar AS entity_type,
    overlay.npi::varchar AS entity_id,
    overlay.npi::bigint AS npi,
    NULL::bigint AS inferred_npi,
    NULL::float8 AS inference_confidence,
    NULL::varchar AS inference_method,
    {entity_name} AS entity_name,
    {entity_subtype} AS entity_subtype,
    'practice'::varchar AS type,
    {taxonomy_array} AS taxonomy_array,
    {plans_network_array} AS plans_network_array,
    {procedures_array} AS procedures_array,
    {medications_array} AS medications_array,
    ARRAY[]::varchar[] AS aca_plan_array,
    ARRAY[]::varchar[] AS aca_network_array,
    ARRAY[]::varchar[] AS ptg_plan_array,
    ARRAY[]::varchar[] AS ptg_source_array,
    ARRAY[]::varchar[] AS group_plan_array,
    '{base_address_version}'::varchar AS base_address_version,
    overlay.first_line::varchar AS first_line,
    overlay.second_line::varchar AS second_line,
    COALESCE(overlay.city_name, '')::varchar AS city_name,
    COALESCE(overlay.state_name, overlay.state_code, '')::varchar AS state_name,
    overlay.postal_code::varchar AS postal_code,
    COALESCE(NULLIF(overlay.country_code, ''), 'US')::varchar AS country_code,
    overlay.telephone_number::varchar AS telephone_number,
    overlay.fax_number::varchar AS fax_number,
    overlay.formatted_address::varchar AS formatted_address,
    overlay.lat::numeric AS lat,
    overlay.long::numeric AS long,
    NULL::date AS date_added,
    NULL::varchar AS place_id,
    overlay.address_key::uuid AS address_key,
    COALESCE(overlay.source_updated_at, overlay.published_at, NOW())::timestamp AS updated_at,
    'provider_directory_fhir'::varchar AS address_source,
    overlay.source_record_id::varchar AS source_record_id
  FROM current_overlay AS overlay
  {npi_join}
  {primary_npi_address_join}
 WHERE overlay.npi BETWEEN 1000000000 AND 9999999999
   AND {address_predicate}
"""

ENTITY_ADDRESS_UNIFIED_SERVING_STAGE_INDEXES = {
    "npi",
    "primary_npi",
    "coalesced_npi",
    "primary_state_city_npi",
    "primary_zip5_npi",
    "serving_zip5_npi",
    "serving_zip5_taxonomy",
    "primary_phone_npi",
    "service_phone_lookup_npi",
    "service_phone_digits_npi",
    "service_phone_number_npi",
    "service_address_key_npi",
    "service_premise_key_npi",
    "address_sources",
    # The API phone-fallback lookup filters "address_key = ANY(..) OR
    # premise_key = ANY(..)"; without a premise_key index the OR forces a full
    # seq scan of the serving table (~3M pages, ~5.7s per location search).
    "premise_key",
    "taxonomy_plans_network",
    "procedures_array",
    "medications_array",
    "geo_idx",
    "geo_bbox",
    "address_key",
}
STAGE_INDEX_PROFILES = {"all", "serving", "none"}
POST_PUBLISH_INDEX_PROFILES = {"all", "serving", "none"}
RAW_GROUP_INDEX_PROFILES = {"group", "shard"}
HOSPITAL_FACILITY_TAXONOMY_CODES = (
    "281P00000X",
    "281PC2000X",
    "282E00000X",
    "282J00000X",
    "282N00000X",
    "282NC0060X",
    "282NC2000X",
    "282NR1301X",
    "282NW0100X",
    "283Q00000X",
    "283X00000X",
    "283XC2000X",
    "284300000X",
    "286500000X",
    "2865M2000X",
    "2865X1600X",
)


@dataclass(frozen=True)
class _SupportStageStatement:
    label: str
    statement: str
    parallel: bool = True


@dataclass(frozen=True)
class _StageTableSwap:
    live_cls: object
    stage_cls: object


class _CutoverLockUnavailable(RuntimeError):
    pass


def _normalize_import_id(raw: str | None) -> str:
    if raw:
        cleaned = "".join(ch for ch in str(raw) if ch.isalnum())
        if cleaned:
            return cleaned[:32]
    return datetime.datetime.now().strftime("%Y%m%d")


def _archived_identifier(name: str, suffix: str = "_old") -> str:
    candidate = f"{name}{suffix}"
    if len(candidate) <= POSTGRES_IDENTIFIER_MAX_LENGTH:
        return candidate
    digest = hashlib.sha1(name.encode("utf-8")).hexdigest()[:8]
    trim_to = max(1, POSTGRES_IDENTIFIER_MAX_LENGTH - len(suffix) - len(digest) - 1)
    return f"{name[:trim_to]}_{digest}{suffix}"


def _validate_schema_name(schema: str) -> str:
    cleaned = (schema or "").strip()
    if not cleaned or not (cleaned[0].isalpha() or cleaned[0] == "_"):
        raise ValueError(f"Invalid schema name: {schema!r}")
    if not all(ch.isalnum() or ch == "_" for ch in cleaned):
        raise ValueError(f"Invalid schema name: {schema!r}")
    return cleaned


def _is_env_enabled(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _is_truthy(value) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _clean_optional(value) -> str | None:
    cleaned = str(value or "").strip()
    return cleaned or None


def _coerce_str_list(value) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        candidates = value.split(",")
    elif isinstance(value, (list, tuple, set)):
        candidates = list(value)
    else:
        candidates = [value]
    cleaned_values: list[str] = []
    seen_values: set[str] = set()
    for item in candidates:
        text = _clean_optional(item)
        if not text or text in seen_values:
            continue
        seen_values.add(text)
        cleaned_values.append(text)
    return cleaned_values


def _entity_address_refresh_mode(task: dict) -> str:
    raw = (
        task.get("refresh_mode")
        or task.get("mode")
        or task.get("refresh_scope")
        or os.getenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_REFRESH_MODE")
        or ""
    )
    normalized_mode = str(raw).strip().lower().replace("_", "-")
    if not normalized_mode:
        return ENTITY_ADDRESS_REFRESH_MODE_FULL
    if normalized_mode in {"full", "rebuild", "full-rebuild"}:
        return ENTITY_ADDRESS_REFRESH_MODE_FULL
    if normalized_mode in {"ptg-partial", "partial-ptg", "ptg", "ptg-source", "ptg-source-refresh"}:
        logger.warning(
            "entity-address-unified refresh_mode=%s is obsolete because PTG no longer "
            "publishes address rows; running a full unified-address refresh instead.",
            raw,
        )
        return ENTITY_ADDRESS_REFRESH_MODE_FULL
    if normalized_mode in {
        "provider-directory-partial",
        "partial-provider-directory",
        "provider-directory",
        "provider-directory-fhir",
        "fhir-provider-directory",
    }:
        return ENTITY_ADDRESS_REFRESH_MODE_PROVIDER_DIRECTORY_PARTIAL
    raise ValueError(
        f"Unsupported entity-address-unified refresh_mode {raw!r}; "
        f"expected one of {sorted(ENTITY_ADDRESS_REFRESH_MODES)}"
    )


def _entity_address_provider_directory_source_ids(task: dict) -> list[str]:
    explicit = (
        task.get("provider_directory_source_ids")
        or task.get("provider_directory_source_id")
        or os.getenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_PROVIDER_DIRECTORY_SOURCE_IDS")
        or os.getenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_PROVIDER_DIRECTORY_SOURCE_ID")
    )
    return _coerce_str_list(explicit)


def _entity_address_provider_directory_run_id(task: dict) -> str | None:
    return _clean_optional(
        task.get("provider_directory_run_id")
        or os.getenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_PROVIDER_DIRECTORY_RUN_ID")
    )


def _entity_address_provider_directory_dataset_id(task: dict) -> str | None:
    return _clean_optional(task.get("provider_directory_dataset_id"))


def _validate_provider_directory_dataset_fence_scope(
    *,
    dataset_id: str | None,
    source_ids: list[str],
    run_id: str | None,
    partial_scope: str | None,
) -> None:
    """Require an exact source and overlay run for an explicit dataset fence."""

    if dataset_id is None:
        return
    if len(source_ids) != 1 or run_id is None or partial_scope != "latest-run":
        raise ValueError(
            "provider_directory_dataset_id requires one explicit source, "
            "provider_directory_run_id, and provider_directory_partial_scope=latest-run"
        )


def _provider_directory_current_dataset_fence_query(db_schema: str) -> str:
    """Build the immutable publication fence query for one source."""

    return f"""
        SELECT dataset.dataset_id
          FROM {db_schema}.provider_directory_source AS source
          JOIN {db_schema}.provider_directory_endpoint_dataset AS dataset
            ON dataset.dataset_id = :expected_dataset_id
         WHERE source.source_id = :source_id
           AND dataset.acquisition_root_run_id = :expected_root_run_id
           AND dataset.status = 'published'
           AND dataset.is_current IS TRUE
           AND dataset.published_at IS NOT NULL
           AND dataset.superseded_at IS NULL
           AND jsonb_typeof(
                   dataset.publication_metadata_json::jsonb -> 'source_ids'
               ) = 'array'
           AND (
                   dataset.publication_metadata_json::jsonb -> 'source_ids'
               ) @> jsonb_build_array(source.source_id)
           AND NOT EXISTS (
                SELECT 1
                  FROM {db_schema}.provider_directory_endpoint_dataset AS competing
                 WHERE competing.dataset_id <> dataset.dataset_id
                   AND competing.status = 'published'
                   AND competing.is_current IS TRUE
                   AND competing.published_at IS NOT NULL
                   AND competing.superseded_at IS NULL
                   AND jsonb_typeof(
                           competing.publication_metadata_json::jsonb
                               -> 'source_ids'
                       ) = 'array'
                   AND (
                           competing.publication_metadata_json::jsonb
                               -> 'source_ids'
                       ) @> jsonb_build_array(source.source_id)
           )
         LIMIT 1;
        """


async def _assert_current_provider_directory_dataset(
    db_schema: str,
    *,
    source_id: str,
    expected_dataset_id: str,
    expected_root_run_id: str,
) -> None:
    """Fail when the source no longer identifies one exact current dataset."""

    dataset_row = await db.first(
        _provider_directory_current_dataset_fence_query(db_schema),
        source_id=source_id,
        expected_dataset_id=expected_dataset_id,
        expected_root_run_id=expected_root_run_id,
    )
    if dataset_row is None or _clean_optional(
        _row_mapping(dataset_row).get("dataset_id")
    ) != expected_dataset_id:
        raise RuntimeError(
            "entity-address-unified Provider Directory dataset fence changed"
        )
    provider_directory_fhir = importlib.import_module(
        "process.provider_directory_fhir"
    )
    try:
        await (
            provider_directory_fhir.ensure_provider_directory_published_source_alias(
                source_id=source_id,
                expected_dataset_id=expected_dataset_id,
                expected_root_run_id=expected_root_run_id,
            )
        )
    except RuntimeError as exc:
        raise RuntimeError(
            "entity-address-unified Provider Directory dataset fence changed"
        ) from exc


def _provider_directory_source_batch_size(task: dict) -> int:
    raw = (
        task.get("provider_directory_source_batch_size")
        or task.get("provider_directory_batch_size")
        or os.getenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_PROVIDER_DIRECTORY_SOURCE_BATCH_SIZE")
        or os.getenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_PROVIDER_DIRECTORY_BATCH_SIZE")
    )
    if raw in (None, ""):
        return DEFAULT_PROVIDER_DIRECTORY_SOURCE_BATCH_SIZE
    return max(int(raw), 0)


def _provider_directory_source_id_batches(
    source_ids: list[str] | tuple[str, ...] | None,
    batch_size: int,
) -> list[list[str]]:
    ids = list(source_ids or [])
    if not ids or batch_size <= 0 or len(ids) <= batch_size:
        return [ids]
    return [ids[index : index + batch_size] for index in range(0, len(ids), batch_size)]


def _entity_address_provider_directory_partial_scope(task: dict) -> str:
    raw = (
        task.get("provider_directory_partial_scope")
        or os.getenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_PROVIDER_DIRECTORY_PARTIAL_SCOPE")
        or DEFAULT_PROVIDER_DIRECTORY_PARTIAL_SCOPE
    )
    value = str(raw).strip().lower().replace("_", "-")
    if value in {"latest", "latest-run", "latest-complete", "latest-completed-run"}:
        return "latest-run"
    if value in {"all", "full", "unscoped"}:
        return "all"
    raise ValueError(
        "Unsupported provider_directory_partial_scope "
        f"{raw!r}; expected latest-run or all"
    )


def _is_publish_requested(task: dict, *, test_mode: bool) -> bool:
    """Return whether this run should publish staged entity-address tables.

    Bounded/test pilots are stage-only by default so they can prove runtime
    behavior without replacing the live serving table with a small sample.
    """

    if task.get("skip_publish") not in (None, ""):
        return not _is_truthy(task.get("skip_publish"))
    if task.get("publish") not in (None, ""):
        return _is_truthy(task.get("publish"))
    env_publish = os.getenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_PUBLISH")
    if env_publish not in (None, ""):
        return _is_truthy(env_publish)
    env_skip = os.getenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_SKIP_PUBLISH")
    if env_skip not in (None, ""):
        return not _is_truthy(env_skip)
    return not test_mode


def _is_task_or_env_enabled(task: dict, key: str, env_name: str, default: bool) -> bool:
    if task.get(key) not in (None, ""):
        return _is_truthy(task.get(key))
    return _is_env_enabled(env_name, default)


def _env_int(name: str, default: int, minimum: int = 0) -> int:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return default
    return max(int(raw), minimum)


def _env_sql_setting(name: str, default: str | None) -> str | None:
    raw = os.getenv(name)
    if raw is None:
        return default
    value = str(raw).strip()
    return value or None


def _sql_literal(value: str | None) -> str:
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def _location_key_shard_filter_sql(
    expression: str,
    *,
    shards: int = 1,
    shard: int | None = None,
) -> str:
    shard_count = max(int(shards or 1), 1)
    if shard_count <= 1:
        return ""
    shard_index = 0 if shard is None else int(shard)
    return f"""
       AND (((hashtext({expression}) % {shard_count}) + {shard_count}) % {shard_count}) = {shard_index}
    """


def _entity_address_sql_settings() -> list[tuple[str, str]]:
    """Postgres settings for each heavy entity-address SQL statement.

    These are intentionally scoped with SET LOCAL inside the statement
    transaction so the importer can use PTG-style tuned bulk execution without
    leaking settings into unrelated DB work.
    """

    candidates = (
        ("work_mem", "HLTHPRT_ENTITY_ADDRESS_UNIFIED_WORK_MEM", DEFAULT_SQL_WORK_MEM),
        (
            "maintenance_work_mem",
            "HLTHPRT_ENTITY_ADDRESS_UNIFIED_MAINTENANCE_WORK_MEM",
            DEFAULT_SQL_MAINTENANCE_WORK_MEM,
        ),
        (
            "temp_file_limit",
            "HLTHPRT_ENTITY_ADDRESS_UNIFIED_TEMP_FILE_LIMIT",
            DEFAULT_SQL_TEMP_FILE_LIMIT,
        ),
        (
            "lock_timeout",
            "HLTHPRT_ENTITY_ADDRESS_UNIFIED_LOCK_TIMEOUT",
            DEFAULT_SQL_LOCK_TIMEOUT,
        ),
        (
            "statement_timeout",
            "HLTHPRT_ENTITY_ADDRESS_UNIFIED_STATEMENT_TIMEOUT",
            DEFAULT_SQL_STATEMENT_TIMEOUT,
        ),
        (
            "synchronous_commit",
            "HLTHPRT_ENTITY_ADDRESS_UNIFIED_SYNCHRONOUS_COMMIT",
            DEFAULT_SQL_SYNCHRONOUS_COMMIT,
        ),
        ("jit", "HLTHPRT_ENTITY_ADDRESS_UNIFIED_JIT", DEFAULT_SQL_JIT),
        (
            "max_parallel_workers_per_gather",
            "HLTHPRT_ENTITY_ADDRESS_UNIFIED_MAX_PARALLEL_WORKERS_PER_GATHER",
            None,
        ),
    )
    return [
        (setting, setting_value)
        for setting, env_name, default in candidates
        if (setting_value := _env_sql_setting(env_name, default)) is not None
    ]


def _format_seconds(seconds: float) -> str:
    if seconds >= 3600:
        return f"{seconds / 3600:.1f}h"
    if seconds >= 60:
        return f"{seconds / 60:.1f}m"
    return f"{seconds:.1f}s"


def _record_phase_timing(
    context: dict,
    phase: str,
    elapsed_seconds: float,
    rowcount: int | None,
) -> None:
    timings = context.setdefault("phase_timings", {})
    entry = timings.setdefault(
        phase,
        {
            "count": 0,
            "seconds": 0.0,
            "max_seconds": 0.0,
            "first_started_at": None,
            "last_finished_at": None,
            "wall_seconds": 0.0,
            "rows": 0,
        },
    )
    finished_at = time.time()
    started_at = finished_at - float(elapsed_seconds or 0.0)
    entry["count"] = int(entry.get("count") or 0) + 1
    entry["seconds"] = round(float(entry.get("seconds") or 0.0) + elapsed_seconds, 3)
    entry["max_seconds"] = round(max(float(entry.get("max_seconds") or 0.0), elapsed_seconds), 3)
    first_started_at = entry.get("first_started_at")
    last_finished_at = entry.get("last_finished_at")
    if first_started_at is None or started_at < float(first_started_at):
        entry["first_started_at"] = round(started_at, 6)
    if last_finished_at is None or finished_at > float(last_finished_at):
        entry["last_finished_at"] = round(finished_at, 6)
    if entry.get("first_started_at") is not None and entry.get("last_finished_at") is not None:
        entry["wall_seconds"] = round(
            float(entry["last_finished_at"]) - float(entry["first_started_at"]),
            3,
        )
    if rowcount is not None and rowcount >= 0:
        entry["rows"] = int(entry.get("rows") or 0) + int(rowcount)


def _coerce_rowcount(value) -> int | None:
    if isinstance(value, int):
        return value
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _int_context_metric(context: dict, name: str) -> int:
    try:
        return int(context.get(name) or 0)
    except (TypeError, ValueError):
        return 0


def _runtime_config_metrics(context: dict) -> dict:
    return {
        "inline_source_evidence": bool(context.get("inline_source_evidence")),
        "split_array_aggregates": bool(context.get("split_array_aggregates")),
        "source_table_shards": _int_context_metric(context, "source_table_shards"),
        "source_select_count": _int_context_metric(context, "source_select_count"),
        "source_concurrency": _int_context_metric(context, "source_concurrency"),
        "raw_location_key_index_skipped": bool(context.get("raw_location_key_index_skipped")),
        "enrich_shards": _int_context_metric(context, "enrich_shards"),
        "enrich_concurrency": _int_context_metric(context, "enrich_concurrency"),
        "aggregate_shards": _int_context_metric(context, "aggregate_shards"),
        "aggregate_concurrency": _int_context_metric(context, "aggregate_concurrency"),
        "stage_index_concurrency": _int_context_metric(context, "stage_index_concurrency"),
        "stage_index_profile": str(context.get("stage_index_profile") or DEFAULT_STAGE_INDEX_PROFILE),
        "post_publish_index_profile": str(
            context.get("post_publish_index_profile") or DEFAULT_POST_PUBLISH_INDEX_PROFILE
        ),
        "post_publish_index_concurrency": _int_context_metric(context, "post_publish_index_concurrency"),
        "post_publish_index_concurrently": bool(
            context.get("post_publish_index_concurrently", DEFAULT_POST_PUBLISH_INDEX_CONCURRENTLY)
        ),
        "post_publish_index_pending": bool(context.get("post_publish_index_pending")),
        "post_publish_index_total": _int_context_metric(context, "post_publish_index_total"),
        "post_publish_index_completed": _int_context_metric(context, "post_publish_index_completed"),
        "post_publish_index_timings": list(context.get("post_publish_index_timings") or []),
        "post_publish_index_error": context.get("post_publish_index_error"),
        "post_publish_skipped_indexes": list(context.get("post_publish_skipped_indexes") or []),
        "raw_group_index_profile": str(context.get("raw_group_index_profile") or DEFAULT_RAW_GROUP_INDEX_PROFILE),
        "stage_index_timings": list(context.get("stage_index_timings") or []),
        "aggregate_source_record_ids": bool(
            context.get("aggregate_source_record_ids", DEFAULT_AGGREGATE_SOURCE_RECORD_IDS)
        ),
        "final_summary_counts": bool(context.get("final_summary_counts", DEFAULT_FINAL_SUMMARY_COUNTS)),
        "raw_stage_kept": bool(context.get("raw_stage_kept")),
        "unlogged_stage": bool(context.get("unlogged_stage")),
        "stage_persistence": context.get("stage_persistence"),
        "cutover_attempts": _int_context_metric(context, "cutover_attempts"),
        "published_elapsed_seconds": (
            round(float(context.get("published_elapsed_seconds")), 3)
            if context.get("published_elapsed_seconds") is not None
            else None
        ),
    }


async def _status_with_entity_address_tuning(statement: str) -> int | None:
    settings = _entity_address_sql_settings()
    acquire = getattr(db, "acquire", None)
    if not settings or not callable(acquire):
        rowcount = await db.status(statement)
        return _coerce_rowcount(rowcount)

    async with db.acquire() as conn:
        for index, (name, value) in enumerate(settings):
            savepoint = f"entity_address_sql_setting_{index}"
            await conn.status(f"SAVEPOINT {savepoint};")
            try:
                await conn.status(f"SET LOCAL {name} = {_sql_literal(value)};")
                await conn.status(f"RELEASE SAVEPOINT {savepoint};")
            except Exception as exc:
                await conn.status(f"ROLLBACK TO SAVEPOINT {savepoint};")
                await conn.status(f"RELEASE SAVEPOINT {savepoint};")
                if "permission denied to set parameter" in str(exc).lower():
                    logger.warning(
                        "Skipping unprivileged entity-address SQL setting %s=%s: %s",
                        name,
                        value,
                        exc,
                    )
                    continue
                raise
        rowcount = await conn.status(statement)
        return _coerce_rowcount(rowcount)


async def _run_sql_phase(
    statement: str,
    *,
    context: dict,
    phase: str,
    run_id: str | None = None,
    unit: str = "run",
    done: int | None = None,
    total: int | None = None,
    pct: float | None = None,
    message: str | None = None,
    emit_start: bool = False,
    emit_done: bool = False,
) -> int | None:
    started = time.monotonic()
    if run_id and emit_start:
        enqueue_live_progress(
            run_id=run_id,
            importer="entity-address-unified",
            status="running",
            phase=phase,
            unit=unit,
            done=done,
            total=total,
            pct=pct,
            message=message or phase,
            source="entity-address-unified-sql-progress",
        )
    rowcount = await _status_with_entity_address_tuning(statement)
    elapsed = time.monotonic() - started
    _record_phase_timing(context, phase, elapsed, rowcount)
    if run_id and emit_done:
        complete_message = message or phase
        if rowcount is not None and rowcount >= 0:
            complete_message = f"{complete_message}: {rowcount:,} row(s), {_format_seconds(elapsed)}"
        else:
            complete_message = f"{complete_message}: {_format_seconds(elapsed)}"
        enqueue_live_progress(
            run_id=run_id,
            importer="entity-address-unified",
            status="running",
            phase=phase,
            unit=unit,
            done=done,
            total=total,
            pct=pct,
            message=complete_message,
            source="entity-address-unified-sql-progress",
        )
    return rowcount


def _row_mapping(row) -> dict:
    mapping = getattr(row, "_mapping", None)
    if mapping is not None:
        return dict(mapping)
    if isinstance(row, dict):
        return row
    return dict(row)


async def _stage_summary_counts(db_schema: str, stage_table: str) -> dict[str, int]:
    rows = await db.all(
        f"""
        SELECT
            COUNT(*)::bigint AS staged_rows,
            COUNT(*) FILTER (WHERE entity_type = 'npi')::bigint AS npi_rows,
            COUNT(*) FILTER (WHERE inferred_npi IS NOT NULL)::bigint AS inferred_rows,
            COUNT(*) FILTER (WHERE multi_source_confirmed IS TRUE)::bigint AS multi_source_rows
          FROM {db_schema}.{stage_table};
        """
    )
    if not rows:
        return {
            "staged_rows": 0,
            "npi_rows": 0,
            "inferred_rows": 0,
            "multi_source_rows": 0,
        }
    row = _row_mapping(rows[0])
    return {
        "staged_rows": int(row.get("staged_rows") or 0),
        "npi_rows": int(row.get("npi_rows") or 0),
        "inferred_rows": int(row.get("inferred_rows") or 0),
        "multi_source_rows": int(row.get("multi_source_rows") or 0),
    }


def _phase_timing_rows(context: dict, phase: str) -> int:
    timings = context.get("phase_timings") if isinstance(context, dict) else None
    if not isinstance(timings, dict):
        return 0
    timing = timings.get(phase)
    if not isinstance(timing, dict):
        return 0
    try:
        return int(timing.get("rows") or 0)
    except (TypeError, ValueError):
        return 0


def _fallback_summary_counts(context: dict) -> dict[str, int] | None:
    replacement_rows = _int_context_metric(context, "partial_provider_directory_replacement_rows")
    if replacement_rows > 0:
        return {
            "staged_rows": replacement_rows,
            "npi_rows": 0,
            "inferred_rows": 0,
            "multi_source_rows": 0,
        }
    aggregated_rows = _phase_timing_rows(context, "entity-address-unified aggregating")
    if aggregated_rows > 0:
        return {
            "staged_rows": aggregated_rows,
            "npi_rows": 0,
            "inferred_rows": 0,
            "multi_source_rows": 0,
        }
    return None


def _hospital_facility_taxonomy_codes_sql(indent: str = "                ") -> str:
    return (",\n" + indent).join(_sql_literal(code) for code in HOSPITAL_FACILITY_TAXONOMY_CODES)


async def _ensure_schema_exists(db_schema: str) -> None:
    db_schema = _validate_schema_name(db_schema)
    try:
        await db.status(f"CREATE SCHEMA IF NOT EXISTS {db_schema};")
    except Exception as exc:
        exists = bool(await db.scalar(f"SELECT to_regnamespace('{db_schema}') IS NOT NULL;"))
        if exists:
            logger.warning(
                "Schema %s already exists but CREATE SCHEMA failed (%s); continuing",
                db_schema,
                exc,
            )
            return
        raise


def _stage_index_name(stage_table: str, index_name: str) -> str:
    return _archived_identifier(f"{stage_table}_idx", f"_{index_name}")


def _disable_autovacuum_sql(db_schema: str, table_name: str) -> str:
    return f"""
    ALTER TABLE {db_schema}.{table_name}
      SET (autovacuum_enabled = false, toast.autovacuum_enabled = false);
    """


def _set_unlogged_table_sql(db_schema: str, table_name: str) -> str:
    return f"ALTER TABLE {db_schema}.{table_name} SET UNLOGGED;"


def _set_logged_table_sql(db_schema: str, table_name: str) -> str:
    return f"ALTER TABLE {db_schema}.{table_name} SET LOGGED;"


def _is_support_code_location_index(stage_cls, index: dict) -> bool:
    index_name = index.get("name", "_".join(index.get("index_elements") or ()))
    if index_name != "code_location":
        return False
    table_name = getattr(stage_cls, "__tablename__", "") or ""
    main_table = getattr(stage_cls, "__main_table__", "") or ""
    code_bridge_tables = {
        EntityAddressProcedureBridge.__main_table__,
        EntityAddressMedicationBridge.__main_table__,
    }
    if main_table in code_bridge_tables:
        return True
    return any(table_name.startswith(f"{bridge_table}_") for bridge_table in code_bridge_tables)


def _stage_index_profile() -> str:
    if _is_env_enabled("HLTHPRT_ENTITY_ADDRESS_UNIFIED_DEFER_ADDITIONAL_INDEXES", False):
        return "none"
    raw = (os.getenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_STAGE_INDEX_PROFILE") or DEFAULT_STAGE_INDEX_PROFILE).strip().lower()
    if raw in STAGE_INDEX_PROFILES:
        return raw
    logger.warning(
        "Unsupported HLTHPRT_ENTITY_ADDRESS_UNIFIED_STAGE_INDEX_PROFILE=%r; using %s",
        raw,
        DEFAULT_STAGE_INDEX_PROFILE,
    )
    return DEFAULT_STAGE_INDEX_PROFILE


def _post_publish_index_profile() -> str:
    raw = (
        os.getenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_POST_PUBLISH_INDEX_PROFILE")
        or DEFAULT_POST_PUBLISH_INDEX_PROFILE
    ).strip().lower()
    if raw in POST_PUBLISH_INDEX_PROFILES:
        return raw
    logger.warning(
        "Unsupported HLTHPRT_ENTITY_ADDRESS_UNIFIED_POST_PUBLISH_INDEX_PROFILE=%r; using %s",
        raw,
        DEFAULT_POST_PUBLISH_INDEX_PROFILE,
    )
    return DEFAULT_POST_PUBLISH_INDEX_PROFILE


def _should_build_post_publish_concurrently() -> bool:
    return _is_env_enabled(
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_POST_PUBLISH_INDEX_CONCURRENTLY",
        DEFAULT_POST_PUBLISH_INDEX_CONCURRENTLY,
    )


def _should_defer_publish_validation() -> bool:
    return _is_env_enabled(
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_DEFER_PUBLISH_VALIDATION",
        DEFAULT_DEFER_PUBLISH_VALIDATION,
    )


def _should_aggregate_source_record_ids() -> bool:
    return _is_env_enabled(
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_AGGREGATE_SOURCE_RECORD_IDS",
        DEFAULT_AGGREGATE_SOURCE_RECORD_IDS,
    )


def _source_record_ids_select_sql() -> str:
    if _should_aggregate_source_record_ids():
        return (
            "ARRAY_REMOVE(ARRAY_AGG(DISTINCT source_record_id ORDER BY source_record_id), NULL)"
            "::varchar[] AS source_record_ids"
        )
    return (
        "COALESCE(ARRAY_REMOVE("
        "ARRAY_AGG(DISTINCT source_record_id ORDER BY source_record_id) "
        "FILTER (WHERE source_record_id LIKE 'provider_directory_fhir:%'), "
        "NULL), ARRAY[]::varchar[])::varchar[] AS source_record_ids"
    )


def _should_require_inline_evidence() -> bool:
    return _is_env_enabled(
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_REQUIRE_INLINE_SOURCE_EVIDENCE",
        DEFAULT_REQUIRE_INLINE_SOURCE_EVIDENCE,
    )


def _should_compute_final_summary_counts() -> bool:
    return _is_env_enabled(
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_FINAL_SUMMARY_COUNTS",
        DEFAULT_FINAL_SUMMARY_COUNTS,
    )


def _should_keep_raw_stage() -> bool:
    return _is_env_enabled(
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_KEEP_RAW_STAGE",
        DEFAULT_KEEP_RAW_STAGE,
    )


def _raw_group_index_profile() -> str:
    raw = (
        os.getenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_RAW_GROUP_INDEX_PROFILE")
        or DEFAULT_RAW_GROUP_INDEX_PROFILE
    ).strip().lower()
    if raw in RAW_GROUP_INDEX_PROFILES:
        return raw
    logger.warning(
        "Unsupported HLTHPRT_ENTITY_ADDRESS_UNIFIED_RAW_GROUP_INDEX_PROFILE=%r; using %s",
        raw,
        DEFAULT_RAW_GROUP_INDEX_PROFILE,
    )
    return DEFAULT_RAW_GROUP_INDEX_PROFILE


def _is_main_index_enabled(index_name: str, profile: str) -> bool:
    if profile == "all":
        return True
    if profile == "none":
        return False
    if profile == "serving":
        return index_name in ENTITY_ADDRESS_UNIFIED_SERVING_STAGE_INDEXES
    return True


def _post_publish_index_plan(
    db_schema: str,
    profile: str,
    *,
    build_concurrently: bool,
) -> tuple[list[tuple[str, str]], list[str]]:
    table_name = EntityAddressUnified.__main_table__
    statements: list[tuple[str, str]] = []
    skipped_indexes: list[str] = []
    for index in getattr(EntityAddressUnified, "__my_additional_indexes__", []) or []:
        index_name = index.get("name", "_".join(index.get("index_elements") or ()))
        if not _is_main_index_enabled(index_name, profile):
            skipped_indexes.append(f"{table_name}.{index_name}")
            continue
        using = f"USING {index.get('using')} " if index.get("using") else ""
        include = (
            f" INCLUDE ({', '.join(index.get('include') or ())})"
            if index.get("include")
            else ""
        )
        where = f" WHERE {index.get('where')}" if index.get("where") else ""
        live_index_name = f"{table_name}_idx_{index_name}"
        concurrently = "CONCURRENTLY " if build_concurrently else ""
        stmt = (
            f"CREATE INDEX {concurrently}IF NOT EXISTS {live_index_name} "
            f"ON {db_schema}.{table_name} {using}"
            f"({', '.join(index.get('index_elements'))}){include}{where};"
        )
        statements.append((index_name, stmt))
    return statements, skipped_indexes


def _is_stage_index_enabled(stage_cls, index: dict) -> bool:
    if _is_support_code_location_index(stage_cls, index):
        return _is_env_enabled(
            "HLTHPRT_ENTITY_ADDRESS_UNIFIED_SUPPORT_CODE_LOCATION_INDEXES",
            DEFAULT_SUPPORT_CODE_LOCATION_INDEXES,
        )
    if getattr(stage_cls, "__main_table__", "") != EntityAddressUnified.__main_table__:
        return True
    index_name = index.get("name", "_".join(index.get("index_elements") or ()))
    return _is_main_index_enabled(index_name, _stage_index_profile())


def _support_stage_classes(import_date: str) -> dict[type, type]:
    return {model: make_class(model, import_date) for model in SUPPORT_TABLE_MODELS}


def _compact_stage_table_name(stage_table: str) -> str:
    return _archived_identifier(stage_table, "_compact")


def _entity_address_unified_columns() -> list[str]:
    return [column.name for column in EntityAddressUnified.__table__.columns]


def _compacted_source_record_ids_expr(source_record_ids: str = "source_record_ids") -> str:
    return (
        "CASE "
        "WHEN address_sources @> ARRAY['provider_directory_fhir']::varchar[] THEN "
        "COALESCE(("
        "SELECT ARRAY_REMOVE(ARRAY_AGG(DISTINCT rid.rid ORDER BY rid.rid), NULL)::varchar[] "
        f"FROM unnest(COALESCE({source_record_ids}, ARRAY[]::varchar[])) AS rid(rid) "
        "WHERE rid.rid LIKE 'provider_directory_fhir:%'"
        "), ARRAY[]::varchar[]) "
        "ELSE ARRAY[]::varchar[] END"
    )


async def _replace_source_record_ids_metadata_column(
    db_schema: str,
    stage_table: str,
    phase_context: dict,
) -> None:
    """Replace the hot identifier column without rewriting the stage table."""

    compact_column = "source_record_ids_compact"
    phase = "entity-address-unified compacting hot rows metadata"
    statements = (
        f"""
        ALTER TABLE {db_schema}.{stage_table}
            DROP COLUMN IF EXISTS {compact_column};
        """,
        f"""
        ALTER TABLE {db_schema}.{stage_table}
            ADD COLUMN {compact_column} varchar[] NOT NULL DEFAULT '{{}}'::varchar[];
        """,
        f"""
        UPDATE {db_schema}.{stage_table}
           SET {compact_column} = {_compacted_source_record_ids_expr()}
         WHERE address_sources @> ARRAY['provider_directory_fhir']::varchar[];
        """,
        f"ALTER TABLE {db_schema}.{stage_table} DROP COLUMN source_record_ids;",
        f"""
        ALTER TABLE {db_schema}.{stage_table}
            RENAME COLUMN {compact_column} TO source_record_ids;
        """,
    )
    for statement in statements:
        await _run_sql_phase(statement, context=phase_context, phase=phase)


async def _compact_record_ids_by_metadata_reset(
    db_schema: str,
    stage_table: str,
    phase_context: dict,
) -> int:
    """Compact identifiers in place and return the stage row estimate."""

    row_estimate = int(
        await db.scalar(
            f"""
            SELECT GREATEST(COALESCE(c.reltuples, 0), 0)::bigint
              FROM pg_class c
              JOIN pg_namespace n
                ON n.oid = c.relnamespace
             WHERE n.nspname = {_sql_literal(db_schema)}
               AND c.relname = {_sql_literal(stage_table)};
            """
        )
        or 0
    )
    await _replace_source_record_ids_metadata_column(
        db_schema,
        stage_table,
        phase_context,
    )
    return row_estimate


async def _rewrite_compacted_source_record_ids_stage(
    db_schema: str,
    stage_table: str,
    phase_context: dict,
) -> int:
    """Rewrite the stage with compact identifiers and swap it into place."""

    compact_table = _compact_stage_table_name(stage_table)
    columns = _entity_address_unified_columns()
    columns_sql = ", ".join(columns)
    select_sql = ", ".join(
        f"{_compacted_source_record_ids_expr()} AS source_record_ids" if column == "source_record_ids" else column
        for column in columns
    )
    await _run_sql_phase(
        f"DROP TABLE IF EXISTS {db_schema}.{compact_table};",
        context=phase_context,
        phase="entity-address-unified compacting hot rows setup",
    )
    await _run_sql_phase(
        f"CREATE TABLE {db_schema}.{compact_table} "
        f"(LIKE {db_schema}.{stage_table} INCLUDING ALL);",
        context=phase_context,
        phase="entity-address-unified compacting hot rows setup",
    )
    rowcount = await _run_sql_phase(
        f"""
        INSERT INTO {db_schema}.{compact_table} ({columns_sql})
        SELECT {select_sql}
          FROM {db_schema}.{stage_table};
        """,
        context=phase_context,
        phase="entity-address-unified compacting hot rows",
    )
    await _run_sql_phase(
        f"DROP TABLE {db_schema}.{stage_table};",
        context=phase_context,
        phase="entity-address-unified compacting hot rows swap",
    )
    await _run_sql_phase(
        f"ALTER TABLE {db_schema}.{compact_table} RENAME TO {stage_table};",
        context=phase_context,
        phase="entity-address-unified compacting hot rows swap",
    )
    return int(rowcount or 0)


async def _compact_hot_row_source_record_ids(
    db_schema: str,
    stage_table: str,
    *,
    context: dict | None = None,
) -> int:
    """Compact source-record identifiers while preserving stage row identity."""

    phase_context = context if context is not None else {}
    if _is_env_enabled(
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_COMPACT_SOURCE_RECORD_IDS_BY_REWRITE",
        DEFAULT_COMPACT_SOURCE_RECORD_IDS_BY_REWRITE,
    ):
        return await _rewrite_compacted_source_record_ids_stage(
            db_schema,
            stage_table,
            phase_context,
        )
    return await _compact_record_ids_by_metadata_reset(
        db_schema,
        stage_table,
        phase_context,
    )


def _stage_index_statements(
    stage_cls,
    db_schema: str,
    indexes: list[dict],
    phase_context: dict,
) -> list[tuple[str, str]]:
    """Build enabled stage-index statements and record skipped indexes."""

    statements: list[tuple[str, str]] = []
    for index in indexes:
        index_name = index.get("name", "_".join(index.get("index_elements")))
        if not _is_stage_index_enabled(stage_cls, index):
            skipped_indexes = phase_context.setdefault("skipped_stage_indexes", [])
            skipped_indexes.append(f"{stage_cls.__tablename__}.{index_name}")
            continue
        using = f"USING {index.get('using')} " if index.get("using") else ""
        include = (
            f" INCLUDE ({', '.join(index.get('include') or ())})"
            if index.get("include")
            else ""
        )
        where = f" WHERE {index.get('where')}" if index.get("where") else ""
        stmt = (
            f"CREATE INDEX IF NOT EXISTS "
            f"{_stage_index_name(stage_cls.__tablename__, index_name)} "
            f"ON {db_schema}.{stage_cls.__tablename__} {using}"
            f"({', '.join(index.get('index_elements'))}){include}{where};"
        )
        statements.append((index_name, stmt))
    return statements


async def _build_stage_index(
    index_name: str,
    statement: str,
    phase_context: dict,
) -> None:
    """Build one stage index while retaining timing and PostGIS fallback."""

    started_at = time.time()
    try:
        await _run_sql_phase(
            statement,
            context=phase_context,
            phase="entity-address-unified indexing stage",
        )
    except Exception as exc:
        message = str(exc).lower()
        if "st_makepoint" in message or "geography" in message or "postgis" in message:
            logger.warning(
                "Skipping geo index %s because PostGIS is unavailable in current DB: %s",
                index_name,
                exc,
            )
            return
        raise
    finally:
        finished_at = time.time()
        timings = phase_context.setdefault("stage_index_timings", [])
        timings.append(
            {
                "index": index_name,
                "seconds": round(finished_at - started_at, 3),
                "started_at": round(started_at, 6),
                "finished_at": round(finished_at, 6),
            }
        )


async def _build_guarded_stage_index(
    index_name: str,
    statement: str,
    phase_context: dict,
    semaphore: asyncio.Semaphore,
) -> None:
    """Build one stage index inside the configured concurrency bound."""

    async with semaphore:
        await _build_stage_index(index_name, statement, phase_context)


async def _run_stage_index_statements(
    statements: list[tuple[str, str]],
    phase_context: dict,
    index_concurrency: int,
) -> None:
    """Run stage-index statements with the original ordering and failures."""

    if index_concurrency <= 1 or len(statements) == 1:
        for index_name, statement in statements:
            await _build_stage_index(index_name, statement, phase_context)
        return
    semaphore = asyncio.Semaphore(index_concurrency)
    index_results = await asyncio.gather(
        *(
            _build_guarded_stage_index(
                index_name,
                statement,
                phase_context,
                semaphore,
            )
            for index_name, statement in statements
        ),
        return_exceptions=True,
    )
    for index_result in index_results:
        if isinstance(index_result, BaseException):
            raise index_result


async def _create_stage_indexes(
    stage_cls,
    db_schema: str,
    *,
    context: dict | None = None,
) -> None:
    """Build configured indexes for one staged entity-address table."""

    phase_context = context if context is not None else {}
    if getattr(stage_cls, "__main_table__", "") == EntityAddressUnified.__main_table__:
        phase_context["stage_index_profile"] = _stage_index_profile()
    indexes = list(getattr(stage_cls, "__my_additional_indexes__", []) or [])
    if not indexes:
        return
    statements = _stage_index_statements(
        stage_cls,
        db_schema,
        indexes,
        phase_context,
    )

    if not statements:
        return

    index_concurrency = min(
        _env_int(
            "HLTHPRT_ENTITY_ADDRESS_UNIFIED_STAGE_INDEX_CONCURRENCY",
            DEFAULT_STAGE_INDEX_CONCURRENCY,
            minimum=1,
        ),
        len(statements),
    )
    phase_context["stage_index_concurrency"] = index_concurrency
    await _run_stage_index_statements(
        statements,
        phase_context,
        index_concurrency,
    )


async def _create_post_publish_indexes(
    db_schema: str,
    *,
    context: dict | None = None,
) -> None:
    """Build and analyze configured indexes on the published address table."""
    phase_context = context if context is not None else {}
    profile = _post_publish_index_profile()
    phase_context["post_publish_index_profile"] = profile
    should_build_concurrently = _should_build_post_publish_concurrently()
    phase_context["post_publish_index_concurrently"] = should_build_concurrently
    if profile == "none":
        phase_context["post_publish_index_pending"] = False
        phase_context["post_publish_index_total"] = 0
        phase_context["post_publish_index_completed"] = 0
        phase_context["post_publish_skipped_indexes"] = []
        return
    table_name = EntityAddressUnified.__main_table__
    statements, skipped_indexes = _post_publish_index_plan(
        db_schema,
        profile,
        build_concurrently=should_build_concurrently,
    )
    phase_context["post_publish_skipped_indexes"] = skipped_indexes

    async def _analyze_live_table() -> None:
        statement = f"ANALYZE {db_schema}.{table_name};"
        started_at = time.time()
        if should_build_concurrently and hasattr(db, "execute_ddl"):
            await db.execute_ddl(statement)
        else:
            await _run_sql_phase(
                statement,
                context=phase_context,
                phase="entity-address-unified post-publish analyze",
            )
        phase_context["post_publish_analyze_seconds"] = round(time.time() - started_at, 3)
        phase_context["post_publish_analyzed"] = True

    if not statements:
        phase_context["post_publish_index_pending"] = False
        phase_context["post_publish_index_total"] = 0
        phase_context["post_publish_index_completed"] = 0
        await _analyze_live_table()
        return

    phase_context["post_publish_index_pending"] = True
    phase_context["post_publish_index_total"] = len(statements)
    phase_context["post_publish_index_completed"] = 0

    if any(" USING gin " in stmt for _name, stmt in statements):
        # serving_zip5_taxonomy mixes a btree-typed expression into a GIN
        # index, which needs the btree_gin extension. It is a trusted
        # extension (DB owner can create it without superuser), so ensure it
        # here instead of failing the whole post-publish index pass on a
        # freshly provisioned database.
        ensure_extension = "CREATE EXTENSION IF NOT EXISTS btree_gin"
        if should_build_concurrently and hasattr(db, "execute_ddl"):
            await db.execute_ddl(ensure_extension)
        else:
            await _run_sql_phase(
                ensure_extension,
                context=phase_context,
                phase="entity-address-unified post-publish extension",
            )

    configured_index_concurrency = _env_int(
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_POST_PUBLISH_INDEX_CONCURRENCY",
        _env_int(
            "HLTHPRT_ENTITY_ADDRESS_UNIFIED_STAGE_INDEX_CONCURRENCY",
            DEFAULT_STAGE_INDEX_CONCURRENCY,
            minimum=1,
        ),
        minimum=1,
    )
    index_concurrency = 1 if should_build_concurrently else min(configured_index_concurrency, len(statements))
    phase_context["post_publish_index_concurrency"] = index_concurrency

    async def _is_post_publish_index_invalid(live_index_name: str) -> bool:
        invalid = await db.scalar(
            f"""
            SELECT 1
              FROM pg_class i
              JOIN pg_namespace n
                ON n.oid = i.relnamespace
              JOIN pg_index ix
                ON ix.indexrelid = i.oid
             WHERE n.nspname = {_sql_literal(db_schema)}
               AND i.relname = {_sql_literal(live_index_name)}
               AND ix.indisvalid IS FALSE
             LIMIT 1;
            """
        )
        return bool(invalid)

    async def _drop_invalid_index(live_index_name: str) -> None:
        if not await _is_post_publish_index_invalid(live_index_name):
            return
        drop_stmt = f"DROP INDEX {'CONCURRENTLY ' if should_build_concurrently else ''}IF EXISTS {db_schema}.{live_index_name};"
        if should_build_concurrently and hasattr(db, "execute_ddl"):
            await db.execute_ddl(drop_stmt)
        else:
            await _run_sql_phase(
                drop_stmt,
                context=phase_context,
                phase="entity-address-unified post-publish invalid index cleanup",
            )

    async def _build_index(index_name: str, stmt: str) -> None:
        started_at = time.time()
        live_index_name = f"{table_name}_idx_{index_name}"
        is_completed = False
        try:
            await _drop_invalid_index(live_index_name)
            if should_build_concurrently and hasattr(db, "execute_ddl"):
                await db.execute_ddl(stmt)
                _record_phase_timing(
                    phase_context,
                    "entity-address-unified post-publish indexing",
                    time.time() - started_at,
                    None,
                )
            else:
                await _run_sql_phase(
                    stmt,
                    context=phase_context,
                    phase="entity-address-unified post-publish indexing",
                )
            is_completed = True
        except Exception as exc:
            msg = str(exc).lower()
            if "st_makepoint" in msg or "geography" in msg or "postgis" in msg:
                logger.warning(
                    "Skipping post-publish geo index %s because PostGIS is unavailable in current DB: %s",
                    index_name,
                    exc,
                )
                is_completed = True
                return
            raise
        finally:
            finished_at = time.time()
            timings = phase_context.setdefault("post_publish_index_timings", [])
            timings.append(
                {
                    "index": index_name,
                    "seconds": round(finished_at - started_at, 3),
                    "started_at": round(started_at, 6),
                    "finished_at": round(finished_at, 6),
                }
            )
            if is_completed:
                phase_context["post_publish_index_completed"] = int(
                    phase_context.get("post_publish_index_completed") or 0
                ) + 1
                phase_context["post_publish_index_pending"] = (
                    int(phase_context.get("post_publish_index_completed") or 0)
                    < int(phase_context.get("post_publish_index_total") or 0)
                )

    if index_concurrency <= 1 or len(statements) == 1:
        for index_name, stmt in statements:
            await _build_index(index_name, stmt)
        await _analyze_live_table()
        return

    semaphore = asyncio.Semaphore(index_concurrency)

    async def _guarded(index_name: str, stmt: str) -> None:
        async with semaphore:
            await _build_index(index_name, stmt)

    index_results = await asyncio.gather(
        *(_guarded(index_name, stmt) for index_name, stmt in statements),
        return_exceptions=True,
    )
    for index_result in index_results:
        if isinstance(index_result, BaseException):
            raise index_result
    await _analyze_live_table()


async def _prepare_inference_stage_indexes(
    db_schema: str,
    stage_table: str,
    *,
    context: dict | None = None,
) -> None:
    phase_context = context if context is not None else {}
    await _run_sql_phase(
        f"""
        CREATE INDEX IF NOT EXISTS {stage_table}_idx_facility_unresolved_identity
        ON {db_schema}.{stage_table} (entity_subtype, entity_id, type, checksum)
        WHERE entity_type = 'facility_anchor'
          AND npi IS NULL
          AND inferred_npi IS NULL;
        """,
        context=phase_context,
        phase="entity-address-unified preparing inference indexes",
    )
    await _run_sql_phase(
        f"""
        CREATE INDEX IF NOT EXISTS {stage_table}_idx_facility_unresolved_address
        ON {db_schema}.{stage_table} (address_key, entity_subtype)
        WHERE entity_type = 'facility_anchor'
          AND npi IS NULL
          AND inferred_npi IS NULL
          AND address_key IS NOT NULL;
        """,
        context=phase_context,
        phase="entity-address-unified preparing inference indexes",
    )
    await _run_sql_phase(
        f"ANALYZE {db_schema}.{stage_table};",
        context=phase_context,
        phase="entity-address-unified preparing inference indexes",
    )


def _drop_stage_primary_key_sql(db_schema: str, table_name: str) -> str:
    return f"""
    DO $$
    DECLARE
        existing_constraint text;
    BEGIN
        SELECT c.conname
          INTO existing_constraint
          FROM pg_constraint c
          JOIN pg_class t
            ON t.oid = c.conrelid
          JOIN pg_namespace n
            ON n.oid = t.relnamespace
         WHERE n.nspname = {_sql_literal(db_schema)}
           AND t.relname = {_sql_literal(table_name)}
           AND c.contype = 'p'
         LIMIT 1;

        IF existing_constraint IS NOT NULL THEN
            EXECUTE format(
                'ALTER TABLE %I.%I DROP CONSTRAINT %I',
                {_sql_literal(db_schema)},
                {_sql_literal(table_name)},
                existing_constraint
            );
        END IF;
    END $$;
    """


_ENSURE_STAGE_PRIMARY_KEY_SQL = """
    DO $$
    DECLARE
        target_table_oid oid;
        conflicting_relation text;
        conflicting_table_oid oid;
        conflicting_constraint text;
        orphan_index text;
        resolved_constraint_name text := {constraint_name_literal};
    BEGIN
        SELECT t.oid
          INTO target_table_oid
          FROM pg_class t
          JOIN pg_namespace n
            ON n.oid = t.relnamespace
         WHERE n.nspname = {schema_literal}
           AND t.relname = {table_literal}
         LIMIT 1;

        IF NOT EXISTS (
            SELECT 1
              FROM pg_constraint c
             WHERE c.conrelid = target_table_oid
               AND c.contype = 'p'
        ) THEN
            SELECT i.relname, ix.indrelid, c.conname
              INTO conflicting_relation, conflicting_table_oid, conflicting_constraint
              FROM pg_class i
              JOIN pg_namespace n
                ON n.oid = i.relnamespace
              LEFT JOIN pg_index ix
                ON ix.indexrelid = i.oid
              LEFT JOIN pg_constraint c
                ON c.conindid = i.oid
             WHERE n.nspname = {schema_literal}
               AND i.relname = {constraint_name_literal}
             LIMIT 1;

            IF conflicting_relation IS NOT NULL
               AND conflicting_table_oid = target_table_oid
               AND conflicting_constraint IS NULL THEN
                orphan_index := conflicting_relation;
            ELSIF conflicting_relation IS NOT NULL
                  AND conflicting_table_oid = target_table_oid
                  AND conflicting_constraint IS NOT NULL THEN
                EXECUTE format(
                    'ALTER TABLE %I.%I DROP CONSTRAINT %I',
                    {schema_literal},
                    {table_literal},
                    conflicting_constraint
                );
            ELSIF conflicting_relation IS NOT NULL THEN
                resolved_constraint_name := LEFT({constraint_name_literal}, 54)
                    || '_'
                    || SUBSTRING(MD5(target_table_oid::text), 1, 8);
            END IF;

            IF orphan_index IS NOT NULL THEN
                EXECUTE format(
                    'DROP INDEX IF EXISTS %I.%I',
                    {schema_literal},
                    orphan_index
                );
            END IF;

            EXECUTE format(
                'ALTER TABLE %I.%I ADD CONSTRAINT %I PRIMARY KEY ({column_sql})',
                {schema_literal},
                {table_literal},
                resolved_constraint_name
            );
        END IF;
    END $$;
"""


def _ensure_stage_primary_key_sql(
    db_schema: str,
    table_name: str,
    primary_key_columns: Iterable[str],
) -> str:
    """Build SQL that restores the requested stage-table primary key."""
    columns = list(primary_key_columns)
    if not columns:
        return ""
    constraint_name = _archived_identifier(table_name, "_pkey")
    column_sql = ", ".join(columns)
    return _ENSURE_STAGE_PRIMARY_KEY_SQL.format(
        schema_literal=_sql_literal(db_schema),
        table_literal=_sql_literal(table_name),
        constraint_name_literal=_sql_literal(constraint_name),
        column_sql=column_sql,
    )


async def _ensure_stage_primary_key(
    stage_cls,
    db_schema: str,
    *,
    context: dict | None = None,
) -> None:
    if not hasattr(stage_cls, "__table__"):
        return
    primary_key_columns = [column.name for column in stage_cls.__table__.primary_key.columns]
    if not primary_key_columns:
        return
    await _run_sql_phase(
        _ensure_stage_primary_key_sql(
            db_schema,
            stage_cls.__tablename__,
            primary_key_columns,
        ),
        context=context,
        phase="entity-address-unified indexing support primary key",
    )


async def _prepare_support_stage_tables(db_schema: str, import_date: str) -> dict[type, type]:
    stage_classes = _support_stage_classes(import_date)
    should_use_heap_load = _is_env_enabled(
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_SUPPORT_HEAP_LOAD",
        DEFAULT_SUPPORT_HEAP_LOAD,
    )
    for stage_cls in stage_classes.values():
        await db.status(f"DROP TABLE IF EXISTS {db_schema}.{stage_cls.__tablename__};")
        await db.create_table(stage_cls.__table__, checkfirst=True)
        if should_use_heap_load:
            await db.status(_drop_stage_primary_key_sql(db_schema, stage_cls.__tablename__))
    return stage_classes


@dataclass
class _SupportIndexProgress:
    context: dict
    db_schema: str
    run_id: str | None
    concurrency: int
    total: int
    lock: asyncio.Lock
    completed: int = 0


async def _report_support_index_progress(
    progress: _SupportIndexProgress,
    index: int,
    table_name: str,
    *,
    is_completed: bool,
) -> None:
    """Emit serialized start or completion progress for one support table."""
    if not progress.run_id:
        return
    async with progress.lock:
        if is_completed:
            progress.completed += 1
        done = progress.completed
        message = (
            f"indexed support table {index}/{progress.total}: {table_name}"
            if is_completed
            else (
                f"indexing support table {index}/{progress.total}: {table_name} "
                f"(concurrency {progress.concurrency})"
            )
        )
        enqueue_live_progress(
            run_id=progress.run_id,
            importer="entity-address-unified",
            status="running",
            phase="entity-address-unified indexing support tables",
            unit="tables",
            done=done,
            total=progress.total,
            pct=99,
            message=message,
        )


async def _index_support_stage(
    progress: _SupportIndexProgress,
    index: int,
    stage_cls,
) -> None:
    """Restore a support table primary key and build its additional indexes."""
    table_name = stage_cls.__tablename__
    await _report_support_index_progress(
        progress,
        index,
        table_name,
        is_completed=False,
    )
    await _ensure_stage_primary_key(
        stage_cls,
        progress.db_schema,
        context=progress.context,
    )
    await _create_stage_indexes(
        stage_cls,
        progress.db_schema,
        context=progress.context,
    )
    await _report_support_index_progress(
        progress,
        index,
        table_name,
        is_completed=True,
    )


async def _run_concurrent_support_indexes(
    progress: _SupportIndexProgress,
    stage_table_classes: list[type],
) -> None:
    """Run support indexing with bounded concurrency and propagate cancellation."""
    semaphore = asyncio.Semaphore(progress.concurrency)

    async def _guarded(index: int, stage_cls) -> None:
        async with semaphore:
            await _index_support_stage(progress, index, stage_cls)

    index_results = await asyncio.gather(
        *(
            _guarded(index, stage_cls)
            for index, stage_cls in enumerate(stage_table_classes, start=1)
        ),
        return_exceptions=True,
    )
    for index_result in index_results:
        if isinstance(index_result, BaseException):
            raise index_result


async def _create_support_stage_indexes(
    stage_classes: dict[type, type],
    db_schema: str,
    *,
    context: dict | None = None,
    run_id: str | None = None,
) -> None:
    """Build support-table primary keys and indexes with bounded concurrency."""
    phase_context = context if context is not None else {}
    stage_table_classes = list(stage_classes.values())
    if not stage_table_classes:
        return
    index_concurrency = min(
        _env_int(
            "HLTHPRT_ENTITY_ADDRESS_UNIFIED_SUPPORT_INDEX_CONCURRENCY",
            DEFAULT_SUPPORT_INDEX_CONCURRENCY,
            minimum=1,
        ),
        len(stage_table_classes),
    )
    phase_context["support_stage_index_concurrency"] = index_concurrency
    progress = _SupportIndexProgress(
        context=phase_context,
        db_schema=db_schema,
        run_id=run_id,
        concurrency=index_concurrency,
        total=len(stage_table_classes),
        lock=asyncio.Lock(),
    )
    if index_concurrency <= 1 or len(stage_table_classes) == 1:
        for index, stage_cls in enumerate(stage_table_classes, start=1):
            await _index_support_stage(progress, index, stage_cls)
        return
    await _run_concurrent_support_indexes(progress, stage_table_classes)


async def _swap_stage_table(db_schema: str, live_cls, stage_cls) -> None:
    table = live_cls.__main_table__
    await db.status(f"DROP TABLE IF EXISTS {db_schema}.{table}_old;")
    await db.status(f"ALTER TABLE IF EXISTS {db_schema}.{table} RENAME TO {table}_old;")
    await db.status(f"ALTER TABLE {db_schema}.{stage_cls.__tablename__} RENAME TO {table};")

    archived = _archived_identifier(f"{table}_idx_primary")
    await db.status(f"DROP INDEX IF EXISTS {db_schema}.{archived};")
    await db.status(f"ALTER INDEX IF EXISTS {db_schema}.{table}_idx_primary RENAME TO {archived};")
    await db.status(
        f"ALTER INDEX IF EXISTS {db_schema}.{stage_cls.__tablename__}_idx_primary "
        f"RENAME TO {table}_idx_primary;"
    )

    for index in getattr(stage_cls, "__my_additional_indexes__", []) or []:
        index_name = index.get("name", "_".join(index.get("index_elements")))
        old_live_name = f"{table}_idx_{index_name}"
        archived_live_name = _archived_identifier(old_live_name)
        await db.status(f"DROP INDEX IF EXISTS {db_schema}.{archived_live_name};")
        await db.status(f"ALTER INDEX IF EXISTS {db_schema}.{old_live_name} RENAME TO {archived_live_name};")
        await db.status(
            f"ALTER INDEX IF EXISTS {db_schema}.{_stage_index_name(stage_cls.__tablename__, index_name)} "
            f"RENAME TO {old_live_name};"
        )


async def _stage_table_persistence(db_schema: str, table_name: str) -> str | None:
    value = await db.scalar(
        """
        SELECT c.relpersistence::text
          FROM pg_class AS c
          JOIN pg_namespace AS n ON n.oid = c.relnamespace
         WHERE n.nspname = :db_schema
           AND c.relname = :table_name
           AND c.relkind IN ('r', 'p');
        """,
        db_schema=db_schema,
        table_name=table_name,
    )
    return str(value) if value is not None else None


async def _ensure_promoted_stage_logged(db_schema: str, table_name: str) -> None:
    persistence = await _stage_table_persistence(db_schema, table_name)
    if persistence is None:
        raise RuntimeError(f"Entity-address cutover stage {db_schema}.{table_name} does not exist")
    if persistence != "p":
        logger.info("Converting entity-address stage %s.%s to LOGGED before cutover", db_schema, table_name)
        await db.status(_set_logged_table_sql(db_schema, table_name))
        persistence = await _stage_table_persistence(db_schema, table_name)
    if persistence != "p":
        raise RuntimeError(
            f"Entity-address cutover stage {db_schema}.{table_name} is not permanent: "
            f"relpersistence={persistence!r}"
        )


def _cutover_relation_sets(
    swaps: list[_StageTableSwap],
    support_stage_class_map: dict[type, type],
    *,
    partial_support_patch: bool,
    affected_group_table: str,
) -> tuple[list[str], list[str]]:
    relation_names: set[str] = set()
    required_names: set[str] = set()
    for swap in swaps:
        live_table = swap.live_cls.__main_table__
        stage_table = swap.stage_cls.__tablename__
        relation_names.update((live_table, f"{live_table}_old", stage_table))
        required_names.add(stage_table)
    if partial_support_patch:
        for live_cls, stage_cls in support_stage_class_map.items():
            relation_names.update((live_cls.__main_table__, stage_cls.__tablename__))
            required_names.add(stage_cls.__tablename__)
        if affected_group_table:
            relation_names.add(affected_group_table)
            required_names.add(affected_group_table)
    return sorted(relation_names), sorted(required_names)


async def _existing_cutover_relations(db_schema: str, relation_names: list[str]) -> list[str]:
    rows = await db.all(
        """
        SELECT c.relname
          FROM pg_class AS c
          JOIN pg_namespace AS n ON n.oid = c.relnamespace
         WHERE n.nspname = :db_schema
           AND c.relkind IN ('r', 'p')
           AND c.relname = ANY(CAST(:relation_names AS text[]))
         ORDER BY c.relname;
        """,
        db_schema=db_schema,
        relation_names=relation_names,
    )
    return [str(row[0]) for row in rows]


async def _assert_cutover_has_no_dependent_views(
    db_schema: str,
    live_table_names: Iterable[str],
) -> None:
    """Fail before materialization when a cutover table has dependent views."""
    relation_names = sorted(
        {
            relation_name
            for live_table_name in live_table_names
            for relation_name in (live_table_name, f"{live_table_name}_old")
        }
    )
    dependency_records = await db.all(
        """
        SELECT DISTINCT
               format('%I.%I', target_namespace.nspname, target_relation.relname),
               format('%I.%I', dependent_namespace.nspname, dependent_relation.relname)
          FROM pg_class AS target_relation
          JOIN pg_namespace AS target_namespace
            ON target_namespace.oid = target_relation.relnamespace
          JOIN pg_depend AS dependency
            ON dependency.refclassid = 'pg_class'::regclass
           AND dependency.refobjid = target_relation.oid
           AND dependency.classid = 'pg_rewrite'::regclass
          JOIN pg_rewrite AS rewrite_rule
            ON rewrite_rule.oid = dependency.objid
          JOIN pg_class AS dependent_relation
            ON dependent_relation.oid = rewrite_rule.ev_class
          JOIN pg_namespace AS dependent_namespace
            ON dependent_namespace.oid = dependent_relation.relnamespace
         WHERE target_namespace.nspname = :db_schema
           AND target_relation.relname = ANY(CAST(:relation_names AS text[]))
           AND target_relation.relkind IN ('r', 'p')
           AND dependent_relation.relkind IN ('v', 'm')
         ORDER BY 1, 2;
        """,
        db_schema=db_schema,
        relation_names=relation_names,
    )
    dependencies = [
        f"{dependency_record[0]} -> {dependency_record[1]}"
        for dependency_record in dependency_records
    ]
    if dependencies:
        raise RuntimeError(
            "entity-address-unified cutover has dependent views: "
            + ", ".join(dependencies)
        )


async def _acquire_cutover_locks(
    db_schema: str,
    relation_names: list[str],
    required_names: list[str],
) -> None:
    publisher_lock = await db.scalar(
        "SELECT pg_try_advisory_xact_lock(hashtextextended(:lock_name, 0));",
        lock_name=f"entity-address-unified:{db_schema}",
    )
    if not publisher_lock:
        raise _CutoverLockUnavailable("another entity-address publisher owns the cutover lock")

    existing_names = await _existing_cutover_relations(db_schema, relation_names)
    missing_names = sorted(set(required_names) - set(existing_names))
    if missing_names:
        raise RuntimeError(
            "Entity-address cutover is missing required staged relations: "
            + ", ".join(missing_names)
        )
    if not existing_names:
        raise RuntimeError("Entity-address cutover found no relations to lock")

    schema = _validate_schema_name(db_schema)
    qualified_names = [f"{schema}.{_validate_schema_name(name)}" for name in existing_names]
    await db.status(
        f"LOCK TABLE {', '.join(qualified_names)} IN ACCESS EXCLUSIVE MODE NOWAIT;"
    )


def _postgres_sqlstate(error: BaseException) -> str | None:
    original = getattr(error, "orig", None)
    candidates = (
        error,
        original,
        getattr(error, "__cause__", None),
        getattr(original, "__cause__", None),
    )
    for candidate in candidates:
        if candidate is None:
            continue
        sqlstate = getattr(candidate, "sqlstate", None) or getattr(candidate, "pgcode", None)
        if sqlstate:
            return str(sqlstate)
    return None


def _is_retryable_cutover_lock_error(error: BaseException) -> bool:
    return isinstance(error, _CutoverLockUnavailable) or _postgres_sqlstate(error) == "55P03"


async def _run_entity_address_cutover(
    db_schema: str,
    swaps: list[_StageTableSwap],
    patch_statements: list[tuple[str, str]],
    relation_names: list[str],
    required_names: list[str],
    context: dict,
) -> None:
    lock_timeout = (
        _env_sql_setting(
            "HLTHPRT_ENTITY_ADDRESS_UNIFIED_CUTOVER_LOCK_TIMEOUT",
            DEFAULT_CUTOVER_LOCK_TIMEOUT,
        )
        or DEFAULT_CUTOVER_LOCK_TIMEOUT
    )
    async with db.transaction():
        await db.status(f"SET LOCAL lock_timeout = {_sql_literal(lock_timeout)};")
        await db.scalar(address_alias_sql.alias_advisory_xact_lock_sql())
        expected_alias_generation = int(context.get("address_alias_generation") or 0)
        current_alias_generation = await _address_alias_generation(db_schema)
        if current_alias_generation != expected_alias_generation:
            raise RuntimeError(
                "address alias generation changed during entity-address-unified build"
            )
        await _assert_provider_directory_overlay_alias_fence(db_schema, context)
        await _acquire_cutover_locks(db_schema, relation_names, required_names)
        await _assert_cutover_has_no_dependent_views(
            db_schema,
            [swap.live_cls.__main_table__ for swap in swaps],
        )
        for swap in swaps:
            await _swap_stage_table(db_schema, swap.live_cls, swap.stage_cls)
        for label, statement in patch_statements:
            started = time.monotonic()
            rowcount = await db.status(statement)
            _record_phase_timing(
                context,
                f"entity-address-unified patching support {label}",
                time.monotonic() - started,
                _coerce_rowcount(rowcount),
            )
        active_table_oid = await db.scalar(
            _activate_geo_assurance_candidate_sql(db_schema)
        )
        if active_table_oid is None:
            raise RuntimeError(
                "geo assurance candidate does not match the published table and sources"
            )
        context["geo_assurance_active_table_oid"] = int(active_table_oid)


def _entity_address_cutover_plan(
    db_schema: str,
    stage_cls,
    support_stage_class_map: dict[type, type],
    *,
    partial_support_patch: bool,
    affected_group_table: str,
    context: dict,
) -> tuple[list[_StageTableSwap], list[tuple[str, str]], list[str], list[str]]:
    swaps = [_StageTableSwap(EntityAddressUnified, stage_cls)]
    patch_statements: list[tuple[str, str]] = []
    if partial_support_patch:
        patch_statements = _partial_support_patch_sql(
            db_schema,
            support_stage_class_map,
            old_entity_table=f"{EntityAddressUnified.__main_table__}_old",
            affected_group_table=affected_group_table,
            build_network_bridge=bool(
                context.get("build_network_bridge", DEFAULT_BUILD_NETWORK_BRIDGE)
            ),
        )
    else:
        swaps.extend(
            _StageTableSwap(live_cls, support_stage_cls)
            for live_cls, support_stage_cls in support_stage_class_map.items()
        )
    relation_names, required_names = _cutover_relation_sets(
        swaps,
        support_stage_class_map,
        partial_support_patch=partial_support_patch,
        affected_group_table=affected_group_table,
    )
    return swaps, patch_statements, relation_names, required_names


def _cutover_retry_settings() -> tuple[int, int, int]:
    return (
        _env_int(
            "HLTHPRT_ENTITY_ADDRESS_UNIFIED_CUTOVER_RETRY_ATTEMPTS",
            DEFAULT_CUTOVER_RETRY_ATTEMPTS,
            minimum=1,
        ),
        _env_int(
            "HLTHPRT_ENTITY_ADDRESS_UNIFIED_CUTOVER_RETRY_BACKOFF_MS",
            DEFAULT_CUTOVER_RETRY_BACKOFF_MS,
        ),
        _env_int(
            "HLTHPRT_ENTITY_ADDRESS_UNIFIED_CUTOVER_RETRY_MAX_BACKOFF_MS",
            DEFAULT_CUTOVER_RETRY_MAX_BACKOFF_MS,
        ),
    )


async def _publish_staged_entity_address_tables(
    db_schema: str,
    stage_cls,
    support_stage_class_map: dict[type, type],
    *,
    partial_support_patch: bool,
    affected_group_table: str,
    context: dict,
) -> None:
    swaps, patch_statements, relation_names, required_names = _entity_address_cutover_plan(
        db_schema,
        stage_cls,
        support_stage_class_map,
        partial_support_patch=partial_support_patch,
        affected_group_table=affected_group_table,
        context=context,
    )
    for swap in swaps:
        await _ensure_promoted_stage_logged(db_schema, swap.stage_cls.__tablename__)
    await _run_sql_phase(
        f"ANALYZE {db_schema}.{stage_cls.__tablename__};",
        context=context,
        phase="entity-address-unified analyzing staged main table",
    )
    context["stage_persistence"] = "p"
    max_attempts, base_backoff_ms, max_backoff_ms = _cutover_retry_settings()
    for attempt in range(1, max_attempts + 1):
        context["cutover_attempts"] = attempt
        try:
            await _run_entity_address_cutover(
                db_schema,
                swaps,
                patch_statements,
                relation_names,
                required_names,
                context,
            )
            return
        except Exception as exc:
            if attempt >= max_attempts or not _is_retryable_cutover_lock_error(exc):
                raise
            delay_ms = min(base_backoff_ms * (2 ** (attempt - 1)), max_backoff_ms)
            logger.warning(
                "Entity-address cutover lock unavailable on attempt %d/%d; retrying in %dms",
                attempt,
                max_attempts,
                delay_ms,
            )
            await asyncio.sleep(delay_ms / 1000)


async def _drop_stage_artifacts(
    db_schema: str,
    stage_cls,
    support_stage_class_map: dict[type, type],
    *,
    extra_tables: Iterable[str] = (),
) -> None:
    await db.status(f"DROP TABLE IF EXISTS {db_schema}.{stage_cls.__tablename__};")
    for stage_model in support_stage_class_map.values():
        await db.status(f"DROP TABLE IF EXISTS {db_schema}.{stage_model.__tablename__};")
    for table_name in extra_tables:
        if table_name:
            await db.status(f"DROP TABLE IF EXISTS {db_schema}.{table_name};")


async def _has_table(db_schema: str, table_name: str) -> bool:
    return bool(await db.scalar(f"SELECT to_regclass('{db_schema}.{table_name}') IS NOT NULL;"))


async def _has_table_column(db_schema: str, table_name: str, column_name: str) -> bool:
    return bool(
        await db.scalar(
            """
            SELECT EXISTS (
                SELECT 1
                  FROM information_schema.columns
                 WHERE table_schema = :db_schema
                   AND table_name = :table_name
                   AND column_name = :column_name
            );
            """,
            db_schema=db_schema,
            table_name=table_name,
            column_name=column_name,
        )
    )


async def _ensure_entity_address_unified_live_columns(
    db_schema: str,
    table_name: str = EntityAddressUnified.__main_table__,
) -> None:
    if not await _has_table(db_schema, table_name):
        return
    existing_rows = await db.all(
        """
        SELECT column_name
          FROM information_schema.columns
         WHERE table_schema = :db_schema
           AND table_name = :table_name
        """,
        db_schema=db_schema,
        table_name=table_name,
    )
    existing_columns = {
        str((row._mapping if hasattr(row, "_mapping") else row).get("column_name"))
        for row in existing_rows
    }
    dialect = postgresql.dialect()
    for column in EntityAddressUnified.__table__.columns:
        if column.name in existing_columns:
            continue
        column_ddl = str(CreateColumn(column).compile(dialect=dialect)).strip()
        await db.status(
            f"ALTER TABLE {db_schema}.{table_name} ADD COLUMN IF NOT EXISTS {column_ddl};"
        )


async def _is_support_bridge_reuse_available(db_schema: str, *, build_network_bridge: bool) -> bool:
    bridge_models: list[type] = [
        EntityAddressPlanBridge,
        EntityAddressProcedureBridge,
        EntityAddressMedicationBridge,
    ]
    if build_network_bridge:
        bridge_models.append(EntityAddressNetworkBridge)
    for model in bridge_models:
        if not await _has_table(db_schema, model.__main_table__):
            return False
    return True


def _promote_facility_npi_candidates_sql(db_schema: str) -> str:
    return f"""
    INSERT INTO {db_schema}.facility_anchor_npi_override (
        facility_anchor_id,
        npi,
        status,
        confidence,
        method,
        source,
        evidence,
        reviewed_by,
        reviewed_at,
        updated_at
    )
    SELECT
        c.facility_anchor_id,
        c.candidate_npi,
        'approved'::varchar AS status,
        c.match_confidence AS confidence,
        COALESCE(c.candidate_method, 'facility_anchor_npi_candidate')::varchar AS method,
        'facility_anchor_npi_candidate'::varchar AS source,
        json_build_object(
            'candidate_id', c.candidate_id,
            'location_key', c.location_key,
            'candidate_status', c.candidate_status,
            'review_status', c.review_status,
            'reviewed_by', c.reviewed_by,
            'reviewed_at', c.reviewed_at,
            'candidate_evidence', c.evidence
        )::json AS evidence,
        c.reviewed_by,
        COALESCE(c.reviewed_at, NOW())::timestamp AS reviewed_at,
        NOW()::timestamp AS updated_at
      FROM {db_schema}.facility_anchor_npi_candidate AS c
     WHERE c.review_status = 'approved'
       AND c.candidate_npi IS NOT NULL
       AND c.facility_anchor_id IS NOT NULL
    ON CONFLICT (facility_anchor_id, npi) DO UPDATE
       SET status = 'approved',
           confidence = EXCLUDED.confidence,
           method = EXCLUDED.method,
           source = EXCLUDED.source,
           evidence = EXCLUDED.evidence,
           reviewed_by = EXCLUDED.reviewed_by,
           reviewed_at = EXCLUDED.reviewed_at,
           updated_at = EXCLUDED.updated_at;
    """


async def _promote_approved_facility_anchor_npi_candidates(db_schema: str) -> int:
    if not (
        await _has_table(db_schema, "facility_anchor_npi_candidate")
        and await _has_table(db_schema, "facility_anchor_npi_override")
    ):
        return 0
    return int(
        await db.status(_promote_facility_npi_candidates_sql(db_schema))
        or 0
    )


async def _is_address_canon_available(db_schema: str) -> bool:
    value = await db.scalar(
        "SELECT to_regprocedure(:signature);",
        signature=f"{db_schema}.addr_key_v1(text,text,text,text,text,text)",
    )
    return isinstance(value, str) and bool(value)


def _npi_entity_name_expr(alias: str = "n") -> str:
    return (
        "NULLIF(TRIM("
        f"COALESCE({alias}.provider_organization_name, '') || ' ' || "
        f"COALESCE({alias}.provider_other_organization_name, '') || ' ' || "
        f"COALESCE({alias}.provider_first_name, '') || ' ' || "
        f"COALESCE({alias}.provider_last_name, '')"
        "), '')"
    )


def _npi_entity_subtype_expr(alias: str = "n") -> str:
    return (
        f"CASE WHEN {alias}.entity_type_code = 1 THEN 'individual' "
        f"WHEN {alias}.entity_type_code = 2 THEN 'organization' ELSE NULL END"
    )


_ADDRESS_CHECKSUM_EXPRESSIONS_BY_FIELD = {
    "entity_type": "entity_type",
    "entity_id": "entity_id",
    "type": "type",
    "first_line": "first_line",
    "second_line": "second_line",
    "city_name": "city_name",
    "state_name": "state_name",
    "postal_code": "postal_code",
    "country_code": "country_code",
    "telephone_number": "telephone_number",
}


def _address_checksum_expr(
    expressions_by_field: Mapping[str, str] | None = None,
) -> str:
    expression_by_field = {
        **_ADDRESS_CHECKSUM_EXPRESSIONS_BY_FIELD,
        **dict(expressions_by_field or {}),
    }
    return (
        "(('x' || substr(md5(lower(concat_ws('|', "
        f"COALESCE({expression_by_field['entity_type']}, ''), "
        f"COALESCE({expression_by_field['entity_id']}, ''), "
        f"COALESCE({expression_by_field['type']}, ''), "
        f"COALESCE({expression_by_field['first_line']}, ''), "
        f"COALESCE({expression_by_field['second_line']}, ''), "
        f"COALESCE({expression_by_field['city_name']}, ''), "
        f"COALESCE({expression_by_field['state_name']}, ''), "
        f"COALESCE({expression_by_field['postal_code']}, ''), "
        f"COALESCE({expression_by_field['country_code']}, ''), "
        f"COALESCE({expression_by_field['telephone_number']}, '')"
        "))), 1, 8))::bit(32)::int)"
    )


def _alnum_norm_expr(expr: str) -> str:
    return (
        "NULLIF("
        f"regexp_replace(lower(COALESCE({expr}, '')), '[^a-z0-9]', '', 'g')"
        ", '')"
    )


def _state_norm_expr(expr: str) -> str:
    return f"NULLIF(upper(trim(COALESCE({expr}, ''))), '')"


def _zip5_norm_expr(expr: str) -> str:
    return (
        "NULLIF("
        f"LEFT(regexp_replace(COALESCE({expr}, ''), '[^0-9]', '', 'g'), 5)"
        ", '')"
    )


def _address_source_text_present_predicate(expr: str) -> str:
    cleaned = f"NULLIF(BTRIM(COALESCE({expr}, '')), '')"
    token = f"UPPER(REGEXP_REPLACE(BTRIM(COALESCE({expr}, '')), '[^A-Za-z0-9]+', '', 'g'))"
    return (
        f"{cleaned} IS NOT NULL "
        f"AND {token} NOT IN ('NULL', 'NONE', 'NA', 'NAN', 'UNKNOWN', 'UNSPECIFIED')"
    )


def _address_source_state_present_predicate(expr: str) -> str:
    cleaned = f"NULLIF(BTRIM(COALESCE({expr}, '')), '')"
    token = f"UPPER(REGEXP_REPLACE(BTRIM(COALESCE({expr}, '')), '[^A-Za-z]+', '', 'g'))"
    return (
        f"{cleaned} IS NOT NULL "
        f"AND {token} NOT IN ('NULL', 'NONE', 'NA', 'NAN', 'UN', 'UNKNOWN', 'UNSPECIFIED', 'XX', 'ZZ')"
    )


def _address_source_zip5_present_predicate(expr: str) -> str:
    zip5 = _zip5_norm_expr(expr)
    return f"{zip5} IS NOT NULL AND {zip5} NOT IN ('00000', '99999')"


def _address_source_us_country_predicate(expr: str) -> str:
    token = f"UPPER(REGEXP_REPLACE(BTRIM(COALESCE({expr}, '')), '[^A-Za-z]+', '', 'g'))"
    return f"({token} = '' OR {token} IN ('US', 'USA', 'UNITEDSTATES', 'UNITEDSTATESOFAMERICA'))"


def _address_source_keyable_predicate(
    *,
    first_line: str,
    city: str,
    state: str,
    zip_code: str,
    country: str = "'US'",
) -> str:
    return (
        f"{_address_source_state_present_predicate(state)}\n"
        f"               AND {_address_source_zip5_present_predicate(zip_code)}\n"
        f"               AND {_address_source_us_country_predicate(country)}\n"
        "               AND (\n"
        f"                    {_address_source_text_present_predicate(first_line)}\n"
        f"                    OR {_address_source_text_present_predicate(city)}\n"
        "               )"
    )


def _phone_norm_expr(expr: str) -> str:
    return f"NULLIF(regexp_replace(COALESCE({expr}, ''), '[^0-9]', '', 'g'), '')"


def _contact_main_expr(expr: str) -> str:
    extension_pattern = (
        "'[[:space:]]*(extension|ext\\.?|;ext=|#|x)"
        "[[:space:]]*[0-9]{1,16}[[:space:]]*$'"
    )
    return f"regexp_replace(COALESCE({expr}, ''), {extension_pattern}, '', 'i')"


def _contact_digits_expr(expr: str) -> str:
    return f"regexp_replace({_contact_main_expr(expr)}, '[^0-9]', '', 'g')"


def _contact_country_key_expr(expr: str) -> str:
    return f"regexp_replace(upper(COALESCE({expr}, '')), '[^A-Z]', '', 'g')"


def _canonical_contact_number_expr(expr: str, country_expr: str = "country_code") -> str:
    digits = _contact_digits_expr(expr)
    country_key = _contact_country_key_expr(country_expr)
    default_us = (
        f"({country_key} = '' OR {country_key} IN "
        "('US', 'USA', 'UNITEDSTATES', 'UNITEDSTATESOFAMERICA'))"
    )
    main = _contact_main_expr(expr)
    return (
        "CASE "
        f"WHEN {default_us} AND length({digits}) = 10 THEN {digits} "
        f"WHEN {default_us} AND length({digits}) = 11 AND left({digits}, 1) = '1' "
        f"THEN substring({digits} from 2) "
        f"WHEN BTRIM({main}) LIKE '+%' AND length({digits}) BETWEEN 8 AND 15 THEN {digits} "
        "ELSE NULL::varchar END"
    )


def _nullish_text_expr(expr: str) -> str:
    """Normalize empty and source-system null sentinel strings to SQL NULL."""

    return (
        f"CASE WHEN LOWER(TRIM(COALESCE(({expr})::text, ''))) "
        "IN ('', 'null', 'none', 'undefined') THEN NULL "
        f"ELSE NULLIF(TRIM(({expr})::text), '') END"
    )


def _coordinate_country_key_expr(expr: str) -> str:
    return f"regexp_replace(upper(COALESCE(({expr})::varchar, '')), '[^A-Z0-9]', '', 'g')"


def _coordinate_pair_plausible_sql(latitude_expr: str, longitude_expr: str, country_expr: str) -> str:
    country_key = _coordinate_country_key_expr(country_expr)
    default_us = f"{country_key} IN ('', 'US', 'USA', 'UNITEDSTATES', 'UNITEDSTATESOFAMERICA', '840', '001')"
    return f"""
        ({latitude_expr}) BETWEEN -90 AND 90
        AND ({longitude_expr}) BETWEEN -180 AND 180
        AND NOT (ABS({latitude_expr}) < 0.0000001 AND ABS({longitude_expr}) < 0.0000001)
        AND (
            NOT ({default_us})
            OR (({latitude_expr}) BETWEEN 24 AND 50 AND ({longitude_expr}) BETWEEN -125 AND -66)
            OR (({latitude_expr}) BETWEEN 51 AND 72 AND ({longitude_expr}) BETWEEN -180 AND -129)
            OR (({latitude_expr}) BETWEEN 18 AND 23 AND ({longitude_expr}) BETWEEN -161 AND -154)
            OR (({latitude_expr}) BETWEEN 17 AND 19 AND ({longitude_expr}) BETWEEN -68 AND -64)
            OR (({latitude_expr}) BETWEEN 13 AND 16 AND ({longitude_expr}) BETWEEN 144 AND 146)
            OR (({latitude_expr}) BETWEEN -15 AND -10 AND ({longitude_expr}) BETWEEN -171 AND -168)
        )
    """


def _coordinate_from_text_pair_sql(
    latitude_expr: str,
    longitude_expr: str,
    country_expr: str,
    *,
    axis: str,
) -> str:
    if axis not in {"lat", "long"}:
        raise ValueError(f"unsupported coordinate axis: {axis}")
    numeric_re = r"^-?[0-9]+(\.[0-9]+)?$"
    raw_lat = f"({latitude_expr})::numeric"
    raw_long = f"({longitude_expr})::numeric"
    scaled_1m_lat = f"(({latitude_expr})::numeric / 1000000)"
    scaled_1m_long = f"(({longitude_expr})::numeric / 1000000)"
    scaled_10m_lat = f"(({latitude_expr})::numeric / 10000000)"
    scaled_10m_long = f"(({longitude_expr})::numeric / 10000000)"
    raw_value = raw_lat if axis == "lat" else raw_long
    scaled_1m_value = scaled_1m_lat if axis == "lat" else scaled_1m_long
    scaled_10m_value = scaled_10m_lat if axis == "lat" else scaled_10m_long
    numeric_guard = (
        f"({latitude_expr}) ~ '{numeric_re}' AND ({longitude_expr}) ~ '{numeric_re}'"
    )
    return f"""
        CASE
            WHEN {numeric_guard}
             AND {_coordinate_pair_plausible_sql(raw_lat, raw_long, country_expr)}
                THEN {raw_value}
            WHEN {numeric_guard}
             AND {_coordinate_pair_plausible_sql(scaled_1m_lat, scaled_1m_long, country_expr)}
                THEN {scaled_1m_value}
            WHEN {numeric_guard}
             AND {_coordinate_pair_plausible_sql(scaled_10m_lat, scaled_10m_long, country_expr)}
                THEN {scaled_10m_value}
            ELSE NULL::numeric
        END
    """


def _contact_extension_expr(expr: str) -> str:
    extension_pattern = (
        "'(extension|ext\\.?|;ext=|#|x)[[:space:]]*[0-9]{1,16}[[:space:]]*$'"
    )
    extract_pattern = (
        "'^.*(extension|ext\\.?|;ext=|#|x)[[:space:]]*([0-9]{1,16})[[:space:]]*$'"
    )
    return (
        "CASE "
        f"WHEN COALESCE({expr}, '') ~* {extension_pattern} "
        f"THEN NULLIF(regexp_replace(COALESCE({expr}, ''), {extract_pattern}, '\\2', 'i'), '')::varchar "
        "ELSE NULL::varchar END"
    )


def _source_priority_expr(expr: str) -> str:
    return (
        "CASE "
        f"WHEN {expr} = 'nppes' THEN 0 "
        f"WHEN {expr} = 'cms_doctors' THEN 1 "
        f"WHEN {expr} = 'provider_enrollment_ffs' THEN 2 "
        f"WHEN {expr} = 'provider_enrollment_ffs_address' THEN 3 "
        f"WHEN {expr} LIKE 'facility_anchor:%' THEN 4 "
        f"WHEN {expr} = 'mrf' THEN 5 "
        f"WHEN {expr} = 'provider_directory_fhir' THEN 6 "
        "ELSE 9 END"
    )


def _source_id_expr(expr: str) -> str:
    return (
        "CASE "
        f"WHEN {expr} = 'nppes' THEN 1 "
        f"WHEN {expr} = 'mrf' THEN 2 "
        f"WHEN {expr} = 'cms_doctors' THEN 3 "
        f"WHEN {expr} = 'provider_enrollment_ffs' THEN 4 "
        f"WHEN {expr} = 'provider_enrollment_ffs_address' THEN 5 "
        f"WHEN {expr} LIKE 'facility_anchor:%' THEN 6 "
        f"WHEN {expr} = 'ptg' THEN 7 "
        f"WHEN {expr} = 'provider_directory_fhir' THEN 8 "
        "ELSE 0 END"
    )


def _source_mask_expr(expr: str) -> str:
    return (
        "CASE "
        f"WHEN {expr} = 'nppes' THEN 1::bigint "
        f"WHEN {expr} = 'mrf' THEN 2::bigint "
        f"WHEN {expr} = 'cms_doctors' THEN 4::bigint "
        f"WHEN {expr} = 'provider_enrollment_ffs' THEN 8::bigint "
        f"WHEN {expr} = 'provider_enrollment_ffs_address' THEN 16::bigint "
        f"WHEN {expr} LIKE 'facility_anchor:%' THEN 32::bigint "
        f"WHEN {expr} = 'ptg' THEN 64::bigint "
        f"WHEN {expr} = 'provider_directory_fhir' THEN 128::bigint "
        "ELSE 0::bigint END"
    )


def _address_role_id_expr(expr: str) -> str:
    return (
        "CASE "
        f"WHEN {expr} = 'primary' THEN 1 "
        f"WHEN {expr} = 'mail' THEN 2 "
        f"WHEN {expr} = 'secondary' THEN 3 "
        f"WHEN {expr} = 'practice' THEN 4 "
        f"WHEN {expr} = 'site' THEN 5 "
        f"WHEN {expr} = 'billing' THEN 6 "
        f"WHEN {expr} = 'inferred' THEN 7 "
        "ELSE 0 END"
    )


_LOCATION_KEY_EXPRESSIONS_BY_FIELD = {
    "entity_type": "entity_type",
    "entity_id": "entity_id",
    "npi": "npi",
    "inferred_npi": "inferred_npi",
    "address_role_id": "address_role_id",
    "row_origin": "row_origin",
    "address_key": "address_key",
    "source_id": "source_id",
    "source_record_id": "source_record_id",
    "zip5": "zip5",
    "state_code": "state_code",
    "city_norm": "city_norm",
}


def _location_key_expr(
    expressions_by_field: Mapping[str, str] | None = None,
) -> str:
    expression_by_field = {
        **_LOCATION_KEY_EXPRESSIONS_BY_FIELD,
        **dict(expressions_by_field or {}),
    }
    identity = (
        "CASE WHEN "
        f"{expression_by_field['address_key']} IS NOT NULL THEN concat_ws('|', "
        "'v1', "
        f"COALESCE({expression_by_field['entity_type']}, ''), "
        f"COALESCE({expression_by_field['entity_id']}, ''), "
        f"COALESCE({expression_by_field['npi']}::text, ''), "
        f"COALESCE({expression_by_field['inferred_npi']}::text, ''), "
        "''::text, "
        f"COALESCE({expression_by_field['address_role_id']}::text, ''), "
        f"COALESCE({expression_by_field['row_origin']}, ''), "
        f"COALESCE({expression_by_field['address_key']}::text, '')) "
        "ELSE concat_ws('|', "
        "'v1', 'fallback', "
        f"COALESCE({expression_by_field['entity_type']}, ''), "
        f"COALESCE({expression_by_field['entity_id']}, ''), "
        f"COALESCE({expression_by_field['npi']}::text, ''), "
        f"COALESCE({expression_by_field['inferred_npi']}::text, ''), "
        "''::text, "
        f"COALESCE({expression_by_field['address_role_id']}::text, ''), "
        f"COALESCE({expression_by_field['row_origin']}, ''), "
        f"COALESCE({expression_by_field['source_id']}::text, ''), "
        f"COALESCE({expression_by_field['source_record_id']}, ''), "
        f"COALESCE({expression_by_field['zip5']}, ''), "
        f"COALESCE({expression_by_field['state_code']}, ''), "
        f"COALESCE({expression_by_field['city_norm']}, '')) END"
    )
    return f"encode(sha256(convert_to(({identity}), 'UTF8')), 'hex')"


def _street_soft_norm_expr(expr: str) -> str:
    # Canonicalize common street word variants so cross-source evidence can converge.
    return (
        "regexp_replace("
        "regexp_replace("
        "regexp_replace("
        "regexp_replace("
        "regexp_replace("
        "regexp_replace("
        "regexp_replace("
        "regexp_replace("
        "regexp_replace("
        "regexp_replace("
        "regexp_replace("
        "regexp_replace(' ' || lower(COALESCE("
        f"{expr}"
        ", '')) || ' ', "
        "'\\mwest\\M', ' w ', 'g'), "
        "'\\meast\\M', ' e ', 'g'), "
        "'\\mnorth\\M', ' n ', 'g'), "
        "'\\msouth\\M', ' s ', 'g'), "
        "'\\mstreet\\M', ' st ', 'g'), "
        "'\\mavenue\\M', ' ave ', 'g'), "
        "'\\mroad\\M', ' rd ', 'g'), "
        "'\\mboulevard\\M', ' blvd ', 'g'), "
        "'\\mdrive\\M', ' dr ', 'g'), "
        "'\\mlane\\M', ' ln ', 'g'), "
        "'\\mhighway\\M', ' hwy ', 'g'), "
        "'[^a-z0-9]', '', 'g')"
    )


def _provider_directory_scope_filter_sql(
    alias: str,
    *,
    source_ids: list[str] | tuple[str, ...] | None = None,
    run_id: str | None = None,
    indent: str = "                   ",
) -> str:
    clauses: list[str] = []
    if source_ids:
        clauses.append(f"{alias}.source_id = ANY({_string_array_literal(list(source_ids))})")
    if run_id:
        clauses.append(f"{alias}.last_seen_run_id = {_sql_literal(run_id)}")
    if not clauses:
        return ""
    return "".join(f"\n{indent}AND {clause}" for clause in clauses)


def _provider_directory_live_source_filter_sql(
    db_schema: str,
    alias: str,
    *,
    source_ids: list[str] | tuple[str, ...] | None = None,
) -> str:
    if not source_ids:
        return ""
    selected_source_ids = _string_array_literal(list(source_ids))
    return f"""
       AND EXISTS (
            SELECT 1
              FROM unnest(COALESCE({alias}.source_record_ids, ARRAY[]::varchar[])) AS pd_rid(rid)
              JOIN {db_schema}.provider_directory_source AS sibling_source
                ON sibling_source.source_id = split_part(pd_rid.rid, ':', 3)
             WHERE pd_rid.rid LIKE 'provider_directory_fhir:%'
               AND NULLIF(split_part(pd_rid.rid, ':', 3), '') IS NOT NULL
               AND sibling_source.endpoint_id IS NOT NULL
               AND EXISTS (
                    SELECT 1
                      FROM {db_schema}.provider_directory_source AS selected_source
                     WHERE selected_source.source_id = ANY({selected_source_ids})
                       AND selected_source.endpoint_id = sibling_source.endpoint_id
               )
       )"""


def _missing_provider_directory_fence_relations(available: dict[str, bool]) -> list[str]:
    return [
        table_name
        for table_name in PROVIDER_DIRECTORY_DATASET_FENCE_TABLES
        if not available.get(table_name, False)
    ]


def _validate_provider_directory_fence(
    available: dict[str, bool],
    *,
    has_compatibility_data: bool,
    partial_refresh: bool,
) -> None:
    missing_relations = _missing_provider_directory_fence_relations(available)
    if not missing_relations or not (partial_refresh or has_compatibility_data):
        return
    raise RuntimeError(
        "entity-address-unified Provider Directory projection requires current "
        "dataset fence relations: "
        + ", ".join(missing_relations)
    )


async def _has_provider_directory_compatibility_data(
    db_schema: str,
    available: dict[str, bool],
) -> bool:
    table_names = [
        table_name
        for table_name in PROVIDER_DIRECTORY_COMPATIBILITY_ADDRESS_TABLES
        if available.get(table_name, False)
    ]
    if not table_names:
        return False
    existence_checks = " OR ".join(
        f"EXISTS (SELECT 1 FROM {db_schema}.{table_name} LIMIT 1)"
        for table_name in table_names
    )
    return bool(await db.scalar(f"SELECT {existence_checks};"))


def _provider_directory_current_overlay_ctes_sql(
    db_schema: str,
    *,
    source_ids: list[str] | tuple[str, ...] | None = None,
    run_id: str | None = None,
    affected_group_table: str | None = None,
) -> str:
    """Return CTEs that retain only current, published dataset overlay rows."""

    source_ref = f"{db_schema}.provider_directory_source"
    dataset_ref = f"{db_schema}.provider_directory_endpoint_dataset"
    dataset_resource_ref = f"{db_schema}.provider_directory_dataset_resource"
    overlay_ref = f"{db_schema}.provider_directory_address_overlay"
    requested_source_filter = (
        f"WHERE source.source_id = ANY({_string_array_literal(list(source_ids))})"
        if source_ids
        else "WHERE source.endpoint_id IS NOT NULL"
    )
    run_filter = (
        f"\n           AND overlay.last_seen_run_id = {_sql_literal(run_id)}"
        if run_id
        else ""
    )
    affected_overlay_ctes = ""
    current_overlay_ref = overlay_ref
    if affected_group_table:
        affected_overlay_ctes = f"""affected_npis AS MATERIALIZED (
    SELECT DISTINCT affected.entity_npi AS npi
      FROM {db_schema}.{affected_group_table} AS affected
     WHERE affected.entity_npi IS NOT NULL
), affected_overlay AS MATERIALIZED (
    SELECT overlay.*
      FROM {overlay_ref} AS overlay
      JOIN affected_npis AS affected_npi
        ON overlay.npi = affected_npi.npi
), """
        current_overlay_ref = "affected_overlay"
    return _PROVIDER_DIRECTORY_CURRENT_OVERLAY_CTES_TEMPLATE.format(
        source_ref=source_ref,
        requested_source_filter=requested_source_filter,
        dataset_ref=dataset_ref,
        affected_overlay_ctes=affected_overlay_ctes,
        current_overlay_ref=current_overlay_ref,
        run_filter=run_filter,
        dataset_resource_ref=dataset_resource_ref,
    )


def _latest_provider_directory_partial_scope_sql(db_schema: str) -> str:
    return f"""
    {_provider_directory_current_overlay_ctes_sql(db_schema)}
    SELECT
        dataset_run_id::varchar AS run_id,
        ARRAY_AGG(DISTINCT source_id ORDER BY source_id)::varchar[] AS source_ids,
        COUNT(*)::bigint AS source_count,
        MAX(dataset_published_at) AS latest_updated_at,
        ARRAY['current_overlay']::varchar[] AS scope_sources
      FROM current_overlay
  GROUP BY dataset_run_id
  ORDER BY MAX(dataset_published_at) DESC NULLS LAST, dataset_run_id DESC
     LIMIT 1;
    """


async def _latest_provider_directory_partial_scope(db_schema: str) -> tuple[str | None, list[str], list[str]]:
    if not await _has_table(db_schema, "provider_directory_source"):
        return None, [], []
    row = await db.first(_latest_provider_directory_partial_scope_sql(db_schema))
    if not row:
        return None, [], []
    values = row._mapping if hasattr(row, "_mapping") else row
    source_ids = _coerce_str_list(values.get("source_ids"))
    scope_sources = _coerce_str_list(values.get("scope_sources"))
    return _clean_optional(values.get("run_id")), source_ids, scope_sources


def _provider_directory_reference_resource_id_sql(reference: str, resource_type: str) -> str:
    return profile_artifact.fhir_reference_resource_id_sql(reference, resource_type)


def _provider_directory_referenced_plans_sql(
    db_schema: str,
    owner_alias: str,
    has_insurance_plan: bool,
) -> str:
    if not has_insurance_plan:
        return (
            "SELECT NULL::varchar AS resource_id, NULL::varchar AS plan_identifier, "
            "'[]'::jsonb AS network_refs WHERE FALSE"
        )
    plan_resource_id = _provider_directory_reference_resource_id_sql(
        "plan_ref.value",
        "InsurancePlan",
    )
    return f"""
        SELECT DISTINCT
            insurance_plan.resource_id::varchar AS resource_id,
            NULLIF(BTRIM(insurance_plan.plan_identifier), '')::varchar AS plan_identifier,
            COALESCE(insurance_plan.network_refs::jsonb, '[]'::jsonb) AS network_refs
          FROM jsonb_array_elements_text(
                COALESCE({owner_alias}.insurance_plan_refs::jsonb, '[]'::jsonb)
          ) AS plan_ref(value)
          JOIN {db_schema}.provider_directory_insurance_plan AS insurance_plan
            ON insurance_plan.source_id = {owner_alias}.source_id
           AND insurance_plan.resource_id = {plan_resource_id}
    """


def _provider_directory_resolved_networks_sql(
    db_schema: str,
    owner_alias: str,
    *,
    has_network_catalog: bool,
    has_organization: bool,
) -> str:
    network_resource_id = _provider_directory_reference_resource_id_sql(
        "network_ref.reference",
        "Organization",
    )
    joins: list[str] = []
    name_candidates: list[str] = []
    if has_network_catalog:
        joins.append(
            f"LEFT JOIN {db_schema}.provider_directory_network_catalog AS network_catalog "
            f"ON network_catalog.source_id = {owner_alias}.source_id "
            f"AND network_catalog.network_resource_id = network_ref.network_resource_id"
        )
        name_candidates.append("network_catalog.provider_directory_network_name")
    if has_organization:
        joins.append(
            f"LEFT JOIN {db_schema}.provider_directory_organization AS network_organization "
            f"ON network_organization.source_id = {owner_alias}.source_id "
            f"AND network_organization.resource_id = network_ref.network_resource_id "
            "AND network_organization.active IS DISTINCT FROM false"
        )
        name_candidates.append("network_organization.name")
    if not name_candidates:
        return "SELECT NULL::varchar AS resource_id, NULL::varchar AS network_name WHERE FALSE"
    network_name = f"NULLIF(BTRIM(COALESCE({', '.join(name_candidates)})), '')"
    return f"""
        SELECT DISTINCT
            network_ref.network_resource_id::varchar AS resource_id,
            {network_name}::varchar AS network_name
          FROM (
                SELECT {network_resource_id}::varchar AS network_resource_id
                  FROM network_references AS network_ref
          ) AS network_ref
          {' '.join(joins)}
         WHERE network_ref.network_resource_id IS NOT NULL
           AND {network_name} IS NOT NULL
    """


def _provider_directory_plan_network_join_sql(
    db_schema: str,
    owner_alias: str,
    available: dict[str, bool],
    *,
    include_insurance_plans: bool,
) -> str:
    referenced_plans_sql = _provider_directory_referenced_plans_sql(
        db_schema,
        owner_alias,
        include_insurance_plans and available.get("provider_directory_insurance_plan", False),
    )
    resolved_networks_sql = _provider_directory_resolved_networks_sql(
        db_schema,
        owner_alias,
        has_network_catalog=available.get("provider_directory_network_catalog", False),
        has_organization=available.get("provider_directory_organization", False),
    )
    return f"""
                  LEFT JOIN LATERAL (
                      WITH referenced_plans AS MATERIALIZED (
                          {referenced_plans_sql}
                      ), network_references AS (
                          SELECT network_ref.value::varchar AS reference
                            FROM jsonb_array_elements_text(
                                  COALESCE({owner_alias}.network_refs::jsonb, '[]'::jsonb)
                            ) AS network_ref(value)
                          UNION
                          SELECT plan_network_ref.value::varchar AS reference
                            FROM referenced_plans AS referenced_plan
                           CROSS JOIN LATERAL jsonb_array_elements_text(
                                  referenced_plan.network_refs
                           ) AS plan_network_ref(value)
                      ), resolved_networks AS (
                          {resolved_networks_sql}
                      )
                      SELECT
                          ARRAY(
                              SELECT DISTINCT plan_identifier
                                FROM referenced_plans
                               WHERE plan_identifier IS NOT NULL
                            ORDER BY plan_identifier
                          )::varchar[] AS plan_identifiers,
                          ARRAY(
                              SELECT network_name
                                FROM resolved_networks
                            ORDER BY network_name
                          )::varchar[] AS network_names
                  ) AS plan_network_evidence ON TRUE"""


def _source_selects(
    db_schema: str,
    available: dict[str, bool],
    *,
    test_limit_per_source: int | None = None,
    provider_directory_source_ids: list[str] | tuple[str, ...] | None = None,
    provider_directory_run_id: str | None = None,
    is_address_canon_available: bool = True,
) -> list[str]:
    """Build normalized source queries for unified address materialization."""
    selects: list[str] = []
    has_npi = available.get("npi", False)
    has_npi_address = available.get("npi_address", False)
    has_doctors = available.get("doctor_clinician_address", False)
    has_ffs = available.get("provider_enrollment_ffs", False)
    has_ffs_address = available.get("provider_enrollment_ffs_address", False)
    has_ffs_additional_npi = available.get("provider_enrollment_ffs_additional_npi", False)
    has_facility = available.get("facility_anchor", False)
    has_hospital_enrollment = available.get("provider_enrollment_hospital", False)
    has_fqhc_enrollment = available.get("provider_enrollment_fqhc", False)
    has_mrf_address = available.get("mrf_address", False)
    has_provider_directory_practitioner = available.get("provider_directory_practitioner", False)
    has_provider_directory_organization = available.get("provider_directory_organization", False)
    has_provider_directory_location = available.get("provider_directory_location", False)
    has_provider_directory_role = available.get("provider_directory_practitioner_role", False)
    has_provider_directory_affiliation = available.get("provider_directory_organization_affiliation", False)
    has_provider_directory_healthcare_service = available.get("provider_directory_healthcare_service", False)
    has_provider_directory_location_address_key = available.get(
        "provider_directory_location.address_key",
        has_provider_directory_location,
    )
    def source_address_key(table_name: str, alias: str) -> str:
        """Return a typed source address-key expression when available."""
        if available.get(f"{table_name}.address_key", available.get(table_name, False)):
            return f"{alias}.address_key::uuid"
        return "NULL::uuid"

    npi_address_key = source_address_key("npi_address", "a")
    doctors_address_key = source_address_key("doctor_clinician_address", "d")
    ffs_address_key = source_address_key("provider_enrollment_ffs_address", "fa")
    facility_address_key = source_address_key("facility_anchor", "fa")
    mrf_address_key = source_address_key("mrf_address", "a")
    provider_directory_address_key = (
        """
        CASE
            WHEN loc.address_key ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                THEN loc.address_key::uuid
            ELSE NULL::uuid
        END
        """
        if has_provider_directory_location_address_key
        else "NULL::uuid"
    )
    ffs_practice_address_predicate = _address_source_keyable_predicate(
        first_line="NULL",
        city="fa.city",
        state="fa.state",
        zip_code="fa.zip_code",
    )
    mrf_address_predicate = _address_source_keyable_predicate(
        first_line="a.first_line",
        city="a.city_name",
        state="a.state_name",
        zip_code="a.postal_code",
        country="a.country_code",
    )
    provider_directory_address_predicate = _address_source_keyable_predicate(
        first_line="loc.first_line",
        city="loc.city_name",
        state="COALESCE(loc.state_name, loc.state_code)",
        zip_code="loc.postal_code",
        country="COALESCE(NULLIF(loc.country_code, ''), 'US')",
    )
    provider_directory_organization_address_predicate = _address_source_keyable_predicate(
        first_line="pd.first_line",
        city="pd.city_name",
        state="pd.state_name",
        zip_code="pd.postal_code",
        country="pd.country_code",
    )
    provider_directory_organization_address_key = (
        f"{db_schema}.addr_key_v1("
        "pd.first_line, pd.second_line, pd.city_name, "
        "pd.state_name, pd.postal_code, pd.country_code"
        ")"
        if is_address_canon_available
        else "NULL::uuid"
    )
    provider_directory_role_scope_filter = _provider_directory_scope_filter_sql(
        "role",
        source_ids=provider_directory_source_ids,
        run_id=provider_directory_run_id,
    )
    provider_directory_affiliation_scope_filter = _provider_directory_scope_filter_sql(
        "affiliation",
        source_ids=provider_directory_source_ids,
        run_id=provider_directory_run_id,
    )
    provider_directory_organization_scope_filter = _provider_directory_scope_filter_sql(
        "organization",
        source_ids=provider_directory_source_ids,
        run_id=provider_directory_run_id,
    )
    provider_directory_role_plan_network_join = _provider_directory_plan_network_join_sql(
        db_schema,
        "role",
        available,
        include_insurance_plans=True,
    )
    provider_directory_affiliation_plan_network_join = _provider_directory_plan_network_join_sql(
        db_schema,
        "affiliation",
        available,
        include_insurance_plans=False,
    )
    if has_provider_directory_healthcare_service:
        provider_directory_role_location_refs = f"""
                  JOIN LATERAL (
                      SELECT direct_location_ref.value
                        FROM jsonb_array_elements_text(
                              COALESCE(role.location_refs::jsonb, '[]'::jsonb)
                        ) AS direct_location_ref(value)
                      UNION
                      SELECT service_location_ref.value
                        FROM jsonb_array_elements_text(
                              COALESCE(role.healthcare_service_refs::jsonb, '[]'::jsonb)
                        ) AS service_ref(value)
                        JOIN LATERAL (
                            SELECT NULLIF(regexp_replace(service_ref.value, '^.*/', ''), '') AS resource_id
                        ) AS service_ref_id ON service_ref_id.resource_id IS NOT NULL
                        JOIN {db_schema}.provider_directory_healthcare_service AS healthcare_service
                          ON healthcare_service.source_id = role.source_id
                         AND healthcare_service.resource_id = service_ref_id.resource_id
                       CROSS JOIN LATERAL jsonb_array_elements_text(
                              COALESCE(healthcare_service.location_refs::jsonb, '[]'::jsonb)
                       ) AS service_location_ref(value)
                       WHERE healthcare_service.active IS DISTINCT FROM false
                  ) AS location_ref(value) ON TRUE"""
        provider_directory_affiliation_location_refs = f"""
                  JOIN LATERAL (
                      SELECT direct_location_ref.value
                        FROM jsonb_array_elements_text(
                              COALESCE(affiliation.location_refs::jsonb, '[]'::jsonb)
                        ) AS direct_location_ref(value)
                      UNION
                      SELECT service_location_ref.value
                        FROM jsonb_array_elements_text(
                              COALESCE(affiliation.healthcare_service_refs::jsonb, '[]'::jsonb)
                        ) AS service_ref(value)
                        JOIN LATERAL (
                            SELECT NULLIF(regexp_replace(service_ref.value, '^.*/', ''), '') AS resource_id
                        ) AS service_ref_id ON service_ref_id.resource_id IS NOT NULL
                        JOIN {db_schema}.provider_directory_healthcare_service AS healthcare_service
                          ON healthcare_service.source_id = affiliation.source_id
                         AND healthcare_service.resource_id = service_ref_id.resource_id
                       CROSS JOIN LATERAL jsonb_array_elements_text(
                              COALESCE(healthcare_service.location_refs::jsonb, '[]'::jsonb)
                       ) AS service_location_ref(value)
                       WHERE healthcare_service.active IS DISTINCT FROM false
                  ) AS location_ref(value) ON TRUE"""
    else:
        provider_directory_role_location_refs = """
                  JOIN LATERAL jsonb_array_elements_text(
                        COALESCE(role.location_refs::jsonb, '[]'::jsonb)
                  ) AS location_ref(value) ON TRUE"""
        provider_directory_affiliation_location_refs = """
                  JOIN LATERAL jsonb_array_elements_text(
                        COALESCE(affiliation.location_refs::jsonb, '[]'::jsonb)
                  ) AS location_ref(value) ON TRUE"""

    npi_join = f"LEFT JOIN {db_schema}.npi AS n ON n.npi = a.npi" if has_npi else ""
    doctors_npi_join = f"LEFT JOIN {db_schema}.npi AS n ON n.npi = d.npi" if has_npi else ""
    ffs_npi_join = f"LEFT JOIN {db_schema}.npi AS n ON n.npi = f.npi" if has_npi else ""
    pa_from = (
        f"LEFT JOIN LATERAL ("
        f"SELECT pa.taxonomy_array, pa.plans_network_array, pa.procedures_array, pa.medications_array "
        f"FROM {db_schema}.npi_address AS pa WHERE pa.npi = d.npi AND pa.type = 'primary' "
        f"ORDER BY pa.checksum LIMIT 1) AS pa ON TRUE"
        if has_npi_address
        else ""
    )
    ffs_pa_from = (
        f"LEFT JOIN LATERAL ("
        f"SELECT pa.taxonomy_array, pa.plans_network_array, pa.procedures_array, pa.medications_array "
        f"FROM {db_schema}.npi_address AS pa WHERE pa.npi = f.npi AND pa.type = 'primary' "
        f"ORDER BY pa.checksum LIMIT 1) AS pa ON TRUE"
        if has_npi_address
        else ""
    )
    mrf_pa_from = (
        f"LEFT JOIN LATERAL ("
        f"SELECT pa.taxonomy_array, pa.plans_network_array, pa.procedures_array, pa.medications_array "
        f"FROM {db_schema}.npi_address AS pa WHERE pa.npi = a.npi AND pa.type = 'primary' "
        f"ORDER BY pa.checksum LIMIT 1) AS pa ON TRUE"
        if has_npi_address
        else ""
    )
    ptg_pa_from = (
        f"LEFT JOIN LATERAL ("
        f"SELECT pa.taxonomy_array, pa.plans_network_array, pa.procedures_array, pa.medications_array "
        f"FROM {db_schema}.npi_address AS pa WHERE pa.npi = p.npi AND pa.type = 'primary' "
        f"ORDER BY pa.checksum LIMIT 1) AS pa ON TRUE"
        if has_npi_address
        else ""
    )
    provider_directory_pa_from = (
        "LEFT JOIN provider_directory_primary_npi_address AS pa ON pa.npi = pd.provider_npi"
        if has_npi_address
        else ""
    )
    def provider_directory_pa_cte(locations_cte_name: str) -> str:
        """Build the primary NPI-address CTE for provider-directory rows."""
        if not has_npi_address:
            return ""
        return f"""
            , provider_directory_primary_npi_address AS MATERIALIZED (
                SELECT DISTINCT ON (pa.npi)
                    pa.npi,
                    pa.taxonomy_array,
                    pa.plans_network_array,
                    pa.procedures_array,
                    pa.medications_array
                  FROM {db_schema}.npi_address AS pa
                  JOIN (
                        SELECT DISTINCT provider_npi
                          FROM {locations_cte_name}
                         WHERE provider_npi IS NOT NULL
                  ) AS provider_directory_npis
                    ON provider_directory_npis.provider_npi = pa.npi
                 WHERE pa.type = 'primary'
                 ORDER BY pa.npi, pa.checksum
            )
        """
    if has_npi_address:
        selects.append(
            f"""
            SELECT
                'npi'::varchar AS entity_type,
                a.npi::varchar AS entity_id,
                a.npi::bigint AS npi,
                NULL::bigint AS inferred_npi,
                NULL::float8 AS inference_confidence,
                NULL::varchar AS inference_method,
                {(_npi_entity_name_expr('n') if has_npi else 'NULL::varchar')} AS entity_name,
                {(_npi_entity_subtype_expr('n') if has_npi else 'NULL::varchar')} AS entity_subtype,
                COALESCE(NULLIF(a.type, ''), 'primary')::varchar AS type,
                COALESCE(a.taxonomy_array, ARRAY[0]::int[])::int[] AS taxonomy_array,
                COALESCE(a.plans_network_array, ARRAY[0]::int[])::int[] AS plans_network_array,
                COALESCE(a.procedures_array, ARRAY[0]::int[])::int[] AS procedures_array,
                COALESCE(a.medications_array, ARRAY[0]::int[])::int[] AS medications_array,
                ARRAY[]::varchar[] AS aca_plan_array,
                ARRAY[]::varchar[] AS aca_network_array,
                ARRAY[]::varchar[] AS ptg_plan_array,
                ARRAY[]::varchar[] AS ptg_source_array,
                ARRAY[]::varchar[] AS group_plan_array,
                '{BASE_ADDRESS_VERSION}'::varchar AS base_address_version,
                a.first_line::varchar AS first_line,
                a.second_line::varchar AS second_line,
                a.city_name::varchar AS city_name,
                a.state_name::varchar AS state_name,
                a.postal_code::varchar AS postal_code,
                COALESCE(NULLIF(a.country_code, ''), 'US')::varchar AS country_code,
                a.telephone_number::varchar AS telephone_number,
                a.fax_number::varchar AS fax_number,
                a.formatted_address::varchar AS formatted_address,
                a.lat::numeric AS lat,
                a.long::numeric AS long,
                a.date_added::date AS date_added,
                a.place_id::varchar AS place_id,
                {npi_address_key} AS address_key,
                NOW()::timestamp AS updated_at,
                'nppes'::varchar AS address_source,
                ('nppes:' || a.npi::varchar || ':' || COALESCE(a.type, '') || ':' || COALESCE(a.checksum::varchar, '0'))::varchar AS source_record_id
              FROM {db_schema}.npi_address AS a
              {npi_join}
             WHERE a.npi IS NOT NULL
            """
        )

    if has_doctors:
        selects.append(
            f"""
            SELECT
                'npi'::varchar AS entity_type,
                d.npi::varchar AS entity_id,
                d.npi::bigint AS npi,
                NULL::bigint AS inferred_npi,
                NULL::float8 AS inference_confidence,
                NULL::varchar AS inference_method,
                {(_npi_entity_name_expr('n') if has_npi else 'NULL::varchar')} AS entity_name,
                {(_npi_entity_subtype_expr('n') if has_npi else 'NULL::varchar')} AS entity_subtype,
                'practice'::varchar AS type,
                {('COALESCE(pa.taxonomy_array, ARRAY[0]::int[])::int[]' if has_npi_address else 'ARRAY[0]::int[]')} AS taxonomy_array,
                {('COALESCE(pa.plans_network_array, ARRAY[0]::int[])::int[]' if has_npi_address else 'ARRAY[0]::int[]')} AS plans_network_array,
                {('COALESCE(pa.procedures_array, ARRAY[0]::int[])::int[]' if has_npi_address else 'ARRAY[0]::int[]')} AS procedures_array,
                {('COALESCE(pa.medications_array, ARRAY[0]::int[])::int[]' if has_npi_address else 'ARRAY[0]::int[]')} AS medications_array,
                ARRAY[]::varchar[] AS aca_plan_array,
                ARRAY[]::varchar[] AS aca_network_array,
                ARRAY[]::varchar[] AS ptg_plan_array,
                ARRAY[]::varchar[] AS ptg_source_array,
                ARRAY[]::varchar[] AS group_plan_array,
                '{BASE_ADDRESS_VERSION}'::varchar AS base_address_version,
                d.address_line1::varchar AS first_line,
                d.address_line2::varchar AS second_line,
                d.city::varchar AS city_name,
                d.state::varchar AS state_name,
                LEFT(d.zip_code, 5)::varchar AS postal_code,
                'US'::varchar AS country_code,
                NULL::varchar AS telephone_number,
                NULL::varchar AS fax_number,
                NULL::varchar AS formatted_address,
                d.latitude::numeric AS lat,
                d.longitude::numeric AS long,
                NULL::date AS date_added,
                NULL::varchar AS place_id,
                {doctors_address_key} AS address_key,
                COALESCE(d.updated_at, NOW())::timestamp AS updated_at,
                'cms_doctors'::varchar AS address_source,
                ('cms_doctors:' || d.npi::varchar || ':' || COALESCE(d.address_checksum::varchar, '0'))::varchar AS source_record_id
              FROM {db_schema}.doctor_clinician_address AS d
              {doctors_npi_join}
              {pa_from}
             WHERE d.npi IS NOT NULL
            """
        )

    if has_ffs and has_ffs_address:
        selects.append(
            f"""
            SELECT
                'npi'::varchar AS entity_type,
                f.npi::varchar AS entity_id,
                f.npi::bigint AS npi,
                NULL::bigint AS inferred_npi,
                NULL::float8 AS inference_confidence,
                NULL::varchar AS inference_method,
                {(_npi_entity_name_expr('n') if has_npi else 'NULL::varchar')} AS entity_name,
                {(_npi_entity_subtype_expr('n') if has_npi else 'NULL::varchar')} AS entity_subtype,
                'secondary'::varchar AS type,
                {('COALESCE(pa.taxonomy_array, ARRAY[0]::int[])::int[]' if has_npi_address else 'ARRAY[0]::int[]')} AS taxonomy_array,
                {('COALESCE(pa.plans_network_array, ARRAY[0]::int[])::int[]' if has_npi_address else 'ARRAY[0]::int[]')} AS plans_network_array,
                {('COALESCE(pa.procedures_array, ARRAY[0]::int[])::int[]' if has_npi_address else 'ARRAY[0]::int[]')} AS procedures_array,
                {('COALESCE(pa.medications_array, ARRAY[0]::int[])::int[]' if has_npi_address else 'ARRAY[0]::int[]')} AS medications_array,
                ARRAY[]::varchar[] AS aca_plan_array,
                ARRAY[]::varchar[] AS aca_network_array,
                ARRAY[]::varchar[] AS ptg_plan_array,
                ARRAY[]::varchar[] AS ptg_source_array,
                ARRAY[]::varchar[] AS group_plan_array,
                '{BASE_ADDRESS_VERSION}'::varchar AS base_address_version,
                NULL::varchar AS first_line,
                NULL::varchar AS second_line,
                fa.city::varchar AS city_name,
                fa.state::varchar AS state_name,
                LEFT(fa.zip_code, 5)::varchar AS postal_code,
                'US'::varchar AS country_code,
                NULL::varchar AS telephone_number,
                NULL::varchar AS fax_number,
                NULL::varchar AS formatted_address,
                NULL::numeric AS lat,
                NULL::numeric AS long,
                fa.reporting_period_end::date AS date_added,
                NULL::varchar AS place_id,
                {ffs_address_key} AS address_key,
                COALESCE(f.imported_at, NOW())::timestamp AS updated_at,
                'provider_enrollment_ffs_address'::varchar AS address_source,
                ('provider_enrollment_ffs_address:' || COALESCE(fa.enrollment_id, fa.record_hash::varchar))::varchar AS source_record_id
              FROM {db_schema}.provider_enrollment_ffs_address AS fa
              JOIN {db_schema}.provider_enrollment_ffs AS f
                ON f.enrollment_id = fa.enrollment_id
              {ffs_npi_join}
              {ffs_pa_from}
             WHERE f.npi IS NOT NULL
               AND {ffs_practice_address_predicate}
            """
        )

    if has_facility:
        ccn_key_sql = (
            "regexp_replace(UPPER(COALESCE(NULLIF(fa.medicare_ccn, ''), "
            "CASE WHEN fa.facility_type = 'Hospital' THEN fa.id ELSE NULL END, '')), "
            "'[^A-Z0-9]', '', 'g')"
        )
        facility_ccn_candidate_fragments: list[str] = []
        if has_hospital_enrollment:
            facility_ccn_candidate_fragments.extend(
                [
                    f"""
                    SELECT
                        'Hospital'::varchar AS facility_type,
                        regexp_replace(UPPER(COALESCE(h.ccn, '')), '[^A-Z0-9]', '', 'g')::varchar AS ccn_key,
                        h.npi::bigint AS candidate_npi,
                        'hospital_pecos_ccn_unique'::varchar AS candidate_method,
                        0.99::float8 AS confidence
                      FROM {db_schema}.provider_enrollment_hospital AS h
                     WHERE h.npi IS NOT NULL
                       AND COALESCE(h.ccn, '') <> ''
                    """,
                    f"""
                    SELECT
                        'Hospital'::varchar AS facility_type,
                        regexp_replace(UPPER(COALESCE(h.cah_or_hospital_ccn, '')), '[^A-Z0-9]', '', 'g')::varchar AS ccn_key,
                        h.npi::bigint AS candidate_npi,
                        'hospital_pecos_cah_ccn_unique'::varchar AS candidate_method,
                        0.99::float8 AS confidence
                      FROM {db_schema}.provider_enrollment_hospital AS h
                     WHERE h.npi IS NOT NULL
                       AND COALESCE(h.cah_or_hospital_ccn, '') <> ''
                    """,
                ]
            )
            if has_ffs_additional_npi:
                facility_ccn_candidate_fragments.extend(
                    [
                        f"""
                        SELECT
                            'Hospital'::varchar AS facility_type,
                            regexp_replace(UPPER(COALESCE(h.ccn, '')), '[^A-Z0-9]', '', 'g')::varchar AS ccn_key,
                            a.additional_npi::bigint AS candidate_npi,
                            'hospital_pecos_additional_npi_unique'::varchar AS candidate_method,
                            0.975::float8 AS confidence
                          FROM {db_schema}.provider_enrollment_hospital AS h
                          JOIN {db_schema}.provider_enrollment_ffs_additional_npi AS a
                            ON a.enrollment_id = h.enrollment_id
                           AND a.additional_npi IS NOT NULL
                         WHERE COALESCE(h.ccn, '') <> ''
                        """,
                        f"""
                        SELECT
                            'Hospital'::varchar AS facility_type,
                            regexp_replace(UPPER(COALESCE(h.cah_or_hospital_ccn, '')), '[^A-Z0-9]', '', 'g')::varchar AS ccn_key,
                            a.additional_npi::bigint AS candidate_npi,
                            'hospital_pecos_additional_npi_unique'::varchar AS candidate_method,
                            0.975::float8 AS confidence
                          FROM {db_schema}.provider_enrollment_hospital AS h
                          JOIN {db_schema}.provider_enrollment_ffs_additional_npi AS a
                            ON a.enrollment_id = h.enrollment_id
                           AND a.additional_npi IS NOT NULL
                         WHERE COALESCE(h.cah_or_hospital_ccn, '') <> ''
                        """,
                    ]
                )
        if has_fqhc_enrollment:
            facility_ccn_candidate_fragments.append(
                f"""
                SELECT
                    'FQHC'::varchar AS facility_type,
                    regexp_replace(UPPER(COALESCE(f.ccn, '')), '[^A-Z0-9]', '', 'g')::varchar AS ccn_key,
                    f.npi::bigint AS candidate_npi,
                    'fqhc_pecos_ccn_unique'::varchar AS candidate_method,
                    0.985::float8 AS confidence
                  FROM {db_schema}.provider_enrollment_fqhc AS f
                 WHERE f.npi IS NOT NULL
                   AND COALESCE(f.ccn, '') <> ''
                """
            )
            if has_ffs_additional_npi:
                facility_ccn_candidate_fragments.append(
                    f"""
                    SELECT
                        'FQHC'::varchar AS facility_type,
                        regexp_replace(UPPER(COALESCE(f.ccn, '')), '[^A-Z0-9]', '', 'g')::varchar AS ccn_key,
                        a.additional_npi::bigint AS candidate_npi,
                        'fqhc_pecos_additional_npi_unique'::varchar AS candidate_method,
                        0.97::float8 AS confidence
                      FROM {db_schema}.provider_enrollment_fqhc AS f
                      JOIN {db_schema}.provider_enrollment_ffs_additional_npi AS a
                        ON a.enrollment_id = f.enrollment_id
                       AND a.additional_npi IS NOT NULL
                     WHERE COALESCE(f.ccn, '') <> ''
                    """
                )
        facility_ccn_candidates_sql = (
            "\n                    UNION ALL\n".join(facility_ccn_candidate_fragments)
            if facility_ccn_candidate_fragments
            else """
                    SELECT
                        NULL::varchar AS facility_type,
                        NULL::varchar AS ccn_key,
                        NULL::bigint AS candidate_npi,
                        NULL::varchar AS candidate_method,
                        NULL::float8 AS confidence
                     WHERE FALSE
            """
        )
        # When NPPES taxonomy is available, resolve CCN->multiple-NPI conflicts by
        # selecting the candidate NPI whose NPPES taxonomy matches the facility type
        # (prefer the NPI carrying it as primary, else the unique any-taxonomy match).
        # CCNs whose conflict is not broken this way stay unresolved and flow to review.
        has_npi_taxonomy = available.get("npi_taxonomy", False)
        if has_npi_taxonomy:
            hospital_taxonomy_codes_sql = _hospital_facility_taxonomy_codes_sql(
                "                                "
            )
            _facility_tax_cond = (
                "(c.facility_type = 'FQHC' AND nt.healthcare_provider_taxonomy_code = '261QF0400X')\n"
                "                                OR (c.facility_type = 'Hospital'\n"
                "                                    AND nt.healthcare_provider_taxonomy_code IN (\n"
                f"{hospital_taxonomy_codes_sql}\n"
                "                                ))"
            )
            facility_taxonomy_ctes = f""",
            facility_ccn_npi_stats AS (
                SELECT facility_type, ccn_key, COUNT(DISTINCT candidate_npi) AS n_distinct_npi
                  FROM facility_ccn_npi_candidates
                 WHERE candidate_npi IS NOT NULL
                   AND COALESCE(ccn_key, '') <> ''
              GROUP BY facility_type, ccn_key
            ),
            facility_ccn_conflict_taxonomy AS (
                SELECT DISTINCT
                    c.facility_type,
                    c.ccn_key,
                    c.candidate_npi,
                    (EXISTS (
                        SELECT 1
                          FROM {db_schema}.npi_taxonomy AS nt
                         WHERE nt.npi = c.candidate_npi
                           AND nt.healthcare_provider_primary_taxonomy_switch = 'Y'
                           AND ({_facility_tax_cond})
                    )) AS tax_primary_match,
                    (EXISTS (
                        SELECT 1
                          FROM {db_schema}.npi_taxonomy AS nt
                         WHERE nt.npi = c.candidate_npi
                           AND ({_facility_tax_cond})
                    )) AS tax_any_match
                  FROM facility_ccn_npi_candidates AS c
                  JOIN facility_ccn_npi_stats AS s
                    ON s.facility_type = c.facility_type
                   AND s.ccn_key = c.ccn_key
                 WHERE c.candidate_npi IS NOT NULL
                   AND COALESCE(c.ccn_key, '') <> ''
                   AND s.n_distinct_npi > 1
            ),
            facility_ccn_taxonomy_npi AS (
                SELECT
                    facility_type,
                    ccn_key,
                    (CASE
                        WHEN COUNT(*) FILTER (WHERE tax_primary_match) = 1
                            THEN MIN(candidate_npi) FILTER (WHERE tax_primary_match)
                        WHEN COUNT(*) FILTER (WHERE tax_any_match) = 1
                            THEN MIN(candidate_npi) FILTER (WHERE tax_any_match)
                    END)::bigint AS npi,
                    0.95::float8 AS confidence,
                    (CASE
                        WHEN facility_type = 'FQHC' THEN 'fqhc_pecos_ccn_taxonomy'
                        ELSE 'hospital_pecos_ccn_taxonomy'
                    END)::varchar AS inference_method
                  FROM facility_ccn_conflict_taxonomy
              GROUP BY facility_type, ccn_key
                HAVING COUNT(*) FILTER (WHERE tax_primary_match) = 1
                    OR COUNT(*) FILTER (WHERE tax_any_match) = 1
            ),
            facility_ccn_resolved_npi AS (
                SELECT facility_type, ccn_key, npi, confidence, inference_method
                  FROM facility_ccn_unique_npi
                 UNION ALL
                SELECT facility_type, ccn_key, npi, confidence, inference_method
                  FROM facility_ccn_taxonomy_npi
                 WHERE npi IS NOT NULL
            )"""
            facility_ccn_relation = "facility_ccn_resolved_npi"
        else:
            facility_taxonomy_ctes = ""
            facility_ccn_relation = "facility_ccn_unique_npi"
        selects.append(
            f"""
            WITH facility_ccn_npi_candidates AS (
                {facility_ccn_candidates_sql}
            ),
            facility_ccn_unique_npi AS (
                SELECT
                    facility_type,
                    ccn_key,
                    MIN(candidate_npi)::bigint AS npi,
                    MAX(confidence)::float8 AS confidence,
                    CASE
                        WHEN COUNT(DISTINCT candidate_method) = 1 THEN MIN(candidate_method)
                        WHEN facility_type = 'FQHC' THEN 'fqhc_pecos_ccn_unique'
                        ELSE 'hospital_pecos_ccn_unique'
                    END::varchar AS inference_method
                  FROM facility_ccn_npi_candidates
                 WHERE candidate_npi IS NOT NULL
                   AND COALESCE(ccn_key, '') <> ''
              GROUP BY facility_type, ccn_key
                HAVING COUNT(DISTINCT candidate_npi) = 1
            ){facility_taxonomy_ctes}
            SELECT
                'facility_anchor'::varchar AS entity_type,
                fa.id::varchar AS entity_id,
                fa.npi::bigint AS npi,
                CASE WHEN fa.npi IS NULL THEN ccn_npi.npi ELSE NULL::bigint END AS inferred_npi,
                CASE WHEN fa.npi IS NULL AND ccn_npi.npi IS NOT NULL THEN ccn_npi.confidence ELSE NULL::float8 END AS inference_confidence,
                CASE WHEN fa.npi IS NULL AND ccn_npi.npi IS NOT NULL THEN ccn_npi.inference_method ELSE NULL::varchar END AS inference_method,
                fa.name::varchar AS entity_name,
                fa.facility_type::varchar AS entity_subtype,
                'site'::varchar AS type,
                ARRAY[0]::int[] AS taxonomy_array,
                ARRAY[0]::int[] AS plans_network_array,
                ARRAY[0]::int[] AS procedures_array,
                ARRAY[0]::int[] AS medications_array,
                ARRAY[]::varchar[] AS aca_plan_array,
                ARRAY[]::varchar[] AS aca_network_array,
                ARRAY[]::varchar[] AS ptg_plan_array,
                ARRAY[]::varchar[] AS ptg_source_array,
                ARRAY[]::varchar[] AS group_plan_array,
                '{BASE_ADDRESS_VERSION}'::varchar AS base_address_version,
                fa.address_line1::varchar AS first_line,
                NULL::varchar AS second_line,
                fa.city::varchar AS city_name,
                fa.state::varchar AS state_name,
                LEFT(fa.zip_code, 5)::varchar AS postal_code,
                'US'::varchar AS country_code,
                fa.telephone_number::varchar AS telephone_number,
                NULL::varchar AS fax_number,
                NULL::varchar AS formatted_address,
                fa.latitude::numeric AS lat,
                fa.longitude::numeric AS long,
                NULL::date AS date_added,
                NULL::varchar AS place_id,
                {facility_address_key} AS address_key,
                COALESCE(fa.updated_at, NOW())::timestamp AS updated_at,
                ('facility_anchor:' || LOWER(COALESCE(fa.source_dataset, 'unknown')))::varchar AS address_source,
                ('facility_anchor:' || COALESCE(fa.id, 'unknown'))::varchar AS source_record_id
              FROM {db_schema}.facility_anchor AS fa
              LEFT JOIN {facility_ccn_relation} AS ccn_npi
                ON ccn_npi.facility_type = fa.facility_type
               AND ccn_npi.ccn_key = {ccn_key_sql}
            """
        )

    if has_mrf_address:
        selects.append(
            f"""
            SELECT
                'npi'::varchar AS entity_type,
                a.npi::varchar AS entity_id,
                a.npi::bigint AS npi,
                NULL::bigint AS inferred_npi,
                NULL::float8 AS inference_confidence,
                NULL::varchar AS inference_method,
                {(_npi_entity_name_expr('n') if has_npi else 'NULL::varchar')} AS entity_name,
                {(_npi_entity_subtype_expr('n') if has_npi else 'NULL::varchar')} AS entity_subtype,
                COALESCE(NULLIF(a.type, ''), 'practice')::varchar AS type,
                {('COALESCE(pa.taxonomy_array, ARRAY[0]::int[])::int[]' if has_npi_address else 'ARRAY[0]::int[]')} AS taxonomy_array,
                {('COALESCE(pa.plans_network_array, ARRAY[0]::int[])::int[]' if has_npi_address else 'ARRAY[0]::int[]')} AS plans_network_array,
                {('COALESCE(pa.procedures_array, ARRAY[0]::int[])::int[]' if has_npi_address else 'ARRAY[0]::int[]')} AS procedures_array,
                {('COALESCE(pa.medications_array, ARRAY[0]::int[])::int[]' if has_npi_address else 'ARRAY[0]::int[]')} AS medications_array,
                ARRAY[]::varchar[] AS aca_plan_array,
                ARRAY[]::varchar[] AS aca_network_array,
                ARRAY[]::varchar[] AS ptg_plan_array,
                ARRAY[]::varchar[] AS ptg_source_array,
                ARRAY[]::varchar[] AS group_plan_array,
                '{BASE_ADDRESS_VERSION}'::varchar AS base_address_version,
                a.first_line::varchar AS first_line,
                a.second_line::varchar AS second_line,
                a.city_name::varchar AS city_name,
                a.state_name::varchar AS state_name,
                a.postal_code::varchar AS postal_code,
                COALESCE(NULLIF(a.country_code, ''), 'US')::varchar AS country_code,
                a.telephone_number::varchar AS telephone_number,
                a.fax_number::varchar AS fax_number,
                a.formatted_address::varchar AS formatted_address,
                a.lat::numeric AS lat,
                a.long::numeric AS long,
                a.date_added::date AS date_added,
                a.place_id::varchar AS place_id,
                {mrf_address_key} AS address_key,
                NOW()::timestamp AS updated_at,
                'mrf'::varchar AS address_source,
                ('mrf:' || a.npi::varchar || ':' || COALESCE(a.type, '') || ':' || COALESCE(a.checksum::varchar, '0'))::varchar AS source_record_id
              FROM {db_schema}.mrf_address AS a
              {npi_join}
              {mrf_pa_from}
             WHERE a.npi IS NOT NULL
               AND {mrf_address_predicate}
            """
        )

    if has_provider_directory_practitioner and has_provider_directory_role and has_provider_directory_location:
        selects.append(
            f"""
            WITH provider_directory_practitioner_locations AS (
                SELECT
                    practitioner.npi::bigint AS provider_npi,
                    practitioner.full_name::varchar AS provider_name,
                    practitioner.updated_at AS provider_updated_at,
                    role.source_id,
                    role.resource_id AS role_resource_id,
                    role.updated_at AS role_updated_at,
                    plan_network_evidence.plan_identifiers,
                    plan_network_evidence.network_names,
                    loc.resource_id AS location_resource_id,
                    loc.name::varchar AS location_name,
                    loc.first_line::varchar AS first_line,
                    loc.second_line::varchar AS second_line,
                    loc.city_name::varchar AS city_name,
                    COALESCE(NULLIF(loc.state_name, ''), loc.state_code)::varchar AS state_name,
                    loc.postal_code::varchar AS postal_code,
                    COALESCE(NULLIF(loc.country_code, ''), 'US')::varchar AS country_code,
                    COALESCE(
                        CASE
                            WHEN loc.phone_number IS NOT NULL THEN loc.telephone_number
                        END,
                        role_phone.telephone_number
                    )::varchar AS telephone_number,
                    loc.fax_number::varchar AS fax_number,
                    loc.latitude::varchar AS latitude,
                    loc.longitude::varchar AS longitude,
                    loc.updated_at AS location_updated_at,
                    {provider_directory_address_key} AS address_key
                  FROM {db_schema}.provider_directory_practitioner_role AS role
                  JOIN {db_schema}.provider_directory_practitioner AS practitioner
                    ON practitioner.source_id = role.source_id
                   AND practitioner.resource_id = NULLIF(
                        regexp_replace(COALESCE(role.practitioner_ref, ''), '^.*/', ''),
                        ''
                   )
                  {provider_directory_role_plan_network_join}
                  {provider_directory_role_location_refs}
                  JOIN LATERAL (
                      SELECT NULLIF(regexp_replace(location_ref.value, '^.*/', ''), '') AS resource_id
                  ) AS location_ref_id ON location_ref_id.resource_id IS NOT NULL
                  JOIN {db_schema}.provider_directory_location AS loc
                    ON loc.source_id = role.source_id
                   AND loc.resource_id = location_ref_id.resource_id
                  LEFT JOIN LATERAL (
                      SELECT telecom.value->>'value' AS telephone_number
                        FROM jsonb_array_elements(COALESCE(role.telecom::jsonb, '[]'::jsonb)) AS telecom(value)
                       WHERE telecom.value->>'system' = 'phone'
                         AND NULLIF(TRIM(telecom.value->>'value'), '') IS NOT NULL
                       LIMIT 1
                  ) AS role_phone ON TRUE
                 WHERE practitioner.npi IS NOT NULL
                   AND practitioner.npi BETWEEN 1000000000 AND 9999999999
                   AND practitioner.active IS DISTINCT FROM false
                   AND role.active IS DISTINCT FROM false
                   AND (loc.status IS NULL OR lower(loc.status) <> 'inactive')
                   AND {provider_directory_address_predicate}
                   {provider_directory_role_scope_filter}
            )
            {provider_directory_pa_cte("provider_directory_practitioner_locations")}
            SELECT
                'npi'::varchar AS entity_type,
                pd.provider_npi::varchar AS entity_id,
                pd.provider_npi::bigint AS npi,
                NULL::bigint AS inferred_npi,
                NULL::float8 AS inference_confidence,
                NULL::varchar AS inference_method,
                pd.provider_name::varchar AS entity_name,
                'provider_directory_practitioner'::varchar AS entity_subtype,
                'practice'::varchar AS type,
                {('COALESCE(pa.taxonomy_array, ARRAY[0]::int[])::int[]' if has_npi_address else 'ARRAY[0]::int[]')} AS taxonomy_array,
                {('COALESCE(pa.plans_network_array, ARRAY[0]::int[])::int[]' if has_npi_address else 'ARRAY[0]::int[]')} AS plans_network_array,
                {('COALESCE(pa.procedures_array, ARRAY[0]::int[])::int[]' if has_npi_address else 'ARRAY[0]::int[]')} AS procedures_array,
                {('COALESCE(pa.medications_array, ARRAY[0]::int[])::int[]' if has_npi_address else 'ARRAY[0]::int[]')} AS medications_array,
                COALESCE(pd.plan_identifiers, ARRAY[]::varchar[])::varchar[] AS aca_plan_array,
                COALESCE(pd.network_names, ARRAY[]::varchar[])::varchar[] AS aca_network_array,
                ARRAY[]::varchar[] AS ptg_plan_array,
                ARRAY[]::varchar[] AS ptg_source_array,
                ARRAY[]::varchar[] AS group_plan_array,
                '{BASE_ADDRESS_VERSION}'::varchar AS base_address_version,
                pd.first_line::varchar AS first_line,
                pd.second_line::varchar AS second_line,
                pd.city_name::varchar AS city_name,
                pd.state_name::varchar AS state_name,
                pd.postal_code::varchar AS postal_code,
                pd.country_code::varchar AS country_code,
                pd.telephone_number::varchar AS telephone_number,
                pd.fax_number::varchar AS fax_number,
                NULL::varchar AS formatted_address,
                {_coordinate_from_text_pair_sql("pd.latitude", "pd.longitude", "pd.country_code", axis="lat")} AS lat,
                {_coordinate_from_text_pair_sql("pd.latitude", "pd.longitude", "pd.country_code", axis="long")} AS long,
                NULL::date AS date_added,
                NULL::varchar AS place_id,
                pd.address_key AS address_key,
                GREATEST(
                    COALESCE(pd.role_updated_at, TIMESTAMP 'epoch'),
                    COALESCE(pd.provider_updated_at, TIMESTAMP 'epoch'),
                    COALESCE(pd.location_updated_at, TIMESTAMP 'epoch')
                )::timestamp AS updated_at,
                'provider_directory_fhir'::varchar AS address_source,
                (
                    'provider_directory_fhir:practitioner_role:'
                    || pd.source_id || ':' || pd.role_resource_id || ':' || pd.location_resource_id
                )::varchar AS source_record_id
              FROM provider_directory_practitioner_locations AS pd
              {provider_directory_pa_from}
            """
        )

    if has_provider_directory_organization and has_provider_directory_affiliation and has_provider_directory_location:
        selects.append(
            f"""
            WITH provider_directory_organization_locations AS (
                SELECT
                    organization.npi::bigint AS provider_npi,
                    organization.name::varchar AS provider_name,
                    organization.updated_at AS provider_updated_at,
                    affiliation.source_id,
                    affiliation.resource_id AS affiliation_resource_id,
                    affiliation.updated_at AS affiliation_updated_at,
                    plan_network_evidence.network_names,
                    loc.resource_id AS location_resource_id,
                    loc.name::varchar AS location_name,
                    loc.first_line::varchar AS first_line,
                    loc.second_line::varchar AS second_line,
                    loc.city_name::varchar AS city_name,
                    COALESCE(NULLIF(loc.state_name, ''), loc.state_code)::varchar AS state_name,
                    loc.postal_code::varchar AS postal_code,
                    COALESCE(NULLIF(loc.country_code, ''), 'US')::varchar AS country_code,
                    loc.telephone_number::varchar AS telephone_number,
                    loc.fax_number::varchar AS fax_number,
                    loc.latitude::varchar AS latitude,
                    loc.longitude::varchar AS longitude,
                    loc.updated_at AS location_updated_at,
                    {provider_directory_address_key} AS address_key
                  FROM {db_schema}.provider_directory_organization_affiliation AS affiliation
                  JOIN LATERAL (
                      SELECT DISTINCT normalized_ref AS resource_id
                        FROM (
                            VALUES
                                (NULLIF(regexp_replace(COALESCE(affiliation.organization_ref, ''), '^.*/', ''), '')),
                                (NULLIF(regexp_replace(COALESCE(affiliation.participating_organization_ref, ''), '^.*/', ''), ''))
                        ) AS refs(normalized_ref)
                       WHERE normalized_ref IS NOT NULL
                  ) AS organization_ref ON TRUE
                  JOIN {db_schema}.provider_directory_organization AS organization
                    ON organization.source_id = affiliation.source_id
                   AND organization.resource_id = organization_ref.resource_id
                  {provider_directory_affiliation_plan_network_join}
                  {provider_directory_affiliation_location_refs}
                  JOIN LATERAL (
                      SELECT NULLIF(regexp_replace(location_ref.value, '^.*/', ''), '') AS resource_id
                  ) AS location_ref_id ON location_ref_id.resource_id IS NOT NULL
                  JOIN {db_schema}.provider_directory_location AS loc
                    ON loc.source_id = affiliation.source_id
                   AND loc.resource_id = location_ref_id.resource_id
                 WHERE organization.npi IS NOT NULL
                   AND organization.npi BETWEEN 1000000000 AND 9999999999
                   AND organization.active IS DISTINCT FROM false
                   AND affiliation.active IS DISTINCT FROM false
                   AND (loc.status IS NULL OR lower(loc.status) <> 'inactive')
                   AND {provider_directory_address_predicate}
                   {provider_directory_affiliation_scope_filter}
            )
            {provider_directory_pa_cte("provider_directory_organization_locations")}
            SELECT
                'npi'::varchar AS entity_type,
                pd.provider_npi::varchar AS entity_id,
                pd.provider_npi::bigint AS npi,
                NULL::bigint AS inferred_npi,
                NULL::float8 AS inference_confidence,
                NULL::varchar AS inference_method,
                pd.provider_name::varchar AS entity_name,
                'provider_directory_organization'::varchar AS entity_subtype,
                'practice'::varchar AS type,
                {('COALESCE(pa.taxonomy_array, ARRAY[0]::int[])::int[]' if has_npi_address else 'ARRAY[0]::int[]')} AS taxonomy_array,
                {('COALESCE(pa.plans_network_array, ARRAY[0]::int[])::int[]' if has_npi_address else 'ARRAY[0]::int[]')} AS plans_network_array,
                {('COALESCE(pa.procedures_array, ARRAY[0]::int[])::int[]' if has_npi_address else 'ARRAY[0]::int[]')} AS procedures_array,
                {('COALESCE(pa.medications_array, ARRAY[0]::int[])::int[]' if has_npi_address else 'ARRAY[0]::int[]')} AS medications_array,
                ARRAY[]::varchar[] AS aca_plan_array,
                COALESCE(pd.network_names, ARRAY[]::varchar[])::varchar[] AS aca_network_array,
                ARRAY[]::varchar[] AS ptg_plan_array,
                ARRAY[]::varchar[] AS ptg_source_array,
                ARRAY[]::varchar[] AS group_plan_array,
                '{BASE_ADDRESS_VERSION}'::varchar AS base_address_version,
                pd.first_line::varchar AS first_line,
                pd.second_line::varchar AS second_line,
                pd.city_name::varchar AS city_name,
                pd.state_name::varchar AS state_name,
                pd.postal_code::varchar AS postal_code,
                pd.country_code::varchar AS country_code,
                pd.telephone_number::varchar AS telephone_number,
                pd.fax_number::varchar AS fax_number,
                NULL::varchar AS formatted_address,
                {_coordinate_from_text_pair_sql("pd.latitude", "pd.longitude", "pd.country_code", axis="lat")} AS lat,
                {_coordinate_from_text_pair_sql("pd.latitude", "pd.longitude", "pd.country_code", axis="long")} AS long,
                NULL::date AS date_added,
                NULL::varchar AS place_id,
                pd.address_key AS address_key,
                GREATEST(
                    COALESCE(pd.affiliation_updated_at, TIMESTAMP 'epoch'),
                    COALESCE(pd.provider_updated_at, TIMESTAMP 'epoch'),
                    COALESCE(pd.location_updated_at, TIMESTAMP 'epoch')
                )::timestamp AS updated_at,
                'provider_directory_fhir'::varchar AS address_source,
                (
                    'provider_directory_fhir:organization_affiliation:'
                    || pd.source_id || ':' || pd.affiliation_resource_id || ':' || pd.location_resource_id
                )::varchar AS source_record_id
              FROM provider_directory_organization_locations AS pd
              {provider_directory_pa_from}
            """
        )

    if has_provider_directory_organization:
        selects.append(
            f"""
            WITH provider_directory_organization_addresses AS (
                SELECT
                    organization.npi::bigint AS provider_npi,
                    organization.name::varchar AS provider_name,
                    organization.source_id,
                    organization.resource_id AS organization_resource_id,
                    organization.updated_at AS organization_updated_at,
                    addr.ordinal::bigint AS address_ordinal,
                    NULLIF(TRIM(addr.value->'line'->>0), '')::varchar AS first_line,
                    NULLIF(TRIM(addr.value->'line'->>1), '')::varchar AS second_line,
                    NULLIF(TRIM(addr.value->>'city'), '')::varchar AS city_name,
                    NULLIF(TRIM(addr.value->>'state'), '')::varchar AS state_name,
                    NULLIF(TRIM(addr.value->>'postalCode'), '')::varchar AS postal_code,
                    CASE
                        WHEN UPPER(REGEXP_REPLACE(TRIM(COALESCE(addr.value->>'country', '')), '[^A-Za-z]+', '', 'g'))
                             IN ('', 'US', 'USA', 'UNITEDSTATES', 'UNITEDSTATESOFAMERICA')
                            THEN 'US'
                        ELSE NULLIF(TRIM(addr.value->>'country'), '')
                    END::varchar AS country_code,
                    org_phone.telephone_number::varchar AS telephone_number,
                    org_fax.fax_number::varchar AS fax_number
                  FROM {db_schema}.provider_directory_organization AS organization
                  JOIN LATERAL jsonb_array_elements(
                        COALESCE(organization.address_json::jsonb, '[]'::jsonb)
                  ) WITH ORDINALITY AS addr(value, ordinal) ON TRUE
                  LEFT JOIN LATERAL (
                      SELECT telecom.value->>'value' AS telephone_number
                        FROM jsonb_array_elements(COALESCE(organization.telecom::jsonb, '[]'::jsonb)) AS telecom(value)
                       WHERE telecom.value->>'system' = 'phone'
                         AND NULLIF(TRIM(telecom.value->>'value'), '') IS NOT NULL
                       LIMIT 1
                  ) AS org_phone ON TRUE
                  LEFT JOIN LATERAL (
                      SELECT telecom.value->>'value' AS fax_number
                        FROM jsonb_array_elements(COALESCE(organization.telecom::jsonb, '[]'::jsonb)) AS telecom(value)
                       WHERE telecom.value->>'system' = 'fax'
                         AND NULLIF(TRIM(telecom.value->>'value'), '') IS NOT NULL
                       LIMIT 1
                  ) AS org_fax ON TRUE
                 WHERE organization.npi IS NOT NULL
                   AND organization.npi BETWEEN 1000000000 AND 9999999999
                   AND organization.active IS DISTINCT FROM false
                   {provider_directory_organization_scope_filter}
            )
            {provider_directory_pa_cte("provider_directory_organization_addresses")}
            SELECT
                'npi'::varchar AS entity_type,
                pd.provider_npi::varchar AS entity_id,
                pd.provider_npi::bigint AS npi,
                NULL::bigint AS inferred_npi,
                NULL::float8 AS inference_confidence,
                NULL::varchar AS inference_method,
                pd.provider_name::varchar AS entity_name,
                'provider_directory_organization'::varchar AS entity_subtype,
                'practice'::varchar AS type,
                {('COALESCE(pa.taxonomy_array, ARRAY[0]::int[])::int[]' if has_npi_address else 'ARRAY[0]::int[]')} AS taxonomy_array,
                {('COALESCE(pa.plans_network_array, ARRAY[0]::int[])::int[]' if has_npi_address else 'ARRAY[0]::int[]')} AS plans_network_array,
                {('COALESCE(pa.procedures_array, ARRAY[0]::int[])::int[]' if has_npi_address else 'ARRAY[0]::int[]')} AS procedures_array,
                {('COALESCE(pa.medications_array, ARRAY[0]::int[])::int[]' if has_npi_address else 'ARRAY[0]::int[]')} AS medications_array,
                ARRAY[]::varchar[] AS aca_plan_array,
                ARRAY[]::varchar[] AS aca_network_array,
                ARRAY[]::varchar[] AS ptg_plan_array,
                ARRAY[]::varchar[] AS ptg_source_array,
                ARRAY[]::varchar[] AS group_plan_array,
                '{BASE_ADDRESS_VERSION}'::varchar AS base_address_version,
                pd.first_line::varchar AS first_line,
                pd.second_line::varchar AS second_line,
                pd.city_name::varchar AS city_name,
                pd.state_name::varchar AS state_name,
                pd.postal_code::varchar AS postal_code,
                pd.country_code::varchar AS country_code,
                pd.telephone_number::varchar AS telephone_number,
                pd.fax_number::varchar AS fax_number,
                NULL::varchar AS formatted_address,
                NULL::numeric AS lat,
                NULL::numeric AS long,
                NULL::date AS date_added,
                NULL::varchar AS place_id,
                {provider_directory_organization_address_key} AS address_key,
                COALESCE(pd.organization_updated_at, NOW())::timestamp AS updated_at,
                'provider_directory_fhir'::varchar AS address_source,
                (
                    'provider_directory_fhir:organization_address:'
                    || pd.source_id || ':' || pd.organization_resource_id || ':' || pd.address_ordinal::varchar
                )::varchar AS source_record_id
              FROM provider_directory_organization_addresses AS pd
              {provider_directory_pa_from}
             WHERE {provider_directory_organization_address_predicate}
            """
        )

    if test_limit_per_source and test_limit_per_source > 0:
        return [
            "(\n"
            f"SELECT * FROM (\n{select.strip()}\n) AS src LIMIT {int(test_limit_per_source)}\n"
            ")"
            for select in selects
        ]
    return selects


def _partial_overlay_npi_context_sql(
    db_schema: str,
    available: dict[str, bool],
) -> tuple[str, str, tuple[str, str, str, str]]:
    """Return optional NPI joins and array expressions for overlay projection."""

    npi_join = (
        f"LEFT JOIN {db_schema}.npi AS n ON n.npi = overlay.npi"
        if available.get("npi", False)
        else ""
    )
    if not available.get("npi_address", False):
        return npi_join, "", ("ARRAY[0]::int[]",) * 4
    return (
        npi_join,
        f"""
        LEFT JOIN LATERAL (
            SELECT
                pa.taxonomy_array,
                pa.plans_network_array,
                pa.procedures_array,
                pa.medications_array
              FROM {db_schema}.npi_address AS pa
             WHERE pa.npi = overlay.npi
               AND pa.type = 'primary'
             ORDER BY pa.checksum
             LIMIT 1
        ) AS pa ON TRUE
        """,
        (
            "COALESCE(pa.taxonomy_array, ARRAY[0]::int[])::int[]",
            "COALESCE(pa.plans_network_array, ARRAY[0]::int[])::int[]",
            "COALESCE(pa.procedures_array, ARRAY[0]::int[])::int[]",
            "COALESCE(pa.medications_array, ARRAY[0]::int[])::int[]",
        ),
    )


def _provider_directory_partial_overlay_source_select(
    db_schema: str,
    available: dict[str, bool],
    *,
    source_ids: list[str] | tuple[str, ...] | None = None,
    run_id: str | None = None,
    affected_group_table: str | None = None,
) -> str:
    """Project only current, dataset-member Provider Directory overlay rows."""

    npi_join, primary_npi_address_join, array_expressions = (
        _partial_overlay_npi_context_sql(db_schema, available)
    )
    entity_name = _npi_entity_name_expr("n") if npi_join else "NULL::varchar"
    entity_subtype = _npi_entity_subtype_expr("n") if npi_join else "NULL::varchar"
    address_predicate = _address_source_keyable_predicate(
        first_line="overlay.first_line",
        city="overlay.city_name",
        state="COALESCE(overlay.state_name, overlay.state_code)",
        zip_code="overlay.postal_code",
        country="COALESCE(NULLIF(overlay.country_code, ''), 'US')",
    )
    return _PROVIDER_DIRECTORY_PARTIAL_OVERLAY_SOURCE_TEMPLATE.format(
        current_overlay_ctes=_provider_directory_current_overlay_ctes_sql(
            db_schema,
            source_ids=source_ids,
            run_id=run_id,
            affected_group_table=affected_group_table,
        ),
        entity_name=entity_name,
        entity_subtype=entity_subtype,
        taxonomy_array=array_expressions[0],
        plans_network_array=array_expressions[1],
        procedures_array=array_expressions[2],
        medications_array=array_expressions[3],
        base_address_version=BASE_ADDRESS_VERSION,
        npi_join=npi_join,
        primary_npi_address_join=primary_npi_address_join,
        address_predicate=address_predicate,
    )


def _bounded_source_select_sql(source_select: str, row_limit: int | None) -> str:
    if not row_limit or row_limit <= 0:
        return source_select
    return (
        "(\n"
        f"SELECT * FROM (\n{source_select.strip()}\n) AS src LIMIT {int(row_limit)}\n"
        ")"
    )


def _current_provider_directory_source_selects(
    db_schema: str,
    available: dict[str, bool],
    source_selects: list[str],
    *,
    source_ids: list[str] | tuple[str, ...] | None = None,
    run_id: str | None = None,
    test_limit_per_source: int | None = None,
    has_compatibility_data: bool = False,
    partial_refresh: bool = False,
) -> list[str]:
    _validate_provider_directory_fence(
        available,
        has_compatibility_data=has_compatibility_data,
        partial_refresh=partial_refresh,
    )
    current_source_selects = [
        source_select
        for source_select in source_selects
        if not _is_provider_directory_source_select(db_schema, source_select)
    ]
    if _missing_provider_directory_fence_relations(available):
        return current_source_selects
    overlay_source_select = _provider_directory_partial_overlay_source_select(
        db_schema,
        available,
        source_ids=source_ids,
        run_id=run_id,
    )
    current_source_selects.append(
        _bounded_source_select_sql(overlay_source_select, test_limit_per_source)
    )
    return current_source_selects


def _string_array_literal(values: list[str]) -> str:
    if not values:
        raise ValueError("values must be non-empty")
    return "ARRAY[" + ", ".join(_sql_literal(value) for value in values) + "]::varchar[]"


def _entity_address_evidence_group_match_sql(group_alias: str, row_alias: str) -> str:
    return (
        f"{group_alias}.entity_type = {row_alias}.entity_type\n"
        f"       AND {group_alias}.entity_id = {row_alias}.entity_id\n"
        f"       AND (\n"
        f"            ({group_alias}.address_key IS NOT NULL AND {row_alias}.address_key IS NOT NULL "
        f"AND {group_alias}.address_key = {row_alias}.address_key)\n"
        f"            OR (\n"
        f"                {group_alias}.street_key IS NOT DISTINCT FROM {_street_soft_norm_expr(f'{row_alias}.first_line')}\n"
        f"            AND {group_alias}.city_key IS NOT DISTINCT FROM {_alnum_norm_expr(f'{row_alias}.city_name')}\n"
        f"            AND {group_alias}.state_key IS NOT DISTINCT FROM {_state_norm_expr(f'{row_alias}.state_name')}\n"
        f"            AND {group_alias}.zip_key IS NOT DISTINCT FROM {_zip5_norm_expr(f'{row_alias}.postal_code')}\n"
        f"            AND {group_alias}.country_key IS NOT DISTINCT FROM {_state_norm_expr(f'{row_alias}.country_code')}\n"
        f"            )\n"
        f"       )"
    )


def _entity_address_row_npi_expr(row_alias: str) -> str:
    return (
        f"COALESCE({row_alias}.npi, CASE\n"
        f"            WHEN {row_alias}.entity_type = 'npi' AND {row_alias}.entity_id ~ '^[0-9]+$'\n"
        f"                THEN {row_alias}.entity_id::bigint\n"
        f"            ELSE NULL::bigint\n"
        f"        END)"
    )


def _partial_affected_group_table(stage_table: str) -> str:
    return _archived_identifier(stage_table, "_pd_groups")


def _affected_live_location_table(stage_table: str) -> str:
    return _archived_identifier(stage_table, "_pd_live_locations")


def _is_provider_directory_source_select(db_schema: str, source_select: str) -> bool:
    return (
        f"FROM {db_schema}.provider_directory_practitioner_role AS role" in source_select
        or f"FROM {db_schema}.provider_directory_organization_affiliation AS affiliation" in source_select
        or f"FROM {db_schema}.provider_directory_organization AS organization" in source_select
        or f"FROM {db_schema}.provider_directory_address_overlay AS overlay" in source_select
    )


def _provider_directory_current_group_select_sql(source_select: str) -> str:
    return f"""
    SELECT DISTINCT
        src.entity_type::varchar AS entity_type,
        src.entity_id::varchar AS entity_id,
        CASE
            WHEN src.entity_type = 'npi' AND src.entity_id ~ '^[0-9]+$'
                THEN src.entity_id::bigint
            ELSE NULL::bigint
        END AS entity_npi,
        src.address_key::uuid AS address_key,
        {_street_soft_norm_expr("src.first_line")}::varchar AS street_key,
        {_alnum_norm_expr("src.city_name")}::varchar AS city_key,
        {_state_norm_expr("src.state_name")}::varchar AS state_key,
        {_zip5_norm_expr("src.postal_code")}::varchar AS zip_key,
        {_state_norm_expr("src.country_code")}::varchar AS country_key
      FROM (
            {source_select.strip()}
      ) AS src
     WHERE src.entity_type IS NOT NULL
       AND src.entity_id IS NOT NULL
       AND (
            src.address_key IS NOT NULL
         OR NULLIF(TRIM(src.first_line), '') IS NOT NULL
         OR NULLIF(TRIM(src.city_name), '') IS NOT NULL
       )
    """


def _prepare_partial_affected_groups_sql(
    db_schema: str,
    group_table: str,
    source_selects: list[str],
    *,
    source_ids: list[str] | tuple[str, ...] | None = None,
) -> str:
    provider_selects = [
        source_select
        for source_select in source_selects
        if _is_provider_directory_source_select(db_schema, source_select)
    ]
    if not provider_selects:
        raise RuntimeError(
            "entity-address-unified provider-directory-partial refresh requires "
            "available Provider Directory source tables."
        )
    current_groups_sql = "\nUNION\n".join(
        _provider_directory_current_group_select_sql(source_select)
        for source_select in provider_selects
    )
    live_source_filter = _provider_directory_live_source_filter_sql(
        db_schema,
        "live",
        source_ids=source_ids,
    )
    return f"""
    CREATE UNLOGGED TABLE {db_schema}.{group_table} AS
    SELECT DISTINCT
        live.entity_type::varchar AS entity_type,
        live.entity_id::varchar AS entity_id,
        CASE
            WHEN live.entity_type = 'npi' AND live.entity_id ~ '^[0-9]+$'
                THEN live.entity_id::bigint
            ELSE NULL::bigint
        END AS entity_npi,
        live.address_key::uuid AS address_key,
        {_street_soft_norm_expr("live.first_line")}::varchar AS street_key,
        {_alnum_norm_expr("live.city_name")}::varchar AS city_key,
        {_state_norm_expr("live.state_name")}::varchar AS state_key,
        {_zip5_norm_expr("live.postal_code")}::varchar AS zip_key,
        {_state_norm_expr("live.country_code")}::varchar AS country_key
      FROM {db_schema}.{EntityAddressUnified.__main_table__} AS live
     WHERE live.address_sources @> ARRAY['provider_directory_fhir']::varchar[]
       AND live.location_key IS NOT NULL
       {live_source_filter}
    UNION
    {current_groups_sql};
    """


def _index_affected_groups_sql(db_schema: str, group_table: str) -> str:
    index_name = _archived_identifier(f"{group_table}_idx_group", "")
    return f"""
    CREATE INDEX {index_name}
        ON {db_schema}.{group_table} (
            entity_npi,
            entity_type,
            entity_id,
            address_key,
            street_key,
            city_key,
            state_key,
            zip_key,
            country_key
        );
    """


def _index_partial_affected_groups_sql(db_schema: str, group_table: str) -> str:
    return _index_affected_groups_sql(db_schema, group_table)


def _partial_scope_index_preflight_sql(db_schema: str) -> str:
    return f"""
    SELECT EXISTS (
        SELECT 1
          FROM pg_catalog.pg_index AS index_meta
          JOIN pg_catalog.pg_class AS table_meta
            ON table_meta.oid = index_meta.indrelid
          JOIN pg_catalog.pg_namespace AS namespace_meta
            ON namespace_meta.oid = table_meta.relnamespace
          JOIN pg_catalog.pg_class AS index_relation
            ON index_relation.oid = index_meta.indexrelid
         WHERE namespace_meta.nspname = {_sql_literal(db_schema)}
           AND table_meta.relname = 'provider_directory_address_overlay'
           AND index_relation.relname = {_sql_literal(PROVIDER_DIRECTORY_PARTIAL_SCOPE_INDEX)}
           AND index_meta.indisvalid IS TRUE
           AND index_meta.indisready IS TRUE
           AND index_meta.indislive IS TRUE
           AND index_meta.indpred IS NULL
           AND index_meta.indexprs IS NULL
           AND (
                SELECT array_agg(attribute_meta.attname::text ORDER BY index_column.ordinality)
                  FROM unnest(index_meta.indkey) WITH ORDINALITY AS index_column(attnum, ordinality)
                  JOIN pg_catalog.pg_attribute AS attribute_meta
                    ON attribute_meta.attrelid = table_meta.oid
                   AND attribute_meta.attnum = index_column.attnum
               ) = ARRAY['source_id', 'last_seen_run_id', 'resource_type', 'resource_id']::text[]
    );
    """


async def _preflight_provider_directory_partial_scope_index(db_schema: str) -> None:
    index_is_valid = await db.scalar(
        _partial_scope_index_preflight_sql(db_schema)
    )
    if index_is_valid:
        return
    raise RuntimeError(
        "entity-address-unified provider-directory-partial refresh requires valid index "
        f"{db_schema}.{PROVIDER_DIRECTORY_PARTIAL_SCOPE_INDEX}; publish "
        "provider_directory_address_overlay through Provider Directory artifact publication "
        "or repair the index online outside the import path before retrying."
    )


def _affected_group_source_select_sql(db_schema: str, source_select: str, group_table: str) -> str:
    source_select = _prefilter_npi_source_select_sql(db_schema, source_select, group_table)
    return f"""
    SELECT src.*
      FROM (
        {source_select.strip()}
      ) AS src
     WHERE EXISTS (
            SELECT 1
              FROM {db_schema}.{group_table} AS affected
             WHERE {_entity_address_evidence_group_match_sql("affected", "src")}
     )
    """


def _affected_npi_source_select_sql(db_schema: str, source_select: str, group_table: str) -> str:
    source_select = _prefilter_npi_source_select_sql(db_schema, source_select, group_table)
    return f"""
    SELECT src.*
      FROM (
        {source_select.strip()}
      ) AS src
     WHERE src.npi IS NOT NULL
       AND EXISTS (
            SELECT 1
              FROM {db_schema}.{group_table} AS affected
             WHERE affected.entity_npi IS NOT NULL
               AND affected.entity_npi = src.npi
     )
    """


def _prefilter_npi_source_select_sql(db_schema: str, source_select: str, group_table: str) -> str:
    affected_join = f"""
              JOIN (
                    SELECT DISTINCT entity_npi
                      FROM {db_schema}.{group_table}
                     WHERE entity_npi IS NOT NULL
              ) AS affected_npi
                ON affected_npi.entity_npi = {{alias}}.npi"""
    markers = (
        (f"FROM {db_schema}.npi_address AS a", "a"),
        (f"FROM {db_schema}.doctor_clinician_address AS d", "d"),
        (f"FROM {db_schema}.mrf_address AS a", "a"),
        (f"FROM {db_schema}.provider_enrollment_ffs AS f", "f"),
    )
    for marker, alias in markers:
        if marker in source_select:
            return source_select.replace(
                marker,
                marker + affected_join.format(alias=alias),
                1,
            )
    return source_select


def _provider_directory_partial_source_selects(
    db_schema: str,
    source_selects: list[str],
    *,
    affected_group_table: str,
) -> list[str]:
    filtered_selects: list[str] = []
    provider_selects = 0
    for source_select in source_selects:
        if _is_provider_directory_source_select(db_schema, source_select):
            filtered_selects.append(source_select)
            provider_selects += 1
        else:
            filtered_selects.append(
                _affected_npi_source_select_sql(db_schema, source_select, affected_group_table)
            )
    if not provider_selects:
        return []
    return filtered_selects


def _provider_directory_partial_replacement_source_selects(
    db_schema: str,
    available: dict[str, bool],
    base_source_selects: list[str],
    *,
    affected_group_table: str,
    test_limit_per_source: int | None = None,
    has_compatibility_data: bool = False,
) -> list[str]:
    """Rebuild affected NPIs with every current Provider Directory source."""

    _validate_provider_directory_fence(
        available,
        has_compatibility_data=has_compatibility_data,
        partial_refresh=True,
    )
    current_source_selects = [
        source_select
        for source_select in base_source_selects
        if not _is_provider_directory_source_select(db_schema, source_select)
    ]
    overlay_source_select = _provider_directory_partial_overlay_source_select(
        db_schema,
        available,
        affected_group_table=affected_group_table,
    )
    current_source_selects.append(
        _bounded_source_select_sql(overlay_source_select, test_limit_per_source)
    )
    return _provider_directory_partial_source_selects(
        db_schema,
        current_source_selects,
        affected_group_table=affected_group_table,
    )


def _entity_address_column_names() -> list[str]:
    return [column.name for column in EntityAddressUnified.__table__.columns]


def _entity_address_column_list() -> str:
    return ", ".join(_entity_address_column_names())


def _copy_unaffected_live_entity_rows_sql(
    db_schema: str,
    *,
    live_table: str,
    stage_table: str,
    affected_group_table: str,
    replacement_lookup_table: str | None = None,
    on_conflict: bool = True,
) -> str:
    columns = _entity_address_column_names()
    column_list = ", ".join(columns)
    select_list = ", ".join(f"live.{column}" for column in columns)
    replacement_lookup_table = replacement_lookup_table or stage_table
    on_conflict_sql = "ON CONFLICT (location_key) DO NOTHING" if on_conflict else ""
    live_npi_expr = _entity_address_row_npi_expr("live")
    return f"""
    WITH affected_npis AS MATERIALIZED (
        SELECT DISTINCT affected.entity_npi
          FROM {db_schema}.{affected_group_table} AS affected
         WHERE affected.entity_npi IS NOT NULL
    ), affected_unknown_groups AS MATERIALIZED (
        SELECT affected.*
          FROM {db_schema}.{affected_group_table} AS affected
         WHERE affected.entity_npi IS NULL
    )
    INSERT INTO {db_schema}.{stage_table} ({column_list})
    SELECT {select_list}
      FROM {db_schema}.{live_table} AS live
      LEFT JOIN affected_npis AS affected_npi
        ON affected_npi.entity_npi = {live_npi_expr}
      LEFT JOIN {db_schema}.{replacement_lookup_table} AS replacement
        ON replacement.location_key = live.location_key
     WHERE affected_npi.entity_npi IS NULL
       AND NOT EXISTS (
            SELECT 1
              FROM affected_unknown_groups AS affected
             WHERE {_entity_address_evidence_group_match_sql("affected", "live")}
       )
       AND replacement.location_key IS NULL
    {on_conflict_sql};
    """


def _prepare_affected_live_locations_sql(
    db_schema: str,
    *,
    live_table: str,
    affected_group_table: str,
    replacement_lookup_table: str,
    affected_location_table: str,
) -> str:
    live_npi_expr = _entity_address_row_npi_expr("live")
    return f"""
    CREATE UNLOGGED TABLE {db_schema}.{affected_location_table} AS
    WITH affected_npis AS MATERIALIZED (
        SELECT DISTINCT affected.entity_npi
          FROM {db_schema}.{affected_group_table} AS affected
         WHERE affected.entity_npi IS NOT NULL
    ), affected_unknown_groups AS MATERIALIZED (
        SELECT affected.*
          FROM {db_schema}.{affected_group_table} AS affected
         WHERE affected.entity_npi IS NULL
    )
    SELECT DISTINCT affected_location.location_key::varchar AS location_key
      FROM (
            SELECT live.location_key::varchar AS location_key
              FROM {db_schema}.{live_table} AS live
              JOIN affected_npis AS affected_npi
                ON live.npi = affected_npi.entity_npi
             WHERE live.location_key IS NOT NULL
            UNION
            SELECT live.location_key::varchar AS location_key
              FROM {db_schema}.{live_table} AS live
              JOIN affected_npis AS affected_npi
                ON live.npi IS NULL
               AND affected_npi.entity_npi = {live_npi_expr}
             WHERE live.location_key IS NOT NULL
            UNION
            SELECT live.location_key::varchar AS location_key
              FROM {db_schema}.{live_table} AS live
             WHERE live.location_key IS NOT NULL
               AND EXISTS (
                    SELECT 1
                      FROM affected_unknown_groups AS affected
                     WHERE {_entity_address_evidence_group_match_sql("affected", "live")}
               )
            UNION
            SELECT replacement.location_key::varchar AS location_key
              FROM {db_schema}.{replacement_lookup_table} AS replacement
             WHERE replacement.location_key IS NOT NULL
          ) AS affected_location
     WHERE affected_location.location_key IS NOT NULL;
    """


def _index_affected_live_locations_sql(
    db_schema: str,
    affected_location_table: str,
) -> str:
    index_name = _archived_identifier(f"{affected_location_table}_idx_location", "")
    return f"""
    CREATE UNIQUE INDEX {index_name}
        ON {db_schema}.{affected_location_table} (location_key);
    """


def _copy_unaffected_rows_by_location_sql(
    db_schema: str,
    *,
    live_table: str,
    target_stage_table: str,
    affected_location_table: str,
) -> str:
    columns = _entity_address_column_names()
    column_list = ", ".join(columns)
    select_list = ", ".join(f"live.{column}" for column in columns)
    return f"""
    INSERT INTO {db_schema}.{target_stage_table} ({column_list})
    SELECT {select_list}
      FROM {db_schema}.{live_table} AS live
      LEFT JOIN {db_schema}.{affected_location_table} AS affected
        ON affected.location_key = live.location_key
     WHERE affected.location_key IS NULL;
    """


def _provider_directory_replacement_stage_table_name(stage_table: str) -> str:
    return _archived_identifier(stage_table, "_pd_replacement")


def _create_provider_directory_replacement_stage_sql(
    db_schema: str,
    *,
    replacement_stage_table: str,
    stage_table: str,
) -> str:
    return f"""
    CREATE UNLOGGED TABLE {db_schema}.{replacement_stage_table}
    (LIKE {db_schema}.{stage_table} INCLUDING DEFAULTS);
    """


def _copy_stage_entity_rows_sql(
    db_schema: str,
    *,
    source_stage_table: str,
    target_stage_table: str,
) -> str:
    columns = _entity_address_column_names()
    column_list = ", ".join(columns)
    select_list = ", ".join(f"stage.{column}" for column in columns)
    return f"""
    INSERT INTO {db_schema}.{target_stage_table} ({column_list})
    SELECT {select_list}
      FROM {db_schema}.{source_stage_table} AS stage;
    """


def _integer_ranges(min_value: int | None, max_value: int | None, shards: int) -> list[tuple[int, int]]:
    if min_value is None or max_value is None or shards <= 1 or min_value > max_value:
        return []
    span = max_value - min_value + 1
    step = max(1, (span + shards - 1) // shards)
    ranges: list[tuple[int, int]] = []
    start = min_value
    while start <= max_value:
        stop = min(start + step, max_value + 1)
        ranges.append((start, stop))
        start = stop
    return ranges


def _source_shard_specs(
    db_schema: str,
    npi_address_ranges: list[tuple[int, int]],
    mrf_address_ranges: list[tuple[int, int]],
    doctor_address_ranges: list[tuple[int, int]],
    enrollment_ranges: list[tuple[int, int]],
) -> list[tuple[str, str, str, str, list[tuple[int, int]]]]:
    """Return table, provenance, predicate, alias, and range shard specs."""
    return [
        (
            f"FROM {db_schema}.npi_address AS a",
            "'nppes'::varchar AS address_source",
            "WHERE a.npi IS NOT NULL",
            "a",
            npi_address_ranges,
        ),
        (
            f"FROM {db_schema}.mrf_address AS a",
            "'mrf'::varchar AS address_source",
            "WHERE a.npi IS NOT NULL",
            "a",
            mrf_address_ranges,
        ),
        (
            f"FROM {db_schema}.doctor_clinician_address AS d",
            "'cms_doctors'::varchar AS address_source",
            "WHERE d.npi IS NOT NULL",
            "d",
            doctor_address_ranges,
        ),
        (
            f"FROM {db_schema}.provider_enrollment_ffs AS f",
            "'provider_enrollment_ffs'::varchar AS address_source",
            "WHERE f.npi IS NOT NULL",
            "f",
            enrollment_ranges,
        ),
        (
            f"FROM {db_schema}.provider_enrollment_ffs_address AS fa",
            "'provider_enrollment_ffs_address'::varchar AS address_source",
            "WHERE f.npi IS NOT NULL",
            "f",
            enrollment_ranges,
        ),
    ]


def _expand_source_shards(
    select_sql: str,
    shard_specs: list[tuple[str, str, str, str, list[tuple[int, int]]]],
) -> list[str]:
    """Expand one eligible source query into its bounded NPI ranges."""
    for table_marker, source_marker, where_marker, alias, ranges in shard_specs:
        if table_marker not in select_sql or source_marker not in select_sql:
            continue
        if not ranges or where_marker not in select_sql:
            return [select_sql]
        return [
            select_sql.replace(
                where_marker,
                f"{where_marker}\n               AND {alias}.npi >= {low}"
                f"\n               AND {alias}.npi < {high}",
                1,
            )
            for low, high in ranges
        ]
    return [select_sql]


def _shard_source_selects(
    db_schema: str,
    source_selects: list[str],
    *,
    npi_address_ranges: list[tuple[int, int]] | None = None,
    mrf_address_ranges: list[tuple[int, int]] | None = None,
    doctor_clinician_address_ranges: list[tuple[int, int]] | None = None,
    provider_enrollment_ffs_ranges: list[tuple[int, int]] | None = None,
) -> list[str]:
    """Split eligible NPI-backed source queries into bounded ranges."""
    shard_specs = _source_shard_specs(
        db_schema,
        npi_address_ranges or [],
        mrf_address_ranges or [],
        doctor_clinician_address_ranges or [],
        provider_enrollment_ffs_ranges or [],
    )
    return [
        sharded_select
        for select_sql in source_selects
        for sharded_select in _expand_source_shards(select_sql, shard_specs)
    ]


async def _npi_table_ranges(db_schema: str, table_name: str, shards: int) -> list[tuple[int, int]]:
    if shards <= 1:
        return []
    row = await db.first(
        f"SELECT MIN(npi)::bigint AS min_npi, MAX(npi)::bigint AS max_npi "
        f"FROM {db_schema}.{table_name} WHERE npi IS NOT NULL;"
    )
    if not row:
        return []
    values = row._mapping
    return _integer_ranges(values.get("min_npi"), values.get("max_npi"), shards)


def _raw_stage_table_name(stage_table: str) -> str:
    return f"{stage_table}_raw"


def _evidence_stage_table_name(stage_table: str) -> str:
    return _archived_identifier(stage_table, "_evidence")


_RAW_STAGE_COLUMNS_SQL = f"""
        entity_type varchar(64) NOT NULL,
        entity_id varchar(128) NOT NULL,
        npi bigint,
        inferred_npi bigint,
        inference_confidence float8,
        inference_method varchar(64),
        entity_name varchar(256),
        entity_subtype varchar(64),
        type varchar(32) NOT NULL,
        taxonomy_array int[] NOT NULL,
        plans_network_array int[] NOT NULL,
        procedures_array int[] NOT NULL,
        medications_array int[] NOT NULL,
        first_line varchar,
        second_line varchar,
        city_name varchar,
        state_name varchar,
        postal_code varchar,
        country_code varchar,
        telephone_number varchar,
        fax_number varchar,
        phone_number varchar(15),
        phone_extension varchar(16),
        fax_number_digits varchar(15),
        fax_extension varchar(16),
        formatted_address varchar,
        formatted_address_version smallint,
        formatted_address_source varchar(32),
        lat numeric(11,8),
        long numeric(11,8),
        date_added date,
        place_id varchar,
        updated_at timestamp,
        source_priority int NOT NULL,
        address_source varchar,
        source_record_id varchar,
        address_key uuid,
        premise_key uuid,
        archive_identity_version varchar(16) NOT NULL DEFAULT '{ARCHIVE_IDENTITY_VERSION}',
        address_precision varchar(32) NOT NULL DEFAULT 'unknown',
        zip5 varchar(5),
        state_code varchar(2),
        city_norm varchar,
        county_fips varchar(5),
        source_id smallint NOT NULL DEFAULT 0,
        source_mask bigint NOT NULL DEFAULT 0,
        address_source_mask bigint NOT NULL DEFAULT 0,
        address_role_id smallint NOT NULL DEFAULT 0,
        location_confidence_id smallint NOT NULL DEFAULT 0,
        row_origin varchar(32) NOT NULL DEFAULT 'base',
        location_key varchar(64),
        evidence_shard int,
        aca_plan_array varchar[] NOT NULL DEFAULT '{{}}',
        aca_network_array varchar[] NOT NULL DEFAULT '{{}}',
        ptg_plan_array varchar[] NOT NULL DEFAULT '{{}}',
        ptg_source_array varchar[] NOT NULL DEFAULT '{{}}',
        group_plan_array varchar[] NOT NULL DEFAULT '{{}}',
        base_address_version varchar(64),
        checksum bigint NOT NULL
"""


def _prepare_raw_stage_sql(db_schema: str, raw_table: str, *, unlogged: bool = True) -> str:
    """Build SQL for the normalized raw entity-address staging table."""
    storage_mode = "UNLOGGED " if unlogged else ""
    return (
        f"CREATE {storage_mode}TABLE {db_schema}.{raw_table} ("
        f"{_RAW_STAGE_COLUMNS_SQL});"
    )


def _address_key_expr(
    db_schema: str,
    available: bool,
    *,
    address_source: str | None = None,
    table_alias: str | None = None,
) -> str:
    def col(name: str) -> str:
        """Qualify one address column for generated SQL."""
        return f"{table_alias}.{name}" if table_alias else name

    if available:
        # Intentional in-DB fallback: this expression runs inside SQL materialization pipelines.
        fallback = (
            f"{db_schema}.addr_key_v1("
            f"{col('first_line')}, {col('second_line')}, {col('city_name')}, "
            f"{col('state_name')}, {col('postal_code')}, {col('country_code')}"
            ")"
        )
    else:
        fallback = "NULL::uuid"
    if address_source:
        return f"CASE WHEN {address_source} = 'ptg' THEN NULL::uuid ELSE {fallback} END"
    return fallback


async def _address_alias_generation(db_schema: str) -> int:
    """Read the supported alias generation used by derived address artifacts."""
    row = await db.first(
        address_alias_sql.active_alias_generation_sql(schema=db_schema)
    )
    if row is None:
        raise RuntimeError("address alias singleton state is missing")
    if int(row.schema_version) != address_alias_sql.ADDRESS_ALIAS_SCHEMA_VERSION:
        raise RuntimeError(f"unsupported address alias schema version: {row.schema_version}")
    if (
        int(row.active_ruleset_version)
        != address_alias_sql.ADDRESS_ALIAS_RULESET_VERSION
    ):
        raise RuntimeError(
            f"unsupported address alias ruleset: {row.active_ruleset_version}"
        )
    return int(row.generation)


def _uses_provider_directory_overlay(
    db_schema: str,
    source_selects: list[str],
) -> bool:
    overlay_from = f"FROM {db_schema}.provider_directory_address_overlay AS overlay"
    return any(overlay_from in source_select for source_select in source_selects)


async def _provider_directory_overlay_alias_fence(
    db_schema: str,
) -> tuple[int, int]:
    """Return the materialized alias generation and live overlay relation OID."""
    row = await db.first(
        f"""
        SELECT
            receipt.generation,
            to_regclass(:overlay_relation)::oid::bigint AS relation_oid
        FROM {db_schema}.{address_alias_sql.ADDRESS_ALIAS_ARTIFACT_STATE_TABLE} AS receipt
        WHERE receipt.artifact_name = :artifact_name;
        """,
        overlay_relation=f"{db_schema}.provider_directory_address_overlay",
        artifact_name="provider_directory_address_overlay",
    )
    if row is None:
        raise RuntimeError(
            "Provider Directory address overlay alias-generation receipt is missing"
        )
    if row.relation_oid is None:
        raise RuntimeError("Provider Directory address overlay relation is missing")
    return int(row.generation), int(row.relation_oid)


async def _capture_provider_directory_overlay_alias_fence(
    db_schema: str,
    source_selects: list[str],
    context: dict,
) -> None:
    """Fence a selected overlay before a long unified-address build."""
    if not _uses_provider_directory_overlay(db_schema, source_selects):
        context.pop("provider_directory_overlay_alias_generation", None)
        context.pop("provider_directory_overlay_relation_oid", None)
        return
    generation, relation_oid = await _provider_directory_overlay_alias_fence(db_schema)
    expected_generation = int(context.get("address_alias_generation") or 0)
    if generation != expected_generation:
        raise RuntimeError(
            "Provider Directory address overlay uses a stale address alias generation; "
            "run a full address overlay rebuild before entity-address-unified"
        )
    context["provider_directory_overlay_alias_generation"] = generation
    context["provider_directory_overlay_relation_oid"] = relation_oid


async def _assert_provider_directory_overlay_alias_fence(
    db_schema: str,
    context: dict,
) -> None:
    """Recheck the selected overlay under the alias cutover lock."""
    expected_oid = context.get("provider_directory_overlay_relation_oid")
    if expected_oid is None:
        return
    expected_generation = int(
        context.get("provider_directory_overlay_alias_generation") or 0
    )
    generation, relation_oid = await _provider_directory_overlay_alias_fence(db_schema)
    if generation != expected_generation or relation_oid != int(expected_oid):
        raise RuntimeError(
            "Provider Directory address overlay changed during "
            "entity-address-unified build"
        )


_RAW_ALIAS_INTEGRITY_SQL = """
    WITH raw_candidates AS (
        SELECT DISTINCT
            raw.address_key,
            raw.address_source,
            raw.first_line,
            raw.second_line,
            raw.city_name,
            raw.state_name,
            raw.postal_code,
            raw.country_code
        FROM {db_schema}.{raw_table} AS raw
        {checksum_where}
    ), matching AS (
        SELECT
            active.source_address_key,
            active.target_address_key,
            active.source_identity_key,
            active.target_identity_key AS recorded_target_identity_key,
            {db_schema}.addr_identity_key_v1(
                raw.first_line,
                raw.second_line,
                raw.city_name,
                raw.state_name,
                raw.postal_code,
                raw.country_code
            ) AS current_source_identity_key,
            target.identity_key AS current_target_identity_key,
            target.address_key AS current_target_address_key
        FROM raw_candidates AS raw
        JOIN {db_schema}.{alias_table} AS active
          ON active.source_address_key IN (
                raw.address_key,
                {computed_address_key}
             )
         AND active.revoked_at IS NULL
        LEFT JOIN {db_schema}.address_archive_v2 AS target
          ON target.address_key = active.target_address_key
         AND target.merged_into IS NULL
    ), violations AS (
        SELECT
            CASE
                WHEN source_identity_key IS DISTINCT FROM current_source_identity_key
                    THEN 'source_identity_mismatch'
                WHEN current_target_address_key IS NULL
                    THEN 'missing_or_merged_target'
                WHEN recorded_target_identity_key IS DISTINCT FROM current_target_identity_key
                    THEN 'target_identity_mismatch'
                ELSE NULL
            END AS violation_kind,
            source_address_key,
            target_address_key
        FROM matching
        UNION ALL
        SELECT
            'multi_hop_alias',
            matching.source_address_key,
            matching.target_address_key
        FROM matching
        JOIN {db_schema}.{alias_table} AS downstream
          ON downstream.source_address_key = matching.target_address_key
         AND downstream.revoked_at IS NULL
    )
    SELECT *
    FROM violations
    WHERE violation_kind IS NOT NULL
    ORDER BY violation_kind, source_address_key
    LIMIT 1;
"""


@dataclass
class _AliasIntegrityProgress:
    run_id: str | None
    total: int
    started: float
    completed: int = 0


async def _report_raw_alias_integrity_progress(
    progress: _AliasIntegrityProgress,
    *,
    is_completed: bool,
) -> None:
    """Emit phase-local progress for the sharded raw-alias fence."""
    if not progress.run_id or progress.total <= 1:
        return
    if not is_completed:
        await asyncio.to_thread(
            write_live_progress,
            run_id=progress.run_id,
            importer="entity-address-unified",
            status="running",
            phase="entity-address-unified validating aliases",
            stage_id="entity-address-unified-alias-integrity",
            stage_pct=0.0,
            unit="shards",
            done=0,
            total=progress.total,
            elapsed_seconds=0.0,
            message=f"validating {progress.total} alias-integrity shards",
        )
        return
    progress.completed += 1
    elapsed_seconds = time.monotonic() - progress.started
    remaining_shards = progress.total - progress.completed
    await asyncio.to_thread(
        write_live_progress,
        run_id=progress.run_id,
        importer="entity-address-unified",
        status="running",
        phase="entity-address-unified validating aliases",
        stage_id="entity-address-unified-alias-integrity",
        stage_pct=(progress.completed / progress.total) * 100.0,
        unit="shards",
        done=progress.completed,
        total=progress.total,
        elapsed_seconds=elapsed_seconds,
        eta_seconds=remaining_shards * elapsed_seconds / progress.completed,
        message=(
            f"validated {progress.completed}/{progress.total} "
            "alias-integrity shards"
        ),
    )


async def _raw_alias_integrity_violation_for_range(
    db_schema: str,
    raw_table: str,
    computed_address_key: str,
    checksum_range: tuple[int | None, int | None],
    semaphore: asyncio.Semaphore,
    progress: _AliasIntegrityProgress,
):
    """Return one range's first alias violation and report its completion."""
    checksum_min, checksum_max = checksum_range
    checksum_where = ""
    if checksum_min is not None and checksum_max is not None:
        checksum_where = (
            f"WHERE raw.checksum >= {int(checksum_min)} "
            f"AND raw.checksum < {int(checksum_max)}"
        )
    async with semaphore:
        violation = await db.first(
            _RAW_ALIAS_INTEGRITY_SQL.format(
                db_schema=db_schema,
                raw_table=raw_table,
                alias_table=address_alias_sql.ADDRESS_ALIAS_TABLE,
                computed_address_key=computed_address_key,
                checksum_where=checksum_where,
            )
        )
    await _report_raw_alias_integrity_progress(progress, is_completed=True)
    return violation


def _first_raw_alias_integrity_violation(shard_outcomes):
    """Propagate database failures or select the deterministic first violation."""
    for shard_outcome in shard_outcomes:
        if isinstance(shard_outcome, BaseException):
            raise shard_outcome
    return min(
        (shard_outcome for shard_outcome in shard_outcomes if shard_outcome is not None),
        key=lambda violation_row: (
            str(violation_row.violation_kind),
            str(violation_row.source_address_key),
            str(violation_row.target_address_key),
        ),
        default=None,
    )


async def _raw_alias_integrity_checksum_ranges(
    db_schema: str,
    raw_table: str,
    checksum_ranges: list[tuple[int, int]],
    *,
    is_raw_stage_reused: bool,
) -> list[tuple[int, int]]:
    """Avoid repeated heap scans when a reused stage lacks its shard index."""
    if not is_raw_stage_reused or not checksum_ranges:
        return checksum_ranges
    has_checksum_index = await _has_table(
        db_schema,
        f"{raw_table}_idx_checksum",
    )
    return checksum_ranges if has_checksum_index else []


async def _validate_raw_alias_integrity(
    db_schema: str,
    raw_table: str,
    *,
    is_address_canon_available: bool,
    checksum_ranges: list[tuple[int, int]] | None = None,
    concurrency: int = 1,
    context: dict | None = None,
    run_id: str | None = None,
) -> None:
    """Fail when an active alias no longer matches enriched raw identity."""
    shard_ranges = checksum_ranges or [(None, None)]
    started = time.monotonic()
    progress = _AliasIntegrityProgress(
        run_id=run_id,
        total=len(shard_ranges),
        started=started,
    )
    computed_address_key = _address_key_expr(
        db_schema,
        is_address_canon_available,
        address_source="raw.address_source",
        table_alias="raw",
    )
    await _report_raw_alias_integrity_progress(progress, is_completed=False)
    semaphore = asyncio.Semaphore(
        max(1, min(int(concurrency), len(shard_ranges)))
    )
    shard_outcomes = await asyncio.gather(
        *(
            _raw_alias_integrity_violation_for_range(
                db_schema,
                raw_table,
                computed_address_key,
                checksum_range,
                semaphore,
                progress,
            )
            for checksum_range in shard_ranges
        ),
        return_exceptions=True,
    )
    violation = _first_raw_alias_integrity_violation(shard_outcomes)
    if context is not None:
        _record_phase_timing(
            context,
            "entity-address-unified validating aliases",
            time.monotonic() - started,
            None,
        )
    if violation:
        raise RuntimeError(
            "entity-address-unified alias integrity violation: "
            f"kind={violation.violation_kind} "
            f"source={violation.source_address_key} "
            f"target={violation.target_address_key}"
        )


def _available_archive_fields_sql() -> str:
    """Return archive columns selected by raw-stage enrichment."""

    return (
        "a.address_key AS archive_address_key, "
        "a.premise_key, "
        "'v' || COALESCE(a.identity_version, 2)::text AS archive_identity_version, "
        "COALESCE(a.precision, 'unknown') AS address_precision, "
        "a.zip5 AS archive_zip5, "
        "NULLIF(upper(left(a.state_code, 2)), '') AS archive_state_code, "
        "a.city_norm AS archive_city_norm, "
        "NULL::varchar AS archive_county_fips, "
        "a.lat::numeric AS archive_lat, "
        "a.long::numeric AS archive_long, "
        "a.place_id::varchar AS archive_place_id"
    )


def _available_archive_enrichment_sql(
    db_schema: str,
    is_address_canon_available: bool = True,
) -> tuple[str, str]:
    """Return archive fields and the exact alias-aware lookup join."""

    computed_address_key = _address_key_expr(
        db_schema,
        is_address_canon_available,
        address_source="r.address_source",
        table_alias="r",
    )
    should_trust_source_key = _is_env_enabled(
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_TRUST_SOURCE_ADDRESS_KEY",
        DEFAULT_TRUST_SOURCE_ADDRESS_KEY,
    )
    source_identity_key = (
        f"{db_schema}.addr_identity_key_v1("
        "r.first_line, r.second_line, r.city_name, r.state_name, "
        "r.postal_code, r.country_code)"
    )
    candidate_values = (
        f"(r.address_key, 0), ({computed_address_key}, 1)"
        if should_trust_source_key
        else f"({computed_address_key}, 0), (r.address_key, 1)"
    )
    archive_join = f"""
          LEFT JOIN LATERAL (
              SELECT archive_row.*
                FROM (VALUES {candidate_values}) AS candidate(address_key, priority)
                LEFT JOIN {db_schema}.address_alias_v1 AS active_alias
                  ON active_alias.source_address_key = candidate.address_key
                 AND active_alias.revoked_at IS NULL
                JOIN {db_schema}.address_archive_v2 AS archive_row
                  ON archive_row.address_key = COALESCE(
                        active_alias.target_address_key,
                        candidate.address_key
                     )
                 AND archive_row.merged_into IS NULL
                 AND (
                        active_alias.source_address_key IS NULL
                        OR (
                            active_alias.source_identity_key = {source_identity_key}
                            AND active_alias.target_identity_key = archive_row.identity_key
                        )
                 )
               WHERE candidate.address_key IS NOT NULL
            ORDER BY candidate.priority
               LIMIT 1
          ) a ON TRUE"""
    return _available_archive_fields_sql(), archive_join


def _unavailable_archive_fields_sql() -> str:
    """Return archive-shaped NULL fields when the archive is unavailable."""

    return (
        "NULL::uuid AS archive_address_key, "
        "NULL::uuid AS premise_key, "
        f"'{ARCHIVE_IDENTITY_VERSION}'::varchar AS archive_identity_version, "
        "CASE WHEN r.address_key IS NULL THEN 'unknown' ELSE 'street' END::varchar AS address_precision, "
        "NULL::varchar AS archive_zip5, "
        "NULL::varchar AS archive_state_code, "
        "NULL::varchar AS archive_city_norm, "
        "NULL::varchar AS archive_county_fips, "
        "NULL::numeric AS archive_lat, "
        "NULL::numeric AS archive_long, "
        "NULL::varchar AS archive_place_id"
    )


def _enriched_raw_cte_sql(
    db_schema: str,
    raw_table: str,
    archive_fields: str,
    archive_join: str,
    checksum_where: str,
) -> str:
    """Build the archive-enriched raw-row CTE."""

    return f"""
    WITH enriched AS (
        SELECT
            r.ctid AS row_id,
            r.entity_type,
            r.entity_id,
            r.first_line,
            r.city_name,
            r.state_name,
            r.postal_code,
            r.country_code,
            r.address_key AS source_address_key,
            {archive_fields},
            NULLIF(LEFT(REGEXP_REPLACE(COALESCE(r.postal_code, ''), '[^0-9]', '', 'g'), 5), '')::varchar AS source_zip5,
            NULLIF(upper(left(BTRIM(COALESCE(r.state_name, '')), 2)), '')::varchar AS source_state_code,
            NULLIF(regexp_replace(lower(COALESCE(r.city_name, '')), '[^a-z0-9]', '', 'g'), '')::varchar AS source_city_norm,
            {_source_id_expr("r.address_source")}::smallint AS source_id,
            {_source_mask_expr("r.address_source")}::bigint AS source_mask,
            {_address_role_id_expr("r.type")}::smallint AS address_role_id
          FROM {db_schema}.{raw_table} r
          {archive_join}
         {checksum_where}
    ),"""


def _enriched_raw_keyed_cte_sql(*, archive_available: bool) -> str:
    """Build normalized address keys and confidence fields."""

    return f"""
    keyed AS (
        SELECT
            row_id,
            entity_type,
            entity_id,
            first_line,
            city_name,
            state_name,
            postal_code,
            country_code,
            {"archive_address_key" if archive_available else "source_address_key"} AS address_key,
            premise_key,
            archive_identity_version,
            address_precision,
            archive_lat,
            archive_long,
            archive_place_id,
            COALESCE(archive_zip5, source_zip5)::varchar AS zip5,
            COALESCE(archive_state_code, source_state_code)::varchar AS state_code,
            COALESCE(archive_city_norm, source_city_norm)::varchar AS city_norm,
            archive_county_fips::varchar AS county_fips,
            source_id,
            source_mask,
            CASE WHEN source_id IN (1, 2, 3, 4, 5, 6, 8) THEN source_mask ELSE 0::bigint END AS address_source_mask,
            address_role_id,
            CASE WHEN source_id = 7 THEN 'ptg_overlay' ELSE 'base' END::varchar AS row_origin,
            CASE
                WHEN source_id = 7 THEN 4
                WHEN address_precision = 'city_zip' THEN 6
                WHEN source_id = 1 THEN 2
                WHEN source_id IN (2, 3, 4, 5, 6) THEN 1
                ELSE 0
            END::smallint AS location_confidence_id
          FROM enriched
    )"""


def _enriched_raw_update_sql(
    db_schema: str,
    raw_table: str,
    evidence_shard_set: str,
) -> str:
    """Build the raw-stage update from enriched keyed rows."""

    return f"""
    UPDATE {db_schema}.{raw_table} r
       SET address_key = k.address_key,
           premise_key = k.premise_key,
           archive_identity_version = k.archive_identity_version,
           address_precision = k.address_precision,
           zip5 = k.zip5,
           state_code = k.state_code,
           city_norm = k.city_norm,
           county_fips = k.county_fips,
           source_id = k.source_id,
           source_mask = k.source_mask,
           address_source_mask = k.address_source_mask,
           address_role_id = k.address_role_id,
           location_confidence_id = k.location_confidence_id,
           lat = COALESCE(k.archive_lat, r.lat),
           long = COALESCE(k.archive_long, r.long),
           place_id = COALESCE(k.archive_place_id, r.place_id),
           row_origin = k.row_origin,
           ptg_source_array = CASE
               WHEN k.source_id = 7 AND COALESCE(CARDINALITY(r.ptg_source_array), 0) = 0
                   THEN ARRAY[r.address_source]::varchar[]
               ELSE r.ptg_source_array
           END,
{evidence_shard_set}           base_address_version = (
               '{ALIAS_BASE_ADDRESS_VERSION_PREFIX}' || (
                    SELECT generation::text
                    FROM {db_schema}.{address_alias_sql.ADDRESS_ALIAS_STATE_TABLE}
                    WHERE singleton = true
               )
           ),
           location_key = {_location_key_expr({
               'entity_type': 'r.entity_type',
               'entity_id': 'r.entity_id',
               'npi': 'r.npi',
               'inferred_npi': 'r.inferred_npi',
               'address_role_id': 'k.address_role_id',
               'row_origin': 'k.row_origin',
               'address_key': 'k.address_key',
               'source_id': 'k.source_id',
               'source_record_id': 'r.source_record_id',
               'zip5': 'k.zip5',
               'state_code': 'k.state_code',
               'city_norm': 'k.city_norm',
           })}
      FROM keyed k
     WHERE r.ctid = k.row_id;
    """


def _enrich_raw_stage_sql(
    db_schema: str,
    raw_table: str,
    *,
    archive_available: bool = True,
    is_address_canon_available: bool = True,
    checksum_min: int | None = None,
    checksum_max: int | None = None,
    evidence_shards: int | None = None,
) -> str:
    """Build SQL that enriches raw addresses with canonical archive evidence."""

    if archive_available:
        archive_fields, archive_join = _available_archive_enrichment_sql(
            db_schema,
            is_address_canon_available,
        )
    else:
        archive_fields = _unavailable_archive_fields_sql()
        archive_join = ""
    checksum_where = ""
    if checksum_min is not None and checksum_max is not None:
        checksum_where = (
            f"WHERE r.checksum >= {int(checksum_min)} "
            f"AND r.checksum < {int(checksum_max)}"
        )
    evidence_shard_set = ""
    if evidence_shards and int(evidence_shards) > 1:
        evidence_shard_set = (
            "           evidence_shard = "
            f"{_evidence_group_hash_expr_for_alias('k', int(evidence_shards))},\n"
        )
    return "".join(
        (
            _enriched_raw_cte_sql(
                db_schema,
                raw_table,
                archive_fields,
                archive_join,
                checksum_where,
            ),
            _enriched_raw_keyed_cte_sql(archive_available=archive_available),
            _enriched_raw_update_sql(db_schema, raw_table, evidence_shard_set),
        )
    )


def _is_key_v2_enabled() -> bool:
    return _is_env_enabled("HLTHPRT_ENTITY_ADDRESS_UNIFIED_KEY_V2", False)


def _dedupe_key_expr(is_address_canon_available: bool) -> str:
    return "location_key"


def _aggregate_shard_expr(dedupe_key_expr: str, aggregate_shards: int) -> str:
    shards = max(int(aggregate_shards), 1)
    return f"((hashtext({dedupe_key_expr}) % {shards} + {shards}) % {shards})"


def _raw_aggregate_group_index_sql(
    db_schema: str,
    raw_table: str,
    *,
    aggregate_shards: int,
    is_address_canon_available: bool = True,
    inline_source_evidence: bool = False,
) -> str:
    dedupe_key_expr = _dedupe_key_expr(is_address_canon_available)
    if aggregate_shards > 1:
        profile = _raw_group_index_profile()
        shard_expr = (
            "evidence_shard"
            if inline_source_evidence
            else _aggregate_shard_expr(dedupe_key_expr, aggregate_shards)
        )
        index_name = (
            f"{raw_table}_idx_evidence_shard_group"
            if inline_source_evidence
            else f"{raw_table}_idx_aggregate_shard_group"
        )
        if inline_source_evidence and profile == "shard":
            return f"""
            CREATE INDEX IF NOT EXISTS {raw_table}_idx_evidence_shard
            ON {db_schema}.{raw_table} (evidence_shard);
            """
        return f"""
        CREATE INDEX {index_name}
        ON {db_schema}.{raw_table}
        (({shard_expr}), entity_type, entity_id, type, {dedupe_key_expr});
        """
    return f"""
    CREATE INDEX {raw_table}_idx_group_key
    ON {db_schema}.{raw_table} (entity_type, entity_id, type, {dedupe_key_expr});
    """


def _validate_publish_row_count(
    *,
    stage_rows: int,
    previous_rows: int,
    test_mode: bool,
    min_rows_required: int,
) -> None:
    if test_mode:
        return
    if stage_rows < min_rows_required:
        raise RuntimeError(
            f"EntityAddressUnified stage row count {stage_rows} below minimum {min_rows_required}; aborting publish."
        )
    min_delta_rows = int(previous_rows * 0.8)
    if previous_rows > 0 and stage_rows < min_delta_rows:
        raise RuntimeError(
            "EntityAddressUnified stage row count "
            f"{stage_rows} below 80% of previous publish {previous_rows}; aborting publish."
        )


async def _invalid_coordinate_count(db_schema: str, table_name: str, *, db_client=None) -> int:
    client = db if db_client is None else db_client
    return int(
        await client.scalar(
            f"""
            SELECT COUNT(*)
              FROM {db_schema}.{table_name}
             WHERE {_coordinate_invalid_sql("")};
            """
        )
        or 0
    )


def _coordinate_invalid_sql(alias: str) -> str:
    prefix = f"{alias}." if alias else ""
    return (
        f"({prefix}lat IS NOT NULL AND ({prefix}lat < -90 OR {prefix}lat > 90)) "
        f"OR ({prefix}long IS NOT NULL AND ({prefix}long < -180 OR {prefix}long > 180)) "
        f"OR ({prefix}lat IS NOT NULL AND {prefix}long IS NOT NULL "
        f"AND ABS({prefix}lat) < 0.0000001 AND ABS({prefix}long) < 0.0000001)"
    )


def _coordinate_missing_or_invalid_sql(alias: str) -> str:
    return (
        f"{alias}.lat IS NULL OR {alias}.long IS NULL "
        f"OR {alias}.lat < -90 OR {alias}.lat > 90 "
        f"OR {alias}.long < -180 OR {alias}.long > 180 "
        f"OR (ABS({alias}.lat) < 0.0000001 AND ABS({alias}.long) < 0.0000001)"
    )


def _geo_projection_reference_sql(db_schema: str) -> tuple[str, str, str, str]:
    """Build the exact legacy identity and point predicates for projection."""

    target_alias = "projection_target"
    identity_joins_sql = provider_address_identity_reference_joins_sql(
        target_alias,
        schema_name=db_schema,
        geo_zip_alias="projection_geo_zip",
        zip_state_alias="projection_zip_state",
    )
    identity_predicate_sql = provider_address_identity_coherence_sql(
        target_alias,
        schema_name=db_schema,
        use_projection=False,
        reference_aliases=("projection_geo_zip", "projection_zip_state"),
    )
    point_join_sql = provider_address_point_reference_join_sql(
        target_alias,
        zcta_alias="projection_zcta",
    )
    point_predicate_sql = provider_address_point_coherence_sql(
        target_alias,
        use_projection=False,
        zcta_alias="projection_zcta",
    )
    return (
        identity_joins_sql,
        identity_predicate_sql,
        point_join_sql,
        point_predicate_sql,
    )


def _geo_projection_evidence_source_sql() -> str:
    """Build evidence precedence from the admitted set-wise keys."""

    return geo_projection.evidence_source_id_case_sql(
        nppes_condition_sql=(
            "(projection_target.address_source_mask & 1) <> 0 "
            "AND projection_nppes.npi IS NOT NULL"
        ),
        mrf_condition_sql="projection_mrf.npi IS NOT NULL",
        cms_condition_sql=(
            "(projection_target.address_source_mask & 4) <> 0 "
            "AND projection_cms.location_key IS NOT NULL"
        ),
    )


def _geo_projection_filter_sql(*, force: bool) -> str:
    """Select every row for reused stages, otherwise only stale projections."""

    valid_source_ids = ", ".join(
        str(source_id) for source_id in geo_projection.GEO_EVIDENCE_SOURCE_IDS
    )
    return "TRUE" if force else f"""(
                target.geo_assurance_version = {geo_projection.GEO_ASSURANCE_VERSION}
            AND target.geo_evidence_source_id IN ({valid_source_ids})
            AND target.geo_identity_coherent IS NOT NULL
            AND target.geo_point_coherent IS NOT NULL
         ) IS NOT TRUE"""


def _geo_projection_target_ctes_sql(
    db_schema: str,
    stage_table: str,
    projection_filter_sql: str,
    identity_joins_sql: str,
    identity_predicate_sql: str,
    point_join_sql: str,
    point_predicate_sql: str,
) -> str:
    """Build target and spatial-coherence admitted-key CTEs."""

    target_alias = "projection_target"
    return f"""
    WITH projection_targets AS MATERIALIZED (
        SELECT
            target.location_key,
            target.npi,
            target.address_key,
            target.premise_key,
            target.address_source_mask,
            target.first_line,
            target.second_line,
            target.postal_code,
            target.country_code,
            target.zip5,
            target.state_code,
            target.state_name,
            target.lat,
            target.long
          FROM {db_schema}.{stage_table} AS target
         WHERE {projection_filter_sql}
    ), projection_identity_admitted AS MATERIALIZED (
        SELECT DISTINCT {target_alias}.location_key
          FROM projection_targets AS {target_alias}
          {identity_joins_sql}
         WHERE {identity_predicate_sql}
    ), projection_point_admitted AS MATERIALIZED (
        SELECT DISTINCT {target_alias}.location_key
          FROM projection_targets AS {target_alias}
          {point_join_sql}
         WHERE {point_predicate_sql}
    ),"""


def _geo_projection_external_evidence_ctes_sql(db_schema: str) -> str:
    """Build set-wise NPPES and MRF evidence admitted-key CTEs."""

    return f""" projection_nppes AS MATERIALIZED (
        SELECT DISTINCT projection_target.npi, projection_target.address_key
          FROM projection_targets AS projection_target
          JOIN {db_schema}.npi_address AS source_nppes
            ON source_nppes.npi = projection_target.npi
           AND source_nppes.address_key = projection_target.address_key
           AND source_nppes.date_added IS NOT NULL
         WHERE (projection_target.address_source_mask & 1) <> 0
           AND projection_target.address_key IS NOT NULL
    ), projection_mrf AS MATERIALIZED (
        SELECT DISTINCT projection_target.npi, projection_target.address_key
          FROM projection_targets AS projection_target
          JOIN {db_schema}.mrf_address AS source_mrf
            ON source_mrf.npi = projection_target.npi
           AND source_mrf.address_key = projection_target.address_key
           AND {geo_projection.independent_issuer_sql('source_mrf.source_issuer_names')}
           AND {geo_projection.mrf_lineage_complete_sql('source_mrf')}
         WHERE projection_target.address_key IS NOT NULL
    ),"""


def _geo_projection_cms_anchor_ctes_sql(
    db_schema: str,
    stage_table: str,
) -> str:
    """Build CMS target premises and their durable NPPES anchors."""

    return f""" projection_cms_premises AS MATERIALIZED (
        SELECT DISTINCT projection_target.npi, projection_target.premise_key
          FROM projection_targets AS projection_target
         WHERE (projection_target.address_source_mask & 4) <> 0
           AND projection_target.address_key IS NOT NULL
           AND projection_target.premise_key IS NOT NULL
    ), projection_nppes_anchors AS MATERIALIZED (
        SELECT DISTINCT requested.npi, requested.premise_key
          FROM projection_cms_premises AS requested
          JOIN {db_schema}.{stage_table} AS candidate
            ON candidate.npi = requested.npi
           AND candidate.premise_key = requested.premise_key
           AND (candidate.address_source_mask & 1) <> 0
           AND candidate.type IN ('primary', 'secondary', 'practice', 'site')
          JOIN {db_schema}.npi_address AS anchor_source
            ON anchor_source.npi = candidate.npi
           AND anchor_source.address_key = candidate.address_key
           AND anchor_source.date_added IS NOT NULL
    ),"""


def _geo_projection_cms_cte_sql(db_schema: str) -> str:
    """Build CMS evidence admitted keys from source rows and anchors."""

    return f""" projection_cms AS MATERIALIZED (
        SELECT DISTINCT projection_target.location_key
          FROM projection_targets AS projection_target
          JOIN {db_schema}.doctor_clinician_address AS source_doctor
            ON source_doctor.npi = projection_target.npi
           AND source_doctor.address_key = projection_target.address_key
           AND source_doctor.updated_at IS NOT NULL
          JOIN projection_nppes_anchors AS anchor
            ON anchor.npi = projection_target.npi
           AND anchor.premise_key = projection_target.premise_key
         WHERE (projection_target.address_source_mask & 4) <> 0
           AND projection_target.address_key IS NOT NULL
           AND projection_target.premise_key IS NOT NULL
    ),"""


def _geo_projection_update_sql(
    db_schema: str,
    stage_table: str,
    evidence_source_id_sql: str,
) -> str:
    """Classify each target once and atomically update its projection."""

    return f""" projection_classified AS MATERIALIZED (
        SELECT
            projection_target.location_key,
            {evidence_source_id_sql} AS geo_evidence_source_id,
            (projection_identity.location_key IS NOT NULL) AS geo_identity_coherent,
            (projection_point.location_key IS NOT NULL) AS geo_point_coherent
          FROM projection_targets AS projection_target
          LEFT JOIN projection_nppes
            ON projection_nppes.npi = projection_target.npi
           AND projection_nppes.address_key = projection_target.address_key
          LEFT JOIN projection_mrf
            ON projection_mrf.npi = projection_target.npi
           AND projection_mrf.address_key = projection_target.address_key
          LEFT JOIN projection_cms
            ON projection_cms.location_key = projection_target.location_key
          LEFT JOIN projection_identity_admitted AS projection_identity
            ON projection_identity.location_key = projection_target.location_key
          LEFT JOIN projection_point_admitted AS projection_point
            ON projection_point.location_key = projection_target.location_key
    )
    UPDATE {db_schema}.{stage_table} AS target
       SET geo_evidence_source_id = projection.geo_evidence_source_id,
           geo_identity_coherent = projection.geo_identity_coherent,
           geo_point_coherent = projection.geo_point_coherent,
           geo_assurance_version = {geo_projection.GEO_ASSURANCE_VERSION}
      FROM projection_classified AS projection
     WHERE projection.location_key = target.location_key;
    """


def _materialize_geo_assurance_sql(
    db_schema: str,
    stage_table: str,
    *,
    force: bool = False,
) -> str:
    """Project exact evidence and spatial coherence onto finalized stage rows."""

    reference_sql = _geo_projection_reference_sql(db_schema)
    return "".join(
        (
            _geo_projection_target_ctes_sql(
                db_schema,
                stage_table,
                _geo_projection_filter_sql(force=force),
                *reference_sql,
            ),
            _geo_projection_external_evidence_ctes_sql(db_schema),
            _geo_projection_cms_anchor_ctes_sql(db_schema, stage_table),
            _geo_projection_cms_cte_sql(db_schema),
            _geo_projection_update_sql(
                db_schema,
                stage_table,
                _geo_projection_evidence_source_sql(),
            ),
        )
    )


def _invalid_geo_assurance_projection_sql(db_schema: str, stage_table: str) -> str:
    valid_source_ids = ", ".join(
        str(value) for value in geo_projection.GEO_EVIDENCE_SOURCE_IDS
    )
    return f"""
    SELECT COUNT(*)
      FROM {db_schema}.{stage_table}
     WHERE geo_assurance_version IS DISTINCT FROM {geo_projection.GEO_ASSURANCE_VERSION}
        OR geo_evidence_source_id IS NULL
        OR geo_evidence_source_id NOT IN ({valid_source_ids})
        OR geo_identity_coherent IS NULL
        OR geo_point_coherent IS NULL;
    """


async def _validate_geo_assurance_projection(
    db_schema: str,
    stage_table: str,
) -> int:
    invalid_rows = int(
        await db.scalar(
            _invalid_geo_assurance_projection_sql(db_schema, stage_table)
        )
        or 0
    )
    if invalid_rows:
        raise RuntimeError(
            f"{invalid_rows} staged rows have incomplete geo assurance"
        )
    return invalid_rows


def _record_geo_assurance_candidate_sql(
    db_schema: str,
    stage_table: str,
    projected_rows: int,
) -> str:
    db_schema = _validate_schema_name(db_schema)
    stage_table = _validate_schema_name(stage_table)
    state_table = geo_projection.GEO_ASSURANCE_STATE_TABLE
    stage_relation = f"{db_schema}.{stage_table}"
    return f"""
    INSERT INTO {db_schema}.{state_table} (
        singleton,
        candidate_geo_assurance_version,
        candidate_table_oid,
        candidate_relation_signature,
        candidate_projected_rows
    )
    SELECT
        true,
        {geo_projection.GEO_ASSURANCE_VERSION},
        to_regclass('{stage_relation}')::oid,
        {geo_projection.projection_relation_signature_sql(db_schema)},
        {int(projected_rows)}::bigint
     WHERE to_regclass('{stage_relation}') IS NOT NULL
    ON CONFLICT (singleton) DO UPDATE SET
        candidate_geo_assurance_version = EXCLUDED.candidate_geo_assurance_version,
        candidate_table_oid = EXCLUDED.candidate_table_oid,
        candidate_relation_signature = EXCLUDED.candidate_relation_signature,
        candidate_projected_rows = EXCLUDED.candidate_projected_rows
    RETURNING candidate_table_oid::bigint;
    """


def _activate_geo_assurance_candidate_sql(db_schema: str) -> str:
    db_schema = _validate_schema_name(db_schema)
    state_table = geo_projection.GEO_ASSURANCE_STATE_TABLE
    live_relation = f"{db_schema}.{EntityAddressUnified.__main_table__}"
    return f"""
    UPDATE {db_schema}.{state_table}
       SET active_geo_assurance_version = candidate_geo_assurance_version,
           active_table_oid = candidate_table_oid,
           active_relation_signature = candidate_relation_signature,
           candidate_geo_assurance_version = NULL,
           candidate_table_oid = NULL,
           candidate_relation_signature = NULL,
           candidate_projected_rows = NULL
     WHERE singleton IS TRUE
       AND candidate_geo_assurance_version = {geo_projection.GEO_ASSURANCE_VERSION}
       AND candidate_table_oid = to_regclass('{live_relation}')::oid
       AND candidate_relation_signature = (
           {geo_projection.projection_relation_signature_sql(db_schema)}
       )
    RETURNING active_table_oid::bigint;
    """


def _emit_geo_assurance_progress(
    run_id: str,
    stage_rows: int,
    *,
    projected_rows: int = 0,
    elapsed: float | None = None,
) -> None:
    if not run_id:
        return
    phase = "entity-address-unified projecting geo assurance"
    message = "projecting durable provider geo assurance"
    if elapsed is not None:
        message = f"{message}: {projected_rows:,} row(s), {_format_seconds(elapsed)}"
    enqueue_live_progress(
        run_id=run_id,
        importer="entity-address-unified",
        status="running",
        phase=phase,
        unit="rows",
        done=projected_rows,
        total=stage_rows,
        pct=97,
        message=message,
        source="entity-address-unified-sql-progress",
    )


async def _materialize_geo_assurance(
    db_schema: str,
    stage_table: str,
    *,
    force: bool,
    context: dict,
    run_id: str,
    stage_rows: int,
) -> int:
    _emit_geo_assurance_progress(run_id, stage_rows)
    started = time.monotonic()
    projected_rows, invalid_rows, candidate_table_oid, effective_force = (
        await _project_geo_assurance_transaction(
            db_schema,
            stage_table,
            force=force,
        )
    )
    elapsed = time.monotonic() - started
    _record_phase_timing(
        context,
        "entity-address-unified projecting geo assurance",
        elapsed,
        projected_rows,
    )
    context["geo_assurance_candidate_table_oid"] = candidate_table_oid
    context["geo_assurance_forced_full_projection"] = effective_force
    context["invalid_geo_assurance_rows"] = invalid_rows
    _emit_geo_assurance_progress(
        run_id,
        stage_rows,
        projected_rows=projected_rows,
        elapsed=elapsed,
    )
    return projected_rows


async def _project_geo_assurance_transaction(
    db_schema: str,
    stage_table: str,
    *,
    force: bool,
) -> tuple[int, int, int, bool]:
    """Project, validate, and receipt one source-stable stage atomically."""

    async with db.transaction():
        await _apply_entity_address_transaction_settings()
        await db.status(geo_projection.projection_dependency_lock_sql(db_schema))
        current_projection_available = bool(
            await db.scalar(
                f"SELECT {geo_projection.projection_state_available_sql(db_schema)};"
            )
        )
        effective_force = force or not current_projection_available
        projected_rows = int(
            _coerce_rowcount(
                await db.status(
                    _materialize_geo_assurance_sql(
                        db_schema,
                        stage_table,
                        force=effective_force,
                    )
                )
            )
            or 0
        )
        invalid_rows = await _validate_geo_assurance_projection(
            db_schema,
            stage_table,
        )
        candidate_table_oid = await db.scalar(
            _record_geo_assurance_candidate_sql(
                db_schema,
                stage_table,
                projected_rows,
            )
        )
        if candidate_table_oid is None:
            raise RuntimeError("geo assurance stage disappeared before receipt")
    return (
        projected_rows,
        invalid_rows,
        int(candidate_table_oid),
        effective_force,
    )


async def _apply_entity_address_transaction_settings() -> None:
    for name, value in _entity_address_sql_settings():
        try:
            async with db.transaction():
                await db.status(f"SET LOCAL {name} = {_sql_literal(value)};")
        except Exception as exc:
            if "permission denied to set parameter" not in str(exc).lower():
                raise
            logger.warning(
                "Skipping unprivileged entity-address SQL setting %s=%s: %s",
                name,
                value,
                exc,
            )


async def _drop_stage_secondary_indexes(stage_cls, db_schema: str) -> int:
    dropped = 0
    for index in getattr(stage_cls, "__my_additional_indexes__", []) or []:
        index_name = index.get("name", "_".join(index.get("index_elements")))
        await db.status(
            f"DROP INDEX IF EXISTS {db_schema}."
            f"{_stage_index_name(stage_cls.__tablename__, index_name)};"
        )
        dropped += 1
    return dropped


async def _compact_geo_assurance_stage(
    db_schema: str,
    stage_table: str,
) -> str:
    persistence = await _stage_table_persistence(db_schema, stage_table)
    if persistence is None:
        raise RuntimeError(f"Geo assurance stage {db_schema}.{stage_table} does not exist")
    if persistence != "p":
        await _ensure_promoted_stage_logged(db_schema, stage_table)
        return "set_logged"
    await db.execute_ddl(f"VACUUM (FULL, ANALYZE) {db_schema}.{stage_table};")
    return "vacuum_full"


def _backfill_archive_coordinates_sql(
    db_schema: str,
    table_name: str,
    *,
    coordinate_scope_table: str | None = None,
) -> str:
    target_coordinate_missing = _coordinate_missing_or_invalid_sql("t")
    archive_coordinate_missing = _coordinate_missing_or_invalid_sql("a")
    if coordinate_scope_table:
        return f"""
        WITH scoped_targets AS MATERIALIZED (
            SELECT t.ctid AS target_row_id, t.address_key
              FROM {db_schema}.{coordinate_scope_table} AS scope
              JOIN {db_schema}.{table_name} AS t
                ON t.location_key = scope.location_key
             WHERE t.address_key IS NOT NULL
               AND ({target_coordinate_missing})
        )
        UPDATE {db_schema}.{table_name} AS t
           SET lat = a.lat,
               long = a.long
          FROM scoped_targets AS scoped
          JOIN {db_schema}.address_archive_v2 AS a
            ON a.address_key = scoped.address_key
         WHERE t.ctid = scoped.target_row_id
           AND a.merged_into IS NULL
           AND NOT ({archive_coordinate_missing})
           AND ({target_coordinate_missing});
        """
    return f"""
    UPDATE {db_schema}.{table_name} AS t
       SET lat = a.lat,
           long = a.long
      FROM {db_schema}.address_archive_v2 AS a
     WHERE t.address_key IS NOT NULL
       AND a.address_key = t.address_key
       AND a.merged_into IS NULL
       AND NOT ({archive_coordinate_missing})
       AND ({target_coordinate_missing});
    """


def _archive_coordinate_eligible_targets_sql(
    db_schema: str,
    table_name: str,
    target_coordinate_missing: str,
    *,
    coordinate_scope_table: str | None = None,
) -> str:
    coordinate_scope_join = ""
    if coordinate_scope_table:
        coordinate_scope_join = f"""
          JOIN {db_schema}.{coordinate_scope_table} AS scope
            ON scope.location_key = target.location_key"""
    return f"""
    eligible_targets AS MATERIALIZED (
        SELECT
            target.ctid AS target_row_id,
            current_archive.address_key AS current_address_key,
            current_archive.identity_version AS current_identity_version,
            current_archive.line1_norm,
            current_archive.city_norm,
            current_archive.state_code,
            current_archive.zip5,
            current_archive.country_code
          FROM {db_schema}.{table_name} AS target
          {coordinate_scope_join}
          JOIN {db_schema}.address_archive_v2 AS current_archive
            ON current_archive.address_key = target.address_key
           AND current_archive.merged_into IS NULL
         WHERE ({target_coordinate_missing})
           AND target.address_precision = 'street'
           AND current_archive.precision = 'street'
           AND current_archive.country_code = 'US'
           AND NULLIF(BTRIM(current_archive.line1_norm), '') IS NOT NULL
           AND NULLIF(BTRIM(current_archive.city_norm), '') IS NOT NULL
           AND NULLIF(BTRIM(current_archive.state_code), '') IS NOT NULL
           AND NULLIF(BTRIM(current_archive.zip5), '') IS NOT NULL
           AND NULLIF(BTRIM(current_archive.country_code), '') IS NOT NULL
    )
    """


def _archive_coordinate_candidate_groups_sql(
    db_schema: str,
    legacy_coordinate_missing: str,
) -> str:
    return f"""
    candidate_groups AS MATERIALIZED (
        SELECT
            eligible.target_row_id,
            MIN(legacy.lat) AS lat,
            MIN(legacy.long) AS long,
            COUNT(DISTINCT legacy.address_key)::bigint AS candidate_count
          FROM eligible_targets AS eligible
          JOIN {db_schema}.address_archive_v2 AS legacy
            ON legacy.identity_version < eligible.current_identity_version
           AND legacy.address_key <> eligible.current_address_key
           AND legacy.line1_norm = eligible.line1_norm
           AND legacy.city_norm = eligible.city_norm
           AND legacy.state_code = eligible.state_code
           AND legacy.zip5 = eligible.zip5
           AND legacy.country_code = eligible.country_code
         WHERE legacy.merged_into IS NULL
           AND legacy.precision = 'street'
           AND legacy.country_code = 'US'
           AND NOT ({legacy_coordinate_missing})
      GROUP BY eligible.target_row_id
    )
    """


def _inherit_archive_coordinates_sql(
    db_schema: str,
    table_name: str,
    *,
    coordinate_scope_table: str | None = None,
) -> str:
    """Inherit coordinates from one exact older identity without changing identity fields."""
    target_coordinate_missing = _coordinate_missing_or_invalid_sql("target")
    legacy_coordinate_missing = _coordinate_missing_or_invalid_sql("legacy")
    eligible_targets_sql = _archive_coordinate_eligible_targets_sql(
        db_schema,
        table_name,
        target_coordinate_missing,
        coordinate_scope_table=coordinate_scope_table,
    )
    candidate_groups_sql = _archive_coordinate_candidate_groups_sql(
        db_schema,
        legacy_coordinate_missing,
    )
    return f"""
    WITH {eligible_targets_sql},
    {candidate_groups_sql},
    inherited AS (
        UPDATE {db_schema}.{table_name} AS target
           SET lat = candidates.lat,
               long = candidates.long
          FROM candidate_groups AS candidates
         WHERE target.ctid = candidates.target_row_id
           AND candidates.candidate_count = 1
           AND ({target_coordinate_missing})
        RETURNING 1
    )
    SELECT
        (SELECT COUNT(*)::bigint FROM inherited) AS inherited_rows,
        (
            SELECT COUNT(*)::bigint
              FROM candidate_groups
             WHERE candidate_count > 1
        ) AS ambiguous_rows;
    """


async def _inherit_archive_coordinates(
    db_schema: str,
    table_name: str,
    *,
    coordinate_scope_table: str | None = None,
) -> dict[str, int]:
    rows = await db.all(
        _inherit_archive_coordinates_sql(
            db_schema,
            table_name,
            coordinate_scope_table=coordinate_scope_table,
        )
    )
    if not rows:
        return {"inherited_rows": 0, "ambiguous_rows": 0}
    metrics = _row_mapping(rows[0])
    return {
        "inherited_rows": int(metrics.get("inherited_rows") or 0),
        "ambiguous_rows": int(metrics.get("ambiguous_rows") or 0),
    }


def _archive_coordinate_publish_metrics(context: dict) -> dict[str, int]:
    return {
        "archive_coordinate_backfill_rows": int(
            context.get("archive_coordinate_backfill_rows") or 0
        ),
        "archive_coordinate_same_key_backfill_rows": int(
            context.get("archive_coordinate_same_key_backfill_rows") or 0
        ),
        "archive_coordinate_inherited_rows": int(
            context.get("archive_coordinate_inherited_rows") or 0
        ),
        "archive_coordinate_ambiguous_rows": int(
            context.get("archive_coordinate_ambiguous_rows") or 0
        ),
    }


def _clear_invalid_coordinates_sql(
    db_schema: str,
    table_name: str,
    *,
    coordinate_scope_table: str | None = None,
) -> str:
    if coordinate_scope_table:
        return f"""
        WITH scoped_targets AS MATERIALIZED (
            SELECT t.ctid AS target_row_id
              FROM {db_schema}.{coordinate_scope_table} AS scope
              JOIN {db_schema}.{table_name} AS t
                ON t.location_key = scope.location_key
        )
        UPDATE {db_schema}.{table_name} AS t
           SET lat = NULL,
               long = NULL
          FROM scoped_targets AS scoped
         WHERE t.ctid = scoped.target_row_id
           AND {_coordinate_invalid_sql("t")};
        """
    return f"""
    UPDATE {db_schema}.{table_name} AS t
       SET lat = NULL,
           long = NULL
     WHERE {_coordinate_invalid_sql("t")};
    """


def _entity_address_provider_npi_expr(row_alias: str | None = None) -> str:
    prefix = f"{row_alias}." if row_alias else ""
    return (
        f"COALESCE({prefix}npi, {prefix}inferred_npi, CASE\n"
        f"            WHEN {prefix}entity_type = 'npi' AND {prefix}entity_id ~ '^[0-9]+$'\n"
        f"                THEN {prefix}entity_id::bigint\n"
        f"            ELSE NULL::bigint\n"
        f"        END)"
    )


def _same_provider_field_aggregate(column_name: str, sql_type: str = "varchar") -> str:
    return (
        f"(ARRAY_AGG({column_name} ORDER BY ({column_name} IS NULL), "
        f"source_count DESC, updated_at DESC NULLS LAST, location_key))[1]::{sql_type} AS {column_name}"
    )


def _same_provider_coordinate_aggregate(column_name: str) -> str:
    return (
        f"(ARRAY_AGG({column_name} ORDER BY ((lat IS NULL) OR (long IS NULL)), "
        f"source_count DESC, updated_at DESC NULLS LAST, location_key))[1]::numeric AS {column_name}"
    )


def _same_provider_update_needed_sql(target_coordinate_missing: str) -> str:
    return f"""
            (target_row.telephone_number IS NULL AND grouped_fields.telephone_number IS NOT NULL)
         OR (target_row.phone_number IS NULL AND grouped_fields.phone_number IS NOT NULL)
         OR (target_row.phone_extension IS NULL AND grouped_fields.phone_extension IS NOT NULL)
         OR (target_row.fax_number IS NULL AND grouped_fields.fax_number IS NOT NULL)
         OR (target_row.fax_number_digits IS NULL AND grouped_fields.fax_number_digits IS NOT NULL)
         OR (target_row.fax_extension IS NULL AND grouped_fields.fax_extension IS NOT NULL)
         OR (({target_coordinate_missing}) AND grouped_fields.lat IS NOT NULL AND grouped_fields.long IS NOT NULL)
    """


def _same_provider_source_rows_sql(
    db_schema: str,
    table_name: str,
) -> str:
    return f"""
    source_rows AS MATERIALIZED (
        SELECT
            location_key,
            {_entity_address_provider_npi_expr()} AS provider_npi,
            address_key,
            telephone_number,
            phone_number,
            phone_extension,
            fax_number,
            fax_number_digits,
            fax_extension,
            lat,
            long,
            source_count,
            updated_at
          FROM {db_schema}.{table_name}
         WHERE address_key IS NOT NULL
    )
    """


def _same_provider_grouped_fields_sql() -> str:
    return f"""
    grouped_fields AS MATERIALIZED (
        SELECT
            provider_npi,
            address_key,
            {_same_provider_field_aggregate("telephone_number")},
            {_same_provider_field_aggregate("phone_number")},
            {_same_provider_field_aggregate("phone_extension")},
            {_same_provider_field_aggregate("fax_number")},
            {_same_provider_field_aggregate("fax_number_digits")},
            {_same_provider_field_aggregate("fax_extension")},
            {_same_provider_coordinate_aggregate("lat")},
            {_same_provider_coordinate_aggregate("long")}
          FROM source_rows
         WHERE provider_npi IS NOT NULL
           AND (
                telephone_number IS NOT NULL
             OR phone_number IS NOT NULL
             OR fax_number IS NOT NULL
             OR fax_number_digits IS NOT NULL
             OR (lat IS NOT NULL AND long IS NOT NULL)
           )
      GROUP BY provider_npi, address_key
    )
    """


def _same_provider_set_clause_sql(target_coordinate_missing: str) -> str:
    return f"""
           telephone_number = COALESCE(target_row.telephone_number, grouped_fields.telephone_number),
           phone_number = COALESCE(target_row.phone_number, grouped_fields.phone_number),
           phone_extension = COALESCE(target_row.phone_extension, grouped_fields.phone_extension),
           fax_number = COALESCE(target_row.fax_number, grouped_fields.fax_number),
           fax_number_digits = COALESCE(target_row.fax_number_digits, grouped_fields.fax_number_digits),
           fax_extension = COALESCE(target_row.fax_extension, grouped_fields.fax_extension),
           lat = CASE WHEN {target_coordinate_missing} THEN grouped_fields.lat ELSE target_row.lat END,
           long = CASE WHEN {target_coordinate_missing} THEN grouped_fields.long ELSE target_row.long END
    """


def _backfill_same_provider_address_fields_sql(
    db_schema: str,
    table_name: str,
    *,
    coordinate_scope_table: str | None = None,
) -> str:
    """Fill missing contacts and coordinates from same-provider rows at the same address key."""
    target_coordinate_missing = _coordinate_missing_or_invalid_sql("target_row")
    scoped_targets_sql = ""
    target_scope_filter = ""
    if coordinate_scope_table:
        scoped_targets_sql = f""",
    scoped_targets AS MATERIALIZED (
        SELECT target_row.ctid AS target_row_id
          FROM {db_schema}.{coordinate_scope_table} AS scope
          JOIN {db_schema}.{table_name} AS target_row
            ON target_row.location_key = scope.location_key
    )"""
        target_scope_filter = "\n       AND target_row.ctid = scoped_targets.target_row_id"
    return f"""
    WITH {_same_provider_source_rows_sql(
        db_schema,
        table_name,
    )},
    {_same_provider_grouped_fields_sql()}{scoped_targets_sql}
    UPDATE {db_schema}.{table_name} AS target_row
       SET {_same_provider_set_clause_sql(target_coordinate_missing)}
      FROM grouped_fields{", scoped_targets" if coordinate_scope_table else ""}
     WHERE target_row.address_key IS NOT NULL
       AND target_row.address_key = grouped_fields.address_key
       AND {_entity_address_provider_npi_expr("target_row")} = grouped_fields.provider_npi
       {target_scope_filter}
       AND ({_same_provider_update_needed_sql(target_coordinate_missing)}
       );
    """


async def _is_location_primary_key_validated(db_schema: str, table_name: str) -> bool:
    """A valid PK on location_key proves both non-null and uniqueness."""
    return bool(
        await db.scalar(
            f"""
            SELECT 1
              FROM pg_constraint con
              JOIN pg_class tbl
                ON tbl.oid = con.conrelid
              JOIN pg_namespace ns
                ON ns.oid = tbl.relnamespace
              JOIN LATERAL unnest(con.conkey) WITH ORDINALITY cols(attnum, ord)
                ON TRUE
              JOIN pg_attribute att
                ON att.attrelid = tbl.oid
               AND att.attnum = cols.attnum
             WHERE ns.nspname = {_sql_literal(db_schema)}
               AND tbl.relname = {_sql_literal(table_name)}
               AND con.contype = 'p'
               AND con.convalidated IS TRUE
          GROUP BY con.oid
            HAVING array_agg(att.attname::text ORDER BY cols.ord) = ARRAY['location_key']::text[]
               AND bool_and(att.attnotnull)
             LIMIT 1;
            """
        )
    )


async def _validate_publish_integrity(
    db_schema: str,
    stage_table: str,
    support_stage_class_map: dict[type, type],
    *,
    test_mode: bool,
) -> dict[str, int | dict[str, int]]:
    """Validate staged address and support-table publish invariants."""
    if test_mode:
        return {}

    failures: list[str] = []
    integrity_metric_map: dict[str, int | dict[str, int]] = {}

    alias_generation = await _address_alias_generation(db_schema)
    expected_base_version = f"{ALIAS_BASE_ADDRESS_VERSION_PREFIX}{alias_generation}"
    residual_alias_source_rows, stale_alias_generation_rows = (
        int(metric_value or 0)
        for metric_value in await asyncio.gather(
            db.scalar(
                f"""
                SELECT count(*)
                FROM {db_schema}.{stage_table} AS staged
                JOIN {db_schema}.{address_alias_sql.ADDRESS_ALIAS_TABLE} AS active
                  ON active.source_address_key = staged.address_key
                 AND active.revoked_at IS NULL;
                """
            ),
            db.scalar(
                f"""
                SELECT count(*)
                FROM {db_schema}.{stage_table}
                WHERE base_address_version IS DISTINCT FROM :base_address_version;
                """,
                base_address_version=expected_base_version,
            ),
        )
    )
    integrity_metric_map["address_alias_generation"] = alias_generation
    integrity_metric_map["residual_alias_source_rows"] = residual_alias_source_rows
    integrity_metric_map["stale_alias_generation_rows"] = stale_alias_generation_rows
    if residual_alias_source_rows:
        failures.append(
            f"{residual_alias_source_rows} staged rows retain active alias source keys"
        )
    if stale_alias_generation_rows:
        failures.append(
            f"{stale_alias_generation_rows} staged rows use a stale address alias generation"
        )

    location_key_constraint_validated = await _is_location_primary_key_validated(db_schema, stage_table)
    integrity_metric_map["location_key_constraint_validated"] = location_key_constraint_validated
    if location_key_constraint_validated:
        null_location_keys = 0
        duplicate_location_keys = 0
    else:
        null_location_keys = int(
            await db.scalar(f"SELECT COUNT(*) FROM {db_schema}.{stage_table} WHERE location_key IS NULL;")
            or 0
        )
        if null_location_keys:
            failures.append(f"{null_location_keys} staged rows have NULL location_key")

        duplicate_location_keys = int(
            await db.scalar(
                f"""
                SELECT COUNT(*)
                  FROM (
                        SELECT location_key
                          FROM {db_schema}.{stage_table}
                         GROUP BY location_key
                        HAVING COUNT(*) > 1
                       ) AS duplicate_locations;
                """
            )
            or 0
        )
    integrity_metric_map["null_location_keys"] = null_location_keys
    integrity_metric_map["duplicate_location_keys"] = duplicate_location_keys
    if duplicate_location_keys:
        failures.append(f"{duplicate_location_keys} duplicate staged location_key values")

    unresolved_merged_into_rows = 0
    missing_archive_address_key_rows = 0
    archive_coordinate_mismatch_rows = 0
    archive_missing_coordinate_rows = 0
    archive_identity_mismatch_rows = 0
    if await _has_table(db_schema, "address_archive_v2"):
        (
            unresolved_merged_into_rows,
            archive_coordinate_mismatch_rows,
            archive_missing_coordinate_rows,
            missing_archive_address_key_rows,
            archive_identity_mismatch_rows,
        ) = (
            int(metric_value or 0)
            for metric_value in await asyncio.gather(
                db.scalar(
                    f"""
                SELECT COUNT(*)
                  FROM {db_schema}.{stage_table} AS t
                  JOIN {db_schema}.address_archive_v2 AS a
                    ON a.address_key = t.address_key
                 WHERE t.address_key IS NOT NULL
                   AND a.merged_into IS NOT NULL;
                """
                ),
                db.scalar(
                    f"""
                SELECT COUNT(*)
                  FROM {db_schema}.{stage_table} AS t
                  JOIN {db_schema}.address_archive_v2 AS a
                    ON a.address_key = t.address_key
                   AND a.merged_into IS NULL
                 WHERE t.address_key IS NOT NULL
                   AND a.lat IS NOT NULL
                   AND a.long IS NOT NULL
                   AND (
                       t.lat IS NULL
                    OR t.long IS NULL
                    OR ABS(t.lat - a.lat) > {ARCHIVE_COORDINATE_EPSILON_DEGREES}
                    OR ABS(t.long - a.long) > {ARCHIVE_COORDINATE_EPSILON_DEGREES}
                   );
                """
                ),
                db.scalar(
                    f"""
                SELECT COUNT(*)
                  FROM {db_schema}.{stage_table} AS t
                  JOIN {db_schema}.address_archive_v2 AS a
                    ON a.address_key = t.address_key
                   AND a.merged_into IS NULL
                 WHERE t.address_key IS NOT NULL
                   AND (a.lat IS NULL OR a.long IS NULL);
                """
                ),
                db.scalar(
                    f"""
                SELECT COUNT(*)
                  FROM {db_schema}.{stage_table} AS t
                 WHERE t.address_key IS NOT NULL
                   AND NOT EXISTS (
                       SELECT 1
                         FROM {db_schema}.address_archive_v2 AS a
                        WHERE a.address_key = t.address_key
                          AND a.merged_into IS NULL
                   );
                """
                ),
                db.scalar(
                    f"""
                SELECT COUNT(*)
                  FROM {db_schema}.{stage_table} AS t
                  JOIN {db_schema}.address_archive_v2 AS a
                    ON a.address_key = t.address_key
                   AND a.merged_into IS NULL
                 WHERE t.address_key IS NOT NULL
                   AND COALESCE(t.archive_identity_version, '')
                       IS DISTINCT FROM ('v' || COALESCE(a.identity_version, 2)::text);
                """
                ),
            )
        )
    integrity_metric_map["unresolved_merged_into_rows"] = unresolved_merged_into_rows
    if unresolved_merged_into_rows:
        failures.append(
            f"{unresolved_merged_into_rows} staged rows point to address_archive_v2.merged_into redirects"
        )
    integrity_metric_map["missing_archive_address_key_rows"] = missing_archive_address_key_rows
    if missing_archive_address_key_rows:
        failures.append(
            f"{missing_archive_address_key_rows} staged rows have address_key values missing from address_archive_v2"
        )
    integrity_metric_map["archive_coordinate_mismatch_rows"] = archive_coordinate_mismatch_rows
    integrity_metric_map["archive_missing_coordinate_rows"] = archive_missing_coordinate_rows
    if archive_missing_coordinate_rows and _is_env_enabled(
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_REQUIRE_ARCHIVE_COORDINATES",
        False,
    ):
        failures.append(
            f"{archive_missing_coordinate_rows} staged rows reference archive addresses without coordinates"
        )

    (
        practice_null_address_key_rows_raw,
        practice_null_address_key_by_source_rows,
        fallback_archive_identity_mismatch_rows_raw,
        invalid_coordinate_rows,
    ) = await asyncio.gather(
        db.scalar(
            f"""
            SELECT COUNT(*)
              FROM {db_schema}.{stage_table}
             WHERE type = 'practice'
               AND address_key IS NULL;
            """
        ),
        db.all(
            f"""
        SELECT COALESCE(source, 'unknown') AS source, COUNT(*)::bigint AS rows
          FROM {db_schema}.{stage_table} AS t
          LEFT JOIN LATERAL unnest(t.address_sources) AS source ON TRUE
         WHERE t.type = 'practice'
           AND t.address_key IS NULL
      GROUP BY COALESCE(source, 'unknown')
      ORDER BY rows DESC, source
         LIMIT 20;
        """
        ),
        db.scalar(
            f"""
              SELECT COUNT(*)
              FROM {db_schema}.{stage_table}
             WHERE address_key IS NULL
               AND COALESCE(archive_identity_version, '') <> '{ARCHIVE_IDENTITY_VERSION}';
            """
        ),
        _invalid_coordinate_count(db_schema, stage_table),
    )
    practice_null_address_key_rows = int(practice_null_address_key_rows_raw or 0)
    integrity_metric_map["practice_null_address_key_rows"] = practice_null_address_key_rows
    integrity_metric_map["practice_null_address_key_by_source"] = {
        str(count_row._mapping["source"]): int(count_row._mapping["rows"] or 0)
        for count_row in practice_null_address_key_by_source_rows
    }

    integrity_metric_map["archive_identity_mismatch_rows"] = archive_identity_mismatch_rows
    if archive_identity_mismatch_rows:
        failures.append(
            f"{archive_identity_mismatch_rows} staged rows do not match address_archive_v2 identity_version"
        )

    fallback_archive_identity_mismatch_rows = int(fallback_archive_identity_mismatch_rows_raw or 0)
    integrity_metric_map["fallback_archive_identity_mismatch_rows"] = fallback_archive_identity_mismatch_rows
    if fallback_archive_identity_mismatch_rows:
        failures.append(
            f"{fallback_archive_identity_mismatch_rows} staged rows without address_key use a non-current archive_identity_version"
        )

    integrity_metric_map["invalid_coordinate_rows"] = invalid_coordinate_rows
    if invalid_coordinate_rows:
        failures.append(f"{invalid_coordinate_rows} staged rows have invalid latitude/longitude values")

    bridge_orphan_count_map: dict[str, int] = {}
    for model, support_stage_cls in support_stage_class_map.items():
        if model is EntityAddressEvidence:
            continue
        bridge_table = support_stage_cls.__tablename__
        if not await _has_table(db_schema, bridge_table):
            failures.append(f"support stage table {bridge_table} is missing")
            bridge_orphan_count_map[bridge_table] = -1
            continue
        orphan_count = int(
            await db.scalar(
                f"""
                SELECT COUNT(*)
                  FROM {db_schema}.{bridge_table} AS b
                 WHERE NOT EXISTS (
                       SELECT 1
                         FROM {db_schema}.{stage_table} AS t
                        WHERE t.location_key = b.location_key
                 );
                """
            )
            or 0
        )
        bridge_orphan_count_map[bridge_table] = orphan_count
        if orphan_count:
            failures.append(f"{orphan_count} rows in {bridge_table} reference missing staged location_key")
    integrity_metric_map["bridge_orphans"] = bridge_orphan_count_map

    if failures:
        raise RuntimeError("EntityAddressUnified publish integrity validation failed: " + "; ".join(failures))
    return integrity_metric_map


def _insert_raw_header_sql(
    db_schema: str,
    raw_table: str,
) -> str:
    """Build the raw-source insert column header."""

    return f"""
    INSERT INTO {db_schema}.{raw_table} (
        entity_type,
        entity_id,
        npi,
        inferred_npi,
        inference_confidence,
        inference_method,
        entity_name,
        entity_subtype,
        type,
        taxonomy_array,
        plans_network_array,
        procedures_array,
        medications_array,
        aca_plan_array,
        aca_network_array,
        ptg_plan_array,
        ptg_source_array,
        group_plan_array,
        base_address_version,
        first_line,
        second_line,
        city_name,
        state_name,
        postal_code,
        country_code,
        telephone_number,
        fax_number,
        phone_number,
        phone_extension,
        fax_number_digits,
        fax_extension,
        formatted_address,
        formatted_address_version,
        formatted_address_source,
        lat,
        long,
        date_added,
        place_id,
        updated_at,
        source_priority,
        address_source,
        source_record_id,
        address_key,
        checksum
    )"""


def _insert_raw_base_rows_cte_sql(source_select: str) -> str:
    """Build the caller-supplied raw base-row CTE."""

    return f"""
    WITH base_rows AS (
        {source_select.strip()}
    ),"""


def _insert_raw_sanitized_cte_sql() -> str:
    """Build the raw value-sanitization CTE."""

    return f"""
    sanitized AS (
        SELECT
            entity_type,
            entity_id,
            npi,
            inferred_npi,
            inference_confidence,
            inference_method,
            NULLIF(TRIM(entity_name), '')::varchar AS entity_name,
            NULLIF(TRIM(entity_subtype), '')::varchar AS entity_subtype,
            COALESCE(NULLIF(TRIM(type), ''), 'primary')::varchar AS type,
            COALESCE(taxonomy_array, ARRAY[0]::int[])::int[] AS taxonomy_array,
            COALESCE(plans_network_array, ARRAY[0]::int[])::int[] AS plans_network_array,
            COALESCE(procedures_array, ARRAY[0]::int[])::int[] AS procedures_array,
            COALESCE(medications_array, ARRAY[0]::int[])::int[] AS medications_array,
            COALESCE(aca_plan_array, ARRAY[]::varchar[])::varchar[] AS aca_plan_array,
            COALESCE(aca_network_array, ARRAY[]::varchar[])::varchar[] AS aca_network_array,
            COALESCE(ptg_plan_array, ARRAY[]::varchar[])::varchar[] AS ptg_plan_array,
            COALESCE(ptg_source_array, ARRAY[]::varchar[])::varchar[] AS ptg_source_array,
            COALESCE(group_plan_array, ARRAY[]::varchar[])::varchar[] AS group_plan_array,
            NULLIF(TRIM(base_address_version), '')::varchar AS base_address_version,
            NULLIF(TRIM(first_line), '')::varchar AS first_line,
            NULLIF(TRIM(second_line), '')::varchar AS second_line,
            NULLIF(TRIM(city_name), '')::varchar AS city_name,
            NULLIF(TRIM(state_name), '')::varchar AS state_name,
            NULLIF(TRIM(postal_code), '')::varchar AS postal_code,
            COALESCE(NULLIF(TRIM(country_code), ''), 'US')::varchar AS country_code,
            {_nullish_text_expr("telephone_number")}::varchar AS telephone_number,
            {_nullish_text_expr("fax_number")}::varchar AS fax_number,
            {_canonical_contact_number_expr("telephone_number", "country_code")}::varchar AS phone_number,
            {_contact_extension_expr("telephone_number")}::varchar AS phone_extension,
            {_canonical_contact_number_expr("fax_number", "country_code")}::varchar AS fax_number_digits,
            {_contact_extension_expr("fax_number")}::varchar AS fax_extension,
            lat::numeric AS lat,
            long::numeric AS long,
            date_added::date AS date_added,
            NULLIF(TRIM(place_id), '')::varchar AS place_id,
            {_source_priority_expr("address_source")}::int AS source_priority,
            address_source::varchar AS address_source,
            source_record_id::varchar AS source_record_id,
            updated_at::timestamp AS updated_at,
            address_key::uuid AS address_key
          FROM base_rows
    ),"""


def _insert_raw_normalized_cte_sql() -> str:
    """Build the normalized raw row and checksum CTE."""

    return f"""
    normalized AS (
        SELECT
            entity_type,
            entity_id,
            npi,
            inferred_npi,
            inference_confidence,
            inference_method,
            entity_name,
            entity_subtype,
            type,
            taxonomy_array,
            plans_network_array,
            procedures_array,
            medications_array,
            aca_plan_array,
            aca_network_array,
            ptg_plan_array,
            ptg_source_array,
            group_plan_array,
            base_address_version,
            first_line,
            second_line,
            city_name,
            state_name,
            postal_code,
            country_code,
            telephone_number,
            fax_number,
            phone_number,
            phone_extension,
            fax_number_digits,
            fax_extension,
            lat,
            long,
            date_added,
            place_id,
            source_priority,
            address_source,
            source_record_id,
            updated_at,
            address_key,
            {_address_checksum_expr({
                "first_line": _alnum_norm_expr("first_line"),
                "second_line": _alnum_norm_expr("second_line"),
                "city_name": _alnum_norm_expr("city_name"),
                "state_name": _state_norm_expr("state_name"),
                "postal_code": _zip5_norm_expr("postal_code"),
                "country_code": _state_norm_expr("country_code"),
                "telephone_number": _phone_norm_expr("telephone_number"),
            })} AS checksum
          FROM sanitized
    )"""


def _insert_raw_select_sql() -> str:
    """Build the normalized raw-row projection."""

    return """
    SELECT
        entity_type,
        entity_id,
        npi,
        inferred_npi,
        inference_confidence,
        inference_method,
        entity_name,
        entity_subtype,
        type,
        taxonomy_array,
        plans_network_array,
        procedures_array,
        medications_array,
        aca_plan_array,
        aca_network_array,
        ptg_plan_array,
        ptg_source_array,
        group_plan_array,
        base_address_version,
        first_line,
        second_line,
        city_name,
        state_name,
        postal_code,
        country_code,
        telephone_number,
        fax_number,
        phone_number,
        phone_extension,
        fax_number_digits,
        fax_extension,
        NULL::varchar AS formatted_address,
        NULL::smallint AS formatted_address_version,
        NULL::varchar AS formatted_address_source,
        lat,
        long,
        date_added,
        place_id,
        updated_at,
        source_priority,
        address_source,
        source_record_id,
        address_key,
        checksum
      FROM normalized;
    """


def _insert_raw_from_source_sql(
    db_schema: str,
    raw_table: str,
    source_select: str,
    *,
    is_address_canon_available: bool = True,
) -> str:
    """Build SQL that normalizes one source query into the raw stage."""

    del is_address_canon_available
    return "".join(
        (
            _insert_raw_header_sql(db_schema, raw_table),
            _insert_raw_base_rows_cte_sql(source_select),
            _insert_raw_sanitized_cte_sql(),
            _insert_raw_normalized_cte_sql(),
            _insert_raw_select_sql(),
        )
    )


def _raw_materialize_shard_filter_sql(
    dedupe_key_expr: str,
    checksum_modulo: int | None,
    checksum_remainder: int | None,
    *,
    inline_source_evidence: bool,
) -> str:
    """Build the optional raw aggregation shard predicate."""

    if not checksum_modulo or checksum_modulo <= 1 or checksum_remainder is None:
        return ""
    shard_expr = (
        "evidence_shard"
        if inline_source_evidence
        else _aggregate_shard_expr(dedupe_key_expr, int(checksum_modulo))
    )
    return f" WHERE {shard_expr} = {int(checksum_remainder)}"


def _raw_split_array_filter_sql(shard_filter: str) -> str:
    """Build the split-array scan predicate."""

    filter_clauses = []
    if shard_filter:
        filter_clauses.append(shard_filter.replace(" WHERE ", "", 1))
    filter_clauses.append(
        "("
        "COALESCE(CARDINALITY(aca_plan_array), 0) > 0 OR "
        "COALESCE(CARDINALITY(aca_network_array), 0) > 0 OR "
        "COALESCE(CARDINALITY(ptg_plan_array), 0) > 0 OR "
        "COALESCE(CARDINALITY(ptg_source_array), 0) > 0 OR "
        "COALESCE(CARDINALITY(group_plan_array), 0) > 0"
        ")"
    )
    return " WHERE " + " AND ".join(filter_clauses)


def _raw_split_array_cte_sql(
    db_schema: str,
    raw_table: str,
    dedupe_key_expr: str,
    array_filter: str,
) -> str:
    """Build the split array-aggregation CTE."""
    return f"""
    ),
    array_aggregates AS (
        SELECT
            entity_type AS aggregate_entity_type,
            entity_id AS aggregate_entity_id,
            type AS aggregate_type,
            {dedupe_key_expr} AS aggregate_key,
            ARRAY_REMOVE(
                ARRAY_AGG(DISTINCT array_value.value ORDER BY array_value.value)
                    FILTER (WHERE array_value.kind = 'aca_plan'),
                NULL
            )::varchar[] AS aca_plan_array,
            ARRAY_REMOVE(
                ARRAY_AGG(DISTINCT array_value.value ORDER BY array_value.value)
                    FILTER (WHERE array_value.kind = 'aca_network'),
                NULL
            )::varchar[] AS aca_network_array,
            ARRAY_REMOVE(
                ARRAY_AGG(DISTINCT array_value.value ORDER BY array_value.value)
                    FILTER (WHERE array_value.kind = 'ptg_plan'),
                NULL
            )::varchar[] AS ptg_plan_array,
            ARRAY_REMOVE(
                ARRAY_AGG(DISTINCT array_value.value ORDER BY array_value.value)
                    FILTER (WHERE array_value.kind = 'ptg_source'),
                NULL
            )::varchar[] AS ptg_source_array,
            ARRAY_REMOVE(
                ARRAY_AGG(DISTINCT array_value.value ORDER BY array_value.value)
                    FILTER (WHERE array_value.kind = 'group_plan'),
                NULL
            )::varchar[] AS group_plan_array
          FROM {db_schema}.{raw_table}
          CROSS JOIN LATERAL (
              SELECT 'aca_plan'::varchar AS kind, u.value::varchar AS value
                FROM unnest(COALESCE(aca_plan_array, ARRAY[]::varchar[])) AS u(value)
              UNION ALL
              SELECT 'aca_network'::varchar AS kind, u.value::varchar AS value
                FROM unnest(COALESCE(aca_network_array, ARRAY[]::varchar[])) AS u(value)
              UNION ALL
              SELECT 'ptg_plan'::varchar AS kind, u.value::varchar AS value
                FROM unnest(COALESCE(ptg_plan_array, ARRAY[]::varchar[])) AS u(value)
              UNION ALL
              SELECT 'ptg_source'::varchar AS kind, u.value::varchar AS value
                FROM unnest(COALESCE(ptg_source_array, ARRAY[]::varchar[])) AS u(value)
              UNION ALL
              SELECT 'group_plan'::varchar AS kind, u.value::varchar AS value
                FROM unnest(COALESCE(group_plan_array, ARRAY[]::varchar[])) AS u(value)
          ) AS array_value
         {array_filter}
         GROUP BY entity_type, entity_id, type, {dedupe_key_expr}
    """


def _raw_split_array_join_sql(dedupe_key_expr: str) -> str:
    """Build the split array-aggregation join."""

    return f"""
      LEFT JOIN array_aggregates arr
        ON arr.aggregate_entity_type = aggregated.entity_type
       AND arr.aggregate_entity_id = aggregated.entity_id
       AND arr.aggregate_type = aggregated.type
       AND arr.aggregate_key IS NOT DISTINCT FROM aggregated.{dedupe_key_expr}"""


def _raw_split_array_selects_sql() -> str:
    """Build split array projections."""

    return (
        "COALESCE(arr.aca_plan_array, ARRAY[]::varchar[]) AS aca_plan_array,\n"
        "        COALESCE(arr.aca_network_array, ARRAY[]::varchar[]) AS aca_network_array,\n"
        "        COALESCE(arr.ptg_plan_array, ARRAY[]::varchar[]) AS ptg_plan_array,\n"
        "        COALESCE(arr.ptg_source_array, ARRAY[]::varchar[]) AS ptg_source_array,\n"
        "        COALESCE(arr.group_plan_array, ARRAY[]::varchar[]) AS group_plan_array"
    )


def _raw_inline_array_aggregation_sql() -> tuple[str, str, str]:
    """Build the legacy lateral array aggregation fragments."""

    raw_array_joins = """
          LEFT JOIN LATERAL unnest(COALESCE(aca_plan_array, ARRAY[]::varchar[])) AS aca_plan(value) ON TRUE
          LEFT JOIN LATERAL unnest(COALESCE(aca_network_array, ARRAY[]::varchar[])) AS aca_network(value) ON TRUE
          LEFT JOIN LATERAL unnest(COALESCE(ptg_plan_array, ARRAY[]::varchar[])) AS ptg_plan(value) ON TRUE
          LEFT JOIN LATERAL unnest(COALESCE(ptg_source_array, ARRAY[]::varchar[])) AS ptg_source(value) ON TRUE
          LEFT JOIN LATERAL unnest(COALESCE(group_plan_array, ARRAY[]::varchar[])) AS group_plan(value) ON TRUE"""
    aggregated_array_columns = """
            ARRAY_REMOVE(ARRAY_AGG(DISTINCT aca_plan.value ORDER BY aca_plan.value), NULL)::varchar[] AS aca_plan_array,
            ARRAY_REMOVE(ARRAY_AGG(DISTINCT aca_network.value ORDER BY aca_network.value), NULL)::varchar[] AS aca_network_array,
            ARRAY_REMOVE(ARRAY_AGG(DISTINCT ptg_plan.value ORDER BY ptg_plan.value), NULL)::varchar[] AS ptg_plan_array,
            ARRAY_REMOVE(ARRAY_AGG(DISTINCT ptg_source.value ORDER BY ptg_source.value), NULL)::varchar[] AS ptg_source_array,
            ARRAY_REMOVE(ARRAY_AGG(DISTINCT group_plan.value ORDER BY group_plan.value), NULL)::varchar[] AS group_plan_array,"""
    array_selects = (
        "COALESCE(aca_plan_array, ARRAY[]::varchar[]) AS aca_plan_array,\n"
        "        COALESCE(aca_network_array, ARRAY[]::varchar[]) AS aca_network_array,\n"
        "        COALESCE(ptg_plan_array, ARRAY[]::varchar[]) AS ptg_plan_array,\n"
        "        COALESCE(ptg_source_array, ARRAY[]::varchar[]) AS ptg_source_array,\n"
        "        COALESCE(group_plan_array, ARRAY[]::varchar[]) AS group_plan_array"
    )
    return raw_array_joins, aggregated_array_columns, array_selects


def _raw_array_aggregation_sql(
    db_schema: str,
    raw_table: str,
    dedupe_key_expr: str,
    shard_filter: str,
    *,
    should_split: bool,
) -> tuple[str, str, str, str, str]:
    """Return raw array join, column, CTE, join, and select fragments."""

    if should_split:
        return (
            "",
            "",
            _raw_split_array_cte_sql(
                db_schema,
                raw_table,
                dedupe_key_expr,
                _raw_split_array_filter_sql(shard_filter),
            ),
            _raw_split_array_join_sql(dedupe_key_expr),
            _raw_split_array_selects_sql(),
        )
    raw_array_joins, aggregated_array_columns, array_selects = (
        _raw_inline_array_aggregation_sql()
    )
    return raw_array_joins, aggregated_array_columns, "", "", array_selects


def _raw_materialize_insert_header_sql(db_schema: str, stage_table: str) -> str:
    """Build the first half of the raw materialization insert columns."""

    return f"""
    INSERT INTO {db_schema}.{stage_table} (
        entity_type,
        entity_id,
        npi,
        inferred_npi,
        inference_confidence,
        inference_method,
        entity_name,
        entity_subtype,
        location_key,
        row_origin,
        archive_identity_version,
        address_precision,
        premise_key,
        zip5,
        state_code,
        city_norm,
        county_fips,
        source_mask,
        address_source_mask,
        source_count,
        independent_source_count,
        multi_source_confirmed,
        location_confidence_id,
        confidence_score,
        freshness_score,
        address_sources,
        source_record_ids,"""


def _raw_materialize_insert_tail_sql() -> str:
    """Build the remaining raw materialization insert columns."""

    return """
        aca_plan_array,
        aca_network_array,
        ptg_plan_array,
        ptg_source_array,
        group_plan_array,
        base_address_version,
        checksum,
        type,
        taxonomy_array,
        plans_network_array,
        procedures_array,
        medications_array,
        first_line,
        second_line,
        city_name,
        state_name,
        postal_code,
        country_code,
        telephone_number,
        fax_number,
        phone_number,
        phone_extension,
        fax_number_digits,
        fax_extension,
        formatted_address,
        formatted_address_version,
        formatted_address_source,
        lat,
        long,
        date_added,
        place_id,
        address_key,
        updated_at,
        last_seen_at
    )"""


def _raw_materialize_aggregate_identity_sql() -> str:
    """Build the identity half of raw aggregation."""

    return """
    WITH aggregated AS (
        SELECT
            entity_type,
            entity_id,
            type,
            (ARRAY_AGG(location_key ORDER BY source_priority ASC, updated_at DESC NULLS LAST))[1]::varchar AS location_key,
            (ARRAY_AGG(row_origin ORDER BY source_priority ASC, updated_at DESC NULLS LAST))[1]::varchar AS row_origin,
            (ARRAY_AGG(archive_identity_version ORDER BY source_priority ASC, updated_at DESC NULLS LAST))[1]::varchar AS archive_identity_version,
            (ARRAY_AGG(address_precision ORDER BY source_priority ASC, updated_at DESC NULLS LAST))[1]::varchar AS address_precision,
            (ARRAY_AGG(premise_key ORDER BY source_priority ASC, (premise_key IS NULL), updated_at DESC NULLS LAST))[1]::uuid AS premise_key,
            (ARRAY_AGG(zip5 ORDER BY source_priority ASC, (zip5 IS NULL), updated_at DESC NULLS LAST))[1]::varchar AS zip5,
            (ARRAY_AGG(state_code ORDER BY source_priority ASC, (state_code IS NULL), updated_at DESC NULLS LAST))[1]::varchar AS state_code,
            (ARRAY_AGG(city_norm ORDER BY source_priority ASC, (city_norm IS NULL), updated_at DESC NULLS LAST))[1]::varchar AS city_norm,
            (ARRAY_AGG(county_fips ORDER BY source_priority ASC, (county_fips IS NULL), updated_at DESC NULLS LAST))[1]::varchar AS county_fips,
            bit_or(COALESCE(source_mask, 0))::bigint AS source_mask,
            bit_or(COALESCE(address_source_mask, 0))::bigint AS address_source_mask,
            MIN(COALESCE(location_confidence_id, 0))::smallint AS location_confidence_id,
            (ARRAY_AGG(base_address_version ORDER BY source_priority ASC, updated_at DESC NULLS LAST))[1]::varchar AS base_address_version,
            (ARRAY_AGG(checksum ORDER BY source_priority ASC, updated_at DESC NULLS LAST, LENGTH(COALESCE(first_line, '')) DESC, source_record_id ASC))[1]::bigint AS checksum,
            MAX(npi)::bigint AS npi,
            MAX(inferred_npi)::bigint AS inferred_npi,
            MAX(inference_confidence)::float8 AS inference_confidence,
            MAX(inference_method)::varchar AS inference_method,
            (ARRAY_AGG(entity_name ORDER BY source_priority ASC, (entity_name IS NULL), LENGTH(COALESCE(entity_name, '')) DESC, updated_at DESC NULLS LAST))[1]::varchar AS entity_name,
            MAX(entity_subtype)::varchar AS entity_subtype,"""


def _raw_materialize_aggregate_address_sql(
    db_schema: str,
    raw_table: str,
    dedupe_key_expr: str,
    source_record_ids_select: str,
    aggregated_array_columns: str,
    raw_array_joins: str,
    shard_filter: str,
    array_cte: str,
) -> str:
    """Build address fields and grouping for raw aggregation."""

    return f"""
            COALESCE(MAX(taxonomy_array), ARRAY[0]::int[])::int[] AS taxonomy_array,
            COALESCE(MAX(plans_network_array), ARRAY[0]::int[])::int[] AS plans_network_array,
            COALESCE(MAX(procedures_array), ARRAY[0]::int[])::int[] AS procedures_array,
            COALESCE(MAX(medications_array), ARRAY[0]::int[])::int[] AS medications_array,
            (ARRAY_AGG(first_line ORDER BY source_priority ASC, (first_line IS NULL), LENGTH(COALESCE(first_line, '')) DESC, updated_at DESC NULLS LAST))[1]::varchar AS first_line,
            (ARRAY_AGG(second_line ORDER BY source_priority ASC, (second_line IS NULL), LENGTH(COALESCE(second_line, '')) DESC, updated_at DESC NULLS LAST))[1]::varchar AS second_line,
            (ARRAY_AGG(city_name ORDER BY source_priority ASC, (city_name IS NULL), LENGTH(COALESCE(city_name, '')) DESC, updated_at DESC NULLS LAST))[1]::varchar AS city_name,
            (ARRAY_AGG(state_name ORDER BY source_priority ASC, (state_name IS NULL), LENGTH(COALESCE(state_name, '')) DESC, updated_at DESC NULLS LAST))[1]::varchar AS state_name,
            (ARRAY_AGG(postal_code ORDER BY source_priority ASC, (postal_code IS NULL), LENGTH(COALESCE(postal_code, '')) DESC, updated_at DESC NULLS LAST))[1]::varchar AS postal_code,
            (ARRAY_AGG(country_code ORDER BY source_priority ASC, (country_code IS NULL), LENGTH(COALESCE(country_code, '')) DESC, updated_at DESC NULLS LAST))[1]::varchar AS country_code,
            (ARRAY_AGG(telephone_number ORDER BY (telephone_number IS NULL), source_priority ASC, LENGTH(COALESCE(telephone_number, '')) DESC, updated_at DESC NULLS LAST))[1]::varchar AS telephone_number,
            (ARRAY_AGG(fax_number ORDER BY (fax_number IS NULL), source_priority ASC, LENGTH(COALESCE(fax_number, '')) DESC, updated_at DESC NULLS LAST))[1]::varchar AS fax_number,
            (ARRAY_AGG(phone_number ORDER BY (phone_number IS NULL), source_priority ASC, updated_at DESC NULLS LAST))[1]::varchar AS phone_number,
            (ARRAY_AGG(phone_extension ORDER BY (phone_extension IS NULL), source_priority ASC, updated_at DESC NULLS LAST))[1]::varchar AS phone_extension,
            (ARRAY_AGG(fax_number_digits ORDER BY (fax_number_digits IS NULL), source_priority ASC, updated_at DESC NULLS LAST))[1]::varchar AS fax_number_digits,
            (ARRAY_AGG(fax_extension ORDER BY (fax_extension IS NULL), source_priority ASC, updated_at DESC NULLS LAST))[1]::varchar AS fax_extension,
            (ARRAY_AGG(lat ORDER BY (lat IS NULL), source_priority ASC, updated_at DESC NULLS LAST))[1]::numeric AS lat,
            (ARRAY_AGG(long ORDER BY (long IS NULL), source_priority ASC, updated_at DESC NULLS LAST))[1]::numeric AS long,
            MAX(date_added)::date AS date_added,
            MAX(place_id)::varchar AS place_id,
            (ARRAY_AGG(address_key ORDER BY source_priority ASC, (address_key IS NULL), updated_at DESC NULLS LAST))[1]::uuid AS address_key,
            ARRAY_REMOVE(ARRAY_AGG(DISTINCT address_source ORDER BY address_source), NULL)::varchar[] AS address_sources,
            {source_record_ids_select},
{aggregated_array_columns}
            MAX(updated_at)::timestamp AS updated_at
          FROM {db_schema}.{raw_table}
{raw_array_joins}
         {shard_filter}
         GROUP BY entity_type, entity_id, type, {dedupe_key_expr}
{array_cte}
    )"""


def _raw_materialize_select_assurance_sql(array_selects: str) -> str:
    """Build the assurance half of the raw materialization projection."""

    return f"""
    SELECT
        entity_type,
        entity_id,
        npi,
        inferred_npi,
        inference_confidence,
        inference_method,
        entity_name,
        entity_subtype,
        location_key,
        COALESCE(row_origin, 'base') AS row_origin,
        COALESCE(archive_identity_version, '{ARCHIVE_IDENTITY_VERSION}') AS archive_identity_version,
        COALESCE(address_precision, 'unknown') AS address_precision,
        premise_key,
        zip5,
        state_code,
        city_norm,
        county_fips,
        COALESCE(source_mask, 0)::bigint AS source_mask,
        COALESCE(address_source_mask, 0)::bigint AS address_source_mask,
        COALESCE(CARDINALITY(address_sources), 0)::int AS source_count,
        COALESCE(CARDINALITY(address_sources), 0)::int AS independent_source_count,
        (COALESCE(CARDINALITY(address_sources), 0) > 1) AS multi_source_confirmed,
        COALESCE(location_confidence_id, 0)::smallint AS location_confidence_id,
        LEAST(
            100,
            GREATEST(
                0,
                (CASE WHEN address_precision = 'city_zip' THEN 5 WHEN address_precision = 'unknown' THEN 0 ELSE 35 END)
                + (CASE WHEN address_key IS NOT NULL THEN 25 ELSE 3 END)
                + LEAST(COALESCE(CARDINALITY(address_sources), 0) * 5, 20)
                + (CASE WHEN lat IS NOT NULL AND long IS NOT NULL THEN 10 ELSE 0 END)
                - (CASE WHEN address_precision = 'city_zip' THEN 25 ELSE 0 END)
            )
        )::smallint AS confidence_score,
        (CASE WHEN updated_at >= NOW() - INTERVAL '12 months' THEN 10 ELSE 0 END)::smallint AS freshness_score,
        COALESCE(address_sources, ARRAY[]::varchar[]) AS address_sources,
        COALESCE(source_record_ids, ARRAY[]::varchar[]) AS source_record_ids,
        {array_selects},"""


def _raw_materialize_select_address_sql(db_schema: str, array_join: str) -> str:
    """Build address fields and the final raw aggregation join."""

    return f"""
        base_address_version,
        checksum,
        type,
        taxonomy_array,
        plans_network_array,
        procedures_array,
        medications_array,
        first_line,
        second_line,
        city_name,
        state_name,
        postal_code,
        country_code,
        telephone_number,
        fax_number,
        phone_number,
        phone_extension,
        fax_number_digits,
        fax_extension,
        {db_schema}.{ADDRESS_FORMAT_FUNCTION}(
            first_line,
            second_line,
            city_name,
            state_name,
            postal_code,
            country_code
        ) AS formatted_address,
        {ADDRESS_FORMAT_VERSION}::smallint AS formatted_address_version,
        '{ADDRESS_FORMAT_SOURCE}'::varchar AS formatted_address_source,
        lat,
        long,
        date_added,
        place_id,
        address_key,
        updated_at,
        updated_at AS last_seen_at
      FROM aggregated{array_join};
    """


def _materialize_from_raw_sql(
    db_schema: str,
    stage_table: str,
    raw_table: str,
    *,
    checksum_modulo: int | None = None,
    checksum_remainder: int | None = None,
    is_address_canon_available: bool = True,
    inline_source_evidence: bool = False,
) -> str:
    """Build SQL that deduplicates raw evidence into unified locations."""

    dedupe_key_expr = _dedupe_key_expr(is_address_canon_available)
    shard_filter = _raw_materialize_shard_filter_sql(
        dedupe_key_expr,
        checksum_modulo,
        checksum_remainder,
        inline_source_evidence=inline_source_evidence,
    )
    array_fragments = _raw_array_aggregation_sql(
        db_schema,
        raw_table,
        dedupe_key_expr,
        shard_filter,
        should_split=_is_env_enabled(
            "HLTHPRT_ENTITY_ADDRESS_UNIFIED_SPLIT_ARRAY_AGGREGATES",
            DEFAULT_SPLIT_ARRAY_AGGREGATES,
        ),
    )
    raw_array_joins, aggregated_array_columns, array_cte, array_join, array_selects = (
        array_fragments
    )
    sql = "".join(
        (
            _raw_materialize_insert_header_sql(db_schema, stage_table),
            _raw_materialize_insert_tail_sql(),
            _raw_materialize_aggregate_identity_sql(),
            _raw_materialize_aggregate_address_sql(
                db_schema,
                raw_table,
                dedupe_key_expr,
                _source_record_ids_select_sql(),
                aggregated_array_columns,
                raw_array_joins,
                shard_filter,
                array_cte,
            ),
            _raw_materialize_select_assurance_sql(array_selects),
            _raw_materialize_select_address_sql(db_schema, array_join),
        )
    )
    if inline_source_evidence:
        return _inline_source_evidence_sql(sql)
    return sql


def _direct_materialize_location_key_sql() -> str:
    """Build the direct-materialization location key expression."""

    return _location_key_expr(
        {
            "entity_type": "entity_type",
            "entity_id": "entity_id",
            "npi": "npi",
            "inferred_npi": "inferred_npi",
            "address_role_id": _address_role_id_expr("type"),
            "row_origin": "'base'",
            "address_key": "address_key",
            "source_id": _source_id_expr("address_source"),
            "source_record_id": "source_record_id",
            "zip5": _zip5_norm_expr("postal_code"),
            "state_code": _state_norm_expr("state_name"),
            "city_norm": _alnum_norm_expr("city_name"),
        }
    )


def _direct_materialize_checksum_sql() -> str:
    """Build the direct-materialization address checksum expression."""

    return _address_checksum_expr(
        {
            "first_line": _alnum_norm_expr("first_line"),
            "second_line": _alnum_norm_expr("second_line"),
            "city_name": _alnum_norm_expr("city_name"),
            "state_name": _state_norm_expr("state_name"),
            "postal_code": _zip5_norm_expr("postal_code"),
            "country_code": _state_norm_expr("country_code"),
            "telephone_number": _phone_norm_expr("telephone_number"),
        }
    )


def _direct_materialize_header_sql(
    db_schema: str,
    stage_table: str,
    selects_sql: str,
) -> str:
    """Build the direct materialization insert header and base CTE."""

    return f"""
    INSERT INTO {db_schema}.{stage_table} (
        entity_type,
        entity_id,
        npi,
        inferred_npi,
        inference_confidence,
        inference_method,
        entity_name,
        entity_subtype,
        location_key,
        source_count,
        multi_source_confirmed,
        address_sources,
        source_record_ids,
        checksum,
        type,
        taxonomy_array,
        plans_network_array,
        procedures_array,
        medications_array,
        first_line,
        second_line,
        city_name,
        state_name,
        postal_code,
        country_code,
        telephone_number,
        fax_number,
        phone_number,
        phone_extension,
        fax_number_digits,
        fax_extension,
        formatted_address,
        formatted_address_version,
        formatted_address_source,
        lat,
        long,
        date_added,
        place_id,
        address_key,
        updated_at
    )
    WITH base_rows AS (
        {selects_sql}
    ),"""


def _direct_materialize_sanitized_cte_sql() -> str:
    """Build direct source value sanitization."""

    return f"""
    sanitized AS (
        SELECT
            entity_type,
            entity_id,
            npi,
            inferred_npi,
            inference_confidence,
            inference_method,
            NULLIF(TRIM(entity_name), '')::varchar AS entity_name,
            NULLIF(TRIM(entity_subtype), '')::varchar AS entity_subtype,
            COALESCE(NULLIF(TRIM(type), ''), 'primary')::varchar AS type,
            COALESCE(taxonomy_array, ARRAY[0]::int[])::int[] AS taxonomy_array,
            COALESCE(plans_network_array, ARRAY[0]::int[])::int[] AS plans_network_array,
            COALESCE(procedures_array, ARRAY[0]::int[])::int[] AS procedures_array,
            COALESCE(medications_array, ARRAY[0]::int[])::int[] AS medications_array,
            NULLIF(TRIM(first_line), '')::varchar AS first_line,
            NULLIF(TRIM(second_line), '')::varchar AS second_line,
            NULLIF(TRIM(city_name), '')::varchar AS city_name,
            NULLIF(TRIM(state_name), '')::varchar AS state_name,
            NULLIF(TRIM(postal_code), '')::varchar AS postal_code,
            COALESCE(NULLIF(TRIM(country_code), ''), 'US')::varchar AS country_code,
            {_nullish_text_expr("telephone_number")}::varchar AS telephone_number,
            {_nullish_text_expr("fax_number")}::varchar AS fax_number,
            {_canonical_contact_number_expr("telephone_number", "country_code")}::varchar AS phone_number,
            {_contact_extension_expr("telephone_number")}::varchar AS phone_extension,
            {_canonical_contact_number_expr("fax_number", "country_code")}::varchar AS fax_number_digits,
            {_contact_extension_expr("fax_number")}::varchar AS fax_extension,
            lat::numeric AS lat,
            long::numeric AS long,
            date_added::date AS date_added,
            NULLIF(TRIM(place_id), '')::varchar AS place_id,
            {_source_priority_expr("address_source")}::int AS source_priority,
            address_source::varchar AS address_source,
            source_record_id::varchar AS source_record_id,
            updated_at::timestamp AS updated_at,
            address_key::uuid AS address_key
          FROM base_rows
    ),"""


def _direct_materialize_normalized_cte_sql() -> str:
    """Build direct normalized location and checksum fields."""

    return f"""
    normalized AS (
        SELECT
            entity_type,
            entity_id,
            npi,
            inferred_npi,
            inference_confidence,
            inference_method,
            entity_name,
            entity_subtype,
            type,
            taxonomy_array,
            plans_network_array,
            procedures_array,
            medications_array,
            first_line,
            second_line,
            city_name,
            state_name,
            postal_code,
            country_code,
            telephone_number,
            fax_number,
            phone_number,
            phone_extension,
            fax_number_digits,
            fax_extension,
            lat,
            long,
            date_added,
            place_id,
            source_priority,
            address_source,
            source_record_id,
            updated_at,
            address_key,
            {_direct_materialize_location_key_sql()} AS location_key,
            {_direct_materialize_checksum_sql()} AS checksum
          FROM sanitized
    ),"""


def _direct_materialize_aggregated_cte_sql(
    dedupe_key_expr: str,
    source_record_ids_select: str,
) -> str:
    """Build direct address aggregation."""

    return f"""
    aggregated AS (
        SELECT
            entity_type,
            entity_id,
            type,
            (ARRAY_AGG(location_key ORDER BY source_priority ASC, updated_at DESC NULLS LAST))[1]::varchar AS location_key,
            (ARRAY_AGG(checksum ORDER BY source_priority ASC, updated_at DESC NULLS LAST, LENGTH(COALESCE(first_line, '')) DESC, source_record_id ASC))[1]::bigint AS checksum,
            MAX(npi)::bigint AS npi,
            MAX(inferred_npi)::bigint AS inferred_npi,
            MAX(inference_confidence)::float8 AS inference_confidence,
            MAX(inference_method)::varchar AS inference_method,
            (ARRAY_AGG(entity_name ORDER BY source_priority ASC, (entity_name IS NULL), LENGTH(COALESCE(entity_name, '')) DESC, updated_at DESC NULLS LAST))[1]::varchar AS entity_name,
            MAX(entity_subtype)::varchar AS entity_subtype,
            COALESCE(MAX(taxonomy_array), ARRAY[0]::int[])::int[] AS taxonomy_array,
            COALESCE(MAX(plans_network_array), ARRAY[0]::int[])::int[] AS plans_network_array,
            COALESCE(MAX(procedures_array), ARRAY[0]::int[])::int[] AS procedures_array,
            COALESCE(MAX(medications_array), ARRAY[0]::int[])::int[] AS medications_array,
            (ARRAY_AGG(first_line ORDER BY source_priority ASC, (first_line IS NULL), LENGTH(COALESCE(first_line, '')) DESC, updated_at DESC NULLS LAST))[1]::varchar AS first_line,
            (ARRAY_AGG(second_line ORDER BY source_priority ASC, (second_line IS NULL), LENGTH(COALESCE(second_line, '')) DESC, updated_at DESC NULLS LAST))[1]::varchar AS second_line,
            (ARRAY_AGG(city_name ORDER BY source_priority ASC, (city_name IS NULL), LENGTH(COALESCE(city_name, '')) DESC, updated_at DESC NULLS LAST))[1]::varchar AS city_name,
            (ARRAY_AGG(state_name ORDER BY source_priority ASC, (state_name IS NULL), LENGTH(COALESCE(state_name, '')) DESC, updated_at DESC NULLS LAST))[1]::varchar AS state_name,
            (ARRAY_AGG(postal_code ORDER BY source_priority ASC, (postal_code IS NULL), LENGTH(COALESCE(postal_code, '')) DESC, updated_at DESC NULLS LAST))[1]::varchar AS postal_code,
            (ARRAY_AGG(country_code ORDER BY source_priority ASC, (country_code IS NULL), LENGTH(COALESCE(country_code, '')) DESC, updated_at DESC NULLS LAST))[1]::varchar AS country_code,
            (ARRAY_AGG(telephone_number ORDER BY (telephone_number IS NULL), source_priority ASC, LENGTH(COALESCE(telephone_number, '')) DESC, updated_at DESC NULLS LAST))[1]::varchar AS telephone_number,
            (ARRAY_AGG(fax_number ORDER BY (fax_number IS NULL), source_priority ASC, LENGTH(COALESCE(fax_number, '')) DESC, updated_at DESC NULLS LAST))[1]::varchar AS fax_number,
            (ARRAY_AGG(phone_number ORDER BY (phone_number IS NULL), source_priority ASC, updated_at DESC NULLS LAST))[1]::varchar AS phone_number,
            (ARRAY_AGG(phone_extension ORDER BY (phone_extension IS NULL), source_priority ASC, updated_at DESC NULLS LAST))[1]::varchar AS phone_extension,
            (ARRAY_AGG(fax_number_digits ORDER BY (fax_number_digits IS NULL), source_priority ASC, updated_at DESC NULLS LAST))[1]::varchar AS fax_number_digits,
            (ARRAY_AGG(fax_extension ORDER BY (fax_extension IS NULL), source_priority ASC, updated_at DESC NULLS LAST))[1]::varchar AS fax_extension,
            (ARRAY_AGG(lat ORDER BY (lat IS NULL), source_priority ASC, updated_at DESC NULLS LAST))[1]::numeric AS lat,
            (ARRAY_AGG(long ORDER BY (long IS NULL), source_priority ASC, updated_at DESC NULLS LAST))[1]::numeric AS long,
            MAX(date_added)::date AS date_added,
            MAX(place_id)::varchar AS place_id,
            (ARRAY_AGG(address_key ORDER BY source_priority ASC, (address_key IS NULL), updated_at DESC NULLS LAST))[1]::uuid AS address_key,
            ARRAY_REMOVE(ARRAY_AGG(DISTINCT address_source ORDER BY address_source), NULL)::varchar[] AS address_sources,
            {source_record_ids_select},
            MAX(updated_at)::timestamp AS updated_at
          FROM normalized
      GROUP BY entity_type, entity_id, type, {dedupe_key_expr}
    )"""


def _direct_materialize_select_sql(db_schema: str) -> str:
    """Build the final direct materialization projection."""

    return f"""
    SELECT
        entity_type,
        entity_id,
        npi,
        inferred_npi,
        inference_confidence,
        inference_method,
        entity_name,
        entity_subtype,
        location_key,
        COALESCE(CARDINALITY(address_sources), 0)::int AS source_count,
        (COALESCE(CARDINALITY(address_sources), 0) > 1) AS multi_source_confirmed,
        COALESCE(address_sources, ARRAY[]::varchar[]) AS address_sources,
        COALESCE(source_record_ids, ARRAY[]::varchar[]) AS source_record_ids,
        checksum,
        type,
        taxonomy_array,
        plans_network_array,
        procedures_array,
        medications_array,
        first_line,
        second_line,
        city_name,
        state_name,
        postal_code,
        country_code,
        telephone_number,
        fax_number,
        phone_number,
        phone_extension,
        fax_number_digits,
        fax_extension,
        {db_schema}.{ADDRESS_FORMAT_FUNCTION}(
            first_line,
            second_line,
            city_name,
            state_name,
            postal_code,
            country_code
        ) AS formatted_address,
        {ADDRESS_FORMAT_VERSION}::smallint AS formatted_address_version,
        '{ADDRESS_FORMAT_SOURCE}'::varchar AS formatted_address_source,
        lat,
        long,
        date_added,
        place_id,
        address_key,
        updated_at
      FROM aggregated;
    """


def _materialize_sql(
    db_schema: str,
    stage_table: str,
    source_selects: Iterable[str],
    *,
    is_address_canon_available: bool = True,
) -> str:
    """Build direct source-to-stage materialization SQL."""

    selects_sql = "\nUNION ALL\n".join(select.strip() for select in source_selects)
    return "".join(
        (
            _direct_materialize_header_sql(db_schema, stage_table, selects_sql),
            _direct_materialize_sanitized_cte_sql(),
            _direct_materialize_normalized_cte_sql(),
            _direct_materialize_aggregated_cte_sql(
                _dedupe_key_expr(is_address_canon_available),
                _source_record_ids_select_sql(),
            ),
            _direct_materialize_select_sql(db_schema),
        )
    )


def _evidence_group_hash_expr(evidence_shards: int) -> str:
    shards = max(int(evidence_shards), 1)
    return f"""
        (((hashtext(CONCAT_WS(
            '|',
            COALESCE(entity_type, ''),
            COALESCE(entity_id, ''),
            COALESCE(address_key::text, ''),
            COALESCE(street_key, ''),
            COALESCE(city_key, ''),
            COALESCE(state_key, ''),
            COALESCE(zip_key, ''),
            COALESCE(country_key, '')
        )) % {shards}) + {shards}) % {shards})::int
    """


def _alias_col(alias: str, column: str) -> str:
    return f"{alias}.{column}" if alias else column


def _evidence_group_key_exprs(alias: str) -> dict[str, str]:
    address_key = _alias_col(alias, "address_key")
    return {
        "street": (
            f"CASE WHEN {address_key} IS NULL THEN "
            f"{_street_soft_norm_expr(_alias_col(alias, 'first_line'))} END"
        ),
        "city": (
            f"CASE WHEN {address_key} IS NULL THEN "
            f"COALESCE(NULLIF({_alias_col(alias, 'city_norm')}, ''), "
            f"{_alnum_norm_expr(_alias_col(alias, 'city_name'))}) END"
        ),
        "state": (
            f"CASE WHEN {address_key} IS NULL THEN "
            f"COALESCE(NULLIF({_alias_col(alias, 'state_code')}, ''), "
            f"{_state_norm_expr(_alias_col(alias, 'state_name'))}) END"
        ),
        "zip": (
            f"CASE WHEN {address_key} IS NULL THEN "
            f"COALESCE(NULLIF({_alias_col(alias, 'zip5')}, ''), "
            f"{_zip5_norm_expr(_alias_col(alias, 'postal_code'))}) END"
        ),
        "country": (
            f"CASE WHEN {address_key} IS NULL THEN "
            f"{_state_norm_expr(_alias_col(alias, 'country_code'))} END"
        ),
    }


def _evidence_group_hash_expr_for_alias(alias: str, evidence_shards: int) -> str:
    shards = max(int(evidence_shards), 1)
    keys = _evidence_group_key_exprs(alias)
    return f"""
        (((hashtext(CONCAT_WS(
            '|',
            COALESCE({_alias_col(alias, 'entity_type')}, ''),
            COALESCE({_alias_col(alias, 'entity_id')}, ''),
            COALESCE({_alias_col(alias, 'address_key')}::text, ''),
            COALESCE(({keys['street']})::varchar, ''),
            COALESCE(({keys['city']})::varchar, ''),
            COALESCE(({keys['state']})::varchar, ''),
            COALESCE(({keys['zip']})::varchar, ''),
            COALESCE(({keys['country']})::varchar, '')
        )) % {shards}) + {shards}) % {shards})::int
    """


def _inline_source_evidence_sql(sql: str) -> str:
    marker = "\n    SELECT\n        entity_type,"
    idx = sql.rindex(marker)
    evidence_keys = _evidence_group_key_exprs("agg")
    evidence_cte = f"""
    , evidence AS (
        SELECT
            agg.entity_type AS e_entity_type,
            agg.entity_id AS e_entity_id,
            agg.address_key AS e_address_key,
            ({evidence_keys['street']})::varchar AS e_street_key,
            ({evidence_keys['city']})::varchar AS e_city_key,
            ({evidence_keys['state']})::varchar AS e_state_key,
            ({evidence_keys['zip']})::varchar AS e_zip_key,
            ({evidence_keys['country']})::varchar AS e_country_key,
            ARRAY_REMOVE(ARRAY_AGG(DISTINCT src.src ORDER BY src.src), NULL)::varchar[] AS evidence_sources,
            ARRAY_REMOVE(ARRAY_AGG(DISTINCT rid.rid ORDER BY rid.rid), NULL)::varchar[] AS evidence_record_ids
          FROM aggregated AS agg
          LEFT JOIN LATERAL unnest(COALESCE(agg.address_sources, ARRAY[]::varchar[])) AS src(src) ON TRUE
          LEFT JOIN LATERAL unnest(COALESCE(agg.source_record_ids, ARRAY[]::varchar[])) AS rid(rid) ON TRUE
         GROUP BY 1,2,3,4,5,6,7,8
    )"""
    sql = sql[:idx] + evidence_cte + sql[idx:]
    sql = sql.replace(
        "COALESCE(CARDINALITY(address_sources), 0)::int AS source_count,\n"
        "        COALESCE(CARDINALITY(address_sources), 0)::int AS independent_source_count,\n"
        "        (COALESCE(CARDINALITY(address_sources), 0) > 1) AS multi_source_confirmed,",
        "COALESCE(CARDINALITY(COALESCE(e.evidence_sources, address_sources)), 0)::int AS source_count,\n"
        "        COALESCE(CARDINALITY(COALESCE(e.evidence_sources, address_sources)), 0)::int AS independent_source_count,\n"
        "        (COALESCE(CARDINALITY(COALESCE(e.evidence_sources, address_sources)), 0) > 1) AS multi_source_confirmed,",
    )
    sql = sql.replace(
        "COALESCE(address_sources, ARRAY[]::varchar[]) AS address_sources,\n"
        "        COALESCE(source_record_ids, ARRAY[]::varchar[]) AS source_record_ids,",
        "COALESCE(e.evidence_sources, address_sources, ARRAY[]::varchar[]) AS address_sources,\n"
        "        COALESCE(e.evidence_record_ids, source_record_ids, ARRAY[]::varchar[]) AS source_record_ids,",
    )
    joined_keys = _evidence_group_key_exprs("aggregated")
    join_sql = f"""FROM aggregated
      LEFT JOIN evidence e
        ON e.e_entity_type = aggregated.entity_type
       AND e.e_entity_id = aggregated.entity_id
       AND e.e_address_key IS NOT DISTINCT FROM aggregated.address_key
       AND e.e_street_key IS NOT DISTINCT FROM ({joined_keys['street']})::varchar
       AND e.e_city_key IS NOT DISTINCT FROM ({joined_keys['city']})::varchar
       AND e.e_state_key IS NOT DISTINCT FROM ({joined_keys['state']})::varchar
       AND e.e_zip_key IS NOT DISTINCT FROM ({joined_keys['zip']})::varchar
       AND e.e_country_key IS NOT DISTINCT FROM ({joined_keys['country']})::varchar"""
    final_from = "\n      FROM aggregated"
    final_idx = sql.rindex(final_from)
    return sql[:final_idx] + "\n      " + join_sql + sql[final_idx + len(final_from):]


def _prepare_multi_source_evidence_table_sql(
    db_schema: str,
    evidence_table: str,
    *,
    unlogged: bool = True,
) -> str:
    storage_mode = "UNLOGGED " if unlogged else ""
    return f"""
    CREATE {storage_mode}TABLE {db_schema}.{evidence_table} (
        location_key varchar(64) PRIMARY KEY,
        evidence_shard int NOT NULL,
        entity_type varchar(64) NOT NULL,
        entity_id varchar(128) NOT NULL,
        address_key uuid,
        street_key varchar,
        city_key varchar,
        state_key varchar,
        zip_key varchar,
        country_key varchar,
        address_sources varchar[] NOT NULL DEFAULT '{{}}',
        source_record_ids varchar[] NOT NULL DEFAULT '{{}}',
        evidence_sources varchar[] NOT NULL DEFAULT '{{}}',
        evidence_record_ids varchar[] NOT NULL DEFAULT '{{}}'
    );
    """


def _multi_source_affected_filter(
    db_schema: str,
    affected_group_table: str | None,
    affected_scope: str,
) -> str:
    """Build the optional NPI- or group-scoped evidence filter."""
    if not affected_group_table:
        return ""
    if affected_scope == "npi":
        row_npi_expr = _entity_address_row_npi_expr("t")
        return f"""
           AND {row_npi_expr} IS NOT NULL
           AND EXISTS (
                SELECT 1
                  FROM {db_schema}.{affected_group_table} AS affected
                 WHERE affected.entity_npi IS NOT NULL
                   AND affected.entity_npi = {row_npi_expr}
           )"""
    return f"""
       AND EXISTS (
            SELECT 1
              FROM {db_schema}.{affected_group_table} AS affected
             WHERE {_entity_address_evidence_group_match_sql("affected", "t")}
       )"""


_LOAD_MULTI_SOURCE_EVIDENCE_SQL = """
    WITH normalized AS (
        SELECT
            t.location_key,
            t.entity_type,
            t.entity_id,
            t.address_key::uuid AS address_key,
            CASE WHEN t.address_key IS NULL THEN {street_norm} END::varchar AS street_key,
            CASE WHEN t.address_key IS NULL
                THEN COALESCE(NULLIF(t.city_norm, ''), {city_norm})
            END::varchar AS city_key,
            CASE WHEN t.address_key IS NULL
                THEN COALESCE(NULLIF(t.state_code, ''), {state_norm})
            END::varchar AS state_key,
            CASE WHEN t.address_key IS NULL
                THEN COALESCE(NULLIF(t.zip5, ''), {zip_norm})
            END::varchar AS zip_key,
            CASE WHEN t.address_key IS NULL THEN {country_norm} END::varchar AS country_key,
            COALESCE(t.address_sources, ARRAY[]::varchar[])::varchar[] AS address_sources,
            COALESCE(t.source_record_ids, ARRAY[]::varchar[])::varchar[] AS source_record_ids
          FROM {db_schema}.{stage_table} AS t
         WHERE t.location_key IS NOT NULL
           {affected_filter}
    )
    INSERT INTO {db_schema}.{evidence_table} (
        location_key,
        evidence_shard,
        entity_type,
        entity_id,
        address_key,
        street_key,
        city_key,
        state_key,
        zip_key,
        country_key,
        address_sources,
        source_record_ids
    )
    SELECT
        location_key,
        {group_hash_expr} AS evidence_shard,
        entity_type,
        entity_id,
        address_key,
        street_key,
        city_key,
        state_key,
        zip_key,
        country_key,
        address_sources,
        source_record_ids
      FROM normalized;
"""


def _load_multi_source_evidence_base_sql(
    db_schema: str,
    stage_table: str,
    evidence_table: str,
    *,
    evidence_shards: int,
    affected_group_table: str | None = None,
    affected_scope: str = "group",
) -> str:
    """Build SQL that seeds normalized multi-source evidence groups."""
    return _LOAD_MULTI_SOURCE_EVIDENCE_SQL.format(
        db_schema=db_schema,
        stage_table=stage_table,
        evidence_table=evidence_table,
        group_hash_expr=_evidence_group_hash_expr(evidence_shards),
        affected_filter=_multi_source_affected_filter(
            db_schema,
            affected_group_table,
            affected_scope,
        ),
        street_norm=_street_soft_norm_expr("t.first_line"),
        city_norm=_alnum_norm_expr("t.city_name"),
        state_norm=_state_norm_expr("t.state_name"),
        zip_norm=_zip5_norm_expr("t.postal_code"),
        country_norm=_state_norm_expr("t.country_code"),
    )


def _multi_source_evidence_keyed_cte_sql(
    db_schema: str,
    evidence_table: str,
    evidence_shard: int,
) -> str:
    """Build the selected evidence-shard key CTE."""

    return f"""
    WITH keyed AS MATERIALIZED (
        SELECT
            location_key,
            entity_type,
            entity_id,
            address_key,
            street_key,
            city_key,
            state_key,
            zip_key,
            country_key,
            address_sources,
            source_record_ids
          FROM {db_schema}.{evidence_table}
         WHERE evidence_shard = {int(evidence_shard)}
    ),"""


def _multi_source_evidence_sources_cte_sql() -> str:
    """Build the source-evidence aggregation CTE."""

    return """
    source_evidence AS (
        SELECT
            k.entity_type,
            k.entity_id,
            k.address_key,
            k.street_key,
            k.city_key,
            k.state_key,
            k.zip_key,
            k.country_key,
            ARRAY_REMOVE(ARRAY_AGG(DISTINCT src.src ORDER BY src.src), NULL)::varchar[] AS evidence_sources
          FROM keyed AS k
          LEFT JOIN LATERAL unnest(COALESCE(k.address_sources, ARRAY[]::varchar[])) AS src(src) ON TRUE
         GROUP BY
            k.entity_type,
            k.entity_id,
            k.address_key,
            k.street_key,
            k.city_key,
            k.state_key,
            k.zip_key,
            k.country_key
    ),"""


def _multi_source_evidence_records_cte_sql() -> str:
    """Build the source-record-evidence aggregation CTE."""

    return """
    record_evidence AS (
        SELECT
            k.entity_type,
            k.entity_id,
            k.address_key,
            k.street_key,
            k.city_key,
            k.state_key,
            k.zip_key,
            k.country_key,
            ARRAY_REMOVE(ARRAY_AGG(DISTINCT rid.rid ORDER BY rid.rid), NULL)::varchar[] AS evidence_record_ids
          FROM keyed AS k
          LEFT JOIN LATERAL unnest(COALESCE(k.source_record_ids, ARRAY[]::varchar[])) AS rid(rid) ON TRUE
         GROUP BY
            k.entity_type,
            k.entity_id,
            k.address_key,
            k.street_key,
            k.city_key,
            k.state_key,
            k.zip_key,
            k.country_key
    )"""


def _multi_source_evidence_update_sql(
    db_schema: str,
    evidence_table: str,
) -> str:
    """Build the evidence-array update from the aggregated CTEs."""

    return f"""
    UPDATE {db_schema}.{evidence_table} AS e
       SET evidence_sources = COALESCE(se.evidence_sources, ARRAY[]::varchar[]),
           evidence_record_ids = COALESCE(re.evidence_record_ids, ARRAY[]::varchar[])
      FROM source_evidence AS se
      LEFT JOIN record_evidence AS re
        ON re.entity_type = se.entity_type
       AND re.entity_id = se.entity_id
       AND re.address_key IS NOT DISTINCT FROM se.address_key
       AND re.street_key IS NOT DISTINCT FROM se.street_key
       AND re.city_key IS NOT DISTINCT FROM se.city_key
       AND re.state_key IS NOT DISTINCT FROM se.state_key
       AND re.zip_key IS NOT DISTINCT FROM se.zip_key
       AND re.country_key IS NOT DISTINCT FROM se.country_key
      JOIN keyed AS k
        ON se.entity_type = k.entity_type
       AND se.entity_id = k.entity_id
       AND se.address_key IS NOT DISTINCT FROM k.address_key
       AND se.street_key IS NOT DISTINCT FROM k.street_key
       AND se.city_key IS NOT DISTINCT FROM k.city_key
       AND se.state_key IS NOT DISTINCT FROM k.state_key
       AND se.zip_key IS NOT DISTINCT FROM k.zip_key
       AND se.country_key IS NOT DISTINCT FROM k.country_key
     WHERE e.location_key = k.location_key;
    """


def _insert_multi_source_evidence_shard_sql(
    db_schema: str,
    stage_table: str,
    evidence_table: str,
    *,
    evidence_shards: int,
    evidence_shard: int,
) -> str:
    """Build SQL that aggregates one multi-source evidence shard."""

    del stage_table, evidence_shards
    return "".join(
        (
            _multi_source_evidence_keyed_cte_sql(
                db_schema,
                evidence_table,
                evidence_shard,
            ),
            _multi_source_evidence_sources_cte_sql(),
            _multi_source_evidence_records_cte_sql(),
            _multi_source_evidence_update_sql(db_schema, evidence_table),
        )
    )


def _index_multi_source_evidence_table_sql(db_schema: str, evidence_table: str) -> str:
    index_name = _archived_identifier(f"{evidence_table}_idx_shard_location", "")
    return f"""
    CREATE INDEX IF NOT EXISTS {index_name}
        ON {db_schema}.{evidence_table} (evidence_shard, location_key);
    """


def _apply_multi_source_evidence_sql(
    db_schema: str,
    stage_table: str,
    evidence_table: str,
    *,
    evidence_shard: int,
) -> str:
    return f"""
    UPDATE {db_schema}.{stage_table} AS t
       SET address_sources = e.evidence_sources,
           source_record_ids = e.evidence_record_ids,
           source_count = COALESCE(CARDINALITY(e.evidence_sources), 0)::int,
           independent_source_count = COALESCE(CARDINALITY(e.evidence_sources), 0)::int,
           multi_source_confirmed = COALESCE(CARDINALITY(e.evidence_sources), 0) > 1
      FROM {db_schema}.{evidence_table} AS e
     WHERE e.evidence_shard = {int(evidence_shard)}
       AND t.location_key = e.location_key
       AND (
            COALESCE(t.address_sources, ARRAY[]::varchar[]) IS DISTINCT FROM e.evidence_sources
            OR COALESCE(t.source_record_ids, ARRAY[]::varchar[]) IS DISTINCT FROM e.evidence_record_ids
            OR COALESCE(t.source_count, 0) IS DISTINCT FROM COALESCE(CARDINALITY(e.evidence_sources), 0)
            OR COALESCE(t.independent_source_count, 0)
                IS DISTINCT FROM COALESCE(CARDINALITY(e.evidence_sources), 0)
            OR COALESCE(t.multi_source_confirmed, FALSE)
                IS DISTINCT FROM (COALESCE(CARDINALITY(e.evidence_sources), 0) > 1)
       );
    """


def _truncate_support_stage_sql(db_schema: str, stage_tables: dict[type, str]) -> str:
    table_names = ", ".join(f"{db_schema}.{table}" for table in stage_tables.values())
    return f"TRUNCATE TABLE {table_names};"


def _affected_stage_row_filter_sql(db_schema: str, affected_group_table: str | None, row_alias: str = "t") -> str:
    if not affected_group_table:
        return ""
    return f"""
       AND EXISTS (
            SELECT 1
              FROM {db_schema}.{affected_group_table} AS affected
             WHERE {_entity_address_evidence_group_match_sql("affected", row_alias)}
       )
    """


def _copy_unaffected_bridge_rows_sql(
    db_schema: str,
    live_bridge_table: str,
    stage_bridge_table: str,
    columns: Iterable[str],
    affected_group_table: str,
) -> str:
    column_list = ", ".join(columns)
    selected_columns = ", ".join(f"b.{column}" for column in columns)
    return f"""
    INSERT INTO {db_schema}.{stage_bridge_table} ({column_list})
    SELECT {selected_columns}
      FROM {db_schema}.{live_bridge_table} AS b
      JOIN {db_schema}.{EntityAddressUnified.__main_table__} AS live
        ON live.location_key = b.location_key
     WHERE NOT EXISTS (
           SELECT 1
             FROM {db_schema}.{affected_group_table} AS affected
            WHERE {_entity_address_evidence_group_match_sql("affected", "live")}
     );
    """


def _support_patch_models(*, build_network_bridge: bool) -> list[type]:
    models: list[type] = [
        EntityAddressEvidence,
        EntityAddressPlanBridge,
        EntityAddressProcedureBridge,
        EntityAddressMedicationBridge,
    ]
    if build_network_bridge:
        models.insert(2, EntityAddressNetworkBridge)
    return models


def _model_column_names(model: type) -> list[str]:
    return [column.name for column in model.__table__.columns]


def _delete_affected_group_support_sql(
    db_schema: str,
    live_support_table: str,
    old_entity_table: str,
    affected_group_table: str,
    *,
    replacement_stage_table: str | None = None,
) -> str:
    replacement_filter = ""
    if replacement_stage_table:
        replacement_filter = f"""
        OR EXISTS (
            SELECT 1
              FROM {db_schema}.{replacement_stage_table} AS replacement
             WHERE replacement.location_key = support.location_key
        )"""
    return f"""
    DELETE FROM {db_schema}.{live_support_table} AS support
     WHERE EXISTS (
            SELECT 1
              FROM {db_schema}.{old_entity_table} AS live
             WHERE live.location_key = support.location_key
               AND EXISTS (
                    SELECT 1
                      FROM {db_schema}.{affected_group_table} AS affected
                     WHERE {_entity_address_evidence_group_match_sql("affected", "live")}
               )
       )
       {replacement_filter};
    """


def _insert_stage_support_into_live_sql(
    db_schema: str,
    model: type,
    live_support_table: str,
    stage_support_table: str,
) -> str:
    columns = _model_column_names(model)
    column_list = ", ".join(columns)
    if model is EntityAddressEvidence:
        select_list = ", ".join(
            (
                "((SELECT COALESCE(MAX(evidence_id), 0) FROM "
                f"{db_schema}.{live_support_table}) + ROW_NUMBER() OVER ())::bigint AS evidence_id"
            )
            if column == "evidence_id"
            else f"stage.{column}"
            for column in columns
        )
    else:
        select_list = ", ".join(f"stage.{column}" for column in columns)
    return f"""
    INSERT INTO {db_schema}.{live_support_table} ({column_list})
    SELECT {select_list}
      FROM {db_schema}.{stage_support_table} AS stage;
    """


def _partial_support_patch_sql(
    db_schema: str,
    stage_classes: dict[type, type],
    *,
    old_entity_table: str,
    affected_group_table: str,
    build_network_bridge: bool,
    replacement_stage_table: str | None = None,
) -> list[tuple[str, str]]:
    statements: list[tuple[str, str]] = []
    for model in _support_patch_models(build_network_bridge=build_network_bridge):
        stage_cls = stage_classes[model]
        label = model.__main_table__.replace("entity_address_", "")
        statements.append(
            (
                f"delete affected {label}",
                _delete_affected_group_support_sql(
                    db_schema,
                    model.__main_table__,
                    old_entity_table,
                    affected_group_table,
                    replacement_stage_table=replacement_stage_table,
                ),
            )
        )
        statements.append(
            (
                f"insert affected {label}",
                _insert_stage_support_into_live_sql(
                    db_schema,
                    model,
                    model.__main_table__,
                    stage_cls.__tablename__,
                ),
            )
        )
    return statements


_EVIDENCE_FROM_RAW_SQL = """
    INSERT INTO {db_schema}.{evidence_stage_table} (
        evidence_id,
        location_key,
        address_key,
        premise_key,
        archive_identity_version,
        entity_type,
        entity_id,
        npi,
        tin,
        source_id,
        source_record_key,
        source_run_id,
        source_snapshot_id,
        node_id,
        plan_id,
        network_id,
        ptg_plan_id,
        ptg_source_key,
        ptg_snapshot_id,
        market_type,
        address_role_id,
        location_confidence_id,
        address_precision,
        observed_at,
        last_seen_at,
        retired_at
    )
    SELECT
        ROW_NUMBER() OVER ()::bigint AS evidence_id,
        location_key,
        address_key,
        premise_key,
        archive_identity_version,
        entity_type,
        entity_id,
        npi,
        NULL::varchar AS tin,
        source_id,
        source_record_id AS source_record_key,
        {source_run_literal}::varchar AS source_run_id,
        CASE WHEN address_source = 'ptg' THEN NULLIF(split_part(source_record_id, ':', 3), '') ELSE NULL END::varchar
            AS source_snapshot_id,
        {node_literal}::varchar AS node_id,
        NULL::varchar AS plan_id,
        NULL::varchar AS network_id,
        CASE WHEN CARDINALITY(ptg_plan_array) = 1 THEN ptg_plan_array[1] ELSE NULL END::varchar AS ptg_plan_id,
        CASE WHEN address_source = 'ptg' THEN NULLIF(split_part(source_record_id, ':', 2), '') ELSE NULL END::varchar
            AS ptg_source_key,
        CASE WHEN address_source = 'ptg' THEN NULLIF(split_part(source_record_id, ':', 3), '') ELSE NULL END::varchar
            AS ptg_snapshot_id,
        NULL::varchar AS market_type,
        address_role_id,
        location_confidence_id,
        address_precision,
        updated_at::timestamptz AS observed_at,
        updated_at::timestamptz AS last_seen_at,
        NULL::timestamptz AS retired_at
      FROM {db_schema}.{raw_table}
     WHERE location_key IS NOT NULL;
"""


def _evidence_from_raw_sql(
    db_schema: str,
    evidence_stage_table: str,
    raw_table: str,
    *,
    source_run_id: str,
    node_id: str | None,
) -> str:
    """Build provenance-evidence SQL from normalized raw locations."""
    return _EVIDENCE_FROM_RAW_SQL.format(
        db_schema=db_schema,
        evidence_stage_table=evidence_stage_table,
        raw_table=raw_table,
        source_run_literal=_sql_literal(source_run_id),
        node_literal=_sql_literal(node_id),
    )


_EVIDENCE_FROM_STAGE_SQL = """
    INSERT INTO {db_schema}.{evidence_stage_table} (
        evidence_id,
        location_key,
        address_key,
        premise_key,
        archive_identity_version,
        entity_type,
        entity_id,
        npi,
        tin,
        source_id,
        source_record_key,
        source_run_id,
        source_snapshot_id,
        node_id,
        plan_id,
        network_id,
        ptg_plan_id,
        ptg_source_key,
        ptg_snapshot_id,
        market_type,
        address_role_id,
        location_confidence_id,
        address_precision,
        observed_at,
        last_seen_at,
        retired_at
    )
    SELECT
        ROW_NUMBER() OVER ()::bigint AS evidence_id,
        t.location_key,
        t.address_key,
        t.premise_key,
        t.archive_identity_version,
        t.entity_type,
        t.entity_id,
        t.npi,
        NULL::varchar AS tin,
        0::smallint AS source_id,
        t.location_key AS source_record_key,
        {source_run_literal}::varchar AS source_run_id,
        NULL::varchar AS source_snapshot_id,
        {node_literal}::varchar AS node_id,
        NULL::varchar AS plan_id,
        NULL::varchar AS network_id,
        CASE WHEN CARDINALITY(t.ptg_plan_array) = 1 THEN t.ptg_plan_array[1] ELSE NULL END::varchar AS ptg_plan_id,
        CASE WHEN CARDINALITY(t.ptg_source_array) = 1 THEN t.ptg_source_array[1] ELSE NULL END::varchar AS ptg_source_key,
        NULL::varchar AS ptg_snapshot_id,
        NULL::varchar AS market_type,
        NULL::smallint AS address_role_id,
        t.location_confidence_id,
        t.address_precision,
        t.updated_at::timestamptz AS observed_at,
        t.last_seen_at::timestamptz AS last_seen_at,
        NULL::timestamptz AS retired_at
      FROM {db_schema}.{stage_table} AS t
     WHERE t.location_key IS NOT NULL
       {affected_filter};
"""


def _evidence_from_stage_sql(
    db_schema: str,
    evidence_stage_table: str,
    stage_table: str,
    *,
    source_run_id: str,
    node_id: str | None,
    affected_group_table: str | None = None,
) -> str:
    """Build provenance-evidence SQL from unified staged locations."""
    affected_filter = _affected_stage_row_filter_sql(db_schema, affected_group_table)
    return _EVIDENCE_FROM_STAGE_SQL.format(
        db_schema=db_schema,
        evidence_stage_table=evidence_stage_table,
        stage_table=stage_table,
        source_run_literal=_sql_literal(source_run_id),
        node_literal=_sql_literal(node_id),
        affected_filter=affected_filter,
    )


def _plan_bridge_sql(
    db_schema: str,
    plan_stage_table: str,
    stage_table: str,
    *,
    affected_group_table: str | None = None,
) -> str:
    affected_filter = _affected_stage_row_filter_sql(db_schema, affected_group_table)
    return f"""
    INSERT INTO {db_schema}.{plan_stage_table} (location_key, entity_type, entity_id, plan_id, market_type)
    SELECT DISTINCT
        t.location_key,
        t.entity_type,
        t.entity_id,
        plan_id.value AS plan_id,
        NULL::varchar AS market_type
      FROM {db_schema}.{stage_table} AS t
      JOIN LATERAL unnest(COALESCE(t.aca_plan_array, ARRAY[]::varchar[])) AS plan_id(value) ON TRUE
     WHERE t.location_key IS NOT NULL
       {affected_filter}
       AND NULLIF(plan_id.value, '') IS NOT NULL;
    """


def _network_bridge_sql(
    db_schema: str,
    network_stage_table: str,
    stage_table: str,
    *,
    affected_group_table: str | None = None,
) -> str:
    affected_filter = _affected_stage_row_filter_sql(db_schema, affected_group_table)
    return f"""
    INSERT INTO {db_schema}.{network_stage_table} (location_key, entity_type, entity_id, network_id)
    SELECT DISTINCT location_key, entity_type, entity_id, network_id
      FROM (
        SELECT
            t.location_key,
            t.entity_type,
            t.entity_id,
            legacy_network.value::text AS network_id
          FROM {db_schema}.{stage_table} AS t
          JOIN LATERAL unnest(COALESCE(t.plans_network_array, ARRAY[]::int[])) AS legacy_network(value) ON TRUE
         WHERE t.location_key IS NOT NULL
           {affected_filter}
           AND legacy_network.value <> 0
        UNION ALL
        SELECT
            t.location_key,
            t.entity_type,
            t.entity_id,
            aca_network.value AS network_id
          FROM {db_schema}.{stage_table} AS t
          JOIN LATERAL unnest(COALESCE(t.aca_network_array, ARRAY[]::varchar[])) AS aca_network(value) ON TRUE
         WHERE t.location_key IS NOT NULL
           {affected_filter}
           AND NULLIF(aca_network.value, '') IS NOT NULL
      ) AS bridge_rows;
    """


def _procedure_bridge_sql(
    db_schema: str,
    procedure_stage_table: str,
    stage_table: str,
    *,
    affected_group_table: str | None = None,
    bridge_shards: int = 1,
    bridge_shard: int | None = None,
) -> str:
    affected_filter = _affected_stage_row_filter_sql(db_schema, affected_group_table)
    shard_filter = _location_key_shard_filter_sql(
        "t.location_key",
        shards=bridge_shards,
        shard=bridge_shard,
    )
    return f"""
    INSERT INTO {db_schema}.{procedure_stage_table} (location_key, npi, code_system, code)
    SELECT DISTINCT
        t.location_key,
        t.npi,
        'HP_PROCEDURE_CODE'::varchar AS code_system,
        procedure_code.value::text AS code
      FROM {db_schema}.{stage_table} AS t
      JOIN LATERAL unnest(COALESCE(t.procedures_array, ARRAY[]::int[])) AS procedure_code(value) ON TRUE
     WHERE t.location_key IS NOT NULL
       AND t.npi IS NOT NULL
       {affected_filter}
       {shard_filter}
       AND procedure_code.value <> 0;
    """


def _medication_bridge_sql(
    db_schema: str,
    medication_stage_table: str,
    stage_table: str,
    *,
    affected_group_table: str | None = None,
    bridge_shards: int = 1,
    bridge_shard: int | None = None,
) -> str:
    affected_filter = _affected_stage_row_filter_sql(db_schema, affected_group_table)
    shard_filter = _location_key_shard_filter_sql(
        "t.location_key",
        shards=bridge_shards,
        shard=bridge_shard,
    )
    return f"""
    INSERT INTO {db_schema}.{medication_stage_table} (location_key, npi, code_system, code)
    SELECT DISTINCT
        t.location_key,
        t.npi,
        'HP_RX_CODE'::varchar AS code_system,
        medication_code.value::text AS code
      FROM {db_schema}.{stage_table} AS t
      JOIN LATERAL unnest(COALESCE(t.medications_array, ARRAY[]::int[])) AS medication_code(value) ON TRUE
     WHERE t.location_key IS NOT NULL
       AND t.npi IS NOT NULL
       {affected_filter}
       {shard_filter}
       AND medication_code.value <> 0;
    """


def _facility_anchor_npi_candidate_sql(
    db_schema: str,
    candidate_stage_table: str,
    stage_table: str,
    *,
    source_run_id: str,
    candidate_options_by_name: Mapping[str, bool] | None = None,
    candidate_shards: int = 1,
    candidate_shard: int | None = None,
) -> str:
    """Build ranked facility-to-NPI candidate materialization SQL."""
    candidate_options_by_name = dict(candidate_options_by_name or {})
    include_hospital_enrollment = candidate_options_by_name.get(
        "include_hospital_enrollment", False
    )
    include_fqhc_enrollment = candidate_options_by_name.get(
        "include_fqhc_enrollment", False
    )
    include_npi_address_key = candidate_options_by_name.get(
        "include_npi_address_key", False
    )
    include_npi_registry = candidate_options_by_name.get(
        "include_npi_registry", False
    )
    include_npi_taxonomy = candidate_options_by_name.get(
        "include_npi_taxonomy", False
    )
    include_nucc_taxonomy = candidate_options_by_name.get(
        "include_nucc_taxonomy", False
    )
    include_npi_other_identifier = candidate_options_by_name.get(
        "include_npi_other_identifier", False
    )
    include_provider_additional_npi = candidate_options_by_name.get(
        "include_provider_additional_npi", False
    )
    include_facility_anchor = candidate_options_by_name.get(
        "include_facility_anchor", False
    )
    candidate_limit = _env_int("HLTHPRT_FACILITY_ANCHOR_NPI_CANDIDATE_LIMIT", 25, minimum=1)
    shard_filter = _location_key_shard_filter_sql(
        "t.location_key",
        shards=candidate_shards,
        shard=candidate_shard,
    )

    def norm_text_sql(expr: str) -> str:
        """Normalize text for generated SQL comparisons."""
        return f"regexp_replace(LOWER(COALESCE({expr}, '')), '[^a-z0-9]', '', 'g')"

    def zip5_sql(expr: str) -> str:
        """Normalize a postal-code expression to ZIP5."""
        return f"LEFT(COALESCE({expr}, ''), 5)"

    def phone_sql(expr: str) -> str:
        """Normalize a phone expression to digits."""
        return f"regexp_replace(COALESCE({expr}, ''), '[^0-9]', '', 'g')"

    def ccn_key_sql(expr: str) -> str:
        """Normalize a CCN expression for deterministic matching."""
        return f"regexp_replace(UPPER(COALESCE({expr}, '')), '[^A-Z0-9]', '', 'g')"

    fa_parent_name = "target.health_center_name"
    fa_parent_address = "target.health_center_organization_address_line1"
    fa_parent_state = "target.health_center_organization_state"
    fa_parent_zip = "target.health_center_organization_zip_code"
    target_ccn = "COALESCE(NULLIF(target.medicare_ccn, ''), target.facility_anchor_id)"
    hospital_taxonomy_codes_sql = _hospital_facility_taxonomy_codes_sql("                ")

    candidate_columns = """
            candidate_id,
            location_key,
            address_key,
            facility_anchor_id,
            facility_type,
            entity_name,
            first_line,
            city_name,
            state_name,
            postal_code,
            telephone_number,
            candidate_npi,
            candidate_method,
            candidate_priority,
            candidate_rank,
            candidate_count,
            candidate_status,
            review_status,
            match_confidence,
            evidence,
            source_run_id,
            updated_at
    """
    empty_candidates_sql = f"""
        SELECT
            NULL::varchar AS location_key,
            NULL::uuid AS address_key,
            NULL::varchar AS facility_anchor_id,
            NULL::varchar AS facility_type,
            NULL::varchar AS entity_name,
            NULL::varchar AS first_line,
            NULL::varchar AS city_name,
            NULL::varchar AS state_name,
            NULL::varchar AS postal_code,
            NULL::varchar AS telephone_number,
            NULL::bigint AS candidate_npi,
            NULL::varchar AS candidate_method,
            NULL::int AS candidate_priority,
            NULL::double precision AS match_confidence,
            NULL::jsonb AS evidence
         WHERE FALSE
    """

    fragments: list[str] = []

    if include_hospital_enrollment:
        fragments.append(
            f"""
        SELECT
            target.location_key,
            target.address_key,
            target.facility_anchor_id,
            target.facility_type,
            target.entity_name,
            target.first_line,
            target.city_name,
            target.state_name,
            target.postal_code,
            target.telephone_number,
            h.npi::bigint AS candidate_npi,
            'hospital_ccn_match'::varchar AS candidate_method,
            10::int AS candidate_priority,
            0.99::double precision AS match_confidence,
            jsonb_build_object('source', 'provider_enrollment_hospital', 'matched_on', 'ccn') AS evidence
          FROM target
          JOIN {db_schema}.provider_enrollment_hospital AS h
            ON h.npi IS NOT NULL
           AND (
                {ccn_key_sql("h.ccn")} = {ccn_key_sql(target_ccn)}
                OR {ccn_key_sql("h.cah_or_hospital_ccn")} = {ccn_key_sql(target_ccn)}
           )
         WHERE target.facility_type = 'Hospital'
            """
        )

    if include_npi_other_identifier:
        fragments.append(
            f"""
        SELECT
            target.location_key,
            target.address_key,
            target.facility_anchor_id,
            target.facility_type,
            target.entity_name,
            target.first_line,
            target.city_name,
            target.state_name,
            target.postal_code,
            target.telephone_number,
            oi.npi::bigint AS candidate_npi,
            CASE
                WHEN target.facility_type = 'Hospital' THEN 'hospital_nppes_other_identifier'
                ELSE 'fqhc_nppes_other_identifier'
            END::varchar AS candidate_method,
            CASE WHEN target.facility_type = 'Hospital' THEN 12 ELSE 15 END::int AS candidate_priority,
            0.96::double precision AS match_confidence,
            jsonb_build_object(
                'source', 'npi_other_identifier',
                'other_provider_identifier', oi.other_provider_identifier,
                'matched_on', 'facility_id_or_ccn'
            ) AS evidence
          FROM target
          JOIN {db_schema}.npi_other_identifier AS oi
            ON oi.npi IS NOT NULL
           AND regexp_replace(COALESCE(oi.other_provider_identifier, ''), '[^A-Za-z0-9]', '', 'g')
               = regexp_replace(
                    COALESCE(NULLIF(target.medicare_ccn, ''), target.facility_anchor_id, ''),
                    '[^A-Za-z0-9]',
                    '',
                    'g'
                 )
           AND (
                UPPER(COALESCE(oi.other_provider_identifier_state, '')) = UPPER(COALESCE(target.state_name, ''))
                OR COALESCE(oi.other_provider_identifier_state, '') = ''
           )
            """
        )

    if include_provider_additional_npi:
        fragments.extend(
            [
                f"""
        SELECT
            target.location_key,
            target.address_key,
            target.facility_anchor_id,
            target.facility_type,
            target.entity_name,
            target.first_line,
            target.city_name,
            target.state_name,
            target.postal_code,
            target.telephone_number,
            a.additional_npi::bigint AS candidate_npi,
            'hospital_pecos_additional_npi'::varchar AS candidate_method,
            18::int AS candidate_priority,
            0.975::double precision AS match_confidence,
            jsonb_build_object(
                'source', 'provider_enrollment_ffs_additional_npi',
                'matched_on', 'hospital_ccn_enrollment_additional_npi',
                'enrollment_id', h.enrollment_id
            ) AS evidence
          FROM target
          JOIN {db_schema}.provider_enrollment_hospital AS h
            ON h.enrollment_id IS NOT NULL
           AND (
                {ccn_key_sql("h.ccn")} = {ccn_key_sql(target_ccn)}
                OR {ccn_key_sql("h.cah_or_hospital_ccn")} = {ccn_key_sql(target_ccn)}
           )
          JOIN {db_schema}.provider_enrollment_ffs_additional_npi AS a
            ON a.enrollment_id = h.enrollment_id
           AND a.additional_npi IS NOT NULL
         WHERE target.facility_type = 'Hospital'
                """,
                f"""
        SELECT
            target.location_key,
            target.address_key,
            target.facility_anchor_id,
            target.facility_type,
            target.entity_name,
            target.first_line,
            target.city_name,
            target.state_name,
            target.postal_code,
            target.telephone_number,
            a.additional_npi::bigint AS candidate_npi,
            'fqhc_pecos_additional_npi'::varchar AS candidate_method,
            28::int AS candidate_priority,
            0.97::double precision AS match_confidence,
            jsonb_build_object(
                'source', 'provider_enrollment_ffs_additional_npi',
                'matched_on', 'fqhc_ccn_enrollment_additional_npi',
                'enrollment_id', f.enrollment_id
            ) AS evidence
          FROM target
          JOIN {db_schema}.provider_enrollment_fqhc AS f
            ON f.enrollment_id IS NOT NULL
           AND {ccn_key_sql("f.ccn")} = {ccn_key_sql("target.medicare_ccn")}
          JOIN {db_schema}.provider_enrollment_ffs_additional_npi AS a
            ON a.enrollment_id = f.enrollment_id
           AND a.additional_npi IS NOT NULL
         WHERE target.facility_type = 'FQHC'
           AND COALESCE(target.medicare_ccn, '') <> ''
                """,
            ]
        )

    if include_fqhc_enrollment:
        fragments.extend(
            [
                f"""
        SELECT
            target.location_key,
            target.address_key,
            target.facility_anchor_id,
            target.facility_type,
            target.entity_name,
            target.first_line,
            target.city_name,
            target.state_name,
            target.postal_code,
            target.telephone_number,
            f.npi::bigint AS candidate_npi,
            'fqhc_ccn_match'::varchar AS candidate_method,
            20::int AS candidate_priority,
            0.965::double precision AS match_confidence,
            jsonb_build_object('source', 'provider_enrollment_fqhc', 'matched_on', 'ccn') AS evidence
          FROM target
          JOIN {db_schema}.provider_enrollment_fqhc AS f
            ON f.npi IS NOT NULL
           AND {ccn_key_sql("f.ccn")} = {ccn_key_sql("target.medicare_ccn")}
         WHERE target.facility_type = 'FQHC'
           AND COALESCE(target.medicare_ccn, '') <> ''
                """,
                f"""
        SELECT
            target.location_key,
            target.address_key,
            target.facility_anchor_id,
            target.facility_type,
            target.entity_name,
            target.first_line,
            target.city_name,
            target.state_name,
            target.postal_code,
            target.telephone_number,
            f.npi::bigint AS candidate_npi,
            'fqhc_enrollment_exact_match'::varchar AS candidate_method,
            30::int AS candidate_priority,
            0.97::double precision AS match_confidence,
            jsonb_build_object('source', 'provider_enrollment_fqhc', 'matched_on', 'site_name_address') AS evidence
          FROM target
          JOIN {db_schema}.provider_enrollment_fqhc AS f
            ON f.npi IS NOT NULL
           AND {zip5_sql("f.zip_code")} = {zip5_sql("target.postal_code")}
           AND UPPER(COALESCE(f.state, '')) = UPPER(COALESCE(target.state_name, ''))
           AND {norm_text_sql("f.address_line_1")} = {norm_text_sql("target.first_line")}
           AND (
                {norm_text_sql("f.organization_name")} = {norm_text_sql("target.entity_name")}
                OR {norm_text_sql("f.doing_business_as_name")} = {norm_text_sql("target.entity_name")}
           )
         WHERE target.facility_type = 'FQHC'
                """,
                f"""
        SELECT
            target.location_key,
            target.address_key,
            target.facility_anchor_id,
            target.facility_type,
            target.entity_name,
            target.first_line,
            target.city_name,
            target.state_name,
            target.postal_code,
            target.telephone_number,
            f.npi::bigint AS candidate_npi,
            'fqhc_parent_enrollment_exact_address'::varchar AS candidate_method,
            35::int AS candidate_priority,
            0.94::double precision AS match_confidence,
            jsonb_build_object('source', 'provider_enrollment_fqhc', 'matched_on', 'hrsa_parent_name_address') AS evidence
          FROM target
          JOIN {db_schema}.provider_enrollment_fqhc AS f
            ON f.npi IS NOT NULL
           AND {zip5_sql("f.zip_code")} = {zip5_sql(fa_parent_zip)}
           AND UPPER(COALESCE(f.state, '')) = UPPER(COALESCE({fa_parent_state}, ''))
           AND {norm_text_sql("f.address_line_1")} = {norm_text_sql(fa_parent_address)}
           AND (
                {norm_text_sql("f.organization_name")} = {norm_text_sql(fa_parent_name)}
                OR {norm_text_sql("f.doing_business_as_name")} = {norm_text_sql(fa_parent_name)}
           )
         WHERE target.facility_type = 'FQHC'
           AND {norm_text_sql(fa_parent_name)} <> ''
           AND {norm_text_sql(fa_parent_address)} <> ''
                """,
                f"""
        SELECT
            target.location_key,
            target.address_key,
            target.facility_anchor_id,
            target.facility_type,
            target.entity_name,
            target.first_line,
            target.city_name,
            target.state_name,
            target.postal_code,
            target.telephone_number,
            f.npi::bigint AS candidate_npi,
            'fqhc_enrollment_phone_zip'::varchar AS candidate_method,
            40::int AS candidate_priority,
            0.93::double precision AS match_confidence,
            jsonb_build_object('source', 'provider_enrollment_fqhc', 'matched_on', 'phone_zip') AS evidence
          FROM target
          JOIN {db_schema}.provider_enrollment_fqhc AS f
            ON f.npi IS NOT NULL
           AND {zip5_sql("f.zip_code")} = {zip5_sql("target.postal_code")}
           AND {phone_sql("f.telephone_number")} = {phone_sql("target.telephone_number")}
           AND LENGTH({phone_sql("f.telephone_number")}) = 10
           AND LENGTH({phone_sql("target.telephone_number")}) = 10
         WHERE target.facility_type = 'FQHC'
                """,
                f"""
        SELECT
            target.location_key,
            target.address_key,
            target.facility_anchor_id,
            target.facility_type,
            target.entity_name,
            target.first_line,
            target.city_name,
            target.state_name,
            target.postal_code,
            target.telephone_number,
            f.npi::bigint AS candidate_npi,
            'fqhc_parent_enrollment_address'::varchar AS candidate_method,
            50::int AS candidate_priority,
            0.90::double precision AS match_confidence,
            jsonb_build_object('source', 'provider_enrollment_fqhc', 'matched_on', 'hrsa_parent_address') AS evidence
          FROM target
          JOIN {db_schema}.provider_enrollment_fqhc AS f
            ON f.npi IS NOT NULL
           AND {zip5_sql("f.zip_code")} = {zip5_sql(fa_parent_zip)}
           AND UPPER(COALESCE(f.state, '')) = UPPER(COALESCE({fa_parent_state}, ''))
           AND {norm_text_sql("f.address_line_1")} = {norm_text_sql(fa_parent_address)}
         WHERE target.facility_type = 'FQHC'
           AND {norm_text_sql(fa_parent_address)} <> ''
                """,
            ]
        )

    if include_npi_address_key and include_npi_taxonomy:
        fragments.append(
            f"""
        SELECT
            target.location_key,
            target.address_key,
            target.facility_anchor_id,
            target.facility_type,
            target.entity_name,
            target.first_line,
            target.city_name,
            target.state_name,
            target.postal_code,
            target.telephone_number,
            a.npi::bigint AS candidate_npi,
            CASE
                WHEN target.facility_type = 'Hospital' THEN 'hospital_nppes_address_key'
                ELSE 'fqhc_nppes_address_key'
            END::varchar AS candidate_method,
            CASE WHEN target.facility_type = 'Hospital' THEN 55 ELSE 75 END::int AS candidate_priority,
            CASE WHEN target.facility_type = 'Hospital' THEN 0.93 ELSE 0.925 END::double precision AS match_confidence,
            jsonb_build_object('source', 'nppes', 'matched_on', 'address_key', 'address_type', a.type) AS evidence
          FROM target
          JOIN {db_schema}.npi_address AS a
            ON a.address_key = target.address_key
           AND a.npi IS NOT NULL
          JOIN {db_schema}.npi AS n
            ON n.npi = a.npi
           AND n.entity_type_code = 2
          JOIN {db_schema}.npi_taxonomy AS nt
            ON nt.npi = a.npi
           AND nt.healthcare_provider_primary_taxonomy_switch = 'Y'
           AND (
                (
                    target.facility_type = 'FQHC'
                    AND nt.healthcare_provider_taxonomy_code = '261QF0400X'
                )
                OR (
                    target.facility_type = 'Hospital'
                    AND nt.healthcare_provider_taxonomy_code IN (
                {hospital_taxonomy_codes_sql}
                    )
                )
           )
         WHERE target.address_key IS NOT NULL
            """
        )
        fragments.extend(
            [
                f"""
        SELECT
            target.location_key,
            target.address_key,
            target.facility_anchor_id,
            target.facility_type,
            target.entity_name,
            target.first_line,
            target.city_name,
            target.state_name,
            target.postal_code,
            target.telephone_number,
            a.npi::bigint AS candidate_npi,
            'fqhc_nppes_dba_phone_zip'::varchar AS candidate_method,
            68::int AS candidate_priority,
            0.91::double precision AS match_confidence,
            jsonb_build_object(
                'source', 'nppes',
                'matched_on', 'dba_phone_zip',
                'address_type', a.type,
                'taxonomy_code', nt.healthcare_provider_taxonomy_code,
                'do_business_as_text', n.do_business_as_text
            ) AS evidence
          FROM target
          JOIN {db_schema}.npi AS n
            ON n.entity_type_code = 2
           AND {norm_text_sql("n.do_business_as_text")} = {norm_text_sql("target.entity_name")}
           AND {norm_text_sql("n.do_business_as_text")} <> ''
          JOIN {db_schema}.npi_address AS a
            ON a.npi = n.npi
           AND a.type = 'primary'
           AND a.npi IS NOT NULL
           AND {zip5_sql("a.postal_code")} = {zip5_sql("target.postal_code")}
           AND {phone_sql("a.telephone_number")} = {phone_sql("target.telephone_number")}
           AND LENGTH({phone_sql("a.telephone_number")}) = 10
           AND LENGTH({phone_sql("target.telephone_number")}) = 10
          JOIN {db_schema}.npi_taxonomy AS nt
            ON nt.npi = n.npi
           AND nt.healthcare_provider_primary_taxonomy_switch = 'Y'
           AND nt.healthcare_provider_taxonomy_code = '261QF0400X'
         WHERE target.facility_type = 'FQHC'
                """,
                f"""
        SELECT
            target.location_key,
            target.address_key,
            target.facility_anchor_id,
            target.facility_type,
            target.entity_name,
            target.first_line,
            target.city_name,
            target.state_name,
            target.postal_code,
            target.telephone_number,
            a.npi::bigint AS candidate_npi,
            'hospital_nppes_dba_zip_state'::varchar AS candidate_method,
            82::int AS candidate_priority,
            0.84::double precision AS match_confidence,
            jsonb_build_object(
                'source', 'nppes',
                'matched_on', 'hospital_dba_zip_state',
                'address_type', a.type,
                'taxonomy_code', nt.healthcare_provider_taxonomy_code,
                'do_business_as_text', n.do_business_as_text
            ) AS evidence
          FROM target
          JOIN {db_schema}.npi AS n
            ON n.entity_type_code = 2
           AND {norm_text_sql("n.do_business_as_text")} = {norm_text_sql("target.entity_name")}
           AND {norm_text_sql("n.do_business_as_text")} <> ''
          JOIN {db_schema}.npi_address AS a
            ON a.npi = n.npi
           AND a.npi IS NOT NULL
           AND {zip5_sql("a.postal_code")} = {zip5_sql("target.postal_code")}
           AND UPPER(COALESCE(a.state_name, '')) = UPPER(COALESCE(target.state_name, ''))
          JOIN {db_schema}.npi_taxonomy AS nt
            ON nt.npi = n.npi
           AND nt.healthcare_provider_primary_taxonomy_switch = 'Y'
           AND nt.healthcare_provider_taxonomy_code IN (
                {hospital_taxonomy_codes_sql}
           )
         WHERE target.facility_type = 'Hospital'
                """,
            ]
        )
        if include_nucc_taxonomy:
            fragments.append(
                f"""
        SELECT
            target.location_key,
            target.address_key,
            target.facility_anchor_id,
            target.facility_type,
            target.entity_name,
            target.first_line,
            target.city_name,
            target.state_name,
            target.postal_code,
            target.telephone_number,
            a.npi::bigint AS candidate_npi,
            'fqhc_clinic_center_address_key'::varchar AS candidate_method,
            78::int AS candidate_priority,
            0.89::double precision AS match_confidence,
            jsonb_build_object(
                'source', 'nppes',
                'matched_on', 'address_key_clinic_center',
                'address_type', a.type,
                'taxonomy_code', nt.healthcare_provider_taxonomy_code
            ) AS evidence
          FROM target
          JOIN {db_schema}.npi_address AS a
            ON a.address_key = target.address_key
           AND a.npi IS NOT NULL
          JOIN {db_schema}.npi AS n
            ON n.npi = a.npi
           AND n.entity_type_code = 2
          JOIN {db_schema}.npi_taxonomy AS nt
            ON nt.npi = a.npi
           AND nt.healthcare_provider_primary_taxonomy_switch = 'Y'
           AND nt.healthcare_provider_taxonomy_code <> '261QF0400X'
          JOIN {db_schema}.nucc_taxonomy AS nu
            ON nu.code = nt.healthcare_provider_taxonomy_code
           AND COALESCE(nu.classification, '') = 'Clinic/Center'
         WHERE target.address_key IS NOT NULL
           AND target.facility_type = 'FQHC'
            """
            )
            fragments.append(
                f"""
        SELECT
            target.location_key,
            target.address_key,
            target.facility_anchor_id,
            target.facility_type,
            target.entity_name,
            target.first_line,
            target.city_name,
            target.state_name,
            target.postal_code,
            target.telephone_number,
            a.npi::bigint AS candidate_npi,
            'fqhc_clinic_center_phone_zip'::varchar AS candidate_method,
            88::int AS candidate_priority,
            0.86::double precision AS match_confidence,
            jsonb_build_object(
                'source', 'nppes',
                'matched_on', 'phone_zip_clinic_center',
                'address_type', a.type,
                'taxonomy_code', nt.healthcare_provider_taxonomy_code
            ) AS evidence
          FROM target
          JOIN {db_schema}.npi_address AS a
            ON a.type = 'primary'
           AND a.npi IS NOT NULL
           AND {zip5_sql("a.postal_code")} = target.postal_code
           AND {phone_sql("a.telephone_number")} = {phone_sql("target.telephone_number")}
           AND LENGTH({phone_sql("a.telephone_number")}) = 10
           AND LENGTH({phone_sql("target.telephone_number")}) = 10
          JOIN {db_schema}.npi AS n
            ON n.npi = a.npi
           AND n.entity_type_code = 2
          JOIN {db_schema}.npi_taxonomy AS nt
            ON nt.npi = a.npi
           AND nt.healthcare_provider_primary_taxonomy_switch = 'Y'
           AND nt.healthcare_provider_taxonomy_code <> '261QF0400X'
          JOIN {db_schema}.nucc_taxonomy AS nu
            ON nu.code = nt.healthcare_provider_taxonomy_code
           AND COALESCE(nu.classification, '') = 'Clinic/Center'
         WHERE target.facility_type = 'FQHC'
            """
            )

    if include_npi_registry and include_npi_taxonomy:
        fragments.extend(
            [
                f"""
        SELECT
            target.location_key,
            target.address_key,
            target.facility_anchor_id,
            target.facility_type,
            target.entity_name,
            target.first_line,
            target.city_name,
            target.state_name,
            target.postal_code,
            target.telephone_number,
            n.npi::bigint AS candidate_npi,
            'hospital_nppes_name_address'::varchar AS candidate_method,
            60::int AS candidate_priority,
            0.94::double precision AS match_confidence,
            jsonb_build_object('source', 'nppes', 'matched_on', 'hospital_name_address') AS evidence
          FROM target
          JOIN {db_schema}.npi AS n
            ON n.entity_type_code = 2
           AND (
                {norm_text_sql("n.provider_organization_name")} = {norm_text_sql("target.entity_name")}
                OR {norm_text_sql("n.provider_other_organization_name")} = {norm_text_sql("target.entity_name")}
                OR {norm_text_sql("n.do_business_as_text")} = {norm_text_sql("target.entity_name")}
           )
          JOIN {db_schema}.npi_address AS a
            ON a.npi = n.npi
           AND {zip5_sql("a.postal_code")} = {zip5_sql("target.postal_code")}
           AND UPPER(COALESCE(a.state_name, '')) = UPPER(COALESCE(target.state_name, ''))
           AND {norm_text_sql("a.first_line")} = {norm_text_sql("target.first_line")}
          JOIN {db_schema}.npi_taxonomy AS nt
            ON nt.npi = n.npi
           AND nt.healthcare_provider_primary_taxonomy_switch = 'Y'
           AND nt.healthcare_provider_taxonomy_code IN (
                {hospital_taxonomy_codes_sql}
           )
         WHERE target.facility_type = 'Hospital'
                """,
                f"""
        SELECT
            target.location_key,
            target.address_key,
            target.facility_anchor_id,
            target.facility_type,
            target.entity_name,
            target.first_line,
            target.city_name,
            target.state_name,
            target.postal_code,
            target.telephone_number,
            n.npi::bigint AS candidate_npi,
            CASE WHEN a.type = 'primary'
                THEN 'fqhc_parent_nppes_exact_address_primary'
                ELSE 'fqhc_parent_nppes_exact_address'
            END::varchar AS candidate_method,
            CASE WHEN a.type = 'primary' THEN 70 ELSE 90 END::int AS candidate_priority,
            CASE WHEN a.type = 'primary' THEN 0.885 ELSE 0.865 END::double precision AS match_confidence,
            jsonb_build_object('source', 'nppes', 'matched_on', 'hrsa_parent_name_address', 'address_type', a.type) AS evidence
          FROM target
          JOIN {db_schema}.npi AS n
            ON n.entity_type_code = 2
           AND (
                {norm_text_sql("n.provider_organization_name")} = {norm_text_sql(fa_parent_name)}
                OR {norm_text_sql("n.provider_other_organization_name")} = {norm_text_sql(fa_parent_name)}
                OR {norm_text_sql("n.do_business_as_text")} = {norm_text_sql(fa_parent_name)}
           )
          JOIN {db_schema}.npi_address AS a
            ON a.npi = n.npi
           AND {zip5_sql("a.postal_code")} = {zip5_sql(fa_parent_zip)}
           AND UPPER(COALESCE(a.state_name, '')) = UPPER(COALESCE({fa_parent_state}, ''))
           AND {norm_text_sql("a.first_line")} = {norm_text_sql(fa_parent_address)}
          JOIN {db_schema}.npi_taxonomy AS nt
            ON nt.npi = n.npi
           AND nt.healthcare_provider_primary_taxonomy_switch = 'Y'
           AND nt.healthcare_provider_taxonomy_code = '261QF0400X'
         WHERE target.facility_type = 'FQHC'
           AND {norm_text_sql(fa_parent_name)} <> ''
           AND {norm_text_sql(fa_parent_address)} <> ''
                """,
                f"""
        SELECT
            target.location_key,
            target.address_key,
            target.facility_anchor_id,
            target.facility_type,
            target.entity_name,
            target.first_line,
            target.city_name,
            target.state_name,
            target.postal_code,
            target.telephone_number,
            a.npi::bigint AS candidate_npi,
            'npi_fqhc_exact_address'::varchar AS candidate_method,
            80::int AS candidate_priority,
            0.945::double precision AS match_confidence,
            jsonb_build_object('source', 'nppes', 'matched_on', 'site_address') AS evidence
          FROM target
          JOIN {db_schema}.npi_address AS a
            ON a.npi IS NOT NULL
           AND {zip5_sql("a.postal_code")} = {zip5_sql("target.postal_code")}
           AND UPPER(COALESCE(a.state_name, '')) = UPPER(COALESCE(target.state_name, ''))
           AND {norm_text_sql("a.first_line")} = {norm_text_sql("target.first_line")}
          JOIN {db_schema}.npi_taxonomy AS nt
            ON nt.npi = a.npi
           AND nt.healthcare_provider_primary_taxonomy_switch = 'Y'
           AND nt.healthcare_provider_taxonomy_code = '261QF0400X'
         WHERE target.facility_type = 'FQHC'
                """,
            ]
        )

    if include_facility_anchor:
        fragments.append(
            f"""
        SELECT
            target.location_key,
            target.address_key,
            target.facility_anchor_id,
            target.facility_type,
            target.entity_name,
            target.first_line,
            target.city_name,
            target.state_name,
            target.postal_code,
            target.telephone_number,
            source_fa.npi::bigint AS candidate_npi,
            'fqhc_sibling_source_npi'::varchar AS candidate_method,
            85::int AS candidate_priority,
            0.88::double precision AS match_confidence,
            jsonb_build_object('source', 'facility_anchor', 'matched_on', 'hrsa_parent_sibling') AS evidence
          FROM target
          JOIN {db_schema}.facility_anchor AS source_fa
            ON source_fa.npi IS NOT NULL
           AND source_fa.id <> target.facility_anchor_id
           AND source_fa.facility_type = 'FQHC'
           AND (
                (
                    COALESCE(target.health_center_number, '') <> ''
                    AND to_jsonb(source_fa)->>'health_center_number' = target.health_center_number
                )
                OR (
                    COALESCE(target.health_center_organization_id, '') <> ''
                    AND to_jsonb(source_fa)->>'health_center_organization_id' = target.health_center_organization_id
                )
           )
         WHERE target.facility_type = 'FQHC'
            """
        )

    candidate_sources_sql = "\n        UNION ALL\n".join(fragments) if fragments else empty_candidates_sql
    return f"""
    INSERT INTO {db_schema}.{candidate_stage_table} (
        candidate_id,
        location_key,
        address_key,
        facility_anchor_id,
        facility_type,
        entity_name,
        first_line,
        city_name,
        state_name,
        postal_code,
        telephone_number,
        candidate_npi,
        candidate_method,
        candidate_priority,
        candidate_rank,
        candidate_count,
        candidate_status,
        review_status,
        match_confidence,
        evidence,
        source_run_id,
        updated_at
    )
    WITH target AS (
        SELECT
            t.location_key::varchar AS location_key,
            t.address_key AS address_key,
            t.entity_id::varchar AS facility_anchor_id,
            COALESCE(t.entity_subtype, '')::varchar AS facility_type,
            t.entity_name::varchar AS entity_name,
            t.first_line::varchar AS first_line,
            t.city_name::varchar AS city_name,
            t.state_name::varchar AS state_name,
            LEFT(COALESCE(t.postal_code, ''), 5)::varchar AS postal_code,
            t.telephone_number::varchar AS telephone_number,
            fa.medicare_ccn::varchar AS medicare_ccn,
            to_jsonb(fa)->>'health_center_number' AS health_center_number,
            to_jsonb(fa)->>'health_center_organization_id' AS health_center_organization_id,
            to_jsonb(fa)->>'health_center_name' AS health_center_name,
            to_jsonb(fa)->>'health_center_organization_address_line1' AS health_center_organization_address_line1,
            to_jsonb(fa)->>'health_center_organization_state' AS health_center_organization_state,
            to_jsonb(fa)->>'health_center_organization_zip_code' AS health_center_organization_zip_code
          FROM {db_schema}.{stage_table} AS t
          JOIN {db_schema}.facility_anchor AS fa
            ON fa.id = t.entity_id
         WHERE t.location_key IS NOT NULL
           AND t.entity_type = 'facility_anchor'
           AND t.npi IS NULL
           AND t.inferred_npi IS NULL
           {shard_filter}
    ),
    candidate_sources AS (
        {candidate_sources_sql}
    ),
    dedup_candidates AS (
        SELECT
            location_key,
            address_key,
            facility_anchor_id,
            facility_type,
            entity_name,
            first_line,
            city_name,
            state_name,
            postal_code,
            telephone_number,
            candidate_npi,
            candidate_method,
            MIN(candidate_priority)::int AS candidate_priority,
            MAX(match_confidence)::double precision AS match_confidence,
            jsonb_build_object('matches', jsonb_agg(evidence ORDER BY candidate_priority, candidate_method)) AS evidence
          FROM candidate_sources
         WHERE candidate_npi IS NOT NULL
      GROUP BY
            location_key,
            address_key,
            facility_anchor_id,
            facility_type,
            entity_name,
            first_line,
            city_name,
            state_name,
            postal_code,
            telephone_number,
            candidate_npi,
            candidate_method
    ),
    candidate_counts AS (
        SELECT
            location_key,
            COUNT(DISTINCT candidate_npi)::int AS candidate_count
          FROM dedup_candidates
      GROUP BY location_key
    ),
    ranked_candidates AS (
        SELECT
            d.*,
            c.candidate_count,
            ROW_NUMBER() OVER (
                PARTITION BY d.location_key
                ORDER BY d.candidate_priority ASC, d.candidate_npi ASC, d.candidate_method ASC
            )::int AS candidate_rank
          FROM dedup_candidates AS d
          JOIN candidate_counts AS c
            ON c.location_key = d.location_key
    ),
    limited_candidates AS (
        SELECT
            md5(concat_ws('|', {_sql_literal(source_run_id)}, location_key, candidate_npi::text, candidate_method))::varchar AS candidate_id,
            location_key,
            address_key,
            facility_anchor_id,
            facility_type,
            entity_name,
            first_line,
            city_name,
            state_name,
            postal_code,
            telephone_number,
            candidate_npi,
            candidate_method,
            candidate_priority,
            candidate_rank,
            candidate_count,
            CASE WHEN candidate_count = 1 THEN 'single_candidate' ELSE 'conflict' END::varchar AS candidate_status,
            'needs_review'::varchar AS review_status,
            match_confidence,
            evidence,
            {_sql_literal(source_run_id)}::varchar AS source_run_id,
            NOW()::timestamp AS updated_at
          FROM ranked_candidates
         WHERE candidate_rank <= {candidate_limit}
    ),
    no_candidate AS (
        SELECT
            md5(concat_ws('|', {_sql_literal(source_run_id)}, target.location_key, 'no_candidate'))::varchar AS candidate_id,
            target.location_key,
            target.address_key,
            target.facility_anchor_id,
            target.facility_type,
            target.entity_name,
            target.first_line,
            target.city_name,
            target.state_name,
            target.postal_code,
            target.telephone_number,
            NULL::bigint AS candidate_npi,
            NULL::varchar AS candidate_method,
            NULL::int AS candidate_priority,
            1::int AS candidate_rank,
            0::int AS candidate_count,
            'no_candidate'::varchar AS candidate_status,
            'no_candidate'::varchar AS review_status,
            NULL::double precision AS match_confidence,
            jsonb_build_object('reason', 'no_candidate_after_inference') AS evidence,
            {_sql_literal(source_run_id)}::varchar AS source_run_id,
            NOW()::timestamp AS updated_at
          FROM target
         WHERE NOT EXISTS (
                SELECT 1
                  FROM dedup_candidates AS d
                 WHERE d.location_key = target.location_key
           )
    )
    SELECT {candidate_columns}
      FROM limited_candidates
    UNION ALL
    SELECT {candidate_columns}
      FROM no_candidate;
    """


def _support_stage_statements(
    db_schema: str,
    stage_table: str,
    stage_classes: dict[type, type],
    *,
    source_run_id: str,
    node_id: str | None,
    raw_table: str | None = None,
    build_network_bridge: bool = True,
    available: dict[str, bool] | None = None,
    affected_group_table: str | None = None,
    copy_unaffected_bridges: bool = True,
) -> list[_SupportStageStatement]:
    """Build the ordered support-table population plan."""
    available = available or {}
    stage_table_map = {
        model: stage_cls.__tablename__ for model, stage_cls in stage_classes.items()
    }
    partial_bridge_reuse = bool(affected_group_table)
    partial_support_patch = partial_bridge_reuse and not copy_unaffected_bridges
    should_build_code_bridges = _is_env_enabled(
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_BUILD_CODE_BRIDGES",
        DEFAULT_BUILD_CODE_BRIDGES,
    )
    should_build_facility_candidates = _is_env_enabled(
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_BUILD_FACILITY_CANDIDATES",
        DEFAULT_BUILD_FACILITY_CANDIDATES,
    )
    evidence_sql = (
        _evidence_from_raw_sql(
            db_schema,
            stage_table_map[EntityAddressEvidence],
            raw_table,
            source_run_id=source_run_id,
            node_id=node_id,
        )
        if raw_table
        else _evidence_from_stage_sql(
            db_schema,
            stage_table_map[EntityAddressEvidence],
            stage_table,
            source_run_id=source_run_id,
            node_id=node_id,
            affected_group_table=affected_group_table if partial_support_patch else None,
        )
    )
    statements = [
        _SupportStageStatement(
            "support tables",
            _truncate_support_stage_sql(db_schema, stage_table_map),
            parallel=False,
        ),
        _SupportStageStatement("evidence", evidence_sql),
    ]
    code_bridge_specs = [
        (
            EntityAddressProcedureBridge,
            "procedure bridge",
            ("location_key", "npi", "code_system", "code"),
            _procedure_bridge_sql,
            "HLTHPRT_ENTITY_ADDRESS_UNIFIED_PROCEDURE_BRIDGE_SHARDS",
            DEFAULT_PROCEDURE_BRIDGE_SHARDS,
        ),
        (
            EntityAddressMedicationBridge,
            "medication bridge",
            ("location_key", "npi", "code_system", "code"),
            _medication_bridge_sql,
            "HLTHPRT_ENTITY_ADDRESS_UNIFIED_MEDICATION_BRIDGE_SHARDS",
            DEFAULT_MEDICATION_BRIDGE_SHARDS,
        ),
    ]
    bridge_specs = [
        (
            EntityAddressPlanBridge,
            "plan bridge",
            ("location_key", "entity_type", "entity_id", "plan_id", "market_type"),
            _plan_bridge_sql,
            None,
            1,
        ),
    ]
    if should_build_code_bridges or partial_bridge_reuse:
        bridge_specs.extend(code_bridge_specs)
    if (
        should_build_facility_candidates
        and not partial_support_patch
        and FacilityAnchorNPICandidate in stage_table_map
        and available.get("facility_anchor", False)
        and available.get("facility_anchor.medicare_ccn", available.get("facility_anchor", False))
    ):
        include_nppes_candidates = _is_env_enabled(
            "HLTHPRT_FACILITY_ANCHOR_NPI_CANDIDATE_INCLUDE_NPPES",
            False,
        )
        include_other_identifier_candidates = _is_env_enabled(
            "HLTHPRT_FACILITY_ANCHOR_NPI_CANDIDATE_INCLUDE_OTHER_IDENTIFIER",
            False,
        )
        facility_candidate_shards = _env_int(
            "HLTHPRT_ENTITY_ADDRESS_UNIFIED_FACILITY_CANDIDATE_SHARDS",
            DEFAULT_FACILITY_CANDIDATE_SHARDS,
            minimum=1,
        )
        facility_candidate_options_by_name = dict(
            include_hospital_enrollment=available.get("provider_enrollment_hospital", False),
            include_fqhc_enrollment=available.get("provider_enrollment_fqhc", False),
            include_npi_address_key=(
                available.get("npi", False)
                and available.get("npi_address", False)
                and available.get("npi_address.address_key", available.get("npi_address", False))
                and available.get("npi_taxonomy", False)
            ),
            include_npi_registry=(
                include_nppes_candidates
                and available.get("npi", False)
                and available.get("npi_address", False)
            ),
            include_npi_taxonomy=available.get("npi_taxonomy", False),
            include_nucc_taxonomy=available.get("nucc_taxonomy", False),
            include_npi_other_identifier=(
                include_other_identifier_candidates
                and available.get("npi_other_identifier", False)
            ),
            include_provider_additional_npi=available.get(
                "provider_enrollment_ffs_additional_npi", False
            ),
            include_facility_anchor=available.get("facility_anchor", False),
        )
        for shard in range(facility_candidate_shards):
            label = "facility anchor npi candidate"
            if facility_candidate_shards > 1:
                label = f"{label} shard {shard + 1}/{facility_candidate_shards}"
            statements.append(
                _SupportStageStatement(
                    label,
                    _facility_anchor_npi_candidate_sql(
                        db_schema,
                        stage_table_map[FacilityAnchorNPICandidate],
                        stage_table,
                        source_run_id=source_run_id,
                        candidate_options_by_name=facility_candidate_options_by_name,
                        candidate_shards=facility_candidate_shards,
                        candidate_shard=shard,
                    ),
                )
            )
    if build_network_bridge:
        bridge_specs.insert(
            1,
            (
                EntityAddressNetworkBridge,
                "network bridge",
                ("location_key", "entity_type", "entity_id", "network_id"),
                _network_bridge_sql,
                None,
                1,
            ),
        )
    for model, label, columns, builder, shard_env, default_shards in bridge_specs:
        if partial_bridge_reuse and copy_unaffected_bridges:
            statements.append(
                _SupportStageStatement(
                    f"reusing {label}",
                    _copy_unaffected_bridge_rows_sql(
                        db_schema,
                        model.__main_table__,
                        stage_table_map[model],
                        columns,
                        affected_group_table,
                    ),
                )
            )
        bridge_shards = (
            _env_int(shard_env, default_shards, minimum=1)
            if shard_env
            else 1
        )
        for shard in range(bridge_shards):
            statement_label = label
            if bridge_shards > 1:
                statement_label = f"{label} shard {shard + 1}/{bridge_shards}"
            statement_option_map = {
                "affected_group_table": affected_group_table if partial_bridge_reuse else None,
            }
            if shard_env:
                statement_option_map.update(
                    {"bridge_shards": bridge_shards, "bridge_shard": shard}
                )
            statements.append(
                _SupportStageStatement(
                    statement_label,
                    builder(
                        db_schema,
                        stage_table_map[model],
                        stage_table,
                        **statement_option_map,
                    ),
                )
            )
    return statements


def _support_stage_sql(
    db_schema: str,
    stage_table: str,
    stage_classes: dict[type, type],
    *,
    source_run_id: str,
    node_id: str | None,
    raw_table: str | None = None,
    build_network_bridge: bool = True,
    available: dict[str, bool] | None = None,
    affected_group_table: str | None = None,
    copy_unaffected_bridges: bool = True,
) -> list[str]:
    return [
        stage_statement.statement
        for stage_statement in _support_stage_statements(
            db_schema,
            stage_table,
            stage_classes,
            source_run_id=source_run_id,
            node_id=node_id,
            raw_table=raw_table,
            build_network_bridge=build_network_bridge,
            available=available,
            affected_group_table=affected_group_table,
            copy_unaffected_bridges=copy_unaffected_bridges,
        )
    ]


async def _populate_support_stage_tables(
    db_schema: str,
    stage_table: str,
    stage_classes: dict[type, type],
    *,
    source_run_id: str,
    node_id: str | None,
    raw_table: str | None = None,
    build_network_bridge: bool = True,
    available: dict[str, bool] | None = None,
    run_id: str | None = None,
    context: dict | None = None,
    affected_group_table: str | None = None,
    copy_unaffected_bridges: bool = True,
) -> dict[str, int]:
    """Populate support stages with bounded parallel execution."""
    phase_context = context if context is not None else {}
    statements = _support_stage_statements(
        db_schema,
        stage_table,
        stage_classes,
        source_run_id=source_run_id,
        node_id=node_id,
        raw_table=raw_table,
        build_network_bridge=build_network_bridge,
        available=available,
        affected_group_table=affected_group_table,
        copy_unaffected_bridges=copy_unaffected_bridges,
    )
    support_concurrency = min(
        _env_int(
            "HLTHPRT_ENTITY_ADDRESS_UNIFIED_SUPPORT_CONCURRENCY",
            DEFAULT_SUPPORT_STAGE_CONCURRENCY,
            minimum=1,
        ),
        max(1, sum(1 for stage_statement in statements if stage_statement.parallel)),
    )
    phase_context["support_stage_concurrency"] = support_concurrency
    total_steps = len(statements)
    step_progress_map = {"completed_steps": 0}
    progress_lock = asyncio.Lock()

    async def _run_item(index: int, stage_statement: _SupportStageStatement) -> None:
        # Progress is held in step_progress_map to avoid nonlocal state.
        if run_id:
            async with progress_lock:
                current_done = step_progress_map["completed_steps"]
                enqueue_live_progress(
                    run_id=run_id,
                    importer="entity-address-unified",
                    status="running",
                    phase=f"entity-address-unified building {stage_statement.label}",
                    unit="steps",
                    done=current_done,
                    total=total_steps,
                    pct=95 + (current_done / max(total_steps, 1)) * 4,
                    message=(
                        f"building support table {index}/{total_steps}: {stage_statement.label} "
                        f"(concurrency {support_concurrency})"
                    ),
                )
        await _run_sql_phase(
            stage_statement.statement,
            context=phase_context,
            phase=f"entity-address-unified building {stage_statement.label}",
        )

    async def _finish_item(index: int, stage_statement: _SupportStageStatement) -> None:
        # Progress is held in step_progress_map to avoid nonlocal state.
        await _run_item(index, stage_statement)
        async with progress_lock:
            step_progress_map["completed_steps"] += 1
            if run_id:
                enqueue_live_progress(
                    run_id=run_id,
                    importer="entity-address-unified",
                    status="running",
                    phase=f"entity-address-unified built {stage_statement.label}",
                    unit="steps",
                    done=step_progress_map["completed_steps"],
                    total=total_steps,
                    pct=95 + (step_progress_map["completed_steps"] / max(total_steps, 1)) * 4,
                    message=(
                        f"built support table {index}/{total_steps}: "
                        f"{stage_statement.label}"
                    ),
                )

    async def _run_parallel_batch(batch: list[tuple[int, _SupportStageStatement]]) -> None:
        if not batch:
            return
        if support_concurrency <= 1 or len(batch) in {1}:
            for index, stage_statement in batch:
                await _finish_item(index, stage_statement)
            return
        semaphore = asyncio.Semaphore(support_concurrency)

        async def _guarded(index: int, stage_statement: _SupportStageStatement) -> None:
            async with semaphore:
                await _finish_item(index, stage_statement)

        results = await asyncio.gather(
            *(
                _guarded(index, stage_statement)
                for index, stage_statement in batch
            ),
            return_exceptions=True,
        )
        for result in results:
            if isinstance(result, BaseException):
                raise result

    parallel_batches: list[tuple[int, _SupportStageStatement]] = []
    for index, stage_statement in enumerate(statements, start=1):
        if stage_statement.parallel:
            parallel_batches.append((index, stage_statement))
            continue
        await _run_parallel_batch(parallel_batches)
        parallel_batches = []
        await _finish_item(index, stage_statement)
    await _run_parallel_batch(parallel_batches)
    row_count_map: dict[str, int] = {}
    for model, stage_cls in stage_classes.items():
        row_count_map[model.__tablename__] = int(
            await db.scalar(f"SELECT COUNT(*) FROM {db_schema}.{stage_cls.__tablename__};")
            or 0
        )
    return row_count_map


def _support_stage_progress_label(statement: str) -> str:
    normalized = " ".join(statement.split())
    match = re.search(
        r"\b(?:INSERT\s+INTO|TRUNCATE\s+TABLE)\s+"
        r"(?:[A-Za-z_][A-Za-z0-9_]*\.)?([A-Za-z0-9_]+)",
        normalized,
        re.IGNORECASE,
    )
    if not match:
        return "support tables"
    table = re.sub(r"_\d{8}$", "", match.group(1))
    if table.startswith("entity_address_"):
        table = table[len("entity_address_") :]
    return table.replace("_", " ")


def _inference_sql(
    db_schema: str,
    stage_table: str,
    *,
    inference_options_by_name: Mapping[str, bool],
) -> str:
    """Build deterministic facility NPI inference SQL."""
    include_hospital_enrollment = inference_options_by_name.get(
        "include_hospital_enrollment", False
    )
    include_fqhc_enrollment = inference_options_by_name.get(
        "include_fqhc_enrollment", False
    )
    include_facility_override = inference_options_by_name.get(
        "include_facility_override", False
    )
    include_npi_other_identifier = inference_options_by_name.get(
        "include_npi_other_identifier", False
    )
    include_name_fallback = inference_options_by_name.get(
        "include_name_fallback", False
    )
    include_nppes_name_inference = inference_options_by_name.get(
        "include_nppes_name_inference", False
    )
    include_nppes_broad_inference = inference_options_by_name.get(
        "include_nppes_broad_inference", False
    )
    def norm_text_sql(expr: str) -> str:
        """Normalize text for generated SQL comparisons."""
        return f"regexp_replace(LOWER(COALESCE({expr}, '')), '[^a-z0-9]', '', 'g')"

    def zip5_sql(expr: str) -> str:
        """Normalize a postal-code expression to ZIP5."""
        return f"LEFT(COALESCE({expr}, ''), 5)"

    fa_parent_name = "to_jsonb(fa)->>'health_center_name'"
    fa_parent_address = "to_jsonb(fa)->>'health_center_organization_address_line1'"
    fa_parent_state = "to_jsonb(fa)->>'health_center_organization_state'"
    fa_parent_zip = "to_jsonb(fa)->>'health_center_organization_zip_code'"
    fa_health_center_number = "to_jsonb(fa)->>'health_center_number'"
    fa_health_center_organization_id = "to_jsonb(fa)->>'health_center_organization_id'"
    source_fa_health_center_number = "to_jsonb(source_fa)->>'health_center_number'"
    source_fa_health_center_organization_id = "to_jsonb(source_fa)->>'health_center_organization_id'"
    hospital_taxonomy_codes_sql = _hospital_facility_taxonomy_codes_sql("                ")
    empty_inference_candidates_sql = """
        SELECT
            NULL::varchar AS entity_type,
            NULL::varchar AS entity_id,
            NULL::varchar AS type,
            NULL::bigint AS checksum,
            NULL::bigint AS candidate_npi,
            0::int AS candidate_npi_count
         WHERE FALSE
    """

    facility_override_candidates_sql = (
        f"""
        SELECT
            t.entity_type,
            t.entity_id,
            t.type,
            t.checksum,
            MIN(o.npi)::bigint AS candidate_npi,
            COUNT(DISTINCT o.npi)::int AS candidate_npi_count
          FROM unresolved_facility AS t
          JOIN {db_schema}.facility_anchor_npi_override AS o
            ON o.facility_anchor_id = t.entity_id
           AND o.npi IS NOT NULL
           AND LOWER(COALESCE(o.status, '')) = 'approved'
         WHERE t.npi IS NULL
           AND t.inferred_npi IS NULL
           AND t.entity_type = 'facility_anchor'
         GROUP BY t.entity_type, t.entity_id, t.type, t.checksum
        """
        if include_facility_override
        else """
        SELECT
            NULL::varchar AS entity_type,
            NULL::varchar AS entity_id,
            NULL::varchar AS type,
            NULL::bigint AS checksum,
            NULL::bigint AS candidate_npi,
            0::int AS candidate_npi_count
         WHERE FALSE
        """
    )

    hospital_ccn_candidates_sql = (
        f"""
        SELECT
            t.entity_type,
            t.entity_id,
            t.type,
            t.checksum,
            MIN(h.npi)::bigint AS candidate_npi,
            COUNT(DISTINCT h.npi)::int AS candidate_npi_count
          FROM unresolved_facility AS t
          JOIN {db_schema}.provider_enrollment_hospital AS h
            ON h.npi IS NOT NULL
           AND (
                h.ccn = t.entity_id
                OR h.cah_or_hospital_ccn = t.entity_id
           )
         WHERE t.npi IS NULL
           AND t.inferred_npi IS NULL
           AND t.entity_type = 'facility_anchor'
           AND COALESCE(t.entity_subtype, '') = 'Hospital'
         GROUP BY t.entity_type, t.entity_id, t.type, t.checksum
        """
        if include_hospital_enrollment
        else """
        SELECT
            NULL::varchar AS entity_type,
            NULL::varchar AS entity_id,
            NULL::varchar AS type,
            NULL::bigint AS checksum,
            NULL::bigint AS candidate_npi,
            0::int AS candidate_npi_count
         WHERE FALSE
        """
    )

    hospital_nppes_name_address_candidates_sql = (
        f"""
        SELECT
            t.entity_type,
            t.entity_id,
            t.type,
            t.checksum,
            MIN(n.npi)::bigint AS candidate_npi,
            COUNT(DISTINCT n.npi)::int AS candidate_npi_count
          FROM unresolved_facility AS t
          JOIN {db_schema}.npi AS n
            ON n.entity_type_code = 2
           AND (
                {norm_text_sql("n.provider_organization_name")}
                    = {norm_text_sql("t.entity_name")}
                OR {norm_text_sql("n.provider_other_organization_name")}
                    = {norm_text_sql("t.entity_name")}
                OR {norm_text_sql("n.do_business_as_text")}
                    = {norm_text_sql("t.entity_name")}
           )
          JOIN {db_schema}.npi_address AS a
            ON a.npi = n.npi
           AND {zip5_sql("a.postal_code")} = {zip5_sql("t.postal_code")}
           AND UPPER(COALESCE(a.state_name, '')) = UPPER(COALESCE(t.state_name, ''))
           AND {norm_text_sql("a.first_line")} = {norm_text_sql("t.first_line")}
          JOIN {db_schema}.npi_taxonomy AS nt
            ON nt.npi = n.npi
           AND nt.healthcare_provider_primary_taxonomy_switch = 'Y'
           AND nt.healthcare_provider_taxonomy_code IN (
                {hospital_taxonomy_codes_sql}
           )
         WHERE t.npi IS NULL
           AND t.inferred_npi IS NULL
           AND t.entity_type = 'facility_anchor'
           AND COALESCE(t.entity_subtype, '') = 'Hospital'
           AND {norm_text_sql("t.entity_name")} <> ''
           AND {norm_text_sql("t.first_line")} <> ''
         GROUP BY t.entity_type, t.entity_id, t.type, t.checksum
    """
        if include_nppes_name_inference
        else empty_inference_candidates_sql
    )

    hospital_other_identifier_candidates_sql = (
        f"""
        SELECT
            t.entity_type,
            t.entity_id,
            t.type,
            t.checksum,
            MIN(oi.npi)::bigint AS candidate_npi,
            COUNT(DISTINCT oi.npi)::int AS candidate_npi_count
          FROM unresolved_facility AS t
          JOIN {db_schema}.npi_other_identifier AS oi
            ON oi.npi IS NOT NULL
           AND regexp_replace(COALESCE(oi.other_provider_identifier, ''), '[^A-Za-z0-9]', '', 'g')
               = regexp_replace(COALESCE(t.entity_id, ''), '[^A-Za-z0-9]', '', 'g')
           AND (
                UPPER(COALESCE(oi.other_provider_identifier_state, '')) = UPPER(COALESCE(t.state_name, ''))
                OR COALESCE(oi.other_provider_identifier_state, '') = ''
           )
         WHERE t.npi IS NULL
           AND t.inferred_npi IS NULL
           AND t.entity_type = 'facility_anchor'
           AND COALESCE(t.entity_subtype, '') = 'Hospital'
           AND COALESCE(t.entity_id, '') <> ''
         GROUP BY t.entity_type, t.entity_id, t.type, t.checksum
        """
        if include_npi_other_identifier
        else """
        SELECT
            NULL::varchar AS entity_type,
            NULL::varchar AS entity_id,
            NULL::varchar AS type,
            NULL::bigint AS checksum,
            NULL::bigint AS candidate_npi,
            0::int AS candidate_npi_count
         WHERE FALSE
        """
    )

    hospital_address_key_candidates_sql = f"""
        SELECT
            t.entity_type,
            t.entity_id,
            t.type,
            t.checksum,
            MIN(a.npi)::bigint AS candidate_npi,
            COUNT(DISTINCT a.npi)::int AS candidate_npi_count
          FROM unresolved_facility AS t
          JOIN {db_schema}.npi_address AS a
            ON a.address_key = t.address_key
           AND a.npi IS NOT NULL
          JOIN {db_schema}.npi AS n
            ON n.npi = a.npi
           AND n.entity_type_code = 2
          JOIN {db_schema}.npi_taxonomy AS nt
            ON nt.npi = a.npi
           AND nt.healthcare_provider_primary_taxonomy_switch = 'Y'
           AND nt.healthcare_provider_taxonomy_code IN (
                {hospital_taxonomy_codes_sql}
           )
         WHERE t.npi IS NULL
           AND t.inferred_npi IS NULL
           AND t.entity_type = 'facility_anchor'
           AND COALESCE(t.entity_subtype, '') = 'Hospital'
           AND t.address_key IS NOT NULL
         GROUP BY t.entity_type, t.entity_id, t.type, t.checksum
    """

    fqhc_exact_candidates_sql = (
        f"""
        SELECT
            t.entity_type,
            t.entity_id,
            t.type,
            t.checksum,
            MIN(f.npi)::bigint AS candidate_npi,
            COUNT(DISTINCT f.npi)::int AS candidate_npi_count
          FROM unresolved_facility AS t
          JOIN {db_schema}.provider_enrollment_fqhc AS f
            ON f.npi IS NOT NULL
           AND LEFT(COALESCE(f.zip_code, ''), 5) = LEFT(COALESCE(t.postal_code, ''), 5)
           AND UPPER(COALESCE(f.state, '')) = UPPER(COALESCE(t.state_name, ''))
           AND regexp_replace(LOWER(COALESCE(f.address_line_1, '')), '[^a-z0-9]', '', 'g')
               = regexp_replace(LOWER(COALESCE(t.first_line, '')), '[^a-z0-9]', '', 'g')
           AND (
                regexp_replace(LOWER(COALESCE(f.organization_name, '')), '[^a-z0-9]', '', 'g')
                    = regexp_replace(LOWER(COALESCE(t.entity_name, '')), '[^a-z0-9]', '', 'g')
                OR regexp_replace(LOWER(COALESCE(f.doing_business_as_name, '')), '[^a-z0-9]', '', 'g')
                    = regexp_replace(LOWER(COALESCE(t.entity_name, '')), '[^a-z0-9]', '', 'g')
           )
         WHERE t.npi IS NULL
           AND t.inferred_npi IS NULL
           AND t.entity_type = 'facility_anchor'
           AND COALESCE(t.entity_subtype, '') = 'FQHC'
         GROUP BY t.entity_type, t.entity_id, t.type, t.checksum
        """
        if include_fqhc_enrollment
        else """
        SELECT
            NULL::varchar AS entity_type,
            NULL::varchar AS entity_id,
            NULL::varchar AS type,
            NULL::bigint AS checksum,
            NULL::bigint AS candidate_npi,
            0::int AS candidate_npi_count
        WHERE FALSE
        """
    )

    fqhc_parent_exact_candidates_sql = (
        f"""
        SELECT
            t.entity_type,
            t.entity_id,
            t.type,
            t.checksum,
            MIN(f.npi)::bigint AS candidate_npi,
            COUNT(DISTINCT f.npi)::int AS candidate_npi_count
          FROM unresolved_facility AS t
          JOIN {db_schema}.facility_anchor AS fa
            ON fa.id = t.entity_id
          JOIN {db_schema}.provider_enrollment_fqhc AS f
            ON f.npi IS NOT NULL
           AND {zip5_sql("f.zip_code")} = {zip5_sql(fa_parent_zip)}
           AND UPPER(COALESCE(f.state, '')) = UPPER(COALESCE({fa_parent_state}, ''))
           AND {norm_text_sql("f.address_line_1")} = {norm_text_sql(fa_parent_address)}
           AND (
                {norm_text_sql("f.organization_name")} = {norm_text_sql(fa_parent_name)}
                OR {norm_text_sql("f.doing_business_as_name")} = {norm_text_sql(fa_parent_name)}
           )
         WHERE t.npi IS NULL
           AND t.inferred_npi IS NULL
           AND t.entity_type = 'facility_anchor'
           AND COALESCE(t.entity_subtype, '') = 'FQHC'
           AND {norm_text_sql(fa_parent_name)} <> ''
           AND {norm_text_sql(fa_parent_address)} <> ''
         GROUP BY t.entity_type, t.entity_id, t.type, t.checksum
        """
        if include_fqhc_enrollment
        else """
        SELECT
            NULL::varchar AS entity_type,
            NULL::varchar AS entity_id,
            NULL::varchar AS type,
            NULL::bigint AS checksum,
            NULL::bigint AS candidate_npi,
            0::int AS candidate_npi_count
         WHERE FALSE
        """
    )

    fqhc_parent_name_state_candidates_sql = (
        f"""
        SELECT
            t.entity_type,
            t.entity_id,
            t.type,
            t.checksum,
            MIN(f.npi)::bigint AS candidate_npi,
            COUNT(DISTINCT f.npi)::int AS candidate_npi_count
          FROM unresolved_facility AS t
          JOIN {db_schema}.facility_anchor AS fa
            ON fa.id = t.entity_id
          JOIN {db_schema}.provider_enrollment_fqhc AS f
            ON f.npi IS NOT NULL
           AND UPPER(COALESCE(f.state, '')) = UPPER(COALESCE({fa_parent_state}, fa.state, ''))
           AND (
                {norm_text_sql("f.organization_name")} = {norm_text_sql(fa_parent_name)}
                OR {norm_text_sql("f.doing_business_as_name")} = {norm_text_sql(fa_parent_name)}
           )
         WHERE t.npi IS NULL
           AND t.inferred_npi IS NULL
           AND t.entity_type = 'facility_anchor'
           AND COALESCE(t.entity_subtype, '') = 'FQHC'
           AND {norm_text_sql(fa_parent_name)} <> ''
         GROUP BY t.entity_type, t.entity_id, t.type, t.checksum
        """
        if include_fqhc_enrollment
        else """
        SELECT
            NULL::varchar AS entity_type,
            NULL::varchar AS entity_id,
            NULL::varchar AS type,
            NULL::bigint AS checksum,
            NULL::bigint AS candidate_npi,
            0::int AS candidate_npi_count
         WHERE FALSE
        """
    )

    fqhc_parent_address_candidates_sql = (
        f"""
        SELECT
            t.entity_type,
            t.entity_id,
            t.type,
            t.checksum,
            MIN(f.npi)::bigint AS candidate_npi,
            COUNT(DISTINCT f.npi)::int AS candidate_npi_count
          FROM unresolved_facility AS t
          JOIN {db_schema}.facility_anchor AS fa
            ON fa.id = t.entity_id
          JOIN {db_schema}.provider_enrollment_fqhc AS f
            ON f.npi IS NOT NULL
           AND {zip5_sql("f.zip_code")} = {zip5_sql(fa_parent_zip)}
           AND UPPER(COALESCE(f.state, '')) = UPPER(COALESCE({fa_parent_state}, ''))
           AND {norm_text_sql("f.address_line_1")} = {norm_text_sql(fa_parent_address)}
         WHERE t.npi IS NULL
           AND t.inferred_npi IS NULL
           AND t.entity_type = 'facility_anchor'
           AND COALESCE(t.entity_subtype, '') = 'FQHC'
           AND {norm_text_sql(fa_parent_address)} <> ''
         GROUP BY t.entity_type, t.entity_id, t.type, t.checksum
        """
        if include_fqhc_enrollment
        else """
        SELECT
            NULL::varchar AS entity_type,
            NULL::varchar AS entity_id,
            NULL::varchar AS type,
            NULL::bigint AS checksum,
            NULL::bigint AS candidate_npi,
            0::int AS candidate_npi_count
         WHERE FALSE
        """
    )

    fqhc_sibling_source_npi_candidates_sql = f"""
        SELECT
            t.entity_type,
            t.entity_id,
            t.type,
            t.checksum,
            MIN(source_fa.npi)::bigint AS candidate_npi,
            COUNT(DISTINCT source_fa.npi)::int AS candidate_npi_count
          FROM unresolved_facility AS t
          JOIN {db_schema}.facility_anchor AS fa
            ON fa.id = t.entity_id
          JOIN {db_schema}.facility_anchor AS source_fa
            ON source_fa.npi IS NOT NULL
           AND source_fa.id <> fa.id
           AND source_fa.facility_type = 'FQHC'
           AND (
                (
                    COALESCE({fa_health_center_number}, '') <> ''
                    AND {source_fa_health_center_number} = {fa_health_center_number}
                )
                OR (
                    COALESCE({fa_health_center_organization_id}, '') <> ''
                    AND {source_fa_health_center_organization_id} = {fa_health_center_organization_id}
                )
           )
         WHERE t.npi IS NULL
           AND t.inferred_npi IS NULL
           AND t.entity_type = 'facility_anchor'
           AND COALESCE(t.entity_subtype, '') = 'FQHC'
         GROUP BY t.entity_type, t.entity_id, t.type, t.checksum
    """

    fqhc_ccn_candidates_sql = (
        f"""
        SELECT
            t.entity_type,
            t.entity_id,
            t.type,
            t.checksum,
            MIN(f.npi)::bigint AS candidate_npi,
            COUNT(DISTINCT f.npi)::int AS candidate_npi_count
          FROM unresolved_facility AS t
          JOIN {db_schema}.facility_anchor AS fa
            ON fa.id = t.entity_id
           AND COALESCE(fa.medicare_ccn, '') <> ''
          JOIN {db_schema}.provider_enrollment_fqhc AS f
            ON f.ccn = fa.medicare_ccn
           AND f.npi IS NOT NULL
         WHERE t.npi IS NULL
           AND t.inferred_npi IS NULL
           AND t.entity_type = 'facility_anchor'
           AND COALESCE(t.entity_subtype, '') = 'FQHC'
         GROUP BY t.entity_type, t.entity_id, t.type, t.checksum
        """
        if include_fqhc_enrollment
        else """
        SELECT
            NULL::varchar AS entity_type,
            NULL::varchar AS entity_id,
            NULL::varchar AS type,
            NULL::bigint AS checksum,
            NULL::bigint AS candidate_npi,
            0::int AS candidate_npi_count
         WHERE FALSE
        """
    )

    fqhc_other_identifier_candidates_sql = (
        f"""
        SELECT
            t.entity_type,
            t.entity_id,
            t.type,
            t.checksum,
            MIN(oi.npi)::bigint AS candidate_npi,
            COUNT(DISTINCT oi.npi)::int AS candidate_npi_count
          FROM unresolved_facility AS t
          JOIN {db_schema}.facility_anchor AS fa
            ON fa.id = t.entity_id
           AND COALESCE(fa.medicare_ccn, '') <> ''
          JOIN {db_schema}.npi_other_identifier AS oi
            ON oi.npi IS NOT NULL
           AND regexp_replace(COALESCE(oi.other_provider_identifier, ''), '[^A-Za-z0-9]', '', 'g')
               = regexp_replace(COALESCE(fa.medicare_ccn, ''), '[^A-Za-z0-9]', '', 'g')
           AND (
                UPPER(COALESCE(oi.other_provider_identifier_state, '')) = UPPER(COALESCE(t.state_name, ''))
                OR COALESCE(oi.other_provider_identifier_state, '') = ''
           )
         WHERE t.npi IS NULL
           AND t.inferred_npi IS NULL
           AND t.entity_type = 'facility_anchor'
           AND COALESCE(t.entity_subtype, '') = 'FQHC'
         GROUP BY t.entity_type, t.entity_id, t.type, t.checksum
        """
        if include_npi_other_identifier
        else """
        SELECT
            NULL::varchar AS entity_type,
            NULL::varchar AS entity_id,
            NULL::varchar AS type,
            NULL::bigint AS checksum,
            NULL::bigint AS candidate_npi,
            0::int AS candidate_npi_count
         WHERE FALSE
        """
    )

    fqhc_enrollment_address_candidates_sql = (
        f"""
        SELECT
            t.entity_type,
            t.entity_id,
            t.type,
            t.checksum,
            MIN(f.npi)::bigint AS candidate_npi,
            COUNT(DISTINCT f.npi)::int AS candidate_npi_count
          FROM unresolved_facility AS t
          JOIN {db_schema}.provider_enrollment_fqhc AS f
            ON f.npi IS NOT NULL
           AND LEFT(COALESCE(f.zip_code, ''), 5) = LEFT(COALESCE(t.postal_code, ''), 5)
           AND UPPER(COALESCE(f.state, '')) = UPPER(COALESCE(t.state_name, ''))
           AND regexp_replace(LOWER(COALESCE(f.address_line_1, '')), '[^a-z0-9]', '', 'g')
               = regexp_replace(LOWER(COALESCE(t.first_line, '')), '[^a-z0-9]', '', 'g')
         WHERE t.npi IS NULL
           AND t.inferred_npi IS NULL
           AND t.entity_type = 'facility_anchor'
           AND COALESCE(t.entity_subtype, '') = 'FQHC'
           AND regexp_replace(LOWER(COALESCE(t.first_line, '')), '[^a-z0-9]', '', 'g') <> ''
         GROUP BY t.entity_type, t.entity_id, t.type, t.checksum
        """
        if include_fqhc_enrollment
        else """
        SELECT
            NULL::varchar AS entity_type,
            NULL::varchar AS entity_id,
            NULL::varchar AS type,
            NULL::bigint AS checksum,
            NULL::bigint AS candidate_npi,
            0::int AS candidate_npi_count
         WHERE FALSE
        """
    )

    fqhc_enrollment_phone_zip_candidates_sql = (
        f"""
        SELECT
            t.entity_type,
            t.entity_id,
            t.type,
            t.checksum,
            MIN(f.npi)::bigint AS candidate_npi,
            COUNT(DISTINCT f.npi)::int AS candidate_npi_count
          FROM unresolved_facility AS t
          JOIN {db_schema}.provider_enrollment_fqhc AS f
            ON f.npi IS NOT NULL
           AND LEFT(COALESCE(f.zip_code, ''), 5) = LEFT(COALESCE(t.postal_code, ''), 5)
           AND regexp_replace(COALESCE(f.telephone_number, ''), '[^0-9]', '', 'g')
               = regexp_replace(COALESCE(t.telephone_number, ''), '[^0-9]', '', 'g')
           AND LENGTH(regexp_replace(COALESCE(f.telephone_number, ''), '[^0-9]', '', 'g')) = 10
           AND LENGTH(regexp_replace(COALESCE(t.telephone_number, ''), '[^0-9]', '', 'g')) = 10
         WHERE t.npi IS NULL
           AND t.inferred_npi IS NULL
           AND t.entity_type = 'facility_anchor'
           AND COALESCE(t.entity_subtype, '') = 'FQHC'
         GROUP BY t.entity_type, t.entity_id, t.type, t.checksum
        """
        if include_fqhc_enrollment
        else """
        SELECT
            NULL::varchar AS entity_type,
            NULL::varchar AS entity_id,
            NULL::varchar AS type,
            NULL::bigint AS checksum,
            NULL::bigint AS candidate_npi,
            0::int AS candidate_npi_count
         WHERE FALSE
        """
    )

    fqhc_enrollment_phone_name_candidates_sql = (
        f"""
        SELECT
            t.entity_type,
            t.entity_id,
            t.type,
            t.checksum,
            MIN(f.npi)::bigint AS candidate_npi,
            COUNT(DISTINCT f.npi)::int AS candidate_npi_count
          FROM unresolved_facility AS t
          JOIN {db_schema}.provider_enrollment_fqhc AS f
            ON f.npi IS NOT NULL
           AND UPPER(COALESCE(f.state, '')) = UPPER(COALESCE(t.state_name, ''))
           AND regexp_replace(COALESCE(f.telephone_number, ''), '[^0-9]', '', 'g')
               = regexp_replace(COALESCE(t.telephone_number, ''), '[^0-9]', '', 'g')
           AND LENGTH(regexp_replace(COALESCE(f.telephone_number, ''), '[^0-9]', '', 'g')) = 10
           AND LENGTH(regexp_replace(COALESCE(t.telephone_number, ''), '[^0-9]', '', 'g')) = 10
           AND (
                regexp_replace(LOWER(COALESCE(f.organization_name, '')), '[^a-z0-9]', '', 'g')
                    = regexp_replace(LOWER(COALESCE(t.entity_name, '')), '[^a-z0-9]', '', 'g')
                OR regexp_replace(LOWER(COALESCE(f.doing_business_as_name, '')), '[^a-z0-9]', '', 'g')
                    = regexp_replace(LOWER(COALESCE(t.entity_name, '')), '[^a-z0-9]', '', 'g')
           )
         WHERE t.npi IS NULL
           AND t.inferred_npi IS NULL
           AND t.entity_type = 'facility_anchor'
           AND COALESCE(t.entity_subtype, '') = 'FQHC'
           AND regexp_replace(LOWER(COALESCE(t.entity_name, '')), '[^a-z0-9]', '', 'g') <> ''
         GROUP BY t.entity_type, t.entity_id, t.type, t.checksum
        """
        if include_fqhc_enrollment
        else """
        SELECT
            NULL::varchar AS entity_type,
            NULL::varchar AS entity_id,
            NULL::varchar AS type,
            NULL::bigint AS checksum,
            NULL::bigint AS candidate_npi,
            0::int AS candidate_npi_count
         WHERE FALSE
        """
    )

    fqhc_provider_npis_sql = (
        f"""
        SELECT DISTINCT npi::bigint AS npi
          FROM {db_schema}.provider_enrollment_fqhc
         WHERE npi IS NOT NULL
        """
        if include_fqhc_enrollment
        else """
        SELECT NULL::bigint AS npi
         WHERE FALSE
        """
    )

    def nppes_address_type_filter(address_type: str | None) -> str:
        """Build an optional NPPES address-type predicate."""
        return f"           AND a.type = '{address_type}'\n" if address_type else ""

    def build_parent_exact_candidates_sql(address_type: str | None) -> str:
        """Build exact FQHC parent candidates from NPPES names and addresses."""
        if not include_nppes_name_inference:
            return empty_inference_candidates_sql
        return f"""
        SELECT
            t.entity_type,
            t.entity_id,
            t.type,
            t.checksum,
            MIN(n.npi)::bigint AS candidate_npi,
            COUNT(DISTINCT n.npi)::int AS candidate_npi_count
          FROM unresolved_facility AS t
          JOIN {db_schema}.facility_anchor AS fa
            ON fa.id = t.entity_id
          JOIN {db_schema}.npi AS n
            ON n.entity_type_code = 2
           AND (
                {norm_text_sql("n.provider_organization_name")} = {norm_text_sql(fa_parent_name)}
                OR {norm_text_sql("n.provider_other_organization_name")} = {norm_text_sql(fa_parent_name)}
                OR {norm_text_sql("n.do_business_as_text")} = {norm_text_sql(fa_parent_name)}
           )
          JOIN {db_schema}.npi_address AS a
            ON a.npi = n.npi
{nppes_address_type_filter(address_type)}           AND {zip5_sql("a.postal_code")} = {zip5_sql(fa_parent_zip)}
           AND UPPER(COALESCE(a.state_name, '')) = UPPER(COALESCE({fa_parent_state}, ''))
           AND {norm_text_sql("a.first_line")} = {norm_text_sql(fa_parent_address)}
          JOIN primary_taxonomy AS pt
            ON pt.npi = n.npi
           AND pt.taxonomy_code = '261QF0400X'
         WHERE t.npi IS NULL
           AND t.inferred_npi IS NULL
           AND t.entity_type = 'facility_anchor'
           AND COALESCE(t.entity_subtype, '') = 'FQHC'
           AND {norm_text_sql(fa_parent_name)} <> ''
           AND {norm_text_sql(fa_parent_address)} <> ''
         GROUP BY t.entity_type, t.entity_id, t.type, t.checksum
        """

    def build_parent_name_state_candidates_sql(address_type: str | None) -> str:
        """Build FQHC parent candidates from NPPES names and states."""
        if not include_nppes_name_inference:
            return empty_inference_candidates_sql
        return f"""
        SELECT
            t.entity_type,
            t.entity_id,
            t.type,
            t.checksum,
            MIN(n.npi)::bigint AS candidate_npi,
            COUNT(DISTINCT n.npi)::int AS candidate_npi_count
          FROM unresolved_facility AS t
          JOIN {db_schema}.facility_anchor AS fa
            ON fa.id = t.entity_id
          JOIN {db_schema}.npi AS n
            ON n.entity_type_code = 2
           AND (
                {norm_text_sql("n.provider_organization_name")} = {norm_text_sql(fa_parent_name)}
                OR {norm_text_sql("n.provider_other_organization_name")} = {norm_text_sql(fa_parent_name)}
                OR {norm_text_sql("n.do_business_as_text")} = {norm_text_sql(fa_parent_name)}
           )
          JOIN {db_schema}.npi_address AS a
            ON a.npi = n.npi
{nppes_address_type_filter(address_type)}           AND UPPER(COALESCE(a.state_name, '')) = UPPER(COALESCE({fa_parent_state}, fa.state, ''))
          JOIN primary_taxonomy AS pt
            ON pt.npi = n.npi
           AND pt.taxonomy_code = '261QF0400X'
         WHERE t.npi IS NULL
           AND t.inferred_npi IS NULL
           AND t.entity_type = 'facility_anchor'
           AND COALESCE(t.entity_subtype, '') = 'FQHC'
           AND {norm_text_sql(fa_parent_name)} <> ''
         GROUP BY t.entity_type, t.entity_id, t.type, t.checksum
        """

    def build_parent_address_candidates_sql(address_type: str | None) -> str:
        """Build FQHC parent candidates from NPPES addresses."""
        if not include_nppes_name_inference:
            return empty_inference_candidates_sql
        return f"""
        SELECT
            t.entity_type,
            t.entity_id,
            t.type,
            t.checksum,
            MIN(n.npi)::bigint AS candidate_npi,
            COUNT(DISTINCT n.npi)::int AS candidate_npi_count
          FROM unresolved_facility AS t
          JOIN {db_schema}.facility_anchor AS fa
            ON fa.id = t.entity_id
          JOIN {db_schema}.npi_address AS a
            ON {zip5_sql("a.postal_code")} = {zip5_sql(fa_parent_zip)}
{nppes_address_type_filter(address_type)}           AND UPPER(COALESCE(a.state_name, '')) = UPPER(COALESCE({fa_parent_state}, ''))
           AND {norm_text_sql("a.first_line")} = {norm_text_sql(fa_parent_address)}
          JOIN {db_schema}.npi AS n
            ON n.npi = a.npi
           AND n.entity_type_code = 2
          JOIN primary_taxonomy AS pt
            ON pt.npi = n.npi
           AND pt.taxonomy_code = '261QF0400X'
         WHERE t.npi IS NULL
           AND t.inferred_npi IS NULL
           AND t.entity_type = 'facility_anchor'
           AND COALESCE(t.entity_subtype, '') = 'FQHC'
           AND {norm_text_sql(fa_parent_address)} <> ''
         GROUP BY t.entity_type, t.entity_id, t.type, t.checksum
        """

    fqhc_parent_nppes_exact_primary_candidates_sql = build_parent_exact_candidates_sql("primary")
    fqhc_parent_nppes_exact_all_candidates_sql = build_parent_exact_candidates_sql(None)
    fqhc_parent_nppes_name_state_primary_candidates_sql = build_parent_name_state_candidates_sql("primary")
    fqhc_parent_nppes_name_state_all_candidates_sql = build_parent_name_state_candidates_sql(None)
    fqhc_parent_nppes_address_primary_candidates_sql = build_parent_address_candidates_sql("primary")
    fqhc_parent_nppes_address_all_candidates_sql = build_parent_address_candidates_sql(None)

    npi_fqhc_exact_address_candidates_sql = (
        f"""
        SELECT
            t.entity_type,
            t.entity_id,
            t.type,
            t.checksum,
            MIN(a.npi)::bigint AS candidate_npi,
            COUNT(DISTINCT a.npi)::int AS candidate_npi_count
          FROM unresolved_facility AS t
          JOIN {db_schema}.npi_address AS a
            ON a.npi IS NOT NULL
           AND LEFT(COALESCE(a.postal_code, ''), 5) = LEFT(COALESCE(t.postal_code, ''), 5)
           AND UPPER(COALESCE(a.state_name, '')) = UPPER(COALESCE(t.state_name, ''))
           AND regexp_replace(LOWER(COALESCE(a.first_line, '')), '[^a-z0-9]', '', 'g')
               = regexp_replace(LOWER(COALESCE(t.first_line, '')), '[^a-z0-9]', '', 'g')
          JOIN fqhc_npis AS fq
            ON fq.npi = a.npi
         WHERE t.npi IS NULL
           AND t.inferred_npi IS NULL
           AND t.entity_type = 'facility_anchor'
           AND COALESCE(t.entity_subtype, '') = 'FQHC'
           AND regexp_replace(LOWER(COALESCE(t.first_line, '')), '[^a-z0-9]', '', 'g') <> ''
         GROUP BY t.entity_type, t.entity_id, t.type, t.checksum
    """
        if include_nppes_broad_inference
        else empty_inference_candidates_sql
    )

    npi_fqhc_address_key_candidates_sql = f"""
        SELECT
            t.entity_type,
            t.entity_id,
            t.type,
            t.checksum,
            MIN(a.npi)::bigint AS candidate_npi,
            COUNT(DISTINCT a.npi)::int AS candidate_npi_count
          FROM unresolved_facility AS t
          JOIN {db_schema}.npi_address AS a
            ON a.address_key = t.address_key
           AND a.npi IS NOT NULL
          JOIN {db_schema}.npi AS n
            ON n.npi = a.npi
           AND n.entity_type_code = 2
          JOIN primary_taxonomy AS pt
            ON pt.npi = a.npi
           AND pt.taxonomy_code = '261QF0400X'
         WHERE t.npi IS NULL
           AND t.inferred_npi IS NULL
           AND t.entity_type = 'facility_anchor'
           AND COALESCE(t.entity_subtype, '') = 'FQHC'
           AND t.address_key IS NOT NULL
         GROUP BY t.entity_type, t.entity_id, t.type, t.checksum
    """

    npi_fqhc_clinic_address_key_candidates_sql = f"""
        SELECT
            t.entity_type,
            t.entity_id,
            t.type,
            t.checksum,
            MIN(a.npi)::bigint AS candidate_npi,
            COUNT(DISTINCT a.npi)::int AS candidate_npi_count
          FROM unresolved_facility AS t
          JOIN {db_schema}.npi_address AS a
            ON a.address_key = t.address_key
           AND a.npi IS NOT NULL
          JOIN {db_schema}.npi AS n
            ON n.npi = a.npi
           AND n.entity_type_code = 2
          JOIN primary_taxonomy AS pt
            ON pt.npi = a.npi
           AND pt.taxonomy_code <> '261QF0400X'
          JOIN {db_schema}.nucc_taxonomy AS nu
            ON nu.code = pt.taxonomy_code
           AND COALESCE(nu.classification, '') = 'Clinic/Center'
         WHERE t.npi IS NULL
           AND t.inferred_npi IS NULL
           AND t.entity_type = 'facility_anchor'
           AND COALESCE(t.entity_subtype, '') = 'FQHC'
           AND t.address_key IS NOT NULL
         GROUP BY t.entity_type, t.entity_id, t.type, t.checksum
    """

    npi_fqhc_phone_zip_candidates_sql = (
        f"""
        SELECT
            t.entity_type,
            t.entity_id,
            t.type,
            t.checksum,
            MIN(a.npi)::bigint AS candidate_npi,
            COUNT(DISTINCT a.npi)::int AS candidate_npi_count
          FROM unresolved_facility AS t
          JOIN {db_schema}.npi_address AS a
            ON a.npi IS NOT NULL
           AND LEFT(COALESCE(a.postal_code, ''), 5) = LEFT(COALESCE(t.postal_code, ''), 5)
           AND regexp_replace(COALESCE(a.telephone_number, ''), '[^0-9]', '', 'g')
               = regexp_replace(COALESCE(t.telephone_number, ''), '[^0-9]', '', 'g')
           AND LENGTH(regexp_replace(COALESCE(a.telephone_number, ''), '[^0-9]', '', 'g')) = 10
           AND LENGTH(regexp_replace(COALESCE(t.telephone_number, ''), '[^0-9]', '', 'g')) = 10
          JOIN fqhc_npis AS fq
            ON fq.npi = a.npi
         WHERE t.npi IS NULL
           AND t.inferred_npi IS NULL
           AND t.entity_type = 'facility_anchor'
           AND COALESCE(t.entity_subtype, '') = 'FQHC'
         GROUP BY t.entity_type, t.entity_id, t.type, t.checksum
    """
        if include_nppes_broad_inference
        else empty_inference_candidates_sql
    )

    npi_fqhc_clinic_phone_zip_candidates_sql = (
        f"""
        SELECT
            t.entity_type,
            t.entity_id,
            t.type,
            t.checksum,
            MIN(a.npi)::bigint AS candidate_npi,
            COUNT(DISTINCT a.npi)::int AS candidate_npi_count
          FROM unresolved_facility AS t
          JOIN {db_schema}.npi_address AS a
            ON a.type = 'primary'
           AND a.npi IS NOT NULL
           AND LEFT(COALESCE(a.postal_code, ''), 5) = LEFT(COALESCE(t.postal_code, ''), 5)
           AND regexp_replace(COALESCE(a.telephone_number, ''), '[^0-9]', '', 'g')
               = regexp_replace(COALESCE(t.telephone_number, ''), '[^0-9]', '', 'g')
           AND LENGTH(regexp_replace(COALESCE(a.telephone_number, ''), '[^0-9]', '', 'g')) = 10
           AND LENGTH(regexp_replace(COALESCE(t.telephone_number, ''), '[^0-9]', '', 'g')) = 10
          JOIN {db_schema}.npi AS n
            ON n.npi = a.npi
           AND n.entity_type_code = 2
          JOIN primary_taxonomy AS pt
            ON pt.npi = a.npi
           AND pt.taxonomy_code <> '261QF0400X'
          JOIN {db_schema}.nucc_taxonomy AS nu
            ON nu.code = pt.taxonomy_code
           AND COALESCE(nu.classification, '') = 'Clinic/Center'
         WHERE t.npi IS NULL
           AND t.inferred_npi IS NULL
           AND t.entity_type = 'facility_anchor'
           AND COALESCE(t.entity_subtype, '') = 'FQHC'
         GROUP BY t.entity_type, t.entity_id, t.type, t.checksum
    """
        if include_nppes_broad_inference
        else empty_inference_candidates_sql
    )

    name_fallback_target_filter = (
        f"""
         WHERE npi IS NULL
           AND inferred_npi IS NULL
           AND entity_type <> 'npi'
           AND COALESCE(entity_name, '') <> ''
           AND NOT EXISTS (
                SELECT 1
                  FROM preselected_winners AS pw
                 WHERE pw.entity_type = {db_schema}.{stage_table}.entity_type
                   AND pw.entity_id = {db_schema}.{stage_table}.entity_id
                   AND pw.type = {db_schema}.{stage_table}.type
                   AND pw.checksum = {db_schema}.{stage_table}.checksum
           )
        """
        if include_name_fallback
        else " WHERE FALSE"
    )

    return f"""
    WITH unresolved_facility AS MATERIALIZED (
        SELECT *
          FROM {db_schema}.{stage_table}
         WHERE npi IS NULL
           AND inferred_npi IS NULL
           AND entity_type = 'facility_anchor'
    ),
    facility_override_candidates AS (
        {facility_override_candidates_sql}
    ),
    facility_override_winners AS (
        SELECT
            entity_type,
            entity_id,
            type,
            checksum,
            candidate_npi,
            0::int AS winner_priority,
            1.0::double precision AS winner_confidence,
            'facility_anchor_npi_override'::varchar AS winner_method
          FROM facility_override_candidates
         WHERE candidate_npi_count = 1
    ),
    hospital_ccn_candidates AS (
        {hospital_ccn_candidates_sql}
    ),
    hospital_ccn_winners AS (
        SELECT
            entity_type,
            entity_id,
            type,
            checksum,
            candidate_npi,
            1::int AS winner_priority,
            0.99::double precision AS winner_confidence,
            'hospital_ccn_match'::varchar AS winner_method
          FROM hospital_ccn_candidates
         WHERE candidate_npi_count = 1
    ),
    hospital_nppes_name_address_candidates AS (
        {hospital_nppes_name_address_candidates_sql}
    ),
    hospital_other_identifier_candidates AS (
        {hospital_other_identifier_candidates_sql}
    ),
    hospital_other_identifier_winners AS (
        SELECT
            entity_type,
            entity_id,
            type,
            checksum,
            candidate_npi,
            3::int AS winner_priority,
            0.96::double precision AS winner_confidence,
            'hospital_nppes_other_identifier'::varchar AS winner_method
          FROM hospital_other_identifier_candidates
         WHERE candidate_npi_count = 1
    ),
    hospital_address_key_candidates AS (
        {hospital_address_key_candidates_sql}
    ),
    hospital_address_key_winners AS (
        SELECT
            entity_type,
            entity_id,
            type,
            checksum,
            candidate_npi,
            6::int AS winner_priority,
            0.93::double precision AS winner_confidence,
            'hospital_nppes_address_key'::varchar AS winner_method
          FROM hospital_address_key_candidates
         WHERE candidate_npi_count = 1
    ),
    hospital_nppes_name_address_winners AS (
        SELECT
            entity_type,
            entity_id,
            type,
            checksum,
            candidate_npi,
            5::int AS winner_priority,
            0.94::double precision AS winner_confidence,
            'hospital_nppes_name_address'::varchar AS winner_method
          FROM hospital_nppes_name_address_candidates
         WHERE candidate_npi_count = 1
    ),
    fqhc_exact_candidates AS (
        {fqhc_exact_candidates_sql}
    ),
    fqhc_exact_winners AS (
        SELECT
            entity_type,
            entity_id,
            type,
            checksum,
            candidate_npi,
            20::int AS winner_priority,
            0.97::double precision AS winner_confidence,
            'fqhc_enrollment_exact_match'::varchar AS winner_method
          FROM fqhc_exact_candidates
         WHERE candidate_npi_count = 1
    ),
    fqhc_ccn_candidates AS (
        {fqhc_ccn_candidates_sql}
    ),
    fqhc_ccn_winners AS (
        SELECT
            entity_type,
            entity_id,
            type,
            checksum,
            candidate_npi,
            10::int AS winner_priority,
            0.965::double precision AS winner_confidence,
            'fqhc_ccn_match'::varchar AS winner_method
          FROM fqhc_ccn_candidates
         WHERE candidate_npi_count = 1
    ),
    fqhc_other_identifier_candidates AS (
        {fqhc_other_identifier_candidates_sql}
    ),
    fqhc_other_identifier_winners AS (
        SELECT
            entity_type,
            entity_id,
            type,
            checksum,
            candidate_npi,
            15::int AS winner_priority,
            0.96::double precision AS winner_confidence,
            'fqhc_nppes_other_identifier'::varchar AS winner_method
          FROM fqhc_other_identifier_candidates
         WHERE candidate_npi_count = 1
    ),
    fqhc_parent_exact_candidates AS (
        {fqhc_parent_exact_candidates_sql}
    ),
    fqhc_parent_exact_winners AS (
        SELECT
            entity_type,
            entity_id,
            type,
            checksum,
            candidate_npi,
            25::int AS winner_priority,
            0.94::double precision AS winner_confidence,
            'fqhc_parent_enrollment_exact_address'::varchar AS winner_method
          FROM fqhc_parent_exact_candidates
         WHERE candidate_npi_count = 1
    ),
    fqhc_parent_name_state_candidates AS (
        {fqhc_parent_name_state_candidates_sql}
    ),
    fqhc_parent_name_state_winners AS (
        SELECT
            entity_type,
            entity_id,
            type,
            checksum,
            candidate_npi,
            37::int AS winner_priority,
            0.91::double precision AS winner_confidence,
            'fqhc_parent_enrollment_name_state'::varchar AS winner_method
          FROM fqhc_parent_name_state_candidates
         WHERE candidate_npi_count = 1
    ),
    fqhc_parent_address_candidates AS (
        {fqhc_parent_address_candidates_sql}
    ),
    fqhc_parent_address_winners AS (
        SELECT
            entity_type,
            entity_id,
            type,
            checksum,
            candidate_npi,
            38::int AS winner_priority,
            0.90::double precision AS winner_confidence,
            'fqhc_parent_enrollment_address'::varchar AS winner_method
          FROM fqhc_parent_address_candidates
         WHERE candidate_npi_count = 1
    ),
    fqhc_sibling_source_npi_candidates AS (
        {fqhc_sibling_source_npi_candidates_sql}
    ),
    fqhc_sibling_source_npi_winners AS (
        SELECT
            entity_type,
            entity_id,
            type,
            checksum,
            candidate_npi,
            39::int AS winner_priority,
            0.88::double precision AS winner_confidence,
            'fqhc_sibling_source_npi'::varchar AS winner_method
          FROM fqhc_sibling_source_npi_candidates
         WHERE candidate_npi_count = 1
    ),
    fqhc_enrollment_address_candidates AS (
        {fqhc_enrollment_address_candidates_sql}
    ),
    fqhc_enrollment_address_winners AS (
        SELECT
            entity_type,
            entity_id,
            type,
            checksum,
            candidate_npi,
            30::int AS winner_priority,
            0.955::double precision AS winner_confidence,
            'fqhc_enrollment_exact_address'::varchar AS winner_method
          FROM fqhc_enrollment_address_candidates
         WHERE candidate_npi_count = 1
    ),
    fqhc_enrollment_phone_zip_candidates AS (
        {fqhc_enrollment_phone_zip_candidates_sql}
    ),
    fqhc_enrollment_phone_zip_winners AS (
        SELECT
            entity_type,
            entity_id,
            type,
            checksum,
            candidate_npi,
            35::int AS winner_priority,
            0.93::double precision AS winner_confidence,
            'fqhc_enrollment_phone_zip'::varchar AS winner_method
          FROM fqhc_enrollment_phone_zip_candidates
         WHERE candidate_npi_count = 1
    ),
    fqhc_enrollment_phone_name_candidates AS (
        {fqhc_enrollment_phone_name_candidates_sql}
    ),
    fqhc_enrollment_phone_name_winners AS (
        SELECT
            entity_type,
            entity_id,
            type,
            checksum,
            candidate_npi,
            36::int AS winner_priority,
            0.94::double precision AS winner_confidence,
            'fqhc_enrollment_phone_name'::varchar AS winner_method
          FROM fqhc_enrollment_phone_name_candidates
         WHERE candidate_npi_count = 1
    ),
    primary_taxonomy AS (
        SELECT
            npi::bigint AS npi,
            healthcare_provider_taxonomy_code AS taxonomy_code
          FROM {db_schema}.npi_taxonomy
         WHERE healthcare_provider_primary_taxonomy_switch = 'Y'
    ),
    fqhc_npis AS (
        {fqhc_provider_npis_sql}
        UNION
        SELECT DISTINCT pt.npi
          FROM primary_taxonomy AS pt
          JOIN {db_schema}.nucc_taxonomy AS nu
            ON nu.code = pt.taxonomy_code
         WHERE pt.taxonomy_code = '261QF0400X'
            OR (
                COALESCE(nu.classification, '') = 'Clinic/Center'
                AND COALESCE(nu.specialization, '') ILIKE '%federally qualified health center%'
            )
    ),
    fqhc_parent_nppes_exact_primary_candidates AS (
        {fqhc_parent_nppes_exact_primary_candidates_sql}
    ),
    fqhc_parent_nppes_exact_primary_winners AS (
        SELECT
            entity_type,
            entity_id,
            type,
            checksum,
            candidate_npi,
            41::int AS winner_priority,
            0.885::double precision AS winner_confidence,
            'fqhc_parent_nppes_exact_address_primary'::varchar AS winner_method
          FROM fqhc_parent_nppes_exact_primary_candidates
         WHERE candidate_npi_count = 1
    ),
    fqhc_parent_nppes_exact_all_candidates AS (
        {fqhc_parent_nppes_exact_all_candidates_sql}
    ),
    fqhc_parent_nppes_exact_all_winners AS (
        SELECT
            entity_type,
            entity_id,
            type,
            checksum,
            candidate_npi,
            44::int AS winner_priority,
            0.865::double precision AS winner_confidence,
            'fqhc_parent_nppes_exact_address'::varchar AS winner_method
          FROM fqhc_parent_nppes_exact_all_candidates AS all_candidates
         WHERE candidate_npi_count = 1
           AND NOT EXISTS (
                SELECT 1
                  FROM fqhc_parent_nppes_exact_primary_candidates AS primary_candidates
                 WHERE primary_candidates.entity_type = all_candidates.entity_type
                   AND primary_candidates.entity_id = all_candidates.entity_id
                   AND primary_candidates.type = all_candidates.type
                   AND primary_candidates.checksum = all_candidates.checksum
           )
    ),
    fqhc_parent_nppes_name_state_primary_candidates AS (
        {fqhc_parent_nppes_name_state_primary_candidates_sql}
    ),
    fqhc_parent_nppes_name_state_primary_winners AS (
        SELECT
            entity_type,
            entity_id,
            type,
            checksum,
            candidate_npi,
            42::int AS winner_priority,
            0.875::double precision AS winner_confidence,
            'fqhc_parent_nppes_name_state_primary'::varchar AS winner_method
          FROM fqhc_parent_nppes_name_state_primary_candidates
         WHERE candidate_npi_count = 1
    ),
    fqhc_parent_nppes_name_state_all_candidates AS (
        {fqhc_parent_nppes_name_state_all_candidates_sql}
    ),
    fqhc_parent_nppes_name_state_all_winners AS (
        SELECT
            entity_type,
            entity_id,
            type,
            checksum,
            candidate_npi,
            45::int AS winner_priority,
            0.86::double precision AS winner_confidence,
            'fqhc_parent_nppes_name_state'::varchar AS winner_method
          FROM fqhc_parent_nppes_name_state_all_candidates AS all_candidates
         WHERE candidate_npi_count = 1
           AND NOT EXISTS (
                SELECT 1
                  FROM fqhc_parent_nppes_name_state_primary_candidates AS primary_candidates
                 WHERE primary_candidates.entity_type = all_candidates.entity_type
                   AND primary_candidates.entity_id = all_candidates.entity_id
                   AND primary_candidates.type = all_candidates.type
                   AND primary_candidates.checksum = all_candidates.checksum
           )
    ),
    fqhc_parent_nppes_address_primary_candidates AS (
        {fqhc_parent_nppes_address_primary_candidates_sql}
    ),
    fqhc_parent_nppes_address_primary_winners AS (
        SELECT
            entity_type,
            entity_id,
            type,
            checksum,
            candidate_npi,
            43::int AS winner_priority,
            0.87::double precision AS winner_confidence,
            'fqhc_parent_nppes_address_primary'::varchar AS winner_method
          FROM fqhc_parent_nppes_address_primary_candidates
         WHERE candidate_npi_count = 1
    ),
    fqhc_parent_nppes_address_all_candidates AS (
        {fqhc_parent_nppes_address_all_candidates_sql}
    ),
    fqhc_parent_nppes_address_all_winners AS (
        SELECT
            entity_type,
            entity_id,
            type,
            checksum,
            candidate_npi,
            46::int AS winner_priority,
            0.855::double precision AS winner_confidence,
            'fqhc_parent_nppes_address'::varchar AS winner_method
          FROM fqhc_parent_nppes_address_all_candidates AS all_candidates
         WHERE candidate_npi_count = 1
           AND NOT EXISTS (
                SELECT 1
                  FROM fqhc_parent_nppes_address_primary_candidates AS primary_candidates
                 WHERE primary_candidates.entity_type = all_candidates.entity_type
                   AND primary_candidates.entity_id = all_candidates.entity_id
                   AND primary_candidates.type = all_candidates.type
                   AND primary_candidates.checksum = all_candidates.checksum
           )
    ),
    npi_fqhc_exact_address_candidates AS (
        {npi_fqhc_exact_address_candidates_sql}
    ),
    npi_fqhc_exact_address_winners AS (
        SELECT
            entity_type,
            entity_id,
            type,
            checksum,
            candidate_npi,
            40::int AS winner_priority,
            0.945::double precision AS winner_confidence,
            'npi_fqhc_exact_address'::varchar AS winner_method
          FROM npi_fqhc_exact_address_candidates
         WHERE candidate_npi_count = 1
    ),
    npi_fqhc_address_key_candidates AS (
        {npi_fqhc_address_key_candidates_sql}
    ),
    npi_fqhc_address_key_winners AS (
        SELECT
            entity_type,
            entity_id,
            type,
            checksum,
            candidate_npi,
            32::int AS winner_priority,
            0.94::double precision AS winner_confidence,
            'npi_fqhc_address_key'::varchar AS winner_method
          FROM npi_fqhc_address_key_candidates
         WHERE candidate_npi_count = 1
    ),
    npi_fqhc_clinic_address_key_candidates AS (
        {npi_fqhc_clinic_address_key_candidates_sql}
    ),
    npi_fqhc_clinic_address_key_winners AS (
        SELECT
            entity_type,
            entity_id,
            type,
            checksum,
            candidate_npi,
            34::int AS winner_priority,
            0.90::double precision AS winner_confidence,
            'npi_fqhc_clinic_address_key'::varchar AS winner_method
          FROM npi_fqhc_clinic_address_key_candidates
         WHERE candidate_npi_count = 1
    ),
    npi_fqhc_phone_zip_candidates AS (
        {npi_fqhc_phone_zip_candidates_sql}
    ),
    npi_fqhc_phone_zip_winners AS (
        SELECT
            entity_type,
            entity_id,
            type,
            checksum,
            candidate_npi,
            50::int AS winner_priority,
            0.90::double precision AS winner_confidence,
            'npi_fqhc_phone_zip'::varchar AS winner_method
          FROM npi_fqhc_phone_zip_candidates
         WHERE candidate_npi_count = 1
    ),
    npi_fqhc_clinic_phone_zip_candidates AS (
        {npi_fqhc_clinic_phone_zip_candidates_sql}
    ),
    npi_fqhc_clinic_phone_zip_winners AS (
        SELECT
            entity_type,
            entity_id,
            type,
            checksum,
            candidate_npi,
            52::int AS winner_priority,
            0.87::double precision AS winner_confidence,
            'npi_fqhc_clinic_phone_zip'::varchar AS winner_method
          FROM npi_fqhc_clinic_phone_zip_candidates
         WHERE candidate_npi_count = 1
    ),
    strong_preselected_candidates AS (
        SELECT * FROM facility_override_winners
        UNION ALL
        SELECT * FROM hospital_ccn_winners
        UNION ALL
        SELECT * FROM hospital_other_identifier_winners
        UNION ALL
        SELECT * FROM hospital_nppes_name_address_winners
        UNION ALL
        SELECT * FROM hospital_address_key_winners
        UNION ALL
        SELECT * FROM fqhc_exact_winners
        UNION ALL
        SELECT * FROM fqhc_ccn_winners
        UNION ALL
        SELECT * FROM fqhc_other_identifier_winners
        UNION ALL
        SELECT * FROM fqhc_parent_exact_winners
        UNION ALL
        SELECT * FROM fqhc_enrollment_address_winners
        UNION ALL
        SELECT * FROM npi_fqhc_address_key_winners
        UNION ALL
        SELECT * FROM npi_fqhc_clinic_address_key_winners
        UNION ALL
        SELECT * FROM fqhc_enrollment_phone_zip_winners
        UNION ALL
        SELECT * FROM fqhc_enrollment_phone_name_winners
        UNION ALL
        SELECT * FROM fqhc_parent_name_state_winners
        UNION ALL
        SELECT * FROM fqhc_parent_address_winners
        UNION ALL
        SELECT * FROM npi_fqhc_exact_address_winners
        UNION ALL
        SELECT * FROM npi_fqhc_phone_zip_winners
        UNION ALL
        SELECT * FROM npi_fqhc_clinic_phone_zip_winners
    ),
    preselected_min_priorities AS (
        SELECT
            entity_type,
            entity_id,
            type,
            checksum,
            MIN(winner_priority)::int AS best_priority
          FROM strong_preselected_candidates
      GROUP BY entity_type, entity_id, type, checksum
    ),
    preselected_candidate_counts AS (
        SELECT
            candidates.entity_type,
            candidates.entity_id,
            candidates.type,
            candidates.checksum,
            priorities.best_priority,
            COUNT(DISTINCT candidate_npi)::int AS distinct_candidate_count
          FROM strong_preselected_candidates AS candidates
          JOIN preselected_min_priorities AS priorities
            ON priorities.entity_type = candidates.entity_type
           AND priorities.entity_id = candidates.entity_id
           AND priorities.type = candidates.type
           AND priorities.checksum = candidates.checksum
           AND priorities.best_priority = candidates.winner_priority
      GROUP BY candidates.entity_type, candidates.entity_id, candidates.type, candidates.checksum, priorities.best_priority
    ),
    preselected_ranked AS (
        SELECT
            pc.*,
            pcc.best_priority,
            pcc.distinct_candidate_count,
            ROW_NUMBER() OVER (
                PARTITION BY pc.entity_type, pc.entity_id, pc.type, pc.checksum
                ORDER BY winner_priority ASC, candidate_npi ASC
            ) AS preselected_rank
          FROM strong_preselected_candidates AS pc
          JOIN preselected_candidate_counts AS pcc
            ON pcc.entity_type = pc.entity_type
           AND pcc.entity_id = pc.entity_id
           AND pcc.type = pc.type
           AND pcc.checksum = pc.checksum
    ),
    strong_preselected_winners AS (
        SELECT
            entity_type,
            entity_id,
            type,
            checksum,
            candidate_npi,
            winner_confidence,
            winner_method
          FROM preselected_ranked
         WHERE winner_priority = best_priority
           AND distinct_candidate_count = 1
           AND preselected_rank = 1
    ),
    fallback_preselected_candidates AS (
        SELECT * FROM fqhc_parent_nppes_exact_primary_winners
        UNION ALL
        SELECT * FROM fqhc_parent_nppes_name_state_primary_winners
        UNION ALL
        SELECT * FROM fqhc_parent_nppes_address_primary_winners
        UNION ALL
        SELECT * FROM fqhc_parent_nppes_exact_all_winners
        UNION ALL
        SELECT * FROM fqhc_parent_nppes_name_state_all_winners
        UNION ALL
        SELECT * FROM fqhc_parent_nppes_address_all_winners
        UNION ALL
        SELECT * FROM fqhc_sibling_source_npi_winners
    ),
    fallback_preselected_candidate_counts AS (
        SELECT
            entity_type,
            entity_id,
            type,
            checksum,
            COUNT(DISTINCT candidate_npi)::int AS distinct_candidate_count
          FROM fallback_preselected_candidates
      GROUP BY entity_type, entity_id, type, checksum
    ),
    fallback_preselected_ranked AS (
        SELECT
            fallback.*,
            counts.distinct_candidate_count,
            ROW_NUMBER() OVER (
                PARTITION BY fallback.entity_type, fallback.entity_id, fallback.type, fallback.checksum
                ORDER BY winner_priority ASC, candidate_npi ASC
            ) AS fallback_rank
          FROM fallback_preselected_candidates AS fallback
          JOIN fallback_preselected_candidate_counts AS counts
            ON counts.entity_type = fallback.entity_type
           AND counts.entity_id = fallback.entity_id
           AND counts.type = fallback.type
           AND counts.checksum = fallback.checksum
    ),
    fallback_preselected_winners AS (
        SELECT
            entity_type,
            entity_id,
            type,
            checksum,
            candidate_npi,
            winner_confidence,
            winner_method
          FROM fallback_preselected_ranked AS fallback
         WHERE distinct_candidate_count = 1
           AND fallback_rank = 1
           AND NOT EXISTS (
                SELECT 1
                  FROM strong_preselected_candidates AS strong
                 WHERE strong.entity_type = fallback.entity_type
                   AND strong.entity_id = fallback.entity_id
                   AND strong.type = fallback.type
                   AND strong.checksum = fallback.checksum
           )
    ),
    preselected_winners AS (
        SELECT * FROM strong_preselected_winners
        UNION ALL
        SELECT * FROM fallback_preselected_winners
    ),
    target AS (
        SELECT
            entity_type,
            entity_id,
            type,
            checksum,
            entity_subtype AS facility_type,
            entity_name,
            first_line,
            state_name,
            LEFT(COALESCE(postal_code, ''), 5) AS zip5
          FROM {db_schema}.{stage_table}
         {name_fallback_target_filter}
    ),
    candidates AS (
        SELECT
            t.entity_type,
            t.entity_id,
            t.type,
            t.checksum,
            t.facility_type,
            t.entity_name AS source_entity_name,
            p.npi::bigint AS candidate_npi,
            CASE
                WHEN p.type = 'primary' THEN 0
                WHEN p.type = 'secondary' THEN 1
                WHEN p.type = 'practice' THEN 2
                ELSE 3
            END::int AS addr_type_rank,
            (
                CASE
                    WHEN regexp_replace(LOWER(COALESCE(t.first_line, '')), '[^a-z0-9]', '', 'g') <> ''
                     AND regexp_replace(LOWER(COALESCE(t.first_line, '')), '[^a-z0-9]', '', 'g')
                         = regexp_replace(LOWER(COALESCE(p.first_line, '')), '[^a-z0-9]', '', 'g')
                    THEN 1 ELSE 0
                END
            )::int AS street_exact,
            (
                CASE
                    WHEN regexp_replace(LOWER(COALESCE(t.first_line, '')), '[^a-z0-9]', '', 'g') <> ''
                     AND regexp_replace(LOWER(COALESCE(t.first_line, '')), '[^a-z0-9]', '', 'g')
                         = regexp_replace(LOWER(COALESCE(p.first_line, '')), '[^a-z0-9]', '', 'g')
                    THEN 2 ELSE 0
                END
                +
                CASE
                    WHEN t.zip5 <> ''
                     AND t.zip5 = LEFT(COALESCE(p.postal_code, ''), 5)
                    THEN 1 ELSE 0
                END
                +
                CASE
                    WHEN COALESCE(t.state_name, '') <> ''
                     AND UPPER(t.state_name) = UPPER(COALESCE(p.state_name, ''))
                    THEN 1 ELSE 0
                END
            )::int AS match_score
          FROM target AS t
          JOIN {db_schema}.npi AS n
            ON regexp_replace(LOWER(COALESCE(t.entity_name, '')), '[^a-z0-9]', '', 'g')
               = regexp_replace(
                    LOWER(
                        COALESCE(NULLIF(n.provider_organization_name, ''), '')
                        || ' ' || COALESCE(NULLIF(n.provider_other_organization_name, ''), '')
                    ),
                    '[^a-z0-9]',
                    '',
                    'g'
               )
          JOIN {db_schema}.npi_address AS p
            ON p.npi = n.npi
           AND p.type IN ('primary', 'secondary', 'practice')
           AND (
                (t.zip5 <> '' AND t.zip5 = LEFT(COALESCE(p.postal_code, ''), 5))
                OR (
                    COALESCE(t.state_name, '') <> ''
                    AND UPPER(t.state_name) = UPPER(COALESCE(p.state_name, ''))
                )
           )
    ),
    candidates_ranked_per_npi AS (
        SELECT
            entity_type,
            entity_id,
            type,
            checksum,
            facility_type,
            source_entity_name,
            candidate_npi,
            match_score,
            street_exact,
            addr_type_rank,
            ROW_NUMBER() OVER (
                PARTITION BY entity_type, entity_id, type, checksum, candidate_npi
                ORDER BY street_exact DESC, match_score DESC, addr_type_rank ASC, candidate_npi
            ) AS candidate_row_rank
          FROM candidates
         WHERE match_score >= 2
    ),
    candidates_by_npi AS (
        SELECT
            entity_type,
            entity_id,
            type,
            checksum,
            facility_type,
            source_entity_name,
            candidate_npi,
            match_score,
            street_exact,
            addr_type_rank
          FROM candidates_ranked_per_npi
         WHERE candidate_row_rank = 1
    ),
    candidate_enriched AS (
        SELECT
            c.entity_type,
            c.entity_id,
            c.type,
            c.checksum,
            c.facility_type,
            c.candidate_npi,
            COALESCE(np.provider_organization_name, '') AS candidate_org_name,
            COALESCE(np.do_business_as_text, '') AS candidate_dba_name,
            COALESCE(np.entity_type_code, 0)::int AS candidate_entity_type_code,
            np.provider_enumeration_date AS candidate_enumeration_date,
            np.npi_deactivation_date AS candidate_deactivation_date,
            c.match_score,
            c.street_exact,
            c.addr_type_rank,
            COALESCE(pes.has_hospital_enrollment, FALSE) AS has_hospital_enrollment,
            COALESCE(pes.has_fqhc_enrollment, FALSE) AS has_fqhc_enrollment,
            pt.taxonomy_code,
            COALESCE(nu.classification, '') AS taxonomy_classification,
            COALESCE(nu.specialization, '') AS taxonomy_specialization,
            CASE
                WHEN c.entity_type = 'facility_anchor'
                 AND COALESCE(c.facility_type, '') = 'Hospital'
                THEN CASE
                    WHEN COALESCE(pes.has_hospital_enrollment, FALSE) THEN 3
                    WHEN pt.taxonomy_code = '282N00000X'
                      OR COALESCE(nu.classification, '') = 'General Acute Care Hospital' THEN 2
                    ELSE 1
                END
                WHEN c.entity_type = 'facility_anchor'
                 AND COALESCE(c.facility_type, '') = 'FQHC'
                THEN CASE
                    WHEN COALESCE(pes.has_fqhc_enrollment, FALSE) THEN 3
                    WHEN pt.taxonomy_code = '261QF0400X'
                      OR (
                          COALESCE(nu.classification, '') = 'Clinic/Center'
                          AND COALESCE(nu.specialization, '') ILIKE '%federally qualified health center%'
                      ) THEN 2
                    ELSE 1
                END
                ELSE 0
            END::int AS facility_tier,
            CASE
                WHEN c.entity_type = 'facility_anchor'
                 AND COALESCE(c.facility_type, '') = 'Hospital'
                THEN CASE
                    WHEN pt.taxonomy_code = '282N00000X'
                      OR COALESCE(nu.classification, '') = 'General Acute Care Hospital' THEN 4
                    WHEN COALESCE(nu.classification, '') ILIKE '%hospital%' THEN 3
                    WHEN COALESCE(nu.classification, '') ILIKE '%unit%' THEN 1
                    ELSE 0
                END
                WHEN c.entity_type = 'facility_anchor'
                 AND COALESCE(c.facility_type, '') = 'FQHC'
                THEN CASE
                    WHEN pt.taxonomy_code = '261QF0400X'
                      OR (
                          COALESCE(nu.classification, '') = 'Clinic/Center'
                          AND COALESCE(nu.specialization, '') ILIKE '%federally qualified health center%'
                      ) THEN 2
                    ELSE 0
                END
                ELSE 0
            END::int AS facility_subtype_rank
          FROM candidates_by_npi AS c
          LEFT JOIN {db_schema}.npi AS np
            ON np.npi = c.candidate_npi
          LEFT JOIN {db_schema}.provider_enrichment_summary AS pes
            ON pes.npi = c.candidate_npi
          LEFT JOIN primary_taxonomy AS pt
            ON pt.npi = c.candidate_npi
          LEFT JOIN {db_schema}.nucc_taxonomy AS nu
            ON nu.code = pt.taxonomy_code
    ),
    ranked AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY entity_type, entity_id, type, checksum
                ORDER BY match_score DESC, candidate_npi
            ) AS rn,
            COUNT(*) OVER (
                PARTITION BY entity_type, entity_id, type, checksum
            ) AS candidate_count,
            DENSE_RANK() OVER (
                PARTITION BY entity_type, entity_id, type, checksum
                ORDER BY facility_tier DESC, facility_subtype_rank DESC, street_exact DESC, match_score DESC, addr_type_rank ASC
            ) AS facility_rank
          FROM candidate_enriched
    ),
    ranked_with_tie_count AS (
        SELECT
            *,
            COUNT(*) OVER (
                PARTITION BY entity_type, entity_id, type, checksum, facility_rank
            ) AS facility_rank_count,
            ROW_NUMBER() OVER (
                PARTITION BY entity_type, entity_id, type, checksum, facility_rank
                ORDER BY
                    CASE WHEN candidate_entity_type_code = 2 THEN 1 ELSE 0 END DESC,
                    CASE WHEN candidate_deactivation_date IS NULL THEN 1 ELSE 0 END DESC,
                    candidate_enumeration_date ASC NULLS LAST,
                    candidate_npi ASC
            ) AS facility_deterministic_rank
          FROM ranked
    ),
    ranked_org_counts AS (
        SELECT
            entity_type,
            entity_id,
            type,
            checksum,
            facility_rank,
            COUNT(DISTINCT regexp_replace(LOWER(COALESCE(candidate_org_name, '')), '[^a-z0-9]', '', 'g')) AS facility_rank_org_count
          FROM ranked_with_tie_count
         GROUP BY entity_type, entity_id, type, checksum, facility_rank
    ),
    ranked_with_org_count AS (
        SELECT
            r.*,
            COALESCE(o.facility_rank_org_count, 0)::int AS facility_rank_org_count
          FROM ranked_with_tie_count AS r
          LEFT JOIN ranked_org_counts AS o
            ON o.entity_type = r.entity_type
           AND o.entity_id = r.entity_id
           AND o.type = r.type
           AND o.checksum = r.checksum
           AND o.facility_rank = r.facility_rank
    ),
    ranked_winners AS (
        SELECT
            r.entity_type,
            r.entity_id,
            r.type,
            r.checksum,
            r.candidate_npi,
            CASE
                WHEN r.candidate_count = 1 AND r.match_score >= 3 THEN 0.95
                WHEN r.candidate_count = 1 THEN 0.85
                WHEN r.entity_type = 'facility_anchor'
                 AND r.facility_rank = 1
                 AND r.facility_rank_count = 1
                 AND r.facility_tier >= 3
                 AND r.match_score >= 4 THEN 0.96
                WHEN r.entity_type = 'facility_anchor'
                 AND r.facility_rank = 1
                 AND r.facility_rank_count = 1
                 AND r.facility_tier >= 3
                 AND r.match_score >= 3 THEN 0.94
                WHEN r.entity_type = 'facility_anchor'
                 AND r.facility_rank = 1
                 AND r.facility_rank_count = 1
                 AND r.facility_tier >= 2 THEN 0.90
                WHEN r.entity_type = 'facility_anchor'
                 AND COALESCE(r.facility_type, '') = 'FQHC'
                 AND r.facility_rank = 1
                 AND r.facility_rank_count > 1
                 AND r.facility_rank_org_count = 1
                 AND r.facility_tier >= 3
                 AND r.facility_deterministic_rank = 1 THEN 0.88
                WHEN r.entity_type = 'facility_anchor'
                 AND COALESCE(r.facility_type, '') = 'Hospital'
                 AND r.facility_rank = 1
                 AND r.facility_rank_count > 1
                 AND r.facility_rank_org_count = 1
                 AND r.facility_tier >= 3
                 AND r.facility_deterministic_rank = 1 THEN 0.89
                ELSE 0.85
            END::double precision AS winner_confidence,
            CASE
                WHEN r.candidate_count = 1 THEN 'name_zip_street_match'
                WHEN r.entity_type = 'facility_anchor'
                 AND COALESCE(r.facility_type, '') = 'FQHC'
                 AND r.facility_rank = 1
                 AND r.facility_rank_count > 1
                 AND r.facility_rank_org_count = 1
                 AND r.facility_tier >= 3
                 AND r.facility_deterministic_rank = 1 THEN 'name_zip_street_facility_rank_deterministic'
                WHEN r.entity_type = 'facility_anchor'
                 AND COALESCE(r.facility_type, '') = 'Hospital'
                 AND r.facility_rank = 1
                 AND r.facility_rank_count > 1
                 AND r.facility_rank_org_count = 1
                 AND r.facility_tier >= 3
                 AND r.facility_deterministic_rank = 1 THEN 'name_zip_street_facility_rank_deterministic'
                ELSE 'name_zip_street_facility_rank'
            END::varchar AS winner_method
          FROM ranked_with_org_count AS r
         WHERE (
                 (r.rn = 1 AND r.candidate_count = 1)
                 OR (
                     r.entity_type = 'facility_anchor'
                     AND r.facility_rank = 1
                     AND r.facility_rank_count = 1
                     AND r.facility_tier >= 2
                 )
                 OR (
                     r.entity_type = 'facility_anchor'
                     AND COALESCE(r.facility_type, '') = 'FQHC'
                     AND r.facility_rank = 1
                     AND r.facility_rank_count > 1
                     AND r.facility_rank_org_count = 1
                     AND r.facility_tier >= 3
                     AND r.facility_deterministic_rank = 1
                 )
                 OR (
                     r.entity_type = 'facility_anchor'
                     AND COALESCE(r.facility_type, '') = 'Hospital'
                     AND r.facility_rank = 1
                     AND r.facility_rank_count > 1
                     AND r.facility_rank_org_count = 1
                     AND r.facility_tier >= 3
                     AND r.facility_deterministic_rank = 1
                 )
               )
    ),
    inference_winners AS (
        SELECT * FROM preselected_winners
        UNION ALL
        SELECT * FROM ranked_winners
    )
    UPDATE {db_schema}.{stage_table} AS t
       SET inferred_npi = w.candidate_npi,
           inference_confidence = w.winner_confidence,
           inference_method = w.winner_method
      FROM inference_winners AS w
     WHERE t.entity_type = w.entity_type
       AND t.entity_id = w.entity_id
       AND t.type = w.type
       AND t.checksum = w.checksum
       AND t.npi IS NULL
       AND t.inferred_npi IS NULL;
    """


async def process_entity_address_unified_data(ctx, task=None):
    """Materialize and optionally publish the unified entity-address dataset."""
    task = task or {}
    ctx.setdefault("context", {})
    context = ctx["context"]
    run_id = str(context.get("control_run_id") or ctx.get("control_run_id") or "").strip()

    if "test_mode" in task:
        context["test_mode"] = bool(task.get("test_mode"))
    test_mode = bool(context.get("test_mode", False))
    context["publish_requested"] = _is_publish_requested(task, test_mode=test_mode)
    refresh_mode = _entity_address_refresh_mode(task)
    is_partial_provider_directory_refresh = (
        refresh_mode == ENTITY_ADDRESS_REFRESH_MODE_PROVIDER_DIRECTORY_PARTIAL
    )
    partial_source_refresh = is_partial_provider_directory_refresh
    provider_directory_partial_scope = (
        _entity_address_provider_directory_partial_scope(task)
        if is_partial_provider_directory_refresh
        else None
    )
    provider_directory_source_ids = (
        _entity_address_provider_directory_source_ids(task)
        if is_partial_provider_directory_refresh
        else []
    )
    provider_directory_run_id = (
        _entity_address_provider_directory_run_id(task)
        if is_partial_provider_directory_refresh
        else None
    )
    provider_directory_dataset_id = (
        _entity_address_provider_directory_dataset_id(task)
        if is_partial_provider_directory_refresh
        else None
    )
    provider_directory_source_batch_size = (
        _provider_directory_source_batch_size(task)
        if is_partial_provider_directory_refresh
        else 0
    )
    provider_directory_scope_sources: list[str] = []
    context["refresh_mode"] = refresh_mode
    context["partial_provider_directory_refresh"] = is_partial_provider_directory_refresh
    context["partial_provider_directory_scope"] = provider_directory_partial_scope
    context["partial_provider_directory_dataset_id"] = (
        provider_directory_dataset_id
    )
    _validate_provider_directory_dataset_fence_scope(
        dataset_id=provider_directory_dataset_id,
        source_ids=provider_directory_source_ids,
        run_id=provider_directory_run_id,
        partial_scope=provider_directory_partial_scope,
    )
    should_aggregate_source_record_ids = _should_aggregate_source_record_ids()
    context["aggregate_source_record_ids"] = should_aggregate_source_record_ids
    serving_only_refresh = is_partial_provider_directory_refresh or (
        not partial_source_refresh
        and _is_task_or_env_enabled(
            task,
            "serving_only_refresh",
            "HLTHPRT_ENTITY_ADDRESS_UNIFIED_SERVING_ONLY",
            DEFAULT_SERVING_ONLY_REFRESH,
        )
    )
    context["serving_only_refresh"] = serving_only_refresh
    context["support_stage_skipped"] = False

    await ensure_database(test_mode)

    import_date = ctx["import_date"]
    db_schema = os.getenv("HLTHPRT_DB_SCHEMA") if os.getenv("HLTHPRT_DB_SCHEMA") else "mrf"
    stage_cls = make_class(EntityAddressUnified, import_date)
    stage_table = stage_cls.__tablename__
    should_reuse_stage = _is_env_enabled("HLTHPRT_ENTITY_ADDRESS_UNIFIED_REUSE_STAGE", False)
    if partial_source_refresh and should_reuse_stage:
        raise RuntimeError(
            f"entity-address-unified {refresh_mode} refresh cannot be combined with "
            "HLTHPRT_ENTITY_ADDRESS_UNIFIED_REUSE_STAGE."
        )

    if context["publish_requested"] and not context.get(
        "cutover_dependency_preflight"
    ):
        live_models = (
            (EntityAddressUnified,)
            if serving_only_refresh
            else (EntityAddressUnified, *SUPPORT_TABLE_MODELS)
        )
        await _assert_cutover_has_no_dependent_views(
            db_schema,
            [model.__main_table__ for model in live_models],
        )
        context["cutover_dependency_preflight"] = True

    if not ctx["context"].get("stage_prepared"):
        await _ensure_schema_exists(db_schema)
        should_use_unlogged_stage = (
            not should_reuse_stage
            and _is_env_enabled(
                "HLTHPRT_ENTITY_ADDRESS_UNIFIED_UNLOGGED_STAGE",
                DEFAULT_UNLOGGED_STAGE,
            )
        )
        context["unlogged_stage"] = should_use_unlogged_stage
        if should_reuse_stage:
            if not await _has_table(db_schema, stage_table):
                raise RuntimeError(
                    f"HLTHPRT_ENTITY_ADDRESS_UNIFIED_REUSE_STAGE requested, "
                    f"but {db_schema}.{stage_table} does not exist"
                )
            await _ensure_entity_address_unified_live_columns(
                db_schema,
                stage_table,
            )
            context["stage_reused"] = True
        else:
            await db.status(f"DROP TABLE IF EXISTS {db_schema}.{stage_table};")
            await db.create_table(stage_cls.__table__, checkfirst=True)
            if should_use_unlogged_stage:
                await db.status(_set_unlogged_table_sql(db_schema, stage_table))
            await db.status(_disable_autovacuum_sql(db_schema, stage_table))
        ctx["context"]["stage_prepared"] = True
        ctx["context"]["stage_indexes_prepared"] = False
        ctx["context"]["support_stage_prepared"] = False
        ctx["context"]["support_stage_indexes_prepared"] = False
        ctx["context"]["support_stage_populated"] = False
        ctx["context"]["support_counts"] = {}
        ctx["context"]["hot_row_source_record_ids_compacted"] = False

    required_checks = [
        "npi",
        "npi_address",
        "npi_taxonomy",
        "nucc_taxonomy",
        "npi_other_identifier",
        "provider_enrollment_hospital",
        "provider_enrollment_fqhc",
        "provider_enrollment_ffs_additional_npi",
        "doctor_clinician_address",
        "geo_zip_lookup",
        "provider_enrollment_ffs",
        "provider_enrollment_ffs_address",
        "facility_anchor",
        "facility_anchor_npi_override",
        "facility_anchor_npi_candidate",
        "mrf_address",
        "provider_directory_practitioner",
        "provider_directory_organization",
        "provider_directory_location",
        "provider_directory_practitioner_role",
        "provider_directory_insurance_plan",
        "provider_directory_network_catalog",
        "provider_directory_healthcare_service",
        "provider_directory_organization_affiliation",
        "provider_directory_source",
        "provider_directory_address_overlay",
        "provider_directory_endpoint_dataset",
        "provider_directory_dataset_resource",
        "address_archive_v2",
        address_alias_sql.ADDRESS_ALIAS_TABLE,
        address_alias_sql.ADDRESS_ALIAS_STATE_TABLE,
        geo_projection.GEO_ASSURANCE_STATE_TABLE,
    ]
    available_relation_map = {table: await _has_table(db_schema, table) for table in required_checks}
    if not available_relation_map.get(geo_projection.GEO_ASSURANCE_STATE_TABLE):
        raise RuntimeError(
            "entity-address-unified requires migrated geo assurance schema"
        )
    if not available_relation_map.get(address_alias_sql.ADDRESS_ALIAS_TABLE) or not (
        available_relation_map.get(address_alias_sql.ADDRESS_ALIAS_STATE_TABLE)
    ):
        raise RuntimeError("entity-address-unified requires migrated address alias schema")
    missing_geo_reference_relations = [
        f"{db_schema}.{table_name}"
        for table_name in (
            "npi_address",
            "mrf_address",
            "doctor_clinician_address",
            "geo_zip_lookup",
        )
        if not available_relation_map.get(table_name)
    ]
    for relation_schema, table_name in (
        ("tiger", "zip_state"),
        ("tiger", "zcta5"),
    ):
        if not await _has_table(relation_schema, table_name):
            missing_geo_reference_relations.append(
                f"{relation_schema}.{table_name}"
            )
    if missing_geo_reference_relations:
        raise RuntimeError(
            "entity-address-unified requires geo assurance relations: "
            f"{', '.join(missing_geo_reference_relations)}"
        )
    alias_generation = await _address_alias_generation(db_schema)
    alias_base_address_version = f"{ALIAS_BASE_ADDRESS_VERSION_PREFIX}{alias_generation}"
    context["address_alias_generation"] = alias_generation
    context["base_address_version"] = alias_base_address_version
    if (is_partial_provider_directory_refresh or should_reuse_stage) and await _has_table(
        db_schema,
        stage_table if should_reuse_stage else EntityAddressUnified.__tablename__,
    ):
        version_table = (
            stage_table if should_reuse_stage else EntityAddressUnified.__tablename__
        )
        stale_versions = int(
            await db.scalar(
                f"""
                SELECT count(*)
                FROM {db_schema}.{version_table}
                WHERE base_address_version IS DISTINCT FROM :base_address_version;
                """,
                base_address_version=alias_base_address_version,
            )
            or 0
        )
        if stale_versions:
            raise RuntimeError(
                "address alias generation changed; a full entity-address-unified "
                "rebuild is required"
            )
    for table_name in (
        "npi_address",
        "doctor_clinician_address",
        "provider_enrollment_ffs_address",
        "facility_anchor",
        "mrf_address",
        "provider_directory_location",
    ):
        available_relation_map[f"{table_name}.address_key"] = (
            await _has_table_column(db_schema, table_name, "address_key")
            if available_relation_map.get(table_name)
            else False
        )
    available_relation_map["facility_anchor.medicare_ccn"] = (
        await _has_table_column(db_schema, "facility_anchor", "medicare_ccn")
        if available_relation_map.get("facility_anchor")
        else False
    )
    approved_candidate_promotions = 0
    if available_relation_map.get("facility_anchor_npi_candidate") and available_relation_map.get("facility_anchor_npi_override"):
        approved_candidate_promotions = await _promote_approved_facility_anchor_npi_candidates(db_schema)
        context["facility_anchor_npi_candidate_promotions"] = approved_candidate_promotions
        if run_id and approved_candidate_promotions:
            enqueue_live_progress(
                run_id=run_id,
                importer="entity-address-unified",
                status="running",
                phase="entity-address-unified promoting reviewed NPI candidates",
                unit="rows",
                done=approved_candidate_promotions,
                total=approved_candidate_promotions,
                message=f"promoted {approved_candidate_promotions} approved facility-anchor NPI candidates",
            )
    test_limit_per_source: int | None = None
    limit_any_mode_raw = (
        task.get("limit_per_source")
        if task.get("limit_per_source") not in (None, "")
        else task.get("source_limit")
    )
    if limit_any_mode_raw in (None, ""):
        limit_any_mode_raw = os.getenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_LIMIT_PER_SOURCE")
    if limit_any_mode_raw not in (None, ""):
        test_limit_per_source = max(int(limit_any_mode_raw), 0)
    elif test_mode:
        test_limit_per_source = int(
            os.getenv(
                "HLTHPRT_ENTITY_ADDRESS_UNIFIED_TEST_LIMIT_PER_SOURCE",
                str(DEFAULT_TEST_LIMIT_PER_SOURCE),
            )
        )
    context["limit_per_source"] = test_limit_per_source
    missing_fence_relations = _missing_provider_directory_fence_relations(available_relation_map)
    has_compatibility_data = False
    if missing_fence_relations:
        if is_partial_provider_directory_refresh:
            _validate_provider_directory_fence(
                available_relation_map,
                has_compatibility_data=False,
                partial_refresh=True,
            )
        else:
            has_compatibility_data = await _has_provider_directory_compatibility_data(
                db_schema,
                available_relation_map,
            )
    if is_partial_provider_directory_refresh:
        await _preflight_provider_directory_partial_scope_index(db_schema)
    if provider_directory_dataset_id is not None:
        await _assert_current_provider_directory_dataset(
            db_schema,
            source_id=provider_directory_source_ids[0],
            expected_dataset_id=provider_directory_dataset_id,
            expected_root_run_id=provider_directory_run_id,
        )
    if (
        is_partial_provider_directory_refresh
        and provider_directory_partial_scope == "latest-run"
        and not provider_directory_source_ids
        and not provider_directory_run_id
    ):
        (
            provider_directory_run_id,
            provider_directory_source_ids,
            provider_directory_scope_sources,
        ) = (
            await _latest_provider_directory_partial_scope(db_schema)
        )
    if is_partial_provider_directory_refresh and provider_directory_partial_scope == "latest-run":
        if not provider_directory_source_ids and not provider_directory_run_id:
            raise RuntimeError(
                "entity-address-unified provider-directory-partial refresh could not "
                "discover a completed Provider Directory FHIR source scope; pass "
                "provider_directory_source_ids/provider_directory_run_id explicitly or "
                "provider_directory_partial_scope=all to run an unscoped refresh."
            )
    context["partial_provider_directory_run_id"] = provider_directory_run_id
    context["partial_provider_directory_source_ids"] = provider_directory_source_ids
    context["partial_provider_directory_source_count"] = len(provider_directory_source_ids)
    context["partial_provider_directory_scope_sources"] = provider_directory_scope_sources
    provider_directory_source_batches = (
        _provider_directory_source_id_batches(
            provider_directory_source_ids,
            provider_directory_source_batch_size,
        )
        if is_partial_provider_directory_refresh
        else [provider_directory_source_ids]
    )
    context["partial_provider_directory_source_batch_size"] = provider_directory_source_batch_size
    context["partial_provider_directory_source_batches"] = len(provider_directory_source_batches)
    is_address_canon_available = await _is_address_canon_available(db_schema)
    base_source_selects = _source_selects(
        db_schema,
        available_relation_map,
        test_limit_per_source=test_limit_per_source,
        is_address_canon_available=is_address_canon_available,
    )
    source_selects = _current_provider_directory_source_selects(
        db_schema,
        available_relation_map,
        base_source_selects,
        source_ids=(provider_directory_source_ids if is_partial_provider_directory_refresh else None),
        run_id=(provider_directory_run_id if is_partial_provider_directory_refresh else None),
        test_limit_per_source=test_limit_per_source,
        has_compatibility_data=has_compatibility_data,
        partial_refresh=is_partial_provider_directory_refresh,
    )
    await _capture_provider_directory_overlay_alias_fence(
        db_schema,
        source_selects,
        context,
    )
    affected_group_table: str | None = None
    if is_partial_provider_directory_refresh:
        if not await _has_table(db_schema, EntityAddressUnified.__main_table__):
            raise RuntimeError(
                "entity-address-unified provider-directory-partial refresh requested, but the live "
                "entity_address_unified table does not exist; run refresh_mode=full first."
            )
        affected_group_table = _partial_affected_group_table(stage_table)
        context["partial_provider_directory_affected_group_table"] = affected_group_table
        context["partial_affected_group_table"] = affected_group_table
        await db.status(f"DROP TABLE IF EXISTS {db_schema}.{affected_group_table};")
        await _run_sql_phase(
            _prepare_partial_affected_groups_sql(
                db_schema,
                affected_group_table,
                source_selects,
                source_ids=provider_directory_source_ids,
            ),
            context=context,
            run_id=run_id,
            phase="entity-address-unified preparing affected Provider Directory groups",
            unit="tables",
            done=0,
            total=1,
            message="preparing affected Provider Directory evidence groups",
            emit_start=True,
            emit_done=True,
        )
        await _run_sql_phase(
            _index_partial_affected_groups_sql(db_schema, affected_group_table),
            context=context,
            run_id=run_id,
            phase="entity-address-unified indexing affected Provider Directory groups",
            unit="indexes",
            done=0,
            total=1,
            message="indexing affected Provider Directory evidence groups",
            emit_start=True,
            emit_done=True,
        )
        await _run_sql_phase(
            f"ANALYZE {db_schema}.{affected_group_table};",
            context=context,
            run_id=run_id,
            phase="entity-address-unified analyzing affected Provider Directory groups",
            unit="tables",
            done=0,
            total=1,
            message="analyzing affected Provider Directory evidence groups",
            emit_start=True,
            emit_done=True,
        )
        affected_group_rows = int(
            await db.scalar(f"SELECT COUNT(*) FROM {db_schema}.{affected_group_table};") or 0
        )
        context["partial_provider_directory_affected_groups"] = affected_group_rows
        source_selects = _provider_directory_partial_replacement_source_selects(
            db_schema,
            available_relation_map,
            base_source_selects,
            affected_group_table=affected_group_table,
            test_limit_per_source=test_limit_per_source,
            has_compatibility_data=has_compatibility_data,
        )
        if not source_selects:
            raise RuntimeError(
                "entity-address-unified provider-directory-partial refresh requires "
                "available Provider Directory source tables."
            )
        context["partial_provider_directory_source_selects"] = len(source_selects)
        context["partial_provider_directory_replacement_publish"] = True
        context["partial_provider_directory_main_patch_publish"] = False
        context["partial_main_patch_publish"] = False
    source_table_shards = _env_int(
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_SOURCE_TABLE_SHARDS",
        DEFAULT_SOURCE_TABLE_SHARDS,
        minimum=1,
    )
    context["source_table_shards"] = source_table_shards
    if source_table_shards > 1 and not test_limit_per_source and not partial_source_refresh:
        source_selects = _shard_source_selects(
            db_schema,
            source_selects,
            npi_address_ranges=(
                await _npi_table_ranges(db_schema, "npi_address", source_table_shards)
                if available_relation_map.get("npi_address", False)
                else []
            ),
            mrf_address_ranges=(
                await _npi_table_ranges(db_schema, "mrf_address", source_table_shards)
                if available_relation_map.get("mrf_address", False)
                else []
            ),
            doctor_clinician_address_ranges=(
                await _npi_table_ranges(db_schema, "doctor_clinician_address", source_table_shards)
                if available_relation_map.get("doctor_clinician_address", False)
                else []
            ),
            provider_enrollment_ffs_ranges=(
                await _npi_table_ranges(db_schema, "provider_enrollment_ffs", source_table_shards)
                if available_relation_map.get("provider_enrollment_ffs", False)
                else []
            ),
        )
    context["source_select_count"] = len(source_selects)
    if not is_address_canon_available:
        message = (
            "canonical address SQL functions are not available; "
            "entity_address_unified will publish with NULL address_key values"
        )
        if run_id:
            enqueue_live_progress(
                run_id=run_id,
                importer="entity-address-unified",
                status="warning",
                phase="entity-address-unified canonical unavailable",
                unit="address_key",
                done=0,
                total=1,
                pct=0,
                message=message,
            )
        logger.warning(
            "Canonical address SQL functions are not available in schema %s; "
            "entity_address_unified will publish with NULL address_key values.",
            db_schema,
        )
    if not source_selects:
        raise RuntimeError("No source tables are available for entity_address_unified materialization.")

    if serving_only_refresh:
        support_stage_class_map = {}
        ctx["context"]["support_stage_prepared"] = False
        ctx["context"]["support_stage_indexes_prepared"] = True
    elif not ctx["context"].get("support_stage_prepared"):
        support_stage_class_map = await _prepare_support_stage_tables(db_schema, import_date)
        ctx["context"]["support_stage_prepared"] = True
        ctx["context"]["support_stage_indexes_prepared"] = False
    else:
        support_stage_class_map = _support_stage_classes(import_date)

    should_build_network_bridge = _is_env_enabled(
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_BUILD_NETWORK_BRIDGE",
        DEFAULT_BUILD_NETWORK_BRIDGE,
    )
    context["build_network_bridge"] = should_build_network_bridge
    context.setdefault("partial_support_patch_publish", False)
    context.setdefault("partial_main_patch_publish", False)

    if run_id:
        enqueue_live_progress(
            run_id=run_id,
            importer="entity-address-unified",
            status="running",
            phase="entity-address-unified sources discovered",
            unit="sources",
            done=0,
            total=len(source_selects),
            message=f"{len(source_selects)} sources discovered",
        )

    should_use_chunked_load = _is_env_enabled("HLTHPRT_ENTITY_ADDRESS_UNIFIED_CHUNKED_LOAD", True)
    if not should_use_chunked_load:
        active_alias_count = int(
            await db.scalar(
                f"""
                SELECT count(*)
                FROM {db_schema}.{address_alias_sql.ADDRESS_ALIAS_TABLE}
                WHERE revoked_at IS NULL;
                """
            )
            or 0
        )
        if active_alias_count:
            raise RuntimeError(
                "active address aliases require chunked entity-address-unified enrichment"
            )
    if partial_source_refresh and not should_use_chunked_load:
        raise RuntimeError(
            f"entity-address-unified {refresh_mode} refresh requires "
            "HLTHPRT_ENTITY_ADDRESS_UNIFIED_CHUNKED_LOAD=true."
        )
    raw_table: str | None = None
    include_inline_source_evidence = False
    if should_use_chunked_load and not should_reuse_stage:
        source_concurrency = _env_int(
            "HLTHPRT_ENTITY_ADDRESS_UNIFIED_SOURCE_CONCURRENCY",
            DEFAULT_SOURCE_CONCURRENCY,
            minimum=1,
        )
        context["source_concurrency"] = source_concurrency
        aggregate_shards = _env_int(
            "HLTHPRT_ENTITY_ADDRESS_UNIFIED_AGGREGATE_SHARDS",
            DEFAULT_AGGREGATE_SHARDS,
            minimum=1,
        )
        context["aggregate_shards"] = aggregate_shards
        context["raw_group_index_profile"] = _raw_group_index_profile()
        aggregate_concurrency = min(
            _env_int(
                "HLTHPRT_ENTITY_ADDRESS_UNIFIED_AGGREGATE_CONCURRENCY",
                DEFAULT_AGGREGATE_CONCURRENCY,
                minimum=1,
            ),
            aggregate_shards,
        )
        context["aggregate_concurrency"] = aggregate_concurrency
        include_inline_source_evidence = (
            _is_env_enabled(
                "HLTHPRT_ENTITY_ADDRESS_UNIFIED_INLINE_SOURCE_EVIDENCE",
                DEFAULT_INLINE_SOURCE_EVIDENCE,
            )
        )
        context["inline_source_evidence"] = include_inline_source_evidence
        if _should_require_inline_evidence() and not include_inline_source_evidence:
            raise RuntimeError(
                "HLTHPRT_ENTITY_ADDRESS_UNIFIED_REQUIRE_INLINE_SOURCE_EVIDENCE requested, "
                "but inline source evidence is inactive "
                f"(partial_source_refresh={partial_source_refresh}, reuse_stage={should_reuse_stage}, "
                f"chunked_load={should_use_chunked_load})."
            )
        should_split_array_aggregates = _is_env_enabled(
            "HLTHPRT_ENTITY_ADDRESS_UNIFIED_SPLIT_ARRAY_AGGREGATES",
            DEFAULT_SPLIT_ARRAY_AGGREGATES,
        )
        context["split_array_aggregates"] = should_split_array_aggregates
        enrich_shards = _env_int(
            "HLTHPRT_ENTITY_ADDRESS_UNIFIED_ENRICH_SHARDS",
            DEFAULT_ENRICH_SHARDS,
            minimum=1,
        )
        context["enrich_shards"] = enrich_shards
        enrich_concurrency = min(
            _env_int(
                "HLTHPRT_ENTITY_ADDRESS_UNIFIED_ENRICH_CONCURRENCY",
                DEFAULT_ENRICH_CONCURRENCY,
                minimum=1,
            ),
            enrich_shards,
        )
        context["enrich_concurrency"] = enrich_concurrency
        checksum_ranges = (
            _integer_ranges(-(2**31), 2**31 - 1, enrich_shards)
            if enrich_shards > 1
            else []
        )
        raw_table = _raw_stage_table_name(stage_table)
        use_unlogged_raw = _is_env_enabled("HLTHPRT_ENTITY_ADDRESS_UNIFIED_UNLOGGED_RAW_STAGE", True)
        should_reuse_raw_stage = _is_task_or_env_enabled(
            task,
            "reuse_raw_stage",
            "HLTHPRT_ENTITY_ADDRESS_UNIFIED_REUSE_RAW_STAGE",
            False,
        )
        if should_reuse_raw_stage:
            if not await _has_table(db_schema, raw_table):
                raise RuntimeError(
                    "entity-address-unified raw-stage reuse requested, "
                    f"but {db_schema}.{raw_table} does not exist"
                )
            raw_rows = int(await db.scalar(f"SELECT COUNT(*) FROM {db_schema}.{raw_table};") or 0)
            if raw_rows <= 0:
                raise RuntimeError(
                    "entity-address-unified raw-stage reuse requested, "
                    f"but {db_schema}.{raw_table} is empty"
                )
            context["raw_stage_reused"] = True
            context["raw_stage_reused_rows"] = raw_rows
            stale_raw_versions = int(
                await db.scalar(
                    f"""
                    SELECT count(*)
                    FROM {db_schema}.{raw_table}
                    WHERE base_address_version IS DISTINCT FROM :base_address_version;
                    """,
                    base_address_version=alias_base_address_version,
                )
                or 0
            )
            if stale_raw_versions:
                raise RuntimeError(
                    "reused entity-address-unified raw stage uses a stale address "
                    "alias generation; reload the raw stage"
                )
            if run_id:
                enqueue_live_progress(
                    run_id=run_id,
                    importer="entity-address-unified",
                    status="running",
                    phase="entity-address-unified reusing raw stage",
                    unit="rows",
                    done=raw_rows,
                    total=raw_rows,
                    message=f"reusing {raw_rows:,} raw rows from {raw_table}",
                )
        else:
            await db.status(f"DROP TABLE IF EXISTS {db_schema}.{raw_table};")
            await db.status(_prepare_raw_stage_sql(db_schema, raw_table, unlogged=use_unlogged_raw))
            await db.status(_disable_autovacuum_sql(db_schema, raw_table))

            sem = asyncio.Semaphore(source_concurrency)
            source_progress_lock = asyncio.Lock()
            source_progress_map = {"loaded_sources": 0}

            async def _load_source(select_sql: str) -> None:
                # Progress is held in source_progress_map to avoid nonlocal state.
                async with sem:
                    await _run_sql_phase(
                        _insert_raw_from_source_sql(
                            db_schema,
                            raw_table,
                            select_sql,
                            is_address_canon_available=is_address_canon_available,
                        ),
                        context=context,
                        run_id=run_id,
                        phase="entity-address-unified loading sources",
                        unit="sources",
                        done=source_progress_map["loaded_sources"],
                        total=len(source_selects),
                        message="loading source shard",
                        emit_start=True,
                    )
                if run_id:
                    async with source_progress_lock:
                        source_progress_map["loaded_sources"] += 1
                        enqueue_live_progress(
                            run_id=run_id,
                            importer="entity-address-unified",
                            status="running",
                            phase="entity-address-unified loading sources",
                            unit="sources",
                            done=source_progress_map["loaded_sources"],
                            total=len(source_selects),
                            message=f"loaded {source_progress_map['loaded_sources']}/{len(source_selects)} sources",
                        )

            if source_concurrency > 1 and len(source_selects) > 1:
                await asyncio.gather(*(_load_source(select_sql) for select_sql in source_selects))
            else:
                for select_sql in source_selects:
                    await _load_source(select_sql)

            if enrich_shards > 1:
                await _run_sql_phase(
                    f"CREATE INDEX IF NOT EXISTS {raw_table}_idx_checksum "
                    f"ON {db_schema}.{raw_table} (checksum);",
                    context=context,
                    run_id=run_id,
                    phase="entity-address-unified indexing raw checksum",
                    unit="indexes",
                    done=0,
                    total=1,
                    message="indexing raw checksum shards",
                    emit_start=True,
                    emit_done=True,
                )
                await _run_sql_phase(
                    f"ANALYZE {db_schema}.{raw_table};",
                    context=context,
                    run_id=run_id,
                    phase="entity-address-unified analyzing raw",
                    unit="tables",
                    done=0,
                    total=1,
                    message="analyzing raw stage",
                    emit_start=True,
                    emit_done=True,
                )
                if run_id:
                    enqueue_live_progress(
                        run_id=run_id,
                        importer="entity-address-unified",
                        status="running",
                        phase="entity-address-unified enriching raw",
                        unit="shards",
                        done=0,
                        total=enrich_shards,
                        message=f"enriching {enrich_shards} checksum shards",
                    )
                enrich_sem = asyncio.Semaphore(enrich_concurrency)
                enrich_progress_lock = asyncio.Lock()
                enrich_progress_map = {"enriched_shards": 0}
                async def _enrich_shard(checksum_min: int, checksum_max: int) -> None:
                    # Progress is held in enrich_progress_map to avoid nonlocal state.
                    async with enrich_sem:
                        await _run_sql_phase(
                            _enrich_raw_stage_sql(
                                db_schema,
                                raw_table,
                                archive_available=available_relation_map.get("address_archive_v2", False),
                                is_address_canon_available=is_address_canon_available,
                                checksum_min=checksum_min,
                                checksum_max=checksum_max,
                                evidence_shards=(
                                    aggregate_shards
                                    if include_inline_source_evidence and aggregate_shards > 1
                                    else None
                                ),
                            ),
                            context=context,
                            run_id=run_id,
                            phase="entity-address-unified enriching raw",
                            unit="shards",
                            done=enrich_progress_map["enriched_shards"],
                            total=enrich_shards,
                            message=f"enriching checksum range {checksum_min}..{checksum_max}",
                            emit_start=True,
                        )
                    if run_id:
                        async with enrich_progress_lock:
                            enrich_progress_map["enriched_shards"] += 1
                            enqueue_live_progress(
                                run_id=run_id,
                                importer="entity-address-unified",
                                status="running",
                                phase="entity-address-unified enriching raw",
                                unit="shards",
                                done=enrich_progress_map["enriched_shards"],
                                total=enrich_shards,
                                message=f"enriched {enrich_progress_map['enriched_shards']}/{enrich_shards} raw shards",
                            )

                await asyncio.gather(*(_enrich_shard(low, high) for low, high in checksum_ranges))
            else:
                await _run_sql_phase(
                    _enrich_raw_stage_sql(
                        db_schema,
                        raw_table,
                        archive_available=available_relation_map.get("address_archive_v2", False),
                        is_address_canon_available=is_address_canon_available,
                        evidence_shards=(
                            aggregate_shards
                            if include_inline_source_evidence and aggregate_shards > 1
                            else None
                        ),
                    ),
                    context=context,
                    run_id=run_id,
                    phase="entity-address-unified enriching raw",
                    unit="run",
                    done=0,
                    total=1,
                    message="enriching raw stage from address archive",
                    emit_start=True,
                    emit_done=True,
                )
        alias_integrity_checksum_ranges = await _raw_alias_integrity_checksum_ranges(
            db_schema,
            raw_table,
            checksum_ranges,
            is_raw_stage_reused=should_reuse_raw_stage,
        )
        await _validate_raw_alias_integrity(
            db_schema,
            raw_table,
            is_address_canon_available=is_address_canon_available,
            checksum_ranges=alias_integrity_checksum_ranges or None,
            concurrency=enrich_concurrency,
            context=context,
            run_id=run_id,
        )
        await _run_sql_phase(
            f"DROP INDEX IF EXISTS {db_schema}.{raw_table}_idx_group_key;",
            context=context,
            phase="entity-address-unified preparing raw group index",
        )
        await _run_sql_phase(
            f"DROP INDEX IF EXISTS {db_schema}.{raw_table}_idx_aggregate_shard_group;",
            context=context,
            phase="entity-address-unified preparing raw group index",
        )
        await _run_sql_phase(
            f"DROP INDEX IF EXISTS {db_schema}.{raw_table}_idx_evidence_shard_group;",
            context=context,
            phase="entity-address-unified preparing raw group index",
        )
        await _run_sql_phase(
            f"DROP INDEX IF EXISTS {db_schema}.{raw_table}_idx_evidence_shard;",
            context=context,
            phase="entity-address-unified preparing raw group index",
        )
        await _run_sql_phase(
            _raw_aggregate_group_index_sql(
                db_schema,
                raw_table,
                aggregate_shards=aggregate_shards,
                is_address_canon_available=is_address_canon_available,
                inline_source_evidence=include_inline_source_evidence,
            ),
            context=context,
            run_id=run_id,
            phase="entity-address-unified indexing raw groups",
            unit="indexes",
            done=0,
            total=2,
            message="indexing raw entity/location groups",
            emit_start=True,
            emit_done=True,
        )
        if serving_only_refresh:
            context["raw_location_key_index_skipped"] = True
        else:
            context["raw_location_key_index_skipped"] = False
            await _run_sql_phase(
                f"CREATE INDEX IF NOT EXISTS {raw_table}_idx_location_key "
                f"ON {db_schema}.{raw_table} (location_key);",
                context=context,
                run_id=run_id,
                phase="entity-address-unified indexing raw location keys",
                unit="indexes",
                done=1,
                total=2,
                message="indexing raw location keys",
                emit_start=True,
                emit_done=True,
            )
        await _run_sql_phase(
            f"ANALYZE {db_schema}.{raw_table};",
            context=context,
            run_id=run_id,
            phase="entity-address-unified analyzing raw aggregate inputs",
            unit="tables",
            done=0,
            total=1,
            message="analyzing raw aggregate inputs",
            emit_start=True,
            emit_done=True,
        )
        await db.status(f"TRUNCATE TABLE {db_schema}.{stage_table};")
        if run_id:
            enqueue_live_progress(
                run_id=run_id,
                importer="entity-address-unified",
                status="running",
                phase="entity-address-unified aggregating",
                unit="shards",
                done=0,
                total=aggregate_shards,
                message=f"aggregating {aggregate_shards} shards",
            )
        aggregate_progress_lock = asyncio.Lock()
        aggregate_progress_map = {"aggregated_shards": 0}

        async def _aggregate_shard(remainder: int) -> None:
            # Progress is held in aggregate_progress_map to avoid nonlocal state.
            await _run_sql_phase(
                _materialize_from_raw_sql(
                    db_schema,
                    stage_table,
                    raw_table,
                    checksum_modulo=aggregate_shards,
                    checksum_remainder=remainder,
                    is_address_canon_available=is_address_canon_available,
                    inline_source_evidence=include_inline_source_evidence,
                ),
                context=context,
                run_id=run_id,
                phase="entity-address-unified aggregating",
                unit="shards",
                done=aggregate_progress_map["aggregated_shards"],
                total=aggregate_shards,
                message=f"aggregating shard {remainder + 1}/{aggregate_shards}",
                emit_start=True,
            )
            if run_id:
                async with aggregate_progress_lock:
                    aggregate_progress_map["aggregated_shards"] += 1
                    enqueue_live_progress(
                        run_id=run_id,
                        importer="entity-address-unified",
                        status="running",
                        phase="entity-address-unified aggregating",
                        unit="shards",
                        done=aggregate_progress_map["aggregated_shards"],
                        total=aggregate_shards,
                        message=f"aggregated {aggregate_progress_map['aggregated_shards']}/{aggregate_shards} shards",
                    )

        if aggregate_shards > 1:
            agg_sem = asyncio.Semaphore(aggregate_concurrency)

            async def _guarded_aggregate(remainder: int) -> None:
                async with agg_sem:
                    await _aggregate_shard(remainder)

            await asyncio.gather(*(_guarded_aggregate(i) for i in range(aggregate_shards)))
        else:
            await _run_sql_phase(
                _materialize_from_raw_sql(
                    db_schema,
                    stage_table,
                    raw_table,
                    is_address_canon_available=is_address_canon_available,
                    inline_source_evidence=include_inline_source_evidence,
                ),
                context=context,
                run_id=run_id,
                phase="entity-address-unified aggregating",
                unit="run",
                done=0,
                total=1,
                message="aggregating raw stage",
                emit_start=True,
                emit_done=True,
            )

    elif should_use_chunked_load:
        raw_table = _raw_stage_table_name(stage_table)
        context["stage_reused"] = True
        if not await _has_table(db_schema, raw_table):
            raw_table = None
        if run_id:
            stage_rows = int(await db.scalar(f"SELECT COUNT(*) FROM {db_schema}.{stage_table};") or 0)
            enqueue_live_progress(
                run_id=run_id,
                importer="entity-address-unified",
                status="running",
                phase="entity-address-unified reusing materialized stage",
                unit="rows",
                done=stage_rows,
                total=stage_rows,
                message=f"reusing {stage_rows:,} staged rows from {stage_table}",
            )

    else:
        if run_id:
            enqueue_live_progress(
                run_id=run_id,
                importer="entity-address-unified",
                status="running",
                phase="entity-address-unified materializing",
                unit="sources",
                done=0,
                total=len(source_selects),
                message="materializing sources",
            )
        await db.status(f"TRUNCATE TABLE {db_schema}.{stage_table};")
        await _run_sql_phase(
            _materialize_sql(
                db_schema,
                stage_table,
                source_selects,
                is_address_canon_available=is_address_canon_available,
            ),
            context=context,
            run_id=run_id,
            phase="entity-address-unified materializing",
            unit="sources",
            done=0,
            total=len(source_selects),
            message="materializing sources",
            emit_start=True,
            emit_done=True,
        )
        await db.status(
            f"""
            UPDATE {db_schema}.{stage_table}
               SET base_address_version = :base_address_version;
            """,
            base_address_version=alias_base_address_version,
        )
        if run_id:
            enqueue_live_progress(
                run_id=run_id,
                importer="entity-address-unified",
                status="running",
                phase="entity-address-unified materialized",
                unit="sources",
                done=len(source_selects),
                total=len(source_selects),
                message="sources materialized",
            )

    should_enable_inference = str(
        os.getenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_ENABLE_INFERENCE", "false")
    ).strip().lower() in {"1", "true", "yes", "on"}
    if test_mode:
        should_enable_inference = str(
            os.getenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_TEST_ENABLE_INFERENCE", "false")
        ).strip().lower() in {"1", "true", "yes", "on"}

    if (
        should_enable_inference
        and available_relation_map.get("npi", False)
        and available_relation_map.get("npi_address", False)
        and available_relation_map.get("npi_taxonomy", False)
        and available_relation_map.get("nucc_taxonomy", False)
    ):
        include_facility_override = False
        if available_relation_map.get("facility_anchor_npi_override", False):
            include_facility_override = bool(
                await db.scalar(
                    f"""
                    SELECT EXISTS (
                        SELECT 1
                          FROM {db_schema}.facility_anchor_npi_override
                         WHERE npi IS NOT NULL
                           AND LOWER(COALESCE(status, '')) = 'approved'
                         LIMIT 1
                    );
                    """
                )
            )
        context["facility_anchor_npi_override_inference_enabled"] = include_facility_override
        include_npi_other_identifier = (
            available_relation_map.get("npi_other_identifier", False)
            and _is_env_enabled(
                "HLTHPRT_ENTITY_ADDRESS_UNIFIED_ENABLE_NPI_OTHER_IDENTIFIER_INFERENCE",
                False,
            )
        )
        context["facility_anchor_npi_other_identifier_inference_enabled"] = (
            include_npi_other_identifier
        )
        await _prepare_inference_stage_indexes(db_schema, stage_table, context=context)
        await _run_sql_phase(
            _inference_sql(
                db_schema,
                stage_table,
                inference_options_by_name={
                    "include_hospital_enrollment": available_relation_map.get(
                        "provider_enrollment_hospital", False
                    ),
                    "include_fqhc_enrollment": available_relation_map.get(
                        "provider_enrollment_fqhc", False
                    ),
                    "include_facility_override": include_facility_override,
                    "include_npi_other_identifier": include_npi_other_identifier,
                    "include_name_fallback": _is_env_enabled(
                        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_ENABLE_NAME_FALLBACK_INFERENCE",
                        False,
                    ),
                    "include_nppes_name_inference": _is_env_enabled(
                        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_ENABLE_NPPES_NAME_INFERENCE",
                        False,
                    ),
                    "include_nppes_broad_inference": _is_env_enabled(
                        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_ENABLE_NPPES_BROAD_INFERENCE",
                        False,
                    ),
                },
            ),
            context=context,
            run_id=run_id,
            phase="entity-address-unified inferring NPIs",
            unit="run",
            done=0,
            total=1,
            message="inferring facility-anchor NPIs",
            emit_start=True,
            emit_done=True,
        )

    if include_inline_source_evidence:
        context["source_evidence_inlined"] = True
    else:
        if _should_require_inline_evidence():
            raise RuntimeError(
                "HLTHPRT_ENTITY_ADDRESS_UNIFIED_REQUIRE_INLINE_SOURCE_EVIDENCE requested, "
                "but the import reached the separate source-evidence work-table path."
            )
        evidence_shards = _env_int(
            "HLTHPRT_ENTITY_ADDRESS_UNIFIED_EVIDENCE_SHARDS",
            DEFAULT_EVIDENCE_SHARDS,
            minimum=1,
        )
        evidence_concurrency = min(
            _env_int(
                "HLTHPRT_ENTITY_ADDRESS_UNIFIED_EVIDENCE_CONCURRENCY",
                DEFAULT_EVIDENCE_CONCURRENCY,
                minimum=1,
            ),
            evidence_shards,
        )
        use_unlogged_evidence = _is_env_enabled(
            "HLTHPRT_ENTITY_ADDRESS_UNIFIED_UNLOGGED_EVIDENCE_STAGE",
            True,
        )
        evidence_table = _evidence_stage_table_name(stage_table)
        await db.status(f"DROP TABLE IF EXISTS {db_schema}.{evidence_table};")
        if run_id:
            enqueue_live_progress(
                run_id=run_id,
                importer="entity-address-unified",
                status="running",
                phase="entity-address-unified preparing source evidence",
                unit="tables",
                done=0,
                total=1,
                message=f"building {evidence_shards}-shard evidence work table",
            )
        await _run_sql_phase(
            _prepare_multi_source_evidence_table_sql(
                db_schema,
                evidence_table,
                unlogged=use_unlogged_evidence,
            ),
            context=context,
            run_id=run_id,
            phase="entity-address-unified preparing source evidence",
            unit="tables",
            done=0,
            total=1,
            message="creating source evidence work table",
            emit_start=True,
            emit_done=True,
        )
        await _run_sql_phase(
            _load_multi_source_evidence_base_sql(
                db_schema,
                stage_table,
                evidence_table,
                evidence_shards=evidence_shards,
                affected_group_table=affected_group_table,
                affected_scope="npi" if is_partial_provider_directory_refresh else "group",
            ),
            context=context,
            run_id=run_id,
            phase="entity-address-unified loading source evidence base",
            unit="rows",
            done=0,
            total=1,
            message="normalizing source evidence once",
            emit_start=True,
            emit_done=True,
        )
        await _run_sql_phase(
            _index_multi_source_evidence_table_sql(db_schema, evidence_table),
            context=context,
            run_id=run_id,
            phase="entity-address-unified indexing source evidence base",
            unit="indexes",
            done=0,
            total=1,
            message="indexing source evidence base",
            emit_start=True,
            emit_done=True,
        )
        await _run_sql_phase(
            f"ANALYZE {db_schema}.{evidence_table};",
            context=context,
            run_id=run_id,
            phase="entity-address-unified analyzing source evidence base",
            unit="tables",
            done=0,
            total=1,
            message="analyzing source evidence base",
            emit_start=True,
            emit_done=True,
        )
        evidence_build_progress_lock = asyncio.Lock()
        evidence_build_progress_map = {"evidence_build_done": 0}

        async def _build_evidence_shard(remainder: int) -> None:
            # Progress is held in evidence_build_progress_map to avoid nonlocal state.
            await _run_sql_phase(
                _insert_multi_source_evidence_shard_sql(
                    db_schema,
                    stage_table,
                    evidence_table,
                    evidence_shards=evidence_shards,
                    evidence_shard=remainder,
                ),
                context=context,
                run_id=run_id,
                phase="entity-address-unified preparing source evidence",
                unit="shards",
                done=evidence_build_progress_map["evidence_build_done"],
                total=evidence_shards,
                message=f"preparing evidence shard {remainder + 1}/{evidence_shards}",
                emit_start=True,
            )
            if run_id:
                async with evidence_build_progress_lock:
                    evidence_build_progress_map["evidence_build_done"] += 1
                    enqueue_live_progress(
                        run_id=run_id,
                        importer="entity-address-unified",
                        status="running",
                        phase="entity-address-unified preparing source evidence",
                        unit="shards",
                        done=evidence_build_progress_map["evidence_build_done"],
                        total=evidence_shards,
                        message=f"prepared {evidence_build_progress_map['evidence_build_done']}/{evidence_shards} evidence shards",
                    )

        if evidence_shards > 1:
            evidence_build_sem = asyncio.Semaphore(evidence_concurrency)

            async def _guarded_build_evidence_shard(remainder: int) -> None:
                async with evidence_build_sem:
                    await _build_evidence_shard(remainder)

            await asyncio.gather(
                *(_guarded_build_evidence_shard(i) for i in range(evidence_shards))
            )
        else:
            await _build_evidence_shard(0)
        await _run_sql_phase(
            _index_multi_source_evidence_table_sql(db_schema, evidence_table),
            context=context,
            run_id=run_id,
            phase="entity-address-unified indexing source evidence",
            unit="indexes",
            done=0,
            total=1,
            message="indexing evidence work table",
            emit_start=True,
            emit_done=True,
        )
        await _run_sql_phase(
            f"ANALYZE {db_schema}.{evidence_table};",
            context=context,
            run_id=run_id,
            phase="entity-address-unified analyzing source evidence",
            unit="tables",
            done=0,
            total=1,
            message="analyzing evidence work table",
            emit_start=True,
            emit_done=True,
        )
        if run_id:
            enqueue_live_progress(
                run_id=run_id,
                importer="entity-address-unified",
                status="running",
                phase="entity-address-unified applying source evidence",
                unit="shards",
                done=0,
                total=evidence_shards,
                message=f"applying evidence across {evidence_shards} shards",
            )
        evidence_progress_lock = asyncio.Lock()
        evidence_progress_map = {"evidence_done": 0}

        async def _apply_evidence_shard(remainder: int) -> None:
            # Progress is held in evidence_progress_map to avoid nonlocal state.
            await _run_sql_phase(
                _apply_multi_source_evidence_sql(
                    db_schema,
                    stage_table,
                    evidence_table,
                    evidence_shard=remainder,
                ),
                context=context,
                run_id=run_id,
                phase="entity-address-unified applying source evidence",
                unit="shards",
                done=evidence_progress_map["evidence_done"],
                total=evidence_shards,
                message=f"applying evidence shard {remainder + 1}/{evidence_shards}",
                emit_start=True,
            )
            if run_id:
                async with evidence_progress_lock:
                    evidence_progress_map["evidence_done"] += 1
                    enqueue_live_progress(
                        run_id=run_id,
                        importer="entity-address-unified",
                        status="running",
                        phase="entity-address-unified applying source evidence",
                        unit="shards",
                        done=evidence_progress_map["evidence_done"],
                        total=evidence_shards,
                        message=f"applied {evidence_progress_map['evidence_done']}/{evidence_shards} evidence shards",
                    )

        if evidence_shards > 1:
            evidence_sem = asyncio.Semaphore(evidence_concurrency)

            async def _guarded_evidence_shard(remainder: int) -> None:
                async with evidence_sem:
                    await _apply_evidence_shard(remainder)

            await asyncio.gather(*(_guarded_evidence_shard(i) for i in range(evidence_shards)))
        else:
            await _apply_evidence_shard(0)
        await db.status(f"DROP TABLE IF EXISTS {db_schema}.{evidence_table};")

    node_id = str(os.getenv("HLTHPRT_IMPORT_NODE_ID") or "").strip() or None
    cached_support_counts = context.get("support_counts")
    if serving_only_refresh:
        support_counts_by_kind = {}
        context["support_stage_populated"] = False
        context["support_stage_skipped"] = True
        context["support_counts"] = support_counts_by_kind
    elif context.get("support_stage_populated") and isinstance(cached_support_counts, dict):
        support_counts_by_kind = {
            str(key): int(cached_count)
            for key, cached_count in cached_support_counts.items()
        }
    else:
        support_raw_table = None if partial_source_refresh else raw_table
        support_affected_group_table = None
        should_copy_unaffected_support_bridges = True
        support_counts_by_kind = await _populate_support_stage_tables(
            db_schema,
            stage_table,
            support_stage_class_map,
            source_run_id=import_date,
            node_id=node_id,
            raw_table=support_raw_table,
            build_network_bridge=should_build_network_bridge,
            available=available_relation_map,
            run_id=run_id,
            context=context,
            affected_group_table=support_affected_group_table,
            copy_unaffected_bridges=should_copy_unaffected_support_bridges,
        )
        context["support_stage_populated"] = True
        context["support_counts"] = support_counts_by_kind
    if raw_table and _should_keep_raw_stage():
        context["raw_stage_kept"] = True
        context["raw_stage_table"] = raw_table
    elif raw_table:
        context["raw_stage_kept"] = False
        await db.status(f"DROP TABLE IF EXISTS {db_schema}.{raw_table};")
    if (
        affected_group_table
        and not context.get("partial_support_patch_publish")
        and not context.get("partial_provider_directory_replacement_publish")
    ):
        await db.status(f"DROP TABLE IF EXISTS {db_schema}.{affected_group_table};")

    if (
        _is_env_enabled(
            "HLTHPRT_ENTITY_ADDRESS_UNIFIED_COMPACT_SOURCE_RECORD_IDS",
            DEFAULT_COMPACT_SOURCE_RECORD_IDS,
        )
        and should_aggregate_source_record_ids
        and not context.get("hot_row_source_record_ids_compacted")
    ):
        if run_id:
            enqueue_live_progress(
                run_id=run_id,
                importer="entity-address-unified",
                status="running",
                phase="entity-address-unified compacting hot rows",
                unit="run",
                done=0,
                total=1,
                pct=88,
                message="rewriting hot rows without source record id arrays",
            )
        compacted_rows = await _compact_hot_row_source_record_ids(
            db_schema,
            stage_table,
            context=context,
        )
        context["hot_row_source_record_ids_compacted"] = True
        context["hot_row_source_record_ids_compacted_rows"] = compacted_rows
        if run_id:
            enqueue_live_progress(
                run_id=run_id,
                importer="entity-address-unified",
                status="running",
                phase="entity-address-unified compacting hot rows",
                unit="rows",
                done=compacted_rows,
                total=compacted_rows,
                pct=89,
                message=f"compacted {compacted_rows:,} hot rows",
            )

    if is_partial_provider_directory_refresh and context.get("partial_provider_directory_replacement_publish"):
        if not affected_group_table or not await _has_table(db_schema, affected_group_table):
            raise RuntimeError(
                "entity-address-unified provider-directory-partial replacement publish requires "
                "the affected group table while composing the replacement stage."
            )
        if not await _has_table(db_schema, EntityAddressUnified.__main_table__):
            raise RuntimeError(
                "entity-address-unified provider-directory-partial replacement publish requires "
                "the live entity_address_unified table to exist."
            )
        await _ensure_entity_address_unified_live_columns(db_schema)
        replacement_stage_table = _provider_directory_replacement_stage_table_name(stage_table)
        context["partial_provider_directory_replacement_stage_table"] = replacement_stage_table
        await _run_sql_phase(
            f"DROP TABLE IF EXISTS {db_schema}.{replacement_stage_table};",
            context=context,
            run_id=run_id,
            phase="entity-address-unified preparing Provider Directory replacement heap",
            unit="tables",
            done=0,
            total=1,
            pct=90,
            message="dropping stale Provider Directory replacement heap",
            emit_done=True,
        )
        await _run_sql_phase(
            _create_provider_directory_replacement_stage_sql(
                db_schema,
                replacement_stage_table=replacement_stage_table,
                stage_table=stage_table,
            ),
            context=context,
            run_id=run_id,
            phase="entity-address-unified preparing Provider Directory replacement heap",
            unit="tables",
            done=1,
            total=1,
            pct=90,
            message="creating Provider Directory replacement heap",
            emit_done=True,
        )
        await db.status(_disable_autovacuum_sql(db_schema, replacement_stage_table))
        affected_live_location_table = _affected_live_location_table(stage_table)
        context["partial_provider_directory_affected_live_location_table"] = affected_live_location_table
        await _run_sql_phase(
            f"DROP TABLE IF EXISTS {db_schema}.{affected_live_location_table};",
            context=context,
            run_id=run_id,
            phase="entity-address-unified preparing Provider Directory affected locations",
            unit="tables",
            done=0,
            total=1,
            pct=90,
            message="dropping stale Provider Directory affected location stage",
            emit_done=True,
        )
        await _run_sql_phase(
            _prepare_affected_live_locations_sql(
                db_schema,
                live_table=EntityAddressUnified.__main_table__,
                affected_group_table=affected_group_table,
                replacement_lookup_table=stage_table,
                affected_location_table=affected_live_location_table,
            ),
            context=context,
            run_id=run_id,
            phase="entity-address-unified preparing Provider Directory affected locations",
            unit="tables",
            done=1,
            total=1,
            pct=90,
            message="materializing Provider Directory affected live locations",
            emit_start=True,
            emit_done=True,
        )
        await _run_sql_phase(
            _index_affected_live_locations_sql(
                db_schema,
                affected_live_location_table,
            ),
            context=context,
            run_id=run_id,
            phase="entity-address-unified indexing Provider Directory affected locations",
            unit="indexes",
            done=0,
            total=1,
            pct=90,
            message="indexing Provider Directory affected live locations",
            emit_start=True,
            emit_done=True,
        )
        await _run_sql_phase(
            f"ANALYZE {db_schema}.{affected_live_location_table};",
            context=context,
            run_id=run_id,
            phase="entity-address-unified analyzing Provider Directory affected locations",
            unit="tables",
            done=0,
            total=1,
            pct=90,
            message="analyzing Provider Directory affected live locations",
            emit_start=True,
            emit_done=True,
        )
        affected_live_locations = int(
            await db.scalar(f"SELECT COUNT(*) FROM {db_schema}.{affected_live_location_table};") or 0
        )
        context["partial_provider_directory_affected_live_locations"] = affected_live_locations
        context["partial_provider_directory_coordinate_scope_table"] = affected_live_location_table
        copied_rows = await _run_sql_phase(
            _copy_unaffected_rows_by_location_sql(
                db_schema,
                live_table=EntityAddressUnified.__main_table__,
                target_stage_table=replacement_stage_table,
                affected_location_table=affected_live_location_table,
            ),
            context=context,
            run_id=run_id,
            phase="entity-address-unified copying unaffected live rows",
            unit="rows",
            done=0,
            total=1,
            pct=90,
            message="copying unaffected live rows into replacement stage",
            emit_start=True,
            emit_done=True,
        )
        context["partial_provider_directory_unaffected_live_rows_copied"] = int(copied_rows or 0)
        affected_stage_rows = await _run_sql_phase(
            _copy_stage_entity_rows_sql(
                db_schema,
                source_stage_table=stage_table,
                target_stage_table=replacement_stage_table,
            ),
            context=context,
            run_id=run_id,
            phase="entity-address-unified copying affected stage rows",
            unit="rows",
            done=0,
            total=1,
            pct=90,
            message="copying affected Provider Directory rows into replacement stage",
            emit_start=True,
            emit_done=True,
        )
        context["partial_provider_directory_affected_stage_rows_copied"] = int(
            affected_stage_rows or 0
        )
        context["partial_provider_directory_replacement_rows"] = (
            int(copied_rows or 0) + int(affected_stage_rows or 0)
        )
        await _run_sql_phase(
            f"DROP TABLE {db_schema}.{stage_table};",
            context=context,
            run_id=run_id,
            phase="entity-address-unified swapping Provider Directory replacement stage",
            unit="tables",
            done=0,
            total=1,
            pct=90,
            message="dropping affected-only stage",
            emit_done=True,
        )
        await _run_sql_phase(
            f"ALTER TABLE {db_schema}.{replacement_stage_table} RENAME TO {stage_table};",
            context=context,
            run_id=run_id,
            phase="entity-address-unified swapping Provider Directory replacement stage",
            unit="tables",
            done=1,
            total=1,
            pct=90,
            message="promoting Provider Directory replacement stage",
            emit_done=True,
        )
        context["stage_indexes_prepared"] = False
        context["partial_provider_directory_replacement_stage_indexes_invalidated"] = True
        await _ensure_stage_primary_key(stage_cls, db_schema, context=context)
        await _run_sql_phase(
            f"DROP TABLE IF EXISTS {db_schema}.{affected_group_table};",
            context=context,
            run_id=run_id,
            phase="entity-address-unified dropping affected Provider Directory groups",
            unit="tables",
            done=1,
            total=1,
            pct=90,
            message="dropping affected Provider Directory groups",
            emit_done=True,
        )
    if (
        not ctx["context"].get("support_stage_indexes_prepared")
        and not context.get("partial_support_patch_publish")
        and not serving_only_refresh
    ):
        await _create_support_stage_indexes(
            support_stage_class_map,
            db_schema,
            context=context,
            run_id=run_id,
        )
        context["support_stage_indexes_prepared"] = True

    should_compute_final_summary_counts = _should_compute_final_summary_counts()
    context["final_summary_counts"] = should_compute_final_summary_counts
    summary_counts = None
    if not should_compute_final_summary_counts:
        summary_counts = _fallback_summary_counts(context)
    if summary_counts is None:
        summary_counts = await _stage_summary_counts(db_schema, stage_table)
    staged_rows = summary_counts["staged_rows"]
    npi_rows = summary_counts["npi_rows"]
    inferred_rows = summary_counts["inferred_rows"]
    multi_source_rows = summary_counts["multi_source_rows"]

    context["run"] = context.get("run", 0) + 1
    context["staged_rows"] = staged_rows
    context["npi_rows"] = npi_rows
    context["inferred_rows"] = inferred_rows
    context["multi_source_rows"] = multi_source_rows
    context["support_counts"] = support_counts_by_kind
    if run_id:
        enqueue_live_progress(
            run_id=run_id,
            importer="entity-address-unified",
            status="running",
            phase="entity-address-unified staged",
            unit="rows",
            done=staged_rows,
            total=staged_rows,
            pct=95,
            message=f"staged {staged_rows} rows",
        )
    logger.info(
        "EntityAddressUnified materialization done: rows=%d npi_rows=%d inferred_rows=%d multi_source_rows=%d",
        staged_rows,
        npi_rows,
        inferred_rows,
        multi_source_rows,
    )


process_data = process_entity_address_unified_data
process_data.__name__ = "process_data"


async def startup(ctx):
    """Initialize one entity-address-unified import context."""
    await my_init_db(db)
    ctx["context"] = {}
    ctx["context"]["start"] = datetime.datetime.utcnow()
    ctx["context"]["run"] = 0
    ctx["context"]["test_mode"] = False
    ctx["context"]["stage_prepared"] = False
    ctx["context"]["stage_indexes_prepared"] = False
    ctx["context"]["support_stage_prepared"] = False
    ctx["context"]["support_stage_indexes_prepared"] = False
    await ensure_database(False)

    override_import_id = os.getenv("HLTHPRT_IMPORT_ID_OVERRIDE")
    ctx["import_date"] = _normalize_import_id(override_import_id)
    logger.info(
        "EntityAddressUnified startup ready: import_date=%s (stage will be prepared in active DB during process_data)",
        ctx["import_date"],
    )


async def publish_entity_address_unified_generation(ctx):
    """Finalize, validate, and publish one entity-address-unified import."""
    import_date = ctx.get("import_date")
    context = ctx.get("context") or {}
    run_id = str(context.get("control_run_id") or ctx.get("control_run_id") or "").strip()

    if not context.get("run"):
        logger.info("No EntityAddressUnified jobs ran; skipping shutdown.")
        return

    await ensure_database(bool(context.get("test_mode")))

    db_schema = os.getenv("HLTHPRT_DB_SCHEMA") if os.getenv("HLTHPRT_DB_SCHEMA") else "mrf"
    stage_cls = make_class(EntityAddressUnified, import_date)
    serving_only_refresh = bool(context.get("serving_only_refresh"))
    support_stage_class_map = {} if serving_only_refresh else _support_stage_classes(import_date)
    affected_group_table = str(
        context.get("partial_affected_group_table")
        or context.get("partial_provider_directory_affected_group_table")
        or ""
    ).strip()
    affected_live_location_table = str(
        context.get("partial_provider_directory_affected_live_location_table") or ""
    ).strip()
    coordinate_scope_table = str(
        context.get("partial_provider_directory_coordinate_scope_table") or ""
    ).strip()
    partial_support_patch = bool(context.get("partial_support_patch_publish"))
    is_partial_provider_directory_refresh = (
        context.get("refresh_mode") == ENTITY_ADDRESS_REFRESH_MODE_PROVIDER_DIRECTORY_PARTIAL
    )
    partial_main_patch = bool(
        context.get("partial_main_patch_publish")
        or context.get("partial_provider_directory_main_patch_publish")
    )
    if partial_main_patch:
        raise RuntimeError(
            "entity-address-unified live main-table patch publishing is disabled; "
            "build a replacement stage table and publish through the table swap."
        )
    if (
        is_partial_provider_directory_refresh and partial_support_patch
    ):
        raise RuntimeError(
            "entity-address-unified provider-directory-partial refresh must publish "
            "through replacement-stage table swap; live support patch publishing is disabled."
        )
    if is_partial_provider_directory_refresh and context.get(
        "partial_provider_directory_replacement_publish"
    ):
        if not coordinate_scope_table or not await _has_table(db_schema, coordinate_scope_table):
            raise RuntimeError(
                "entity-address-unified provider-directory-partial replacement publish requires "
                "the affected location scope through staged coordinate backfill."
            )

    cached_stage_rows = _int_context_metric(context, "staged_rows")
    stage_rows = cached_stage_rows
    if stage_rows <= 0:
        stage_rows = int(await db.scalar(f"SELECT COUNT(*) FROM {db_schema}.{stage_cls.__tablename__};") or 0)
    context["staged_rows"] = stage_rows
    min_rows_required = int(
        os.getenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_MIN_ROWS", str(DEFAULT_MIN_ROWS))
    )
    previous_rows = 0
    live_table_exists = await _has_table(db_schema, EntityAddressUnified.__main_table__)
    if live_table_exists:
        previous_rows = int(
            await db.scalar(f"SELECT COUNT(*) FROM {db_schema}.{EntityAddressUnified.__main_table__};")
            or 0
        )
    if context.get("test_mode"):
        logger.info("EntityAddressUnified test mode: staged rows=%d", stage_rows)

    if not bool(context.get("publish_requested", True)):
        context["publish_validation"] = {}
        logger.info("EntityAddressUnified publish skipped: staged rows=%d", stage_rows)
        await _drop_stage_artifacts(
            db_schema,
            stage_cls,
            support_stage_class_map,
            extra_tables=[affected_group_table, affected_live_location_table],
        )
        await mark_control_run(
            run_id,
            status="succeeded",
            phase_detail="entity-address-unified staged; publish skipped",
            progress_message="staged; publish skipped",
            progress={
                "unit": "rows",
                "done": stage_rows,
                "total": stage_rows,
                "pct": 100,
                "message": "staged; publish skipped",
                "phase": "entity-address-unified staged",
            },
            metrics={
                "rows": stage_rows,
                "publish_skipped": True,
                "refresh_mode": context.get("refresh_mode") or ENTITY_ADDRESS_REFRESH_MODE_FULL,
                "partial_provider_directory_affected_groups": int(
                    context.get("partial_provider_directory_affected_groups") or 0
                ),
                "partial_provider_directory_patched_rows": int(
                    context.get("partial_provider_directory_patched_rows") or 0
                ),
                "partial_provider_directory_replacement_publish": bool(
                    context.get("partial_provider_directory_replacement_publish")
                ),
                "partial_provider_directory_unaffected_live_rows_copied": int(
                    context.get("partial_provider_directory_unaffected_live_rows_copied") or 0
                ),
                "partial_provider_directory_affected_stage_rows_copied": int(
                    context.get("partial_provider_directory_affected_stage_rows_copied") or 0
                ),
                "partial_provider_directory_affected_live_locations": int(
                    context.get("partial_provider_directory_affected_live_locations") or 0
                ),
                "npi_rows": int(context.get("npi_rows") or 0),
                "inferred_rows": int(context.get("inferred_rows") or 0),
                "multi_source_rows": int(context.get("multi_source_rows") or 0),
                "support_counts": context.get("support_counts") or {},
                "serving_only_refresh": serving_only_refresh,
                "support_stage_skipped": bool(context.get("support_stage_skipped")),
                **_runtime_config_metrics(context),
                "publish_validation": {},
                "phase_timings": context.get("phase_timings") or {},
                "skipped_stage_indexes": context.get("skipped_stage_indexes") or [],
            },
        )
        print_time_info(context.get("start"))
        return

    _validate_publish_row_count(
        stage_rows=stage_rows,
        previous_rows=previous_rows,
        test_mode=bool(context.get("test_mode")),
        min_rows_required=min_rows_required,
    )
    defer_publish_validation = (
        serving_only_refresh
        and not partial_support_patch
        and not is_partial_provider_directory_refresh
        and _should_defer_publish_validation()
    )
    if await _has_table(db_schema, "address_archive_v2"):
        archive_coordinate_same_key_backfill_rows = await _run_sql_phase(
            _backfill_archive_coordinates_sql(
                db_schema,
                stage_cls.__tablename__,
                coordinate_scope_table=coordinate_scope_table or None,
            ),
            context=context,
            run_id=run_id,
            phase="entity-address-unified backfilling archive coordinates",
            unit="rows",
            done=0,
            total=1,
            pct=96,
            message="backfilling missing coordinates from address archive",
            emit_start=True,
            emit_done=True,
        )
        context["archive_coordinate_same_key_backfill_rows"] = int(
            archive_coordinate_same_key_backfill_rows or 0
        )
        inheritance_phase = "entity-address-unified inheriting exact legacy archive coordinates"
        inheritance_started = time.monotonic()
        inheritance_metrics = await _inherit_archive_coordinates(
            db_schema,
            stage_cls.__tablename__,
            coordinate_scope_table=coordinate_scope_table or None,
        )
        _record_phase_timing(
            context,
            inheritance_phase,
            time.monotonic() - inheritance_started,
            inheritance_metrics["inherited_rows"],
        )
        context["archive_coordinate_inherited_rows"] = inheritance_metrics["inherited_rows"]
        context["archive_coordinate_ambiguous_rows"] = inheritance_metrics["ambiguous_rows"]
        context["archive_coordinate_backfill_rows"] = (
            context["archive_coordinate_same_key_backfill_rows"]
            + context["archive_coordinate_inherited_rows"]
        )
        logger.info(
            "EntityAddressUnified exact legacy coordinate inheritance: inherited=%d ambiguous=%d",
            context["archive_coordinate_inherited_rows"],
            context["archive_coordinate_ambiguous_rows"],
        )
    else:
        context["archive_coordinate_backfill_rows"] = 0
        context["archive_coordinate_same_key_backfill_rows"] = 0
        context["archive_coordinate_inherited_rows"] = 0
        context["archive_coordinate_ambiguous_rows"] = 0
    same_provider_address_backfill_rows = await _run_sql_phase(
        _backfill_same_provider_address_fields_sql(
            db_schema,
            stage_cls.__tablename__,
            coordinate_scope_table=coordinate_scope_table or None,
        ),
        context=context,
        run_id=run_id,
        phase="entity-address-unified backfilling same-provider address fields",
        unit="rows",
        done=0,
        total=1,
        pct=96,
        message="backfilling missing contact and coordinates across same-provider addresses",
        emit_start=True,
        emit_done=True,
    )
    context["same_provider_address_backfill_rows"] = int(same_provider_address_backfill_rows or 0)
    invalid_coordinate_clear_rows = await _run_sql_phase(
        _clear_invalid_coordinates_sql(
            db_schema,
            stage_cls.__tablename__,
            coordinate_scope_table=coordinate_scope_table or None,
        ),
        context=context,
        run_id=run_id,
        phase="entity-address-unified clearing invalid coordinates",
        unit="rows",
        done=0,
        total=1,
        pct=97,
        message="clearing invalid staged coordinates",
        emit_start=True,
        emit_done=True,
    )
    context["invalid_coordinate_clear_rows"] = int(invalid_coordinate_clear_rows or 0)
    context["geo_assurance_stage_index_drop_attempts"] = (
        await _drop_stage_secondary_indexes(stage_cls, db_schema)
    )
    context["stage_indexes_prepared"] = False
    context["geo_assurance_projected_rows"] = await _materialize_geo_assurance(
        db_schema,
        stage_cls.__tablename__,
        force=bool(context.get("stage_reused")),
        context=context,
        run_id=run_id,
        stage_rows=stage_rows,
    )
    compaction_started = time.monotonic()
    context["geo_assurance_compaction"] = await _compact_geo_assurance_stage(
        db_schema,
        stage_cls.__tablename__,
    )
    _record_phase_timing(
        context,
        "entity-address-unified compacting geo assurance",
        time.monotonic() - compaction_started,
        stage_rows,
    )
    if run_id:
        enqueue_live_progress(
            run_id=run_id,
            importer="entity-address-unified",
            status="running",
            phase="entity-address-unified indexing",
            unit="run",
            done=0,
            total=1,
            pct=98,
            message="building indexes on finalized geo assurance",
        )
    await _create_stage_indexes(stage_cls, db_schema, context=context)
    context["stage_indexes_prepared"] = True
    if coordinate_scope_table:
        await _run_sql_phase(
            f"DROP TABLE IF EXISTS {db_schema}.{coordinate_scope_table};",
            context=context,
            run_id=run_id,
            phase="entity-address-unified dropping Provider Directory post-build scope",
            unit="tables",
            done=1,
            total=1,
            pct=97,
            message="dropping Provider Directory post-build scope",
            emit_done=True,
        )
    context["publish_validation_deferred"] = defer_publish_validation
    if defer_publish_validation:
        context["publish_validation"] = {
            "deferred": True,
            "status": "pending",
        }
    else:
        publish_validation = await _validate_publish_integrity(
            db_schema,
            stage_cls.__tablename__,
            support_stage_class_map,
            test_mode=bool(context.get("test_mode")),
        )
        context["publish_validation"] = publish_validation

    if partial_support_patch and (
        not affected_group_table or not await _has_table(db_schema, affected_group_table)
    ):
        raise RuntimeError(
            "entity-address-unified support patch publish requires "
            "the affected group table to remain available through shutdown."
        )
    expected_provider_directory_dataset_id = _clean_optional(
        context.get("partial_provider_directory_dataset_id")
    )
    if expected_provider_directory_dataset_id is not None:
        partial_source_ids = _coerce_str_list(
            context.get("partial_provider_directory_source_ids")
        )
        expected_provider_directory_root_run_id = _clean_optional(
            context.get("partial_provider_directory_run_id")
        )
        _validate_provider_directory_dataset_fence_scope(
            dataset_id=expected_provider_directory_dataset_id,
            source_ids=partial_source_ids,
            run_id=expected_provider_directory_root_run_id,
            partial_scope=_clean_optional(
                context.get("partial_provider_directory_scope")
            ),
        )
        await _assert_current_provider_directory_dataset(
            db_schema,
            source_id=partial_source_ids[0],
            expected_dataset_id=expected_provider_directory_dataset_id,
            expected_root_run_id=expected_provider_directory_root_run_id,
        )
    current_alias_generation = await _address_alias_generation(db_schema)
    if current_alias_generation != int(context.get("address_alias_generation") or 0):
        raise RuntimeError(
            "address alias generation changed during entity-address-unified build"
        )
    await _publish_staged_entity_address_tables(
        db_schema,
        stage_cls,
        support_stage_class_map,
        partial_support_patch=partial_support_patch,
        affected_group_table=affected_group_table,
        context=context,
    )

    if partial_support_patch:
        await _drop_stage_artifacts(
            db_schema,
            stage_cls,
            support_stage_class_map,
            extra_tables=[affected_group_table, affected_live_location_table],
        )

    published_rows = stage_rows
    started_at = context.get("start")
    if isinstance(started_at, datetime.datetime):
        context["published_elapsed_seconds"] = round(
            (datetime.datetime.utcnow() - started_at).total_seconds(),
            3,
        )
    post_publish_profile = (
        "none" if is_partial_provider_directory_refresh else _post_publish_index_profile()
    )
    should_build_post_publish_concurrently = _should_build_post_publish_concurrently()
    context["post_publish_index_profile"] = post_publish_profile
    context["post_publish_index_concurrently"] = should_build_post_publish_concurrently
    context["post_publish_index_completed"] = int(context.get("post_publish_index_completed") or 0)
    context["post_publish_index_total"] = int(context.get("post_publish_index_total") or 0)
    context["post_publish_skipped_indexes"] = list(context.get("post_publish_skipped_indexes") or [])
    if post_publish_profile != "none":
        planned_statements, skipped_indexes = _post_publish_index_plan(
            db_schema,
            post_publish_profile,
            build_concurrently=should_build_post_publish_concurrently,
        )
        context["post_publish_index_total"] = len(planned_statements)
        context["post_publish_index_completed"] = 0
        context["post_publish_skipped_indexes"] = skipped_indexes
        context["post_publish_index_pending"] = bool(planned_statements)
    else:
        context["post_publish_index_pending"] = False
        context["post_publish_index_total"] = 0
        context["post_publish_index_completed"] = 0

    logger.info("EntityAddressUnified publish complete: rows=%d", published_rows)

    def _published_metrics() -> dict:
        """Return the stable control-run metrics emitted after stage publication."""
        return {
            "rows": published_rows,
            "refresh_mode": context.get("refresh_mode") or ENTITY_ADDRESS_REFRESH_MODE_FULL,
            "partial_patched_rows": int(context.get("partial_patched_rows") or 0),
            "partial_provider_directory_affected_groups": int(
                context.get("partial_provider_directory_affected_groups") or 0
            ),
            "partial_provider_directory_patched_rows": int(
                context.get("partial_provider_directory_patched_rows") or 0
            ),
            "partial_provider_directory_replacement_publish": bool(
                context.get("partial_provider_directory_replacement_publish")
            ),
            "partial_provider_directory_unaffected_live_rows_copied": int(
                context.get("partial_provider_directory_unaffected_live_rows_copied") or 0
            ),
            "partial_provider_directory_affected_stage_rows_copied": int(
                context.get("partial_provider_directory_affected_stage_rows_copied") or 0
            ),
            "partial_provider_directory_affected_live_locations": int(
                context.get("partial_provider_directory_affected_live_locations") or 0
            ),
            "partial_provider_directory_scope": context.get("partial_provider_directory_scope"),
            "partial_provider_directory_run_id": context.get("partial_provider_directory_run_id"),
            "partial_provider_directory_dataset_id": context.get(
                "partial_provider_directory_dataset_id"
            ),
            "partial_provider_directory_source_count": int(
                context.get("partial_provider_directory_source_count") or 0
            ),
            "partial_provider_directory_scope_sources": (
                context.get("partial_provider_directory_scope_sources") or []
            ),
            "npi_rows": int(context.get("npi_rows") or 0),
            "inferred_rows": int(context.get("inferred_rows") or 0),
            "multi_source_rows": int(context.get("multi_source_rows") or 0),
            "support_counts": context.get("support_counts") or {},
            "serving_only_refresh": serving_only_refresh,
            "support_stage_skipped": bool(context.get("support_stage_skipped")),
            **_archive_coordinate_publish_metrics(context),
            "same_provider_address_backfill_rows": int(
                context.get("same_provider_address_backfill_rows") or 0
            ),
            "invalid_coordinate_clear_rows": int(
                context.get("invalid_coordinate_clear_rows") or 0
            ),
            "geo_assurance_projected_rows": int(
                context.get("geo_assurance_projected_rows") or 0
            ),
            "invalid_geo_assurance_rows": int(
                context.get("invalid_geo_assurance_rows") or 0
            ),
            **_runtime_config_metrics(context),
            "publish_validation": context.get("publish_validation") or {},
            "phase_timings": context.get("phase_timings") or {},
            "skipped_stage_indexes": context.get("skipped_stage_indexes") or [],
        }

    if defer_publish_validation:
        await mark_control_run(
            run_id,
            status="running",
            phase_detail="entity-address-unified published; post-publish validation pending",
            progress_message="published; validating",
            progress={
                "unit": "rows",
                "done": published_rows,
                "total": published_rows,
                "pct": 99,
                "message": "published; validating",
                "phase": "entity-address-unified post-publish validation",
            },
            metrics=_published_metrics(),
        )
        try:
            started = time.monotonic()
            publish_validation = await _validate_publish_integrity(
                db_schema,
                EntityAddressUnified.__main_table__,
                {},
                test_mode=bool(context.get("test_mode")),
            )
            publish_validation["deferred"] = True
            publish_validation["status"] = "complete"
            context["publish_validation"] = publish_validation
            _record_phase_timing(
                context,
                "entity-address-unified post-publish validation",
                time.monotonic() - started,
                None,
            )
            await mark_control_run(
                run_id,
                status="succeeded",
                phase_detail="entity-address-unified post-publish validation complete",
                progress_message="post-publish validation complete",
                progress={
                    "unit": "rows",
                    "done": published_rows,
                    "total": published_rows,
                    "pct": 100,
                    "message": "post-publish validation complete",
                    "phase": "entity-address-unified post-publish validation complete",
                },
                metrics=_published_metrics(),
            )
            context["preserve_control_run_finished_at"] = True
        except Exception as exc:
            context["publish_validation"] = {
                "deferred": True,
                "status": "failed",
                "error": str(exc),
            }
            await mark_control_run(
                run_id,
                status="failed",
                phase_detail="entity-address-unified post-publish validation failed",
                progress_message="post-publish validation failed",
                error={"code": "post_publish_validation_failed", "message": str(exc)},
                progress={
                    "unit": "rows",
                    "done": published_rows,
                    "total": published_rows,
                    "pct": 100,
                    "message": "post-publish validation failed",
                    "phase": "entity-address-unified post-publish validation failed",
                },
                metrics=_published_metrics(),
            )
            raise
    else:
        await mark_control_run(
            run_id,
            status="succeeded",
            phase_detail="entity-address-unified published",
            progress_message="succeeded",
            progress={
                "unit": "rows",
                "done": published_rows,
                "total": published_rows,
                "pct": 100,
                "message": "succeeded",
                "phase": "entity-address-unified published",
            },
            metrics=_published_metrics(),
        )
        context["preserve_control_run_finished_at"] = True
    if context["post_publish_index_profile"] != "none":
        try:
            await _create_post_publish_indexes(db_schema, context=context)
            logger.info(
                "EntityAddressUnified post-publish index warmup complete: profile=%s",
                context["post_publish_index_profile"],
            )
            await mark_control_run(
                run_id,
                status="succeeded",
                phase_detail="entity-address-unified post-publish indexes warmed",
                progress_message="post-publish indexes warmed",
                progress={
                    "unit": "rows",
                    "done": published_rows,
                    "total": published_rows,
                    "pct": 100,
                    "message": "post-publish indexes warmed",
                    "phase": "entity-address-unified post-publish indexes warmed",
                },
                metrics=_published_metrics(),
                preserve_finished_at=True,
            )
        except Exception as exc:  # pragma: no cover - defensive; publish already succeeded
            context["post_publish_index_error"] = str(exc)
            logger.exception("EntityAddressUnified post-publish index warmup failed: %s", exc)
            await mark_control_run(
                run_id,
                status="succeeded",
                phase_detail="entity-address-unified post-publish index warmup failed",
                progress_message="published; post-publish index warmup failed",
                progress={
                    "unit": "rows",
                    "done": published_rows,
                    "total": published_rows,
                    "pct": 100,
                    "message": "published; post-publish index warmup failed",
                    "phase": "entity-address-unified post-publish index warmup failed",
                },
                metrics=_published_metrics(),
                preserve_finished_at=True,
            )
    print_time_info(context.get("start"))


shutdown = publish_entity_address_unified_generation
shutdown.__name__ = "shutdown"


async def run_entity_address_unified_command(
    test_mode: bool = False,
    limit_per_source: int | None = None,
    publish: bool | None = None,
    refresh_mode: str | None = None,
    serving_only_refresh: bool | None = None,
    reuse_raw_stage: bool | None = None,
    provider_directory_run_id: str | None = None,
    provider_directory_source_ids: list[str] | tuple[str, ...] | str | None = None,
    provider_directory_dataset_id: str | None = None,
    provider_directory_partial_scope: str | None = None,
    provider_directory_source_batch_size: int | None = None,
):
    """Run the entity-address-unified import lifecycle."""
    redis = await create_pool(
        build_redis_settings(),
        job_serializer=serialize_job,
        job_deserializer=deserialize_job,
    )
    task_payload_map = {"test_mode": bool(test_mode)}
    if limit_per_source is not None:
        task_payload_map["limit_per_source"] = max(int(limit_per_source), 0)
    if publish is not None:
        task_payload_map["publish"] = bool(publish)
    if refresh_mode:
        task_payload_map["refresh_mode"] = refresh_mode
    if serving_only_refresh is not None:
        task_payload_map["serving_only_refresh"] = bool(serving_only_refresh)
    if reuse_raw_stage is not None:
        task_payload_map["reuse_raw_stage"] = bool(reuse_raw_stage)
    if provider_directory_run_id:
        task_payload_map["provider_directory_run_id"] = provider_directory_run_id
    source_ids = _coerce_str_list(provider_directory_source_ids)
    if source_ids:
        task_payload_map["provider_directory_source_ids"] = source_ids
    if provider_directory_dataset_id:
        task_payload_map["provider_directory_dataset_id"] = (
            provider_directory_dataset_id
        )
    if provider_directory_partial_scope:
        task_payload_map["provider_directory_partial_scope"] = provider_directory_partial_scope
    if provider_directory_source_batch_size is not None:
        task_payload_map["provider_directory_source_batch_size"] = max(
            int(provider_directory_source_batch_size), 0
        )
    await redis.enqueue_job(
        "process_data",
        task_payload_map,
        _queue_name=ENTITY_ADDRESS_UNIFIED_QUEUE_NAME,
    )


main = run_entity_address_unified_command
main.__name__ = "main"
