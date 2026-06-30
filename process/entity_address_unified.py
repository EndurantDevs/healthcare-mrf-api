# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
# pylint: disable=too-many-lines

from __future__ import annotations

import asyncio
import datetime
import hashlib
import logging
import os
import re
import time
from dataclasses import dataclass
from typing import Iterable

from arq import create_pool
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateColumn

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
from process.ext.utils import ensure_database, make_class, my_init_db, print_time_info
from process.live_progress import enqueue_live_progress
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
DEFAULT_PROVIDER_DIRECTORY_PARTIAL_SCOPE = "latest-run"
DEFAULT_PROVIDER_DIRECTORY_SOURCE_BATCH_SIZE = 100
ENTITY_ADDRESS_REFRESH_MODE_FULL = "full"
ENTITY_ADDRESS_REFRESH_MODE_PROVIDER_DIRECTORY_PARTIAL = "provider-directory-partial"
ENTITY_ADDRESS_REFRESH_MODES = {
    ENTITY_ADDRESS_REFRESH_MODE_FULL,
    ENTITY_ADDRESS_REFRESH_MODE_PROVIDER_DIRECTORY_PARTIAL,
}
ARCHIVE_IDENTITY_VERSION = "v2"
BASE_ADDRESS_VERSION = "address_archive_v2:v2"
SUPPORT_TABLE_MODELS = (
    EntityAddressEvidence,
    EntityAddressPlanBridge,
    EntityAddressNetworkBridge,
    EntityAddressProcedureBridge,
    EntityAddressMedicationBridge,
    FacilityAnchorNPICandidate,
)

ENTITY_ADDRESS_UNIFIED_SERVING_STAGE_INDEXES = {
    "npi",
    "primary_npi",
    "coalesced_npi",
    "primary_state_city_npi",
    "primary_zip5_npi",
    "primary_phone_npi",
    "service_phone_digits_npi",
    "service_phone_number_npi",
    "service_address_key_npi",
    "taxonomy_plans_network",
    "procedures_array",
    "medications_array",
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


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _coerce_bool(value) -> bool:
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
    cleaned: list[str] = []
    seen: set[str] = set()
    for item in candidates:
        text = _clean_optional(item)
        if not text or text in seen:
            continue
        seen.add(text)
        cleaned.append(text)
    return cleaned


def _entity_address_refresh_mode(task: dict) -> str:
    raw = (
        task.get("refresh_mode")
        or task.get("mode")
        or task.get("refresh_scope")
        or os.getenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_REFRESH_MODE")
        or ""
    )
    value = str(raw).strip().lower().replace("_", "-")
    if not value:
        return ENTITY_ADDRESS_REFRESH_MODE_FULL
    if value in {"full", "rebuild", "full-rebuild"}:
        return ENTITY_ADDRESS_REFRESH_MODE_FULL
    if value in {"ptg-partial", "partial-ptg", "ptg", "ptg-source", "ptg-source-refresh"}:
        logger.warning(
            "entity-address-unified refresh_mode=%s is obsolete because PTG no longer "
            "publishes address rows; running a full unified-address refresh instead.",
            raw,
        )
        return ENTITY_ADDRESS_REFRESH_MODE_FULL
    if value in {
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


def _entity_address_provider_directory_source_batch_size(task: dict) -> int:
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


def _publish_requested(task: dict, *, test_mode: bool) -> bool:
    """Return whether this run should publish staged entity-address tables.

    Bounded/test pilots are stage-only by default so they can prove runtime
    behavior without replacing the live serving table with a small sample.
    """

    if task.get("skip_publish") not in (None, ""):
        return not _coerce_bool(task.get("skip_publish"))
    if task.get("publish") not in (None, ""):
        return _coerce_bool(task.get("publish"))
    env_publish = os.getenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_PUBLISH")
    if env_publish not in (None, ""):
        return _coerce_bool(env_publish)
    env_skip = os.getenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_SKIP_PUBLISH")
    if env_skip not in (None, ""):
        return not _coerce_bool(env_skip)
    return not test_mode


def _task_bool_or_env(task: dict, key: str, env_name: str, default: bool) -> bool:
    if task.get(key) not in (None, ""):
        return _coerce_bool(task.get(key))
    return _env_bool(env_name, default)


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
        (setting, value)
        for setting, env_name, default in candidates
        if (value := _env_sql_setting(env_name, default)) is not None
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
    if _env_bool("HLTHPRT_ENTITY_ADDRESS_UNIFIED_DEFER_ADDITIONAL_INDEXES", False):
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


def _post_publish_index_concurrently() -> bool:
    return _env_bool(
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_POST_PUBLISH_INDEX_CONCURRENTLY",
        DEFAULT_POST_PUBLISH_INDEX_CONCURRENTLY,
    )


def _defer_publish_validation() -> bool:
    return _env_bool(
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_DEFER_PUBLISH_VALIDATION",
        DEFAULT_DEFER_PUBLISH_VALIDATION,
    )


def _aggregate_source_record_ids() -> bool:
    return _env_bool(
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_AGGREGATE_SOURCE_RECORD_IDS",
        DEFAULT_AGGREGATE_SOURCE_RECORD_IDS,
    )


def _source_record_ids_select_sql() -> str:
    if _aggregate_source_record_ids():
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


def _require_inline_source_evidence() -> bool:
    return _env_bool(
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_REQUIRE_INLINE_SOURCE_EVIDENCE",
        DEFAULT_REQUIRE_INLINE_SOURCE_EVIDENCE,
    )


def _final_summary_counts() -> bool:
    return _env_bool(
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_FINAL_SUMMARY_COUNTS",
        DEFAULT_FINAL_SUMMARY_COUNTS,
    )


def _keep_raw_stage() -> bool:
    return _env_bool(
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


def _main_index_enabled_for_profile(index_name: str, profile: str) -> bool:
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
        if not _main_index_enabled_for_profile(index_name, profile):
            skipped_indexes.append(f"{table_name}.{index_name}")
            continue
        using = f"USING {index.get('using')} " if index.get("using") else ""
        where = f" WHERE {index.get('where')}" if index.get("where") else ""
        live_index_name = f"{table_name}_idx_{index_name}"
        concurrently = "CONCURRENTLY " if build_concurrently else ""
        stmt = (
            f"CREATE INDEX {concurrently}IF NOT EXISTS {live_index_name} "
            f"ON {db_schema}.{table_name} {using}"
            f"({', '.join(index.get('index_elements'))}){where};"
        )
        statements.append((index_name, stmt))
    return statements, skipped_indexes


def _stage_index_enabled(stage_cls, index: dict) -> bool:
    if _is_support_code_location_index(stage_cls, index):
        return _env_bool(
            "HLTHPRT_ENTITY_ADDRESS_UNIFIED_SUPPORT_CODE_LOCATION_INDEXES",
            DEFAULT_SUPPORT_CODE_LOCATION_INDEXES,
        )
    if getattr(stage_cls, "__main_table__", "") != EntityAddressUnified.__main_table__:
        return True
    index_name = index.get("name", "_".join(index.get("index_elements") or ()))
    return _main_index_enabled_for_profile(index_name, _stage_index_profile())


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


async def _compact_hot_row_source_record_ids(
    db_schema: str,
    stage_table: str,
    *,
    context: dict | None = None,
) -> int:
    phase_context = context if context is not None else {}
    if not _env_bool(
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_COMPACT_SOURCE_RECORD_IDS_BY_REWRITE",
        DEFAULT_COMPACT_SOURCE_RECORD_IDS_BY_REWRITE,
    ):
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
        compact_column = "source_record_ids_compact"
        await _run_sql_phase(
            f"""
            ALTER TABLE {db_schema}.{stage_table}
                DROP COLUMN IF EXISTS {compact_column};
            """,
            context=phase_context,
            phase="entity-address-unified compacting hot rows metadata",
        )
        await _run_sql_phase(
            f"""
            ALTER TABLE {db_schema}.{stage_table}
                ADD COLUMN {compact_column} varchar[] NOT NULL DEFAULT '{{}}'::varchar[];
            """,
            context=phase_context,
            phase="entity-address-unified compacting hot rows metadata",
        )
        await _run_sql_phase(
            f"""
            UPDATE {db_schema}.{stage_table}
               SET {compact_column} = {_compacted_source_record_ids_expr()}
             WHERE address_sources @> ARRAY['provider_directory_fhir']::varchar[];
            """,
            context=phase_context,
            phase="entity-address-unified compacting hot rows metadata",
        )
        await _run_sql_phase(
            f"ALTER TABLE {db_schema}.{stage_table} DROP COLUMN source_record_ids;",
            context=phase_context,
            phase="entity-address-unified compacting hot rows metadata",
        )
        await _run_sql_phase(
            f"""
            ALTER TABLE {db_schema}.{stage_table}
                RENAME COLUMN {compact_column} TO source_record_ids;
            """,
            context=phase_context,
            phase="entity-address-unified compacting hot rows metadata",
        )
        return row_estimate

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


async def _create_stage_indexes(
    stage_cls,
    db_schema: str,
    *,
    context: dict | None = None,
) -> None:
    phase_context = context if context is not None else {}
    if getattr(stage_cls, "__main_table__", "") == EntityAddressUnified.__main_table__:
        phase_context["stage_index_profile"] = _stage_index_profile()
    indexes = list(getattr(stage_cls, "__my_additional_indexes__", []) or [])
    if not indexes:
        return

    statements: list[tuple[str, str]] = []
    for index in indexes:
        index_name = index.get("name", "_".join(index.get("index_elements")))
        if not _stage_index_enabled(stage_cls, index):
            skipped_indexes = phase_context.setdefault("skipped_stage_indexes", [])
            skipped_indexes.append(f"{stage_cls.__tablename__}.{index_name}")
            continue
        using = f"USING {index.get('using')} " if index.get("using") else ""
        where = f" WHERE {index.get('where')}" if index.get("where") else ""
        stmt = (
            f"CREATE INDEX IF NOT EXISTS "
            f"{_stage_index_name(stage_cls.__tablename__, index_name)} "
            f"ON {db_schema}.{stage_cls.__tablename__} {using}"
            f"({', '.join(index.get('index_elements'))}){where};"
        )
        statements.append((index_name, stmt))

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

    async def _build_index(index_name: str, stmt: str) -> None:
        started_at = time.time()
        try:
            await _run_sql_phase(
                stmt,
                context=phase_context,
                phase="entity-address-unified indexing stage",
            )
        except Exception as exc:
            msg = str(exc).lower()
            if "st_makepoint" in msg or "geography" in msg or "postgis" in msg:
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

    if index_concurrency <= 1 or len(statements) == 1:
        for index_name, stmt in statements:
            await _build_index(index_name, stmt)
        return

    semaphore = asyncio.Semaphore(index_concurrency)

    async def _guarded(index_name: str, stmt: str) -> None:
        async with semaphore:
            await _build_index(index_name, stmt)

    results = await asyncio.gather(
        *(_guarded(index_name, stmt) for index_name, stmt in statements),
        return_exceptions=True,
    )
    for result in results:
        if isinstance(result, BaseException):
            raise result


async def _create_post_publish_indexes(
    db_schema: str,
    *,
    context: dict | None = None,
) -> None:
    phase_context = context if context is not None else {}
    profile = _post_publish_index_profile()
    phase_context["post_publish_index_profile"] = profile
    build_concurrently = _post_publish_index_concurrently()
    phase_context["post_publish_index_concurrently"] = build_concurrently
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
        build_concurrently=build_concurrently,
    )
    phase_context["post_publish_skipped_indexes"] = skipped_indexes

    if not statements:
        phase_context["post_publish_index_pending"] = False
        phase_context["post_publish_index_total"] = 0
        phase_context["post_publish_index_completed"] = 0
        return

    phase_context["post_publish_index_pending"] = True
    phase_context["post_publish_index_total"] = len(statements)
    phase_context["post_publish_index_completed"] = 0

    configured_index_concurrency = _env_int(
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_POST_PUBLISH_INDEX_CONCURRENCY",
        _env_int(
            "HLTHPRT_ENTITY_ADDRESS_UNIFIED_STAGE_INDEX_CONCURRENCY",
            DEFAULT_STAGE_INDEX_CONCURRENCY,
            minimum=1,
        ),
        minimum=1,
    )
    index_concurrency = 1 if build_concurrently else min(configured_index_concurrency, len(statements))
    phase_context["post_publish_index_concurrency"] = index_concurrency

    async def _post_publish_index_invalid(live_index_name: str) -> bool:
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
        if not await _post_publish_index_invalid(live_index_name):
            return
        drop_stmt = f"DROP INDEX {'CONCURRENTLY ' if build_concurrently else ''}IF EXISTS {db_schema}.{live_index_name};"
        if build_concurrently and hasattr(db, "execute_ddl"):
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
        completed = False
        try:
            await _drop_invalid_index(live_index_name)
            if build_concurrently and hasattr(db, "execute_ddl"):
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
            completed = True
        except Exception as exc:
            msg = str(exc).lower()
            if "st_makepoint" in msg or "geography" in msg or "postgis" in msg:
                logger.warning(
                    "Skipping post-publish geo index %s because PostGIS is unavailable in current DB: %s",
                    index_name,
                    exc,
                )
                completed = True
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
            if completed:
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
        return

    semaphore = asyncio.Semaphore(index_concurrency)

    async def _guarded(index_name: str, stmt: str) -> None:
        async with semaphore:
            await _build_index(index_name, stmt)

    results = await asyncio.gather(
        *(_guarded(index_name, stmt) for index_name, stmt in statements),
        return_exceptions=True,
    )
    for result in results:
        if isinstance(result, BaseException):
            raise result


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


def _ensure_stage_primary_key_sql(
    db_schema: str,
    table_name: str,
    primary_key_columns: Iterable[str],
) -> str:
    columns = list(primary_key_columns)
    if not columns:
        return ""
    constraint_name = _archived_identifier(table_name, "_pkey")
    column_sql = ", ".join(columns)
    return f"""
    DO $$
    DECLARE
        target_table_oid oid;
        conflicting_relation text;
        conflicting_table_oid oid;
        conflicting_constraint text;
        orphan_index text;
        resolved_constraint_name text := {_sql_literal(constraint_name)};
    BEGIN
        SELECT t.oid
          INTO target_table_oid
          FROM pg_class t
          JOIN pg_namespace n
            ON n.oid = t.relnamespace
         WHERE n.nspname = {_sql_literal(db_schema)}
           AND t.relname = {_sql_literal(table_name)}
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
             WHERE n.nspname = {_sql_literal(db_schema)}
               AND i.relname = {_sql_literal(constraint_name)}
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
                    {_sql_literal(db_schema)},
                    {_sql_literal(table_name)},
                    conflicting_constraint
                );
            ELSIF conflicting_relation IS NOT NULL THEN
                resolved_constraint_name := LEFT({_sql_literal(constraint_name)}, 54)
                    || '_'
                    || SUBSTRING(MD5(target_table_oid::text), 1, 8);
            END IF;

            IF orphan_index IS NOT NULL THEN
                EXECUTE format(
                    'DROP INDEX IF EXISTS %I.%I',
                    {_sql_literal(db_schema)},
                    orphan_index
                );
            END IF;

            EXECUTE format(
                'ALTER TABLE %I.%I ADD CONSTRAINT %I PRIMARY KEY ({column_sql})',
                {_sql_literal(db_schema)},
                {_sql_literal(table_name)},
                resolved_constraint_name
            );
        END IF;
    END $$;
    """


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
    heap_load = _env_bool(
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_SUPPORT_HEAP_LOAD",
        DEFAULT_SUPPORT_HEAP_LOAD,
    )
    for stage_cls in stage_classes.values():
        await db.status(f"DROP TABLE IF EXISTS {db_schema}.{stage_cls.__tablename__};")
        await db.create_table(stage_cls.__table__, checkfirst=True)
        if heap_load:
            await db.status(_drop_stage_primary_key_sql(db_schema, stage_cls.__tablename__))
    return stage_classes


async def _create_support_stage_indexes(
    stage_classes: dict[type, type],
    db_schema: str,
    *,
    context: dict | None = None,
    run_id: str | None = None,
) -> None:
    phase_context = context if context is not None else {}
    stage_tables = list(stage_classes.values())
    if not stage_tables:
        return
    index_concurrency = min(
        _env_int(
            "HLTHPRT_ENTITY_ADDRESS_UNIFIED_SUPPORT_INDEX_CONCURRENCY",
            DEFAULT_SUPPORT_INDEX_CONCURRENCY,
            minimum=1,
        ),
        len(stage_tables),
    )
    phase_context["support_stage_index_concurrency"] = index_concurrency
    progress_lock = asyncio.Lock()
    completed = 0
    total = len(stage_tables)

    async def _index_stage_table(index: int, stage_cls) -> None:
        nonlocal completed
        table_name = stage_cls.__tablename__
        if run_id:
            async with progress_lock:
                current_done = completed
                enqueue_live_progress(
                    run_id=run_id,
                    importer="entity-address-unified",
                    status="running",
                    phase="entity-address-unified indexing support tables",
                    unit="tables",
                    done=current_done,
                    total=total,
                    pct=99,
                    message=(
                        f"indexing support table {index}/{total}: {table_name} "
                        f"(concurrency {index_concurrency})"
                    ),
                )
        await _ensure_stage_primary_key(stage_cls, db_schema, context=phase_context)
        await _create_stage_indexes(stage_cls, db_schema, context=phase_context)
        if run_id:
            async with progress_lock:
                completed += 1
                enqueue_live_progress(
                    run_id=run_id,
                    importer="entity-address-unified",
                    status="running",
                    phase="entity-address-unified indexing support tables",
                    unit="tables",
                    done=completed,
                    total=total,
                    pct=99,
                    message=f"indexed support table {index}/{total}: {table_name}",
                )

    if index_concurrency <= 1 or len(stage_tables) == 1:
        for index, stage_cls in enumerate(stage_tables, start=1):
            await _index_stage_table(index, stage_cls)
        return

    semaphore = asyncio.Semaphore(index_concurrency)

    async def _guarded(index: int, stage_cls) -> None:
        async with semaphore:
            await _index_stage_table(index, stage_cls)

    results = await asyncio.gather(
        *(_guarded(index, stage_cls) for index, stage_cls in enumerate(stage_tables, start=1)),
        return_exceptions=True,
    )
    for result in results:
        if isinstance(result, BaseException):
            raise result


async def _swap_stage_table(db_schema: str, live_cls, stage_cls) -> None:
    table = live_cls.__main_table__
    await db.status(f"DROP TABLE IF EXISTS {db_schema}.{table}_old;")
    await db.status(f"ALTER TABLE IF EXISTS {db_schema}.{table} RENAME TO {table}_old;")
    await db.status(f"ALTER TABLE IF EXISTS {db_schema}.{stage_cls.__tablename__} RENAME TO {table};")

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


async def _drop_stage_artifacts(
    db_schema: str,
    stage_cls,
    support_stage_classes: dict[type, type],
    *,
    extra_tables: Iterable[str] = (),
) -> None:
    await db.status(f"DROP TABLE IF EXISTS {db_schema}.{stage_cls.__tablename__};")
    for stage_model in support_stage_classes.values():
        await db.status(f"DROP TABLE IF EXISTS {db_schema}.{stage_model.__tablename__};")
    for table_name in extra_tables:
        if table_name:
            await db.status(f"DROP TABLE IF EXISTS {db_schema}.{table_name};")


async def _table_exists(db_schema: str, table_name: str) -> bool:
    return bool(await db.scalar(f"SELECT to_regclass('{db_schema}.{table_name}') IS NOT NULL;"))


async def _table_column_exists(db_schema: str, table_name: str, column_name: str) -> bool:
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
    if not await _table_exists(db_schema, table_name):
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
    existing = {
        str((row._mapping if hasattr(row, "_mapping") else row).get("column_name"))
        for row in existing_rows
    }
    dialect = postgresql.dialect()
    for column in EntityAddressUnified.__table__.columns:
        if column.name in existing:
            continue
        column_ddl = str(CreateColumn(column).compile(dialect=dialect)).strip()
        await db.status(
            f"ALTER TABLE {db_schema}.{table_name} ADD COLUMN IF NOT EXISTS {column_ddl};"
        )


async def _support_bridge_reuse_available(db_schema: str, *, build_network_bridge: bool) -> bool:
    bridge_models: list[type] = [
        EntityAddressPlanBridge,
        EntityAddressProcedureBridge,
        EntityAddressMedicationBridge,
    ]
    if build_network_bridge:
        bridge_models.append(EntityAddressNetworkBridge)
    for model in bridge_models:
        if not await _table_exists(db_schema, model.__main_table__):
            return False
    return True


def _promote_approved_facility_anchor_npi_candidates_sql(db_schema: str) -> str:
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
        await _table_exists(db_schema, "facility_anchor_npi_candidate")
        and await _table_exists(db_schema, "facility_anchor_npi_override")
    ):
        return 0
    return int(
        await db.status(_promote_approved_facility_anchor_npi_candidates_sql(db_schema))
        or 0
    )


async def _address_canon_available(db_schema: str) -> bool:
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


def _address_checksum_expr(
    *,
    entity_type_expr: str = "entity_type",
    entity_id_expr: str = "entity_id",
    type_expr: str = "type",
    first_line_expr: str = "first_line",
    second_line_expr: str = "second_line",
    city_name_expr: str = "city_name",
    state_name_expr: str = "state_name",
    postal_code_expr: str = "postal_code",
    country_code_expr: str = "country_code",
    telephone_number_expr: str = "telephone_number",
) -> str:
    return (
        "(('x' || substr(md5(lower(concat_ws('|', "
        f"COALESCE({entity_type_expr}, ''), "
        f"COALESCE({entity_id_expr}, ''), "
        f"COALESCE({type_expr}, ''), "
        f"COALESCE({first_line_expr}, ''), "
        f"COALESCE({second_line_expr}, ''), "
        f"COALESCE({city_name_expr}, ''), "
        f"COALESCE({state_name_expr}, ''), "
        f"COALESCE({postal_code_expr}, ''), "
        f"COALESCE({country_code_expr}, ''), "
        f"COALESCE({telephone_number_expr}, '')"
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


def _location_key_expr(
    *,
    entity_type: str = "entity_type",
    entity_id: str = "entity_id",
    npi: str = "npi",
    inferred_npi: str = "inferred_npi",
    address_role_id: str = "address_role_id",
    row_origin: str = "row_origin",
    address_key: str = "address_key",
    source_id: str = "source_id",
    source_record_id: str = "source_record_id",
    zip5: str = "zip5",
    state_code: str = "state_code",
    city_norm: str = "city_norm",
) -> str:
    identity = (
        "CASE WHEN "
        f"{address_key} IS NOT NULL THEN concat_ws('|', "
        "'v1', "
        f"COALESCE({entity_type}, ''), COALESCE({entity_id}, ''), "
        f"COALESCE({npi}::text, ''), COALESCE({inferred_npi}::text, ''), "
        "''::text, "
        f"COALESCE({address_role_id}::text, ''), COALESCE({row_origin}, ''), "
        f"COALESCE({address_key}::text, '')) "
        "ELSE concat_ws('|', "
        "'v1', 'fallback', "
        f"COALESCE({entity_type}, ''), COALESCE({entity_id}, ''), "
        f"COALESCE({npi}::text, ''), COALESCE({inferred_npi}::text, ''), "
        "''::text, "
        f"COALESCE({address_role_id}::text, ''), COALESCE({row_origin}, ''), "
        f"COALESCE({source_id}::text, ''), COALESCE({source_record_id}, ''), "
        f"COALESCE({zip5}, ''), COALESCE({state_code}, ''), COALESCE({city_norm}, '')) END"
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


def _nppes_zip_restore_join(db_schema: str, *, alias: str = "a") -> str:
    """Return a lateral join that safely restores missing NPPES ZIPs from archive evidence."""
    return f"""
              LEFT JOIN LATERAL (
                WITH candidate_rows AS (
                    SELECT
                        aa.zip5::varchar AS zip5,
                        COUNT(*)::int AS archive_rows,
                        bit_count(bit_or(COALESCE(aa.source_bits, 0))::bit(64))::int AS source_ref_count,
                        bit_or(COALESCE(aa.source_bits, 0))::bigint AS source_bits
                      FROM {db_schema}.address_archive_v2 AS aa
                     WHERE aa.merged_into IS NULL
                       AND aa.zip5 IS NOT NULL
                       AND {db_schema}.addr_country_code_v1(COALESCE(NULLIF({alias}.country_code, ''), 'US')) = 'US'
                       AND {db_schema}.addr_zip5_norm_v1({alias}.postal_code) IS NULL
                       AND {db_schema}.addr_street_norm_v1({alias}.first_line, {alias}.second_line) IS NOT NULL
                       AND aa.line1_norm = {db_schema}.addr_street_norm_v1({alias}.first_line, {alias}.second_line)
                       AND COALESCE(aa.unit_norm, '') = ''
                       AND aa.city_norm = {db_schema}.addr_city_norm_v1({alias}.city_name)
                       AND aa.state_code = {db_schema}.addr_state_code_v1({alias}.state_name)
                       AND COALESCE(aa.country_code, 'US') = 'US'
                     GROUP BY aa.zip5
                ),
                ranked AS (
                    SELECT
                        candidate_rows.*,
                        COUNT(*) OVER () AS candidate_count,
                        LEAD(zip5) OVER restore_rank AS next_zip5,
                        LEAD(source_ref_count) OVER restore_rank AS next_source_ref_count,
                        LEAD(archive_rows) OVER restore_rank AS next_archive_rows,
                        ROW_NUMBER() OVER restore_rank AS restore_rank
                      FROM candidate_rows
                    WINDOW restore_rank AS (
                        ORDER BY source_ref_count DESC, archive_rows DESC, source_bits DESC, zip5 ASC
                    )
                )
                SELECT zip5 AS restored_zip5
                  FROM ranked
                 WHERE restore_rank = 1
                   AND (
                        candidate_count = 1
                        OR (
                            candidate_count > 1
                            AND (
                                source_ref_count > COALESCE(next_source_ref_count, -1)
                                OR (
                                    source_ref_count = COALESCE(next_source_ref_count, -1)
                                    AND archive_rows > COALESCE(next_archive_rows, -1)
                                )
                            )
                            AND (source_ref_count > 1 OR archive_rows > 1)
                            AND next_zip5 IS NOT NULL
                            AND (
                                LEFT(zip5, 3) = LEFT(next_zip5, 3)
                                OR ABS(zip5::int - next_zip5::int) <= 10
                            )
                        )
                   )
                 LIMIT 1
              ) AS nppes_zip_restore ON TRUE"""


def _nppes_postal_code_expr(db_schema: str, *, alias: str = "a", restore_alias: str = "nppes_zip_restore") -> str:
    return (
        "CASE "
        f"WHEN {restore_alias}.restored_zip5 IS NOT NULL "
        f"AND {db_schema}.addr_zip5_norm_v1({alias}.postal_code) IS NULL "
        f"THEN {restore_alias}.restored_zip5 "
        f"ELSE {alias}.postal_code END"
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
    selected_source_filter = (
        f"split_part(pd_rid.rid, ':', 3) = ANY({_string_array_literal(list(source_ids))})"
    )
    return f"""
       AND EXISTS (
            SELECT 1
              FROM unnest(COALESCE({alias}.source_record_ids, ARRAY[]::varchar[])) AS pd_rid(rid)
             WHERE pd_rid.rid LIKE 'provider_directory_fhir:%'
               AND NULLIF(split_part(pd_rid.rid, ':', 3), '') IS NOT NULL
               AND (
                    {selected_source_filter}
                    OR NOT EXISTS (
                        SELECT 1
                          FROM {db_schema}.provider_directory_source AS source
                         WHERE source.source_id = split_part(pd_rid.rid, ':', 3)
                    )
               )
       )"""


def _latest_provider_directory_partial_scope_sql(db_schema: str) -> str:
    source_ref = f"{db_schema}.provider_directory_source"
    entity_address_ref = f"{db_schema}.{EntityAddressUnified.__main_table__}"
    location_ref = f"{db_schema}.provider_directory_location"
    organization_ref = f"{db_schema}.provider_directory_organization"
    role_ref = f"{db_schema}.provider_directory_practitioner_role"
    healthcare_service_ref = f"{db_schema}.provider_directory_healthcare_service"
    affiliation_ref = f"{db_schema}.provider_directory_organization_affiliation"
    return f"""
    WITH completed_sources AS (
        SELECT
            source_id::varchar AS source_id,
            metadata_json::jsonb->'last_resource_import'->>'run_id' AS run_id,
            updated_at,
            'metadata'::varchar AS scope_source
          FROM {source_ref}
         WHERE COALESCE(metadata_json::jsonb, '{{}}'::jsonb) ? 'last_resource_import'
           AND (
                (
                    metadata_json::jsonb->'last_resource_import'->'resources'->'Location'->>'complete' = 'true'
                    AND (
                        metadata_json::jsonb->'last_resource_import'->'resources'->'PractitionerRole'->>'complete' = 'true'
                     OR metadata_json::jsonb->'last_resource_import'->'resources'->'OrganizationAffiliation'->>'complete' = 'true'
                    )
                )
             OR metadata_json::jsonb->'last_resource_import'->'resources'->'Organization'->>'complete' = 'true'
           )
    ), location_eligible_source_runs AS (
        SELECT DISTINCT
            role.source_id::varchar AS source_id,
            role.last_seen_run_id::varchar AS run_id
          FROM {role_ref} AS role
          JOIN {db_schema}.provider_directory_practitioner AS practitioner
            ON practitioner.source_id = role.source_id
           AND practitioner.resource_id = NULLIF(
                regexp_replace(COALESCE(role.practitioner_ref, ''), '^.*/', ''),
                ''
           )
         WHERE role.last_seen_run_id IS NOT NULL
           AND practitioner.npi BETWEEN 1000000000 AND 9999999999
           AND practitioner.active IS DISTINCT FROM false
           AND role.active IS DISTINCT FROM false
        UNION
        SELECT DISTINCT
            affiliation.source_id::varchar AS source_id,
            affiliation.last_seen_run_id::varchar AS run_id
          FROM {affiliation_ref} AS affiliation
          JOIN LATERAL (
              SELECT DISTINCT normalized_ref AS resource_id
                FROM (
                    VALUES
                        (NULLIF(regexp_replace(COALESCE(affiliation.organization_ref, ''), '^.*/', ''), '')),
                        (NULLIF(regexp_replace(COALESCE(affiliation.participating_organization_ref, ''), '^.*/', ''), ''))
                ) AS refs(normalized_ref)
               WHERE normalized_ref IS NOT NULL
          ) AS organization_ref ON TRUE
          JOIN {organization_ref} AS organization
            ON organization.source_id = affiliation.source_id
           AND organization.resource_id = organization_ref.resource_id
         WHERE affiliation.last_seen_run_id IS NOT NULL
           AND organization.npi BETWEEN 1000000000 AND 9999999999
           AND organization.active IS DISTINCT FROM false
           AND affiliation.active IS DISTINCT FROM false
        UNION
        SELECT DISTINCT
            healthcare_service.source_id::varchar AS source_id,
            healthcare_service.last_seen_run_id::varchar AS run_id
          FROM {healthcare_service_ref} AS healthcare_service
         WHERE healthcare_service.last_seen_run_id IS NOT NULL
           AND healthcare_service.active IS DISTINCT FROM false
           AND jsonb_array_length(COALESCE(healthcare_service.location_refs::jsonb, '[]'::jsonb)) > 0
    ), location_row_sources AS (
        SELECT
            loc.source_id::varchar AS source_id,
            loc.last_seen_run_id::varchar AS run_id,
            MAX(loc.updated_at) AS updated_at,
            'resource_rows'::varchar AS scope_source
          FROM {location_ref} AS loc
          JOIN location_eligible_source_runs AS eligible
            ON eligible.source_id = loc.source_id
           AND eligible.run_id = loc.last_seen_run_id
         WHERE loc.last_seen_run_id IS NOT NULL
      GROUP BY loc.source_id, loc.last_seen_run_id
    ), organization_row_sources AS (
        SELECT
            organization.source_id::varchar AS source_id,
            organization.last_seen_run_id::varchar AS run_id,
            MAX(organization.updated_at) AS updated_at,
            'resource_rows'::varchar AS scope_source
          FROM {organization_ref} AS organization
         WHERE organization.last_seen_run_id IS NOT NULL
           AND organization.npi BETWEEN 1000000000 AND 9999999999
           AND organization.active IS DISTINCT FROM false
           AND jsonb_array_length(COALESCE(organization.address_json::jsonb, '[]'::jsonb)) > 0
      GROUP BY organization.source_id, organization.last_seen_run_id
    ), resource_row_sources AS (
        SELECT
            source_id,
            run_id,
            MAX(updated_at) AS updated_at,
            'resource_rows'::varchar AS scope_source
          FROM (
                SELECT * FROM location_row_sources
                UNION ALL
                SELECT * FROM organization_row_sources
          ) AS row_sources
      GROUP BY source_id, run_id
    ), eligible_completed_sources AS (
        SELECT completed.*
          FROM completed_sources AS completed
         WHERE EXISTS (
                SELECT 1
                  FROM resource_row_sources AS row_source
                 WHERE row_source.source_id = completed.source_id
         )
    ), candidate_sources AS (
        SELECT * FROM eligible_completed_sources
        UNION ALL
        SELECT resource_row_sources.*
          FROM resource_row_sources
         WHERE NOT EXISTS (SELECT 1 FROM eligible_completed_sources)
    ), projected_sources AS (
        SELECT DISTINCT split_part(pd_rid.rid, ':', 3)::varchar AS source_id
          FROM {entity_address_ref} AS live
          JOIN LATERAL unnest(COALESCE(live.source_record_ids, ARRAY[]::varchar[])) AS pd_rid(rid)
            ON TRUE
         WHERE pd_rid.rid LIKE 'provider_directory_fhir:%'
           AND NULLIF(split_part(pd_rid.rid, ':', 3), '') IS NOT NULL
    ), unprojected_sources AS (
        SELECT
            candidate.source_id,
            NULL::varchar AS run_id,
            MAX(candidate.updated_at) AS updated_at,
            ('unprojected_' || MAX(candidate.scope_source))::varchar AS scope_source
          FROM candidate_sources AS candidate
         WHERE NOT EXISTS (
                SELECT 1
                  FROM projected_sources AS projected
                 WHERE projected.source_id = candidate.source_id
         )
      GROUP BY candidate.source_id
    ), selected_sources AS (
        SELECT * FROM unprojected_sources
         WHERE EXISTS (SELECT 1 FROM unprojected_sources)
        UNION ALL
        SELECT * FROM candidate_sources
         WHERE NOT EXISTS (SELECT 1 FROM unprojected_sources)
    )
    SELECT
        run_id,
        ARRAY_AGG(source_id ORDER BY source_id)::varchar[] AS source_ids,
        COUNT(*)::bigint AS source_count,
        MAX(updated_at) AS latest_updated_at,
        ARRAY_AGG(DISTINCT scope_source ORDER BY scope_source)::varchar[] AS scope_sources
      FROM selected_sources
  GROUP BY run_id
  ORDER BY (run_id IS NULL) DESC, MAX(updated_at) DESC NULLS LAST, run_id DESC
     LIMIT 1;
    """


async def _latest_provider_directory_partial_scope(db_schema: str) -> tuple[str | None, list[str], list[str]]:
    if not await _table_exists(db_schema, "provider_directory_source"):
        return None, [], []
    row = await db.first(_latest_provider_directory_partial_scope_sql(db_schema))
    if not row:
        return None, [], []
    values = row._mapping if hasattr(row, "_mapping") else row
    source_ids = _coerce_str_list(values.get("source_ids"))
    scope_sources = _coerce_str_list(values.get("scope_sources"))
    return _clean_optional(values.get("run_id")), source_ids, scope_sources


def _source_selects(
    db_schema: str,
    available: dict[str, bool],
    *,
    test_limit_per_source: int | None = None,
    provider_directory_source_ids: list[str] | tuple[str, ...] | None = None,
    provider_directory_run_id: str | None = None,
) -> list[str]:
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
    has_archive = available.get("address_archive_v2", False)

    def source_address_key(table_name: str, alias: str) -> str:
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
    nppes_zip_restore_join = _nppes_zip_restore_join(db_schema) if has_archive else ""
    nppes_postal_code = _nppes_postal_code_expr(db_schema, alias="a") if has_archive else "a.postal_code"
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
                {nppes_postal_code}::varchar AS postal_code,
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
              {nppes_zip_restore_join}
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
                    role.network_refs::jsonb AS network_refs,
                    loc.resource_id AS location_resource_id,
                    loc.name::varchar AS location_name,
                    loc.first_line::varchar AS first_line,
                    loc.second_line::varchar AS second_line,
                    loc.city_name::varchar AS city_name,
                    COALESCE(NULLIF(loc.state_name, ''), loc.state_code)::varchar AS state_name,
                    loc.postal_code::varchar AS postal_code,
                    COALESCE(NULLIF(loc.country_code, ''), 'US')::varchar AS country_code,
                    COALESCE(role_phone.telephone_number, loc.telephone_number)::varchar AS telephone_number,
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
                ARRAY[]::varchar[] AS aca_plan_array,
                ARRAY(
                    SELECT DISTINCT network_ref.value::varchar
                      FROM jsonb_array_elements_text(COALESCE(pd.network_refs, '[]'::jsonb)) AS network_ref(value)
                     WHERE NULLIF(network_ref.value, '') IS NOT NULL
                  ORDER BY 1
                )::varchar[] AS aca_network_array,
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
                CASE
                    WHEN pd.latitude ~ '^-?[0-9]+(\\.[0-9]+)?$'
                     AND pd.latitude::numeric BETWEEN -90 AND 90
                        THEN pd.latitude::numeric
                    ELSE NULL::numeric
                END AS lat,
                CASE
                    WHEN pd.longitude ~ '^-?[0-9]+(\\.[0-9]+)?$'
                     AND pd.longitude::numeric BETWEEN -180 AND 180
                        THEN pd.longitude::numeric
                    ELSE NULL::numeric
                END AS long,
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
                    affiliation.network_refs::jsonb AS network_refs,
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
                ARRAY(
                    SELECT DISTINCT network_ref.value::varchar
                      FROM jsonb_array_elements_text(COALESCE(pd.network_refs, '[]'::jsonb)) AS network_ref(value)
                     WHERE NULLIF(network_ref.value, '') IS NOT NULL
                  ORDER BY 1
                )::varchar[] AS aca_network_array,
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
                CASE
                    WHEN pd.latitude ~ '^-?[0-9]+(\\.[0-9]+)?$'
                     AND pd.latitude::numeric BETWEEN -90 AND 90
                        THEN pd.latitude::numeric
                    ELSE NULL::numeric
                END AS lat,
                CASE
                    WHEN pd.longitude ~ '^-?[0-9]+(\\.[0-9]+)?$'
                     AND pd.longitude::numeric BETWEEN -180 AND 180
                        THEN pd.longitude::numeric
                    ELSE NULL::numeric
                END AS long,
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
                NULL::uuid AS address_key,
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


def _provider_directory_partial_affected_group_table_name(stage_table: str) -> str:
    return _archived_identifier(stage_table, "_pd_groups")


def _provider_directory_affected_live_location_table_name(stage_table: str) -> str:
    return _archived_identifier(stage_table, "_pd_live_locations")


def _is_provider_directory_source_select(db_schema: str, source_select: str) -> bool:
    return (
        f"FROM {db_schema}.provider_directory_practitioner_role AS role" in source_select
        or f"FROM {db_schema}.provider_directory_organization_affiliation AS affiliation" in source_select
        or f"FROM {db_schema}.provider_directory_organization AS organization" in source_select
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


def _prepare_provider_directory_partial_affected_groups_sql(
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


def _index_provider_directory_partial_affected_groups_sql(db_schema: str, group_table: str) -> str:
    return _index_affected_groups_sql(db_schema, group_table)


def _provider_directory_partial_scope_index_sql(db_schema: str) -> list[str]:
    return [
        f"""
        CREATE INDEX IF NOT EXISTS provider_directory_role_run_source_idx
            ON {db_schema}.provider_directory_practitioner_role (last_seen_run_id, source_id);
        """,
        f"""
        CREATE INDEX IF NOT EXISTS provider_directory_organization_run_source_idx
            ON {db_schema}.provider_directory_organization (last_seen_run_id, source_id);
        """,
        f"""
        CREATE INDEX IF NOT EXISTS provider_directory_affiliation_run_source_idx
            ON {db_schema}.provider_directory_organization_affiliation (last_seen_run_id, source_id);
        """,
        f"""
        CREATE INDEX IF NOT EXISTS provider_directory_healthcare_service_run_source_idx
            ON {db_schema}.provider_directory_healthcare_service (last_seen_run_id, source_id);
        """,
    ]


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
    filtered: list[str] = []
    provider_selects = 0
    for source_select in source_selects:
        if _is_provider_directory_source_select(db_schema, source_select):
            filtered.append(source_select)
            provider_selects += 1
        else:
            filtered.append(
                _affected_npi_source_select_sql(db_schema, source_select, affected_group_table)
            )
    if provider_selects == 0:
        return []
    return filtered


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


def _prepare_provider_directory_affected_live_locations_sql(
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


def _index_provider_directory_affected_live_locations_sql(
    db_schema: str,
    affected_location_table: str,
) -> str:
    index_name = _archived_identifier(f"{affected_location_table}_idx_location", "")
    return f"""
    CREATE UNIQUE INDEX {index_name}
        ON {db_schema}.{affected_location_table} (location_key);
    """


def _copy_unaffected_live_entity_rows_by_location_sql(
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


def _shard_source_selects(
    db_schema: str,
    source_selects: list[str],
    *,
    npi_address_ranges: list[tuple[int, int]] | None = None,
    mrf_address_ranges: list[tuple[int, int]] | None = None,
    doctor_clinician_address_ranges: list[tuple[int, int]] | None = None,
    provider_enrollment_ffs_ranges: list[tuple[int, int]] | None = None,
) -> list[str]:
    npi_address_ranges = npi_address_ranges or []
    mrf_address_ranges = mrf_address_ranges or []
    doctor_clinician_address_ranges = doctor_clinician_address_ranges or []
    provider_enrollment_ffs_ranges = provider_enrollment_ffs_ranges or []
    sharded: list[str] = []
    shard_specs = [
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
            doctor_clinician_address_ranges,
        ),
        (
            f"FROM {db_schema}.provider_enrollment_ffs AS f",
            "'provider_enrollment_ffs'::varchar AS address_source",
            "WHERE f.npi IS NOT NULL",
            "f",
            provider_enrollment_ffs_ranges,
        ),
        (
            f"FROM {db_schema}.provider_enrollment_ffs_address AS fa",
            "'provider_enrollment_ffs_address'::varchar AS address_source",
            "WHERE f.npi IS NOT NULL",
            "f",
            provider_enrollment_ffs_ranges,
        ),
    ]

    for select_sql in source_selects:
        ranges: list[tuple[int, int]] = []
        where_marker = ""
        alias = ""
        for table_marker, source_marker, candidate_where, candidate_alias, candidate_ranges in shard_specs:
            if table_marker in select_sql and source_marker in select_sql:
                ranges = candidate_ranges
                where_marker = candidate_where
                alias = candidate_alias
                break
        if not ranges or not where_marker or where_marker not in select_sql:
            sharded.append(select_sql)
            continue
        for low, high in ranges:
            predicate = f"{where_marker}\n               AND {alias}.npi >= {low}\n               AND {alias}.npi < {high}"
            sharded.append(select_sql.replace(where_marker, predicate, 1))
    return sharded


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


def _prepare_raw_stage_sql(db_schema: str, raw_table: str, *, unlogged: bool = True) -> str:
    storage_mode = "UNLOGGED " if unlogged else ""
    return f"""
    CREATE {storage_mode}TABLE {db_schema}.{raw_table} (
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
    );
    """


def _address_key_expr(
    db_schema: str,
    available: bool,
    *,
    address_source: str | None = None,
    table_alias: str | None = None,
) -> str:
    def col(name: str) -> str:
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


def _enrich_raw_stage_sql(
    db_schema: str,
    raw_table: str,
    *,
    archive_available: bool = True,
    address_canon_available: bool = True,
    checksum_min: int | None = None,
    checksum_max: int | None = None,
    evidence_shards: int | None = None,
) -> str:
    archive_join = ""
    archive_fields = ""
    if archive_available:
        computed_address_key = _address_key_expr(
            db_schema,
            address_canon_available,
            address_source="r.address_source",
            table_alias="r",
        )
        trust_source_key = _env_bool(
            "HLTHPRT_ENTITY_ADDRESS_UNIFIED_TRUST_SOURCE_ADDRESS_KEY",
            DEFAULT_TRUST_SOURCE_ADDRESS_KEY,
        )
        if trust_source_key:
            archive_fields = (
                "COALESCE(a_direct.address_key, a_fallback.address_key) AS archive_address_key, "
                "COALESCE(a_direct.premise_key, a_fallback.premise_key) AS premise_key, "
                "'v' || COALESCE(COALESCE(a_direct.identity_version, a_fallback.identity_version), 2)::text AS archive_identity_version, "
                "COALESCE(a_direct.precision, a_fallback.precision, 'unknown') AS address_precision, "
                "COALESCE(a_direct.zip5, a_fallback.zip5) AS archive_zip5, "
                "NULLIF(upper(left(COALESCE(a_direct.state_code, a_fallback.state_code), 2)), '') AS archive_state_code, "
                "COALESCE(a_direct.city_norm, a_fallback.city_norm) AS archive_city_norm, "
                "NULL::varchar AS archive_county_fips, "
                "COALESCE(a_direct.formatted_address, a_fallback.formatted_address)::varchar AS archive_formatted_address, "
                "COALESCE(a_direct.lat, a_fallback.lat)::numeric AS archive_lat, "
                "COALESCE(a_direct.long, a_fallback.long)::numeric AS archive_long, "
                "COALESCE(a_direct.place_id, a_fallback.place_id)::varchar AS archive_place_id"
            )
            archive_join = (
                f"""
          LEFT JOIN {db_schema}.address_archive_v2 a_direct
            ON a_direct.address_key = r.address_key
           AND a_direct.merged_into IS NULL
          LEFT JOIN LATERAL (
              SELECT aa.*
                FROM {db_schema}.address_archive_v2 aa
               WHERE a_direct.address_key IS NULL
                 AND aa.address_key = {computed_address_key}
                 AND aa.merged_into IS NULL
               LIMIT 1
          ) a_fallback ON TRUE"""
            )
        else:
            archive_fields = (
                "a.address_key AS archive_address_key, "
                "a.premise_key, "
                "'v' || COALESCE(a.identity_version, 2)::text AS archive_identity_version, "
                "COALESCE(a.precision, 'unknown') AS address_precision, "
                "a.zip5 AS archive_zip5, "
                "NULLIF(upper(left(a.state_code, 2)), '') AS archive_state_code, "
                "a.city_norm AS archive_city_norm, "
                "NULL::varchar AS archive_county_fips, "
                "a.formatted_address::varchar AS archive_formatted_address, "
                "a.lat::numeric AS archive_lat, "
                "a.long::numeric AS archive_long, "
                "a.place_id::varchar AS archive_place_id"
            )
            archive_join = (
                f"""
          LEFT JOIN LATERAL (
              SELECT aa.*
                FROM (VALUES ({computed_address_key}, 0), (r.address_key, 1)) AS candidate(address_key, priority)
                JOIN {db_schema}.address_archive_v2 aa
                  ON aa.address_key = candidate.address_key
                 AND aa.merged_into IS NULL
               WHERE candidate.address_key IS NOT NULL
            ORDER BY candidate.priority
               LIMIT 1
          ) a ON TRUE"""
            )
    else:
        archive_fields = (
            "NULL::uuid AS archive_address_key, "
            "NULL::uuid AS premise_key, "
            f"'{ARCHIVE_IDENTITY_VERSION}'::varchar AS archive_identity_version, "
            "CASE WHEN r.address_key IS NULL THEN 'unknown' ELSE 'street' END::varchar AS address_precision, "
            "NULL::varchar AS archive_zip5, "
            "NULL::varchar AS archive_state_code, "
            "NULL::varchar AS archive_city_norm, "
            "NULL::varchar AS archive_county_fips, "
            "NULL::varchar AS archive_formatted_address, "
            "NULL::numeric AS archive_lat, "
            "NULL::numeric AS archive_long, "
            "NULL::varchar AS archive_place_id"
        )
    checksum_where = ""
    if checksum_min is not None and checksum_max is not None:
        checksum_where = f"WHERE r.checksum >= {int(checksum_min)} AND r.checksum < {int(checksum_max)}"
    evidence_shard_set = ""
    if evidence_shards and int(evidence_shards) > 1:
        evidence_shard_set = (
            "           evidence_shard = "
            f"{_evidence_group_hash_expr_for_alias('k', int(evidence_shards))},\n"
        )
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
    ),
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
            archive_formatted_address,
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
    )
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
           formatted_address = COALESCE(k.archive_formatted_address, r.formatted_address),
           lat = COALESCE(k.archive_lat, r.lat),
           long = COALESCE(k.archive_long, r.long),
           place_id = COALESCE(k.archive_place_id, r.place_id),
           row_origin = k.row_origin,
           ptg_source_array = CASE
               WHEN k.source_id = 7 AND COALESCE(CARDINALITY(r.ptg_source_array), 0) = 0
                   THEN ARRAY[r.address_source]::varchar[]
               ELSE r.ptg_source_array
           END,
{evidence_shard_set}           base_address_version = '{BASE_ADDRESS_VERSION}',
           location_key = {_location_key_expr(
               entity_type='r.entity_type',
               entity_id='r.entity_id',
               npi='r.npi',
               inferred_npi='r.inferred_npi',
               address_role_id='k.address_role_id',
               row_origin='k.row_origin',
               address_key='k.address_key',
               source_id='k.source_id',
               source_record_id='r.source_record_id',
               zip5='k.zip5',
               state_code='k.state_code',
               city_norm='k.city_norm',
           )}
      FROM keyed k
     WHERE r.ctid = k.row_id;
    """


def _key_v2_enabled() -> bool:
    return _env_bool("HLTHPRT_ENTITY_ADDRESS_UNIFIED_KEY_V2", False)


def _dedupe_key_expr(address_canon_available: bool) -> str:
    return "location_key"


def _aggregate_shard_expr(dedupe_key_expr: str, aggregate_shards: int) -> str:
    shards = max(int(aggregate_shards), 1)
    return f"((hashtext({dedupe_key_expr}) % {shards} + {shards}) % {shards})"


def _raw_aggregate_group_index_sql(
    db_schema: str,
    raw_table: str,
    *,
    aggregate_shards: int,
    address_canon_available: bool = True,
    inline_source_evidence: bool = False,
) -> str:
    dedupe_key_expr = _dedupe_key_expr(address_canon_available)
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
             WHERE (lat IS NOT NULL AND (lat < -90 OR lat > 90))
                OR (long IS NOT NULL AND (long < -180 OR long > 180));
            """
        )
        or 0
    )


async def _location_key_primary_key_validated(db_schema: str, table_name: str) -> bool:
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
    support_stage_classes: dict[type, type],
    *,
    test_mode: bool,
) -> dict[str, int | dict[str, int]]:
    if test_mode:
        return {}

    failures: list[str] = []
    metrics: dict[str, int | dict[str, int]] = {}

    location_key_constraint_validated = await _location_key_primary_key_validated(db_schema, stage_table)
    metrics["location_key_constraint_validated"] = location_key_constraint_validated
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
    metrics["null_location_keys"] = null_location_keys
    metrics["duplicate_location_keys"] = duplicate_location_keys
    if duplicate_location_keys:
        failures.append(f"{duplicate_location_keys} duplicate staged location_key values")

    unresolved_merged_into_rows = 0
    missing_archive_address_key_rows = 0
    archive_coordinate_mismatch_rows = 0
    archive_missing_coordinate_rows = 0
    archive_identity_mismatch_rows = 0
    if await _table_exists(db_schema, "address_archive_v2"):
        (
            unresolved_merged_into_rows,
            archive_coordinate_mismatch_rows,
            archive_missing_coordinate_rows,
            missing_archive_address_key_rows,
            archive_identity_mismatch_rows,
        ) = (
            int(value or 0)
            for value in await asyncio.gather(
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
                       t.lat IS DISTINCT FROM a.lat
                    OR t.long IS DISTINCT FROM a.long
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
    metrics["unresolved_merged_into_rows"] = unresolved_merged_into_rows
    if unresolved_merged_into_rows:
        failures.append(
            f"{unresolved_merged_into_rows} staged rows point to address_archive_v2.merged_into redirects"
        )
    metrics["missing_archive_address_key_rows"] = missing_archive_address_key_rows
    if missing_archive_address_key_rows:
        failures.append(
            f"{missing_archive_address_key_rows} staged rows have address_key values missing from address_archive_v2"
        )
    metrics["archive_coordinate_mismatch_rows"] = archive_coordinate_mismatch_rows
    if archive_coordinate_mismatch_rows:
        failures.append(
            f"{archive_coordinate_mismatch_rows} staged rows do not match address_archive_v2 coordinates"
        )
    metrics["archive_missing_coordinate_rows"] = archive_missing_coordinate_rows
    if archive_missing_coordinate_rows and _env_bool(
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
    metrics["practice_null_address_key_rows"] = practice_null_address_key_rows
    metrics["practice_null_address_key_by_source"] = {
        str(row._mapping["source"]): int(row._mapping["rows"] or 0)
        for row in practice_null_address_key_by_source_rows
    }

    metrics["archive_identity_mismatch_rows"] = archive_identity_mismatch_rows
    if archive_identity_mismatch_rows:
        failures.append(
            f"{archive_identity_mismatch_rows} staged rows do not match address_archive_v2 identity_version"
        )

    fallback_archive_identity_mismatch_rows = int(fallback_archive_identity_mismatch_rows_raw or 0)
    metrics["fallback_archive_identity_mismatch_rows"] = fallback_archive_identity_mismatch_rows
    if fallback_archive_identity_mismatch_rows:
        failures.append(
            f"{fallback_archive_identity_mismatch_rows} staged rows without address_key use a non-current archive_identity_version"
        )

    metrics["invalid_coordinate_rows"] = invalid_coordinate_rows
    if invalid_coordinate_rows:
        failures.append(f"{invalid_coordinate_rows} staged rows have invalid latitude/longitude values")

    bridge_orphans: dict[str, int] = {}
    for model, support_stage_cls in support_stage_classes.items():
        if model is EntityAddressEvidence:
            continue
        bridge_table = support_stage_cls.__tablename__
        if not await _table_exists(db_schema, bridge_table):
            failures.append(f"support stage table {bridge_table} is missing")
            bridge_orphans[bridge_table] = -1
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
        bridge_orphans[bridge_table] = orphan_count
        if orphan_count:
            failures.append(f"{orphan_count} rows in {bridge_table} reference missing staged location_key")
    metrics["bridge_orphans"] = bridge_orphans

    if failures:
        raise RuntimeError("EntityAddressUnified publish integrity validation failed: " + "; ".join(failures))
    return metrics


def _insert_raw_from_source_sql(
    db_schema: str,
    raw_table: str,
    source_select: str,
    *,
    address_canon_available: bool = True,
) -> str:
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
    )
    WITH base_rows AS (
        {source_select.strip()}
    ),
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
            NULLIF(TRIM(telephone_number), '')::varchar AS telephone_number,
            NULLIF(TRIM(fax_number), '')::varchar AS fax_number,
            {_canonical_contact_number_expr("telephone_number", "country_code")}::varchar AS phone_number,
            {_contact_extension_expr("telephone_number")}::varchar AS phone_extension,
            {_canonical_contact_number_expr("fax_number", "country_code")}::varchar AS fax_number_digits,
            {_contact_extension_expr("fax_number")}::varchar AS fax_extension,
            NULLIF(TRIM(formatted_address), '')::varchar AS formatted_address,
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
    ),
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
            formatted_address,
            lat,
            long,
            date_added,
            place_id,
            source_priority,
            address_source,
            source_record_id,
            updated_at,
            address_key,
            {_address_checksum_expr(
                first_line_expr=_alnum_norm_expr("first_line"),
                second_line_expr=_alnum_norm_expr("second_line"),
                city_name_expr=_alnum_norm_expr("city_name"),
                state_name_expr=_state_norm_expr("state_name"),
                postal_code_expr=_zip5_norm_expr("postal_code"),
                country_code_expr=_state_norm_expr("country_code"),
                telephone_number_expr=_phone_norm_expr("telephone_number"),
            )} AS checksum
          FROM sanitized
    )
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
        formatted_address,
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


def _materialize_from_raw_sql(
    db_schema: str,
    stage_table: str,
    raw_table: str,
    *,
    checksum_modulo: int | None = None,
    checksum_remainder: int | None = None,
    address_canon_available: bool = True,
    inline_source_evidence: bool = False,
) -> str:
    dedupe_key_expr = _dedupe_key_expr(address_canon_available)
    source_record_ids_select = _source_record_ids_select_sql()
    split_array_aggregates = _env_bool(
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_SPLIT_ARRAY_AGGREGATES",
        DEFAULT_SPLIT_ARRAY_AGGREGATES,
    )
    shard_filter = ""
    if checksum_modulo and checksum_modulo > 1 and checksum_remainder is not None:
        shard_expr = (
            "evidence_shard"
            if inline_source_evidence
            else _aggregate_shard_expr(dedupe_key_expr, int(checksum_modulo))
        )
        shard_filter = f" WHERE {shard_expr} = {int(checksum_remainder)}"
    if split_array_aggregates:
        array_filter_clauses = []
        if shard_filter:
            array_filter_clauses.append(shard_filter.replace(" WHERE ", "", 1))
        array_filter_clauses.append(
            "("
            "COALESCE(CARDINALITY(aca_plan_array), 0) > 0 OR "
            "COALESCE(CARDINALITY(aca_network_array), 0) > 0 OR "
            "COALESCE(CARDINALITY(ptg_plan_array), 0) > 0 OR "
            "COALESCE(CARDINALITY(ptg_source_array), 0) > 0 OR "
            "COALESCE(CARDINALITY(group_plan_array), 0) > 0"
            ")"
        )
        array_filter = " WHERE " + " AND ".join(array_filter_clauses)
        raw_array_joins = ""
        aggregated_array_columns = ""
        array_cte = f"""
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
        array_join = f"""
      LEFT JOIN array_aggregates arr
        ON arr.aggregate_entity_type = aggregated.entity_type
       AND arr.aggregate_entity_id = aggregated.entity_id
       AND arr.aggregate_type = aggregated.type
       AND arr.aggregate_key IS NOT DISTINCT FROM aggregated.{dedupe_key_expr}"""
        array_selects = (
            "COALESCE(arr.aca_plan_array, ARRAY[]::varchar[]) AS aca_plan_array,\n"
            "        COALESCE(arr.aca_network_array, ARRAY[]::varchar[]) AS aca_network_array,\n"
            "        COALESCE(arr.ptg_plan_array, ARRAY[]::varchar[]) AS ptg_plan_array,\n"
            "        COALESCE(arr.ptg_source_array, ARRAY[]::varchar[]) AS ptg_source_array,\n"
            "        COALESCE(arr.group_plan_array, ARRAY[]::varchar[]) AS group_plan_array"
        )
    else:
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
        array_cte = ""
        array_join = ""
        array_selects = (
            "COALESCE(aca_plan_array, ARRAY[]::varchar[]) AS aca_plan_array,\n"
            "        COALESCE(aca_network_array, ARRAY[]::varchar[]) AS aca_network_array,\n"
            "        COALESCE(ptg_plan_array, ARRAY[]::varchar[]) AS ptg_plan_array,\n"
            "        COALESCE(ptg_source_array, ARRAY[]::varchar[]) AS ptg_source_array,\n"
            "        COALESCE(group_plan_array, ARRAY[]::varchar[]) AS group_plan_array"
        )
    sql = f"""
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
        source_record_ids,
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
        lat,
        long,
        date_added,
        place_id,
        address_key,
        updated_at,
        last_seen_at
    )
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
            (ARRAY_AGG(telephone_number ORDER BY source_priority ASC, (telephone_number IS NULL), LENGTH(COALESCE(telephone_number, '')) DESC, updated_at DESC NULLS LAST))[1]::varchar AS telephone_number,
            (ARRAY_AGG(fax_number ORDER BY source_priority ASC, (fax_number IS NULL), LENGTH(COALESCE(fax_number, '')) DESC, updated_at DESC NULLS LAST))[1]::varchar AS fax_number,
            (ARRAY_AGG(phone_number ORDER BY source_priority ASC, (phone_number IS NULL), updated_at DESC NULLS LAST))[1]::varchar AS phone_number,
            (ARRAY_AGG(phone_extension ORDER BY source_priority ASC, (phone_extension IS NULL), updated_at DESC NULLS LAST))[1]::varchar AS phone_extension,
            (ARRAY_AGG(fax_number_digits ORDER BY source_priority ASC, (fax_number_digits IS NULL), updated_at DESC NULLS LAST))[1]::varchar AS fax_number_digits,
            (ARRAY_AGG(fax_extension ORDER BY source_priority ASC, (fax_extension IS NULL), updated_at DESC NULLS LAST))[1]::varchar AS fax_extension,
            (ARRAY_AGG(formatted_address ORDER BY source_priority ASC, (formatted_address IS NULL), LENGTH(COALESCE(formatted_address, '')) DESC, updated_at DESC NULLS LAST))[1]::varchar AS formatted_address,
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
    )
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
        {array_selects},
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
        lat,
        long,
        date_added,
        place_id,
        address_key,
        updated_at,
        updated_at AS last_seen_at
      FROM aggregated{array_join};
    """
    if inline_source_evidence:
        return _inline_source_evidence_sql(sql)
    return sql


def _materialize_sql(
    db_schema: str,
    stage_table: str,
    source_selects: Iterable[str],
    *,
    address_canon_available: bool = True,
) -> str:
    selects_sql = "\nUNION ALL\n".join(select.strip() for select in source_selects)
    dedupe_key_expr = _dedupe_key_expr(address_canon_available)
    source_record_ids_select = _source_record_ids_select_sql()
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
        lat,
        long,
        date_added,
        place_id,
        address_key,
        updated_at
    )
    WITH base_rows AS (
        {selects_sql}
    ),
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
            NULLIF(TRIM(telephone_number), '')::varchar AS telephone_number,
            NULLIF(TRIM(fax_number), '')::varchar AS fax_number,
            {_canonical_contact_number_expr("telephone_number", "country_code")}::varchar AS phone_number,
            {_contact_extension_expr("telephone_number")}::varchar AS phone_extension,
            {_canonical_contact_number_expr("fax_number", "country_code")}::varchar AS fax_number_digits,
            {_contact_extension_expr("fax_number")}::varchar AS fax_extension,
            NULLIF(TRIM(formatted_address), '')::varchar AS formatted_address,
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
    ),
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
            formatted_address,
            lat,
            long,
            date_added,
            place_id,
            source_priority,
            address_source,
            source_record_id,
            updated_at,
            address_key,
            {_location_key_expr(
                entity_type="entity_type",
                entity_id="entity_id",
                npi="npi",
                inferred_npi="inferred_npi",
                address_role_id=_address_role_id_expr("type"),
                row_origin="'base'",
                address_key="address_key",
                source_id=_source_id_expr("address_source"),
                source_record_id="source_record_id",
                zip5=_zip5_norm_expr("postal_code"),
                state_code=_state_norm_expr("state_name"),
                city_norm=_alnum_norm_expr("city_name"),
            )} AS location_key,
            {_address_checksum_expr(
                first_line_expr=_alnum_norm_expr("first_line"),
                second_line_expr=_alnum_norm_expr("second_line"),
                city_name_expr=_alnum_norm_expr("city_name"),
                state_name_expr=_state_norm_expr("state_name"),
                postal_code_expr=_zip5_norm_expr("postal_code"),
                country_code_expr=_state_norm_expr("country_code"),
                telephone_number_expr=_phone_norm_expr("telephone_number"),
            )} AS checksum
          FROM sanitized
    ),
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
            (ARRAY_AGG(telephone_number ORDER BY source_priority ASC, (telephone_number IS NULL), LENGTH(COALESCE(telephone_number, '')) DESC, updated_at DESC NULLS LAST))[1]::varchar AS telephone_number,
            (ARRAY_AGG(fax_number ORDER BY source_priority ASC, (fax_number IS NULL), LENGTH(COALESCE(fax_number, '')) DESC, updated_at DESC NULLS LAST))[1]::varchar AS fax_number,
            (ARRAY_AGG(phone_number ORDER BY source_priority ASC, (phone_number IS NULL), updated_at DESC NULLS LAST))[1]::varchar AS phone_number,
            (ARRAY_AGG(phone_extension ORDER BY source_priority ASC, (phone_extension IS NULL), updated_at DESC NULLS LAST))[1]::varchar AS phone_extension,
            (ARRAY_AGG(fax_number_digits ORDER BY source_priority ASC, (fax_number_digits IS NULL), updated_at DESC NULLS LAST))[1]::varchar AS fax_number_digits,
            (ARRAY_AGG(fax_extension ORDER BY source_priority ASC, (fax_extension IS NULL), updated_at DESC NULLS LAST))[1]::varchar AS fax_extension,
            (ARRAY_AGG(formatted_address ORDER BY source_priority ASC, (formatted_address IS NULL), LENGTH(COALESCE(formatted_address, '')) DESC, updated_at DESC NULLS LAST))[1]::varchar AS formatted_address,
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
    )
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
        formatted_address,
        lat,
        long,
        date_added,
        place_id,
        address_key,
        updated_at
      FROM aggregated;
    """


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


def _load_multi_source_evidence_base_sql(
    db_schema: str,
    stage_table: str,
    evidence_table: str,
    *,
    evidence_shards: int,
    affected_group_table: str | None = None,
    affected_scope: str = "group",
) -> str:
    group_hash_expr = _evidence_group_hash_expr(evidence_shards)
    affected_filter = ""
    if affected_group_table:
        if affected_scope == "npi":
            affected_filter = f"""
           AND {_entity_address_row_npi_expr("t")} IS NOT NULL
           AND EXISTS (
                SELECT 1
                  FROM {db_schema}.{affected_group_table} AS affected
                 WHERE affected.entity_npi IS NOT NULL
                   AND affected.entity_npi = {_entity_address_row_npi_expr("t")}
           )"""
        else:
            affected_filter = f"""
           AND EXISTS (
                SELECT 1
                  FROM {db_schema}.{affected_group_table} AS affected
                 WHERE {_entity_address_evidence_group_match_sql("affected", "t")}
           )"""
    return f"""
    WITH normalized AS (
        SELECT
            t.location_key,
            t.entity_type,
            t.entity_id,
            t.address_key::uuid AS address_key,
            CASE WHEN t.address_key IS NULL THEN {_street_soft_norm_expr("t.first_line")} END::varchar AS street_key,
            CASE WHEN t.address_key IS NULL
                THEN COALESCE(NULLIF(t.city_norm, ''), {_alnum_norm_expr("t.city_name")})
            END::varchar AS city_key,
            CASE WHEN t.address_key IS NULL
                THEN COALESCE(NULLIF(t.state_code, ''), {_state_norm_expr("t.state_name")})
            END::varchar AS state_key,
            CASE WHEN t.address_key IS NULL
                THEN COALESCE(NULLIF(t.zip5, ''), {_zip5_norm_expr("t.postal_code")})
            END::varchar AS zip_key,
            CASE WHEN t.address_key IS NULL THEN {_state_norm_expr("t.country_code")} END::varchar AS country_key,
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


def _insert_multi_source_evidence_shard_sql(
    db_schema: str,
    stage_table: str,
    evidence_table: str,
    *,
    evidence_shards: int,
    evidence_shard: int,
) -> str:
    del stage_table, evidence_shards
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
    ),
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
    ),
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
    )
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


def _delete_live_support_for_affected_groups_sql(
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
                _delete_live_support_for_affected_groups_sql(
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


def _evidence_from_raw_sql(
    db_schema: str,
    evidence_stage_table: str,
    raw_table: str,
    *,
    source_run_id: str,
    node_id: str | None,
) -> str:
    return f"""
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
        {_sql_literal(source_run_id)}::varchar AS source_run_id,
        CASE WHEN address_source = 'ptg' THEN NULLIF(split_part(source_record_id, ':', 3), '') ELSE NULL END::varchar
            AS source_snapshot_id,
        {_sql_literal(node_id)}::varchar AS node_id,
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


def _evidence_from_stage_sql(
    db_schema: str,
    evidence_stage_table: str,
    stage_table: str,
    *,
    source_run_id: str,
    node_id: str | None,
    affected_group_table: str | None = None,
) -> str:
    affected_filter = _affected_stage_row_filter_sql(db_schema, affected_group_table)
    return f"""
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
        {_sql_literal(source_run_id)}::varchar AS source_run_id,
        NULL::varchar AS source_snapshot_id,
        {_sql_literal(node_id)}::varchar AS node_id,
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
    include_hospital_enrollment: bool = False,
    include_fqhc_enrollment: bool = False,
    include_npi_address_key: bool = False,
    include_npi_registry: bool = False,
    include_npi_taxonomy: bool = False,
    include_nucc_taxonomy: bool = False,
    include_npi_other_identifier: bool = False,
    include_provider_additional_npi: bool = False,
    include_facility_anchor: bool = False,
    candidate_shards: int = 1,
    candidate_shard: int | None = None,
) -> str:
    candidate_limit = _env_int("HLTHPRT_FACILITY_ANCHOR_NPI_CANDIDATE_LIMIT", 25, minimum=1)
    shard_filter = _location_key_shard_filter_sql(
        "t.location_key",
        shards=candidate_shards,
        shard=candidate_shard,
    )

    def norm_text_sql(expr: str) -> str:
        return f"regexp_replace(LOWER(COALESCE({expr}, '')), '[^a-z0-9]', '', 'g')"

    def zip5_sql(expr: str) -> str:
        return f"LEFT(COALESCE({expr}, ''), 5)"

    def phone_sql(expr: str) -> str:
        return f"regexp_replace(COALESCE({expr}, ''), '[^0-9]', '', 'g')"

    def ccn_key_sql(expr: str) -> str:
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
    available = available or {}
    stage_tables = {model: stage_cls.__tablename__ for model, stage_cls in stage_classes.items()}
    partial_bridge_reuse = bool(affected_group_table)
    partial_support_patch = partial_bridge_reuse and not copy_unaffected_bridges
    build_code_bridges = _env_bool(
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_BUILD_CODE_BRIDGES",
        DEFAULT_BUILD_CODE_BRIDGES,
    )
    build_facility_candidates = _env_bool(
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_BUILD_FACILITY_CANDIDATES",
        DEFAULT_BUILD_FACILITY_CANDIDATES,
    )
    evidence_sql = (
        _evidence_from_raw_sql(
            db_schema,
            stage_tables[EntityAddressEvidence],
            raw_table,
            source_run_id=source_run_id,
            node_id=node_id,
        )
        if raw_table
        else _evidence_from_stage_sql(
            db_schema,
            stage_tables[EntityAddressEvidence],
            stage_table,
            source_run_id=source_run_id,
            node_id=node_id,
            affected_group_table=affected_group_table if partial_support_patch else None,
        )
    )
    statements = [
        _SupportStageStatement(
            "support tables",
            _truncate_support_stage_sql(db_schema, stage_tables),
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
    if build_code_bridges or partial_bridge_reuse:
        bridge_specs.extend(code_bridge_specs)
    if (
        build_facility_candidates
        and not partial_support_patch
        and FacilityAnchorNPICandidate in stage_tables
        and available.get("facility_anchor", False)
        and available.get("facility_anchor.medicare_ccn", available.get("facility_anchor", False))
    ):
        include_nppes_candidates = _env_bool(
            "HLTHPRT_FACILITY_ANCHOR_NPI_CANDIDATE_INCLUDE_NPPES",
            False,
        )
        include_other_identifier_candidates = _env_bool(
            "HLTHPRT_FACILITY_ANCHOR_NPI_CANDIDATE_INCLUDE_OTHER_IDENTIFIER",
            False,
        )
        facility_candidate_shards = _env_int(
            "HLTHPRT_ENTITY_ADDRESS_UNIFIED_FACILITY_CANDIDATE_SHARDS",
            DEFAULT_FACILITY_CANDIDATE_SHARDS,
            minimum=1,
        )
        facility_candidate_kwargs = dict(
            source_run_id=source_run_id,
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
                        stage_tables[FacilityAnchorNPICandidate],
                        stage_table,
                        candidate_shards=facility_candidate_shards,
                        candidate_shard=shard,
                        **facility_candidate_kwargs,
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
                        stage_tables[model],
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
            kwargs = {
                "affected_group_table": affected_group_table if partial_bridge_reuse else None,
            }
            if shard_env:
                kwargs.update({"bridge_shards": bridge_shards, "bridge_shard": shard})
            statements.append(
                _SupportStageStatement(
                    statement_label,
                    builder(
                        db_schema,
                        stage_tables[model],
                        stage_table,
                        **kwargs,
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
        item.statement
        for item in _support_stage_statements(
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
        max(1, sum(1 for item in statements if item.parallel)),
    )
    phase_context["support_stage_concurrency"] = support_concurrency
    total_steps = len(statements)
    completed_steps = 0
    progress_lock = asyncio.Lock()

    async def _run_item(index: int, item: _SupportStageStatement) -> None:
        nonlocal completed_steps
        if run_id:
            async with progress_lock:
                current_done = completed_steps
                enqueue_live_progress(
                    run_id=run_id,
                    importer="entity-address-unified",
                    status="running",
                    phase=f"entity-address-unified building {item.label}",
                    unit="steps",
                    done=current_done,
                    total=total_steps,
                    pct=95 + (current_done / max(total_steps, 1)) * 4,
                    message=(
                        f"building support table {index}/{total_steps}: {item.label} "
                        f"(concurrency {support_concurrency})"
                    ),
                )
        await _run_sql_phase(
            item.statement,
            context=phase_context,
            phase=f"entity-address-unified building {item.label}",
        )

    async def _finish_item(index: int, item: _SupportStageStatement) -> None:
        nonlocal completed_steps
        await _run_item(index, item)
        async with progress_lock:
            completed_steps += 1
            if run_id:
                enqueue_live_progress(
                    run_id=run_id,
                    importer="entity-address-unified",
                    status="running",
                    phase=f"entity-address-unified built {item.label}",
                    unit="steps",
                    done=completed_steps,
                    total=total_steps,
                    pct=95 + (completed_steps / max(total_steps, 1)) * 4,
                    message=f"built support table {index}/{total_steps}: {item.label}",
                )

    async def _run_parallel_batch(batch: list[tuple[int, _SupportStageStatement]]) -> None:
        if not batch:
            return
        if support_concurrency <= 1 or len(batch) == 1:
            for index, item in batch:
                await _finish_item(index, item)
            return
        semaphore = asyncio.Semaphore(support_concurrency)

        async def _guarded(index: int, item: _SupportStageStatement) -> None:
            async with semaphore:
                await _finish_item(index, item)

        results = await asyncio.gather(
            *(_guarded(index, item) for index, item in batch),
            return_exceptions=True,
        )
        for result in results:
            if isinstance(result, BaseException):
                raise result

    parallel_batch: list[tuple[int, _SupportStageStatement]] = []
    for index, item in enumerate(statements, start=1):
        if item.parallel:
            parallel_batch.append((index, item))
            continue
        await _run_parallel_batch(parallel_batch)
        parallel_batch = []
        await _finish_item(index, item)
    await _run_parallel_batch(parallel_batch)
    counts: dict[str, int] = {}
    for model, stage_cls in stage_classes.items():
        counts[model.__tablename__] = int(
            await db.scalar(f"SELECT COUNT(*) FROM {db_schema}.{stage_cls.__tablename__};")
            or 0
        )
    return counts


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
    include_hospital_enrollment: bool,
    include_fqhc_enrollment: bool,
    include_facility_override: bool = False,
    include_npi_other_identifier: bool = False,
    include_name_fallback: bool = False,
    include_nppes_name_inference: bool = False,
    include_nppes_broad_inference: bool = False,
) -> str:
    def norm_text_sql(expr: str) -> str:
        return f"regexp_replace(LOWER(COALESCE({expr}, '')), '[^a-z0-9]', '', 'g')"

    def zip5_sql(expr: str) -> str:
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
        return f"           AND a.type = '{address_type}'\n" if address_type else ""

    def build_fqhc_parent_nppes_exact_candidates_sql(address_type: str | None) -> str:
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

    def build_fqhc_parent_nppes_name_state_candidates_sql(address_type: str | None) -> str:
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

    def build_fqhc_parent_nppes_address_candidates_sql(address_type: str | None) -> str:
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

    fqhc_parent_nppes_exact_primary_candidates_sql = build_fqhc_parent_nppes_exact_candidates_sql("primary")
    fqhc_parent_nppes_exact_all_candidates_sql = build_fqhc_parent_nppes_exact_candidates_sql(None)
    fqhc_parent_nppes_name_state_primary_candidates_sql = build_fqhc_parent_nppes_name_state_candidates_sql("primary")
    fqhc_parent_nppes_name_state_all_candidates_sql = build_fqhc_parent_nppes_name_state_candidates_sql(None)
    fqhc_parent_nppes_address_primary_candidates_sql = build_fqhc_parent_nppes_address_candidates_sql("primary")
    fqhc_parent_nppes_address_all_candidates_sql = build_fqhc_parent_nppes_address_candidates_sql(None)

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


async def process_data(ctx, task=None):
    task = task or {}
    ctx.setdefault("context", {})
    context = ctx["context"]
    run_id = str(context.get("control_run_id") or ctx.get("control_run_id") or "").strip()

    if "test_mode" in task:
        context["test_mode"] = bool(task.get("test_mode"))
    test_mode = bool(context.get("test_mode", False))
    context["publish_requested"] = _publish_requested(task, test_mode=test_mode)
    refresh_mode = _entity_address_refresh_mode(task)
    partial_provider_directory_refresh = (
        refresh_mode == ENTITY_ADDRESS_REFRESH_MODE_PROVIDER_DIRECTORY_PARTIAL
    )
    partial_source_refresh = partial_provider_directory_refresh
    provider_directory_partial_scope = (
        _entity_address_provider_directory_partial_scope(task)
        if partial_provider_directory_refresh
        else None
    )
    provider_directory_source_ids = (
        _entity_address_provider_directory_source_ids(task)
        if partial_provider_directory_refresh
        else []
    )
    provider_directory_run_id = (
        _entity_address_provider_directory_run_id(task)
        if partial_provider_directory_refresh
        else None
    )
    provider_directory_source_batch_size = (
        _entity_address_provider_directory_source_batch_size(task)
        if partial_provider_directory_refresh
        else 0
    )
    provider_directory_scope_sources: list[str] = []
    context["refresh_mode"] = refresh_mode
    context["partial_provider_directory_refresh"] = partial_provider_directory_refresh
    context["partial_provider_directory_scope"] = provider_directory_partial_scope
    aggregate_source_record_ids = _aggregate_source_record_ids()
    context["aggregate_source_record_ids"] = aggregate_source_record_ids
    serving_only_refresh = partial_provider_directory_refresh or (
        not partial_source_refresh
        and _task_bool_or_env(
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
    reuse_stage = _env_bool("HLTHPRT_ENTITY_ADDRESS_UNIFIED_REUSE_STAGE", False)
    if partial_source_refresh and reuse_stage:
        raise RuntimeError(
            f"entity-address-unified {refresh_mode} refresh cannot be combined with "
            "HLTHPRT_ENTITY_ADDRESS_UNIFIED_REUSE_STAGE."
        )

    if not ctx["context"].get("stage_prepared"):
        await _ensure_schema_exists(db_schema)
        unlogged_stage = (
            not reuse_stage
            and _env_bool(
                "HLTHPRT_ENTITY_ADDRESS_UNIFIED_UNLOGGED_STAGE",
                DEFAULT_UNLOGGED_STAGE,
            )
        )
        context["unlogged_stage"] = unlogged_stage
        if reuse_stage:
            if not await _table_exists(db_schema, stage_table):
                raise RuntimeError(
                    f"HLTHPRT_ENTITY_ADDRESS_UNIFIED_REUSE_STAGE requested, "
                    f"but {db_schema}.{stage_table} does not exist"
                )
            context["stage_reused"] = True
        else:
            await db.status(f"DROP TABLE IF EXISTS {db_schema}.{stage_table};")
            await db.create_table(stage_cls.__table__, checkfirst=True)
            if unlogged_stage:
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
        "provider_directory_organization_affiliation",
        "address_archive_v2",
    ]
    available = {table: await _table_exists(db_schema, table) for table in required_checks}
    for table_name in (
        "npi_address",
        "doctor_clinician_address",
        "provider_enrollment_ffs_address",
        "facility_anchor",
        "mrf_address",
        "provider_directory_location",
    ):
        available[f"{table_name}.address_key"] = (
            await _table_column_exists(db_schema, table_name, "address_key")
            if available.get(table_name)
            else False
        )
    available["facility_anchor.medicare_ccn"] = (
        await _table_column_exists(db_schema, "facility_anchor", "medicare_ccn")
        if available.get("facility_anchor")
        else False
    )
    approved_candidate_promotions = 0
    if available.get("facility_anchor_npi_candidate") and available.get("facility_anchor_npi_override"):
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
    if (
        partial_provider_directory_refresh
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
    if partial_provider_directory_refresh and provider_directory_partial_scope == "latest-run":
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
        if partial_provider_directory_refresh
        else [provider_directory_source_ids]
    )
    context["partial_provider_directory_source_batch_size"] = provider_directory_source_batch_size
    context["partial_provider_directory_source_batches"] = len(provider_directory_source_batches)
    source_selects: list[str] = []
    for provider_directory_source_batch in provider_directory_source_batches:
        source_selects.extend(
            _source_selects(
                db_schema,
                available,
                test_limit_per_source=test_limit_per_source,
                provider_directory_source_ids=provider_directory_source_batch,
                provider_directory_run_id=provider_directory_run_id,
            )
        )
    affected_group_table: str | None = None
    if partial_provider_directory_refresh:
        if not await _table_exists(db_schema, EntityAddressUnified.__main_table__):
            raise RuntimeError(
                "entity-address-unified provider-directory-partial refresh requested, but the live "
                "entity_address_unified table does not exist; run refresh_mode=full first."
            )
        affected_group_table = _provider_directory_partial_affected_group_table_name(stage_table)
        context["partial_provider_directory_affected_group_table"] = affected_group_table
        context["partial_affected_group_table"] = affected_group_table
        scope_index_sql = _provider_directory_partial_scope_index_sql(db_schema)
        for index_idx, index_sql in enumerate(scope_index_sql):
            await _run_sql_phase(
                index_sql,
                context=context,
                run_id=run_id,
                phase="entity-address-unified ensuring Provider Directory scope indexes",
                unit="indexes",
                done=index_idx,
                total=len(scope_index_sql),
                message="ensuring Provider Directory scope indexes",
                emit_start=True,
                emit_done=True,
            )
        await db.status(f"DROP TABLE IF EXISTS {db_schema}.{affected_group_table};")
        await _run_sql_phase(
            _prepare_provider_directory_partial_affected_groups_sql(
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
            _index_provider_directory_partial_affected_groups_sql(db_schema, affected_group_table),
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
        source_selects = _provider_directory_partial_source_selects(
            db_schema,
            source_selects,
            affected_group_table=affected_group_table,
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
                if available.get("npi_address", False)
                else []
            ),
            mrf_address_ranges=(
                await _npi_table_ranges(db_schema, "mrf_address", source_table_shards)
                if available.get("mrf_address", False)
                else []
            ),
            doctor_clinician_address_ranges=(
                await _npi_table_ranges(db_schema, "doctor_clinician_address", source_table_shards)
                if available.get("doctor_clinician_address", False)
                else []
            ),
            provider_enrollment_ffs_ranges=(
                await _npi_table_ranges(db_schema, "provider_enrollment_ffs", source_table_shards)
                if available.get("provider_enrollment_ffs", False)
                else []
            ),
        )
    context["source_select_count"] = len(source_selects)
    address_canon_available = await _address_canon_available(db_schema)
    if not address_canon_available:
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
        support_stage_classes = {}
        ctx["context"]["support_stage_prepared"] = False
        ctx["context"]["support_stage_indexes_prepared"] = True
    elif not ctx["context"].get("support_stage_prepared"):
        support_stage_classes = await _prepare_support_stage_tables(db_schema, import_date)
        ctx["context"]["support_stage_prepared"] = True
        ctx["context"]["support_stage_indexes_prepared"] = False
    else:
        support_stage_classes = _support_stage_classes(import_date)

    build_network_bridge = _env_bool(
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_BUILD_NETWORK_BRIDGE",
        DEFAULT_BUILD_NETWORK_BRIDGE,
    )
    context["build_network_bridge"] = build_network_bridge
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

    chunked_load = _env_bool("HLTHPRT_ENTITY_ADDRESS_UNIFIED_CHUNKED_LOAD", True)
    if partial_source_refresh and not chunked_load:
        raise RuntimeError(
            f"entity-address-unified {refresh_mode} refresh requires "
            "HLTHPRT_ENTITY_ADDRESS_UNIFIED_CHUNKED_LOAD=true."
        )
    raw_table: str | None = None
    inline_source_evidence = False
    if chunked_load and not reuse_stage:
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
        inline_source_evidence = (
            _env_bool(
                "HLTHPRT_ENTITY_ADDRESS_UNIFIED_INLINE_SOURCE_EVIDENCE",
                DEFAULT_INLINE_SOURCE_EVIDENCE,
            )
        )
        context["inline_source_evidence"] = inline_source_evidence
        if _require_inline_source_evidence() and not inline_source_evidence:
            raise RuntimeError(
                "HLTHPRT_ENTITY_ADDRESS_UNIFIED_REQUIRE_INLINE_SOURCE_EVIDENCE requested, "
                "but inline source evidence is inactive "
                f"(partial_source_refresh={partial_source_refresh}, reuse_stage={reuse_stage}, "
                f"chunked_load={chunked_load})."
            )
        split_array_aggregates = _env_bool(
            "HLTHPRT_ENTITY_ADDRESS_UNIFIED_SPLIT_ARRAY_AGGREGATES",
            DEFAULT_SPLIT_ARRAY_AGGREGATES,
        )
        context["split_array_aggregates"] = split_array_aggregates
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
        raw_table = _raw_stage_table_name(stage_table)
        use_unlogged_raw = _env_bool("HLTHPRT_ENTITY_ADDRESS_UNIFIED_UNLOGGED_RAW_STAGE", True)
        reuse_raw_stage = _env_bool("HLTHPRT_ENTITY_ADDRESS_UNIFIED_REUSE_RAW_STAGE", False)

        if reuse_raw_stage:
            if not await _table_exists(db_schema, raw_table):
                raise RuntimeError(
                    f"HLTHPRT_ENTITY_ADDRESS_UNIFIED_REUSE_RAW_STAGE requested, "
                    f"but {db_schema}.{raw_table} does not exist"
                )
            raw_rows = int(await db.scalar(f"SELECT COUNT(*) FROM {db_schema}.{raw_table};") or 0)
            if raw_rows <= 0:
                raise RuntimeError(
                    f"HLTHPRT_ENTITY_ADDRESS_UNIFIED_REUSE_RAW_STAGE requested, "
                    f"but {db_schema}.{raw_table} is empty"
                )
            context["raw_stage_reused"] = True
            context["raw_stage_reused_rows"] = raw_rows
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
            loaded_sources = 0

            async def _load_source(select_sql: str) -> None:
                nonlocal loaded_sources
                async with sem:
                    await _run_sql_phase(
                        _insert_raw_from_source_sql(
                            db_schema,
                            raw_table,
                            select_sql,
                            address_canon_available=address_canon_available,
                        ),
                        context=context,
                        run_id=run_id,
                        phase="entity-address-unified loading sources",
                        unit="sources",
                        done=loaded_sources,
                        total=len(source_selects),
                        message="loading source shard",
                        emit_start=True,
                    )
                if run_id:
                    async with source_progress_lock:
                        loaded_sources += 1
                        enqueue_live_progress(
                            run_id=run_id,
                            importer="entity-address-unified",
                            status="running",
                            phase="entity-address-unified loading sources",
                            unit="sources",
                            done=loaded_sources,
                            total=len(source_selects),
                            message=f"loaded {loaded_sources}/{len(source_selects)} sources",
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
                enriched_shards = 0
                checksum_ranges = _integer_ranges(-(2**31), 2**31 - 1, enrich_shards)

                async def _enrich_shard(checksum_min: int, checksum_max: int) -> None:
                    nonlocal enriched_shards
                    async with enrich_sem:
                        await _run_sql_phase(
                            _enrich_raw_stage_sql(
                                db_schema,
                                raw_table,
                                archive_available=available.get("address_archive_v2", False),
                                address_canon_available=address_canon_available,
                                checksum_min=checksum_min,
                                checksum_max=checksum_max,
                                evidence_shards=(
                                    aggregate_shards
                                    if inline_source_evidence and aggregate_shards > 1
                                    else None
                                ),
                            ),
                            context=context,
                            run_id=run_id,
                            phase="entity-address-unified enriching raw",
                            unit="shards",
                            done=enriched_shards,
                            total=enrich_shards,
                            message=f"enriching checksum range {checksum_min}..{checksum_max}",
                            emit_start=True,
                        )
                    if run_id:
                        async with enrich_progress_lock:
                            enriched_shards += 1
                            enqueue_live_progress(
                                run_id=run_id,
                                importer="entity-address-unified",
                                status="running",
                                phase="entity-address-unified enriching raw",
                                unit="shards",
                                done=enriched_shards,
                                total=enrich_shards,
                                message=f"enriched {enriched_shards}/{enrich_shards} raw shards",
                            )

                await asyncio.gather(*(_enrich_shard(low, high) for low, high in checksum_ranges))
            else:
                await _run_sql_phase(
                    _enrich_raw_stage_sql(
                        db_schema,
                        raw_table,
                        archive_available=available.get("address_archive_v2", False),
                        address_canon_available=address_canon_available,
                        evidence_shards=(
                            aggregate_shards
                            if inline_source_evidence and aggregate_shards > 1
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
                address_canon_available=address_canon_available,
                inline_source_evidence=inline_source_evidence,
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
        aggregated_shards = 0

        async def _aggregate_shard(remainder: int) -> None:
            nonlocal aggregated_shards
            await _run_sql_phase(
                _materialize_from_raw_sql(
                    db_schema,
                    stage_table,
                    raw_table,
                    checksum_modulo=aggregate_shards,
                    checksum_remainder=remainder,
                    address_canon_available=address_canon_available,
                    inline_source_evidence=inline_source_evidence,
                ),
                context=context,
                run_id=run_id,
                phase="entity-address-unified aggregating",
                unit="shards",
                done=aggregated_shards,
                total=aggregate_shards,
                message=f"aggregating shard {remainder + 1}/{aggregate_shards}",
                emit_start=True,
            )
            if run_id:
                async with aggregate_progress_lock:
                    aggregated_shards += 1
                    enqueue_live_progress(
                        run_id=run_id,
                        importer="entity-address-unified",
                        status="running",
                        phase="entity-address-unified aggregating",
                        unit="shards",
                        done=aggregated_shards,
                        total=aggregate_shards,
                        message=f"aggregated {aggregated_shards}/{aggregate_shards} shards",
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
                    address_canon_available=address_canon_available,
                    inline_source_evidence=inline_source_evidence,
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

    elif chunked_load:
        raw_table = _raw_stage_table_name(stage_table)
        context["stage_reused"] = True
        if not await _table_exists(db_schema, raw_table):
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
                address_canon_available=address_canon_available,
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

    enable_inference = str(
        os.getenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_ENABLE_INFERENCE", "false")
    ).strip().lower() in {"1", "true", "yes", "on"}
    if test_mode:
        enable_inference = str(
            os.getenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_TEST_ENABLE_INFERENCE", "false")
        ).strip().lower() in {"1", "true", "yes", "on"}

    if (
        enable_inference
        and available.get("npi", False)
        and available.get("npi_address", False)
        and available.get("npi_taxonomy", False)
        and available.get("nucc_taxonomy", False)
    ):
        include_facility_override = False
        if available.get("facility_anchor_npi_override", False):
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
            available.get("npi_other_identifier", False)
            and _env_bool(
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
                include_hospital_enrollment=available.get("provider_enrollment_hospital", False),
                include_fqhc_enrollment=available.get("provider_enrollment_fqhc", False),
                include_facility_override=include_facility_override,
                include_npi_other_identifier=include_npi_other_identifier,
                include_name_fallback=_env_bool(
                    "HLTHPRT_ENTITY_ADDRESS_UNIFIED_ENABLE_NAME_FALLBACK_INFERENCE",
                    False,
                ),
                include_nppes_name_inference=_env_bool(
                    "HLTHPRT_ENTITY_ADDRESS_UNIFIED_ENABLE_NPPES_NAME_INFERENCE",
                    False,
                ),
                include_nppes_broad_inference=_env_bool(
                    "HLTHPRT_ENTITY_ADDRESS_UNIFIED_ENABLE_NPPES_BROAD_INFERENCE",
                    False,
                ),
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

    if inline_source_evidence:
        context["source_evidence_inlined"] = True
    else:
        if _require_inline_source_evidence():
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
        use_unlogged_evidence = _env_bool(
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
                affected_scope="npi" if partial_provider_directory_refresh else "group",
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
        evidence_build_done = 0

        async def _build_evidence_shard(remainder: int) -> None:
            nonlocal evidence_build_done
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
                done=evidence_build_done,
                total=evidence_shards,
                message=f"preparing evidence shard {remainder + 1}/{evidence_shards}",
                emit_start=True,
            )
            if run_id:
                async with evidence_build_progress_lock:
                    evidence_build_done += 1
                    enqueue_live_progress(
                        run_id=run_id,
                        importer="entity-address-unified",
                        status="running",
                        phase="entity-address-unified preparing source evidence",
                        unit="shards",
                        done=evidence_build_done,
                        total=evidence_shards,
                        message=f"prepared {evidence_build_done}/{evidence_shards} evidence shards",
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
        evidence_done = 0

        async def _apply_evidence_shard(remainder: int) -> None:
            nonlocal evidence_done
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
                done=evidence_done,
                total=evidence_shards,
                message=f"applying evidence shard {remainder + 1}/{evidence_shards}",
                emit_start=True,
            )
            if run_id:
                async with evidence_progress_lock:
                    evidence_done += 1
                    enqueue_live_progress(
                        run_id=run_id,
                        importer="entity-address-unified",
                        status="running",
                        phase="entity-address-unified applying source evidence",
                        unit="shards",
                        done=evidence_done,
                        total=evidence_shards,
                        message=f"applied {evidence_done}/{evidence_shards} evidence shards",
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
        support_counts = {}
        context["support_stage_populated"] = False
        context["support_stage_skipped"] = True
        context["support_counts"] = support_counts
    elif context.get("support_stage_populated") and isinstance(cached_support_counts, dict):
        support_counts = {str(key): int(value) for key, value in cached_support_counts.items()}
    else:
        support_raw_table = None if partial_source_refresh else raw_table
        support_affected_group_table = None
        copy_unaffected_support_bridges = True
        support_counts = await _populate_support_stage_tables(
            db_schema,
            stage_table,
            support_stage_classes,
            source_run_id=import_date,
            node_id=node_id,
            raw_table=support_raw_table,
            build_network_bridge=build_network_bridge,
            available=available,
            run_id=run_id,
            context=context,
            affected_group_table=support_affected_group_table,
            copy_unaffected_bridges=copy_unaffected_support_bridges,
        )
        context["support_stage_populated"] = True
        context["support_counts"] = support_counts
    if raw_table and _keep_raw_stage():
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
        _env_bool(
            "HLTHPRT_ENTITY_ADDRESS_UNIFIED_COMPACT_SOURCE_RECORD_IDS",
            DEFAULT_COMPACT_SOURCE_RECORD_IDS,
        )
        and aggregate_source_record_ids
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

    if partial_provider_directory_refresh and context.get("partial_provider_directory_replacement_publish"):
        if not affected_group_table or not await _table_exists(db_schema, affected_group_table):
            raise RuntimeError(
                "entity-address-unified provider-directory-partial replacement publish requires "
                "the affected group table while composing the replacement stage."
            )
        if not await _table_exists(db_schema, EntityAddressUnified.__main_table__):
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
        affected_live_location_table = _provider_directory_affected_live_location_table_name(stage_table)
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
            _prepare_provider_directory_affected_live_locations_sql(
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
            _index_provider_directory_affected_live_locations_sql(
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
        copied_rows = await _run_sql_phase(
            _copy_unaffected_live_entity_rows_by_location_sql(
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
        await _run_sql_phase(
            f"DROP TABLE IF EXISTS {db_schema}.{affected_live_location_table};",
            context=context,
            run_id=run_id,
            phase="entity-address-unified dropping Provider Directory affected locations",
            unit="tables",
            done=1,
            total=1,
            pct=90,
            message="dropping Provider Directory affected live locations",
            emit_done=True,
        )

    if not ctx["context"].get("stage_indexes_prepared"):
        if run_id:
            enqueue_live_progress(
                run_id=run_id,
                importer="entity-address-unified",
                status="running",
                phase="entity-address-unified indexing",
                unit="run",
                done=0,
                total=1,
                pct=90,
                message="building indexes",
            )
        await _create_stage_indexes(stage_cls, db_schema, context=context)
        context["stage_indexes_prepared"] = True

    if (
        not ctx["context"].get("support_stage_indexes_prepared")
        and not context.get("partial_support_patch_publish")
        and not serving_only_refresh
    ):
        await _create_support_stage_indexes(
            support_stage_classes,
            db_schema,
            context=context,
            run_id=run_id,
        )
        context["support_stage_indexes_prepared"] = True

    final_summary_counts = _final_summary_counts()
    context["final_summary_counts"] = final_summary_counts
    summary_counts = None
    if not final_summary_counts:
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
    context["support_counts"] = support_counts
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


async def startup(ctx):
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


async def shutdown(ctx):
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
    support_stage_classes = {} if serving_only_refresh else _support_stage_classes(import_date)
    affected_group_table = str(
        context.get("partial_affected_group_table")
        or context.get("partial_provider_directory_affected_group_table")
        or ""
    ).strip()
    affected_live_location_table = str(
        context.get("partial_provider_directory_affected_live_location_table") or ""
    ).strip()
    partial_support_patch = bool(context.get("partial_support_patch_publish"))
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
        context.get("refresh_mode") == ENTITY_ADDRESS_REFRESH_MODE_PROVIDER_DIRECTORY_PARTIAL
        and partial_support_patch
    ):
        raise RuntimeError(
            "entity-address-unified provider-directory-partial refresh must publish "
            "through replacement-stage table swap; live support patch publishing is disabled."
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
    live_table_exists = await _table_exists(db_schema, EntityAddressUnified.__main_table__)
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
            support_stage_classes,
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
        and _defer_publish_validation()
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
            support_stage_classes,
            test_mode=bool(context.get("test_mode")),
        )
        context["publish_validation"] = publish_validation

    async with db.transaction():
        table = EntityAddressUnified.__main_table__
        await db.status(f"DROP TABLE IF EXISTS {db_schema}.{table}_old;")
        await db.status(f"ALTER TABLE IF EXISTS {db_schema}.{table} RENAME TO {table}_old;")
        await db.status(f"ALTER TABLE IF EXISTS {db_schema}.{stage_cls.__tablename__} RENAME TO {table};")

        archived = _archived_identifier(f"{table}_idx_primary")
        await db.status(f"DROP INDEX IF EXISTS {db_schema}.{archived};")
        await db.status(f"ALTER INDEX IF EXISTS {db_schema}.{table}_idx_primary RENAME TO {archived};")
        await db.status(
            f"ALTER INDEX IF EXISTS {db_schema}.{stage_cls.__tablename__}_idx_primary "
            f"RENAME TO {table}_idx_primary;"
        )

        if hasattr(stage_cls, "__my_additional_indexes__") and stage_cls.__my_additional_indexes__:
            for index in stage_cls.__my_additional_indexes__:
                index_name = index.get("name", "_".join(index.get("index_elements")))
                old_live_name = f"{table}_idx_{index_name}"
                archived_live_name = _archived_identifier(old_live_name)
                await db.status(f"DROP INDEX IF EXISTS {db_schema}.{archived_live_name};")
                await db.status(
                    f"ALTER INDEX IF EXISTS {db_schema}.{old_live_name} "
                    f"RENAME TO {archived_live_name};"
                )
                await db.status(
                    f"ALTER INDEX IF EXISTS "
                    f"{db_schema}.{_stage_index_name(stage_cls.__tablename__, index_name)} "
                    f"RENAME TO {old_live_name};"
                )

        if partial_support_patch:
            if not affected_group_table or not await _table_exists(db_schema, affected_group_table):
                raise RuntimeError(
                    "entity-address-unified support patch publish requires "
                    "the affected group table to remain available through shutdown."
                )
            for label, statement in _partial_support_patch_sql(
                db_schema,
                support_stage_classes,
                old_entity_table=f"{table}_old",
                affected_group_table=affected_group_table,
                build_network_bridge=bool(context.get("build_network_bridge", DEFAULT_BUILD_NETWORK_BRIDGE)),
            ):
                started = time.monotonic()
                rowcount = await db.status(statement)
                _record_phase_timing(
                    context,
                    f"entity-address-unified patching support {label}",
                    time.monotonic() - started,
                    _coerce_rowcount(rowcount),
                )
        else:
            for live_cls, support_stage_cls in support_stage_classes.items():
                await _swap_stage_table(db_schema, live_cls, support_stage_cls)

    if partial_support_patch:
        await _drop_stage_artifacts(
            db_schema,
            stage_cls,
            support_stage_classes,
            extra_tables=[affected_group_table, affected_live_location_table],
        )

    published_rows = stage_rows
    started_at = context.get("start")
    if isinstance(started_at, datetime.datetime):
        context["published_elapsed_seconds"] = round(
            (datetime.datetime.utcnow() - started_at).total_seconds(),
            3,
        )
    post_publish_profile = _post_publish_index_profile()
    post_publish_concurrently = _post_publish_index_concurrently()
    context["post_publish_index_profile"] = post_publish_profile
    context["post_publish_index_concurrently"] = post_publish_concurrently
    context["post_publish_index_completed"] = int(context.get("post_publish_index_completed") or 0)
    context["post_publish_index_total"] = int(context.get("post_publish_index_total") or 0)
    context["post_publish_skipped_indexes"] = list(context.get("post_publish_skipped_indexes") or [])
    if post_publish_profile != "none":
        planned_statements, skipped_indexes = _post_publish_index_plan(
            db_schema,
            post_publish_profile,
            build_concurrently=post_publish_concurrently,
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
            **_runtime_config_metrics(context),
            "publish_validation": context.get("publish_validation") or {},
            "phase_timings": context.get("phase_timings") or {},
            "skipped_stage_indexes": context.get("skipped_stage_indexes") or [],
        }

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
    if defer_publish_validation:
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
                preserve_finished_at=True,
            )
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


async def main(
    test_mode: bool = False,
    limit_per_source: int | None = None,
    publish: bool | None = None,
    refresh_mode: str | None = None,
    serving_only_refresh: bool | None = None,
    provider_directory_run_id: str | None = None,
    provider_directory_source_ids: list[str] | tuple[str, ...] | str | None = None,
    provider_directory_partial_scope: str | None = None,
    provider_directory_source_batch_size: int | None = None,
):
    redis = await create_pool(
        build_redis_settings(),
        job_serializer=serialize_job,
        job_deserializer=deserialize_job,
    )
    payload = {"test_mode": bool(test_mode)}
    if limit_per_source is not None:
        payload["limit_per_source"] = max(int(limit_per_source), 0)
    if publish is not None:
        payload["publish"] = bool(publish)
    if refresh_mode:
        payload["refresh_mode"] = refresh_mode
    if serving_only_refresh is not None:
        payload["serving_only_refresh"] = bool(serving_only_refresh)
    if provider_directory_run_id:
        payload["provider_directory_run_id"] = provider_directory_run_id
    source_ids = _coerce_str_list(provider_directory_source_ids)
    if source_ids:
        payload["provider_directory_source_ids"] = source_ids
    if provider_directory_partial_scope:
        payload["provider_directory_partial_scope"] = provider_directory_partial_scope
    if provider_directory_source_batch_size is not None:
        payload["provider_directory_source_batch_size"] = max(int(provider_directory_source_batch_size), 0)
    await redis.enqueue_job("process_data", payload, _queue_name=ENTITY_ADDRESS_UNIFIED_QUEUE_NAME)
