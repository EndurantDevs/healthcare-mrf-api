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

from db.models import (
    EntityAddressEvidence,
    EntityAddressMedicationBridge,
    EntityAddressNetworkBridge,
    EntityAddressPTGBridge,
    EntityAddressPlanBridge,
    EntityAddressProcedureBridge,
    EntityAddressUnified,
    FacilityAnchorNPICandidate,
    PTGAddress,
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
DEFAULT_SUPPORT_STAGE_CONCURRENCY = 4
DEFAULT_SUPPORT_INDEX_CONCURRENCY = 2
DEFAULT_BUILD_NETWORK_BRIDGE = False
DEFAULT_COMPACT_SOURCE_RECORD_IDS = True
DEFAULT_SQL_WORK_MEM = "256MB"
DEFAULT_SQL_MAINTENANCE_WORK_MEM = "2GB"
DEFAULT_SQL_TEMP_FILE_LIMIT = "128GB"
DEFAULT_SQL_LOCK_TIMEOUT = "30s"
DEFAULT_SQL_STATEMENT_TIMEOUT = "0"
DEFAULT_SQL_SYNCHRONOUS_COMMIT = "off"
DEFAULT_SQL_JIT = "off"
DEFAULT_PTG_PARTIAL_PATCH_SUPPORT = True
ENTITY_ADDRESS_REFRESH_MODE_FULL = "full"
ENTITY_ADDRESS_REFRESH_MODE_PTG_PARTIAL = "ptg-partial"
ENTITY_ADDRESS_REFRESH_MODES = {ENTITY_ADDRESS_REFRESH_MODE_FULL, ENTITY_ADDRESS_REFRESH_MODE_PTG_PARTIAL}
ARCHIVE_IDENTITY_VERSION = "v2"
BASE_ADDRESS_VERSION = "address_archive_v2:v2"
SUPPORT_TABLE_MODELS = (
    EntityAddressEvidence,
    EntityAddressPlanBridge,
    EntityAddressNetworkBridge,
    EntityAddressPTGBridge,
    EntityAddressProcedureBridge,
    EntityAddressMedicationBridge,
    FacilityAnchorNPICandidate,
)
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
        return ENTITY_ADDRESS_REFRESH_MODE_PTG_PARTIAL
    raise ValueError(
        f"Unsupported entity-address-unified refresh_mode {raw!r}; "
        f"expected one of {sorted(ENTITY_ADDRESS_REFRESH_MODES)}"
    )


def _entity_address_ptg_source_keys(task: dict) -> list[str]:
    explicit = (
        task.get("ptg_source_keys")
        or task.get("ptg_source_key")
        or task.get("source_keys")
        or task.get("source_key")
        or os.getenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_PTG_SOURCE_KEYS")
        or os.getenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_PTG_SOURCE_KEY")
    )
    return _coerce_str_list(explicit)


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
            "rows": 0,
        },
    )
    entry["count"] = int(entry.get("count") or 0) + 1
    entry["seconds"] = round(float(entry.get("seconds") or 0.0) + elapsed_seconds, 3)
    entry["max_seconds"] = round(max(float(entry.get("max_seconds") or 0.0), elapsed_seconds), 3)
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


def _support_stage_classes(import_date: str) -> dict[type, type]:
    return {model: make_class(model, import_date) for model in SUPPORT_TABLE_MODELS}


def _compact_stage_table_name(stage_table: str) -> str:
    return _archived_identifier(stage_table, "_compact")


def _entity_address_unified_columns() -> list[str]:
    return [column.name for column in EntityAddressUnified.__table__.columns]


async def _compact_hot_row_source_record_ids(
    db_schema: str,
    stage_table: str,
    *,
    context: dict | None = None,
) -> int:
    phase_context = context if context is not None else {}
    compact_table = _compact_stage_table_name(stage_table)
    columns = _entity_address_unified_columns()
    columns_sql = ", ".join(columns)
    select_sql = ", ".join(
        "ARRAY[]::varchar[] AS source_record_ids" if column == "source_record_ids" else column
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
    if hasattr(stage_cls, "__my_additional_indexes__") and stage_cls.__my_additional_indexes__:
        for index in stage_cls.__my_additional_indexes__:
            index_name = index.get("name", "_".join(index.get("index_elements")))
            using = f"USING {index.get('using')} " if index.get("using") else ""
            where = f" WHERE {index.get('where')}" if index.get("where") else ""
            stmt = (
                f"CREATE INDEX IF NOT EXISTS "
                f"{_stage_index_name(stage_cls.__tablename__, index_name)} "
                f"ON {db_schema}.{stage_cls.__tablename__} {using}"
                f"({', '.join(index.get('index_elements'))}){where};"
            )
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
                    continue
                raise


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


async def _prepare_support_stage_tables(db_schema: str, import_date: str) -> dict[type, type]:
    stage_classes = _support_stage_classes(import_date)
    for stage_cls in stage_classes.values():
        await db.status(f"DROP TABLE IF EXISTS {db_schema}.{stage_cls.__tablename__};")
        await db.create_table(stage_cls.__table__, checkfirst=True)
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


async def _support_bridge_reuse_available(db_schema: str, *, build_network_bridge: bool) -> bool:
    bridge_models: list[type] = [
        EntityAddressPlanBridge,
        EntityAddressPTGBridge,
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


def _source_priority_expr(expr: str) -> str:
    return (
        "CASE "
        f"WHEN {expr} = 'nppes' THEN 0 "
        f"WHEN {expr} = 'cms_doctors' THEN 1 "
        f"WHEN {expr} = 'provider_enrollment_ffs' THEN 2 "
        f"WHEN {expr} = 'provider_enrollment_ffs_address' THEN 3 "
        f"WHEN {expr} LIKE 'facility_anchor:%' THEN 4 "
        f"WHEN {expr} = 'mrf' THEN 5 "
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


def _source_selects(
    db_schema: str,
    available: dict[str, bool],
    *,
    test_limit_per_source: int | None = None,
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
    has_ptg_address = available.get("ptg_address", False)
    has_ptg_address_key = available.get("ptg_address.address_key", has_ptg_address)
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
    ptg_address_key = source_address_key("ptg_address", "p")
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
    ptg_npi_join = f"LEFT JOIN {db_schema}.npi AS n ON n.npi = p.npi" if has_npi else ""
    ptg_archive_join = (
        f"LEFT JOIN {db_schema}.address_archive_v2 AS aa ON aa.address_key = p.address_key"
        if has_archive and has_ptg_address_key
        else ""
    )
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
                NULL::varchar AS ptg_address_version,
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
                NULL::varchar AS ptg_address_version,
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
                NULL::varchar AS ptg_address_version,
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
                NULL::varchar AS ptg_address_version,
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
                NULL::varchar AS ptg_address_version,
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

    if has_ptg_address:
        selects.append(
            f"""
            SELECT
                'npi'::varchar AS entity_type,
                p.npi::varchar AS entity_id,
                p.npi::bigint AS npi,
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
                COALESCE(p.ptg_plan_array, ARRAY[]::varchar[])::varchar[] AS ptg_plan_array,
                CASE
                    WHEN COALESCE(CARDINALITY(p.ptg_source_array), 0) = 0 THEN ARRAY[p.source_key]::varchar[]
                    ELSE p.ptg_source_array
                END::varchar[] AS ptg_source_array,
                COALESCE(p.group_plan_array, ARRAY[]::varchar[])::varchar[] AS group_plan_array,
                COALESCE(p.base_address_version, '{BASE_ADDRESS_VERSION}')::varchar AS base_address_version,
                ('ptg:' || p.snapshot_id)::varchar AS ptg_address_version,
                {('aa.first_line::varchar' if has_archive else 'NULL::varchar')} AS first_line,
                {('aa.second_line::varchar' if has_archive else 'NULL::varchar')} AS second_line,
                {('COALESCE(aa.city_name, p.city_norm)::varchar' if has_archive else 'p.city_norm::varchar')} AS city_name,
                {('COALESCE(aa.state_name, p.state_code)::varchar' if has_archive else 'p.state_code::varchar')} AS state_name,
                {('COALESCE(aa.postal_code, p.zip5)::varchar' if has_archive else 'p.zip5::varchar')} AS postal_code,
                'US'::varchar AS country_code,
                NULL::varchar AS telephone_number,
                NULL::varchar AS fax_number,
                {('aa.formatted_address::varchar' if has_archive else 'NULL::varchar')} AS formatted_address,
                p.lat::numeric AS lat,
                p.long::numeric AS long,
                NULL::date AS date_added,
                NULL::varchar AS place_id,
                {ptg_address_key} AS address_key,
                COALESCE(p.updated_at, NOW())::timestamp AS updated_at,
                'ptg'::varchar AS address_source,
                ('ptg:' || p.source_key || ':' || p.snapshot_id || ':' || p.location_key)::varchar AS source_record_id
              FROM {db_schema}.ptg_address AS p
              {ptg_npi_join}
              {ptg_pa_from}
              {ptg_archive_join}
             WHERE p.npi IS NOT NULL
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


def _ptg_source_key_filter(
    db_schema: str,
    source_keys: list[str],
    *,
    column_sql: str = "p.source_key",
) -> str:
    if not source_keys:
        raise ValueError("source_keys must be non-empty for PTG partial refresh")
    source_literals = ", ".join(_sql_literal(source_key) for source_key in source_keys)
    source_array = _string_array_literal(source_keys)
    return (
        f"({column_sql} IN ({source_literals}) "
        f"OR COALESCE(p.ptg_source_array, ARRAY[]::varchar[]) && {source_array} "
        "OR EXISTS ("
        f"SELECT 1 FROM {db_schema}.{EntityAddressUnified.__main_table__} AS live "
        "WHERE live.location_key = p.location_key "
        f"AND {_entity_address_ptg_source_overlap_sql(source_keys, alias='live')}"
        "))"
    )


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


def _ptg_partial_affected_group_table_name(stage_table: str) -> str:
    return _archived_identifier(stage_table, "_ptg_groups")


def _prepare_ptg_partial_affected_groups_sql(db_schema: str, group_table: str, source_keys: list[str]) -> str:
    return f"""
    CREATE UNLOGGED TABLE {db_schema}.{group_table} AS
    SELECT DISTINCT
        live.entity_type::varchar AS entity_type,
        live.entity_id::varchar AS entity_id,
        live.address_key::uuid AS address_key,
        {_street_soft_norm_expr("live.first_line")}::varchar AS street_key,
        {_alnum_norm_expr("live.city_name")}::varchar AS city_key,
        {_state_norm_expr("live.state_name")}::varchar AS state_key,
        {_zip5_norm_expr("live.postal_code")}::varchar AS zip_key,
        {_state_norm_expr("live.country_code")}::varchar AS country_key
      FROM {db_schema}.{EntityAddressUnified.__main_table__} AS live
     WHERE {_entity_address_ptg_source_overlap_sql(source_keys, alias="live")}
       AND live.location_key IS NOT NULL
    UNION
    SELECT DISTINCT
        'npi'::varchar AS entity_type,
        p.npi::varchar AS entity_id,
        p.address_key::uuid AS address_key,
        NULL::varchar AS street_key,
        {_alnum_norm_expr("p.city_norm")}::varchar AS city_key,
        {_state_norm_expr("p.state_code")}::varchar AS state_key,
        {_zip5_norm_expr("p.zip5")}::varchar AS zip_key,
        'us'::varchar AS country_key
      FROM {db_schema}.ptg_address AS p
     WHERE p.npi IS NOT NULL
       AND p.location_key IS NOT NULL
       AND {_ptg_source_key_filter(db_schema, source_keys)};
    """


def _index_ptg_partial_affected_groups_sql(db_schema: str, group_table: str) -> str:
    index_name = _archived_identifier(f"{group_table}_idx_group", "")
    return f"""
    CREATE INDEX {index_name}
        ON {db_schema}.{group_table} (
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


def _affected_group_source_select_sql(db_schema: str, source_select: str, group_table: str) -> str:
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


def _ptg_partial_source_selects(
    db_schema: str,
    source_selects: list[str],
    *,
    source_keys: list[str],
    affected_group_table: str | None = None,
) -> list[str]:
    marker = f"FROM {db_schema}.ptg_address AS p"
    where_marker = "WHERE p.npi IS NOT NULL"
    filtered: list[str] = []
    for source_select in source_selects:
        if marker not in source_select:
            if affected_group_table:
                filtered.append(
                    _affected_group_source_select_sql(db_schema, source_select, affected_group_table)
                )
            continue
        if where_marker not in source_select:
            raise RuntimeError("PTG source select shape changed; cannot apply ptg-partial source filter")
        filtered.append(
            source_select.replace(
                where_marker,
                f"{where_marker}\n               AND {_ptg_source_key_filter(db_schema, source_keys)}",
                1,
            )
        )
    return filtered


def _entity_address_column_list() -> str:
    return ", ".join(column.name for column in EntityAddressUnified.__table__.columns)


def _entity_address_ptg_source_overlap_sql(source_keys: list[str], *, alias: str = "live") -> str:
    return (
        f"COALESCE({alias}.ptg_source_array, ARRAY[]::varchar[]) "
        f"&& {_string_array_literal(source_keys)}"
    )


def _entity_address_reuse_live_rows_sql(
    db_schema: str,
    stage_table: str,
    raw_table: str,
    *,
    source_keys: list[str],
    affected_group_table: str | None = None,
) -> str:
    columns = _entity_address_column_list()
    affected_group_exclusion = ""
    if affected_group_table:
        affected_group_exclusion = f"""
       AND NOT EXISTS (
            SELECT 1
              FROM {db_schema}.{affected_group_table} AS affected
             WHERE {_entity_address_evidence_group_match_sql("affected", "live")}
       )"""
    return f"""
    INSERT INTO {db_schema}.{stage_table} ({columns})
    SELECT {columns}
      FROM {db_schema}.{EntityAddressUnified.__main_table__} AS live
     WHERE NOT ({_entity_address_ptg_source_overlap_sql(source_keys, alias="live")})
       {affected_group_exclusion}
       AND NOT EXISTS (
            SELECT 1
              FROM {db_schema}.{raw_table} AS raw
             WHERE raw.location_key = live.location_key
       );
    """


async def _copy_reusable_entity_address_rows(
    db_schema: str,
    stage_table: str,
    raw_table: str,
    *,
    source_keys: list[str],
    affected_group_table: str | None = None,
) -> int:
    if not await _table_exists(db_schema, EntityAddressUnified.__main_table__):
        raise RuntimeError(
            "entity-address-unified ptg-partial refresh requested, but the live "
            "entity_address_unified table does not exist; run refresh_mode=full first."
        )
    rowcount = await db.status(
        _entity_address_reuse_live_rows_sql(
            db_schema,
            stage_table,
            raw_table,
            source_keys=source_keys,
            affected_group_table=affected_group_table,
        )
    )
    return int(rowcount or 0)


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
        aca_plan_array varchar[] NOT NULL DEFAULT '{{}}',
        aca_network_array varchar[] NOT NULL DEFAULT '{{}}',
        ptg_plan_array varchar[] NOT NULL DEFAULT '{{}}',
        ptg_source_array varchar[] NOT NULL DEFAULT '{{}}',
        group_plan_array varchar[] NOT NULL DEFAULT '{{}}',
        base_address_version varchar(64),
        ptg_address_version varchar(64),
        checksum bigint NOT NULL
    );
    """


def _address_key_expr(db_schema: str, available: bool, *, address_source: str | None = None) -> str:
    if available:
        # Intentional in-DB fallback: this expression runs inside SQL materialization pipelines.
        fallback = (
            f"{db_schema}.addr_key_v1("
            "first_line, second_line, city_name, state_name, postal_code, country_code"
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
    checksum_min: int | None = None,
    checksum_max: int | None = None,
) -> str:
    archive_join = ""
    archive_fields = (
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
    if archive_available:
        archive_join = (
            f"LEFT JOIN {db_schema}.address_archive_v2 a "
            "ON a.address_key = r.address_key AND a.merged_into IS NULL"
        )
    else:
        archive_fields = (
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
    return f"""
    WITH enriched AS (
        SELECT
            r.ctid AS row_id,
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
            CASE WHEN source_id IN (1, 2, 3, 4, 5, 6) THEN source_mask ELSE 0::bigint END AS address_source_mask,
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
       SET premise_key = k.premise_key,
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
           base_address_version = '{BASE_ADDRESS_VERSION}',
           location_key = {_location_key_expr(
               entity_type='r.entity_type',
               entity_id='r.entity_id',
               npi='r.npi',
               inferred_npi='r.inferred_npi',
               address_role_id='k.address_role_id',
               row_origin='k.row_origin',
               address_key='r.address_key',
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

    null_location_keys = int(
        await db.scalar(f"SELECT COUNT(*) FROM {db_schema}.{stage_table} WHERE location_key IS NULL;")
        or 0
    )
    metrics["null_location_keys"] = null_location_keys
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
    metrics["duplicate_location_keys"] = duplicate_location_keys
    if duplicate_location_keys:
        failures.append(f"{duplicate_location_keys} duplicate staged location_key values")

    unresolved_merged_into_rows = 0
    missing_archive_address_key_rows = 0
    archive_coordinate_mismatch_rows = 0
    archive_missing_coordinate_rows = 0
    archive_identity_mismatch_rows = 0
    if await _table_exists(db_schema, "address_archive_v2"):
        unresolved_merged_into_rows = int(
            await db.scalar(
                f"""
                SELECT COUNT(*)
                  FROM {db_schema}.{stage_table} AS t
                  JOIN {db_schema}.address_archive_v2 AS a
                    ON a.address_key = t.address_key
                 WHERE t.address_key IS NOT NULL
                   AND a.merged_into IS NOT NULL;
                """
            )
            or 0
        )
        archive_coordinate_mismatch_rows = int(
            await db.scalar(
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
            )
            or 0
        )
        archive_missing_coordinate_rows = int(
            await db.scalar(
                f"""
                SELECT COUNT(*)
                  FROM {db_schema}.{stage_table} AS t
                  JOIN {db_schema}.address_archive_v2 AS a
                    ON a.address_key = t.address_key
                   AND a.merged_into IS NULL
                 WHERE t.address_key IS NOT NULL
                   AND (a.lat IS NULL OR a.long IS NULL);
                """
            )
            or 0
        )
        missing_archive_address_key_rows = int(
            await db.scalar(
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
            )
            or 0
        )
        archive_identity_mismatch_rows = int(
            await db.scalar(
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
            )
            or 0
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

    practice_null_address_key_rows = int(
        await db.scalar(
            f"""
            SELECT COUNT(*)
              FROM {db_schema}.{stage_table}
             WHERE type = 'practice'
               AND address_key IS NULL;
            """
        )
        or 0
    )
    metrics["practice_null_address_key_rows"] = practice_null_address_key_rows
    practice_null_address_key_by_source_rows = await db.all(
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
    )
    metrics["practice_null_address_key_by_source"] = {
        str(row._mapping["source"]): int(row._mapping["rows"] or 0)
        for row in practice_null_address_key_by_source_rows
    }

    metrics["archive_identity_mismatch_rows"] = archive_identity_mismatch_rows
    if archive_identity_mismatch_rows:
        failures.append(
            f"{archive_identity_mismatch_rows} staged rows do not match address_archive_v2 identity_version"
        )

    fallback_archive_identity_mismatch_rows = int(
        await db.scalar(
            f"""
            SELECT COUNT(*)
              FROM {db_schema}.{stage_table}
             WHERE address_key IS NULL
               AND COALESCE(archive_identity_version, '') <> '{ARCHIVE_IDENTITY_VERSION}';
            """
        )
        or 0
    )
    metrics["fallback_archive_identity_mismatch_rows"] = fallback_archive_identity_mismatch_rows
    if fallback_archive_identity_mismatch_rows:
        failures.append(
            f"{fallback_archive_identity_mismatch_rows} staged rows without address_key use a non-current archive_identity_version"
        )

    invalid_coordinate_rows = await _invalid_coordinate_count(db_schema, stage_table)
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
        ptg_address_version,
        first_line,
        second_line,
        city_name,
        state_name,
        postal_code,
        country_code,
        telephone_number,
        fax_number,
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
            NULLIF(TRIM(ptg_address_version), '')::varchar AS ptg_address_version,
            NULLIF(TRIM(first_line), '')::varchar AS first_line,
            NULLIF(TRIM(second_line), '')::varchar AS second_line,
            NULLIF(TRIM(city_name), '')::varchar AS city_name,
            NULLIF(TRIM(state_name), '')::varchar AS state_name,
            NULLIF(TRIM(postal_code), '')::varchar AS postal_code,
            COALESCE(NULLIF(TRIM(country_code), ''), 'US')::varchar AS country_code,
            NULLIF(TRIM(telephone_number), '')::varchar AS telephone_number,
            NULLIF(TRIM(fax_number), '')::varchar AS fax_number,
            NULLIF(TRIM(formatted_address), '')::varchar AS formatted_address,
            lat::numeric AS lat,
            long::numeric AS long,
            date_added::date AS date_added,
            NULLIF(TRIM(place_id), '')::varchar AS place_id,
            {_source_priority_expr("address_source")}::int AS source_priority,
            address_source::varchar AS address_source,
            source_record_id::varchar AS source_record_id,
            updated_at::timestamp AS updated_at,
            COALESCE(
                address_key::uuid,
                {_address_key_expr(db_schema, address_canon_available, address_source="address_source")}
            ) AS address_key
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
            ptg_address_version,
            first_line,
            second_line,
            city_name,
            state_name,
            postal_code,
            country_code,
            telephone_number,
            fax_number,
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
        ptg_address_version,
        first_line,
        second_line,
        city_name,
        state_name,
        postal_code,
        country_code,
        telephone_number,
        fax_number,
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
) -> str:
    dedupe_key_expr = _dedupe_key_expr(address_canon_available)
    shard_filter = ""
    if checksum_modulo and checksum_modulo > 1 and checksum_remainder is not None:
        shard_filter = (
            f" WHERE ((hashtext({dedupe_key_expr}) % {int(checksum_modulo)} "
            f"+ {int(checksum_modulo)}) % {int(checksum_modulo)}) = {int(checksum_remainder)}"
        )
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
        source_record_ids,
        aca_plan_array,
        aca_network_array,
        ptg_plan_array,
        ptg_source_array,
        group_plan_array,
        base_address_version,
        ptg_address_version,
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
            (ARRAY_AGG(ptg_address_version ORDER BY source_priority ASC, updated_at DESC NULLS LAST))[1]::varchar AS ptg_address_version,
            (ARRAY_AGG(checksum ORDER BY source_priority ASC, updated_at DESC NULLS LAST))[1]::bigint AS checksum,
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
            (ARRAY_AGG(formatted_address ORDER BY source_priority ASC, (formatted_address IS NULL), LENGTH(COALESCE(formatted_address, '')) DESC, updated_at DESC NULLS LAST))[1]::varchar AS formatted_address,
            (ARRAY_AGG(lat ORDER BY (lat IS NULL), source_priority ASC, updated_at DESC NULLS LAST))[1]::numeric AS lat,
            (ARRAY_AGG(long ORDER BY (long IS NULL), source_priority ASC, updated_at DESC NULLS LAST))[1]::numeric AS long,
            MAX(date_added)::date AS date_added,
            MAX(place_id)::varchar AS place_id,
            (ARRAY_AGG(address_key ORDER BY source_priority ASC, (address_key IS NULL), updated_at DESC NULLS LAST))[1]::uuid AS address_key,
            ARRAY_REMOVE(ARRAY_AGG(DISTINCT address_source ORDER BY address_source), NULL)::varchar[] AS address_sources,
            ARRAY_REMOVE(ARRAY_AGG(DISTINCT source_record_id ORDER BY source_record_id), NULL)::varchar[] AS source_record_ids,
            ARRAY_REMOVE(ARRAY_AGG(DISTINCT aca_plan.value ORDER BY aca_plan.value), NULL)::varchar[] AS aca_plan_array,
            ARRAY_REMOVE(ARRAY_AGG(DISTINCT aca_network.value ORDER BY aca_network.value), NULL)::varchar[] AS aca_network_array,
            ARRAY_REMOVE(ARRAY_AGG(DISTINCT ptg_plan.value ORDER BY ptg_plan.value), NULL)::varchar[] AS ptg_plan_array,
            ARRAY_REMOVE(ARRAY_AGG(DISTINCT ptg_source.value ORDER BY ptg_source.value), NULL)::varchar[] AS ptg_source_array,
            ARRAY_REMOVE(ARRAY_AGG(DISTINCT group_plan.value ORDER BY group_plan.value), NULL)::varchar[] AS group_plan_array,
            MAX(updated_at)::timestamp AS updated_at
          FROM {db_schema}.{raw_table}
          LEFT JOIN LATERAL unnest(COALESCE(aca_plan_array, ARRAY[]::varchar[])) AS aca_plan(value) ON TRUE
          LEFT JOIN LATERAL unnest(COALESCE(aca_network_array, ARRAY[]::varchar[])) AS aca_network(value) ON TRUE
          LEFT JOIN LATERAL unnest(COALESCE(ptg_plan_array, ARRAY[]::varchar[])) AS ptg_plan(value) ON TRUE
          LEFT JOIN LATERAL unnest(COALESCE(ptg_source_array, ARRAY[]::varchar[])) AS ptg_source(value) ON TRUE
          LEFT JOIN LATERAL unnest(COALESCE(group_plan_array, ARRAY[]::varchar[])) AS group_plan(value) ON TRUE
         {shard_filter}
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
        COALESCE(aca_plan_array, ARRAY[]::varchar[]) AS aca_plan_array,
        COALESCE(aca_network_array, ARRAY[]::varchar[]) AS aca_network_array,
        COALESCE(ptg_plan_array, ARRAY[]::varchar[]) AS ptg_plan_array,
        COALESCE(ptg_source_array, ARRAY[]::varchar[]) AS ptg_source_array,
        COALESCE(group_plan_array, ARRAY[]::varchar[]) AS group_plan_array,
        base_address_version,
        ptg_address_version,
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
        formatted_address,
        lat,
        long,
        date_added,
        place_id,
        address_key,
        updated_at,
        updated_at AS last_seen_at
      FROM aggregated;
    """


def _materialize_sql(
    db_schema: str,
    stage_table: str,
    source_selects: Iterable[str],
    *,
    address_canon_available: bool = True,
) -> str:
    selects_sql = "\nUNION ALL\n".join(select.strip() for select in source_selects)
    dedupe_key_expr = _dedupe_key_expr(address_canon_available)
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
            NULLIF(TRIM(formatted_address), '')::varchar AS formatted_address,
            lat::numeric AS lat,
            long::numeric AS long,
            date_added::date AS date_added,
            NULLIF(TRIM(place_id), '')::varchar AS place_id,
            {_source_priority_expr("address_source")}::int AS source_priority,
            address_source::varchar AS address_source,
            source_record_id::varchar AS source_record_id,
            updated_at::timestamp AS updated_at,
            COALESCE(
                address_key::uuid,
                {_address_key_expr(db_schema, address_canon_available, address_source="address_source")}
            ) AS address_key
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
            (ARRAY_AGG(checksum ORDER BY source_priority ASC, updated_at DESC NULLS LAST))[1]::bigint AS checksum,
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
            (ARRAY_AGG(formatted_address ORDER BY source_priority ASC, (formatted_address IS NULL), LENGTH(COALESCE(formatted_address, '')) DESC, updated_at DESC NULLS LAST))[1]::varchar AS formatted_address,
            (ARRAY_AGG(lat ORDER BY (lat IS NULL), source_priority ASC, updated_at DESC NULLS LAST))[1]::numeric AS lat,
            (ARRAY_AGG(long ORDER BY (long IS NULL), source_priority ASC, updated_at DESC NULLS LAST))[1]::numeric AS long,
            MAX(date_added)::date AS date_added,
            MAX(place_id)::varchar AS place_id,
            (ARRAY_AGG(address_key ORDER BY source_priority ASC, (address_key IS NULL), updated_at DESC NULLS LAST))[1]::uuid AS address_key,
            ARRAY_REMOVE(ARRAY_AGG(DISTINCT address_source ORDER BY address_source), NULL)::varchar[] AS address_sources,
            ARRAY_REMOVE(ARRAY_AGG(DISTINCT source_record_id ORDER BY source_record_id), NULL)::varchar[] AS source_record_ids,
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
            COALESCE(street_key, ''),
            COALESCE(city_key, ''),
            COALESCE(state_key, ''),
            COALESCE(zip_key, ''),
            COALESCE(country_key, '')
        )) % {shards}) + {shards}) % {shards})::int
    """


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
) -> str:
    group_hash_expr = _evidence_group_hash_expr(evidence_shards)
    affected_filter = ""
    if affected_group_table:
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
            {_street_soft_norm_expr("t.first_line")}::varchar AS street_key,
            {_alnum_norm_expr("t.city_name")}::varchar AS city_key,
            {_state_norm_expr("t.state_name")}::varchar AS state_key,
            {_zip5_norm_expr("t.postal_code")}::varchar AS zip_key,
            {_state_norm_expr("t.country_code")}::varchar AS country_key,
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
       AND re.street_key IS NOT DISTINCT FROM se.street_key
       AND re.city_key IS NOT DISTINCT FROM se.city_key
       AND re.state_key IS NOT DISTINCT FROM se.state_key
       AND re.zip_key IS NOT DISTINCT FROM se.zip_key
       AND re.country_key IS NOT DISTINCT FROM se.country_key
      JOIN keyed AS k
        ON se.entity_type = k.entity_type
       AND se.entity_id = k.entity_id
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
        EntityAddressPTGBridge,
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
) -> str:
    return f"""
    DELETE FROM {db_schema}.{live_support_table} AS support
     USING {db_schema}.{old_entity_table} AS live
     WHERE support.location_key = live.location_key
       AND EXISTS (
            SELECT 1
              FROM {db_schema}.{affected_group_table} AS affected
             WHERE {_entity_address_evidence_group_match_sql("affected", "live")}
       );
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


def _ptg_bridge_sql(
    db_schema: str,
    ptg_stage_table: str,
    stage_table: str,
    *,
    affected_group_table: str | None = None,
) -> str:
    affected_filter = _affected_stage_row_filter_sql(db_schema, affected_group_table)
    return f"""
    INSERT INTO {db_schema}.{ptg_stage_table} (
        location_key,
        entity_type,
        entity_id,
        source_key,
        snapshot_id,
        ptg_plan_id
    )
    WITH source_record_bridge AS (
        SELECT DISTINCT
            t.location_key,
            t.entity_type,
            t.entity_id,
            NULLIF(split_part(record_id.value, ':', 2), '') AS source_key,
            NULLIF(split_part(record_id.value, ':', 3), '') AS snapshot_id,
            plan_id.value AS ptg_plan_id
          FROM {db_schema}.{stage_table} AS t
          JOIN LATERAL unnest(COALESCE(t.source_record_ids, ARRAY[]::varchar[])) AS record_id(value) ON TRUE
          JOIN LATERAL unnest(COALESCE(t.ptg_plan_array, ARRAY[]::varchar[])) AS plan_id(value) ON TRUE
         WHERE t.location_key IS NOT NULL
           {affected_filter}
           AND record_id.value LIKE 'ptg:%'
           AND NULLIF(plan_id.value, '') IS NOT NULL
           AND NULLIF(split_part(record_id.value, ':', 2), '') IS NOT NULL
           AND NULLIF(split_part(record_id.value, ':', 3), '') IS NOT NULL
    ),
    array_bridge AS (
        SELECT DISTINCT
            t.location_key,
            t.entity_type,
            t.entity_id,
            source_key.value AS source_key,
            NULLIF(regexp_replace(COALESCE(t.ptg_address_version, ''), '^ptg:', ''), '') AS snapshot_id,
            plan_id.value AS ptg_plan_id
          FROM {db_schema}.{stage_table} AS t
          JOIN LATERAL unnest(COALESCE(t.ptg_source_array, ARRAY[]::varchar[])) AS source_key(value) ON TRUE
          JOIN LATERAL unnest(COALESCE(t.ptg_plan_array, ARRAY[]::varchar[])) AS plan_id(value) ON TRUE
         WHERE t.location_key IS NOT NULL
           {affected_filter}
           AND NULLIF(source_key.value, '') IS NOT NULL
           AND NULLIF(plan_id.value, '') IS NOT NULL
           AND NULLIF(regexp_replace(COALESCE(t.ptg_address_version, ''), '^ptg:', ''), '') IS NOT NULL
           AND NOT EXISTS (
                SELECT 1
                  FROM source_record_bridge AS existing
                 WHERE existing.location_key = t.location_key
                   AND existing.entity_type = t.entity_type
                   AND existing.entity_id = t.entity_id
           )
    )
    SELECT DISTINCT
        location_key,
        entity_type,
        entity_id,
        source_key,
        snapshot_id,
        ptg_plan_id
      FROM source_record_bridge
    UNION
    SELECT DISTINCT
        location_key,
        entity_type,
        entity_id,
        source_key,
        snapshot_id,
        ptg_plan_id
      FROM array_bridge;
    """


def _procedure_bridge_sql(
    db_schema: str,
    procedure_stage_table: str,
    stage_table: str,
    *,
    affected_group_table: str | None = None,
) -> str:
    affected_filter = _affected_stage_row_filter_sql(db_schema, affected_group_table)
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
       AND procedure_code.value <> 0;
    """


def _medication_bridge_sql(
    db_schema: str,
    medication_stage_table: str,
    stage_table: str,
    *,
    affected_group_table: str | None = None,
) -> str:
    affected_filter = _affected_stage_row_filter_sql(db_schema, affected_group_table)
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
) -> str:
    candidate_limit = _env_int("HLTHPRT_FACILITY_ANCHOR_NPI_CANDIDATE_LIMIT", 25, minimum=1)

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
    bridge_specs = [
        (
            EntityAddressPlanBridge,
            "plan bridge",
            ("location_key", "entity_type", "entity_id", "plan_id", "market_type"),
            _plan_bridge_sql,
        ),
        (
            EntityAddressPTGBridge,
            "ptg bridge",
            ("location_key", "entity_type", "entity_id", "source_key", "snapshot_id", "ptg_plan_id"),
            _ptg_bridge_sql,
        ),
        (
            EntityAddressProcedureBridge,
            "procedure bridge",
            ("location_key", "npi", "code_system", "code"),
            _procedure_bridge_sql,
        ),
        (
            EntityAddressMedicationBridge,
            "medication bridge",
            ("location_key", "npi", "code_system", "code"),
            _medication_bridge_sql,
        ),
    ]
    if (
        not partial_support_patch
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
        statements.append(
            _SupportStageStatement(
                "facility anchor npi candidate",
                _facility_anchor_npi_candidate_sql(
                    db_schema,
                    stage_tables[FacilityAnchorNPICandidate],
                    stage_table,
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
                ),
            ),
        )
    if build_network_bridge:
        bridge_specs.insert(
            1,
            (
                EntityAddressNetworkBridge,
                "network bridge",
                ("location_key", "entity_type", "entity_id", "network_id"),
                _network_bridge_sql,
            ),
        )
    for model, label, columns, builder in bridge_specs:
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
        statements.append(
            _SupportStageStatement(
                label,
                builder(
                    db_schema,
                    stage_tables[model],
                    stage_table,
                    affected_group_table=affected_group_table if partial_bridge_reuse else None,
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
    partial_ptg_refresh = refresh_mode == ENTITY_ADDRESS_REFRESH_MODE_PTG_PARTIAL
    ptg_source_keys = _entity_address_ptg_source_keys(task) if partial_ptg_refresh else []
    if partial_ptg_refresh and not ptg_source_keys:
        raise RuntimeError("entity-address-unified ptg-partial refresh requires ptg_source_key.")
    context["refresh_mode"] = refresh_mode
    context["partial_ptg_source_keys"] = ptg_source_keys

    await ensure_database(test_mode)

    import_date = ctx["import_date"]
    db_schema = os.getenv("HLTHPRT_DB_SCHEMA") if os.getenv("HLTHPRT_DB_SCHEMA") else "mrf"
    stage_cls = make_class(EntityAddressUnified, import_date)
    stage_table = stage_cls.__tablename__
    reuse_stage = _env_bool("HLTHPRT_ENTITY_ADDRESS_UNIFIED_REUSE_STAGE", False)
    if partial_ptg_refresh and reuse_stage:
        raise RuntimeError(
            "entity-address-unified ptg-partial refresh cannot be combined with "
            "HLTHPRT_ENTITY_ADDRESS_UNIFIED_REUSE_STAGE."
        )

    if not ctx["context"].get("stage_prepared"):
        await _ensure_schema_exists(db_schema)
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
        "ptg_address",
        "address_archive_v2",
    ]
    available = {table: await _table_exists(db_schema, table) for table in required_checks}
    for table_name in (
        "npi_address",
        "doctor_clinician_address",
        "provider_enrollment_ffs_address",
        "facility_anchor",
        "mrf_address",
        "ptg_address",
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
    source_selects = _source_selects(
        db_schema,
        available,
        test_limit_per_source=test_limit_per_source,
    )
    affected_group_table: str | None = None
    if partial_ptg_refresh:
        if not await _table_exists(db_schema, EntityAddressUnified.__main_table__):
            raise RuntimeError(
                "entity-address-unified ptg-partial refresh requested, but the live "
                "entity_address_unified table does not exist; run refresh_mode=full first."
            )
        affected_group_table = _ptg_partial_affected_group_table_name(stage_table)
        context["partial_ptg_affected_group_table"] = affected_group_table
        await db.status(f"DROP TABLE IF EXISTS {db_schema}.{affected_group_table};")
        await _run_sql_phase(
            _prepare_ptg_partial_affected_groups_sql(
                db_schema,
                affected_group_table,
                ptg_source_keys,
            ),
            context=context,
            run_id=run_id,
            phase="entity-address-unified preparing affected PTG groups",
            unit="tables",
            done=0,
            total=1,
            message="preparing affected PTG evidence groups",
            emit_start=True,
            emit_done=True,
        )
        await _run_sql_phase(
            _index_ptg_partial_affected_groups_sql(db_schema, affected_group_table),
            context=context,
            run_id=run_id,
            phase="entity-address-unified indexing affected PTG groups",
            unit="indexes",
            done=0,
            total=1,
            message="indexing affected PTG evidence groups",
            emit_start=True,
            emit_done=True,
        )
        await _run_sql_phase(
            f"ANALYZE {db_schema}.{affected_group_table};",
            context=context,
            run_id=run_id,
            phase="entity-address-unified analyzing affected PTG groups",
            unit="tables",
            done=0,
            total=1,
            message="analyzing affected PTG evidence groups",
            emit_start=True,
            emit_done=True,
        )
        affected_group_rows = int(
            await db.scalar(f"SELECT COUNT(*) FROM {db_schema}.{affected_group_table};") or 0
        )
        context["partial_ptg_affected_groups"] = affected_group_rows
        source_selects = _ptg_partial_source_selects(
            db_schema,
            source_selects,
            source_keys=ptg_source_keys,
            affected_group_table=affected_group_table,
        )
        if not source_selects:
            raise RuntimeError(
                "entity-address-unified ptg-partial refresh requires an available ptg_address source table."
            )
        context["partial_ptg_source_selects"] = len(source_selects)
    source_table_shards = _env_int(
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_SOURCE_TABLE_SHARDS",
        DEFAULT_SOURCE_TABLE_SHARDS,
        minimum=1,
    )
    if source_table_shards > 1 and not test_limit_per_source and not partial_ptg_refresh:
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

    if not ctx["context"].get("support_stage_prepared"):
        support_stage_classes = await _prepare_support_stage_tables(db_schema, import_date)
        ctx["context"]["support_stage_prepared"] = True
        ctx["context"]["support_stage_indexes_prepared"] = False
    else:
        support_stage_classes = _support_stage_classes(import_date)

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
    if partial_ptg_refresh and not chunked_load:
        raise RuntimeError(
            "entity-address-unified ptg-partial refresh requires "
            "HLTHPRT_ENTITY_ADDRESS_UNIFIED_CHUNKED_LOAD=true."
        )
    raw_table: str | None = None
    if chunked_load and not reuse_stage:
        source_concurrency = _env_int(
            "HLTHPRT_ENTITY_ADDRESS_UNIFIED_SOURCE_CONCURRENCY",
            DEFAULT_SOURCE_CONCURRENCY,
            minimum=1,
        )
        aggregate_shards = _env_int(
            "HLTHPRT_ENTITY_ADDRESS_UNIFIED_AGGREGATE_SHARDS",
            DEFAULT_AGGREGATE_SHARDS,
            minimum=1,
        )
        aggregate_concurrency = min(
            _env_int(
                "HLTHPRT_ENTITY_ADDRESS_UNIFIED_AGGREGATE_CONCURRENCY",
                DEFAULT_AGGREGATE_CONCURRENCY,
                minimum=1,
            ),
            aggregate_shards,
        )
        enrich_shards = _env_int(
            "HLTHPRT_ENTITY_ADDRESS_UNIFIED_ENRICH_SHARDS",
            DEFAULT_ENRICH_SHARDS,
            minimum=1,
        )
        enrich_concurrency = min(
            _env_int(
                "HLTHPRT_ENTITY_ADDRESS_UNIFIED_ENRICH_CONCURRENCY",
                DEFAULT_ENRICH_CONCURRENCY,
                minimum=1,
            ),
            enrich_shards,
        )
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
                                checksum_min=checksum_min,
                                checksum_max=checksum_max,
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
            f"CREATE INDEX {raw_table}_idx_group_key "
            f"ON {db_schema}.{raw_table} "
            "(entity_type, entity_id, type, location_key);",
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
        await db.status(f"TRUNCATE TABLE {db_schema}.{stage_table};")
        if partial_ptg_refresh:
            reused_rows = await _copy_reusable_entity_address_rows(
                db_schema,
                stage_table,
                raw_table,
                source_keys=ptg_source_keys,
                affected_group_table=affected_group_table,
            )
            context["partial_ptg_reused_rows"] = reused_rows
            if run_id:
                enqueue_live_progress(
                    run_id=run_id,
                    importer="entity-address-unified",
                    status="running",
                    phase="entity-address-unified reusing unchanged rows",
                    unit="rows",
                    done=reused_rows,
                    total=reused_rows,
                    pct=55,
                    message=f"reused {reused_rows:,} live entity-address rows",
                )
        else:
            context["partial_ptg_reused_rows"] = 0
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
    build_network_bridge = _env_bool(
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_BUILD_NETWORK_BRIDGE",
        DEFAULT_BUILD_NETWORK_BRIDGE,
    )
    context["build_network_bridge"] = build_network_bridge
    cached_support_counts = context.get("support_counts")
    if context.get("support_stage_populated") and isinstance(cached_support_counts, dict):
        support_counts = {str(key): int(value) for key, value in cached_support_counts.items()}
    else:
        support_raw_table = None if partial_ptg_refresh else raw_table
        support_affected_group_table = None
        copy_unaffected_support_bridges = True
        if partial_ptg_refresh and affected_group_table:
            support_reuse_available = await _support_bridge_reuse_available(
                db_schema,
                build_network_bridge=build_network_bridge,
            )
            context["partial_ptg_support_bridge_reuse"] = support_reuse_available
            if support_reuse_available:
                support_affected_group_table = affected_group_table
                patch_support = _env_bool(
                    "HLTHPRT_ENTITY_ADDRESS_UNIFIED_PTG_PARTIAL_PATCH_SUPPORT",
                    DEFAULT_PTG_PARTIAL_PATCH_SUPPORT,
                )
                context["partial_ptg_support_patch_publish"] = patch_support
                copy_unaffected_support_bridges = not patch_support
            else:
                context["partial_ptg_support_patch_publish"] = False
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
    if raw_table:
        await db.status(f"DROP TABLE IF EXISTS {db_schema}.{raw_table};")
    if affected_group_table and not context.get("partial_ptg_support_patch_publish"):
        await db.status(f"DROP TABLE IF EXISTS {db_schema}.{affected_group_table};")

    if (
        _env_bool(
            "HLTHPRT_ENTITY_ADDRESS_UNIFIED_COMPACT_SOURCE_RECORD_IDS",
            DEFAULT_COMPACT_SOURCE_RECORD_IDS,
        )
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
        and not context.get("partial_ptg_support_patch_publish")
    ):
        await _create_support_stage_indexes(
            support_stage_classes,
            db_schema,
            context=context,
            run_id=run_id,
        )
        context["support_stage_indexes_prepared"] = True

    staged_rows = int(await db.scalar(f"SELECT COUNT(*) FROM {db_schema}.{stage_table};") or 0)
    npi_rows = int(
        await db.scalar(f"SELECT COUNT(*) FROM {db_schema}.{stage_table} WHERE entity_type = 'npi';")
        or 0
    )
    inferred_rows = int(
        await db.scalar(f"SELECT COUNT(*) FROM {db_schema}.{stage_table} WHERE inferred_npi IS NOT NULL;")
        or 0
    )
    multi_source_rows = int(
        await db.scalar(
            f"SELECT COUNT(*) FROM {db_schema}.{stage_table} WHERE multi_source_confirmed IS TRUE;"
        )
        or 0
    )

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
    support_stage_classes = _support_stage_classes(import_date)
    affected_group_table = str(context.get("partial_ptg_affected_group_table") or "").strip()
    partial_support_patch = bool(context.get("partial_ptg_support_patch_publish"))

    stage_rows = int(await db.scalar(f"SELECT COUNT(*) FROM {db_schema}.{stage_cls.__tablename__};") or 0)
    min_rows_required = int(
        os.getenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_MIN_ROWS", str(DEFAULT_MIN_ROWS))
    )
    previous_rows = 0
    if await _table_exists(db_schema, EntityAddressUnified.__main_table__):
        previous_rows = int(
            await db.scalar(f"SELECT COUNT(*) FROM {db_schema}.{EntityAddressUnified.__main_table__};")
            or 0
        )
    if context.get("test_mode"):
        logger.info("EntityAddressUnified test mode: staged rows=%d", stage_rows)
    _validate_publish_row_count(
        stage_rows=stage_rows,
        previous_rows=previous_rows,
        test_mode=bool(context.get("test_mode")),
        min_rows_required=min_rows_required,
    )
    publish_validation = await _validate_publish_integrity(
        db_schema,
        stage_cls.__tablename__,
        support_stage_classes,
        test_mode=bool(context.get("test_mode")),
    )
    context["publish_validation"] = publish_validation

    if not bool(context.get("publish_requested", True)):
        logger.info("EntityAddressUnified publish skipped: staged rows=%d", stage_rows)
        await _drop_stage_artifacts(
            db_schema,
            stage_cls,
            support_stage_classes,
            extra_tables=[affected_group_table],
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
                "partial_ptg_source_keys": context.get("partial_ptg_source_keys") or [],
                "partial_ptg_reused_rows": int(context.get("partial_ptg_reused_rows") or 0),
                "npi_rows": int(context.get("npi_rows") or 0),
                "inferred_rows": int(context.get("inferred_rows") or 0),
                "multi_source_rows": int(context.get("multi_source_rows") or 0),
                "support_counts": context.get("support_counts") or {},
                "publish_validation": context.get("publish_validation") or {},
                "phase_timings": context.get("phase_timings") or {},
            },
        )
        print_time_info(context.get("start"))
        return

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
                    "entity-address-unified ptg-partial support patch publish requires "
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
            extra_tables=[affected_group_table],
        )

    logger.info("EntityAddressUnified publish complete: rows=%d", stage_rows)
    await mark_control_run(
        run_id,
        status="succeeded",
        phase_detail="entity-address-unified published",
        progress_message="succeeded",
        progress={
            "unit": "rows",
            "done": stage_rows,
            "total": stage_rows,
            "pct": 100,
            "message": "succeeded",
            "phase": "entity-address-unified published",
        },
        metrics={
            "rows": stage_rows,
            "refresh_mode": context.get("refresh_mode") or ENTITY_ADDRESS_REFRESH_MODE_FULL,
            "partial_ptg_source_keys": context.get("partial_ptg_source_keys") or [],
            "partial_ptg_reused_rows": int(context.get("partial_ptg_reused_rows") or 0),
            "npi_rows": int(context.get("npi_rows") or 0),
            "inferred_rows": int(context.get("inferred_rows") or 0),
            "multi_source_rows": int(context.get("multi_source_rows") or 0),
            "support_counts": context.get("support_counts") or {},
            "publish_validation": context.get("publish_validation") or {},
            "phase_timings": context.get("phase_timings") or {},
        },
    )
    print_time_info(context.get("start"))


async def main(
    test_mode: bool = False,
    limit_per_source: int | None = None,
    publish: bool | None = None,
    refresh_mode: str | None = None,
    ptg_source_key: str | None = None,
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
    if ptg_source_key:
        payload["ptg_source_key"] = ptg_source_key
    await redis.enqueue_job("process_data", payload, _queue_name=ENTITY_ADDRESS_UNIFIED_QUEUE_NAME)
