# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import csv
import datetime
import hashlib
import json
import logging
import math
import os
import re
import secrets
import shutil
import statistics
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import aiohttp
from aiofile import async_open
from aiocsv import AsyncDictReader
from arq import Retry, create_pool

import db.models as db_models
from db.connection import db
from db.models import (
    GeoZipLookup,
    NPIDataTaxonomy,
    NUCCTaxonomy,
    PricingProvider,
    PricingProviderProcedure,
    PricingProviderPrescription,
    PricingProviderQualityDomain,
    PricingProviderQualityMeasure,
    PricingProviderQualityScore,
    PricingQppProvider,
    PricingQualityRun,
    PricingSviZcta,
)
from process.ext.utils import (
    download_it_and_save,
    ensure_database,
    get_http_client,
    get_import_schema,
    make_class,
    push_objects,
    return_checksum,
)
from process.redis_config import build_redis_settings
from process.serialization import deserialize_job, serialize_job

logger = logging.getLogger(__name__)

# CSV rows can exceed Python's default field limit.
_csv_limit = sys.maxsize
while True:
    try:
        csv.field_size_limit(_csv_limit)
        break
    except OverflowError:
        _csv_limit = _csv_limit // 10


QPP_CSV_URL_TEMPLATE = str(os.getenv("HLTHPRT_PROVIDER_QUALITY_QPP_CSV_URL", "")).strip()
SVI_CSV_URL_TEMPLATE = str(os.getenv("HLTHPRT_PROVIDER_QUALITY_SVI_CSV_URL", "")).strip()

TEST_MAX_DOWNLOAD_BYTES = int(
    os.getenv(
        "HLTHPRT_PROVIDER_QUALITY_TEST_MAX_DOWNLOAD_BYTES",
        os.getenv("HLTHPRT_CLAIMS_TEST_MAX_DOWNLOAD_BYTES", str(25 * 1024 * 1024)),
    )
)
IMPORT_BATCH_SIZE = max(
    int(
        os.getenv(
            "HLTHPRT_PROVIDER_QUALITY_IMPORT_BATCH_SIZE",
            os.getenv("HLTHPRT_CLAIMS_IMPORT_BATCH_SIZE", "100000"),
        )
    ),
    100,
)
DOWNLOAD_RETRIES = max(
    int(
        os.getenv(
            "HLTHPRT_PROVIDER_QUALITY_DOWNLOAD_RETRIES",
            os.getenv("HLTHPRT_CLAIMS_DOWNLOAD_RETRIES", "3"),
        )
    ),
    1,
)
ROW_PROGRESS_INTERVAL_SECONDS = max(
    float(
        os.getenv(
            "HLTHPRT_PROVIDER_QUALITY_ROW_PROGRESS_SECONDS",
            os.getenv("HLTHPRT_CLAIMS_ROW_PROGRESS_SECONDS", "2"),
        )
    ),
    0.5,
)


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on", "y"}


PROVIDER_QUALITY_QUEUE_NAME = "arq:ProviderQuality"
PROVIDER_QUALITY_FINISH_QUEUE_NAME = "arq:ProviderQuality_finish"
PROVIDER_QUALITY_MATERIALIZE_SHARD_QUEUE_NAME = str(
    os.getenv("HLTHPRT_PROVIDER_QUALITY_MATERIALIZE_SHARD_QUEUE", PROVIDER_QUALITY_FINISH_QUEUE_NAME)
).strip() or PROVIDER_QUALITY_FINISH_QUEUE_NAME
# Kept for compatibility with tests/config; current ARQ client ignores per-job max tries kwargs.
PROVIDER_QUALITY_MATERIALIZE_SHARD_MAX_TRIES = max(
    int(os.getenv("HLTHPRT_PROVIDER_QUALITY_MATERIALIZE_SHARD_MAX_TRIES", "20")),
    1,
)
PROVIDER_QUALITY_FINALIZE_MAX_TRIES = max(
    int(os.getenv("HLTHPRT_PROVIDER_QUALITY_FINALIZE_MAX_TRIES", "720")),
    1,
)
PROVIDER_QUALITY_CHUNK_TARGET_MB = max(
    int(os.getenv("HLTHPRT_PROVIDER_QUALITY_CHUNK_TARGET_MB", "16")),
    1,
)
PROVIDER_QUALITY_CHUNK_TARGET_BYTES = PROVIDER_QUALITY_CHUNK_TARGET_MB * 1024 * 1024
PROVIDER_QUALITY_FINISH_RETRY_SECONDS = max(
    int(os.getenv("HLTHPRT_PROVIDER_QUALITY_FINISH_RETRY_SECONDS", "15")),
    1,
)
PROVIDER_QUALITY_REDIS_TTL_SECONDS = max(
    int(os.getenv("HLTHPRT_PROVIDER_QUALITY_REDIS_TTL_SECONDS", "172800")),
    3600,
)
PROVIDER_QUALITY_WORKDIR = os.getenv(
    "HLTHPRT_PROVIDER_QUALITY_WORKDIR",
    os.getenv("HLTHPRT_CLAIMS_WORKDIR", "/tmp/healthporta_provider_quality"),
)
PROVIDER_QUALITY_KEEP_WORKDIR = _env_bool("HLTHPRT_PROVIDER_QUALITY_KEEP_WORKDIR", default=False)
PROVIDER_QUALITY_DOWNLOAD_CONCURRENCY = max(
    int(os.getenv("HLTHPRT_PROVIDER_QUALITY_DOWNLOAD_CONCURRENCY", "2")),
    1,
)
PROVIDER_QUALITY_DEFER_STAGE_INDEXES = _env_bool(
    "HLTHPRT_PROVIDER_QUALITY_DEFER_STAGE_INDEXES",
    default=True,
)
PROVIDER_QUALITY_DB_DEADLOCK_RETRIES = max(
    int(os.getenv("HLTHPRT_PROVIDER_QUALITY_DB_DEADLOCK_RETRIES", "6")),
    1,
)
PROVIDER_QUALITY_DB_DEADLOCK_BASE_DELAY_SECONDS = max(
    float(os.getenv("HLTHPRT_PROVIDER_QUALITY_DB_DEADLOCK_BASE_DELAY_SECONDS", "0.25")),
    0.05,
)
PROVIDER_QUALITY_MARK_DONE_RETRIES = max(
    int(os.getenv("HLTHPRT_PROVIDER_QUALITY_MARK_DONE_RETRIES", "8")),
    1,
)
PROVIDER_QUALITY_MARK_DONE_RETRY_BASE_SECONDS = max(
    float(os.getenv("HLTHPRT_PROVIDER_QUALITY_MARK_DONE_RETRY_BASE_SECONDS", "0.5")),
    0.05,
)
PROVIDER_QUALITY_MARK_DONE_RETRY_MAX_SECONDS = max(
    float(os.getenv("HLTHPRT_PROVIDER_QUALITY_MARK_DONE_RETRY_MAX_SECONDS", "10")),
    PROVIDER_QUALITY_MARK_DONE_RETRY_BASE_SECONDS,
)

PROVIDER_QUALITY_MIN_YEAR = max(int(os.getenv("HLTHPRT_PROVIDER_QUALITY_MIN_YEAR", "2023")), 2013)
PROVIDER_QUALITY_MAX_YEAR = max(
    int(os.getenv("HLTHPRT_PROVIDER_QUALITY_MAX_YEAR", str(PROVIDER_QUALITY_MIN_YEAR))),
    PROVIDER_QUALITY_MIN_YEAR,
)
PROVIDER_QUALITY_YEAR_WINDOW = tuple(range(PROVIDER_QUALITY_MIN_YEAR, PROVIDER_QUALITY_MAX_YEAR + 1))

PROVIDER_QUALITY_MODEL_VERSION = str(os.getenv("HLTHPRT_PROVIDER_QUALITY_MODEL_VERSION", "v2.0")).strip() or "v2.0"
_raw_provider_quality_benchmark_mode = str(os.getenv("HLTHPRT_PROVIDER_QUALITY_BENCHMARK_MODE", "national")).strip().lower()
PROVIDER_QUALITY_BENCHMARK_MODE = (
    _raw_provider_quality_benchmark_mode
    if _raw_provider_quality_benchmark_mode in {"national", "state", "zip"}
    else "national"
)
PROVIDER_QUALITY_BENCHMARK_ORDER = ("zip", "state", "national")
PROVIDER_QUALITY_MIN_STATE_PEER_N = max(int(os.getenv("HLTHPRT_PROVIDER_QUALITY_MIN_STATE_PEER_N", "30")), 1)
PROVIDER_QUALITY_ZIP_MIN_PEER_N = max(int(os.getenv("HLTHPRT_PROVIDER_QUALITY_ZIP_MIN_PEER_N", "30")), 1)
PROVIDER_QUALITY_COHORT_ENABLED = _env_bool("HLTHPRT_PROVIDER_QUALITY_COHORT_ENABLED", default=True)
PROVIDER_QUALITY_MATERIALIZE_SHARDED_ENABLED = _env_bool(
    "HLTHPRT_PROVIDER_QUALITY_MATERIALIZE_SHARDED_ENABLED",
    default=False,
)
PROVIDER_QUALITY_LSH_SHARDS = max(int(os.getenv("HLTHPRT_PROVIDER_QUALITY_LSH_SHARDS", "8")), 1)
PROVIDER_QUALITY_MEASURE_SHARDS = max(int(os.getenv("HLTHPRT_PROVIDER_QUALITY_MEASURE_SHARDS", "8")), 1)
PROVIDER_QUALITY_DOMAIN_SHARDS = max(int(os.getenv("HLTHPRT_PROVIDER_QUALITY_DOMAIN_SHARDS", "8")), 1)
PROVIDER_QUALITY_SCORE_SHARDS = max(int(os.getenv("HLTHPRT_PROVIDER_QUALITY_SCORE_SHARDS", "8")), 1)
PROVIDER_QUALITY_SHARD_PARALLELISM = max(int(os.getenv("HLTHPRT_PROVIDER_QUALITY_SHARD_PARALLELISM", "8")), 1)
_raw_provider_quality_shard_work_mem = str(
    os.getenv("HLTHPRT_PROVIDER_QUALITY_SHARD_WORK_MEM", "64MB")
).strip()
if re.fullmatch(r"\d+(kB|MB|GB)", _raw_provider_quality_shard_work_mem):
    PROVIDER_QUALITY_SHARD_WORK_MEM = _raw_provider_quality_shard_work_mem
else:
    PROVIDER_QUALITY_SHARD_WORK_MEM = "64MB"
PROVIDER_QUALITY_SHARD_JIT = "on" if _env_bool("HLTHPRT_PROVIDER_QUALITY_SHARD_JIT", default=False) else "off"
PROVIDER_QUALITY_SHARD_MAX_PARALLEL_GATHER = int(
    os.getenv("HLTHPRT_PROVIDER_QUALITY_SHARD_MAX_PARALLEL_GATHER", "0")
)
PROVIDER_QUALITY_NATIONAL_MIN_PEER_N = max(
    int(os.getenv("HLTHPRT_PROVIDER_QUALITY_NATIONAL_MIN_PEER_N", "100")),
    1,
)
PROVIDER_QUALITY_MINHASH_NUM_PERM = max(
    int(os.getenv("HLTHPRT_PROVIDER_QUALITY_MINHASH_NUM_PERM", "108")),
    1,
)
PROVIDER_QUALITY_MINHASH_BANDS = max(
    int(os.getenv("HLTHPRT_PROVIDER_QUALITY_MINHASH_BANDS", "36")),
    1,
)
PROVIDER_QUALITY_MINHASH_ROWS_PER_BAND = max(
    int(os.getenv("HLTHPRT_PROVIDER_QUALITY_MINHASH_ROWS_PER_BAND", "3")),
    1,
)
PROVIDER_QUALITY_MINHASH_NUM_PERM = max(
    PROVIDER_QUALITY_MINHASH_NUM_PERM,
    PROVIDER_QUALITY_MINHASH_BANDS * PROVIDER_QUALITY_MINHASH_ROWS_PER_BAND,
)
PROVIDER_QUALITY_PROCEDURE_MATCH_THRESHOLD = min(
    max(float(os.getenv("HLTHPRT_PROVIDER_QUALITY_PROCEDURE_MATCH_THRESHOLD", "0.30")), 0.0),
    1.0,
)
PROVIDER_QUALITY_PROCEDURE_MATCH_BANDS = max(
    1,
    min(
        PROVIDER_QUALITY_MINHASH_BANDS,
        int(math.ceil(PROVIDER_QUALITY_MINHASH_BANDS * PROVIDER_QUALITY_PROCEDURE_MATCH_THRESHOLD)),
    ),
)
PROVIDER_QUALITY_ZIP_MAX_RADIUS_MILES = max(
    float(os.getenv("HLTHPRT_PROVIDER_QUALITY_ZIP_MAX_RADIUS_MILES", "50")),
    1.0,
)
PROVIDER_QUALITY_ZIP_RADIUS_STEP_MILES = max(
    float(os.getenv("HLTHPRT_PROVIDER_QUALITY_ZIP_RADIUS_STEP_MILES", "10")),
    1.0,
)
PROVIDER_QUALITY_ZIP_MAX_RING = max(
    1,
    int(math.ceil(PROVIDER_QUALITY_ZIP_MAX_RADIUS_MILES / PROVIDER_QUALITY_ZIP_RADIUS_STEP_MILES)),
)
PROVIDER_QUALITY_SHRINKAGE_ALPHA = max(float(os.getenv("HLTHPRT_PROVIDER_QUALITY_SHRINKAGE_ALPHA", "100.0")), 1.0)
PROVIDER_QUALITY_DEFAULT_SVI = min(
    max(float(os.getenv("HLTHPRT_PROVIDER_QUALITY_DEFAULT_SVI", "0.5")), 0.0),
    1.0,
)
PROVIDER_QUALITY_FAIL_ON_SOURCE_ERROR = _env_bool("HLTHPRT_PROVIDER_QUALITY_FAIL_ON_SOURCE_ERROR", default=True)
PROVIDER_QUALITY_ALLOW_DEGRADED_TEST_ONLY = _env_bool("HLTHPRT_PROVIDER_QUALITY_ALLOW_DEGRADED_TEST_ONLY", default=True)
PROVIDER_QUALITY_TEST_QPP_ROWS = max(int(os.getenv("HLTHPRT_PROVIDER_QUALITY_TEST_QPP_ROWS", "5000")), 10)
PROVIDER_QUALITY_TEST_SVI_ROWS = max(int(os.getenv("HLTHPRT_PROVIDER_QUALITY_TEST_SVI_ROWS", "5000")), 10)
POSTGRES_IDENTIFIER_MAX_LENGTH = 63
MAX_NPI = 9_999_999_999
PROVIDER_QUALITY_GLOBAL_FINALIZE_LOCK_KEY = "imports:finalize_mutex"
PROVIDER_QUALITY_GLOBAL_FINALIZE_LOCK_TTL_SECONDS = max(
    int(os.getenv("HLTHPRT_PROVIDER_QUALITY_GLOBAL_FINALIZE_LOCK_TTL_SECONDS", "7200")),
    300,
)
COHORT_MODEL_CLASS_NAMES = (
    "PricingProviderQualityFeature",
    "PricingProviderQualityProcedureLSH",
    "PricingProviderQualityPeerTarget",
)
MAT_PHASE_1_BUILD_FEATURES = "phase_1_build_features"
MAT_PHASE_2_BUILD_LSH_SHARDED = "phase_2_build_lsh_sharded"
MAT_PHASE_3_UPDATE_PROCEDURE_BUCKET = "phase_3_update_procedure_bucket"
MAT_PHASE_4_BUILD_PEER_TARGETS = "phase_4_build_peer_targets"
MAT_PHASE_5_BUILD_MEASURE_SHARDED = "phase_5_build_measure_sharded"
MAT_PHASE_6_BUILD_DOMAIN_SHARDED = "phase_6_build_domain_sharded"
MAT_PHASE_7_BUILD_SCORE_SHARDED = "phase_7_build_score_sharded"
MAT_PHASE_DONE = "done"
MAT_PHASE_SEQUENCE = (
    MAT_PHASE_1_BUILD_FEATURES,
    MAT_PHASE_2_BUILD_LSH_SHARDED,
    MAT_PHASE_3_UPDATE_PROCEDURE_BUCKET,
    MAT_PHASE_4_BUILD_PEER_TARGETS,
    MAT_PHASE_5_BUILD_MEASURE_SHARDED,
    MAT_PHASE_6_BUILD_DOMAIN_SHARDED,
    MAT_PHASE_7_BUILD_SCORE_SHARDED,
    MAT_PHASE_DONE,
)


def _resolve_optional_model(name: str) -> type | None:
    return getattr(db_models, name, None)


PricingProviderQualityFeature = _resolve_optional_model("PricingProviderQualityFeature")
PricingProviderQualityProcedureLSH = _resolve_optional_model("PricingProviderQualityProcedureLSH")
PricingProviderQualityPeerTarget = _resolve_optional_model("PricingProviderQualityPeerTarget")


def _benchmark_modes_for_materialization(mode: str) -> tuple[str, ...]:
    normalized = str(mode or "").strip().lower()
    if normalized == "zip":
        return ("zip", "state", "national")
    if normalized == "state":
        return ("state", "national")
    return ("national",)


@dataclass(frozen=True)
class DatasetConfig:
    key: str
    row_limit_test: int
    header: tuple[str, ...]


DATASETS = (
    DatasetConfig(
        "qpp_provider",
        PROVIDER_QUALITY_TEST_QPP_ROWS,
        ("npi", "year", "quality_score", "cost_score", "final_score"),
    ),
    DatasetConfig(
        "svi_zcta",
        PROVIDER_QUALITY_TEST_SVI_ROWS,
        (
            "zcta",
            "year",
            "svi_overall",
            "svi_socioeconomic",
            "svi_household",
            "svi_minority",
            "svi_housing",
        ),
    ),
)
DATASET_BY_KEY = {dataset.key: dataset for dataset in DATASETS}


def _normalize_run_id(run_id: str | None) -> str:
    if run_id:
        normalized = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in str(run_id))
        normalized = normalized.strip("_")
        if normalized:
            return normalized
    token = secrets.token_hex(4)
    return f"{datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S')}_{token}"


def _normalize_import_id(import_id: str | None) -> str:
    if not import_id:
        return datetime.date.today().strftime("%Y%m%d")
    normalized = "".join(ch if ch.isalnum() else "_" for ch in str(import_id))
    return normalized or datetime.date.today().strftime("%Y%m%d")


def _state_key(run_id: str, suffix: str) -> str:
    return f"provider_quality:{run_id}:{suffix}"


def _mat_phase_key(run_id: str) -> str:
    return _state_key(run_id, "mat_phase")


def _mat_total_key(run_id: str) -> str:
    return _state_key(run_id, "mat_total")


def _mat_done_key(run_id: str) -> str:
    return _state_key(run_id, "mat_done")


def _mat_failed_key(run_id: str) -> str:
    return _state_key(run_id, "mat_failed")


def _mat_phase_started_at_key(run_id: str) -> str:
    return _state_key(run_id, "mat_phase_started_at")


def _mat_phase_duration_key(run_id: str, phase: str) -> str:
    return _state_key(run_id, f"mat_durations:{phase}")


def _should_fail_on_source_error(test_mode: bool) -> bool:
    if test_mode and PROVIDER_QUALITY_ALLOW_DEGRADED_TEST_ONLY:
        return False
    return PROVIDER_QUALITY_FAIL_ON_SOURCE_ERROR


def _decode_redis_str(raw: Any) -> str:
    if isinstance(raw, bytes):
        return raw.decode("utf-8")
    return str(raw or "")


async def _reset_materialize_state(redis, run_id: str) -> None:
    duration_keys = [
        _mat_phase_duration_key(run_id, phase)
        for phase in MAT_PHASE_SEQUENCE
    ]
    await redis.delete(
        _mat_phase_key(run_id),
        _mat_total_key(run_id),
        _mat_done_key(run_id),
        _mat_failed_key(run_id),
        _mat_phase_started_at_key(run_id),
        *duration_keys,
    )


async def _set_materialize_phase(redis, run_id: str, phase: str, total: int = 0) -> None:
    await redis.set(_mat_phase_key(run_id), phase, ex=PROVIDER_QUALITY_REDIS_TTL_SECONDS)
    await redis.set(_mat_total_key(run_id), int(total), ex=PROVIDER_QUALITY_REDIS_TTL_SECONDS)
    await redis.set(_mat_done_key(run_id), 0, ex=PROVIDER_QUALITY_REDIS_TTL_SECONDS)
    await redis.set(_mat_failed_key(run_id), 0, ex=PROVIDER_QUALITY_REDIS_TTL_SECONDS)
    await redis.set(
        _mat_phase_started_at_key(run_id),
        f"{time.time():.6f}",
        ex=PROVIDER_QUALITY_REDIS_TTL_SECONDS,
    )
    await redis.delete(_mat_phase_duration_key(run_id, phase))


async def _get_materialize_phase(redis, run_id: str) -> str:
    return _decode_redis_str(await redis.get(_mat_phase_key(run_id)))


async def _get_materialize_progress(redis, run_id: str) -> tuple[int, int, int]:
    total = _safe_int(await redis.get(_mat_total_key(run_id)), 0)
    done = _safe_int(await redis.get(_mat_done_key(run_id)), 0)
    failed = _safe_int(await redis.get(_mat_failed_key(run_id)), 0)
    return total, done, failed


def _format_duration_compact(total_seconds: float) -> str:
    seconds = max(int(total_seconds), 0)
    hours, remainder = divmod(seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours > 0:
        return f"{hours}h{minutes:02d}m{secs:02d}s"
    if minutes > 0:
        return f"{minutes}m{secs:02d}s"
    return f"{secs}s"


async def _get_materialize_phase_elapsed_seconds(redis, run_id: str) -> float:
    raw_started = await redis.get(_mat_phase_started_at_key(run_id))
    try:
        started_at = float(_decode_redis_str(raw_started))
    except (TypeError, ValueError):
        return 0.0
    if started_at <= 0:
        return 0.0
    return max(0.0, time.time() - started_at)


async def _mark_materialize_done(redis, run_id: str) -> None:
    await redis.incrby(_mat_done_key(run_id), 1)
    await redis.expire(_mat_done_key(run_id), PROVIDER_QUALITY_REDIS_TTL_SECONDS)


async def _mark_materialize_failed(redis, run_id: str) -> None:
    await redis.incrby(_mat_failed_key(run_id), 1)
    await redis.expire(_mat_failed_key(run_id), PROVIDER_QUALITY_REDIS_TTL_SECONDS)


async def _record_materialize_phase_duration(redis, run_id: str, phase: str, duration_sec: float) -> None:
    key = _mat_phase_duration_key(run_id, phase)
    await redis.rpush(key, f"{max(duration_sec, 0.0):.6f}")
    await redis.expire(key, PROVIDER_QUALITY_REDIS_TTL_SECONDS)


async def _log_materialize_phase_summary(redis, run_id: str, phase: str, *, total: int, done: int, failed: int) -> None:
    key = _mat_phase_duration_key(run_id, phase)
    raw_values = await redis.lrange(key, 0, -1)
    durations: list[float] = []
    for raw in raw_values:
        try:
            durations.append(float(_decode_redis_str(raw)))
        except (TypeError, ValueError):
            continue

    if durations:
        logger.info(
            "provider quality materialize phase complete run_id=%s phase=%s total=%s done=%s failed=%s shard_sec_min=%.3f shard_sec_median=%.3f shard_sec_max=%.3f",
            run_id,
            phase,
            total,
            done,
            failed,
            min(durations),
            statistics.median(durations),
            max(durations),
        )
        return

    logger.info(
        "provider quality materialize phase complete run_id=%s phase=%s total=%s done=%s failed=%s",
        run_id,
        phase,
        total,
        done,
        failed,
    )


def _materialize_reporting_years(manifest: dict[str, Any]) -> tuple[int, ...]:
    years: set[int] = set()
    sources = manifest.get("sources")
    if isinstance(sources, dict):
        for dataset_sources in sources.values():
            if not isinstance(dataset_sources, list):
                continue
            for source in dataset_sources:
                if not isinstance(source, dict):
                    continue
                year = _safe_int(source.get("reporting_year"), 0)
                if year > 0:
                    years.add(year)

    manifest_year = _safe_int(manifest.get("year"), 0)
    if manifest_year > 0:
        years.add(manifest_year)

    if not years:
        years.update(PROVIDER_QUALITY_YEAR_WINDOW)

    bounded = sorted(
        year
        for year in years
        if PROVIDER_QUALITY_MIN_YEAR <= year <= PROVIDER_QUALITY_MAX_YEAR
    )
    if bounded:
        return tuple(bounded)
    return (PROVIDER_QUALITY_MAX_YEAR,)


def _materialize_shard_job_id(run_id: str, phase: str, year: int, shard_id: int) -> str:
    return f"provider_quality_materialize_{phase}_{run_id}_{year}_{shard_id}"


def _npi_shard_predicate(expr: str) -> str:
    return f"MOD(({expr})::bigint, :shard_count) = :shard_id"


def _archived_identifier(name: str, suffix: str = "_old") -> str:
    candidate = f"{name}{suffix}"
    if len(candidate) <= POSTGRES_IDENTIFIER_MAX_LENGTH:
        return candidate
    digest = hashlib.sha1(name.encode("utf-8")).hexdigest()[:8]
    trim_to = max(1, POSTGRES_IDENTIFIER_MAX_LENGTH - len(suffix) - len(digest) - 1)
    return f"{name[:trim_to]}_{digest}{suffix}"


def _run_dir(import_id: str, run_id: str) -> Path:
    return Path(PROVIDER_QUALITY_WORKDIR) / import_id / run_id


def _manifest_path(work_dir: Path) -> Path:
    return work_dir / "manifest.json"


def _read_manifest(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def _write_manifest(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=True, indent=2, sort_keys=True)


def _cohort_model_classes() -> tuple[type, ...]:
    classes: list[type] = []
    for cls in (
        PricingProviderQualityFeature,
        PricingProviderQualityProcedureLSH,
        PricingProviderQualityPeerTarget,
    ):
        if cls is not None:
            classes.append(cls)
    return tuple(classes)


def _cohort_models_present(classes: dict[str, type]) -> bool:
    return all(name in classes for name in COHORT_MODEL_CLASS_NAMES)


def _model_columns(model: type | None) -> set[str]:
    if model is None or not hasattr(model, "__table__"):
        return set()
    return {column.name for column in model.__table__.columns}


def _first_existing_column(columns: set[str], *candidates: str) -> str | None:
    for candidate in candidates:
        if candidate in columns:
            return candidate
    return None


def _optional_column_pairs(
    available_columns: set[str],
    mapping: tuple[tuple[str, str], ...],
) -> list[tuple[str, str]]:
    used: set[str] = set()
    pairs: list[tuple[str, str]] = []
    for column_name, sql_expr in mapping:
        if column_name in available_columns and column_name not in used:
            pairs.append((column_name, sql_expr))
            used.add(column_name)
    return pairs


def _staging_classes(stage_suffix: str, schema: str) -> dict[str, type]:
    return {
        cls.__name__: make_class(cls, stage_suffix, schema_override=schema)
        for cls in (
            PricingQppProvider,
            PricingSviZcta,
            PricingProviderQualityMeasure,
            PricingProviderQualityDomain,
            PricingProviderQualityScore,
            *_cohort_model_classes(),
        )
    }


def _chunk_job_id(run_id: str, dataset_key: str, source_index: int, reporting_year: int, chunk_index: int) -> str:
    return f"provider_quality_chunk_{run_id}_{dataset_key}_{reporting_year}_{source_index}_{chunk_index}"


def _safe_int(raw: Any, default: int = 0) -> int:
    if raw is None:
        return default
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


async def _init_run_state(redis, run_id: str, total_chunks: int) -> None:
    total_key = _state_key(run_id, "total_chunks")
    done_key = _state_key(run_id, "done_chunks")
    lock_key = _state_key(run_id, "finalize_lock")
    finalized_key = _state_key(run_id, "finalized")
    duration_keys = [_mat_phase_duration_key(run_id, phase) for phase in MAT_PHASE_SEQUENCE]

    await redis.delete(
        total_key,
        done_key,
        lock_key,
        finalized_key,
        _mat_phase_key(run_id),
        _mat_total_key(run_id),
        _mat_done_key(run_id),
        _mat_failed_key(run_id),
        *duration_keys,
    )
    await redis.set(total_key, str(total_chunks))
    await redis.expire(total_key, PROVIDER_QUALITY_REDIS_TTL_SECONDS)
    await redis.sadd(done_key, "__init__")
    await redis.srem(done_key, "__init__")
    await redis.expire(done_key, PROVIDER_QUALITY_REDIS_TTL_SECONDS)


async def _increment_total_chunks(redis, run_id: str, delta: int) -> None:
    if delta <= 0:
        return
    total_key = _state_key(run_id, "total_chunks")
    await redis.incrby(total_key, int(delta))
    await redis.expire(total_key, PROVIDER_QUALITY_REDIS_TTL_SECONDS)


async def _mark_chunk_done(redis, run_id: str, chunk_id: str) -> None:
    done_key = _state_key(run_id, "done_chunks")
    await redis.sadd(done_key, chunk_id)
    await redis.expire(done_key, PROVIDER_QUALITY_REDIS_TTL_SECONDS)


async def _mark_chunk_done_with_retry(redis, run_id: str, chunk_id: str) -> None:
    last_exc: Exception | None = None
    for attempt in range(1, PROVIDER_QUALITY_MARK_DONE_RETRIES + 1):
        try:
            await _mark_chunk_done(redis, run_id, chunk_id)
            return
        except Exception as exc:  # pylint: disable=broad-exception-caught
            last_exc = exc
            if attempt >= PROVIDER_QUALITY_MARK_DONE_RETRIES:
                break
            delay = min(
                PROVIDER_QUALITY_MARK_DONE_RETRY_BASE_SECONDS * (2 ** (attempt - 1)),
                PROVIDER_QUALITY_MARK_DONE_RETRY_MAX_SECONDS,
            )
            logger.warning(
                "Retrying mark_chunk_done for run_id=%s chunk_id=%s after error (%s/%s): %r",
                run_id,
                chunk_id,
                attempt,
                PROVIDER_QUALITY_MARK_DONE_RETRIES,
                exc,
            )
            await asyncio.sleep(delay)
    if last_exc is not None:
        raise last_exc


async def _get_run_progress(redis, run_id: str, expected_default: int) -> tuple[int, int]:
    total_key = _state_key(run_id, "total_chunks")
    done_key = _state_key(run_id, "done_chunks")
    total_chunks = _safe_int(await redis.get(total_key), expected_default)
    done_chunks = _safe_int(await redis.scard(done_key), 0)
    return total_chunks, done_chunks


async def _claim_finalize_lock(redis, run_id: str) -> bool:
    lock_key = _state_key(run_id, "finalize_lock")
    lock_set = await redis.set(lock_key, run_id, ex=PROVIDER_QUALITY_REDIS_TTL_SECONDS, nx=True)
    if lock_set:
        return True
    return bool(await redis.get(lock_key))


async def _claim_global_finalize_lock(redis, run_id: str) -> bool:
    lock_set = await redis.set(
        PROVIDER_QUALITY_GLOBAL_FINALIZE_LOCK_KEY,
        run_id,
        ex=PROVIDER_QUALITY_GLOBAL_FINALIZE_LOCK_TTL_SECONDS,
        nx=True,
    )
    if lock_set:
        return True
    current_owner = _decode_redis_str(await redis.get(PROVIDER_QUALITY_GLOBAL_FINALIZE_LOCK_KEY))
    return current_owner == run_id


async def _release_global_finalize_lock(redis, run_id: str) -> None:
    current_owner = _decode_redis_str(await redis.get(PROVIDER_QUALITY_GLOBAL_FINALIZE_LOCK_KEY))
    if current_owner == run_id:
        await redis.delete(PROVIDER_QUALITY_GLOBAL_FINALIZE_LOCK_KEY)


def _print_row_progress(stage: str, parsed: int, accepted: int, start_time: float, final: bool = False) -> None:
    elapsed = max(time.monotonic() - start_time, 0.001)
    rate = parsed / elapsed
    line = f"\r[rows:{stage}] parsed={parsed:,} accepted={accepted:,} rate={rate:,.0f} rows/s"
    print(line, end="\n" if final else "", flush=True)


def _step_start(label: str) -> float:
    start = time.monotonic()
    print(f"[step] START {label}", flush=True)
    return start


def _step_end(label: str, started_at: float) -> None:
    elapsed = max(time.monotonic() - started_at, 0.001)
    print(f"[step] DONE  {label} in {elapsed:.1f}s", flush=True)


def _is_deadlock_error(exc: BaseException) -> bool:
    return "deadlock detected" in str(exc).lower()


async def _execute_shard_sql(sql: str, **params: Any) -> None:
    async with db.transaction():
        await db.status(f"SET LOCAL work_mem = '{PROVIDER_QUALITY_SHARD_WORK_MEM}';")
        await db.status(f"SET LOCAL jit = {PROVIDER_QUALITY_SHARD_JIT};")
        await db.status(
            f"SET LOCAL max_parallel_workers_per_gather = {PROVIDER_QUALITY_SHARD_MAX_PARALLEL_GATHER};"
        )
        await db.status(sql, **params)


async def _count_shard_rows(schema: str, table_name: str, *, year: int, shard_id: int, shard_count: int) -> int:
    stmt = db.text(
        f"""
        SELECT COUNT(*)::bigint AS row_count
        FROM {schema}.{table_name} t
        WHERE t.year = :year
          AND {_npi_shard_predicate("t.npi")}
        """
    )
    async with db.session() as session:
        result = await session.execute(
            stmt,
            {
                "year": year,
                "shard_id": shard_id,
                "shard_count": shard_count,
            },
        )
        return int(result.scalar() or 0)


async def _push_objects_with_retry(
    rows: list[dict[str, Any]],
    cls: type,
    *,
    rewrite: bool = False,
    use_copy: bool = True,
) -> None:
    if not rows:
        return
    for attempt in range(1, PROVIDER_QUALITY_DB_DEADLOCK_RETRIES + 1):
        try:
            await push_objects(rows, cls, rewrite=rewrite, use_copy=use_copy)
            return
        except Exception as exc:  # pylint: disable=broad-exception-caught
            if not _is_deadlock_error(exc) or attempt >= PROVIDER_QUALITY_DB_DEADLOCK_RETRIES:
                raise
            delay = PROVIDER_QUALITY_DB_DEADLOCK_BASE_DELAY_SECONDS * (2 ** (attempt - 1))
            logger.warning(
                "Deadlock while inserting into %s (attempt %s/%s), retrying in %.2fs",
                cls.__tablename__,
                attempt,
                PROVIDER_QUALITY_DB_DEADLOCK_RETRIES,
                delay,
            )
            await asyncio.sleep(delay)


def _build_stage_suffix(import_id: str, run_id: str) -> str:
    base = "".join(ch if ch.isalnum() else "_" for ch in import_id).strip("_")[:12] or "import"
    checksum = return_checksum([import_id, run_id]) & 0xFFFFFFFF
    return f"{base}_{checksum:08x}"


def _row_allowed_for_test(row_number: int) -> bool:
    return row_number % 11 == 0


async def _ensure_indexes(obj: type, db_schema: str) -> None:
    if hasattr(obj, "__my_index_elements__") and obj.__my_index_elements__:
        cols = ", ".join(obj.__my_index_elements__)
        await db.status(
            "CREATE UNIQUE INDEX IF NOT EXISTS "
            + f"{obj.__tablename__}_idx_primary ON {db_schema}.{obj.__tablename__} ({cols});"
        )
    if hasattr(obj, "__my_additional_indexes__") and obj.__my_additional_indexes__:
        for idx in obj.__my_additional_indexes__:
            elements = idx.get("index_elements")
            if not elements:
                continue
            base_name = idx.get("name") or f"{obj.__tablename__}_{'_'.join(elements)}_idx"
            if getattr(obj, "__main_table__", obj.__tablename__) != obj.__tablename__:
                name = f"{obj.__tablename__}_{base_name}"
            else:
                name = base_name
            using = idx.get("using")
            where = idx.get("where")
            cols = ", ".join(elements)
            statement = f"CREATE INDEX IF NOT EXISTS {name} ON {db_schema}.{obj.__tablename__}"
            if using:
                statement += f" USING {using}"
            statement += f" ({cols})"
            if where:
                statement += f" WHERE {where}"
            statement += ";"
            await db.status(statement)


async def _prepare_tables(stage_suffix: str, test_mode: bool) -> tuple[dict[str, type], str]:
    db_schema = get_import_schema("HLTHPRT_DB_SCHEMA", "mrf", test_mode)
    await db.status(f"CREATE SCHEMA IF NOT EXISTS {db_schema};")
    dynamic: dict[str, type] = {}
    staging_models = (
        PricingQppProvider,
        PricingSviZcta,
        PricingProviderQualityMeasure,
        PricingProviderQualityDomain,
        PricingProviderQualityScore,
        *_cohort_model_classes(),
    )

    for cls in staging_models:
        obj = make_class(cls, stage_suffix, schema_override=db_schema)
        dynamic[cls.__name__] = obj
        await db.status(f"DROP TABLE IF EXISTS {db_schema}.{obj.__tablename__};")
        await db.status(f"DROP TYPE IF EXISTS {db_schema}.{obj.__tablename__} CASCADE;")
        await db.create_table(obj.__table__, checkfirst=True)
        if not PROVIDER_QUALITY_DEFER_STAGE_INDEXES:
            await _ensure_indexes(obj, db_schema)

    for cls in (*staging_models, PricingQualityRun):
        await db.create_table(cls.__table__, checkfirst=True)

    return dynamic, db_schema


async def _build_staging_indexes(classes: dict[str, type], schema: str) -> None:
    for model in classes.values():
        await _ensure_indexes(model, schema)


async def _table_exists(schema: str, table: str) -> bool:
    table_ref = f"{schema}.{table}"
    result = await db.scalar("SELECT to_regclass(:table_ref)", table_ref=table_ref)
    return result is not None


async def _download_csv_head(url: str, path: str, max_bytes: int) -> None:
    client = await get_http_client()
    downloaded = 0
    async with client:
        async with client.get(
            url,
            timeout=aiohttp.ClientTimeout(total=600, connect=60, sock_read=600),
        ) as response:
            response.raise_for_status()
            async with async_open(path, "wb+") as afp:
                async for chunk in response.content.iter_chunked(1024 * 1024):
                    if not chunk:
                        break
                    remaining = max_bytes - downloaded
                    if remaining <= 0:
                        break
                    data = chunk if len(chunk) <= remaining else chunk[:remaining]
                    await afp.write(data)
                    downloaded += len(data)
                    if downloaded >= max_bytes:
                        break


def _qpp_url_for_year(year: int) -> str:
    template = QPP_CSV_URL_TEMPLATE
    if not template:
        return ""
    if "{year}" in template:
        return template.format(year=year)
    return template


def _svi_url_for_year(year: int) -> str:
    template = SVI_CSV_URL_TEMPLATE
    if not template:
        return ""
    if "{year}" in template:
        return template.format(year=year)
    return template


def _resolve_sources(test_mode: bool = False) -> dict[str, list[dict[str, Any]]]:
    years = list(PROVIDER_QUALITY_YEAR_WINDOW)
    if test_mode and years:
        years = [max(years)]

    resolved: dict[str, list[dict[str, Any]]] = {"qpp_provider": [], "svi_zcta": []}
    for year in years:
        qpp_url = _qpp_url_for_year(year)
        if qpp_url:
            resolved["qpp_provider"].append(
                {
                    "url": qpp_url,
                    "reporting_year": year,
                    "dataset_title": "QPP Provider Performance",
                }
            )
        svi_url = _svi_url_for_year(year)
        if svi_url:
            resolved["svi_zcta"].append(
                {
                    "url": svi_url,
                    "reporting_year": year,
                    "dataset_title": "CDC SVI ZCTA",
                }
            )
    return resolved


async def _create_empty_source_file(dataset_key: str, temp_dir: str, reporting_year: int) -> str:
    Path(temp_dir).mkdir(parents=True, exist_ok=True)
    filename = f"{dataset_key}_{reporting_year}_empty.csv"
    path = str(Path(temp_dir) / filename)
    dataset = DATASET_BY_KEY[dataset_key]
    with open(path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(dataset.header)
    return path


async def _download_source_file(
    dataset_key: str,
    source: dict[str, Any],
    temp_dir: str,
    test_mode: bool,
) -> tuple[str, bool]:
    reporting_year = max(_safe_int(source.get("reporting_year"), PROVIDER_QUALITY_MIN_YEAR), 2013)
    Path(temp_dir).mkdir(parents=True, exist_ok=True)
    filename = f"{dataset_key}_{reporting_year}.csv"
    path = str(Path(temp_dir) / filename)

    url = str(source.get("url") or "").strip()
    if not url:
        msg = (
            "provider-quality source URL is missing: "
            f"dataset={dataset_key} year={reporting_year}"
        )
        if _should_fail_on_source_error(test_mode):
            raise RuntimeError(msg)
        logger.warning("%s; using empty fallback in degraded test mode", msg)
        return await _create_empty_source_file(dataset_key, temp_dir, reporting_year), True

    for attempt in range(1, DOWNLOAD_RETRIES + 1):
        try:
            if test_mode:
                await _download_csv_head(url, path, TEST_MAX_DOWNLOAD_BYTES)
            else:
                await download_it_and_save(url, path)
            return path, False
        except Retry:
            if attempt >= DOWNLOAD_RETRIES:
                break
            await asyncio.sleep(min(5 * attempt, 20))
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.warning(
                "provider-quality download failed (%s/%s) for %s: %r",
                attempt,
                DOWNLOAD_RETRIES,
                url,
                exc,
            )
            if attempt >= DOWNLOAD_RETRIES:
                break
            await asyncio.sleep(min(5 * attempt, 20))

    msg = (
        "provider-quality source download failed after retries: "
        f"dataset={dataset_key} year={reporting_year} url={url}"
    )
    if _should_fail_on_source_error(test_mode):
        raise RuntimeError(msg)
    logger.warning("%s; using empty fallback in degraded test mode", msg)
    return await _create_empty_source_file(dataset_key, temp_dir, reporting_year), True


async def _split_source_into_chunks(
    dataset_key: str,
    source_path: str,
    chunks_dir: Path,
    test_mode: bool,
) -> list[dict[str, Any]]:
    chunks_dir.mkdir(parents=True, exist_ok=True)
    dataset = DATASET_BY_KEY.get(dataset_key)
    row_limit = dataset.row_limit_test if dataset else 5000

    chunks: list[dict[str, Any]] = []
    chunk_rows: list[str] = []
    chunk_bytes = 0
    chunk_index = 0

    with open(source_path, "r", encoding="utf-8-sig", newline="") as src:
        header = src.readline()
        if not header:
            return []
        row_number = 0
        accepted = 0

        def flush_chunk() -> None:
            nonlocal chunk_rows, chunk_bytes, chunk_index
            if not chunk_rows:
                return
            chunk_path = chunks_dir / f"chunk_{chunk_index:05d}.csv"
            with open(chunk_path, "w", encoding="utf-8", newline="") as out:
                out.write(header)
                out.writelines(chunk_rows)
            chunks.append(
                {
                    "dataset_key": dataset_key,
                    "chunk_index": chunk_index,
                    "chunk_path": str(chunk_path),
                }
            )
            chunk_rows = []
            chunk_bytes = 0
            chunk_index += 1

        for line in src:
            row_number += 1
            if test_mode:
                if not _row_allowed_for_test(row_number):
                    continue
                if accepted >= row_limit:
                    break

            chunk_rows.append(line)
            chunk_bytes += len(line.encode("utf-8", errors="ignore"))
            accepted += 1
            if chunk_bytes >= PROVIDER_QUALITY_CHUNK_TARGET_BYTES:
                flush_chunk()

        flush_chunk()

    return chunks


def _to_float(value: Any) -> float | None:
    if value in (None, "", "*", "NA"):
        return None
    try:
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return None


def _to_int(value: Any) -> int | None:
    if value in (None, "", "*", "NA"):
        return None
    try:
        return int(float(str(value).replace(",", "")))
    except (TypeError, ValueError):
        return None


def _to_npi(value: Any) -> int | None:
    if value in (None, "", "*", "NA"):
        return None
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    if text.endswith(".0"):
        text = text[:-2]
    if not text.isdigit():
        return None
    npi = int(text)
    if npi <= 0 or npi > MAX_NPI:
        return None
    return npi


def _normalize_zcta(value: Any) -> str | None:
    if value in (None, "", "*", "NA"):
        return None
    text = str(value).strip()
    if not text:
        return None
    # Prefer a standalone 5-digit token (e.g. "01001", "ZCTA5 01001", "01001.0").
    zcta_tokens = re.findall(r"(?<!\d)(\d{5})(?!\d)", text)
    if zcta_tokens:
        return zcta_tokens[-1]
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) == 5:
        return digits
    # CDC LOCATION can look like "ZCTA5 01001" => digits "501001".
    if len(digits) == 6 and digits.startswith("5"):
        return digits[1:]
    if len(digits) > 5:
        return digits[-5:]
    return None


def _pick_first(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row:
            return row.get(key)
    return None


async def _load_qpp_rows(path: str, qpp_cls: type, reporting_year: int, test_mode: bool) -> None:
    rows: list[dict[str, Any]] = []
    accepted = 0
    progress_start = time.monotonic()
    progress_last = progress_start

    async with async_open(path, "r", encoding="utf-8-sig") as handle:
        reader = AsyncDictReader(handle)
        row_number = 0
        async for row in reader:
            row_number += 1
            now = time.monotonic()
            if now - progress_last >= ROW_PROGRESS_INTERVAL_SECONDS:
                _print_row_progress("qpp_provider", row_number, accepted, progress_start)
                progress_last = now

            npi = _to_npi(_pick_first(row, "npi", "NPI", "clinician_npi", "Clinician NPI"))
            if npi is None:
                continue
            year = _to_int(_pick_first(row, "year", "Year", "performance_year", "Performance Year"))
            year = max(year or reporting_year, 2013)

            quality_score = _to_float(_pick_first(row, "quality_score", "Quality Score", "quality", "quality_percent"))
            cost_score = _to_float(_pick_first(row, "cost_score", "Cost Score", "cost", "cost_percent"))
            final_score = _to_float(_pick_first(row, "final_score", "Final Score", "final", "final_percent"))

            rows.append(
                {
                    "npi": npi,
                    "year": year,
                    "quality_score": quality_score,
                    "cost_score": cost_score,
                    "final_score": final_score,
                    "raw_json_text": json.dumps(row, ensure_ascii=True),
                    "updated_at": datetime.datetime.utcnow(),
                }
            )
            accepted += 1
            if len(rows) >= IMPORT_BATCH_SIZE:
                deduped = {(
                    item.get("npi"),
                    item.get("year"),
                ): item for item in rows}
                await _push_objects_with_retry(list(deduped.values()), qpp_cls)
                rows.clear()

            if test_mode and accepted >= PROVIDER_QUALITY_TEST_QPP_ROWS:
                break

    if rows:
        deduped = {(item.get("npi"), item.get("year")): item for item in rows}
        await _push_objects_with_retry(list(deduped.values()), qpp_cls)
    _print_row_progress("qpp_provider", row_number, accepted, progress_start, final=True)


async def _load_svi_rows(path: str, svi_cls: type, reporting_year: int, test_mode: bool) -> None:
    rows: list[dict[str, Any]] = []
    accepted = 0
    skipped_missing_zcta = 0
    header_keys: tuple[str, ...] = ()
    missing_zcta_examples: list[str] = []
    progress_start = time.monotonic()
    progress_last = progress_start

    async with async_open(path, "r", encoding="utf-8-sig") as handle:
        reader = AsyncDictReader(handle)
        row_number = 0
        async for row in reader:
            row_number += 1
            if row_number == 1:
                header_keys = tuple(str(key) for key in row.keys())
            now = time.monotonic()
            if now - progress_last >= ROW_PROGRESS_INTERVAL_SECONDS:
                _print_row_progress("svi_zcta", row_number, accepted, progress_start)
                progress_last = now

            zcta = _normalize_zcta(
                _pick_first(
                    row,
                    "zcta",
                    "ZCTA",
                    "ZCTA5",
                    "ZIP",
                    "zip",
                    "FIPS",
                    "fips",
                    "LOCATION",
                    "location",
                )
            )
            if not zcta:
                skipped_missing_zcta += 1
                if len(missing_zcta_examples) < 3:
                    sample = {
                        "FIPS": row.get("FIPS"),
                        "LOCATION": row.get("LOCATION"),
                        "ZCTA": row.get("ZCTA"),
                        "ZCTA5": row.get("ZCTA5"),
                        "ZIP": row.get("ZIP"),
                    }
                    compact_sample = {key: value for key, value in sample.items() if value not in (None, "")}
                    if not compact_sample:
                        compact_sample = {"keys": list(row.keys())[:6]}
                    missing_zcta_examples.append(json.dumps(compact_sample, ensure_ascii=True)[:220])
                continue
            year = _to_int(_pick_first(row, "year", "Year"))
            year = max(year or reporting_year, 2013)

            rows.append(
                {
                    "zcta": zcta,
                    "year": year,
                    "svi_overall": _to_float(_pick_first(row, "svi_overall", "RPL_THEMES", "SVI Overall")),
                    "svi_socioeconomic": _to_float(_pick_first(row, "svi_socioeconomic", "RPL_THEME1", "SVI Theme 1")),
                    "svi_household": _to_float(_pick_first(row, "svi_household", "RPL_THEME2", "SVI Theme 2")),
                    "svi_minority": _to_float(_pick_first(row, "svi_minority", "RPL_THEME3", "SVI Theme 3")),
                    "svi_housing": _to_float(_pick_first(row, "svi_housing", "RPL_THEME4", "SVI Theme 4")),
                    "updated_at": datetime.datetime.utcnow(),
                }
            )
            accepted += 1
            if len(rows) >= IMPORT_BATCH_SIZE:
                deduped = {(item.get("zcta"), item.get("year")): item for item in rows}
                await _push_objects_with_retry(list(deduped.values()), svi_cls)
                rows.clear()

            if test_mode and accepted >= PROVIDER_QUALITY_TEST_SVI_ROWS:
                break

    if rows:
        deduped = {(item.get("zcta"), item.get("year")): item for item in rows}
        await _push_objects_with_retry(list(deduped.values()), svi_cls)
    _print_row_progress("svi_zcta", row_number, accepted, progress_start, final=True)
    logger.info(
        "SVI ingest summary: path=%s year=%s parsed=%s accepted=%s skipped_missing_zcta=%s",
        path,
        reporting_year,
        row_number,
        accepted,
        skipped_missing_zcta,
    )
    if accepted == 0:
        logger.warning(
            "SVI ingest accepted=0: path=%s year=%s header_keys=%s missing_zcta_examples=%s",
            path,
            reporting_year,
            ",".join(header_keys[:12]),
            " | ".join(missing_zcta_examples) if missing_zcta_examples else "none",
        )


async def _build_cohort_materialization_context(classes: dict[str, type], schema: str) -> dict[str, Any]:
    qpp_table = classes["PricingQppProvider"].__tablename__
    svi_table = classes["PricingSviZcta"].__tablename__
    measure_table = classes["PricingProviderQualityMeasure"].__tablename__
    domain_table = classes["PricingProviderQualityDomain"].__tablename__
    score_table = classes["PricingProviderQualityScore"].__tablename__
    feature_table = classes["PricingProviderQualityFeature"].__tablename__
    lsh_table = classes["PricingProviderQualityProcedureLSH"].__tablename__
    peer_target_table = classes["PricingProviderQualityPeerTarget"].__tablename__
    rx_table = PricingProviderPrescription.__tablename__
    rx_table_exists = await _table_exists(schema, rx_table)

    feature_columns = _model_columns(classes.get("PricingProviderQualityFeature"))
    lsh_columns = _model_columns(classes.get("PricingProviderQualityProcedureLSH"))
    peer_target_columns = _model_columns(classes.get("PricingProviderQualityPeerTarget"))
    measure_columns = _model_columns(classes.get("PricingProviderQualityMeasure"))
    score_columns = _model_columns(classes.get("PricingProviderQualityScore"))

    feature_npi_col = _first_existing_column(feature_columns, "npi")
    feature_year_col = _first_existing_column(feature_columns, "year")
    if not feature_npi_col or not feature_year_col:
        raise RuntimeError("cohort feature model must expose npi/year columns")

    lsh_npi_col = _first_existing_column(lsh_columns, "npi")
    lsh_year_col = _first_existing_column(lsh_columns, "year")
    lsh_band_no_col = _first_existing_column(lsh_columns, "band_no", "band_index")
    lsh_band_hash_col = _first_existing_column(lsh_columns, "band_hash", "hash_value")
    if not lsh_npi_col or not lsh_year_col or not lsh_band_no_col or not lsh_band_hash_col:
        raise RuntimeError("cohort LSH model must expose npi/year/band_no/band_hash columns")

    peer_target_year_col = _first_existing_column(peer_target_columns, "year")
    peer_target_benchmark_mode_col = _first_existing_column(peer_target_columns, "benchmark_mode")
    peer_target_geo_scope_col = _first_existing_column(peer_target_columns, "geography_scope", "geo_scope")
    peer_target_geo_value_col = _first_existing_column(peer_target_columns, "geography_value", "geo_value")
    peer_target_cohort_level_col = _first_existing_column(peer_target_columns, "cohort_level", "cohort_tier")
    peer_target_peer_n_col = _first_existing_column(peer_target_columns, "peer_n", "peer_count", "provider_count")
    peer_target_specialty_col = _first_existing_column(peer_target_columns, "specialty", "specialty_key")
    peer_target_taxonomy_col = _first_existing_column(peer_target_columns, "taxonomy", "taxonomy_code")
    peer_target_classification_col = _first_existing_column(peer_target_columns, "classification")
    peer_target_procedure_bucket_col = _first_existing_column(peer_target_columns, "procedure_bucket")
    peer_target_appropr_col = _first_existing_column(peer_target_columns, "target_appropriateness")
    peer_target_cost_col = _first_existing_column(peer_target_columns, "target_cost")
    peer_target_effect_col = _first_existing_column(peer_target_columns, "target_effectiveness")
    peer_target_qpp_cost_col = _first_existing_column(peer_target_columns, "target_qpp_cost")
    peer_target_rx_appropr_col = _first_existing_column(peer_target_columns, "target_rx_appropriateness")
    if (
        not peer_target_year_col
        or not peer_target_geo_scope_col
        or not peer_target_geo_value_col
        or not peer_target_cohort_level_col
        or not peer_target_peer_n_col
        or not peer_target_appropr_col
        or not peer_target_cost_col
        or not peer_target_effect_col
        or not peer_target_qpp_cost_col
        or not peer_target_rx_appropr_col
    ):
        raise RuntimeError("cohort peer-target model is missing required columns")

    feature_insert_pairs = _optional_column_pairs(
        feature_columns,
        (
            ("npi", "f.npi"),
            ("year", "f.year"),
            ("specialty", "f.specialty"),
            ("specialty_key", "f.specialty"),
            ("taxonomy", "f.taxonomy"),
            ("taxonomy_code", "f.taxonomy"),
            ("classification", "f.classification"),
            ("procedure_count", "f.procedure_count"),
            ("procedure_bucket", "f.procedure_bucket"),
            ("source", "f.source"),
            ("updated_at", "f.updated_at"),
        ),
    )
    lsh_insert_pairs = _optional_column_pairs(
        lsh_columns,
        (
            ("npi", "s.npi"),
            ("year", "s.year"),
            ("band_no", "s.band_no"),
            ("band_index", "s.band_no"),
            ("band_hash", "s.band_hash"),
            ("hash_value", "s.band_hash"),
            ("rows_in_band", "s.rows_in_band"),
            ("source", "s.source"),
            ("updated_at", "s.updated_at"),
        ),
    )
    peer_target_insert_pairs = _optional_column_pairs(
        peer_target_columns,
        (
            ("year", "t.year"),
            ("benchmark_mode", "t.benchmark_mode"),
            ("geography_scope", "t.geography_scope"),
            ("geo_scope", "t.geography_scope"),
            ("geography_value", "t.geography_value"),
            ("geo_value", "t.geography_value"),
            ("cohort_level", "t.cohort_level"),
            ("cohort_tier", "t.cohort_level"),
            ("specialty", "t.specialty"),
            ("specialty_key", "t.specialty"),
            ("taxonomy", "t.taxonomy"),
            ("taxonomy_code", "t.taxonomy"),
            ("classification", "t.classification"),
            ("procedure_bucket", "t.procedure_bucket"),
            ("peer_n", "t.peer_n"),
            ("peer_count", "t.peer_n"),
            ("provider_count", "t.peer_n"),
            ("target_appropriateness", "t.target_appropriateness"),
            ("target_cost", "t.target_cost"),
            ("target_effectiveness", "t.target_effectiveness"),
            ("target_qpp_cost", "t.target_qpp_cost"),
            ("target_rx_appropriateness", "t.target_rx_appropriateness"),
            ("source", "t.source"),
            ("updated_at", "t.updated_at"),
        ),
    )
    if not feature_insert_pairs or not lsh_insert_pairs or not peer_target_insert_pairs:
        raise RuntimeError("cohort models do not expose insertable columns")

    feature_insert_cols_sql = ",\n            ".join(column for column, _ in feature_insert_pairs)
    feature_select_cols_sql = ",\n            ".join(expr for _, expr in feature_insert_pairs)
    lsh_insert_cols_sql = ",\n            ".join(column for column, _ in lsh_insert_pairs)
    lsh_select_cols_sql = ",\n            ".join(expr for _, expr in lsh_insert_pairs)
    peer_target_insert_cols_sql = ",\n            ".join(column for column, _ in peer_target_insert_pairs)
    peer_target_select_cols_sql = ",\n            ".join(expr for _, expr in peer_target_insert_pairs)

    feature_specialty_col = _first_existing_column(feature_columns, "specialty", "specialty_key")
    feature_taxonomy_col = _first_existing_column(feature_columns, "taxonomy", "taxonomy_code")
    feature_classification_col = _first_existing_column(feature_columns, "classification")
    feature_procedure_bucket_col = _first_existing_column(feature_columns, "procedure_bucket")

    feature_specialty_expr = (
        f"COALESCE(NULLIF(LOWER(BTRIM(COALESCE(f.{feature_specialty_col}, ''))), ''), 'unknown')"
        if feature_specialty_col
        else "COALESCE(NULLIF(LOWER(BTRIM(COALESCE(p.provider_type, ''))), ''), 'unknown')"
    )
    feature_taxonomy_expr = (
        f"COALESCE(NULLIF(LOWER(BTRIM(COALESCE(f.{feature_taxonomy_col}, ''))), ''), 'unknown')"
        if feature_taxonomy_col
        else "'unknown'::varchar"
    )
    feature_classification_expr = (
        f"COALESCE(NULLIF(LOWER(BTRIM(COALESCE(f.{feature_classification_col}, ''))), ''), 'unknown')"
        if feature_classification_col
        else "'unknown'::varchar"
    )
    feature_procedure_bucket_expr = (
        f"COALESCE(NULLIF(BTRIM(COALESCE(f.{feature_procedure_bucket_col}, '')), ''), 'bucket:none')"
        if feature_procedure_bucket_col
        else "'bucket:none'::varchar"
    )

    peer_scope_expr = f"t.{peer_target_geo_scope_col}"
    peer_benchmark_mode_expr = (
        f"t.{peer_target_benchmark_mode_col}"
        if peer_target_benchmark_mode_col
        else "bm.benchmark_mode"
    )
    peer_geo_value_expr = f"t.{peer_target_geo_value_col}"
    peer_cohort_level_expr = f"t.{peer_target_cohort_level_col}"
    peer_peer_n_expr = f"t.{peer_target_peer_n_col}"
    peer_specialty_expr = (
        f"t.{peer_target_specialty_col}"
        if peer_target_specialty_col
        else "'unknown'::varchar"
    )
    peer_taxonomy_expr = (
        f"t.{peer_target_taxonomy_col}"
        if peer_target_taxonomy_col
        else "'unknown'::varchar"
    )
    peer_classification_expr = (
        f"t.{peer_target_classification_col}"
        if peer_target_classification_col
        else "'unknown'::varchar"
    )
    peer_procedure_bucket_expr = (
        f"t.{peer_target_procedure_bucket_col}"
        if peer_target_procedure_bucket_col
        else "'bucket:none'::varchar"
    )
    peer_specialty_match = f"COALESCE({peer_specialty_expr}, 'unknown') = COALESCE(p.specialty, 'unknown')"
    peer_taxonomy_match = f"COALESCE({peer_taxonomy_expr}, 'unknown') = COALESCE(p.taxonomy, 'unknown')"
    peer_procedure_bucket_match = f"COALESCE({peer_procedure_bucket_expr}, 'bucket:none') = COALESCE(p.procedure_bucket, 'bucket:none')"

    measure_optional_pairs = _optional_column_pairs(
        measure_columns,
        (
            ("cohort_level", "s.selected_cohort_level"),
            ("cohort_tier", "s.selected_cohort_level"),
            ("cohort_geography_scope", "s.selected_geography_scope"),
            ("cohort_geo_scope", "s.selected_geography_scope"),
            ("cohort_geography_value", "s.selected_geography_value"),
            ("cohort_geo_value", "s.selected_geography_value"),
            ("cohort_specialty", "s.selected_specialty"),
            ("cohort_specialty_key", "s.selected_specialty"),
            ("cohort_taxonomy", "s.selected_taxonomy"),
            ("cohort_taxonomy_code", "s.selected_taxonomy"),
            ("cohort_classification", "s.selected_classification"),
            ("cohort_procedure_bucket", "s.selected_procedure_bucket"),
            ("cohort_peer_n", "ROUND(s.selected_peer_n)::int"),
        ),
    )
    measure_insert_columns = [
        "npi",
        "year",
        "benchmark_mode",
        "measure_id",
        "domain",
        "observed",
        "target",
        "risk_ratio",
        "ci75_low",
        "ci75_high",
        "ci90_low",
        "ci90_high",
        "evidence_n",
        "peer_group",
        "direction",
        "source",
        "updated_at",
    ]
    measure_select_columns = [
        "s.npi",
        "s.year",
        "s.benchmark_mode",
        "s.measure_id",
        "s.domain",
        "s.observed_value",
        "s.target_value",
        "(1.0 + (s.rr_raw - 1.0) * s.shrink_weight)::float8 AS risk_ratio",
        "GREATEST(0.01, (1.0 + (s.rr_raw - 1.0) * s.shrink_weight) - 1.15 * s.stderr)::float8 AS ci75_low",
        "((1.0 + (s.rr_raw - 1.0) * s.shrink_weight) + 1.15 * s.stderr)::float8 AS ci75_high",
        "GREATEST(0.01, (1.0 + (s.rr_raw - 1.0) * s.shrink_weight) - 1.64 * s.stderr)::float8 AS ci90_low",
        "((1.0 + (s.rr_raw - 1.0) * s.shrink_weight) + 1.64 * s.stderr)::float8 AS ci90_high",
        "ROUND(s.evidence_n)::int",
        "s.peer_group",
        "s.direction",
        "s.source",
        "NOW() AS updated_at",
    ]
    measure_insert_columns.extend(column for column, _ in measure_optional_pairs)
    measure_select_columns.extend(expr for _, expr in measure_optional_pairs)
    measure_insert_cols_sql = ",\n            ".join(measure_insert_columns)
    measure_select_cols_sql = ",\n            ".join(measure_select_columns)

    measure_meta_sources = {
        "selected_cohort_level": _first_existing_column(measure_columns, "cohort_level", "cohort_tier"),
        "selected_geography_scope": _first_existing_column(measure_columns, "cohort_geography_scope", "cohort_geo_scope"),
        "selected_geography_value": _first_existing_column(measure_columns, "cohort_geography_value", "cohort_geo_value"),
        "selected_specialty": _first_existing_column(measure_columns, "cohort_specialty", "cohort_specialty_key"),
        "selected_taxonomy": _first_existing_column(measure_columns, "cohort_taxonomy", "cohort_taxonomy_code"),
        "selected_classification": _first_existing_column(measure_columns, "cohort_classification"),
        "selected_procedure_bucket": _first_existing_column(measure_columns, "cohort_procedure_bucket"),
        "selected_peer_n": _first_existing_column(measure_columns, "cohort_peer_n"),
    }
    measure_meta_sources = {alias: column for alias, column in measure_meta_sources.items() if column}

    score_optional_mapping: list[tuple[str, str]] = []
    if "selected_cohort_level" in measure_meta_sources:
        score_optional_mapping.extend(
            (
                ("cohort_level", "cm.selected_cohort_level"),
                ("cohort_tier", "cm.selected_cohort_level"),
            )
        )
    if "selected_geography_scope" in measure_meta_sources:
        score_optional_mapping.extend(
            (
                ("cohort_geography_scope", "cm.selected_geography_scope"),
                ("cohort_geo_scope", "cm.selected_geography_scope"),
            )
        )
    if "selected_geography_value" in measure_meta_sources:
        score_optional_mapping.extend(
            (
                ("cohort_geography_value", "cm.selected_geography_value"),
                ("cohort_geo_value", "cm.selected_geography_value"),
            )
        )
    if "selected_specialty" in measure_meta_sources:
        score_optional_mapping.extend(
            (
                ("cohort_specialty", "cm.selected_specialty"),
                ("cohort_specialty_key", "cm.selected_specialty"),
            )
        )
    if "selected_taxonomy" in measure_meta_sources:
        score_optional_mapping.extend(
            (
                ("cohort_taxonomy", "cm.selected_taxonomy"),
                ("cohort_taxonomy_code", "cm.selected_taxonomy"),
            )
        )
    if "selected_classification" in measure_meta_sources:
        score_optional_mapping.append(("cohort_classification", "cm.selected_classification"))
    if "selected_procedure_bucket" in measure_meta_sources:
        score_optional_mapping.append(("cohort_procedure_bucket", "cm.selected_procedure_bucket"))
    if "selected_peer_n" in measure_meta_sources:
        score_optional_mapping.append(("cohort_peer_n", "ROUND(cm.selected_peer_n)::int"))
    score_optional_pairs = _optional_column_pairs(score_columns, tuple(score_optional_mapping))

    score_insert_columns = [
        "npi",
        "year",
        "model_version",
        "benchmark_mode",
        "risk_ratio_point",
        "ci75_low",
        "ci75_high",
        "ci90_low",
        "ci90_high",
        "score_0_100",
        "tier",
        "borderline_status",
        "low_score_threshold_failed",
        "low_confidence_threshold_failed",
        "high_score_threshold_passed",
        "high_confidence_threshold_passed",
        "estimated_cost_level",
        "run_id",
        "updated_at",
    ]
    score_select_columns = [
        "c.npi",
        "c.year",
        f"'{PROVIDER_QUALITY_MODEL_VERSION}'::varchar AS model_version",
        "c.benchmark_mode::varchar AS benchmark_mode",
        "c.rr_overall::float8 AS risk_ratio_point",
        "c.ci75_low::float8",
        "c.ci75_high::float8",
        "c.ci90_low::float8",
        "c.ci90_high::float8",
        "ROUND((LEAST(100.0, GREATEST(0.0, 50.0 - 125.0 * (c.rr_overall - 1.0))))::numeric, 2)::float8 AS score_0_100",
        "CASE WHEN c.low_score_check AND c.low_confidence_check THEN 'low' "
        "WHEN (c.low_score_check::int + c.low_confidence_check::int) = 1 THEN 'acceptable' "
        "WHEN c.high_score_check AND c.high_confidence_check THEN 'high' ELSE 'acceptable' END::varchar AS tier",
        "((c.low_score_check::int + c.low_confidence_check::int) = 1)::boolean AS borderline_status",
        "c.low_score_check",
        "c.low_confidence_check",
        "c.high_score_check",
        "c.high_confidence_check",
        "CASE WHEN c.rr_cost <= 0.80 THEN '$' WHEN c.rr_cost <= 0.90 THEN '$$' "
        "WHEN c.rr_cost <= 1.10 THEN '$$$' WHEN c.rr_cost <= 1.25 THEN '$$$$' ELSE '$$$$$' END::varchar AS estimated_cost_level",
        "CAST(:run_id AS varchar) AS run_id",
        "NOW() AS updated_at",
    ]
    score_insert_columns.extend(column for column, _ in score_optional_pairs)
    score_select_columns.extend(expr for _, expr in score_optional_pairs)
    score_insert_cols_sql = ",\n            ".join(score_insert_columns)
    score_select_cols_sql = ",\n            ".join(score_select_columns)

    benchmark_modes = _benchmark_modes_for_materialization(PROVIDER_QUALITY_BENCHMARK_MODE)
    benchmark_mode_values = ",\n                ".join(f"('{mode}'::varchar)" for mode in benchmark_modes)

    if rx_table_exists:
        provider_rx_cte = f"""
        provider_rx AS (
            SELECT
                r.npi,
                r.year,
                COALESCE(SUM(COALESCE(r.total_claims, 0.0)), 0.0)::float8 AS total_rx_claims,
                COALESCE(SUM(COALESCE(r.total_benes, 0.0)), 0.0)::float8 AS total_rx_beneficiaries
            FROM {schema}.{rx_table} r
            WHERE r.year BETWEEN {PROVIDER_QUALITY_MIN_YEAR} AND {PROVIDER_QUALITY_MAX_YEAR}
            GROUP BY r.npi, r.year
        ),
        """
    else:
        provider_rx_cte = """
        provider_rx AS (
            SELECT
                NULL::bigint AS npi,
                NULL::int AS year,
                0.0::float8 AS total_rx_claims,
                0.0::float8 AS total_rx_beneficiaries
            WHERE FALSE
        ),
        """

    return {
        "schema": schema,
        "qpp_table": qpp_table,
        "svi_table": svi_table,
        "measure_table": measure_table,
        "domain_table": domain_table,
        "score_table": score_table,
        "feature_table": feature_table,
        "lsh_table": lsh_table,
        "peer_target_table": peer_target_table,
        "feature_npi_col": feature_npi_col,
        "feature_year_col": feature_year_col,
        "feature_procedure_bucket_col": feature_procedure_bucket_col,
        "lsh_npi_col": lsh_npi_col,
        "lsh_year_col": lsh_year_col,
        "lsh_band_no_col": lsh_band_no_col,
        "lsh_band_hash_col": lsh_band_hash_col,
        "peer_target_year_col": peer_target_year_col,
        "peer_target_approp_col": peer_target_appropr_col,
        "peer_target_cost_col": peer_target_cost_col,
        "peer_target_effect_col": peer_target_effect_col,
        "peer_target_qpp_cost_col": peer_target_qpp_cost_col,
        "peer_target_rx_appropr_col": peer_target_rx_appropr_col,
        "feature_insert_cols_sql": feature_insert_cols_sql,
        "feature_select_cols_sql": feature_select_cols_sql,
        "lsh_insert_cols_sql": lsh_insert_cols_sql,
        "lsh_select_cols_sql": lsh_select_cols_sql,
        "peer_target_insert_cols_sql": peer_target_insert_cols_sql,
        "peer_target_select_cols_sql": peer_target_select_cols_sql,
        "measure_insert_cols_sql": measure_insert_cols_sql,
        "measure_select_cols_sql": measure_select_cols_sql,
        "score_insert_cols_sql": score_insert_cols_sql,
        "score_select_cols_sql": score_select_cols_sql,
        "feature_specialty_expr": feature_specialty_expr,
        "feature_taxonomy_expr": feature_taxonomy_expr,
        "feature_classification_expr": feature_classification_expr,
        "feature_procedure_bucket_expr": feature_procedure_bucket_expr,
        "peer_scope_expr": peer_scope_expr,
        "peer_benchmark_mode_expr": peer_benchmark_mode_expr,
        "peer_geo_value_expr": peer_geo_value_expr,
        "peer_cohort_level_expr": peer_cohort_level_expr,
        "peer_peer_n_expr": peer_peer_n_expr,
        "peer_specialty_expr": peer_specialty_expr,
        "peer_taxonomy_expr": peer_taxonomy_expr,
        "peer_classification_expr": peer_classification_expr,
        "peer_procedure_bucket_expr": peer_procedure_bucket_expr,
        "peer_specialty_match": peer_specialty_match,
        "peer_taxonomy_match": peer_taxonomy_match,
        "peer_procedure_bucket_match": peer_procedure_bucket_match,
        "provider_rx_cte": provider_rx_cte,
        "benchmark_mode_values": benchmark_mode_values,
        "measure_meta_sources": measure_meta_sources,
        "score_optional_pairs": score_optional_pairs,
    }


def _cohort_sql_phase_1_build_features(ctx: dict[str, Any]) -> str:
    schema = ctx["schema"]
    feature_table = ctx["feature_table"]
    feature_insert_cols_sql = ctx["feature_insert_cols_sql"]
    feature_select_cols_sql = ctx["feature_select_cols_sql"]
    return f"""
        INSERT INTO {schema}.{feature_table}
        (
            {feature_insert_cols_sql}
        )
        WITH provider_base AS (
            SELECT
                p.npi,
                p.year,
                p.provider_type
            FROM {schema}.{PricingProvider.__tablename__} p
            WHERE p.year BETWEEN {PROVIDER_QUALITY_MIN_YEAR} AND {PROVIDER_QUALITY_MAX_YEAR}
        ),
        taxonomy_ranked AS (
            SELECT
                t.npi::bigint AS npi,
                NULLIF(BTRIM(COALESCE(t.healthcare_provider_taxonomy_code, '')), '')::varchar AS taxonomy_code,
                ROW_NUMBER() OVER (
                    PARTITION BY t.npi
                    ORDER BY
                        CASE
                            WHEN UPPER(COALESCE(t.healthcare_provider_primary_taxonomy_switch, '')) = 'Y' THEN 0
                            ELSE 1
                        END,
                        t.checksum
                ) AS rn
            FROM {schema}.{NPIDataTaxonomy.__tablename__} t
            WHERE NULLIF(BTRIM(COALESCE(t.healthcare_provider_taxonomy_code, '')), '') IS NOT NULL
        ),
        taxonomy_choice AS (
            SELECT
                tr.npi,
                LOWER(BTRIM(tr.taxonomy_code))::varchar AS taxonomy
            FROM taxonomy_ranked tr
            WHERE tr.rn = 1
        ),
        procedure_counts AS (
            SELECT
                pp.npi,
                pp.year,
                COUNT(DISTINCT pp.procedure_code)::int AS procedure_count
            FROM {schema}.{PricingProviderProcedure.__tablename__} pp
            WHERE pp.year BETWEEN {PROVIDER_QUALITY_MIN_YEAR} AND {PROVIDER_QUALITY_MAX_YEAR}
            GROUP BY pp.npi, pp.year
        ),
        features_src AS (
            SELECT
                p.npi,
                p.year,
                COALESCE(NULLIF(LOWER(BTRIM(COALESCE(p.provider_type, ''))), ''), 'unknown')::varchar AS specialty,
                COALESCE(tc.taxonomy, 'unknown')::varchar AS taxonomy,
                COALESCE(NULLIF(LOWER(BTRIM(COALESCE(nt.classification, ''))), ''), 'unknown')::varchar AS classification,
                COALESCE(pc.procedure_count, 0)::int AS procedure_count,
                'bucket:none'::varchar AS procedure_bucket,
                'provider_quality'::varchar AS source,
                NOW() AS updated_at
            FROM provider_base p
            LEFT JOIN taxonomy_choice tc
              ON tc.npi = p.npi
            LEFT JOIN {schema}.{NUCCTaxonomy.__tablename__} nt
              ON LOWER(BTRIM(COALESCE(nt.code, ''))) = tc.taxonomy
            LEFT JOIN procedure_counts pc
              ON pc.npi = p.npi
             AND pc.year = p.year
        )
        SELECT
            {feature_select_cols_sql}
        FROM features_src f;
    """


def _cohort_sql_phase_2_build_lsh_shard(ctx: dict[str, Any]) -> str:
    schema = ctx["schema"]
    lsh_table = ctx["lsh_table"]
    lsh_insert_cols_sql = ctx["lsh_insert_cols_sql"]
    lsh_select_cols_sql = ctx["lsh_select_cols_sql"]
    return f"""
        WITH deleted AS (
            DELETE FROM {schema}.{lsh_table} d
            WHERE d.year = :year
              AND {_npi_shard_predicate("d.npi")}
            RETURNING 1
        ),
        provider_proc AS (
            SELECT DISTINCT
                pp.npi,
                pp.year,
                pp.procedure_code::varchar AS procedure_key
            FROM {schema}.{PricingProviderProcedure.__tablename__} pp
            WHERE pp.year = :year
              AND {_npi_shard_predicate("pp.npi")}
        ),
        perms AS (
            SELECT generate_series(0, {PROVIDER_QUALITY_MINHASH_NUM_PERM} - 1) AS perm_no
        ),
        signatures AS (
            SELECT
                p.npi,
                p.year,
                perm.perm_no,
                MIN(ABS(hashtextextended(p.procedure_key, perm.perm_no::bigint)))::bigint AS min_hash
            FROM provider_proc p
            CROSS JOIN perms perm
            GROUP BY p.npi, p.year, perm.perm_no
        ),
        bands AS (
            SELECT
                s.npi,
                s.year,
                FLOOR(s.perm_no / {PROVIDER_QUALITY_MINHASH_ROWS_PER_BAND})::int AS band_no,
                STRING_AGG(
                    LPAD(TO_HEX((s.min_hash % 4294967295)::bigint), 8, '0'),
                    '|' ORDER BY s.perm_no
                )::varchar AS band_signature,
                COUNT(*)::int AS rows_in_band
            FROM signatures s
            GROUP BY s.npi, s.year, FLOOR(s.perm_no / {PROVIDER_QUALITY_MINHASH_ROWS_PER_BAND})
        ),
        lsh_src AS (
            SELECT
                b.npi,
                b.year,
                b.band_no,
                MD5(b.band_signature)::varchar AS band_hash,
                b.rows_in_band,
                'provider_quality'::varchar AS source,
                NOW() AS updated_at
            FROM bands b
            WHERE b.band_no < {PROVIDER_QUALITY_MINHASH_BANDS}
        )
        INSERT INTO {schema}.{lsh_table}
        (
            {lsh_insert_cols_sql}
        )
        SELECT
            {lsh_select_cols_sql}
        FROM lsh_src s;
    """


def _cohort_sql_phase_3_update_procedure_bucket(ctx: dict[str, Any]) -> str | None:
    if not ctx["feature_procedure_bucket_col"]:
        return None
    schema = ctx["schema"]
    lsh_table = ctx["lsh_table"]
    feature_table = ctx["feature_table"]
    return f"""
        WITH bucket_source AS (
            SELECT
                l.{ctx["lsh_npi_col"]} AS npi,
                l.{ctx["lsh_year_col"]} AS year,
                COALESCE(
                    CASE
                        WHEN COUNT(*) FILTER (WHERE l.{ctx["lsh_band_no_col"]} < {PROVIDER_QUALITY_PROCEDURE_MATCH_BANDS})
                             >= {PROVIDER_QUALITY_PROCEDURE_MATCH_BANDS}
                        THEN MD5(
                            STRING_AGG(
                                l.{ctx["lsh_band_hash_col"]},
                                '|' ORDER BY l.{ctx["lsh_band_no_col"]}
                            ) FILTER (WHERE l.{ctx["lsh_band_no_col"]} < {PROVIDER_QUALITY_PROCEDURE_MATCH_BANDS})
                        )::varchar
                        ELSE NULL
                    END,
                    MAX(CASE WHEN l.{ctx["lsh_band_no_col"]} = 0 THEN l.{ctx["lsh_band_hash_col"]} END),
                    'bucket:none'
                )::varchar AS procedure_bucket
            FROM {schema}.{lsh_table} l
            GROUP BY l.{ctx["lsh_npi_col"]}, l.{ctx["lsh_year_col"]}
        )
        UPDATE {schema}.{feature_table} f
        SET {ctx["feature_procedure_bucket_col"]} = b.procedure_bucket
        FROM bucket_source b
        WHERE f.{ctx["feature_npi_col"]} = b.npi
          AND f.{ctx["feature_year_col"]} = b.year;
    """


def _cohort_sql_phase_4_build_peer_targets(ctx: dict[str, Any]) -> str:
    schema = ctx["schema"]
    qpp_table = ctx["qpp_table"]
    svi_table = ctx["svi_table"]
    feature_table = ctx["feature_table"]
    peer_target_table = ctx["peer_target_table"]
    return f"""
        INSERT INTO {schema}.{peer_target_table}
        (
            {ctx["peer_target_insert_cols_sql"]}
        )
        WITH provider_base AS (
            SELECT
                p.npi,
                p.year,
                p.provider_type,
                NULLIF(BTRIM(COALESCE(p.zip5, '')), '')::varchar AS zip5,
                UPPER(NULLIF(BTRIM(COALESCE(p.state, '')), ''))::varchar AS state_key,
                COALESCE(p.total_services, 0.0)::float8 AS total_services,
                COALESCE(p.total_beneficiaries, 0.0)::float8 AS total_beneficiaries,
                COALESCE(p.total_allowed_amount, 0.0)::float8 AS total_allowed_amount,
                CASE
                    WHEN COALESCE(p.total_beneficiaries, 0.0) > 0
                    THEN COALESCE(p.total_services, 0.0)::float8 / NULLIF(COALESCE(p.total_beneficiaries, 0.0), 0.0)
                    ELSE NULL
                END AS utilization_rate
            FROM {schema}.{PricingProvider.__tablename__} p
            WHERE p.year BETWEEN {PROVIDER_QUALITY_MIN_YEAR} AND {PROVIDER_QUALITY_MAX_YEAR}
        ),
        {ctx["provider_rx_cte"]}
        provider_plus AS (
            SELECT
                b.*,
                q.quality_score AS qpp_quality_score,
                q.cost_score AS qpp_cost_score,
                COALESCE(rx.total_rx_claims, 0.0)::float8 AS total_rx_claims,
                COALESCE(rx.total_rx_beneficiaries, 0.0)::float8 AS total_rx_beneficiaries,
                COALESCE(s.svi_overall, {PROVIDER_QUALITY_DEFAULT_SVI}) AS svi_overall,
                COALESCE(
                    b.utilization_rate / NULLIF(1.0 + 0.2 * (COALESCE(s.svi_overall, {PROVIDER_QUALITY_DEFAULT_SVI}) - 0.5), 0.0),
                    b.utilization_rate
                ) AS utilization_adjusted,
                COALESCE(
                    b.total_allowed_amount / NULLIF(1.0 + 0.2 * (COALESCE(s.svi_overall, {PROVIDER_QUALITY_DEFAULT_SVI}) - 0.5), 0.0),
                    b.total_allowed_amount
                ) AS cost_adjusted,
                CASE
                    WHEN COALESCE(rx.total_rx_beneficiaries, 0.0) > 0
                    THEN COALESCE(rx.total_rx_claims, 0.0)::float8 / NULLIF(COALESCE(rx.total_rx_beneficiaries, 0.0), 0.0)
                    ELSE NULL
                END AS rx_claim_rate
            FROM provider_base b
            LEFT JOIN {schema}.{qpp_table} q
              ON q.npi = b.npi
             AND q.year = b.year
            LEFT JOIN provider_rx rx
              ON rx.npi = b.npi
             AND rx.year = b.year
            LEFT JOIN {schema}.{svi_table} s
              ON s.zcta = b.zip5
             AND s.year = b.year
        ),
        provider_featured AS (
            SELECT
                p.*,
                {ctx["feature_specialty_expr"]} AS specialty,
                {ctx["feature_taxonomy_expr"]} AS taxonomy,
                {ctx["feature_classification_expr"]} AS classification,
                {ctx["feature_procedure_bucket_expr"]} AS procedure_bucket
            FROM provider_plus p
            LEFT JOIN {schema}.{feature_table} f
              ON f.{ctx["feature_npi_col"]} = p.npi
             AND f.{ctx["feature_year_col"]} = p.year
        ),
        benchmark_modes AS (
            SELECT v.benchmark_mode
            FROM (
                VALUES
                {ctx["benchmark_mode_values"]}
            ) AS v(benchmark_mode)
        ),
        cohort_expanded AS (
            SELECT
                p.year,
                bm.benchmark_mode,
                gs.geography_scope,
                CASE
                    WHEN gs.geography_scope = 'zip' THEN p.zip5
                    WHEN gs.geography_scope = 'state' THEN p.state_key
                    ELSE 'US'
                END::varchar AS geography_value,
                lv.cohort_level,
                CASE
                    WHEN lv.cohort_level IN ('L0', 'L1', 'L2') THEN p.specialty
                    ELSE NULL
                END::varchar AS specialty,
                CASE
                    WHEN lv.cohort_level IN ('L0', 'L1') THEN p.taxonomy
                    ELSE NULL
                END::varchar AS taxonomy,
                CASE
                    WHEN lv.cohort_level IN ('L0', 'L1') THEN p.classification
                    ELSE NULL
                END::varchar AS classification,
                CASE
                    WHEN lv.cohort_level = 'L0' THEN p.procedure_bucket
                    ELSE NULL
                END::varchar AS procedure_bucket,
                p.utilization_adjusted,
                p.cost_adjusted,
                p.qpp_quality_score,
                p.qpp_cost_score,
                p.rx_claim_rate
            FROM provider_featured p
            CROSS JOIN benchmark_modes bm
            CROSS JOIN (VALUES ('national'), ('state'), ('zip')) AS gs(geography_scope)
            CROSS JOIN (VALUES ('L0'), ('L1'), ('L2'), ('L3')) AS lv(cohort_level)
            WHERE (
                (bm.benchmark_mode = 'zip' AND gs.geography_scope IN ('zip', 'state', 'national'))
                OR (bm.benchmark_mode = 'state' AND gs.geography_scope IN ('state', 'national'))
                OR (bm.benchmark_mode = 'national' AND gs.geography_scope = 'national')
            )
            AND (
                gs.geography_scope = 'national'
                OR (gs.geography_scope = 'state' AND p.state_key IS NOT NULL)
                OR (gs.geography_scope = 'zip' AND p.zip5 IS NOT NULL)
            )
        ),
        peer_target_src AS (
            SELECT
                c.year,
                c.benchmark_mode,
                c.geography_scope,
                c.geography_value,
                c.cohort_level,
                COALESCE(c.specialty, 'unknown')::varchar AS specialty,
                COALESCE(c.taxonomy, 'unknown')::varchar AS taxonomy,
                CASE
                    WHEN c.cohort_level IN ('L0', 'L1') THEN MAX(c.classification)
                    ELSE NULL
                END::varchar AS classification,
                COALESCE(c.procedure_bucket, 'bucket:none')::varchar AS procedure_bucket,
                COUNT(*)::int AS peer_n,
                AVG(NULLIF(c.utilization_adjusted, 0.0)) AS target_appropriateness,
                AVG(NULLIF(c.cost_adjusted, 0.0)) AS target_cost,
                AVG(NULLIF(c.qpp_quality_score, 0.0)) AS target_effectiveness,
                AVG(NULLIF(c.qpp_cost_score, 0.0)) AS target_qpp_cost,
                AVG(NULLIF(c.rx_claim_rate, 0.0)) AS target_rx_appropriateness,
                'provider_quality'::varchar AS source,
                NOW() AS updated_at
            FROM cohort_expanded c
            GROUP BY
                c.year,
                c.benchmark_mode,
                c.geography_scope,
                c.geography_value,
                c.cohort_level,
                COALESCE(c.specialty, 'unknown')::varchar,
                COALESCE(c.taxonomy, 'unknown')::varchar,
                COALESCE(c.procedure_bucket, 'bucket:none')::varchar
        )
        SELECT
            {ctx["peer_target_select_cols_sql"]}
        FROM peer_target_src t;
    """


def _cohort_sql_phase_5_build_measure_shard(ctx: dict[str, Any]) -> str:
    schema = ctx["schema"]
    qpp_table = ctx["qpp_table"]
    svi_table = ctx["svi_table"]
    feature_table = ctx["feature_table"]
    peer_target_table = ctx["peer_target_table"]
    return f"""
        WITH deleted AS (
            DELETE FROM {schema}.{ctx["measure_table"]} d
            WHERE d.year = :year
              AND {_npi_shard_predicate("d.npi")}
            RETURNING 1
        ),
        provider_base AS (
            SELECT
                p.npi,
                p.year,
                p.provider_type,
                NULLIF(BTRIM(COALESCE(p.zip5, '')), '')::varchar AS zip5,
                UPPER(NULLIF(BTRIM(COALESCE(p.state, '')), ''))::varchar AS state_key,
                COALESCE(p.total_services, 0.0)::float8 AS total_services,
                COALESCE(p.total_beneficiaries, 0.0)::float8 AS total_beneficiaries,
                COALESCE(p.total_allowed_amount, 0.0)::float8 AS total_allowed_amount,
                CASE
                    WHEN COALESCE(p.total_beneficiaries, 0.0) > 0
                    THEN COALESCE(p.total_services, 0.0)::float8 / NULLIF(COALESCE(p.total_beneficiaries, 0.0), 0.0)
                    ELSE NULL
                END AS utilization_rate
            FROM {schema}.{PricingProvider.__tablename__} p
            WHERE p.year = :year
              AND {_npi_shard_predicate("p.npi")}
        ),
        {ctx["provider_rx_cte"]}
        provider_plus AS (
            SELECT
                b.*,
                q.quality_score AS qpp_quality_score,
                q.cost_score AS qpp_cost_score,
                COALESCE(rx.total_rx_claims, 0.0)::float8 AS total_rx_claims,
                COALESCE(rx.total_rx_beneficiaries, 0.0)::float8 AS total_rx_beneficiaries,
                COALESCE(s.svi_overall, {PROVIDER_QUALITY_DEFAULT_SVI}) AS svi_overall,
                COALESCE(
                    b.utilization_rate / NULLIF(1.0 + 0.2 * (COALESCE(s.svi_overall, {PROVIDER_QUALITY_DEFAULT_SVI}) - 0.5), 0.0),
                    b.utilization_rate
                ) AS utilization_adjusted,
                COALESCE(
                    b.total_allowed_amount / NULLIF(1.0 + 0.2 * (COALESCE(s.svi_overall, {PROVIDER_QUALITY_DEFAULT_SVI}) - 0.5), 0.0),
                    b.total_allowed_amount
                ) AS cost_adjusted,
                CASE
                    WHEN COALESCE(rx.total_rx_beneficiaries, 0.0) > 0
                    THEN COALESCE(rx.total_rx_claims, 0.0)::float8 / NULLIF(COALESCE(rx.total_rx_beneficiaries, 0.0), 0.0)
                    ELSE NULL
                END AS rx_claim_rate
            FROM provider_base b
            LEFT JOIN {schema}.{qpp_table} q
              ON q.npi = b.npi
             AND q.year = b.year
            LEFT JOIN provider_rx rx
              ON rx.npi = b.npi
             AND rx.year = b.year
            LEFT JOIN {schema}.{svi_table} s
              ON s.zcta = b.zip5
             AND s.year = b.year
        ),
        provider_featured AS (
            SELECT
                p.*,
                {ctx["feature_specialty_expr"]} AS specialty,
                {ctx["feature_taxonomy_expr"]} AS taxonomy,
                {ctx["feature_classification_expr"]} AS classification,
                {ctx["feature_procedure_bucket_expr"]} AS procedure_bucket
            FROM provider_plus p
            LEFT JOIN {schema}.{feature_table} f
              ON f.{ctx["feature_npi_col"]} = p.npi
             AND f.{ctx["feature_year_col"]} = p.year
        ),
        benchmark_modes AS (
            SELECT v.benchmark_mode
            FROM (
                VALUES
                {ctx["benchmark_mode_values"]}
            ) AS v(benchmark_mode)
        ),
        target_candidates AS (
            SELECT
                p.npi,
                p.year,
                bm.benchmark_mode,
                ({ctx["peer_scope_expr"]})::varchar AS selected_geography_scope,
                ({ctx["peer_geo_value_expr"]})::varchar AS selected_geography_value,
                ({ctx["peer_cohort_level_expr"]})::varchar AS selected_cohort_level,
                ({ctx["peer_specialty_expr"]})::varchar AS selected_specialty,
                ({ctx["peer_taxonomy_expr"]})::varchar AS selected_taxonomy,
                ({ctx["peer_classification_expr"]})::varchar AS selected_classification,
                ({ctx["peer_procedure_bucket_expr"]})::varchar AS selected_procedure_bucket,
                ({ctx["peer_peer_n_expr"]})::float8 AS selected_peer_n,
                t.{ctx["peer_target_approp_col"]}::float8 AS target_appropriateness,
                t.{ctx["peer_target_cost_col"]}::float8 AS target_cost,
                t.{ctx["peer_target_effect_col"]}::float8 AS target_effectiveness,
                t.{ctx["peer_target_qpp_cost_col"]}::float8 AS target_qpp_cost,
                t.{ctx["peer_target_rx_appropr_col"]}::float8 AS target_rx_appropriateness,
                CASE
                    WHEN bm.benchmark_mode = 'zip' THEN
                        CASE
                            WHEN {ctx["peer_scope_expr"]} = 'zip' THEN 1
                            WHEN {ctx["peer_scope_expr"]} = 'state' THEN 2
                            WHEN {ctx["peer_scope_expr"]} = 'national' THEN 3
                            ELSE 9
                        END
                    WHEN bm.benchmark_mode = 'state' THEN
                        CASE
                            WHEN {ctx["peer_scope_expr"]} = 'state' THEN 1
                            WHEN {ctx["peer_scope_expr"]} = 'national' THEN 2
                            ELSE 9
                        END
                    ELSE
                        CASE WHEN {ctx["peer_scope_expr"]} = 'national' THEN 1 ELSE 9 END
                END AS geography_rank,
                CASE
                    WHEN {ctx["peer_cohort_level_expr"]} = 'L0' THEN 1
                    WHEN {ctx["peer_cohort_level_expr"]} = 'L1' THEN 2
                    WHEN {ctx["peer_cohort_level_expr"]} = 'L2' THEN 3
                    ELSE 4
                END AS level_rank,
                CASE
                    WHEN {ctx["peer_scope_expr"]} = 'zip' THEN ({ctx["peer_peer_n_expr"]}) >= {PROVIDER_QUALITY_ZIP_MIN_PEER_N}
                    WHEN {ctx["peer_scope_expr"]} = 'state' THEN ({ctx["peer_peer_n_expr"]}) >= {PROVIDER_QUALITY_MIN_STATE_PEER_N}
                    ELSE ({ctx["peer_peer_n_expr"]}) >= {PROVIDER_QUALITY_NATIONAL_MIN_PEER_N}
                END AS threshold_met
            FROM provider_featured p
            CROSS JOIN benchmark_modes bm
            JOIN {schema}.{peer_target_table} t
              ON t.{ctx["peer_target_year_col"]} = p.year
             AND {ctx["peer_benchmark_mode_expr"]} = bm.benchmark_mode
             AND (
                    ({ctx["peer_scope_expr"]} = 'zip' AND bm.benchmark_mode = 'zip' AND p.zip5 IS NOT NULL AND {ctx["peer_geo_value_expr"]} = p.zip5)
                    OR (
                        {ctx["peer_scope_expr"]} = 'state'
                        AND bm.benchmark_mode IN ('zip', 'state')
                        AND p.state_key IS NOT NULL
                        AND UPPER({ctx["peer_geo_value_expr"]}) = p.state_key
                    )
                    OR ({ctx["peer_scope_expr"]} = 'national' AND bm.benchmark_mode IN ('zip', 'state', 'national'))
                )
             AND (
                    ({ctx["peer_cohort_level_expr"]} = 'L0' AND {ctx["peer_specialty_match"]} AND {ctx["peer_taxonomy_match"]} AND {ctx["peer_procedure_bucket_match"]})
                    OR ({ctx["peer_cohort_level_expr"]} = 'L1' AND {ctx["peer_specialty_match"]} AND {ctx["peer_taxonomy_match"]})
                    OR ({ctx["peer_cohort_level_expr"]} = 'L2' AND {ctx["peer_specialty_match"]})
                    OR ({ctx["peer_cohort_level_expr"]} = 'L3')
                )
        ),
        target_selected AS (
            SELECT DISTINCT ON (c.npi, c.year, c.benchmark_mode)
                c.npi,
                c.year,
                c.benchmark_mode,
                c.selected_geography_scope,
                c.selected_geography_value,
                c.selected_cohort_level,
                c.selected_specialty,
                c.selected_taxonomy,
                c.selected_classification,
                c.selected_procedure_bucket,
                c.selected_peer_n,
                c.target_appropriateness,
                c.target_cost,
                c.target_effectiveness,
                c.target_qpp_cost,
                c.target_rx_appropriateness
            FROM target_candidates c
            WHERE c.geography_rank < 9
            ORDER BY
                c.npi,
                c.year,
                c.benchmark_mode,
                CASE WHEN c.threshold_met THEN 0 ELSE 1 END,
                c.geography_rank,
                c.level_rank,
                c.selected_peer_n DESC,
                c.selected_geography_scope,
                c.selected_geography_value,
                c.selected_cohort_level
        ),
        measure_inputs AS (
            SELECT
                p.npi,
                p.year,
                ts.benchmark_mode,
                p.total_services,
                p.total_beneficiaries,
                p.utilization_adjusted,
                p.cost_adjusted,
                p.qpp_quality_score,
                p.qpp_cost_score,
                p.rx_claim_rate,
                ts.target_appropriateness,
                ts.target_cost,
                ts.target_effectiveness,
                ts.target_qpp_cost,
                ts.target_rx_appropriateness,
                ts.selected_cohort_level,
                ts.selected_geography_scope,
                ts.selected_geography_value,
                ts.selected_specialty,
                ts.selected_taxonomy,
                ts.selected_classification,
                ts.selected_procedure_bucket,
                ts.selected_peer_n,
                (
                    'cohort:'
                    || ts.selected_geography_scope
                    || ':'
                    || COALESCE(ts.selected_geography_value, 'US')
                    || ':'
                    || ts.selected_cohort_level
                )::varchar AS peer_group,
                GREATEST(COALESCE(p.total_services, 0.0), COALESCE(p.total_beneficiaries, 0.0), 1.0) AS evidence_n_claims,
                GREATEST(COALESCE(p.total_rx_claims, 0.0), COALESCE(p.total_rx_beneficiaries, 0.0), 1.0) AS evidence_n_rx
            FROM provider_featured p
            JOIN target_selected ts
              ON ts.npi = p.npi
             AND ts.year = p.year
        ),
        expanded AS (
            SELECT
                npi,
                year,
                benchmark_mode,
                'appropriateness_utilization'::varchar AS measure_id,
                'appropriateness'::varchar AS domain,
                utilization_adjusted::float8 AS observed_value,
                target_appropriateness::float8 AS target_value,
                CASE
                    WHEN target_appropriateness IS NULL OR target_appropriateness <= 0 THEN 1.0
                    WHEN utilization_adjusted IS NULL OR utilization_adjusted <= 0 THEN 1.0
                    ELSE utilization_adjusted / target_appropriateness
                END AS rr_raw,
                evidence_n_claims AS evidence_n,
                peer_group,
                'higher_worse'::varchar AS direction,
                'claims_pricing'::varchar AS source,
                selected_cohort_level,
                selected_geography_scope,
                selected_geography_value,
                selected_specialty,
                selected_taxonomy,
                selected_classification,
                selected_procedure_bucket,
                selected_peer_n
            FROM measure_inputs
            UNION ALL
            SELECT
                npi,
                year,
                benchmark_mode,
                'appropriateness_drug_proxy'::varchar AS measure_id,
                'appropriateness'::varchar AS domain,
                rx_claim_rate::float8 AS observed_value,
                target_rx_appropriateness::float8 AS target_value,
                CASE
                    WHEN target_rx_appropriateness IS NULL OR target_rx_appropriateness <= 0 THEN 1.0
                    WHEN rx_claim_rate IS NULL OR rx_claim_rate <= 0 THEN 1.0
                    ELSE rx_claim_rate / target_rx_appropriateness
                END AS rr_raw,
                evidence_n_rx AS evidence_n,
                peer_group,
                'higher_worse'::varchar AS direction,
                'drug_claims'::varchar AS source,
                selected_cohort_level,
                selected_geography_scope,
                selected_geography_value,
                selected_specialty,
                selected_taxonomy,
                selected_classification,
                selected_procedure_bucket,
                selected_peer_n
            FROM measure_inputs
            UNION ALL
            SELECT
                npi,
                year,
                benchmark_mode,
                'effectiveness_qpp_quality'::varchar AS measure_id,
                'effectiveness'::varchar AS domain,
                qpp_quality_score::float8 AS observed_value,
                target_effectiveness::float8 AS target_value,
                CASE
                    WHEN qpp_quality_score IS NULL OR qpp_quality_score <= 0 THEN 1.0
                    WHEN target_effectiveness IS NULL OR target_effectiveness <= 0 THEN 1.0
                    ELSE target_effectiveness / qpp_quality_score
                END AS rr_raw,
                evidence_n_claims AS evidence_n,
                peer_group,
                'lower_worse'::varchar AS direction,
                'qpp'::varchar AS source,
                selected_cohort_level,
                selected_geography_scope,
                selected_geography_value,
                selected_specialty,
                selected_taxonomy,
                selected_classification,
                selected_procedure_bucket,
                selected_peer_n
            FROM measure_inputs
            UNION ALL
            SELECT
                npi,
                year,
                benchmark_mode,
                'cost_qpp_component'::varchar AS measure_id,
                'cost'::varchar AS domain,
                qpp_cost_score::float8 AS observed_value,
                target_qpp_cost::float8 AS target_value,
                CASE
                    WHEN qpp_cost_score IS NULL OR qpp_cost_score <= 0 THEN 1.0
                    WHEN target_qpp_cost IS NULL OR target_qpp_cost <= 0 THEN 1.0
                    ELSE target_qpp_cost / qpp_cost_score
                END AS rr_raw,
                evidence_n_claims AS evidence_n,
                peer_group,
                'lower_worse'::varchar AS direction,
                'qpp'::varchar AS source,
                selected_cohort_level,
                selected_geography_scope,
                selected_geography_value,
                selected_specialty,
                selected_taxonomy,
                selected_classification,
                selected_procedure_bucket,
                selected_peer_n
            FROM measure_inputs
            UNION ALL
            SELECT
                npi,
                year,
                benchmark_mode,
                'cost_allowed_amount'::varchar AS measure_id,
                'cost'::varchar AS domain,
                cost_adjusted::float8 AS observed_value,
                target_cost::float8 AS target_value,
                CASE
                    WHEN target_cost IS NULL OR target_cost <= 0 THEN 1.0
                    WHEN cost_adjusted IS NULL OR cost_adjusted <= 0 THEN 1.0
                    ELSE cost_adjusted / target_cost
                END AS rr_raw,
                evidence_n_claims AS evidence_n,
                peer_group,
                'higher_worse'::varchar AS direction,
                'claims_pricing'::varchar AS source,
                selected_cohort_level,
                selected_geography_scope,
                selected_geography_value,
                selected_specialty,
                selected_taxonomy,
                selected_classification,
                selected_procedure_bucket,
                selected_peer_n
            FROM measure_inputs
        ),
        shrunk AS (
            SELECT
                e.*,
                LEAST(1.0, GREATEST(0.0, e.evidence_n / NULLIF(e.evidence_n + {PROVIDER_QUALITY_SHRINKAGE_ALPHA}, 0.0))) AS shrink_weight,
                GREATEST(0.0001, SQRT(1.0 / NULLIF(e.evidence_n + 1.0, 0.0))) AS stderr
            FROM expanded e
        )
        INSERT INTO {schema}.{ctx["measure_table"]}
        (
            {ctx["measure_insert_cols_sql"]}
        )
        SELECT
            {ctx["measure_select_cols_sql"]}
        FROM shrunk s;
    """


def _cohort_sql_phase_6_build_domain_shard(ctx: dict[str, Any]) -> str:
    schema = ctx["schema"]
    return f"""
        WITH deleted AS (
            DELETE FROM {schema}.{ctx["domain_table"]} d
            WHERE d.year = :year
              AND {_npi_shard_predicate("d.npi")}
            RETURNING 1
        )
        INSERT INTO {schema}.{ctx["domain_table"]}
        (
            npi,
            year,
            benchmark_mode,
            domain,
            risk_ratio,
            score_0_100,
            ci75_low,
            ci75_high,
            ci90_low,
            ci90_high,
            evidence_n,
            measure_count,
            updated_at
        )
        SELECT
            m.npi,
            m.year,
            m.benchmark_mode,
            m.domain,
            EXP(AVG(LN(GREATEST(m.risk_ratio, 0.0001))))::float8 AS risk_ratio,
            ROUND((LEAST(100.0, GREATEST(0.0, 50.0 - 125.0 * (EXP(AVG(LN(GREATEST(m.risk_ratio, 0.0001)))) - 1.0))))::numeric, 2)::float8 AS score_0_100,
            EXP(AVG(LN(GREATEST(m.ci75_low, 0.0001))))::float8 AS ci75_low,
            EXP(AVG(LN(GREATEST(m.ci75_high, 0.0001))))::float8 AS ci75_high,
            EXP(AVG(LN(GREATEST(m.ci90_low, 0.0001))))::float8 AS ci90_low,
            EXP(AVG(LN(GREATEST(m.ci90_high, 0.0001))))::float8 AS ci90_high,
            SUM(COALESCE(m.evidence_n, 0))::int AS evidence_n,
            COUNT(*)::int AS measure_count,
            NOW() AS updated_at
        FROM {schema}.{ctx["measure_table"]} m
        WHERE m.year = :year
          AND {_npi_shard_predicate("m.npi")}
        GROUP BY m.npi, m.year, m.benchmark_mode, m.domain;
    """


def _cohort_sql_phase_7_build_score_shard(ctx: dict[str, Any]) -> str:
    schema = ctx["schema"]
    cohort_meta_cte = ""
    cohort_meta_join = ""
    if ctx["score_optional_pairs"] and ctx["measure_meta_sources"]:
        cohort_meta_cte = (
            ",\n        cohort_meta AS (\n"
            "            SELECT\n"
            "                m.npi,\n"
            "                m.year,\n"
            "                m.benchmark_mode,\n"
            + ",\n".join(
                f"                MAX(m.{column_name}) AS {alias}"
                for alias, column_name in ctx["measure_meta_sources"].items()
            )
            + f"\n            FROM {schema}.{ctx['measure_table']} m\n"
            "            WHERE m.year = :year\n"
            f"              AND {_npi_shard_predicate('m.npi')}\n"
            "            GROUP BY m.npi, m.year, m.benchmark_mode\n"
            "        )\n"
        )
        cohort_meta_join = (
            "\n        LEFT JOIN cohort_meta cm\n"
            "          ON cm.npi = c.npi\n"
            "         AND cm.year = c.year\n"
            "         AND cm.benchmark_mode = c.benchmark_mode"
        )
    return f"""
        WITH deleted AS (
            DELETE FROM {schema}.{ctx["score_table"]} d
            WHERE d.year = :year
              AND {_npi_shard_predicate("d.npi")}
            RETURNING 1
        ),
        domain_pivot AS (
            SELECT
                d.npi,
                d.year,
                d.benchmark_mode,
                MAX(CASE WHEN d.domain = 'appropriateness' THEN d.risk_ratio END) AS rr_appropr,
                MAX(CASE WHEN d.domain = 'effectiveness' THEN d.risk_ratio END) AS rr_effect,
                MAX(CASE WHEN d.domain = 'cost' THEN d.risk_ratio END) AS rr_cost,
                MAX(CASE WHEN d.domain = 'appropriateness' THEN d.ci75_low END) AS ci75_appropr_low,
                MAX(CASE WHEN d.domain = 'effectiveness' THEN d.ci75_low END) AS ci75_effect_low,
                MAX(CASE WHEN d.domain = 'cost' THEN d.ci75_low END) AS ci75_cost_low,
                MAX(CASE WHEN d.domain = 'appropriateness' THEN d.ci75_high END) AS ci75_appropr_high,
                MAX(CASE WHEN d.domain = 'effectiveness' THEN d.ci75_high END) AS ci75_effect_high,
                MAX(CASE WHEN d.domain = 'cost' THEN d.ci75_high END) AS ci75_cost_high,
                MAX(CASE WHEN d.domain = 'appropriateness' THEN d.ci90_low END) AS ci90_appropr_low,
                MAX(CASE WHEN d.domain = 'effectiveness' THEN d.ci90_low END) AS ci90_effect_low,
                MAX(CASE WHEN d.domain = 'cost' THEN d.ci90_low END) AS ci90_cost_low,
                MAX(CASE WHEN d.domain = 'appropriateness' THEN d.ci90_high END) AS ci90_appropr_high,
                MAX(CASE WHEN d.domain = 'effectiveness' THEN d.ci90_high END) AS ci90_effect_high,
                MAX(CASE WHEN d.domain = 'cost' THEN d.ci90_high END) AS ci90_cost_high
            FROM {schema}.{ctx["domain_table"]} d
            WHERE d.year = :year
              AND {_npi_shard_predicate("d.npi")}
            GROUP BY d.npi, d.year, d.benchmark_mode
        ){cohort_meta_cte},
        blended AS (
            SELECT
                p.npi,
                p.year,
                p.benchmark_mode,
                COALESCE(p.rr_appropr, 1.0) AS rr_appropr,
                COALESCE(p.rr_effect, 1.0) AS rr_effect,
                COALESCE(p.rr_cost, 1.0) AS rr_cost,
                SQRT(COALESCE(p.rr_appropr, 1.0) * COALESCE(p.rr_effect, 1.0)) AS rr_clinical,
                EXP(
                    0.5 * LN(GREATEST(SQRT(COALESCE(p.rr_appropr, 1.0) * COALESCE(p.rr_effect, 1.0)), 0.0001))
                    + 0.5 * LN(GREATEST(COALESCE(p.rr_cost, 1.0), 0.0001))
                ) AS rr_overall,
                EXP(
                    0.5 * LN(GREATEST(SQRT(COALESCE(p.ci75_appropr_low, 1.0) * COALESCE(p.ci75_effect_low, 1.0)), 0.0001))
                    + 0.5 * LN(GREATEST(COALESCE(p.ci75_cost_low, 1.0), 0.0001))
                ) AS ci75_low,
                EXP(
                    0.5 * LN(GREATEST(SQRT(COALESCE(p.ci75_appropr_high, 1.0) * COALESCE(p.ci75_effect_high, 1.0)), 0.0001))
                    + 0.5 * LN(GREATEST(COALESCE(p.ci75_cost_high, 1.0), 0.0001))
                ) AS ci75_high,
                EXP(
                    0.5 * LN(GREATEST(SQRT(COALESCE(p.ci90_appropr_low, 1.0) * COALESCE(p.ci90_effect_low, 1.0)), 0.0001))
                    + 0.5 * LN(GREATEST(COALESCE(p.ci90_cost_low, 1.0), 0.0001))
                ) AS ci90_low,
                EXP(
                    0.5 * LN(GREATEST(SQRT(COALESCE(p.ci90_appropr_high, 1.0) * COALESCE(p.ci90_effect_high, 1.0)), 0.0001))
                    + 0.5 * LN(GREATEST(COALESCE(p.ci90_cost_high, 1.0), 0.0001))
                ) AS ci90_high
            FROM domain_pivot p
        ),
        checks AS (
            SELECT
                b.*,
                (b.rr_overall >= 1.12)::boolean AS low_score_check,
                (b.ci90_low >= 1.08)::boolean AS low_confidence_check,
                (b.rr_overall <= 0.88)::boolean AS high_score_check,
                (b.ci75_high < 1.0)::boolean AS high_confidence_check
            FROM blended b
        )
        INSERT INTO {schema}.{ctx["score_table"]}
        (
            {ctx["score_insert_cols_sql"]}
        )
        SELECT
            {ctx["score_select_cols_sql"]}
        FROM checks c{cohort_meta_join};
    """


async def _materialize_quality_rows_cohort(classes: dict[str, type], schema: str, run_id: str) -> None:
    qpp_table = classes["PricingQppProvider"].__tablename__
    svi_table = classes["PricingSviZcta"].__tablename__
    measure_table = classes["PricingProviderQualityMeasure"].__tablename__
    domain_table = classes["PricingProviderQualityDomain"].__tablename__
    score_table = classes["PricingProviderQualityScore"].__tablename__
    feature_table = classes["PricingProviderQualityFeature"].__tablename__
    lsh_table = classes["PricingProviderQualityProcedureLSH"].__tablename__
    peer_target_table = classes["PricingProviderQualityPeerTarget"].__tablename__
    rx_table = PricingProviderPrescription.__tablename__
    rx_table_exists = await _table_exists(schema, rx_table)

    feature_columns = _model_columns(classes.get("PricingProviderQualityFeature"))
    lsh_columns = _model_columns(classes.get("PricingProviderQualityProcedureLSH"))
    peer_target_columns = _model_columns(classes.get("PricingProviderQualityPeerTarget"))
    measure_columns = _model_columns(classes.get("PricingProviderQualityMeasure"))
    score_columns = _model_columns(classes.get("PricingProviderQualityScore"))

    feature_npi_col = _first_existing_column(feature_columns, "npi")
    feature_year_col = _first_existing_column(feature_columns, "year")
    if not feature_npi_col or not feature_year_col:
        raise RuntimeError("cohort feature model must expose npi/year columns")

    lsh_npi_col = _first_existing_column(lsh_columns, "npi")
    lsh_year_col = _first_existing_column(lsh_columns, "year")
    lsh_band_no_col = _first_existing_column(lsh_columns, "band_no", "band_index")
    lsh_band_hash_col = _first_existing_column(lsh_columns, "band_hash", "hash_value")
    if not lsh_npi_col or not lsh_year_col or not lsh_band_no_col or not lsh_band_hash_col:
        raise RuntimeError("cohort LSH model must expose npi/year/band_no/band_hash columns")

    peer_target_year_col = _first_existing_column(peer_target_columns, "year")
    peer_target_benchmark_mode_col = _first_existing_column(peer_target_columns, "benchmark_mode")
    peer_target_geo_scope_col = _first_existing_column(peer_target_columns, "geography_scope", "geo_scope")
    peer_target_geo_value_col = _first_existing_column(peer_target_columns, "geography_value", "geo_value")
    peer_target_cohort_level_col = _first_existing_column(peer_target_columns, "cohort_level", "cohort_tier")
    peer_target_peer_n_col = _first_existing_column(peer_target_columns, "peer_n", "peer_count", "provider_count")
    peer_target_specialty_col = _first_existing_column(peer_target_columns, "specialty", "specialty_key")
    peer_target_taxonomy_col = _first_existing_column(peer_target_columns, "taxonomy", "taxonomy_code")
    peer_target_classification_col = _first_existing_column(peer_target_columns, "classification")
    peer_target_procedure_bucket_col = _first_existing_column(peer_target_columns, "procedure_bucket")
    peer_target_appropr_col = _first_existing_column(peer_target_columns, "target_appropriateness")
    peer_target_cost_col = _first_existing_column(peer_target_columns, "target_cost")
    peer_target_effect_col = _first_existing_column(peer_target_columns, "target_effectiveness")
    peer_target_qpp_cost_col = _first_existing_column(peer_target_columns, "target_qpp_cost")
    peer_target_rx_appropr_col = _first_existing_column(peer_target_columns, "target_rx_appropriateness")
    if (
        not peer_target_year_col
        or not peer_target_geo_scope_col
        or not peer_target_geo_value_col
        or not peer_target_cohort_level_col
        or not peer_target_peer_n_col
        or not peer_target_appropr_col
        or not peer_target_cost_col
        or not peer_target_effect_col
        or not peer_target_qpp_cost_col
        or not peer_target_rx_appropr_col
    ):
        raise RuntimeError("cohort peer-target model is missing required columns")

    feature_insert_pairs = _optional_column_pairs(
        feature_columns,
        (
            ("npi", "f.npi"),
            ("year", "f.year"),
            ("specialty", "f.specialty"),
            ("specialty_key", "f.specialty"),
            ("taxonomy", "f.taxonomy"),
            ("taxonomy_code", "f.taxonomy"),
            ("classification", "f.classification"),
            ("procedure_count", "f.procedure_count"),
            ("procedure_bucket", "f.procedure_bucket"),
            ("source", "f.source"),
            ("updated_at", "f.updated_at"),
        ),
    )
    lsh_insert_pairs = _optional_column_pairs(
        lsh_columns,
        (
            ("npi", "s.npi"),
            ("year", "s.year"),
            ("band_no", "s.band_no"),
            ("band_index", "s.band_no"),
            ("band_hash", "s.band_hash"),
            ("hash_value", "s.band_hash"),
            ("rows_in_band", "s.rows_in_band"),
            ("source", "s.source"),
            ("updated_at", "s.updated_at"),
        ),
    )
    peer_target_insert_pairs = _optional_column_pairs(
        peer_target_columns,
        (
            ("year", "t.year"),
            ("benchmark_mode", "t.benchmark_mode"),
            ("geography_scope", "t.geography_scope"),
            ("geo_scope", "t.geography_scope"),
            ("geography_value", "t.geography_value"),
            ("geo_value", "t.geography_value"),
            ("cohort_level", "t.cohort_level"),
            ("cohort_tier", "t.cohort_level"),
            ("specialty", "t.specialty"),
            ("specialty_key", "t.specialty"),
            ("taxonomy", "t.taxonomy"),
            ("taxonomy_code", "t.taxonomy"),
            ("classification", "t.classification"),
            ("procedure_bucket", "t.procedure_bucket"),
            ("peer_n", "t.peer_n"),
            ("peer_count", "t.peer_n"),
            ("provider_count", "t.peer_n"),
            ("target_appropriateness", "t.target_appropriateness"),
            ("target_cost", "t.target_cost"),
            ("target_effectiveness", "t.target_effectiveness"),
            ("target_qpp_cost", "t.target_qpp_cost"),
            ("target_rx_appropriateness", "t.target_rx_appropriateness"),
            ("source", "t.source"),
            ("updated_at", "t.updated_at"),
        ),
    )
    if not feature_insert_pairs or not lsh_insert_pairs or not peer_target_insert_pairs:
        raise RuntimeError("cohort models do not expose insertable columns")

    feature_insert_cols_sql = ",\n            ".join(column for column, _ in feature_insert_pairs)
    feature_select_cols_sql = ",\n            ".join(expr for _, expr in feature_insert_pairs)
    lsh_insert_cols_sql = ",\n            ".join(column for column, _ in lsh_insert_pairs)
    lsh_select_cols_sql = ",\n            ".join(expr for _, expr in lsh_insert_pairs)
    peer_target_insert_cols_sql = ",\n            ".join(column for column, _ in peer_target_insert_pairs)
    peer_target_select_cols_sql = ",\n            ".join(expr for _, expr in peer_target_insert_pairs)

    feature_specialty_col = _first_existing_column(feature_columns, "specialty", "specialty_key")
    feature_taxonomy_col = _first_existing_column(feature_columns, "taxonomy", "taxonomy_code")
    feature_classification_col = _first_existing_column(feature_columns, "classification")
    feature_procedure_bucket_col = _first_existing_column(feature_columns, "procedure_bucket")

    feature_specialty_expr = (
        f"COALESCE(NULLIF(LOWER(BTRIM(COALESCE(f.{feature_specialty_col}, ''))), ''), 'unknown')"
        if feature_specialty_col
        else "COALESCE(NULLIF(LOWER(BTRIM(COALESCE(p.provider_type, ''))), ''), 'unknown')"
    )
    feature_taxonomy_expr = (
        f"COALESCE(NULLIF(LOWER(BTRIM(COALESCE(f.{feature_taxonomy_col}, ''))), ''), 'unknown')"
        if feature_taxonomy_col
        else "'unknown'::varchar"
    )
    feature_classification_expr = (
        f"COALESCE(NULLIF(LOWER(BTRIM(COALESCE(f.{feature_classification_col}, ''))), ''), 'unknown')"
        if feature_classification_col
        else "'unknown'::varchar"
    )
    feature_procedure_bucket_expr = (
        f"COALESCE(NULLIF(BTRIM(COALESCE(f.{feature_procedure_bucket_col}, '')), ''), 'bucket:none')"
        if feature_procedure_bucket_col
        else "'bucket:none'::varchar"
    )

    peer_scope_expr = f"LOWER(COALESCE(t.{peer_target_geo_scope_col}, 'national'))"
    peer_benchmark_mode_expr = (
        f"LOWER(COALESCE(t.{peer_target_benchmark_mode_col}, 'national'))"
        if peer_target_benchmark_mode_col
        else "bm.benchmark_mode"
    )
    peer_geo_value_expr = f"NULLIF(BTRIM(COALESCE(t.{peer_target_geo_value_col}, '')), '')"
    peer_cohort_level_expr = f"UPPER(COALESCE(t.{peer_target_cohort_level_col}, 'L3'))"
    peer_peer_n_expr = f"COALESCE(t.{peer_target_peer_n_col}, 0)"
    peer_specialty_expr = (
        f"COALESCE(NULLIF(LOWER(BTRIM(COALESCE(t.{peer_target_specialty_col}, ''))), ''), 'unknown')"
        if peer_target_specialty_col
        else "'unknown'::varchar"
    )
    peer_taxonomy_expr = (
        f"COALESCE(NULLIF(LOWER(BTRIM(COALESCE(t.{peer_target_taxonomy_col}, ''))), ''), 'unknown')"
        if peer_target_taxonomy_col
        else "'unknown'::varchar"
    )
    peer_classification_expr = (
        f"COALESCE(NULLIF(LOWER(BTRIM(COALESCE(t.{peer_target_classification_col}, ''))), ''), 'unknown')"
        if peer_target_classification_col
        else "'unknown'::varchar"
    )
    peer_procedure_bucket_expr = (
        f"COALESCE(NULLIF(BTRIM(COALESCE(t.{peer_target_procedure_bucket_col}, '')), ''), 'bucket:none')"
        if peer_target_procedure_bucket_col
        else "'bucket:none'::varchar"
    )
    peer_specialty_match = f"{peer_specialty_expr} = COALESCE(p.specialty, 'unknown')"
    peer_taxonomy_match = f"{peer_taxonomy_expr} = COALESCE(p.taxonomy, 'unknown')"
    peer_procedure_bucket_match = f"{peer_procedure_bucket_expr} = COALESCE(p.procedure_bucket, 'bucket:none')"

    measure_optional_pairs = _optional_column_pairs(
        measure_columns,
        (
            ("cohort_level", "s.selected_cohort_level"),
            ("cohort_tier", "s.selected_cohort_level"),
            ("cohort_geography_scope", "s.selected_geography_scope"),
            ("cohort_geo_scope", "s.selected_geography_scope"),
            ("cohort_geography_value", "s.selected_geography_value"),
            ("cohort_geo_value", "s.selected_geography_value"),
            ("cohort_specialty", "s.selected_specialty"),
            ("cohort_specialty_key", "s.selected_specialty"),
            ("cohort_taxonomy", "s.selected_taxonomy"),
            ("cohort_taxonomy_code", "s.selected_taxonomy"),
            ("cohort_classification", "s.selected_classification"),
            ("cohort_procedure_bucket", "s.selected_procedure_bucket"),
            ("cohort_peer_n", "ROUND(s.selected_peer_n)::int"),
        ),
    )
    measure_insert_columns = [
        "npi",
        "year",
        "benchmark_mode",
        "measure_id",
        "domain",
        "observed",
        "target",
        "risk_ratio",
        "ci75_low",
        "ci75_high",
        "ci90_low",
        "ci90_high",
        "evidence_n",
        "peer_group",
        "direction",
        "source",
        "updated_at",
    ]
    measure_select_columns = [
        "s.npi",
        "s.year",
        "s.benchmark_mode",
        "s.measure_id",
        "s.domain",
        "s.observed_value",
        "s.target_value",
        "(1.0 + (s.rr_raw - 1.0) * s.shrink_weight)::float8 AS risk_ratio",
        "GREATEST(0.01, (1.0 + (s.rr_raw - 1.0) * s.shrink_weight) - 1.15 * s.stderr)::float8 AS ci75_low",
        "((1.0 + (s.rr_raw - 1.0) * s.shrink_weight) + 1.15 * s.stderr)::float8 AS ci75_high",
        "GREATEST(0.01, (1.0 + (s.rr_raw - 1.0) * s.shrink_weight) - 1.64 * s.stderr)::float8 AS ci90_low",
        "((1.0 + (s.rr_raw - 1.0) * s.shrink_weight) + 1.64 * s.stderr)::float8 AS ci90_high",
        "ROUND(s.evidence_n)::int",
        "s.peer_group",
        "s.direction",
        "s.source",
        "NOW() AS updated_at",
    ]
    measure_insert_columns.extend(column for column, _ in measure_optional_pairs)
    measure_select_columns.extend(expr for _, expr in measure_optional_pairs)
    measure_insert_cols_sql = ",\n            ".join(measure_insert_columns)
    measure_select_cols_sql = ",\n            ".join(measure_select_columns)

    measure_meta_sources = {
        "selected_cohort_level": _first_existing_column(measure_columns, "cohort_level", "cohort_tier"),
        "selected_geography_scope": _first_existing_column(measure_columns, "cohort_geography_scope", "cohort_geo_scope"),
        "selected_geography_value": _first_existing_column(measure_columns, "cohort_geography_value", "cohort_geo_value"),
        "selected_specialty": _first_existing_column(measure_columns, "cohort_specialty", "cohort_specialty_key"),
        "selected_taxonomy": _first_existing_column(measure_columns, "cohort_taxonomy", "cohort_taxonomy_code"),
        "selected_classification": _first_existing_column(measure_columns, "cohort_classification"),
        "selected_procedure_bucket": _first_existing_column(measure_columns, "cohort_procedure_bucket"),
        "selected_peer_n": _first_existing_column(measure_columns, "cohort_peer_n"),
    }
    measure_meta_sources = {alias: column for alias, column in measure_meta_sources.items() if column}

    score_optional_mapping: list[tuple[str, str]] = []
    if "selected_cohort_level" in measure_meta_sources:
        score_optional_mapping.extend(
            (
                ("cohort_level", "cm.selected_cohort_level"),
                ("cohort_tier", "cm.selected_cohort_level"),
            )
        )
    if "selected_geography_scope" in measure_meta_sources:
        score_optional_mapping.extend(
            (
                ("cohort_geography_scope", "cm.selected_geography_scope"),
                ("cohort_geo_scope", "cm.selected_geography_scope"),
            )
        )
    if "selected_geography_value" in measure_meta_sources:
        score_optional_mapping.extend(
            (
                ("cohort_geography_value", "cm.selected_geography_value"),
                ("cohort_geo_value", "cm.selected_geography_value"),
            )
        )
    if "selected_specialty" in measure_meta_sources:
        score_optional_mapping.extend(
            (
                ("cohort_specialty", "cm.selected_specialty"),
                ("cohort_specialty_key", "cm.selected_specialty"),
            )
        )
    if "selected_taxonomy" in measure_meta_sources:
        score_optional_mapping.extend(
            (
                ("cohort_taxonomy", "cm.selected_taxonomy"),
                ("cohort_taxonomy_code", "cm.selected_taxonomy"),
            )
        )
    if "selected_classification" in measure_meta_sources:
        score_optional_mapping.append(("cohort_classification", "cm.selected_classification"))
    if "selected_procedure_bucket" in measure_meta_sources:
        score_optional_mapping.append(("cohort_procedure_bucket", "cm.selected_procedure_bucket"))
    if "selected_peer_n" in measure_meta_sources:
        score_optional_mapping.append(("cohort_peer_n", "ROUND(cm.selected_peer_n)::int"))
    score_optional_pairs = _optional_column_pairs(score_columns, tuple(score_optional_mapping))

    score_insert_columns = [
        "npi",
        "year",
        "model_version",
        "benchmark_mode",
        "risk_ratio_point",
        "ci75_low",
        "ci75_high",
        "ci90_low",
        "ci90_high",
        "score_0_100",
        "tier",
        "borderline_status",
        "low_score_threshold_failed",
        "low_confidence_threshold_failed",
        "high_score_threshold_passed",
        "high_confidence_threshold_passed",
        "estimated_cost_level",
        "run_id",
        "updated_at",
    ]
    score_select_columns = [
        "c.npi",
        "c.year",
        f"'{PROVIDER_QUALITY_MODEL_VERSION}'::varchar AS model_version",
        "c.benchmark_mode::varchar AS benchmark_mode",
        "c.rr_overall::float8 AS risk_ratio_point",
        "c.ci75_low::float8",
        "c.ci75_high::float8",
        "c.ci90_low::float8",
        "c.ci90_high::float8",
        "ROUND((LEAST(100.0, GREATEST(0.0, 50.0 - 125.0 * (c.rr_overall - 1.0))))::numeric, 2)::float8 AS score_0_100",
        "CASE WHEN c.low_score_check AND c.low_confidence_check THEN 'low' "
        "WHEN (c.low_score_check::int + c.low_confidence_check::int) = 1 THEN 'acceptable' "
        "WHEN c.high_score_check AND c.high_confidence_check THEN 'high' ELSE 'acceptable' END::varchar AS tier",
        "((c.low_score_check::int + c.low_confidence_check::int) = 1)::boolean AS borderline_status",
        "c.low_score_check",
        "c.low_confidence_check",
        "c.high_score_check",
        "c.high_confidence_check",
        "CASE WHEN c.rr_cost <= 0.80 THEN '$' WHEN c.rr_cost <= 0.90 THEN '$$' "
        "WHEN c.rr_cost <= 1.10 THEN '$$$' WHEN c.rr_cost <= 1.25 THEN '$$$$' ELSE '$$$$$' END::varchar AS estimated_cost_level",
        "CAST(:run_id AS varchar) AS run_id",
        "NOW() AS updated_at",
    ]
    score_insert_columns.extend(column for column, _ in score_optional_pairs)
    score_select_columns.extend(expr for _, expr in score_optional_pairs)
    score_insert_cols_sql = ",\n            ".join(score_insert_columns)
    score_select_cols_sql = ",\n            ".join(score_select_columns)

    benchmark_modes = _benchmark_modes_for_materialization(PROVIDER_QUALITY_BENCHMARK_MODE)
    benchmark_mode_values = ",\n                ".join(f"('{mode}'::varchar)" for mode in benchmark_modes)

    if rx_table_exists:
        provider_rx_cte = f"""
        provider_rx AS (
            SELECT
                r.npi,
                r.year,
                COALESCE(SUM(COALESCE(r.total_claims, 0.0)), 0.0)::float8 AS total_rx_claims,
                COALESCE(SUM(COALESCE(r.total_benes, 0.0)), 0.0)::float8 AS total_rx_beneficiaries
            FROM {schema}.{rx_table} r
            WHERE r.year BETWEEN {PROVIDER_QUALITY_MIN_YEAR} AND {PROVIDER_QUALITY_MAX_YEAR}
            GROUP BY r.npi, r.year
        ),
        """
    else:
        provider_rx_cte = """
        provider_rx AS (
            SELECT
                NULL::bigint AS npi,
                NULL::int AS year,
                0.0::float8 AS total_rx_claims,
                0.0::float8 AS total_rx_beneficiaries
            WHERE FALSE
        ),
        """

    await db.status(f"TRUNCATE TABLE {schema}.{feature_table};")
    await db.status(f"TRUNCATE TABLE {schema}.{lsh_table};")
    await db.status(f"TRUNCATE TABLE {schema}.{peer_target_table};")
    await db.status(f"TRUNCATE TABLE {schema}.{measure_table};")
    await db.status(f"TRUNCATE TABLE {schema}.{domain_table};")
    await db.status(f"TRUNCATE TABLE {schema}.{score_table};")

    await db.status(
        f"""
        INSERT INTO {schema}.{feature_table}
        (
            {feature_insert_cols_sql}
        )
        WITH provider_base AS (
            SELECT
                p.npi,
                p.year,
                p.provider_type
            FROM {schema}.{PricingProvider.__tablename__} p
            WHERE p.year BETWEEN {PROVIDER_QUALITY_MIN_YEAR} AND {PROVIDER_QUALITY_MAX_YEAR}
        ),
        taxonomy_ranked AS (
            SELECT
                t.npi::bigint AS npi,
                NULLIF(BTRIM(COALESCE(t.healthcare_provider_taxonomy_code, '')), '')::varchar AS taxonomy_code,
                ROW_NUMBER() OVER (
                    PARTITION BY t.npi
                    ORDER BY
                        CASE
                            WHEN UPPER(COALESCE(t.healthcare_provider_primary_taxonomy_switch, '')) = 'Y' THEN 0
                            ELSE 1
                        END,
                        t.checksum
                ) AS rn
            FROM {schema}.{NPIDataTaxonomy.__tablename__} t
            WHERE NULLIF(BTRIM(COALESCE(t.healthcare_provider_taxonomy_code, '')), '') IS NOT NULL
        ),
        taxonomy_choice AS (
            SELECT
                tr.npi,
                LOWER(BTRIM(tr.taxonomy_code))::varchar AS taxonomy
            FROM taxonomy_ranked tr
            WHERE tr.rn = 1
        ),
        procedure_counts AS (
            SELECT
                pp.npi,
                pp.year,
                COUNT(DISTINCT pp.procedure_code)::int AS procedure_count
            FROM {schema}.{PricingProviderProcedure.__tablename__} pp
            WHERE pp.year BETWEEN {PROVIDER_QUALITY_MIN_YEAR} AND {PROVIDER_QUALITY_MAX_YEAR}
            GROUP BY pp.npi, pp.year
        ),
        features_src AS (
            SELECT
                p.npi,
                p.year,
                COALESCE(NULLIF(LOWER(BTRIM(COALESCE(p.provider_type, ''))), ''), 'unknown')::varchar AS specialty,
                COALESCE(tc.taxonomy, 'unknown')::varchar AS taxonomy,
                COALESCE(NULLIF(LOWER(BTRIM(COALESCE(nt.classification, ''))), ''), 'unknown')::varchar AS classification,
                COALESCE(pc.procedure_count, 0)::int AS procedure_count,
                'bucket:none'::varchar AS procedure_bucket,
                'provider_quality'::varchar AS source,
                NOW() AS updated_at
            FROM provider_base p
            LEFT JOIN taxonomy_choice tc
              ON tc.npi = p.npi
            LEFT JOIN {schema}.{NUCCTaxonomy.__tablename__} nt
              ON LOWER(BTRIM(COALESCE(nt.code, ''))) = tc.taxonomy
            LEFT JOIN procedure_counts pc
              ON pc.npi = p.npi
             AND pc.year = p.year
        )
        SELECT
            {feature_select_cols_sql}
        FROM features_src f;
        """
    )

    await db.status(
        f"""
        INSERT INTO {schema}.{lsh_table}
        (
            {lsh_insert_cols_sql}
        )
        WITH provider_proc AS (
            SELECT DISTINCT
                pp.npi,
                pp.year,
                pp.procedure_code::varchar AS procedure_key
            FROM {schema}.{PricingProviderProcedure.__tablename__} pp
            WHERE pp.year BETWEEN {PROVIDER_QUALITY_MIN_YEAR} AND {PROVIDER_QUALITY_MAX_YEAR}
        ),
        perms AS (
            SELECT generate_series(0, {PROVIDER_QUALITY_MINHASH_NUM_PERM} - 1) AS perm_no
        ),
        signatures AS (
            SELECT
                p.npi,
                p.year,
                perm.perm_no,
                MIN(ABS(hashtextextended(p.procedure_key, perm.perm_no::bigint)))::bigint AS min_hash
            FROM provider_proc p
            CROSS JOIN perms perm
            GROUP BY p.npi, p.year, perm.perm_no
        ),
        bands AS (
            SELECT
                s.npi,
                s.year,
                FLOOR(s.perm_no / {PROVIDER_QUALITY_MINHASH_ROWS_PER_BAND})::int AS band_no,
                STRING_AGG(
                    LPAD(TO_HEX((s.min_hash % 4294967295)::bigint), 8, '0'),
                    '|' ORDER BY s.perm_no
                )::varchar AS band_signature,
                COUNT(*)::int AS rows_in_band
            FROM signatures s
            GROUP BY s.npi, s.year, FLOOR(s.perm_no / {PROVIDER_QUALITY_MINHASH_ROWS_PER_BAND})
        ),
        lsh_src AS (
            SELECT
                b.npi,
                b.year,
                b.band_no,
                MD5(b.band_signature)::varchar AS band_hash,
                b.rows_in_band,
                'provider_quality'::varchar AS source,
                NOW() AS updated_at
            FROM bands b
            WHERE b.band_no < {PROVIDER_QUALITY_MINHASH_BANDS}
        )
        SELECT
            {lsh_select_cols_sql}
        FROM lsh_src s;
        """
    )

    if feature_procedure_bucket_col:
        await db.status(
            f"""
            WITH bucket_source AS (
                SELECT
                    l.{lsh_npi_col} AS npi,
                    l.{lsh_year_col} AS year,
                    COALESCE(
                        CASE
                            WHEN COUNT(*) FILTER (WHERE l.{lsh_band_no_col} < {PROVIDER_QUALITY_PROCEDURE_MATCH_BANDS})
                                 >= {PROVIDER_QUALITY_PROCEDURE_MATCH_BANDS}
                            THEN MD5(
                                STRING_AGG(
                                    l.{lsh_band_hash_col},
                                    '|' ORDER BY l.{lsh_band_no_col}
                                ) FILTER (WHERE l.{lsh_band_no_col} < {PROVIDER_QUALITY_PROCEDURE_MATCH_BANDS})
                            )::varchar
                            ELSE NULL
                        END,
                        MAX(CASE WHEN l.{lsh_band_no_col} = 0 THEN l.{lsh_band_hash_col} END),
                        'bucket:none'
                    )::varchar AS procedure_bucket
                FROM {schema}.{lsh_table} l
                GROUP BY l.{lsh_npi_col}, l.{lsh_year_col}
            )
            UPDATE {schema}.{feature_table} f
            SET {feature_procedure_bucket_col} = b.procedure_bucket
            FROM bucket_source b
            WHERE f.{feature_npi_col} = b.npi
              AND f.{feature_year_col} = b.year;
            """
        )

    await db.status(
        f"""
        INSERT INTO {schema}.{peer_target_table}
        (
            {peer_target_insert_cols_sql}
        )
        WITH provider_base AS (
            SELECT
                p.npi,
                p.year,
                p.provider_type,
                NULLIF(BTRIM(COALESCE(p.zip5, '')), '')::varchar AS zip5,
                UPPER(NULLIF(BTRIM(COALESCE(p.state, '')), ''))::varchar AS state_key,
                COALESCE(p.total_services, 0.0)::float8 AS total_services,
                COALESCE(p.total_beneficiaries, 0.0)::float8 AS total_beneficiaries,
                COALESCE(p.total_allowed_amount, 0.0)::float8 AS total_allowed_amount,
                CASE
                    WHEN COALESCE(p.total_beneficiaries, 0.0) > 0
                    THEN COALESCE(p.total_services, 0.0)::float8 / NULLIF(COALESCE(p.total_beneficiaries, 0.0), 0.0)
                    ELSE NULL
                END AS utilization_rate
            FROM {schema}.{PricingProvider.__tablename__} p
            WHERE p.year BETWEEN {PROVIDER_QUALITY_MIN_YEAR} AND {PROVIDER_QUALITY_MAX_YEAR}
        ),
        {provider_rx_cte}
        provider_plus AS (
            SELECT
                b.*,
                q.quality_score AS qpp_quality_score,
                q.cost_score AS qpp_cost_score,
                COALESCE(rx.total_rx_claims, 0.0)::float8 AS total_rx_claims,
                COALESCE(rx.total_rx_beneficiaries, 0.0)::float8 AS total_rx_beneficiaries,
                COALESCE(s.svi_overall, {PROVIDER_QUALITY_DEFAULT_SVI}) AS svi_overall,
                COALESCE(
                    b.utilization_rate / NULLIF(1.0 + 0.2 * (COALESCE(s.svi_overall, {PROVIDER_QUALITY_DEFAULT_SVI}) - 0.5), 0.0),
                    b.utilization_rate
                ) AS utilization_adjusted,
                COALESCE(
                    b.total_allowed_amount / NULLIF(1.0 + 0.2 * (COALESCE(s.svi_overall, {PROVIDER_QUALITY_DEFAULT_SVI}) - 0.5), 0.0),
                    b.total_allowed_amount
                ) AS cost_adjusted,
                CASE
                    WHEN COALESCE(rx.total_rx_beneficiaries, 0.0) > 0
                    THEN COALESCE(rx.total_rx_claims, 0.0)::float8 / NULLIF(COALESCE(rx.total_rx_beneficiaries, 0.0), 0.0)
                    ELSE NULL
                END AS rx_claim_rate
            FROM provider_base b
            LEFT JOIN {schema}.{qpp_table} q
              ON q.npi = b.npi
             AND q.year = b.year
            LEFT JOIN provider_rx rx
              ON rx.npi = b.npi
             AND rx.year = b.year
            LEFT JOIN {schema}.{svi_table} s
              ON s.zcta = b.zip5
             AND s.year = b.year
        ),
        provider_featured AS (
            SELECT
                p.*,
                {feature_specialty_expr} AS specialty,
                {feature_taxonomy_expr} AS taxonomy,
                {feature_classification_expr} AS classification,
                {feature_procedure_bucket_expr} AS procedure_bucket
            FROM provider_plus p
            LEFT JOIN {schema}.{feature_table} f
              ON f.{feature_npi_col} = p.npi
             AND f.{feature_year_col} = p.year
        ),
        benchmark_modes AS (
            SELECT v.benchmark_mode
            FROM (
                VALUES
                {benchmark_mode_values}
            ) AS v(benchmark_mode)
        ),
        cohort_expanded AS (
            SELECT
                p.year,
                bm.benchmark_mode,
                gs.geography_scope,
                CASE
                    WHEN gs.geography_scope = 'zip' THEN p.zip5
                    WHEN gs.geography_scope = 'state' THEN p.state_key
                    ELSE 'US'
                END::varchar AS geography_value,
                lv.cohort_level,
                CASE
                    WHEN lv.cohort_level IN ('L0', 'L1', 'L2') THEN p.specialty
                    ELSE NULL
                END::varchar AS specialty,
                CASE
                    WHEN lv.cohort_level IN ('L0', 'L1') THEN p.taxonomy
                    ELSE NULL
                END::varchar AS taxonomy,
                CASE
                    WHEN lv.cohort_level IN ('L0', 'L1') THEN p.classification
                    ELSE NULL
                END::varchar AS classification,
                CASE
                    WHEN lv.cohort_level = 'L0' THEN p.procedure_bucket
                    ELSE NULL
                END::varchar AS procedure_bucket,
                p.utilization_adjusted,
                p.cost_adjusted,
                p.qpp_quality_score,
                p.qpp_cost_score,
                p.rx_claim_rate
            FROM provider_featured p
            CROSS JOIN benchmark_modes bm
            CROSS JOIN (VALUES ('national'), ('state'), ('zip')) AS gs(geography_scope)
            CROSS JOIN (VALUES ('L0'), ('L1'), ('L2'), ('L3')) AS lv(cohort_level)
            WHERE (
                (bm.benchmark_mode = 'zip' AND gs.geography_scope IN ('zip', 'state', 'national'))
                OR (bm.benchmark_mode = 'state' AND gs.geography_scope IN ('state', 'national'))
                OR (bm.benchmark_mode = 'national' AND gs.geography_scope = 'national')
            )
            AND (
                gs.geography_scope = 'national'
                OR (gs.geography_scope = 'state' AND p.state_key IS NOT NULL)
                OR (gs.geography_scope = 'zip' AND p.zip5 IS NOT NULL)
            )
        ),
        peer_target_src AS (
            SELECT
                c.year,
                c.benchmark_mode,
                c.geography_scope,
                c.geography_value,
                c.cohort_level,
                COALESCE(c.specialty, 'unknown')::varchar AS specialty,
                COALESCE(c.taxonomy, 'unknown')::varchar AS taxonomy,
                CASE
                    WHEN c.cohort_level IN ('L0', 'L1') THEN MAX(c.classification)
                    ELSE NULL
                END::varchar AS classification,
                COALESCE(c.procedure_bucket, 'bucket:none')::varchar AS procedure_bucket,
                COUNT(*)::int AS peer_n,
                AVG(NULLIF(c.utilization_adjusted, 0.0)) AS target_appropriateness,
                AVG(NULLIF(c.cost_adjusted, 0.0)) AS target_cost,
                AVG(NULLIF(c.qpp_quality_score, 0.0)) AS target_effectiveness,
                AVG(NULLIF(c.qpp_cost_score, 0.0)) AS target_qpp_cost,
                AVG(NULLIF(c.rx_claim_rate, 0.0)) AS target_rx_appropriateness,
                'provider_quality'::varchar AS source,
                NOW() AS updated_at
            FROM cohort_expanded c
            GROUP BY
                c.year,
                c.benchmark_mode,
                c.geography_scope,
                c.geography_value,
                c.cohort_level,
                COALESCE(c.specialty, 'unknown')::varchar,
                COALESCE(c.taxonomy, 'unknown')::varchar,
                COALESCE(c.procedure_bucket, 'bucket:none')::varchar
        )
        SELECT
            {peer_target_select_cols_sql}
        FROM peer_target_src t;
        """
    )

    await db.status(
        f"""
        INSERT INTO {schema}.{measure_table}
        (
            {measure_insert_cols_sql}
        )
        WITH provider_base AS (
            SELECT
                p.npi,
                p.year,
                p.provider_type,
                NULLIF(BTRIM(COALESCE(p.zip5, '')), '')::varchar AS zip5,
                UPPER(NULLIF(BTRIM(COALESCE(p.state, '')), ''))::varchar AS state_key,
                COALESCE(p.total_services, 0.0)::float8 AS total_services,
                COALESCE(p.total_beneficiaries, 0.0)::float8 AS total_beneficiaries,
                COALESCE(p.total_allowed_amount, 0.0)::float8 AS total_allowed_amount,
                CASE
                    WHEN COALESCE(p.total_beneficiaries, 0.0) > 0
                    THEN COALESCE(p.total_services, 0.0)::float8 / NULLIF(COALESCE(p.total_beneficiaries, 0.0), 0.0)
                    ELSE NULL
                END AS utilization_rate
            FROM {schema}.{PricingProvider.__tablename__} p
            WHERE p.year BETWEEN {PROVIDER_QUALITY_MIN_YEAR} AND {PROVIDER_QUALITY_MAX_YEAR}
        ),
        {provider_rx_cte}
        provider_plus AS (
            SELECT
                b.*,
                q.quality_score AS qpp_quality_score,
                q.cost_score AS qpp_cost_score,
                COALESCE(rx.total_rx_claims, 0.0)::float8 AS total_rx_claims,
                COALESCE(rx.total_rx_beneficiaries, 0.0)::float8 AS total_rx_beneficiaries,
                COALESCE(s.svi_overall, {PROVIDER_QUALITY_DEFAULT_SVI}) AS svi_overall,
                COALESCE(
                    b.utilization_rate / NULLIF(1.0 + 0.2 * (COALESCE(s.svi_overall, {PROVIDER_QUALITY_DEFAULT_SVI}) - 0.5), 0.0),
                    b.utilization_rate
                ) AS utilization_adjusted,
                COALESCE(
                    b.total_allowed_amount / NULLIF(1.0 + 0.2 * (COALESCE(s.svi_overall, {PROVIDER_QUALITY_DEFAULT_SVI}) - 0.5), 0.0),
                    b.total_allowed_amount
                ) AS cost_adjusted,
                CASE
                    WHEN COALESCE(rx.total_rx_beneficiaries, 0.0) > 0
                    THEN COALESCE(rx.total_rx_claims, 0.0)::float8 / NULLIF(COALESCE(rx.total_rx_beneficiaries, 0.0), 0.0)
                    ELSE NULL
                END AS rx_claim_rate
            FROM provider_base b
            LEFT JOIN {schema}.{qpp_table} q
              ON q.npi = b.npi
             AND q.year = b.year
            LEFT JOIN provider_rx rx
              ON rx.npi = b.npi
             AND rx.year = b.year
            LEFT JOIN {schema}.{svi_table} s
              ON s.zcta = b.zip5
             AND s.year = b.year
        ),
        provider_featured AS (
            SELECT
                p.*,
                {feature_specialty_expr} AS specialty,
                {feature_taxonomy_expr} AS taxonomy,
                {feature_classification_expr} AS classification,
                {feature_procedure_bucket_expr} AS procedure_bucket
            FROM provider_plus p
            LEFT JOIN {schema}.{feature_table} f
              ON f.{feature_npi_col} = p.npi
             AND f.{feature_year_col} = p.year
        ),
        benchmark_modes AS (
            SELECT v.benchmark_mode
            FROM (
                VALUES
                {benchmark_mode_values}
            ) AS v(benchmark_mode)
        ),
        target_candidates AS (
            SELECT
                p.npi,
                p.year,
                bm.benchmark_mode,
                ({peer_scope_expr})::varchar AS selected_geography_scope,
                ({peer_geo_value_expr})::varchar AS selected_geography_value,
                ({peer_cohort_level_expr})::varchar AS selected_cohort_level,
                ({peer_specialty_expr})::varchar AS selected_specialty,
                ({peer_taxonomy_expr})::varchar AS selected_taxonomy,
                ({peer_classification_expr})::varchar AS selected_classification,
                ({peer_procedure_bucket_expr})::varchar AS selected_procedure_bucket,
                ({peer_peer_n_expr})::float8 AS selected_peer_n,
                t.{peer_target_appropr_col}::float8 AS target_appropriateness,
                t.{peer_target_cost_col}::float8 AS target_cost,
                t.{peer_target_effect_col}::float8 AS target_effectiveness,
                t.{peer_target_qpp_cost_col}::float8 AS target_qpp_cost,
                t.{peer_target_rx_appropr_col}::float8 AS target_rx_appropriateness,
                CASE
                    WHEN bm.benchmark_mode = 'zip' THEN
                        CASE
                            WHEN {peer_scope_expr} = 'zip' THEN 1
                            WHEN {peer_scope_expr} = 'state' THEN 2
                            WHEN {peer_scope_expr} = 'national' THEN 3
                            ELSE 9
                        END
                    WHEN bm.benchmark_mode = 'state' THEN
                        CASE
                            WHEN {peer_scope_expr} = 'state' THEN 1
                            WHEN {peer_scope_expr} = 'national' THEN 2
                            ELSE 9
                        END
                    ELSE
                        CASE WHEN {peer_scope_expr} = 'national' THEN 1 ELSE 9 END
                END AS geography_rank,
                CASE
                    WHEN {peer_cohort_level_expr} = 'L0' THEN 1
                    WHEN {peer_cohort_level_expr} = 'L1' THEN 2
                    WHEN {peer_cohort_level_expr} = 'L2' THEN 3
                    ELSE 4
                END AS level_rank,
                CASE
                    WHEN {peer_scope_expr} = 'zip' THEN ({peer_peer_n_expr}) >= {PROVIDER_QUALITY_ZIP_MIN_PEER_N}
                    WHEN {peer_scope_expr} = 'state' THEN ({peer_peer_n_expr}) >= {PROVIDER_QUALITY_MIN_STATE_PEER_N}
                    ELSE ({peer_peer_n_expr}) >= {PROVIDER_QUALITY_NATIONAL_MIN_PEER_N}
                END AS threshold_met
            FROM provider_featured p
            CROSS JOIN benchmark_modes bm
            JOIN {schema}.{peer_target_table} t
              ON t.{peer_target_year_col} = p.year
             AND {peer_benchmark_mode_expr} = bm.benchmark_mode
             AND (
                    ({peer_scope_expr} = 'zip' AND bm.benchmark_mode = 'zip' AND p.zip5 IS NOT NULL AND {peer_geo_value_expr} = p.zip5)
                    OR (
                        {peer_scope_expr} = 'state'
                        AND bm.benchmark_mode IN ('zip', 'state')
                        AND p.state_key IS NOT NULL
                        AND UPPER({peer_geo_value_expr}) = p.state_key
                    )
                    OR ({peer_scope_expr} = 'national' AND bm.benchmark_mode IN ('zip', 'state', 'national'))
                )
             AND (
                    ({peer_cohort_level_expr} = 'L0' AND {peer_specialty_match} AND {peer_taxonomy_match} AND {peer_procedure_bucket_match})
                    OR ({peer_cohort_level_expr} = 'L1' AND {peer_specialty_match} AND {peer_taxonomy_match})
                    OR ({peer_cohort_level_expr} = 'L2' AND {peer_specialty_match})
                    OR ({peer_cohort_level_expr} = 'L3')
                )
        ),
        target_selected AS (
            SELECT DISTINCT ON (c.npi, c.year, c.benchmark_mode)
                c.npi,
                c.year,
                c.benchmark_mode,
                c.selected_geography_scope,
                c.selected_geography_value,
                c.selected_cohort_level,
                c.selected_specialty,
                c.selected_taxonomy,
                c.selected_classification,
                c.selected_procedure_bucket,
                c.selected_peer_n,
                c.target_appropriateness,
                c.target_cost,
                c.target_effectiveness,
                c.target_qpp_cost,
                c.target_rx_appropriateness
            FROM target_candidates c
            WHERE c.geography_rank < 9
            ORDER BY
                c.npi,
                c.year,
                c.benchmark_mode,
                CASE WHEN c.threshold_met THEN 0 ELSE 1 END,
                c.geography_rank,
                c.level_rank,
                c.selected_peer_n DESC,
                c.selected_geography_scope,
                c.selected_geography_value,
                c.selected_cohort_level
        ),
        measure_inputs AS (
            SELECT
                p.npi,
                p.year,
                ts.benchmark_mode,
                p.total_services,
                p.total_beneficiaries,
                p.utilization_adjusted,
                p.cost_adjusted,
                p.qpp_quality_score,
                p.qpp_cost_score,
                p.rx_claim_rate,
                ts.target_appropriateness,
                ts.target_cost,
                ts.target_effectiveness,
                ts.target_qpp_cost,
                ts.target_rx_appropriateness,
                ts.selected_cohort_level,
                ts.selected_geography_scope,
                ts.selected_geography_value,
                ts.selected_specialty,
                ts.selected_taxonomy,
                ts.selected_classification,
                ts.selected_procedure_bucket,
                ts.selected_peer_n,
                (
                    'cohort:'
                    || ts.selected_geography_scope
                    || ':'
                    || COALESCE(ts.selected_geography_value, 'US')
                    || ':'
                    || ts.selected_cohort_level
                )::varchar AS peer_group,
                GREATEST(COALESCE(p.total_services, 0.0), COALESCE(p.total_beneficiaries, 0.0), 1.0) AS evidence_n_claims,
                GREATEST(COALESCE(p.total_rx_claims, 0.0), COALESCE(p.total_rx_beneficiaries, 0.0), 1.0) AS evidence_n_rx
            FROM provider_featured p
            JOIN target_selected ts
              ON ts.npi = p.npi
             AND ts.year = p.year
        ),
        expanded AS (
            SELECT
                npi,
                year,
                benchmark_mode,
                'appropriateness_utilization'::varchar AS measure_id,
                'appropriateness'::varchar AS domain,
                utilization_adjusted::float8 AS observed_value,
                target_appropriateness::float8 AS target_value,
                CASE
                    WHEN target_appropriateness IS NULL OR target_appropriateness <= 0 THEN 1.0
                    WHEN utilization_adjusted IS NULL OR utilization_adjusted <= 0 THEN 1.0
                    ELSE utilization_adjusted / target_appropriateness
                END AS rr_raw,
                evidence_n_claims AS evidence_n,
                peer_group,
                'higher_worse'::varchar AS direction,
                'claims_pricing'::varchar AS source,
                selected_cohort_level,
                selected_geography_scope,
                selected_geography_value,
                selected_specialty,
                selected_taxonomy,
                selected_classification,
                selected_procedure_bucket,
                selected_peer_n
            FROM measure_inputs
            UNION ALL
            SELECT
                npi,
                year,
                benchmark_mode,
                'appropriateness_drug_proxy'::varchar AS measure_id,
                'appropriateness'::varchar AS domain,
                rx_claim_rate::float8 AS observed_value,
                target_rx_appropriateness::float8 AS target_value,
                CASE
                    WHEN target_rx_appropriateness IS NULL OR target_rx_appropriateness <= 0 THEN 1.0
                    WHEN rx_claim_rate IS NULL OR rx_claim_rate <= 0 THEN 1.0
                    ELSE rx_claim_rate / target_rx_appropriateness
                END AS rr_raw,
                evidence_n_rx AS evidence_n,
                peer_group,
                'higher_worse'::varchar AS direction,
                'drug_claims'::varchar AS source,
                selected_cohort_level,
                selected_geography_scope,
                selected_geography_value,
                selected_specialty,
                selected_taxonomy,
                selected_classification,
                selected_procedure_bucket,
                selected_peer_n
            FROM measure_inputs
            UNION ALL
            SELECT
                npi,
                year,
                benchmark_mode,
                'effectiveness_qpp_quality'::varchar AS measure_id,
                'effectiveness'::varchar AS domain,
                qpp_quality_score::float8 AS observed_value,
                target_effectiveness::float8 AS target_value,
                CASE
                    WHEN qpp_quality_score IS NULL OR qpp_quality_score <= 0 THEN 1.0
                    WHEN target_effectiveness IS NULL OR target_effectiveness <= 0 THEN 1.0
                    ELSE target_effectiveness / qpp_quality_score
                END AS rr_raw,
                evidence_n_claims AS evidence_n,
                peer_group,
                'lower_worse'::varchar AS direction,
                'qpp'::varchar AS source,
                selected_cohort_level,
                selected_geography_scope,
                selected_geography_value,
                selected_specialty,
                selected_taxonomy,
                selected_classification,
                selected_procedure_bucket,
                selected_peer_n
            FROM measure_inputs
            UNION ALL
            SELECT
                npi,
                year,
                benchmark_mode,
                'cost_qpp_component'::varchar AS measure_id,
                'cost'::varchar AS domain,
                qpp_cost_score::float8 AS observed_value,
                target_qpp_cost::float8 AS target_value,
                CASE
                    WHEN qpp_cost_score IS NULL OR qpp_cost_score <= 0 THEN 1.0
                    WHEN target_qpp_cost IS NULL OR target_qpp_cost <= 0 THEN 1.0
                    ELSE target_qpp_cost / qpp_cost_score
                END AS rr_raw,
                evidence_n_claims AS evidence_n,
                peer_group,
                'lower_worse'::varchar AS direction,
                'qpp'::varchar AS source,
                selected_cohort_level,
                selected_geography_scope,
                selected_geography_value,
                selected_specialty,
                selected_taxonomy,
                selected_classification,
                selected_procedure_bucket,
                selected_peer_n
            FROM measure_inputs
            UNION ALL
            SELECT
                npi,
                year,
                benchmark_mode,
                'cost_allowed_amount'::varchar AS measure_id,
                'cost'::varchar AS domain,
                cost_adjusted::float8 AS observed_value,
                target_cost::float8 AS target_value,
                CASE
                    WHEN target_cost IS NULL OR target_cost <= 0 THEN 1.0
                    WHEN cost_adjusted IS NULL OR cost_adjusted <= 0 THEN 1.0
                    ELSE cost_adjusted / target_cost
                END AS rr_raw,
                evidence_n_claims AS evidence_n,
                peer_group,
                'higher_worse'::varchar AS direction,
                'claims_pricing'::varchar AS source,
                selected_cohort_level,
                selected_geography_scope,
                selected_geography_value,
                selected_specialty,
                selected_taxonomy,
                selected_classification,
                selected_procedure_bucket,
                selected_peer_n
            FROM measure_inputs
        ),
        shrunk AS (
            SELECT
                e.*,
                LEAST(1.0, GREATEST(0.0, e.evidence_n / NULLIF(e.evidence_n + {PROVIDER_QUALITY_SHRINKAGE_ALPHA}, 0.0))) AS shrink_weight,
                GREATEST(0.0001, SQRT(1.0 / NULLIF(e.evidence_n + 1.0, 0.0))) AS stderr
            FROM expanded e
        )
        SELECT
            {measure_select_cols_sql}
        FROM shrunk s;
        """
    )

    await db.status(
        f"""
        INSERT INTO {schema}.{domain_table}
        (
            npi,
            year,
            benchmark_mode,
            domain,
            risk_ratio,
            score_0_100,
            ci75_low,
            ci75_high,
            ci90_low,
            ci90_high,
            evidence_n,
            measure_count,
            updated_at
        )
        SELECT
            m.npi,
            m.year,
            m.benchmark_mode,
            m.domain,
            EXP(AVG(LN(GREATEST(m.risk_ratio, 0.0001))))::float8 AS risk_ratio,
            ROUND((LEAST(100.0, GREATEST(0.0, 50.0 - 125.0 * (EXP(AVG(LN(GREATEST(m.risk_ratio, 0.0001)))) - 1.0))))::numeric, 2)::float8 AS score_0_100,
            EXP(AVG(LN(GREATEST(m.ci75_low, 0.0001))))::float8 AS ci75_low,
            EXP(AVG(LN(GREATEST(m.ci75_high, 0.0001))))::float8 AS ci75_high,
            EXP(AVG(LN(GREATEST(m.ci90_low, 0.0001))))::float8 AS ci90_low,
            EXP(AVG(LN(GREATEST(m.ci90_high, 0.0001))))::float8 AS ci90_high,
            SUM(COALESCE(m.evidence_n, 0))::int AS evidence_n,
            COUNT(*)::int AS measure_count,
            NOW() AS updated_at
        FROM {schema}.{measure_table} m
        GROUP BY m.npi, m.year, m.benchmark_mode, m.domain;
        """
    )

    cohort_meta_cte = ""
    cohort_meta_join = ""
    if score_optional_pairs and measure_meta_sources:
        cohort_meta_cte = (
            ",\n        cohort_meta AS (\n"
            "            SELECT\n"
            "                m.npi,\n"
            "                m.year,\n"
            "                m.benchmark_mode,\n"
            + ",\n".join(
                f"                MAX(m.{column_name}) AS {alias}"
                for alias, column_name in measure_meta_sources.items()
            )
            + f"\n            FROM {schema}.{measure_table} m\n"
            "            GROUP BY m.npi, m.year, m.benchmark_mode\n"
            "        )\n"
        )
        cohort_meta_join = (
            "\n        LEFT JOIN cohort_meta cm\n"
            "          ON cm.npi = c.npi\n"
            "         AND cm.year = c.year\n"
            "         AND cm.benchmark_mode = c.benchmark_mode"
        )

    await db.status(
        f"""
        INSERT INTO {schema}.{score_table}
        (
            {score_insert_cols_sql}
        )
        WITH domain_pivot AS (
            SELECT
                d.npi,
                d.year,
                d.benchmark_mode,
                MAX(CASE WHEN d.domain = 'appropriateness' THEN d.risk_ratio END) AS rr_appropr,
                MAX(CASE WHEN d.domain = 'effectiveness' THEN d.risk_ratio END) AS rr_effect,
                MAX(CASE WHEN d.domain = 'cost' THEN d.risk_ratio END) AS rr_cost,
                MAX(CASE WHEN d.domain = 'appropriateness' THEN d.ci75_low END) AS ci75_appropr_low,
                MAX(CASE WHEN d.domain = 'effectiveness' THEN d.ci75_low END) AS ci75_effect_low,
                MAX(CASE WHEN d.domain = 'cost' THEN d.ci75_low END) AS ci75_cost_low,
                MAX(CASE WHEN d.domain = 'appropriateness' THEN d.ci75_high END) AS ci75_appropr_high,
                MAX(CASE WHEN d.domain = 'effectiveness' THEN d.ci75_high END) AS ci75_effect_high,
                MAX(CASE WHEN d.domain = 'cost' THEN d.ci75_high END) AS ci75_cost_high,
                MAX(CASE WHEN d.domain = 'appropriateness' THEN d.ci90_low END) AS ci90_appropr_low,
                MAX(CASE WHEN d.domain = 'effectiveness' THEN d.ci90_low END) AS ci90_effect_low,
                MAX(CASE WHEN d.domain = 'cost' THEN d.ci90_low END) AS ci90_cost_low,
                MAX(CASE WHEN d.domain = 'appropriateness' THEN d.ci90_high END) AS ci90_appropr_high,
                MAX(CASE WHEN d.domain = 'effectiveness' THEN d.ci90_high END) AS ci90_effect_high,
                MAX(CASE WHEN d.domain = 'cost' THEN d.ci90_high END) AS ci90_cost_high
            FROM {schema}.{domain_table} d
            GROUP BY d.npi, d.year, d.benchmark_mode
        ){cohort_meta_cte},
        blended AS (
            SELECT
                p.npi,
                p.year,
                p.benchmark_mode,
                COALESCE(p.rr_appropr, 1.0) AS rr_appropr,
                COALESCE(p.rr_effect, 1.0) AS rr_effect,
                COALESCE(p.rr_cost, 1.0) AS rr_cost,
                SQRT(COALESCE(p.rr_appropr, 1.0) * COALESCE(p.rr_effect, 1.0)) AS rr_clinical,
                EXP(
                    0.5 * LN(GREATEST(SQRT(COALESCE(p.rr_appropr, 1.0) * COALESCE(p.rr_effect, 1.0)), 0.0001))
                    + 0.5 * LN(GREATEST(COALESCE(p.rr_cost, 1.0), 0.0001))
                ) AS rr_overall,
                EXP(
                    0.5 * LN(GREATEST(SQRT(COALESCE(p.ci75_appropr_low, 1.0) * COALESCE(p.ci75_effect_low, 1.0)), 0.0001))
                    + 0.5 * LN(GREATEST(COALESCE(p.ci75_cost_low, 1.0), 0.0001))
                ) AS ci75_low,
                EXP(
                    0.5 * LN(GREATEST(SQRT(COALESCE(p.ci75_appropr_high, 1.0) * COALESCE(p.ci75_effect_high, 1.0)), 0.0001))
                    + 0.5 * LN(GREATEST(COALESCE(p.ci75_cost_high, 1.0), 0.0001))
                ) AS ci75_high,
                EXP(
                    0.5 * LN(GREATEST(SQRT(COALESCE(p.ci90_appropr_low, 1.0) * COALESCE(p.ci90_effect_low, 1.0)), 0.0001))
                    + 0.5 * LN(GREATEST(COALESCE(p.ci90_cost_low, 1.0), 0.0001))
                ) AS ci90_low,
                EXP(
                    0.5 * LN(GREATEST(SQRT(COALESCE(p.ci90_appropr_high, 1.0) * COALESCE(p.ci90_effect_high, 1.0)), 0.0001))
                    + 0.5 * LN(GREATEST(COALESCE(p.ci90_cost_high, 1.0), 0.0001))
                ) AS ci90_high
            FROM domain_pivot p
        ),
        checks AS (
            SELECT
                b.*,
                (b.rr_overall >= 1.12)::boolean AS low_score_check,
                (b.ci90_low >= 1.08)::boolean AS low_confidence_check,
                (b.rr_overall <= 0.88)::boolean AS high_score_check,
                (b.ci75_high < 1.0)::boolean AS high_confidence_check
            FROM blended b
        )
        SELECT
            {score_select_cols_sql}
        FROM checks c{cohort_meta_join};
        """,
        run_id=run_id,
    )


async def _materialize_quality_rows(classes: dict[str, type], schema: str, run_id: str) -> None:
    if PROVIDER_QUALITY_COHORT_ENABLED and _cohort_models_present(classes):
        await _materialize_quality_rows_cohort(classes, schema, run_id)
        return
    if PROVIDER_QUALITY_COHORT_ENABLED and not _cohort_models_present(classes):
        logger.warning(
            "Provider-quality cohort mode requested, but cohort models are unavailable; using legacy geo-only materialization",
        )

    qpp_table = classes["PricingQppProvider"].__tablename__
    svi_table = classes["PricingSviZcta"].__tablename__
    rx_table = PricingProviderPrescription.__tablename__
    geo_zip_table = GeoZipLookup.__tablename__
    measure_table = classes["PricingProviderQualityMeasure"].__tablename__
    domain_table = classes["PricingProviderQualityDomain"].__tablename__
    score_table = classes["PricingProviderQualityScore"].__tablename__
    rx_table_exists = await _table_exists(schema, rx_table)
    geo_zip_table_exists = await _table_exists(schema, geo_zip_table)
    if rx_table_exists:
        provider_rx_cte = f"""
        provider_rx AS (
            SELECT
                r.npi,
                r.year,
                COALESCE(SUM(COALESCE(r.total_claims, 0.0)), 0.0)::float8 AS total_rx_claims,
                COALESCE(SUM(COALESCE(r.total_benes, 0.0)), 0.0)::float8 AS total_rx_beneficiaries
            FROM {schema}.{rx_table} r
            WHERE r.year BETWEEN {PROVIDER_QUALITY_MIN_YEAR} AND {PROVIDER_QUALITY_MAX_YEAR}
            GROUP BY r.npi, r.year
        ),
        """
    else:
        provider_rx_cte = """
        provider_rx AS (
            SELECT
                NULL::bigint AS npi,
                NULL::int AS year,
                0.0::float8 AS total_rx_claims,
                0.0::float8 AS total_rx_beneficiaries
            WHERE FALSE
        ),
        """
    if geo_zip_table_exists:
        geo_zip_cte = f"""
        geo_zip_coords AS (
            SELECT
                z.zip_code::varchar AS zip5,
                z.latitude::float8 AS latitude,
                z.longitude::float8 AS longitude
            FROM {schema}.{geo_zip_table} z
            WHERE z.latitude IS NOT NULL
              AND z.longitude IS NOT NULL
        ),
        """
    else:
        geo_zip_cte = """
        geo_zip_coords AS (
            SELECT
                NULL::varchar AS zip5,
                NULL::float8 AS latitude,
                NULL::float8 AS longitude
            WHERE FALSE
        ),
        """
    benchmark_modes = _benchmark_modes_for_materialization(PROVIDER_QUALITY_BENCHMARK_MODE)
    benchmark_mode_values = ",\n                ".join(f"('{mode}'::varchar)" for mode in benchmark_modes)

    await db.status(f"TRUNCATE TABLE {schema}.{measure_table};")
    await db.status(f"TRUNCATE TABLE {schema}.{domain_table};")
    await db.status(f"TRUNCATE TABLE {schema}.{score_table};")

    await db.status(
        f"""
        INSERT INTO {schema}.{measure_table}
        (
            npi,
            year,
            benchmark_mode,
            measure_id,
            domain,
            observed,
            target,
            risk_ratio,
            ci75_low,
            ci75_high,
            ci90_low,
            ci90_high,
            evidence_n,
            peer_group,
            direction,
            source,
            updated_at
        )
        WITH provider_base AS (
            SELECT
                p.npi,
                p.year,
                p.zip5,
                UPPER(NULLIF(TRIM(COALESCE(p.state, '')), ''))::varchar AS state_key,
                COALESCE(p.total_services, 0.0)::float8 AS total_services,
                COALESCE(p.total_beneficiaries, 0.0)::float8 AS total_beneficiaries,
                COALESCE(p.total_allowed_amount, 0.0)::float8 AS total_allowed_amount,
                CASE
                    WHEN COALESCE(p.total_beneficiaries, 0.0) > 0
                    THEN COALESCE(p.total_services, 0.0)::float8 / NULLIF(COALESCE(p.total_beneficiaries, 0.0), 0.0)
                    ELSE NULL
                END AS utilization_rate
            FROM {schema}.{PricingProvider.__tablename__} p
            WHERE p.year BETWEEN {PROVIDER_QUALITY_MIN_YEAR} AND {PROVIDER_QUALITY_MAX_YEAR}
        ),
        {provider_rx_cte}
        {geo_zip_cte}
        provider_plus AS (
            SELECT
                b.npi,
                b.year,
                NULLIF(TRIM(COALESCE(b.zip5, '')), '')::varchar AS zip5,
                b.state_key,
                b.total_services,
                b.total_beneficiaries,
                b.total_allowed_amount,
                b.utilization_rate,
                COALESCE(q.quality_score, NULL) AS qpp_quality_score,
                COALESCE(q.cost_score, NULL) AS qpp_cost_score,
                COALESCE(rx.total_rx_claims, 0.0)::float8 AS total_rx_claims,
                COALESCE(rx.total_rx_beneficiaries, 0.0)::float8 AS total_rx_beneficiaries,
                COALESCE(s.svi_overall, {PROVIDER_QUALITY_DEFAULT_SVI}) AS svi_overall,
                COALESCE(
                    b.utilization_rate / NULLIF(1.0 + 0.2 * (COALESCE(s.svi_overall, {PROVIDER_QUALITY_DEFAULT_SVI}) - 0.5), 0.0),
                    b.utilization_rate
                ) AS utilization_adjusted,
                COALESCE(
                    b.total_allowed_amount / NULLIF(1.0 + 0.2 * (COALESCE(s.svi_overall, {PROVIDER_QUALITY_DEFAULT_SVI}) - 0.5), 0.0),
                    b.total_allowed_amount
                ) AS cost_adjusted,
                CASE
                    WHEN COALESCE(rx.total_rx_beneficiaries, 0.0) > 0
                    THEN COALESCE(rx.total_rx_claims, 0.0)::float8 / NULLIF(COALESCE(rx.total_rx_beneficiaries, 0.0), 0.0)
                    ELSE NULL
                END AS rx_claim_rate
            FROM provider_base b
            LEFT JOIN {schema}.{qpp_table} q
              ON q.npi = b.npi
             AND q.year = b.year
            LEFT JOIN provider_rx rx
              ON rx.npi = b.npi
             AND rx.year = b.year
            LEFT JOIN {schema}.{svi_table} s
              ON s.zcta = b.zip5
             AND s.year = b.year
        ),
        peers_national AS (
            SELECT
                year,
                COUNT(*)::int AS peer_n,
                AVG(NULLIF(utilization_adjusted, 0.0)) AS target_appropriateness,
                AVG(NULLIF(cost_adjusted, 0.0)) AS target_cost,
                AVG(NULLIF(qpp_quality_score, 0.0)) AS target_effectiveness,
                AVG(NULLIF(qpp_cost_score, 0.0)) AS target_qpp_cost,
                AVG(NULLIF(rx_claim_rate, 0.0)) AS target_rx_appropriateness
            FROM provider_plus
            GROUP BY year
        ),
        peers_state AS (
            SELECT
                year,
                state_key,
                COUNT(*)::int AS peer_n,
                AVG(NULLIF(utilization_adjusted, 0.0)) AS target_appropriateness,
                AVG(NULLIF(cost_adjusted, 0.0)) AS target_cost,
                AVG(NULLIF(qpp_quality_score, 0.0)) AS target_effectiveness,
                AVG(NULLIF(qpp_cost_score, 0.0)) AS target_qpp_cost,
                AVG(NULLIF(rx_claim_rate, 0.0)) AS target_rx_appropriateness
            FROM provider_plus
            WHERE state_key IS NOT NULL
            GROUP BY year, state_key
        ),
        peers_zip_exact AS (
            SELECT
                year,
                zip5,
                state_key,
                COUNT(*)::int AS peer_n,
                AVG(NULLIF(utilization_adjusted, 0.0)) AS target_appropriateness,
                AVG(NULLIF(cost_adjusted, 0.0)) AS target_cost,
                AVG(NULLIF(qpp_quality_score, 0.0)) AS target_effectiveness,
                AVG(NULLIF(qpp_cost_score, 0.0)) AS target_qpp_cost,
                AVG(NULLIF(rx_claim_rate, 0.0)) AS target_rx_appropriateness
            FROM provider_plus
            WHERE zip5 IS NOT NULL
              AND state_key IS NOT NULL
            GROUP BY year, zip5, state_key
        ),
        zip_neighbor_candidates AS (
            SELECT
                a.year,
                a.zip5 AS anchor_zip5,
                a.state_key AS anchor_state_key,
                b.zip5 AS neighbor_zip5,
                b.peer_n,
                b.target_appropriateness,
                b.target_cost,
                b.target_effectiveness,
                b.target_qpp_cost,
                b.target_rx_appropriateness,
                (
                    69.0 * SQRT(
                        POWER(za.latitude - zb.latitude, 2)
                        + POWER(
                            (za.longitude - zb.longitude) * COS(RADIANS((za.latitude + zb.latitude) / 2.0)),
                            2
                        )
                    )
                )::float8 AS distance_miles
            FROM peers_zip_exact a
            JOIN peers_zip_exact b
              ON b.year = a.year
             AND b.state_key = a.state_key
            JOIN geo_zip_coords za
              ON za.zip5 = a.zip5
            JOIN geo_zip_coords zb
              ON zb.zip5 = b.zip5
             AND ABS(za.latitude - zb.latitude) <= ({PROVIDER_QUALITY_ZIP_MAX_RADIUS_MILES} / 69.0)
        ),
        zip_neighbor_filtered AS (
            SELECT
                year,
                anchor_zip5,
                anchor_state_key,
                neighbor_zip5,
                peer_n,
                target_appropriateness,
                target_cost,
                target_effectiveness,
                target_qpp_cost,
                target_rx_appropriateness,
                CASE
                    WHEN anchor_zip5 = neighbor_zip5 THEN 0
                    ELSE LEAST(
                        {PROVIDER_QUALITY_ZIP_MAX_RING},
                        GREATEST(
                            1,
                            CEIL(distance_miles / {PROVIDER_QUALITY_ZIP_RADIUS_STEP_MILES})::int
                        )
                    )
                END AS ring
            FROM zip_neighbor_candidates
            WHERE distance_miles <= {PROVIDER_QUALITY_ZIP_MAX_RADIUS_MILES}
        ),
        zip_ring_rollup AS (
            SELECT
                year,
                anchor_zip5,
                anchor_state_key,
                ring,
                SUM(peer_n)::int AS ring_peer_n,
                SUM(COALESCE(target_appropriateness, 1.0) * peer_n)::float8 AS ring_appropr_weighted,
                SUM(COALESCE(target_cost, 1.0) * peer_n)::float8 AS ring_cost_weighted,
                SUM(COALESCE(target_effectiveness, 1.0) * peer_n)::float8 AS ring_effect_weighted,
                SUM(COALESCE(target_qpp_cost, 1.0) * peer_n)::float8 AS ring_qpp_cost_weighted,
                SUM(COALESCE(target_rx_appropriateness, 1.0) * peer_n)::float8 AS ring_rx_appropr_weighted
            FROM zip_neighbor_filtered
            GROUP BY year, anchor_zip5, anchor_state_key, ring
        ),
        zip_cumulative AS (
            SELECT
                year,
                anchor_zip5,
                anchor_state_key,
                ring,
                SUM(ring_peer_n) OVER (
                    PARTITION BY year, anchor_zip5
                    ORDER BY ring
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                )::int AS cum_peer_n,
                SUM(ring_appropr_weighted) OVER (
                    PARTITION BY year, anchor_zip5
                    ORDER BY ring
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                )::float8 AS cum_appropr_weighted,
                SUM(ring_cost_weighted) OVER (
                    PARTITION BY year, anchor_zip5
                    ORDER BY ring
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                )::float8 AS cum_cost_weighted,
                SUM(ring_effect_weighted) OVER (
                    PARTITION BY year, anchor_zip5
                    ORDER BY ring
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                )::float8 AS cum_effect_weighted,
                SUM(ring_qpp_cost_weighted) OVER (
                    PARTITION BY year, anchor_zip5
                    ORDER BY ring
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                )::float8 AS cum_qpp_cost_weighted,
                SUM(ring_rx_appropr_weighted) OVER (
                    PARTITION BY year, anchor_zip5
                    ORDER BY ring
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                )::float8 AS cum_rx_appropr_weighted
            FROM zip_ring_rollup
        ),
        zip_choice AS (
            SELECT DISTINCT ON (year, anchor_zip5)
                year,
                anchor_zip5 AS zip5,
                anchor_state_key AS state_key,
                ring AS selected_ring,
                cum_peer_n AS peer_n,
                (cum_appropr_weighted / NULLIF(cum_peer_n, 0))::float8 AS target_appropriateness,
                (cum_cost_weighted / NULLIF(cum_peer_n, 0))::float8 AS target_cost,
                (cum_effect_weighted / NULLIF(cum_peer_n, 0))::float8 AS target_effectiveness,
                (cum_qpp_cost_weighted / NULLIF(cum_peer_n, 0))::float8 AS target_qpp_cost,
                (cum_rx_appropr_weighted / NULLIF(cum_peer_n, 0))::float8 AS target_rx_appropriateness
            FROM zip_cumulative
            WHERE cum_peer_n >= {PROVIDER_QUALITY_ZIP_MIN_PEER_N}
            ORDER BY year, anchor_zip5, ring
        ),
        benchmark_modes AS (
            SELECT v.benchmark_mode
            FROM (
                VALUES
                {benchmark_mode_values}
            ) AS v(benchmark_mode)
        ),
        measure_inputs AS (
            SELECT
                p.npi,
                p.year,
                bm.benchmark_mode,
                p.total_services,
                p.total_beneficiaries,
                p.utilization_adjusted,
                p.cost_adjusted,
                p.qpp_quality_score,
                p.qpp_cost_score,
                p.rx_claim_rate,
                CASE
                    WHEN bm.benchmark_mode = 'zip'
                         AND COALESCE(zc.peer_n, 0) >= {PROVIDER_QUALITY_ZIP_MIN_PEER_N}
                    THEN zc.target_appropriateness
                    WHEN bm.benchmark_mode IN ('state', 'zip')
                         AND COALESCE(ps.peer_n, 0) >= {PROVIDER_QUALITY_MIN_STATE_PEER_N}
                    THEN ps.target_appropriateness
                    ELSE pn.target_appropriateness
                END AS target_appropriateness,
                CASE
                    WHEN bm.benchmark_mode = 'zip'
                         AND COALESCE(zc.peer_n, 0) >= {PROVIDER_QUALITY_ZIP_MIN_PEER_N}
                    THEN zc.target_cost
                    WHEN bm.benchmark_mode IN ('state', 'zip')
                         AND COALESCE(ps.peer_n, 0) >= {PROVIDER_QUALITY_MIN_STATE_PEER_N}
                    THEN ps.target_cost
                    ELSE pn.target_cost
                END AS target_cost,
                CASE
                    WHEN bm.benchmark_mode = 'zip'
                         AND COALESCE(zc.peer_n, 0) >= {PROVIDER_QUALITY_ZIP_MIN_PEER_N}
                    THEN zc.target_effectiveness
                    WHEN bm.benchmark_mode IN ('state', 'zip')
                         AND COALESCE(ps.peer_n, 0) >= {PROVIDER_QUALITY_MIN_STATE_PEER_N}
                    THEN ps.target_effectiveness
                    ELSE pn.target_effectiveness
                END AS target_effectiveness,
                CASE
                    WHEN bm.benchmark_mode = 'zip'
                         AND COALESCE(zc.peer_n, 0) >= {PROVIDER_QUALITY_ZIP_MIN_PEER_N}
                    THEN zc.target_qpp_cost
                    WHEN bm.benchmark_mode IN ('state', 'zip')
                         AND COALESCE(ps.peer_n, 0) >= {PROVIDER_QUALITY_MIN_STATE_PEER_N}
                    THEN ps.target_qpp_cost
                    ELSE pn.target_qpp_cost
                END AS target_qpp_cost,
                CASE
                    WHEN bm.benchmark_mode = 'zip'
                         AND COALESCE(zc.peer_n, 0) >= {PROVIDER_QUALITY_ZIP_MIN_PEER_N}
                    THEN zc.target_rx_appropriateness
                    WHEN bm.benchmark_mode IN ('state', 'zip')
                         AND COALESCE(ps.peer_n, 0) >= {PROVIDER_QUALITY_MIN_STATE_PEER_N}
                    THEN ps.target_rx_appropriateness
                    ELSE pn.target_rx_appropriateness
                END AS target_rx_appropriateness,
                CASE
                    WHEN bm.benchmark_mode = 'zip'
                         AND COALESCE(zc.peer_n, 0) >= {PROVIDER_QUALITY_ZIP_MIN_PEER_N}
                         AND p.zip5 IS NOT NULL
                         AND COALESCE(zc.selected_ring, 0) = 0
                    THEN ('zip:' || p.zip5)
                    WHEN bm.benchmark_mode = 'zip'
                         AND COALESCE(zc.peer_n, 0) >= {PROVIDER_QUALITY_ZIP_MIN_PEER_N}
                         AND p.zip5 IS NOT NULL
                    THEN ('zip_ring:' || p.zip5 || ':' || 'r' || zc.selected_ring::varchar)
                    WHEN bm.benchmark_mode IN ('state', 'zip')
                         AND COALESCE(ps.peer_n, 0) >= {PROVIDER_QUALITY_MIN_STATE_PEER_N}
                         AND p.state_key IS NOT NULL
                    THEN ('state:' || p.state_key)
                    ELSE 'national'
                END::varchar AS peer_group,
                GREATEST(COALESCE(p.total_services, 0.0), COALESCE(p.total_beneficiaries, 0.0), 1.0) AS evidence_n_claims,
                GREATEST(COALESCE(p.total_rx_claims, 0.0), COALESCE(p.total_rx_beneficiaries, 0.0), 1.0) AS evidence_n_rx
            FROM provider_plus p
            CROSS JOIN benchmark_modes bm
            JOIN peers_national pn
              ON pn.year = p.year
            LEFT JOIN peers_state ps
              ON ps.year = p.year
             AND ps.state_key = p.state_key
            LEFT JOIN zip_choice zc
              ON zc.year = p.year
             AND zc.zip5 = p.zip5
        ),
        expanded AS (
            SELECT
                npi,
                year,
                benchmark_mode,
                'appropriateness_utilization'::varchar AS measure_id,
                'appropriateness'::varchar AS domain,
                utilization_adjusted::float8 AS observed_value,
                target_appropriateness::float8 AS target_value,
                CASE
                    WHEN target_appropriateness IS NULL OR target_appropriateness <= 0 THEN 1.0
                    WHEN utilization_adjusted IS NULL OR utilization_adjusted <= 0 THEN 1.0
                    ELSE utilization_adjusted / target_appropriateness
                END AS rr_raw,
                evidence_n_claims AS evidence_n,
                peer_group,
                'higher_worse'::varchar AS direction,
                'claims_pricing'::varchar AS source
            FROM measure_inputs
            UNION ALL
            SELECT
                npi,
                year,
                benchmark_mode,
                'appropriateness_drug_proxy'::varchar AS measure_id,
                'appropriateness'::varchar AS domain,
                rx_claim_rate::float8 AS observed_value,
                target_rx_appropriateness::float8 AS target_value,
                CASE
                    WHEN target_rx_appropriateness IS NULL OR target_rx_appropriateness <= 0 THEN 1.0
                    WHEN rx_claim_rate IS NULL OR rx_claim_rate <= 0 THEN 1.0
                    ELSE rx_claim_rate / target_rx_appropriateness
                END AS rr_raw,
                evidence_n_rx AS evidence_n,
                peer_group,
                'higher_worse'::varchar AS direction,
                'drug_claims'::varchar AS source
            FROM measure_inputs
            UNION ALL
            SELECT
                npi,
                year,
                benchmark_mode,
                'effectiveness_qpp_quality'::varchar AS measure_id,
                'effectiveness'::varchar AS domain,
                qpp_quality_score::float8 AS observed_value,
                target_effectiveness::float8 AS target_value,
                CASE
                    WHEN qpp_quality_score IS NULL OR qpp_quality_score <= 0 THEN 1.0
                    WHEN target_effectiveness IS NULL OR target_effectiveness <= 0 THEN 1.0
                    ELSE target_effectiveness / qpp_quality_score
                END AS rr_raw,
                evidence_n_claims AS evidence_n,
                peer_group,
                'lower_worse'::varchar AS direction,
                'qpp'::varchar AS source
            FROM measure_inputs
            UNION ALL
            SELECT
                npi,
                year,
                benchmark_mode,
                'cost_qpp_component'::varchar AS measure_id,
                'cost'::varchar AS domain,
                qpp_cost_score::float8 AS observed_value,
                target_qpp_cost::float8 AS target_value,
                CASE
                    WHEN qpp_cost_score IS NULL OR qpp_cost_score <= 0 THEN 1.0
                    WHEN target_qpp_cost IS NULL OR target_qpp_cost <= 0 THEN 1.0
                    ELSE target_qpp_cost / qpp_cost_score
                END AS rr_raw,
                evidence_n_claims AS evidence_n,
                peer_group,
                'lower_worse'::varchar AS direction,
                'qpp'::varchar AS source
            FROM measure_inputs
            UNION ALL
            SELECT
                npi,
                year,
                benchmark_mode,
                'cost_allowed_amount'::varchar AS measure_id,
                'cost'::varchar AS domain,
                cost_adjusted::float8 AS observed_value,
                target_cost::float8 AS target_value,
                CASE
                    WHEN target_cost IS NULL OR target_cost <= 0 THEN 1.0
                    WHEN cost_adjusted IS NULL OR cost_adjusted <= 0 THEN 1.0
                    ELSE cost_adjusted / target_cost
                END AS rr_raw,
                evidence_n_claims AS evidence_n,
                peer_group,
                'higher_worse'::varchar AS direction,
                'claims_pricing'::varchar AS source
            FROM measure_inputs
        ),
        shrunk AS (
            SELECT
                e.*,
                LEAST(1.0, GREATEST(0.0, e.evidence_n / NULLIF(e.evidence_n + {PROVIDER_QUALITY_SHRINKAGE_ALPHA}, 0.0))) AS shrink_weight,
                GREATEST(0.0001, SQRT(1.0 / NULLIF(e.evidence_n + 1.0, 0.0))) AS stderr
            FROM expanded e
        )
        SELECT
            s.npi,
            s.year,
            s.benchmark_mode,
            s.measure_id,
            s.domain,
            s.observed_value,
            s.target_value,
            (1.0 + (s.rr_raw - 1.0) * s.shrink_weight)::float8 AS risk_ratio,
            GREATEST(0.01, (1.0 + (s.rr_raw - 1.0) * s.shrink_weight) - 1.15 * s.stderr)::float8 AS ci75_low,
            ((1.0 + (s.rr_raw - 1.0) * s.shrink_weight) + 1.15 * s.stderr)::float8 AS ci75_high,
            GREATEST(0.01, (1.0 + (s.rr_raw - 1.0) * s.shrink_weight) - 1.64 * s.stderr)::float8 AS ci90_low,
            ((1.0 + (s.rr_raw - 1.0) * s.shrink_weight) + 1.64 * s.stderr)::float8 AS ci90_high,
            ROUND(s.evidence_n)::int,
            s.peer_group,
            s.direction,
            s.source,
            NOW() AS updated_at
        FROM shrunk s;
        """
    )

    await db.status(
        f"""
        INSERT INTO {schema}.{domain_table}
        (
            npi,
            year,
            benchmark_mode,
            domain,
            risk_ratio,
            score_0_100,
            ci75_low,
            ci75_high,
            ci90_low,
            ci90_high,
            evidence_n,
            measure_count,
            updated_at
        )
        SELECT
            m.npi,
            m.year,
            m.benchmark_mode,
            m.domain,
            EXP(AVG(LN(GREATEST(m.risk_ratio, 0.0001))))::float8 AS risk_ratio,
            ROUND((LEAST(100.0, GREATEST(0.0, 50.0 - 125.0 * (EXP(AVG(LN(GREATEST(m.risk_ratio, 0.0001)))) - 1.0))))::numeric, 2)::float8 AS score_0_100,
            EXP(AVG(LN(GREATEST(m.ci75_low, 0.0001))))::float8 AS ci75_low,
            EXP(AVG(LN(GREATEST(m.ci75_high, 0.0001))))::float8 AS ci75_high,
            EXP(AVG(LN(GREATEST(m.ci90_low, 0.0001))))::float8 AS ci90_low,
            EXP(AVG(LN(GREATEST(m.ci90_high, 0.0001))))::float8 AS ci90_high,
            SUM(COALESCE(m.evidence_n, 0))::int AS evidence_n,
            COUNT(*)::int AS measure_count,
            NOW() AS updated_at
        FROM {schema}.{measure_table} m
        GROUP BY m.npi, m.year, m.benchmark_mode, m.domain;
        """
    )

    await db.status(
        f"""
        INSERT INTO {schema}.{score_table}
        (
            npi,
            year,
            model_version,
            benchmark_mode,
            risk_ratio_point,
            ci75_low,
            ci75_high,
            ci90_low,
            ci90_high,
            score_0_100,
            tier,
            borderline_status,
            low_score_threshold_failed,
            low_confidence_threshold_failed,
            high_score_threshold_passed,
            high_confidence_threshold_passed,
            estimated_cost_level,
            run_id,
            updated_at
        )
        WITH domain_pivot AS (
            SELECT
                d.npi,
                d.year,
                d.benchmark_mode,
                MAX(CASE WHEN d.domain = 'appropriateness' THEN d.risk_ratio END) AS rr_appropr,
                MAX(CASE WHEN d.domain = 'effectiveness' THEN d.risk_ratio END) AS rr_effect,
                MAX(CASE WHEN d.domain = 'cost' THEN d.risk_ratio END) AS rr_cost,
                MAX(CASE WHEN d.domain = 'appropriateness' THEN d.ci75_low END) AS ci75_appropr_low,
                MAX(CASE WHEN d.domain = 'effectiveness' THEN d.ci75_low END) AS ci75_effect_low,
                MAX(CASE WHEN d.domain = 'cost' THEN d.ci75_low END) AS ci75_cost_low,
                MAX(CASE WHEN d.domain = 'appropriateness' THEN d.ci75_high END) AS ci75_appropr_high,
                MAX(CASE WHEN d.domain = 'effectiveness' THEN d.ci75_high END) AS ci75_effect_high,
                MAX(CASE WHEN d.domain = 'cost' THEN d.ci75_high END) AS ci75_cost_high,
                MAX(CASE WHEN d.domain = 'appropriateness' THEN d.ci90_low END) AS ci90_appropr_low,
                MAX(CASE WHEN d.domain = 'effectiveness' THEN d.ci90_low END) AS ci90_effect_low,
                MAX(CASE WHEN d.domain = 'cost' THEN d.ci90_low END) AS ci90_cost_low,
                MAX(CASE WHEN d.domain = 'appropriateness' THEN d.ci90_high END) AS ci90_appropr_high,
                MAX(CASE WHEN d.domain = 'effectiveness' THEN d.ci90_high END) AS ci90_effect_high,
                MAX(CASE WHEN d.domain = 'cost' THEN d.ci90_high END) AS ci90_cost_high
            FROM {schema}.{domain_table} d
            GROUP BY d.npi, d.year, d.benchmark_mode
        ),
        blended AS (
            SELECT
                p.npi,
                p.year,
                p.benchmark_mode,
                COALESCE(p.rr_appropr, 1.0) AS rr_appropr,
                COALESCE(p.rr_effect, 1.0) AS rr_effect,
                COALESCE(p.rr_cost, 1.0) AS rr_cost,
                SQRT(COALESCE(p.rr_appropr, 1.0) * COALESCE(p.rr_effect, 1.0)) AS rr_clinical,
                EXP(
                    0.5 * LN(GREATEST(SQRT(COALESCE(p.rr_appropr, 1.0) * COALESCE(p.rr_effect, 1.0)), 0.0001))
                    + 0.5 * LN(GREATEST(COALESCE(p.rr_cost, 1.0), 0.0001))
                ) AS rr_overall,
                EXP(
                    0.5 * LN(GREATEST(SQRT(COALESCE(p.ci75_appropr_low, 1.0) * COALESCE(p.ci75_effect_low, 1.0)), 0.0001))
                    + 0.5 * LN(GREATEST(COALESCE(p.ci75_cost_low, 1.0), 0.0001))
                ) AS ci75_low,
                EXP(
                    0.5 * LN(GREATEST(SQRT(COALESCE(p.ci75_appropr_high, 1.0) * COALESCE(p.ci75_effect_high, 1.0)), 0.0001))
                    + 0.5 * LN(GREATEST(COALESCE(p.ci75_cost_high, 1.0), 0.0001))
                ) AS ci75_high,
                EXP(
                    0.5 * LN(GREATEST(SQRT(COALESCE(p.ci90_appropr_low, 1.0) * COALESCE(p.ci90_effect_low, 1.0)), 0.0001))
                    + 0.5 * LN(GREATEST(COALESCE(p.ci90_cost_low, 1.0), 0.0001))
                ) AS ci90_low,
                EXP(
                    0.5 * LN(GREATEST(SQRT(COALESCE(p.ci90_appropr_high, 1.0) * COALESCE(p.ci90_effect_high, 1.0)), 0.0001))
                    + 0.5 * LN(GREATEST(COALESCE(p.ci90_cost_high, 1.0), 0.0001))
                ) AS ci90_high
            FROM domain_pivot p
        ),
        checks AS (
            SELECT
                b.*,
                (b.rr_overall >= 1.12)::boolean AS low_score_check,
                (b.ci90_low >= 1.08)::boolean AS low_confidence_check,
                (b.rr_overall <= 0.88)::boolean AS high_score_check,
                (b.ci75_high < 1.0)::boolean AS high_confidence_check
            FROM blended b
        )
        SELECT
            c.npi,
            c.year,
            '{PROVIDER_QUALITY_MODEL_VERSION}'::varchar AS model_version,
            c.benchmark_mode::varchar AS benchmark_mode,
            c.rr_overall::float8 AS risk_ratio_point,
            c.ci75_low::float8,
            c.ci75_high::float8,
            c.ci90_low::float8,
            c.ci90_high::float8,
            ROUND((LEAST(100.0, GREATEST(0.0, 50.0 - 125.0 * (c.rr_overall - 1.0))))::numeric, 2)::float8 AS score_0_100,
            CASE
                WHEN c.low_score_check AND c.low_confidence_check THEN 'low'
                WHEN (c.low_score_check::int + c.low_confidence_check::int) = 1 THEN 'acceptable'
                WHEN c.high_score_check AND c.high_confidence_check THEN 'high'
                ELSE 'acceptable'
            END::varchar AS tier,
            ((c.low_score_check::int + c.low_confidence_check::int) = 1)::boolean AS borderline_status,
            c.low_score_check,
            c.low_confidence_check,
            c.high_score_check,
            c.high_confidence_check,
            CASE
                WHEN c.rr_cost <= 0.80 THEN '$'
                WHEN c.rr_cost <= 0.90 THEN '$$'
                WHEN c.rr_cost <= 1.10 THEN '$$$'
                WHEN c.rr_cost <= 1.25 THEN '$$$$'
                ELSE '$$$$$'
            END::varchar AS estimated_cost_level,
            CAST(:run_id AS varchar) AS run_id,
            NOW() AS updated_at
        FROM checks c;
        """,
        run_id=run_id,
    )


async def _enqueue_materialize_phase_shards(
    redis,
    *,
    run_id: str,
    phase: str,
    years: tuple[int, ...],
    shard_count: int,
    stage_suffix: str,
    schema: str,
    test_mode: bool,
    job_name: str,
) -> int:
    payloads: list[dict[str, Any]] = []
    for year in years:
        for shard_id in range(shard_count):
            payloads.append(
                {
                    "run_id": run_id,
                    "stage_suffix": stage_suffix,
                    "schema": schema,
                    "test_mode": bool(test_mode),
                    "year": year,
                    "shard_id": shard_id,
                    "shard_count": shard_count,
                }
            )

    await _set_materialize_phase(redis, run_id, phase, total=len(payloads))
    logger.info(
        "provider quality materialize enqueue run_id=%s phase=%s shards=%s years=%s job=%s",
        run_id,
        phase,
        len(payloads),
        ",".join(str(y) for y in years),
        job_name,
    )
    for payload in payloads:
        year = _safe_int(payload.get("year"), PROVIDER_QUALITY_MIN_YEAR)
        shard_id = _safe_int(payload.get("shard_id"), 0)
        await redis.enqueue_job(
            job_name,
            payload,
            _queue_name=PROVIDER_QUALITY_MATERIALIZE_SHARD_QUEUE_NAME,
            _job_id=_materialize_shard_job_id(run_id, phase, year, shard_id),
        )
    return len(payloads)


async def _wait_for_materialize_phase_completion(redis, run_id: str, phase: str) -> None:
    total, done, failed = await _get_materialize_progress(redis, run_id)
    elapsed_sec = await _get_materialize_phase_elapsed_seconds(redis, run_id)
    pending = max(total - done - failed, 0)
    progress_pct = (float(done) * 100.0 / float(total)) if total > 0 else 100.0
    shards_per_sec = (float(done) / elapsed_sec) if elapsed_sec > 0.0 else 0.0
    eta_sec = (float(pending) / shards_per_sec) if shards_per_sec > 0.0 else 0.0
    eta_text = _format_duration_compact(eta_sec) if shards_per_sec > 0.0 else "n/a"
    elapsed_text = _format_duration_compact(elapsed_sec)
    if failed > 0:
        raise RuntimeError(
            "Provider quality materialization failed in "
            f"{phase}: failed_shards={failed} total={total} done={done} "
            f"elapsed={elapsed_text}"
        )
    if done < total:
        print(
            f"[finalize] materialize progress phase={phase} done={done}/{total} "
            f"pending={pending} failed={failed} pct={progress_pct:.1f}% "
            f"elapsed={elapsed_text} rate={shards_per_sec:.4f}/s eta={eta_text} run_id={run_id}",
            flush=True,
        )
        raise Retry(defer=PROVIDER_QUALITY_FINISH_RETRY_SECONDS)
    await _log_materialize_phase_summary(redis, run_id, phase, total=total, done=done, failed=failed)


async def _materialize_quality_rows_sharded(
    redis,
    *,
    classes: dict[str, type],
    schema: str,
    run_id: str,
    stage_suffix: str,
    test_mode: bool,
    manifest: dict[str, Any],
) -> None:
    years = _materialize_reporting_years(manifest)
    context = await _build_cohort_materialization_context(classes, schema)
    phase = await _get_materialize_phase(redis, run_id)
    if phase and phase not in MAT_PHASE_SEQUENCE:
        await _reset_materialize_state(redis, run_id)
        phase = ""

    if not phase:
        await _set_materialize_phase(redis, run_id, MAT_PHASE_1_BUILD_FEATURES, total=0)
        phase = MAT_PHASE_1_BUILD_FEATURES

    if phase == MAT_PHASE_1_BUILD_FEATURES:
        logger.info("provider quality materialize phase start run_id=%s phase=%s", run_id, phase)
        await db.status(f"TRUNCATE TABLE {schema}.{context['feature_table']};")
        await db.status(f"TRUNCATE TABLE {schema}.{context['lsh_table']};")
        await db.status(f"TRUNCATE TABLE {schema}.{context['peer_target_table']};")
        await db.status(f"TRUNCATE TABLE {schema}.{context['measure_table']};")
        await db.status(f"TRUNCATE TABLE {schema}.{context['domain_table']};")
        await db.status(f"TRUNCATE TABLE {schema}.{context['score_table']};")
        await db.status(_cohort_sql_phase_1_build_features(context))
        await _enqueue_materialize_phase_shards(
            redis,
            run_id=run_id,
            phase=MAT_PHASE_2_BUILD_LSH_SHARDED,
            years=years,
            shard_count=PROVIDER_QUALITY_LSH_SHARDS,
            stage_suffix=stage_suffix,
            schema=schema,
            test_mode=test_mode,
            job_name="provider_quality_materialize_lsh_shard",
        )
        raise Retry(defer=PROVIDER_QUALITY_FINISH_RETRY_SECONDS)

    if phase == MAT_PHASE_2_BUILD_LSH_SHARDED:
        logger.info("provider quality materialize phase wait run_id=%s phase=%s", run_id, phase)
        await _wait_for_materialize_phase_completion(redis, run_id, phase)
        await _set_materialize_phase(redis, run_id, MAT_PHASE_3_UPDATE_PROCEDURE_BUCKET, total=0)
        phase = MAT_PHASE_3_UPDATE_PROCEDURE_BUCKET

    if phase == MAT_PHASE_3_UPDATE_PROCEDURE_BUCKET:
        logger.info("provider quality materialize phase start run_id=%s phase=%s", run_id, phase)
        procedure_bucket_sql = _cohort_sql_phase_3_update_procedure_bucket(context)
        if procedure_bucket_sql:
            await db.status(procedure_bucket_sql)
        await db.status(f"ANALYZE {schema}.{context['feature_table']};")
        await _set_materialize_phase(redis, run_id, MAT_PHASE_4_BUILD_PEER_TARGETS, total=0)
        phase = MAT_PHASE_4_BUILD_PEER_TARGETS

    if phase == MAT_PHASE_4_BUILD_PEER_TARGETS:
        logger.info("provider quality materialize phase start run_id=%s phase=%s", run_id, phase)
        await db.status(_cohort_sql_phase_4_build_peer_targets(context))
        await db.status(f"ANALYZE {schema}.{context['peer_target_table']};")
        await _enqueue_materialize_phase_shards(
            redis,
            run_id=run_id,
            phase=MAT_PHASE_5_BUILD_MEASURE_SHARDED,
            years=years,
            shard_count=PROVIDER_QUALITY_MEASURE_SHARDS,
            stage_suffix=stage_suffix,
            schema=schema,
            test_mode=test_mode,
            job_name="provider_quality_materialize_measure_shard",
        )
        raise Retry(defer=PROVIDER_QUALITY_FINISH_RETRY_SECONDS)

    if phase == MAT_PHASE_5_BUILD_MEASURE_SHARDED:
        logger.info("provider quality materialize phase wait run_id=%s phase=%s", run_id, phase)
        await _wait_for_materialize_phase_completion(redis, run_id, phase)
        await _enqueue_materialize_phase_shards(
            redis,
            run_id=run_id,
            phase=MAT_PHASE_6_BUILD_DOMAIN_SHARDED,
            years=years,
            shard_count=PROVIDER_QUALITY_DOMAIN_SHARDS,
            stage_suffix=stage_suffix,
            schema=schema,
            test_mode=test_mode,
            job_name="provider_quality_materialize_domain_shard",
        )
        raise Retry(defer=PROVIDER_QUALITY_FINISH_RETRY_SECONDS)

    if phase == MAT_PHASE_6_BUILD_DOMAIN_SHARDED:
        logger.info("provider quality materialize phase wait run_id=%s phase=%s", run_id, phase)
        await _wait_for_materialize_phase_completion(redis, run_id, phase)
        await _enqueue_materialize_phase_shards(
            redis,
            run_id=run_id,
            phase=MAT_PHASE_7_BUILD_SCORE_SHARDED,
            years=years,
            shard_count=PROVIDER_QUALITY_SCORE_SHARDS,
            stage_suffix=stage_suffix,
            schema=schema,
            test_mode=test_mode,
            job_name="provider_quality_materialize_score_shard",
        )
        raise Retry(defer=PROVIDER_QUALITY_FINISH_RETRY_SECONDS)

    if phase == MAT_PHASE_7_BUILD_SCORE_SHARDED:
        logger.info("provider quality materialize phase wait run_id=%s phase=%s", run_id, phase)
        await _wait_for_materialize_phase_completion(redis, run_id, phase)
        await _set_materialize_phase(redis, run_id, MAT_PHASE_DONE, total=0)
        return

    if phase != MAT_PHASE_DONE:
        raise Retry(defer=PROVIDER_QUALITY_FINISH_RETRY_SECONDS)


def _materialize_shard_task_values(task: dict[str, Any] | None) -> tuple[str, str, str, bool, int, int, int]:
    payload = task or {}
    run_id = str(payload.get("run_id") or "")
    stage_suffix = str(payload.get("stage_suffix") or "")
    schema = str(payload.get("schema") or get_import_schema("HLTHPRT_DB_SCHEMA", "mrf", bool(payload.get("test_mode"))))
    test_mode = bool(payload.get("test_mode", False))
    year = max(_safe_int(payload.get("year"), PROVIDER_QUALITY_MIN_YEAR), 2013)
    shard_count = max(_safe_int(payload.get("shard_count"), 1), 1)
    shard_id = _safe_int(payload.get("shard_id"), 0)
    if shard_id < 0 or shard_id >= shard_count:
        raise RuntimeError(f"Invalid shard payload: shard_id={shard_id} shard_count={shard_count}")
    if not run_id or not stage_suffix:
        raise RuntimeError("Shard payload is missing required fields: run_id/stage_suffix.")
    return run_id, stage_suffix, schema, test_mode, year, shard_id, shard_count


async def _run_materialize_shard_job(
    ctx,
    task: dict[str, Any] | None,
    *,
    sql_builder,
    phase: str,
    target_table_key: str,
    include_run_id: bool = False,
) -> dict[str, Any]:
    run_id, stage_suffix, schema, test_mode, year, shard_id, shard_count = _materialize_shard_task_values(task)
    redis = ctx.get("redis")
    started_at = time.monotonic()
    try:
        await ensure_database(test_mode)
        classes = _staging_classes(stage_suffix, schema)
        cohort_context = await _build_cohort_materialization_context(classes, schema)
        target_table = str(cohort_context[target_table_key])
        rows_before = await _count_shard_rows(
            schema,
            target_table,
            year=year,
            shard_id=shard_id,
            shard_count=shard_count,
        )
        sql = sql_builder(cohort_context)
        params: dict[str, Any] = {"year": year, "shard_id": shard_id, "shard_count": shard_count}
        if include_run_id:
            params["run_id"] = run_id
        await _execute_shard_sql(sql, **params)
        rows_after = await _count_shard_rows(
            schema,
            target_table,
            year=year,
            shard_id=shard_id,
            shard_count=shard_count,
        )
        duration_sec = max(time.monotonic() - started_at, 0.0)
        rows_deleted = rows_before
        rows_inserted = rows_after
        logger.info(
            "provider quality materialize shard done run_id=%s phase=%s year=%s shard_id=%s shard_count=%s duration_sec=%.3f rows_deleted=%s rows_inserted=%s rows_final=%s",
            run_id,
            phase,
            year,
            shard_id,
            shard_count,
            duration_sec,
            rows_deleted,
            rows_inserted,
            rows_after,
        )
        if redis is not None:
            await _mark_materialize_done(redis, run_id)
            await _record_materialize_phase_duration(redis, run_id, phase, duration_sec)
    except Exception:
        if redis is not None:
            await _mark_materialize_failed(redis, run_id)
        logger.exception(
            "provider quality materialize shard failed run_id=%s phase=%s year=%s shard_id=%s shard_count=%s",
            run_id,
            phase,
            year,
            shard_id,
            shard_count,
        )
        raise
    return {
        "ok": True,
        "run_id": run_id,
        "phase": phase,
        "year": year,
        "shard_id": shard_id,
        "shard_count": shard_count,
    }


async def provider_quality_materialize_lsh_shard(
    ctx, task: dict[str, Any] | None = None, **_kwargs: Any
) -> dict[str, Any]:
    return await _run_materialize_shard_job(
        ctx,
        task,
        sql_builder=_cohort_sql_phase_2_build_lsh_shard,
        phase=MAT_PHASE_2_BUILD_LSH_SHARDED,
        target_table_key="lsh_table",
    )


async def provider_quality_materialize_measure_shard(
    ctx, task: dict[str, Any] | None = None, **_kwargs: Any
) -> dict[str, Any]:
    return await _run_materialize_shard_job(
        ctx,
        task,
        sql_builder=_cohort_sql_phase_5_build_measure_shard,
        phase=MAT_PHASE_5_BUILD_MEASURE_SHARDED,
        target_table_key="measure_table",
    )


async def provider_quality_materialize_domain_shard(
    ctx, task: dict[str, Any] | None = None, **_kwargs: Any
) -> dict[str, Any]:
    return await _run_materialize_shard_job(
        ctx,
        task,
        sql_builder=_cohort_sql_phase_6_build_domain_shard,
        phase=MAT_PHASE_6_BUILD_DOMAIN_SHARDED,
        target_table_key="domain_table",
    )


async def provider_quality_materialize_score_shard(
    ctx, task: dict[str, Any] | None = None, **_kwargs: Any
) -> dict[str, Any]:
    return await _run_materialize_shard_job(
        ctx,
        task,
        sql_builder=_cohort_sql_phase_7_build_score_shard,
        phase=MAT_PHASE_7_BUILD_SCORE_SHARDED,
        target_table_key="score_table",
        include_run_id=True,
    )


async def _publish_by_table_rename(classes: dict[str, type], schema: str) -> None:
    final_classes = (
        PricingQppProvider,
        PricingSviZcta,
        PricingProviderQualityMeasure,
        PricingProviderQualityDomain,
        PricingProviderQualityScore,
        *_cohort_model_classes(),
    )

    async def archive_index(index_name: str) -> str:
        archived_name = _archived_identifier(index_name)
        await db.status(f"DROP INDEX IF EXISTS {schema}.{archived_name};")
        await db.status(f"ALTER INDEX IF EXISTS {schema}.{index_name} RENAME TO {archived_name};")
        return archived_name

    for cls in final_classes:
        obj = classes[cls.__name__]
        stage_exists = await _table_exists(schema, obj.__tablename__)
        if not stage_exists:
            raise RuntimeError(
                f"Staging table missing for publish: {schema}.{obj.__tablename__}. "
                "Aborting publish to protect live tables."
            )

    async with db.transaction():
        for cls in final_classes:
            obj = classes[cls.__name__]
            table = cls.__main_table__
            await db.status(f"DROP TABLE IF EXISTS {schema}.{table}_old;")
            await db.status(f"ALTER TABLE IF EXISTS {schema}.{table} RENAME TO {table}_old;")
            await db.status(f"ALTER TABLE IF EXISTS {schema}.{obj.__tablename__} RENAME TO {table};")

            await archive_index(f"{table}_idx_primary")
            await db.status(
                f"ALTER INDEX IF EXISTS {schema}.{obj.__tablename__}_idx_primary RENAME TO {table}_idx_primary;"
            )

            move_indexes = []
            if hasattr(cls, "__my_initial_indexes__") and cls.__my_initial_indexes__:
                move_indexes += cls.__my_initial_indexes__
            if hasattr(cls, "__my_additional_indexes__") and cls.__my_additional_indexes__:
                move_indexes += cls.__my_additional_indexes__

            for index in move_indexes:
                elements = index.get("index_elements")
                if not elements:
                    continue
                base_name = index.get("name") or f"{table}_{'_'.join(elements)}_idx"
                await archive_index(base_name)
                await db.status(
                    f"ALTER INDEX IF EXISTS {schema}.{obj.__tablename__}_{base_name} RENAME TO {base_name};"
                )


async def _insert_run_metadata(
    schema: str,
    run_id: str,
    import_id: str,
    manifest: dict[str, Any],
    *,
    status: str = "published",
) -> None:
    qpp_rows = _safe_int(
        await db.scalar(f"SELECT COUNT(*) FROM {schema}.{PricingQppProvider.__tablename__}"),
        0,
    )
    svi_rows = _safe_int(
        await db.scalar(f"SELECT COUNT(*) FROM {schema}.{PricingSviZcta.__tablename__}"),
        0,
    )
    measure_rows = _safe_int(
        await db.scalar(f"SELECT COUNT(*) FROM {schema}.{PricingProviderQualityMeasure.__tablename__}"),
        0,
    )
    domain_rows = _safe_int(
        await db.scalar(f"SELECT COUNT(*) FROM {schema}.{PricingProviderQualityDomain.__tablename__}"),
        0,
    )
    score_rows = _safe_int(
        await db.scalar(f"SELECT COUNT(*) FROM {schema}.{PricingProviderQualityScore.__tablename__}"),
        0,
    )
    row = {
        "run_id": run_id,
        "import_id": import_id,
        "year": _safe_int(manifest.get("year"), PROVIDER_QUALITY_MAX_YEAR),
        "qpp_source_version": json.dumps(manifest.get("sources", {}).get("qpp_provider", []), ensure_ascii=True)[:128],
        "svi_source_version": json.dumps(manifest.get("sources", {}).get("svi_zcta", []), ensure_ascii=True)[:128],
        "status": status,
        "qpp_rows": qpp_rows,
        "svi_rows": svi_rows,
        "measure_rows": measure_rows,
        "domain_rows": domain_rows,
        "score_rows": score_rows,
        "started_at": datetime.datetime.utcnow(),
        "finished_at": datetime.datetime.utcnow(),
        "created_at": datetime.datetime.utcnow(),
        "updated_at": datetime.datetime.utcnow(),
    }
    await _push_objects_with_retry([row], PricingQualityRun, rewrite=False)


async def provider_quality_start(ctx, task: dict[str, Any] | None = None, **_kwargs: Any) -> dict[str, Any]:
    task = task or {}
    test_mode = bool(task.get("test_mode", False))
    import_id_val = _normalize_import_id(task.get("import_id"))
    run_id = _normalize_run_id(task.get("run_id"))
    stage_suffix = _build_stage_suffix(import_id_val, run_id)
    redis = ctx.get("redis")
    if redis is None:
        raise RuntimeError("ARQ redis context is unavailable for provider quality start job.")

    total_start = _step_start(
        "provider-quality enqueue+split "
        f"(test_mode={test_mode}, import_id={import_id_val}, run_id={run_id}, stage={stage_suffix})"
    )

    await ensure_database(test_mode)

    t = _step_start("prepare staging tables")
    _classes, schema = await _prepare_tables(stage_suffix, test_mode)
    _step_end("prepare staging tables", t)

    t = _step_start("resolve provider quality sources")
    sources = _resolve_sources(test_mode=test_mode)
    _step_end("resolve provider quality sources", t)

    work_dir = _run_dir(import_id_val, run_id)
    downloads_dir = work_dir / "downloads"
    chunks_root = work_dir / "chunks"
    chunks_root.mkdir(parents=True, exist_ok=True)

    chunks: list[dict[str, Any]] = []
    local_paths: dict[str, list[str]] = {}
    degraded_sources: list[dict[str, Any]] = []

    await _init_run_state(redis, run_id, 0)

    semaphore = asyncio.Semaphore(PROVIDER_QUALITY_DOWNLOAD_CONCURRENCY)

    async def _download_split_source(dataset_key: str, source: dict[str, Any], source_index: int):
        async with semaphore:
            reporting_year = max(_safe_int(source.get("reporting_year"), PROVIDER_QUALITY_MIN_YEAR), 2013)
            step = _step_start(f"download+split {dataset_key} year={reporting_year}")
            try:
                local_path, degraded = await _download_source_file(dataset_key, source, str(downloads_dir), test_mode)
                split_chunks = await _split_source_into_chunks(
                    dataset_key=dataset_key,
                    source_path=local_path,
                    chunks_dir=chunks_root / dataset_key,
                    test_mode=test_mode,
                )
                for chunk in split_chunks:
                    chunk["reporting_year"] = reporting_year
                    chunk["source_index"] = source_index
                return dataset_key, local_path, split_chunks, degraded, reporting_year
            finally:
                _step_end(f"download+split {dataset_key} year={reporting_year}", step)

    dataset_tasks = [
        asyncio.create_task(_download_split_source(dataset.key, source, idx))
        for dataset in DATASETS
        for idx, source in enumerate(sources.get(dataset.key, []))
    ]

    t = _step_start("download+split+enqueue chunks (streaming)")
    try:
        try:
            for completed in asyncio.as_completed(dataset_tasks):
                dataset_key, local_path, split_chunks, degraded, reporting_year = await completed
                local_paths.setdefault(dataset_key, []).append(local_path)
                if degraded:
                    degraded_sources.append({"dataset_key": dataset_key, "reporting_year": reporting_year})
                for chunk in split_chunks:
                    reporting_year = max(_safe_int(chunk.get("reporting_year"), PROVIDER_QUALITY_MIN_YEAR), 2013)
                    source_index = max(_safe_int(chunk.get("source_index"), 0), 0)
                    chunk_index = max(_safe_int(chunk.get("chunk_index"), 0), 0)
                    unique_chunk_id = f"{chunk['dataset_key']}:{reporting_year}:{source_index}:{chunk_index}"
                    payload = {
                        "import_id": import_id_val,
                        "run_id": run_id,
                        "stage_suffix": stage_suffix,
                        "schema": schema,
                        "test_mode": test_mode,
                        "dataset_key": chunk["dataset_key"],
                        "chunk_id": unique_chunk_id,
                        "chunk_path": chunk["chunk_path"],
                        "reporting_year": reporting_year,
                    }
                    await redis.enqueue_job(
                        "provider_quality_process_chunk",
                        payload,
                        _queue_name=PROVIDER_QUALITY_QUEUE_NAME,
                        _job_id=_chunk_job_id(run_id, chunk["dataset_key"], source_index, reporting_year, chunk_index),
                    )
                if split_chunks:
                    await _increment_total_chunks(redis, run_id, len(split_chunks))
                    chunks.extend(split_chunks)
        except Exception:
            failure_manifest = {
                "year": max(PROVIDER_QUALITY_YEAR_WINDOW) if PROVIDER_QUALITY_YEAR_WINDOW else PROVIDER_QUALITY_MIN_YEAR,
                "sources": sources,
            }
            try:
                await _insert_run_metadata(
                    schema,
                    run_id,
                    import_id_val,
                    failure_manifest,
                    status="failed_source",
                )
            except Exception as metadata_exc:  # pylint: disable=broad-exception-caught
                logger.warning("Failed to write provider-quality failed_source metadata: %r", metadata_exc)
            raise
    finally:
        for task_ref in dataset_tasks:
            if not task_ref.done():
                task_ref.cancel()
    _step_end("download+split+enqueue chunks (streaming)", t)

    manifest = {
        "import_id": import_id_val,
        "run_id": run_id,
        "stage_suffix": stage_suffix,
        "schema": schema,
        "test_mode": test_mode,
        "sources": sources,
        "local_paths": local_paths,
        "degraded_sources": degraded_sources,
        "chunks": chunks,
        "total_chunks": len(chunks),
        "total_rows": 0,
        "year": max(PROVIDER_QUALITY_YEAR_WINDOW) if PROVIDER_QUALITY_YEAR_WINDOW else PROVIDER_QUALITY_MIN_YEAR,
        "created_at": datetime.datetime.utcnow().isoformat(),
        "work_dir": str(work_dir),
    }

    manifest_path = _manifest_path(work_dir)
    _write_manifest(manifest_path, manifest)

    await redis.enqueue_job(
        "provider_quality_finalize",
        {
            "import_id": import_id_val,
            "run_id": run_id,
            "stage_suffix": stage_suffix,
            "schema": schema,
            "manifest_path": str(manifest_path),
            "test_mode": test_mode,
        },
        _queue_name=PROVIDER_QUALITY_FINISH_QUEUE_NAME,
        _job_id=f"provider_quality_finalize_{run_id}",
    )

    _step_end(
        "provider-quality enqueue+split "
        f"(test_mode={test_mode}, import_id={import_id_val}, run_id={run_id}, stage={stage_suffix})",
        total_start,
    )

    return {
        "ok": True,
        "queued": True,
        "import_id": import_id_val,
        "run_id": run_id,
        "stage_suffix": stage_suffix,
        "total_chunks": len(chunks),
        "manifest_path": str(manifest_path),
    }


async def provider_quality_process_chunk(ctx, task: dict[str, Any] | None = None, **_kwargs: Any) -> dict[str, Any]:
    task = task or {}
    dataset_key = str(task.get("dataset_key") or "")
    chunk_id = str(task.get("chunk_id") or "")
    chunk_path = str(task.get("chunk_path") or "")
    import_id_val = _normalize_import_id(task.get("import_id"))
    stage_suffix = str(task.get("stage_suffix") or _build_stage_suffix(import_id_val, str(task.get("run_id") or "")))
    schema = str(task.get("schema") or get_import_schema("HLTHPRT_DB_SCHEMA", "mrf", bool(task.get("test_mode"))))
    run_id = str(task.get("run_id") or "")
    test_mode = bool(task.get("test_mode", False))

    if not dataset_key or not chunk_id or not chunk_path:
        raise RuntimeError("Chunk payload is missing required fields: dataset_key/chunk_id/chunk_path.")

    if not Path(chunk_path).exists():
        raise RuntimeError(f"Chunk file does not exist: {chunk_path}")

    await ensure_database(test_mode)
    classes = _staging_classes(stage_suffix, schema)
    year = max(_safe_int(task.get("reporting_year"), PROVIDER_QUALITY_MIN_YEAR), 2013)

    if dataset_key == "qpp_provider":
        await _load_qpp_rows(chunk_path, classes["PricingQppProvider"], year, test_mode=test_mode)
    elif dataset_key == "svi_zcta":
        await _load_svi_rows(chunk_path, classes["PricingSviZcta"], year, test_mode=test_mode)
    else:
        raise RuntimeError(f"Unsupported dataset_key for chunk processing: {dataset_key}")

    redis = ctx.get("redis")
    if redis is not None and run_id:
        await _mark_chunk_done_with_retry(redis, run_id, chunk_id)

    return {
        "ok": True,
        "chunk_id": chunk_id,
        "dataset_key": dataset_key,
    }


async def provider_quality_finalize(ctx, task: dict[str, Any] | None = None, **_kwargs: Any) -> dict[str, Any]:
    task = task or {}
    import_id_val = _normalize_import_id(task.get("import_id"))
    run_id = str(task.get("run_id") or "")
    test_mode = bool(task.get("test_mode", False))
    manifest_path = str(task.get("manifest_path") or "")
    schema = str(task.get("schema") or get_import_schema("HLTHPRT_DB_SCHEMA", "mrf", test_mode))
    stage_suffix = str(task.get("stage_suffix") or "")
    redis = ctx.get("redis")

    await ensure_database(test_mode)

    manifest = _read_manifest(manifest_path) if manifest_path else {}
    expected_chunks = _safe_int(manifest.get("total_chunks"), _safe_int(task.get("total_chunks"), 0))
    if not run_id:
        run_id = str(manifest.get("run_id") or "")
    if not stage_suffix:
        stage_suffix = str(manifest.get("stage_suffix") or _build_stage_suffix(import_id_val, run_id))

    global_lock_acquired = False
    if redis is not None and run_id:
        finalized_key = _state_key(run_id, "finalized")
        if await redis.get(finalized_key):
            return {
                "ok": True,
                "already_finalized": True,
                "run_id": run_id,
                "import_id": import_id_val,
            }

        total_chunks, done_chunks = await _get_run_progress(redis, run_id, expected_chunks)
        if done_chunks < total_chunks:
            print(
                f"[finalize] waiting for chunks: done={done_chunks}/{total_chunks} run_id={run_id}",
                flush=True,
            )
            raise Retry(defer=PROVIDER_QUALITY_FINISH_RETRY_SECONDS)

        if not await _claim_finalize_lock(redis, run_id):
            raise Retry(defer=PROVIDER_QUALITY_FINISH_RETRY_SECONDS)
        if not await _claim_global_finalize_lock(redis, run_id):
            raise Retry(defer=PROVIDER_QUALITY_FINISH_RETRY_SECONDS)
        global_lock_acquired = True

    try:
        classes = _staging_classes(stage_suffix, schema)

        t = _step_start("materialize provider quality rows")
        use_sharded_materialization = (
            PROVIDER_QUALITY_COHORT_ENABLED
            and PROVIDER_QUALITY_MATERIALIZE_SHARDED_ENABLED
            and _cohort_models_present(classes)
        )
        if use_sharded_materialization:
            if redis is None or not run_id:
                raise RuntimeError("Sharded provider-quality materialization requires redis and run_id.")
            await _materialize_quality_rows_sharded(
                redis,
                classes=classes,
                schema=schema,
                run_id=run_id,
                stage_suffix=stage_suffix,
                test_mode=test_mode,
                manifest=manifest,
            )
        else:
            await _materialize_quality_rows(classes, schema, run_id)
        _step_end("materialize provider quality rows", t)

        if PROVIDER_QUALITY_DEFER_STAGE_INDEXES:
            t = _step_start("build provider quality staging indexes")
            await _build_staging_indexes(classes, schema)
            _step_end("build provider quality staging indexes", t)

        t = _step_start("publish provider quality staging -> final")
        await _publish_by_table_rename(classes, schema)
        _step_end("publish provider quality staging -> final", t)

        t = _step_start("write provider quality run metadata")
        run_status = "degraded_test" if bool(manifest.get("degraded_sources")) else "published"
        await _insert_run_metadata(schema, run_id, import_id_val, manifest, status=run_status)
        _step_end("write provider quality run metadata", t)

        if redis is not None and run_id:
            await redis.set(_state_key(run_id, "finalized"), "1", ex=PROVIDER_QUALITY_REDIS_TTL_SECONDS)
            await redis.expire(_state_key(run_id, "finalized"), PROVIDER_QUALITY_REDIS_TTL_SECONDS)
    finally:
        if redis is not None and run_id and global_lock_acquired:
            await _release_global_finalize_lock(redis, run_id)

    if manifest:
        run_work_dir = Path(manifest.get("work_dir", ""))
        if run_work_dir and run_work_dir.exists() and not PROVIDER_QUALITY_KEEP_WORKDIR:
            shutil.rmtree(run_work_dir, ignore_errors=True)

    logger.info(
        "provider quality import finalized: test_mode=%s import_id=%s run_id=%s",
        test_mode,
        import_id_val,
        run_id,
    )

    return {
        "ok": True,
        "import_id": import_id_val,
        "run_id": run_id,
        "stage_suffix": stage_suffix,
        "schema": schema,
    }


async def main(test_mode: bool = False, import_id: str | None = None) -> dict[str, Any]:
    redis = await create_pool(build_redis_settings(), job_serializer=serialize_job, job_deserializer=deserialize_job)
    run_id = _normalize_run_id(None)
    payload = {
        "test_mode": bool(test_mode),
        "import_id": import_id,
        "run_id": run_id,
    }
    await redis.enqueue_job(
        "provider_quality_start",
        payload,
        _queue_name=PROVIDER_QUALITY_QUEUE_NAME,
        _job_id=f"provider_quality_start_{run_id}",
    )
    stage_suffix = _build_stage_suffix(_normalize_import_id(import_id), run_id)
    print(
        f"Queued provider-quality run: import_id={_normalize_import_id(import_id)} run_id={run_id} "
        f"stage={stage_suffix} test_mode={bool(test_mode)}",
        flush=True,
    )
    return {
        "ok": True,
        "queued": True,
        "run_id": run_id,
        "stage_suffix": stage_suffix,
        "import_id": _normalize_import_id(import_id),
        "test_mode": bool(test_mode),
    }


async def finish_main(
    import_id: str,
    run_id: str,
    test_mode: bool = False,
    manifest_path: str | None = None,
) -> dict[str, Any]:
    redis = await create_pool(build_redis_settings(), job_serializer=serialize_job, job_deserializer=deserialize_job)
    stage_suffix = _build_stage_suffix(_normalize_import_id(import_id), run_id)
    payload = {
        "import_id": import_id,
        "run_id": run_id,
        "stage_suffix": stage_suffix,
        "test_mode": bool(test_mode),
    }
    if manifest_path:
        payload["manifest_path"] = manifest_path
    await redis.enqueue_job(
        "provider_quality_finalize",
        payload,
        _queue_name=PROVIDER_QUALITY_FINISH_QUEUE_NAME,
        _job_id=f"provider_quality_finalize_{run_id}",
    )
    print(
        f"Queued provider-quality finalize: import_id={_normalize_import_id(import_id)} run_id={run_id} stage={stage_suffix}",
        flush=True,
    )
    return {
        "ok": True,
        "queued": True,
        "run_id": run_id,
        "stage_suffix": stage_suffix,
        "import_id": _normalize_import_id(import_id),
    }


__all__ = [
    "main",
    "provider_quality_start",
    "provider_quality_process_chunk",
    "provider_quality_materialize_lsh_shard",
    "provider_quality_materialize_measure_shard",
    "provider_quality_materialize_domain_shard",
    "provider_quality_materialize_score_shard",
    "provider_quality_finalize",
    "finish_main",
]
