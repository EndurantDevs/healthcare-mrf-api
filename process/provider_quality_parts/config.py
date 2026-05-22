# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import math
import os
import re
from dataclasses import dataclass


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
_raw_provider_quality_shard_work_mem = str(os.getenv("HLTHPRT_PROVIDER_QUALITY_SHARD_WORK_MEM", "64MB")).strip()
if re.fullmatch(r"\d+(kB|MB|GB)", _raw_provider_quality_shard_work_mem):
    PROVIDER_QUALITY_SHARD_WORK_MEM = _raw_provider_quality_shard_work_mem
else:
    PROVIDER_QUALITY_SHARD_WORK_MEM = "64MB"
PROVIDER_QUALITY_SHARD_JIT = "on" if _env_bool("HLTHPRT_PROVIDER_QUALITY_SHARD_JIT", default=False) else "off"
PROVIDER_QUALITY_SHARD_MAX_PARALLEL_GATHER = int(os.getenv("HLTHPRT_PROVIDER_QUALITY_SHARD_MAX_PARALLEL_GATHER", "0"))
PROVIDER_QUALITY_NATIONAL_MIN_PEER_N = max(
    int(os.getenv("HLTHPRT_PROVIDER_QUALITY_NATIONAL_MIN_PEER_N", "100")),
    1,
)
PROVIDER_QUALITY_MINHASH_NUM_PERM = max(int(os.getenv("HLTHPRT_PROVIDER_QUALITY_MINHASH_NUM_PERM", "108")), 1)
PROVIDER_QUALITY_MINHASH_BANDS = max(int(os.getenv("HLTHPRT_PROVIDER_QUALITY_MINHASH_BANDS", "36")), 1)
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
PROVIDER_QUALITY_ZIP_MAX_RADIUS_MILES = max(float(os.getenv("HLTHPRT_PROVIDER_QUALITY_ZIP_MAX_RADIUS_MILES", "50")), 1.0)
PROVIDER_QUALITY_ZIP_RADIUS_STEP_MILES = max(float(os.getenv("HLTHPRT_PROVIDER_QUALITY_ZIP_RADIUS_STEP_MILES", "10")), 1.0)
PROVIDER_QUALITY_ZIP_MAX_RING = max(
    1,
    int(math.ceil(PROVIDER_QUALITY_ZIP_MAX_RADIUS_MILES / PROVIDER_QUALITY_ZIP_RADIUS_STEP_MILES)),
)
PROVIDER_QUALITY_SHRINKAGE_ALPHA = max(float(os.getenv("HLTHPRT_PROVIDER_QUALITY_SHRINKAGE_ALPHA", "100.0")), 1.0)
PROVIDER_QUALITY_DEFAULT_SVI = min(max(float(os.getenv("HLTHPRT_PROVIDER_QUALITY_DEFAULT_SVI", "0.5")), 0.0), 1.0)
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
