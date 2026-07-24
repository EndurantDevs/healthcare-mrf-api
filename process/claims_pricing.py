# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import datetime
import hashlib
import json
import logging
import os
import re
import asyncio
import csv
import sys
import shutil
import secrets
import time
import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import aiohttp
from aiofile import async_open
from aiocsv import AsyncDictReader
from arq import create_pool
from arq import Retry

from db.connection import db
from db.models import (
    CodeCatalog,
    CodeCrosswalk,
    PricingProcedure,
    PricingProcedureGeoBenchmark,
    PricingProvider,
    PricingProviderProcedureCostProfile,
    PricingProviderProcedure,
    PricingProviderProcedureLocation,
    PricingProcedurePeerStats,
)
from process.ext.utils import (
    download_it_and_save,
    ensure_database,
    get_import_schema,
    get_http_client,
    make_class,
    push_objects,
    return_checksum,
)
from process.redis_config import build_redis_settings
from process.serialization import deserialize_job, serialize_job
from process.control_lifecycle import mark_control_run
from process.live_progress import enqueue_live_progress

logger = logging.getLogger(__name__)


def _safe_print(*args: Any, **kwargs: Any) -> None:
    try:
        print(*args, **kwargs)
    except (BrokenPipeError, OSError, ValueError):
        return


# CMS CSV rows can exceed Python's default field limit (131072).
_csv_limit = sys.maxsize
while True:
    try:
        csv.field_size_limit(_csv_limit)
        break
    except OverflowError:
        _csv_limit = _csv_limit // 10

CATALOG_URL = "https://data.cms.gov/data.json"

PROVIDER_LANDING_PAGE = (
    "https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners/"
    "medicare-physician-other-practitioners-by-provider"
)
PROVIDER_SERVICE_LANDING_PAGE = (
    "https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners/"
    "medicare-physician-other-practitioners-by-provider-and-service"
)
GEO_SERVICE_LANDING_PAGE = (
    "https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners/"
    "medicare-physician-other-practitioners-by-geography-and-service"
)

TEST_PROVIDER_ROW_LIMIT = int(os.getenv("HLTHPRT_CLAIMS_TEST_PROVIDER_ROWS", "5000"))
TEST_PROVIDER_SERVICE_ROW_LIMIT = int(os.getenv("HLTHPRT_CLAIMS_TEST_PROVIDER_SERVICE_ROWS", os.getenv("HLTHPRT_CLAIMS_TEST_PROVIDER_DRUG_ROWS", "10000")))
TEST_GEO_SERVICE_ROW_LIMIT = int(os.getenv("HLTHPRT_CLAIMS_TEST_GEO_SERVICE_ROWS", os.getenv("HLTHPRT_CLAIMS_TEST_DRUG_SPENDING_ROWS", "2000")))
TEST_MAX_DOWNLOAD_BYTES = int(os.getenv("HLTHPRT_CLAIMS_TEST_MAX_DOWNLOAD_BYTES", str(25 * 1024 * 1024)))
IMPORT_BATCH_SIZE = max(int(os.getenv("HLTHPRT_CLAIMS_IMPORT_BATCH_SIZE", "100000")), 100)
DOWNLOAD_RETRIES = max(int(os.getenv("HLTHPRT_CLAIMS_DOWNLOAD_RETRIES", "3")), 1)
CLAIMS_USE_PROXY = os.getenv("HLTHPRT_CLAIMS_USE_PROXY", "false").strip().lower() in {"1", "true", "yes", "on"}
ROW_PROGRESS_INTERVAL_SECONDS = max(float(os.getenv("HLTHPRT_CLAIMS_ROW_PROGRESS_SECONDS", "2")), 0.5)
INTERNAL_CODE_SYSTEM = "HP_PROCEDURE_CODE"
SOURCE_OBSERVED_PROCEDURE_ATTRIBUTION = (
    "Source-observed label from claims/pricing data; not an AMA CPT or ADA CDT reference import."
)
# Default to the latest stable CMS year for faster imports; can be widened via env.
CLAIMS_MIN_YEAR = max(int(os.getenv("HLTHPRT_CLAIMS_MIN_YEAR", "2023")), 2013)
CLAIMS_MAX_YEAR = max(int(os.getenv("HLTHPRT_CLAIMS_MAX_YEAR", "2023")), CLAIMS_MIN_YEAR)
CLAIMS_YEAR_WINDOW = tuple(range(CLAIMS_MIN_YEAR, CLAIMS_MAX_YEAR + 1))


def _is_env_enabled(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on", "y"}


_env_bool = _is_env_enabled


CLAIMS_PARALLEL_LOAD = _env_bool("HLTHPRT_CLAIMS_PARALLEL_LOAD", default=True)
CLAIMS_QUEUE_NAME = "arq:ClaimsPricing"
CLAIMS_FINISH_QUEUE_NAME = "arq:ClaimsPricing_finish"
CLAIMS_CHUNK_TARGET_MB = max(int(os.getenv("HLTHPRT_CLAIMS_CHUNK_TARGET_MB", "128")), 4)
CLAIMS_CHUNK_TARGET_BYTES = CLAIMS_CHUNK_TARGET_MB * 1024 * 1024
CLAIMS_FINISH_RETRY_SECONDS = max(int(os.getenv("HLTHPRT_CLAIMS_FINISH_RETRY_SECONDS", "15")), 1)
CLAIMS_REDIS_TTL_SECONDS = max(int(os.getenv("HLTHPRT_CLAIMS_REDIS_TTL_SECONDS", "172800")), 3600)
CLAIMS_WORKDIR = os.getenv("HLTHPRT_CLAIMS_WORKDIR", "/tmp/healthporta_claims")
CLAIMS_KEEP_WORKDIR = _env_bool("HLTHPRT_CLAIMS_KEEP_WORKDIR", default=False)
CLAIMS_DOWNLOAD_CONCURRENCY = max(int(os.getenv("HLTHPRT_CLAIMS_DOWNLOAD_CONCURRENCY", "3")), 1)
CLAIMS_DEFER_STAGE_INDEXES = _env_bool("HLTHPRT_CLAIMS_DEFER_STAGE_INDEXES", default=True)
CLAIMS_DB_DEADLOCK_RETRIES = max(int(os.getenv("HLTHPRT_CLAIMS_DB_DEADLOCK_RETRIES", "6")), 1)
CLAIMS_DB_DEADLOCK_BASE_DELAY_SECONDS = max(
    float(os.getenv("HLTHPRT_CLAIMS_DB_DEADLOCK_BASE_DELAY_SECONDS", "0.25")), 0.05
)
CLAIMS_PROVIDER_DRUG_MAX_BUCKETS = max(
    int(os.getenv("HLTHPRT_CLAIMS_PROVIDER_DRUG_MAX_BUCKETS", "64")), 1
)
CLAIMS_MARK_DONE_RETRIES = max(int(os.getenv("HLTHPRT_CLAIMS_MARK_DONE_RETRIES", "8")), 1)
CLAIMS_MARK_DONE_RETRY_BASE_SECONDS = max(float(os.getenv("HLTHPRT_CLAIMS_MARK_DONE_RETRY_BASE_SECONDS", "0.5")), 0.05)
CLAIMS_MARK_DONE_RETRY_MAX_SECONDS = max(
    float(os.getenv("HLTHPRT_CLAIMS_MARK_DONE_RETRY_MAX_SECONDS", "10")),
    CLAIMS_MARK_DONE_RETRY_BASE_SECONDS,
)
COST_LEVEL_MIN_PEER_CLAIMS = max(int(os.getenv("HLTHPRT_COST_LEVEL_MIN_PEER_CLAIMS", "11")), 1)
COST_LEVEL_MIN_PEER_PROVIDERS = max(int(os.getenv("HLTHPRT_COST_LEVEL_MIN_PEER_PROVIDERS", "10")), 2)
COST_LEVEL_OUTLIER_IQR_FACTOR = max(float(os.getenv("HLTHPRT_COST_LEVEL_OUTLIER_IQR_FACTOR", "1.5")), 0.0)
MAX_NPI = 9_999_999_999
SERVICE_CODE_PATTERN = re.compile(r"^[A-Z0-9]{5}$")
SERVICE_CODE_EXTRACT_PATTERN = re.compile(r"\b([A-Z0-9]{5})\b")


@dataclass(frozen=True)
class DatasetConfig:
    key: str
    landing_page: str
    row_limit_test: int


DATASETS = (
    DatasetConfig("provider", PROVIDER_LANDING_PAGE, TEST_PROVIDER_ROW_LIMIT),
    DatasetConfig("provider_service", PROVIDER_SERVICE_LANDING_PAGE, TEST_PROVIDER_SERVICE_ROW_LIMIT),
    DatasetConfig("geo_service", GEO_SERVICE_LANDING_PAGE, TEST_GEO_SERVICE_ROW_LIMIT),
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


def _state_key(run_id: str, suffix: str) -> str:
    return f"claims_pricing:{run_id}:{suffix}"


def _run_dir(import_id: str, run_id: str) -> Path:
    return Path(CLAIMS_WORKDIR) / import_id / run_id


def _manifest_path(work_dir: Path) -> Path:
    return work_dir / "manifest.json"


def _read_manifest(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def _write_manifest(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=True, indent=2, sort_keys=True)


def _staging_classes(stage_suffix: str, schema: str) -> dict[str, type]:
    return {
        cls.__name__: make_class(cls, stage_suffix, schema_override=schema)
        for cls in (
            PricingProvider,
            PricingProcedure,
            PricingProviderProcedure,
            PricingProviderProcedureLocation,
            PricingProviderProcedureCostProfile,
            PricingProcedurePeerStats,
            PricingProcedureGeoBenchmark,
        )
    }


def _chunk_job_id(run_id: str, dataset_key: str, source_index: int, reporting_year: int, chunk_index: int) -> str:
    return f"claims_chunk_{run_id}_{dataset_key}_{reporting_year}_{source_index}_{chunk_index}"


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

    await redis.delete(total_key, done_key, lock_key, finalized_key)
    await redis.set(total_key, str(total_chunks))
    await redis.expire(total_key, CLAIMS_REDIS_TTL_SECONDS)
    await redis.sadd(done_key, "__init__")
    await redis.srem(done_key, "__init__")
    await redis.expire(done_key, CLAIMS_REDIS_TTL_SECONDS)


async def _increment_total_chunks(redis, run_id: str, delta: int) -> None:
    if delta <= 0:
        return
    total_key = _state_key(run_id, "total_chunks")
    await redis.incrby(total_key, int(delta))
    await redis.expire(total_key, CLAIMS_REDIS_TTL_SECONDS)


async def _mark_chunk_done(redis, run_id: str, chunk_id: str) -> None:
    done_key = _state_key(run_id, "done_chunks")
    await redis.sadd(done_key, chunk_id)
    await redis.expire(done_key, CLAIMS_REDIS_TTL_SECONDS)


async def _mark_chunk_done_with_retry(redis, run_id: str, chunk_id: str) -> None:
    last_exc: Exception | None = None
    for attempt in range(1, CLAIMS_MARK_DONE_RETRIES + 1):
        try:
            await _mark_chunk_done(redis, run_id, chunk_id)
            return
        except Exception as exc:
            last_exc = exc
            if attempt >= CLAIMS_MARK_DONE_RETRIES:
                break
            delay = min(
                CLAIMS_MARK_DONE_RETRY_BASE_SECONDS * (2 ** (attempt - 1)),
                CLAIMS_MARK_DONE_RETRY_MAX_SECONDS,
            )
            logger.warning(
                "Retrying mark_chunk_done for run_id=%s chunk_id=%s after error (%s/%s): %r",
                run_id,
                chunk_id,
                attempt,
                CLAIMS_MARK_DONE_RETRIES,
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


async def _is_finalize_lock_claimed(
    redis,
    run_id: str,
    owner_token: str | None = None,
) -> bool:
    lock_key = _state_key(run_id, "finalize_lock")
    lock_set = await redis.set(
        lock_key,
        owner_token or secrets.token_hex(16),
        ex=CLAIMS_REDIS_TTL_SECONDS,
        nx=True,
    )
    return bool(lock_set)


_claim_finalize_lock = _is_finalize_lock_claimed


async def _is_claims_finalize_lock_released(
    redis: Any,
    run_id: str,
    owner_token: str,
) -> bool:
    """Release only the finalize lock owned by this worker attempt."""

    if redis is None or not run_id or not owner_token:
        return False
    lock_key = _state_key(run_id, "finalize_lock")
    release_script = (
        "if redis.call('get', KEYS[1]) == ARGV[1] then "
        "return redis.call('del', KEYS[1]) else return 0 end"
    )
    released = await redis.eval(
        release_script,
        1,
        lock_key,
        owner_token,
    )
    return bool(released)


def _print_row_progress(stage: str, parsed: int, accepted: int, start_time: float, final: bool = False) -> None:
    elapsed = max(time.monotonic() - start_time, 0.001)
    rate = parsed / elapsed
    line = (
        f"\r[rows:{stage}] parsed={parsed:,} accepted={accepted:,} "
        f"rate={rate:,.0f} rows/s"
    )
    _safe_print(line, end="\n" if final else "", flush=True)


def _step_start(label: str) -> float:
    start = time.monotonic()
    _safe_print(f"[step] START {label}", flush=True)
    return start


def _step_end(label: str, started_at: float) -> None:
    elapsed = max(time.monotonic() - started_at, 0.001)
    _safe_print(f"[step] DONE  {label} in {elapsed:.1f}s", flush=True)


def _step_failed(
    label: str,
    started_at: float,
    error: BaseException,
) -> None:
    elapsed = max(time.monotonic() - started_at, 0.001)
    _safe_print(
        f"[step] FAILED {label} in {elapsed:.1f}s "
        f"({type(error).__name__})",
        flush=True,
    )


async def _run_timed_step(label: str, coro) -> None:
    started_at = _step_start(label)
    try:
        await coro
    except BaseException as error:
        _step_failed(label, started_at, error)
        raise
    _step_end(label, started_at)


def _normalize_import_id(import_id: str | None) -> str:
    if not import_id:
        return datetime.date.today().strftime("%Y%m%d")
    normalized = "".join(ch if ch.isalnum() else "_" for ch in str(import_id))
    return normalized or datetime.date.today().strftime("%Y%m%d")


def _is_deadlock_error(exc: BaseException) -> bool:
    return "deadlock detected" in str(exc).lower()


def _dedupe_rows(rows: list[dict[str, Any]], key_fields: tuple[str, ...]) -> list[dict[str, Any]]:
    if not rows:
        return rows
    row_by_key: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in rows:
        row_by_key[tuple(row.get(field) for field in key_fields)] = row
    return list(row_by_key.values())


def _chunk_rows(rows: list[dict[str, Any]], size: int) -> list[list[dict[str, Any]]]:
    if not rows:
        return []
    return [rows[idx: idx + size] for idx in range(0, len(rows), size)]


def _sum_optional(a: float | None, b: float | None) -> float | None:
    if a is None and b is None:
        return None
    return float(a or 0.0) + float(b or 0.0)


async def _push_objects_with_retry(
    rows: list[dict[str, Any]],
    cls: type,
    *,
    rewrite: bool = False,
    use_copy: bool = True,
) -> None:
    if not rows:
        return
    for attempt in range(1, CLAIMS_DB_DEADLOCK_RETRIES + 1):
        try:
            await push_objects(rows, cls, rewrite=rewrite, use_copy=use_copy)
            return
        except Exception as exc:
            if not _is_deadlock_error(exc) or attempt >= CLAIMS_DB_DEADLOCK_RETRIES:
                raise
            delay = CLAIMS_DB_DEADLOCK_BASE_DELAY_SECONDS * (2 ** (attempt - 1))
            logger.warning(
                "Deadlock while inserting into %s (attempt %s/%s), retrying in %.2fs",
                cls.__tablename__,
                attempt,
                CLAIMS_DB_DEADLOCK_RETRIES,
                delay,
            )
            await asyncio.sleep(delay)


def _build_stage_suffix(import_id: str, run_id: str) -> str:
    base = "".join(ch if ch.isalnum() else "_" for ch in import_id).strip("_")[:12] or "import"
    checksum = return_checksum([import_id, run_id]) & 0xFFFFFFFF
    return f"{base}_{checksum:08x}"


def _normalize_landing_page(url: str) -> str:
    normalized = (url or "").strip().rstrip("/")
    if normalized.endswith("/data"):
        normalized = normalized[:-5]
    return normalized


def _extract_reporting_year(url: str) -> int:
    patterns = (
        r"DY(\d{2})(?:_|\.|$)",
        r"_D(\d{2})(?:_|\.|$)",
    )
    for pattern in patterns:
        match = re.search(pattern, url, flags=re.IGNORECASE)
        if not match:
            continue
        short_year = int(match.group(1))
        return 2000 + short_year
    return -1


def _parse_modified(distribution: dict[str, Any]) -> str:
    return str(distribution.get("modified") or distribution.get("issued") or "")


def _csv_distributions(dataset: dict[str, Any]) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for distribution in dataset.get("distribution", []):
        url = str(distribution.get("downloadURL") or "").strip()
        if not url:
            continue
        media_type = str(distribution.get("mediaType") or "").lower()
        fmt = str(distribution.get("format") or "").lower()
        lower_url = url.lower()
        if not (
            "csv" in media_type
            or fmt == "csv"
            or lower_url.endswith(".csv")
            or lower_url.endswith(".csv.gz")
        ):
            continue
        candidates.append(distribution)
    return candidates


def _select_csv_distribution(dataset: dict[str, Any]) -> dict[str, Any]:
    candidates = _csv_distributions(dataset)
    if not candidates:
        raise LookupError(f"No CSV distribution found for dataset: {dataset.get('title')}")

    candidates.sort(
        key=lambda item: (
            _extract_reporting_year(str(item.get("downloadURL") or "")),
            _parse_modified(item),
            str(item.get("downloadURL") or ""),
        ),
        reverse=True,
    )
    return candidates[0]


def _select_csv_distributions_by_year(
    dataset: dict[str, Any],
    years: set[int],
) -> dict[int, dict[str, Any]]:
    distribution_by_year: dict[int, dict[str, Any]] = {}
    for distribution in _csv_distributions(dataset):
        url = str(distribution.get("downloadURL") or "")
        year = _extract_reporting_year(url)
        if year not in years:
            continue
        previous = distribution_by_year.get(year)
        if previous is None:
            distribution_by_year[year] = distribution
            continue
        previous_key = (_parse_modified(previous), str(previous.get("downloadURL") or ""))
        candidate_key = (_parse_modified(distribution), url)
        if candidate_key > previous_key:
            distribution_by_year[year] = distribution
    return distribution_by_year


def _find_dataset(catalog: dict[str, Any], landing_page: str) -> dict[str, Any]:
    wanted = _normalize_landing_page(landing_page)
    for dataset in catalog.get("dataset", []):
        candidate = _normalize_landing_page(str(dataset.get("landingPage") or ""))
        if candidate == wanted:
            return dataset
    raise LookupError(f"CMS dataset not found for landing page: {landing_page}")


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

    if re.fullmatch(r"\d+\.0+", text):
        text = text.split(".", 1)[0]

    if not re.fullmatch(r"\d{1,10}", text):
        return None

    npi = int(text)
    if npi <= 0 or npi > MAX_NPI:
        return None
    return npi


def _to_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).replace("\x00", "").strip()
    return text or None


def _has_value(value: Any) -> bool:
    return value not in (None, "", "*", "NA")


def _normalize_state(value: Any) -> str | None:
    text = _to_str(value)
    if text is None:
        return None
    letters_only = "".join(ch for ch in text.upper() if "A" <= ch <= "Z")
    if len(letters_only) == 2:
        return letters_only
    return None


def _normalize_zip5(value: Any) -> str | None:
    text = _to_str(value)
    if text is None:
        return None
    match = re.search(r"\d{5}", text)
    if match:
        return match.group(0)
    return None


def _normalize_state_fips(value: Any) -> str | None:
    text = _to_str(value)
    if text is None:
        return None
    if re.fullmatch(r"\d+(\.0+)?", text):
        digits = text.split(".", 1)[0]
    else:
        digits = "".join(ch for ch in text if ch.isdigit())
    digits = digits.lstrip("0") or "0"
    if 1 <= len(digits) <= 4:
        return digits
    return None


def _row_value(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row:
            return row.get(key)
    return None


def _provider_name(last_org_name: str | None, first_name: str | None) -> str | None:
    if last_org_name and first_name:
        return f"{last_org_name}, {first_name}"
    return last_org_name or first_name


def _normalize_service_code(raw_code: str | None) -> str | None:
    if raw_code is None:
        return None
    value = str(raw_code).strip().upper()
    if not value:
        return None
    if SERVICE_CODE_PATTERN.fullmatch(value) and any(ch.isdigit() for ch in value):
        return value
    # Some CMS rows include additional text around the code; recover a 5-char code token.
    match = SERVICE_CODE_EXTRACT_PATTERN.search(value)
    if match:
        token = match.group(1)
        if any(ch.isdigit() for ch in token):
            return token
    return None


def _detect_code_system(code: str | None) -> str:
    if code and re.fullmatch(r"\d{5}", code):
        return "CPT"
    if code and re.fullmatch(r"D\d{4}", code):
        return "CDT"
    return "HCPCS"


def _procedure_code_from_service(code_system: str, code: str) -> int:
    return return_checksum([code_system.upper(), code.upper()])


def _provider_key(npi: int, year: int) -> int:
    return _signed_hash64([npi, year])


def _signed_hash64(values: list[Any]) -> int:
    payload = "|".join(str(value) for value in values).encode("utf-8")
    unsigned_value = int.from_bytes(hashlib.blake2b(payload, digest_size=8).digest(), "big", signed=False)
    return unsigned_value - (1 << 63)


def _location_key(
    npi: int,
    year: int,
    procedure_code: int,
    city: str | None,
    state: str | None,
    zip5: str | None,
    key_extra: str | None = None,
) -> int:
    return _signed_hash64([npi, year, procedure_code, city or "", state or "", zip5 or "", key_extra or ""])


def _is_row_allowed_for_test(row_number: int) -> bool:
    # Deterministic sparse sampling pattern for large files.
    return row_number % 11 == 0


_row_allowed_for_test = _is_row_allowed_for_test


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
    classes_by_name: dict[str, type] = {}

    for cls in (
        PricingProvider,
        PricingProcedure,
        PricingProviderProcedure,
        PricingProviderProcedureLocation,
        PricingProviderProcedureCostProfile,
        PricingProcedurePeerStats,
        PricingProcedureGeoBenchmark,
    ):
        obj = make_class(cls, stage_suffix, schema_override=db_schema)
        classes_by_name[cls.__name__] = obj
        await db.status(f"DROP TABLE IF EXISTS {db_schema}.{obj.__tablename__};")
        # Defensive cleanup for rare orphan composite types after interrupted DDL.
        await db.status(f"DROP TYPE IF EXISTS {db_schema}.{obj.__tablename__} CASCADE;")
        await db.create_table(obj.__table__, checkfirst=True)
        if not CLAIMS_DEFER_STAGE_INDEXES:
            await _ensure_indexes(obj, db_schema)

    return classes_by_name, db_schema


async def _build_staging_indexes(classes: dict[str, type], schema: str) -> None:
    for cls_name in (
        "PricingProvider",
        "PricingProcedure",
        "PricingProviderProcedure",
        "PricingProviderProcedureLocation",
        "PricingProviderProcedureCostProfile",
        "PricingProcedurePeerStats",
        "PricingProcedureGeoBenchmark",
    ):
        await _ensure_indexes(classes[cls_name], schema)


async def _ensure_live_code_tables(schema: str) -> None:
    await db.create_table(CodeCatalog.__table__, checkfirst=True)
    await db.create_table(CodeCrosswalk.__table__, checkfirst=True)
    await db.status(
        f"ALTER TABLE {schema}.{CodeCatalog.__tablename__} ADD COLUMN IF NOT EXISTS source_attribution TEXT;"
    )
    await db.status(
        f"ALTER TABLE {schema}.{CodeCrosswalk.__tablename__} ADD COLUMN IF NOT EXISTS source_attribution TEXT;"
    )
    await _ensure_indexes(CodeCatalog, schema)
    await _ensure_indexes(CodeCrosswalk, schema)


async def _fetch_catalog() -> dict[str, Any]:
    last_error: Exception | None = None
    for attempt in range(1, DOWNLOAD_RETRIES + 1):
        try:
            client = await get_http_client(use_proxy=CLAIMS_USE_PROXY)
            async with client:
                response = await client.get(
                    CATALOG_URL,
                    timeout=aiohttp.ClientTimeout(total=120, connect=60, sock_read=120),
                )
                raw = await response.text()
            return json.loads(raw)
        except json.JSONDecodeError as exc:
            last_error = RuntimeError("Invalid CMS catalog payload")
            logger.warning(
                "Retrying catalog fetch (%s/%s) due to JSON decode error: %r",
                attempt,
                DOWNLOAD_RETRIES,
                exc,
            )
        except Exception as exc:
            last_error = exc
            logger.warning("Retrying catalog fetch (%s/%s): %r", attempt, DOWNLOAD_RETRIES, exc)
        await asyncio.sleep(min(3 * attempt, 10))

    if last_error is not None:
        raise last_error
    raise RuntimeError("Failed to fetch CMS catalog")


def _select_csv_distribution_for_test(dataset: dict[str, Any]) -> dict[str, Any]:
    candidates = _csv_distributions(dataset)
    if not candidates:
        raise LookupError(f"No CSV distribution found for dataset: {dataset.get('title')}")

    # In --test mode prefer the oldest reporting year to keep files smaller and iteration faster.
    candidates.sort(
        key=lambda item: (
            _extract_reporting_year(str(item.get("downloadURL") or "")),
            _parse_modified(item),
            str(item.get("downloadURL") or ""),
        )
    )
    return candidates[0]


def _resolve_sources(catalog: dict[str, Any], test_mode: bool = False) -> dict[str, list[dict[str, Any]]]:
    sources_by_dataset: dict[str, list[dict[str, Any]]] = {}
    requested_years = set(CLAIMS_YEAR_WINDOW)
    for config in DATASETS:
        dataset = _find_dataset(catalog, config.landing_page)
        distributions_by_year = _select_csv_distributions_by_year(dataset, requested_years)
        missing = sorted(requested_years.difference(distributions_by_year.keys()))
        if missing:
            raise LookupError(
                f"Missing CSV distributions for dataset {config.key}: years={missing} "
                f"(requested {sorted(requested_years)})"
            )

        years = sorted(distributions_by_year.keys())
        if test_mode:
            # Keep --test fast by importing a single year, but align with API default behavior (latest year).
            years = [max(years)]

        per_dataset_sources: list[dict[str, Any]] = []
        for year in years:
            distribution = distributions_by_year[year]
            url = str(distribution.get("downloadURL") or "").strip()
            if not url:
                raise LookupError(f"Missing downloadURL for dataset: {config.key}, year={year}")
            per_dataset_sources.append(
                {
                    "url": url,
                    "reporting_year": year,
                    "dataset_title": dataset.get("title"),
                }
            )
        sources_by_dataset[config.key] = per_dataset_sources
    return sources_by_dataset


async def _write_bounded_csv_content(response: Any, path: str, max_bytes: int) -> None:
    downloaded_bytes = 0
    async with async_open(path, "wb+") as destination:
        async for response_chunk in response.content.iter_chunked(1024 * 1024):
            if not response_chunk:
                break
            remaining_bytes = max_bytes - downloaded_bytes
            if remaining_bytes <= 0:
                break
            bounded_chunk = response_chunk[:remaining_bytes]
            await destination.write(bounded_chunk)
            downloaded_bytes += len(bounded_chunk)
            if downloaded_bytes >= max_bytes:
                break


async def _download_csv_head(url: str, path: str, max_bytes: int) -> None:
    """Download a bounded CSV prefix after validating the HTTP response."""

    client = await get_http_client(use_proxy=CLAIMS_USE_PROXY)
    async with client:
        async with client.get(
            url,
            timeout=aiohttp.ClientTimeout(total=600, connect=60, sock_read=600),
        ) as response:
            response.raise_for_status()
            await _write_bounded_csv_content(response, path, max_bytes)


async def _download_source_file(
    dataset_key: str,
    source_by_field: dict[str, Any],
    temp_dir: str,
    test_mode: bool,
    reporting_year: int | None = None,
) -> str:
    Path(temp_dir).mkdir(parents=True, exist_ok=True)
    year_suffix = f"_{reporting_year}" if reporting_year else ""
    filename = f"{dataset_key}{year_suffix}.csv"
    path = str(Path(temp_dir) / filename)
    if test_mode:
        _safe_print(
            f"[test-mode] partial download for {dataset_key}: up to {TEST_MAX_DOWNLOAD_BYTES:,} bytes",
            flush=True,
        )
    for attempt in range(1, DOWNLOAD_RETRIES + 1):
        try:
            if test_mode:
                await _download_csv_head(
                    source_by_field["url"],
                    path,
                    TEST_MAX_DOWNLOAD_BYTES,
                )
            else:
                await download_it_and_save(source_by_field["url"], path)
            return path
        except Retry as exc:
            if attempt >= DOWNLOAD_RETRIES:
                raise
            logger.warning(
                "Retrying download (%s/%s) for %s due to %r",
                attempt,
                DOWNLOAD_RETRIES,
                source_by_field["url"],
                exc,
            )
            await asyncio.sleep(min(5 * attempt, 20))
        except Exception as exc:
            if attempt >= DOWNLOAD_RETRIES:
                raise
            logger.warning(
                "Retrying download (%s/%s) for %s due to %r",
                attempt,
                DOWNLOAD_RETRIES,
                source_by_field["url"],
                exc,
            )
            await asyncio.sleep(min(5 * attempt, 20))

    raise RuntimeError(f"Failed to download dataset: {dataset_key}")


async def _download_sources(sources: dict[str, dict[str, Any]], temp_dir: str, test_mode: bool) -> dict[str, str]:
    Path(temp_dir).mkdir(parents=True, exist_ok=True)

    async def _download_one(key: str, source: dict[str, Any]) -> tuple[str, str]:
        path = await _download_source_file(key, source, temp_dir, test_mode)
        return key, path

    semaphore = asyncio.Semaphore(min(CLAIMS_DOWNLOAD_CONCURRENCY, max(len(sources), 1)))

    async def _bounded_download(key: str, source: dict[str, Any]) -> tuple[str, str]:
        async with semaphore:
            return await _download_one(key, source)

    download_tasks = [_bounded_download(key, source) for key, source in sources.items()]
    results = await asyncio.gather(*download_tasks)
    return {key: path for key, path in results}


@dataclass
class _ProviderChunkBuckets:
    chunks_dir: Path
    bucket_count: int
    writer_by_bucket: dict[int, Any]
    handle_by_bucket: dict[int, Any]
    row_count_by_bucket: dict[int, int]

    @classmethod
    def create(cls, chunks_dir: Path, bucket_count: int) -> "_ProviderChunkBuckets":
        """Create empty per-bucket writer state for one source file."""

        return cls(
            chunks_dir=chunks_dir,
            bucket_count=bucket_count,
            writer_by_bucket={},
            handle_by_bucket={},
            row_count_by_bucket={bucket: 0 for bucket in range(bucket_count)},
        )

    def write_source_row(self, source_row: dict[str, Any], npi: int) -> None:
        """Write one source row to the bucket deterministically owned by its NPI."""

        bucket = abs(npi) % self.bucket_count
        if bucket not in self.writer_by_bucket:
            chunk_path = self.chunks_dir / f"chunk_{bucket:05d}.csv"
            chunk_handle = open(chunk_path, "w", encoding="utf-8", newline="")
            field_names = [field_name for field_name in source_row if field_name is not None]
            chunk_writer = csv.DictWriter(chunk_handle, fieldnames=field_names, extrasaction="ignore")
            chunk_writer.writeheader()
            self.handle_by_bucket[bucket] = chunk_handle
            self.writer_by_bucket[bucket] = chunk_writer
        self.writer_by_bucket[bucket].writerow(source_row)
        self.row_count_by_bucket[bucket] += 1

    def close(self) -> None:
        """Close every bucket file opened during partitioning."""

        for chunk_handle in self.handle_by_bucket.values():
            chunk_handle.close()

    def build_chunk_entries(self, parsed_rows: int, accepted_rows: int) -> list[dict[str, Any]]:
        """Describe non-empty bucket files using the importer chunk contract."""

        chunk_entries: list[dict[str, Any]] = []
        for bucket in range(self.bucket_count):
            rows_in_bucket = self.row_count_by_bucket.get(bucket, 0)
            if rows_in_bucket <= 0:
                continue
            chunk_entries.append(
                {
                    "dataset_key": "provider_service",
                    "chunk_id": f"provider_service:{len(chunk_entries)}",
                    "chunk_index": len(chunk_entries),
                    "chunk_path": str(self.chunks_dir / f"chunk_{bucket:05d}.csv"),
                    "parsed_rows": parsed_rows,
                    "accepted_rows": accepted_rows,
                    "rows_in_bucket": rows_in_bucket,
                }
            )
        return chunk_entries


def _provider_service_bucket_count(source_path: str) -> int:
    total_size = max(Path(source_path).stat().st_size, 1)
    estimated_chunks = max(1, math.ceil(total_size / max(CLAIMS_CHUNK_TARGET_BYTES, 1)))
    return max(1, min(estimated_chunks, CLAIMS_PROVIDER_DRUG_MAX_BUCKETS))


async def _split_provider_service_into_chunks(
    source_path: str,
    chunks_dir: Path,
    test_mode: bool,
) -> list[dict[str, Any]]:
    """Partition provider-service rows by NPI for parallel loading."""

    chunks_dir.mkdir(parents=True, exist_ok=True)
    chunk_buckets = _ProviderChunkBuckets.create(
        chunks_dir,
        _provider_service_bucket_count(source_path),
    )
    row_limit = TEST_PROVIDER_SERVICE_ROW_LIMIT if test_mode else None
    parsed_rows = 0
    accepted_rows = 0
    try:
        async with async_open(source_path, "r", encoding="utf-8-sig") as source_handle:
            async for source_row in AsyncDictReader(source_handle):
                parsed_rows += 1
                if test_mode and not _row_allowed_for_test(parsed_rows):
                    continue
                if test_mode and row_limit is not None and accepted_rows >= row_limit:
                    break
                npi = _to_npi(_row_value(source_row, "Rndrng_NPI", "PRSCRBR_NPI", "Prscrbr_NPI"))
                if npi is None:
                    continue
                chunk_buckets.write_source_row(source_row, npi)
                accepted_rows += 1
    finally:
        chunk_buckets.close()

    chunk_entries = chunk_buckets.build_chunk_entries(parsed_rows, accepted_rows)
    if not chunk_entries:
        _safe_print(
            f"[warn] no chunks generated for provider_service (parsed={parsed_rows:,}, accepted={accepted_rows:,})",
            flush=True,
        )
    else:
        _safe_print(
            f"[split:provider_service] chunks={len(chunk_entries)} parsed={parsed_rows:,} "
            f"accepted={accepted_rows:,} bucketed_by=npi buckets={chunk_buckets.bucket_count}",
            flush=True,
        )
    return chunk_entries


@dataclass
class _ByteChunkWriter:
    dataset_key: str
    chunks_dir: Path
    header_line: bytes
    chunk_entries: list[dict[str, Any]]
    chunk_handle: Any = None
    chunk_path: Path | None = None
    current_size: int = 0

    def _open_chunk(self) -> None:
        self.chunk_path = self.chunks_dir / f"chunk_{len(self.chunk_entries):05d}.csv"
        self.chunk_handle = open(self.chunk_path, "wb")
        self.chunk_handle.write(self.header_line)
        self.current_size = len(self.header_line)

    def write_line(self, source_line: bytes, parsed_rows: int, accepted_rows: int) -> None:
        """Append one CSV row and close the chunk once its byte target is reached."""

        if self.chunk_handle is None:
            self._open_chunk()
        self.chunk_handle.write(source_line)
        self.current_size += len(source_line)
        if self.current_size >= CLAIMS_CHUNK_TARGET_BYTES:
            self.close_chunk(parsed_rows, accepted_rows)

    def close_chunk(self, parsed_rows: int, accepted_rows: int) -> None:
        """Close the current file and record its cumulative split counters."""

        if self.chunk_handle is None:
            return
        self.chunk_handle.close()
        self.chunk_entries.append(
            {
                "dataset_key": self.dataset_key,
                "chunk_id": f"{self.dataset_key}:{len(self.chunk_entries)}",
                "chunk_index": len(self.chunk_entries),
                "chunk_path": str(self.chunk_path),
                "parsed_rows": parsed_rows,
                "accepted_rows": accepted_rows,
            }
        )
        self.chunk_handle = None
        self.chunk_path = None


async def _split_source_into_chunks(
    dataset_key: str,
    source_path: str,
    chunks_dir: Path,
    test_mode: bool,
) -> list[dict[str, Any]]:
    """Split one claims source into bounded import chunks."""

    if dataset_key == "provider_service":
        return await _split_provider_service_into_chunks(source_path, chunks_dir, test_mode)
    chunks_dir.mkdir(parents=True, exist_ok=True)
    chunk_entries: list[dict[str, Any]] = []
    chunk_writer: _ByteChunkWriter | None = None
    parsed_rows = 0
    accepted_rows = 0
    row_limit = DATASET_BY_KEY[dataset_key].row_limit_test if test_mode else None
    try:
        async with async_open(source_path, "rb") as source_handle:
            async for source_line in source_handle:
                if chunk_writer is None:
                    chunk_writer = _ByteChunkWriter(dataset_key, chunks_dir, source_line, chunk_entries)
                    continue
                parsed_rows += 1
                if test_mode and not _row_allowed_for_test(parsed_rows):
                    continue
                if test_mode and row_limit is not None and accepted_rows >= row_limit:
                    break
                accepted_rows += 1
                chunk_writer.write_line(source_line, parsed_rows, accepted_rows)
    finally:
        if chunk_writer is not None:
            chunk_writer.close_chunk(parsed_rows, accepted_rows)

    if not chunk_entries:
        _safe_print(f"[warn] no chunks generated for {dataset_key} (parsed={parsed_rows:,}, accepted={accepted_rows:,})")
    else:
        _safe_print(
            f"[split:{dataset_key}] chunks={len(chunk_entries)} parsed={parsed_rows:,} accepted={accepted_rows:,}",
            flush=True,
        )
    return chunk_entries


async def _split_sources_to_chunks(
    local_paths: dict[str, str],
    run_dir: Path,
    sources: dict[str, list[dict[str, Any]]],
    test_mode: bool,
) -> list[dict[str, Any]]:
    chunks_root = run_dir / "chunks"
    chunks_root.mkdir(parents=True, exist_ok=True)
    all_chunks: list[dict[str, Any]] = []
    for dataset in DATASETS:
        split_chunks = await _split_source_into_chunks(
            dataset_key=dataset.key,
            source_path=local_paths[dataset.key],
            chunks_dir=chunks_root / dataset.key,
            test_mode=test_mode,
        )
        for chunk in split_chunks:
            source = (sources.get(dataset.key) or [{}])[0]
            chunk["reporting_year"] = max(_safe_int(source.get("reporting_year"), 2013), 2013)
        all_chunks.extend(split_chunks)
    return all_chunks


def _provider_row_from_source(
    source_row: dict[str, Any],
    year: int,
) -> tuple[dict[str, Any] | None, bool]:
    npi = _to_npi(_row_value(source_row, "Rndrng_NPI", "PRSCRBR_NPI", "Prscrbr_NPI"))
    if npi is None:
        return None, False
    raw_state = _row_value(source_row, "Rndrng_Prvdr_State_Abrvtn", "Prscrbr_State_Abrvtn")
    state = _normalize_state(raw_state)
    if _has_value(raw_state) and state is None:
        return None, True
    first_name = _to_str(_row_value(source_row, "Rndrng_Prvdr_First_Name", "Prscrbr_First_Name"))
    last_org_name = _to_str(_row_value(source_row, "Rndrng_Prvdr_Last_Org_Name", "Prscrbr_Last_Org_Name"))
    provider_row_by_field = {
        "provider_key": _provider_key(npi, year),
        "npi": npi,
        "year": year,
        "provider_name": _provider_name(last_org_name, first_name),
        "first_name": first_name,
        "last_org_name": last_org_name,
        "credentials": _to_str(_row_value(source_row, "Rndrng_Prvdr_Crdntls", "Prscrbr_Crdntls")),
        "provider_type": _to_str(_row_value(source_row, "Rndrng_Prvdr_Type", "Prscrbr_Type")),
        "city": _to_str(_row_value(source_row, "Rndrng_Prvdr_City", "Prscrbr_City")),
        "state": state,
        "zip5": _normalize_zip5(_row_value(source_row, "Rndrng_Prvdr_Zip5", "Prscrbr_zip5")),
        "country": _to_str(_row_value(source_row, "Rndrng_Prvdr_Cntry", "Prscrbr_Cntry")),
        "total_services": _to_float(_row_value(source_row, "Tot_Srvcs", "Tot_Clms")),
        "total_distinct_hcpcs_codes": _to_float(
            _row_value(source_row, "Tot_HCPCS_Cds", "Tot_30day_Fills")
        ),
        "total_allowed_amount": _to_float(_row_value(source_row, "Tot_Mdcr_Alowd_Amt", "Tot_Drug_Cst")),
        "total_submitted_charges": _to_float(_row_value(source_row, "Tot_Sbmtd_Chrg", "Tot_Day_Suply")),
        "total_beneficiaries": _to_float(_row_value(source_row, "Tot_Benes")),
    }
    return provider_row_by_field, False


def _maybe_print_row_progress(
    stage: str,
    parsed_rows: int,
    accepted_rows: int,
    progress_start: float,
    progress_last: float,
) -> float:
    current_time = time.monotonic()
    if current_time - progress_last < ROW_PROGRESS_INTERVAL_SECONDS:
        return progress_last
    _print_row_progress(stage, parsed_rows, accepted_rows, progress_start)
    return current_time


async def _flush_provider_rows(provider_rows: list[dict[str, Any]], provider_cls: type) -> None:
    if not provider_rows:
        return
    unique_provider_rows = _dedupe_rows(provider_rows, ("provider_key",))
    await _push_objects_with_retry(unique_provider_rows, provider_cls)
    provider_rows.clear()


async def _load_provider_rows(path: str, provider_cls: type, year: int, test_mode: bool) -> None:
    """Load normalized provider rows from a claims source file."""

    provider_rows: list[dict[str, Any]] = []
    accepted_rows = 0
    invalid_state_rows = 0
    progress_start = time.monotonic()
    progress_last = progress_start
    row_number = 0
    async with async_open(path, "r", encoding="utf-8-sig") as source_handle:
        async for source_row in AsyncDictReader(source_handle):
            row_number += 1
            progress_last = _maybe_print_row_progress(
                "providers", row_number, accepted_rows, progress_start, progress_last
            )
            if test_mode and not _row_allowed_for_test(row_number):
                continue
            provider_row_by_field, has_invalid_state = _provider_row_from_source(source_row, year)
            if provider_row_by_field is None:
                invalid_state_rows += int(has_invalid_state)
                continue
            provider_rows.append(provider_row_by_field)
            accepted_rows += 1
            if len(provider_rows) >= IMPORT_BATCH_SIZE:
                await _flush_provider_rows(provider_rows, provider_cls)
            if test_mode and accepted_rows >= TEST_PROVIDER_ROW_LIMIT:
                break

    await _flush_provider_rows(provider_rows, provider_cls)
    _print_row_progress("providers", row_number, accepted_rows, progress_start, final=True)
    if accepted_rows == 0:
        _safe_print(
            "[warn] providers stage accepted 0 rows; verify CSV columns include NPI values.",
            flush=True,
        )
    if invalid_state_rows:
        _safe_print(
            f"[warn] providers stage skipped {invalid_state_rows:,} rows due to invalid state values.",
            flush=True,
        )


@dataclass(frozen=True)
class _ProviderServiceCandidate:
    npi: int
    year: int
    procedure_code: int
    service_description: str | None
    reported_code: str
    total_services: float | None
    total_beneficiary_day_services: float | None
    total_submitted_charges: float | None
    total_allowed_amount: float | None
    total_beneficiaries: float | None
    city: str | None
    state: str | None
    zip5: str | None
    place_of_service: str | None
    state_fips: str | None
    country: str

    @property
    def procedure_key(self) -> tuple[int, int, int]:
        """Return the stable aggregation key for provider-procedure claims."""

        return self.npi, self.year, self.procedure_code

    @property
    def location_key(self) -> int:
        """Return the stable provider-procedure-location identity."""

        return _location_key(
            self.npi,
            self.year,
            self.procedure_code,
            self.city,
            self.state,
            self.zip5,
            key_extra=self.place_of_service,
        )


def _weighted_total(average_amount: float | None, total_services: float | None) -> float | None:
    if average_amount is None:
        return None
    if total_services is None:
        return average_amount
    return average_amount * total_services


def _provider_service_candidate(
    source_row: dict[str, Any],
    year: int,
) -> tuple[_ProviderServiceCandidate | None, bool]:
    npi = _to_npi(_row_value(source_row, "Rndrng_NPI", "PRSCRBR_NPI", "Prscrbr_NPI"))
    service_code = _normalize_service_code(_row_value(source_row, "HCPCS_Cd", "HCPCS_CD"))
    if npi is None or service_code is None:
        return None, False
    raw_state = _row_value(source_row, "Rndrng_Prvdr_State_Abrvtn", "Prscrbr_State_Abrvtn")
    state = _normalize_state(raw_state)
    if _has_value(raw_state) and state is None:
        return None, True
    total_services = _to_float(_row_value(source_row, "Tot_Srvcs", "Tot_Clms"))
    average_allowed_amount = _to_float(_row_value(source_row, "Avg_Mdcr_Alowd_Amt"))
    average_submitted_charge = _to_float(_row_value(source_row, "Avg_Sbmtd_Chrg"))
    return (
        _ProviderServiceCandidate(
            npi=npi,
            year=year,
            procedure_code=_procedure_code_from_service(_detect_code_system(service_code), service_code),
            service_description=_to_str(_row_value(source_row, "HCPCS_Desc", "HCPCS_DESC")),
            reported_code=service_code,
            total_services=total_services,
            total_beneficiary_day_services=_to_float(
                _row_value(source_row, "Tot_Bene_Day_Srvcs", "Tot_30day_Fills")
            ),
            total_submitted_charges=_weighted_total(average_submitted_charge, total_services),
            total_allowed_amount=_weighted_total(average_allowed_amount, total_services),
            total_beneficiaries=_to_float(_row_value(source_row, "Tot_Benes")),
            city=_to_str(_row_value(source_row, "Rndrng_Prvdr_City", "Prscrbr_City")),
            state=state,
            zip5=_normalize_zip5(_row_value(source_row, "Rndrng_Prvdr_Zip5", "Prscrbr_zip5")),
            place_of_service=_to_str(_row_value(source_row, "Place_Of_Srvc", "PLACE_OF_SRVC")),
            state_fips=_normalize_state_fips(
                _row_value(source_row, "Rndrng_Prvdr_State_FIPS", "Prscrbr_State_FIPS")
            ),
            country=_to_str(_row_value(source_row, "Rndrng_Prvdr_Cntry", "Prscrbr_Cntry")) or "US",
        ),
        False,
    )


def _provider_procedure_fields(candidate: _ProviderServiceCandidate) -> dict[str, Any]:
    return {
        "npi": candidate.npi,
        "year": candidate.year,
        "procedure_code": candidate.procedure_code,
        "service_description": candidate.service_description,
        "reported_code": candidate.reported_code,
        "total_services": candidate.total_services,
        "total_beneficiary_day_services": candidate.total_beneficiary_day_services,
        "total_submitted_charges": candidate.total_submitted_charges,
        "total_allowed_amount": candidate.total_allowed_amount,
        "total_beneficiaries": candidate.total_beneficiaries,
        "ge65_total_services": None,
        "ge65_total_allowed_amount": None,
        "ge65_total_beneficiaries": None,
    }


def _merge_provider_procedure_fields(
    accumulated_fields: dict[str, Any],
    candidate_fields: dict[str, Any],
) -> None:
    amount_fields = (
        "total_services",
        "total_beneficiary_day_services",
        "total_submitted_charges",
        "total_allowed_amount",
        "total_beneficiaries",
    )
    for field_name in amount_fields:
        accumulated_fields[field_name] = _sum_optional(
            accumulated_fields.get(field_name),
            candidate_fields.get(field_name),
        )
    for descriptive_field in ("service_description", "reported_code"):
        if not accumulated_fields.get(descriptive_field) and candidate_fields.get(descriptive_field):
            accumulated_fields[descriptive_field] = candidate_fields[descriptive_field]


def _provider_location_fields(candidate: _ProviderServiceCandidate) -> dict[str, Any]:
    return {
        "location_key": candidate.location_key,
        "npi": candidate.npi,
        "year": candidate.year,
        "procedure_code": candidate.procedure_code,
        "place_of_service": candidate.place_of_service,
        "city": candidate.city,
        "state": candidate.state,
        "zip5": candidate.zip5,
        "state_fips": candidate.state_fips,
        "country": candidate.country,
    }


async def _flush_location_rows(location_rows: list[dict[str, Any]], location_cls: type) -> None:
    if not location_rows:
        return
    unique_location_rows = _dedupe_rows(location_rows, ("location_key",))
    unique_location_rows.sort(key=lambda location_fields: location_fields.get("location_key"))
    await _push_objects_with_retry(unique_location_rows, location_cls)
    location_rows.clear()


async def _flush_provider_procedure_rows(
    provider_procedure_by_key: dict[tuple[int, int, int], dict[str, Any]],
    provider_procedure_cls: type,
) -> None:
    procedure_rows = list(provider_procedure_by_key.values())
    procedure_rows.sort(
        key=lambda procedure_fields: (
            procedure_fields.get("npi"),
            procedure_fields.get("year"),
            procedure_fields.get("procedure_code"),
        )
    )
    for procedure_batch in _chunk_rows(procedure_rows, IMPORT_BATCH_SIZE):
        await _push_objects_with_retry(procedure_batch, provider_procedure_cls)


async def _load_provider_service_rows(
    path: str,
    provider_procedure_cls: type,
    location_cls: type,
    year: int,
    test_mode: bool,
) -> None:
    """Load provider procedure and location rows from claims data."""

    provider_procedure_by_key: dict[tuple[int, int, int], dict[str, Any]] = {}
    location_rows: list[dict[str, Any]] = []
    seen_location_keys: set[int] = set()
    accepted_rows = 0
    invalid_state_rows = 0
    progress_start = time.monotonic()
    progress_last = progress_start
    row_number = 0
    async with async_open(path, "r", encoding="utf-8-sig") as source_handle:
        async for source_row in AsyncDictReader(source_handle):
            row_number += 1
            progress_last = _maybe_print_row_progress(
                "provider_service", row_number, accepted_rows, progress_start, progress_last
            )
            if test_mode and not _row_allowed_for_test(row_number):
                continue
            candidate, has_invalid_state = _provider_service_candidate(source_row, year)
            if candidate is None:
                invalid_state_rows += int(has_invalid_state)
                continue
            candidate_fields = _provider_procedure_fields(candidate)
            accumulated_fields = provider_procedure_by_key.get(candidate.procedure_key)
            if accumulated_fields is None:
                provider_procedure_by_key[candidate.procedure_key] = candidate_fields
            else:
                _merge_provider_procedure_fields(accumulated_fields, candidate_fields)
            if candidate.location_key not in seen_location_keys:
                seen_location_keys.add(candidate.location_key)
                location_rows.append(_provider_location_fields(candidate))
            if len(location_rows) >= IMPORT_BATCH_SIZE:
                await _flush_location_rows(location_rows, location_cls)
            accepted_rows += 1
            if test_mode and accepted_rows >= TEST_PROVIDER_SERVICE_ROW_LIMIT:
                break

    await _flush_location_rows(location_rows, location_cls)
    await _flush_provider_procedure_rows(provider_procedure_by_key, provider_procedure_cls)
    _print_row_progress("provider_service", row_number, accepted_rows, progress_start, final=True)
    if invalid_state_rows:
        _safe_print(
            f"[warn] provider_service stage skipped {invalid_state_rows:,} rows due to invalid state values.",
            flush=True,
        )


def _geo_level_priority(source_row: dict[str, Any]) -> int:
    geo_level = str(
        _row_value(source_row, "Rndrng_Prvdr_Geo_Lvl", "RNDRNG_PRVDR_GEO_LVL") or ""
    ).strip().lower()
    if geo_level == "national":
        return 3
    if geo_level == "state":
        return 2
    if geo_level == "county":
        return 1
    return 0


def _geo_scope_value_from_row(source_row: dict[str, Any]) -> tuple[str, str] | None:
    geo_level = str(
        _row_value(source_row, "Rndrng_Prvdr_Geo_Lvl", "RNDRNG_PRVDR_GEO_LVL") or ""
    ).strip().lower()
    if geo_level == "national":
        return "national", "US"
    if geo_level == "state":
        state = _normalize_state(
            _row_value(
                source_row,
                "Rndrng_Prvdr_State_Abrvtn",
                "RNDRNG_PRVDR_STATE_ABRVTN",
                "Prscrbr_State_Abrvtn",
            )
        )
        if state:
            return "state", state
    return None


@dataclass(frozen=True)
class _GeoServiceCandidate:
    procedure_code: int
    total_services: float
    procedure_priority: int
    procedure_row_by_field: dict[str, Any]
    benchmark_key: tuple[int, int, str, str] | None
    benchmark_row_by_field: dict[str, Any] | None


def _geo_service_candidate(source_row: dict[str, Any], year: int) -> _GeoServiceCandidate | None:
    service_code = _normalize_service_code(_row_value(source_row, "HCPCS_Cd", "HCPCS_CD"))
    if service_code is None:
        return None
    procedure_code = _procedure_code_from_service(_detect_code_system(service_code), service_code)
    total_services = _to_float(_row_value(source_row, "Tot_Srvcs", "TOT_SRVCS")) or 0.0
    average_allowed_amount = _to_float(
        _row_value(source_row, "Avg_Mdcr_Alowd_Amt", "AVG_MDCR_ALOWD_AMT")
    )
    average_payment_amount = _to_float(
        _row_value(source_row, "Avg_Mdcr_Pymt_Amt", "AVG_MDCR_PYMT_AMT")
    )
    average_standardized_amount = _to_float(
        _row_value(source_row, "Avg_Mdcr_Stdzd_Amt", "AVG_MDCR_STDZD_AMT")
    )
    procedure_row_by_field = {
        "procedure_code": procedure_code,
        "service_description": _to_str(_row_value(source_row, "HCPCS_Desc", "HCPCS_DESC")),
        "reported_code": service_code,
        "avg_submitted_charge": _to_float(_row_value(source_row, "Avg_Sbmtd_Chrg", "AVG_SBMTD_CHRG")),
        "avg_allowed_amount": average_allowed_amount,
        "avg_payment_amount": average_payment_amount,
        "avg_standardized_amount": average_standardized_amount,
        "total_allowed_amount": _weighted_total(average_allowed_amount, total_services),
        "total_services": total_services,
        "total_beneficiaries": _to_float(_row_value(source_row, "Tot_Benes", "TOT_BENES")),
        "source_year": year,
    }
    geography = _geo_scope_value_from_row(source_row)
    if geography is None:
        return _GeoServiceCandidate(
            procedure_code,
            total_services,
            _geo_level_priority(source_row),
            procedure_row_by_field,
            None,
            None,
        )
    geography_scope, geography_value = geography
    benchmark_key = (procedure_code, year, geography_scope, geography_value)
    benchmark_row_by_field = {
        "procedure_code": procedure_code,
        "year": year,
        "geography_scope": geography_scope,
        "geography_value": geography_value,
        "total_services": total_services,
        "avg_submitted_charge": average_allowed_amount,
        "avg_payment_amount": average_payment_amount,
        "avg_standardized_amount": average_standardized_amount,
        "updated_at": datetime.datetime.utcnow(),
    }
    return _GeoServiceCandidate(
        procedure_code,
        total_services,
        _geo_level_priority(source_row),
        procedure_row_by_field,
        benchmark_key,
        benchmark_row_by_field,
    )


async def _push_geo_service_candidates(
    procedure_candidate_by_code: dict[int, tuple[int, float, dict[str, Any]]],
    benchmark_candidate_by_key: dict[tuple[int, int, str, str], tuple[float, dict[str, Any]]],
    procedure_cls: type,
    geo_benchmark_cls: type,
) -> None:
    procedure_rows = [procedure_fields for _priority, _total, procedure_fields in procedure_candidate_by_code.values()]
    if procedure_rows:
        procedure_rows.sort(key=lambda procedure_fields: procedure_fields.get("procedure_code"))
        await _push_objects_with_retry(procedure_rows, procedure_cls, rewrite=True, use_copy=False)
    benchmark_rows = [benchmark_fields for _weight, benchmark_fields in benchmark_candidate_by_key.values()]
    if benchmark_rows:
        benchmark_rows.sort(
            key=lambda benchmark_fields: (
                benchmark_fields.get("year"),
                benchmark_fields.get("procedure_code"),
                benchmark_fields.get("geography_scope"),
                benchmark_fields.get("geography_value"),
            )
        )
        await _push_objects_with_retry(benchmark_rows, geo_benchmark_cls, rewrite=True, use_copy=False)


async def _load_geo_service_rows(
    path: str,
    procedure_cls: type,
    geo_benchmark_cls: type,
    year: int,
    test_mode: bool,
) -> None:
    """Materialize procedure and geographic benchmark rows from one source."""

    procedure_candidate_by_code: dict[int, tuple[int, float, dict[str, Any]]] = {}
    benchmark_candidate_by_key: dict[tuple[int, int, str, str], tuple[float, dict[str, Any]]] = {}
    accepted_rows = 0
    progress_start = time.monotonic()
    progress_last = progress_start
    row_number = 0
    async with async_open(path, "r", encoding="utf-8-sig") as source_handle:
        async for source_row in AsyncDictReader(source_handle):
            row_number += 1
            progress_last = _maybe_print_row_progress(
                "geo_service", row_number, accepted_rows, progress_start, progress_last
            )
            if test_mode and not _row_allowed_for_test(row_number):
                continue
            candidate = _geo_service_candidate(source_row, year)
            if candidate is None:
                continue
            previous_procedure = procedure_candidate_by_code.get(candidate.procedure_code)
            candidate_rank = candidate.procedure_priority, candidate.total_services
            if previous_procedure is None or candidate_rank > previous_procedure[:2]:
                procedure_candidate_by_code[candidate.procedure_code] = (
                    *candidate_rank,
                    candidate.procedure_row_by_field,
                )
            if candidate.benchmark_key is not None and candidate.benchmark_row_by_field is not None:
                previous_benchmark = benchmark_candidate_by_key.get(candidate.benchmark_key)
                if previous_benchmark is None or candidate.total_services > previous_benchmark[0]:
                    benchmark_candidate_by_key[candidate.benchmark_key] = (
                        candidate.total_services,
                        candidate.benchmark_row_by_field,
                    )
            accepted_rows += 1
            if test_mode and accepted_rows >= TEST_GEO_SERVICE_ROW_LIMIT:
                break
    await _push_geo_service_candidates(
        procedure_candidate_by_code,
        benchmark_candidate_by_key,
        procedure_cls,
        geo_benchmark_cls,
    )
    _print_row_progress("geo_service", row_number, accepted_rows, progress_start, final=True)


async def _materialize_code_and_crosswalk_rows(classes: dict[str, type], schema: str) -> None:
    """Build procedure code dimensions and observed crosswalk edges."""

    procedure_table = classes["PricingProcedure"].__tablename__
    code_catalog_table = CodeCatalog.__tablename__
    code_crosswalk_table = CodeCrosswalk.__tablename__

    await db.status(
        f"""
        WITH src AS (
            SELECT
                procedure_code,
                UPPER(BTRIM(reported_code)) AS service_code,
                NULLIF(BTRIM(service_description), '') AS service_desc,
                CASE
                    WHEN UPPER(BTRIM(reported_code)) ~ '^[0-9]{{5}}$' THEN 'CPT'
                    WHEN UPPER(BTRIM(reported_code)) ~ '^D[0-9]{{4}}$' THEN 'CDT'
                    ELSE 'HCPCS'
                END AS primary_system
            FROM {schema}.{procedure_table}
            WHERE COALESCE(BTRIM(reported_code), '') <> ''
              AND UPPER(BTRIM(reported_code)) ~ '^[A-Z0-9]{{5}}$'
              AND UPPER(BTRIM(reported_code)) ~ '[0-9]'
        )
        INSERT INTO {schema}.{code_catalog_table}
            (
                code_system,
                code,
                display_name,
                short_description,
                long_description,
                is_active,
                source,
                source_attribution,
                updated_at
            )
        SELECT
            src.primary_system,
            src.service_code,
            COALESCE(src.service_desc, src.service_code),
            src.service_desc,
            NULL,
            TRUE,
            'cms_physician_provider_service',
            :source_attribution,
            NOW()
        FROM src
        ON CONFLICT (code_system, code) DO UPDATE
        SET
            display_name = excluded.display_name,
            short_description = excluded.short_description,
            is_active = excluded.is_active,
            source = excluded.source,
            source_attribution = excluded.source_attribution,
            updated_at = excluded.updated_at;
        """,
        source_attribution=SOURCE_OBSERVED_PROCEDURE_ATTRIBUTION,
    )

    await db.status(
        f"""
        WITH src AS (
            SELECT
                procedure_code,
                UPPER(BTRIM(reported_code)) AS service_code,
                NULLIF(BTRIM(service_description), '') AS service_desc
            FROM {schema}.{procedure_table}
            WHERE COALESCE(BTRIM(reported_code), '') <> ''
              AND UPPER(BTRIM(reported_code)) ~ '^[A-Z0-9]{{5}}$'
              AND UPPER(BTRIM(reported_code)) ~ '[0-9]'
        )
        INSERT INTO {schema}.{code_catalog_table}
            (
                code_system,
                code,
                display_name,
                short_description,
                long_description,
                is_active,
                source,
                source_attribution,
                updated_at
            )
        SELECT
            '{INTERNAL_CODE_SYSTEM}',
            src.procedure_code::text,
            COALESCE(src.service_desc, src.service_code),
            src.service_desc,
            NULL,
            TRUE,
            'cms_physician_provider_service',
            :source_attribution,
            NOW()
        FROM src
        ON CONFLICT (code_system, code) DO UPDATE
        SET
            display_name = excluded.display_name,
            short_description = excluded.short_description,
            is_active = excluded.is_active,
            source = excluded.source,
            source_attribution = excluded.source_attribution,
            updated_at = excluded.updated_at;
        """,
        source_attribution=SOURCE_OBSERVED_PROCEDURE_ATTRIBUTION,
    )

    await db.status(
        f"""
        WITH src AS (
            SELECT
                procedure_code,
                UPPER(BTRIM(reported_code)) AS service_code,
                NULLIF(BTRIM(service_description), '') AS service_desc
            FROM {schema}.{procedure_table}
            WHERE COALESCE(BTRIM(reported_code), '') <> ''
              AND (
                    UPPER(BTRIM(reported_code)) ~ '^[0-9]{{5}}$'
                 OR UPPER(BTRIM(reported_code)) ~ '^D[0-9]{{4}}$'
              )
        )
        INSERT INTO {schema}.{code_catalog_table}
            (
                code_system,
                code,
                display_name,
                short_description,
                long_description,
                is_active,
                source,
                source_attribution,
                updated_at
            )
        SELECT
            'HCPCS',
            src.service_code,
            COALESCE(src.service_desc, src.service_code),
            src.service_desc,
            NULL,
            TRUE,
            'cms_physician_provider_service',
            :source_attribution,
            NOW()
        FROM src
        ON CONFLICT (code_system, code) DO UPDATE
        SET
            display_name = excluded.display_name,
            short_description = excluded.short_description,
            is_active = excluded.is_active,
            source = excluded.source,
            source_attribution = excluded.source_attribution,
            updated_at = excluded.updated_at;
        """,
        source_attribution=SOURCE_OBSERVED_PROCEDURE_ATTRIBUTION,
    )

    await db.status(
        f"""
        WITH src AS (
            SELECT
                procedure_code,
                UPPER(BTRIM(reported_code)) AS service_code,
                CASE
                    WHEN UPPER(BTRIM(reported_code)) ~ '^[0-9]{{5}}$' THEN 'CPT'
                    WHEN UPPER(BTRIM(reported_code)) ~ '^D[0-9]{{4}}$' THEN 'CDT'
                    ELSE 'HCPCS'
                END AS primary_system
            FROM {schema}.{procedure_table}
            WHERE COALESCE(BTRIM(reported_code), '') <> ''
              AND UPPER(BTRIM(reported_code)) ~ '^[A-Z0-9]{{5}}$'
              AND UPPER(BTRIM(reported_code)) ~ '[0-9]'
        ),
        edges AS (
            SELECT
                src.primary_system AS from_system,
                src.service_code AS from_code,
                '{INTERNAL_CODE_SYSTEM}' AS to_system,
                src.procedure_code::text AS to_code
            FROM src
            UNION ALL
            SELECT
                '{INTERNAL_CODE_SYSTEM}' AS from_system,
                src.procedure_code::text AS from_code,
                src.primary_system AS to_system,
                src.service_code AS to_code
            FROM src
            UNION ALL
            SELECT
                src.primary_system AS from_system,
                src.service_code AS from_code,
                'HCPCS' AS to_system,
                src.service_code AS to_code
            FROM src
            WHERE src.primary_system IN ('CPT', 'CDT')
            UNION ALL
            SELECT
                'HCPCS' AS from_system,
                src.service_code AS from_code,
                src.primary_system AS to_system,
                src.service_code AS to_code
            FROM src
            WHERE src.primary_system IN ('CPT', 'CDT')
            UNION ALL
            SELECT
                'HCPCS' AS from_system,
                src.service_code AS from_code,
                '{INTERNAL_CODE_SYSTEM}' AS to_system,
                src.procedure_code::text AS to_code
            FROM src
            WHERE src.primary_system IN ('CPT', 'CDT')
            UNION ALL
            SELECT
                '{INTERNAL_CODE_SYSTEM}' AS from_system,
                src.procedure_code::text AS from_code,
                'HCPCS' AS to_system,
                src.service_code AS to_code
            FROM src
            WHERE src.primary_system IN ('CPT', 'CDT')
        )
        INSERT INTO {schema}.{code_crosswalk_table}
            (
                from_system,
                from_code,
                to_system,
                to_code,
                match_type,
                confidence,
                source,
                source_attribution,
                updated_at
            )
        SELECT
            edges.from_system,
            edges.from_code,
            edges.to_system,
            edges.to_code,
            'exact',
            1.0,
            'cms_physician_provider_service',
            :source_attribution,
            NOW()
        FROM edges
        ON CONFLICT (from_system, from_code, to_system, to_code) DO UPDATE
        SET
            match_type = excluded.match_type,
            confidence = excluded.confidence,
            source = excluded.source,
            source_attribution = excluded.source_attribution,
            updated_at = excluded.updated_at;
        """,
        source_attribution=SOURCE_OBSERVED_PROCEDURE_ATTRIBUTION,
    )


async def _materialize_cost_level_rows(classes: dict[str, type], schema: str) -> None:
    """Build provider cost profiles and peer statistics from staged claims."""

    provider_table = classes["PricingProvider"].__tablename__
    provider_procedure_table = classes["PricingProviderProcedure"].__tablename__
    provider_cost_profile_table = classes["PricingProviderProcedureCostProfile"].__tablename__
    procedure_peer_table = classes["PricingProcedurePeerStats"].__tablename__

    await db.status(f"TRUNCATE TABLE {schema}.{provider_cost_profile_table};")
    await db.status(f"TRUNCATE TABLE {schema}.{procedure_peer_table};")

    await db.status(
        f"""
        WITH base AS (
            SELECT
                pp.npi,
                pp.year,
                pp.procedure_code,
                GREATEST(COALESCE(pp.total_services, 0)::double precision, 0.0) AS claim_count,
                GREATEST(
                    COALESCE(pp.total_submitted_charges, pp.total_allowed_amount, 0)::double precision,
                    0.0
                ) AS total_submitted_charge,
                CASE
                    WHEN COALESCE(pp.total_services, 0) > 0
                        AND COALESCE(pp.total_submitted_charges, pp.total_allowed_amount, 0) > 0
                        THEN (
                            COALESCE(pp.total_submitted_charges, pp.total_allowed_amount)::double precision
                            / pp.total_services::double precision
                        )
                    ELSE NULL
                END AS avg_submitted_charge,
                COALESCE(NULLIF(LOWER(BTRIM(p.provider_type)), ''), 'unknown') AS specialty_key,
                UPPER(NULLIF(BTRIM(p.state), '')) AS state_code,
                LOWER(NULLIF(BTRIM(p.city), '')) AS city_name,
                NULLIF(BTRIM(p.zip5), '') AS zip5
            FROM {schema}.{provider_procedure_table} pp
            JOIN {schema}.{provider_table} p
              ON p.npi = pp.npi
             AND p.year = pp.year
            WHERE COALESCE(pp.total_services, 0) > 0
        ),
        scoped AS (
            SELECT
                npi,
                year,
                procedure_code,
                claim_count,
                total_submitted_charge,
                avg_submitted_charge,
                specialty_key,
                'all'::varchar AS setting_key,
                'national'::varchar AS geography_scope,
                'US'::varchar AS geography_value
            FROM base
            UNION ALL
            SELECT
                npi,
                year,
                procedure_code,
                claim_count,
                total_submitted_charge,
                avg_submitted_charge,
                specialty_key,
                'all'::varchar AS setting_key,
                'state'::varchar AS geography_scope,
                state_code AS geography_value
            FROM base
            WHERE state_code IS NOT NULL
            UNION ALL
            SELECT
                npi,
                year,
                procedure_code,
                claim_count,
                total_submitted_charge,
                avg_submitted_charge,
                specialty_key,
                'all'::varchar AS setting_key,
                'state_city'::varchar AS geography_scope,
                state_code || '|' || city_name AS geography_value
            FROM base
            WHERE state_code IS NOT NULL AND city_name IS NOT NULL
            UNION ALL
            SELECT
                npi,
                year,
                procedure_code,
                claim_count,
                total_submitted_charge,
                avg_submitted_charge,
                specialty_key,
                'all'::varchar AS setting_key,
                'zip5'::varchar AS geography_scope,
                zip5 AS geography_value
            FROM base
            WHERE zip5 IS NOT NULL
        )
        INSERT INTO {schema}.{provider_cost_profile_table}
            (
                npi,
                year,
                procedure_code,
                geography_scope,
                geography_value,
                specialty_key,
                setting_key,
                claim_count,
                total_submitted_charge,
                avg_submitted_charge,
                updated_at
            )
        SELECT
            scoped.npi,
            scoped.year,
            scoped.procedure_code,
            scoped.geography_scope,
            scoped.geography_value,
            scoped.specialty_key,
            scoped.setting_key,
            SUM(scoped.claim_count) AS claim_count,
            SUM(scoped.total_submitted_charge) AS total_submitted_charge,
            CASE
                WHEN SUM(scoped.claim_count) > 0
                    THEN SUM(scoped.total_submitted_charge) / SUM(scoped.claim_count)
                ELSE NULL
            END AS avg_submitted_charge,
            NOW() AS updated_at
        FROM scoped
        GROUP BY
            scoped.npi,
            scoped.year,
            scoped.procedure_code,
            scoped.geography_scope,
            scoped.geography_value,
            scoped.specialty_key,
            scoped.setting_key;
        """
    )

    # Robust peer cutoffs: log-space IQR trimming prevents outlier providers from skewing percentiles.
    await db.status(
        f"""
        WITH base AS (
            SELECT
                c.npi,
                c.procedure_code,
                c.year,
                c.geography_scope,
                c.geography_value,
                c.specialty_key,
                c.setting_key,
                c.claim_count,
                c.avg_submitted_charge
            FROM {schema}.{provider_cost_profile_table} c
            WHERE
                COALESCE(c.claim_count, 0) >= {COST_LEVEL_MIN_PEER_CLAIMS}
                AND COALESCE(c.avg_submitted_charge, 0) > 0
        ),
        expanded AS (
            SELECT
                b.npi,
                b.procedure_code,
                b.year,
                b.geography_scope,
                b.geography_value,
                b.specialty_key,
                b.setting_key,
                b.claim_count,
                b.avg_submitted_charge
            FROM base b
            UNION ALL
            SELECT
                b.npi,
                b.procedure_code,
                b.year,
                b.geography_scope,
                b.geography_value,
                '__all__'::varchar AS specialty_key,
                b.setting_key,
                b.claim_count,
                b.avg_submitted_charge
            FROM base b
        ),
        quartiles AS (
            SELECT
                e.procedure_code,
                e.year,
                e.geography_scope,
                e.geography_value,
                e.specialty_key,
                e.setting_key,
                percentile_cont(0.25) WITHIN GROUP (ORDER BY LN(e.avg_submitted_charge)) AS q1_ln,
                percentile_cont(0.75) WITHIN GROUP (ORDER BY LN(e.avg_submitted_charge)) AS q3_ln
            FROM expanded e
            GROUP BY
                e.procedure_code,
                e.year,
                e.geography_scope,
                e.geography_value,
                e.specialty_key,
                e.setting_key
        ),
        trimmed AS (
            SELECT
                e.procedure_code,
                e.year,
                e.geography_scope,
                e.geography_value,
                e.specialty_key,
                e.setting_key,
                e.claim_count,
                e.avg_submitted_charge
            FROM expanded e
            JOIN quartiles q
              ON q.procedure_code = e.procedure_code
             AND q.year = e.year
             AND q.geography_scope = e.geography_scope
             AND q.geography_value = e.geography_value
             AND q.specialty_key = e.specialty_key
             AND q.setting_key = e.setting_key
            WHERE
                q.q1_ln IS NULL
                OR q.q3_ln IS NULL
                OR q.q3_ln <= q.q1_ln
                OR LN(e.avg_submitted_charge) BETWEEN
                    (q.q1_ln - {COST_LEVEL_OUTLIER_IQR_FACTOR} * (q.q3_ln - q.q1_ln))
                    AND
                    (q.q3_ln + {COST_LEVEL_OUTLIER_IQR_FACTOR} * (q.q3_ln - q.q1_ln))
        ),
        scored AS (
            SELECT
                t.procedure_code,
                t.year,
                t.geography_scope,
                t.geography_value,
                t.specialty_key,
                t.setting_key,
                COUNT(*) AS provider_count,
                MIN(t.claim_count) AS min_claim_count,
                MAX(t.claim_count) AS max_claim_count,
                percentile_cont(0.10) WITHIN GROUP (ORDER BY t.avg_submitted_charge) AS p10,
                percentile_cont(0.20) WITHIN GROUP (ORDER BY t.avg_submitted_charge) AS p20,
                percentile_cont(0.40) WITHIN GROUP (ORDER BY t.avg_submitted_charge) AS p40,
                percentile_cont(0.50) WITHIN GROUP (ORDER BY t.avg_submitted_charge) AS p50,
                percentile_cont(0.60) WITHIN GROUP (ORDER BY t.avg_submitted_charge) AS p60,
                percentile_cont(0.80) WITHIN GROUP (ORDER BY t.avg_submitted_charge) AS p80,
                percentile_cont(0.90) WITHIN GROUP (ORDER BY t.avg_submitted_charge) AS p90
            FROM trimmed t
            GROUP BY
                t.procedure_code,
                t.year,
                t.geography_scope,
                t.geography_value,
                t.specialty_key,
                t.setting_key
            HAVING COUNT(*) >= {COST_LEVEL_MIN_PEER_PROVIDERS}
        )
        INSERT INTO {schema}.{procedure_peer_table}
            (
                procedure_code,
                year,
                geography_scope,
                geography_value,
                specialty_key,
                setting_key,
                provider_count,
                min_claim_count,
                max_claim_count,
                p10,
                p20,
                p40,
                p50,
                p60,
                p80,
                p90,
                updated_at
            )
        SELECT
            scored.procedure_code,
            scored.year,
            scored.geography_scope,
            scored.geography_value,
            scored.specialty_key,
            scored.setting_key,
            scored.provider_count,
            scored.min_claim_count,
            scored.max_claim_count,
            scored.p10,
            scored.p20,
            scored.p40,
            scored.p50,
            scored.p60,
            scored.p80,
            scored.p90,
            NOW()
        FROM scored
        ON CONFLICT (procedure_code, year, geography_scope, geography_value, specialty_key, setting_key) DO UPDATE
        SET
            provider_count = excluded.provider_count,
            min_claim_count = excluded.min_claim_count,
            max_claim_count = excluded.max_claim_count,
            p10 = excluded.p10,
            p20 = excluded.p20,
            p40 = excluded.p40,
            p50 = excluded.p50,
            p60 = excluded.p60,
            p80 = excluded.p80,
            p90 = excluded.p90,
            updated_at = excluded.updated_at;
        """
    )


async def _collect_cost_level_diagnostics(classes: dict[str, type], schema: str) -> dict[str, Any]:
    """Return publication-gate coverage diagnostics for cost-level tables."""

    provider_cost_profile_table = classes["PricingProviderProcedureCostProfile"].__tablename__
    procedure_peer_table = classes["PricingProcedurePeerStats"].__tablename__

    profile_scope_result = await db.all(
        f"""
        SELECT
            geography_scope,
            COUNT(*)::bigint AS rows,
            COUNT(
                DISTINCT (
                    year,
                    procedure_code,
                    geography_scope,
                    geography_value,
                    specialty_key,
                    setting_key
                )
            )::bigint AS unique_keys
        FROM {schema}.{provider_cost_profile_table}
        GROUP BY geography_scope
        ORDER BY geography_scope;
        """
    )
    peer_scope_result = await db.all(
        f"""
        SELECT
            geography_scope,
            COUNT(*)::bigint AS rows,
            COUNT(
                DISTINCT (
                    year,
                    procedure_code,
                    geography_scope,
                    geography_value,
                    specialty_key,
                    setting_key
                )
            )::bigint AS unique_keys
        FROM {schema}.{procedure_peer_table}
        GROUP BY geography_scope
        ORDER BY geography_scope;
        """
    )
    coverage_result = await db.all(
        f"""
        WITH profile_keys AS (
            SELECT DISTINCT
                year,
                procedure_code,
                geography_scope,
                geography_value,
                setting_key
            FROM {schema}.{provider_cost_profile_table}
        ),
        peer_keys AS (
            SELECT DISTINCT
                year,
                procedure_code,
                geography_scope,
                geography_value,
                setting_key
            FROM {schema}.{procedure_peer_table}
        )
        SELECT
            p.geography_scope,
            COUNT(*)::bigint AS profile_keys,
            COUNT(k.procedure_code)::bigint AS peer_keys,
            CASE
                WHEN COUNT(*) = 0 THEN 0.0
                ELSE ROUND((100.0 * COUNT(k.procedure_code)::numeric / COUNT(*)::numeric), 2)::float8
            END AS coverage_pct
        FROM profile_keys p
        LEFT JOIN peer_keys k
          ON k.year = p.year
         AND k.procedure_code = p.procedure_code
         AND k.geography_scope = p.geography_scope
         AND k.geography_value = p.geography_value
         AND k.setting_key = p.setting_key
        GROUP BY p.geography_scope
        ORDER BY p.geography_scope;
        """
    )

    profile_scope_rows = [
        dict(getattr(query_row, "_mapping", query_row))
        for query_row in profile_scope_result
    ]
    peer_scope_rows = [
        dict(getattr(query_row, "_mapping", query_row))
        for query_row in peer_scope_result
    ]
    coverage_rows = [
        dict(getattr(query_row, "_mapping", query_row))
        for query_row in coverage_result
    ]

    _safe_print(
        "[diagnostic] cost-level profile scope rows: "
        + ", ".join(
            f"{scope_summary.get('geography_scope')}={int(scope_summary.get('rows') or 0)}"
            for scope_summary in profile_scope_rows
        ),
        flush=True,
    )
    _safe_print(
        "[diagnostic] cost-level peer scope rows: "
        + ", ".join(
            f"{scope_summary.get('geography_scope')}={int(scope_summary.get('rows') or 0)}"
            for scope_summary in peer_scope_rows
        ),
        flush=True,
    )
    _safe_print(
        "[diagnostic] cost-level key coverage: "
        + ", ".join(
            f"{scope_summary.get('geography_scope')}={float(scope_summary.get('coverage_pct') or 0.0):.2f}%"
            f" ({int(scope_summary.get('peer_keys') or 0)}/{int(scope_summary.get('profile_keys') or 0)})"
            for scope_summary in coverage_rows
        ),
        flush=True,
    )

    return {
        "profile_scope_rows": profile_scope_rows,
        "peer_scope_rows": peer_scope_rows,
        "key_coverage": coverage_rows,
    }


async def _publish_by_table_rename(classes: dict[str, type], schema: str) -> None:
    final_classes = (
        PricingProvider,
        PricingProcedure,
        PricingProviderProcedure,
        PricingProviderProcedureLocation,
        PricingProviderProcedureCostProfile,
        PricingProcedurePeerStats,
        PricingProcedureGeoBenchmark,
    )

    async with db.transaction():
        for cls in final_classes:
            staged_class = classes[cls.__name__]
            table = cls.__main_table__
            await db.status(f"DROP TABLE IF EXISTS {schema}.{table};")
            await db.status(
                f"ALTER TABLE {schema}.{staged_class.__tablename__} "
                f"RENAME TO {table};"
            )

            await db.status(
                f"ALTER INDEX IF EXISTS {schema}.{staged_class.__tablename__}_idx_primary "
                f"RENAME TO {table}_idx_primary;"
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
                await db.status(
                    f"ALTER INDEX IF EXISTS {schema}.{staged_class.__tablename__}_{base_name} "
                    f"RENAME TO {base_name};"
                )


@dataclass(frozen=True)
class _ClaimsRunIdentity:
    test_mode: bool
    import_id: str
    run_id: str
    stage_suffix: str
    work_dir: Path

    @property
    def downloads_dir(self) -> Path:
        """Return the run-owned directory for downloaded source files."""

        return self.work_dir / "downloads"

    @property
    def chunks_root(self) -> Path:
        """Return the run-owned root for generated chunk files."""

        return self.work_dir / "chunks"


@dataclass(frozen=True)
class _DownloadedClaimsSource:
    chunk_entries: list[dict[str, Any]]


def _claims_run_identity(task_by_field: dict[str, Any]) -> _ClaimsRunIdentity:
    test_mode = bool(task_by_field.get("test_mode", False))
    import_id = _normalize_import_id(task_by_field.get("import_id"))
    run_id = _normalize_run_id(task_by_field.get("run_id"))
    return _ClaimsRunIdentity(
        test_mode=test_mode,
        import_id=import_id,
        run_id=run_id,
        stage_suffix=_build_stage_suffix(import_id, run_id),
        work_dir=_run_dir(import_id, run_id),
    )


async def _timed_value(label: str, awaitable: Any) -> Any:
    step_started_at = _step_start(label)
    try:
        value = await awaitable
    except BaseException as error:
        _step_failed(label, step_started_at, error)
        raise
    _step_end(label, step_started_at)
    return value


async def _resolve_sources_async(
    catalog_by_field: dict[str, Any],
    test_mode: bool,
) -> dict[str, list[dict[str, Any]]]:
    return _resolve_sources(catalog_by_field, test_mode=test_mode)


async def _download_split_claims_source(
    run_identity: _ClaimsRunIdentity,
    dataset_key: str,
    source_by_field: dict[str, Any],
    source_index: int,
    semaphore: asyncio.Semaphore,
) -> _DownloadedClaimsSource:
    reporting_year = max(_safe_int(source_by_field.get("reporting_year"), 2013), 2013)
    step_label = f"download+split {dataset_key} year={reporting_year}"
    async with semaphore:
        step_started_at = _step_start(step_label)
        try:
            local_path = await _download_source_file(
                dataset_key,
                source_by_field,
                str(run_identity.downloads_dir),
                run_identity.test_mode,
                reporting_year=reporting_year,
            )
            chunk_entries = await _split_source_into_chunks(
                dataset_key,
                local_path,
                run_identity.chunks_root
                / dataset_key
                / f"{reporting_year}_{source_index:04d}",
                run_identity.test_mode,
            )
            for chunk_by_field in chunk_entries:
                chunk_by_field["reporting_year"] = reporting_year
                chunk_by_field["source_index"] = source_index
            downloaded_source = _DownloadedClaimsSource(chunk_entries)
        except BaseException as error:
            _step_failed(step_label, step_started_at, error)
            raise
        _step_end(step_label, step_started_at)
        return downloaded_source


async def _enqueue_claim_chunk(
    redis: Any,
    run_identity: _ClaimsRunIdentity,
    schema: str,
    chunk_by_field: dict[str, Any],
) -> None:
    reporting_year = max(_safe_int(chunk_by_field.get("reporting_year"), 2013), 2013)
    source_index = max(_safe_int(chunk_by_field.get("source_index"), 0), 0)
    chunk_index = max(_safe_int(chunk_by_field.get("chunk_index"), 0), 0)
    dataset_key = str(chunk_by_field["dataset_key"])
    chunk_job_by_field = {
        "import_id": run_identity.import_id,
        "run_id": run_identity.run_id,
        "stage_suffix": run_identity.stage_suffix,
        "schema": schema,
        "test_mode": run_identity.test_mode,
        "dataset_key": dataset_key,
        "chunk_id": f"{dataset_key}:{reporting_year}:{source_index}:{chunk_index}",
        "chunk_path": chunk_by_field["chunk_path"],
        "reporting_year": reporting_year,
    }
    await redis.enqueue_job(
        "claims_pricing_process_chunk",
        chunk_job_by_field,
        _queue_name=CLAIMS_QUEUE_NAME,
        _job_id=_chunk_job_id(
            run_identity.run_id,
            dataset_key,
            source_index,
            reporting_year,
            chunk_index,
        ),
    )


async def _stream_claim_chunks(
    redis: Any,
    run_identity: _ClaimsRunIdentity,
    schema: str,
    sources_by_dataset: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    source_count = sum(len(sources_by_dataset.get(dataset.key, [])) for dataset in DATASETS)
    semaphore = asyncio.Semaphore(min(CLAIMS_DOWNLOAD_CONCURRENCY, max(source_count, 1)))
    download_tasks = [
        asyncio.create_task(
            _download_split_claims_source(run_identity, dataset.key, source_by_field, source_index, semaphore)
        )
        for dataset in DATASETS
        for source_index, source_by_field in enumerate(sources_by_dataset.get(dataset.key, []))
    ]
    chunk_entries: list[dict[str, Any]] = []
    try:
        for completed_download in asyncio.as_completed(download_tasks):
            downloaded_source = await completed_download
            for chunk_by_field in downloaded_source.chunk_entries:
                await _enqueue_claim_chunk(redis, run_identity, schema, chunk_by_field)
            if downloaded_source.chunk_entries:
                await _increment_total_chunks(
                    redis,
                    run_identity.run_id,
                    len(downloaded_source.chunk_entries),
                )
                chunk_entries.extend(downloaded_source.chunk_entries)
    finally:
        for download_task in download_tasks:
            if not download_task.done():
                download_task.cancel()
    return chunk_entries


def _claims_manifest_by_field(
    run_identity: _ClaimsRunIdentity,
    schema: str,
    sources_by_dataset: dict[str, list[dict[str, Any]]],
    chunk_entries: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "import_id": run_identity.import_id,
        "run_id": run_identity.run_id,
        "stage_suffix": run_identity.stage_suffix,
        "schema": schema,
        "test_mode": run_identity.test_mode,
        "sources": sources_by_dataset,
        "chunks": chunk_entries,
        "total_chunks": len(chunk_entries),
        "created_at": datetime.datetime.utcnow().isoformat(),
        "work_dir": str(run_identity.work_dir),
    }


async def _enqueue_claims_finalize(
    redis: Any,
    run_identity: _ClaimsRunIdentity,
    schema: str,
    manifest_path: Path,
) -> None:
    finalize_job_by_field = {
        "import_id": run_identity.import_id,
        "run_id": run_identity.run_id,
        "stage_suffix": run_identity.stage_suffix,
        "schema": schema,
        "manifest_path": str(manifest_path),
        "test_mode": run_identity.test_mode,
    }
    await redis.enqueue_job(
        "claims_pricing_finalize",
        finalize_job_by_field,
        _queue_name=CLAIMS_FINISH_QUEUE_NAME,
        _job_id=f"claims_finalize_{run_identity.run_id}",
    )


async def _mark_claim_chunks_queued(
    run_identity: _ClaimsRunIdentity,
    total_chunks: int,
) -> None:
    await mark_control_run(
        run_identity.run_id,
        status="running",
        phase_detail="claims-pricing chunks queued",
        progress_message="chunks queued",
        metrics={"total_chunks": total_chunks, "stage_suffix": run_identity.stage_suffix},
        progress={
            "unit": "chunks",
            "total": total_chunks,
            "done": 0,
            "pct": 0,
            "message": "chunks queued",
            "phase": "claims-pricing chunks queued",
        },
    )


def _find_missing_claim_sources(
    sources_by_dataset: dict[str, list[dict[str, Any]]],
    chunk_entries: list[dict[str, Any]],
) -> list[str]:
    """Identify required source files that produced no processable chunk."""

    observed_source_keys = {
        (
            str(chunk_by_field.get("dataset_key") or ""),
            _safe_int(chunk_by_field.get("source_index"), -1),
        )
        for chunk_by_field in chunk_entries
    }
    missing_sources = []
    for dataset in DATASETS:
        source_descriptors = sources_by_dataset.get(dataset.key) or []
        if not source_descriptors:
            missing_sources.append(f"{dataset.key}:no-source")
            continue
        for source_index, _source_descriptor in enumerate(source_descriptors):
            if (dataset.key, source_index) not in observed_source_keys:
                missing_sources.append(f"{dataset.key}:{source_index}")
    return missing_sources


def _validate_claims_finalize_manifest(
    manifest_by_field: dict[str, Any],
    finalize_spec: _ClaimsFinalizeSpec,
) -> None:
    """Require a complete durable handoff before live-table publication."""

    chunk_entries = manifest_by_field.get("chunks")
    sources_by_dataset = manifest_by_field.get("sources")
    total_chunks = _safe_int(manifest_by_field.get("total_chunks"), -1)
    if not isinstance(chunk_entries, list) or not chunk_entries:
        raise RuntimeError(
            "Claims pricing finalize requires a nonempty chunk manifest."
        )
    if total_chunks != len(chunk_entries):
        raise RuntimeError(
            "Claims pricing finalize chunk count does not match its manifest."
        )
    if not isinstance(sources_by_dataset, dict):
        raise RuntimeError(
            "Claims pricing finalize requires source descriptors."
        )
    missing_sources = _find_missing_claim_sources(
        sources_by_dataset,
        chunk_entries,
    )
    if missing_sources:
        raise RuntimeError(
            "Claims pricing finalize is missing required source chunks: "
            f"{', '.join(missing_sources)}"
        )
    if (
        str(manifest_by_field.get("import_id") or "")
        != finalize_spec.import_id
        or str(manifest_by_field.get("run_id") or "")
        != finalize_spec.run_id
        or str(manifest_by_field.get("stage_suffix") or "")
        != finalize_spec.stage_suffix
    ):
        raise RuntimeError(
            "Claims pricing finalize identity does not match its manifest."
        )


async def _fail_empty_claim_sources(
    run_identity: _ClaimsRunIdentity,
    missing_sources: list[str],
) -> None:
    """Record a fail-closed start result before preserving live tables."""

    await mark_control_run(
        run_identity.run_id,
        status="failed",
        phase_detail="claims-pricing source produced no rows",
        progress_message="source produced no processable rows",
        metrics={"missing_sources": missing_sources},
    )


async def claims_pricing_start(ctx, task: dict[str, Any] | None = None) -> dict[str, Any]:
    """Prepare staging, split sources, and enqueue claims-pricing chunks."""

    run_identity = _claims_run_identity(task or {})
    redis = ctx.get("redis")
    if redis is None:
        raise RuntimeError("ARQ redis context is unavailable for claims pricing start job.")
    total_label = (
        "claims-pricing enqueue+split "
        f"(test_mode={run_identity.test_mode}, import_id={run_identity.import_id}, "
        f"run_id={run_identity.run_id}, stage={run_identity.stage_suffix})"
    )
    total_started_at = _step_start(total_label)
    await mark_control_run(
        run_identity.run_id,
        status="running",
        phase_detail="claims-pricing split running",
        progress_message="splitting source files",
    )
    await ensure_database(run_identity.test_mode)
    _classes_by_name, schema = await _timed_value(
        "prepare staging tables",
        _prepare_tables(run_identity.stage_suffix, run_identity.test_mode),
    )
    catalog_by_field = await _timed_value("fetch CMS catalog", _fetch_catalog())
    sources_by_dataset = await _timed_value(
        "resolve CMS sources",
        _resolve_sources_async(catalog_by_field, run_identity.test_mode),
    )
    run_identity.chunks_root.mkdir(parents=True, exist_ok=True)
    await _init_run_state(redis, run_identity.run_id, 0)
    chunk_entries = await _timed_value(
        "download+split+enqueue chunks (streaming)",
        _stream_claim_chunks(redis, run_identity, schema, sources_by_dataset),
    )
    missing_sources = _find_missing_claim_sources(
        sources_by_dataset,
        chunk_entries,
    )
    if missing_sources:
        await _fail_empty_claim_sources(run_identity, missing_sources)
        raise RuntimeError(
            "Claims pricing import produced no processable rows for required "
            f"sources: {', '.join(missing_sources)}"
        )
    manifest_by_field = _claims_manifest_by_field(run_identity, schema, sources_by_dataset, chunk_entries)
    manifest_path = _manifest_path(run_identity.work_dir)
    _write_manifest(manifest_path, manifest_by_field)
    await _enqueue_claims_finalize(redis, run_identity, schema, manifest_path)
    _step_end(total_label, total_started_at)
    await _mark_claim_chunks_queued(run_identity, len(chunk_entries))
    return {
        "ok": True,
        "queued": True,
        "import_id": run_identity.import_id,
        "run_id": run_identity.run_id,
        "stage_suffix": run_identity.stage_suffix,
        "total_chunks": len(chunk_entries),
        "manifest_path": str(manifest_path),
    }


@dataclass(frozen=True)
class _ClaimsChunkSpec:
    dataset_key: str
    chunk_id: str
    chunk_path: str
    run_id: str
    stage_suffix: str
    schema: str
    reporting_year: int
    test_mode: bool


def _claims_chunk_spec(task_by_field: dict[str, Any]) -> _ClaimsChunkSpec:
    import_id = _normalize_import_id(task_by_field.get("import_id"))
    run_id = str(task_by_field.get("run_id") or "")
    test_mode = bool(task_by_field.get("test_mode", False))
    return _ClaimsChunkSpec(
        dataset_key=str(task_by_field.get("dataset_key") or ""),
        chunk_id=str(task_by_field.get("chunk_id") or ""),
        chunk_path=str(task_by_field.get("chunk_path") or ""),
        run_id=run_id,
        stage_suffix=str(
            task_by_field.get("stage_suffix") or _build_stage_suffix(import_id, run_id)
        ),
        schema=str(
            task_by_field.get("schema")
            or get_import_schema("HLTHPRT_DB_SCHEMA", "mrf", test_mode)
        ),
        reporting_year=max(_safe_int(task_by_field.get("reporting_year"), 2013), 2013),
        test_mode=test_mode,
    )


async def _load_claims_chunk(chunk_spec: _ClaimsChunkSpec) -> None:
    await ensure_database(chunk_spec.test_mode)
    classes_by_name = _staging_classes(chunk_spec.stage_suffix, chunk_spec.schema)
    if chunk_spec.dataset_key == "provider":
        await _load_provider_rows(
            chunk_spec.chunk_path,
            classes_by_name["PricingProvider"],
            chunk_spec.reporting_year,
            test_mode=False,
        )
        return
    if chunk_spec.dataset_key in {"provider_service", "provider_drug"}:
        await _load_provider_service_rows(
            chunk_spec.chunk_path,
            classes_by_name["PricingProviderProcedure"],
            classes_by_name["PricingProviderProcedureLocation"],
            chunk_spec.reporting_year,
            test_mode=False,
        )
        return
    if chunk_spec.dataset_key in {"geo_service", "drug_spending"}:
        await _load_geo_service_rows(
            chunk_spec.chunk_path,
            classes_by_name["PricingProcedure"],
            classes_by_name["PricingProcedureGeoBenchmark"],
            chunk_spec.reporting_year,
            test_mode=False,
        )
        return
    raise RuntimeError(f"Unsupported dataset_key for chunk processing: {chunk_spec.dataset_key}")


async def _record_claims_chunk_complete(redis: Any, chunk_spec: _ClaimsChunkSpec) -> None:
    await _mark_chunk_done_with_retry(redis, chunk_spec.run_id, chunk_spec.chunk_id)
    total_chunks, done_chunks = await _get_run_progress(redis, chunk_spec.run_id, 0)
    enqueue_live_progress(
        run_id=chunk_spec.run_id,
        importer="claims-pricing",
        status="running",
        phase="claims-pricing chunks running",
        unit="chunks",
        done=done_chunks,
        total=total_chunks,
        message=f"processed {done_chunks}/{total_chunks} chunks",
    )


async def claims_pricing_process_chunk(ctx, task: dict[str, Any] | None = None) -> dict[str, Any]:
    """Process one validated claims-pricing chunk and record progress."""

    chunk_spec = _claims_chunk_spec(task or {})
    if not chunk_spec.dataset_key or not chunk_spec.chunk_id or not chunk_spec.chunk_path:
        raise RuntimeError("Chunk payload is missing required fields: dataset_key/chunk_id/chunk_path.")
    if not Path(chunk_spec.chunk_path).exists():
        raise RuntimeError(f"Chunk file does not exist: {chunk_spec.chunk_path}")
    await _load_claims_chunk(chunk_spec)
    redis = ctx.get("redis")
    if redis is not None and chunk_spec.run_id:
        await _record_claims_chunk_complete(redis, chunk_spec)
    return {
        "ok": True,
        "chunk_id": chunk_spec.chunk_id,
        "dataset_key": chunk_spec.dataset_key,
    }


@dataclass(frozen=True)
class _ClaimsFinalizeSpec:
    import_id: str
    run_id: str
    test_mode: bool
    schema: str
    stage_suffix: str
    expected_chunks: int
    finalize_lock_token: str = field(
        default_factory=lambda: secrets.token_hex(16),
        compare=False,
        repr=False,
    )


def _claims_finalize_spec(
    task_by_field: dict[str, Any],
    manifest_by_field: dict[str, Any],
) -> _ClaimsFinalizeSpec:
    import_id = _normalize_import_id(task_by_field.get("import_id"))
    run_id = str(task_by_field.get("run_id") or manifest_by_field.get("run_id") or "")
    test_mode = bool(task_by_field.get("test_mode", False))
    schema = str(
        task_by_field.get("schema")
        or get_import_schema("HLTHPRT_DB_SCHEMA", "mrf", test_mode)
    )
    stage_suffix = str(
        task_by_field.get("stage_suffix")
        or manifest_by_field.get("stage_suffix")
        or _build_stage_suffix(import_id, run_id)
    )
    expected_chunks = _safe_int(
        manifest_by_field.get("total_chunks"),
        _safe_int(task_by_field.get("total_chunks"), 0),
    )
    return _ClaimsFinalizeSpec(
        import_id,
        run_id,
        test_mode,
        schema,
        stage_suffix,
        expected_chunks,
    )


async def _wait_for_claims_finalize_turn(
    redis: Any,
    finalize_spec: _ClaimsFinalizeSpec,
) -> dict[str, Any] | None:
    if redis is None:
        raise RuntimeError(
            "ARQ redis context is unavailable for claims pricing finalize job."
        )
    if not finalize_spec.run_id:
        raise RuntimeError("Claims pricing finalize run_id is required.")
    finalized_key = _state_key(finalize_spec.run_id, "finalized")
    if await redis.get(finalized_key):
        return {
            "ok": True,
            "already_finalized": True,
            "run_id": finalize_spec.run_id,
            "import_id": finalize_spec.import_id,
        }
    total_chunks, done_chunks = await _get_run_progress(
        redis,
        finalize_spec.run_id,
        finalize_spec.expected_chunks,
    )
    if done_chunks < total_chunks:
        enqueue_live_progress(
            run_id=finalize_spec.run_id,
            importer="claims-pricing",
            status="running",
            phase="claims-pricing chunks running",
            unit="chunks",
            done=done_chunks,
            total=total_chunks,
            message=f"waiting for chunks {done_chunks}/{total_chunks}",
        )
        _safe_print(
            f"[finalize] waiting for chunks: done={done_chunks}/{total_chunks} "
            f"run_id={finalize_spec.run_id}",
            flush=True,
        )
        raise Retry(defer=CLAIMS_FINISH_RETRY_SECONDS)
    if not await _claim_finalize_lock(
        redis,
        finalize_spec.run_id,
        finalize_spec.finalize_lock_token,
    ):
        raise Retry(defer=CLAIMS_FINISH_RETRY_SECONDS)
    try:
        await _mark_claims_finalize_started(
            finalize_spec,
            total_chunks,
            done_chunks,
        )
    except BaseException:
        await _release_claims_finalize_lock_safely(redis, finalize_spec)
        raise
    return None


async def _mark_claims_finalize_started(
    finalize_spec: _ClaimsFinalizeSpec,
    total_chunks: int,
    done_chunks: int,
) -> None:
    """Record that one lock-owning worker has begun finalization."""

    await mark_control_run(
        finalize_spec.run_id,
        status="finalizing",
        phase_detail="claims-pricing finalizing",
        progress_message="finalizing",
        progress={
            "unit": "chunks",
            "total": total_chunks,
            "done": done_chunks,
            "pct": 99,
            "message": "finalizing",
            "phase": "claims-pricing finalizing",
        },
    )


async def _materialize_and_publish_claims(
    classes_by_name: dict[str, type],
    schema: str,
) -> dict[str, Any]:
    await _timed_value("ensure live code tables", _ensure_live_code_tables(schema))
    await _timed_value(
        "materialize procedure/code dimensions",
        _materialize_code_and_crosswalk_rows(classes_by_name, schema),
    )
    await _timed_value(
        "materialize cost-level profile and peer stats",
        _materialize_cost_level_rows(classes_by_name, schema),
    )
    cost_level_diagnostics = await _timed_value(
        "verify cost-level coverage diagnostics",
        _collect_cost_level_diagnostics(classes_by_name, schema),
    )
    if CLAIMS_DEFER_STAGE_INDEXES:
        await _timed_value(
            "build staging indexes",
            _build_staging_indexes(classes_by_name, schema),
        )
    await _timed_value(
        "publish staging -> final (transactional rename)",
        _publish_by_table_rename(classes_by_name, schema),
    )
    return cost_level_diagnostics


async def _record_claims_finalized(redis: Any, finalize_spec: _ClaimsFinalizeSpec) -> None:
    if redis is None or not finalize_spec.run_id:
        return
    finalized_key = _state_key(finalize_spec.run_id, "finalized")
    await redis.set(finalized_key, "1", ex=CLAIMS_REDIS_TTL_SECONDS)
    await redis.expire(finalized_key, CLAIMS_REDIS_TTL_SECONDS)


async def _release_claims_finalize_lock_safely(
    redis: Any,
    finalize_spec: _ClaimsFinalizeSpec,
) -> None:
    """Release the owned lock without masking finalize success or failure."""

    try:
        await _is_claims_finalize_lock_released(
            redis,
            finalize_spec.run_id,
            finalize_spec.finalize_lock_token,
        )
    except Exception as exc:
        logger.warning(
            "Failed to release claims-pricing finalize lock for run_id=%s: %s",
            finalize_spec.run_id,
            exc,
        )


def _cleanup_claims_work_dir(
    manifest_by_field: dict[str, Any],
    finalize_spec: _ClaimsFinalizeSpec,
) -> None:
    work_dir_text = str(manifest_by_field.get("work_dir") or "")
    if not work_dir_text or CLAIMS_KEEP_WORKDIR:
        return
    work_dir_root = Path(CLAIMS_WORKDIR).expanduser().resolve()
    expected_work_dir = (
        work_dir_root / finalize_spec.import_id / finalize_spec.run_id
    ).resolve()
    run_work_dir = Path(work_dir_text).expanduser().resolve()
    try:
        expected_work_dir.relative_to(work_dir_root)
    except ValueError:
        logger.warning("Refusing unsafe claims-pricing workspace cleanup")
        return
    if (
        not finalize_spec.run_id
        or work_dir_root == Path(work_dir_root.anchor)
        or expected_work_dir == work_dir_root
        or run_work_dir != expected_work_dir
    ):
        logger.warning("Refusing mismatched claims-pricing workspace cleanup")
        return
    if run_work_dir.exists():
        shutil.rmtree(run_work_dir, ignore_errors=True)


async def _mark_claims_succeeded(finalize_spec: _ClaimsFinalizeSpec) -> None:
    await mark_control_run(
        finalize_spec.run_id,
        status="succeeded",
        phase_detail="claims-pricing finalized",
        progress_message="succeeded",
        metrics={
            "stage_suffix": finalize_spec.stage_suffix,
            "schema": finalize_spec.schema,
        },
    )


async def claims_pricing_finalize(ctx, task: dict[str, Any] | None = None) -> dict[str, Any]:
    """Wait for all chunks, validate staging, and publish claims pricing."""

    task_by_field = task or {}
    redis = ctx.get("redis")
    task_run_id = str(task_by_field.get("run_id") or "")
    if (
        redis is not None
        and task_run_id
        and await redis.get(_state_key(task_run_id, "finalized"))
    ):
        return {
            "ok": True,
            "already_finalized": True,
            "run_id": task_run_id,
            "import_id": _normalize_import_id(
                task_by_field.get("import_id")
            ),
        }
    manifest_path = str(task_by_field.get("manifest_path") or "")
    test_mode = bool(task_by_field.get("test_mode", False))
    await ensure_database(test_mode)
    manifest_by_field = _read_manifest(manifest_path) if manifest_path else {}
    finalize_spec = _claims_finalize_spec(task_by_field, manifest_by_field)
    _validate_claims_finalize_manifest(manifest_by_field, finalize_spec)
    early_response = await _wait_for_claims_finalize_turn(redis, finalize_spec)
    if early_response is not None:
        return early_response
    try:
        classes_by_name = _staging_classes(
            finalize_spec.stage_suffix,
            finalize_spec.schema,
        )
        cost_level_diagnostics = await _materialize_and_publish_claims(
            classes_by_name,
            finalize_spec.schema,
        )
        await _record_claims_finalized(redis, finalize_spec)
        _cleanup_claims_work_dir(manifest_by_field, finalize_spec)
        logger.info(
            "CMS claims pricing import finalized: "
            "test_mode=%s import_id=%s run_id=%s",
            finalize_spec.test_mode,
            finalize_spec.import_id,
            finalize_spec.run_id,
        )
        await _mark_claims_succeeded(finalize_spec)
        return {
            "ok": True,
            "import_id": finalize_spec.import_id,
            "run_id": finalize_spec.run_id,
            "stage_suffix": finalize_spec.stage_suffix,
            "schema": finalize_spec.schema,
            "cost_level_diagnostics": cost_level_diagnostics,
        }
    finally:
        await _release_claims_finalize_lock_safely(redis, finalize_spec)


def _queued_claims_run_response(
    run_id: str,
    stage_suffix: str,
    import_id: str,
    test_mode: bool,
) -> dict[str, Any]:
    return {
        "ok": True,
        "queued": True,
        "run_id": run_id,
        "stage_suffix": stage_suffix,
        "import_id": import_id,
        "test_mode": test_mode,
    }


async def _create_claims_pool() -> Any:
    return await create_pool(
        build_redis_settings(),
        job_serializer=serialize_job,
        job_deserializer=deserialize_job,
    )


async def main(test_mode: bool = False, import_id: str | None = None) -> dict[str, Any]:
    """Queue a new claims-pricing control run and return its identifiers."""

    redis = await _create_claims_pool()
    run_id = _normalize_run_id(None)
    start_job_by_field = {
        "test_mode": bool(test_mode),
        "import_id": import_id,
        "run_id": run_id,
    }
    await redis.enqueue_job(
        "claims_pricing_start",
        start_job_by_field,
        _queue_name=CLAIMS_QUEUE_NAME,
        _job_id=f"claims_start_{run_id}",
    )
    normalized_import_id = _normalize_import_id(import_id)
    stage_suffix = _build_stage_suffix(normalized_import_id, run_id)
    _safe_print(
        f"Queued claims-pricing run: import_id={normalized_import_id} run_id={run_id} stage={stage_suffix} "
        f"test_mode={bool(test_mode)}",
        flush=True,
    )
    return _queued_claims_run_response(
        run_id,
        stage_suffix,
        normalized_import_id,
        bool(test_mode),
    )


async def finish_main(
    import_id: str,
    run_id: str,
    test_mode: bool = False,
    manifest_path: str | None = None,
) -> dict[str, Any]:
    """Queue explicit finalization for an existing claims-pricing run."""

    redis = await _create_claims_pool()
    normalized_import_id = _normalize_import_id(import_id)
    stage_suffix = _build_stage_suffix(normalized_import_id, run_id)
    resolved_manifest_path = manifest_path or str(
        _manifest_path(_run_dir(normalized_import_id, run_id))
    )
    finalize_job_by_field = {
        "import_id": import_id,
        "run_id": run_id,
        "stage_suffix": stage_suffix,
        "test_mode": bool(test_mode),
        "manifest_path": resolved_manifest_path,
    }
    await redis.enqueue_job(
        "claims_pricing_finalize",
        finalize_job_by_field,
        _queue_name=CLAIMS_FINISH_QUEUE_NAME,
        _job_id=f"claims_finalize_{run_id}_{secrets.token_hex(4)}",
    )
    _safe_print(
        f"Queued claims-pricing finalize: import_id={_normalize_import_id(import_id)} run_id={run_id} stage={stage_suffix}",
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
    "claims_pricing_start",
    "claims_pricing_process_chunk",
    "claims_pricing_finalize",
    "finish_main",
    "_find_dataset",
    "_select_csv_distribution",
    "_resolve_sources",
    "_row_allowed_for_test",
]
