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
from dataclasses import dataclass
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

logger = logging.getLogger(__name__)

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
# Default to the latest stable CMS year for faster imports; can be widened via env.
CLAIMS_MIN_YEAR = max(int(os.getenv("HLTHPRT_CLAIMS_MIN_YEAR", "2023")), 2013)
CLAIMS_MAX_YEAR = max(int(os.getenv("HLTHPRT_CLAIMS_MAX_YEAR", "2023")), CLAIMS_MIN_YEAR)
CLAIMS_YEAR_WINDOW = tuple(range(CLAIMS_MIN_YEAR, CLAIMS_MAX_YEAR + 1))


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on", "y"}


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
POSTGRES_IDENTIFIER_MAX_LENGTH = 63


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


def _archived_identifier(name: str, suffix: str = "_old") -> str:
    candidate = f"{name}{suffix}"
    if len(candidate) <= POSTGRES_IDENTIFIER_MAX_LENGTH:
        return candidate
    digest = hashlib.sha1(name.encode("utf-8")).hexdigest()[:8]
    trim_to = max(1, POSTGRES_IDENTIFIER_MAX_LENGTH - len(suffix) - len(digest) - 1)
    return f"{name[:trim_to]}_{digest}{suffix}"


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
        except Exception as exc:  # pylint: disable=broad-exception-caught
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


async def _claim_finalize_lock(redis, run_id: str) -> bool:
    lock_key = _state_key(run_id, "finalize_lock")
    lock_set = await redis.set(lock_key, "1", ex=CLAIMS_REDIS_TTL_SECONDS, nx=True)
    return bool(lock_set)


def _print_row_progress(stage: str, parsed: int, accepted: int, start_time: float, final: bool = False) -> None:
    elapsed = max(time.monotonic() - start_time, 0.001)
    rate = parsed / elapsed
    line = (
        f"\r[rows:{stage}] parsed={parsed:,} accepted={accepted:,} "
        f"rate={rate:,.0f} rows/s"
    )
    print(line, end="\n" if final else "", flush=True)


def _step_start(label: str) -> float:
    start = time.monotonic()
    print(f"[step] START {label}", flush=True)
    return start


def _step_end(label: str, started_at: float) -> None:
    elapsed = max(time.monotonic() - started_at, 0.001)
    print(f"[step] DONE  {label} in {elapsed:.1f}s", flush=True)


async def _run_timed_step(label: str, coro) -> None:
    started_at = _step_start(label)
    try:
        await coro
    finally:
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
    deduped: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in rows:
        deduped[tuple(row.get(field) for field in key_fields)] = row
    return list(deduped.values())


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
        except Exception as exc:  # pylint: disable=broad-exception-caught
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
    selected: dict[int, dict[str, Any]] = {}
    for distribution in _csv_distributions(dataset):
        url = str(distribution.get("downloadURL") or "")
        year = _extract_reporting_year(url)
        if year not in years:
            continue
        previous = selected.get(year)
        if previous is None:
            selected[year] = distribution
            continue
        previous_key = (_parse_modified(previous), str(previous.get("downloadURL") or ""))
        candidate_key = (_parse_modified(distribution), url)
        if candidate_key > previous_key:
            selected[year] = distribution
    return selected


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
    text = str(value).strip()
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
    return "HCPCS"


def _procedure_code_from_service(code_system: str, code: str) -> int:
    return return_checksum([code_system.upper(), code.upper()])


def _provider_key(npi: int, year: int) -> int:
    return return_checksum([npi, year])


def _location_key(
    npi: int,
    year: int,
    procedure_code: int,
    city: str | None,
    state: str | None,
    zip5: str | None,
    key_extra: str | None = None,
) -> int:
    return return_checksum([npi, year, procedure_code, city or "", state or "", zip5 or "", key_extra or ""])


def _row_allowed_for_test(row_number: int) -> bool:
    # Deterministic sparse sampling pattern for large files.
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
        dynamic[cls.__name__] = obj
        await db.status(f"DROP TABLE IF EXISTS {db_schema}.{obj.__tablename__};")
        # Defensive cleanup for rare orphan composite types after interrupted DDL.
        await db.status(f"DROP TYPE IF EXISTS {db_schema}.{obj.__tablename__} CASCADE;")
        await db.create_table(obj.__table__, checkfirst=True)
        if not CLAIMS_DEFER_STAGE_INDEXES:
            await _ensure_indexes(obj, db_schema)

    # Ensure destination tables exist before swap.
    # Do not build indexes on live tables here: legacy live schemas can differ
    # from current models, and pre-swap index DDL can fail. Finalization
    # renames fully-indexed staging tables into live names.
    for cls in (
        PricingProvider,
        PricingProcedure,
        PricingProviderProcedure,
        PricingProviderProcedureLocation,
        PricingProviderProcedureCostProfile,
        PricingProcedurePeerStats,
        PricingProcedureGeoBenchmark,
    ):
        await db.create_table(cls.__table__, checkfirst=True)

    return dynamic, db_schema


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
    # Normalize legacy table definitions created before current model widths.
    await db.status(
        f"""
        ALTER TABLE {schema}.{CodeCatalog.__tablename__}
            ALTER COLUMN code_system TYPE VARCHAR(32),
            ALTER COLUMN code TYPE VARCHAR(128),
            ALTER COLUMN display_name TYPE TEXT,
            ALTER COLUMN short_description TYPE TEXT,
            ALTER COLUMN long_description TYPE TEXT,
            ALTER COLUMN source TYPE VARCHAR(128);
        """
    )
    await db.status(
        f"ALTER TABLE {schema}.{CodeCatalog.__tablename__} ADD COLUMN IF NOT EXISTS code_checksum INTEGER;"
    )
    await db.status(
        f"ALTER TABLE {schema}.{CodeCatalog.__tablename__} DROP COLUMN IF EXISTS effective_year;"
    )
    await db.status(
        f"""
        UPDATE {schema}.{CodeCatalog.__tablename__}
        SET code_checksum = hashtext(UPPER(COALESCE(code_system, '')) || '|' || UPPER(COALESCE(code, '')))
        WHERE code_checksum IS NULL;
        """
    )
    await db.status(
        f"""
        ALTER TABLE {schema}.{CodeCrosswalk.__tablename__}
            ALTER COLUMN from_system TYPE VARCHAR(32),
            ALTER COLUMN from_code TYPE VARCHAR(128),
            ALTER COLUMN to_system TYPE VARCHAR(32),
            ALTER COLUMN to_code TYPE VARCHAR(128),
            ALTER COLUMN match_type TYPE VARCHAR(32),
            ALTER COLUMN source TYPE VARCHAR(128);
        """
    )
    await db.status(
        f"ALTER TABLE {schema}.{CodeCrosswalk.__tablename__} ADD COLUMN IF NOT EXISTS from_checksum INTEGER;"
    )
    await db.status(
        f"ALTER TABLE {schema}.{CodeCrosswalk.__tablename__} ADD COLUMN IF NOT EXISTS to_checksum INTEGER;"
    )
    await db.status(
        f"""
        UPDATE {schema}.{CodeCrosswalk.__tablename__}
        SET
            from_checksum = hashtext(UPPER(COALESCE(from_system, '')) || '|' || UPPER(COALESCE(from_code, ''))),
            to_checksum = hashtext(UPPER(COALESCE(to_system, '')) || '|' || UPPER(COALESCE(to_code, '')))
        WHERE from_checksum IS NULL OR to_checksum IS NULL;
        """
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
        except Exception as exc:  # pylint: disable=broad-exception-caught
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
    resolved: dict[str, list[dict[str, Any]]] = {}
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
        resolved[config.key] = per_dataset_sources
    return resolved


async def _download_csv_head(url: str, path: str, max_bytes: int) -> None:
    # Test mode helper: keep download bounded while still exercising parser and DB pipelines.
    client = await get_http_client(use_proxy=CLAIMS_USE_PROXY)
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


async def _download_source_file(
    dataset_key: str,
    source: dict[str, Any],
    temp_dir: str,
    test_mode: bool,
    reporting_year: int | None = None,
) -> str:
    Path(temp_dir).mkdir(parents=True, exist_ok=True)
    year_suffix = f"_{reporting_year}" if reporting_year else ""
    filename = f"{dataset_key}{year_suffix}.csv"
    path = str(Path(temp_dir) / filename)
    if test_mode:
        print(
            f"[test-mode] partial download for {dataset_key}: up to {TEST_MAX_DOWNLOAD_BYTES:,} bytes",
            flush=True,
        )
    for attempt in range(1, DOWNLOAD_RETRIES + 1):
        try:
            if test_mode:
                await _download_csv_head(source["url"], path, TEST_MAX_DOWNLOAD_BYTES)
            else:
                await download_it_and_save(source["url"], path)
            return path
        except Retry as exc:
            if attempt >= DOWNLOAD_RETRIES:
                raise
            logger.warning(
                "Retrying download (%s/%s) for %s due to %r",
                attempt,
                DOWNLOAD_RETRIES,
                source["url"],
                exc,
            )
            await asyncio.sleep(min(5 * attempt, 20))
        except Exception as exc:  # pylint: disable=broad-exception-caught
            if attempt >= DOWNLOAD_RETRIES:
                raise
            logger.warning(
                "Retrying download (%s/%s) for %s due to %r",
                attempt,
                DOWNLOAD_RETRIES,
                source["url"],
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


async def _split_provider_service_into_chunks(
    source_path: str,
    chunks_dir: Path,
    test_mode: bool,
) -> list[dict[str, Any]]:
    chunks_dir.mkdir(parents=True, exist_ok=True)
    total_size = max(Path(source_path).stat().st_size, 1)
    est_chunks = max(1, int(math.ceil(total_size / max(CLAIMS_CHUNK_TARGET_BYTES, 1))))
    bucket_count = max(1, min(est_chunks, CLAIMS_PROVIDER_DRUG_MAX_BUCKETS))
    row_limit = TEST_PROVIDER_SERVICE_ROW_LIMIT if test_mode else None

    writers: dict[int, Any] = {}
    handles: dict[int, Any] = {}
    rows_per_bucket: dict[int, int] = {idx: 0 for idx in range(bucket_count)}
    parsed_rows = 0
    accepted_rows = 0

    try:
        async with async_open(source_path, "r", encoding="utf-8-sig") as source_handle:
            reader = AsyncDictReader(source_handle)
            async for row in reader:
                parsed_rows += 1
                if test_mode:
                    if not _row_allowed_for_test(parsed_rows):
                        continue
                    if row_limit is not None and accepted_rows >= row_limit:
                        break

                npi = _to_npi(_row_value(row, "Rndrng_NPI", "PRSCRBR_NPI", "Prscrbr_NPI"))
                if npi is None:
                    continue
                bucket = abs(int(npi)) % bucket_count

                if bucket not in writers:
                    chunk_path = chunks_dir / f"chunk_{bucket:05d}.csv"
                    handle = open(chunk_path, "w", encoding="utf-8", newline="")
                    fieldnames = list(row.keys())
                    writer = csv.DictWriter(handle, fieldnames=fieldnames)
                    writer.writeheader()
                    handles[bucket] = handle
                    writers[bucket] = writer

                writers[bucket].writerow(row)
                rows_per_bucket[bucket] += 1
                accepted_rows += 1
    finally:
        for handle in handles.values():
            handle.close()

    chunks: list[dict[str, Any]] = []
    chunk_index = 0
    for bucket in range(bucket_count):
        rows_in_bucket = rows_per_bucket.get(bucket, 0)
        if rows_in_bucket <= 0:
            continue
        chunk_path = chunks_dir / f"chunk_{bucket:05d}.csv"
        chunks.append(
            {
                "dataset_key": "provider_service",
                "chunk_id": f"provider_service:{chunk_index}",
                "chunk_index": chunk_index,
                "chunk_path": str(chunk_path),
                "parsed_rows": parsed_rows,
                "accepted_rows": accepted_rows,
                "rows_in_bucket": rows_in_bucket,
            }
        )
        chunk_index += 1

    if not chunks:
        print(
            f"[warn] no chunks generated for provider_service (parsed={parsed_rows:,}, accepted={accepted_rows:,})",
            flush=True,
        )
    else:
        print(
            f"[split:provider_service] chunks={len(chunks)} parsed={parsed_rows:,} accepted={accepted_rows:,} "
            f"bucketed_by=npi buckets={bucket_count}",
            flush=True,
        )

    return chunks


async def _split_source_into_chunks(
    dataset_key: str,
    source_path: str,
    chunks_dir: Path,
    test_mode: bool,
) -> list[dict[str, Any]]:
    if dataset_key == "provider_service":
        return await _split_provider_service_into_chunks(source_path, chunks_dir, test_mode)

    dataset_config = DATASET_BY_KEY[dataset_key]
    chunks_dir.mkdir(parents=True, exist_ok=True)

    chunks: list[dict[str, Any]] = []
    header_line: bytes | None = None
    parsed_rows = 0
    accepted_rows = 0
    chunk_index = 0
    current_size = 0
    chunk_handle = None
    chunk_path: Path | None = None
    row_limit = dataset_config.row_limit_test if test_mode else None

    def _open_chunk() -> tuple[Any, Path]:
        nonlocal chunk_index, current_size
        chunk_path_local = chunks_dir / f"chunk_{chunk_index:05d}.csv"
        chunk_index += 1
        handle = open(chunk_path_local, "wb")
        handle.write(header_line or b"")
        current_size = len(header_line or b"")
        return handle, chunk_path_local

    try:
        async with async_open(source_path, "rb") as source_handle:
            async for line in source_handle:
                if header_line is None:
                    header_line = line
                    continue
                parsed_rows += 1
                if test_mode:
                    if not _row_allowed_for_test(parsed_rows):
                        continue
                    if row_limit is not None and accepted_rows >= row_limit:
                        break

                if chunk_handle is None:
                    chunk_handle, chunk_path = _open_chunk()

                chunk_handle.write(line)
                current_size += len(line)
                accepted_rows += 1

                if current_size >= CLAIMS_CHUNK_TARGET_BYTES:
                    chunk_handle.close()
                    chunks.append(
                        {
                            "dataset_key": dataset_key,
                            "chunk_id": f"{dataset_key}:{len(chunks)}",
                            "chunk_index": len(chunks),
                            "chunk_path": str(chunk_path),
                            "parsed_rows": parsed_rows,
                            "accepted_rows": accepted_rows,
                        }
                    )
                    chunk_handle = None
                    chunk_path = None
    finally:
        if chunk_handle is not None:
            chunk_handle.close()
            chunks.append(
                {
                    "dataset_key": dataset_key,
                    "chunk_id": f"{dataset_key}:{len(chunks)}",
                    "chunk_index": len(chunks),
                    "chunk_path": str(chunk_path),
                    "parsed_rows": parsed_rows,
                    "accepted_rows": accepted_rows,
                }
            )

    if not chunks:
        print(f"[warn] no chunks generated for {dataset_key} (parsed={parsed_rows:,}, accepted={accepted_rows:,})")
    else:
        print(
            f"[split:{dataset_key}] chunks={len(chunks)} parsed={parsed_rows:,} accepted={accepted_rows:,}",
            flush=True,
        )

    return chunks


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


async def _load_provider_rows(path: str, provider_cls: type, year: int, test_mode: bool) -> None:
    rows: list[dict[str, Any]] = []
    accepted = 0
    skipped_invalid_state = 0
    progress_start = time.monotonic()
    progress_last = progress_start
    async with async_open(path, "r", encoding="utf-8-sig") as handle:
        reader = AsyncDictReader(handle)
        row_number = 0
        async for row in reader:
            row_number += 1
            now = time.monotonic()
            if now - progress_last >= ROW_PROGRESS_INTERVAL_SECONDS:
                _print_row_progress("providers", row_number, accepted, progress_start)
                progress_last = now
            npi = _to_npi(_row_value(row, "Rndrng_NPI", "PRSCRBR_NPI", "Prscrbr_NPI"))
            if npi is None:
                continue
            if test_mode and not _row_allowed_for_test(row_number):
                continue
            raw_state = _row_value(row, "Rndrng_Prvdr_State_Abrvtn", "Prscrbr_State_Abrvtn")
            state = _normalize_state(raw_state)
            if _has_value(raw_state) and state is None:
                skipped_invalid_state += 1
                continue
            raw_zip5 = _row_value(row, "Rndrng_Prvdr_Zip5", "Prscrbr_zip5")
            first_name = _to_str(_row_value(row, "Rndrng_Prvdr_First_Name", "Prscrbr_First_Name"))
            last_org_name = _to_str(_row_value(row, "Rndrng_Prvdr_Last_Org_Name", "Prscrbr_Last_Org_Name"))
            provider_row = {
                "provider_key": _provider_key(npi, year),
                "npi": npi,
                "year": year,
                "provider_name": _provider_name(last_org_name, first_name),
                "first_name": first_name,
                "last_org_name": last_org_name,
                "credentials": _to_str(_row_value(row, "Rndrng_Prvdr_Crdntls", "Prscrbr_Crdntls")),
                "provider_type": _to_str(_row_value(row, "Rndrng_Prvdr_Type", "Prscrbr_Type")),
                "city": _to_str(_row_value(row, "Rndrng_Prvdr_City", "Prscrbr_City")),
                "state": state,
                "zip5": _normalize_zip5(raw_zip5),
                "country": _to_str(_row_value(row, "Rndrng_Prvdr_Cntry", "Prscrbr_Cntry")),
                "total_services": _to_float(_row_value(row, "Tot_Srvcs", "Tot_Clms")),
                "total_distinct_hcpcs_codes": _to_float(_row_value(row, "Tot_HCPCS_Cds", "Tot_30day_Fills")),
                "total_allowed_amount": _to_float(_row_value(row, "Tot_Mdcr_Alowd_Amt", "Tot_Drug_Cst")),
                "total_submitted_charges": _to_float(_row_value(row, "Tot_Sbmtd_Chrg", "Tot_Day_Suply")),
                "total_beneficiaries": _to_float(_row_value(row, "Tot_Benes")),
            }
            rows.append(provider_row)
            accepted += 1
            if len(rows) >= IMPORT_BATCH_SIZE:
                rows = _dedupe_rows(rows, ("provider_key",))
                await _push_objects_with_retry(rows, provider_cls)
                rows.clear()
            if test_mode and accepted >= TEST_PROVIDER_ROW_LIMIT:
                break

    if rows:
        rows = _dedupe_rows(rows, ("provider_key",))
        await _push_objects_with_retry(rows, provider_cls)
    _print_row_progress("providers", row_number, accepted, progress_start, final=True)
    if accepted == 0:
        print(
            "[warn] providers stage accepted 0 rows; verify CSV columns include NPI values.",
            flush=True,
        )
    if skipped_invalid_state:
        print(
            f"[warn] providers stage skipped {skipped_invalid_state:,} rows due to invalid state values.",
            flush=True,
        )


async def _load_provider_service_rows(
    path: str,
    provider_procedure_cls: type,
    location_cls: type,
    year: int,
    test_mode: bool,
) -> None:
    provider_procedure_map: dict[tuple[int, int, int], dict[str, Any]] = {}
    location_rows: list[dict[str, Any]] = []
    seen_location_keys: set[int] = set()
    accepted = 0
    skipped_invalid_state = 0
    progress_start = time.monotonic()
    progress_last = progress_start

    async def _flush_location_batches() -> None:
        nonlocal location_rows
        if not location_rows:
            return
        location_rows = _dedupe_rows(location_rows, ("location_key",))
        location_rows.sort(key=lambda item: item.get("location_key"))
        await _push_objects_with_retry(location_rows, location_cls)
        location_rows.clear()

    async with async_open(path, "r", encoding="utf-8-sig") as handle:
        reader = AsyncDictReader(handle)
        row_number = 0
        async for row in reader:
            row_number += 1
            now = time.monotonic()
            if now - progress_last >= ROW_PROGRESS_INTERVAL_SECONDS:
                _print_row_progress("provider_service", row_number, accepted, progress_start)
                progress_last = now
            npi = _to_npi(_row_value(row, "Rndrng_NPI", "PRSCRBR_NPI", "Prscrbr_NPI"))
            if npi is None:
                continue
            if test_mode and not _row_allowed_for_test(row_number):
                continue

            service_code = _normalize_service_code(_row_value(row, "HCPCS_Cd", "HCPCS_CD"))
            if service_code is None:
                continue
            code_system = _detect_code_system(service_code)
            procedure_code = _procedure_code_from_service(code_system, service_code)
            service_desc = _to_str(_row_value(row, "HCPCS_Desc", "HCPCS_DESC"))
            city = _to_str(_row_value(row, "Rndrng_Prvdr_City", "Prscrbr_City"))
            raw_state = _row_value(row, "Rndrng_Prvdr_State_Abrvtn", "Prscrbr_State_Abrvtn")
            state = _normalize_state(raw_state)
            if _has_value(raw_state) and state is None:
                skipped_invalid_state += 1
                continue
            zip5 = _normalize_zip5(_row_value(row, "Rndrng_Prvdr_Zip5", "Prscrbr_zip5"))
            place_of_service = _to_str(_row_value(row, "Place_Of_Srvc", "PLACE_OF_SRVC"))
            avg_allowed_amount = _to_float(_row_value(row, "Avg_Mdcr_Alowd_Amt"))
            avg_submitted_charge = _to_float(_row_value(row, "Avg_Sbmtd_Chrg"))
            total_services = _to_float(_row_value(row, "Tot_Srvcs", "Tot_Clms"))
            total_allowed_amount = None
            if avg_allowed_amount is not None and total_services is not None:
                total_allowed_amount = avg_allowed_amount * total_services
            elif avg_allowed_amount is not None:
                total_allowed_amount = avg_allowed_amount
            total_submitted_charge = None
            if avg_submitted_charge is not None and total_services is not None:
                total_submitted_charge = avg_submitted_charge * total_services
            elif avg_submitted_charge is not None:
                total_submitted_charge = avg_submitted_charge

            provider_row = {
                "npi": npi,
                "year": year,
                "procedure_code": procedure_code,
                "service_description": service_desc,
                "reported_code": service_code,
                "total_services": total_services,
                "total_beneficiary_day_services": _to_float(_row_value(row, "Tot_Bene_Day_Srvcs", "Tot_30day_Fills")),
                "total_submitted_charges": total_submitted_charge,
                "total_allowed_amount": total_allowed_amount,
                "total_beneficiaries": _to_float(_row_value(row, "Tot_Benes")),
                "ge65_total_services": None,
                "ge65_total_allowed_amount": None,
                "ge65_total_beneficiaries": None,
            }
            proc_key = (npi, year, procedure_code)
            existing = provider_procedure_map.get(proc_key)
            if existing is None:
                provider_procedure_map[proc_key] = provider_row
            else:
                existing["total_services"] = _sum_optional(existing.get("total_services"), provider_row.get("total_services"))
                existing["total_beneficiary_day_services"] = _sum_optional(
                    existing.get("total_beneficiary_day_services"), provider_row.get("total_beneficiary_day_services")
                )
                existing["total_submitted_charges"] = _sum_optional(
                    existing.get("total_submitted_charges"), provider_row.get("total_submitted_charges")
                )
                existing["total_allowed_amount"] = _sum_optional(existing.get("total_allowed_amount"), provider_row.get("total_allowed_amount"))
                existing["total_beneficiaries"] = _sum_optional(existing.get("total_beneficiaries"), provider_row.get("total_beneficiaries"))
                if not existing.get("service_description") and provider_row.get("service_description"):
                    existing["service_description"] = provider_row["service_description"]
                if not existing.get("reported_code") and provider_row.get("reported_code"):
                    existing["reported_code"] = provider_row["reported_code"]

            location_key = _location_key(npi, year, procedure_code, city, state, zip5, key_extra=place_of_service)
            if location_key not in seen_location_keys:
                seen_location_keys.add(location_key)
                location_rows.append(
                    {
                        "location_key": location_key,
                        "npi": npi,
                        "year": year,
                        "procedure_code": procedure_code,
                        "place_of_service": place_of_service,
                        "city": city,
                        "state": state,
                        "zip5": zip5,
                        "state_fips": _normalize_state_fips(
                            _row_value(row, "Rndrng_Prvdr_State_FIPS", "Prscrbr_State_FIPS")
                        ),
                        "country": _to_str(_row_value(row, "Rndrng_Prvdr_Cntry", "Prscrbr_Cntry")) or "US",
                    }
                )
                if len(location_rows) >= IMPORT_BATCH_SIZE:
                    await _flush_location_batches()

            accepted += 1
            if test_mode and accepted >= TEST_PROVIDER_SERVICE_ROW_LIMIT:
                break

    if location_rows:
        await _flush_location_batches()

    if provider_procedure_map:
        provider_rows = list(provider_procedure_map.values())
        provider_rows.sort(key=lambda item: (item.get("npi"), item.get("year"), item.get("procedure_code")))
        for batch in _chunk_rows(provider_rows, IMPORT_BATCH_SIZE):
            await _push_objects_with_retry(batch, provider_procedure_cls)
    _print_row_progress("provider_service", row_number, accepted, progress_start, final=True)
    if skipped_invalid_state:
        print(
            f"[warn] provider_service stage skipped {skipped_invalid_state:,} rows due to invalid state values.",
            flush=True,
        )


def _geo_level_priority(row: dict[str, Any]) -> int:
    geo_level = str(_row_value(row, "Rndrng_Prvdr_Geo_Lvl", "RNDRNG_PRVDR_GEO_LVL") or "").strip().lower()
    if geo_level == "national":
        return 3
    if geo_level == "state":
        return 2
    if geo_level == "county":
        return 1
    return 0


def _geo_scope_value_from_row(row: dict[str, Any]) -> tuple[str, str] | None:
    geo_level = str(_row_value(row, "Rndrng_Prvdr_Geo_Lvl", "RNDRNG_PRVDR_GEO_LVL") or "").strip().lower()
    if geo_level == "national":
        return "national", "US"
    if geo_level == "state":
        state = _normalize_state(
            _row_value(
                row,
                "Rndrng_Prvdr_State_Abrvtn",
                "RNDRNG_PRVDR_STATE_ABRVTN",
                "Prscrbr_State_Abrvtn",
            )
        )
        if state:
            return "state", state
    return None


async def _load_geo_service_rows(
    path: str,
    procedure_cls: type,
    geo_benchmark_cls: type,
    year: int,
    test_mode: bool,
) -> None:
    procedure_rows_map: dict[int, tuple[int, float, dict[str, Any]]] = {}
    geo_benchmark_map: dict[tuple[int, int, str, str], tuple[float, dict[str, Any]]] = {}
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
                _print_row_progress("geo_service", row_number, accepted, progress_start)
                progress_last = now
            if test_mode and not _row_allowed_for_test(row_number):
                continue

            service_code = _normalize_service_code(_row_value(row, "HCPCS_Cd", "HCPCS_CD"))
            if service_code is None:
                continue
            code_system = _detect_code_system(service_code)
            procedure_code = _procedure_code_from_service(code_system, service_code)
            service_desc = _to_str(_row_value(row, "HCPCS_Desc", "HCPCS_DESC"))
            total_services = _to_float(_row_value(row, "Tot_Srvcs", "TOT_SRVCS")) or 0.0
            avg_allowed_amount = _to_float(_row_value(row, "Avg_Mdcr_Alowd_Amt", "AVG_MDCR_ALOWD_AMT"))
            avg_payment_amount = _to_float(_row_value(row, "Avg_Mdcr_Pymt_Amt", "AVG_MDCR_PYMT_AMT"))
            avg_standardized_amount = _to_float(_row_value(row, "Avg_Mdcr_Stdzd_Amt", "AVG_MDCR_STDZD_AMT"))
            total_spending = None
            if avg_allowed_amount is not None:
                total_spending = avg_allowed_amount * total_services

            row_payload = {
                "procedure_code": procedure_code,
                "service_description": service_desc,
                "reported_code": service_code,
                "avg_submitted_charge": _to_float(_row_value(row, "Avg_Sbmtd_Chrg", "AVG_SBMTD_CHRG")),
                "avg_allowed_amount": avg_allowed_amount,
                "avg_payment_amount": avg_payment_amount,
                "avg_standardized_amount": avg_standardized_amount,
                "total_allowed_amount": total_spending,
                "total_services": total_services,
                "total_beneficiaries": _to_float(_row_value(row, "Tot_Benes", "TOT_BENES")),
                "source_year": year,
            }
            rank = (_geo_level_priority(row), total_services)
            previous = procedure_rows_map.get(procedure_code)
            if previous is None or rank > (previous[0], previous[1]):
                procedure_rows_map[procedure_code] = (rank[0], rank[1], row_payload)

            geo_scope_value = _geo_scope_value_from_row(row)
            if geo_scope_value is not None:
                geography_scope, geography_value = geo_scope_value
                geo_key = (procedure_code, year, geography_scope, geography_value)
                geo_payload = {
                    "procedure_code": procedure_code,
                    "year": year,
                    "geography_scope": geography_scope,
                    "geography_value": geography_value,
                    "total_services": total_services,
                    "avg_submitted_charge": avg_allowed_amount,
                    "avg_payment_amount": avg_payment_amount,
                    "avg_standardized_amount": avg_standardized_amount,
                    "updated_at": datetime.datetime.utcnow(),
                }
                geo_previous = geo_benchmark_map.get(geo_key)
                if geo_previous is None or total_services > geo_previous[0]:
                    geo_benchmark_map[geo_key] = (total_services, geo_payload)

            accepted += 1
            if test_mode and accepted >= TEST_GEO_SERVICE_ROW_LIMIT:
                break

    rows = [payload for _priority, _total, payload in procedure_rows_map.values()]
    if rows:
        rows.sort(key=lambda item: item.get("procedure_code"))
        await _push_objects_with_retry(rows, procedure_cls, rewrite=True, use_copy=False)
    geo_rows = [payload for _weight, payload in geo_benchmark_map.values()]
    if geo_rows:
        geo_rows.sort(
            key=lambda item: (
                item.get("year"),
                item.get("procedure_code"),
                item.get("geography_scope"),
                item.get("geography_value"),
            )
        )
        await _push_objects_with_retry(geo_rows, geo_benchmark_cls, rewrite=True, use_copy=False)
    _print_row_progress("geo_service", row_number, accepted, progress_start, final=True)


async def _materialize_code_and_crosswalk_rows(classes: dict[str, type], schema: str) -> None:
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
                CASE WHEN UPPER(BTRIM(reported_code)) ~ '^[0-9]{{5}}$' THEN 'CPT' ELSE 'HCPCS' END AS primary_system
            FROM {schema}.{procedure_table}
            WHERE COALESCE(BTRIM(reported_code), '') <> ''
              AND UPPER(BTRIM(reported_code)) ~ '^[A-Z0-9]{{5}}$'
              AND UPPER(BTRIM(reported_code)) ~ '[0-9]'
        )
        INSERT INTO {schema}.{code_catalog_table}
            (code_system, code, code_checksum, display_name, short_description, long_description, is_active, source, updated_at)
        SELECT
            src.primary_system,
            src.service_code,
            hashtext(UPPER(src.primary_system) || '|' || UPPER(src.service_code)),
            COALESCE(src.service_desc, src.service_code),
            src.service_desc,
            NULL,
            TRUE,
            'cms_physician_provider_service',
            NOW()
        FROM src
        ON CONFLICT (code_system, code) DO UPDATE
        SET
            code_checksum = excluded.code_checksum,
            display_name = excluded.display_name,
            short_description = excluded.short_description,
            is_active = excluded.is_active,
            source = excluded.source,
            updated_at = excluded.updated_at;
        """
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
            (code_system, code, code_checksum, display_name, short_description, long_description, is_active, source, updated_at)
        SELECT
            '{INTERNAL_CODE_SYSTEM}',
            src.procedure_code::text,
            hashtext('{INTERNAL_CODE_SYSTEM}' || '|' || src.procedure_code::text),
            COALESCE(src.service_desc, src.service_code),
            src.service_desc,
            NULL,
            TRUE,
            'cms_physician_provider_service',
            NOW()
        FROM src
        ON CONFLICT (code_system, code) DO UPDATE
        SET
            code_checksum = excluded.code_checksum,
            display_name = excluded.display_name,
            short_description = excluded.short_description,
            is_active = excluded.is_active,
            source = excluded.source,
            updated_at = excluded.updated_at;
        """
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
              AND UPPER(BTRIM(reported_code)) ~ '^[0-9]{{5}}$'
        )
        INSERT INTO {schema}.{code_catalog_table}
            (code_system, code, code_checksum, display_name, short_description, long_description, is_active, source, updated_at)
        SELECT
            'HCPCS',
            src.service_code,
            hashtext('HCPCS' || '|' || src.service_code),
            COALESCE(src.service_desc, src.service_code),
            src.service_desc,
            NULL,
            TRUE,
            'cms_physician_provider_service',
            NOW()
        FROM src
        ON CONFLICT (code_system, code) DO UPDATE
        SET
            code_checksum = excluded.code_checksum,
            display_name = excluded.display_name,
            short_description = excluded.short_description,
            is_active = excluded.is_active,
            source = excluded.source,
            updated_at = excluded.updated_at;
        """
    )

    await db.status(
        f"""
        WITH src AS (
            SELECT
                procedure_code,
                UPPER(BTRIM(reported_code)) AS service_code,
                CASE WHEN UPPER(BTRIM(reported_code)) ~ '^[0-9]{{5}}$' THEN 'CPT' ELSE 'HCPCS' END AS primary_system
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
                'CPT' AS from_system,
                src.service_code AS from_code,
                'HCPCS' AS to_system,
                src.service_code AS to_code
            FROM src
            WHERE src.primary_system = 'CPT'
            UNION ALL
            SELECT
                'HCPCS' AS from_system,
                src.service_code AS from_code,
                'CPT' AS to_system,
                src.service_code AS to_code
            FROM src
            WHERE src.primary_system = 'CPT'
            UNION ALL
            SELECT
                'HCPCS' AS from_system,
                src.service_code AS from_code,
                '{INTERNAL_CODE_SYSTEM}' AS to_system,
                src.procedure_code::text AS to_code
            FROM src
            WHERE src.primary_system = 'CPT'
            UNION ALL
            SELECT
                '{INTERNAL_CODE_SYSTEM}' AS from_system,
                src.procedure_code::text AS from_code,
                'HCPCS' AS to_system,
                src.service_code AS to_code
            FROM src
            WHERE src.primary_system = 'CPT'
        )
        INSERT INTO {schema}.{code_crosswalk_table}
            (
                from_system,
                from_code,
                from_checksum,
                to_system,
                to_code,
                to_checksum,
                match_type,
                confidence,
                source,
                updated_at
            )
        SELECT
            edges.from_system,
            edges.from_code,
            hashtext(UPPER(edges.from_system) || '|' || UPPER(edges.from_code)),
            edges.to_system,
            edges.to_code,
            hashtext(UPPER(edges.to_system) || '|' || UPPER(edges.to_code)),
            'exact',
            1.0,
            'cms_physician_provider_service',
            NOW()
        FROM edges
        ON CONFLICT (from_system, from_code, to_system, to_code) DO UPDATE
        SET
            from_checksum = excluded.from_checksum,
            to_checksum = excluded.to_checksum,
            match_type = excluded.match_type,
            confidence = excluded.confidence,
            source = excluded.source,
            updated_at = excluded.updated_at;
        """
    )


async def _materialize_cost_level_rows(classes: dict[str, type], schema: str) -> None:
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

    profile_scope = [dict(getattr(row, "_mapping", row)) for row in profile_scope_result]
    peer_scope = [dict(getattr(row, "_mapping", row)) for row in peer_scope_result]
    coverage = [dict(getattr(row, "_mapping", row)) for row in coverage_result]

    print(
        "[diagnostic] cost-level profile scope rows: "
        + ", ".join(
            f"{item.get('geography_scope')}={int(item.get('rows') or 0)}"
            for item in profile_scope
        ),
        flush=True,
    )
    print(
        "[diagnostic] cost-level peer scope rows: "
        + ", ".join(
            f"{item.get('geography_scope')}={int(item.get('rows') or 0)}"
            for item in peer_scope
        ),
        flush=True,
    )
    print(
        "[diagnostic] cost-level key coverage: "
        + ", ".join(
            f"{item.get('geography_scope')}={float(item.get('coverage_pct') or 0.0):.2f}%"
            f" ({int(item.get('peer_keys') or 0)}/{int(item.get('profile_keys') or 0)})"
            for item in coverage
        ),
        flush=True,
    )

    return {
        "profile_scope_rows": profile_scope,
        "peer_scope_rows": peer_scope,
        "key_coverage": coverage,
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

    async def archive_index(index_name: str) -> str:
        archived_name = _archived_identifier(index_name)
        await db.status(f"DROP INDEX IF EXISTS {schema}.{archived_name};")
        await db.status(
            f"ALTER INDEX IF EXISTS {schema}.{index_name} RENAME TO {archived_name};"
        )
        return archived_name

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


async def claims_pricing_start(ctx, task: dict[str, Any] | None = None) -> dict[str, Any]:
    task = task or {}
    test_mode = bool(task.get("test_mode", False))
    import_id_val = _normalize_import_id(task.get("import_id"))
    run_id = _normalize_run_id(task.get("run_id"))
    stage_suffix = _build_stage_suffix(import_id_val, run_id)
    redis = ctx.get("redis")
    if redis is None:
        raise RuntimeError("ARQ redis context is unavailable for claims pricing start job.")

    total_start = _step_start(
        "claims-pricing enqueue+split "
        f"(test_mode={test_mode}, import_id={import_id_val}, run_id={run_id}, stage={stage_suffix})"
    )
    await ensure_database(test_mode)

    t = _step_start("prepare staging tables")
    _classes, schema = await _prepare_tables(stage_suffix, test_mode)
    _step_end("prepare staging tables", t)

    t = _step_start("fetch CMS catalog")
    catalog = await _fetch_catalog()
    _step_end("fetch CMS catalog", t)

    t = _step_start("resolve CMS sources")
    sources = _resolve_sources(catalog, test_mode=test_mode)
    _step_end("resolve CMS sources", t)

    work_dir = _run_dir(import_id_val, run_id)
    downloads_dir = work_dir / "downloads"
    chunks_root = work_dir / "chunks"
    chunks_root.mkdir(parents=True, exist_ok=True)
    local_paths: dict[str, list[str]] = {}
    chunks: list[dict[str, Any]] = []

    await _init_run_state(redis, run_id, 0)

    source_count = sum(len(sources.get(dataset.key, [])) for dataset in DATASETS)
    semaphore = asyncio.Semaphore(min(CLAIMS_DOWNLOAD_CONCURRENCY, max(source_count, 1)))

    async def _download_split_source(
        dataset_key: str,
        source: dict[str, Any],
        source_index: int,
    ) -> tuple[str, str, list[dict[str, Any]]]:
        async with semaphore:
            reporting_year = max(_safe_int(source.get("reporting_year"), 2013), 2013)
            step = _step_start(f"download+split {dataset_key} year={reporting_year}")
            try:
                local_path = await _download_source_file(
                    dataset_key,
                    source,
                    str(downloads_dir),
                    test_mode,
                    reporting_year=reporting_year,
                )
                split_chunks = await _split_source_into_chunks(
                    dataset_key=dataset_key,
                    source_path=local_path,
                    chunks_dir=chunks_root / dataset_key,
                    test_mode=test_mode,
                )
                for chunk in split_chunks:
                    chunk["reporting_year"] = reporting_year
                    chunk["source_index"] = source_index
                return dataset_key, local_path, split_chunks
            finally:
                _step_end(f"download+split {dataset_key} year={reporting_year}", step)

    t = _step_start("download+split+enqueue chunks (streaming)")
    dataset_tasks = [
        asyncio.create_task(_download_split_source(dataset.key, source, idx))
        for dataset in DATASETS
        for idx, source in enumerate(sources.get(dataset.key, []))
    ]
    try:
        for completed in asyncio.as_completed(dataset_tasks):
            dataset_key, local_path, split_chunks = await completed
            local_paths.setdefault(dataset_key, []).append(local_path)
            for chunk in split_chunks:
                reporting_year = max(_safe_int(chunk.get("reporting_year"), 2013), 2013)
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
                    "claims_pricing_process_chunk",
                    payload,
                    _queue_name=CLAIMS_QUEUE_NAME,
                    _job_id=_chunk_job_id(
                        run_id,
                        chunk["dataset_key"],
                        source_index,
                        reporting_year,
                        chunk_index,
                    ),
                )
            if split_chunks:
                await _increment_total_chunks(redis, run_id, len(split_chunks))
                chunks.extend(split_chunks)
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
        "chunks": chunks,
        "total_chunks": len(chunks),
        "created_at": datetime.datetime.utcnow().isoformat(),
        "work_dir": str(work_dir),
    }
    manifest_path = _manifest_path(work_dir)
    _write_manifest(manifest_path, manifest)

    await redis.enqueue_job(
        "claims_pricing_finalize",
        {
            "import_id": import_id_val,
            "run_id": run_id,
            "stage_suffix": stage_suffix,
            "schema": schema,
            "manifest_path": str(manifest_path),
            "test_mode": test_mode,
        },
        _queue_name=CLAIMS_FINISH_QUEUE_NAME,
        _job_id=f"claims_finalize_{run_id}",
    )

    _step_end(
        "claims-pricing enqueue+split "
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


async def claims_pricing_process_chunk(ctx, task: dict[str, Any] | None = None) -> dict[str, Any]:
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
    year = max(_safe_int(task.get("reporting_year"), 2013), 2013)

    if dataset_key == "provider":
        await _load_provider_rows(chunk_path, classes["PricingProvider"], year, test_mode=False)
    elif dataset_key in {"provider_service", "provider_drug"}:
        await _load_provider_service_rows(
            chunk_path,
            classes["PricingProviderProcedure"],
            classes["PricingProviderProcedureLocation"],
            year,
            test_mode=False,
        )
    elif dataset_key in {"geo_service", "drug_spending"}:
        await _load_geo_service_rows(
            chunk_path,
            classes["PricingProcedure"],
            classes["PricingProcedureGeoBenchmark"],
            year,
            test_mode=False,
        )
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


async def claims_pricing_finalize(ctx, task: dict[str, Any] | None = None) -> dict[str, Any]:
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
            raise Retry(defer=CLAIMS_FINISH_RETRY_SECONDS)

        if not await _claim_finalize_lock(redis, run_id):
            raise Retry(defer=CLAIMS_FINISH_RETRY_SECONDS)

    classes = _staging_classes(stage_suffix, schema)
    t = _step_start("ensure live code tables")
    await _ensure_live_code_tables(schema)
    _step_end("ensure live code tables", t)
    t = _step_start("materialize procedure/code dimensions")
    await _materialize_code_and_crosswalk_rows(classes, schema)
    _step_end("materialize procedure/code dimensions", t)
    t = _step_start("materialize cost-level profile and peer stats")
    await _materialize_cost_level_rows(classes, schema)
    _step_end("materialize cost-level profile and peer stats", t)
    t = _step_start("verify cost-level coverage diagnostics")
    cost_level_diagnostics = await _collect_cost_level_diagnostics(classes, schema)
    _step_end("verify cost-level coverage diagnostics", t)
    if CLAIMS_DEFER_STAGE_INDEXES:
        t = _step_start("build staging indexes")
        await _build_staging_indexes(classes, schema)
        _step_end("build staging indexes", t)
    t = _step_start("publish staging -> final (transactional rename)")
    await _publish_by_table_rename(classes, schema)
    _step_end("publish staging -> final (transactional rename)", t)

    if redis is not None and run_id:
        await redis.set(_state_key(run_id, "finalized"), "1", ex=CLAIMS_REDIS_TTL_SECONDS)
        await redis.expire(_state_key(run_id, "finalized"), CLAIMS_REDIS_TTL_SECONDS)

    if manifest:
        run_work_dir = Path(manifest.get("work_dir", ""))
        if run_work_dir and run_work_dir.exists() and not CLAIMS_KEEP_WORKDIR:
            shutil.rmtree(run_work_dir, ignore_errors=True)

    logger.info(
        "CMS claims pricing import finalized: test_mode=%s import_id=%s run_id=%s",
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
        "cost_level_diagnostics": cost_level_diagnostics,
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
        "claims_pricing_start",
        payload,
        _queue_name=CLAIMS_QUEUE_NAME,
        _job_id=f"claims_start_{run_id}",
    )
    stage_suffix = _build_stage_suffix(_normalize_import_id(import_id), run_id)
    print(
        f"Queued claims-pricing run: import_id={_normalize_import_id(import_id)} run_id={run_id} stage={stage_suffix} "
        f"test_mode={bool(test_mode)}",
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
        "claims_pricing_finalize",
        payload,
        _queue_name=CLAIMS_FINISH_QUEUE_NAME,
        _job_id=f"claims_finalize_{run_id}",
    )
    print(
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
