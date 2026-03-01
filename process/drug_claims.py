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
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote

import aiohttp
from aiofile import async_open
from aiocsv import AsyncDictReader
from arq import Retry, create_pool

from db.connection import db
from db.models import (CodeCatalog, CodeCrosswalk, PricingPrescription,
                       PricingProviderPrescription)
from process.ext.utils import (download_it_and_save, ensure_database,
                               get_http_client, get_import_schema, make_class,
                               push_objects, return_checksum)
from process.redis_config import build_redis_settings
from process.serialization import deserialize_job, serialize_job

logger = logging.getLogger(__name__)

# CMS CSV rows can exceed Python's default field limit.
_csv_limit = sys.maxsize
while True:
    try:
        csv.field_size_limit(_csv_limit)
        break
    except OverflowError:
        _csv_limit = _csv_limit // 10


CATALOG_URL = "https://data.cms.gov/data.json"

PROVIDER_DRUG_LANDING_PAGE = (
    "https://data.cms.gov/provider-summary-by-type-of-service/medicare-part-d-prescribers/"
    "medicare-part-d-prescribers-by-provider-and-drug"
)
DRUG_SPENDING_LANDING_PAGE = (
    "https://data.cms.gov/summary-statistics-on-use-and-payments/medicare-medicaid-spending-by-drug/"
    "medicare-part-d-spending-by-drug"
)

RX_INTERNAL_CODE_SYSTEM = "HP_RX_CODE"


TEST_PROVIDER_DRUG_ROW_LIMIT = int(
    os.getenv("HLTHPRT_DRUG_CLAIMS_TEST_PROVIDER_DRUG_ROWS", os.getenv("HLTHPRT_CLAIMS_TEST_PROVIDER_DRUG_ROWS", "10000"))
)
TEST_DRUG_SPENDING_ROW_LIMIT = int(
    os.getenv("HLTHPRT_DRUG_CLAIMS_TEST_SPENDING_ROWS", os.getenv("HLTHPRT_CLAIMS_TEST_DRUG_SPENDING_ROWS", "2000"))
)
TEST_MAX_DOWNLOAD_BYTES = int(
    os.getenv("HLTHPRT_DRUG_CLAIMS_TEST_MAX_DOWNLOAD_BYTES", os.getenv("HLTHPRT_CLAIMS_TEST_MAX_DOWNLOAD_BYTES", str(25 * 1024 * 1024)))
)
IMPORT_BATCH_SIZE = max(int(os.getenv("HLTHPRT_DRUG_CLAIMS_IMPORT_BATCH_SIZE", os.getenv("HLTHPRT_CLAIMS_IMPORT_BATCH_SIZE", "100000"))), 100)
DOWNLOAD_RETRIES = max(int(os.getenv("HLTHPRT_DRUG_CLAIMS_DOWNLOAD_RETRIES", os.getenv("HLTHPRT_CLAIMS_DOWNLOAD_RETRIES", "3"))), 1)
ROW_PROGRESS_INTERVAL_SECONDS = max(float(os.getenv("HLTHPRT_DRUG_CLAIMS_ROW_PROGRESS_SECONDS", os.getenv("HLTHPRT_CLAIMS_ROW_PROGRESS_SECONDS", "2"))), 0.5)
DRUG_CLAIMS_USE_PROXY = os.getenv("HLTHPRT_DRUG_CLAIMS_USE_PROXY", os.getenv("HLTHPRT_CLAIMS_USE_PROXY", "false")).strip().lower() in {"1", "true", "yes", "on"}


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on", "y"}


DRUG_CLAIMS_QUEUE_NAME = "arq:DrugClaims"
DRUG_CLAIMS_FINISH_QUEUE_NAME = "arq:DrugClaims_finish"
DRUG_CLAIMS_CHUNK_TARGET_MB = max(int(os.getenv("HLTHPRT_DRUG_CLAIMS_CHUNK_TARGET_MB", os.getenv("HLTHPRT_CLAIMS_CHUNK_TARGET_MB", "128"))), 4)
DRUG_CLAIMS_CHUNK_TARGET_BYTES = DRUG_CLAIMS_CHUNK_TARGET_MB * 1024 * 1024
DRUG_CLAIMS_FINISH_RETRY_SECONDS = max(int(os.getenv("HLTHPRT_DRUG_CLAIMS_FINISH_RETRY_SECONDS", os.getenv("HLTHPRT_CLAIMS_FINISH_RETRY_SECONDS", "15"))), 1)
DRUG_CLAIMS_REDIS_TTL_SECONDS = max(int(os.getenv("HLTHPRT_DRUG_CLAIMS_REDIS_TTL_SECONDS", os.getenv("HLTHPRT_CLAIMS_REDIS_TTL_SECONDS", "172800"))), 3600)
DRUG_CLAIMS_WORKDIR = os.getenv("HLTHPRT_DRUG_CLAIMS_WORKDIR", os.getenv("HLTHPRT_CLAIMS_WORKDIR", "/tmp/healthporta_claims"))
DRUG_CLAIMS_KEEP_WORKDIR = _env_bool("HLTHPRT_DRUG_CLAIMS_KEEP_WORKDIR", default=_env_bool("HLTHPRT_CLAIMS_KEEP_WORKDIR", default=False))
DRUG_CLAIMS_DOWNLOAD_CONCURRENCY = max(int(os.getenv("HLTHPRT_DRUG_CLAIMS_DOWNLOAD_CONCURRENCY", os.getenv("HLTHPRT_CLAIMS_DOWNLOAD_CONCURRENCY", "3"))), 1)
DRUG_CLAIMS_DEFER_STAGE_INDEXES = _env_bool("HLTHPRT_DRUG_CLAIMS_DEFER_STAGE_INDEXES", default=_env_bool("HLTHPRT_CLAIMS_DEFER_STAGE_INDEXES", default=True))
DRUG_CLAIMS_DB_DEADLOCK_RETRIES = max(int(os.getenv("HLTHPRT_DRUG_CLAIMS_DB_DEADLOCK_RETRIES", os.getenv("HLTHPRT_CLAIMS_DB_DEADLOCK_RETRIES", "6"))), 1)
DRUG_CLAIMS_DB_DEADLOCK_BASE_DELAY_SECONDS = max(
    float(os.getenv("HLTHPRT_DRUG_CLAIMS_DB_DEADLOCK_BASE_DELAY_SECONDS", os.getenv("HLTHPRT_CLAIMS_DB_DEADLOCK_BASE_DELAY_SECONDS", "0.25"))),
    0.05,
)
DRUG_CLAIMS_PROVIDER_DRUG_MAX_BUCKETS = max(
    int(os.getenv("HLTHPRT_DRUG_CLAIMS_PROVIDER_DRUG_MAX_BUCKETS", os.getenv("HLTHPRT_CLAIMS_PROVIDER_DRUG_MAX_BUCKETS", "64"))),
    1,
)
DRUG_CLAIMS_MARK_DONE_RETRIES = max(
    int(os.getenv("HLTHPRT_DRUG_CLAIMS_MARK_DONE_RETRIES", os.getenv("HLTHPRT_CLAIMS_MARK_DONE_RETRIES", "8"))),
    1,
)
DRUG_CLAIMS_MARK_DONE_RETRY_BASE_SECONDS = max(
    float(
        os.getenv(
            "HLTHPRT_DRUG_CLAIMS_MARK_DONE_RETRY_BASE_SECONDS",
            os.getenv("HLTHPRT_CLAIMS_MARK_DONE_RETRY_BASE_SECONDS", "0.5"),
        )
    ),
    0.05,
)
DRUG_CLAIMS_MARK_DONE_RETRY_MAX_SECONDS = max(
    float(
        os.getenv(
            "HLTHPRT_DRUG_CLAIMS_MARK_DONE_RETRY_MAX_SECONDS",
            os.getenv("HLTHPRT_CLAIMS_MARK_DONE_RETRY_MAX_SECONDS", "10"),
        )
    ),
    DRUG_CLAIMS_MARK_DONE_RETRY_BASE_SECONDS,
)

DRUG_CLAIMS_MIN_YEAR = max(int(os.getenv("HLTHPRT_DRUG_CLAIMS_MIN_YEAR", "2023")), 2013)
DRUG_CLAIMS_MAX_YEAR = max(int(os.getenv("HLTHPRT_DRUG_CLAIMS_MAX_YEAR", "2023")), DRUG_CLAIMS_MIN_YEAR)
DRUG_CLAIMS_YEAR_WINDOW = tuple(range(DRUG_CLAIMS_MIN_YEAR, DRUG_CLAIMS_MAX_YEAR + 1))
MAX_NPI = 9_999_999_999
POSTGRES_IDENTIFIER_MAX_LENGTH = 63

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_ALNUM_NORMALIZER_RE = re.compile(r"[^A-Z0-9]+")
_DIGITS_ONLY_RE = re.compile(r"[^0-9]+")


def _sanitize_identifier(value: str | None, fallback: str, field_name: str) -> str:
    text = (value or "").strip()
    if not text:
        return fallback
    if _IDENTIFIER_RE.fullmatch(text):
        return text
    logger.warning("Invalid %s=%r; using %r", field_name, value, fallback)
    return fallback


def _archived_identifier(name: str, suffix: str = "_old") -> str:
    candidate = f"{name}{suffix}"
    if len(candidate) <= POSTGRES_IDENTIFIER_MAX_LENGTH:
        return candidate
    digest = hashlib.sha1(name.encode("utf-8")).hexdigest()[:8]
    trim_to = max(1, POSTGRES_IDENTIFIER_MAX_LENGTH - len(suffix) - len(digest) - 1)
    return f"{name[:trim_to]}_{digest}{suffix}"


RX_CROSSWALK_SOURCE = str(os.getenv("HLTHPRT_RX_CROSSWALK_SOURCE", "hybrid")).strip().lower() or "hybrid"
if RX_CROSSWALK_SOURCE not in {"hybrid", "snapshot", "live"}:
    logger.warning("Unsupported HLTHPRT_RX_CROSSWALK_SOURCE=%r; falling back to 'hybrid'", RX_CROSSWALK_SOURCE)
    RX_CROSSWALK_SOURCE = "hybrid"
RX_CROSSWALK_LIVE_FALLBACK = _env_bool("HLTHPRT_RX_CROSSWALK_LIVE_FALLBACK", default=True)
RX_CROSSWALK_CONFIDENCE_MIN = max(
    min(float(os.getenv("HLTHPRT_RX_CROSSWALK_CONFIDENCE_MIN", "0.85")), 1.0),
    0.0,
)
RX_CROSSWALK_BATCH_SIZE = max(int(os.getenv("HLTHPRT_RX_CROSSWALK_BATCH_SIZE", "5000")), 100)
RX_CROSSWALK_MAX_NDC_PER_CODE = max(int(os.getenv("HLTHPRT_RX_CROSSWALK_MAX_NDC_PER_CODE", "5")), 1)
RX_CROSSWALK_MAX_RXNORM_PER_CODE = max(int(os.getenv("HLTHPRT_RX_CROSSWALK_MAX_RXNORM_PER_CODE", "5")), 1)
RX_CROSSWALK_LIVE_MAX_CODES = max(int(os.getenv("HLTHPRT_RX_CROSSWALK_LIVE_MAX_CODES", "400")), 0)
RX_CROSSWALK_LIVE_MAX_PRODUCTS_PER_CODE = max(
    int(os.getenv("HLTHPRT_RX_CROSSWALK_LIVE_MAX_PRODUCTS_PER_CODE", "12")),
    1,
)
RX_CROSSWALK_LIVE_CONCURRENCY = max(int(os.getenv("HLTHPRT_RX_CROSSWALK_LIVE_CONCURRENCY", "8")), 1)
RX_CROSSWALK_LIVE_TIMEOUT_SECONDS = max(float(os.getenv("HLTHPRT_RX_CROSSWALK_LIVE_TIMEOUT_SECONDS", "12")), 1.0)

RX_CROSSWALK_SNAPSHOT_SCHEMA = _sanitize_identifier(
    os.getenv("HLTHPRT_RX_CROSSWALK_SNAPSHOT_SCHEMA", "rx_data"),
    "rx_data",
    "HLTHPRT_RX_CROSSWALK_SNAPSHOT_SCHEMA",
)
RX_CROSSWALK_SNAPSHOT_PRODUCT_TABLE = _sanitize_identifier(
    os.getenv("HLTHPRT_RX_CROSSWALK_SNAPSHOT_PRODUCT_TABLE", "product"),
    "product",
    "HLTHPRT_RX_CROSSWALK_SNAPSHOT_PRODUCT_TABLE",
)
RX_CROSSWALK_SNAPSHOT_PACKAGE_TABLE = _sanitize_identifier(
    os.getenv("HLTHPRT_RX_CROSSWALK_SNAPSHOT_PACKAGE_TABLE", "package"),
    "package",
    "HLTHPRT_RX_CROSSWALK_SNAPSHOT_PACKAGE_TABLE",
)
RX_CROSSWALK_LIVE_BASE_URL = str(
    os.getenv("HLTHPRT_RX_CROSSWALK_LIVE_BASE_URL", os.getenv("HLTHPRT_DRUG_API_BASE_URL", "http://127.0.0.1:8087"))
).strip().rstrip("/")


@dataclass(frozen=True)
class DatasetConfig:
    key: str
    landing_page: str
    row_limit_test: int


DATASETS = (
    DatasetConfig("provider_drug", PROVIDER_DRUG_LANDING_PAGE, TEST_PROVIDER_DRUG_ROW_LIMIT),
    DatasetConfig("drug_spending", DRUG_SPENDING_LANDING_PAGE, TEST_DRUG_SPENDING_ROW_LIMIT),
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
    return f"drug_claims:{run_id}:{suffix}"


def _run_dir(import_id: str, run_id: str) -> Path:
    return Path(DRUG_CLAIMS_WORKDIR) / import_id / run_id


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
            PricingPrescription,
            PricingProviderPrescription,
        )
    }


def _chunk_job_id(run_id: str, dataset_key: str, source_index: int, reporting_year: int, chunk_index: int) -> str:
    return f"drug_claims_chunk_{run_id}_{dataset_key}_{reporting_year}_{source_index}_{chunk_index}"


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
    await redis.expire(total_key, DRUG_CLAIMS_REDIS_TTL_SECONDS)
    await redis.sadd(done_key, "__init__")
    await redis.srem(done_key, "__init__")
    await redis.expire(done_key, DRUG_CLAIMS_REDIS_TTL_SECONDS)


async def _increment_total_chunks(redis, run_id: str, delta: int) -> None:
    if delta <= 0:
        return
    total_key = _state_key(run_id, "total_chunks")
    await redis.incrby(total_key, int(delta))
    await redis.expire(total_key, DRUG_CLAIMS_REDIS_TTL_SECONDS)


async def _mark_chunk_done(redis, run_id: str, chunk_id: str) -> None:
    done_key = _state_key(run_id, "done_chunks")
    await redis.sadd(done_key, chunk_id)
    await redis.expire(done_key, DRUG_CLAIMS_REDIS_TTL_SECONDS)


async def _mark_chunk_done_with_retry(redis, run_id: str, chunk_id: str) -> None:
    last_exc: Exception | None = None
    for attempt in range(1, DRUG_CLAIMS_MARK_DONE_RETRIES + 1):
        try:
            await _mark_chunk_done(redis, run_id, chunk_id)
            return
        except Exception as exc:  # pylint: disable=broad-exception-caught
            last_exc = exc
            if attempt >= DRUG_CLAIMS_MARK_DONE_RETRIES:
                break
            delay = min(
                DRUG_CLAIMS_MARK_DONE_RETRY_BASE_SECONDS * (2 ** (attempt - 1)),
                DRUG_CLAIMS_MARK_DONE_RETRY_MAX_SECONDS,
            )
            logger.warning(
                "Retrying mark_chunk_done for run_id=%s chunk_id=%s after error (%s/%s): %r",
                run_id,
                chunk_id,
                attempt,
                DRUG_CLAIMS_MARK_DONE_RETRIES,
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
    lock_set = await redis.set(lock_key, "1", ex=DRUG_CLAIMS_REDIS_TTL_SECONDS, nx=True)
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
    for attempt in range(1, DRUG_CLAIMS_DB_DEADLOCK_RETRIES + 1):
        try:
            await push_objects(rows, cls, rewrite=rewrite, use_copy=use_copy)
            return
        except Exception as exc:  # pylint: disable=broad-exception-caught
            if not _is_deadlock_error(exc) or attempt >= DRUG_CLAIMS_DB_DEADLOCK_RETRIES:
                raise
            delay = DRUG_CLAIMS_DB_DEADLOCK_BASE_DELAY_SECONDS * (2 ** (attempt - 1))
            logger.warning(
                "Deadlock while inserting into %s (attempt %s/%s), retrying in %.2fs",
                cls.__tablename__,
                attempt,
                DRUG_CLAIMS_DB_DEADLOCK_RETRIES,
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


def _select_csv_distributions_by_year(dataset: dict[str, Any], years: set[int]) -> dict[int, dict[str, Any]]:
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


def _row_value(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row:
            return row.get(key)
    return None


def _provider_name(last_org_name: str | None, first_name: str | None) -> str | None:
    if last_org_name and first_name:
        return f"{last_org_name}, {first_name}"
    return last_org_name or first_name


def _row_allowed_for_test(row_number: int) -> bool:
    return row_number % 11 == 0


def _rx_code_from_names(generic_name: str | None, brand_name: str | None) -> str:
    normalized_generic = str(generic_name or "").strip().upper()
    normalized_brand = str(brand_name or "").strip().upper()
    return str(return_checksum([RX_INTERNAL_CODE_SYSTEM, normalized_generic, normalized_brand]))


def _normalize_rx_name_key(value: Any) -> str:
    text = str(value or "").strip().upper()
    if not text:
        return ""
    return _ALNUM_NORMALIZER_RE.sub("", text)


def _normalize_rxnorm_code(value: Any) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    normalized = _DIGITS_ONLY_RE.sub("", text)
    if not normalized:
        return None
    return normalized


def _normalize_ndc11_code(value: Any) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    normalized = _DIGITS_ONLY_RE.sub("", text)
    if len(normalized) != 11:
        return None
    return normalized


def _row_to_dict(row: Any) -> dict[str, Any]:
    mapping = getattr(row, "_mapping", None)
    if mapping is not None:
        return dict(mapping)
    if isinstance(row, dict):
        return dict(row)
    try:
        return dict(row)
    except Exception:  # pragma: no cover - defensive for driver-specific rows
        return {}


def _drug_api_url(path: str) -> str:
    base = RX_CROSSWALK_LIVE_BASE_URL.rstrip("/")
    if base.endswith("/api/v1/drug"):
        return f"{base}{path}"
    if base.endswith("/api/v1"):
        return f"{base}/drug{path}"
    return f"{base}/api/v1/drug{path}"


def _extract_product_ndcs(payload: Any) -> list[str]:
    if not isinstance(payload, dict):
        return []
    candidates: list[str] = []
    for key in ("generic", "brand"):
        rows = payload.get(key)
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            product_ndc = str(row.get("product_ndc") or "").strip()
            if product_ndc:
                candidates.append(product_ndc)
    # preserve order while deduping
    return list(dict.fromkeys(candidates))


async def _table_exists(schema: str, table: str) -> bool:
    value = await db.scalar(
        "SELECT to_regclass(:qualified_name) IS NOT NULL;",
        qualified_name=f"{schema}.{table}",
    )
    return bool(value)


async def _column_exists(schema: str, table: str, column: str) -> bool:
    value = await db.scalar(
        """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = :schema_name
              AND table_name = :table_name
              AND column_name = :column_name
        );
        """,
        schema_name=schema,
        table_name=table,
        column_name=column,
    )
    return bool(value)


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
        PricingPrescription,
        PricingProviderPrescription,
    ):
        obj = make_class(cls, stage_suffix, schema_override=db_schema)
        dynamic[cls.__name__] = obj
        await db.status(f"DROP TABLE IF EXISTS {db_schema}.{obj.__tablename__};")
        await db.status(f"DROP TYPE IF EXISTS {db_schema}.{obj.__tablename__} CASCADE;")
        await db.create_table(obj.__table__, checkfirst=True)
        if not DRUG_CLAIMS_DEFER_STAGE_INDEXES:
            await _ensure_indexes(obj, db_schema)

    for cls in (
        PricingPrescription,
        PricingProviderPrescription,
    ):
        # Ensure destination tables exist before swap.
        # Indexes are built on staging tables and moved during finalization.
        # Avoid pre-swap index DDL on live legacy tables.
        await db.create_table(cls.__table__, checkfirst=True)

    return dynamic, db_schema


async def _build_staging_indexes(classes: dict[str, type], schema: str) -> None:
    for cls_name in (
        "PricingPrescription",
        "PricingProviderPrescription",
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
            client = await get_http_client(use_proxy=DRUG_CLAIMS_USE_PROXY)
            async with client:
                response = await client.get(
                    CATALOG_URL,
                    timeout=aiohttp.ClientTimeout(total=120, connect=60, sock_read=120),
                )
                raw = await response.text()
            return json.loads(raw)
        except json.JSONDecodeError:
            last_error = RuntimeError("Invalid CMS catalog payload")
        except Exception as exc:  # pylint: disable=broad-exception-caught
            last_error = exc
            logger.warning("Retrying catalog fetch (%s/%s): %r", attempt, DOWNLOAD_RETRIES, exc)
        await asyncio.sleep(min(3 * attempt, 10))

    if last_error is not None:
        raise last_error
    raise RuntimeError("Failed to fetch CMS catalog")


def _resolve_sources(catalog: dict[str, Any], test_mode: bool = False) -> dict[str, list[dict[str, Any]]]:
    resolved: dict[str, list[dict[str, Any]]] = {}
    requested_years = set(DRUG_CLAIMS_YEAR_WINDOW)
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
    client = await get_http_client(use_proxy=DRUG_CLAIMS_USE_PROXY)
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


async def _split_provider_drug_into_chunks(
    source_path: str,
    chunks_dir: Path,
    test_mode: bool,
) -> list[dict[str, Any]]:
    chunks_dir.mkdir(parents=True, exist_ok=True)
    total_size = max(Path(source_path).stat().st_size, 1)
    est_chunks = max(1, int(math.ceil(total_size / max(DRUG_CLAIMS_CHUNK_TARGET_BYTES, 1))))
    bucket_count = max(1, min(est_chunks, DRUG_CLAIMS_PROVIDER_DRUG_MAX_BUCKETS))
    row_limit = TEST_PROVIDER_DRUG_ROW_LIMIT if test_mode else None

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
                "dataset_key": "provider_drug",
                "chunk_id": f"provider_drug:{chunk_index}",
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
            f"[warn] no chunks generated for provider_drug (parsed={parsed_rows:,}, accepted={accepted_rows:,})",
            flush=True,
        )
    else:
        print(
            f"[split:provider_drug] chunks={len(chunks)} parsed={parsed_rows:,} accepted={accepted_rows:,} "
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
    if dataset_key == "provider_drug":
        return await _split_provider_drug_into_chunks(source_path, chunks_dir, test_mode)

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

                if current_size >= DRUG_CLAIMS_CHUNK_TARGET_BYTES:
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


async def _load_provider_drug_rows(
    path: str,
    provider_prescription_cls: type,
    year: int,
    test_mode: bool,
) -> None:
    provider_rx_map: dict[tuple[int, int, str, str], dict[str, Any]] = {}
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
                _print_row_progress("provider_drug", row_number, accepted, progress_start)
                progress_last = now

            npi = _to_npi(_row_value(row, "Rndrng_NPI", "PRSCRBR_NPI", "Prscrbr_NPI"))
            if npi is None:
                continue
            if test_mode and not _row_allowed_for_test(row_number):
                continue

            generic_name = _to_str(_row_value(row, "Gnrc_Name", "GNRC_NAME", "generic_name"))
            brand_name = _to_str(_row_value(row, "Brnd_Name", "BRND_NAME", "brand_name"))
            rx_name = generic_name or brand_name
            if not rx_name:
                continue

            rx_code = _rx_code_from_names(generic_name, brand_name)
            key = (npi, year, RX_INTERNAL_CODE_SYSTEM, rx_code)

            row_payload = {
                "npi": npi,
                "year": year,
                "rx_code_system": RX_INTERNAL_CODE_SYSTEM,
                "rx_code": rx_code,
                "rx_name": rx_name,
                "generic_name": generic_name,
                "brand_name": brand_name,
                "provider_name": _provider_name(
                    _to_str(_row_value(row, "Rndrng_Prvdr_Last_Org_Name", "Prscrbr_Last_Org_Name")),
                    _to_str(_row_value(row, "Rndrng_Prvdr_First_Name", "Prscrbr_First_Name")),
                ),
                "provider_type": _to_str(_row_value(row, "Rndrng_Prvdr_Type", "Prscrbr_Type")),
                "city": _to_str(_row_value(row, "Rndrng_Prvdr_City", "Prscrbr_City")),
                "state": _to_str(_row_value(row, "Rndrng_Prvdr_State_Abrvtn", "Prscrbr_State_Abrvtn")),
                "zip5": _to_str(_row_value(row, "Rndrng_Prvdr_Zip5", "Prscrbr_Zip5", "Prscrbr_zip5")),
                "country": _to_str(_row_value(row, "Rndrng_Prvdr_Cntry", "Prscrbr_Cntry")) or "US",
                "total_claims": _to_float(_row_value(row, "Tot_Clms", "TOT_CLAIMS")),
                "total_30day_fills": _to_float(_row_value(row, "Tot_30day_Fills", "TOT_30DAY_FILLS")),
                "total_day_supply": _to_float(_row_value(row, "Tot_Day_Suply", "TOT_DAY_SUPLY", "Tot_Day_Supply")),
                "total_drug_cost": _to_float(_row_value(row, "Tot_Drug_Cst", "TOT_DRUG_CST", "Tot_Drug_Cost")),
                "total_benes": _to_float(_row_value(row, "Tot_Benes", "TOT_BENES")),
                "ge65_total_claims": _to_float(_row_value(row, "GE65_Tot_Clms", "GE65_TOT_CLAIMS")),
                "ge65_total_30day_fills": _to_float(_row_value(row, "GE65_Tot_30day_Fills", "GE65_TOT_30DAY_FILLS")),
                "ge65_total_day_supply": _to_float(_row_value(row, "GE65_Tot_Day_Suply", "GE65_TOT_DAY_SUPLY")),
                "ge65_total_drug_cost": _to_float(_row_value(row, "GE65_Tot_Drug_Cst", "GE65_TOT_DRUG_CST")),
                "ge65_total_benes": _to_float(_row_value(row, "GE65_Tot_Benes", "GE65_TOT_BENES")),
            }

            existing = provider_rx_map.get(key)
            if existing is None:
                provider_rx_map[key] = row_payload
            else:
                for metric_key in (
                    "total_claims",
                    "total_30day_fills",
                    "total_day_supply",
                    "total_drug_cost",
                    "total_benes",
                    "ge65_total_claims",
                    "ge65_total_30day_fills",
                    "ge65_total_day_supply",
                    "ge65_total_drug_cost",
                    "ge65_total_benes",
                ):
                    existing[metric_key] = _sum_optional(existing.get(metric_key), row_payload.get(metric_key))
                if not existing.get("rx_name") and row_payload.get("rx_name"):
                    existing["rx_name"] = row_payload["rx_name"]
                if not existing.get("generic_name") and row_payload.get("generic_name"):
                    existing["generic_name"] = row_payload["generic_name"]
                if not existing.get("brand_name") and row_payload.get("brand_name"):
                    existing["brand_name"] = row_payload["brand_name"]

            accepted += 1
            if test_mode and accepted >= TEST_PROVIDER_DRUG_ROW_LIMIT:
                break

    if provider_rx_map:
        rows = list(provider_rx_map.values())
        rows.sort(key=lambda item: (item.get("npi"), item.get("year"), item.get("rx_code_system"), item.get("rx_code")))
        for batch in _chunk_rows(rows, IMPORT_BATCH_SIZE):
            await _push_objects_with_retry(batch, provider_prescription_cls)
    _print_row_progress("provider_drug", row_number, accepted, progress_start, final=True)


async def _load_drug_spending_rows(
    path: str,
    prescription_cls: type,
    year: int,
    test_mode: bool,
) -> None:
    prescription_map: dict[tuple[str, str], dict[str, Any]] = {}
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
                _print_row_progress("drug_spending", row_number, accepted, progress_start)
                progress_last = now
            if test_mode and not _row_allowed_for_test(row_number):
                continue

            generic_name = _to_str(_row_value(row, "Gnrc_Name", "GNRC_NAME", "generic_name"))
            brand_name = _to_str(_row_value(row, "Brnd_Name", "BRND_NAME", "brand_name"))
            rx_name = generic_name or brand_name
            if not rx_name:
                continue

            rx_code = _rx_code_from_names(generic_name, brand_name)
            key = (RX_INTERNAL_CODE_SYSTEM, rx_code)
            row_payload = {
                "rx_code_system": RX_INTERNAL_CODE_SYSTEM,
                "rx_code": rx_code,
                "rx_name": rx_name,
                "generic_name": generic_name,
                "brand_name": brand_name,
                "total_claims": _to_float(_row_value(row, "Tot_Clms", "TOT_CLAIMS", "Tot_Rx")),
                "total_30day_fills": _to_float(_row_value(row, "Tot_30day_Fills", "TOT_30DAY_FILLS")),
                "total_day_supply": _to_float(_row_value(row, "Tot_Day_Suply", "TOT_DAY_SUPLY", "Tot_Day_Supply")),
                "total_drug_cost": _to_float(_row_value(row, "Tot_Drug_Cst", "TOT_DRUG_CST", "Tot_Spndng", "TOT_SPNDG")),
                "total_benes": _to_float(_row_value(row, "Tot_Benes", "TOT_BENES", "Bene_Cnt", "BENE_CNT")),
                "source_year": year,
            }

            existing = prescription_map.get(key)
            if existing is None:
                prescription_map[key] = row_payload
            else:
                for metric_key in ("total_claims", "total_30day_fills", "total_day_supply", "total_drug_cost", "total_benes"):
                    existing[metric_key] = _sum_optional(existing.get(metric_key), row_payload.get(metric_key))
                if not existing.get("rx_name") and row_payload.get("rx_name"):
                    existing["rx_name"] = row_payload["rx_name"]
                if not existing.get("generic_name") and row_payload.get("generic_name"):
                    existing["generic_name"] = row_payload["generic_name"]
                if not existing.get("brand_name") and row_payload.get("brand_name"):
                    existing["brand_name"] = row_payload["brand_name"]
                existing["source_year"] = max(_safe_int(existing.get("source_year"), year), year)

            accepted += 1
            if test_mode and accepted >= TEST_DRUG_SPENDING_ROW_LIMIT:
                break

    if prescription_map:
        rows = list(prescription_map.values())
        rows.sort(key=lambda item: (item.get("rx_code_system"), item.get("rx_code")))
        for batch in _chunk_rows(rows, IMPORT_BATCH_SIZE):
            await _push_objects_with_retry(batch, prescription_cls, rewrite=True, use_copy=False)
    _print_row_progress("drug_spending", row_number, accepted, progress_start, final=True)


async def _materialize_prescription_and_code_rows(classes: dict[str, type], schema: str) -> None:
    prescription_table = classes["PricingPrescription"].__tablename__
    provider_prescription_table = classes["PricingProviderPrescription"].__tablename__
    code_catalog_table = CodeCatalog.__tablename__
    code_crosswalk_table = CodeCrosswalk.__tablename__

    await db.status(
        f"""
        WITH src AS (
            SELECT
                rx_code_system,
                rx_code,
                MAX(rx_name) AS rx_name,
                MAX(generic_name) AS generic_name,
                MAX(brand_name) AS brand_name,
                SUM(total_claims) AS total_claims,
                SUM(total_30day_fills) AS total_30day_fills,
                SUM(total_day_supply) AS total_day_supply,
                SUM(total_drug_cost) AS total_drug_cost,
                SUM(total_benes) AS total_benes,
                MAX(year) AS source_year
            FROM {schema}.{provider_prescription_table}
            GROUP BY rx_code_system, rx_code
        )
        INSERT INTO {schema}.{prescription_table}
            (rx_code_system, rx_code, rx_name, generic_name, brand_name, total_claims, total_30day_fills,
             total_day_supply, total_drug_cost, total_benes, source_year)
        SELECT
            src.rx_code_system,
            src.rx_code,
            src.rx_name,
            src.generic_name,
            src.brand_name,
            src.total_claims,
            src.total_30day_fills,
            src.total_day_supply,
            src.total_drug_cost,
            src.total_benes,
            src.source_year
        FROM src
        ON CONFLICT (rx_code_system, rx_code) DO UPDATE
        SET
            rx_name = COALESCE(excluded.rx_name, {prescription_table}.rx_name),
            generic_name = COALESCE(excluded.generic_name, {prescription_table}.generic_name),
            brand_name = COALESCE(excluded.brand_name, {prescription_table}.brand_name),
            total_claims = COALESCE(excluded.total_claims, {prescription_table}.total_claims),
            total_30day_fills = COALESCE(excluded.total_30day_fills, {prescription_table}.total_30day_fills),
            total_day_supply = COALESCE(excluded.total_day_supply, {prescription_table}.total_day_supply),
            total_drug_cost = COALESCE(excluded.total_drug_cost, {prescription_table}.total_drug_cost),
            total_benes = COALESCE(excluded.total_benes, {prescription_table}.total_benes),
            source_year = GREATEST(COALESCE({prescription_table}.source_year, 0), COALESCE(excluded.source_year, 0));
        """
    )

    await db.status(
        f"""
        INSERT INTO {schema}.{code_catalog_table}
            (code_system, code, code_checksum, display_name, short_description, long_description, is_active, source, updated_at)
        SELECT
            rx_code_system,
            rx_code,
            hashtext(UPPER(rx_code_system) || '|' || UPPER(rx_code)),
            COALESCE(rx_name, generic_name, brand_name, rx_code),
            generic_name,
            NULL,
            TRUE,
            'cms_partd_provider_drug',
            NOW()
        FROM {schema}.{prescription_table}
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
            rx_code_system,
            rx_code,
            hashtext(UPPER(rx_code_system) || '|' || UPPER(rx_code)),
            rx_code_system,
            rx_code,
            hashtext(UPPER(rx_code_system) || '|' || UPPER(rx_code)),
            'exact',
            1.0,
            'cms_partd_provider_drug',
            NOW()
        FROM {schema}.{prescription_table}
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

    await _enrich_external_rx_crosswalk(
        schema=schema,
        prescription_table=prescription_table,
        code_catalog_table=code_catalog_table,
        code_crosswalk_table=code_crosswalk_table,
    )


async def _enrich_external_rx_crosswalk(
    *,
    schema: str,
    prescription_table: str,
    code_catalog_table: str,
    code_crosswalk_table: str,
) -> None:
    source_mode = RX_CROSSWALK_SOURCE
    if source_mode not in {"hybrid", "snapshot", "live"}:
        source_mode = "hybrid"

    summary: dict[str, Any] = {
        "source_mode": source_mode,
        "snapshot": {"mapped_codes": 0, "edges": 0},
        "live": {"attempted": 0, "mapped_codes": 0, "edges": 0},
    }

    should_run_snapshot = source_mode in {"hybrid", "snapshot"}
    should_run_live = source_mode == "live" or (source_mode == "hybrid" and RX_CROSSWALK_LIVE_FALLBACK)

    if should_run_snapshot:
        t = _step_start("enrich rx crosswalk (snapshot)")
        snapshot_result = await _enrich_rx_crosswalk_from_snapshot(
            schema=schema,
            prescription_table=prescription_table,
            code_catalog_table=code_catalog_table,
            code_crosswalk_table=code_crosswalk_table,
        )
        summary["snapshot"] = snapshot_result
        _step_end("enrich rx crosswalk (snapshot)", t)

    if should_run_live:
        t = _step_start("enrich rx crosswalk (live fallback)")
        live_result = await _enrich_rx_crosswalk_from_live(
            schema=schema,
            prescription_table=prescription_table,
            code_catalog_table=code_catalog_table,
            code_crosswalk_table=code_crosswalk_table,
        )
        summary["live"] = live_result
        _step_end("enrich rx crosswalk (live fallback)", t)

    logger.info(
        "RX crosswalk enrichment summary: mode=%s snapshot_mapped=%s snapshot_edges=%s live_attempted=%s live_mapped=%s live_edges=%s",
        summary["source_mode"],
        summary["snapshot"].get("mapped_codes", 0),
        summary["snapshot"].get("edges", 0),
        summary["live"].get("attempted", 0),
        summary["live"].get("mapped_codes", 0),
        summary["live"].get("edges", 0),
    )


async def _enrich_rx_crosswalk_from_snapshot(
    *,
    schema: str,
    prescription_table: str,
    code_catalog_table: str,
    code_crosswalk_table: str,
) -> dict[str, int]:
    snapshot_schema = RX_CROSSWALK_SNAPSHOT_SCHEMA
    product_table = RX_CROSSWALK_SNAPSHOT_PRODUCT_TABLE
    package_table = RX_CROSSWALK_SNAPSHOT_PACKAGE_TABLE

    product_exists = await _table_exists(snapshot_schema, product_table)
    package_exists = await _table_exists(snapshot_schema, package_table)
    if not (product_exists and package_exists):
        logger.warning(
            "Skipping snapshot RX crosswalk enrichment; missing source tables %s.%s or %s.%s",
            snapshot_schema,
            product_table,
            snapshot_schema,
            package_table,
        )
        return {"mapped_codes": 0, "edges": 0}

    product_has_rxnorm_ids = await _column_exists(snapshot_schema, product_table, "rxnorm_ids")
    product_rxnorm_scalar_column = ""
    if not product_has_rxnorm_ids:
        for candidate in ("rxnorm_id", "rxnorm", "rxcui"):
            if await _column_exists(snapshot_schema, product_table, candidate):
                product_rxnorm_scalar_column = candidate
                break

    package_has_product_ndc = await _column_exists(snapshot_schema, package_table, "product_ndc")
    package_has_ndc11 = await _column_exists(snapshot_schema, package_table, "ndc11")
    package_has_package_ndc = await _column_exists(snapshot_schema, package_table, "package_ndc")

    if product_has_rxnorm_ids:
        rxnorm_join_sql = "LEFT JOIN LATERAL unnest(COALESCE(p.rxnorm_ids, ARRAY[]::varchar[])) AS rx(rxnorm_id) ON TRUE"
        rxnorm_source_sql = "COALESCE(BTRIM(rx.rxnorm_id), '')"
    elif product_rxnorm_scalar_column:
        rxnorm_join_sql = ""
        rxnorm_source_sql = f"COALESCE(BTRIM(p.{product_rxnorm_scalar_column}::varchar), '')"
        logger.info(
            "Snapshot RX crosswalk: using scalar %s.%s.%s as RXNORM source",
            snapshot_schema,
            product_table,
            product_rxnorm_scalar_column,
        )
    else:
        rxnorm_join_sql = ""
        rxnorm_source_sql = "''"
        logger.warning(
            "Snapshot RX crosswalk: no rxnorm_ids/rxnorm_id-like column on %s.%s; snapshot RXNORM mapping disabled",
            snapshot_schema,
            product_table,
        )

    ndc_sources: list[str] = ["p.product_ndc"]
    if package_has_product_ndc and (package_has_ndc11 or package_has_package_ndc):
        package_join_sql = f"LEFT JOIN {snapshot_schema}.{package_table} pkg ON pkg.product_ndc = p.product_ndc"
        if package_has_ndc11:
            ndc_sources.insert(0, "pkg.ndc11")
        if package_has_package_ndc:
            ndc_sources.insert(1 if package_has_ndc11 else 0, "pkg.package_ndc")
    else:
        package_join_sql = ""
        if not package_has_product_ndc:
            logger.warning(
                "Snapshot RX crosswalk: %s.%s is missing product_ndc; package NDC fields are skipped",
                snapshot_schema,
                package_table,
            )
    ndc_source_sql = f"COALESCE({', '.join(ndc_sources)}, '')"

    stage_token = secrets.token_hex(6)
    tmp_hp_rx_codes = f"{schema}.tmp_hp_rx_codes_{stage_token}"
    tmp_rx_snapshot_codes = f"{schema}.tmp_rx_snapshot_codes_{stage_token}"
    tmp_rx_crosswalk_candidates = f"{schema}.tmp_rx_crosswalk_candidates_{stage_token}"
    stage_tables = (
        tmp_rx_crosswalk_candidates,
        tmp_rx_snapshot_codes,
        tmp_hp_rx_codes,
    )

    async def _drop_stage_tables() -> None:
        for table_name in stage_tables:
            try:
                await db.status(f"DROP TABLE IF EXISTS {table_name};")
            except Exception as exc:  # pylint: disable=broad-exception-caught
                logger.warning("Failed to drop staging table %s: %r", table_name, exc)

    await _drop_stage_tables()
    try:
        await db.status(
            f"""
            CREATE UNLOGGED TABLE {tmp_hp_rx_codes} AS
            SELECT
                rx_code,
                COALESCE(total_claims, 0)::double precision AS total_claims,
                NULLIF(UPPER(REGEXP_REPLACE(COALESCE(generic_name, ''), '[^A-Za-z0-9]+', '', 'g')), '') AS generic_key,
                NULLIF(UPPER(REGEXP_REPLACE(COALESCE(brand_name, ''), '[^A-Za-z0-9]+', '', 'g')), '') AS brand_key,
                NULLIF(BTRIM(generic_name), '') AS generic_name,
                NULLIF(BTRIM(brand_name), '') AS brand_name,
                NULLIF(BTRIM(rx_name), '') AS rx_name
            FROM {schema}.{prescription_table}
            WHERE UPPER(COALESCE(rx_code_system, '')) = '{RX_INTERNAL_CODE_SYSTEM}'
              AND (COALESCE(BTRIM(generic_name), '') <> '' OR COALESCE(BTRIM(brand_name), '') <> '');
            """
        )
        await db.status(f"CREATE INDEX ON {tmp_hp_rx_codes} (generic_key);")
        await db.status(f"CREATE INDEX ON {tmp_hp_rx_codes} (brand_key);")
        await db.status(f"CREATE INDEX ON {tmp_hp_rx_codes} (rx_code);")

        await db.status(
            f"""
            CREATE UNLOGGED TABLE {tmp_rx_snapshot_codes} AS
            SELECT
                NULLIF(UPPER(REGEXP_REPLACE(COALESCE(p.generic_name, ''), '[^A-Za-z0-9]+', '', 'g')), '') AS generic_key,
                NULLIF(UPPER(REGEXP_REPLACE(COALESCE(p.brand_name, ''), '[^A-Za-z0-9]+', '', 'g')), '') AS brand_key,
                NULLIF(BTRIM(p.generic_name), '') AS generic_name,
                NULLIF(BTRIM(p.brand_name), '') AS brand_name,
                NULLIF(REGEXP_REPLACE({rxnorm_source_sql}, '[^0-9]+', '', 'g'), '') AS rxnorm_code,
                NULLIF(REGEXP_REPLACE({ndc_source_sql}, '[^0-9]+', '', 'g'), '') AS ndc_code
            FROM {snapshot_schema}.{product_table} p
            {rxnorm_join_sql}
            {package_join_sql}
            WHERE COALESCE(BTRIM(p.generic_name), '') <> '' OR COALESCE(BTRIM(p.brand_name), '') <> '';
            """
        )
        await db.status(f"CREATE INDEX ON {tmp_rx_snapshot_codes} (generic_key);")
        await db.status(f"CREATE INDEX ON {tmp_rx_snapshot_codes} (brand_key);")
        await db.status(f"CREATE INDEX ON {tmp_rx_snapshot_codes} (rxnorm_code);")
        await db.status(f"CREATE INDEX ON {tmp_rx_snapshot_codes} (ndc_code);")

        await db.status(
            f"""
            CREATE UNLOGGED TABLE {tmp_rx_crosswalk_candidates} AS
            WITH generic_matches AS (
                SELECT
                    hp.rx_code,
                    hp.generic_name AS hp_generic_name,
                    hp.brand_name AS hp_brand_name,
                    hp.rx_name AS hp_rx_name,
                    snapshot.generic_name AS ext_generic_name,
                    snapshot.brand_name AS ext_brand_name,
                    snapshot.rxnorm_code,
                    snapshot.ndc_code,
                    CASE
                        WHEN hp.generic_key IS NOT NULL
                             AND snapshot.generic_key IS NOT NULL
                             AND hp.generic_key = snapshot.generic_key
                             AND hp.brand_key IS NOT NULL
                             AND snapshot.brand_key IS NOT NULL
                             AND hp.brand_key = snapshot.brand_key
                            THEN 'exact'
                        ELSE 'normalized'
                    END AS match_type,
                    CASE
                        WHEN hp.generic_key IS NOT NULL
                             AND snapshot.generic_key IS NOT NULL
                             AND hp.generic_key = snapshot.generic_key
                             AND hp.brand_key IS NOT NULL
                             AND snapshot.brand_key IS NOT NULL
                             AND hp.brand_key = snapshot.brand_key
                            THEN 1.0
                        ELSE 0.95
                    END AS confidence
                FROM {tmp_hp_rx_codes} hp
                INNER JOIN {tmp_rx_snapshot_codes} snapshot
                    ON hp.generic_key IS NOT NULL
                   AND snapshot.generic_key = hp.generic_key
            ),
            brand_matches AS (
                SELECT
                    hp.rx_code,
                    hp.generic_name AS hp_generic_name,
                    hp.brand_name AS hp_brand_name,
                    hp.rx_name AS hp_rx_name,
                    snapshot.generic_name AS ext_generic_name,
                    snapshot.brand_name AS ext_brand_name,
                    snapshot.rxnorm_code,
                    snapshot.ndc_code,
                    CASE
                        WHEN hp.generic_key IS NOT NULL
                             AND snapshot.generic_key IS NOT NULL
                             AND hp.generic_key = snapshot.generic_key
                             AND hp.brand_key IS NOT NULL
                             AND snapshot.brand_key IS NOT NULL
                             AND hp.brand_key = snapshot.brand_key
                            THEN 'exact'
                        ELSE 'normalized'
                    END AS match_type,
                    CASE
                        WHEN hp.generic_key IS NOT NULL
                             AND snapshot.generic_key IS NOT NULL
                             AND hp.generic_key = snapshot.generic_key
                             AND hp.brand_key IS NOT NULL
                             AND snapshot.brand_key IS NOT NULL
                             AND hp.brand_key = snapshot.brand_key
                            THEN 1.0
                        ELSE 0.90
                    END AS confidence
                FROM {tmp_hp_rx_codes} hp
                INNER JOIN {tmp_rx_snapshot_codes} snapshot
                    ON hp.brand_key IS NOT NULL
                   AND snapshot.brand_key = hp.brand_key
            ),
            combined AS (
                SELECT * FROM generic_matches
                UNION ALL
                SELECT * FROM brand_matches
            ),
            normalized AS (
                SELECT
                    rx_code,
                    'RXNORM'::varchar AS to_system,
                    rxnorm_code AS to_code,
                    match_type,
                    confidence,
                    COALESCE(hp_generic_name, ext_generic_name, hp_brand_name, ext_brand_name, hp_rx_name) AS display_name,
                    COALESCE(hp_generic_name, ext_generic_name) AS generic_name,
                    COALESCE(hp_brand_name, ext_brand_name) AS brand_name
                FROM combined
                WHERE rxnorm_code ~ '^[0-9]+$'
                UNION ALL
                SELECT
                    rx_code,
                    'NDC'::varchar AS to_system,
                    ndc_code AS to_code,
                    match_type,
                    confidence,
                    COALESCE(hp_generic_name, ext_generic_name, hp_brand_name, ext_brand_name, hp_rx_name) AS display_name,
                    COALESCE(hp_generic_name, ext_generic_name) AS generic_name,
                    COALESCE(hp_brand_name, ext_brand_name) AS brand_name
                FROM combined
                WHERE ndc_code ~ '^[0-9]{11}$'
            ),
            dedup AS (
                SELECT
                    rx_code,
                    to_system,
                    to_code,
                    CASE WHEN bool_or(match_type = 'exact') THEN 'exact' ELSE 'normalized' END AS match_type,
                    MAX(confidence) AS confidence,
                    MAX(display_name) AS display_name,
                    MAX(generic_name) AS generic_name,
                    MAX(brand_name) AS brand_name
                FROM normalized
                GROUP BY rx_code, to_system, to_code
            ),
            ranked AS (
                SELECT
                    dedup.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY dedup.rx_code, dedup.to_system
                        ORDER BY
                            dedup.confidence DESC,
                            CASE WHEN dedup.match_type = 'exact' THEN 1 ELSE 0 END DESC,
                            dedup.to_code ASC
                    ) AS rank_in_system
                FROM dedup
                WHERE dedup.confidence >= :confidence_min
            )
            SELECT
                rx_code,
                to_system,
                to_code,
                match_type,
                confidence,
                display_name,
                generic_name,
                brand_name
            FROM ranked
            WHERE (to_system = 'NDC' AND rank_in_system <= :max_ndc_per_code)
               OR (to_system = 'RXNORM' AND rank_in_system <= :max_rxnorm_per_code);
            """,
            confidence_min=RX_CROSSWALK_CONFIDENCE_MIN,
            max_ndc_per_code=RX_CROSSWALK_MAX_NDC_PER_CODE,
            max_rxnorm_per_code=RX_CROSSWALK_MAX_RXNORM_PER_CODE,
        )
        await db.status(f"CREATE INDEX ON {tmp_rx_crosswalk_candidates} (rx_code);")
        await db.status(f"CREATE INDEX ON {tmp_rx_crosswalk_candidates} (to_system, to_code);")

        mapped_codes = _safe_int(
            await db.scalar(f"SELECT COUNT(DISTINCT rx_code) FROM {tmp_rx_crosswalk_candidates};"),
            0,
        )
        if mapped_codes <= 0:
            return {"mapped_codes": 0, "edges": 0}

        await db.status(
            f"""
            WITH dedup_catalog AS (
                SELECT DISTINCT ON (to_system, to_code)
                    to_system,
                    to_code,
                    display_name,
                    generic_name,
                    brand_name,
                    confidence,
                    match_type,
                    rx_code
                FROM {tmp_rx_crosswalk_candidates}
                ORDER BY
                    to_system,
                    to_code,
                    confidence DESC,
                    CASE WHEN match_type = 'exact' THEN 1 ELSE 0 END DESC,
                    rx_code ASC
            )
            INSERT INTO {schema}.{code_catalog_table}
                (code_system, code, code_checksum, display_name, short_description, long_description, is_active, source, updated_at)
            SELECT
                to_system,
                to_code,
                hashtext(UPPER(to_system) || '|' || UPPER(to_code)),
                COALESCE(NULLIF(BTRIM(display_name), ''), to_code),
                NULLIF(BTRIM(COALESCE(generic_name, brand_name)), ''),
                NULL,
                TRUE,
                'drug_api_snapshot',
                NOW()
            FROM dedup_catalog
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

        edge_rows = await db.status(
            f"""
            WITH edges AS (
                SELECT
                    '{RX_INTERNAL_CODE_SYSTEM}'::varchar AS from_system,
                    rx_code AS from_code,
                    to_system,
                    to_code,
                    match_type,
                    confidence
                FROM {tmp_rx_crosswalk_candidates}
                UNION ALL
                SELECT
                    to_system AS from_system,
                    to_code AS from_code,
                    '{RX_INTERNAL_CODE_SYSTEM}'::varchar AS to_system,
                    rx_code AS to_code,
                    match_type,
                    confidence
                FROM {tmp_rx_crosswalk_candidates}
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
                edges.match_type,
                edges.confidence,
                'drug_api_snapshot',
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
        return {"mapped_codes": mapped_codes, "edges": _safe_int(edge_rows, 0)}
    finally:
        await _drop_stage_tables()


async def _collect_unresolved_hp_rx_codes(
    *,
    schema: str,
    prescription_table: str,
    code_crosswalk_table: str,
    limit: int,
) -> list[dict[str, Any]]:
    if limit <= 0:
        return []
    rows = await db.all(
        f"""
        SELECT
            p.rx_code,
            p.rx_name,
            p.generic_name,
            p.brand_name,
            p.total_claims
        FROM {schema}.{prescription_table} p
        WHERE UPPER(COALESCE(p.rx_code_system, '')) = '{RX_INTERNAL_CODE_SYSTEM}'
          AND NOT EXISTS (
              SELECT 1
              FROM {schema}.{code_crosswalk_table} cw
              WHERE UPPER(COALESCE(cw.from_system, '')) = '{RX_INTERNAL_CODE_SYSTEM}'
                AND cw.from_code = p.rx_code
                AND UPPER(COALESCE(cw.to_system, '')) IN ('NDC', 'RXNORM')
          )
        ORDER BY COALESCE(p.total_claims, 0) DESC, p.rx_code ASC
        LIMIT :limit;
        """,
        limit=limit,
    )
    return [_row_to_dict(row) for row in rows or []]


async def _live_get_json(client: aiohttp.ClientSession, url: str, timeout: aiohttp.ClientTimeout) -> Any | None:
    try:
        async with client.get(url, timeout=timeout) as response:
            if response.status == 404:
                return None
            if response.status >= 400:
                logger.warning("Live RX crosswalk call failed status=%s url=%s", response.status, url)
                return None
            return await response.json(content_type=None)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.warning("Live RX crosswalk call failed url=%s error=%r", url, exc)
        return None


async def _resolve_live_external_codes_for_entry(
    client: aiohttp.ClientSession,
    timeout: aiohttp.ClientTimeout,
    entry: dict[str, Any],
) -> dict[str, Any]:
    rx_code = str(entry.get("rx_code") or "").strip()
    if not rx_code:
        return {"rx_code": "", "rxnorm_codes": [], "ndc_codes": [], "display_name": None}

    terms: list[str] = []
    for candidate in (entry.get("generic_name"), entry.get("brand_name"), entry.get("rx_name")):
        text = str(candidate or "").strip()
        if not text:
            continue
        if _normalize_rx_name_key(text) and text not in terms:
            terms.append(text)

    product_ndcs: list[str] = []
    for term in terms:
        payload = await _live_get_json(client, _drug_api_url(f"/name/{quote(term, safe='')}/products"), timeout)
        product_ndcs.extend(_extract_product_ndcs(payload))
        if len(product_ndcs) >= RX_CROSSWALK_LIVE_MAX_PRODUCTS_PER_CODE:
            break
    product_ndcs = list(dict.fromkeys(product_ndcs))[:RX_CROSSWALK_LIVE_MAX_PRODUCTS_PER_CODE]

    rxnorm_codes: set[str] = set()
    ndc_codes: set[str] = set()
    display_name = str(entry.get("generic_name") or entry.get("brand_name") or entry.get("rx_name") or "").strip() or None

    for product_ndc in product_ndcs:
        encoded_ndc = quote(str(product_ndc), safe="")
        product_payload = await _live_get_json(client, _drug_api_url(f"/ndc/{encoded_ndc}"), timeout)
        if isinstance(product_payload, dict):
            display_name = (
                str(
                    product_payload.get("generic_name")
                    or product_payload.get("brand_name")
                    or display_name
                    or ""
                ).strip()
                or display_name
            )
            for raw_rxnorm in product_payload.get("rxnorm_ids") or []:
                normalized_rxnorm = _normalize_rxnorm_code(raw_rxnorm)
                if normalized_rxnorm:
                    rxnorm_codes.add(normalized_rxnorm)
            normalized_product_ndc = _normalize_ndc11_code(product_payload.get("product_ndc"))
            if normalized_product_ndc:
                ndc_codes.add(normalized_product_ndc)

        packages_payload = await _live_get_json(client, _drug_api_url(f"/ndc/{encoded_ndc}/packages"), timeout)
        if isinstance(packages_payload, list):
            for package in packages_payload:
                if not isinstance(package, dict):
                    continue
                normalized_ndc = _normalize_ndc11_code(package.get("ndc11")) or _normalize_ndc11_code(package.get("package_ndc"))
                if normalized_ndc:
                    ndc_codes.add(normalized_ndc)

    return {
        "rx_code": rx_code,
        "display_name": display_name,
        "rxnorm_codes": sorted(rxnorm_codes)[:RX_CROSSWALK_MAX_RXNORM_PER_CODE],
        "ndc_codes": sorted(ndc_codes)[:RX_CROSSWALK_MAX_NDC_PER_CODE],
    }


async def _upsert_external_code_and_edges(
    *,
    schema: str,
    code_catalog_table: str,
    code_crosswalk_table: str,
    hp_code: str,
    to_system: str,
    to_code: str,
    display_name: str | None,
    confidence: float,
    source: str,
) -> int:
    if not hp_code or not to_system or not to_code:
        return 0

    await db.status(
        f"""
        INSERT INTO {schema}.{code_catalog_table}
            (code_system, code, code_checksum, display_name, short_description, long_description, is_active, source, updated_at)
        VALUES (
            CAST(:code_system AS varchar),
            CAST(:code AS varchar),
            hashtext(
                UPPER(CAST(:code_system AS varchar))
                || '|'
                || UPPER(CAST(:code AS varchar))
            ),
            :display_name,
            NULL,
            NULL,
            TRUE,
            :source,
            NOW()
        )
        ON CONFLICT (code_system, code) DO UPDATE
        SET
            code_checksum = excluded.code_checksum,
            display_name = excluded.display_name,
            is_active = excluded.is_active,
            source = excluded.source,
            updated_at = excluded.updated_at;
        """,
        code_system=to_system,
        code=to_code,
        display_name=display_name or to_code,
        source=source,
    )

    rowcount = await db.status(
        f"""
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
        VALUES (
            CAST(:from_system AS varchar),
            CAST(:from_code AS varchar),
            hashtext(
                UPPER(CAST(:from_system AS varchar))
                || '|'
                || UPPER(CAST(:from_code AS varchar))
            ),
            CAST(:to_system AS varchar),
            CAST(:to_code AS varchar),
            hashtext(
                UPPER(CAST(:to_system AS varchar))
                || '|'
                || UPPER(CAST(:to_code AS varchar))
            ),
            :match_type,
            :confidence,
            :source,
            NOW()
        )
        ON CONFLICT (from_system, from_code, to_system, to_code) DO UPDATE
        SET
            from_checksum = excluded.from_checksum,
            to_checksum = excluded.to_checksum,
            match_type = excluded.match_type,
            confidence = excluded.confidence,
            source = excluded.source,
            updated_at = excluded.updated_at;
        """,
        from_system=RX_INTERNAL_CODE_SYSTEM,
        from_code=hp_code,
        to_system=to_system,
        to_code=to_code,
        match_type="normalized",
        confidence=confidence,
        source=source,
    )

    reverse_rowcount = await db.status(
        f"""
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
        VALUES (
            CAST(:from_system AS varchar),
            CAST(:from_code AS varchar),
            hashtext(
                UPPER(CAST(:from_system AS varchar))
                || '|'
                || UPPER(CAST(:from_code AS varchar))
            ),
            CAST(:to_system AS varchar),
            CAST(:to_code AS varchar),
            hashtext(
                UPPER(CAST(:to_system AS varchar))
                || '|'
                || UPPER(CAST(:to_code AS varchar))
            ),
            :match_type,
            :confidence,
            :source,
            NOW()
        )
        ON CONFLICT (from_system, from_code, to_system, to_code) DO UPDATE
        SET
            from_checksum = excluded.from_checksum,
            to_checksum = excluded.to_checksum,
            match_type = excluded.match_type,
            confidence = excluded.confidence,
            source = excluded.source,
            updated_at = excluded.updated_at;
        """,
        from_system=to_system,
        from_code=to_code,
        to_system=RX_INTERNAL_CODE_SYSTEM,
        to_code=hp_code,
        match_type="normalized",
        confidence=confidence,
        source=source,
    )
    return _safe_int(rowcount, 0) + _safe_int(reverse_rowcount, 0)


async def _enrich_rx_crosswalk_from_live(
    *,
    schema: str,
    prescription_table: str,
    code_catalog_table: str,
    code_crosswalk_table: str,
) -> dict[str, int]:
    unresolved_rows = await _collect_unresolved_hp_rx_codes(
        schema=schema,
        prescription_table=prescription_table,
        code_crosswalk_table=code_crosswalk_table,
        limit=RX_CROSSWALK_LIVE_MAX_CODES,
    )
    attempted = len(unresolved_rows)
    if attempted <= 0:
        return {"attempted": 0, "mapped_codes": 0, "edges": 0}

    mapped_codes = 0
    total_edges = 0
    source = "drug_api_live"
    default_confidence = max(RX_CROSSWALK_CONFIDENCE_MIN, 0.88)

    client = await get_http_client(use_proxy=DRUG_CLAIMS_USE_PROXY)
    timeout = aiohttp.ClientTimeout(
        total=RX_CROSSWALK_LIVE_TIMEOUT_SECONDS,
        connect=min(5.0, RX_CROSSWALK_LIVE_TIMEOUT_SECONDS),
        sock_read=RX_CROSSWALK_LIVE_TIMEOUT_SECONDS,
    )

    async with client:
        healthcheck = await _live_get_json(client, _drug_api_url("/"), timeout)
        if not isinstance(healthcheck, dict):
            logger.warning(
                "Skipping live RX crosswalk enrichment; drug-api unavailable at %s",
                _drug_api_url("/"),
            )
            return {"attempted": attempted, "mapped_codes": 0, "edges": 0}

        semaphore = asyncio.Semaphore(RX_CROSSWALK_LIVE_CONCURRENCY)

        async def _resolve_one(entry: dict[str, Any]) -> dict[str, Any]:
            async with semaphore:
                return await _resolve_live_external_codes_for_entry(client, timeout, entry)

        for batch in _chunk_rows(unresolved_rows, RX_CROSSWALK_BATCH_SIZE):
            resolved = await asyncio.gather(*[_resolve_one(entry) for entry in batch], return_exceptions=True)
            for result in resolved:
                if isinstance(result, Exception):
                    logger.warning("Live RX crosswalk resolver task failed: %r", result)
                    continue
                hp_code = str(result.get("rx_code") or "").strip()
                rxnorm_codes = result.get("rxnorm_codes") or []
                ndc_codes = result.get("ndc_codes") or []
                if not hp_code or (not rxnorm_codes and not ndc_codes):
                    continue

                mapped_codes += 1
                display_name = str(result.get("display_name") or "").strip() or None

                for rxnorm_code in rxnorm_codes:
                    normalized = _normalize_rxnorm_code(rxnorm_code)
                    if not normalized:
                        continue
                    total_edges += await _upsert_external_code_and_edges(
                        schema=schema,
                        code_catalog_table=code_catalog_table,
                        code_crosswalk_table=code_crosswalk_table,
                        hp_code=hp_code,
                        to_system="RXNORM",
                        to_code=normalized,
                        display_name=display_name,
                        confidence=default_confidence,
                        source=source,
                    )

                for ndc_code in ndc_codes:
                    normalized = _normalize_ndc11_code(ndc_code)
                    if not normalized:
                        continue
                    total_edges += await _upsert_external_code_and_edges(
                        schema=schema,
                        code_catalog_table=code_catalog_table,
                        code_crosswalk_table=code_crosswalk_table,
                        hp_code=hp_code,
                        to_system="NDC",
                        to_code=normalized,
                        display_name=display_name,
                        confidence=default_confidence,
                        source=source,
                    )

    return {"attempted": attempted, "mapped_codes": mapped_codes, "edges": total_edges}


async def _publish_by_table_rename(classes: dict[str, type], schema: str) -> None:
    final_classes = (
        PricingPrescription,
        PricingProviderPrescription,
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


async def drug_claims_start(ctx, task: dict[str, Any] | None = None) -> dict[str, Any]:
    task = task or {}
    test_mode = bool(task.get("test_mode", False))
    import_id_val = _normalize_import_id(task.get("import_id"))
    run_id = _normalize_run_id(task.get("run_id"))
    stage_suffix = _build_stage_suffix(import_id_val, run_id)
    redis = ctx.get("redis")
    if redis is None:
        raise RuntimeError("ARQ redis context is unavailable for drug claims start job.")

    total_start = _step_start(
        "drug-claims enqueue+split "
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
    semaphore = asyncio.Semaphore(min(DRUG_CLAIMS_DOWNLOAD_CONCURRENCY, max(source_count, 1)))

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
                    "drug_claims_process_chunk",
                    payload,
                    _queue_name=DRUG_CLAIMS_QUEUE_NAME,
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
        "drug_claims_finalize",
        {
            "import_id": import_id_val,
            "run_id": run_id,
            "stage_suffix": stage_suffix,
            "schema": schema,
            "manifest_path": str(manifest_path),
            "test_mode": test_mode,
        },
        _queue_name=DRUG_CLAIMS_FINISH_QUEUE_NAME,
        _job_id=f"drug_claims_finalize_{run_id}",
    )

    _step_end(
        "drug-claims enqueue+split "
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


async def drug_claims_process_chunk(ctx, task: dict[str, Any] | None = None) -> dict[str, Any]:
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

    if dataset_key == "provider_drug":
        await _load_provider_drug_rows(
            chunk_path,
            classes["PricingProviderPrescription"],
            year,
            test_mode=False,
        )
    elif dataset_key == "drug_spending":
        await _load_drug_spending_rows(
            chunk_path,
            classes["PricingPrescription"],
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


async def drug_claims_finalize(ctx, task: dict[str, Any] | None = None) -> dict[str, Any]:
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
            raise Retry(defer=DRUG_CLAIMS_FINISH_RETRY_SECONDS)

        if not await _claim_finalize_lock(redis, run_id):
            raise Retry(defer=DRUG_CLAIMS_FINISH_RETRY_SECONDS)

    classes = _staging_classes(stage_suffix, schema)
    t = _step_start("ensure live code tables")
    await _ensure_live_code_tables(schema)
    _step_end("ensure live code tables", t)
    t = _step_start("materialize prescription/code dimensions")
    await _materialize_prescription_and_code_rows(classes, schema)
    _step_end("materialize prescription/code dimensions", t)

    if DRUG_CLAIMS_DEFER_STAGE_INDEXES:
        t = _step_start("build staging indexes")
        await _build_staging_indexes(classes, schema)
        _step_end("build staging indexes", t)

    t = _step_start("publish staging -> final (transactional rename)")
    await _publish_by_table_rename(classes, schema)
    _step_end("publish staging -> final (transactional rename)", t)

    if redis is not None and run_id:
        await redis.set(_state_key(run_id, "finalized"), "1", ex=DRUG_CLAIMS_REDIS_TTL_SECONDS)
        await redis.expire(_state_key(run_id, "finalized"), DRUG_CLAIMS_REDIS_TTL_SECONDS)

    if manifest:
        run_work_dir = Path(manifest.get("work_dir", ""))
        if run_work_dir and run_work_dir.exists() and not DRUG_CLAIMS_KEEP_WORKDIR:
            shutil.rmtree(run_work_dir, ignore_errors=True)

    logger.info(
        "CMS drug claims import finalized: test_mode=%s import_id=%s run_id=%s",
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
        "drug_claims_start",
        payload,
        _queue_name=DRUG_CLAIMS_QUEUE_NAME,
        _job_id=f"drug_claims_start_{run_id}",
    )
    stage_suffix = _build_stage_suffix(_normalize_import_id(import_id), run_id)
    print(
        f"Queued drug-claims run: import_id={_normalize_import_id(import_id)} run_id={run_id} stage={stage_suffix} "
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
        "drug_claims_finalize",
        payload,
        _queue_name=DRUG_CLAIMS_FINISH_QUEUE_NAME,
        _job_id=f"drug_claims_finalize_{run_id}",
    )
    print(
        f"Queued drug-claims finalize: import_id={_normalize_import_id(import_id)} run_id={run_id} stage={stage_suffix}",
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
    "drug_claims_start",
    "drug_claims_process_chunk",
    "drug_claims_finalize",
    "finish_main",
    "_find_dataset",
    "_resolve_sources",
    "_row_allowed_for_test",
]
