# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""OpenAddresses US geocode cache and archive backfill."""

from __future__ import annotations

import asyncio
import datetime
import gzip
import hashlib
import json
import logging
import os
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import aiohttp
import ijson
from arq import create_pool

from db.models import OpenAddressesGeocode, OpenAddressesZipRecovery, db
from process.control_cancel import raise_if_cancelled
from process.ext import address_canon
from process.ext.utils import ensure_database, make_class, my_init_db, print_time_info, push_objects
from process.live_progress import enqueue_live_progress
from process.redis_config import build_redis_settings
from process.serialization import deserialize_job, serialize_job

logger = logging.getLogger(__name__)

OPENADDRESSES_QUEUE_NAME = "arq:OpenAddresses"
OPENADDRESSES_TABLE = "openaddresses_geocode"
OPENADDRESSES_API_BASE = os.getenv("HLTHPRT_OPENADDRESSES_API_BASE", "https://batch.openaddresses.io/api").rstrip("/")
DEFAULT_BATCH_SIZE = 5000
DEFAULT_MIN_ROWS = 1000
DEFAULT_TEST_FILE_LIMIT = 2
DEFAULT_TEST_ROW_LIMIT = 1000
DEFAULT_SOURCE_CONCURRENCY = 8
DEFAULT_BACKFILL_CONCURRENCY = 4
DEFAULT_BACKFILL_ZIP_PREFIX_LENGTH = 2
DEFAULT_ZIP_RESTORE_CONCURRENCY = 8
DEFAULT_FUZZY_THRESHOLD = 0.92
DEFAULT_FUZZY_MARGIN = 0.08
DEFAULT_RELAXED_STREET_THRESHOLD = 0.86
DEFAULT_RELAXED_MARGIN = 0.10
DEFAULT_DUPLICATE_COORD_TOLERANCE = 0.0005
DEFAULT_DOWNLOAD_RETRIES = 4
DEFAULT_DOWNLOAD_RETRY_BASE_SECONDS = 2.0
RETRYABLE_DOWNLOAD_STATUSES = {429, 500, 502, 503, 504}
POSTGRES_IDENTIFIER_MAX_LENGTH = 63
HOUSE_NUMBER_RE = re.compile(r"^\s*([0-9]+[a-zA-Z]?)\b")


@dataclass(frozen=True)
class OpenAddressesBackfillStats:
    exact_updates: int
    fuzzy_updates: int
    relaxed_updates: int


@dataclass(frozen=True)
class OpenAddressesZipRestoreStats:
    candidates: int = 0
    restored: int = 0
    discarded: int = 0
    shards: int = 0


@dataclass(frozen=True)
class OpenAddressesBackfillShard:
    state_code: str | None
    zip_prefix: str | None
    candidate_count: int = 0

    @property
    def label(self) -> str:
        parts = []
        if self.state_code:
            parts.append(self.state_code)
        if self.zip_prefix:
            parts.append(f"ZIP {self.zip_prefix}*")
        return " ".join(parts) or "all"


@dataclass(frozen=True)
class _RecordBatch:
    processed: int
    rows: list[dict[str, Any]]
    zip_recovery_rows: list[dict[str, Any]]
    rejection_counts: dict[str, int]


def _env_positive_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    return _positive_int_value(raw, default)


def _positive_int_value(raw: Any, default: int) -> int:
    if not raw:
        return default
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return default
    return value if value > 0 else default


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if not raw:
        return default
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return default
    return value if value > 0 else default


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _validate_schema_name(schema: str) -> str:
    cleaned = (schema or "").strip()
    if not cleaned or not (cleaned[0].isalpha() or cleaned[0] == "_"):
        raise ValueError(f"Invalid schema name: {schema!r}")
    if not all(ch.isalnum() or ch == "_" for ch in cleaned):
        raise ValueError(f"Invalid schema name: {schema!r}")
    return cleaned


def _quote_ident(value: str) -> str:
    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", value or ""):
        raise ValueError(f"Unsafe SQL identifier: {value!r}")
    return f'"{value}"'


def _qtable(schema: str, table: str) -> str:
    return f"{_quote_ident(schema)}.{_quote_ident(table)}"


def _normalize_import_id(raw: str | None) -> str:
    if raw:
        cleaned = "".join(ch for ch in str(raw) if ch.isalnum())
        if cleaned:
            return cleaned[:32]
    return datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S")


def _archived_identifier(name: str, suffix: str = "_old") -> str:
    candidate = f"{name}{suffix}"
    if len(candidate) <= POSTGRES_IDENTIFIER_MAX_LENGTH:
        return candidate
    digest = hashlib.sha1(name.encode("utf-8")).hexdigest()[:8]
    trim_to = max(1, POSTGRES_IDENTIFIER_MAX_LENGTH - len(suffix) - len(digest) - 1)
    return f"{name[:trim_to]}_{digest}{suffix}"


def _bounded_identifier(name: str) -> str:
    if len(name) <= POSTGRES_IDENTIFIER_MAX_LENGTH:
        return name
    digest = hashlib.sha1(name.encode("utf-8")).hexdigest()[:8]
    trim_to = max(1, POSTGRES_IDENTIFIER_MAX_LENGTH - len(digest) - 1)
    return f"{name[:trim_to]}_{digest}"


def _status_count(status: Any) -> int:
    if status is None:
        return 0
    if isinstance(status, int):
        return status
    text = str(status).strip()
    if text.isdigit():
        return int(text)
    parts = text.split()
    if parts and parts[-1].isdigit():
        return int(parts[-1])
    return 0


def _safe_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalize_house_number(value: Any) -> str | None:
    text = _safe_text(value)
    if not text:
        return None
    match = HOUSE_NUMBER_RE.match(text)
    if match:
        return match.group(1).lower()
    cleaned = re.sub(r"[^0-9A-Za-z]", "", text)
    return cleaned.lower() if cleaned else None


def _house_number_from_line(first_line: str | None) -> str | None:
    return _normalize_house_number(first_line)


def _street_after_house(first_line: str | None) -> str | None:
    text = _safe_text(first_line)
    if not text:
        return None
    match = HOUSE_NUMBER_RE.match(text)
    if not match:
        return text
    return text[match.end() :].strip() or None


def _street_match_key(value: Any) -> str | None:
    text = _safe_text(value)
    if not text:
        return None
    parts = []
    for token in re.findall(r"[a-z0-9]+", text.lower()):
        normalized = address_canon._street_token_norm(token)  # pylint: disable=protected-access
        if normalized:
            parts.append(normalized)
    return "".join(parts) or None


def _valid_us_coordinate(lat: Any, lon: Any) -> tuple[float, float] | None:
    try:
        lat_value = float(lat)
        lon_value = float(lon)
    except (TypeError, ValueError):
        return None
    if not (-15.0 <= lat_value <= 75.0):
        return None
    if not ((-180.0 <= lon_value <= -60.0) or (140.0 <= lon_value <= 180.0)):
        return None
    return lat_value, lon_value


def _source_state(source: str | None) -> str | None:
    parts = [part for part in (source or "").split("/") if part]
    if len(parts) >= 2 and parts[0].lower() == "us":
        return address_canon.state_code(parts[1])
    return None


def _source_updated(value: Any) -> datetime.datetime | None:
    try:
        numeric = int(value)
    except (TypeError, ValueError):
        return None
    # OpenAddresses timestamps are milliseconds in current API responses.
    if numeric > 10_000_000_000:
        numeric = numeric // 1000
    return datetime.datetime.fromtimestamp(numeric, tz=datetime.timezone.utc)


def _format_address(
    *,
    first_line: str,
    unit: str | None,
    city: str | None,
    state: str,
    zip5: str,
) -> str:
    state_postal = " ".join(part for part in [state, zip5] if part)
    return ", ".join(part for part in [first_line, unit, city, state_postal] if part)


def _openaddresses_row_hash(
    *,
    source: str | None,
    feature_id: str | None,
    house_number: str,
    street_match: str,
    city: str | None,
    state: str,
    zip5: str,
    lat: float,
    lon: float,
) -> str:
    return hashlib.sha256(
        "|".join(
            [
                source or "",
                feature_id or "",
                house_number,
                street_match,
                city or "",
                state,
                zip5,
                f"{lat:.8f}",
                f"{lon:.8f}",
            ]
        ).encode("utf-8")
    ).hexdigest()


def _openaddresses_recovery_hash(
    *,
    source: str | None,
    feature_id: str | None,
    house_number: str,
    street_match: str,
    city: str | None,
    state: str,
    lat: float,
    lon: float,
) -> str:
    return hashlib.sha256(
        "|".join(
            [
                source or "",
                feature_id or "",
                house_number,
                street_match,
                city or "",
                state,
                f"{lat:.8f}",
                f"{lon:.8f}",
            ]
        ).encode("utf-8")
    ).hexdigest()


def _zip_restore_bucket(raw_hash: str, shard_count: int) -> int:
    bounded_shards = max(int(shard_count or 1), 1)
    return int(str(raw_hash)[:16], 16) % bounded_shards


def _feature_parts(
    feature: dict[str, Any],
    *,
    source: str | None,
    updated: Any,
) -> tuple[dict[str, Any] | None, str | None]:
    if not isinstance(feature, dict):
        return None, "not_dict"
    properties = feature.get("properties") or {}
    geometry = feature.get("geometry") or {}
    coordinates = geometry.get("coordinates") or []
    if geometry.get("type") != "Point" or len(coordinates) < 2:
        return None, "not_point"
    parsed_coord = _valid_us_coordinate(coordinates[1], coordinates[0])
    if not parsed_coord:
        return None, "invalid_coordinate"
    lat, lon = parsed_coord

    house_number = _normalize_house_number(properties.get("number") or properties.get("addr:housenumber"))
    street_name = _safe_text(properties.get("street") or properties.get("addr:street"))
    street_match = _street_match_key(street_name)
    state = (
        address_canon.state_code(properties.get("region"))
        or address_canon.state_code(properties.get("state"))
        or _source_state(source)
    )
    zip5 = address_canon.zip5_norm(properties.get("postcode") or properties.get("zip"))
    if not house_number:
        return None, "missing_house_number"
    if not street_name:
        return None, "missing_street"
    if not street_match:
        return None, "missing_street_match_key"
    if not state:
        return None, "missing_state"

    unit = _safe_text(properties.get("unit") or properties.get("addr:unit"))
    city = _safe_text(properties.get("city"))
    feature_id = _safe_text(properties.get("id") or properties.get("hash"))
    parts = {
        "house_number": house_number,
        "street_name": street_name,
        "street_match": street_match,
        "state": state,
        "zip5": zip5,
        "unit": unit,
        "city": city,
        "feature_id": feature_id,
        "lat": lat,
        "lon": lon,
        "accuracy": _safe_text(properties.get("accuracy")),
        "source_updated": _source_updated(updated),
    }
    if not zip5:
        return parts, "missing_zip5"
    return parts, None


def lookup_params_from_address(address: dict[str, Any]) -> dict[str, Any] | None:
    first_line = _safe_text(address.get("first_line"))
    if not first_line:
        return None
    country = address_canon.country_code(address.get("country_code") or "US")
    if country != "US":
        return None
    house_number = _house_number_from_line(first_line)
    street_match_key = _street_match_key(_street_after_house(first_line))
    city_norm = address_canon.city_norm(address.get("city_name") or address.get("city"))
    state_code = address_canon.state_code(address.get("state_name"))
    zip5 = address_canon.zip5_norm(address.get("postal_code"))
    if not (house_number and street_match_key and state_code and zip5):
        return None
    return {
        "house_number": house_number,
        "street_match_key": street_match_key,
        "city_norm": city_norm,
        "state_code": state_code,
        "zip5": zip5,
        "coord_tolerance": _env_float(
            "HLTHPRT_OPENADDRESSES_DUPLICATE_COORD_TOLERANCE",
            DEFAULT_DUPLICATE_COORD_TOLERANCE,
        ),
        "fuzzy_threshold": _env_float("HLTHPRT_OPENADDRESSES_FUZZY_THRESHOLD", DEFAULT_FUZZY_THRESHOLD),
        "fuzzy_margin": _env_float("HLTHPRT_OPENADDRESSES_FUZZY_MARGIN", DEFAULT_FUZZY_MARGIN),
        "relaxed_threshold": _env_float(
            "HLTHPRT_OPENADDRESSES_RELAXED_STREET_THRESHOLD",
            DEFAULT_RELAXED_STREET_THRESHOLD,
        ),
        "relaxed_margin": _env_float("HLTHPRT_OPENADDRESSES_RELAXED_MARGIN", DEFAULT_RELAXED_MARGIN),
    }


def exact_lookup_sql(schema: str, table_name: str = OPENADDRESSES_TABLE) -> str:
    table = _qtable(schema, table_name)
    qschema = _quote_ident(schema)
    return f"""
        WITH grouped AS (
            SELECT
                state_code, zip5, house_number, street_match_key,
                avg(lat)::numeric(11,8) AS lat,
                avg(long)::numeric(11,8) AS long,
                min(formatted_address) AS formatted_address,
                min(feature_id) AS place_id,
                count(*) AS row_count,
                NULLIF(max(NULLIF(accuracy, '')), '') AS accuracy
              FROM {table}
             WHERE state_code = :state_code
               AND zip5 = :zip5
               AND house_number = :house_number
               AND street_match_key = :street_match_key
               AND (
                   :city_norm IS NULL
                   OR NULLIF({qschema}.addr_city_norm_v1(city_name), '') = :city_norm
               )
             GROUP BY state_code, zip5, house_number, street_match_key
            HAVING count(*) = 1
                OR (
                    max(lat) - min(lat) <= :coord_tolerance
                    AND max(long) - min(long) <= :coord_tolerance
                )
        )
        SELECT
            long, lat, formatted_address, place_id,
            'openaddresses' AS geo_source,
            'openaddresses_exact' AS geocode_source,
            COALESCE(accuracy, 'unknown') AS geocode_quality
          FROM grouped
         LIMIT 1;
    """


def fuzzy_lookup_sql(schema: str, table_name: str = OPENADDRESSES_TABLE) -> str:
    table = _qtable(schema, table_name)
    return f"""
        WITH grouped AS (
            SELECT
                state_code, zip5, house_number, street_match_key,
                avg(lat)::numeric(11,8) AS lat,
                avg(long)::numeric(11,8) AS long,
                min(formatted_address) AS formatted_address,
                min(feature_id) AS place_id,
                count(*) AS row_count,
                NULLIF(max(NULLIF(accuracy, '')), '') AS accuracy
              FROM {table}
             WHERE state_code = :state_code
               AND zip5 = :zip5
               AND house_number = :house_number
             GROUP BY state_code, zip5, house_number, street_match_key
            HAVING count(*) = 1
                OR (
                    max(lat) - min(lat) <= :coord_tolerance
                    AND max(long) - min(long) <= :coord_tolerance
                )
        ),
        scored AS (
            SELECT
                *,
                similarity(street_match_key, :street_match_key) AS score
              FROM grouped
             WHERE street_match_key <> :street_match_key
               AND similarity(street_match_key, :street_match_key) >= :fuzzy_threshold
        ),
        ranked AS (
            SELECT
                *,
                row_number() OVER (ORDER BY score DESC, row_count ASC, street_match_key) AS rn,
                lead(score) OVER (ORDER BY score DESC, row_count ASC, street_match_key) AS next_score
              FROM scored
        )
        SELECT
            long, lat, formatted_address, place_id,
            'openaddresses' AS geo_source,
            'openaddresses_fuzzy_zip' AS geocode_source,
            COALESCE(accuracy, 'strict_fuzzy') AS geocode_quality
          FROM ranked
         WHERE rn = 1
           AND (next_score IS NULL OR score - next_score >= :fuzzy_margin)
         LIMIT 1;
    """


def relaxed_lookup_sql(schema: str, table_name: str = OPENADDRESSES_TABLE) -> str:
    table = _qtable(schema, table_name)
    qschema = _quote_ident(schema)
    return f"""
        WITH grouped AS (
            SELECT
                state_code,
                zip5,
                house_number,
                NULLIF({qschema}.addr_city_norm_v1(city_name), '') AS city_norm,
                street_match_key,
                avg(lat)::numeric(11,8) AS lat,
                avg(long)::numeric(11,8) AS long,
                min(formatted_address) AS formatted_address,
                min(feature_id) AS place_id,
                count(*) AS row_count,
                NULLIF(max(NULLIF(accuracy, '')), '') AS accuracy
              FROM {table}
             WHERE state_code = :state_code
               AND zip5 = :zip5
               AND house_number = :house_number
               AND :city_norm IS NOT NULL
               AND NULLIF({qschema}.addr_city_norm_v1(city_name), '') = :city_norm
             GROUP BY state_code, zip5, house_number, city_norm, street_match_key
            HAVING count(*) = 1
                OR (
                    max(lat) - min(lat) <= :coord_tolerance
                    AND max(long) - min(long) <= :coord_tolerance
                )
        ),
        scored AS (
            SELECT
                *,
                similarity(street_match_key, :street_match_key) AS score
              FROM grouped
             WHERE street_match_key <> :street_match_key
               AND similarity(street_match_key, :street_match_key) >= :relaxed_threshold
        ),
        ranked AS (
            SELECT
                *,
                row_number() OVER (ORDER BY score DESC, row_count ASC, street_match_key) AS rn,
                lead(score) OVER (ORDER BY score DESC, row_count ASC, street_match_key) AS next_score
              FROM scored
        )
        SELECT
            long, lat, formatted_address, place_id,
            'openaddresses' AS geo_source,
            'openaddresses_relaxed_city_zip' AS geocode_source,
            COALESCE(accuracy, 'relaxed_city_zip') AS geocode_quality
          FROM ranked
         WHERE rn = 1
           AND (next_score IS NULL OR score - next_score >= :relaxed_margin)
         LIMIT 1;
    """


def _record_from_feature(
    feature: dict[str, Any],
    *,
    source: str | None,
    data_id: int | None,
    job_id: int | None,
    updated: Any,
) -> dict[str, Any] | None:
    parts, reason = _feature_parts(feature, source=source, updated=updated)
    if reason or not parts:
        return None
    house_number = parts["house_number"]
    street_name = parts["street_name"]
    street_match = parts["street_match"]
    state = parts["state"]
    zip5 = parts["zip5"]
    unit = parts["unit"]
    city = parts["city"]
    lat = parts["lat"]
    lon = parts["lon"]
    first_line = f"{house_number} {street_name}"
    identity_key = address_canon.identity_key_v1(first_line, unit, city, state, zip5, "US")
    address_key = address_canon.address_key_v1(first_line, unit, city, state, zip5, "US")
    formatted_address = _format_address(first_line=first_line, unit=unit, city=city, state=state, zip5=zip5)
    feature_id = parts["feature_id"]
    row_hash = _openaddresses_row_hash(
        source=source,
        feature_id=feature_id,
        house_number=house_number,
        street_match=street_match,
        city=city,
        state=state,
        zip5=zip5,
        lat=lat,
        lon=lon,
    )
    return {
        "row_hash": row_hash,
        "address_key": address_key,
        "identity_key": identity_key,
        "house_number": house_number,
        "street_match_key": street_match,
        "street_name": street_name,
        "unit": unit,
        "city_name": city,
        "state_code": state,
        "zip5": zip5,
        "zip5_source": "openaddresses_postcode",
        "zip5_restored_at": None,
        "formatted_address": formatted_address,
        "lat": lat,
        "long": lon,
        "source": source,
        "data_id": data_id,
        "job_id": job_id,
        "feature_id": feature_id,
        "accuracy": parts["accuracy"],
        "source_updated": parts["source_updated"],
        "imported_at": datetime.datetime.now(datetime.timezone.utc),
    }


def _zip_recovery_record_from_feature(
    feature: dict[str, Any],
    *,
    source: str | None,
    data_id: int | None,
    job_id: int | None,
    updated: Any,
    restore_shards: int,
) -> tuple[dict[str, Any] | None, str | None]:
    parts, reason = _feature_parts(feature, source=source, updated=updated)
    if reason != "missing_zip5" or not parts:
        return None, reason
    raw_hash = _openaddresses_recovery_hash(
        source=source,
        feature_id=parts["feature_id"],
        house_number=parts["house_number"],
        street_match=parts["street_match"],
        city=parts["city"],
        state=parts["state"],
        lat=parts["lat"],
        lon=parts["lon"],
    )
    return {
        "raw_hash": raw_hash,
        "restore_bucket": _zip_restore_bucket(raw_hash, restore_shards),
        "house_number": parts["house_number"],
        "street_match_key": parts["street_match"],
        "street_name": parts["street_name"],
        "unit": parts["unit"],
        "city_name": parts["city"],
        "state_code": parts["state"],
        "lat": parts["lat"],
        "long": parts["lon"],
        "source": source,
        "data_id": data_id,
        "job_id": job_id,
        "feature_id": parts["feature_id"],
        "accuracy": parts["accuracy"],
        "source_updated": parts["source_updated"],
        "imported_at": datetime.datetime.now(datetime.timezone.utc),
    }, reason


def _open_geojson(path: Path):
    if path.suffix == ".gz":
        return gzip.open(path, "rb")
    return path.open("rb")


def _iter_geojson_lines(handle) -> Iterable[dict[str, Any]]:
    for raw_line in handle:
        line = raw_line.strip()
        if not line:
            continue
        feature = json.loads(line)
        if isinstance(feature, dict) and isinstance(feature.get("features"), list):
            for item in feature["features"]:
                yield item
        else:
            yield feature


def _iter_geojson_features(path: Path) -> Iterable[dict[str, Any]]:
    with _open_geojson(path) as handle:
        first_line = handle.readline()
        handle.seek(0)
        if first_line:
            try:
                first_obj = json.loads(first_line)
            except json.JSONDecodeError:
                first_obj = None
            if (
                isinstance(first_obj, dict)
                and "features" not in first_obj
                and (first_obj.get("type") == "Feature" or ("properties" in first_obj and "geometry" in first_obj))
            ):
                yield from _iter_geojson_lines(handle)
                return

    yielded = False
    try:
        with _open_geojson(path) as handle:
            for feature in ijson.items(handle, "features.item"):
                yielded = True
                yield feature
    except ijson.JSONError:
        with _open_geojson(path) as handle:
            yield from _iter_geojson_lines(handle)
        return
    if yielded:
        return
    with _open_geojson(path) as handle:
        for feature in ijson.items(handle, "item"):
            yield feature


async def _flush_rows(rows: list[dict[str, Any]], stage_cls) -> int:
    if not rows:
        return 0
    accepted = len(rows)
    await push_objects(rows, stage_cls, rewrite=True, use_copy=True)
    rows.clear()
    return accepted


async def _flush_zip_recovery_rows(rows: list[dict[str, Any]], recovery_cls) -> int:
    if not rows:
        return 0
    accepted = len(rows)
    await push_objects(rows, recovery_cls, rewrite=True, use_copy=True)
    rows.clear()
    return accepted


async def _fetch_json(client: aiohttp.ClientSession, url: str, token: str | None) -> Any:
    headers = {"Authorization": f"Bearer {token}"} if token else None
    async with client.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=120)) as response:
        if response.status != 200:
            body = await response.text()
            raise RuntimeError(f"OpenAddresses metadata fetch failed HTTP {response.status}: {body[:200]}")
        return await response.json(content_type=None)


async def _maybe_raise_if_cancelled(ctx: dict[str, Any] | None, task: dict[str, Any] | None) -> None:
    if ctx is not None and task is not None:
        await raise_if_cancelled(ctx, task)


def _iter_record_batches(
    path: Path,
    *,
    batch_size: int,
    source: str | None = None,
    data_id: int | None = None,
    job_id: int | None = None,
    updated: Any = None,
    row_limit: int | None = None,
    restore_shards: int = 1,
) -> Iterable[_RecordBatch]:
    processed = 0
    rows: list[dict[str, Any]] = []
    zip_recovery_rows: list[dict[str, Any]] = []
    rejection_counts: dict[str, int] = {}
    for feature in _iter_geojson_features(path):
        processed += 1
        record = _record_from_feature(
            feature,
            source=source,
            data_id=data_id,
            job_id=job_id,
            updated=updated,
        )
        if record:
            rows.append(record)
        else:
            recovery_record, reason = _zip_recovery_record_from_feature(
                feature,
                source=source,
                data_id=data_id,
                job_id=job_id,
                updated=updated,
                restore_shards=restore_shards,
            )
            if recovery_record:
                zip_recovery_rows.append(recovery_record)
            rejection_counts[reason or "rejected"] = rejection_counts.get(reason or "rejected", 0) + 1
        if len(rows) >= batch_size or len(zip_recovery_rows) >= batch_size:
            yield _RecordBatch(
                processed=processed,
                rows=rows,
                zip_recovery_rows=zip_recovery_rows,
                rejection_counts=rejection_counts,
            )
            rows = []
            zip_recovery_rows = []
            rejection_counts = {}
        if row_limit and processed >= row_limit:
            break
    if rows or zip_recovery_rows or processed:
        yield _RecordBatch(
            processed=processed,
            rows=rows,
            zip_recovery_rows=zip_recovery_rows,
            rejection_counts=rejection_counts,
        )


def _next_record_batch(iterator: Iterable[_RecordBatch]) -> _RecordBatch | None:
    try:
        return next(iterator)  # type: ignore[arg-type]
    except StopIteration:
        return None


async def _download_file(
    client: aiohttp.ClientSession,
    url: str,
    path: Path,
    token: str,
    *,
    ctx: dict[str, Any] | None = None,
    task: dict[str, Any] | None = None,
) -> None:
    await _maybe_raise_if_cancelled(ctx, task)
    headers = {"Authorization": f"Bearer {token}"}
    retry_task = task or {}
    retries = _task_or_env_positive_int(
        retry_task,
        "download_retries",
        "HLTHPRT_OPENADDRESSES_DOWNLOAD_RETRIES",
        DEFAULT_DOWNLOAD_RETRIES,
    )
    max_attempts = max(1, retries + 1)
    for attempt in range(1, max_attempts + 1):
        try:
            async with client.get(
                url,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=None, connect=60, sock_read=600),
            ) as response:
                if response.status != 200:
                    body = await response.text()
                    message = f"OpenAddresses download failed HTTP {response.status}: {body[:200]}"
                    if response.status not in RETRYABLE_DOWNLOAD_STATUSES or attempt >= max_attempts:
                        raise RuntimeError(message)
                    raise _RetryableOpenAddressesDownload(message)
                with path.open("wb") as handle:
                    async for chunk_index, chunk in _aiter_enumerate(response.content.iter_chunked(1024 * 1024), start=1):
                        handle.write(chunk)
                        if chunk_index % 16 == 0:
                            await _maybe_raise_if_cancelled(ctx, task)
            await _maybe_raise_if_cancelled(ctx, task)
            return
        except _RetryableOpenAddressesDownload as exc:
            _remove_partial_download(path)
            logger.warning(
                "Retrying OpenAddresses download after transient response attempt=%s/%s url=%s error=%s",
                attempt,
                max_attempts,
                url,
                exc,
            )
            await _sleep_before_download_retry(attempt, ctx=ctx, task=task)
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            _remove_partial_download(path)
            if attempt >= max_attempts:
                raise RuntimeError(f"OpenAddresses download failed after {attempt} attempt(s): {exc}") from exc
            logger.warning(
                "Retrying OpenAddresses download after transport error attempt=%s/%s url=%s error=%s",
                attempt,
                max_attempts,
                url,
                exc,
            )
            await _sleep_before_download_retry(attempt, ctx=ctx, task=task)


class _RetryableOpenAddressesDownload(RuntimeError):
    pass


def _remove_partial_download(path: Path) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        pass


async def _sleep_before_download_retry(
    attempt: int,
    *,
    ctx: dict[str, Any] | None,
    task: dict[str, Any] | None,
) -> None:
    await _maybe_raise_if_cancelled(ctx, task)
    delay = min(DEFAULT_DOWNLOAD_RETRY_BASE_SECONDS * (2 ** max(attempt - 1, 0)), 30.0)
    await asyncio.sleep(delay)
    await _maybe_raise_if_cancelled(ctx, task)


async def _aiter_enumerate(iterable, *, start: int = 0):
    index = start
    async for item in iterable:
        yield index, item
        index += 1


def _us_data_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    selected = []
    for item in items:
        source = str(item.get("source") or "")
        output = item.get("output") or {}
        if not source.startswith("us/"):
            continue
        if item.get("layer") != "addresses":
            continue
        if output and not output.get("output"):
            continue
        selected.append(item)
    selected.sort(key=lambda item: str(item.get("source") or ""))
    return selected


def _local_files_from_env() -> list[Path]:
    raw = os.getenv("HLTHPRT_OPENADDRESSES_LOCAL_FILES", "").strip()
    if not raw:
        return []
    return [Path(part.strip()) for part in raw.split(",") if part.strip()]


def _local_files_from_task_or_env(task: dict[str, Any]) -> list[Path]:
    raw = task.get("local_files")
    if raw is None:
        raw = task.get("local_file")
    if raw is None:
        raw = task.get("openaddresses_local_files")
    if raw is None:
        return _local_files_from_env()
    if isinstance(raw, (list, tuple, set)):
        return [Path(str(part).strip()) for part in raw if str(part).strip()]
    return [Path(part.strip()) for part in str(raw).split(",") if part.strip()]


def _task_or_env_positive_int(task: dict[str, Any], key: str, env_name: str, default: int) -> int:
    if key in task and task.get(key) not in {None, ""}:
        return _positive_int_value(task.get(key), default)
    return _env_positive_int(env_name, default)


def _task_or_env_int_range(
    task: dict[str, Any],
    key: str,
    env_name: str,
    default: int,
    *,
    minimum: int,
    maximum: int,
) -> int:
    value = _task_or_env_positive_int(task, key, env_name, default)
    return min(max(value, minimum), maximum)


def _task_or_env_bool(task: dict[str, Any], key: str, env_name: str, default: bool = False) -> bool:
    if key in task and task.get(key) is not None:
        value = task.get(key)
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in {"1", "true", "yes", "on"}
    return _env_bool(env_name, default)


def _progress_run_id(ctx: dict[str, Any], task: dict[str, Any]) -> str | None:
    context = ctx.get("context") if isinstance(ctx, dict) else {}
    if not isinstance(context, dict):
        context = {}
    run_id = (
        task.get("run_id")
        or task.get("control_run_id")
        or ctx.get("control_run_id")
        or context.get("control_run_id")
    )
    value = str(run_id or "").strip()
    return value or None


def _emit_load_progress(
    *,
    processed_files: int,
    total_files: int,
    processed_rows: int,
    accepted_rows: int,
    label: str | None = None,
    phase: str = "loading OpenAddresses sources",
    run_id: str | None = None,
) -> None:
    total = max(int(total_files or 0), 1)
    done = max(int(processed_files or 0), 0)
    pct = min((done / total) * 100.0, 100.0)
    message = (
        f"{done:,}/{total_files:,} sources; "
        f"{processed_rows:,} rows processed; {accepted_rows:,} rows accepted"
    )
    payload = dict(
        importer="openaddresses",
        status="running",
        unit="sources",
        done=done,
        total=total_files,
        pct=pct,
        phase=phase,
        message=message,
        label=label,
        step=label,
        source="openaddresses-load-progress",
        confidence="live",
    )
    if run_id:
        payload["run_id"] = run_id
    enqueue_live_progress(**payload)


def _emit_backfill_progress(
    *,
    completed_shards: int,
    total_shards: int,
    stats: OpenAddressesBackfillStats,
    label: str | None = None,
    total_candidates: int = 0,
    run_id: str | None = None,
) -> None:
    total = max(int(total_shards or 0), 1)
    done = max(int(completed_shards or 0), 0)
    pct = min((done / total) * 100.0, 100.0)
    updated = stats.exact_updates + stats.fuzzy_updates + stats.relaxed_updates
    candidate_text = f"; {total_candidates:,} candidate rows" if total_candidates else ""
    message = (
        f"{done:,}/{total_shards:,} shards; {updated:,} archive rows updated "
        f"(exact={stats.exact_updates:,}, fuzzy={stats.fuzzy_updates:,}, "
        f"relaxed={stats.relaxed_updates:,}){candidate_text}"
    )
    payload = dict(
        importer="openaddresses",
        status="running",
        unit="shards",
        done=done,
        total=total_shards,
        pct=pct,
        phase="backfilling address archive from OpenAddresses",
        message=message,
        label=label,
        step=label,
        source="openaddresses-backfill-progress",
        confidence="live",
    )
    if run_id:
        payload["run_id"] = run_id
    enqueue_live_progress(**payload)


async def _load_file(
    path: Path,
    *,
    stage_cls,
    recovery_cls=None,
    batch_size: int,
    source: str | None = None,
    data_id: int | None = None,
    job_id: int | None = None,
    updated: Any = None,
    row_limit: int | None = None,
    restore_shards: int = 1,
    ctx: dict[str, Any] | None = None,
    task: dict[str, Any] | None = None,
) -> tuple[int, int, int, dict[str, int]]:
    await _maybe_raise_if_cancelled(ctx, task)
    accepted = 0
    zip_recovery = 0
    processed = 0
    rejection_counts: dict[str, int] = {}
    batch_iter = iter(
        _iter_record_batches(
            path,
            batch_size=batch_size,
            source=source,
            data_id=data_id,
            job_id=job_id,
            updated=updated,
            row_limit=row_limit,
            restore_shards=restore_shards,
        )
    )
    while True:
        batch = await asyncio.to_thread(_next_record_batch, batch_iter)
        if batch is None:
            break
        processed = batch.processed
        accepted += await _flush_rows(batch.rows, stage_cls)
        if recovery_cls is not None:
            zip_recovery += await _flush_zip_recovery_rows(batch.zip_recovery_rows, recovery_cls)
        for reason, count in batch.rejection_counts.items():
            rejection_counts[reason] = rejection_counts.get(reason, 0) + count
        await _maybe_raise_if_cancelled(ctx, task)
    return processed, accepted, zip_recovery, rejection_counts


async def _load_source_item(
    *,
    ctx: dict[str, Any],
    task: dict[str, Any],
    client: aiohttp.ClientSession,
    item: dict[str, Any],
    tmpdir_path: Path,
    token: str,
    stage_cls,
    recovery_cls,
    batch_size: int,
    test_mode: bool,
    test_row_limit: int,
    restore_shards: int,
) -> tuple[str, int, int, int, dict[str, int]]:
    job_id = int(item["job"])
    data_id = int(item["id"])
    source = str(item.get("source") or "")
    url = f"{OPENADDRESSES_API_BASE}/job/{job_id}/output/source.geojson.gz"
    path = tmpdir_path / f"openaddresses-{data_id}-{job_id}.geojson.gz"
    logger.info("Downloading OpenAddresses source=%s job=%s", source, job_id)
    await _download_file(client, url, path, token, ctx=ctx, task=task)
    file_processed, file_accepted, file_zip_recovery, rejection_counts = await _load_file(
        path,
        stage_cls=stage_cls,
        recovery_cls=recovery_cls,
        batch_size=batch_size,
        source=source,
        data_id=data_id,
        job_id=job_id,
        updated=item.get("updated"),
        row_limit=test_row_limit if test_mode else None,
        restore_shards=restore_shards,
        ctx=ctx,
        task=task,
    )
    try:
        path.unlink()
    except FileNotFoundError:
        pass
    return source, file_processed, file_accepted, file_zip_recovery, rejection_counts


async def _load_openaddresses_data(ctx: dict[str, Any], task: dict[str, Any], stage_cls, recovery_cls) -> dict[str, int]:
    await _maybe_raise_if_cancelled(ctx, task)
    test_mode = bool(ctx["context"].get("test_mode"))
    batch_size = _task_or_env_positive_int(task, "batch_size", "HLTHPRT_OPENADDRESSES_BATCH_SIZE", DEFAULT_BATCH_SIZE)
    test_file_limit = _task_or_env_positive_int(
        task,
        "test_file_limit",
        "HLTHPRT_OPENADDRESSES_TEST_FILE_LIMIT",
        DEFAULT_TEST_FILE_LIMIT,
    )
    test_row_limit = _task_or_env_positive_int(
        task,
        "test_row_limit",
        "HLTHPRT_OPENADDRESSES_TEST_ROW_LIMIT",
        DEFAULT_TEST_ROW_LIMIT,
    )
    max_files = _task_or_env_positive_int(task, "max_files", "HLTHPRT_OPENADDRESSES_MAX_FILES", 0)
    source_concurrency = _task_or_env_positive_int(
        task,
        "source_concurrency",
        "HLTHPRT_OPENADDRESSES_SOURCE_CONCURRENCY",
        DEFAULT_SOURCE_CONCURRENCY,
    )
    local_files = _local_files_from_task_or_env(task)
    zip_restore_concurrency = _task_or_env_positive_int(
        task,
        "zip_restore_concurrency",
        "HLTHPRT_OPENADDRESSES_ZIP_RESTORE_CONCURRENCY",
        DEFAULT_ZIP_RESTORE_CONCURRENCY,
    )
    restore_shards = _task_or_env_positive_int(
        task,
        "zip_restore_shards",
        "HLTHPRT_OPENADDRESSES_ZIP_RESTORE_SHARDS",
        max(zip_restore_concurrency * 8, 64),
    )
    ctx["context"]["zip_restore_concurrency"] = zip_restore_concurrency
    ctx["context"]["zip_restore_shards"] = restore_shards

    processed_files = 0
    processed_rows = 0
    accepted_rows = 0
    zip_recovery_rows = 0
    rejection_counts: dict[str, int] = {}
    progress_run_id = _progress_run_id(ctx, task)
    timeout = aiohttp.ClientTimeout(total=120, connect=30, sock_read=120)

    if local_files:
        total_files = len(local_files)
        local_concurrency = min(max(source_concurrency, 1), max(total_files, 1))
        print(f"OpenAddresses local source concurrency={local_concurrency}", flush=True)
        _emit_load_progress(
            processed_files=0,
            total_files=total_files,
            processed_rows=0,
            accepted_rows=0,
            phase="loading local OpenAddresses files",
            run_id=progress_run_id,
        )
        progress_lock = asyncio.Lock()
        queue: asyncio.Queue[tuple[int, Path]] = asyncio.Queue()
        for source_index, path in enumerate(local_files, start=1):
            queue.put_nowait((source_index, path))

        async def local_worker(worker_id: int) -> None:
            nonlocal processed_files, processed_rows, accepted_rows, zip_recovery_rows, rejection_counts
            while True:
                await _maybe_raise_if_cancelled(ctx, task)
                try:
                    source_index, path = queue.get_nowait()
                except asyncio.QueueEmpty:
                    return
                try:
                    if not path.exists():
                        raise FileNotFoundError(f"OpenAddresses local file not found: {path}")
                    file_processed, file_accepted, file_zip_recovery, file_rejection_counts = await _load_file(
                        path,
                        stage_cls=stage_cls,
                        recovery_cls=recovery_cls,
                        batch_size=batch_size,
                        source="local/openaddresses",
                        row_limit=test_row_limit if test_mode else None,
                        restore_shards=restore_shards,
                        ctx=ctx,
                        task=task,
                    )
                    async with progress_lock:
                        await _maybe_raise_if_cancelled(ctx, task)
                        processed_files += 1
                        processed_rows += file_processed
                        accepted_rows += file_accepted
                        zip_recovery_rows += file_zip_recovery
                        for reason, count in file_rejection_counts.items():
                            rejection_counts[reason] = rejection_counts.get(reason, 0) + count
                        print(
                            "OpenAddresses local source "
                            f"{processed_files:,}/{total_files:,} "
                            f"index={source_index:,} worker={worker_id} {path}: "
                            f"processed={file_processed:,} accepted={file_accepted:,} "
                            f"zip_recovery={file_zip_recovery:,} "
                            f"accepted_total={accepted_rows:,}",
                            flush=True,
                        )
                        _emit_load_progress(
                            processed_files=processed_files,
                            total_files=total_files,
                            processed_rows=processed_rows,
                            accepted_rows=accepted_rows,
                            label=str(path),
                            phase="loading local OpenAddresses files",
                            run_id=progress_run_id,
                        )
                finally:
                    queue.task_done()

        async with asyncio.TaskGroup() as task_group:
            for worker_id in range(1, local_concurrency + 1):
                task_group.create_task(local_worker(worker_id))
        return {
            "processed_files": processed_files,
            "processed_rows": processed_rows,
            "accepted_rows": accepted_rows,
            "zip_recovery_rows": zip_recovery_rows,
            "rejected_rows": max(processed_rows - accepted_rows - zip_recovery_rows, 0),
            "rejection_counts": rejection_counts,
            "zip_restore_shards": restore_shards,
            "zip_restore_concurrency": zip_restore_concurrency,
        }

    token = os.getenv("HLTHPRT_OPENADDRESSES_API_TOKEN", "").strip()
    if not token:
        raise RuntimeError("HLTHPRT_OPENADDRESSES_API_TOKEN is required for OpenAddresses downloads.")

    data_url = f"{OPENADDRESSES_API_BASE}/data?layer=addresses"
    async with aiohttp.ClientSession(timeout=timeout) as client:
        items = _us_data_items(await _fetch_json(client, data_url, token))
        if test_mode:
            items = items[:test_file_limit]
        elif max_files:
            items = items[:max_files]
        first_source_index = 1
        start_index_raw = task.get("start_index") or os.getenv("HLTHPRT_OPENADDRESSES_START_INDEX")
        if start_index_raw:
            try:
                start_index = max(int(start_index_raw), 1)
            except (TypeError, ValueError) as exc:
                raise RuntimeError(f"Invalid OpenAddresses start index: {start_index_raw!r}") from exc
            if start_index > len(items):
                raise RuntimeError(f"OpenAddresses start index {start_index:,} exceeds source count {len(items):,}")
            first_source_index = start_index
            items = items[start_index - 1:]
            print(
                f"OpenAddresses resume: starting at source index {start_index:,} "
                f"of {start_index + len(items) - 1:,}",
                flush=True,
            )
        end_index_raw = task.get("end_index") or os.getenv("HLTHPRT_OPENADDRESSES_END_INDEX")
        if end_index_raw:
            try:
                end_index = max(int(end_index_raw), 1)
            except (TypeError, ValueError) as exc:
                raise RuntimeError(f"Invalid OpenAddresses end index: {end_index_raw!r}") from exc
            if end_index < first_source_index:
                raise RuntimeError(
                    f"OpenAddresses end index {end_index:,} is before start index {first_source_index:,}"
                )
            item_count = end_index - first_source_index + 1
            items = items[:item_count]
            print(
                f"OpenAddresses shard: ending at source index {end_index:,} "
                f"({len(items):,} sources in shard)",
                flush=True,
            )
        start_source = str(task.get("start_source") or os.getenv("HLTHPRT_OPENADDRESSES_START_SOURCE") or "").strip()
        if start_source:
            for start_index, item in enumerate(items):
                if str(item.get("source") or "") == start_source:
                    items = items[start_index:]
                    first_source_index += start_index
                    print(
                        f"OpenAddresses resume: starting at source {start_source} "
                        f"({first_source_index:,}/{first_source_index + len(items) - 1:,})",
                        flush=True,
                    )
                    break
            else:
                raise RuntimeError(f"OpenAddresses start source not found: {start_source}")
        total_files = len(items)
        source_concurrency = min(max(source_concurrency, 1), max(total_files, 1))
        print(f"OpenAddresses source concurrency={source_concurrency}", flush=True)
        _emit_load_progress(
            processed_files=0,
            total_files=total_files,
            processed_rows=0,
            accepted_rows=0,
            run_id=progress_run_id,
        )

        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
            tmpdir_path = Path(tmpdir)
            progress_lock = asyncio.Lock()
            queue: asyncio.Queue[tuple[int, dict[str, Any]]] = asyncio.Queue()
            for source_index, item in enumerate(items, start=first_source_index):
                queue.put_nowait((source_index, item))

            async def worker(worker_id: int) -> None:
                nonlocal processed_files, processed_rows, accepted_rows, zip_recovery_rows, rejection_counts
                while True:
                    await _maybe_raise_if_cancelled(ctx, task)
                    try:
                        source_index, item = queue.get_nowait()
                    except asyncio.QueueEmpty:
                        return
                    try:
                        (
                            source,
                            file_processed,
                            file_accepted,
                            file_zip_recovery,
                            file_rejection_counts,
                        ) = await _load_source_item(
                            ctx=ctx,
                            task=task,
                            client=client,
                            item=item,
                            tmpdir_path=tmpdir_path,
                            token=token,
                            stage_cls=stage_cls,
                            recovery_cls=recovery_cls,
                            batch_size=batch_size,
                            test_mode=test_mode,
                            test_row_limit=test_row_limit,
                            restore_shards=restore_shards,
                        )
                        async with progress_lock:
                            await _maybe_raise_if_cancelled(ctx, task)
                            processed_files += 1
                            processed_rows += file_processed
                            accepted_rows += file_accepted
                            zip_recovery_rows += file_zip_recovery
                            for reason, count in file_rejection_counts.items():
                                rejection_counts[reason] = rejection_counts.get(reason, 0) + count
                            print(
                                "OpenAddresses source "
                                f"{processed_files:,}/{total_files:,} "
                                f"index={source_index:,} worker={worker_id} {source}: "
                                f"processed={file_processed:,} accepted={file_accepted:,} "
                                f"zip_recovery={file_zip_recovery:,} "
                                f"accepted_total={accepted_rows:,}",
                                flush=True,
                            )
                            _emit_load_progress(
                                processed_files=processed_files,
                                total_files=total_files,
                                processed_rows=processed_rows,
                                accepted_rows=accepted_rows,
                                label=source,
                                run_id=progress_run_id,
                            )
                    finally:
                        queue.task_done()

            async with asyncio.TaskGroup() as task_group:
                for worker_id in range(1, source_concurrency + 1):
                    task_group.create_task(worker(worker_id))

    return {
        "processed_files": processed_files,
        "processed_rows": processed_rows,
        "accepted_rows": accepted_rows,
        "zip_recovery_rows": zip_recovery_rows,
        "rejected_rows": max(processed_rows - accepted_rows - zip_recovery_rows, 0),
        "rejection_counts": rejection_counts,
        "zip_restore_shards": restore_shards,
        "zip_restore_concurrency": zip_restore_concurrency,
    }


async def _create_indexes(table_name: str, schema: str) -> None:
    table = _qtable(schema, table_name)
    for index in OpenAddressesGeocode.__my_additional_indexes__:
        index_name = index.get("name", "_".join(index.get("index_elements")))
        bounded_index_name = _bounded_identifier(f"{table_name}_idx_{index_name}")
        where = f" WHERE {index.get('where')}" if index.get("where") else ""
        await db.status(
            f"CREATE INDEX IF NOT EXISTS {_quote_ident(bounded_index_name)} "
            f"ON {table} ({', '.join(index.get('index_elements'))}){where};"
        )


def _zip_recovery_table_name(stage_table: str) -> str:
    prefix = f"{OPENADDRESSES_TABLE}_"
    suffix = stage_table[len(prefix):] if stage_table.startswith(prefix) else stage_table
    return _bounded_identifier(f"{OpenAddressesZipRecovery.__tablename__}_{suffix}")


async def _create_zip_recovery_indexes(table_name: str, schema: str) -> None:
    table = _qtable(schema, table_name)
    await db.status(
        f"CREATE INDEX IF NOT EXISTS {_quote_ident(_bounded_identifier(f'{table_name}_idx_restore_bucket'))} "
        f"ON {table} (restore_bucket);"
    )
    await db.status(
        f"CREATE INDEX IF NOT EXISTS {_quote_ident(_bounded_identifier(f'{table_name}_idx_state_bucket'))} "
        f"ON {table} (state_code, restore_bucket);"
    )


async def _ensure_openaddresses_stage_schema(table_name: str, schema: str) -> None:
    """Repair dynamic stage tables created by older importer code before resuming."""
    schema = _validate_schema_name(schema)
    table = _qtable(schema, table_name)
    row_hash = await db.first(
        """
        SELECT data_type, character_maximum_length
          FROM information_schema.columns
         WHERE table_schema = :schema
           AND table_name = :table_name
           AND column_name = 'row_hash';
        """,
        schema=schema,
        table_name=table_name,
    )
    if not row_hash:
        return
    row_hash_mapping = row_hash._mapping if hasattr(row_hash, "_mapping") else row_hash
    data_type = str(row_hash_mapping["data_type"] or "").lower()
    max_length = row_hash_mapping["character_maximum_length"]
    if data_type in {"character varying", "character"} and (max_length is None or int(max_length) < 64):
        await db.status(f"ALTER TABLE {table} ALTER COLUMN row_hash TYPE varchar(64);")
    await db.status(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS zip5_source text;")
    await db.status(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS zip5_restored_at timestamptz;")


async def _prepare_zip_recovery_table(recovery_cls, schema: str, *, reset: bool) -> None:
    await db.status(f"CREATE SCHEMA IF NOT EXISTS {_quote_ident(schema)};")
    exists = await _table_exists(schema, recovery_cls.__tablename__)
    if reset:
        await db.status(f"DROP TABLE IF EXISTS {_qtable(schema, recovery_cls.__tablename__)};")
        await db.create_table(recovery_cls.__table__, checkfirst=True)
    elif exists:
        print(f"OpenAddresses resume: preserving existing ZIP recovery table {schema}.{recovery_cls.__tablename__}", flush=True)
    else:
        await db.create_table(recovery_cls.__table__, checkfirst=True)


def _emit_zip_restore_progress(
    *,
    completed_shards: int,
    total_shards: int,
    candidates: int,
    restored: int,
    label: str | None = None,
    run_id: str | None = None,
) -> None:
    total = max(int(total_shards or 0), 1)
    done = max(int(completed_shards or 0), 0)
    pct = min((done / total) * 100.0, 100.0)
    message = (
        f"{done:,}/{total_shards:,} ZIP restore shards; "
        f"{restored:,}/{candidates:,} candidates restored"
    )
    payload = dict(
        importer="openaddresses",
        status="running",
        unit="shards",
        done=done,
        total=total_shards,
        pct=pct,
        phase="restoring missing OpenAddresses ZIPs from TIGER ZCTA",
        message=message,
        label=label,
        step=label,
        source="openaddresses-zip-restore-progress",
        confidence="live",
    )
    if run_id:
        payload["run_id"] = run_id
    enqueue_live_progress(**payload)


def _openaddresses_zip_restore_insert_sql(schema: str, stage_table: str, recovery_table: str) -> str:
    stage = _qtable(schema, stage_table)
    recovery = _qtable(schema, recovery_table)
    qschema = _quote_ident(schema)
    return f"""
        WITH shard AS (
            SELECT *
              FROM {recovery}
             WHERE restore_bucket = :restore_bucket
        ),
        matches AS (
            SELECT
                r.*,
                z.zcta5ce AS restored_zip5
              FROM shard AS r
              JOIN tiger.zcta5 AS z
                ON z.the_geom && ST_SetSRID(ST_Point(r.long::double precision, r.lat::double precision), 4269)
               AND ST_Covers(z.the_geom, ST_SetSRID(ST_Point(r.long::double precision, r.lat::double precision), 4269))
              JOIN {qschema}.geo_zip_lookup AS g
                ON g.zip_code = z.zcta5ce
               AND g.state = r.state_code
        ),
        winners AS (
            SELECT
                raw_hash,
                min(house_number) AS house_number,
                min(street_match_key) AS street_match_key,
                min(street_name) AS street_name,
                min(unit) AS unit,
                min(city_name) AS city_name,
                min(state_code) AS state_code,
                min(restored_zip5) AS zip5,
                min(lat) AS lat,
                min(long) AS long,
                min(source) AS source,
                min(data_id) AS data_id,
                min(job_id) AS job_id,
                min(feature_id) AS feature_id,
                min(accuracy) AS accuracy,
                min(source_updated) AS source_updated,
                min(imported_at) AS imported_at
              FROM matches
             GROUP BY raw_hash
            HAVING count(DISTINCT restored_zip5) = 1
        )
        INSERT INTO {stage} (
            row_hash,
            address_key,
            identity_key,
            house_number,
            street_match_key,
            street_name,
            unit,
            city_name,
            state_code,
            zip5,
            zip5_source,
            zip5_restored_at,
            formatted_address,
            lat,
            long,
            source,
            data_id,
            job_id,
            feature_id,
            accuracy,
            source_updated,
            imported_at
        )
        SELECT
            encode(sha256(convert_to(concat_ws('|',
                COALESCE(source, ''),
                COALESCE(feature_id, ''),
                house_number,
                street_match_key,
                COALESCE(city_name, ''),
                state_code,
                zip5,
                to_char(lat, 'FM999999990.00000000'),
                to_char(long, 'FM999999990.00000000')
            ), 'UTF8')), 'hex') AS row_hash,
            {qschema}.addr_key_v1(house_number || ' ' || street_name, unit, city_name, state_code, zip5, 'US') AS address_key,
            {qschema}.addr_identity_key_v1(house_number || ' ' || street_name, unit, city_name, state_code, zip5, 'US') AS identity_key,
            house_number,
            street_match_key,
            street_name,
            unit,
            city_name,
            state_code,
            zip5,
            'tiger_zcta_point' AS zip5_source,
            now() AS zip5_restored_at,
            concat_ws(', ',
                house_number || ' ' || street_name,
                NULLIF(unit, ''),
                NULLIF(city_name, ''),
                concat_ws(' ', state_code, zip5)
            ) AS formatted_address,
            lat,
            long,
            source,
            data_id,
            job_id,
            feature_id,
            accuracy,
            source_updated,
            imported_at
          FROM winners
        ON CONFLICT (row_hash) DO NOTHING;
    """


async def restore_openaddresses_zip5_from_tiger_zcta(
    *,
    schema: str,
    stage_table: str,
    recovery_table: str,
    concurrency: int,
    run_id: str | None = None,
) -> OpenAddressesZipRestoreStats:
    if not await _table_exists(schema, recovery_table):
        return OpenAddressesZipRestoreStats()
    if not await _table_exists("tiger", "zcta5"):
        if _env_bool("HLTHPRT_OPENADDRESSES_ZIP_RESTORE_REQUIRED"):
            raise RuntimeError("OpenAddresses ZIP restore requires tiger.zcta5, but it is missing.")
        logger.warning("Skipping OpenAddresses ZIP restore: tiger.zcta5 is missing")
        return OpenAddressesZipRestoreStats()
    if not await _table_exists(schema, "geo_zip_lookup"):
        if _env_bool("HLTHPRT_OPENADDRESSES_ZIP_RESTORE_REQUIRED"):
            raise RuntimeError("OpenAddresses ZIP restore requires geo_zip_lookup, but it is missing.")
        logger.warning("Skipping OpenAddresses ZIP restore: %s.geo_zip_lookup is missing", schema)
        return OpenAddressesZipRestoreStats()

    await _create_zip_recovery_indexes(recovery_table, schema)
    rows = await db.all(
        f"""
        SELECT restore_bucket, count(*)::bigint AS candidates
          FROM {_qtable(schema, recovery_table)}
         GROUP BY restore_bucket
         ORDER BY restore_bucket;
        """
    )
    shard_counts = {
        int(_row_mapping(row)["restore_bucket"]): int(_row_mapping(row)["candidates"] or 0)
        for row in rows or []
    }
    total_candidates = sum(shard_counts.values())
    if not total_candidates:
        return OpenAddressesZipRestoreStats()

    buckets = sorted(shard_counts)
    total_shards = len(buckets)
    restored = 0
    completed = 0
    concurrency = max(1, min(int(concurrency or 1), total_shards))
    semaphore = asyncio.Semaphore(concurrency)
    progress_lock = asyncio.Lock()
    insert_sql = _openaddresses_zip_restore_insert_sql(schema, stage_table, recovery_table)
    _emit_zip_restore_progress(
        completed_shards=0,
        total_shards=total_shards,
        candidates=total_candidates,
        restored=0,
        label="planned ZIP restore shards",
        run_id=run_id,
    )

    async def run_bucket(bucket: int) -> tuple[int, int]:
        async with semaphore:
            inserted = _status_count(await db.status(insert_sql, restore_bucket=bucket))
            return bucket, inserted

    tasks = [asyncio.create_task(run_bucket(bucket)) for bucket in buckets]
    try:
        for finished in asyncio.as_completed(tasks):
            bucket, inserted = await finished
            async with progress_lock:
                restored += inserted
                completed += 1
                _emit_zip_restore_progress(
                    completed_shards=completed,
                    total_shards=total_shards,
                    candidates=total_candidates,
                    restored=restored,
                    label=f"bucket {bucket}",
                    run_id=run_id,
                )
    except Exception:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise

    discarded = max(total_candidates - restored, 0)
    print(
        "OpenAddresses ZIP restore complete: "
        f"candidates={total_candidates:,} restored={restored:,} discarded={discarded:,} "
        f"shards={total_shards:,} concurrency={concurrency:,}",
        flush=True,
    )
    return OpenAddressesZipRestoreStats(
        candidates=total_candidates,
        restored=restored,
        discarded=discarded,
        shards=total_shards,
    )


async def _table_exists(schema: str, table_name: str) -> bool:
    schema = _validate_schema_name(schema)
    return bool(await db.scalar("SELECT to_regclass(:qualified_name) IS NOT NULL;", qualified_name=f"{schema}.{table_name}"))


async def _table_has_column(schema: str, table_name: str, column_name: str) -> bool:
    return bool(
        await db.scalar(
            """
            SELECT EXISTS (
                SELECT 1
                  FROM information_schema.columns
                 WHERE table_schema = :schema
                   AND table_name = :table_name
                   AND column_name = :column_name
            );
            """,
            schema=schema,
            table_name=table_name,
            column_name=column_name,
        )
    )


async def _prepare_stage_table(stage_cls, schema: str, *, reset: bool) -> None:
    await db.status(f"CREATE SCHEMA IF NOT EXISTS {_quote_ident(schema)};")
    exists = await _table_exists(schema, stage_cls.__tablename__)
    if reset:
        await db.status(f"DROP TABLE IF EXISTS {_qtable(schema, stage_cls.__tablename__)};")
        await db.create_table(stage_cls.__table__, checkfirst=True)
    elif exists:
        print(f"OpenAddresses resume: preserving existing stage table {schema}.{stage_cls.__tablename__}", flush=True)
    else:
        await db.create_table(stage_cls.__table__, checkfirst=True)
    await _ensure_openaddresses_stage_schema(stage_cls.__tablename__, schema)


def _backfill_shard_filter(*, state_code: str | None, zip_prefix: str | None) -> str:
    clauses = []
    if state_code:
        clauses.append("           AND state_code = :backfill_state_code")
    if zip_prefix:
        clauses.append("           AND zip5 >= :backfill_zip_lower")
        if _zip_prefix_upper_bound(zip_prefix):
            clauses.append("           AND zip5 < :backfill_zip_upper")
    return "\n".join(clauses)


def _normalize_backfill_state_code(value: Any) -> str | None:
    text = _safe_text(value)
    if not text:
        return None
    state = address_canon.state_code(text)
    if not state:
        raise ValueError(f"Invalid OpenAddresses backfill state code: {value!r}")
    return state


def _normalize_backfill_zip_prefix(value: Any) -> str | None:
    text = _safe_text(value)
    if not text:
        return None
    if not text.isdigit() or len(text) > 5:
        raise ValueError(f"Invalid OpenAddresses backfill ZIP prefix: {value!r}")
    return text


def _zip_prefix_lower_bound(zip_prefix: str) -> str:
    return zip_prefix.ljust(5, "0")


def _zip_prefix_upper_bound(zip_prefix: str) -> str | None:
    value = int(zip_prefix)
    width = len(zip_prefix)
    if value >= (10**width) - 1:
        return None
    return str(value + 1).zfill(width).ljust(5, "0")


def _row_mapping(row: Any) -> Any:
    return row._mapping if hasattr(row, "_mapping") else row


def _archive_match_components_cte(
    schema: str,
    archive_table: str,
    *,
    state_code: str | None = None,
    zip_prefix: str | None = None,
) -> str:
    qschema = _quote_ident(schema)
    archive = _qtable(schema, archive_table)
    shard_filter = _backfill_shard_filter(state_code=state_code, zip_prefix=zip_prefix)
    return f"""
        SELECT
            address_key,
            substring(first_line from '^\\s*([0-9]+[A-Za-z]?)') AS house_number,
            {qschema}.addr_street_norm_v1(
                regexp_replace(COALESCE(first_line, ''), '^\\s*[0-9]+[A-Za-z]?\\s*', ''),
                NULL
            ) AS street_match_key,
            COALESCE(NULLIF(city_norm, ''), NULLIF({qschema}.addr_city_norm_v1(city_name), '')) AS city_norm,
            state_code,
            zip5
          FROM {archive}
         WHERE lat IS NULL
           AND long IS NULL
           AND COALESCE(country_code, 'US') = 'US'
           AND precision = 'street'
           AND first_line IS NOT NULL
           AND state_code IS NOT NULL
           AND zip5 IS NOT NULL
{shard_filter}
    """


def _openaddresses_grouped_cte(
    schema: str,
    source_table: str,
    *,
    state_code: str | None = None,
    zip_prefix: str | None = None,
) -> str:
    table = _qtable(schema, source_table)
    shard_filter = _backfill_shard_filter(state_code=state_code, zip_prefix=zip_prefix)
    return f"""
        SELECT
            state_code,
            zip5,
            house_number,
            street_match_key,
            avg(lat)::numeric(11,8) AS lat,
            avg(long)::numeric(11,8) AS long,
            min(formatted_address) AS formatted_address,
            min(feature_id) AS place_id,
            count(*) AS row_count,
            NULLIF(max(NULLIF(accuracy, '')), '') AS accuracy
          FROM {table}
         WHERE lat IS NOT NULL
           AND long IS NOT NULL
           AND state_code IS NOT NULL
           AND zip5 IS NOT NULL
           AND house_number IS NOT NULL
           AND street_match_key IS NOT NULL
{shard_filter}
         GROUP BY state_code, zip5, house_number, street_match_key
        HAVING count(*) = 1
            OR (
                max(lat) - min(lat) <= :coord_tolerance
                AND max(long) - min(long) <= :coord_tolerance
            )
    """


def _openaddresses_city_grouped_cte(
    schema: str,
    source_table: str,
    *,
    state_code: str | None = None,
    zip_prefix: str | None = None,
) -> str:
    qschema = _quote_ident(schema)
    table = _qtable(schema, source_table)
    shard_filter = _backfill_shard_filter(state_code=state_code, zip_prefix=zip_prefix)
    return f"""
        SELECT
            state_code,
            zip5,
            house_number,
            city_norm,
            street_match_key,
            avg(lat)::numeric(11,8) AS lat,
            avg(long)::numeric(11,8) AS long,
            min(formatted_address) AS formatted_address,
            min(feature_id) AS place_id,
            count(*) AS row_count,
            NULLIF(max(NULLIF(accuracy, '')), '') AS accuracy
          FROM (
              SELECT
                  state_code,
                  zip5,
                  house_number,
                  NULLIF({qschema}.addr_city_norm_v1(city_name), '') AS city_norm,
                  street_match_key,
                  lat,
                  long,
                  formatted_address,
                  feature_id,
                  accuracy
                FROM {table}
               WHERE lat IS NOT NULL
                 AND long IS NOT NULL
                 AND state_code IS NOT NULL
                 AND zip5 IS NOT NULL
                 AND house_number IS NOT NULL
                 AND street_match_key IS NOT NULL
{shard_filter}
          ) AS normalized
         WHERE city_norm IS NOT NULL
         GROUP BY state_code, zip5, house_number, city_norm, street_match_key
        HAVING count(*) = 1
            OR (
                max(lat) - min(lat) <= :coord_tolerance
                AND max(long) - min(long) <= :coord_tolerance
            )
    """


async def _plan_openaddresses_backfill_shards(
    *,
    schema: str,
    archive_table: str,
    state_code: str | None = None,
    zip_prefix: str | None = None,
    zip_prefix_length: int = DEFAULT_BACKFILL_ZIP_PREFIX_LENGTH,
) -> list[OpenAddressesBackfillShard]:
    if zip_prefix:
        return [OpenAddressesBackfillShard(state_code=state_code, zip_prefix=zip_prefix)]

    archive = _qtable(schema, archive_table)
    shard_filter = _backfill_shard_filter(state_code=state_code, zip_prefix=None)
    rows = await db.all(
        f"""
        SELECT
            state_code,
            substring(zip5 from 1 for :backfill_zip_prefix_length) AS zip_prefix,
            count(*)::bigint AS candidate_count
          FROM {archive}
         WHERE lat IS NULL
           AND long IS NULL
           AND COALESCE(country_code, 'US') = 'US'
           AND precision = 'street'
           AND first_line IS NOT NULL
           AND state_code IS NOT NULL
           AND zip5 IS NOT NULL
           AND zip5 ~ '^[0-9]{{5}}$'
{shard_filter}
         GROUP BY state_code, substring(zip5 from 1 for :backfill_zip_prefix_length)
         ORDER BY candidate_count DESC, state_code, zip_prefix;
        """,
        backfill_state_code=state_code,
        backfill_zip_prefix_length=zip_prefix_length,
    )
    shards: list[OpenAddressesBackfillShard] = []
    for row in rows or []:
        mapping = _row_mapping(row)
        shard_state = _normalize_backfill_state_code(mapping["state_code"])
        shard_zip = _normalize_backfill_zip_prefix(mapping["zip_prefix"])
        candidate_count = int(mapping["candidate_count"] or 0)
        if shard_state and shard_zip:
            shards.append(
                OpenAddressesBackfillShard(
                    state_code=shard_state,
                    zip_prefix=shard_zip,
                    candidate_count=candidate_count,
                )
            )
    if shards:
        return shards
    return []


def _add_backfill_stats(
    left: OpenAddressesBackfillStats,
    right: OpenAddressesBackfillStats,
) -> OpenAddressesBackfillStats:
    return OpenAddressesBackfillStats(
        exact_updates=left.exact_updates + right.exact_updates,
        fuzzy_updates=left.fuzzy_updates + right.fuzzy_updates,
        relaxed_updates=left.relaxed_updates + right.relaxed_updates,
    )


async def refresh_archive_geocodes_from_openaddresses_sharded(
    *,
    schema: str | None = None,
    archive_table: str | None = None,
    source_table: str = OPENADDRESSES_TABLE,
    state_code: str | None = None,
    zip_prefix: str | None = None,
    concurrency: int | None = None,
    zip_prefix_length: int | None = None,
    run_id: str | None = None,
    sharded: bool = True,
) -> OpenAddressesBackfillStats:
    schema = _validate_schema_name(schema or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf")
    archive_table = archive_table or os.getenv("HLTHPRT_ADDRESS_ARCHIVE_TABLE", "address_archive_v2").strip() or "address_archive_v2"
    state_code = _normalize_backfill_state_code(
        state_code if state_code is not None else os.getenv("HLTHPRT_OPENADDRESSES_BACKFILL_STATE_CODE")
    )
    zip_prefix = _normalize_backfill_zip_prefix(
        zip_prefix if zip_prefix is not None else os.getenv("HLTHPRT_OPENADDRESSES_BACKFILL_ZIP_PREFIX")
    )
    if not sharded:
        return await refresh_archive_geocodes_from_openaddresses(
            schema=schema,
            archive_table=archive_table,
            source_table=source_table,
            state_code=state_code,
            zip_prefix=zip_prefix,
        )
    if zip_prefix:
        shard = OpenAddressesBackfillShard(state_code=state_code, zip_prefix=zip_prefix)
        empty = OpenAddressesBackfillStats(exact_updates=0, fuzzy_updates=0, relaxed_updates=0)
        _emit_backfill_progress(
            completed_shards=0,
            total_shards=1,
            stats=empty,
            label=shard.label,
            run_id=run_id,
        )
        stats = await refresh_archive_geocodes_from_openaddresses(
            schema=schema,
            archive_table=archive_table,
            source_table=source_table,
            state_code=state_code,
            zip_prefix=zip_prefix,
        )
        _emit_backfill_progress(
            completed_shards=1,
            total_shards=1,
            stats=stats,
            label=shard.label,
            run_id=run_id,
        )
        return stats

    if not await _table_exists(schema, source_table):
        return await refresh_archive_geocodes_from_openaddresses(
            schema=schema,
            archive_table=archive_table,
            source_table=source_table,
            state_code=state_code,
            zip_prefix=zip_prefix,
        )
    if not (
        await _table_has_column(schema, archive_table, "address_key")
        and await _table_has_column(schema, archive_table, "geo_source")
    ):
        return await refresh_archive_geocodes_from_openaddresses(
            schema=schema,
            archive_table=archive_table,
            source_table=source_table,
            state_code=state_code,
            zip_prefix=zip_prefix,
        )

    concurrency = max(int(concurrency or _env_positive_int("HLTHPRT_OPENADDRESSES_BACKFILL_CONCURRENCY", DEFAULT_BACKFILL_CONCURRENCY)), 1)
    zip_prefix_length = min(
        max(
            int(
                zip_prefix_length
                or _env_positive_int(
                    "HLTHPRT_OPENADDRESSES_BACKFILL_ZIP_PREFIX_LENGTH",
                    DEFAULT_BACKFILL_ZIP_PREFIX_LENGTH,
                )
            ),
            1,
        ),
        5,
    )
    shards = await _plan_openaddresses_backfill_shards(
        schema=schema,
        archive_table=archive_table,
        state_code=state_code,
        zip_prefix=zip_prefix,
        zip_prefix_length=zip_prefix_length,
    )
    if not shards:
        stats = OpenAddressesBackfillStats(exact_updates=0, fuzzy_updates=0, relaxed_updates=0)
        _emit_backfill_progress(
            completed_shards=1,
            total_shards=1,
            stats=stats,
            label="no candidates",
            run_id=run_id,
        )
        return stats

    total_shards = len(shards)
    total_candidates = sum(shard.candidate_count for shard in shards)
    aggregate = OpenAddressesBackfillStats(exact_updates=0, fuzzy_updates=0, relaxed_updates=0)
    _emit_backfill_progress(
        completed_shards=0,
        total_shards=total_shards,
        stats=aggregate,
        label="planned shards",
        total_candidates=total_candidates,
        run_id=run_id,
    )
    print(
        "OpenAddresses archive backfill shards: "
        f"shards={total_shards:,} concurrency={min(concurrency, total_shards):,} "
        f"zip_prefix_length={zip_prefix_length} candidates={total_candidates:,}",
        flush=True,
    )

    semaphore = asyncio.Semaphore(min(concurrency, total_shards))

    async def run_shard(index: int, shard: OpenAddressesBackfillShard):
        async with semaphore:
            stats = await refresh_archive_geocodes_from_openaddresses(
                schema=schema,
                archive_table=archive_table,
                source_table=source_table,
                state_code=shard.state_code,
                zip_prefix=shard.zip_prefix,
            )
            return index, shard, stats

    tasks = [asyncio.create_task(run_shard(index, shard)) for index, shard in enumerate(shards, start=1)]
    completed = 0
    try:
        for finished in asyncio.as_completed(tasks):
            _index, shard, stats = await finished
            completed += 1
            aggregate = _add_backfill_stats(aggregate, stats)
            _emit_backfill_progress(
                completed_shards=completed,
                total_shards=total_shards,
                stats=aggregate,
                label=shard.label,
                total_candidates=total_candidates,
                run_id=run_id,
            )
            print(
                "OpenAddresses archive backfill shard complete: "
                f"{completed:,}/{total_shards:,} {shard.label} "
                f"exact={stats.exact_updates:,} fuzzy={stats.fuzzy_updates:,} "
                f"relaxed={stats.relaxed_updates:,}",
                flush=True,
            )
    except Exception:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise

    return aggregate


async def refresh_archive_geocodes_from_openaddresses(
    *,
    schema: str | None = None,
    archive_table: str | None = None,
    source_table: str = OPENADDRESSES_TABLE,
    state_code: str | None = None,
    zip_prefix: str | None = None,
) -> OpenAddressesBackfillStats:
    schema = _validate_schema_name(schema or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf")
    archive_table = archive_table or os.getenv("HLTHPRT_ADDRESS_ARCHIVE_TABLE", "address_archive_v2").strip() or "address_archive_v2"
    state_code = _normalize_backfill_state_code(
        state_code if state_code is not None else os.getenv("HLTHPRT_OPENADDRESSES_BACKFILL_STATE_CODE")
    )
    zip_prefix = _normalize_backfill_zip_prefix(
        zip_prefix if zip_prefix is not None else os.getenv("HLTHPRT_OPENADDRESSES_BACKFILL_ZIP_PREFIX")
    )
    if not await _table_exists(schema, source_table):
        logger.warning("Skipping OpenAddresses backfill: %s.%s is missing", schema, source_table)
        return OpenAddressesBackfillStats(exact_updates=0, fuzzy_updates=0, relaxed_updates=0)
    if not (
        await _table_has_column(schema, archive_table, "address_key")
        and await _table_has_column(schema, archive_table, "geo_source")
    ):
        logger.warning("Skipping OpenAddresses backfill: canonical archive table is unavailable.")
        return OpenAddressesBackfillStats(exact_updates=0, fuzzy_updates=0, relaxed_updates=0)

    await db.status("CREATE EXTENSION IF NOT EXISTS pg_trgm;")

    archive = _qtable(schema, archive_table)
    archive_components = _archive_match_components_cte(
        schema,
        archive_table,
        state_code=state_code,
        zip_prefix=zip_prefix,
    )
    source = _qtable(schema, source_table)
    qschema = _quote_ident(schema)
    params = {
        "coord_tolerance": _env_float(
            "HLTHPRT_OPENADDRESSES_DUPLICATE_COORD_TOLERANCE",
            DEFAULT_DUPLICATE_COORD_TOLERANCE,
        ),
        "fuzzy_threshold": _env_float("HLTHPRT_OPENADDRESSES_FUZZY_THRESHOLD", DEFAULT_FUZZY_THRESHOLD),
        "fuzzy_margin": _env_float("HLTHPRT_OPENADDRESSES_FUZZY_MARGIN", DEFAULT_FUZZY_MARGIN),
        "relaxed_threshold": _env_float(
            "HLTHPRT_OPENADDRESSES_RELAXED_STREET_THRESHOLD",
            DEFAULT_RELAXED_STREET_THRESHOLD,
        ),
        "relaxed_margin": _env_float("HLTHPRT_OPENADDRESSES_RELAXED_MARGIN", DEFAULT_RELAXED_MARGIN),
    }
    if state_code:
        params["backfill_state_code"] = state_code
    if zip_prefix:
        params["backfill_zip_lower"] = _zip_prefix_lower_bound(zip_prefix)
        zip_upper = _zip_prefix_upper_bound(zip_prefix)
        if zip_upper:
            params["backfill_zip_upper"] = zip_upper

    city_exact_updates = _status_count(
        await db.status(
            f"""
            WITH missing AS ({archive_components}),
            winners AS (
                SELECT
                    missing.address_key,
                    avg(oa.lat)::numeric(11,8) AS lat,
                    avg(oa.long)::numeric(11,8) AS long,
                    min(oa.formatted_address) AS formatted_address,
                    min(oa.feature_id) AS place_id,
                    NULLIF(max(NULLIF(oa.accuracy, '')), '') AS accuracy
                  FROM missing
                  JOIN {source} AS oa
                    ON oa.state_code = missing.state_code
                   AND oa.zip5 = missing.zip5
                   AND oa.house_number = missing.house_number
                   AND oa.street_match_key = missing.street_match_key
                   AND NULLIF({qschema}.addr_city_norm_v1(oa.city_name), '') = missing.city_norm
                 WHERE missing.house_number IS NOT NULL
                   AND missing.street_match_key IS NOT NULL
                   AND missing.city_norm IS NOT NULL
                   AND oa.lat IS NOT NULL
                   AND oa.long IS NOT NULL
                   AND oa.state_code IS NOT NULL
                   AND oa.zip5 IS NOT NULL
                   AND oa.house_number IS NOT NULL
                   AND oa.street_match_key IS NOT NULL
                 GROUP BY missing.address_key
                HAVING count(*) = 1
                    OR (
                        max(oa.lat) - min(oa.lat) <= :coord_tolerance
                        AND max(oa.long) - min(oa.long) <= :coord_tolerance
                    )
            )
            UPDATE {archive} AS archive
               SET lat = winners.lat,
                   long = winners.long,
                   formatted_address = COALESCE(archive.formatted_address, winners.formatted_address),
                   place_id = COALESCE(archive.place_id, winners.place_id),
                   geo_source = 'openaddresses'::{_quote_ident(schema)}.address_archive_geo_source,
                   geocode_source = 'openaddresses_exact_city',
                   geocode_quality = COALESCE(NULLIF(winners.accuracy, ''), 'city_exact'),
                   geocoded_at = now()
              FROM winners
             WHERE archive.address_key = winners.address_key
               AND archive.lat IS NULL
               AND archive.long IS NULL;
            """,
            **params,
        )
    )

    broad_exact_updates = _status_count(
        await db.status(
            f"""
            WITH missing AS ({archive_components}),
            winners AS (
                SELECT
                    missing.address_key,
                    avg(oa.lat)::numeric(11,8) AS lat,
                    avg(oa.long)::numeric(11,8) AS long,
                    min(oa.formatted_address) AS formatted_address,
                    min(oa.feature_id) AS place_id,
                    NULLIF(max(NULLIF(oa.accuracy, '')), '') AS accuracy
                  FROM missing
                  JOIN {source} AS oa
                    ON oa.state_code = missing.state_code
                   AND oa.zip5 = missing.zip5
                   AND oa.house_number = missing.house_number
                   AND oa.street_match_key = missing.street_match_key
                 WHERE missing.house_number IS NOT NULL
                   AND missing.street_match_key IS NOT NULL
                   AND oa.lat IS NOT NULL
                   AND oa.long IS NOT NULL
                   AND oa.state_code IS NOT NULL
                   AND oa.zip5 IS NOT NULL
                   AND oa.house_number IS NOT NULL
                   AND oa.street_match_key IS NOT NULL
                 GROUP BY missing.address_key
                HAVING count(*) = 1
                    OR (
                        max(oa.lat) - min(oa.lat) <= :coord_tolerance
                        AND max(oa.long) - min(oa.long) <= :coord_tolerance
                    )
            )
            UPDATE {archive} AS archive
               SET lat = winners.lat,
                   long = winners.long,
                   formatted_address = COALESCE(archive.formatted_address, winners.formatted_address),
                   place_id = COALESCE(archive.place_id, winners.place_id),
                   geo_source = 'openaddresses'::{_quote_ident(schema)}.address_archive_geo_source,
                   geocode_source = 'openaddresses_exact',
                   geocode_quality = COALESCE(NULLIF(winners.accuracy, ''), 'unknown'),
                   geocoded_at = now()
              FROM winners
             WHERE archive.address_key = winners.address_key
               AND archive.lat IS NULL
               AND archive.long IS NULL;
            """,
            **params,
        )
    )
    exact_updates = city_exact_updates + broad_exact_updates

    fuzzy_updates = _status_count(
        await db.status(
            f"""
            WITH missing AS ({archive_components}),
            scored AS (
                SELECT
                    missing.address_key,
                    avg(oa.lat)::numeric(11,8) AS lat,
                    avg(oa.long)::numeric(11,8) AS long,
                    min(oa.formatted_address) AS formatted_address,
                    min(oa.feature_id) AS place_id,
                    NULLIF(max(NULLIF(oa.accuracy, '')), '') AS accuracy,
                    count(*) AS row_count,
                    similarity(oa.street_match_key, missing.street_match_key) AS score
                  FROM missing
                  JOIN {source} AS oa
                    ON oa.state_code = missing.state_code
                   AND oa.zip5 = missing.zip5
                   AND oa.house_number = missing.house_number
                 WHERE missing.house_number IS NOT NULL
                   AND missing.street_match_key IS NOT NULL
                   AND oa.lat IS NOT NULL
                   AND oa.long IS NOT NULL
                   AND oa.state_code IS NOT NULL
                   AND oa.zip5 IS NOT NULL
                   AND oa.house_number IS NOT NULL
                   AND oa.street_match_key IS NOT NULL
                   AND oa.street_match_key <> missing.street_match_key
                   AND similarity(oa.street_match_key, missing.street_match_key) >= :fuzzy_threshold
                 GROUP BY missing.address_key, missing.street_match_key, oa.street_match_key
                HAVING count(*) = 1
                    OR (
                        max(oa.lat) - min(oa.lat) <= :coord_tolerance
                        AND max(oa.long) - min(oa.long) <= :coord_tolerance
                    )
            ),
            ranked AS (
                SELECT
                    *,
                    row_number() OVER (
                        PARTITION BY address_key
                        ORDER BY score DESC, row_count ASC, formatted_address
                    ) AS rn,
                    lead(score) OVER (
                        PARTITION BY address_key
                        ORDER BY score DESC, row_count ASC, formatted_address
                    ) AS next_score
                  FROM scored
            ),
            winners AS (
                SELECT *
                  FROM ranked
                 WHERE rn = 1
                   AND (next_score IS NULL OR score - next_score >= :fuzzy_margin)
            )
            UPDATE {archive} AS archive
               SET lat = winners.lat,
                   long = winners.long,
                   formatted_address = COALESCE(archive.formatted_address, winners.formatted_address),
                   place_id = COALESCE(archive.place_id, winners.place_id),
                   geo_source = 'openaddresses'::{_quote_ident(schema)}.address_archive_geo_source,
                   geocode_source = 'openaddresses_fuzzy_zip',
                   geocode_quality = COALESCE(NULLIF(winners.accuracy, ''), 'strict_fuzzy'),
                   geocoded_at = now()
              FROM winners
             WHERE archive.address_key = winners.address_key
               AND archive.lat IS NULL
               AND archive.long IS NULL;
            """,
            **params,
        )
    )

    relaxed_updates = _status_count(
        await db.status(
            f"""
            WITH missing AS ({archive_components}),
            scored AS (
                SELECT
                    missing.address_key,
                    avg(oa.lat)::numeric(11,8) AS lat,
                    avg(oa.long)::numeric(11,8) AS long,
                    min(oa.formatted_address) AS formatted_address,
                    min(oa.feature_id) AS place_id,
                    NULLIF(max(NULLIF(oa.accuracy, '')), '') AS accuracy,
                    count(*) AS row_count,
                    similarity(oa.street_match_key, missing.street_match_key) AS score
                  FROM missing
                  JOIN {source} AS oa
                    ON oa.state_code = missing.state_code
                   AND oa.zip5 = missing.zip5
                   AND oa.house_number = missing.house_number
                   AND NULLIF({qschema}.addr_city_norm_v1(oa.city_name), '') = missing.city_norm
                 WHERE missing.house_number IS NOT NULL
                   AND missing.street_match_key IS NOT NULL
                   AND missing.city_norm IS NOT NULL
                   AND oa.lat IS NOT NULL
                   AND oa.long IS NOT NULL
                   AND oa.state_code IS NOT NULL
                   AND oa.zip5 IS NOT NULL
                   AND oa.house_number IS NOT NULL
                   AND oa.street_match_key IS NOT NULL
                   AND oa.street_match_key <> missing.street_match_key
                   AND similarity(oa.street_match_key, missing.street_match_key) >= :relaxed_threshold
                 GROUP BY missing.address_key, missing.street_match_key, oa.street_match_key
                HAVING count(*) = 1
                    OR (
                        max(oa.lat) - min(oa.lat) <= :coord_tolerance
                        AND max(oa.long) - min(oa.long) <= :coord_tolerance
                    )
            ),
            ranked AS (
                SELECT
                    *,
                    row_number() OVER (
                        PARTITION BY address_key
                        ORDER BY score DESC, row_count ASC, formatted_address
                    ) AS rn,
                    lead(score) OVER (
                        PARTITION BY address_key
                        ORDER BY score DESC, row_count ASC, formatted_address
                    ) AS next_score
                  FROM scored
            ),
            winners AS (
                SELECT *
                  FROM ranked
                 WHERE rn = 1
                   AND (next_score IS NULL OR score - next_score >= :relaxed_margin)
            )
            UPDATE {archive} AS archive
               SET lat = winners.lat,
                   long = winners.long,
                   formatted_address = COALESCE(archive.formatted_address, winners.formatted_address),
                   place_id = COALESCE(archive.place_id, winners.place_id),
                   geo_source = 'openaddresses'::{_quote_ident(schema)}.address_archive_geo_source,
                   geocode_source = 'openaddresses_relaxed_city_zip',
                   geocode_quality = COALESCE(NULLIF(winners.accuracy, ''), 'relaxed_city_zip'),
                   geocoded_at = now()
              FROM winners
             WHERE archive.address_key = winners.address_key
               AND archive.lat IS NULL
               AND archive.long IS NULL;
            """,
            **params,
        )
    )
    return OpenAddressesBackfillStats(
        exact_updates=exact_updates,
        fuzzy_updates=fuzzy_updates,
        relaxed_updates=relaxed_updates,
    )


async def process_data(ctx, task=None):  # pragma: no cover
    task = task or {}
    await _maybe_raise_if_cancelled(ctx, task)
    ctx.setdefault("context", {})
    if "test_mode" in task:
        ctx["context"]["test_mode"] = bool(task.get("test_mode"))
    task_import_id = task.get("import_id") or task.get("stage_suffix")
    if task_import_id:
        ctx["import_date"] = _normalize_import_id(task_import_id)
    ctx["context"]["import_date"] = ctx["import_date"]
    if _task_or_env_bool(task, "publish_only", "HLTHPRT_OPENADDRESSES_PUBLISH_ONLY"):
        ctx["context"]["run"] = ctx["context"].get("run", 0) + 1
        ctx["context"]["publish_only"] = True
        print("OpenAddresses publish-only mode: using existing stage table", flush=True)
        return
    if task.get("backfill_only"):
        await ensure_database(bool(ctx["context"].get("test_mode", False)))
        backfill_state_code = task.get("state_code") or task.get("backfill_state_code")
        backfill_zip_prefix = task.get("zip_prefix") or task.get("backfill_zip_prefix")
        backfill_concurrency = _task_or_env_positive_int(
            task,
            "backfill_concurrency",
            "HLTHPRT_OPENADDRESSES_BACKFILL_CONCURRENCY",
            DEFAULT_BACKFILL_CONCURRENCY,
        )
        backfill_zip_prefix_length = _task_or_env_int_range(
            task,
            "backfill_zip_prefix_length",
            "HLTHPRT_OPENADDRESSES_BACKFILL_ZIP_PREFIX_LENGTH",
            DEFAULT_BACKFILL_ZIP_PREFIX_LENGTH,
            minimum=1,
            maximum=5,
        )
        stats = await refresh_archive_geocodes_from_openaddresses_sharded(
            state_code=backfill_state_code,
            zip_prefix=backfill_zip_prefix,
            concurrency=backfill_concurrency,
            zip_prefix_length=backfill_zip_prefix_length,
            run_id=_progress_run_id(ctx, task),
            sharded=_task_or_env_bool(task, "sharded_backfill", "HLTHPRT_OPENADDRESSES_SHARDED_BACKFILL", True),
        )
        ctx["context"]["backfill"] = {
            "exact_updates": stats.exact_updates,
            "fuzzy_updates": stats.fuzzy_updates,
            "relaxed_updates": stats.relaxed_updates,
            "state_code": _normalize_backfill_state_code(backfill_state_code),
            "zip_prefix": _normalize_backfill_zip_prefix(backfill_zip_prefix),
            "concurrency": backfill_concurrency,
            "zip_prefix_length": backfill_zip_prefix_length,
        }
        ctx["context"]["run"] = ctx["context"].get("run", 0) + 1
        ctx["context"]["backfill_only"] = True
        shard = []
        if ctx["context"]["backfill"]["state_code"]:
            shard.append(f"state={ctx['context']['backfill']['state_code']}")
        if ctx["context"]["backfill"]["zip_prefix"]:
            shard.append(f"zip_prefix={ctx['context']['backfill']['zip_prefix']}")
        print(
            "OpenAddresses backfill complete: "
            f"exact={stats.exact_updates:,} fuzzy={stats.fuzzy_updates:,} "
            f"relaxed={stats.relaxed_updates:,}"
            f"{' ' + ' '.join(shard) if shard else ''}"
        )
        return

    await ensure_database(bool(ctx["context"].get("test_mode", False)))
    if _task_or_env_bool(task, "load_only", "HLTHPRT_OPENADDRESSES_LOAD_ONLY"):
        ctx["context"]["load_only"] = True
    if "min_rows" in task:
        ctx["context"]["min_rows"] = _positive_int_value(task.get("min_rows"), DEFAULT_MIN_ROWS)
    import_date = ctx["context"].get("import_date") or ctx["import_date"]
    stage_cls = make_class(OpenAddressesGeocode, import_date)
    recovery_cls = make_class(OpenAddressesZipRecovery, import_date)
    schema = _validate_schema_name(os.getenv("HLTHPRT_DB_SCHEMA") or "mrf")
    await _prepare_stage_table(
        stage_cls,
        schema,
        reset=not _task_or_env_bool(task, "resume_stage", "HLTHPRT_OPENADDRESSES_RESUME_STAGE"),
    )
    await _prepare_zip_recovery_table(
        recovery_cls,
        schema,
        reset=not _task_or_env_bool(task, "resume_stage", "HLTHPRT_OPENADDRESSES_RESUME_STAGE"),
    )
    stats = await _load_openaddresses_data(ctx, task, stage_cls, recovery_cls)
    ctx["context"]["audit"] = stats
    ctx["context"]["run"] = ctx["context"].get("run", 0) + 1
    print(
        "OpenAddresses load complete: "
        f"files={stats['processed_files']:,} processed={stats['processed_rows']:,} "
        f"accepted={stats['accepted_rows']:,}"
        f"{' load_only=true' if ctx['context'].get('load_only') else ''}"
    )


async def startup(ctx):  # pragma: no cover
    await my_init_db(db)
    ctx["context"] = {}
    ctx["context"]["start"] = datetime.datetime.utcnow()
    ctx["context"]["run"] = 0
    ctx["context"]["test_mode"] = False
    await ensure_database(False)

    override_import_id = os.getenv("HLTHPRT_IMPORT_ID_OVERRIDE")
    ctx["import_date"] = _normalize_import_id(override_import_id)
    schema = _validate_schema_name(os.getenv("HLTHPRT_DB_SCHEMA") or "mrf")

    await db.status("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
    await db.status(f"CREATE SCHEMA IF NOT EXISTS {_quote_ident(schema)};")


async def shutdown(ctx):  # pragma: no cover
    context = ctx.get("context") or {}
    if not context.get("run") or context.get("backfill_only"):
        return
    if context.get("load_only"):
        print("OpenAddresses load-only mode: stage table preserved without publish/backfill", flush=True)
        return

    await ensure_database(bool(context.get("test_mode")))
    schema = _validate_schema_name(os.getenv("HLTHPRT_DB_SCHEMA") or "mrf")
    import_date = context.get("import_date") or ctx["import_date"]
    stage_cls = make_class(OpenAddressesGeocode, import_date)
    recovery_cls = make_class(OpenAddressesZipRecovery, import_date)
    stage_table = stage_cls.__tablename__
    recovery_table = recovery_cls.__tablename__
    if not await _table_exists(schema, stage_table):
        raise RuntimeError(f"OpenAddresses staging table {schema}.{stage_table} is missing.")

    zip_restore_stats = await restore_openaddresses_zip5_from_tiger_zcta(
        schema=schema,
        stage_table=stage_table,
        recovery_table=recovery_table,
        concurrency=int(
            context.get("zip_restore_concurrency")
            or _env_positive_int(
                "HLTHPRT_OPENADDRESSES_ZIP_RESTORE_CONCURRENCY",
                DEFAULT_ZIP_RESTORE_CONCURRENCY,
            )
        ),
        run_id=_progress_run_id(ctx, {}),
    )

    stage_rows = int(
        await db.scalar(f"SELECT COUNT(*) FROM {_qtable(schema, stage_table)};")
        or 0
    )
    min_rows = int(context.get("min_rows") or _env_positive_int("HLTHPRT_OPENADDRESSES_MIN_ROWS", DEFAULT_MIN_ROWS))
    if context.get("test_mode"):
        print(f"OpenAddresses test mode: staged rows={stage_rows:,}")
    elif stage_rows < min_rows:
        raise RuntimeError(
            f"OpenAddresses stage row count {stage_rows:,} is below minimum {min_rows:,}; aborting publish."
        )
    null_zip_rows = int(
        await db.scalar(f"SELECT COUNT(*) FROM {_qtable(schema, stage_table)} WHERE zip5 IS NULL;")
        or 0
    )
    if null_zip_rows:
        raise RuntimeError(f"OpenAddresses stage contains {null_zip_rows:,} rows with null zip5; aborting publish.")

    await _create_indexes(stage_table, schema)
    await db.execute_ddl(f"ANALYZE {_qtable(schema, stage_table)};")

    async with db.transaction():
        live_table = OpenAddressesGeocode.__main_table__
        archived = _archived_identifier(live_table)
        await db.status(f"DROP TABLE IF EXISTS {_qtable(schema, archived)};")
        await db.status(f"ALTER TABLE IF EXISTS {_qtable(schema, live_table)} RENAME TO {_quote_ident(archived)};")
        await db.status(f"ALTER TABLE {_qtable(schema, stage_table)} RENAME TO {_quote_ident(live_table)};")

    stats = await refresh_archive_geocodes_from_openaddresses_sharded(
        schema=schema,
        run_id=_progress_run_id(ctx, {}),
    )
    context["backfill"] = {
        "exact_updates": stats.exact_updates,
        "fuzzy_updates": stats.fuzzy_updates,
        "relaxed_updates": stats.relaxed_updates,
    }
    context["zip_restore"] = {
        "candidates": zip_restore_stats.candidates,
        "restored": zip_restore_stats.restored,
        "discarded": zip_restore_stats.discarded,
        "shards": zip_restore_stats.shards,
    }
    await db.status(f"DROP TABLE IF EXISTS {_qtable(schema, recovery_table)};")
    print(
        "OpenAddresses publish/backfill complete: "
        f"rows={stage_rows:,} exact={stats.exact_updates:,} fuzzy={stats.fuzzy_updates:,} "
        f"relaxed={stats.relaxed_updates:,} zip_restored={zip_restore_stats.restored:,}"
    )
    print_time_info(context.get("start"))


async def main(
    test_mode: bool = False,
    backfill_only: bool = False,
    **params: Any,
):  # pragma: no cover
    redis = await create_pool(
        build_redis_settings(),
        job_serializer=serialize_job,
        job_deserializer=deserialize_job,
    )
    payload = {"test_mode": bool(test_mode), "backfill_only": bool(backfill_only)}
    payload.update(
        {
            key: value
            for key, value in params.items()
            if value is not None and value is not False and value != "" and value != () and value != []
        }
    )
    await redis.enqueue_job("process_data", payload, _queue_name=OPENADDRESSES_QUEUE_NAME)


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main(bool("--test" in os.sys.argv), bool("--backfill-only" in os.sys.argv)))
