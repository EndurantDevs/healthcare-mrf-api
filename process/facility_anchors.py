# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import csv
import datetime
import hashlib
import logging
import os
import re
import uuid

from arq import create_pool
from sqlalchemy import and_, select

from db.models import FacilityAnchor, GeoZipLookup, db
from process.ext.utils import (ensure_database, make_class, my_init_db,
                               print_time_info, push_objects)
from process.redis_config import build_redis_settings
from process.serialization import deserialize_job, serialize_job

logger = logging.getLogger(__name__)

FACILITY_ANCHORS_QUEUE_NAME = "arq:FacilityAnchors"
POSTGRES_IDENTIFIER_MAX_LENGTH = 63

HRSA_FQHC_URL = (
    "https://data.hrsa.gov/DataDownload/DD_Files/"
    "Health_Center_Service_Delivery_and_Lookalike_Sites.csv"
)
CMS_PROVIDER_DATA_JSON_URL = "https://data.cms.gov/provider-data/data.json"
DEFAULT_CMS_HOSPITAL_DATASET_ID = os.getenv("HLTHPRT_CMS_HOSPITAL_DATASET_ID", "xubh-q36u").lower()
DEFAULT_BATCH_SIZE = 5000
DEFAULT_MIN_ROWS = 100
DEFAULT_MIN_HOSPITAL_COORD_COVERAGE = 0.90

POINT_WKT_RE = re.compile(
    r"POINT\s*\(\s*(?P<lng>-?\d+(?:\.\d+)?)\s+(?P<lat>-?\d+(?:\.\d+)?)\s*\)",
    re.IGNORECASE,
)


def _normalize_zip(raw: str | None) -> str:
    digits = "".join(ch for ch in str(raw or "").strip() if ch.isdigit())
    return digits[:5].rjust(5, "0") if len(digits) >= 5 else ""


def _validate_schema_name(schema: str) -> str:
    cleaned = (schema or "").strip()
    if not cleaned or not (cleaned[0].isalpha() or cleaned[0] == "_"):
        raise ValueError(f"Invalid schema name: {schema!r}")
    if not all(ch.isalnum() or ch == "_" for ch in cleaned):
        raise ValueError(f"Invalid schema name: {schema!r}")
    return cleaned


async def _ensure_schema_exists(db_schema: str) -> None:
    db_schema = _validate_schema_name(db_schema)
    try:
        await db.status(f"CREATE SCHEMA IF NOT EXISTS {db_schema};")
    except Exception as exc:
        exists = bool(
            await db.scalar(f"SELECT to_regnamespace('{db_schema}') IS NOT NULL;")
        )
        if exists:
            logger.warning(
                "Schema %s already exists but CREATE SCHEMA failed (%s); continuing",
                db_schema,
                exc,
            )
            return
        raise


def _stage_index_name(stage_table: str, index_name: str) -> str:
    return f"{stage_table}_idx_{index_name}"


def _parse_lat_lng(row: dict) -> tuple[float | None, float | None]:
    lat_raw = row.get("Latitude")
    lng_raw = row.get("Longitude")

    try:
        lat = float(str(lat_raw).strip()) if lat_raw not in (None, "") else None
    except (TypeError, ValueError):
        lat = None
    try:
        lng = float(str(lng_raw).strip()) if lng_raw not in (None, "") else None
    except (TypeError, ValueError):
        lng = None

    if lat is not None and lng is not None:
        return lat, lng

    location = str(row.get("Location") or "").strip()
    if not location:
        return lat, lng

    wkt = POINT_WKT_RE.search(location)
    if wkt:
        try:
            return float(wkt.group("lat")), float(wkt.group("lng"))
        except (TypeError, ValueError):
            return lat, lng

    tokens = re.findall(r"-?\d+(?:\.\d+)?", location)
    if len(tokens) >= 2:
        try:
            first = float(tokens[0])
            second = float(tokens[1])
            # Prefer lat,lng ordering by default when plausible.
            if -90 <= first <= 90 and -180 <= second <= 180:
                return first, second
            if -180 <= first <= 180 and -90 <= second <= 90:
                return second, first
        except (TypeError, ValueError):
            return lat, lng

    return lat, lng


async def _load_zip_centroids() -> dict[str, tuple[float, float]]:
    stmt = (
        select(GeoZipLookup.zip_code, GeoZipLookup.latitude, GeoZipLookup.longitude)
        .where(
            and_(
                GeoZipLookup.zip_code.isnot(None),
                GeoZipLookup.latitude.isnot(None),
                GeoZipLookup.longitude.isnot(None),
            )
        )
    )
    result = await db.all(stmt)
    centroids: dict[str, tuple[float, float]] = {}
    for row in result:
        zip_code = _normalize_zip(getattr(row, "zip_code", None))
        if not zip_code:
            continue
        try:
            lat = float(getattr(row, "latitude"))
            lng = float(getattr(row, "longitude"))
        except (TypeError, ValueError):
            continue
        centroids[zip_code] = (lat, lng)
    return centroids


async def _create_stage_indexes(stage_cls, db_schema: str) -> None:
    if hasattr(stage_cls, "__my_index_elements__") and stage_cls.__my_index_elements__:
        await db.status(
            f"CREATE UNIQUE INDEX IF NOT EXISTS {stage_cls.__tablename__}_idx_primary "
            f"ON {db_schema}.{stage_cls.__tablename__} "
            f"({', '.join(stage_cls.__my_index_elements__)});"
        )

    if hasattr(stage_cls, "__my_additional_indexes__") and stage_cls.__my_additional_indexes__:
        for index in stage_cls.__my_additional_indexes__:
            index_name = index.get("name", "_".join(index.get("index_elements")))
            using = f"USING {index.get('using')} " if index.get("using") else ""
            where = f" WHERE {index.get('where')}" if index.get("where") else ""
            await db.status(
                f"CREATE INDEX IF NOT EXISTS "
                f"{_stage_index_name(stage_cls.__tablename__, index_name)} "
                f"ON {db_schema}.{stage_cls.__tablename__} {using}"
                f"({', '.join(index.get('index_elements'))}){where};"
            )


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


async def _fetch_and_parse_hrsa(client, stage_cls, batch_size: int, test_mode: bool, test_limit: int) -> int:
    logger.info("Fetching HRSA FQHC data...")
    accepted = 0
    batch = []
    now = datetime.datetime.utcnow()

    try:
        async with client.get(HRSA_FQHC_URL, timeout=120) as response:
            if response.status != 200:
                logger.error("Failed HRSA fetch: HTTP %s", response.status)
                return 0
            content = await response.read()

        reader = csv.DictReader(content.decode("utf-8", errors="replace").splitlines())
        for row in reader:
            site_name = row.get("Site Name")
            if not site_name:
                continue

            try:
                # Current HRSA file provides X=lng, Y=lat
                lat = float(
                    row.get("Geocode Latitude")
                    or row.get("Geocoding Artifact Address Primary Y Coordinate")
                    or 0.0
                )
                lng = float(
                    row.get("Geocode Longitude")
                    or row.get("Geocoding Artifact Address Primary X Coordinate")
                    or 0.0
                )
            except (TypeError, ValueError):
                continue

            if lat == 0.0 and lng == 0.0:
                continue

            batch.append({
                "id": str(uuid.uuid4()),
                "name": site_name,
                "facility_type": "FQHC",
                "address_line1": row.get("Site Address"),
                "city": row.get("Site City"),
                "state": row.get("Site State Abbreviation"),
                "zip_code": str(row.get("Site Postal Code") or "")[:5],
                "latitude": lat,
                "longitude": lng,
                "source_dataset": "HRSA_HEALTH_CENTER_SITES",
                "updated_at": now,
            })

            if len(batch) >= batch_size:
                await push_objects(batch, stage_cls)
                accepted += len(batch)
                batch.clear()

            if test_mode and accepted + len(batch) >= test_limit:
                break

        if batch:
            await push_objects(batch, stage_cls)
            accepted += len(batch)

        logger.info("HRSA FQHC: %d rows accepted", accepted)

    except Exception as e:
        logger.error("HRSA fetch failed: %s", str(e))

    return accepted


async def _fetch_cms_hospital_csv_url(client) -> str:
    async with client.get(CMS_PROVIDER_DATA_JSON_URL, timeout=60) as response:
        catalog = await response.json(content_type=None)

    selected_dataset = None
    for dataset in catalog.get("dataset", []):
        identifier = str(dataset.get("identifier", "")).lower()
        landing_page = str(dataset.get("landingPage", "")).lower()
        title = str(dataset.get("title", "")).lower()
        if (
            identifier == DEFAULT_CMS_HOSPITAL_DATASET_ID
            or f"/dataset/{DEFAULT_CMS_HOSPITAL_DATASET_ID}" in landing_page
            or title == "hospital general information"
        ):
            selected_dataset = dataset
            break

    if not selected_dataset:
        raise ValueError("Could not find Hospital General Information dataset in provider-data catalog.")

    for dist in selected_dataset.get("distribution", []):
        url = str(dist.get("downloadURL", "")).strip()
        if url.lower().endswith(".csv"):
            return url

    raise ValueError("Could not find Hospital General Information CSV URL in dataset distributions.")


async def _fetch_and_parse_cms_hospitals(
    client,
    stage_cls,
    batch_size: int,
    test_mode: bool,
    test_limit: int,
    already_accepted: int,
    zip_centroids: dict[str, tuple[float, float]],
) -> tuple[int, int]:
    """Fetch CMS Hospital General Information CSV and store Hospital anchors."""
    logger.info("Fetching CMS Hospital General Information...")
    accepted = 0
    with_coordinates = 0
    batch = []
    now = datetime.datetime.utcnow()

    url = os.getenv("HLTHPRT_CMS_HOSPITAL_CSV_URL")
    if not url:
        url = await _fetch_cms_hospital_csv_url(client)

    try:
        async with client.get(url, timeout=120) as response:
            if response.status != 200:
                logger.error("Failed CMS Hospital fetch: HTTP %s", response.status)
                return 0
            content = await response.read()

        reader = csv.DictReader(content.decode("utf-8", errors="replace").splitlines())
        for row in reader:
            facility_id = row.get("Facility ID") or row.get("Provider ID")
            facility_name = row.get("Facility Name") or row.get("Hospital Name")
            if not facility_name:
                continue

            # CMS Hospital dataset has Address, City, State, ZIP Code columns
            state = row.get("State")
            zip_code = _normalize_zip(row.get("ZIP Code"))

            lat, lng = _parse_lat_lng(row)
            if (lat is None or lng is None) and zip_code in zip_centroids:
                lat, lng = zip_centroids[zip_code]
            if lat is not None and lng is not None:
                with_coordinates += 1

            batch.append({
                "id": facility_id or str(uuid.uuid4()),
                "name": facility_name,
                "facility_type": "Hospital",
                "address_line1": row.get("Address"),
                "city": row.get("City") or row.get("City/Town"),
                "state": state,
                "zip_code": zip_code,
                "latitude": lat,
                "longitude": lng,
                "source_dataset": "CMS_HOSPITAL_GENERAL_INFO",
                "updated_at": now,
            })

            if len(batch) >= batch_size:
                await push_objects(batch, stage_cls)
                accepted += len(batch)
                batch.clear()

            if test_mode and already_accepted + accepted + len(batch) >= test_limit:
                break

        if batch:
            await push_objects(batch, stage_cls)
            accepted += len(batch)

        logger.info(
            "CMS Hospitals: %d rows accepted (%d with coordinates)",
            accepted,
            with_coordinates,
        )

    except Exception as e:
        logger.error("CMS Hospital fetch failed: %s", str(e))

    return accepted, with_coordinates


async def process_data(ctx, task=None):
    task = task or {}
    ctx.setdefault("context", {})

    if "test_mode" in task:
        ctx["context"]["test_mode"] = bool(task.get("test_mode"))
    test_mode = bool(ctx["context"].get("test_mode", False))

    await ensure_database(test_mode)

    import_date = ctx["import_date"]
    stage_cls = make_class(FacilityAnchor, import_date)
    batch_size = int(os.getenv("HLTHPRT_FACILITY_ANCHORS_BATCH_SIZE", str(DEFAULT_BATCH_SIZE)))
    test_limit = 500

    import aiohttp
    client = aiohttp.ClientSession()
    try:
        zip_centroids = await _load_zip_centroids()
        hrsa_count = await _fetch_and_parse_hrsa(client, stage_cls, batch_size, test_mode, test_limit)
        hosp_count, hosp_with_coords = await _fetch_and_parse_cms_hospitals(
            client,
            stage_cls,
            batch_size,
            test_mode,
            test_limit,
            hrsa_count,
            zip_centroids,
        )
    finally:
        await client.close()

    ctx["context"]["run"] = ctx["context"].get("run", 0) + 1
    ctx["context"]["hospital_count"] = hosp_count
    ctx["context"]["hospital_with_coords"] = hosp_with_coords
    logger.info("Facility Anchors import done: %d FQHC + %d Hospital rows", hrsa_count, hosp_count)


async def startup(ctx):
    await my_init_db(db)
    ctx["context"] = {}
    ctx["context"]["start"] = datetime.datetime.utcnow()
    ctx["context"]["run"] = 0
    ctx["context"]["test_mode"] = False
    await ensure_database(False)

    override_import_id = os.getenv("HLTHPRT_IMPORT_ID_OVERRIDE")
    ctx["import_date"] = _normalize_import_id(override_import_id)
    import_date = ctx["import_date"]
    db_schema = os.getenv("HLTHPRT_DB_SCHEMA") if os.getenv("HLTHPRT_DB_SCHEMA") else "mrf"

    stage_cls = make_class(FacilityAnchor, import_date)

    await _ensure_schema_exists(db_schema)
    await db.status(f"DROP TABLE IF EXISTS {db_schema}.{stage_cls.__tablename__};")
    await db.create_table(stage_cls.__table__, checkfirst=True)
    await _create_stage_indexes(stage_cls, db_schema)

    logger.info("Facility Anchors startup ready: schema=%s import_date=%s", db_schema, import_date)


async def shutdown(ctx):
    import_date = ctx.get("import_date")
    context = ctx.get("context") or {}

    if not context.get("run"):
        logger.info("No Facility Anchors jobs ran; skipping shutdown.")
        return

    await ensure_database(bool(context.get("test_mode")))

    db_schema = os.getenv("HLTHPRT_DB_SCHEMA") if os.getenv("HLTHPRT_DB_SCHEMA") else "mrf"
    stage_cls = make_class(FacilityAnchor, import_date)

    stage_rows = int(await db.scalar(
        f"SELECT COUNT(*) FROM {db_schema}.{stage_cls.__tablename__};"
    ) or 0)
    hospital_rows = int(await db.scalar(
        f"""
        SELECT COUNT(*)
          FROM {db_schema}.{stage_cls.__tablename__}
         WHERE facility_type = 'Hospital'
        """
    ) or 0)
    hospital_with_coords = int(await db.scalar(
        f"""
        SELECT COUNT(*)
          FROM {db_schema}.{stage_cls.__tablename__}
         WHERE facility_type = 'Hospital'
           AND latitude IS NOT NULL
           AND longitude IS NOT NULL
        """
    ) or 0)
    hospital_coord_ratio = (
        hospital_with_coords / hospital_rows if hospital_rows else 0.0
    )
    min_hospital_coord_ratio = float(
        os.getenv(
            "HLTHPRT_FACILITY_ANCHORS_MIN_HOSPITAL_COORD_RATIO",
            str(DEFAULT_MIN_HOSPITAL_COORD_COVERAGE),
        )
    )

    if context.get("test_mode"):
        logger.info("Facility Anchors test mode: staged rows=%d", stage_rows)
    elif stage_rows < DEFAULT_MIN_ROWS:
        raise RuntimeError(
            f"Facility Anchors stage row count {stage_rows} below minimum {DEFAULT_MIN_ROWS}; aborting."
        )
    elif hospital_rows > 0 and hospital_coord_ratio < min_hospital_coord_ratio:
        raise RuntimeError(
            "Facility Anchors hospital geocode coverage "
            f"{hospital_coord_ratio:.3f} below minimum {min_hospital_coord_ratio:.2f}; "
            "aborting publish."
        )

    async with db.transaction():
        table = FacilityAnchor.__main_table__
        await db.status(f"DROP TABLE IF EXISTS {db_schema}.{table}_old;")
        await db.status(f"ALTER TABLE IF EXISTS {db_schema}.{table} RENAME TO {table}_old;")
        await db.status(
            f"ALTER TABLE IF EXISTS {db_schema}.{stage_cls.__tablename__} RENAME TO {table};"
        )

        archived = _archived_identifier(f"{table}_idx_primary")
        await db.status(f"DROP INDEX IF EXISTS {db_schema}.{archived};")
        await db.status(
            f"ALTER INDEX IF EXISTS {db_schema}.{table}_idx_primary RENAME TO {archived};"
        )
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

    logger.info(
        "Facility Anchors publish complete: rows=%d hospitals=%d hospital_with_coords=%d ratio=%.3f",
        stage_rows,
        hospital_rows,
        hospital_with_coords,
        hospital_coord_ratio,
    )
    print_time_info(context.get("start"))


async def main(test_mode: bool = False):
    redis = await create_pool(
        build_redis_settings(),
        job_serializer=serialize_job,
        job_deserializer=deserialize_job,
    )
    payload = {"test_mode": bool(test_mode)}
    await redis.enqueue_job("process_data", payload, _queue_name=FACILITY_ANCHORS_QUEUE_NAME)
