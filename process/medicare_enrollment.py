# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import datetime
import hashlib
import logging
import os
import urllib.parse
from collections import defaultdict

from arq import create_pool
from sqlalchemy import and_, select

from db.models import (GeoZipLookup, MedicareEnrollmentCountyStats,
                       MedicareEnrollmentStats, db)
from process.ext.utils import (ensure_database, make_class, my_init_db,
                               print_time_info, push_objects)
from process.redis_config import build_redis_settings
from process.serialization import deserialize_job, serialize_job

logger = logging.getLogger(__name__)

MEDICARE_ENROLLMENT_QUEUE_NAME = "arq:MedicareEnrollment"
POSTGRES_IDENTIFIER_MAX_LENGTH = 63

CMS_DATA_JSON_URL = "https://data.cms.gov/data.json"
ENROLLMENT_TITLE_KEYWORDS = "medicare monthly enrollment"
DEFAULT_BATCH_SIZE = 10_000
DEFAULT_MIN_COUNTY_ROWS = 500
DEFAULT_MIN_ZIP_ROWS = 5000
DEFAULT_TEST_ROWS = 2000
DEFAULT_PAGE_SIZE = 5000
DEFAULT_MAX_UNMATCHED_COUNTY_RATIO = 0.10


def _stage_index_name(stage_table: str, index_name: str) -> str:
    return f"{stage_table}_idx_{index_name}"


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


async def _resolve_enrollment_api_url(client) -> str:
    async with client.get(CMS_DATA_JSON_URL, timeout=60) as response:
        catalog = await response.json(content_type=None)

    for dataset in catalog.get("dataset", []):
        title = str(dataset.get("title", "")).lower()
        if ENROLLMENT_TITLE_KEYWORDS in title:
            for dist in dataset.get("distribution", []):
                url = str(dist.get("accessURL", "")).strip()
                if "/data-api/v1/dataset/" in url and url.rstrip("/").endswith("/data"):
                    return url
    raise ValueError("Could not find Medicare Monthly Enrollment API URL in CMS catalog.")


def _to_int(raw) -> int:
    if raw in (None, "", " ", "     "):
        return 0
    text = str(raw).replace(",", "").strip()
    if not text:
        return 0
    try:
        return int(float(text))
    except (TypeError, ValueError):
        return 0


def _normalize_fips(raw) -> str:
    digits = "".join(ch for ch in str(raw or "").strip() if ch.isdigit())
    if not digits:
        return ""
    return digits[-5:].rjust(5, "0")


def _normalize_zip(raw) -> str:
    digits = "".join(ch for ch in str(raw or "").strip() if ch.isdigit())
    if len(digits) < 5:
        return ""
    return digits[:5]


async def _resolve_latest_annual_year(client, api_url: str) -> int:
    params = {
        "size": "1",
        "sort": "-YEAR",
        "filter[MONTH]": "Year",
        "filter[BENE_GEO_LVL]": "County",
    }
    query = urllib.parse.urlencode(params)
    async with client.get(f"{api_url}?{query}", timeout=60) as response:
        rows = await response.json(content_type=None)
    if not rows:
        raise ValueError("No annual county rows returned from Medicare Monthly Enrollment API.")
    year = _to_int(rows[0].get("YEAR"))
    if year <= 0:
        raise ValueError("Could not resolve latest annual YEAR from Medicare Monthly Enrollment API.")
    return year


def _allocate_by_weights(total: int, zip_weights: list[tuple[str, int]]) -> dict[str, int]:
    if total <= 0 or not zip_weights:
        return {}

    positive = [(zip_code, max(int(weight or 0), 0)) for zip_code, weight in zip_weights]
    if not positive:
        return {}

    weight_sum = sum(weight for _, weight in positive)
    if weight_sum <= 0:
        positive = [(zip_code, 1) for zip_code, _ in positive]
        weight_sum = len(positive)

    base_alloc: dict[str, int] = {}
    remainders: list[tuple[float, str]] = []
    assigned = 0
    for zip_code, weight in positive:
        exact = (total * weight) / weight_sum
        base = int(exact)
        base_alloc[zip_code] = base
        assigned += base
        remainders.append((exact - base, zip_code))

    remainder = total - assigned
    if remainder > 0:
        remainders.sort(key=lambda item: item[0], reverse=True)
        idx = 0
        while remainder > 0 and remainders:
            _, zip_code = remainders[idx % len(remainders)]
            base_alloc[zip_code] += 1
            remainder -= 1
            idx += 1

    return base_alloc


async def _load_county_zip_weights() -> dict[str, list[tuple[str, int]]]:
    stmt = (
        select(GeoZipLookup.county_code, GeoZipLookup.zip_code, GeoZipLookup.population)
        .where(
            and_(
                GeoZipLookup.county_code.isnot(None),
                GeoZipLookup.zip_code.isnot(None),
            )
        )
    )
    rows = await db.all(stmt)
    mapping: dict[str, dict[str, int]] = defaultdict(dict)
    for row in rows:
        county_code = _normalize_fips(getattr(row, "county_code", None))
        zip_code = _normalize_zip(getattr(row, "zip_code", None))
        if not county_code or not zip_code:
            continue
        try:
            population = int(getattr(row, "population") or 0)
        except (TypeError, ValueError):
            population = 0
        existing = mapping[county_code].get(zip_code, 0)
        mapping[county_code][zip_code] = max(existing, population)

    return {
        county_code: sorted(zip_weights.items())
        for county_code, zip_weights in mapping.items()
    }


async def _publish_stage_table(db_schema: str, model_cls, stage_cls) -> None:
    table = model_cls.__main_table__
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


async def process_data(ctx, task=None):
    task = task or {}
    ctx.setdefault("context", {})

    if "test_mode" in task:
        ctx["context"]["test_mode"] = bool(task.get("test_mode"))
    test_mode = bool(ctx["context"].get("test_mode", False))

    await ensure_database(test_mode)

    import_date = ctx["import_date"]
    county_stage_cls = make_class(MedicareEnrollmentCountyStats, import_date)
    zip_stage_cls = make_class(MedicareEnrollmentStats, import_date)
    batch_size = int(os.getenv("HLTHPRT_MEDICARE_ENROLLMENT_BATCH_SIZE", str(DEFAULT_BATCH_SIZE)))
    page_size = int(os.getenv("HLTHPRT_MEDICARE_ENROLLMENT_PAGE_SIZE", str(DEFAULT_PAGE_SIZE)))
    test_row_limit = int(os.getenv("HLTHPRT_MEDICARE_ENROLLMENT_TEST_ROWS", str(DEFAULT_TEST_ROWS)))

    import aiohttp
    client = aiohttp.ClientSession()
    latest_year = 0
    county_agg: dict[tuple[str, int], dict[str, int]] = {}
    try:
        api_url = await _resolve_enrollment_api_url(client)
        latest_year = await _resolve_latest_annual_year(client, api_url)
        logger.info(
            "Using Medicare Monthly Enrollment API: %s (annual county rows for YEAR=%s)",
            api_url,
            latest_year,
        )

        offset = 0
        while True:
            params = {
                "size": str(page_size),
                "offset": str(offset),
                "sort": "-YEAR",
                "filter[MONTH]": "Year",
                "filter[BENE_GEO_LVL]": "County",
                "filter[YEAR]": str(latest_year),
            }
            query = urllib.parse.urlencode(params)
            async with client.get(f"{api_url}?{query}", timeout=60) as response:
                if response.status != 200:
                    raise ValueError(
                        f"Medicare Monthly Enrollment API returned HTTP {response.status} "
                        f"at offset {offset}"
                    )
                rows = await response.json(content_type=None)

            if not rows:
                break

            for row in rows:
                county_fips = _normalize_fips(row.get("BENE_FIPS_CD"))
                if not county_fips:
                    continue

                year = _to_int(row.get("YEAR"))
                total_benes = _to_int(row.get("TOT_BENES"))
                part_d_benes = _to_int(row.get("PRSCRPTN_DRUG_TOT_BENES"))

                if year <= 0 or total_benes <= 0:
                    continue

                key = (county_fips, year)
                current = county_agg.get(key)
                if current is None:
                    county_agg[key] = {
                        "total_beneficiaries": total_benes,
                        "part_d_beneficiaries": max(part_d_benes, 0),
                    }
                else:
                    current["total_beneficiaries"] += total_benes
                    current["part_d_beneficiaries"] += max(part_d_benes, 0)

                if test_mode and len(county_agg) >= test_row_limit:
                    break

            if test_mode and len(county_agg) >= test_row_limit:
                break
            offset += len(rows)
    finally:
        await client.close()

    now = datetime.datetime.utcnow()
    county_rows = []
    for (county_fips, year), values in sorted(county_agg.items()):
        county_rows.append(
            {
                "county_fips": county_fips,
                "year": year,
                "part_d_beneficiaries": int(values.get("part_d_beneficiaries", 0) or 0),
                "total_beneficiaries": int(values.get("total_beneficiaries", 0) or 0),
                "updated_at": now,
            }
        )
    for idx in range(0, len(county_rows), batch_size):
        await push_objects(county_rows[idx: idx + batch_size], county_stage_cls)

    county_zip_weights = await _load_county_zip_weights()
    zip_agg: dict[tuple[str, int], dict[str, int]] = defaultdict(
        lambda: {"total_beneficiaries": 0, "part_d_beneficiaries": 0}
    )
    unmatched_counties = 0
    for county_row in county_rows:
        county_fips = county_row["county_fips"]
        year = county_row["year"]
        zip_weights = county_zip_weights.get(county_fips)
        if not zip_weights:
            unmatched_counties += 1
            continue

        total_alloc = _allocate_by_weights(county_row["total_beneficiaries"], zip_weights)
        partd_alloc = _allocate_by_weights(county_row["part_d_beneficiaries"], zip_weights)
        for zip_code, total_value in total_alloc.items():
            key = (zip_code, year)
            zip_agg[key]["total_beneficiaries"] += total_value
            zip_agg[key]["part_d_beneficiaries"] += partd_alloc.get(zip_code, 0)

    zip_rows = []
    for (zip_code, year), values in sorted(zip_agg.items()):
        zip_rows.append(
            {
                "zcta_code": zip_code,
                "year": year,
                "part_d_beneficiaries": int(values["part_d_beneficiaries"]),
                "total_beneficiaries": int(values["total_beneficiaries"]),
                "updated_at": now,
            }
        )
    for idx in range(0, len(zip_rows), batch_size):
        await push_objects(zip_rows[idx: idx + batch_size], zip_stage_cls)

    ctx["context"]["run"] = ctx["context"].get("run", 0) + 1
    ctx["context"]["latest_year"] = latest_year
    ctx["context"]["county_rows"] = len(county_rows)
    ctx["context"]["zip_rows"] = len(zip_rows)
    ctx["context"]["unmatched_counties"] = unmatched_counties
    logger.info(
        "Medicare Enrollment import done: county_rows=%d zip_rows=%d unmatched_counties=%d",
        len(county_rows),
        len(zip_rows),
        unmatched_counties,
    )


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

    county_stage_cls = make_class(MedicareEnrollmentCountyStats, import_date)
    zip_stage_cls = make_class(MedicareEnrollmentStats, import_date)

    await _ensure_schema_exists(db_schema)

    await db.status(f"DROP TABLE IF EXISTS {db_schema}.{county_stage_cls.__tablename__};")
    await db.create_table(county_stage_cls.__table__, checkfirst=True)
    await _create_stage_indexes(county_stage_cls, db_schema)

    await db.status(f"DROP TABLE IF EXISTS {db_schema}.{zip_stage_cls.__tablename__};")
    await db.create_table(zip_stage_cls.__table__, checkfirst=True)
    await _create_stage_indexes(zip_stage_cls, db_schema)

    logger.info(
        "Medicare Enrollment startup ready: schema=%s import_date=%s",
        db_schema,
        import_date,
    )


async def shutdown(ctx):
    import_date = ctx.get("import_date")
    context = ctx.get("context") or {}

    if not context.get("run"):
        logger.info("No Medicare Enrollment jobs ran; skipping shutdown.")
        return

    await ensure_database(bool(context.get("test_mode")))

    db_schema = os.getenv("HLTHPRT_DB_SCHEMA") if os.getenv("HLTHPRT_DB_SCHEMA") else "mrf"
    county_stage_cls = make_class(MedicareEnrollmentCountyStats, import_date)
    zip_stage_cls = make_class(MedicareEnrollmentStats, import_date)

    county_stage_rows = int(
        await db.scalar(f"SELECT COUNT(*) FROM {db_schema}.{county_stage_cls.__tablename__};") or 0
    )
    zip_stage_rows = int(
        await db.scalar(f"SELECT COUNT(*) FROM {db_schema}.{zip_stage_cls.__tablename__};") or 0
    )
    unmatched_counties = int(context.get("unmatched_counties") or 0)
    county_rows = int(context.get("county_rows") or county_stage_rows or 0)
    unmatched_ratio = (unmatched_counties / county_rows) if county_rows else 0.0
    max_unmatched_ratio = float(
        os.getenv(
            "HLTHPRT_MEDICARE_ENROLLMENT_MAX_UNMATCHED_COUNTY_RATIO",
            str(DEFAULT_MAX_UNMATCHED_COUNTY_RATIO),
        )
    )

    if context.get("test_mode"):
        logger.info(
            "Medicare Enrollment test mode: county_rows=%d zip_rows=%d unmatched_counties=%d",
            county_stage_rows,
            zip_stage_rows,
            unmatched_counties,
        )
    elif county_stage_rows < DEFAULT_MIN_COUNTY_ROWS:
        raise RuntimeError(
            "Medicare Enrollment county stage row count "
            f"{county_stage_rows} below minimum {DEFAULT_MIN_COUNTY_ROWS}; aborting."
        )
    elif zip_stage_rows < DEFAULT_MIN_ZIP_ROWS:
        raise RuntimeError(
            "Medicare Enrollment ZIP stage row count "
            f"{zip_stage_rows} below minimum {DEFAULT_MIN_ZIP_ROWS}; aborting."
        )
    elif unmatched_ratio > max_unmatched_ratio:
        raise RuntimeError(
            "Medicare Enrollment unmatched county ratio "
            f"{unmatched_ratio:.3f} above maximum {max_unmatched_ratio:.2f}; aborting."
        )

    async with db.transaction():
        await _publish_stage_table(db_schema, MedicareEnrollmentCountyStats, county_stage_cls)
        await _publish_stage_table(db_schema, MedicareEnrollmentStats, zip_stage_cls)

    logger.info(
        "Medicare Enrollment publish complete: county_rows=%d zip_rows=%d unmatched_ratio=%.3f",
        county_stage_rows,
        zip_stage_rows,
        unmatched_ratio,
    )
    print_time_info(context.get("start"))


async def main(test_mode: bool = False):
    redis = await create_pool(
        build_redis_settings(),
        job_serializer=serialize_job,
        job_deserializer=deserialize_job,
    )
    payload = {"test_mode": bool(test_mode)}
    await redis.enqueue_job("process_data", payload, _queue_name=MEDICARE_ENROLLMENT_QUEUE_NAME)
