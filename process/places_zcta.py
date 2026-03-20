# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import datetime
import hashlib
import os
import tempfile
import re
from pathlib import PurePath
from typing import Any

from aiofile import async_open
from aiocsv import AsyncDictReader
from arq import create_pool

from db.models import PricingPlacesZcta, db
from process.ext.utils import (download_it_and_save, ensure_database, make_class,
                               my_init_db, print_time_info, push_objects)
from process.redis_config import build_redis_settings
from process.serialization import deserialize_job, serialize_job

PLACES_ZCTA_QUEUE_NAME = "arq:PlacesZcta"
POSTGRES_IDENTIFIER_MAX_LENGTH = 63
DEFAULT_DOWNLOAD_URL = (
    "https://chronicdata.cdc.gov/api/views/"
    "qnzd-25i4/rows.csv?accessType=DOWNLOAD"
)
SOURCE_DOWNLOAD_CHUNK_SIZE = 10 * 1024 * 1024
DEFAULT_BATCH_SIZE = 5000
DEFAULT_TEST_ROWS = 1500
DEFAULT_MIN_ROWS = 1000
ZIP5_TOKEN = re.compile(r"(?<!\d)(\d{5})(?!\d)")


def _env_positive_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if not raw:
        return default
    try:
        value = int(raw)
        return value if value > 0 else default
    except (TypeError, ValueError):
        return default


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


def _safe_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def _safe_int(value: Any) -> int | None:
    text = _safe_text(value)
    if not text:
        return None
    try:
        return int(float(text.replace(",", "")))
    except (TypeError, ValueError):
        return None


def _safe_float(value: Any) -> float | None:
    text = _safe_text(value)
    if not text:
        return None
    try:
        return float(text.replace(",", ""))
    except (TypeError, ValueError):
        return None


def _normalize_zcta(raw: Any) -> str | None:
    text = _safe_text(raw)
    if not text:
        return None
    matches = ZIP5_TOKEN.findall(text)
    if matches:
        return matches[-1]
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) < 5:
        return None
    return digits[-5:]


def _build_places_record(row: dict[str, Any], latest_year: int) -> dict[str, Any] | None:
    year = _safe_int(row.get("Year"))
    if year != latest_year:
        return None

    zcta = _normalize_zcta(row.get("LocationID") or row.get("LocationName"))
    if not zcta:
        return None

    measure_id = _safe_text(row.get("MeasureId") or row.get("MeasureID"))
    if not measure_id:
        return None

    measure_name = _safe_text(row.get("Measure")) or _safe_text(row.get("Short_Question_Text"))

    return {
        "zcta": zcta,
        "year": year,
        "measure_id": measure_id,
        "measure_name": measure_name,
        "data_value": _safe_float(row.get("Data_Value")),
        "low_ci": _safe_float(row.get("Low_Confidence_Limit")),
        "high_ci": _safe_float(row.get("High_Confidence_Limit")),
        "data_value_type": _safe_text(row.get("Data_Value_Type") or row.get("DataValueTypeID")),
        "source": _safe_text(row.get("DataSource")) or "CDC PLACES",
        "updated_at": datetime.datetime.utcnow(),
    }


async def _detect_latest_year(csv_path: str) -> int:
    latest_year: int | None = None

    async with async_open(csv_path, "r", encoding="utf-8-sig") as handle:
        reader = AsyncDictReader(handle, delimiter=",")
        async for row in reader:
            year = _safe_int(row.get("Year"))
            if year is None:
                continue
            if latest_year is None or year > latest_year:
                latest_year = year

    if latest_year is None:
        raise RuntimeError("CDC PLACES file has no valid Year values")
    return latest_year


async def _flush_places_rows(
    row_buffer: dict[tuple[str, int, str], dict[str, Any]],
    target_cls,
) -> int:
    if not row_buffer:
        return 0
    rows = list(row_buffer.values())
    row_buffer.clear()
    await push_objects(rows, target_cls, rewrite=True, use_copy=False)
    return len(rows)


async def process_data(ctx, task=None):  # pragma: no cover
    task = task or {}
    ctx.setdefault("context", {})

    if "test_mode" in task:
        ctx["context"]["test_mode"] = bool(task.get("test_mode"))
    test_mode = bool(ctx["context"].get("test_mode", False))

    await ensure_database(test_mode)

    import_date = ctx["import_date"]
    target_cls = make_class(PricingPlacesZcta, import_date)

    batch_size = _env_positive_int("HLTHPRT_PLACES_ZCTA_BATCH_SIZE", DEFAULT_BATCH_SIZE)
    test_row_limit = _env_positive_int("HLTHPRT_PLACES_ZCTA_TEST_ROWS", DEFAULT_TEST_ROWS)
    source_url = os.getenv("HLTHPRT_PLACES_ZCTA_DOWNLOAD_URL", DEFAULT_DOWNLOAD_URL)

    with tempfile.TemporaryDirectory() as tmpdirname:
        tmp_filename = str(PurePath(str(tmpdirname), "places_zcta.csv"))
        await download_it_and_save(
            source_url,
            tmp_filename,
            chunk_size=SOURCE_DOWNLOAD_CHUNK_SIZE,
            cache_dir="/tmp",
        )

        latest_year = await _detect_latest_year(tmp_filename)
        row_buffer: dict[tuple[str, int, str], dict[str, Any]] = {}
        processed_rows = 0
        accepted_rows = 0
        matched_rows = 0

        async with async_open(tmp_filename, "r", encoding="utf-8-sig") as handle:
            reader = AsyncDictReader(handle, delimiter=",")
            async for row in reader:
                processed_rows += 1
                record = _build_places_record(row, latest_year)
                if not record:
                    continue

                matched_rows += 1
                key = (record["zcta"], record["year"], record["measure_id"])
                row_buffer[key] = record

                if len(row_buffer) >= batch_size:
                    accepted_rows += await _flush_places_rows(row_buffer, target_cls)

                if test_mode and matched_rows >= test_row_limit:
                    break

        accepted_rows += await _flush_places_rows(row_buffer, target_cls)

    if accepted_rows <= 0:
        raise RuntimeError(
            f"CDC PLACES import produced no rows for latest year={latest_year}; aborting stage publish"
        )

    ctx["context"]["audit"] = {
        "source_url": source_url,
        "latest_year": latest_year,
        "processed_rows": processed_rows,
        "accepted_rows": accepted_rows,
    }
    ctx["context"]["run"] = ctx["context"].get("run", 0) + 1

    print(
        "PLACES ZCTA import done: "
        f"latest_year={latest_year} processed={processed_rows:,} accepted={accepted_rows:,}"
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
    import_date = ctx["import_date"]
    db_schema = os.getenv("HLTHPRT_DB_SCHEMA") if os.getenv("HLTHPRT_DB_SCHEMA") else "mrf"

    stage_cls = make_class(PricingPlacesZcta, import_date)

    await db.status(f"CREATE SCHEMA IF NOT EXISTS {db_schema};")
    await db.status(f"DROP TABLE IF EXISTS {db_schema}.{stage_cls.__tablename__};")
    await db.create_table(stage_cls.__table__, checkfirst=True)

    if hasattr(stage_cls, "__my_index_elements__") and stage_cls.__my_index_elements__:
        await db.status(
            f"CREATE UNIQUE INDEX IF NOT EXISTS {stage_cls.__tablename__}_idx_primary "
            f"ON {db_schema}.{stage_cls.__tablename__} "
            f"({', '.join(stage_cls.__my_index_elements__)});"
        )

    print(
        f"PLACES ZCTA startup ready for schema={db_schema} "
        f"import_date={import_date}"
    )


async def _table_exists(schema: str, table_name: str) -> bool:
    exists = await db.scalar(f"SELECT to_regclass('{schema}.{table_name}');")
    return bool(exists)


async def shutdown(ctx):  # pragma: no cover
    import_date = ctx.get("import_date")
    context = ctx.get("context") or {}

    if not context.get("run"):
        print("No PLACES ZCTA jobs ran in this worker session; skipping shutdown finalization.")
        return

    await ensure_database(bool(context.get("test_mode")))

    db_schema = os.getenv("HLTHPRT_DB_SCHEMA") if os.getenv("HLTHPRT_DB_SCHEMA") else "mrf"
    stage_cls = make_class(PricingPlacesZcta, import_date)

    if not await _table_exists(db_schema, stage_cls.__tablename__):
        raise RuntimeError(
            f"Staging table {db_schema}.{stage_cls.__tablename__} is missing; cannot finalize PLACES publish."
        )

    stage_rows = int(
        await db.scalar(
            f"SELECT COUNT(*) FROM {db_schema}.{stage_cls.__tablename__};"
        )
        or 0
    )
    min_rows = _env_positive_int("HLTHPRT_PLACES_ZCTA_MIN_ROWS", DEFAULT_MIN_ROWS)

    if context.get("test_mode"):
        print(f"PLACES ZCTA test mode: staged rows={stage_rows:,}")
    elif stage_rows < min_rows:
        raise RuntimeError(
            f"PLACES ZCTA stage row count {stage_rows:,} is below minimum {min_rows:,}; aborting publish."
        )

    async def archive_index(index_name: str) -> str:
        archived_name = _archived_identifier(index_name)
        await db.status(f"DROP INDEX IF EXISTS {db_schema}.{archived_name};")
        await db.status(
            f"ALTER INDEX IF EXISTS {db_schema}.{index_name} RENAME TO {archived_name};"
        )
        return archived_name

    async with db.transaction():
        if (
            hasattr(PricingPlacesZcta, "__my_additional_indexes__")
            and PricingPlacesZcta.__my_additional_indexes__
        ):
            for index in PricingPlacesZcta.__my_additional_indexes__:
                index_name = index.get("name", "_".join(index.get("index_elements")))
                using = f"USING {index.get('using')} " if index.get("using") else ""
                where_clause = f" WHERE {index.get('where')}" if index.get("where") else ""
                create_index_sql = (
                    f"CREATE INDEX IF NOT EXISTS {stage_cls.__tablename__}_idx_{index_name} "
                    f"ON {db_schema}.{stage_cls.__tablename__} {using}"
                    f"({', '.join(index.get('index_elements'))}){where_clause};"
                )
                print(create_index_sql)
                await db.status(create_index_sql)

    await db.execute_ddl(f"ANALYZE {db_schema}.{stage_cls.__tablename__};")

    async with db.transaction():
        table = stage_cls.__main_table__
        await db.status(f"DROP TABLE IF EXISTS {db_schema}.{table}_old;")
        await db.status(f"ALTER TABLE IF EXISTS {db_schema}.{table} RENAME TO {table}_old;")
        await db.status(
            f"ALTER TABLE IF EXISTS {db_schema}.{stage_cls.__tablename__} RENAME TO {table};"
        )

        await archive_index(f"{table}_idx_primary")
        await db.status(
            f"ALTER INDEX IF EXISTS {db_schema}.{stage_cls.__tablename__}_idx_primary "
            f"RENAME TO {table}_idx_primary;"
        )

        for index in PricingPlacesZcta.__my_additional_indexes__:
            index_name = index.get("name", "_".join(index.get("index_elements")))
            await archive_index(f"{table}_idx_{index_name}")
            await db.status(
                f"ALTER INDEX IF EXISTS {db_schema}.{stage_cls.__tablename__}_idx_{index_name} "
                f"RENAME TO {table}_idx_{index_name};"
            )

    print_time_info(context.get("start"))


async def main(test_mode: bool = False):  # pragma: no cover
    redis = await create_pool(
        build_redis_settings(),
        job_serializer=serialize_job,
        job_deserializer=deserialize_job,
    )
    payload = {"test_mode": bool(test_mode)}
    await redis.enqueue_job("process_data", payload, _queue_name=PLACES_ZCTA_QUEUE_NAME)


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main(bool("--test" in os.sys.argv)))
