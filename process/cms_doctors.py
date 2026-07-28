# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import csv
import datetime
import hashlib
import logging
import os
import tempfile
import zipfile
from io import BytesIO, TextIOWrapper
from pathlib import PurePath

from arq import create_pool

from db.models import DoctorClinicianAddress, db
from process.control_cancel import raise_if_cancelled
from process.control_lifecycle import mark_control_run
from process.cms_doctors_rows import doctor_address_row
from process.ext.address_canon import resolve_into_archive, source_enabled, stamp_address_keys
from process.ext.utils import (ensure_database, make_class, my_init_db,
                               print_time_info, push_objects)
from process.redis_config import build_redis_settings
from process.serialization import deserialize_job, serialize_job

logger = logging.getLogger(__name__)

CMS_DOCTORS_QUEUE_NAME = "arq:CMSDoctors"
POSTGRES_IDENTIFIER_MAX_LENGTH = 63

CMS_PROVIDER_DATA_JSON_URL = "https://data.cms.gov/provider-data/data.json"
CMS_PROVIDER_METASTORE_DATASET_URL = (
    "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items/{dataset_id}"
)
DEFAULT_DOCTORS_DATASET_ID = os.getenv("HLTHPRT_CMS_DOCTORS_DATASET_ID", "mj5m-pzi6").lower()
DEFAULT_BATCH_SIZE = 10_000
DEFAULT_MIN_ROWS = 10_000
DEFAULT_TEST_ROWS = 5000


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


def _distribution_urls(dataset: dict) -> list[str]:
    urls: list[str] = []
    for dist in dataset.get("distribution", []):
        url = str(dist.get("downloadURL", "")).strip()
        if url and (url.lower().endswith((".csv", ".zip")) or "dac_nationaldownloadablefile" in url.lower()):
            urls.append(url)
    return urls


async def _first_reachable_url(client, urls: list[str]) -> str | None:
    for url in urls:
        try:
            async with client.head(url, allow_redirects=True, timeout=60) as response:
                if response.status < 400:
                    return url
                logger.warning("CMS Doctors source candidate returned HTTP %s: %s", response.status, url)
        except Exception as exc:
            logger.warning("CMS Doctors source candidate probe failed: %s (%s)", url, exc)
    return None


async def _fetch_doctors_download_url(client) -> str:
    metastore_url = CMS_PROVIDER_METASTORE_DATASET_URL.format(dataset_id=DEFAULT_DOCTORS_DATASET_ID)
    try:
        async with client.get(metastore_url, timeout=60) as response:
            response.raise_for_status()
            dataset = await response.json(content_type=None)
        url = await _first_reachable_url(client, _distribution_urls(dataset))
        if url:
            return url
    except Exception as exc:
        logger.warning("Could not resolve CMS Doctors metastore URL, falling back to catalog: %s", exc)

    async with client.get(CMS_PROVIDER_DATA_JSON_URL, timeout=60) as response:
        response.raise_for_status()
        catalog = await response.json(content_type=None)

    selected_dataset = None
    for dataset in catalog.get("dataset", []):
        identifier = str(dataset.get("identifier", "")).lower()
        landing_page = str(dataset.get("landingPage", "")).lower()
        title = str(dataset.get("title", "")).lower()
        description = str(dataset.get("description", "")).lower()
        if (
            identifier == DEFAULT_DOCTORS_DATASET_ID
            or f"/dataset/{DEFAULT_DOCTORS_DATASET_ID}" in landing_page
            or (
                "national downloadable file" in title
                and "doctors and clinicians" in description
            )
        ):
            selected_dataset = dataset
            break

    if not selected_dataset:
        raise ValueError("Could not find CMS Doctors dataset in provider-data catalog.")

    candidates = _distribution_urls(selected_dataset)
    url = await _first_reachable_url(client, candidates)
    if url:
        return url

    raise ValueError("Could not find CMS Doctors CSV/ZIP download URL in dataset.")


async def _consume_doctors_reader(
    reader,
    *,
    ctx,
    task,
    stage_cls,
    batch_size: int,
    test_mode: bool,
    test_row_limit: int,
) -> int:
    """Normalize reader rows and persist bounded, deduplicated batches."""
    accepted_rows = 0
    provider_batch_rows = []
    seen_checksums: set[int] = set()
    now = datetime.datetime.utcnow()
    for provider_row in reader:
        address_row = doctor_address_row(provider_row, now)
        if address_row is None:
            continue
        address_checksum = address_row["address_checksum"]
        if address_checksum in seen_checksums:
            continue
        seen_checksums.add(address_checksum)
        provider_batch_rows.append(address_row)
        if len(provider_batch_rows) >= batch_size:
            await raise_if_cancelled(ctx, task)
            await push_objects(provider_batch_rows, stage_cls)
            accepted_rows += len(provider_batch_rows)
            provider_batch_rows.clear()
        if test_mode and accepted_rows + len(provider_batch_rows) >= test_row_limit:
            break
    if provider_batch_rows:
        await raise_if_cancelled(ctx, task)
        await push_objects(provider_batch_rows, stage_cls)
        accepted_rows += len(provider_batch_rows)
    return accepted_rows


async def _import_doctors_source(
    source_path: str,
    *,
    ctx,
    task,
    stage_cls,
    batch_size: int,
    test_mode: bool,
    test_row_limit: int,
) -> int:
    """Open a downloaded CSV or ZIP and stream its rows through one importer."""
    reader_kwargs_by_name = {
        "ctx": ctx,
        "task": task,
        "stage_cls": stage_cls,
        "batch_size": batch_size,
        "test_mode": test_mode,
        "test_row_limit": test_row_limit,
    }
    if source_path.lower().endswith(".zip"):
        with zipfile.ZipFile(source_path) as archive:
            csv_filename = next(
                (name for name in archive.namelist() if name.lower().endswith(".csv")),
                None,
            )
            if not csv_filename:
                raise ValueError("No CSV inside the CMS Doctors ZIP")
            logger.info("Streaming CSV from ZIP: %s", csv_filename)
            with archive.open(csv_filename) as raw_file:
                text_file = TextIOWrapper(
                    raw_file,
                    encoding="utf-8",
                    errors="replace",
                )
                return await _consume_doctors_reader(
                    csv.DictReader(text_file),
                    **reader_kwargs_by_name,
                )
    logger.info("Streaming CSV: %s", os.path.basename(source_path))
    with open(
        source_path,
        "r",
        encoding="utf-8",
        errors="replace",
        newline="",
    ) as raw_file:
        return await _consume_doctors_reader(
            csv.DictReader(raw_file),
            **reader_kwargs_by_name,
        )


async def _download_doctors_source(client, url: str, source_path: str) -> None:
    """Stream one CMS Doctors source into a temporary local file."""
    async with client.get(url, timeout=600) as response:
        response.raise_for_status()
        with open(source_path, "wb") as destination:
            async for chunk in response.content.iter_chunked(10 * 1024 * 1024):
                destination.write(chunk)


async def import_cms_doctors_data(ctx, task=None):
    """Download and import the current CMS doctors address dataset."""

    task = task or {}
    await raise_if_cancelled(ctx, task)
    ctx.setdefault("context", {})

    if "test_mode" in task:
        ctx["context"]["test_mode"] = bool(task.get("test_mode"))
    test_mode = bool(ctx["context"].get("test_mode", False))

    await ensure_database(test_mode)

    import_date = ctx["import_date"]
    stage_cls = make_class(DoctorClinicianAddress, import_date)
    batch_size = int(os.getenv("HLTHPRT_CMS_DOCTORS_BATCH_SIZE", str(DEFAULT_BATCH_SIZE)))
    test_row_limit = int(os.getenv("HLTHPRT_CMS_DOCTORS_TEST_ROWS", str(DEFAULT_TEST_ROWS)))

    import aiohttp
    client = aiohttp.ClientSession()
    accepted_rows = 0

    try:
        url = await _fetch_doctors_download_url(client)
        logger.info("Found CMS Doctors source: %s", url)

        # Download to temp file to avoid loading large files into memory
        with tempfile.TemporaryDirectory() as tmpdir:
            source_ext = ".zip" if url.lower().endswith(".zip") else ".csv"
            source_path = os.path.join(tmpdir, f"cms_doctors{source_ext}")

            await _download_doctors_source(client, url, source_path)
            accepted_rows += await _import_doctors_source(
                source_path,
                ctx=ctx,
                task=task,
                stage_cls=stage_cls,
                batch_size=batch_size,
                test_mode=test_mode,
                test_row_limit=test_row_limit,
            )
    finally:
        await client.close()

    ctx["context"]["run"] = ctx["context"].get("run", 0) + 1
    logger.info("CMS Doctors import done: %d rows accepted", accepted_rows)


process_data = import_cms_doctors_data
process_data.__name__ = "process_data"


async def startup(ctx):
    """Initialize database and control-run context for CMS Doctors workers."""

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

    stage_cls = make_class(DoctorClinicianAddress, import_date)

    await _ensure_schema_exists(db_schema)
    await db.status(f"DROP TABLE IF EXISTS {db_schema}.{stage_cls.__tablename__};")
    await db.create_table(stage_cls.__table__, checkfirst=True)
    await _create_stage_indexes(stage_cls, db_schema)

    logger.info("CMS Doctors startup ready: schema=%s import_date=%s", db_schema, import_date)


async def publish_cms_doctors_generation(ctx):
    """Publish a completed CMS Doctors stage or record its terminal failure."""

    import_date = ctx.get("import_date")
    context = ctx.get("context") or {}
    run_id = str(context.get("control_run_id") or ctx.get("control_run_id") or "").strip()

    if not context.get("run"):
        logger.info("No CMS Doctors jobs ran; skipping shutdown.")
        return

    await ensure_database(bool(context.get("test_mode")))

    db_schema = os.getenv("HLTHPRT_DB_SCHEMA") if os.getenv("HLTHPRT_DB_SCHEMA") else "mrf"
    stage_cls = make_class(DoctorClinicianAddress, import_date)

    stage_rows = int(await db.scalar(
        f"SELECT COUNT(*) FROM {db_schema}.{stage_cls.__tablename__};"
    ) or 0)

    if context.get("test_mode"):
        logger.info("CMS Doctors test mode: staged rows=%d", stage_rows)
    elif stage_rows < DEFAULT_MIN_ROWS:
        raise RuntimeError(
            f"CMS Doctors stage row count {stage_rows} below minimum {DEFAULT_MIN_ROWS}; aborting."
        )

    address_stats = None
    if source_enabled("cms_doctors"):
        async def _cancel_check():
            await raise_if_cancelled(ctx, {})

        await stamp_address_keys(
            stage_cls.__tablename__,
            {
                "first_line": "address_line1",
                "second_line": "address_line2",
                "city": "city",
                "state": "state",
                "zip": "zip_code",
                "country": "'US'",
            },
            schema=db_schema,
            cancel_check=_cancel_check,
        )
        address_stats = await resolve_into_archive(
            stage_cls.__tablename__,
            {
                "first_line": "address_line1",
                "second_line": "address_line2",
                "city": "city",
                "state": "state",
                "zip": "zip_code",
                "country": "'US'",
            },
            source_bit=2,
            priority=1,
            schema=db_schema,
            cancel_check=_cancel_check,
        )
        logger.info("CMS Doctors canonical address resolve complete: %s", address_stats)

    async with db.transaction():
        table = DoctorClinicianAddress.__main_table__
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

    logger.info("CMS Doctors publish complete: %d rows", stage_rows)
    print_time_info(context.get("start"))
    await mark_control_run(
        run_id,
        status="succeeded",
        phase_detail="cms-doctors published",
        progress_message="succeeded",
        metrics={
            "rows": stage_rows,
            **({"address_resolve": address_stats.__dict__} if address_stats else {}),
        },
    )


shutdown = publish_cms_doctors_generation
shutdown.__name__ = "shutdown"


async def main(test_mode: bool = False):
    """Queue the CMS Doctors import with the requested bounded test mode."""

    redis = await create_pool(
        build_redis_settings(),
        job_serializer=serialize_job,
        job_deserializer=deserialize_job,
    )
    payload = {"test_mode": bool(test_mode)}
    await redis.enqueue_job("process_data", payload, _queue_name=CMS_DOCTORS_QUEUE_NAME)
