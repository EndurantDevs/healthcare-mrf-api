# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import csv
import datetime
import hashlib
import logging
import os
import re
import tempfile

from arq import create_pool

from db.models import PharmacyEconomicsSummary, db
from process.ext.utils import (ensure_database, make_class, my_init_db,
                               print_time_info, push_objects)
from process.redis_config import build_redis_settings
from process.serialization import deserialize_job, serialize_job

logger = logging.getLogger(__name__)

PHARMACY_ECON_QUEUE_NAME = "arq:PharmacyEconomics"
POSTGRES_IDENTIFIER_MAX_LENGTH = 63

MEDICAID_DATA_JSON_URL = "https://data.medicaid.gov/data.json"
SDUD_TITLE_PREFIX = "State Drug Utilization Data "
SDUD_MERGED_TITLE = "SDUD"
NADAC_TITLE_PREFIX = "NADAC (National Average Drug Acquisition Cost) "
FUL_TITLE = "ACA Federal Upper Limits"

# Medicaid professional dispensing fees by state (sourced from CMS/Pharm.D references)
# These represent the state Medicaid FFS dispensing fee per prescription.
STATE_DISPENSING_FEES: dict[str, float] = {
    "AL": 10.64, "AK": 10.81, "AZ": 12.00, "AR": 6.65, "CA": 10.05,
    "CO": 10.17, "CT": 11.90, "DE": 3.65, "DC": 12.00, "FL": 10.24,
    "GA": 10.93, "HI": 10.49, "ID": 10.00, "IL": 13.18, "IN": 10.00,
    "IA": 10.02, "KS": 10.65, "KY": 10.76, "LA": 10.15, "ME": 11.52,
    "MD": 5.61, "MA": 13.70, "MI": 13.00, "MN": 10.48, "MS": 7.21,
    "MO": 6.15, "MT": 12.00, "NE": 10.57, "NV": 10.18, "NH": 11.00,
    "NJ": 10.05, "NM": 10.31, "NY": 10.08, "NC": 6.00, "ND": 10.07,
    "OH": 10.49, "OK": 10.18, "OR": 10.13, "PA": 10.49, "RI": 13.75,
    "SC": 10.32, "SD": 10.86, "TN": 10.15, "TX": 7.93, "UT": 10.23,
    "VT": 10.14, "VA": 10.49, "WA": 10.15, "WV": 12.55, "WI": 10.51,
    "WY": 12.00,
}

DEFAULT_BATCH_SIZE = 5000
DEFAULT_MIN_ROWS = 10
VALID_STATE_CODES = frozenset(STATE_DISPENSING_FEES.keys())


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


def _extract_trailing_year(title: str, prefix: str) -> int | None:
    if not title.startswith(prefix):
        return None
    match = re.search(r"(\d{4})\s*$", title)
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


async def _resolve_source_urls(client) -> tuple[str, str, str]:
    async with client.get(MEDICAID_DATA_JSON_URL, timeout=120) as response:
        catalog = await response.json(content_type=None)
    datasets = catalog.get("dataset", [])

    def _env_override(name: str) -> str | None:
        value = (os.getenv(name) or "").strip()
        return value or None

    def _latest_url_for_prefix(prefix: str) -> str | None:
        candidates: list[tuple[int, str]] = []
        for ds in datasets:
            title = str(ds.get("title") or "")
            year = _extract_trailing_year(title, prefix)
            if year is None:
                continue
            for dist in ds.get("distribution", []):
                url = str(dist.get("downloadURL") or dist.get("accessURL") or "").strip()
                if url.lower().endswith(".csv"):
                    candidates.append((year, url))
                    break
        if not candidates:
            return None
        candidates.sort(key=lambda item: item[0], reverse=True)
        return candidates[0][1]

    def _url_for_exact_title(title_value: str) -> str | None:
        for ds in datasets:
            if str(ds.get("title") or "") != title_value:
                continue
            for dist in ds.get("distribution", []):
                url = str(dist.get("downloadURL") or dist.get("accessURL") or "").strip()
                if url.lower().endswith(".csv"):
                    return url
        return None

    sdud_url = _env_override("HLTHPRT_PHARMACY_ECON_SDUD_URL") or _latest_url_for_prefix(SDUD_TITLE_PREFIX)
    # Fallback to merged SDUD when yearly resources are not available.
    if not sdud_url:
        sdud_url = _url_for_exact_title(SDUD_MERGED_TITLE)

    nadac_url = _env_override("HLTHPRT_PHARMACY_ECON_NADAC_URL") or _latest_url_for_prefix(NADAC_TITLE_PREFIX)
    ful_url = _env_override("HLTHPRT_PHARMACY_ECON_FUL_URL") or _url_for_exact_title(FUL_TITLE)

    if not sdud_url or not nadac_url or not ful_url:
        raise ValueError(
            "Could not resolve one or more Medicaid source URLs "
            f"(sdud={bool(sdud_url)}, nadac={bool(nadac_url)}, ful={bool(ful_url)})."
        )

    logger.info("Resolved SDUD source: %s", sdud_url)
    logger.info("Resolved NADAC source: %s", nadac_url)
    logger.info("Resolved FUL source: %s", ful_url)
    return sdud_url, nadac_url, ful_url


async def _download_to_temp_csv(client, url: str, tmp_dir: str, file_name: str) -> str:
    path = os.path.join(tmp_dir, file_name)
    async with client.get(url, timeout=600) as response:
        if response.status != 200:
            raise ValueError(f"Source download failed with HTTP {response.status} for {url}")
        with open(path, "wb") as fh:
            async for chunk in response.content.iter_chunked(10 * 1024 * 1024):
                fh.write(chunk)
    return path


def _parse_int(raw) -> int:
    if raw in (None, "", " ", "     "):
        return 0
    text = str(raw).replace(",", "").strip()
    if not text:
        return 0
    try:
        return int(float(text))
    except (TypeError, ValueError):
        return 0


def _parse_float(raw) -> float | None:
    if raw in (None, "", " ", "     "):
        return None
    text = str(raw).replace(",", "").replace("$", "").strip()
    if not text:
        return None
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


async def _fetch_sdud(client, sdud_url: str) -> dict[str, dict[str, dict]]:
    """Fetch SDUD and aggregate to state -> NDC11 volume."""
    logger.info("Fetching SDUD (State Drug Utilization Data) from CSV...")
    by_state: dict[str, dict[str, dict]] = {}
    dropped_invalid_state = 0
    with tempfile.TemporaryDirectory() as tmpdir:
        csv_path = await _download_to_temp_csv(client, sdud_url, tmpdir, "sdud.csv")
        with open(csv_path, "r", encoding="utf-8", errors="replace", newline="") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                state = (row.get("state") or row.get("State") or "").upper().strip()
                ndc = row.get("ndc") or row.get("NDC") or ""
                product_name = row.get("product_name") or row.get("Product Name") or ""
                qty = _parse_int(
                    row.get("number_of_prescriptions")
                    or row.get("Number of Prescriptions")
                )

                if not state or len(state) != 2 or not ndc or qty <= 0:
                    continue
                if state not in VALID_STATE_CODES:
                    dropped_invalid_state += 1
                    continue

                ndc_clean = ndc.replace("-", "").strip()
                if len(ndc_clean) < 11:
                    continue
                ndc11 = ndc_clean[:11]

                state_dict = by_state.setdefault(state, {})
                if ndc11 in state_dict:
                    state_dict[ndc11]["volume"] += qty
                else:
                    state_dict[ndc11] = {"name": product_name, "volume": qty}

    logger.info(
        "SDUD parsed: %d states, %d total state-NDC combos (dropped_invalid_state=%d)",
        len(by_state),
        sum(len(v) for v in by_state.values()),
        dropped_invalid_state,
    )
    return by_state


async def _fetch_nadac(client, nadac_url: str) -> dict[str, float]:
    logger.info("Fetching NADAC from CSV...")
    nadac_map: dict[str, float] = {}
    with tempfile.TemporaryDirectory() as tmpdir:
        csv_path = await _download_to_temp_csv(client, nadac_url, tmpdir, "nadac.csv")
        with open(csv_path, "r", encoding="utf-8", errors="replace", newline="") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                ndc = row.get("NDC") or row.get("ndc") or ""
                price = _parse_float(row.get("NADAC Per Unit") or row.get("nadac_per_unit"))
                if not ndc or price is None or price <= 0:
                    continue
                ndc_clean = ndc.replace("-", "").strip()
                if len(ndc_clean) < 11:
                    continue
                nadac_map[ndc_clean[:11]] = price
    logger.info("NADAC parsed: %d NDC prices loaded", len(nadac_map))
    return nadac_map


async def _fetch_ful(client, ful_url: str) -> dict[str, float]:
    logger.info("Fetching FUL from CSV...")
    ful_map: dict[str, float] = {}
    with tempfile.TemporaryDirectory() as tmpdir:
        csv_path = await _download_to_temp_csv(client, ful_url, tmpdir, "ful.csv")
        with open(csv_path, "r", encoding="utf-8", errors="replace", newline="") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                ndc = row.get("NDC") or row.get("ndc") or ""
                price = _parse_float(row.get("ACA FUL") or row.get("ful_per_unit") or row.get("Weighted Average of AMPs"))
                if not ndc or price is None or price <= 0:
                    continue
                ndc_clean = ndc.replace("-", "").strip()
                if len(ndc_clean) < 11:
                    continue
                ful_map[ndc_clean[:11]] = price
    logger.info("FUL parsed: %d NDC ceilings loaded", len(ful_map))
    return ful_map


async def process_data(ctx, task=None):
    task = task or {}
    ctx.setdefault("context", {})

    if "test_mode" in task:
        ctx["context"]["test_mode"] = bool(task.get("test_mode"))
    test_mode = bool(ctx["context"].get("test_mode", False))

    await ensure_database(test_mode)

    import_date = ctx["import_date"]
    stage_cls = make_class(PharmacyEconomicsSummary, import_date)
    batch_size = int(os.getenv("HLTHPRT_PHARMACY_ECON_BATCH_SIZE", str(DEFAULT_BATCH_SIZE)))

    import aiohttp
    client = aiohttp.ClientSession()
    accepted_rows = 0

    try:
        sdud_url, nadac_url, ful_url = await _resolve_source_urls(client)
        sdud_data = await _fetch_sdud(client, sdud_url)
        nadac_data = await _fetch_nadac(client, nadac_url)
        ful_data = await _fetch_ful(client, ful_url)
    finally:
        await client.close()

    now = datetime.datetime.utcnow()
    batch = []

    for state, ndcs in sdud_data.items():
        dispensing_fee = STATE_DISPENSING_FEES.get(state, 10.00)

        for ndc11, info in ndcs.items():
            nadac_cost = nadac_data.get(ndc11)
            ful_ceiling = ful_data.get(ndc11)

            if nadac_cost is None:
                continue

            # Standard 30-day quantity baseline
            qty = 30
            reimb_per_unit = min(nadac_cost, ful_ceiling) if ful_ceiling else nadac_cost
            total_reimb = (reimb_per_unit * qty) + dispensing_fee
            total_cost = nadac_cost * qty
            gross_margin = total_reimb - total_cost

            batch.append({
                "state": state,
                "ndc11": ndc11,
                "drug_name": info["name"],
                "sdud_volume": info["volume"],
                "nadac_per_unit": nadac_cost,
                "ful_per_unit": ful_ceiling,
                "medicaid_dispensing_fee": dispensing_fee,
                "estimated_gross_margin": round(gross_margin, 2),
                "updated_at": now,
            })

            if len(batch) >= batch_size:
                await push_objects(batch, stage_cls)
                accepted_rows += len(batch)
                batch.clear()

    if batch:
        await push_objects(batch, stage_cls)
        accepted_rows += len(batch)

    ctx["context"]["run"] = ctx["context"].get("run", 0) + 1
    logger.info("Pharmacy Economics import done: %d rows accepted", accepted_rows)


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

    stage_cls = make_class(PharmacyEconomicsSummary, import_date)

    await _ensure_schema_exists(db_schema)
    await db.status(f"DROP TABLE IF EXISTS {db_schema}.{stage_cls.__tablename__};")
    await db.create_table(stage_cls.__table__, checkfirst=True)
    await _create_stage_indexes(stage_cls, db_schema)

    logger.info("Pharmacy Economics startup ready: schema=%s import_date=%s", db_schema, import_date)


async def shutdown(ctx):
    import_date = ctx.get("import_date")
    context = ctx.get("context") or {}

    if not context.get("run"):
        logger.info("No Pharmacy Economics jobs ran; skipping shutdown.")
        return

    await ensure_database(bool(context.get("test_mode")))

    db_schema = os.getenv("HLTHPRT_DB_SCHEMA") if os.getenv("HLTHPRT_DB_SCHEMA") else "mrf"
    stage_cls = make_class(PharmacyEconomicsSummary, import_date)

    stage_rows = int(await db.scalar(
        f"SELECT COUNT(*) FROM {db_schema}.{stage_cls.__tablename__};"
    ) or 0)

    if context.get("test_mode"):
        logger.info("Pharmacy Economics test mode: staged rows=%d", stage_rows)
    elif stage_rows < DEFAULT_MIN_ROWS:
        raise RuntimeError(
            f"Pharmacy Economics stage row count {stage_rows} below minimum {DEFAULT_MIN_ROWS}; aborting."
        )

    async with db.transaction():
        table = PharmacyEconomicsSummary.__main_table__
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

    logger.info("Pharmacy Economics publish complete: %d rows", stage_rows)
    print_time_info(context.get("start"))


async def main(test_mode: bool = False):
    redis = await create_pool(
        build_redis_settings(),
        job_serializer=serialize_job,
        job_deserializer=deserialize_job,
    )
    payload = {"test_mode": bool(test_mode)}
    await redis.enqueue_job("process_data", payload, _queue_name=PHARMACY_ECON_QUEUE_NAME)
