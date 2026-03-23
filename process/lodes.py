# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import csv
import datetime
import gzip
import hashlib
import logging
import os
from collections import defaultdict

from arq import create_pool

from db.models import LODESWorkplaceAggregate, db
from process.ext.utils import (download_it_and_save, ensure_database,
                               make_class, my_init_db, print_time_info,
                               push_objects)
from process.redis_config import build_redis_settings
from process.serialization import deserialize_job, serialize_job

logger = logging.getLogger(__name__)

LODES_QUEUE_NAME = "arq:LODES"
POSTGRES_IDENTIFIER_MAX_LENGTH = 63

# All 50 states + DC
ALL_STATES = [
    "al", "ak", "az", "ar", "ca", "co", "ct", "de", "dc", "fl",
    "ga", "hi", "id", "il", "in", "ia", "ks", "ky", "la", "me",
    "md", "ma", "mi", "mn", "ms", "mo", "mt", "ne", "nv", "nh",
    "nj", "nm", "ny", "nc", "nd", "oh", "ok", "or", "pa", "ri",
    "sc", "sd", "tn", "tx", "ut", "vt", "va", "wa", "wv", "wi", "wy",
]
DEFAULT_TEST_STATES = ["tx", "ca", "fl"]


def _resolve_test_states() -> list[str]:
    raw = (os.getenv("HLTHPRT_LODES_TEST_STATES") or "").strip()
    if not raw:
        return list(DEFAULT_TEST_STATES)
    parsed = []
    for token in raw.split(","):
        state = token.strip().lower()
        if len(state) == 2 and state in ALL_STATES:
            parsed.append(state)
    return parsed or list(DEFAULT_TEST_STATES)


TEST_STATES = _resolve_test_states()

LODES_TARGET_YEAR = int(os.getenv("HLTHPRT_LODES_YEAR", "2021"))
LODES_MIN_YEAR = int(os.getenv("HLTHPRT_LODES_MIN_YEAR", "2010"))
LODES_BASE_URL = "https://lehd.ces.census.gov/data/lodes/LODES8"

# HUD USPS ZIP Crosswalk (TRACT-to-ZIP)
HUD_CROSSWALK_URL = (
    "https://www.huduser.gov/hudapi/public/usps?type=2&query=All"
)
# Census 2020 ZCTA-to-tract relationship file (public, no token required).
# We invert it to tract->ZCTA by selecting the ZCTA with max overlap area.
CENSUS_TRACT_ZCTA_REL_URL = (
    "https://www2.census.gov/geo/docs/maps-data/data/rel2020/zcta520/"
    "tab20_zcta520_tract20_natl.txt"
)
DEFAULT_BATCH_SIZE = 5000
DEFAULT_MIN_ROWS = 5000
DEFAULT_MIN_DISTINCT_ZCTAS = 5000
DEFAULT_MIN_GEO_MATCH_RATIO = 0.85


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


async def _load_tract_to_zip_crosswalk(client) -> dict[str, str]:
    """Download HUD USPS crosswalk to map Census Tract → ZIP code.

    Returns an empty mapping when no usable crosswalk is available.
    """
    crosswalk: dict[str, str] = {}

    # The HUD crosswalk API requires a token; fall back to the publicly
    # available crosswalk CSV when the env var is set.
    hud_token = os.getenv("HLTHPRT_HUD_API_TOKEN")
    crosswalk_file = os.getenv("HLTHPRT_LODES_CROSSWALK_FILE")

    if crosswalk_file and os.path.exists(crosswalk_file):
        # Local crosswalk CSV: columns TRACT, ZIP
        logger.info("Loading local crosswalk file: %s", crosswalk_file)
        with open(crosswalk_file, "r", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                tract = row.get("TRACT") or row.get("tract") or ""
                zip_code = row.get("ZIP") or row.get("zip") or ""
                if tract and len(zip_code) >= 5:
                    crosswalk[tract] = zip_code[:5]
        logger.info("Loaded %d tract→zip mappings from file", len(crosswalk))
        return crosswalk

    if hud_token:
        try:
            headers = {"Authorization": f"Bearer {hud_token}"}
            async with client.get(HUD_CROSSWALK_URL, headers=headers, timeout=120) as resp:
                if resp.status == 200:
                    data = await resp.json(content_type=None)
                    for item in data.get("data", {}).get("results", data if isinstance(data, list) else []):
                        tract = str(item.get("geoid", ""))
                        zip_code = str(item.get("zip", ""))
                        if tract and len(zip_code) >= 5:
                            crosswalk[tract] = zip_code[:5]
                    logger.info("Loaded %d tract→zip mappings from HUD API", len(crosswalk))
                    return crosswalk
        except Exception as e:
            logger.warning("HUD crosswalk API failed (%s); trying Census fallback", e)

    # Public Census fallback: map GEOID_TRACT_20 -> GEOID_ZCTA5_20 by max AREALAND_PART.
    # This preserves real ZIP/ZCTA granularity without requiring HUD credentials.
    try:
        async with client.get(CENSUS_TRACT_ZCTA_REL_URL, timeout=300) as resp:
            if resp.status == 200:
                content = (await resp.read()).decode("utf-8-sig", errors="replace")
                reader = csv.DictReader(content.splitlines(), delimiter="|")
                best_by_tract: dict[str, tuple[int, str]] = {}
                for row in reader:
                    tract = (row.get("GEOID_TRACT_20") or "").strip()
                    zcta = (row.get("GEOID_ZCTA5_20") or "").strip()
                    if not tract or not zcta or len(zcta) != 5:
                        continue
                    try:
                        area = int(float((row.get("AREALAND_PART") or "0").strip() or "0"))
                    except ValueError:
                        area = 0
                    prev = best_by_tract.get(tract)
                    if prev is None or area > prev[0]:
                        best_by_tract[tract] = (area, zcta)
                for tract, (_area, zcta) in best_by_tract.items():
                    crosswalk[tract] = zcta
                if crosswalk:
                    logger.info(
                        "Loaded %d tract→zip mappings from Census tract/ZCTA relationship file",
                        len(crosswalk),
                    )
                    return crosswalk
            else:
                logger.warning(
                    "Census tract/ZCTA fallback fetch failed with HTTP %s",
                    resp.status,
                )
    except Exception as e:
        logger.warning("Census tract/ZCTA fallback failed (%s)", e)

    logger.warning(
        "No tract→ZIP crosswalk available (set HLTHPRT_HUD_API_TOKEN or "
        "HLTHPRT_LODES_CROSSWALK_FILE)."
    )
    return crosswalk


def _block_to_zcta(block_geocode: str, crosswalk: dict[str, str]) -> str | None:
    """Map a 15-digit Census block geocode to a ZIP/ZCTA.

    block_geocode format: SSCCCTTTTTTBBBB (2 state + 3 county + 6 tract + 4 block)
    We use the tract portion (first 11 chars) to look up the crosswalk.
    """
    if len(block_geocode) < 11:
        return None
    tract = block_geocode[:11]
    return crosswalk.get(tract)


def _state_wac_url(state: str, year: int) -> str:
    return f"{LODES_BASE_URL}/{state}/wac/{state}_wac_S000_JT00_{year}.csv.gz"


async def _resolve_state_year(
    client,
    state: str,
    target_year: int,
    min_year: int,
) -> int | None:
    for year in range(target_year, min_year - 1, -1):
        url = _state_wac_url(state, year)
        try:
            async with client.head(url, timeout=30, allow_redirects=True) as resp:
                if resp.status == 200:
                    return year
                if resp.status in (404, 403):
                    continue
        except Exception:
            pass

        try:
            async with client.get(
                url,
                timeout=45,
                allow_redirects=True,
                headers={"Range": "bytes=0-0"},
            ) as resp:
                if resp.status in (200, 206):
                    return year
        except Exception:
            continue

    return None


async def _process_lodes_state(
    client,
    state: str,
    year: int,
    crosswalk: dict[str, str],
    stage_cls,
    batch_size: int,
):
    """Download and aggregate LODES WAC data for a single state."""
    url = _state_wac_url(state, year)
    logger.info("Downloading LODES WAC for %s: %s", state, url)

    try:
        async with client.get(url, timeout=300) as response:
            if response.status != 200:
                logger.error("Failed to fetch LODES for %s: HTTP %s", state, response.status)
                return 0

            content = await response.read()
            decompressed = gzip.decompress(content).decode("utf-8")

            reader = csv.DictReader(decompressed.splitlines())
            zcta_totals: dict[str, int] = defaultdict(int)

            for row in reader:
                block_id = row.get("w_geocode", "")
                try:
                    c000 = int(float(row.get("C000") or 0))
                except (TypeError, ValueError):
                    c000 = 0

                if not block_id or c000 == 0:
                    continue

                zcta = _block_to_zcta(block_id, crosswalk)

                if zcta:
                    zcta_totals[zcta] += c000

            now = datetime.datetime.utcnow()
            flush_batch = []
            for zcta_code, total_workers in zcta_totals.items():
                flush_batch.append({
                    "zcta_code": zcta_code[:5],
                    "total_workers": total_workers,
                    "year": year,
                    "updated_at": now,
                })
                if len(flush_batch) >= batch_size:
                    await push_objects(flush_batch, stage_cls)
                    flush_batch.clear()

            if flush_batch:
                await push_objects(flush_batch, stage_cls)

            logger.info("LODES %s: %d ZCTAs aggregated", state, len(zcta_totals))
            return len(zcta_totals)

    except Exception as e:
        logger.error("Error processing LODES for %s: %s", state, str(e))
        return 0


async def process_data(ctx, task=None):
    task = task or {}
    ctx.setdefault("context", {})

    if "test_mode" in task:
        ctx["context"]["test_mode"] = bool(task.get("test_mode"))
    test_mode = bool(ctx["context"].get("test_mode", False))

    await ensure_database(test_mode)

    import_date = ctx["import_date"]
    stage_cls = make_class(LODESWorkplaceAggregate, import_date)
    batch_size = int(os.getenv("HLTHPRT_LODES_BATCH_SIZE", str(DEFAULT_BATCH_SIZE)))

    import aiohttp
    client = aiohttp.ClientSession()
    try:
        crosswalk = await _load_tract_to_zip_crosswalk(client)
        require_crosswalk = str(
            os.getenv("HLTHPRT_LODES_REQUIRE_CROSSWALK", "true")
        ).strip().lower() not in {"0", "false", "no"}
        if require_crosswalk and not crosswalk:
            raise RuntimeError(
                "LODES crosswalk is required but unavailable. "
                "Set HLTHPRT_HUD_API_TOKEN or HLTHPRT_LODES_CROSSWALK_FILE."
            )

        states = TEST_STATES if test_mode else ALL_STATES
        total_zctas = 0
        processed_states: dict[str, int] = {}
        skipped_states: list[str] = []

        for state in states:
            resolved_year = await _resolve_state_year(client, state, LODES_TARGET_YEAR, LODES_MIN_YEAR)
            if resolved_year is None:
                skipped_states.append(state)
                logger.warning(
                    "LODES %s: no available WAC year between %s and %s; state skipped",
                    state,
                    LODES_MIN_YEAR,
                    LODES_TARGET_YEAR,
                )
                continue

            processed_states[state] = resolved_year
            total_zctas += await _process_lodes_state(
                client=client,
                state=state,
                year=resolved_year,
                crosswalk=crosswalk,
                stage_cls=stage_cls,
                batch_size=batch_size,
            )
    finally:
        await client.close()

    ctx["context"]["run"] = ctx["context"].get("run", 0) + 1
    ctx["context"]["processed_states"] = processed_states
    ctx["context"]["skipped_states"] = skipped_states
    logger.info("LODES import done: %d total ZCTA rows", total_zctas)


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

    stage_cls = make_class(LODESWorkplaceAggregate, import_date)

    await _ensure_schema_exists(db_schema)
    await db.status(f"DROP TABLE IF EXISTS {db_schema}.{stage_cls.__tablename__};")
    await db.create_table(stage_cls.__table__, checkfirst=True)

    if hasattr(stage_cls, "__my_index_elements__") and stage_cls.__my_index_elements__:
        await db.status(
            f"CREATE UNIQUE INDEX IF NOT EXISTS {stage_cls.__tablename__}_idx_primary "
            f"ON {db_schema}.{stage_cls.__tablename__} "
            f"({', '.join(stage_cls.__my_index_elements__)});"
        )

    logger.info("LODES startup ready: schema=%s import_date=%s", db_schema, import_date)


async def shutdown(ctx):
    import_date = ctx.get("import_date")
    context = ctx.get("context") or {}

    if not context.get("run"):
        logger.info("No LODES jobs ran; skipping shutdown.")
        return

    await ensure_database(bool(context.get("test_mode")))

    db_schema = os.getenv("HLTHPRT_DB_SCHEMA") if os.getenv("HLTHPRT_DB_SCHEMA") else "mrf"
    stage_cls = make_class(LODESWorkplaceAggregate, import_date)

    stage_rows = int(await db.scalar(
        f"SELECT COUNT(*) FROM {db_schema}.{stage_cls.__tablename__};"
    ) or 0)
    distinct_zctas = int(await db.scalar(
        f"SELECT COUNT(DISTINCT zcta_code) FROM {db_schema}.{stage_cls.__tablename__};"
    ) or 0)
    matched_zctas = int(await db.scalar(
        f"""
        SELECT COUNT(DISTINCT s.zcta_code)
          FROM {db_schema}.{stage_cls.__tablename__} AS s
          JOIN {db_schema}.geo_zip_lookup AS g
            ON g.zip_code = s.zcta_code
        """
    ) or 0)
    geo_match_ratio = (matched_zctas / distinct_zctas) if distinct_zctas else 0.0

    if context.get("test_mode"):
        logger.info(
            "LODES test mode: staged rows=%d distinct_zctas=%d geo_match_ratio=%.3f",
            stage_rows,
            distinct_zctas,
            geo_match_ratio,
        )
    elif stage_rows < DEFAULT_MIN_ROWS:
        raise RuntimeError(
            f"LODES stage row count {stage_rows} is below minimum {DEFAULT_MIN_ROWS}; aborting publish."
        )
    elif distinct_zctas < DEFAULT_MIN_DISTINCT_ZCTAS:
        raise RuntimeError(
            f"LODES distinct ZCTA count {distinct_zctas} is below minimum "
            f"{DEFAULT_MIN_DISTINCT_ZCTAS}; aborting publish."
        )
    elif geo_match_ratio < DEFAULT_MIN_GEO_MATCH_RATIO:
        raise RuntimeError(
            f"LODES geo match ratio {geo_match_ratio:.3f} below minimum "
            f"{DEFAULT_MIN_GEO_MATCH_RATIO:.2f}; aborting publish."
        )

    # Atomic swap: staging → live
    async with db.transaction():
        table = LODESWorkplaceAggregate.__main_table__
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

    logger.info(
        "LODES publish complete: rows=%d distinct_zctas=%d geo_match_ratio=%.3f",
        stage_rows,
        distinct_zctas,
        geo_match_ratio,
    )
    print_time_info(context.get("start"))


async def main(test_mode: bool = False):
    redis = await create_pool(
        build_redis_settings(),
        job_serializer=serialize_job,
        job_deserializer=deserialize_job,
    )
    payload = {"test_mode": bool(test_mode)}
    await redis.enqueue_job("process_data", payload, _queue_name=LODES_QUEUE_NAME)
