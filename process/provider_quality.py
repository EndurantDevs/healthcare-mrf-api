# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import csv
import datetime
import json
import logging
import secrets
import shutil
import sys
import time
from pathlib import Path
from typing import Any

import aiohttp
from aiofile import async_open
from aiocsv import AsyncDictReader
from arq import Retry, create_pool

from db.connection import db
from db.models import (
    DoctorClinicianAddress,
    EntityAddressUnified,
    GeoZipLookup,
    NPIAddress,
    NPIData,
    NPIDataTaxonomy,
    NUCCTaxonomy,
    PricingProvider,
    PricingProviderProcedure,
    PricingProviderPrescription,
    PricingProviderQualityDomain,
    PricingProviderQualityMeasure,
    PricingProviderQualityScore,
    PricingQppProvider,
    PricingQualityRun,
    PricingSviZcta,
    ProviderEnrichmentSummary,
)
from process.ext.utils import (
    download_it_and_save,
    ensure_database,
    get_http_client,
    get_import_schema,
    make_class,
)
from process.redis_config import build_redis_settings
from process.serialization import deserialize_job, serialize_job
from process.control_lifecycle import mark_control_run
from process.live_progress import enqueue_live_progress
from process.provider_quality_parts.config import (
    COHORT_MODEL_CLASS_NAMES,
    DATASET_BY_KEY,
    DATASETS,
    DOWNLOAD_RETRIES,
    IMPORT_BATCH_SIZE,
    MAT_PHASE_1_BUILD_FEATURES,
    MAT_PHASE_2_BUILD_LSH_SHARDED,
    MAT_PHASE_3_UPDATE_PROCEDURE_BUCKET,
    MAT_PHASE_4_BUILD_PEER_TARGETS,
    MAT_PHASE_5_BUILD_MEASURE_SHARDED,
    MAT_PHASE_6_BUILD_DOMAIN_SHARDED,
    MAT_PHASE_7_BUILD_SCORE_SHARDED,
    MAT_PHASE_DONE,
    MAT_PHASE_SEQUENCE,
    MAX_NPI,
    PROVIDER_QUALITY_ALLOW_DEGRADED_TEST_ONLY,
    PROVIDER_QUALITY_BENCHMARK_MODE,
    PROVIDER_QUALITY_BENCHMARK_ORDER,
    PROVIDER_QUALITY_CHUNK_TARGET_BYTES,
    PROVIDER_QUALITY_COHORT_ENABLED,
    PROVIDER_QUALITY_DEFAULT_SVI,
    PROVIDER_QUALITY_DEFER_STAGE_INDEXES,
    PROVIDER_QUALITY_DOMAIN_SHARDS,
    PROVIDER_QUALITY_DOWNLOAD_CONCURRENCY,
    PROVIDER_QUALITY_FAIL_ON_SOURCE_ERROR,
    PROVIDER_QUALITY_FINALIZE_MAX_TRIES,
    PROVIDER_QUALITY_FINISH_QUEUE_NAME,
    PROVIDER_QUALITY_FINISH_RETRY_SECONDS,
    PROVIDER_QUALITY_KEEP_WORKDIR,
    PROVIDER_QUALITY_LSH_SHARDS,
    PROVIDER_QUALITY_MATERIALIZE_SHARD_MAX_TRIES,
    PROVIDER_QUALITY_MATERIALIZE_SHARD_QUEUE_NAME,
    PROVIDER_QUALITY_MATERIALIZE_SHARDED_ENABLED,
    PROVIDER_QUALITY_MAX_YEAR,
    PROVIDER_QUALITY_MEASURE_SHARDS,
    PROVIDER_QUALITY_MIN_STATE_PEER_N,
    PROVIDER_QUALITY_MIN_YEAR,
    PROVIDER_QUALITY_MINHASH_BANDS,
    PROVIDER_QUALITY_MINHASH_NUM_PERM,
    PROVIDER_QUALITY_MINHASH_ROWS_PER_BAND,
    PROVIDER_QUALITY_MODEL_VERSION,
    PROVIDER_QUALITY_NATIONAL_MIN_PEER_N,
    PROVIDER_QUALITY_PROCEDURE_MATCH_BANDS,
    PROVIDER_QUALITY_PROCEDURE_MATCH_THRESHOLD,
    PROVIDER_QUALITY_QUEUE_NAME,
    PROVIDER_QUALITY_REDIS_TTL_SECONDS,
    PROVIDER_QUALITY_SCORE_SHARDS,
    PROVIDER_QUALITY_SHARD_PARALLELISM,
    PROVIDER_QUALITY_SHRINKAGE_ALPHA,
    PROVIDER_QUALITY_TEST_QPP_ROWS,
    PROVIDER_QUALITY_TEST_SVI_ROWS,
    PROVIDER_QUALITY_YEAR_WINDOW,
    PROVIDER_QUALITY_ZIP_MAX_RADIUS_MILES,
    PROVIDER_QUALITY_ZIP_MAX_RING,
    PROVIDER_QUALITY_ZIP_MIN_PEER_N,
    PROVIDER_QUALITY_ZIP_RADIUS_STEP_MILES,
    QPP_CSV_URL_TEMPLATE,
    ROW_PROGRESS_INTERVAL_SECONDS,
    SVI_CSV_URL_TEMPLATE,
    TEST_MAX_DOWNLOAD_BYTES,
    _benchmark_modes_for_materialization,
    _env_bool,
)
from process.provider_quality_parts.cohort_sql import (
    _cohort_sql_phase_1_build_features,
    _cohort_sql_phase_2_build_lsh_shard,
    _cohort_sql_phase_3_update_procedure_bucket,
    _cohort_sql_phase_4_build_peer_targets,
    _cohort_sql_phase_5_build_measure_shard,
    _cohort_sql_phase_6_build_domain_shard,
    _cohort_sql_phase_7_build_score_shard,
)
from process.provider_quality_parts.cohort_context import (
    _build_cohort_materialization_context as _build_cohort_materialization_context_impl,
)
from process.provider_quality_parts.execution_helpers import (
    _count_shard_rows,
    _execute_shard_sql,
    _is_deadlock_error,
    _print_row_progress,
    _push_objects_with_retry,
    _row_allowed_for_test,
    _step_end,
    _step_start,
)
from process.provider_quality_parts.lifecycle import (
    _archived_identifier,
    _format_duration_compact,
    _manifest_path,
    _materialize_shard_job_id,
    _normalize_import_id,
    _normalize_run_id,
    _npi_shard_predicate,
    _read_manifest,
    _run_dir,
    _write_manifest,
)
from process.provider_quality_parts.materialize_shards import (
    _materialize_shard_task_values,
    _run_materialize_shard_job,
    provider_quality_materialize_domain_shard,
    provider_quality_materialize_lsh_shard,
    provider_quality_materialize_measure_shard,
    provider_quality_materialize_score_shard,
)
from process.provider_quality_parts.model_helpers import (
    PricingProviderQualityFeature,
    PricingProviderQualityPeerTarget,
    PricingProviderQualityProcedureLSH,
    _build_stage_suffix,
    _chunk_job_id,
    _cohort_model_classes,
    _cohort_models_present,
    _first_existing_column,
    _materialize_reporting_years,
    _model_columns,
    _optional_column_pairs,
    _resolve_optional_model,
    _staging_classes,
)
from process.provider_quality_parts.normalize import (
    _normalize_zcta,
    _pick_first,
    _pick_first_ci,
    _to_float,
    _to_int,
    _to_npi,
)
from process.provider_quality_parts.publish_helpers import (
    _insert_run_metadata,
    _publish_by_table_rename,
)
from process.provider_quality_parts.sql_helpers import (
    _provider_class_case_sql,
    _state_code_sql,
)
from process.provider_quality_parts.state import (
    _claim_finalize_lock,
    _claim_global_finalize_lock,
    _decode_redis_str,
    _get_materialize_phase,
    _get_materialize_phase_elapsed_seconds,
    _get_materialize_progress,
    _get_run_progress,
    _increment_total_chunks,
    _init_run_state,
    _log_materialize_phase_summary,
    _mark_chunk_done,
    _mark_chunk_done_with_retry,
    _mark_materialize_done,
    _mark_materialize_failed,
    _mat_done_key,
    _mat_failed_key,
    _mat_phase_duration_key,
    _mat_phase_key,
    _mat_total_key,
    _record_materialize_phase_duration,
    _release_global_finalize_lock,
    _reset_materialize_state,
    _safe_int,
    _set_materialize_phase,
    _state_key,
)
from process.provider_quality_parts.table_helpers import (
    _build_staging_indexes,
    _ensure_indexes,
    _index_name_for_table,
    _table_columns,
    _table_exists,
)

logger = logging.getLogger(__name__)

# CSV rows can exceed Python's default field limit.
_csv_limit = sys.maxsize
while True:
    try:
        csv.field_size_limit(_csv_limit)
        break
    except OverflowError:
        _csv_limit = _csv_limit // 10


def _should_fail_on_source_error(test_mode: bool) -> bool:
    if test_mode and PROVIDER_QUALITY_ALLOW_DEGRADED_TEST_ONLY:
        return False
    return PROVIDER_QUALITY_FAIL_ON_SOURCE_ERROR


async def _mark_provider_quality_finalize_failed(run_id: str, exc: BaseException) -> None:
    if not run_id:
        return
    await mark_control_run(
        run_id,
        status="failed",
        phase_detail="provider-quality finalization failed",
        progress_message="failed",
        error={
            "code": "provider_quality_finalize_failed",
            "message": str(exc),
        },
        progress={
            "unit": "run",
            "total": 1,
            "done": 1,
            "pct": 100,
            "message": "failed",
            "phase": "provider-quality finalization failed",
        },
    )


async def _ensure_materialize_indexes(classes: dict[str, type], schema: str, *model_names: str) -> None:
    for model_name in model_names:
        model = classes.get(model_name)
        if model is None:
            continue
        await _ensure_indexes(model, schema)
        await db.status(f"ANALYZE {schema}.{model.__tablename__};")


def _provider_quality_rx_agg_table(classes: dict[str, type]) -> str:
    qpp_model = classes.get("PricingQppProvider")
    qpp_table = str(getattr(qpp_model, "__tablename__", "") or "")
    if qpp_table.startswith("pricing_qpp_provider_"):
        return qpp_table.replace("pricing_qpp_provider_", "pricing_provider_quality_rx_agg_", 1)
    return "pricing_provider_quality_rx_agg"


async def _ensure_provider_quality_rx_agg_table(classes: dict[str, type], schema: str, years: tuple[int, ...]) -> None:
    if not years:
        return
    if not await _table_exists(schema, PricingProviderPrescription.__tablename__):
        return
    table = _provider_quality_rx_agg_table(classes)
    year_values = ", ".join(str(int(year)) for year in years)
    await db.status(
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            npi BIGINT NOT NULL,
            year INTEGER NOT NULL,
            total_rx_claims DOUBLE PRECISION NOT NULL DEFAULT 0,
            total_rx_beneficiaries DOUBLE PRECISION NOT NULL DEFAULT 0
        );
        """
    )
    existing_rows = await db.scalar(f"SELECT COUNT(*) FROM {schema}.{table};")
    if _safe_int(existing_rows) > 0:
        await db.status(f"ANALYZE {schema}.{table};")
        return
    await db.status(
        f"""
        INSERT INTO {schema}.{table} (
            npi,
            year,
            total_rx_claims,
            total_rx_beneficiaries
        )
        SELECT
            r.npi,
            r.year,
            COALESCE(SUM(COALESCE(r.total_claims, 0.0)), 0.0)::float8 AS total_rx_claims,
            COALESCE(SUM(COALESCE(r.total_benes, 0.0)), 0.0)::float8 AS total_rx_beneficiaries
        FROM {schema}.{PricingProviderPrescription.__tablename__} r
        WHERE r.year IN ({year_values})
        GROUP BY r.npi, r.year;
        """
    )
    index_name = _index_name_for_table(table, f"{table}_npi_year_idx")
    await db.status(f"CREATE UNIQUE INDEX IF NOT EXISTS {index_name} ON {schema}.{table} (npi, year);")
    await db.status(f"ANALYZE {schema}.{table};")


async def _prepare_tables(stage_suffix: str, test_mode: bool) -> tuple[dict[str, type], str]:
    db_schema = get_import_schema("HLTHPRT_DB_SCHEMA", "mrf", test_mode)
    await db.status(f"CREATE SCHEMA IF NOT EXISTS {db_schema};")
    dynamic: dict[str, type] = {}
    staging_models = (
        PricingQppProvider,
        PricingSviZcta,
        PricingProviderQualityMeasure,
        PricingProviderQualityDomain,
        PricingProviderQualityScore,
        *_cohort_model_classes(),
    )

    for cls in staging_models:
        obj = make_class(cls, stage_suffix, schema_override=db_schema)
        dynamic[cls.__name__] = obj
        await db.status(f"DROP TABLE IF EXISTS {db_schema}.{obj.__tablename__};")
        await db.status(f"DROP TYPE IF EXISTS {db_schema}.{obj.__tablename__} CASCADE;")
        await db.create_table(obj.__table__, checkfirst=True)
        if not PROVIDER_QUALITY_DEFER_STAGE_INDEXES:
            await _ensure_indexes(obj, db_schema)

    for cls in (*staging_models, PricingQualityRun):
        await db.create_table(cls.__table__, checkfirst=True)

    return dynamic, db_schema


async def _download_csv_head(url: str, path: str, max_bytes: int) -> None:
    client = await get_http_client()
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


def _qpp_url_for_year(year: int) -> str:
    template = QPP_CSV_URL_TEMPLATE
    if not template:
        return ""
    if "{year}" in template:
        return template.format(year=year)
    return template


def _svi_url_for_year(year: int) -> str:
    template = SVI_CSV_URL_TEMPLATE
    if not template:
        return ""
    if "{year}" in template:
        return template.format(year=year)
    return template


def _resolve_sources(test_mode: bool = False) -> dict[str, list[dict[str, Any]]]:
    years = list(PROVIDER_QUALITY_YEAR_WINDOW)
    if test_mode and years:
        years = [max(years)]

    resolved: dict[str, list[dict[str, Any]]] = {"qpp_provider": [], "svi_zcta": []}
    for year in years:
        qpp_url = _qpp_url_for_year(year)
        if qpp_url:
            resolved["qpp_provider"].append(
                {
                    "url": qpp_url,
                    "reporting_year": year,
                    "dataset_title": "QPP Provider Performance",
                }
            )
        svi_url = _svi_url_for_year(year)
        if svi_url:
            resolved["svi_zcta"].append(
                {
                    "url": svi_url,
                    "reporting_year": year,
                    "dataset_title": "CDC SVI ZCTA",
                }
            )
    return resolved


async def _create_empty_source_file(dataset_key: str, temp_dir: str, reporting_year: int) -> str:
    Path(temp_dir).mkdir(parents=True, exist_ok=True)
    filename = f"{dataset_key}_{reporting_year}_empty.csv"
    path = str(Path(temp_dir) / filename)
    dataset = DATASET_BY_KEY[dataset_key]
    with open(path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(dataset.header)
    return path


async def _download_source_file(
    dataset_key: str,
    source: dict[str, Any],
    temp_dir: str,
    test_mode: bool,
) -> tuple[str, bool]:
    reporting_year = max(_safe_int(source.get("reporting_year"), PROVIDER_QUALITY_MIN_YEAR), 2013)
    Path(temp_dir).mkdir(parents=True, exist_ok=True)
    filename = f"{dataset_key}_{reporting_year}.csv"
    path = str(Path(temp_dir) / filename)

    url = str(source.get("url") or "").strip()
    if not url:
        msg = (
            "provider-quality source URL is missing: "
            f"dataset={dataset_key} year={reporting_year}"
        )
        if _should_fail_on_source_error(test_mode):
            raise RuntimeError(msg)
        logger.warning("%s; using empty fallback in degraded test mode", msg)
        return await _create_empty_source_file(dataset_key, temp_dir, reporting_year), True

    for attempt in range(1, DOWNLOAD_RETRIES + 1):
        try:
            if test_mode:
                await _download_csv_head(url, path, TEST_MAX_DOWNLOAD_BYTES)
            else:
                await download_it_and_save(url, path)
            return path, False
        except Retry:
            if attempt >= DOWNLOAD_RETRIES:
                break
            await asyncio.sleep(min(5 * attempt, 20))
        except Exception as exc:
            logger.warning(
                "provider-quality download failed (%s/%s) for %s: %r",
                attempt,
                DOWNLOAD_RETRIES,
                url,
                exc,
            )
            if attempt >= DOWNLOAD_RETRIES:
                break
            await asyncio.sleep(min(5 * attempt, 20))

    msg = (
        "provider-quality source download failed after retries: "
        f"dataset={dataset_key} year={reporting_year} url={url}"
    )
    if _should_fail_on_source_error(test_mode):
        raise RuntimeError(msg)
    logger.warning("%s; using empty fallback in degraded test mode", msg)
    return await _create_empty_source_file(dataset_key, temp_dir, reporting_year), True


async def _split_source_into_chunks(
    dataset_key: str,
    source_path: str,
    chunks_dir: Path,
    test_mode: bool,
) -> list[dict[str, Any]]:
    chunks_dir.mkdir(parents=True, exist_ok=True)
    dataset = DATASET_BY_KEY.get(dataset_key)
    row_limit = dataset.row_limit_test if dataset else 5000

    chunks: list[dict[str, Any]] = []
    chunk_rows: list[str] = []
    chunk_bytes = 0
    chunk_index = 0

    with open(source_path, "r", encoding="utf-8-sig", newline="") as src:
        header = src.readline()
        if not header:
            return []
        row_number = 0
        accepted = 0

        def flush_chunk() -> None:
            nonlocal chunk_rows, chunk_bytes, chunk_index
            if not chunk_rows:
                return
            chunk_path = chunks_dir / f"chunk_{chunk_index:05d}.csv"
            with open(chunk_path, "w", encoding="utf-8", newline="") as out:
                out.write(header)
                out.writelines(chunk_rows)
            chunks.append(
                {
                    "dataset_key": dataset_key,
                    "chunk_index": chunk_index,
                    "chunk_path": str(chunk_path),
                }
            )
            chunk_rows = []
            chunk_bytes = 0
            chunk_index += 1

        for line in src:
            row_number += 1
            if test_mode:
                if not _row_allowed_for_test(row_number):
                    continue
                if accepted >= row_limit:
                    break

            chunk_rows.append(line)
            chunk_bytes += len(line.encode("utf-8", errors="ignore"))
            accepted += 1
            if chunk_bytes >= PROVIDER_QUALITY_CHUNK_TARGET_BYTES:
                flush_chunk()

        flush_chunk()

    return chunks


def _extract_qpp_score(row: dict[str, Any], *keys: str) -> float | None:
    value = _pick_first_ci(row, *keys)
    if isinstance(value, str):
        cleaned = value.strip()
        if cleaned.endswith("%"):
            cleaned = cleaned[:-1]
        value = cleaned
    return _to_float(value)


async def _load_qpp_rows(path: str, qpp_cls: type, reporting_year: int, test_mode: bool) -> None:
    rows: list[dict[str, Any]] = []
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
                _print_row_progress("qpp_provider", row_number, accepted, progress_start)
                progress_last = now

            npi = _to_npi(
                _pick_first_ci(
                    row,
                    "npi",
                    "clinician_npi",
                    "clinician npi",
                    "organization_npi",
                    "group_practice_npi",
                )
            )
            if npi is None:
                continue
            year = _to_int(
                _pick_first_ci(
                    row,
                    "year",
                    "performance_year",
                    "performance year",
                    "reporting_year",
                    "reporting year",
                )
            )
            year = max(year or reporting_year, 2013)

            quality_score = _extract_qpp_score(
                row,
                "quality_score",
                "quality score",
                "quality",
                "quality_percent",
                "quality_category_score",
                "quality category score",
                "quality_performance_category_score",
                "quality performance category score",
            )
            cost_score = _extract_qpp_score(
                row,
                "cost_score",
                "cost score",
                "cost",
                "cost_percent",
                "cost_category_score",
                "cost category score",
                "cost_performance_category_score",
                "cost performance category score",
            )
            final_score = _extract_qpp_score(
                row,
                "final_score",
                "final score",
                "final",
                "final_percent",
                "final_mips_score",
                "final mips score",
                "finalscore",
                "final score performance year",
            )

            rows.append(
                {
                    "npi": npi,
                    "year": year,
                    "quality_score": quality_score,
                    "cost_score": cost_score,
                    "final_score": final_score,
                    "raw_json_text": json.dumps(row, ensure_ascii=True),
                    "updated_at": datetime.datetime.utcnow(),
                }
            )
            accepted += 1
            if len(rows) >= IMPORT_BATCH_SIZE:
                deduped = {(
                    item.get("npi"),
                    item.get("year"),
                ): item for item in rows}
                await _push_objects_with_retry(list(deduped.values()), qpp_cls)
                rows.clear()

            if test_mode and accepted >= PROVIDER_QUALITY_TEST_QPP_ROWS:
                break

    if rows:
        deduped = {(item.get("npi"), item.get("year")): item for item in rows}
        await _push_objects_with_retry(list(deduped.values()), qpp_cls)
    _print_row_progress("qpp_provider", row_number, accepted, progress_start, final=True)


async def _load_svi_rows(path: str, svi_cls: type, reporting_year: int, test_mode: bool) -> None:
    rows: list[dict[str, Any]] = []
    accepted = 0
    skipped_missing_zcta = 0
    header_keys: tuple[str, ...] = ()
    missing_zcta_examples: list[str] = []
    progress_start = time.monotonic()
    progress_last = progress_start

    async with async_open(path, "r", encoding="utf-8-sig") as handle:
        reader = AsyncDictReader(handle)
        row_number = 0
        async for row in reader:
            row_number += 1
            if row_number == 1:
                header_keys = tuple(str(key) for key in row.keys())
            now = time.monotonic()
            if now - progress_last >= ROW_PROGRESS_INTERVAL_SECONDS:
                _print_row_progress("svi_zcta", row_number, accepted, progress_start)
                progress_last = now

            zcta = _normalize_zcta(
                _pick_first(
                    row,
                    "zcta",
                    "ZCTA",
                    "ZCTA5",
                    "ZIP",
                    "zip",
                    "FIPS",
                    "fips",
                    "LOCATION",
                    "location",
                )
            )
            if not zcta:
                skipped_missing_zcta += 1
                if len(missing_zcta_examples) < 3:
                    sample = {
                        "FIPS": row.get("FIPS"),
                        "LOCATION": row.get("LOCATION"),
                        "ZCTA": row.get("ZCTA"),
                        "ZCTA5": row.get("ZCTA5"),
                        "ZIP": row.get("ZIP"),
                    }
                    compact_sample = {key: value for key, value in sample.items() if value not in (None, "")}
                    if not compact_sample:
                        compact_sample = {"keys": list(row.keys())[:6]}
                    missing_zcta_examples.append(json.dumps(compact_sample, ensure_ascii=True)[:220])
                continue
            year = _to_int(_pick_first(row, "year", "Year"))
            year = max(year or reporting_year, 2013)

            rows.append(
                {
                    "zcta": zcta,
                    "year": year,
                    "svi_overall": _to_float(_pick_first(row, "svi_overall", "RPL_THEMES", "SVI Overall")),
                    "svi_socioeconomic": _to_float(_pick_first(row, "svi_socioeconomic", "RPL_THEME1", "SVI Theme 1")),
                    "svi_household": _to_float(_pick_first(row, "svi_household", "RPL_THEME2", "SVI Theme 2")),
                    "svi_minority": _to_float(_pick_first(row, "svi_minority", "RPL_THEME3", "SVI Theme 3")),
                    "svi_housing": _to_float(_pick_first(row, "svi_housing", "RPL_THEME4", "SVI Theme 4")),
                    "updated_at": datetime.datetime.utcnow(),
                }
            )
            accepted += 1
            if len(rows) >= IMPORT_BATCH_SIZE:
                deduped = {(item.get("zcta"), item.get("year")): item for item in rows}
                await _push_objects_with_retry(list(deduped.values()), svi_cls)
                rows.clear()

            if test_mode and accepted >= PROVIDER_QUALITY_TEST_SVI_ROWS:
                break

    if rows:
        deduped = {(item.get("zcta"), item.get("year")): item for item in rows}
        await _push_objects_with_retry(list(deduped.values()), svi_cls)
    _print_row_progress("svi_zcta", row_number, accepted, progress_start, final=True)
    logger.info(
        "SVI ingest summary: path=%s year=%s parsed=%s accepted=%s skipped_missing_zcta=%s",
        path,
        reporting_year,
        row_number,
        accepted,
        skipped_missing_zcta,
    )
    if accepted == 0:
        logger.warning(
            "SVI ingest accepted=0: path=%s year=%s header_keys=%s missing_zcta_examples=%s",
            path,
            reporting_year,
            ",".join(header_keys[:12]),
            " | ".join(missing_zcta_examples) if missing_zcta_examples else "none",
        )


async def _build_cohort_materialization_context(classes: dict[str, type], schema: str) -> dict[str, Any]:
    return await _build_cohort_materialization_context_impl(
        classes,
        schema,
        table_exists=_table_exists,
        table_columns=_table_columns,
    )


async def _materialize_quality_rows_cohort(classes: dict[str, type], schema: str, run_id: str) -> None:
    ctx = await _build_cohort_materialization_context(classes, schema)

    await db.status(f"TRUNCATE TABLE {schema}.{ctx['feature_table']};")
    await db.status(f"TRUNCATE TABLE {schema}.{ctx['lsh_table']};")
    await db.status(f"TRUNCATE TABLE {schema}.{ctx['peer_target_table']};")
    await db.status(f"TRUNCATE TABLE {schema}.{ctx['measure_table']};")
    await db.status(f"TRUNCATE TABLE {schema}.{ctx['domain_table']};")
    await db.status(f"TRUNCATE TABLE {schema}.{ctx['score_table']};")

    await db.status(_cohort_sql_phase_1_build_features(ctx))

    for year in PROVIDER_QUALITY_YEAR_WINDOW:
        await db.status(
            _cohort_sql_phase_2_build_lsh_shard(ctx),
            year=year,
            shard_id=0,
            shard_count=1,
        )

    phase_3_sql = _cohort_sql_phase_3_update_procedure_bucket(ctx)
    if phase_3_sql:
        await db.status(phase_3_sql)

    await db.status(_cohort_sql_phase_4_build_peer_targets(ctx))

    for year in PROVIDER_QUALITY_YEAR_WINDOW:
        shard_params = {"year": year, "shard_id": 0, "shard_count": 1}
        await db.status(_cohort_sql_phase_5_build_measure_shard(ctx), **shard_params)
        await db.status(_cohort_sql_phase_6_build_domain_shard(ctx), **shard_params)
        await db.status(
            _cohort_sql_phase_7_build_score_shard(ctx),
            run_id=run_id,
            **shard_params,
        )
async def _materialize_quality_rows(classes: dict[str, type], schema: str, run_id: str) -> None:
    if PROVIDER_QUALITY_COHORT_ENABLED and _cohort_models_present(classes):
        await _materialize_quality_rows_cohort(classes, schema, run_id)
        return
    if PROVIDER_QUALITY_COHORT_ENABLED and not _cohort_models_present(classes):
        logger.warning(
            "Provider-quality cohort mode requested, but cohort models are unavailable; using legacy geo-only materialization",
        )

    qpp_table = classes["PricingQppProvider"].__tablename__
    svi_table = classes["PricingSviZcta"].__tablename__
    rx_table = PricingProviderPrescription.__tablename__
    geo_zip_table = GeoZipLookup.__tablename__
    measure_table = classes["PricingProviderQualityMeasure"].__tablename__
    domain_table = classes["PricingProviderQualityDomain"].__tablename__
    score_table = classes["PricingProviderQualityScore"].__tablename__
    rx_table_exists = await _table_exists(schema, rx_table)
    geo_zip_table_exists = await _table_exists(schema, geo_zip_table)
    if rx_table_exists:
        provider_rx_cte = f"""
        provider_rx AS (
            SELECT
                r.npi,
                r.year,
                COALESCE(SUM(COALESCE(r.total_claims, 0.0)), 0.0)::float8 AS total_rx_claims,
                COALESCE(SUM(COALESCE(r.total_benes, 0.0)), 0.0)::float8 AS total_rx_beneficiaries
            FROM {schema}.{rx_table} r
            WHERE r.year BETWEEN {PROVIDER_QUALITY_MIN_YEAR} AND {PROVIDER_QUALITY_MAX_YEAR}
            GROUP BY r.npi, r.year
        ),
        """
    else:
        provider_rx_cte = """
        provider_rx AS (
            SELECT
                NULL::bigint AS npi,
                NULL::int AS year,
                0.0::float8 AS total_rx_claims,
                0.0::float8 AS total_rx_beneficiaries
            WHERE FALSE
        ),
        """
    if geo_zip_table_exists:
        geo_zip_cte = f"""
        geo_zip_coords AS (
            SELECT
                z.zip_code::varchar AS zip5,
                z.latitude::float8 AS latitude,
                z.longitude::float8 AS longitude
            FROM {schema}.{geo_zip_table} z
            WHERE z.latitude IS NOT NULL
              AND z.longitude IS NOT NULL
        ),
        """
    else:
        geo_zip_cte = """
        geo_zip_coords AS (
            SELECT
                NULL::varchar AS zip5,
                NULL::float8 AS latitude,
                NULL::float8 AS longitude
            WHERE FALSE
        ),
        """
    benchmark_modes = _benchmark_modes_for_materialization(PROVIDER_QUALITY_BENCHMARK_MODE)
    benchmark_mode_values = ",\n                ".join(f"('{mode}'::varchar)" for mode in benchmark_modes)

    await db.status(f"TRUNCATE TABLE {schema}.{measure_table};")
    await db.status(f"TRUNCATE TABLE {schema}.{domain_table};")
    await db.status(f"TRUNCATE TABLE {schema}.{score_table};")

    await db.status(
        f"""
        INSERT INTO {schema}.{measure_table}
        (
            npi,
            year,
            benchmark_mode,
            measure_id,
            domain,
            observed,
            target,
            risk_ratio,
            ci75_low,
            ci75_high,
            ci90_low,
            ci90_high,
            evidence_n,
            peer_group,
            direction,
            source,
            updated_at
        )
        WITH provider_base AS (
            SELECT
                p.npi,
                p.year,
                p.zip5,
                UPPER(NULLIF(TRIM(COALESCE(p.state, '')), ''))::varchar AS state_key,
                COALESCE(p.total_services, 0.0)::float8 AS total_services,
                COALESCE(p.total_beneficiaries, 0.0)::float8 AS total_beneficiaries,
                COALESCE(p.total_allowed_amount, 0.0)::float8 AS total_allowed_amount,
                CASE
                    WHEN COALESCE(p.total_beneficiaries, 0.0) > 0
                    THEN COALESCE(p.total_services, 0.0)::float8 / NULLIF(COALESCE(p.total_beneficiaries, 0.0), 0.0)
                    ELSE NULL
                END AS utilization_rate
            FROM {schema}.{PricingProvider.__tablename__} p
            WHERE p.year BETWEEN {PROVIDER_QUALITY_MIN_YEAR} AND {PROVIDER_QUALITY_MAX_YEAR}
        ),
        {provider_rx_cte}
        {geo_zip_cte}
        provider_plus AS (
            SELECT
                b.npi,
                b.year,
                NULLIF(TRIM(COALESCE(b.zip5, '')), '')::varchar AS zip5,
                b.state_key,
                b.total_services,
                b.total_beneficiaries,
                b.total_allowed_amount,
                b.utilization_rate,
                COALESCE(q.quality_score, NULL) AS qpp_quality_score,
                COALESCE(q.cost_score, NULL) AS qpp_cost_score,
                COALESCE(rx.total_rx_claims, 0.0)::float8 AS total_rx_claims,
                COALESCE(rx.total_rx_beneficiaries, 0.0)::float8 AS total_rx_beneficiaries,
                COALESCE(s.svi_overall, {PROVIDER_QUALITY_DEFAULT_SVI}) AS svi_overall,
                COALESCE(
                    b.utilization_rate / NULLIF(1.0 + 0.2 * (COALESCE(s.svi_overall, {PROVIDER_QUALITY_DEFAULT_SVI}) - 0.5), 0.0),
                    b.utilization_rate
                ) AS utilization_adjusted,
                COALESCE(
                    b.total_allowed_amount / NULLIF(1.0 + 0.2 * (COALESCE(s.svi_overall, {PROVIDER_QUALITY_DEFAULT_SVI}) - 0.5), 0.0),
                    b.total_allowed_amount
                ) AS cost_adjusted,
                CASE
                    WHEN COALESCE(rx.total_rx_beneficiaries, 0.0) > 0
                    THEN COALESCE(rx.total_rx_claims, 0.0)::float8 / NULLIF(COALESCE(rx.total_rx_beneficiaries, 0.0), 0.0)
                    ELSE NULL
                END AS rx_claim_rate
            FROM provider_base b
            LEFT JOIN {schema}.{qpp_table} q
              ON q.npi = b.npi
             AND q.year = b.year
            LEFT JOIN provider_rx rx
              ON rx.npi = b.npi
             AND rx.year = b.year
            LEFT JOIN {schema}.{svi_table} s
              ON s.zcta = b.zip5
             AND s.year = b.year
        ),
        peers_national AS (
            SELECT
                year,
                COUNT(*)::int AS peer_n,
                AVG(NULLIF(utilization_adjusted, 0.0)) AS target_appropriateness,
                AVG(NULLIF(cost_adjusted, 0.0)) AS target_cost,
                AVG(NULLIF(qpp_quality_score, 0.0)) AS target_effectiveness,
                AVG(NULLIF(qpp_cost_score, 0.0)) AS target_qpp_cost,
                AVG(NULLIF(rx_claim_rate, 0.0)) AS target_rx_appropriateness
            FROM provider_plus
            GROUP BY year
        ),
        peers_state AS (
            SELECT
                year,
                state_key,
                COUNT(*)::int AS peer_n,
                AVG(NULLIF(utilization_adjusted, 0.0)) AS target_appropriateness,
                AVG(NULLIF(cost_adjusted, 0.0)) AS target_cost,
                AVG(NULLIF(qpp_quality_score, 0.0)) AS target_effectiveness,
                AVG(NULLIF(qpp_cost_score, 0.0)) AS target_qpp_cost,
                AVG(NULLIF(rx_claim_rate, 0.0)) AS target_rx_appropriateness
            FROM provider_plus
            WHERE state_key IS NOT NULL
            GROUP BY year, state_key
        ),
        peers_zip_exact AS (
            SELECT
                year,
                zip5,
                state_key,
                COUNT(*)::int AS peer_n,
                AVG(NULLIF(utilization_adjusted, 0.0)) AS target_appropriateness,
                AVG(NULLIF(cost_adjusted, 0.0)) AS target_cost,
                AVG(NULLIF(qpp_quality_score, 0.0)) AS target_effectiveness,
                AVG(NULLIF(qpp_cost_score, 0.0)) AS target_qpp_cost,
                AVG(NULLIF(rx_claim_rate, 0.0)) AS target_rx_appropriateness
            FROM provider_plus
            WHERE zip5 IS NOT NULL
              AND state_key IS NOT NULL
            GROUP BY year, zip5, state_key
        ),
        zip_neighbor_candidates AS (
            SELECT
                a.year,
                a.zip5 AS anchor_zip5,
                a.state_key AS anchor_state_key,
                b.zip5 AS neighbor_zip5,
                b.peer_n,
                b.target_appropriateness,
                b.target_cost,
                b.target_effectiveness,
                b.target_qpp_cost,
                b.target_rx_appropriateness,
                (
                    69.0 * SQRT(
                        POWER(za.latitude - zb.latitude, 2)
                        + POWER(
                            (za.longitude - zb.longitude) * COS(RADIANS((za.latitude + zb.latitude) / 2.0)),
                            2
                        )
                    )
                )::float8 AS distance_miles
            FROM peers_zip_exact a
            JOIN peers_zip_exact b
              ON b.year = a.year
             AND b.state_key = a.state_key
            JOIN geo_zip_coords za
              ON za.zip5 = a.zip5
            JOIN geo_zip_coords zb
              ON zb.zip5 = b.zip5
             AND ABS(za.latitude - zb.latitude) <= ({PROVIDER_QUALITY_ZIP_MAX_RADIUS_MILES} / 69.0)
        ),
        zip_neighbor_filtered AS (
            SELECT
                year,
                anchor_zip5,
                anchor_state_key,
                neighbor_zip5,
                peer_n,
                target_appropriateness,
                target_cost,
                target_effectiveness,
                target_qpp_cost,
                target_rx_appropriateness,
                CASE
                    WHEN anchor_zip5 = neighbor_zip5 THEN 0
                    ELSE LEAST(
                        {PROVIDER_QUALITY_ZIP_MAX_RING},
                        GREATEST(
                            1,
                            CEIL(distance_miles / {PROVIDER_QUALITY_ZIP_RADIUS_STEP_MILES})::int
                        )
                    )
                END AS ring
            FROM zip_neighbor_candidates
            WHERE distance_miles <= {PROVIDER_QUALITY_ZIP_MAX_RADIUS_MILES}
        ),
        zip_ring_rollup AS (
            SELECT
                year,
                anchor_zip5,
                anchor_state_key,
                ring,
                SUM(peer_n)::int AS ring_peer_n,
                SUM(COALESCE(target_appropriateness, 1.0) * peer_n)::float8 AS ring_appropr_weighted,
                SUM(COALESCE(target_cost, 1.0) * peer_n)::float8 AS ring_cost_weighted,
                SUM(COALESCE(target_effectiveness, 1.0) * peer_n)::float8 AS ring_effect_weighted,
                SUM(COALESCE(target_qpp_cost, 1.0) * peer_n)::float8 AS ring_qpp_cost_weighted,
                SUM(COALESCE(target_rx_appropriateness, 1.0) * peer_n)::float8 AS ring_rx_appropr_weighted
            FROM zip_neighbor_filtered
            GROUP BY year, anchor_zip5, anchor_state_key, ring
        ),
        zip_cumulative AS (
            SELECT
                year,
                anchor_zip5,
                anchor_state_key,
                ring,
                SUM(ring_peer_n) OVER (
                    PARTITION BY year, anchor_zip5
                    ORDER BY ring
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                )::int AS cum_peer_n,
                SUM(ring_appropr_weighted) OVER (
                    PARTITION BY year, anchor_zip5
                    ORDER BY ring
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                )::float8 AS cum_appropr_weighted,
                SUM(ring_cost_weighted) OVER (
                    PARTITION BY year, anchor_zip5
                    ORDER BY ring
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                )::float8 AS cum_cost_weighted,
                SUM(ring_effect_weighted) OVER (
                    PARTITION BY year, anchor_zip5
                    ORDER BY ring
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                )::float8 AS cum_effect_weighted,
                SUM(ring_qpp_cost_weighted) OVER (
                    PARTITION BY year, anchor_zip5
                    ORDER BY ring
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                )::float8 AS cum_qpp_cost_weighted,
                SUM(ring_rx_appropr_weighted) OVER (
                    PARTITION BY year, anchor_zip5
                    ORDER BY ring
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                )::float8 AS cum_rx_appropr_weighted
            FROM zip_ring_rollup
        ),
        zip_choice AS (
            SELECT DISTINCT ON (year, anchor_zip5)
                year,
                anchor_zip5 AS zip5,
                anchor_state_key AS state_key,
                ring AS selected_ring,
                cum_peer_n AS peer_n,
                (cum_appropr_weighted / NULLIF(cum_peer_n, 0))::float8 AS target_appropriateness,
                (cum_cost_weighted / NULLIF(cum_peer_n, 0))::float8 AS target_cost,
                (cum_effect_weighted / NULLIF(cum_peer_n, 0))::float8 AS target_effectiveness,
                (cum_qpp_cost_weighted / NULLIF(cum_peer_n, 0))::float8 AS target_qpp_cost,
                (cum_rx_appropr_weighted / NULLIF(cum_peer_n, 0))::float8 AS target_rx_appropriateness
            FROM zip_cumulative
            WHERE cum_peer_n >= {PROVIDER_QUALITY_ZIP_MIN_PEER_N}
            ORDER BY year, anchor_zip5, ring
        ),
        benchmark_modes AS (
            SELECT v.benchmark_mode
            FROM (
                VALUES
                {benchmark_mode_values}
            ) AS v(benchmark_mode)
        ),
        measure_inputs AS (
            SELECT
                p.npi,
                p.year,
                bm.benchmark_mode,
                p.total_services,
                p.total_beneficiaries,
                p.utilization_adjusted,
                p.cost_adjusted,
                p.qpp_quality_score,
                p.qpp_cost_score,
                p.rx_claim_rate,
                CASE
                    WHEN bm.benchmark_mode = 'zip'
                         AND COALESCE(zc.peer_n, 0) >= {PROVIDER_QUALITY_ZIP_MIN_PEER_N}
                    THEN zc.target_appropriateness
                    WHEN bm.benchmark_mode IN ('state', 'zip')
                         AND COALESCE(ps.peer_n, 0) >= {PROVIDER_QUALITY_MIN_STATE_PEER_N}
                    THEN ps.target_appropriateness
                    ELSE pn.target_appropriateness
                END AS target_appropriateness,
                CASE
                    WHEN bm.benchmark_mode = 'zip'
                         AND COALESCE(zc.peer_n, 0) >= {PROVIDER_QUALITY_ZIP_MIN_PEER_N}
                    THEN zc.target_cost
                    WHEN bm.benchmark_mode IN ('state', 'zip')
                         AND COALESCE(ps.peer_n, 0) >= {PROVIDER_QUALITY_MIN_STATE_PEER_N}
                    THEN ps.target_cost
                    ELSE pn.target_cost
                END AS target_cost,
                CASE
                    WHEN bm.benchmark_mode = 'zip'
                         AND COALESCE(zc.peer_n, 0) >= {PROVIDER_QUALITY_ZIP_MIN_PEER_N}
                    THEN zc.target_effectiveness
                    WHEN bm.benchmark_mode IN ('state', 'zip')
                         AND COALESCE(ps.peer_n, 0) >= {PROVIDER_QUALITY_MIN_STATE_PEER_N}
                    THEN ps.target_effectiveness
                    ELSE pn.target_effectiveness
                END AS target_effectiveness,
                CASE
                    WHEN bm.benchmark_mode = 'zip'
                         AND COALESCE(zc.peer_n, 0) >= {PROVIDER_QUALITY_ZIP_MIN_PEER_N}
                    THEN zc.target_qpp_cost
                    WHEN bm.benchmark_mode IN ('state', 'zip')
                         AND COALESCE(ps.peer_n, 0) >= {PROVIDER_QUALITY_MIN_STATE_PEER_N}
                    THEN ps.target_qpp_cost
                    ELSE pn.target_qpp_cost
                END AS target_qpp_cost,
                CASE
                    WHEN bm.benchmark_mode = 'zip'
                         AND COALESCE(zc.peer_n, 0) >= {PROVIDER_QUALITY_ZIP_MIN_PEER_N}
                    THEN zc.target_rx_appropriateness
                    WHEN bm.benchmark_mode IN ('state', 'zip')
                         AND COALESCE(ps.peer_n, 0) >= {PROVIDER_QUALITY_MIN_STATE_PEER_N}
                    THEN ps.target_rx_appropriateness
                    ELSE pn.target_rx_appropriateness
                END AS target_rx_appropriateness,
                CASE
                    WHEN bm.benchmark_mode = 'zip'
                         AND COALESCE(zc.peer_n, 0) >= {PROVIDER_QUALITY_ZIP_MIN_PEER_N}
                         AND p.zip5 IS NOT NULL
                         AND COALESCE(zc.selected_ring, 0) = 0
                    THEN ('zip:' || p.zip5)
                    WHEN bm.benchmark_mode = 'zip'
                         AND COALESCE(zc.peer_n, 0) >= {PROVIDER_QUALITY_ZIP_MIN_PEER_N}
                         AND p.zip5 IS NOT NULL
                    THEN ('zip_ring:' || p.zip5 || ':' || 'r' || zc.selected_ring::varchar)
                    WHEN bm.benchmark_mode IN ('state', 'zip')
                         AND COALESCE(ps.peer_n, 0) >= {PROVIDER_QUALITY_MIN_STATE_PEER_N}
                         AND p.state_key IS NOT NULL
                    THEN ('state:' || p.state_key)
                    ELSE 'national'
                END::varchar AS peer_group,
                GREATEST(COALESCE(p.total_services, 0.0), COALESCE(p.total_beneficiaries, 0.0), 1.0) AS evidence_n_claims,
                GREATEST(COALESCE(p.total_rx_claims, 0.0), COALESCE(p.total_rx_beneficiaries, 0.0), 1.0) AS evidence_n_rx
            FROM provider_plus p
            CROSS JOIN benchmark_modes bm
            JOIN peers_national pn
              ON pn.year = p.year
            LEFT JOIN peers_state ps
              ON ps.year = p.year
             AND ps.state_key = p.state_key
            LEFT JOIN zip_choice zc
              ON zc.year = p.year
             AND zc.zip5 = p.zip5
        ),
        expanded AS (
            SELECT
                npi,
                year,
                benchmark_mode,
                'appropriateness_utilization'::varchar AS measure_id,
                'appropriateness'::varchar AS domain,
                utilization_adjusted::float8 AS observed_value,
                target_appropriateness::float8 AS target_value,
                CASE
                    WHEN target_appropriateness IS NULL OR target_appropriateness <= 0 THEN 1.0
                    WHEN utilization_adjusted IS NULL OR utilization_adjusted <= 0 THEN 1.0
                    ELSE utilization_adjusted / target_appropriateness
                END AS rr_raw,
                evidence_n_claims AS evidence_n,
                peer_group,
                'higher_worse'::varchar AS direction,
                'claims_pricing'::varchar AS source
            FROM measure_inputs
            UNION ALL
            SELECT
                npi,
                year,
                benchmark_mode,
                'appropriateness_drug_proxy'::varchar AS measure_id,
                'appropriateness'::varchar AS domain,
                rx_claim_rate::float8 AS observed_value,
                target_rx_appropriateness::float8 AS target_value,
                CASE
                    WHEN target_rx_appropriateness IS NULL OR target_rx_appropriateness <= 0 THEN 1.0
                    WHEN rx_claim_rate IS NULL OR rx_claim_rate <= 0 THEN 1.0
                    ELSE rx_claim_rate / target_rx_appropriateness
                END AS rr_raw,
                evidence_n_rx AS evidence_n,
                peer_group,
                'higher_worse'::varchar AS direction,
                'drug_claims'::varchar AS source
            FROM measure_inputs
            UNION ALL
            SELECT
                npi,
                year,
                benchmark_mode,
                'effectiveness_qpp_quality'::varchar AS measure_id,
                'effectiveness'::varchar AS domain,
                qpp_quality_score::float8 AS observed_value,
                target_effectiveness::float8 AS target_value,
                CASE
                    WHEN qpp_quality_score IS NULL OR qpp_quality_score <= 0 THEN 1.0
                    WHEN target_effectiveness IS NULL OR target_effectiveness <= 0 THEN 1.0
                    ELSE target_effectiveness / qpp_quality_score
                END AS rr_raw,
                evidence_n_claims AS evidence_n,
                peer_group,
                'lower_worse'::varchar AS direction,
                'qpp'::varchar AS source
            FROM measure_inputs
            UNION ALL
            SELECT
                npi,
                year,
                benchmark_mode,
                'cost_qpp_component'::varchar AS measure_id,
                'cost'::varchar AS domain,
                qpp_cost_score::float8 AS observed_value,
                target_qpp_cost::float8 AS target_value,
                CASE
                    WHEN qpp_cost_score IS NULL OR qpp_cost_score <= 0 THEN 1.0
                    WHEN target_qpp_cost IS NULL OR target_qpp_cost <= 0 THEN 1.0
                    ELSE target_qpp_cost / qpp_cost_score
                END AS rr_raw,
                evidence_n_claims AS evidence_n,
                peer_group,
                'lower_worse'::varchar AS direction,
                'qpp'::varchar AS source
            FROM measure_inputs
            UNION ALL
            SELECT
                npi,
                year,
                benchmark_mode,
                'cost_allowed_amount'::varchar AS measure_id,
                'cost'::varchar AS domain,
                cost_adjusted::float8 AS observed_value,
                target_cost::float8 AS target_value,
                CASE
                    WHEN target_cost IS NULL OR target_cost <= 0 THEN 1.0
                    WHEN cost_adjusted IS NULL OR cost_adjusted <= 0 THEN 1.0
                    ELSE cost_adjusted / target_cost
                END AS rr_raw,
                evidence_n_claims AS evidence_n,
                peer_group,
                'higher_worse'::varchar AS direction,
                'claims_pricing'::varchar AS source
            FROM measure_inputs
        ),
        shrunk AS (
            SELECT
                e.*,
                LEAST(1.0, GREATEST(0.0, e.evidence_n / NULLIF(e.evidence_n + {PROVIDER_QUALITY_SHRINKAGE_ALPHA}, 0.0))) AS shrink_weight,
                GREATEST(0.0001, SQRT(1.0 / NULLIF(e.evidence_n + 1.0, 0.0))) AS stderr
            FROM expanded e
        )
        SELECT
            s.npi,
            s.year,
            s.benchmark_mode,
            s.measure_id,
            s.domain,
            s.observed_value,
            s.target_value,
            (1.0 + (s.rr_raw - 1.0) * s.shrink_weight)::float8 AS risk_ratio,
            GREATEST(0.01, (1.0 + (s.rr_raw - 1.0) * s.shrink_weight) - 1.15 * s.stderr)::float8 AS ci75_low,
            ((1.0 + (s.rr_raw - 1.0) * s.shrink_weight) + 1.15 * s.stderr)::float8 AS ci75_high,
            GREATEST(0.01, (1.0 + (s.rr_raw - 1.0) * s.shrink_weight) - 1.64 * s.stderr)::float8 AS ci90_low,
            ((1.0 + (s.rr_raw - 1.0) * s.shrink_weight) + 1.64 * s.stderr)::float8 AS ci90_high,
            ROUND(s.evidence_n)::int,
            s.peer_group,
            s.direction,
            s.source,
            NOW() AS updated_at
        FROM shrunk s;
        """
    )

    await db.status(
        f"""
        INSERT INTO {schema}.{domain_table}
        (
            npi,
            year,
            benchmark_mode,
            domain,
            risk_ratio,
            score_0_100,
            ci75_low,
            ci75_high,
            ci90_low,
            ci90_high,
            evidence_n,
            measure_count,
            updated_at
        )
        SELECT
            m.npi,
            m.year,
            m.benchmark_mode,
            m.domain,
            EXP(AVG(LN(GREATEST(m.risk_ratio, 0.0001))))::float8 AS risk_ratio,
            ROUND((LEAST(100.0, GREATEST(0.0, 50.0 - 125.0 * (EXP(AVG(LN(GREATEST(m.risk_ratio, 0.0001)))) - 1.0))))::numeric, 2)::float8 AS score_0_100,
            EXP(AVG(LN(GREATEST(m.ci75_low, 0.0001))))::float8 AS ci75_low,
            EXP(AVG(LN(GREATEST(m.ci75_high, 0.0001))))::float8 AS ci75_high,
            EXP(AVG(LN(GREATEST(m.ci90_low, 0.0001))))::float8 AS ci90_low,
            EXP(AVG(LN(GREATEST(m.ci90_high, 0.0001))))::float8 AS ci90_high,
            SUM(COALESCE(m.evidence_n, 0))::int AS evidence_n,
            COUNT(*)::int AS measure_count,
            NOW() AS updated_at
        FROM {schema}.{measure_table} m
        GROUP BY m.npi, m.year, m.benchmark_mode, m.domain;
        """
    )

    await db.status(
        f"""
        INSERT INTO {schema}.{score_table}
        (
            npi,
            year,
            model_version,
            benchmark_mode,
            risk_ratio_point,
            ci75_low,
            ci75_high,
            ci90_low,
            ci90_high,
            score_0_100,
            tier,
            borderline_status,
            low_score_threshold_failed,
            low_confidence_threshold_failed,
            high_score_threshold_passed,
            high_confidence_threshold_passed,
            estimated_cost_level,
            run_id,
            updated_at
        )
        WITH domain_pivot AS (
            SELECT
                d.npi,
                d.year,
                d.benchmark_mode,
                MAX(CASE WHEN d.domain = 'appropriateness' THEN d.risk_ratio END) AS rr_appropr,
                MAX(CASE WHEN d.domain = 'effectiveness' THEN d.risk_ratio END) AS rr_effect,
                MAX(CASE WHEN d.domain = 'cost' THEN d.risk_ratio END) AS rr_cost,
                MAX(CASE WHEN d.domain = 'appropriateness' THEN d.ci75_low END) AS ci75_appropr_low,
                MAX(CASE WHEN d.domain = 'effectiveness' THEN d.ci75_low END) AS ci75_effect_low,
                MAX(CASE WHEN d.domain = 'cost' THEN d.ci75_low END) AS ci75_cost_low,
                MAX(CASE WHEN d.domain = 'appropriateness' THEN d.ci75_high END) AS ci75_appropr_high,
                MAX(CASE WHEN d.domain = 'effectiveness' THEN d.ci75_high END) AS ci75_effect_high,
                MAX(CASE WHEN d.domain = 'cost' THEN d.ci75_high END) AS ci75_cost_high,
                MAX(CASE WHEN d.domain = 'appropriateness' THEN d.ci90_low END) AS ci90_appropr_low,
                MAX(CASE WHEN d.domain = 'effectiveness' THEN d.ci90_low END) AS ci90_effect_low,
                MAX(CASE WHEN d.domain = 'cost' THEN d.ci90_low END) AS ci90_cost_low,
                MAX(CASE WHEN d.domain = 'appropriateness' THEN d.ci90_high END) AS ci90_appropr_high,
                MAX(CASE WHEN d.domain = 'effectiveness' THEN d.ci90_high END) AS ci90_effect_high,
                MAX(CASE WHEN d.domain = 'cost' THEN d.ci90_high END) AS ci90_cost_high
            FROM {schema}.{domain_table} d
            GROUP BY d.npi, d.year, d.benchmark_mode
        ),
        blended AS (
            SELECT
                p.npi,
                p.year,
                p.benchmark_mode,
                COALESCE(p.rr_appropr, 1.0) AS rr_appropr,
                COALESCE(p.rr_effect, 1.0) AS rr_effect,
                COALESCE(p.rr_cost, 1.0) AS rr_cost,
                SQRT(COALESCE(p.rr_appropr, 1.0) * COALESCE(p.rr_effect, 1.0)) AS rr_clinical,
                EXP(
                    0.5 * LN(GREATEST(SQRT(COALESCE(p.rr_appropr, 1.0) * COALESCE(p.rr_effect, 1.0)), 0.0001))
                    + 0.5 * LN(GREATEST(COALESCE(p.rr_cost, 1.0), 0.0001))
                ) AS rr_overall,
                EXP(
                    0.5 * LN(GREATEST(SQRT(COALESCE(p.ci75_appropr_low, 1.0) * COALESCE(p.ci75_effect_low, 1.0)), 0.0001))
                    + 0.5 * LN(GREATEST(COALESCE(p.ci75_cost_low, 1.0), 0.0001))
                ) AS ci75_low,
                EXP(
                    0.5 * LN(GREATEST(SQRT(COALESCE(p.ci75_appropr_high, 1.0) * COALESCE(p.ci75_effect_high, 1.0)), 0.0001))
                    + 0.5 * LN(GREATEST(COALESCE(p.ci75_cost_high, 1.0), 0.0001))
                ) AS ci75_high,
                EXP(
                    0.5 * LN(GREATEST(SQRT(COALESCE(p.ci90_appropr_low, 1.0) * COALESCE(p.ci90_effect_low, 1.0)), 0.0001))
                    + 0.5 * LN(GREATEST(COALESCE(p.ci90_cost_low, 1.0), 0.0001))
                ) AS ci90_low,
                EXP(
                    0.5 * LN(GREATEST(SQRT(COALESCE(p.ci90_appropr_high, 1.0) * COALESCE(p.ci90_effect_high, 1.0)), 0.0001))
                    + 0.5 * LN(GREATEST(COALESCE(p.ci90_cost_high, 1.0), 0.0001))
                ) AS ci90_high
            FROM domain_pivot p
        ),
        checks AS (
            SELECT
                b.*,
                (b.rr_overall >= 1.12)::boolean AS low_score_check,
                (b.ci90_low >= 1.08)::boolean AS low_confidence_check,
                (b.rr_overall <= 0.88)::boolean AS high_score_check,
                (b.ci75_high < 1.0)::boolean AS high_confidence_check
            FROM blended b
        )
        SELECT
            c.npi,
            c.year,
            '{PROVIDER_QUALITY_MODEL_VERSION}'::varchar AS model_version,
            c.benchmark_mode::varchar AS benchmark_mode,
            c.rr_overall::float8 AS risk_ratio_point,
            c.ci75_low::float8,
            c.ci75_high::float8,
            c.ci90_low::float8,
            c.ci90_high::float8,
            ROUND((LEAST(100.0, GREATEST(0.0, 50.0 - 125.0 * (c.rr_overall - 1.0))))::numeric, 2)::float8 AS score_0_100,
            CASE
                WHEN c.low_score_check AND c.low_confidence_check THEN 'low'
                WHEN (c.low_score_check::int + c.low_confidence_check::int) = 1 THEN 'acceptable'
                WHEN c.high_score_check AND c.high_confidence_check THEN 'high'
                ELSE 'acceptable'
            END::varchar AS tier,
            ((c.low_score_check::int + c.low_confidence_check::int) = 1)::boolean AS borderline_status,
            c.low_score_check,
            c.low_confidence_check,
            c.high_score_check,
            c.high_confidence_check,
            CASE
                WHEN c.rr_cost <= 0.80 THEN '$'
                WHEN c.rr_cost <= 0.90 THEN '$$'
                WHEN c.rr_cost <= 1.10 THEN '$$$'
                WHEN c.rr_cost <= 1.25 THEN '$$$$'
                ELSE '$$$$$'
            END::varchar AS estimated_cost_level,
            CAST(:run_id AS varchar) AS run_id,
            NOW() AS updated_at
        FROM checks c;
        """,
        run_id=run_id,
    )


async def _enqueue_materialize_phase_shards(
    redis,
    *,
    run_id: str,
    phase: str,
    years: tuple[int, ...],
    shard_count: int,
    stage_suffix: str,
    schema: str,
    test_mode: bool,
    job_name: str,
) -> int:
    payloads: list[dict[str, Any]] = []
    for year in years:
        for shard_id in range(shard_count):
            payloads.append(
                {
                    "run_id": run_id,
                    "stage_suffix": stage_suffix,
                    "schema": schema,
                    "test_mode": bool(test_mode),
                    "year": year,
                    "shard_id": shard_id,
                    "shard_count": shard_count,
                }
            )

    await _set_materialize_phase(redis, run_id, phase, total=len(payloads))
    logger.info(
        "provider quality materialize enqueue run_id=%s phase=%s shards=%s years=%s job=%s",
        run_id,
        phase,
        len(payloads),
        ",".join(str(y) for y in years),
        job_name,
    )
    for payload in payloads:
        year = _safe_int(payload.get("year"), PROVIDER_QUALITY_MIN_YEAR)
        shard_id = _safe_int(payload.get("shard_id"), 0)
        await redis.enqueue_job(
            job_name,
            payload,
            _queue_name=PROVIDER_QUALITY_MATERIALIZE_SHARD_QUEUE_NAME,
            _job_id=_materialize_shard_job_id(run_id, phase, year, shard_id),
        )
    return len(payloads)


async def _wait_for_materialize_phase_completion(redis, run_id: str, phase: str) -> None:
    total, done, failed = await _get_materialize_progress(redis, run_id)
    elapsed_sec = await _get_materialize_phase_elapsed_seconds(redis, run_id)
    pending = max(total - done - failed, 0)
    progress_pct = (float(done) * 100.0 / float(total)) if total > 0 else 100.0
    shards_per_sec = (float(done) / elapsed_sec) if elapsed_sec > 0.0 else 0.0
    eta_sec = (float(pending) / shards_per_sec) if shards_per_sec > 0.0 else 0.0
    eta_text = _format_duration_compact(eta_sec) if shards_per_sec > 0.0 else "n/a"
    elapsed_text = _format_duration_compact(elapsed_sec)
    if failed > 0:
        raise RuntimeError(
            "Provider quality materialization failed in "
            f"{phase}: failed_shards={failed} total={total} done={done} "
            f"elapsed={elapsed_text}"
        )
    if done < total:
        enqueue_live_progress(
            run_id=run_id,
            importer="provider-quality",
            status="finalizing",
            phase=f"provider-quality materialize {phase}",
            unit="shards",
            done=done,
            total=total,
            eta_seconds=eta_sec if shards_per_sec > 0.0 else None,
            message=f"materializing {phase} {done}/{total} shards",
        )
        print(
            f"[finalize] materialize progress phase={phase} done={done}/{total} "
            f"pending={pending} failed={failed} pct={progress_pct:.1f}% "
            f"elapsed={elapsed_text} rate={shards_per_sec:.4f}/s eta={eta_text} run_id={run_id}",
            flush=True,
        )
        raise Retry(defer=PROVIDER_QUALITY_FINISH_RETRY_SECONDS)
    await _log_materialize_phase_summary(redis, run_id, phase, total=total, done=done, failed=failed)


async def _materialize_quality_rows_sharded(
    redis,
    *,
    classes: dict[str, type],
    schema: str,
    run_id: str,
    stage_suffix: str,
    test_mode: bool,
    manifest: dict[str, Any],
) -> None:
    years = _materialize_reporting_years(manifest)
    await _ensure_provider_quality_rx_agg_table(classes, schema, years)
    context = await _build_cohort_materialization_context(classes, schema)
    phase = await _get_materialize_phase(redis, run_id)
    if phase and phase not in MAT_PHASE_SEQUENCE:
        await _reset_materialize_state(redis, run_id)
        phase = ""

    if not phase:
        await _set_materialize_phase(redis, run_id, MAT_PHASE_1_BUILD_FEATURES, total=0)
        phase = MAT_PHASE_1_BUILD_FEATURES

    if phase == MAT_PHASE_1_BUILD_FEATURES:
        logger.info("provider quality materialize phase start run_id=%s phase=%s", run_id, phase)
        await db.status(f"TRUNCATE TABLE {schema}.{context['feature_table']};")
        await db.status(f"TRUNCATE TABLE {schema}.{context['lsh_table']};")
        await db.status(f"TRUNCATE TABLE {schema}.{context['peer_target_table']};")
        await db.status(f"TRUNCATE TABLE {schema}.{context['measure_table']};")
        await db.status(f"TRUNCATE TABLE {schema}.{context['domain_table']};")
        await db.status(f"TRUNCATE TABLE {schema}.{context['score_table']};")
        await db.status(_cohort_sql_phase_1_build_features(context))
        await _ensure_materialize_indexes(classes, schema, "PricingProviderQualityFeature")
        await _enqueue_materialize_phase_shards(
            redis,
            run_id=run_id,
            phase=MAT_PHASE_2_BUILD_LSH_SHARDED,
            years=years,
            shard_count=PROVIDER_QUALITY_LSH_SHARDS,
            stage_suffix=stage_suffix,
            schema=schema,
            test_mode=test_mode,
            job_name="provider_quality_materialize_lsh_shard",
        )
        raise Retry(defer=PROVIDER_QUALITY_FINISH_RETRY_SECONDS)

    if phase == MAT_PHASE_2_BUILD_LSH_SHARDED:
        logger.info("provider quality materialize phase wait run_id=%s phase=%s", run_id, phase)
        await _wait_for_materialize_phase_completion(redis, run_id, phase)
        await _ensure_materialize_indexes(classes, schema, "PricingProviderQualityProcedureLSH")
        await _set_materialize_phase(redis, run_id, MAT_PHASE_3_UPDATE_PROCEDURE_BUCKET, total=0)
        phase = MAT_PHASE_3_UPDATE_PROCEDURE_BUCKET

    if phase == MAT_PHASE_3_UPDATE_PROCEDURE_BUCKET:
        logger.info("provider quality materialize phase start run_id=%s phase=%s", run_id, phase)
        procedure_bucket_sql = _cohort_sql_phase_3_update_procedure_bucket(context)
        if procedure_bucket_sql:
            await db.status(procedure_bucket_sql)
        await db.status(f"ANALYZE {schema}.{context['feature_table']};")
        await _set_materialize_phase(redis, run_id, MAT_PHASE_4_BUILD_PEER_TARGETS, total=0)
        phase = MAT_PHASE_4_BUILD_PEER_TARGETS

    if phase == MAT_PHASE_4_BUILD_PEER_TARGETS:
        logger.info("provider quality materialize phase start run_id=%s phase=%s", run_id, phase)
        await db.status(_cohort_sql_phase_4_build_peer_targets(context))
        await _ensure_materialize_indexes(classes, schema, "PricingProviderQualityPeerTarget")
        await _enqueue_materialize_phase_shards(
            redis,
            run_id=run_id,
            phase=MAT_PHASE_5_BUILD_MEASURE_SHARDED,
            years=years,
            shard_count=PROVIDER_QUALITY_MEASURE_SHARDS,
            stage_suffix=stage_suffix,
            schema=schema,
            test_mode=test_mode,
            job_name="provider_quality_materialize_measure_shard",
        )
        raise Retry(defer=PROVIDER_QUALITY_FINISH_RETRY_SECONDS)

    if phase == MAT_PHASE_5_BUILD_MEASURE_SHARDED:
        logger.info("provider quality materialize phase wait run_id=%s phase=%s", run_id, phase)
        await _wait_for_materialize_phase_completion(redis, run_id, phase)
        await _ensure_materialize_indexes(classes, schema, "PricingProviderQualityMeasure")
        await _enqueue_materialize_phase_shards(
            redis,
            run_id=run_id,
            phase=MAT_PHASE_6_BUILD_DOMAIN_SHARDED,
            years=years,
            shard_count=PROVIDER_QUALITY_DOMAIN_SHARDS,
            stage_suffix=stage_suffix,
            schema=schema,
            test_mode=test_mode,
            job_name="provider_quality_materialize_domain_shard",
        )
        raise Retry(defer=PROVIDER_QUALITY_FINISH_RETRY_SECONDS)

    if phase == MAT_PHASE_6_BUILD_DOMAIN_SHARDED:
        logger.info("provider quality materialize phase wait run_id=%s phase=%s", run_id, phase)
        await _wait_for_materialize_phase_completion(redis, run_id, phase)
        await _ensure_materialize_indexes(classes, schema, "PricingProviderQualityDomain")
        await _enqueue_materialize_phase_shards(
            redis,
            run_id=run_id,
            phase=MAT_PHASE_7_BUILD_SCORE_SHARDED,
            years=years,
            shard_count=PROVIDER_QUALITY_SCORE_SHARDS,
            stage_suffix=stage_suffix,
            schema=schema,
            test_mode=test_mode,
            job_name="provider_quality_materialize_score_shard",
        )
        raise Retry(defer=PROVIDER_QUALITY_FINISH_RETRY_SECONDS)

    if phase == MAT_PHASE_7_BUILD_SCORE_SHARDED:
        logger.info("provider quality materialize phase wait run_id=%s phase=%s", run_id, phase)
        await _wait_for_materialize_phase_completion(redis, run_id, phase)
        await _set_materialize_phase(redis, run_id, MAT_PHASE_DONE, total=0)
        return

    if phase != MAT_PHASE_DONE:
        raise Retry(defer=PROVIDER_QUALITY_FINISH_RETRY_SECONDS)




async def provider_quality_start(ctx, task: dict[str, Any] | None = None, **_kwargs: Any) -> dict[str, Any]:
    task = task or {}
    test_mode = bool(task.get("test_mode", False))
    import_id_val = _normalize_import_id(task.get("import_id"))
    run_id = _normalize_run_id(task.get("run_id"))
    stage_suffix = _build_stage_suffix(import_id_val, run_id)
    redis = ctx.get("redis")
    if redis is None:
        raise RuntimeError("ARQ redis context is unavailable for provider quality start job.")

    total_start = _step_start(
        "provider-quality enqueue+split "
        f"(test_mode={test_mode}, import_id={import_id_val}, run_id={run_id}, stage={stage_suffix})"
    )
    await mark_control_run(
        run_id,
        status="running",
        phase_detail="provider-quality split running",
        progress_message="splitting source files",
    )

    await ensure_database(test_mode)

    t = _step_start("prepare staging tables")
    _classes, schema = await _prepare_tables(stage_suffix, test_mode)
    _step_end("prepare staging tables", t)

    t = _step_start("resolve provider quality sources")
    sources = _resolve_sources(test_mode=test_mode)
    _step_end("resolve provider quality sources", t)

    work_dir = _run_dir(import_id_val, run_id)
    downloads_dir = work_dir / "downloads"
    chunks_root = work_dir / "chunks"
    chunks_root.mkdir(parents=True, exist_ok=True)

    chunks: list[dict[str, Any]] = []
    local_paths: dict[str, list[str]] = {}
    degraded_sources: list[dict[str, Any]] = []

    await _init_run_state(redis, run_id, 0)

    semaphore = asyncio.Semaphore(PROVIDER_QUALITY_DOWNLOAD_CONCURRENCY)

    async def _download_split_source(dataset_key: str, source: dict[str, Any], source_index: int):
        async with semaphore:
            reporting_year = max(_safe_int(source.get("reporting_year"), PROVIDER_QUALITY_MIN_YEAR), 2013)
            step = _step_start(f"download+split {dataset_key} year={reporting_year}")
            try:
                local_path, degraded = await _download_source_file(dataset_key, source, str(downloads_dir), test_mode)
                split_chunks = await _split_source_into_chunks(
                    dataset_key=dataset_key,
                    source_path=local_path,
                    chunks_dir=chunks_root / dataset_key,
                    test_mode=test_mode,
                )
                for chunk in split_chunks:
                    chunk["reporting_year"] = reporting_year
                    chunk["source_index"] = source_index
                return dataset_key, local_path, split_chunks, degraded, reporting_year
            finally:
                _step_end(f"download+split {dataset_key} year={reporting_year}", step)

    dataset_tasks = [
        asyncio.create_task(_download_split_source(dataset.key, source, idx))
        for dataset in DATASETS
        for idx, source in enumerate(sources.get(dataset.key, []))
    ]

    t = _step_start("download+split+enqueue chunks (streaming)")
    try:
        try:
            for completed in asyncio.as_completed(dataset_tasks):
                dataset_key, local_path, split_chunks, degraded, reporting_year = await completed
                local_paths.setdefault(dataset_key, []).append(local_path)
                if degraded:
                    degraded_sources.append({"dataset_key": dataset_key, "reporting_year": reporting_year})
                for chunk in split_chunks:
                    reporting_year = max(_safe_int(chunk.get("reporting_year"), PROVIDER_QUALITY_MIN_YEAR), 2013)
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
                        "provider_quality_process_chunk",
                        payload,
                        _queue_name=PROVIDER_QUALITY_QUEUE_NAME,
                        _job_id=_chunk_job_id(run_id, chunk["dataset_key"], source_index, reporting_year, chunk_index),
                    )
                if split_chunks:
                    await _increment_total_chunks(redis, run_id, len(split_chunks))
                    chunks.extend(split_chunks)
        except Exception:
            failure_manifest = {
                "year": max(PROVIDER_QUALITY_YEAR_WINDOW) if PROVIDER_QUALITY_YEAR_WINDOW else PROVIDER_QUALITY_MIN_YEAR,
                "sources": sources,
            }
            try:
                await _insert_run_metadata(
                    schema,
                    run_id,
                    import_id_val,
                    failure_manifest,
                    status="failed_source",
                )
            except Exception as metadata_exc:
                logger.warning("Failed to write provider-quality failed_source metadata: %r", metadata_exc)
            raise
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
        "local_paths": local_paths,
        "degraded_sources": degraded_sources,
        "chunks": chunks,
        "total_chunks": len(chunks),
        "total_rows": 0,
        "year": max(PROVIDER_QUALITY_YEAR_WINDOW) if PROVIDER_QUALITY_YEAR_WINDOW else PROVIDER_QUALITY_MIN_YEAR,
        "created_at": datetime.datetime.utcnow().isoformat(),
        "work_dir": str(work_dir),
    }

    manifest_path = _manifest_path(work_dir)
    _write_manifest(manifest_path, manifest)

    await redis.enqueue_job(
        "provider_quality_finalize",
        {
            "import_id": import_id_val,
            "run_id": run_id,
            "stage_suffix": stage_suffix,
            "schema": schema,
            "manifest_path": str(manifest_path),
            "test_mode": test_mode,
        },
        _queue_name=PROVIDER_QUALITY_FINISH_QUEUE_NAME,
        _job_id=f"provider_quality_finalize_{run_id}",
    )

    _step_end(
        "provider-quality enqueue+split "
        f"(test_mode={test_mode}, import_id={import_id_val}, run_id={run_id}, stage={stage_suffix})",
        total_start,
    )
    await mark_control_run(
        run_id,
        status="running",
        phase_detail="provider-quality chunks queued",
        progress_message="chunks queued",
        metrics={"total_chunks": len(chunks), "stage_suffix": stage_suffix},
        progress={
            "unit": "chunks",
            "total": len(chunks),
            "done": 0,
            "pct": 0,
            "message": "chunks queued",
            "phase": "provider-quality chunks queued",
        },
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


async def provider_quality_process_chunk(ctx, task: dict[str, Any] | None = None, **_kwargs: Any) -> dict[str, Any]:
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
    year = max(_safe_int(task.get("reporting_year"), PROVIDER_QUALITY_MIN_YEAR), 2013)

    if dataset_key == "qpp_provider":
        await _load_qpp_rows(chunk_path, classes["PricingQppProvider"], year, test_mode=test_mode)
    elif dataset_key == "svi_zcta":
        await _load_svi_rows(chunk_path, classes["PricingSviZcta"], year, test_mode=test_mode)
    else:
        raise RuntimeError(f"Unsupported dataset_key for chunk processing: {dataset_key}")

    redis = ctx.get("redis")
    if redis is not None and run_id:
        await _mark_chunk_done_with_retry(redis, run_id, chunk_id)
        total_chunks, done_chunks = await _get_run_progress(redis, run_id, 0)
        enqueue_live_progress(
            run_id=run_id,
            importer="provider-quality",
            status="running",
            phase="provider-quality chunks running",
            unit="chunks",
            done=done_chunks,
            total=total_chunks,
            message=f"processed {done_chunks}/{total_chunks} chunks",
        )

    return {
        "ok": True,
        "chunk_id": chunk_id,
        "dataset_key": dataset_key,
    }


async def provider_quality_finalize(ctx, task: dict[str, Any] | None = None, **_kwargs: Any) -> dict[str, Any]:
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

    global_lock_acquired = False
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
            enqueue_live_progress(
                run_id=run_id,
                importer="provider-quality",
                status="running",
                phase="provider-quality chunks running",
                unit="chunks",
                done=done_chunks,
                total=total_chunks,
                message=f"waiting for chunks {done_chunks}/{total_chunks}",
            )
            print(
                f"[finalize] waiting for chunks: done={done_chunks}/{total_chunks} run_id={run_id}",
                flush=True,
            )
            raise Retry(defer=PROVIDER_QUALITY_FINISH_RETRY_SECONDS)

        if not await _claim_finalize_lock(redis, run_id):
            raise Retry(defer=PROVIDER_QUALITY_FINISH_RETRY_SECONDS)
        if not await _claim_global_finalize_lock(redis, run_id):
            raise Retry(defer=PROVIDER_QUALITY_FINISH_RETRY_SECONDS)
        global_lock_acquired = True
        await mark_control_run(
            run_id,
            status="finalizing",
            phase_detail="provider-quality finalizing",
            progress_message="finalizing",
            progress={
                "unit": "chunks",
                "total": total_chunks,
                "done": done_chunks,
                "pct": 99,
                "message": "finalizing",
                "phase": "provider-quality finalizing",
            },
        )

    try:
        classes = _staging_classes(stage_suffix, schema)

        t = _step_start("materialize provider quality rows")
        use_sharded_materialization = (
            PROVIDER_QUALITY_COHORT_ENABLED
            and PROVIDER_QUALITY_MATERIALIZE_SHARDED_ENABLED
            and _cohort_models_present(classes)
        )
        if use_sharded_materialization:
            if redis is None or not run_id:
                raise RuntimeError("Sharded provider-quality materialization requires redis and run_id.")
            await _materialize_quality_rows_sharded(
                redis,
                classes=classes,
                schema=schema,
                run_id=run_id,
                stage_suffix=stage_suffix,
                test_mode=test_mode,
                manifest=manifest,
            )
        else:
            await _materialize_quality_rows(classes, schema, run_id)
        _step_end("materialize provider quality rows", t)

        if PROVIDER_QUALITY_DEFER_STAGE_INDEXES:
            t = _step_start("build provider quality staging indexes")
            await _build_staging_indexes(classes, schema)
            _step_end("build provider quality staging indexes", t)

        t = _step_start("publish provider quality staging -> final")
        await _publish_by_table_rename(classes, schema)
        _step_end("publish provider quality staging -> final", t)

        t = _step_start("write provider quality run metadata")
        run_status = "degraded_test" if bool(manifest.get("degraded_sources")) else "published"
        await _insert_run_metadata(schema, run_id, import_id_val, manifest, status=run_status)
        _step_end("write provider quality run metadata", t)

        if redis is not None and run_id:
            await redis.set(_state_key(run_id, "finalized"), "1", ex=PROVIDER_QUALITY_REDIS_TTL_SECONDS)
            await redis.expire(_state_key(run_id, "finalized"), PROVIDER_QUALITY_REDIS_TTL_SECONDS)
    except Retry:
        raise
    except Exception as exc:
        logger.exception("provider quality finalize failed run_id=%s import_id=%s", run_id, import_id_val)
        await _mark_provider_quality_finalize_failed(run_id, exc)
        raise
    finally:
        if redis is not None and run_id and global_lock_acquired:
            await _release_global_finalize_lock(redis, run_id)

    if manifest:
        run_work_dir = Path(manifest.get("work_dir", ""))
        if run_work_dir and run_work_dir.exists() and not PROVIDER_QUALITY_KEEP_WORKDIR:
            shutil.rmtree(run_work_dir, ignore_errors=True)

    logger.info(
        "provider quality import finalized: test_mode=%s import_id=%s run_id=%s",
        test_mode,
        import_id_val,
        run_id,
    )
    await mark_control_run(
        run_id,
        status="succeeded",
        phase_detail="provider-quality finalized",
        progress_message="succeeded",
        metrics={"stage_suffix": stage_suffix, "schema": schema},
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
        "provider_quality_start",
        payload,
        _queue_name=PROVIDER_QUALITY_QUEUE_NAME,
        _job_id=f"provider_quality_start_{run_id}",
    )
    stage_suffix = _build_stage_suffix(_normalize_import_id(import_id), run_id)
    print(
        f"Queued provider-quality run: import_id={_normalize_import_id(import_id)} run_id={run_id} "
        f"stage={stage_suffix} test_mode={bool(test_mode)}",
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
        "provider_quality_finalize",
        payload,
        _queue_name=PROVIDER_QUALITY_FINISH_QUEUE_NAME,
        _job_id=f"provider_quality_finalize_{run_id}_{secrets.token_hex(4)}",
    )
    print(
        f"Queued provider-quality finalize: import_id={_normalize_import_id(import_id)} run_id={run_id} stage={stage_suffix}",
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
    "provider_quality_start",
    "provider_quality_process_chunk",
    "provider_quality_materialize_lsh_shard",
    "provider_quality_materialize_measure_shard",
    "provider_quality_materialize_domain_shard",
    "provider_quality_materialize_score_shard",
    "provider_quality_finalize",
    "finish_main",
]
