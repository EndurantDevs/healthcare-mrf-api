# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
# pylint: disable=broad-exception-caught,too-many-branches,too-many-locals,too-many-statements,too-many-lines

import datetime
import hashlib
import json
import logging
import os
import tempfile
import time
from collections import OrderedDict
from dataclasses import asdict
from pathlib import Path
from typing import Any

import ijson
import asyncio
import concurrent.futures
import multiprocessing

from db.connection import db
from db.models import (
    ImportLog,
    PTGAllowedItem,
    PTGAllowedPayment,
    PTGAllowedProviderPayment,
    PTGBillingCode,
    PTG2CurrentPlanSource,
    PTG2CurrentSnapshot,
    PTG2CurrentSourceSnapshot,
    PTG2FactChunk,
    PTG2GCCandidate,
    PTG2ImportJob,
    PTG2ImportRun,
    PTG2LocationSet,
    PTG2LocationSetMember,
    PTG2Plan,
    PTG2PlanAlias,
    PTG2PlanMonth,
    PTG2PlanRateSet,
    PTG2PriceCodeSet,
    PTG2PriceSet,
    PTG2PriceSetEntry,
    PTG2ProviderEntryComponent,
    PTG2ProviderSetEntry,
    PTG2ProviderSetMember,
    PTG2RateSet,
    PTG2RateSetContext,
    PTG2RelatedCodeSet,
    PTG2ServingRate,
    PTG2ServingRateCompact,
    PTG2Snapshot,
    PTG2SourceCatalog,
    PTG2SourceTrace,
    PTG2SourceTraceSet,
    PTGFile,
    PTGInNetworkItem,
    PTGNegotiatedPrice,
    PTGNegotiatedRate,
    PTGProviderGroup,
)
from process.ext.utils import (
    ensure_database,
    flush_error_log,
    get_import_schema,
    log_error,
    make_class,
    push_objects,
    return_checksum,
)
from process.ptg_parts.artifacts import (
    PTG2ArtifactStore,
    _hash_existing_file_into,
    _load_completed_ranges,
    _range_sidecar_path,
    _safe_url_suffix,
    _write_completed_ranges,
    choose_reusable_raw_artifact,
    content_addressed_path,
    ptg2_temp_parent,
    resolve_ptg2_artifact_dir,
    sha256_file,
)
from process.ptg_parts.artifact_streams import (
    load_json_artifact,
    logical_artifact_identity,
    open_json_artifact_stream,
    stream_logical_artifact,
)
from process.ptg_parts.compact_indexes import (
    _PTG2_COMPACT_MODEL_BY_KIND,
    _index_snapshot_compact_table_entries,
    _index_snapshot_compact_tables,
    _ptg2_compact_dictionary_index_mode,
    _ptg2_compact_serving_index_mode,
    _ptg2_compact_serving_reported_index_statement,
    _ptg2_index_timestamp,
    _ptg2_model_index_statements_for_table,
    _ptg2_model_snapshot_index_role,
    _run_ptg2_index_statement,
)
from process.ptg_parts.compact_state import (
    _compact_add_unique,
    _compact_state,
    _compact_streaming_dedupe_tables,
    _ptg2_price_atom_payload,
    _ptg2_source_trace_payload,
)
from process.ptg_parts.compact_writes import (
    _drain_compact_writes,
    _existing_price_set_hashes,
    _flush_compact_rate_pack_groups,
    _flush_compact_rows,
    _schedule_compact_write,
)
from process.ptg_parts.canonical import (
    _canonical_key,
    _canonical_sort_key,
    _canonicalize_for_json,
    canonical_json_dumps,
    canonicalize_url,
    hash_prefix,
    normalize_date,
    normalize_import_month,
    normalize_money,
    normalize_tic_source_url,
    semantic_hash,
    sha256_bytes,
)
from process.ptg_parts.config import (
    PTG2_ASYNC_WRITE_TASKS_ENV,
    PTG2_BILLING_BATCH_ROWS_ENV,
    PTG2_COMPACT_BATCH_ROWS_ENV,
    PTG2_COMPACT_BULK_DROP_INDEXES_ENV,
    PTG2_COMPACT_COPY_KIND_TASKS_ENV,
    PTG2_COMPACT_COPY_TASKS_ENV,
    PTG2_COMPACT_IMPORT_ENV,
    PTG2_COMPACT_SERVING_COPY_TASKS_ENV,
    PTG2_COMPACT_SERVING_TABLE_ENV,
    PTG2_COPY_UPSERT_ROWS_ENV,
    PTG2_DEDUPE_SERVING_STAGE_MERGE_ENV,
    PTG2_DEFAULT_COMPACT_COPY_KIND_TASKS,
    PTG2_DEFAULT_COMPACT_COPY_TASKS,
    PTG2_DEFAULT_COMPACT_SERVING_COPY_TASKS,
    PTG2_DEFAULT_DOWNLOAD_TASKS,
    PTG2_DEFAULT_RANGE_DOWNLOAD_CHUNK_BYTES,
    PTG2_DEFAULT_RANGE_DOWNLOAD_MIN_BYTES,
    PTG2_DEFAULT_RANGE_DOWNLOAD_TASKS,
    PTG2_DEFAULT_RUST_WORKERS,
    PTG2_DEFER_PROVIDER_LOCATIONS_ENV,
    PTG2_DIRECT_COPY_SERVING_RATE_ENV,
    PTG2_DOWNLOAD_PROGRESS_BYTES_ENV,
    PTG2_DOWNLOAD_RETRIES_ENV,
    PTG2_DOWNLOAD_RETRY_DELAY_SECONDS_ENV,
    PTG2_DOWNLOAD_TASKS_ENV,
    PTG2_EXPECTED_IN_NETWORK_ITEMS_ENV,
    PTG2_FAST_FINAL_REBUILD_ENV,
    PTG2_FAST_OBJECT_ITERATOR_ENV,
    PTG2_FAST_PROVIDER_AGGREGATION_ENV,
    PTG2_FAST_PROVIDER_UNION_ENV,
    PTG2_HASH_MODE_ENV,
    PTG2_ITEM_BATCH_ROWS_ENV,
    PTG2_JSON_DECODER_ITERATOR_ENV,
    PTG2_KEEP_PARTIAL_ENV,
    PTG2_KEEP_PRICE_SET_STAGE_ENV,
    PTG2_KEEP_SERVING_RATE_STAGE_ENV,
    PTG2_PRICE_BATCH_ROWS_ENV,
    PTG2_PROGRESS_INTERVAL_SECONDS_ENV,
    PTG2_PROVIDER_BUCKET_COUNT_ENV,
    PTG2_PROVIDER_CACHE_BACKEND_ENV,
    PTG2_PROVIDER_CACHE_MEMORY_REFS_ENV,
    PTG2_PROVIDER_COMBO_CACHE_REFS_ENV,
    PTG2_PROVIDER_REF_BATCH_ROWS_ENV,
    PTG2_PROVIDER_SET_INLINE_NPI_LIMIT_ENV,
    PTG2_RANGE_DOWNLOAD_CHUNK_BYTES_ENV,
    PTG2_RANGE_DOWNLOAD_MIN_BYTES_ENV,
    PTG2_RANGE_DOWNLOAD_TASKS_ENV,
    PTG2_RANGE_DOWNLOADS_ENV,
    PTG2_RATE_BATCH_ROWS_ENV,
    PTG2_RATE_GROUP_FLUSH_ITEMS_ENV,
    PTG2_RAW_WORKER_OBJECTS_ENV,
    PTG2_RUST_COMPACT_SERVING_ENV,
    PTG2_RUST_EVENT_QUEUE_ENV,
    PTG2_RUST_SCANNER_BIN_ENV,
    PTG2_RUST_SCANNER_ENV,
    PTG2_RUST_WORKERS_ENV,
    PTG2_SERVING_ONLY_IMPORT_ENV,
    PTG2_SERVING_WORKERS_ENV,
    PTG2_SKIP_BULK_INDEX_ENSURE_ENV,
    PTG2_SKIP_COMPACT_SERVING_INDEX_ENSURE_ENV,
    PTG2_SKIP_EXISTING_PRICE_SETS_ENV,
    PTG2_SLIM_SERVING_ROWS_ENV,
    PTG2_STAGE_COPY_DEDUPE_DEFAULT_KINDS,
    PTG2_STAGE_COPY_DEDUPE_ENV,
    PTG2_STAGE_INDEXES_ENV,
    PTG2_STAGE_PRICE_SETS_ENV,
    PTG2_STAGE_SERVING_AS_FINAL_ENV,
    PTG2_STAGE_SERVING_RATES_ENV,
    PTG2_STREAMING_DEDUPE_ENV,
    PTG2_UNLOGGED_FINAL_ENV,
    PTG2_UNLOGGED_STAGE_ENV,
    PTG2_WORKER_CHUNK_BYTES_ENV,
    PTG2_WORKER_CHUNK_ITEMS_ENV,
    PTG2_WORKER_MAX_PENDING_BATCHES_ENV,
    PTG2_WORKER_MAX_PENDING_BYTES_ENV,
    PTG2_WORKER_RESULT_FILES_ENV,
    TEST_ALLOWED_ITEMS,
    TEST_IN_NETWORK_ITEMS,
    TEST_NEGOTIATED_PRICES,
    TEST_PROVIDER_GROUPS,
    TEST_TOC_FILES,
    TEST_TOC_JOBS,
    _env_bool,
    _env_int,
    _ptg2_stage_copy_dedupe_enabled,
    _use_compact_serving_table,
    _use_rust_compact_serving,
    _use_serving_only_import,
    _use_stage_serving_as_final,
)
from process.ptg_parts.copy_load import (
    _copy_compact_serving_rate_file,
    _copy_compact_serving_rate_rows,
    _copy_compact_serving_rate_source,
    _copy_ignore_ptg2_objects,
    _copy_insert_ptg2_objects,
    _copy_ptg2_dictionary_file,
    _copy_stage_price_set_rows,
    _copy_stage_serving_rate_rows,
    _copy_upsert_ptg2_objects,
    _json_default,
    _primary_key_column_names,
    _ptg2_conflict_targets,
    _ptg2_copy_record,
    _ptg2_json_columns,
)
from process.ptg_parts.domain import (
    PTG2_ARTIFACT_RAW,
    PTG2_CONFIDENCE_NPPES_MAILING_LOCATION,
    PTG2_CONFIDENCE_NPPES_PRACTICE_LOCATION,
    PTG2_CONFIDENCE_PAYER_DIRECTORY,
    PTG2_CONFIDENCE_TIC_RATE_NPI_TIN,
    PTG2_DOMAIN_ALLOWED_AMOUNT,
    PTG2_DOMAIN_DRUG,
    PTG2_DOMAIN_IN_NETWORK,
    PTG2_MODE_EXACT_SOURCE,
    PTG2_MODE_PRODUCT_SEARCH,
    PTG2_STATUS_BUILDING,
    PTG2_STATUS_DEAD_LETTER,
    PTG2_STATUS_FAILED,
    PTG2_STATUS_PENDING,
    PTG2_STATUS_PUBLISHED,
    PTG2_STATUS_RUNNING,
    PTG2_STATUS_VALIDATED,
    PTG2ConfidenceEnum,
    PTG2ContentIdentityValue,
    PTG2ContractEvent,
    PTG2DownloadedJob,
    PTG2FileProcessResult,
    PTG2HeadMetadata,
    PTG2LogicalArtifact,
    PTG2PriceAtomEvent,
    PTG2PriceSetValue,
    PTG2ProcedureEvent,
    PTG2ProviderGroupEvent,
    PTG2ProviderSetValue,
    PTG2RatePackValue,
    PTG2RawArtifact,
    PTG2SourceCatalogEntry,
    PTG2SourceTraceSetValue,
    PTG2SourceVersion,
    normalize_ptg2_search_mode,
    ptg2_confidence_statement,
)
from process.ptg_parts.db_tables import (
    _estimated_table_rows,
    _exact_table_rows,
    _quote_ident,
    _table_exists,
    _table_has_rows,
)
from process.ptg_parts.import_rows import (
    _build_provider_set_entry,
    _combine_provider_set_entries,
    _fast_provider_entry_from_parts,
    _fast_provider_entry_from_provider_refs,
    _normalize_import_id,
    _ptg2_context_row,
    _ptg2_plan_rows,
    _ptg2_price_atom_row,
    _ptg2_procedure_row,
    _ptg2_provider_group_rows,
    _ptg2_provider_set_row,
    _ptg2_source_trace_rows,
)
from process.ptg_parts.json_streams import (
    _iter_top_level_object_bytes,
    _iter_top_level_objects,
    _iter_top_level_objects_fast,
    _iter_top_level_objects_jsondecoder,
    _json_loads,
)
from process.ptg_parts.provider_cache import (
    PTG2InMemoryProviderReferenceCache,
    PTG2ProviderReferenceCache,
    _normalize_provider_ref,
    _provider_cache_get,
    _provider_cache_hashes,
    _provider_cache_put,
    _provider_combo_cache_get,
    _provider_combo_cache_key,
    _provider_combo_cache_put,
)
from process.ptg_parts.progress import (
    _artifact_progress_position,
    _format_duration,
    _maybe_log_artifact_progress,
    _utcnow,
)
from process.ptg_parts.row_helpers import (
    _as_int_list,
    _as_list,
    _coerce_date,
    _make_checksum,
    _normalize_code_component,
    _normalize_tin_type,
    _normalize_tin_value,
    _normalized_npi_list,
    _provider_group_hash_prefix,
    _provider_group_identity_hash,
)
from process.ptg_parts.rust_scanner import (
    _aiter_compact_serving_records_rust,
    _iter_compact_serving_records_rust,
    _iter_top_level_object_bytes_rust,
    _ptg2_rust_scanner_binary,
)
from process.ptg_parts.screen import _emit_screen_line
from process.ptg_parts.rust_publish import (
    _ptg2_publish_timestamp,
    _ptg2_serving_child_table_name,
    _publish_renamed_rust_dictionary_table,
    _publish_rust_compact_snapshot_tables,
    _publish_rust_serving_stage_tables,
)
from process.ptg_parts.rust_stage import (
    PTG2_SERVING_STAGE_LANE_PREFIX,
    _RUST_COPY_TABLE_SPECS,
    _create_rust_copy_stage_tables,
    _merge_rust_copy_stage_tables,
    _ptg2_dictionary_select_columns,
    _rust_copy_stage_table_name,
    _serving_stage_lane_key,
    _serving_stage_table_for_copy,
    _serving_stage_tables,
)
from process.ptg_parts.serving_rows import (
    _provider_group_member_rows,
    _provider_set_component_rows,
    _ptg2_compact_serving_rate_row,
    _ptg2_hp_procedure_code,
    _ptg2_serving_rate_row,
)
from process.ptg_parts.serving_maintenance import (
    _build_ptg2_provider_locations,
    _copy_simple_rows,
    _count_compact_serving_rate_rows,
    _merge_staged_price_sets,
    _merge_staged_serving_rates,
)
from process.ptg_parts.serving_index import (
    _ptg2_table_available,
    build_ptg2_compact_serving_index,
    build_ptg2_db_serving_index,
    build_ptg2_stage_serving_index,
    finalize_ptg2_incremental_serving_index,
)
from process.ptg_parts.serving_only import (
    _iter_worker_result_rows,
    _normalize_serving_price_payload,
    _ptg2_worker_capacity_wait_needed,
    _serving_only_hash_int_sets,
    _serving_only_hash_price_key,
    _serving_only_hash_text,
    _serving_only_key_list,
    _serving_only_key_value,
    _serving_only_merge_worker_result,
    _serving_only_price_payload,
    _serving_only_price_payload_and_key,
    _serving_only_worker_process_chunk_to_files as _serving_only_worker_process_chunk_to_files_impl,
    _worker_payload_size,
)
from process.ptg_parts.snapshot_tables import (
    _normalize_source_key,
    _ptg2_snapshot_index_name,
    _ptg2_snapshot_table_name,
    _ptg2_snapshot_table_token,
)
from process.ptg_parts.snapshot_cleanup import (
    _cleanup_old_ptg2_source_tables,
    _drop_ptg2_snapshot_table_names,
    _drop_ptg2_snapshot_tables_for_manifest,
    _snapshot_manifest_table_names,
)
from process.ptg_parts.source_pointers import (
    _current_source_snapshot_id,
    _ptg2_plan_source_key,
    _source_plan_rows,
)
from process.ptg_parts.snapshot_artifacts import (
    _row_mapping,
    build_ptg2_compact_snapshot_index_artifact,
    build_ptg2_snapshot_index_artifact,
)
from process.ptg_parts.source_versions import _record_source_version
from process.ptg_parts.source_jobs import (
    _dedupe_preserve,
    _dedupe_ptg_jobs,
    _dedupe_rows_by,
    _filter_jobs_by_url_contains,
    _filter_reporting_plans,
    _load_toc_urls_from_file,
    _merge_ptg_job,
    _normalize_filter_values,
    _normalize_plan_payload,
    _plan_identity,
    _plan_matches_filters,
    _ptg_job_identity,
    parse_toc_catalog_entries,
)
from process.ptg_parts.source_download import (
    _download_raw_artifact_ranges,
    _emit_download_progress,
    _format_eta_seconds,
    _probe_http_range_support,
    download_raw_artifact,
    fetch_head_metadata,
    materialize_json_source,
)
from process.ptg_parts.source_files import (
    _build_file_row,
    _derive_plan_fields,
    _extract_metadata_fields,
    _maybe_unzip,
)
from process.ptg_parts.table_setup import (
    PTG2_MODEL_CLASSES,
    _drop_ptg2_columns,
    _ensure_indexes,
    _ensure_ptg2_price_atom_columns,
    _ensure_ptg2_price_set_columns,
    _ensure_ptg2_price_set_stage_table,
    _ensure_ptg2_provider_set_columns,
    _ensure_ptg2_serving_rate_columns,
    _ensure_ptg2_serving_rate_stage_table,
    _prepare_ptg_tables,
    ensure_ptg2_tables,
)
from process.ptg_parts.values import (
    _catalog_entry_id,
    build_fact_chunk,
    build_price_atom,
    build_price_set,
    build_procedure_collection,
    build_provider_set,
    build_provider_set_collection,
    build_rate_pack,
    build_rate_pack_group,
    build_rate_pack_procedure_group,
    build_rate_set,
    build_source_trace_set,
    provider_hash_bucket,
    ptg2_provider_bucket_count,
)

logger = logging.getLogger(__name__)


async def _download_ptg_job_artifact(
    job: dict[str, Any],
    *,
    reuse_raw_artifacts: bool,
    max_bytes: int | None,
    keep_partial_artifacts: bool | None,
) -> PTG2DownloadedJob:
    try:
        with tempfile.TemporaryDirectory(dir=ptg2_temp_parent()) as tmpdir:
            raw_artifact, logical_artifact = await materialize_json_source(
                job["url"],
                tmpdir,
                reuse_raw_artifacts=reuse_raw_artifacts,
                max_bytes=max_bytes,
                materialize_logical=False,
                keep_partial_artifacts=keep_partial_artifacts,
            )
        return PTG2DownloadedJob(job=job, raw_artifact=raw_artifact, logical_artifact=logical_artifact)
    except Exception as exc:
        return PTG2DownloadedJob(job=job, error=str(exc))


def _download_ptg_job_artifact_sync(
    job: dict[str, Any],
    *,
    reuse_raw_artifacts: bool,
    max_bytes: int | None,
    keep_partial_artifacts: bool | None,
) -> PTG2DownloadedJob:
    return asyncio.run(
        _download_ptg_job_artifact(
            job,
            reuse_raw_artifacts=reuse_raw_artifacts,
            max_bytes=max_bytes,
            keep_partial_artifacts=keep_partial_artifacts,
        )
    )


async def _iter_downloaded_ptg_jobs(
    jobs: list[dict[str, Any]],
    *,
    reuse_raw_artifacts: bool,
    max_bytes: int | None,
    keep_partial_artifacts: bool | None,
):
    download_tasks = max(_env_int(PTG2_DOWNLOAD_TASKS_ENV, PTG2_DEFAULT_DOWNLOAD_TASKS), 1)
    executor = concurrent.futures.ThreadPoolExecutor(
        max_workers=download_tasks,
        thread_name_prefix="ptg2-download",
    )
    pending: set[asyncio.Future[PTG2DownloadedJob]] = set()
    job_iter = iter(jobs)

    def schedule_more() -> None:
        while len(pending) < download_tasks:
            try:
                job = next(job_iter)
            except StopIteration:
                return
            pending.add(
                asyncio.wrap_future(
                    executor.submit(
                        _download_ptg_job_artifact_sync,
                        job,
                        reuse_raw_artifacts=reuse_raw_artifacts,
                        max_bytes=max_bytes,
                        keep_partial_artifacts=keep_partial_artifacts,
                    )
                )
            )

    schedule_more()
    try:
        while pending:
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            schedule_more()
            for task in done:
                yield task.result()
    finally:
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        executor.shutdown(wait=False, cancel_futures=True)


async def _publish_ptg2_source_pointers(
    *,
    source_key: str,
    snapshot_id: str,
    previous_snapshot_id: str | None,
    import_month: datetime.date,
    updated_at: datetime.datetime,
    serving_index: dict[str, Any] | None,
) -> None:
    await _push_ptg2_objects(
        [
            {
                "source_key": source_key,
                "snapshot_id": snapshot_id,
                "previous_snapshot_id": previous_snapshot_id,
                "import_month": import_month,
                "updated_at": updated_at,
            }
        ],
        PTG2CurrentSourceSnapshot,
        rewrite=True,
    )
    plan_rows = await _source_plan_rows(
        snapshot_id=snapshot_id,
        source_key=source_key,
        import_month=import_month,
        previous_snapshot_id=previous_snapshot_id,
        updated_at=updated_at,
        serving_index=serving_index,
    )
    if plan_rows:
        await _push_ptg2_objects(plan_rows, PTG2CurrentPlanSource, rewrite=True)


async def _push_ptg2_objects(rows: list[dict[str, Any]], cls, rewrite: bool = True) -> None:
    if rows and cls is PTG2PriceSet and _env_bool(PTG2_STREAMING_DEDUPE_ENV, False):
        try:
            await _copy_ignore_ptg2_objects(rows, cls)
            return
        except Exception as exc:
            logger.warning("PTG2 copy/ignore fallback for %s: %s", cls.__tablename__, exc)
    if rows and cls is PTG2ServingRate and _env_bool(PTG2_DIRECT_COPY_SERVING_RATE_ENV, False):
        try:
            await _copy_insert_ptg2_objects(rows, cls)
            return
        except Exception as exc:
            logger.warning("PTG2 direct COPY fallback for %s: %s", cls.__tablename__, exc)
    if rows and rewrite and len(rows) >= max(_env_int(PTG2_COPY_UPSERT_ROWS_ENV, 250), 1):
        try:
            await _copy_upsert_ptg2_objects(rows, cls)
            return
        except Exception as exc:
            logger.warning("PTG2 copy/upsert fallback for %s: %s", cls.__tablename__, exc)
    try:
        await push_objects(rows, cls, rewrite=rewrite, use_copy=False)
    except TypeError as exc:
        if "use_copy" not in str(exc):
            raise
        await push_objects(rows, cls, rewrite=rewrite)

async def prepare_ptg2_compact_bulk_load() -> None:
    if not _env_bool(PTG2_COMPACT_BULK_DROP_INDEXES_ENV, True):
        return
    schema = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    fast_rebuild = _env_bool(PTG2_FAST_FINAL_REBUILD_ENV, False)
    unlogged_final = _env_bool(PTG2_UNLOGGED_FINAL_ENV, fast_rebuild)
    statements = [
        f"ALTER TABLE {schema}.ptg2_serving_rate_compact SET (autovacuum_enabled = false, toast.autovacuum_enabled = false)",
        f"ALTER TABLE {schema}.ptg2_provider_set_component SET (autovacuum_enabled = false)",
        f"ALTER TABLE {schema}.ptg2_provider_group_member SET (autovacuum_enabled = false)",
        f"ALTER TABLE {schema}.ptg2_price_set SET (autovacuum_enabled = false, toast.autovacuum_enabled = false)",
        f"ALTER TABLE {schema}.ptg2_serving_rate_compact DROP CONSTRAINT IF EXISTS ptg2_serving_rate_compact_pkey",
        f"DROP INDEX IF EXISTS {schema}.ptg2_serving_rate_compact_billing_idx",
        f"DROP INDEX IF EXISTS {schema}.ptg2_serving_rate_compact_billing_order_idx",
        f"DROP INDEX IF EXISTS {schema}.ptg2_serving_rate_compact_reported_idx",
        f"DROP INDEX IF EXISTS {schema}.ptg2_serving_rate_compact_reported_order_idx",
        f"DROP INDEX IF EXISTS {schema}.ptg2_serving_rate_compact_hp_idx",
        f"DROP INDEX IF EXISTS {schema}.ptg2_serving_rate_compact_provider_idx",
        f"DROP INDEX IF EXISTS {schema}.ptg2_serving_rate_compact_price_idx",
        f"DROP INDEX IF EXISTS {schema}.ptg2_serving_rate_compact_idx_primary",
        f"DROP INDEX IF EXISTS {schema}.ptg2_provider_set_component_group_idx",
        f"DROP INDEX IF EXISTS {schema}.ptg2_provider_set_component_idx_primary",
        f"DROP INDEX IF EXISTS {schema}.ptg2_provider_group_member_npi_idx",
        f"DROP INDEX IF EXISTS {schema}.ptg2_provider_group_member_idx_primary",
        f"DROP INDEX IF EXISTS {schema}.ptg2_price_set_idx_primary",
    ]
    if fast_rebuild:
        statements = [
            f"TRUNCATE {schema}.ptg2_serving_rate_compact",
            f"TRUNCATE {schema}.ptg2_provider_set_component",
            f"TRUNCATE {schema}.ptg2_provider_group_member",
            f"TRUNCATE {schema}.ptg2_provider_set",
            f"TRUNCATE {schema}.ptg2_price_set",
        ] + statements + [
            f"DROP INDEX IF EXISTS {schema}.ptg2_serving_rate_compact_pkey",
            f"DROP INDEX IF EXISTS {schema}.ptg2_provider_set_component_pkey",
            f"DROP INDEX IF EXISTS {schema}.ptg2_provider_group_member_pkey",
            f"DROP INDEX IF EXISTS {schema}.ptg2_provider_set_pkey",
            f"DROP INDEX IF EXISTS {schema}.ptg2_price_set_pkey",
        ]
    if unlogged_final:
        statements = [
            f"ALTER TABLE {schema}.ptg2_serving_rate_compact SET UNLOGGED",
            f"ALTER TABLE {schema}.ptg2_provider_set_component SET UNLOGGED",
            f"ALTER TABLE {schema}.ptg2_provider_group_member SET UNLOGGED",
            f"ALTER TABLE {schema}.ptg2_provider_set SET UNLOGGED",
            f"ALTER TABLE {schema}.ptg2_price_set SET UNLOGGED",
        ] + statements
    for statement in statements:
        try:
            await db.status(statement)
        except Exception as exc:
            logger.debug("Skipping compact bulk-load prep statement %s: %s", statement, exc)


async def _flush_in_network_rows(
    item_rows: list[dict[str, Any]],
    billing_rows: list[dict[str, Any]],
    rate_rows: list[dict[str, Any]],
    price_rows: list[dict[str, Any]],
    item_cls,
    billing_cls,
    rate_cls,
    price_cls,
    *,
    force: bool = False,
    item_batch_rows: int = 1000,
    billing_batch_rows: int = 5000,
    rate_batch_rows: int = 5000,
    price_batch_rows: int = 10000,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    if item_rows and (force or len(item_rows) >= item_batch_rows):
        await push_objects(_dedupe_rows_by(item_rows, "item_hash"), item_cls)
        item_rows = []
    if billing_rows and (force or len(billing_rows) >= billing_batch_rows):
        await push_objects(_dedupe_rows_by(billing_rows, "code_hash"), billing_cls)
        billing_rows = []
    if rate_rows and (force or len(rate_rows) >= rate_batch_rows):
        await push_objects(_dedupe_rows_by(rate_rows, "rate_hash"), rate_cls)
        rate_rows = []
    if price_rows and (force or len(price_rows) >= price_batch_rows):
        await push_objects(_dedupe_rows_by(price_rows, "price_hash"), price_cls)
        price_rows = []
    return item_rows, billing_rows, rate_rows, price_rows


_PTG2_WORKER_PROVIDER_MAP = None
_PTG2_WORKER_PLAN_FIELDS: dict[str, Any] | None = None
_PTG2_WORKER_SNAPSHOT_ID: str | None = None
_PTG2_WORKER_PLAN_MONTH_ID: str | None = None
_PTG2_WORKER_SOURCE_TRACE: list[dict[str, Any]] | None = None
_PTG2_WORKER_SOURCE_TRACE_SET_HASH: str | None = None
_PTG2_WORKER_SLIM_SERVING_ROWS = False
_PTG2_WORKER_COMPACT_SERVING = False


def _serving_only_rows_for_payload(
    payload: dict[str, Any],
    *,
    provider_map,
    plan_fields: dict[str, Any],
    snapshot_id: str,
    plan_month_id: str | None = None,
    source_trace_payload: list[dict[str, Any]],
    slim_serving_rows: bool = False,
    compact_serving: bool = False,
    source_trace_set_hash: str | None = None,
    include_price_set_rows: bool = False,
) -> list[dict[str, Any]] | dict[str, list[dict[str, Any]]]:
    procedure_payload = {
        "billing_code_type": payload.get("billing_code_type"),
        "billing_code": payload.get("billing_code"),
        "name": payload.get("name"),
        "description": payload.get("description"),
    }
    procedure_hash = semantic_hash(
        {
            "billing_code_type": payload.get("billing_code_type"),
            "billing_code_type_version": payload.get("billing_code_type_version"),
            "billing_code": payload.get("billing_code"),
            "name": payload.get("name"),
            "description": payload.get("description"),
        },
        domain="procedure",
    )
    procedure_row = {
        "procedure_hash": procedure_hash,
        "hash_prefix": hash_prefix(procedure_hash),
        "billing_code_type": payload.get("billing_code_type"),
        "billing_code_type_version": payload.get("billing_code_type_version"),
        "billing_code": payload.get("billing_code"),
        "name": payload.get("name"),
        "description": payload.get("description"),
        "canonical_payload": _canonicalize_for_json(
            {
                "billing_code_type": payload.get("billing_code_type"),
                "billing_code_type_version": payload.get("billing_code_type_version"),
                "billing_code": payload.get("billing_code"),
                "name": payload.get("name"),
                "description": payload.get("description"),
            }
        ),
        "created_at": _utcnow(),
    }
    item_price_groups: dict[str, dict[str, Any]] = {}
    for negotiated_rate in payload.get("negotiated_rates", []):
        provider_groups_inline = negotiated_rate.get("provider_groups") or []
        provider_refs = negotiated_rate.get("provider_references") or []
        combined_entry = None
        if provider_refs and not provider_groups_inline:
            combined_entry, _missing_refs = _fast_provider_entry_from_provider_refs(provider_map, provider_refs)
        else:
            groups_to_use: list[dict[str, Any]] = []
            for provider_ref in provider_refs:
                groups_to_use.extend(
                    _provider_cache_get(provider_map, _normalize_provider_ref(provider_ref))
                    or _provider_cache_get(provider_map, provider_ref)
                )
            if provider_groups_inline:
                inline_entry, _inline_row = _build_provider_set_entry(
                    file_id=0,
                    provider_group_ref=None,
                    provider_groups=provider_groups_inline,
                    network_names=negotiated_rate.get("network_name") or [],
                )
                if inline_entry is not None:
                    groups_to_use.append(inline_entry)
            combined_entry, _combined_row = _combine_provider_set_entries(
                file_id=0,
                entries=groups_to_use,
                network_names=negotiated_rate.get("network_name") or [],
            )
        if combined_entry is None:
            continue
        price_payload, price_key = _serving_only_price_payload_and_key(negotiated_rate.get("negotiated_prices", []))
        if not price_payload:
            continue
        item_group = item_price_groups.setdefault(
            price_key,
            {
                "prices": price_payload,
                "provider_entry_hashes": set(),
                "provider_group_hashes": set(),
                "provider_group_member_rows": [],
                "provider_count": 0,
            },
        )
        entry_hash = int(combined_entry["__hash__"])
        if entry_hash in item_group["provider_entry_hashes"]:
            continue
        item_group["provider_entry_hashes"].add(entry_hash)
        item_group["provider_group_hashes"].update(
            int(value) for value in combined_entry.get("provider_group_hashes") or [entry_hash]
        )
        item_group["provider_group_member_rows"].extend(_provider_group_member_rows(combined_entry))
        item_group["provider_count"] += int(
            combined_entry.get("provider_count") or len(_as_int_list(combined_entry.get("npi")))
        )

    rows: list[dict[str, Any]] = []
    compact_rows: list[dict[str, Any]] = []
    provider_set_rows: list[dict[str, Any]] = []
    provider_set_component_rows: list[dict[str, Any]] = []
    provider_group_member_rows: list[dict[str, Any]] = []
    price_set_rows: list[dict[str, Any]] = []
    confidence_payload = {
        "network": PTG2_CONFIDENCE_TIC_RATE_NPI_TIN,
        "location": PTG2_CONFIDENCE_NPPES_PRACTICE_LOCATION,
        "acceptance_statement": ptg2_confidence_statement(PTG2_CONFIDENCE_TIC_RATE_NPI_TIN),
    }
    for price_key, item_group in item_price_groups.items():
        provider_group_hashes = sorted({int(value) for value in item_group["provider_group_hashes"]})
        provider_set_hash = _serving_only_hash_int_sets(
            "serving_provider_set",
            provider_group_hashes,
        )
        provider_count = int(item_group["provider_count"] or 0)
        provider_set_rows.append(
            {
                "provider_set_hash": provider_set_hash,
                "provider_count": provider_count,
                "created_at": _utcnow(),
            }
        )
        price_set_hash = _serving_only_hash_price_key(price_key)
        rate_pack_hash = _serving_only_hash_text(
            "serving_rate_pack",
            snapshot_id,
            procedure_hash,
            provider_set_hash,
            price_set_hash,
        )
        rows.append(
            _ptg2_serving_rate_row(
                snapshot_id=snapshot_id,
                plan_fields=plan_fields,
                procedure_payload=procedure_payload,
                rate_pack_row={
                    "rate_pack_hash": rate_pack_hash,
                    "provider_set_hash": provider_set_hash,
                    "price_set_hash": price_set_hash,
                },
                provider_set_hashes=[provider_set_hash],
                provider_count=provider_count,
                provider_set_count=1,
                prices=None if slim_serving_rows else item_group["prices"],
                source_trace=None if slim_serving_rows else source_trace_payload,
                source_trace_set_hash=source_trace_set_hash if slim_serving_rows else None,
                confidence={} if slim_serving_rows else confidence_payload,
                confidence_code=PTG2_CONFIDENCE_TIC_RATE_NPI_TIN,
            )
        )
        if not compact_serving:
            provider_set_component_rows.extend(
                _provider_set_component_rows(provider_set_hash, provider_group_hashes)
            )
        provider_group_member_rows.extend(item_group["provider_group_member_rows"])
        if compact_serving and plan_month_id:
            compact_rows.append(
                _ptg2_compact_serving_rate_row(rows[-1], plan_month_id=plan_month_id, procedure_hash=procedure_hash)
            )
    if include_price_set_rows:
        return {
            "serving_rows": rows,
            "serving_rate_compact_rows": compact_rows,
            "provider_set_rows": provider_set_rows,
            "price_set_rows": price_set_rows,
            "provider_set_component_rows": provider_set_component_rows,
            "provider_group_member_rows": provider_group_member_rows,
            "procedure_rows": [procedure_row],
        }
    return rows


def _serving_only_worker_process(payload_or_raw: dict[str, Any] | bytes) -> dict[str, list[dict[str, Any]]]:
    if (
        _PTG2_WORKER_PROVIDER_MAP is None
        or _PTG2_WORKER_PLAN_FIELDS is None
        or _PTG2_WORKER_SNAPSHOT_ID is None
        or _PTG2_WORKER_PLAN_MONTH_ID is None
        or _PTG2_WORKER_SOURCE_TRACE is None
    ):
        raise RuntimeError("PTG2 serving worker was not initialized")
    payload = _json_loads(payload_or_raw) if isinstance(payload_or_raw, (bytes, bytearray)) else payload_or_raw
    return _serving_only_rows_for_payload(
        payload,
        provider_map=_PTG2_WORKER_PROVIDER_MAP,
        plan_fields=_PTG2_WORKER_PLAN_FIELDS,
        snapshot_id=_PTG2_WORKER_SNAPSHOT_ID,
        plan_month_id=_PTG2_WORKER_PLAN_MONTH_ID,
        source_trace_payload=_PTG2_WORKER_SOURCE_TRACE,
        slim_serving_rows=_PTG2_WORKER_SLIM_SERVING_ROWS,
        compact_serving=_PTG2_WORKER_COMPACT_SERVING,
        source_trace_set_hash=_PTG2_WORKER_SOURCE_TRACE_SET_HASH,
        include_price_set_rows=True,
    )


def _serving_only_worker_process_chunk(
    payloads_or_raw: list[dict[str, Any] | bytes | bytearray],
) -> dict[str, list[dict[str, Any]]]:
    merged: dict[str, list[dict[str, Any]]] = {}
    for payload_or_raw in payloads_or_raw:
        result = _serving_only_worker_process(payload_or_raw)
        _serving_only_merge_worker_result(merged, result)
    merged.pop("__seen__", None)
    return merged


def _serving_only_worker_process_chunk_to_files(
    payloads_or_raw: list[dict[str, Any] | bytes | bytearray],
) -> dict[str, Any]:
    return _serving_only_worker_process_chunk_to_files_impl(payloads_or_raw, _serving_only_worker_process)


async def _parse_in_network_file_serving_only(
    file_path: str,
    file_id: int,
    meta: dict[str, Any],
    plan_info: list[dict[str, Any]] | None,
    provider_map,
    test_mode: bool,
    import_log_cls,
    source_url: str,
    source_version: PTG2SourceVersion | None,
    snapshot_id: str,
    import_month: datetime.date,
    max_items: int | None = None,
    rust_stage_tables: dict[str, str] | None = None,
) -> dict[str, Any]:
    plan_fields = _derive_plan_fields(meta, plan_info)
    plan_row, alias_rows, plan_month_row = _ptg2_plan_rows(plan_fields, snapshot_id, import_month)
    context_row = _ptg2_context_row(plan_fields, import_month, source_version)
    source_trace_row, _source_trace_set_row = _ptg2_source_trace_rows(source_version, source_url)
    compact_serving = _use_compact_serving_table()
    slim_serving_rows = compact_serving or _env_bool(PTG2_SLIM_SERVING_ROWS_ENV, False)
    await _push_ptg2_objects([plan_row], PTG2Plan, rewrite=True)
    if alias_rows:
        await _push_ptg2_objects(alias_rows, PTG2PlanAlias, rewrite=True)
    await _push_ptg2_objects([plan_month_row], PTG2PlanMonth, rewrite=True)
    await _push_ptg2_objects([context_row], PTG2RateSetContext, rewrite=True)
    if slim_serving_rows or compact_serving:
        await _push_ptg2_objects([source_trace_row], PTG2SourceTrace, rewrite=True)
        await _push_ptg2_objects([_source_trace_set_row], PTG2SourceTraceSet, rewrite=True)

    state = _compact_state()
    state["snapshot_id"] = snapshot_id
    state["plan_fields"] = plan_fields
    source_trace_payload = _ptg2_source_trace_payload(source_trace_row)
    source_trace_set_hash = _source_trace_set_row["source_trace_set_hash"]
    provider_ref_count = 0
    item_count = 0
    serving_rows = 0
    progress_state: dict[str, Any] = {}
    provider_combo_cache_limit = max(_env_int(PTG2_PROVIDER_COMBO_CACHE_REFS_ENV, 32768), 0)
    provider_combo_stats = {
        "provider_combo_cache_gets": 0,
        "provider_combo_cache_hits": 0,
        "provider_combo_cache_misses": 0,
        "provider_combo_cache_size": 0,
        "provider_combo_cache_limit": provider_combo_cache_limit,
    }
    serving_workers = max(_env_int(PTG2_SERVING_WORKERS_ENV, 1), 1)
    worker_chunk_items = max(_env_int(PTG2_WORKER_CHUNK_ITEMS_ENV, 128), 1)
    worker_chunk_bytes = max(_env_int(PTG2_WORKER_CHUNK_BYTES_ENV, 16 * 1024 * 1024), 1024 * 1024)
    worker_max_pending_batches = max(
        _env_int(PTG2_WORKER_MAX_PENDING_BATCHES_ENV, min(serving_workers, 2)),
        1,
    )
    worker_max_pending_bytes = max(
        _env_int(PTG2_WORKER_MAX_PENDING_BYTES_ENV, worker_chunk_bytes * worker_max_pending_batches),
        worker_chunk_bytes,
    )
    worker_result_files = _env_bool(PTG2_WORKER_RESULT_FILES_ENV, True)

    async def add_serving_result_parts(
        *,
        price_set_rows=(),
        result_rows=(),
        compact_rows=(),
        provider_set_rows=(),
        procedure_rows=(),
        component_rows=(),
        member_rows=(),
    ) -> None:
        nonlocal serving_rows
        if price_set_rows and _env_bool(PTG2_SKIP_EXISTING_PRICE_SETS_ENV, compact_serving):
            if state.get("existing_price_set_hashes") is None:
                state["existing_price_set_hashes"] = await _existing_price_set_hashes()
            existing_price_set_hashes = state["existing_price_set_hashes"]
        else:
            existing_price_set_hashes = None
        for price_set_row in price_set_rows:
            price_set_hash = str(price_set_row.get("price_set_hash") or "")
            if existing_price_set_hashes is not None and price_set_hash in existing_price_set_hashes:
                continue
            if _compact_add_unique(state, "price_set", "price_set_hash", price_set_row):
                if existing_price_set_hashes is not None and price_set_hash:
                    existing_price_set_hashes.add(price_set_hash)
        for provider_set_row in provider_set_rows:
            _compact_add_unique(state, "provider_set", "provider_set_hash", provider_set_row)
        for procedure_row in procedure_rows:
            _compact_add_unique(state, "procedure", "procedure_hash", procedure_row)
        for component_row in component_rows:
            _compact_add_unique(state, "provider_set_component", ("provider_set_hash", "provider_group_hash"), component_row)
        for member_row in member_rows:
            _compact_add_unique(state, "provider_group_member", ("provider_group_hash", "npi"), member_row)
        for compact_row in compact_rows:
            if _compact_add_unique(state, "serving_rate_compact", "serving_rate_id", compact_row):
                serving_rows += 1
        if compact_serving:
            await _flush_compact_rows(state)
            return
        for serving_row in result_rows:
            if _compact_add_unique(state, "serving_rate", "serving_rate_id", serving_row):
                serving_rows += 1
        await _flush_compact_rows(state)

    async def add_serving_result(result: list[dict[str, Any]] | dict[str, Any]) -> None:
        if isinstance(result, dict) and result.get("__worker_result_files__"):
            temp_dir = Path(result["temp_dir"])
            paths = result.get("paths") or {}
            try:
                await add_serving_result_parts(
                    price_set_rows=_iter_worker_result_rows(paths["price_set_rows"]) if "price_set_rows" in paths else (),
                    result_rows=_iter_worker_result_rows(paths["serving_rows"]) if "serving_rows" in paths else (),
                    compact_rows=_iter_worker_result_rows(paths["serving_rate_compact_rows"]) if "serving_rate_compact_rows" in paths else (),
                    provider_set_rows=_iter_worker_result_rows(paths["provider_set_rows"]) if "provider_set_rows" in paths else (),
                    procedure_rows=_iter_worker_result_rows(paths["procedure_rows"]) if "procedure_rows" in paths else (),
                    component_rows=_iter_worker_result_rows(paths["provider_set_component_rows"]) if "provider_set_component_rows" in paths else (),
                    member_rows=_iter_worker_result_rows(paths["provider_group_member_rows"]) if "provider_group_member_rows" in paths else (),
                )
            finally:
                for path in paths.values():
                    try:
                        Path(path).unlink(missing_ok=True)
                    except Exception:
                        logger.debug("Failed to remove PTG2 worker result file %s", path, exc_info=True)
                try:
                    temp_dir.rmdir()
                except Exception:
                    logger.debug("Failed to remove PTG2 worker result dir %s", temp_dir, exc_info=True)
            return
        if isinstance(result, dict):
            await add_serving_result_parts(
                price_set_rows=result.get("price_set_rows") or [],
                result_rows=result.get("serving_rows") or [],
                compact_rows=result.get("serving_rate_compact_rows") or [],
                provider_set_rows=result.get("provider_set_rows") or [],
                procedure_rows=result.get("procedure_rows") or [],
                component_rows=result.get("provider_set_component_rows") or [],
                member_rows=result.get("provider_group_member_rows") or [],
            )
            return
        await add_serving_result_parts(result_rows=result)

    if (
        compact_serving
        and _use_rust_compact_serving()
        and max_items is None
        and not test_mode
    ):
        rust_batches = {
            "price_set_rows": [],
            "serving_rate_compact_rows": [],
            "provider_set_rows": [],
            "provider_set_component_rows": [],
            "provider_group_member_rows": [],
            "procedure_rows": [],
        }
        rust_batch_limit = max(_env_int(PTG2_COMPACT_BATCH_ROWS_ENV, 5000), 1)
        rust_records = 0
        rust_dedupe_summary: dict[str, Any] = {}
        rust_item_count: set[str] = set()
        copy_tmp_dir = ptg2_temp_parent()
        compact_copy_fd, compact_copy_name = tempfile.mkstemp(
            prefix="ptg2_compact_serving_",
            suffix=".copy",
            dir=copy_tmp_dir,
        )
        os.close(compact_copy_fd)
        compact_copy_path = Path(compact_copy_name)
        dictionary_copy_paths: dict[str, Path] = {}
        dictionary_kinds = (
            "procedure",
            "price_code_set",
            "price_atom",
            "price_set_entry",
            "provider_set",
            "provider_set_component",
            "provider_group_member",
        )
        for dictionary_kind in dictionary_kinds:
            fd, name = tempfile.mkstemp(
                prefix=f"ptg2_{dictionary_kind}_",
                suffix=".copy",
                dir=copy_tmp_dir,
            )
            os.close(fd)
            dictionary_copy_paths[dictionary_kind] = Path(name)
        compact_copy_rows = 0
        procedure_copy_rows = 0
        compact_copy_completed = False
        compact_copy_tasks: set[asyncio.Task] = set()
        compact_copy_task_limit = max(
            _env_int(PTG2_COMPACT_COPY_TASKS_ENV, PTG2_DEFAULT_COMPACT_COPY_TASKS),
            1,
        )
        compact_copy_kind_task_limit = max(
            _env_int(PTG2_COMPACT_COPY_KIND_TASKS_ENV, PTG2_DEFAULT_COMPACT_COPY_KIND_TASKS),
            1,
        )
        compact_serving_copy_task_limit = max(
            _env_int(
                PTG2_COMPACT_SERVING_COPY_TASKS_ENV,
                PTG2_DEFAULT_COMPACT_SERVING_COPY_TASKS,
            ),
            1,
        )
        compact_copy_semaphore = asyncio.Semaphore(compact_copy_task_limit)
        compact_copy_kind_semaphores: dict[str, asyncio.Semaphore] = {
            kind: asyncio.Semaphore(compact_copy_kind_task_limit)
            for kind in dictionary_kinds
        }
        compact_serving_copy_semaphores: dict[str, asyncio.Semaphore] = {}
        stage_tables: dict[str, str] = dict(rust_stage_tables or {})
        owns_stage_tables = rust_stage_tables is None
        stage_completed = False
        if owns_stage_tables:
            stage_token = hashlib.md5(f"{snapshot_id}:{os.getpid()}:{time.time_ns()}".encode("utf-8")).hexdigest()[:12]
            stage_tables = await _create_rust_copy_stage_tables(
                stage_token,
                serving_lanes=max(_env_int(PTG2_RUST_WORKERS_ENV, PTG2_DEFAULT_RUST_WORKERS), 1),
            )

        def emit_copy_status(
            status: str,
            *,
            kind: str,
            copy_file: Path,
            rows: int,
            target_table: str | None = None,
            started_at: float | None = None,
        ) -> None:
            elapsed = "" if started_at is None else f"\telapsed_seconds={time.monotonic() - started_at:.2f}"
            try:
                bytes_text = str(copy_file.stat().st_size)
            except FileNotFoundError:
                bytes_text = "0"
            target_text = "" if not target_table else f"\ttarget_table={target_table}"
            line = (
                f"PTG2_COPY_SHARD_{status}"
                f"\tkind={kind}"
                f"\tpath={copy_file}"
                f"{target_text}"
                f"\trows={rows}"
                f"\tbytes={bytes_text}"
                f"{elapsed}"
            )
            _emit_screen_line(line)
            logger.info(line)

        async def copy_ready_compact_file(copy_row: dict[str, Any]) -> None:
            nonlocal compact_copy_rows
            raw_copy_path = str(copy_row.get("path") or "").strip()
            if not raw_copy_path:
                return
            copied_rows = int(copy_row.get("row_count") or 0)
            copy_file = Path(raw_copy_path)
            target_table = _serving_stage_table_for_copy(stage_tables, copy_file)
            async with compact_copy_semaphore:
                async with compact_serving_copy_semaphores.setdefault(
                    target_table,
                    asyncio.Semaphore(compact_serving_copy_task_limit),
                ):
                    started_at = time.monotonic()
                    emit_copy_status(
                        "START",
                        kind="serving_rate_compact",
                        copy_file=copy_file,
                        rows=copied_rows,
                        target_table=target_table,
                    )
                    await _copy_compact_serving_rate_file(
                        copy_file,
                        target_table=target_table,
                    )
                    emit_copy_status(
                        "DONE",
                        kind="serving_rate_compact",
                        copy_file=copy_file,
                        rows=copied_rows,
                        target_table=target_table,
                        started_at=started_at,
                    )
            compact_copy_rows += copied_rows
            try:
                copy_file.unlink(missing_ok=True)
            except Exception:
                logger.debug("Failed to remove PTG2 compact copy chunk %s", copy_file, exc_info=True)

        async def copy_ready_dictionary_file(kind: str, copy_row: dict[str, Any]) -> None:
            nonlocal procedure_copy_rows
            raw_copy_path = str(copy_row.get("path") or "").strip()
            if not raw_copy_path:
                return
            copied_rows = int(copy_row.get("row_count") or 0)
            copy_file = Path(raw_copy_path)
            async with compact_copy_semaphore:
                async with compact_copy_kind_semaphores.setdefault(
                    kind,
                    asyncio.Semaphore(compact_copy_kind_task_limit),
                ):
                    started_at = time.monotonic()
                    emit_copy_status(
                        "START",
                        kind=kind,
                        copy_file=copy_file,
                        rows=copied_rows,
                        target_table=stage_tables.get(kind),
                    )
                    await _copy_ptg2_dictionary_file(copy_file, kind, target_table=stage_tables.get(kind))
                    emit_copy_status(
                        "DONE",
                        kind=kind,
                        copy_file=copy_file,
                        rows=copied_rows,
                        target_table=stage_tables.get(kind),
                        started_at=started_at,
                    )
            if kind == "procedure":
                procedure_copy_rows += copied_rows
            try:
                copy_file.unlink(missing_ok=True)
            except Exception:
                logger.debug("Failed to remove PTG2 %s copy chunk %s", kind, copy_file, exc_info=True)

        async def wait_for_some_copy_tasks(force: bool = False) -> None:
            nonlocal compact_copy_tasks
            if not compact_copy_tasks:
                return
            if force:
                done, compact_copy_tasks = await asyncio.wait(
                    compact_copy_tasks,
                    return_when=asyncio.ALL_COMPLETED,
                )
                compact_copy_tasks = set()
            elif len(compact_copy_tasks) < compact_copy_task_limit * 2:
                return
            else:
                done, compact_copy_tasks = await asyncio.wait(
                    compact_copy_tasks,
                    return_when=asyncio.FIRST_COMPLETED,
                )
            for task in done:
                task.result()
        try:
            async for record_kind, record_row in _aiter_compact_serving_records_rust(
                file_path,
                snapshot_id=snapshot_id,
                plan_id=str(plan_fields.get("plan_id") or ""),
                plan_month_id=str(plan_month_row["plan_month_id"]),
                source_trace_set_hash=source_trace_set_hash,
                compact_copy_path=compact_copy_path,
                procedure_copy_path=dictionary_copy_paths.get("procedure"),
                price_code_set_copy_path=dictionary_copy_paths.get("price_code_set"),
                price_atom_copy_path=dictionary_copy_paths.get("price_atom"),
                price_set_entry_copy_path=dictionary_copy_paths.get("price_set_entry"),
                provider_set_copy_path=dictionary_copy_paths.get("provider_set"),
                provider_group_member_copy_path=dictionary_copy_paths.get("provider_group_member"),
                provider_set_component_copy_path=dictionary_copy_paths.get("provider_set_component"),
            ):
                if record_kind == "dedupe_summary":
                    rust_dedupe_summary = dict(record_row or {})
                    continue
                rust_records += 1
                if record_kind == "price_atom_copy_file":
                    compact_copy_tasks.add(asyncio.create_task(copy_ready_dictionary_file("price_atom", record_row)))
                    await wait_for_some_copy_tasks()
                elif record_kind == "price_code_set_copy_file":
                    compact_copy_tasks.add(asyncio.create_task(copy_ready_dictionary_file("price_code_set", record_row)))
                    await wait_for_some_copy_tasks()
                elif record_kind == "price_set_entry_copy_file":
                    compact_copy_tasks.add(asyncio.create_task(copy_ready_dictionary_file("price_set_entry", record_row)))
                    await wait_for_some_copy_tasks()
                elif record_kind == "compact_copy_file":
                    compact_copy_tasks.add(asyncio.create_task(copy_ready_compact_file(record_row)))
                    await wait_for_some_copy_tasks()
                elif record_kind == "serving_rate_compact":
                    rust_batches["serving_rate_compact_rows"].append(record_row)
                    if record_row.get("procedure_hash"):
                        rust_item_count.add(str(record_row.get("procedure_hash")))
                elif record_kind == "provider_set":
                    rust_batches["provider_set_rows"].append(record_row)
                elif record_kind == "provider_set_copy_file":
                    compact_copy_tasks.add(asyncio.create_task(copy_ready_dictionary_file("provider_set", record_row)))
                    await wait_for_some_copy_tasks()
                elif record_kind == "provider_set_component":
                    rust_batches.setdefault("provider_set_component_rows", []).append(record_row)
                elif record_kind == "provider_set_component_copy_file":
                    compact_copy_tasks.add(asyncio.create_task(copy_ready_dictionary_file("provider_set_component", record_row)))
                    await wait_for_some_copy_tasks()
                elif record_kind == "provider_set_entry":
                    rust_batches.setdefault("provider_set_component_rows", []).append(
                        {
                            "provider_set_hash": record_row.get("provider_set_hash"),
                            "provider_group_hash": record_row.get("provider_entry_hash"),
                        }
                    )
                elif record_kind == "provider_set_entry_copy_file":
                    compact_copy_tasks.add(asyncio.create_task(copy_ready_dictionary_file("provider_set_entry", record_row)))
                    await wait_for_some_copy_tasks()
                elif record_kind == "provider_entry_component":
                    rust_batches.setdefault("provider_group_member_rows", []).append(
                        {
                            "provider_group_hash": record_row.get("provider_entry_hash"),
                            "npi": record_row.get("npi"),
                        }
                    )
                elif record_kind == "provider_entry_component_copy_file":
                    compact_copy_tasks.add(asyncio.create_task(copy_ready_dictionary_file("provider_entry_component", record_row)))
                    await wait_for_some_copy_tasks()
                elif record_kind == "provider_group_member_copy_file":
                    compact_copy_tasks.add(asyncio.create_task(copy_ready_dictionary_file("provider_group_member", record_row)))
                    await wait_for_some_copy_tasks()
                elif record_kind == "procedure":
                    rust_batches["procedure_rows"].append(record_row)
                    if record_row.get("procedure_hash"):
                        rust_item_count.add(str(record_row.get("procedure_hash")))
                elif record_kind == "procedure_copy_file":
                    compact_copy_tasks.add(asyncio.create_task(copy_ready_dictionary_file("procedure", record_row)))
                    await wait_for_some_copy_tasks()
                if sum(len(rows) for rows in rust_batches.values()) >= rust_batch_limit:
                    await add_serving_result_parts(
                        price_set_rows=rust_batches["price_set_rows"],
                        compact_rows=rust_batches["serving_rate_compact_rows"],
                        provider_set_rows=rust_batches["provider_set_rows"],
                        procedure_rows=rust_batches["procedure_rows"],
                        component_rows=rust_batches["provider_set_component_rows"],
                        member_rows=rust_batches["provider_group_member_rows"],
                    )
                    for rows in rust_batches.values():
                        rows.clear()
            if any(rust_batches.values()):
                await add_serving_result_parts(
                    price_set_rows=rust_batches["price_set_rows"],
                    compact_rows=rust_batches["serving_rate_compact_rows"],
                    provider_set_rows=rust_batches["provider_set_rows"],
                    procedure_rows=rust_batches["procedure_rows"],
                    component_rows=rust_batches["provider_set_component_rows"],
                    member_rows=rust_batches["provider_group_member_rows"],
                )
            await _flush_compact_rows(state, force=True)
            await _drain_compact_writes(state)
            await wait_for_some_copy_tasks(force=True)
            if compact_copy_path is not None and compact_copy_path.exists() and compact_copy_path.stat().st_size > 0:
                await copy_ready_compact_file({"path": str(compact_copy_path), "row_count": 0})
                compact_copy_completed = True
            for dictionary_kind, dictionary_path in dictionary_copy_paths.items():
                if dictionary_path.exists() and dictionary_path.stat().st_size > 0:
                    await copy_ready_dictionary_file(dictionary_kind, {"path": str(dictionary_path), "row_count": 0})
            if stage_tables and owns_stage_tables:
                await _merge_rust_copy_stage_tables(stage_tables, drop=True)
                stage_completed = True
            elif stage_tables:
                stage_completed = True
            if rust_stage_tables and compact_copy_rows:
                serving_rows = compact_copy_rows
            elif rust_stage_tables:
                serving_rows = await _estimated_table_rows(
                    os.getenv("HLTHPRT_DB_SCHEMA") or "mrf",
                    stage_tables.get("serving_rate_compact", "ptg2_serving_rate_compact"),
                )
            else:
                serving_rows = await _count_compact_serving_rate_rows(
                    snapshot_id,
                    str(plan_fields.get("plan_id") or ""),
                    table_name=stage_tables.get("serving_rate_compact", "ptg2_serving_rate_compact"),
                )
            compact_copy_completed = True
        finally:
            for task in compact_copy_tasks:
                task.cancel()
            if compact_copy_completed or not _env_bool(PTG2_KEEP_PARTIAL_ENV, False):
                try:
                    if compact_copy_path is not None:
                        compact_copy_path.unlink(missing_ok=True)
                except Exception:
                    logger.debug("Failed to remove PTG2 compact copy file %s", compact_copy_path, exc_info=True)
                for dictionary_path in dictionary_copy_paths.values():
                    try:
                        dictionary_path.unlink(missing_ok=True)
                    except Exception:
                        logger.debug("Failed to remove PTG2 dictionary copy file %s", dictionary_path, exc_info=True)
                if stage_tables and owns_stage_tables and not stage_completed:
                    schema_name = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
                    for stage_table in stage_tables.values():
                        try:
                            await db.status(
                                f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(stage_table)};"
                            )
                        except Exception:
                            logger.debug("Failed to drop PTG2 Rust stage table %s", stage_table, exc_info=True)
        item_count = len(rust_item_count) or procedure_copy_rows
        logger.info(
            "PTG2 Rust compact serving emitted %s dictionary records, %s copy rows for %s procedure hashes",
            rust_records,
            compact_copy_rows,
            item_count,
        )
        if slim_serving_rows and _env_bool(PTG2_STAGE_PRICE_SETS_ENV, True) and not rust_stage_tables:
            await _merge_staged_price_sets(snapshot_id)
        if not _env_bool(PTG2_DEFER_PROVIDER_LOCATIONS_ENV, True):
            await _build_ptg2_provider_locations(snapshot_id)
        await flush_error_log(import_log_cls)
        summary = {
            "provider_refs": provider_ref_count,
            "in_network_items": item_count,
            "serving_rates": serving_rows,
            "serving_only": True,
            "serving_workers": 0,
            "worker_chunk_items": 0,
            "rust_compact_serving": True,
            "rust_records": rust_records,
        }
        if rust_dedupe_summary:
            summary["dedupe"] = rust_dedupe_summary
        _emit_screen_line(f"PTG2 serving-only import summary: {summary}")
        logger.info("PTG2 serving-only import summary: %s", summary)
        return summary

    with open_json_artifact_stream(file_path) as afp:
        def progress_callback() -> None:
            _maybe_log_artifact_progress(
                file_path,
                afp,
                progress_state,
                "serving-only in-network import",
                ref_count=provider_ref_count,
                item_count=item_count,
            )

        if serving_workers > 1 and _env_bool(PTG2_FAST_OBJECT_ITERATOR_ENV, True):
            global _PTG2_WORKER_PROVIDER_MAP, _PTG2_WORKER_PLAN_FIELDS, _PTG2_WORKER_SNAPSHOT_ID, _PTG2_WORKER_PLAN_MONTH_ID, _PTG2_WORKER_SOURCE_TRACE
            global _PTG2_WORKER_SOURCE_TRACE_SET_HASH, _PTG2_WORKER_SLIM_SERVING_ROWS, _PTG2_WORKER_COMPACT_SERVING
            pool: concurrent.futures.ProcessPoolExecutor | None = None
            pending: set[concurrent.futures.Future] = set()
            pending_input_bytes_by_future: dict[concurrent.futures.Future, int] = {}
            pending_input_bytes = 0
            pending_batches: list[dict[str, Any] | bytes | bytearray] = []
            pending_batch_bytes = 0
            mp_context = multiprocessing.get_context("fork")

            async def _drain_completed_worker_results(done: set[concurrent.futures.Future]) -> None:
                nonlocal pending_input_bytes
                for future in done:
                    pending_input_bytes -= pending_input_bytes_by_future.pop(future, 0)
                    await add_serving_result(future.result())

            async def _wait_for_worker_capacity(next_batch_bytes: int) -> None:
                nonlocal pending
                while pending and _ptg2_worker_capacity_wait_needed(
                    pending_count=len(pending),
                    pending_input_bytes=pending_input_bytes,
                    next_batch_bytes=next_batch_bytes,
                    max_pending_batches=worker_max_pending_batches,
                    max_pending_bytes=worker_max_pending_bytes,
                ):
                    done, pending = concurrent.futures.wait(
                        pending,
                        return_when=concurrent.futures.FIRST_COMPLETED,
                    )
                    await _drain_completed_worker_results(done)

            async def _flush_pending_batches(force: bool = False) -> None:
                nonlocal pending_batches, pending_batch_bytes, pending_input_bytes
                if not pending_batches:
                    return
                if not force and len(pending_batches) < worker_chunk_items and pending_batch_bytes < worker_chunk_bytes:
                    return
                assert pool is not None
                batch = pending_batches
                batch_bytes = pending_batch_bytes
                pending_batches = []
                pending_batch_bytes = 0
                await _wait_for_worker_capacity(batch_bytes)
                worker_fn = (
                    _serving_only_worker_process_chunk_to_files
                    if worker_result_files
                    else _serving_only_worker_process_chunk
                )
                future = pool.submit(worker_fn, batch)
                pending.add(future)
                pending_input_bytes_by_future[future] = batch_bytes
                pending_input_bytes += batch_bytes

            try:
                item_prefixes = {
                    "provider_reference": "provider_references.item",
                    "in_network": "in_network.item",
                }
                if _env_bool(PTG2_RUST_SCANNER_ENV, False):
                    object_iterator = (
                        (
                            "provider_reference" if array_name == "provider_references" else "in_network",
                            _json_loads(raw_object) if array_name == "provider_references" else raw_object,
                        )
                        for array_name, raw_object in _iter_top_level_object_bytes_rust(
                            file_path,
                            {"provider_references", "in_network"},
                        )
                    )
                elif _env_bool(PTG2_JSON_DECODER_ITERATOR_ENV, True):
                    raw_object_names = {"in_network"} if _env_bool(PTG2_RAW_WORKER_OBJECTS_ENV, True) else set()
                    object_iterator = _iter_top_level_objects_jsondecoder(
                        afp,
                        item_prefixes,
                        progress_callback=progress_callback,
                        raw_object_names=raw_object_names,
                    )
                else:
                    object_iterator = (
                        (
                            "provider_reference" if array_name == "provider_references" else "in_network",
                            _json_loads(raw_object) if array_name == "provider_references" else raw_object,
                        )
                        for array_name, raw_object in _iter_top_level_object_bytes(
                            afp,
                            {"provider_references", "in_network"},
                            progress_callback=progress_callback,
                        )
                    )
                for object_name, payload in object_iterator:
                    if object_name == "provider_reference":
                        provider_ref_count += 1
                        if test_mode and provider_ref_count > TEST_PROVIDER_GROUPS:
                            continue
                        provider_group_id = _normalize_provider_ref(payload.get("provider_group_id"))
                        provider_entry, _provider_row = _build_provider_set_entry(
                            file_id=file_id,
                            provider_group_ref=provider_group_id,
                            provider_groups=payload.get("provider_groups", []),
                            network_names=payload.get("network_name") or payload.get("network_names") or [],
                        )
                        if provider_entry is not None:
                            _provider_cache_put(provider_map, provider_group_id, [provider_entry])
                            for member_row in _provider_group_member_rows(provider_entry):
                                _compact_add_unique(state, "provider_group_member", ("provider_group_hash", "npi"), member_row)
                            await _flush_compact_rows(state)
                        continue
                    if object_name != "in_network":
                        continue
                    if pool is None:
                        _PTG2_WORKER_PROVIDER_MAP = provider_map
                        _PTG2_WORKER_PLAN_FIELDS = plan_fields
                        _PTG2_WORKER_SNAPSHOT_ID = snapshot_id
                        _PTG2_WORKER_PLAN_MONTH_ID = plan_month_row["plan_month_id"]
                        _PTG2_WORKER_SOURCE_TRACE = source_trace_payload
                        _PTG2_WORKER_SOURCE_TRACE_SET_HASH = source_trace_set_hash
                        _PTG2_WORKER_SLIM_SERVING_ROWS = slim_serving_rows
                        _PTG2_WORKER_COMPACT_SERVING = compact_serving
                        pool = concurrent.futures.ProcessPoolExecutor(
                            max_workers=serving_workers,
                            mp_context=mp_context,
                        )
                    item_count += 1
                    payload_size = _worker_payload_size(payload)
                    if pending_batches and pending_batch_bytes + payload_size > worker_chunk_bytes:
                        await _flush_pending_batches(force=True)
                    pending_batches.append(payload)
                    pending_batch_bytes += payload_size
                    await _flush_pending_batches()
                    if max_items is not None and item_count >= max_items:
                        break
                    if test_mode and item_count >= TEST_IN_NETWORK_ITEMS:
                        break
                await _flush_pending_batches(force=True)
                while pending:
                    done, pending = concurrent.futures.wait(
                        pending,
                        return_when=concurrent.futures.FIRST_COMPLETED,
                    )
                    await _drain_completed_worker_results(done)
            finally:
                if pool is not None:
                    pool.shutdown(wait=True, cancel_futures=True)
        else:
            if _env_bool(PTG2_RUST_SCANNER_ENV, False):
                object_iterator = (
                    (
                        "provider_reference" if array_name == "provider_references" else "in_network",
                        _json_loads(raw_object),
                    )
                    for array_name, raw_object in _iter_top_level_object_bytes_rust(
                        file_path,
                        {"provider_references", "in_network"},
                    )
                )
            else:
                iterator_fn = (
                    _iter_top_level_objects_fast
                    if _env_bool(PTG2_FAST_OBJECT_ITERATOR_ENV, True)
                    else _iter_top_level_objects
                )
                object_iterator = iterator_fn(
                    afp,
                    {
                        "provider_reference": "provider_references.item",
                        "in_network": "in_network.item",
                    },
                    use_float=True,
                    progress_callback=progress_callback,
                )
            for object_name, payload in object_iterator:
                if object_name == "provider_reference":
                    provider_ref_count += 1
                    if test_mode and provider_ref_count > TEST_PROVIDER_GROUPS:
                        continue
                    provider_group_id = _normalize_provider_ref(payload.get("provider_group_id"))
                    provider_entry, _provider_row = _build_provider_set_entry(
                        file_id=file_id,
                        provider_group_ref=provider_group_id,
                        provider_groups=payload.get("provider_groups", []),
                        network_names=payload.get("network_name") or payload.get("network_names") or [],
                    )
                    if provider_entry is not None:
                        _provider_cache_put(provider_map, provider_group_id, [provider_entry])
                        for member_row in _provider_group_member_rows(provider_entry):
                            _compact_add_unique(state, "provider_group_member", ("provider_group_hash", "npi"), member_row)
                        await _flush_compact_rows(state)
                    continue

                if object_name != "in_network":
                    continue
                item_count += 1
                result = _serving_only_rows_for_payload(
                    payload,
                    provider_map=provider_map,
                    plan_fields=plan_fields,
                    snapshot_id=snapshot_id,
                    plan_month_id=plan_month_row["plan_month_id"],
                    source_trace_payload=source_trace_payload,
                    slim_serving_rows=slim_serving_rows,
                    compact_serving=compact_serving,
                    source_trace_set_hash=source_trace_set_hash,
                    include_price_set_rows=True,
                )
                await add_serving_result(result)
                if max_items is not None and item_count >= max_items:
                    break
                if test_mode and item_count >= TEST_IN_NETWORK_ITEMS:
                    break

    await _flush_compact_rows(state, force=True)
    await _drain_compact_writes(state)
    if _env_bool(PTG2_STAGE_SERVING_RATES_ENV, True) and not _use_stage_serving_as_final() and not compact_serving:
        await _merge_staged_serving_rates(snapshot_id)
    if slim_serving_rows and _env_bool(PTG2_STAGE_PRICE_SETS_ENV, True):
        await _merge_staged_price_sets(snapshot_id)
    if compact_serving and not _env_bool(PTG2_DEFER_PROVIDER_LOCATIONS_ENV, True):
        await _build_ptg2_provider_locations(snapshot_id)
    await flush_error_log(import_log_cls)
    summary = {
        "provider_refs": provider_ref_count,
        "in_network_items": item_count,
        "serving_rates": serving_rows,
        "serving_only": True,
        "serving_workers": serving_workers,
        "worker_chunk_items": worker_chunk_items,
    }
    if hasattr(provider_map, "stats"):
        summary.update(provider_map.stats())
    summary.update(provider_combo_stats)
    _emit_screen_line(f"PTG2 serving-only import summary: {summary}")
    logger.info("PTG2 serving-only import summary: %s", summary)
    return summary


async def _load_provider_references_from_file(
    file_path: str,
    file_id: int,
    provider_cls,
    provider_map,
    test_mode: bool,
    log_cls,
    source_url: str,
) -> None:
    rows: list[dict[str, Any]] = []
    seen_hashes: set[int] = set()
    provider_ref_batch_rows = max(_env_int(PTG2_PROVIDER_REF_BATCH_ROWS_ENV, 5000), 1)
    with open_json_artifact_stream(file_path) as afp:
        ref_count = 0
        for ref in ijson.items(afp, "provider_references.item"):
            ref_count += 1
            provider_group_id = _normalize_provider_ref(ref.get("provider_group_id"))
            network_names = ref.get("network_name") or ref.get("network_names") or []
            provider_entry, provider_row = _build_provider_set_entry(
                file_id=file_id,
                provider_group_ref=provider_group_id,
                provider_groups=ref.get("provider_groups", []),
                network_names=network_names,
            )
            if provider_entry is None or provider_row is None:
                continue
            provider_hash = provider_entry["__hash__"]
            if provider_hash not in seen_hashes:
                seen_hashes.add(provider_hash)
                rows.append(provider_row)
            if len(rows) >= provider_ref_batch_rows:
                await push_objects(_dedupe_rows_by(rows, "provider_group_hash"), provider_cls)
                rows = []
            _provider_cache_put(provider_map, provider_group_id, [provider_entry])
            if test_mode and ref_count >= TEST_PROVIDER_GROUPS:
                break
    if isinstance(provider_map, PTG2ProviderReferenceCache):
        provider_map.commit()
    if rows:
        await push_objects(_dedupe_rows_by(rows, "provider_group_hash"), provider_cls)
    await flush_error_log(log_cls)
    return None


async def _parse_in_network_items(
    file_path: str,
    file_id: int,
    meta: dict[str, Any],
    plan_info: list[dict[str, Any]] | None,
    provider_map,
    classes: dict[str, type],
    test_mode: bool,
    import_log_cls,
    source_url: str,
    max_items: int | None = None,
) -> None:
    provider_cls = classes["PTGProviderGroup"]
    item_cls = classes["PTGInNetworkItem"]
    billing_cls = classes["PTGBillingCode"]
    rate_cls = classes["PTGNegotiatedRate"]
    price_cls = classes["PTGNegotiatedPrice"]

    item_rows: list[dict[str, Any]] = []
    billing_rows: list[dict[str, Any]] = []
    rate_rows: list[dict[str, Any]] = []
    price_rows: list[dict[str, Any]] = []
    provider_rows: list[dict[str, Any]] = []
    provider_hash_seen: set[int] = _provider_cache_hashes(provider_map)
    rate_hash_seen: set[int] = set()
    price_hash_seen: set[int] = set()
    item_batch_rows = max(_env_int(PTG2_ITEM_BATCH_ROWS_ENV, 1000), 1)
    billing_batch_rows = max(_env_int(PTG2_BILLING_BATCH_ROWS_ENV, 5000), 1)
    rate_batch_rows = max(_env_int(PTG2_RATE_BATCH_ROWS_ENV, 5000), 1)
    price_batch_rows = max(_env_int(PTG2_PRICE_BATCH_ROWS_ENV, 10000), 1)

    plan_fields = _derive_plan_fields(meta, plan_info)

    with open_json_artifact_stream(file_path) as afp:
        count = 0
        for in_item in ijson.items(afp, "in_network.item", use_float=True):
            count += 1
            item_hash = _make_checksum(
                file_id,
                in_item.get("billing_code_type"),
                in_item.get("billing_code"),
                in_item.get("negotiation_arrangement"),
                in_item.get("name"),
            )
            item_rows.append(
                {
                    "item_hash": item_hash,
                    "file_id": file_id,
                    "negotiation_arrangement": in_item.get("negotiation_arrangement"),
                    "name": in_item.get("name"),
                    "billing_code_type": in_item.get("billing_code_type"),
                    "billing_code_type_version": in_item.get("billing_code_type_version"),
                    "billing_code": in_item.get("billing_code"),
                    "description": in_item.get("description"),
                    "severity_of_illness": in_item.get("severity_of_illness"),
                    "plan_name": plan_fields.get("plan_name"),
                    "plan_id_type": plan_fields.get("plan_id_type"),
                    "plan_id": plan_fields.get("plan_id"),
                    "plan_market_type": plan_fields.get("plan_market_type"),
                    "issuer_name": plan_fields.get("issuer_name"),
                    "plan_sponsor_name": plan_fields.get("plan_sponsor_name"),
                }
            )
            for code in in_item.get("bundled_codes", []):
                billing_rows.append(
                    {
                        "code_hash": _make_checksum(item_hash, "bundle", code.get("billing_code")),
                        "item_hash": item_hash,
                        "code_role": "bundle",
                        "billing_code_type": code.get("billing_code_type"),
                        "billing_code_type_version": code.get("billing_code_type_version"),
                        "billing_code": code.get("billing_code"),
                        "description": code.get("description"),
                    }
                )
            for code in in_item.get("covered_services", []):
                billing_rows.append(
                    {
                        "code_hash": _make_checksum(item_hash, "covered", code.get("billing_code")),
                        "item_hash": item_hash,
                        "code_role": "covered",
                        "billing_code_type": code.get("billing_code_type"),
                        "billing_code_type_version": code.get("billing_code_type_version"),
                        "billing_code": code.get("billing_code"),
                        "description": code.get("description"),
                    }
                )

            for negotiated_rate in in_item.get("negotiated_rates", []):
                provider_refs = negotiated_rate.get("provider_references") or []
                provider_groups_inline = negotiated_rate.get("provider_groups") or []
                groups_to_use: list[dict[str, Any]] = []
                for provider_ref in provider_refs:
                    provider_key = _normalize_provider_ref(provider_ref)
                    groups = _provider_cache_get(provider_map, provider_key) or _provider_cache_get(provider_map, provider_ref)
                    if not groups:
                        await log_error(
                            "err",
                            f"Provider reference {provider_ref} not found for file {source_url}",
                            [0],
                            source_url,
                            "ptg-in-network",
                            "json",
                            import_log_cls,
                        )
                        continue
                    groups_to_use.extend(groups)
                if provider_groups_inline:
                    inline_ref = (
                        _normalize_provider_ref(provider_groups_inline[0].get("provider_group_id"))
                        if len(provider_groups_inline) == 1
                        else None
                    )
                    inline_entry, inline_row = _build_provider_set_entry(
                        file_id=file_id,
                        provider_group_ref=inline_ref,
                        provider_groups=provider_groups_inline,
                        network_names=negotiated_rate.get("network_name") or [],
                    )
                    if inline_entry is not None and inline_row is not None:
                        inline_hash = inline_entry["__hash__"]
                        if inline_hash not in provider_hash_seen:
                            provider_hash_seen.add(inline_hash)
                            provider_rows.append(inline_row)
                        groups_to_use.append(inline_entry)
                combined_entry, combined_row = _combine_provider_set_entries(
                    file_id=file_id,
                    entries=groups_to_use,
                    network_names=negotiated_rate.get("network_name") or [],
                )
                if combined_entry is not None and combined_row is not None:
                    combined_hash = combined_entry["__hash__"]
                    if combined_hash not in provider_hash_seen:
                        provider_hash_seen.add(combined_hash)
                        provider_rows.append(combined_row)
                    groups_to_use = [combined_entry]
                else:
                    groups_to_use = []
                for group in groups_to_use:
                    rate_hash = _make_checksum(item_hash, group.get("__hash__") or "")
                    if rate_hash not in rate_hash_seen:
                        rate_hash_seen.add(rate_hash)
                        rate_rows.append(
                            {
                                "rate_hash": rate_hash,
                                "item_hash": item_hash,
                                "provider_group_ref": group.get("provider_group_id"),
                                "provider_group_hash": group.get("__hash__"),
                            }
                        )
                    for negotiated_price in negotiated_rate.get("negotiated_prices", []):
                        price_hash = _make_checksum(
                            rate_hash,
                            negotiated_price.get("negotiated_type"),
                            negotiated_price.get("negotiated_rate"),
                            negotiated_price.get("expiration_date") or "",
                            negotiated_price.get("billing_class"),
                            negotiated_price.get("setting"),
                            "|".join(_as_list(negotiated_price.get("service_code"))),
                            "|".join(_as_list(negotiated_price.get("billing_code_modifier"))),
                        )
                        if price_hash in price_hash_seen:
                            continue
                        price_hash_seen.add(price_hash)
                        price_rows.append(
                            {
                                "price_hash": price_hash,
                                "rate_hash": rate_hash,
                                "negotiated_type": negotiated_price.get("negotiated_type"),
                                "negotiated_rate": negotiated_price.get("negotiated_rate"),
                                "expiration_date": _coerce_date(negotiated_price.get("expiration_date")),
                                "service_code": _as_list(negotiated_price.get("service_code")),
                                "billing_class": negotiated_price.get("billing_class"),
                                "setting": negotiated_price.get("setting"),
                                "billing_code_modifier": _as_list(negotiated_price.get("billing_code_modifier")),
                                "additional_information": negotiated_price.get("additional_information"),
                            }
                        )
                        if test_mode and len(price_rows) >= TEST_NEGOTIATED_PRICES:
                            break
                if test_mode and len(price_rows) >= TEST_NEGOTIATED_PRICES:
                    break

            if len(item_rows) >= item_batch_rows:
                await push_objects(_dedupe_rows_by(item_rows, "item_hash"), item_cls)
                item_rows = []
            if len(billing_rows) >= billing_batch_rows:
                await push_objects(_dedupe_rows_by(billing_rows, "code_hash"), billing_cls)
                billing_rows = []
            if len(rate_rows) >= rate_batch_rows:
                await push_objects(_dedupe_rows_by(rate_rows, "rate_hash"), rate_cls)
                rate_rows = []
            if len(price_rows) >= price_batch_rows:
                await push_objects(_dedupe_rows_by(price_rows, "price_hash"), price_cls)
                price_rows = []

            if max_items is not None and count >= max_items:
                break
            if test_mode and count >= TEST_IN_NETWORK_ITEMS:
                break

    if item_rows:
        await push_objects(_dedupe_rows_by(item_rows, "item_hash"), item_cls)
    if billing_rows:
        await push_objects(_dedupe_rows_by(billing_rows, "code_hash"), billing_cls)
    if rate_rows:
        await push_objects(_dedupe_rows_by(rate_rows, "rate_hash"), rate_cls)
    if price_rows:
        await push_objects(_dedupe_rows_by(price_rows, "price_hash"), price_cls)
    if provider_rows:
        await push_objects(_dedupe_rows_by(provider_rows, "provider_group_hash"), provider_cls)
    await flush_error_log(import_log_cls)


async def _parse_in_network_file_single_pass(
    file_path: str,
    file_id: int,
    meta: dict[str, Any],
    plan_info: list[dict[str, Any]] | None,
    provider_map,
    classes: dict[str, type],
    test_mode: bool,
    import_log_cls,
    source_url: str,
    max_items: int | None = None,
) -> None:
    """
    Parse provider_references and in_network arrays in one physical pass.

    Large UHC files are gzip streams, so a second parser pass means repeating
    all decompression. The spec places provider_references before in_network;
    when that is true this keeps provider reference storage bounded without
    replaying the gzip.
    """
    provider_cls = classes["PTGProviderGroup"]
    item_cls = classes["PTGInNetworkItem"]
    billing_cls = classes["PTGBillingCode"]
    rate_cls = classes["PTGNegotiatedRate"]
    price_cls = classes["PTGNegotiatedPrice"]

    provider_ref_batch_rows = max(_env_int(PTG2_PROVIDER_REF_BATCH_ROWS_ENV, 5000), 1)
    item_batch_rows = max(_env_int(PTG2_ITEM_BATCH_ROWS_ENV, 1000), 1)
    billing_batch_rows = max(_env_int(PTG2_BILLING_BATCH_ROWS_ENV, 5000), 1)
    rate_batch_rows = max(_env_int(PTG2_RATE_BATCH_ROWS_ENV, 5000), 1)
    price_batch_rows = max(_env_int(PTG2_PRICE_BATCH_ROWS_ENV, 10000), 1)

    plan_fields = _derive_plan_fields(meta, plan_info)
    provider_rows: list[dict[str, Any]] = []
    item_rows: list[dict[str, Any]] = []
    billing_rows: list[dict[str, Any]] = []
    rate_rows: list[dict[str, Any]] = []
    price_rows: list[dict[str, Any]] = []
    provider_ref_hash_seen: set[int] = set()
    provider_hash_seen: set[int] = _provider_cache_hashes(provider_map)
    item_hash_seen: set[int] = set()
    billing_hash_seen: set[int] = set()
    rate_hash_seen: set[int] = set()
    price_hash_seen: set[int] = set()
    ref_count = 0
    item_count = 0
    progress_state: dict[str, Any] = {}

    with open_json_artifact_stream(file_path) as afp:
        def progress_callback() -> None:
            _maybe_log_artifact_progress(
                file_path,
                afp,
                progress_state,
                "in-network import",
                ref_count=ref_count,
                item_count=item_count,
            )

        iterator_fn = (
            _iter_top_level_objects_fast
            if _env_bool(PTG2_FAST_OBJECT_ITERATOR_ENV, True)
            else _iter_top_level_objects
        )
        for object_name, payload in iterator_fn(
            afp,
            {
                "provider_reference": "provider_references.item",
                "in_network": "in_network.item",
            },
            use_float=True,
            progress_callback=progress_callback,
        ):
            if object_name == "provider_reference":
                ref_count += 1
                if test_mode and ref_count > TEST_PROVIDER_GROUPS:
                    continue
                provider_group_id = _normalize_provider_ref(payload.get("provider_group_id"))
                network_names = payload.get("network_name") or payload.get("network_names") or []
                provider_entry, provider_row = _build_provider_set_entry(
                    file_id=file_id,
                    provider_group_ref=provider_group_id,
                    provider_groups=payload.get("provider_groups", []),
                    network_names=network_names,
                )
                if provider_entry is None or provider_row is None:
                    continue
                provider_hash = provider_entry["__hash__"]
                if provider_hash not in provider_ref_hash_seen:
                    provider_ref_hash_seen.add(provider_hash)
                    provider_hash_seen.add(provider_hash)
                    provider_rows.append(provider_row)
                _provider_cache_put(provider_map, provider_group_id, [provider_entry])
                if len(provider_rows) >= provider_ref_batch_rows:
                    await push_objects(_dedupe_rows_by(provider_rows, "provider_group_hash"), provider_cls)
                    provider_rows = []
                    if isinstance(provider_map, PTG2ProviderReferenceCache):
                        provider_map.commit()
                continue

            if object_name != "in_network":
                continue
            item_count += 1
            item_hash = _make_checksum(
                file_id,
                payload.get("billing_code_type"),
                payload.get("billing_code"),
                payload.get("negotiation_arrangement"),
                payload.get("name"),
            )
            if item_hash not in item_hash_seen:
                item_hash_seen.add(item_hash)
                item_rows.append(
                    {
                        "item_hash": item_hash,
                        "file_id": file_id,
                        "negotiation_arrangement": payload.get("negotiation_arrangement"),
                        "name": payload.get("name"),
                        "billing_code_type": payload.get("billing_code_type"),
                        "billing_code_type_version": payload.get("billing_code_type_version"),
                        "billing_code": payload.get("billing_code"),
                        "description": payload.get("description"),
                        "severity_of_illness": payload.get("severity_of_illness"),
                        "plan_name": plan_fields.get("plan_name"),
                        "plan_id_type": plan_fields.get("plan_id_type"),
                        "plan_id": plan_fields.get("plan_id"),
                        "plan_market_type": plan_fields.get("plan_market_type"),
                        "issuer_name": plan_fields.get("issuer_name"),
                        "plan_sponsor_name": plan_fields.get("plan_sponsor_name"),
                    }
            )
            for code in payload.get("bundled_codes", []):
                code_hash = _make_checksum(item_hash, "bundle", code.get("billing_code"))
                if code_hash in billing_hash_seen:
                    continue
                billing_hash_seen.add(code_hash)
                billing_rows.append(
                    {
                        "code_hash": code_hash,
                        "item_hash": item_hash,
                        "code_role": "bundle",
                        "billing_code_type": code.get("billing_code_type"),
                        "billing_code_type_version": code.get("billing_code_type_version"),
                        "billing_code": code.get("billing_code"),
                        "description": code.get("description"),
                    }
                )
            for code in payload.get("covered_services", []):
                code_hash = _make_checksum(item_hash, "covered", code.get("billing_code"))
                if code_hash in billing_hash_seen:
                    continue
                billing_hash_seen.add(code_hash)
                billing_rows.append(
                    {
                        "code_hash": code_hash,
                        "item_hash": item_hash,
                        "code_role": "covered",
                        "billing_code_type": code.get("billing_code_type"),
                        "billing_code_type_version": code.get("billing_code_type_version"),
                        "billing_code": code.get("billing_code"),
                        "description": code.get("description"),
                    }
                )

            for negotiated_rate in payload.get("negotiated_rates", []):
                provider_refs = negotiated_rate.get("provider_references") or []
                provider_groups_inline = negotiated_rate.get("provider_groups") or []
                groups_to_use: list[dict[str, Any]] = []
                for provider_ref in provider_refs:
                    provider_key = _normalize_provider_ref(provider_ref)
                    groups = _provider_cache_get(provider_map, provider_key) or _provider_cache_get(provider_map, provider_ref)
                    if not groups:
                        await log_error(
                            "err",
                            f"Provider reference {provider_ref} not found for file {source_url}",
                            [0],
                            source_url,
                            "ptg-in-network",
                            "json",
                            import_log_cls,
                        )
                        continue
                    groups_to_use.extend(groups)
                if provider_groups_inline:
                    inline_ref = (
                        _normalize_provider_ref(provider_groups_inline[0].get("provider_group_id"))
                        if len(provider_groups_inline) == 1
                        else None
                    )
                    inline_entry, inline_row = _build_provider_set_entry(
                        file_id=file_id,
                        provider_group_ref=inline_ref,
                        provider_groups=provider_groups_inline,
                        network_names=negotiated_rate.get("network_name") or [],
                    )
                    if inline_entry is not None and inline_row is not None:
                        inline_hash = inline_entry["__hash__"]
                        if inline_hash not in provider_hash_seen:
                            provider_hash_seen.add(inline_hash)
                            provider_rows.append(inline_row)
                        groups_to_use.append(inline_entry)
                combined_entry, combined_row = _combine_provider_set_entries(
                    file_id=file_id,
                    entries=groups_to_use,
                    network_names=negotiated_rate.get("network_name") or [],
                )
                if combined_entry is not None and combined_row is not None:
                    combined_hash = combined_entry["__hash__"]
                    if combined_hash not in provider_hash_seen:
                        provider_hash_seen.add(combined_hash)
                        provider_rows.append(combined_row)
                    groups_to_use = [combined_entry]
                else:
                    groups_to_use = []
                for group in groups_to_use:
                    rate_hash = _make_checksum(item_hash, group.get("__hash__") or "")
                    if rate_hash not in rate_hash_seen:
                        rate_hash_seen.add(rate_hash)
                        rate_rows.append(
                            {
                                "rate_hash": rate_hash,
                                "item_hash": item_hash,
                                "provider_group_ref": group.get("provider_group_id"),
                                "provider_group_hash": group.get("__hash__"),
                            }
                        )
                    for negotiated_price in negotiated_rate.get("negotiated_prices", []):
                        price_hash = _make_checksum(
                            rate_hash,
                            negotiated_price.get("negotiated_type"),
                            negotiated_price.get("negotiated_rate"),
                            negotiated_price.get("expiration_date") or "",
                            negotiated_price.get("billing_class"),
                            negotiated_price.get("setting"),
                            "|".join(_as_list(negotiated_price.get("service_code"))),
                            "|".join(_as_list(negotiated_price.get("billing_code_modifier"))),
                        )
                        if price_hash in price_hash_seen:
                            continue
                        price_hash_seen.add(price_hash)
                        price_rows.append(
                            {
                                "price_hash": price_hash,
                                "rate_hash": rate_hash,
                                "negotiated_type": negotiated_price.get("negotiated_type"),
                                "negotiated_rate": negotiated_price.get("negotiated_rate"),
                                "expiration_date": _coerce_date(negotiated_price.get("expiration_date")),
                                "service_code": _as_list(negotiated_price.get("service_code")),
                                "billing_class": negotiated_price.get("billing_class"),
                                "setting": negotiated_price.get("setting"),
                                "billing_code_modifier": _as_list(negotiated_price.get("billing_code_modifier")),
                                "additional_information": negotiated_price.get("additional_information"),
                            }
                        )
                        if test_mode and len(price_rows) >= TEST_NEGOTIATED_PRICES:
                            break
                if test_mode and len(price_rows) >= TEST_NEGOTIATED_PRICES:
                    break

            if provider_rows and len(provider_rows) >= provider_ref_batch_rows:
                await push_objects(_dedupe_rows_by(provider_rows, "provider_group_hash"), provider_cls)
                provider_rows = []
            item_rows, billing_rows, rate_rows, price_rows = await _flush_in_network_rows(
                item_rows,
                billing_rows,
                rate_rows,
                price_rows,
                item_cls,
                billing_cls,
                rate_cls,
                price_cls,
                item_batch_rows=item_batch_rows,
                billing_batch_rows=billing_batch_rows,
                rate_batch_rows=rate_batch_rows,
                price_batch_rows=price_batch_rows,
            )
            if isinstance(provider_map, PTG2ProviderReferenceCache):
                provider_map.commit()

            if max_items is not None and item_count >= max_items:
                break
            if test_mode and item_count >= TEST_IN_NETWORK_ITEMS:
                break

    if progress_state:
        progress_state["last_log"] = 0
        _maybe_log_artifact_progress(
            file_path,
            afp if "afp" in locals() else None,
            progress_state,
            "in-network import",
            ref_count=ref_count,
            item_count=item_count,
        )
    if isinstance(provider_map, PTG2ProviderReferenceCache):
        provider_map.commit()
    if provider_rows:
        await push_objects(_dedupe_rows_by(provider_rows, "provider_group_hash"), provider_cls)
    await _flush_in_network_rows(
        item_rows,
        billing_rows,
        rate_rows,
        price_rows,
        item_cls,
        billing_cls,
        rate_cls,
        price_cls,
        force=True,
        item_batch_rows=item_batch_rows,
        billing_batch_rows=billing_batch_rows,
        rate_batch_rows=rate_batch_rows,
        price_batch_rows=price_batch_rows,
    )
    await flush_error_log(import_log_cls)


async def _parse_in_network_file_compact(
    file_path: str,
    file_id: int,
    meta: dict[str, Any],
    plan_info: list[dict[str, Any]] | None,
    provider_map,
    test_mode: bool,
    import_log_cls,
    source_url: str,
    source_version: PTG2SourceVersion | None,
    snapshot_id: str,
    import_month: datetime.date,
    max_items: int | None = None,
) -> dict[str, Any]:
    plan_fields = _derive_plan_fields(meta, plan_info)
    plan_row, alias_rows, plan_month_row = _ptg2_plan_rows(plan_fields, snapshot_id, import_month)
    context_row = _ptg2_context_row(plan_fields, import_month, source_version)
    source_trace_row, source_trace_set_row = _ptg2_source_trace_rows(source_version, source_url)
    await _push_ptg2_objects([plan_row], PTG2Plan, rewrite=True)
    if alias_rows:
        await _push_ptg2_objects(alias_rows, PTG2PlanAlias, rewrite=True)
    await _push_ptg2_objects([plan_month_row], PTG2PlanMonth, rewrite=True)
    await _push_ptg2_objects([context_row], PTG2RateSetContext, rewrite=True)

    state = _compact_state()
    state["snapshot_id"] = snapshot_id
    state["plan_fields"] = plan_fields
    state["source_trace_payload"] = _ptg2_source_trace_payload(source_trace_row)
    _compact_add_unique(state, "source_trace", "source_trace_hash", source_trace_row)
    _compact_add_unique(state, "source_trace_set", "source_trace_set_hash", source_trace_set_row)
    source_trace_set_hash = source_trace_set_row["source_trace_set_hash"]
    context_hash = context_row["context_hash"]
    provider_ref_count = 0
    item_count = 0
    progress_state: dict[str, Any] = {}
    rate_group_flush_items = max(_env_int(PTG2_RATE_GROUP_FLUSH_ITEMS_ENV, 1000), 0)
    provider_combo_cache_limit = max(_env_int(PTG2_PROVIDER_COMBO_CACHE_REFS_ENV, 32768), 0)
    fast_provider_aggregation = _env_bool(
        PTG2_FAST_PROVIDER_AGGREGATION_ENV,
        _env_bool(PTG2_FAST_PROVIDER_UNION_ENV, False),
    )
    provider_combo_cache: OrderedDict[tuple[str, ...], dict[str, Any]] = OrderedDict()
    provider_combo_stats = {
        "provider_combo_cache_gets": 0,
        "provider_combo_cache_hits": 0,
        "provider_combo_cache_misses": 0,
        "provider_combo_cache_size": 0,
        "provider_combo_cache_limit": provider_combo_cache_limit,
    }

    with open_json_artifact_stream(file_path) as afp:
        def progress_callback() -> None:
            _maybe_log_artifact_progress(
                file_path,
                afp,
                progress_state,
                "compact in-network import",
                ref_count=provider_ref_count,
                item_count=item_count,
            )

        for object_name, payload in _iter_top_level_objects(
            afp,
            {
                "provider_reference": "provider_references.item",
                "in_network": "in_network.item",
            },
            use_float=True,
            progress_callback=progress_callback,
        ):
            if object_name == "provider_reference":
                provider_ref_count += 1
                if test_mode and provider_ref_count > TEST_PROVIDER_GROUPS:
                    continue
                provider_group_id = _normalize_provider_ref(payload.get("provider_group_id"))
                network_names = payload.get("network_name") or payload.get("network_names") or []
                for provider_group_row in _ptg2_provider_group_rows(
                    provider_groups=payload.get("provider_groups", []),
                ):
                    if _compact_add_unique(state, "provider_group", "provider_group_hash", provider_group_row):
                        state["counts"]["provider_groups"] += 1
                provider_entry, _provider_row = _build_provider_set_entry(
                    file_id=file_id,
                    provider_group_ref=provider_group_id,
                    provider_groups=payload.get("provider_groups", []),
                    network_names=network_names,
                )
                if provider_entry is None:
                    continue
                _provider_cache_put(provider_map, provider_group_id, [provider_entry])
                continue

            if object_name != "in_network":
                continue
            item_count += 1
            procedure_row = _ptg2_procedure_row(payload)
            procedure_hash = procedure_row["procedure_hash"]
            state["procedure_payloads"][procedure_hash] = {
                "billing_code_type": procedure_row.get("billing_code_type"),
                "billing_code": procedure_row.get("billing_code"),
                "name": procedure_row.get("name"),
                "description": procedure_row.get("description"),
            }
            if _compact_add_unique(state, "procedure", "procedure_hash", procedure_row):
                state["counts"]["procedures"] += 1

            item_price_groups: dict[str, dict[str, Any]] = {}
            for negotiated_rate in payload.get("negotiated_rates", []):
                provider_groups_inline = negotiated_rate.get("provider_groups") or []
                provider_refs = negotiated_rate.get("provider_references") or []
                combined_entry = None
                if provider_refs and not provider_groups_inline:
                    combo_key = _provider_combo_cache_key(provider_refs)
                    combined_entry = _provider_combo_cache_get(provider_combo_cache, combo_key, provider_combo_stats)
                else:
                    combo_key = ()
                if combined_entry is None:
                    groups_to_use: list[dict[str, Any]] = []
                    if fast_provider_aggregation and provider_refs and not provider_groups_inline:
                        combined_entry, missing_refs = _fast_provider_entry_from_provider_refs(provider_map, provider_refs)
                        for provider_ref in missing_refs:
                            await log_error(
                                "err",
                                f"Provider reference {provider_ref} not found for file {source_url}",
                                [0],
                                source_url,
                                "ptg-in-network-compact",
                                "json",
                                import_log_cls,
                            )
                    else:
                        for provider_ref in provider_refs:
                            provider_key = _normalize_provider_ref(provider_ref)
                            groups = _provider_cache_get(provider_map, provider_key) or _provider_cache_get(provider_map, provider_ref)
                            if not groups:
                                await log_error(
                                    "err",
                                    f"Provider reference {provider_ref} not found for file {source_url}",
                                    [0],
                                    source_url,
                                    "ptg-in-network-compact",
                                    "json",
                                    import_log_cls,
                                )
                                continue
                            groups_to_use.extend(groups)
                    if provider_groups_inline:
                        for provider_group_row in _ptg2_provider_group_rows(provider_groups=provider_groups_inline):
                            if _compact_add_unique(state, "provider_group", "provider_group_hash", provider_group_row):
                                state["counts"]["provider_groups"] += 1
                        inline_ref = (
                            _normalize_provider_ref(provider_groups_inline[0].get("provider_group_id"))
                            if len(provider_groups_inline) == 1
                            else None
                        )
                        inline_entry, _inline_row = _build_provider_set_entry(
                            file_id=file_id,
                            provider_group_ref=inline_ref,
                            provider_groups=provider_groups_inline,
                            network_names=negotiated_rate.get("network_name") or [],
                        )
                        if inline_entry is not None:
                            groups_to_use.append(inline_entry)
                    if combined_entry is None:
                        combined_entry, _combined_row = _combine_provider_set_entries(
                            file_id=file_id,
                            entries=groups_to_use,
                            network_names=negotiated_rate.get("network_name") or [],
                        )
                    if combined_entry is not None and combo_key:
                        _provider_combo_cache_put(
                            provider_combo_cache,
                            combo_key,
                            combined_entry,
                            provider_combo_stats,
                            provider_combo_cache_limit,
                        )
                if combined_entry is None:
                    continue
                price_atom_hashes: list[str] = []
                price_payload: list[dict[str, Any]] = []
                for negotiated_price in negotiated_rate.get("negotiated_prices", []):
                    price_atom_row = _ptg2_price_atom_row(negotiated_price)
                    if _compact_add_unique(state, "price_atom", "price_atom_hash", price_atom_row):
                        state["counts"]["price_atoms"] += 1
                    price_atom_hashes.append(price_atom_row["price_atom_hash"])
                    price_payload.append(_ptg2_price_atom_payload(price_atom_row))
                if not price_atom_hashes:
                    continue
                price_set = build_price_set(price_atom_hashes)
                price_set_row = {
                    **price_set,
                    "created_at": _utcnow(),
                }
                if _compact_add_unique(state, "price_set", "price_set_hash", price_set_row):
                    state["counts"]["price_sets"] += 1
                state["price_payloads"][price_set_row["price_set_hash"]] = price_payload

                item_group = item_price_groups.setdefault(
                    price_set_row["price_set_hash"],
                    {
                        "price_set_hash": price_set_row["price_set_hash"],
                        "provider_entries": [],
                        "provider_entry_hashes": set(),
                        "provider_group_hashes": set(),
                        "provider_count": 0,
                        "network_names": set(),
                    },
                )
                if fast_provider_aggregation:
                    entry_hash = int(combined_entry["__hash__"])
                    if entry_hash not in item_group["provider_entry_hashes"]:
                        item_group["provider_entry_hashes"].add(entry_hash)
                        item_group["provider_group_hashes"].update(
                            int(value) for value in combined_entry.get("provider_group_hashes") or [entry_hash]
                        )
                        item_group["provider_count"] += int(
                            combined_entry.get("provider_count") or len(_as_int_list(combined_entry.get("npi")))
                        )
                        item_group["network_names"].update(
                            str(value) for value in _as_list(combined_entry.get("network_name")) if value
                        )
                else:
                    item_group["provider_entries"].append(combined_entry)

            for item_group in item_price_groups.values():
                if fast_provider_aggregation:
                    super_entry = _fast_provider_entry_from_parts(
                        entry_hashes=item_group["provider_entry_hashes"],
                        provider_group_hashes=item_group["provider_group_hashes"],
                        provider_count=item_group["provider_count"],
                        network_names=item_group["network_names"],
                    )
                else:
                    super_entry, _super_row = _combine_provider_set_entries(
                        file_id=file_id,
                        entries=item_group["provider_entries"],
                        network_names=[],
                    )
                if super_entry is None:
                    continue
                provider_set_row = _ptg2_provider_set_row(super_entry)
                state["provider_set_counts"][provider_set_row["provider_set_hash"]] = provider_set_row["provider_count"]
                if _compact_add_unique(state, "provider_set", "provider_set_hash", provider_set_row):
                    state["counts"]["provider_sets"] += 1
                provider_set_hash = provider_set_row["provider_set_hash"]
                group_key = (procedure_hash, item_group["price_set_hash"], source_trace_set_hash)
                state["rate_pack_groups"].setdefault(group_key, set()).add(provider_set_hash)
            await _flush_compact_rows(state)
            if rate_group_flush_items and item_count % rate_group_flush_items == 0:
                await _flush_compact_rate_pack_groups(state, context_hash)

            if max_items is not None and item_count >= max_items:
                break
            if test_mode and item_count >= TEST_IN_NETWORK_ITEMS:
                break

    await _flush_compact_rows(state, force=True)
    await _flush_compact_rate_pack_groups(state, context_hash)
    await _drain_compact_writes(state)
    if _env_bool(PTG2_STAGE_SERVING_RATES_ENV, True) and not _use_stage_serving_as_final():
        await _merge_staged_serving_rates(snapshot_id)
    if _env_bool(PTG2_STAGE_PRICE_SETS_ENV, True):
        await _merge_staged_price_sets(snapshot_id)
    chunk_rows: list[dict[str, Any]] = []
    chunk_hashes: list[str] = []
    for (procedure_hash, provider_bucket), rate_pack_hashes in state["chunk_rate_packs"].items():
        chunk = build_fact_chunk(
            context_hash,
            PTG2_DOMAIN_IN_NETWORK,
            procedure_hash,
            provider_bucket,
            list(rate_pack_hashes),
        )
        chunk_rows.append({**chunk, "created_at": _utcnow()})
        chunk_hashes.append(chunk["fact_chunk_hash"])
    if chunk_rows:
        await _push_ptg2_objects(chunk_rows, PTG2FactChunk, rewrite=True)
    rate_set = build_rate_set(context_hash, chunk_hashes)
    rate_set_row = {**rate_set, "created_at": _utcnow()}
    await _push_ptg2_objects([rate_set_row], PTG2RateSet, rewrite=True)
    await _push_ptg2_objects(
        [
            {
                "plan_rate_set_id": semantic_hash(
                    {
                        "plan_month_id": plan_month_row["plan_month_id"],
                        "rate_set_hash": rate_set_row["rate_set_hash"],
                        "domain": PTG2_DOMAIN_IN_NETWORK,
                    },
                    domain="plan_rate_set",
                )[:32],
                "plan_month_id": plan_month_row["plan_month_id"],
                "rate_set_hash": rate_set_row["rate_set_hash"],
                "domain": PTG2_DOMAIN_IN_NETWORK,
                "created_at": _utcnow(),
            }
        ],
        PTG2PlanRateSet,
        rewrite=True,
    )
    await flush_error_log(import_log_cls)
    summary = {
        **state["counts"],
        "provider_refs": provider_ref_count,
        "in_network_items": item_count,
        "fact_chunks": len(chunk_rows),
        "rate_set_hash": rate_set_row["rate_set_hash"],
    }
    if hasattr(provider_map, "stats"):
        summary.update(provider_map.stats())
    summary.update(provider_combo_stats)
    _emit_screen_line(f"PTG2 compact import summary: {summary}")
    logger.info("PTG2 compact import summary: %s", summary)
    return summary


async def _parse_allowed_amounts(
    file_path: str,
    file_id: int,
    meta: dict[str, Any],
    plan_info: list[dict[str, Any]] | None,
    classes: dict[str, type],
    test_mode: bool,
    import_log_cls,
    source_url: str,
    max_items: int | None = None,
) -> None:
    item_cls = classes["PTGAllowedItem"]
    payment_cls = classes["PTGAllowedPayment"]
    provider_payment_cls = classes["PTGAllowedProviderPayment"]

    plan_fields = _derive_plan_fields(meta, plan_info)

    item_rows: list[dict[str, Any]] = []
    payment_rows: list[dict[str, Any]] = []
    provider_payment_rows: list[dict[str, Any]] = []

    with open_json_artifact_stream(file_path) as afp:
        count = 0
        for out_item in ijson.items(afp, "out_of_network.item", use_float=True):
            count += 1
            item_hash = _make_checksum(
                file_id,
                out_item.get("billing_code_type"),
                out_item.get("billing_code"),
                out_item.get("name"),
            )
            item_rows.append(
                {
                    "allowed_item_hash": item_hash,
                    "file_id": file_id,
                    "name": out_item.get("name"),
                    "billing_code_type": out_item.get("billing_code_type"),
                    "billing_code_type_version": out_item.get("billing_code_type_version"),
                    "billing_code": out_item.get("billing_code"),
                    "description": out_item.get("description"),
                    "plan_name": plan_fields.get("plan_name"),
                    "plan_id_type": plan_fields.get("plan_id_type"),
                    "plan_id": plan_fields.get("plan_id"),
                    "plan_market_type": plan_fields.get("plan_market_type"),
                    "issuer_name": plan_fields.get("issuer_name"),
                    "plan_sponsor_name": plan_fields.get("plan_sponsor_name"),
                }
            )
            for allowed_amount in out_item.get("allowed_amounts", []):
                tin_info = allowed_amount.get("tin") or {}
                for payment in allowed_amount.get("payments", []):
                    payment_hash = _make_checksum(
                        item_hash,
                        tin_info.get("value") or "",
                        "|".join(_as_list(allowed_amount.get("service_code"))),
                        allowed_amount.get("billing_class") or "",
                        allowed_amount.get("setting") or "",
                        payment.get("allowed_amount"),
                        "|".join(_as_list(payment.get("billing_code_modifier"))),
                    )
                    payment_rows.append(
                        {
                            "payment_hash": payment_hash,
                            "allowed_item_hash": item_hash,
                            "tin_type": tin_info.get("type"),
                            "tin_value": tin_info.get("value"),
                            "service_code": _as_list(allowed_amount.get("service_code")),
                            "billing_class": allowed_amount.get("billing_class"),
                            "setting": allowed_amount.get("setting"),
                            "allowed_amount": payment.get("allowed_amount"),
                            "billing_code_modifier": _as_list(payment.get("billing_code_modifier")),
                        }
                    )
                    for provider in payment.get("providers", []):
                        provider_payment_rows.append(
                            {
                                "provider_payment_hash": _make_checksum(
                                    payment_hash,
                                    provider.get("billed_charge"),
                                    "|".join(str(n) for n in _as_int_list(provider.get("npi"))),
                                ),
                                "payment_hash": payment_hash,
                                "billed_charge": provider.get("billed_charge"),
                                "npi": _as_int_list(provider.get("npi")),
                            }
                        )
            if len(item_rows) >= 100:
                await push_objects(_dedupe_rows_by(item_rows, "allowed_item_hash"), item_cls)
                item_rows = []
            if len(payment_rows) >= 200:
                await push_objects(payment_rows, payment_cls)
                payment_rows = []
            if len(provider_payment_rows) >= 200:
                await push_objects(provider_payment_rows, provider_payment_cls)
                provider_payment_rows = []
            if max_items is not None and count >= max_items:
                break
            if test_mode and count >= TEST_ALLOWED_ITEMS:
                break

    if item_rows:
        await push_objects(_dedupe_rows_by(item_rows, "allowed_item_hash"), item_cls)
    if payment_rows:
        await push_objects(payment_rows, payment_cls)
    if provider_payment_rows:
        await push_objects(provider_payment_rows, provider_payment_cls)
    await flush_error_log(import_log_cls)


async def _process_table_of_contents(
    toc_url: str,
    classes: dict[str, type],
    test_mode: bool,
    plan_ids: list[str] | None = None,
    plan_name_contains: list[str] | None = None,
    plan_market_types: list[str] | None = None,
    import_run_id: str | None = None,
    reuse_raw_artifacts: bool = True,
    max_bytes: int | None = None,
    keep_partial_artifacts: bool | None = None,
    raise_on_error: bool = False,
) -> list[dict[str, Any]]:
    file_cls = classes["PTGFile"]
    import_log_cls = classes["ImportLog"]
    jobs: list[dict[str, Any]] = []
    file_rows: list[dict[str, Any]] = []
    seen_files: set[int] = set()

    with tempfile.TemporaryDirectory(dir=ptg2_temp_parent()) as tmpdir:
        try:
            raw_artifact, logical_artifact = await materialize_json_source(
                toc_url,
                tmpdir,
                reuse_raw_artifacts=reuse_raw_artifacts,
                max_bytes=max_bytes,
                keep_partial_artifacts=keep_partial_artifacts,
            )
        except Exception as exc:
            logger.warning("Failed to download table-of-contents from %s: %s", toc_url, exc)
            if raise_on_error:
                raise RuntimeError(f"Failed to download table-of-contents from {toc_url}: {exc}") from exc
            return []
        toc_content = load_json_artifact(logical_artifact.logical_path)
        if import_run_id:
            await _record_source_version(
                source_type="table-of-contents",
                domain="catalog",
                raw_artifact=raw_artifact,
                logical_artifact=logical_artifact,
                import_run_id=import_run_id,
            )

    if import_run_id:
        catalog_rows = []
        for entry in parse_toc_catalog_entries(
            toc_content,
            toc_url,
            plan_ids=plan_ids,
            plan_name_contains=plan_name_contains,
            plan_market_types=plan_market_types,
        ):
            first_plan = entry.plan_info[0] if len(entry.plan_info) == 1 else {}
            catalog_rows.append(
                {
                    "source_catalog_id": _catalog_entry_id(entry),
                    "import_run_id": import_run_id,
                    "source_type": entry.source_type,
                    "domain": entry.domain,
                    "original_url": entry.original_url,
                    "canonical_url": entry.canonical_url,
                    "from_index_url": entry.from_index_url,
                    "description": entry.description,
                    "reporting_entity_name": entry.reporting_entity_name,
                    "reporting_entity_type": entry.reporting_entity_type,
                    "plan_name": first_plan.get("plan_name"),
                    "plan_id_type": first_plan.get("plan_id_type"),
                    "plan_id": first_plan.get("plan_id"),
                    "plan_market_type": first_plan.get("plan_market_type"),
                    "issuer_name": first_plan.get("issuer_name"),
                    "plan_sponsor_name": first_plan.get("plan_sponsor_name") or first_plan.get("plan_sponser_name"),
                    "payload": _canonicalize_for_json(entry),
                    "created_at": _utcnow(),
                }
            )
        if catalog_rows:
            await _push_ptg2_objects(catalog_rows, PTG2SourceCatalog, rewrite=True)

    toc_meta = {
        "reporting_entity_name": toc_content.get("reporting_entity_name"),
        "reporting_entity_type": toc_content.get("reporting_entity_type"),
        "last_updated_on": toc_content.get("last_updated_on"),
        "version": toc_content.get("version"),
    }
    file_rows.append(
        _build_file_row(
            toc_url,
            "table-of-contents",
            toc_meta,
            None,
            toc_content.get("description"),
            None,
        )
    )

    for structure in toc_content.get("reporting_structure", []):
        plans = _filter_reporting_plans(
            [_normalize_plan_payload(plan) for plan in (structure.get("reporting_plans") or [])],
            plan_ids=plan_ids,
            plan_name_contains=plan_name_contains,
            plan_market_types=plan_market_types,
        )
        if not plans:
            continue
        in_network_files = structure.get("in_network_files") or []
        allowed_amount_file = structure.get("allowed_amount_file")

        for entry in in_network_files:
            location = entry.get("location")
            if not location:
                continue
            location = normalize_tic_source_url(location)
            meta = dict(toc_meta)
            file_row = _build_file_row(location, "in-network", meta, plans, entry.get("description"), toc_url)
            if file_row["file_id"] not in seen_files:
                file_rows.append(file_row)
                seen_files.add(file_row["file_id"])
            jobs.append(
                {
                    "type": "in_network",
                    "url": location,
                    "description": entry.get("description"),
                    "plan_info": plans,
                    "from_index_url": toc_url,
                    "meta": meta,
                }
            )
            if test_mode and len(jobs) >= TEST_TOC_JOBS:
                break

        if allowed_amount_file:
            location = allowed_amount_file.get("location")
            if location:
                location = normalize_tic_source_url(location)
                meta = dict(toc_meta)
                file_row = _build_file_row(
                    location, "allowed-amounts", meta, plans, allowed_amount_file.get("description"), toc_url
                )
                if file_row["file_id"] not in seen_files:
                    file_rows.append(file_row)
                    seen_files.add(file_row["file_id"])
                jobs.append(
                    {
                        "type": "allowed_amounts",
                        "url": location,
                        "description": allowed_amount_file.get("description"),
                        "plan_info": plans,
                        "from_index_url": toc_url,
                        "meta": meta,
                    }
                )
        if test_mode and len(jobs) >= TEST_TOC_JOBS:
            break

    if file_rows:
        await push_objects(file_rows, file_cls, rewrite=True)
    await flush_error_log(import_log_cls)
    return jobs


async def _process_provider_reference_file(
    url: str,
    classes: dict[str, type],
    test_mode: bool,
    reuse_raw_artifacts: bool = True,
    max_bytes: int | None = None,
    import_run_id: str | None = None,
    keep_partial_artifacts: bool | None = None,
) -> dict[int, list[dict[str, Any]]]:
    provider_cls = classes["PTGProviderGroup"]
    file_cls = classes["PTGFile"]
    import_log_cls = classes["ImportLog"]
    provider_map: dict[int, list[dict[str, Any]]] = {}

    with tempfile.TemporaryDirectory(dir=ptg2_temp_parent()) as tmpdir:
        try:
            raw_artifact, logical_artifact = await materialize_json_source(
                url,
                tmpdir,
                reuse_raw_artifacts=reuse_raw_artifacts,
                max_bytes=max_bytes,
                keep_partial_artifacts=keep_partial_artifacts,
            )
        except Exception as exc:
            logger.warning("Failed to download provider-reference from %s: %s", url, exc)
            return provider_map
        provider_content = load_json_artifact(logical_artifact.logical_path)
        await _record_source_version(
            source_type="provider-reference",
            domain="provider_reference",
            raw_artifact=raw_artifact,
            logical_artifact=logical_artifact,
            import_run_id=import_run_id,
        )

    meta = {
        "version": provider_content.get("version"),
    }
    file_row = _build_file_row(url, "provider-reference", meta, None, None, None)
    await _push_ptg2_objects([file_row], file_cls, rewrite=True)

    provider_groups = provider_content.get("provider_groups") or []
    rows: list[dict[str, Any]] = []
    for idx, group in enumerate(provider_groups):
        tin_info = group.get("tin") or {}
        npi_list = group.get("npi") or []
        normalized_npi = _normalized_npi_list(npi_list)
        provider_group_ref = _normalize_provider_ref(
            group.get("provider_group_id") or group.get("provider_group_ref") or (idx + 1)
        )
        provider_hash = _provider_group_identity_hash(tin_info, normalized_npi)
        rows.append(
            {
                "provider_group_hash": provider_hash,
                "provider_group_ref": provider_group_ref,
                "file_id": file_row["file_id"],
                "network_names": group.get("network_name") or group.get("network_names") or [],
                "tin_type": tin_info.get("type"),
                "tin_value": tin_info.get("value"),
                "tin_business_name": tin_info.get("business_name"),
                "npi": normalized_npi,
            }
        )
        provider_map.setdefault(provider_group_ref, []).append(
            {
                **group,
                "network_name": group.get("network_name") or [],
                "__hash__": provider_hash,
                "provider_group_id": provider_group_ref,
            }
        )
        if test_mode and len(rows) >= TEST_PROVIDER_GROUPS:
            break

    if rows:
        await push_objects(_dedupe_rows_by(rows, "provider_group_hash"), provider_cls)
    await flush_error_log(import_log_cls)
    return provider_map


async def _process_in_network_file(
    job: dict[str, Any],
    classes: dict[str, type],
    provider_ref_cache: dict[int, list[dict[str, Any]]],
    test_mode: bool,
    reuse_raw_artifacts: bool = True,
    max_bytes: int | None = None,
    max_items: int | None = None,
    import_run_id: str | None = None,
    keep_partial_artifacts: bool | None = None,
    compact_import: bool = False,
    snapshot_id: str | None = None,
    import_month: datetime.date | None = None,
    rust_stage_tables: dict[str, str] | None = None,
    raw_artifact: PTG2RawArtifact | None = None,
    logical_artifact: PTG2LogicalArtifact | None = None,
) -> PTG2FileProcessResult:
    url = job["url"]
    description = job.get("description")
    plan_info = job.get("plan_info")
    from_index_url = job.get("from_index_url")
    provided_meta = job.get("meta") or {}

    file_cls = classes["PTGFile"]
    import_log_cls = classes["ImportLog"]

    with tempfile.TemporaryDirectory(dir=ptg2_temp_parent()) as tmpdir:
        if raw_artifact is None or logical_artifact is None:
            try:
                raw_artifact, logical_artifact = await materialize_json_source(
                    url,
                    tmpdir,
                    reuse_raw_artifacts=reuse_raw_artifacts,
                    max_bytes=max_bytes,
                    materialize_logical=False,
                    keep_partial_artifacts=keep_partial_artifacts,
                )
            except Exception as exc:
                logger.warning("Failed to download in-network file from %s: %s", url, exc)
                return PTG2FileProcessResult("in_network", url, False, error=str(exc))
        extracted = logical_artifact.logical_path
        meta = provided_meta or await _extract_metadata_fields(extracted)
        file_row = _build_file_row(url, "in-network", meta, plan_info, description, from_index_url)
        await _push_ptg2_objects([file_row], file_cls, rewrite=True)
        source_version = await _record_source_version(
            source_type="in-network",
            domain=PTG2_DOMAIN_IN_NETWORK,
            raw_artifact=raw_artifact,
            logical_artifact=logical_artifact,
            import_run_id=import_run_id,
        )
        provider_cache_backend = os.getenv(PTG2_PROVIDER_CACHE_BACKEND_ENV, "sqlite").strip().lower()
        if provider_cache_backend in {"memory", "in-memory", "ram"}:
            provider_cache = PTG2InMemoryProviderReferenceCache(provider_ref_cache)
        else:
            provider_cache = PTG2ProviderReferenceCache(Path(tmpdir) / "provider_refs.sqlite", provider_ref_cache)
        parse_summary: dict[str, Any] | None = None
        try:
            if compact_import and _use_serving_only_import():
                parse_summary = await _parse_in_network_file_serving_only(
                    extracted,
                    file_row["file_id"],
                    meta,
                    plan_info,
                    provider_cache,
                    test_mode,
                    import_log_cls,
                    url,
                    source_version,
                    snapshot_id or "ptg2:unknown",
                    import_month or normalize_import_month(None),
                    max_items=max_items,
                    rust_stage_tables=rust_stage_tables,
                )
            elif compact_import:
                parse_summary = await _parse_in_network_file_compact(
                    extracted,
                    file_row["file_id"],
                    meta,
                    plan_info,
                    provider_cache,
                    test_mode,
                    import_log_cls,
                    url,
                    source_version,
                    snapshot_id or "ptg2:unknown",
                    import_month or normalize_import_month(None),
                    max_items=max_items,
                )
            else:
                parse_summary = await _parse_in_network_file_single_pass(
                    extracted, file_row["file_id"], meta, plan_info, provider_cache, classes, test_mode, import_log_cls, url,
                    max_items=max_items,
                )
        finally:
            provider_cache.close()
    if (
        compact_import
        and _use_serving_only_import()
        and not test_mode
        and int((parse_summary or {}).get("serving_rates") or 0) <= 0
    ):
        no_data_summary = dict(parse_summary or {})
        no_data_summary["skipped_reason"] = "parsed zero serving rates"
        return PTG2FileProcessResult(
            "in_network",
            url,
            True,
            file_id=file_row["file_id"],
            summary=no_data_summary,
            skipped=True,
        )
    return PTG2FileProcessResult("in_network", url, True, file_id=file_row["file_id"], summary=parse_summary)


async def _process_allowed_amounts_file(
    job: dict[str, Any],
    classes: dict[str, type],
    test_mode: bool,
    reuse_raw_artifacts: bool = True,
    max_bytes: int | None = None,
    max_items: int | None = None,
    import_run_id: str | None = None,
    keep_partial_artifacts: bool | None = None,
    raw_artifact: PTG2RawArtifact | None = None,
    logical_artifact: PTG2LogicalArtifact | None = None,
) -> PTG2FileProcessResult:
    url = job["url"]
    description = job.get("description")
    plan_info = job.get("plan_info")
    from_index_url = job.get("from_index_url")
    provided_meta = job.get("meta") or {}

    file_cls = classes["PTGFile"]
    import_log_cls = classes["ImportLog"]

    with tempfile.TemporaryDirectory(dir=ptg2_temp_parent()) as tmpdir:
        if raw_artifact is None or logical_artifact is None:
            try:
                raw_artifact, logical_artifact = await materialize_json_source(
                    url,
                    tmpdir,
                    reuse_raw_artifacts=reuse_raw_artifacts,
                    max_bytes=max_bytes,
                    materialize_logical=False,
                    keep_partial_artifacts=keep_partial_artifacts,
                )
            except Exception as exc:
                logger.warning("Failed to download allowed-amounts file from %s: %s", url, exc)
                return PTG2FileProcessResult("allowed_amounts", url, False, error=str(exc))
        extracted = logical_artifact.logical_path
        meta = provided_meta or await _extract_metadata_fields(extracted)
        file_row = _build_file_row(url, "allowed-amounts", meta, plan_info, description, from_index_url)
        await _push_ptg2_objects([file_row], file_cls, rewrite=True)
        await _record_source_version(
            source_type="allowed-amounts",
            domain=PTG2_DOMAIN_ALLOWED_AMOUNT,
            raw_artifact=raw_artifact,
            logical_artifact=logical_artifact,
            import_run_id=import_run_id,
        )
        await _parse_allowed_amounts(
            extracted, file_row["file_id"], meta, plan_info, classes, test_mode, import_log_cls, url,
            max_items=max_items,
        )
    return PTG2FileProcessResult("allowed_amounts", url, True, file_id=file_row["file_id"])


async def _mark_ptg2_import_failed(
    import_run_id: str,
    snapshot_id: str,
    import_month: datetime.date,
    started_at: datetime.datetime,
    error: BaseException | str,
    report: dict[str, Any] | None = None,
    options: dict[str, Any] | None = None,
) -> None:
    finished = _utcnow()
    error_text = str(error)
    report_payload = dict(report or {})
    report_payload.setdefault("snapshot_id", snapshot_id)
    try:
        await _push_ptg2_objects(
            [
                {
                    "snapshot_id": snapshot_id,
                    "import_run_id": import_run_id,
                    "import_month": import_month,
                    "status": PTG2_STATUS_FAILED,
                    "created_at": started_at,
                    "validated_at": None,
                    "published_at": None,
                    "previous_snapshot_id": None,
                    "manifest": {**report_payload, "error": error_text},
                }
            ],
            PTG2Snapshot,
            rewrite=True,
        )
        await _push_ptg2_objects(
            [
                {
                    "import_run_id": import_run_id,
                    "import_month": import_month,
                    "status": PTG2_STATUS_FAILED,
                    "started_at": started_at,
                    "finished_at": finished,
                    "heartbeat_at": finished,
                    "options": dict(options or {}),
                    "report": report_payload,
                    "error": error_text,
                }
            ],
            PTG2ImportRun,
            rewrite=True,
        )
    except Exception as mark_exc:
        logger.error("Failed to mark PTG2 import %s as failed: %s", import_run_id, mark_exc)


async def main(
    test_mode: bool = False,
    toc_urls: list[str] | None = None,
    toc_list: str | None = None,
    in_network_url: str | None = None,
    allowed_url: str | None = None,
    provider_ref_url: str | None = None,
    import_id: str | None = None,
    source_key: str | None = None,
    import_month: str | datetime.date | None = None,
    max_files: int | None = None,
    max_items: int | None = None,
    plan_ids: list[str] | None = None,
    plan_name_contains: list[str] | None = None,
    plan_market_types: list[str] | None = None,
    file_url_contains: list[str] | None = None,
    reuse_raw_artifacts: bool = True,
    keep_partial_artifacts: bool | None = None,
) -> None:
    """
    PTG2 entry point for the Transparency in Coverage importer.
    """
    import_started_monotonic = time.monotonic()
    import_month_value = normalize_import_month(import_month)
    import_id_val = _normalize_import_id(import_id or import_month_value.strftime("%Y%m%d"))
    source_key_val = _normalize_source_key(source_key or os.getenv("HLTHPRT_PTG2_SOURCE_KEY"))
    import_run_id = f"ptg2:{import_id_val}"
    snapshot_id = f"ptg2:{import_month_value.strftime('%Y%m')}:{hash_prefix(semantic_hash(import_run_id), 12)}"
    max_bytes = None
    if test_mode:
        raw_max_bytes = os.getenv("HLTHPRT_PTG2_TEST_MAX_BYTES")
        if raw_max_bytes:
            try:
                max_bytes = int(raw_max_bytes)
            except ValueError:
                logger.warning("Ignoring invalid HLTHPRT_PTG2_TEST_MAX_BYTES=%s", raw_max_bytes)
    await ensure_database(test_mode)
    await ensure_ptg2_tables()
    compact_import = _env_bool(PTG2_COMPACT_IMPORT_ENV, not test_mode)
    source_scoped_compact = (
        compact_import
        and not test_mode
        and _use_compact_serving_table()
        and _use_rust_compact_serving()
    )
    if source_scoped_compact and source_key_val is None:
        if test_mode:
            source_key_val = _normalize_source_key(import_id_val)
        else:
            raise ValueError("PTG2 Rust compact serving imports require --source-key or HLTHPRT_PTG2_SOURCE_KEY")
    if compact_import and _use_compact_serving_table() and not source_scoped_compact:
        await prepare_ptg2_compact_bulk_load()
    now = _utcnow()
    options_payload = {
        "toc_urls": toc_urls or [],
        "toc_list": toc_list,
        "in_network_url": in_network_url,
        "allowed_url": allowed_url,
        "provider_ref_url": provider_ref_url,
        "source_key": source_key_val,
        "plan_ids": plan_ids or [],
        "plan_name_contains": plan_name_contains or [],
        "plan_market_types": plan_market_types or [],
        "file_url_contains": file_url_contains or [],
        "max_files": max_files,
        "max_items": max_items,
        "reuse_raw_artifacts": reuse_raw_artifacts,
        "keep_partial_artifacts": _env_bool(PTG2_KEEP_PARTIAL_ENV, True)
        if keep_partial_artifacts is None else keep_partial_artifacts,
        "compact_import": compact_import,
        "async_write_tasks": max(_env_int(PTG2_ASYNC_WRITE_TASKS_ENV, 1), 1),
        "fast_provider_union": _env_bool(PTG2_FAST_PROVIDER_UNION_ENV, False),
        "serving_only_import": _use_serving_only_import(),
        "source_scoped_compact": source_scoped_compact,
        "stage_serving_as_final": _use_stage_serving_as_final(),
        "test_mode": test_mode,
    }
    await _push_ptg2_objects(
        [
            {
                "import_run_id": import_run_id,
                "import_month": import_month_value,
                "status": PTG2_STATUS_RUNNING,
                "started_at": now,
                "finished_at": None,
                "heartbeat_at": now,
                "options": options_payload,
                "report": {},
                "error": None,
            }
        ],
        PTG2ImportRun,
        rewrite=True,
    )
    await _push_ptg2_objects(
        [
            {
                "snapshot_id": snapshot_id,
                "import_run_id": import_run_id,
                "import_month": import_month_value,
                "status": PTG2_STATUS_BUILDING,
                "created_at": now,
                "validated_at": None,
                "published_at": None,
                "previous_snapshot_id": None,
                "manifest": {},
            }
        ],
        PTG2Snapshot,
        rewrite=True,
    )
    failure_report: dict[str, Any] = {"snapshot_id": snapshot_id, "legacy_table_suffix": import_id_val}
    rust_snapshot_stage_tables: dict[str, str] = {}
    current_pointer_published = False
    try:
        if compact_import:
            schema = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
            await db.status(f"DELETE FROM {schema}.ptg2_serving_rate WHERE snapshot_id = :snapshot_id", snapshot_id=snapshot_id)
        if source_scoped_compact:
            assert source_key_val is not None
            rust_snapshot_stage_tables = await _create_rust_copy_stage_tables(
                _ptg2_snapshot_table_token(source_key_val, snapshot_id),
                serving_lanes=max(_env_int(PTG2_RUST_WORKERS_ENV, PTG2_DEFAULT_RUST_WORKERS), 1),
            )
        classes = await _prepare_ptg_tables(import_id_val, test_mode)

        provider_ref_cache: dict[int, list[dict[str, Any]]] = {}
        jobs: list[dict[str, Any]] = []
        data_started_monotonic = time.monotonic()

        toc_candidates: list[str] = []
        if toc_urls:
            toc_candidates.extend([u for u in toc_urls if u])
        if toc_list:
            toc_candidates.extend(_load_toc_urls_from_file(toc_list))
        toc_candidates = _dedupe_preserve([u.strip() for u in toc_candidates if u.strip()])

        toc_failures: list[dict[str, Any]] = []
        for idx, toc_url in enumerate(toc_candidates):
            if test_mode and idx >= TEST_TOC_FILES:
                break
            try:
                toc_jobs = await _process_table_of_contents(
                    toc_url,
                    classes,
                    test_mode,
                    plan_ids=plan_ids,
                    plan_name_contains=plan_name_contains,
                    plan_market_types=plan_market_types,
                    import_run_id=import_run_id,
                    reuse_raw_artifacts=reuse_raw_artifacts,
                    max_bytes=max_bytes,
                    keep_partial_artifacts=keep_partial_artifacts,
                    raise_on_error=True,
                )
            except Exception as exc:
                toc_failures.append({"url": toc_url, "error": str(exc)})
                continue
            jobs.extend(toc_jobs)

        if provider_ref_url:
            provider_ref_cache.update(
                await _process_provider_reference_file(
                    provider_ref_url,
                    classes,
                    test_mode,
                    reuse_raw_artifacts=reuse_raw_artifacts,
                    max_bytes=max_bytes,
                    import_run_id=import_run_id,
                    keep_partial_artifacts=keep_partial_artifacts,
                )
            )

        if in_network_url:
            jobs.append({"type": "in_network", "url": in_network_url})
        if allowed_url:
            jobs.append({"type": "allowed_amounts", "url": allowed_url})
        jobs = _filter_jobs_by_url_contains(jobs, file_url_contains)
        jobs_discovered_before_dedupe = len(jobs)
        jobs, duplicate_jobs_skipped = _dedupe_ptg_jobs(jobs)
        if duplicate_jobs_skipped:
            _emit_screen_line(
                "PTG2_JOB_DEDUPE"
                f"\traw_jobs={jobs_discovered_before_dedupe}"
                f"\tunique_jobs={len(jobs)}"
                f"\tduplicates_skipped={duplicate_jobs_skipped}"
            )
        if toc_failures and not _env_bool("HLTHPRT_PTG2_ALLOW_PARTIAL_IMPORT", False):
            failure_report = {
                "toc_urls": toc_candidates,
                "toc_failures": toc_failures,
                "jobs_discovered": jobs_discovered_before_dedupe,
                "jobs_unique": len(jobs),
                "duplicate_jobs_skipped": duplicate_jobs_skipped,
                "files_attempted": 0,
                "files_processed": 0,
                "files_failed": 0,
                "snapshot_id": snapshot_id,
                "legacy_table_suffix": import_id_val,
            }
            raise RuntimeError(
                f"PTG2 import failed {len(toc_failures)} table-of-contents file(s); "
                "set HLTHPRT_PTG2_ALLOW_PARTIAL_IMPORT=true to continue with other discovered jobs"
            )
        if toc_candidates and not jobs and not in_network_url and not allowed_url:
            failure_report = {
                "toc_urls": toc_candidates,
                "toc_failures": toc_failures,
                "jobs_discovered": 0,
                "files_attempted": 0,
                "files_processed": 0,
                "files_failed": 0,
                "snapshot_id": snapshot_id,
                "legacy_table_suffix": import_id_val,
            }
            raise RuntimeError("PTG2 import processed table-of-contents input but discovered zero rate files")

        seen_jobs: set[tuple[str, str]] = set()
        selected_jobs: list[dict[str, Any]] = []
        for job in jobs:
            job_key = _ptg_job_identity(job)
            if job_key in seen_jobs:
                continue
            seen_jobs.add(job_key)
            if max_files is not None and len(selected_jobs) >= max_files:
                break
            if job.get("type") in {"in_network", "allowed_amounts"}:
                selected_jobs.append(job)
        processed_files = 0
        attempted_files = len(selected_jobs)
        failed_files: list[dict[str, Any]] = []
        skipped_files: list[dict[str, Any]] = []
        successful_files: list[dict[str, Any]] = []
        seen_raw_artifact_hashes: set[str] = set()
        duplicate_raw_files_skipped = 0
        async for downloaded in _iter_downloaded_ptg_jobs(
            selected_jobs,
            reuse_raw_artifacts=reuse_raw_artifacts,
            max_bytes=max_bytes,
            keep_partial_artifacts=keep_partial_artifacts,
        ):
            job = downloaded.job
            result: PTG2FileProcessResult | None = None
            if downloaded.error:
                logger.warning("Failed to download %s file from %s: %s", job.get("type"), job.get("url"), downloaded.error)
                result = PTG2FileProcessResult(
                    str(job.get("type") or "unknown"),
                    str(job.get("url") or ""),
                    False,
                    error=downloaded.error,
                )
            elif downloaded.raw_artifact is None or downloaded.logical_artifact is None:
                result = PTG2FileProcessResult(
                    str(job.get("type") or "unknown"),
                    str(job.get("url") or ""),
                    False,
                    error="download did not produce an artifact",
                )
            elif downloaded.raw_artifact.raw_sha256 in seen_raw_artifact_hashes:
                duplicate_raw_files_skipped += 1
                _emit_screen_line(
                    "PTG2_RAW_JOB_DEDUPE"
                    f"\ttype={job.get('type')}"
                    f"\turl={job.get('url')}"
                    f"\traw_sha256={downloaded.raw_artifact.raw_sha256}"
                    "\treason=duplicate_raw_artifact"
                )
                result = PTG2FileProcessResult(
                    str(job.get("type") or "unknown"),
                    str(job.get("url") or ""),
                    True,
                    summary={
                        "raw_sha256": downloaded.raw_artifact.raw_sha256,
                        "raw_storage_uri": downloaded.raw_artifact.raw_storage_uri,
                        "reason": "duplicate_raw_artifact",
                    },
                    skipped=True,
                )
            elif downloaded.raw_artifact.raw_sha256:
                seen_raw_artifact_hashes.add(downloaded.raw_artifact.raw_sha256)
            if job.get("type") == "in_network":
                if result is None:
                    result = await _process_in_network_file(
                        job,
                        classes,
                        provider_ref_cache,
                        test_mode,
                        reuse_raw_artifacts=reuse_raw_artifacts,
                        max_bytes=max_bytes,
                        max_items=max_items,
                        import_run_id=import_run_id,
                        keep_partial_artifacts=keep_partial_artifacts,
                        compact_import=compact_import,
                        snapshot_id=snapshot_id,
                        import_month=import_month_value,
                        rust_stage_tables=rust_snapshot_stage_tables if source_scoped_compact else None,
                        raw_artifact=downloaded.raw_artifact,
                        logical_artifact=downloaded.logical_artifact,
                    )
            elif job.get("type") == "allowed_amounts":
                if result is None:
                    result = await _process_allowed_amounts_file(
                        job,
                        classes,
                        test_mode,
                        reuse_raw_artifacts=reuse_raw_artifacts,
                        max_bytes=max_bytes,
                        max_items=max_items,
                        import_run_id=import_run_id,
                        keep_partial_artifacts=keep_partial_artifacts,
                        raw_artifact=downloaded.raw_artifact,
                        logical_artifact=downloaded.logical_artifact,
                    )
            if result is None:
                continue
            if result.success:
                if result.skipped:
                    skipped_files.append(asdict(result))
                else:
                    processed_files += 1
                    successful_files.append(asdict(result))
            else:
                failed_files.append(asdict(result))

        failure_report = {
            "jobs_discovered": jobs_discovered_before_dedupe,
            "jobs_unique": len(jobs),
            "duplicate_jobs_skipped": duplicate_jobs_skipped,
            "duplicate_raw_files_skipped": duplicate_raw_files_skipped,
            "files_attempted": attempted_files,
            "files_processed": processed_files,
            "files_failed": len(failed_files),
            "files_skipped": len(skipped_files),
            "successful_files": successful_files,
            "skipped_files": skipped_files,
            "failed_files": failed_files,
            "toc_failures": toc_failures,
            "snapshot_id": snapshot_id,
            "legacy_table_suffix": import_id_val,
        }
        if jobs and processed_files == 0:
            raise RuntimeError(
                f"PTG2 import discovered {len(jobs)} job(s) but processed zero files successfully"
            )
        if failed_files and not _env_bool("HLTHPRT_PTG2_ALLOW_PARTIAL_IMPORT", False):
            raise RuntimeError(
                f"PTG2 import failed {len(failed_files)} of {attempted_files} attempted file(s); "
                "set HLTHPRT_PTG2_ALLOW_PARTIAL_IMPORT=true to publish a partial snapshot"
            )

        await flush_error_log(classes["ImportLog"])
        data_seconds = time.monotonic() - data_started_monotonic
        publish_started_monotonic = time.monotonic()
        if source_scoped_compact and rust_snapshot_stage_tables:
            assert source_key_val is not None
            serving_index = await _publish_rust_compact_snapshot_tables(
                rust_snapshot_stage_tables,
                snapshot_id=snapshot_id,
                import_run_id=import_run_id,
                source_key=source_key_val,
            )
            rust_snapshot_stage_tables = {}
        elif compact_import and _use_compact_serving_table():
            serving_index = await build_ptg2_compact_serving_index(snapshot_id, import_run_id)
        elif compact_import and _use_stage_serving_as_final():
            serving_index = await build_ptg2_stage_serving_index(snapshot_id, import_run_id)
        else:
            serving_index = (
                await finalize_ptg2_incremental_serving_index(snapshot_id)
                if compact_import
                else await build_ptg2_snapshot_index_artifact(classes, snapshot_id, import_run_id)
            )
        if compact_import and serving_index is None:
            serving_index = await build_ptg2_db_serving_index(snapshot_id, import_run_id)
        publish_seconds = time.monotonic() - publish_started_monotonic
        finished = _utcnow()
        previous_snapshot_id = None
        global_previous_snapshot_id = None
        try:
            row = await (
                db.select(PTG2CurrentSnapshot.__table__.c.snapshot_id)
                .where(PTG2CurrentSnapshot.__table__.c.slot == "current")
                .first()
            )
            if row is not None:
                global_previous_snapshot_id = row[0]
        except Exception as exc:
            logger.debug("No PTG2 current snapshot found before publish: %s", exc)
        if source_scoped_compact and source_key_val:
            previous_snapshot_id = await _current_source_snapshot_id(source_key_val)
        else:
            previous_snapshot_id = global_previous_snapshot_id
        serving_timings = serving_index.get("timings", {}) if isinstance(serving_index, dict) else {}
        timing_payload = {
            "total_seconds": time.monotonic() - import_started_monotonic,
            "data_seconds": data_seconds,
            "publish_seconds": publish_seconds,
        }
        if isinstance(serving_timings, dict):
            for key, value in serving_timings.items():
                try:
                    timing_payload[key] = float(value)
                except (TypeError, ValueError):
                    continue
        report_payload = {**failure_report, "serving_index": serving_index, "timings": timing_payload}
        if isinstance(serving_index, dict):
            authoritative_rate_count = serving_index.get("serving_rates", serving_index.get("rate_count"))
            if authoritative_rate_count is not None:
                report_payload["serving_rates"] = int(authoritative_rate_count)
                report_payload["rate_count"] = int(authoritative_rate_count)
        await _push_ptg2_objects(
            [
                {
                    "snapshot_id": snapshot_id,
                    "import_run_id": import_run_id,
                    "import_month": import_month_value,
                    "status": PTG2_STATUS_PUBLISHED,
                    "created_at": now,
                    "validated_at": finished,
                    "published_at": finished,
                    "previous_snapshot_id": previous_snapshot_id,
                    "manifest": report_payload,
                }
            ],
            PTG2Snapshot,
            rewrite=True,
        )
        if source_scoped_compact and source_key_val:
            await _publish_ptg2_source_pointers(
                source_key=source_key_val,
                snapshot_id=snapshot_id,
                previous_snapshot_id=previous_snapshot_id,
                import_month=import_month_value,
                updated_at=finished,
                serving_index=serving_index,
            )
        await _push_ptg2_objects(
            [
                {
                    "slot": "current",
                    "snapshot_id": snapshot_id,
                    "previous_snapshot_id": global_previous_snapshot_id,
                    "updated_at": finished,
                }
            ],
            PTG2CurrentSnapshot,
            rewrite=True,
        )
        current_pointer_published = True
        if source_scoped_compact and source_key_val:
            keep_snapshot_ids = {snapshot_id}
            if previous_snapshot_id:
                keep_snapshot_ids.add(previous_snapshot_id)
            await _cleanup_old_ptg2_source_tables(source_key_val, keep_snapshot_ids)
        await _push_ptg2_objects(
            [
                {
                    "import_run_id": import_run_id,
                    "import_month": import_month_value,
                    "status": PTG2_STATUS_VALIDATED,
                    "started_at": now,
                    "finished_at": finished,
                    "heartbeat_at": finished,
                    "options": options_payload,
                    "report": report_payload,
                    "error": None,
                }
            ],
            PTG2ImportRun,
            rewrite=True,
        )
        done_line = (
            "PTG2_IMPORT_DONE"
            f"\timport_run_id={import_run_id}"
            f"\tsnapshot_id={snapshot_id}"
            f"\tstatus={PTG2_STATUS_VALIDATED}"
            f"\tfiles_processed={processed_files}"
            f"\tfiles_failed={len(failed_files)}"
            f"\tserving_rates={report_payload.get('serving_rates', 'unknown')}"
            f"\ttotal_seconds={timing_payload['total_seconds']:.2f}"
            f"\tdata_seconds={timing_payload['data_seconds']:.2f}"
            f"\tpublish_seconds={timing_payload['publish_seconds']:.2f}"
            f"\tindex_seconds={float(timing_payload.get('index_seconds', 0.0)):.2f}"
            f"\tanalyze_seconds={float(timing_payload.get('analyze_seconds', 0.0)):.2f}"
        )
        _emit_screen_line(done_line)
        logger.info(done_line)
    except Exception as exc:
        if source_scoped_compact and not current_pointer_published:
            serving_index = failure_report.get("serving_index")
            try:
                await _drop_ptg2_snapshot_tables_for_manifest(serving_index if isinstance(serving_index, dict) else None)
                if source_key_val:
                    await _drop_ptg2_snapshot_table_names(
                        [
                            _ptg2_snapshot_table_name(kind, source_key_val, snapshot_id)
                            for kind in (
                                "serving_rate_compact",
                                "procedure",
                                "price_set",
                                "provider_set",
                                "provider_set_component",
                                "provider_set_entry",
                                "provider_entry_component",
                                "provider_group_member",
                            )
                        ]
                    )
                if rust_snapshot_stage_tables:
                    await _drop_ptg2_snapshot_table_names(list(rust_snapshot_stage_tables.values()))
            except Exception:
                logger.debug("Failed to clean PTG2 source-scoped tables for failed import", exc_info=True)
        await _mark_ptg2_import_failed(
            import_run_id,
            snapshot_id,
            import_month_value,
            now,
            exc,
            report=failure_report,
            options=options_payload,
        )
        raise


__all__ = [
    "PTG2ArtifactStore",
    "PTG2ContentIdentityValue",
    "PTG2ContractEvent",
    "PTG2FileProcessResult",
    "PTG2HeadMetadata",
    "PTG2LogicalArtifact",
    "PTG2PriceAtomEvent",
    "PTG2PriceSetValue",
    "PTG2ProcedureEvent",
    "PTG2ProviderGroupEvent",
    "PTG2InMemoryProviderReferenceCache",
    "PTG2ProviderReferenceCache",
    "PTG2ProviderSetValue",
    "PTG2RawArtifact",
    "PTG2RatePackValue",
    "PTG2SourceCatalogEntry",
    "PTG2SourceTraceSetValue",
    "PTG2SourceVersion",
    "build_fact_chunk",
    "build_price_atom",
    "build_price_set",
    "build_procedure_collection",
    "build_provider_set",
    "build_provider_set_collection",
    "build_ptg2_db_serving_index",
    "build_ptg2_compact_snapshot_index_artifact",
    "build_ptg2_compact_serving_index",
    "build_ptg2_stage_serving_index",
    "build_rate_pack",
    "build_rate_pack_group",
    "build_rate_pack_procedure_group",
    "build_rate_set",
    "build_ptg2_snapshot_index_artifact",
    "build_source_trace_set",
    "canonical_json_dumps",
    "canonicalize_url",
    "choose_reusable_raw_artifact",
    "content_addressed_path",
    "download_raw_artifact",
    "ensure_ptg2_tables",
    "fetch_head_metadata",
    "finalize_ptg2_incremental_serving_index",
    "hash_prefix",
    "logical_artifact_identity",
    "main",
    "materialize_json_source",
    "normalize_date",
    "normalize_import_month",
    "normalize_money",
    "normalize_ptg2_search_mode",
    "parse_toc_catalog_entries",
    "provider_hash_bucket",
    "ptg2_provider_bucket_count",
    "ptg2_confidence_statement",
    "open_json_artifact_stream",
    "semantic_hash",
    "sha256_bytes",
    "sha256_file",
    "stream_logical_artifact",
]
