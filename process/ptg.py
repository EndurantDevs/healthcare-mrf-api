# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import concurrent.futures
import datetime
import hashlib
import json
import logging
import multiprocessing
import os
import re
import subprocess
import tempfile
import time
from collections import OrderedDict
from dataclasses import asdict
from pathlib import Path
from typing import Any

import ijson

from db.connection import db
from db.models import (ImportLog, PTG2CurrentPlanSource, PTG2CurrentSnapshot,
                       PTG2CurrentSourceSnapshot, PTG2FactChunk,
                       PTG2GCCandidate, PTG2ImportJob, PTG2ImportRun,
                       PTG2LocationSet, PTG2LocationSetMember, PTG2Plan,
                       PTG2PlanAlias, PTG2PlanMonth, PTG2PlanRateSet,
                       PTG2PriceCodeSet, PTG2PriceSet, PTG2PriceSetEntry,
                       PTG2ProviderEntryComponent, PTG2ProviderSetEntry,
                       PTG2ProviderSetMember, PTG2RateSet, PTG2RateSetContext,
                       PTG2RelatedCodeSet, PTG2ServingRate,
                       PTG2ServingRateCompact, PTG2Snapshot, PTG2SourceCatalog,
                       PTG2SourceTrace, PTG2SourceTraceSet, PTGAllowedItem,
                       PTGAllowedPayment, PTGAllowedProviderPayment,
                       PTGBillingCode, PTGFile, PTGInNetworkItem,
                       PTGNegotiatedPrice, PTGNegotiatedRate, PTGProviderGroup)
from process.ext.utils import (ensure_database, flush_error_log,
                               get_import_schema, log_error, make_class,
                               push_objects, return_checksum)
from process.ptg_parts.allowed_amounts import (_parse_allowed_amounts,
                                               _process_allowed_amounts_file)
from process.ptg_parts.artifact_streams import (load_json_artifact,
                                                logical_artifact_identity,
                                                open_json_artifact_stream,
                                                stream_logical_artifact)
from process.ptg_parts.artifacts import (PTG2ArtifactStore,
                                         _hash_existing_file_into,
                                         _load_completed_ranges,
                                         _range_sidecar_path, _safe_url_suffix,
                                         _write_completed_ranges,
                                         choose_reusable_raw_artifact,
                                         content_addressed_path,
                                         ptg2_temp_parent,
                                         resolve_ptg2_artifact_dir,
                                         sha256_file)
from process.ptg_parts.canonical import (_canonical_key, _canonical_sort_key,
                                         _canonicalize_for_json,
                                         canonical_json_dumps,
                                         canonicalize_url, hash_prefix,
                                         normalize_date,
                                         normalize_import_month,
                                         normalize_money,
                                         normalize_tic_source_url,
                                         semantic_hash, sha256_bytes)
from process.ptg_parts.compact_bulk import (_flush_in_network_rows,
                                            prepare_ptg2_compact_bulk_load)
from process.ptg_parts.compact_indexes import (
    _PTG2_COMPACT_MODEL_BY_KIND, _index_snapshot_compact_table_entries,
    _index_snapshot_compact_tables, _ptg2_compact_dictionary_index_mode,
    _ptg2_compact_serving_index_mode,
    _ptg2_compact_serving_reported_index_statement, _ptg2_index_timestamp,
    _ptg2_model_index_statements_for_table, _ptg2_model_snapshot_index_role,
    _run_ptg2_index_statement)
from process.ptg_parts.compact_state import (_compact_add_unique,
                                             _compact_state,
                                             _compact_streaming_dedupe_tables,
                                             _ptg2_price_atom_payload,
                                             _ptg2_source_trace_payload)
from process.ptg_parts.compact_writes import (_drain_compact_writes,
                                              _existing_price_set_hashes,
                                              _flush_compact_rate_pack_groups,
                                              _flush_compact_rows,
                                              _schedule_compact_write)
from process.ptg_parts.config import (
    PTG2_ASYNC_WRITE_TASKS_ENV, PTG2_BILLING_BATCH_ROWS_ENV,
    PTG2_COMPACT_BATCH_ROWS_ENV, PTG2_COMPACT_BULK_DROP_INDEXES_ENV,
    PTG2_COMPACT_COPY_KIND_TASKS_ENV, PTG2_COMPACT_COPY_TASKS_ENV,
    PTG2_COMPACT_IMPORT_ENV, PTG2_COMPACT_SERVING_COPY_TASKS_ENV,
    PTG2_COMPACT_SERVING_TABLE_ENV, PTG2_COPY_UPSERT_ROWS_ENV,
    PTG2_DEDUPE_SERVING_STAGE_MERGE_ENV, PTG2_DEFAULT_COMPACT_COPY_KIND_TASKS,
    PTG2_DEFAULT_COMPACT_COPY_TASKS, PTG2_DEFAULT_COMPACT_SERVING_COPY_TASKS,
    PTG2_DEFAULT_DOWNLOAD_TASKS, PTG2_DEFAULT_RANGE_DOWNLOAD_CHUNK_BYTES,
    PTG2_DEFAULT_RANGE_DOWNLOAD_MIN_BYTES, PTG2_DEFAULT_RANGE_DOWNLOAD_TASKS,
    PTG2_DEFAULT_RUST_WORKERS, PTG2_DEFER_PROVIDER_LOCATIONS_ENV,
    PTG2_DIRECT_COPY_SERVING_RATE_ENV, PTG2_DOWNLOAD_PROGRESS_BYTES_ENV,
    PTG2_DOWNLOAD_RETRIES_ENV, PTG2_DOWNLOAD_RETRY_DELAY_SECONDS_ENV,
    PTG2_DOWNLOAD_TASKS_ENV, PTG2_EXPECTED_IN_NETWORK_ITEMS_ENV,
    PTG2_FAST_FINAL_REBUILD_ENV, PTG2_FAST_OBJECT_ITERATOR_ENV,
    PTG2_FAST_PROVIDER_AGGREGATION_ENV, PTG2_FAST_PROVIDER_UNION_ENV,
    PTG2_FILE_PROCESS_CONCURRENCY_ENV, PTG2_HASH_MODE_ENV,
    PTG2_ITEM_BATCH_ROWS_ENV, PTG2_JSON_DECODER_ITERATOR_ENV,
    PTG2_KEEP_PARTIAL_ENV, PTG2_KEEP_PRICE_SET_STAGE_ENV,
    PTG2_KEEP_SERVING_RATE_STAGE_ENV, PTG2_MANIFEST_PRECOPY_MERGE_ENV,
    PTG2_MANIFEST_PROVIDER_NPI_SIDECAR_ENABLED_ENV,
    PTG2_PRICE_BATCH_ROWS_ENV,
    PTG2_PROGRESS_INTERVAL_SECONDS_ENV, PTG2_PROVIDER_BUCKET_COUNT_ENV,
    PTG2_PROVIDER_CACHE_BACKEND_ENV, PTG2_PROVIDER_CACHE_MEMORY_REFS_ENV,
    PTG2_PROVIDER_COMBO_CACHE_REFS_ENV, PTG2_PROVIDER_REF_BATCH_ROWS_ENV,
    PTG2_PROVIDER_SET_INLINE_NPI_LIMIT_ENV,
    PTG2_RANGE_DOWNLOAD_CHUNK_BYTES_ENV, PTG2_RANGE_DOWNLOAD_MIN_BYTES_ENV,
    PTG2_RANGE_DOWNLOAD_TASKS_ENV, PTG2_RANGE_DOWNLOADS_ENV,
    PTG2_RATE_BATCH_ROWS_ENV, PTG2_RATE_GROUP_FLUSH_ITEMS_ENV,
    PTG2_RAW_WORKER_OBJECTS_ENV, PTG2_RUST_COMPACT_SERVING_ENV,
    PTG2_RUST_EVENT_QUEUE_ENV, PTG2_RUST_PARSE_IN_WORKERS_ENV, PTG2_RUST_SCANNER_BIN_ENV,
    PTG2_RUST_SCANNER_ENV, PTG2_RUST_WORKERS_ENV, PTG2_SERVING_ONLY_IMPORT_ENV,
    PTG2_SERVING_WORKERS_ENV, PTG2_SKIP_BULK_INDEX_ENSURE_ENV,
    PTG2_SKIP_COMPACT_SERVING_INDEX_ENSURE_ENV,
    PTG2_SKIP_EXISTING_PRICE_SETS_ENV, PTG2_SLIM_SERVING_ROWS_ENV,
    PTG2_STAGE_COPY_DEDUPE_DEFAULT_KINDS, PTG2_STAGE_COPY_DEDUPE_ENV,
    PTG2_STAGE_INDEXES_ENV, PTG2_STAGE_PRICE_SETS_ENV,
    PTG2_STAGE_SERVING_AS_FINAL_ENV, PTG2_STAGE_SERVING_RATES_ENV,
    PTG2_STREAMING_DEDUPE_ENV, PTG2_UNLOGGED_FINAL_ENV,
    PTG2_UNLOGGED_STAGE_ENV, PTG2_WORKER_CHUNK_BYTES_ENV,
    PTG2_WORKER_CHUNK_ITEMS_ENV, PTG2_WORKER_MAX_PENDING_BATCHES_ENV,
    PTG2_WORKER_MAX_PENDING_BYTES_ENV, PTG2_WORKER_RESULT_FILES_ENV,
    TEST_ALLOWED_ITEMS, TEST_IN_NETWORK_ITEMS, TEST_NEGOTIATED_PRICES,
    TEST_PROVIDER_GROUPS, TEST_TOC_FILES, TEST_TOC_JOBS, _env_bool, _env_int,
    _ptg2_stage_copy_dedupe_enabled, _use_compact_serving_table,
    _use_rust_compact_serving, _use_serving_only_import,
    _use_stage_serving_as_final)
from process.ptg_parts.copy_load import (_copy_compact_serving_rate_file,
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
                                         _ptg2_copy_record, _ptg2_json_columns)
from process.ptg_parts.db_tables import (_estimated_table_rows,
                                         _exact_table_rows, _quote_ident,
                                         _table_exists, _table_has_rows)
from process.ptg_parts.domain import (PTG2_ARTIFACT_RAW,
                                      PTG2_CONFIDENCE_NPPES_MAILING_LOCATION,
                                      PTG2_CONFIDENCE_NPPES_PRACTICE_LOCATION,
                                      PTG2_CONFIDENCE_PAYER_DIRECTORY,
                                      PTG2_CONFIDENCE_TIC_RATE_NPI_TIN,
                                      PTG2_DOMAIN_ALLOWED_AMOUNT,
                                      PTG2_DOMAIN_DRUG, PTG2_DOMAIN_IN_NETWORK,
                                      PTG2_MODE_EXACT_SOURCE,
                                      PTG2_MODE_PRODUCT_SEARCH,
                                      PTG2_STATUS_BUILDING,
                                      PTG2_STATUS_DEAD_LETTER,
                                      PTG2_STATUS_FAILED, PTG2_STATUS_PENDING,
                                      PTG2_STATUS_PUBLISHED,
                                      PTG2_STATUS_RUNNING,
                                      PTG2_STATUS_VALIDATED,
                                      PTG2ConfidenceEnum,
                                      PTG2ContentIdentityValue,
                                      PTG2ContractEvent, PTG2DownloadedJob,
                                      PTG2FileProcessResult, PTG2HeadMetadata,
                                      PTG2LogicalArtifact, PTG2PriceAtomEvent,
                                      PTG2PriceSetValue, PTG2ProcedureEvent,
                                      PTG2ProviderGroupEvent,
                                      PTG2ProviderSetValue, PTG2RatePackValue,
                                      PTG2RawArtifact, PTG2SourceCatalogEntry,
                                      PTG2SourceTraceSetValue,
                                      PTG2SourceVersion,
                                      normalize_ptg2_search_mode,
                                      ptg2_confidence_statement)
from process.ptg_parts.import_rows import (
    _build_provider_set_entry, _combine_provider_set_entries,
    _fast_provider_entry_from_parts, _fast_provider_entry_from_provider_refs,
    _normalize_import_id, _ptg2_context_row, _ptg2_plan_rows,
    _ptg2_price_atom_row, _ptg2_procedure_row, _ptg2_provider_group_rows,
    _ptg2_provider_set_row, _ptg2_source_trace_rows)
from process.ptg_parts.json_streams import (
    _iter_top_level_object_bytes, _iter_top_level_objects,
    _iter_top_level_objects_fast, _iter_top_level_objects_jsondecoder,
    _json_loads)
from process.ptg_parts.live_progress import (current_live_progress_context,
                                             reset_live_progress_context,
                                             set_live_progress_context,
                                             write_live_progress)
from process.ptg_parts.progress import (_artifact_progress_position,
                                        _format_duration,
                                        _maybe_log_artifact_progress, _utcnow)
from process.ptg_parts.provider_cache import (
    PTG2InMemoryProviderReferenceCache, PTG2ProviderReferenceCache,
    _normalize_provider_ref, _provider_cache_get, _provider_cache_hashes,
    _provider_cache_put, _provider_combo_cache_get, _provider_combo_cache_key,
    _provider_combo_cache_put)
from process.ptg_parts.provider_references import (
    _load_provider_references_from_file, _process_provider_reference_file)
from process.ptg_parts.ptg2_manifest_artifacts import (
    PTG2_MANIFEST_DENSE_MEMBERSHIP_FORMAT, PTG2_MANIFEST_MEMBERSHIP_FORMAT)
from process.ptg_parts.ptg2_manifest_publish import (
    _copy_ptg2_manifest_price_atom_file,
    _copy_ptg2_manifest_provider_group_member_file,
    _copy_ptg2_manifest_serving_file,
    _create_ptg2_manifest_serving_stage_table,
    _ptg2_manifest_support_stage_table,
    _publish_ptg2_manifest_serving_snapshot)
from process.ptg_parts.row_helpers import (_as_int_list, _as_list,
                                           _coerce_date, _make_checksum,
                                           _normalize_code_component,
                                           _normalize_tin_type,
                                           _normalize_tin_value,
                                           _normalized_npi_list,
                                           _provider_group_hash_prefix,
                                           _provider_group_identity_hash)
from process.ptg_parts.rust_publish import (
    _ptg2_publish_timestamp, _ptg2_serving_child_table_name,
    _publish_renamed_rust_dictionary_table,
    _publish_rust_compact_snapshot_tables, _publish_rust_serving_stage_tables)
from process.ptg_parts.rust_scanner import (
    _aiter_compact_serving_records_rust, _iter_compact_serving_records_rust,
    _iter_top_level_object_bytes_rust, _ptg2_rust_scanner_binary)
from process.ptg_parts.rust_stage import (_RUST_COPY_TABLE_SPECS,
                                          PTG2_SERVING_STAGE_LANE_PREFIX,
                                          _create_rust_copy_stage_tables,
                                          _merge_rust_copy_stage_tables,
                                          _ptg2_dictionary_select_columns,
                                          _rust_copy_stage_table_name,
                                          _serving_stage_lane_key,
                                          _serving_stage_table_for_copy,
                                          _serving_stage_tables)
from process.ptg_parts.screen import _emit_screen_line
from process.ptg_parts.serving_index import (
    _ptg2_table_available, build_ptg2_compact_serving_index,
    build_ptg2_db_serving_index, build_ptg2_stage_serving_index,
    finalize_ptg2_incremental_serving_index)
from process.ptg_parts.serving_maintenance import (
    _build_ptg2_provider_locations, _copy_simple_rows,
    _count_compact_serving_rate_rows, _merge_staged_price_sets,
    _merge_staged_serving_rates)
from process.ptg_parts.serving_only import (
    _iter_worker_result_rows, _normalize_serving_price_payload,
    _ptg2_worker_capacity_wait_needed, _serving_only_hash_int_sets,
    _serving_only_hash_price_key, _serving_only_hash_text,
    _serving_only_key_list, _serving_only_key_value,
    _serving_only_merge_worker_result, _serving_only_price_payload,
    _serving_only_price_payload_and_key, _serving_only_rows_for_payload)
from process.ptg_parts.serving_only import \
    _serving_only_worker_process_chunk_to_files as \
    _serving_only_worker_process_chunk_to_files_impl
from process.ptg_parts.serving_only import _worker_payload_size
from process.ptg_parts.serving_rows import (_provider_group_member_rows,
                                            _provider_set_component_rows,
                                            _ptg2_compact_serving_rate_row,
                                            _ptg2_hp_procedure_code,
                                            _ptg2_serving_rate_row)
from process.ptg_parts.snapshot_artifacts import (
    _row_mapping, build_ptg2_compact_snapshot_index_artifact,
    build_ptg2_snapshot_index_artifact)
from process.ptg_parts.snapshot_cleanup import (
    _cleanup_old_ptg2_source_tables, _drop_ptg2_snapshot_table_names,
    _drop_ptg2_snapshot_tables_for_manifest, _snapshot_manifest_table_names)
from process.ptg_parts.snapshot_tables import (_normalize_source_key,
                                               _ptg2_snapshot_index_name,
                                               _ptg2_snapshot_table_name,
                                               _ptg2_snapshot_table_token)
from process.ptg_parts.source_download import (PTG2_DEFAULT_MAX_BYTES,
                                               _download_ptg_job_artifact,
                                               _download_ptg_job_artifact_sync,
                                               _download_raw_artifact_ranges,
                                               _emit_download_progress,
                                               _format_eta_seconds,
                                               _iter_downloaded_ptg_jobs,
                                               _probe_http_range_support,
                                               download_raw_artifact,
                                               fetch_head_metadata,
                                               materialize_json_source)
from process.ptg_parts.source_files import (_build_file_row,
                                            _derive_plan_fields,
                                            _extract_metadata_fields,
                                            _maybe_unzip)
from process.ptg_parts.source_jobs import (_dedupe_preserve, _dedupe_ptg_jobs,
                                           _dedupe_rows_by,
                                           _filter_jobs_by_url_contains,
                                           _filter_reporting_plans,
                                           _load_toc_urls_from_file,
                                           _looks_like_toc_body_file_location,
                                           _merge_ptg_job,
                                           _normalize_filter_values,
                                           _normalize_plan_payload,
                                           _plan_identity,
                                           _plan_matches_filters,
                                           _ptg_job_identity,
                                           parse_toc_catalog_entries)
from process.ptg_parts.source_pointers import (_current_source_snapshot_id,
                                               _ptg2_plan_source_key,
                                               _publish_ptg2_source_pointers,
                                               _source_plan_rows)
from process.ptg_parts.source_versions import _record_source_version
from process.ptg_parts.table_setup import (
    PTG2_MODEL_CLASSES, _drop_ptg2_columns, _ensure_indexes,
    _ensure_ptg2_price_atom_columns, _ensure_ptg2_price_set_columns,
    _ensure_ptg2_price_set_stage_table, _ensure_ptg2_provider_set_columns,
    _ensure_ptg2_serving_rate_columns, _ensure_ptg2_serving_rate_stage_table,
    _prepare_ptg_tables, ensure_ptg2_tables)
from process.ptg_parts.values import (_catalog_entry_id, build_fact_chunk,
                                      build_price_atom, build_price_set,
                                      build_procedure_collection,
                                      build_provider_set,
                                      build_provider_set_collection,
                                      build_rate_pack, build_rate_pack_group,
                                      build_rate_pack_procedure_group,
                                      build_rate_set, build_source_trace_set,
                                      provider_hash_bucket,
                                      ptg2_provider_bucket_count)
from process.url_security import fetch_max_bytes

logger = logging.getLogger(__name__)

PTG2_SOURCE_SCOPED_TEST_ENV = "HLTHPRT_PTG2_SOURCE_SCOPED_TEST"
PTG2_AUTO_ADDRESS_REFRESH_ENV = "HLTHPRT_PTG2_AUTO_ADDRESS_REFRESH"
PTG2_AUTO_ADDRESS_REFRESH_TEST_ENV = "HLTHPRT_PTG2_AUTO_ADDRESS_REFRESH_TEST"
PTG2_AUTO_ADDRESS_REFRESH_LIMIT_ENV = "HLTHPRT_PTG2_AUTO_ADDRESS_REFRESH_LIMIT_PER_SOURCE"
PTG2_AUTO_ADDRESS_REFRESH_PUBLISH_ENV = "HLTHPRT_PTG2_AUTO_ADDRESS_REFRESH_PUBLISH"


def _ptg2_auto_address_refresh_enabled(*, test_mode: bool) -> tuple[bool, str | None]:
    # PTG/TiC files do not carry authoritative provider locations. Address
    # refreshes are now owned by address-bearing sources and the unified address
    # importer, so PTG imports should not rebuild a synthetic pricing address layer by
    # default. Keep the env gate for one-off operator-triggered unified refreshes.
    if not _env_bool(PTG2_AUTO_ADDRESS_REFRESH_ENV, False):
        return False, "disabled"
    if test_mode and not _env_bool(PTG2_AUTO_ADDRESS_REFRESH_TEST_ENV, False):
        return False, "test-mode-disabled"
    return True, None


def _ptg2_auto_address_refresh_payload(
    *,
    source_key: str,
    snapshot_id: str,
    import_run_id: str,
    test_mode: bool,
) -> dict[str, Any]:
    params: dict[str, Any] = {
        "refresh_mode": "full",
        "trigger_source_key": source_key,
        "trigger_snapshot_id": snapshot_id,
        "publish": _env_bool(PTG2_AUTO_ADDRESS_REFRESH_PUBLISH_ENV, True),
    }
    if test_mode:
        params["test_mode"] = True
    limit_per_source = max(_env_int(PTG2_AUTO_ADDRESS_REFRESH_LIMIT_ENV, 0), 0)
    if limit_per_source:
        params["limit_per_source"] = limit_per_source
    return {
        "run_id": None,
        "importer": "entity-address-unified",
        "params": params,
        "idempotency_key": f"entity-address-unified:{source_key}:{snapshot_id}",
        "triggered_by": "ptg_import",
        "schedule_id": None,
        "subscription_id": None,
        "import_id": f"entity-address-unified:{import_run_id}",
    }


async def _enqueue_ptg2_auto_address_refresh_after_import(
    *,
    source_key: str | None,
    snapshot_id: str,
    import_run_id: str,
    has_serving_files: bool,
    source_scoped_compact: bool,
    test_mode: bool,
) -> dict[str, Any]:
    if not has_serving_files:
        return {"status": "skipped", "reason": "no-serving-files"}
    if not source_scoped_compact:
        return {"status": "skipped", "reason": "not-source-scoped"}
    if not source_key:
        return {"status": "skipped", "reason": "missing-source-key"}
    enabled, reason = _ptg2_auto_address_refresh_enabled(test_mode=test_mode)
    if not enabled:
        return {"status": "skipped", "reason": reason}
    payload = _ptg2_auto_address_refresh_payload(
        source_key=source_key,
        snapshot_id=snapshot_id,
        import_run_id=import_run_id,
        test_mode=test_mode,
    )
    try:
        from api.control_imports import create_import_run, ensure_import_run_table

        await ensure_import_run_table()
        run, created = await create_import_run(payload)
        return {
            "status": "queued" if created else "existing",
            "created": bool(created),
            "run_id": run.get("run_id"),
            "importer": run.get("importer") or payload["importer"],
            "idempotency_key": payload["idempotency_key"],
            "params": payload["params"],
        }
    except Exception as exc:
        logger.exception("Failed to enqueue pricing address refresh after PTG import %s", import_run_id)
        return {
            "status": "enqueue_failed",
            "error": str(exc),
            "idempotency_key": payload["idempotency_key"],
            "params": payload["params"],
        }


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

_PTG2_WORKER_PROVIDER_MAP = None
_PTG2_WORKER_PLAN_FIELDS: dict[str, Any] | None = None
_PTG2_WORKER_SNAPSHOT_ID: str | None = None
_PTG2_WORKER_PLAN_MONTH_ID: str | None = None
_PTG2_WORKER_SOURCE_TRACE: list[dict[str, Any]] | None = None
_PTG2_WORKER_SOURCE_TRACE_SET_HASH: str | None = None
_PTG2_WORKER_SLIM_SERVING_ROWS = False
_PTG2_WORKER_COMPACT_SERVING = False


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


def _ptg2_copy_file_row_count(path: Path) -> int:
    if not path.exists() or path.stat().st_size <= 0:
        return 0
    with path.open("rb") as fp:
        return sum(1 for _line in fp)


def _collect_ptg2_manifest_sidecar_artifacts(
    sidecar_paths: dict[str, Path | None],
) -> dict[str, dict[str, Any]]:
    artifacts: dict[str, dict[str, Any]] = {}
    for artifact_kind, artifact_path in sidecar_paths.items():
        if (
            artifact_path is None
            or not artifact_path.exists()
            or artifact_path.stat().st_size <= 0
        ):
            continue
        digest, byte_count = sha256_file(artifact_path)
        record_format = PTG2_MANIFEST_MEMBERSHIP_FORMAT
        with artifact_path.open("rb") as artifact_fp:
            if artifact_fp.read(8) == b"PTG2MNDS":
                record_format = PTG2_MANIFEST_DENSE_MEMBERSHIP_FORMAT
        artifacts[artifact_kind] = {
            "name": artifact_kind,
            "path": str(artifact_path),
            "record_format": record_format,
            "sha256": digest,
            "byte_count": byte_count,
        }
    return artifacts


def _run_ptg2_manifest_copy_merge_sync(kind: str, output_path: Path, input_paths: list[Path]) -> dict[str, Any]:
    binary = _ptg2_rust_scanner_binary()
    if binary is None:
        raise RuntimeError(
            "PTG2 manifest pre-COPY merge requires the Rust scanner binary; "
            "build it with `cargo build --release --manifest-path support/ptg2_scanner/Cargo.toml`"
        )
    existing_paths = [path for path in input_paths if path.exists() and path.stat().st_size > 0]
    if not existing_paths:
        output_path.touch()
        return {"kind": kind, "input_files": 0, "input_rows": 0, "output_rows": 0, "dropped_rows": 0}
    process = subprocess.run(
        [
            str(binary),
            "--merge-manifest-copy",
            kind,
            str(output_path),
            *[str(path) for path in existing_paths],
        ],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=False,
    )
    if process.returncode != 0:
        stderr = process.stderr.decode("utf-8", errors="replace")
        raise RuntimeError(f"PTG2 manifest pre-COPY merge failed for {kind}: {stderr[-1000:]}")
    try:
        header, rest = process.stdout.split(b"\n", 1)
        record_kind, length_bytes = header.split(b"\t", 1)
        payload_len = int(length_bytes)
        payload = rest[:payload_len]
        if record_kind == b"manifest_copy_merge_summary":
            return json.loads(payload.decode("utf-8"))
    except Exception as exc:
        logger.debug("failed to parse PTG2 manifest pre-COPY merge summary: %s", exc)
    output_rows = _ptg2_copy_file_row_count(output_path)
    input_rows = sum(_ptg2_copy_file_row_count(path) for path in existing_paths)
    return {
        "kind": kind,
        "input_files": len(existing_paths),
        "input_rows": input_rows,
        "output_rows": output_rows,
        "dropped_rows": max(input_rows - output_rows, 0),
    }


async def _run_ptg2_manifest_copy_merge(kind: str, output_path: Path, input_paths: list[Path]) -> dict[str, Any]:
    return await asyncio.to_thread(_run_ptg2_manifest_copy_merge_sync, kind, output_path, input_paths)


async def _merge_and_copy_ptg2_manifest_files(
    *,
    successful_files: list[dict[str, Any]],
    manifest_stage_table: str,
) -> dict[str, Any]:
    copy_files_by_kind: dict[str, list[Path]] = {
        "manifest_serving": [],
        "price_atom": [],
        "provider_group_member": [],
    }
    emitted_rows_by_kind: dict[str, int] = {kind: 0 for kind in copy_files_by_kind}
    for file_summary in successful_files:
        summary_payload = file_summary.get("summary") if isinstance(file_summary, dict) else None
        manifest_payload = summary_payload.get("manifest") if isinstance(summary_payload, dict) else None
        copy_files = manifest_payload.get("copy_files") if isinstance(manifest_payload, dict) else None
        if not isinstance(copy_files, dict):
            continue
        for kind in copy_files_by_kind:
            entries = copy_files.get(kind) or []
            if not isinstance(entries, list):
                continue
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                raw_path = str(entry.get("path") or "").strip()
                if not raw_path:
                    continue
                copy_files_by_kind[kind].append(Path(raw_path))
                try:
                    emitted_rows_by_kind[kind] += int(entry.get("row_count") or 0)
                except (TypeError, ValueError):
                    pass
    if not any(copy_files_by_kind.values()):
        return {"enabled": False, "reason": "no_deferred_copy_files"}

    def _new_merge_path(prefix: str) -> Path:
        fd, name = tempfile.mkstemp(prefix=prefix, suffix=".copy", dir=ptg2_temp_parent())
        os.close(fd)
        path = Path(name)
        path.unlink(missing_ok=True)
        return path

    merged_serving = _new_merge_path("ptg2_manifest_serving_merged_")
    merged_price_atom = _new_merge_path("ptg2_manifest_price_atom_merged_")
    merged_provider_group_member = _new_merge_path("ptg2_manifest_provider_group_member_merged_")
    merge_metrics: dict[str, Any] = {"enabled": True, "kinds": {}, "emitted_rows": emitted_rows_by_kind}
    try:
        serving_metrics = await _run_ptg2_manifest_copy_merge(
            "manifest_serving",
            merged_serving,
            copy_files_by_kind["manifest_serving"],
        )
        price_atom_metrics = await _run_ptg2_manifest_copy_merge(
            "price_atom",
            merged_price_atom,
            copy_files_by_kind["price_atom"],
        )
        provider_group_member_metrics = await _run_ptg2_manifest_copy_merge(
            "provider_group_member",
            merged_provider_group_member,
            copy_files_by_kind["provider_group_member"],
        )
        price_atom_table = _ptg2_manifest_support_stage_table(manifest_stage_table, "price_atom")
        provider_group_member_table = _ptg2_manifest_support_stage_table(manifest_stage_table, "provider_group_member")
        await _copy_ptg2_manifest_serving_file(merged_serving, target_table=manifest_stage_table)
        await _copy_ptg2_manifest_price_atom_file(merged_price_atom, target_table=price_atom_table)
        await _copy_ptg2_manifest_provider_group_member_file(
            merged_provider_group_member,
            target_table=provider_group_member_table,
        )
        merge_metrics["kinds"] = {
            "manifest_serving": serving_metrics,
            "price_atom": price_atom_metrics,
            "provider_group_member": provider_group_member_metrics,
        }
        merge_metrics["serving_rows"] = int(serving_metrics.get("output_rows") or 0)
        _emit_screen_line(f"PTG2_MANIFEST_PRECOPY_MERGE\t{json.dumps(merge_metrics, sort_keys=True)}")
        return merge_metrics
    finally:
        for path in (
            merged_serving,
            merged_price_atom,
            merged_provider_group_member,
            *copy_files_by_kind["manifest_serving"],
            *copy_files_by_kind["price_atom"],
            *copy_files_by_kind["provider_group_member"],
        ):
            try:
                path.unlink(missing_ok=True)
            except Exception:
                logger.debug("Failed to remove PTG2 manifest merge file %s", path, exc_info=True)


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
    ptg2_manifest_stage_table: str | None = None,
    source_network_names: list[str] | str | None = None,
) -> dict[str, Any]:
    if not ptg2_manifest_stage_table:
        raise RuntimeError("PTG imports require manifest serving stage tables")
    if rust_stage_tables:
        raise RuntimeError("Legacy PTG Rust stage tables are no longer supported")
    if max_items is not None:
        logger.info("Ignoring max_items=%s for manifest-backed Rust PTG import", max_items)

    plan_fields = _derive_plan_fields(meta, plan_info)
    source_network_name_values = _normalize_source_network_names(source_network_names)
    plan_row, alias_rows, plan_month_row = _ptg2_plan_rows(plan_fields, snapshot_id, import_month)
    _source_trace_row, _source_trace_set_row = _ptg2_source_trace_rows(source_version, source_url)
    source_trace_set_hash = _source_trace_set_row["source_trace_set_hash"]

    await _push_ptg2_objects([plan_row], PTG2Plan, rewrite=True)
    if alias_rows:
        await _push_ptg2_objects(alias_rows, PTG2PlanAlias, rewrite=True)
    await _push_ptg2_objects([plan_month_row], PTG2PlanMonth, rewrite=True)

    copy_tmp_dir = ptg2_temp_parent()
    manifest_stage_table = ptg2_manifest_stage_table
    manifest_copy_rows = 0
    rust_records = 0
    rust_dedupe_summary: dict[str, Any] = {}
    rust_scanner_config: dict[str, Any] = {}
    rust_scanner_summary: dict[str, Any] = {}
    procedure_hashes: set[str] = set()
    defer_manifest_copy = _env_bool(PTG2_MANIFEST_PRECOPY_MERGE_ENV, True)
    deferred_copy_files: dict[str, list[dict[str, Any]]] = {
        "manifest_serving": [],
        "price_atom": [],
        "provider_group_member": [],
    }
    compact_copy_tasks: set[asyncio.Task] = set()
    compact_copy_task_limit = max(_env_int(PTG2_COMPACT_COPY_TASKS_ENV, PTG2_DEFAULT_COMPACT_COPY_TASKS), 1)
    compact_copy_semaphore = asyncio.Semaphore(compact_copy_task_limit)
    copy_completed = False

    def _new_copy_path(prefix: str) -> Path:
        fd, name = tempfile.mkstemp(prefix=prefix, suffix=".copy", dir=copy_tmp_dir)
        os.close(fd)
        return Path(name)

    manifest_serving_copy_path = _new_copy_path("ptg2_manifest_serving_")
    manifest_price_atom_copy_path = _new_copy_path("ptg2_manifest_price_atom_")
    manifest_provider_group_member_copy_path = _new_copy_path("ptg2_manifest_provider_group_member_")
    manifest_artifact_dir = resolve_ptg2_artifact_dir() / "serving" / _ptg2_snapshot_table_token(
        str(plan_fields.get("plan_id") or "plan"),
        snapshot_id,
    )
    manifest_artifact_dir.mkdir(parents=True, exist_ok=True)
    manifest_file_token = hashlib.sha256(str(Path(file_path).resolve()).encode("utf-8")).hexdigest()[:16]
    provider_npi_sidecar_enabled = _env_bool(PTG2_MANIFEST_PROVIDER_NPI_SIDECAR_ENABLED_ENV, True)
    manifest_sidecar_paths = {
        "provider_forward": manifest_artifact_dir / f"provider_forward_{manifest_file_token}.ptg2sc",
        "provider_inverted": manifest_artifact_dir / f"provider_inverted_{manifest_file_token}.ptg2sc",
        "provider_npi": (
            manifest_artifact_dir / f"provider_npi_{manifest_file_token}.ptg2sc"
            if provider_npi_sidecar_enabled
            else None
        ),
        "price_forward": manifest_artifact_dir / f"price_forward_{manifest_file_token}.ptg2sc",
    }

    def emit_copy_status(status: str, *, kind: str, copy_file: Path, rows: int, target_table: str | None, started_at: float | None = None) -> None:
        payload: dict[str, Any] = {
            "event": f"PTG2_COPY_SHARD_{status}",
            "kind": kind,
            "path": str(copy_file),
            "rows": rows,
            "target_table": target_table,
        }
        if started_at is not None:
            payload["elapsed_seconds"] = round(time.monotonic() - started_at, 3)
        _emit_screen_line(json.dumps(payload, sort_keys=True))

    async def copy_ready_manifest_serving_file(copy_row: dict[str, Any]) -> None:
        nonlocal manifest_copy_rows
        raw_copy_path = str(copy_row.get("path") or "").strip()
        if not raw_copy_path:
            return
        copied_rows = int(copy_row.get("row_count") or 0)
        copy_file = Path(raw_copy_path)
        if defer_manifest_copy:
            if copied_rows <= 0:
                copied_rows = _ptg2_copy_file_row_count(copy_file)
            manifest_copy_rows += copied_rows
            deferred_copy_files["manifest_serving"].append({"path": str(copy_file), "row_count": copied_rows})
            return
        async with compact_copy_semaphore:
            started_at = time.monotonic()
            emit_copy_status("START", kind="manifest_serving", copy_file=copy_file, rows=copied_rows, target_table=manifest_stage_table)
            await _copy_ptg2_manifest_serving_file(copy_file, target_table=manifest_stage_table)
            emit_copy_status("DONE", kind="manifest_serving", copy_file=copy_file, rows=copied_rows, target_table=manifest_stage_table, started_at=started_at)
        manifest_copy_rows += copied_rows
        copy_file.unlink(missing_ok=True)

    async def copy_ready_manifest_dictionary_file(kind: str, copy_row: dict[str, Any]) -> None:
        raw_copy_path = str(copy_row.get("path") or "").strip()
        if not raw_copy_path:
            return
        copy_file = Path(raw_copy_path)
        copied_rows = int(copy_row.get("row_count") or 0)
        if copied_rows <= 0:
            copied_rows = _ptg2_copy_file_row_count(copy_file)
        if kind == "price_atom":
            target_table = _ptg2_manifest_support_stage_table(manifest_stage_table, "price_atom")
            copy_func = _copy_ptg2_manifest_price_atom_file
        elif kind == "provider_group_member":
            target_table = _ptg2_manifest_support_stage_table(manifest_stage_table, "provider_group_member")
            copy_func = _copy_ptg2_manifest_provider_group_member_file
        else:
            return
        if defer_manifest_copy:
            deferred_copy_files[kind].append({"path": str(copy_file), "row_count": copied_rows})
            return
        async with compact_copy_semaphore:
            started_at = time.monotonic()
            emit_copy_status("START", kind=f"manifest_{kind}", copy_file=copy_file, rows=copied_rows, target_table=target_table)
            await copy_func(copy_file, target_table=target_table)
            emit_copy_status("DONE", kind=f"manifest_{kind}", copy_file=copy_file, rows=copied_rows, target_table=target_table, started_at=started_at)
        copy_file.unlink(missing_ok=True)

    async def wait_for_some_copy_tasks(force: bool = False) -> None:
        nonlocal compact_copy_tasks
        if not compact_copy_tasks:
            return
        if force:
            done, compact_copy_tasks = await asyncio.wait(compact_copy_tasks, return_when=asyncio.ALL_COMPLETED)
            compact_copy_tasks = set()
        elif len(compact_copy_tasks) < compact_copy_task_limit * 2:
            return
        else:
            done, compact_copy_tasks = await asyncio.wait(compact_copy_tasks, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            task.result()

    try:
        async for record_kind, record_row in _aiter_compact_serving_records_rust(
            file_path,
            snapshot_id=snapshot_id,
            plan_id=str(plan_fields.get("plan_id") or ""),
            plan_month_id=str(plan_month_row["plan_month_id"]),
            source_trace_set_hash=source_trace_set_hash,
            manifest_serving_copy_path=manifest_serving_copy_path,
            manifest_provider_forward_sidecar_path=manifest_sidecar_paths.get("provider_forward"),
            manifest_provider_inverted_sidecar_path=manifest_sidecar_paths.get("provider_inverted"),
            manifest_provider_npi_sidecar_path=manifest_sidecar_paths.get("provider_npi"),
            manifest_price_forward_sidecar_path=manifest_sidecar_paths.get("price_forward"),
            manifest_price_atom_copy_path=manifest_price_atom_copy_path,
            manifest_provider_group_member_copy_path=manifest_provider_group_member_copy_path,
            source_network_names=source_network_name_values,
            manifest_only=True,
        ):
            if record_kind == "dedupe_summary":
                rust_dedupe_summary = dict(record_row or {})
                continue
            if record_kind == "scanner_config":
                rust_scanner_config = dict(record_row or {})
                continue
            if record_kind == "scanner_summary":
                rust_scanner_summary = dict(record_row or {})
                continue
            rust_records += 1
            if record_kind == "manifest_serving_copy_file":
                compact_copy_tasks.add(asyncio.create_task(copy_ready_manifest_serving_file(record_row)))
                await wait_for_some_copy_tasks()
            elif record_kind == "manifest_price_atom_copy_file":
                compact_copy_tasks.add(asyncio.create_task(copy_ready_manifest_dictionary_file("price_atom", record_row)))
                await wait_for_some_copy_tasks()
            elif record_kind == "manifest_provider_group_member_copy_file":
                compact_copy_tasks.add(asyncio.create_task(copy_ready_manifest_dictionary_file("provider_group_member", record_row)))
                await wait_for_some_copy_tasks()
            elif record_kind in {"procedure", "serving_rate_compact"} and record_row.get("procedure_hash"):
                procedure_hashes.add(str(record_row.get("procedure_hash")))
        await wait_for_some_copy_tasks(force=True)
        for copy_path, copy_func in (
            (manifest_serving_copy_path, copy_ready_manifest_serving_file),
            (manifest_price_atom_copy_path, lambda row: copy_ready_manifest_dictionary_file("price_atom", row)),
            (manifest_provider_group_member_copy_path, lambda row: copy_ready_manifest_dictionary_file("provider_group_member", row)),
        ):
            if copy_path.exists() and copy_path.stat().st_size > 0:
                await copy_func({"path": str(copy_path), "row_count": 0})
        if manifest_copy_rows == 0 and not defer_manifest_copy:
            manifest_copy_rows = await _estimated_table_rows(os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", manifest_stage_table)
        copy_completed = True
    finally:
        for task in compact_copy_tasks:
            task.cancel()
        if (copy_completed and not defer_manifest_copy) or not _env_bool(PTG2_KEEP_PARTIAL_ENV, False):
            for copy_path in (
                manifest_serving_copy_path,
                manifest_price_atom_copy_path,
                manifest_provider_group_member_copy_path,
            ):
                try:
                    if not defer_manifest_copy or not copy_path.exists() or copy_path.stat().st_size == 0:
                        copy_path.unlink(missing_ok=True)
                except Exception:
                    logger.debug("Failed to remove PTG2 manifest copy file %s", copy_path, exc_info=True)
                for worker_copy_path in copy_path.parent.glob(f"{copy_path.name}.worker*"):
                    try:
                        if worker_copy_path.exists() and worker_copy_path.stat().st_size == 0:
                            worker_copy_path.unlink(missing_ok=True)
                    except Exception:
                        logger.debug("Failed to remove empty PTG2 manifest worker copy file %s", worker_copy_path, exc_info=True)

    await flush_error_log(import_log_cls)
    manifest_artifacts = _collect_ptg2_manifest_sidecar_artifacts(manifest_sidecar_paths)
    summary = {
        "provider_refs": 0,
        "in_network_items": len(procedure_hashes),
        "serving_rates": manifest_copy_rows,
        "serving_only": True,
        "serving_workers": 0,
        "worker_chunk_items": 0,
        "rust_manifest_serving": True,
        "rust_records": rust_records,
        "manifest": {
            "serving_rows": manifest_copy_rows,
            "sidecars": manifest_artifacts,
            "sidecar_paths": {
                name: str(path)
                for name, path in manifest_sidecar_paths.items()
                if path is not None
            },
            "copy_files": deferred_copy_files if defer_manifest_copy else {},
            "precopy_merge_deferred": defer_manifest_copy,
        },
    }
    if rust_dedupe_summary:
        summary["dedupe"] = rust_dedupe_summary
    if rust_scanner_config or rust_scanner_summary:
        summary["scanner"] = {
            "config": rust_scanner_config,
            "summary": rust_scanner_summary,
        }
    _emit_screen_line(f"PTG2 serving-only import summary: {summary}")
    logger.info("PTG2 serving-only import summary: %s", summary)
    return summary


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
                state["provider_set_network_names"][provider_set_row["provider_set_hash"]] = sorted(
                    {
                        str(value)
                        for value in _as_list(super_entry.get("network_name"))
                        if str(value or "").strip()
                    }
                )
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


def _toc_file_url_match_tokens(file_url_contains: list[str] | None) -> list[str]:
    """Normalize targeted source-file URL filters for early TOC entry checks."""
    return [
        str(value or "").strip().lower()
        for value in (file_url_contains or [])
        if str(value or "").strip()
    ]


def _is_requested_toc_body_file_url(location: str, file_url_match_tokens: list[str]) -> bool:
    """Return whether a TOC body-file URL satisfies the requested file filters."""
    if not file_url_match_tokens:
        return True
    normalized_location = str(location or "").lower()
    return any(token in normalized_location for token in file_url_match_tokens)


def _has_reached_toc_file_limit(jobs: list[dict[str, Any]], max_files: int | None) -> bool:
    """Return whether TOC processing has selected enough body-file jobs."""
    return max_files is not None and len(jobs) >= max_files


async def _process_table_of_contents(
    toc_url: str,
    classes: dict[str, type],
    test_mode: bool,
    plan_ids: list[str] | None = None,
    plan_name_contains: list[str] | None = None,
    plan_market_types: list[str] | None = None,
    file_url_contains: list[str] | None = None,
    max_files: int | None = None,
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
        toc_content = _load_table_of_contents_artifact(logical_artifact.logical_path)
        if import_run_id:
            await _record_source_version(
                source_type="table-of-contents",
                domain="catalog",
                raw_artifact=raw_artifact,
                logical_artifact=logical_artifact,
                import_run_id=import_run_id,
            )

    file_url_match_tokens = _toc_file_url_match_tokens(file_url_contains)
    targeted_file_import = bool(file_url_match_tokens) and max_files is not None

    if import_run_id and not targeted_file_import:
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
        if _has_reached_toc_file_limit(jobs, max_files):
            break
        plans = _filter_reporting_plans(
            [_normalize_plan_payload(plan) for plan in (structure.get("reporting_plans") or [])],
            plan_ids=plan_ids,
            plan_name_contains=plan_name_contains,
            plan_market_types=plan_market_types,
        )
        if not plans:
            continue
        in_network_files = [
            item for item in _as_list(structure.get("in_network_files")) if isinstance(item, dict)
        ]
        allowed_amount_files = [
            item
            for item in (
                _as_list(structure.get("allowed_amount_file"))
                + _as_list(structure.get("allowed_amount_files"))
            )
            if isinstance(item, dict)
        ]

        for entry in in_network_files:
            location = entry.get("location")
            if not _looks_like_toc_body_file_location(location):
                continue
            location = normalize_tic_source_url(location)
            if not _is_requested_toc_body_file_url(location, file_url_match_tokens):
                continue
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
            if _has_reached_toc_file_limit(jobs, max_files):
                break

        for allowed_amount_file in allowed_amount_files:
            if _has_reached_toc_file_limit(jobs, max_files):
                break
            location = allowed_amount_file.get("location")
            if not _looks_like_toc_body_file_location(location):
                continue
            location = normalize_tic_source_url(location)
            if not _is_requested_toc_body_file_url(location, file_url_match_tokens):
                continue
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


def _looks_tic_toc_json_text(text: str) -> bool:
    normalized = str(text or "").lower()
    return (
        '"reporting_structure"' in normalized
        and '"reporting_plans"' in normalized
        and (
            '"in_network_files"' in normalized
            or '"allowed_amount_file"' in normalized
            or '"allowed_amount_files"' in normalized
        )
    )


def _repair_missing_array_object_commas(text: str) -> str:
    repaired: list[str] = []
    in_string = False
    escaped = False
    length = len(text)
    for idx, char in enumerate(text):
        repaired.append(char)
        if in_string:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
            continue
        if char == '"':
            in_string = True
            continue
        if char != "}":
            continue
        lookahead = idx + 1
        while lookahead < length and text[lookahead].isspace():
            lookahead += 1
        if lookahead < length and text[lookahead] == "{":
            repaired.append(",")
    return "".join(repaired)


def _load_table_of_contents_artifact(path: str | Path) -> dict[str, Any]:
    try:
        toc = load_json_artifact(path)
    except json.JSONDecodeError:
        with open_json_artifact_stream(path) as fp:
            raw = fp.read()
        text = raw.decode("utf-8", errors="replace")
        if not _looks_tic_toc_json_text(text):
            raise
        toc = json.loads(_repair_missing_array_object_commas(text))
    if not isinstance(toc, dict):
        raise ValueError("expected table-of-contents JSON object")
    return toc


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
    ptg2_manifest_stage_table: str | None = None,
    source_network_names: list[str] | str | None = None,
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
        source_network_name_values = _normalize_source_network_names(source_network_names or job.get("source_network_names"))
        try:
            if compact_import:
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
                    ptg2_manifest_stage_table=ptg2_manifest_stage_table,
                    source_network_names=source_network_name_values,
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
        no_data_summary.update(_source_version_summary(source_version))
        return PTG2FileProcessResult(
            "in_network",
            url,
            True,
            file_id=file_row["file_id"],
            summary=no_data_summary,
            skipped=True,
        )
    summary_payload = dict(parse_summary or {})
    summary_payload.update(_source_version_summary(source_version))
    return PTG2FileProcessResult("in_network", url, True, file_id=file_row["file_id"], summary=summary_payload)


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


def _source_version_summary(source_version: PTG2SourceVersion | None) -> dict[str, Any]:
    if source_version is None:
        return {}
    return {
        "engine_source_identity_hash": source_version.source_identity_hash,
        "engine_source_file_version_id": source_version.source_file_version_id,
        "canonical_url": source_version.canonical_url,
        "raw_sha256": source_version.raw_sha256,
        "logical_sha256": source_version.logical_sha256,
        "content_length": source_version.content_length,
        "etag": source_version.etag,
        "last_modified": source_version.last_modified,
    }


def _ptg2_source_file_versions_from_results(files: list[dict[str, Any]]) -> list[dict[str, Any]]:
    versions: list[dict[str, Any]] = []
    seen: set[tuple[str | None, str | None]] = set()
    for item in files:
        summary = item.get("summary") if isinstance(item.get("summary"), dict) else {}
        version_id = summary.get("engine_source_file_version_id") or summary.get("source_file_version_id")
        identity_hash = summary.get("engine_source_identity_hash") or summary.get("source_identity_hash")
        if not version_id and not identity_hash:
            continue
        key = (str(version_id) if version_id else None, str(identity_hash) if identity_hash else None)
        if key in seen:
            continue
        seen.add(key)
        versions.append(
            {
                "source_type": item.get("source_type"),
                "url": item.get("url"),
                "file_id": item.get("file_id"),
                "engine_source_identity_hash": identity_hash,
                "engine_source_file_version_id": version_id,
                "canonical_url": summary.get("canonical_url") or item.get("url"),
                "raw_sha256": summary.get("raw_sha256"),
                "logical_sha256": summary.get("logical_sha256"),
                "content_length": summary.get("content_length"),
                "etag": summary.get("etag"),
                "last_modified": summary.get("last_modified"),
            }
        )
    return versions


def _normalize_source_network_names(value: Any) -> list[str]:
    names: list[str] = []
    seen: set[str] = set()
    for raw_value in _as_list(value):
        name = str(raw_value or "").strip()
        if not name or name in seen:
            continue
        seen.add(name)
        names.append(name)
    return names


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
    source_network_names: list[str] | str | None = None,
    reuse_raw_artifacts: bool = True,
    keep_partial_artifacts: bool | None = None,
    control_run_id: str | None = None,
) -> dict[str, Any]:
    """
    PTG2 entry point for the Transparency in Coverage importer.
    """
    import_started_monotonic = time.monotonic()
    import_month_value = normalize_import_month(import_month)
    source_key_val = _normalize_source_key(source_key or os.getenv("HLTHPRT_PTG2_SOURCE_KEY"))
    import_id_val = _normalize_import_id(
        import_id
        or _default_ptg2_import_id(
            import_month_value,
            source_key_val,
            toc_urls=toc_urls,
            toc_list=toc_list,
            in_network_url=in_network_url,
            allowed_url=allowed_url,
            provider_ref_url=provider_ref_url,
        )
    )
    import_run_id = f"ptg2:{import_id_val}"
    snapshot_id = f"ptg2:{import_month_value.strftime('%Y%m')}:{hash_prefix(semantic_hash(import_run_id), 12)}"
    live_run_id = str(control_run_id or "").strip()
    live_token = set_live_progress_context(
        run_id=live_run_id,
        source_key=source_key_val,
        snapshot_id=snapshot_id,
        import_run_id=import_run_id,
    )
    source_network_name_values = _normalize_source_network_names(source_network_names)
    # Enforce a streaming size cap on every caller-supplied URL (never None for
    # control-triggered runs) so a malicious/huge target cannot OOM or fill the node.
    max_bytes = fetch_max_bytes(PTG2_DEFAULT_MAX_BYTES)
    if test_mode:
        raw_max_bytes = os.getenv("HLTHPRT_PTG2_TEST_MAX_BYTES")
        if raw_max_bytes:
            try:
                max_bytes = int(raw_max_bytes)
            except ValueError:
                logger.warning("Ignoring invalid HLTHPRT_PTG2_TEST_MAX_BYTES=%s", raw_max_bytes)
    write_live_progress(phase="initializing", pct=1, message="initializing PTG import")
    await ensure_database(test_mode)
    await ensure_ptg2_tables()
    compact_import = True
    source_scoped_compact = True
    if source_key_val is None:
        if test_mode:
            source_key_val = _normalize_source_key(import_id_val)
        else:
            raise ValueError("PTG imports require --source-key or HLTHPRT_PTG2_SOURCE_KEY")
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
        "source_network_names": source_network_name_values,
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
        "stage_serving_as_final": False,
        "serving_storage": "manifest_snapshot",
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
    ptg2_manifest_stage_table: str | None = None
    current_pointer_published = False

    async def mark_import_failed(error: BaseException | str, *, progress_message: str | None = None) -> None:
        """Persist import failure state and drop unpublished source-scoped staging tables."""
        error_text = str(error) or "worker task was cancelled"
        write_live_progress(
            status="failed",
            phase="failed",
            pct=100,
            eta_seconds=0,
            message=progress_message or f"PTG import failed: {error_text}",
        )
        if source_scoped_compact and not current_pointer_published:
            serving_index = failure_report.get("serving_index")
            try:
                await _drop_ptg2_snapshot_tables_for_manifest(serving_index if isinstance(serving_index, dict) else None)
                if ptg2_manifest_stage_table:
                    await _drop_ptg2_snapshot_table_names(
                        [
                            ptg2_manifest_stage_table,
                            _ptg2_manifest_support_stage_table(ptg2_manifest_stage_table, "price_atom"),
                            _ptg2_manifest_support_stage_table(ptg2_manifest_stage_table, "provider_group_member"),
                        ]
                    )
            except Exception:
                logger.debug("Failed to clean PTG2 source-scoped tables for failed import", exc_info=True)
        await _mark_ptg2_import_failed(
            import_run_id,
            snapshot_id,
            import_month_value,
            now,
            error_text,
            report=failure_report,
            options=options_payload,
        )

    try:
        assert source_key_val is not None
        write_live_progress(phase="planning", pct=3, message="planning PTG files")
        stage_token = _ptg2_snapshot_table_token(source_key_val, snapshot_id)
        ptg2_manifest_stage_table = await _create_ptg2_manifest_serving_stage_table(stage_token)
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
                    file_url_contains=file_url_contains,
                    max_files=max_files,
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
            job: dict[str, Any] = {"type": "in_network", "url": in_network_url}
            if source_network_name_values:
                job["source_network_names"] = source_network_name_values
            jobs.append(job)
        if allowed_url:
            jobs.append({"type": "allowed_amounts", "url": allowed_url})
        jobs = _filter_jobs_by_url_contains(jobs, file_url_contains)
        if source_network_name_values:
            for job in jobs:
                if job.get("type") == "in_network" and not _normalize_source_network_names(
                    job.get("source_network_names")
                ):
                    job["source_network_names"] = source_network_name_values
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
        processed_file_count_map = {"done": 0}
        attempted_files = len(selected_jobs)
        for progress_index, job in enumerate(selected_jobs):
            job["_ptg_progress_index"] = progress_index
            job["_ptg_progress_total"] = max(attempted_files, 1)
        write_live_progress(
            phase="download",
            unit="files",
            done=0,
            total=attempted_files,
            pct=5 if attempted_files else 20,
            message=f"downloading {attempted_files} PTG file(s)",
        )
        failed_files: list[dict[str, Any]] = []
        skipped_files: list[dict[str, Any]] = []
        successful_files: list[dict[str, Any]] = []
        seen_raw_artifact_hashes: set[str] = set()
        duplicate_raw_files_skipped = 0
        file_process_concurrency = 1
        if compact_import and _use_serving_only_import() and not test_mode:
            file_process_concurrency = max(_env_int(PTG2_FILE_PROCESS_CONCURRENCY_ENV, 1), 1)
        if file_process_concurrency > 1:
            _emit_screen_line(
                "PTG2_FILE_PROCESS_CONCURRENCY"
                f"\tvalue={file_process_concurrency}"
                f"\tfiles={attempted_files}"
            )
        processing_tasks: set[asyncio.Task[PTG2FileProcessResult | None]] = set()

        async def record_file_result(result: PTG2FileProcessResult | None) -> None:
            if result is None:
                return
            if result.success:
                if result.skipped:
                    skipped_files.append(asdict(result))
                else:
                    processed_file_count_map["done"] += 1
                    successful_files.append(asdict(result))
                    if attempted_files:
                        write_live_progress(
                            phase="processing files",
                            unit="files",
                            done=processed_file_count_map["done"],
                            total=attempted_files,
                            pct=min(90.0, 20.0 + (processed_file_count_map["done"] / attempted_files) * 70.0),
                            message=f"processed {processed_file_count_map['done']} of {attempted_files} PTG file(s)",
                        )
            else:
                failed_files.append(asdict(result))

        async def drain_processing_tasks(*, force: bool = False) -> None:
            if not processing_tasks:
                return
            if force:
                done, pending = await asyncio.wait(processing_tasks, return_when=asyncio.ALL_COMPLETED)
            elif len(processing_tasks) < file_process_concurrency:
                return
            else:
                done, pending = await asyncio.wait(processing_tasks, return_when=asyncio.FIRST_COMPLETED)
            processing_tasks.clear()
            processing_tasks.update(pending)
            for task in done:
                await record_file_result(task.result())

        def file_progress_context(job: dict[str, Any], *, start_pct: float, end_pct: float) -> dict[str, Any]:
            try:
                job_index = max(int(job.get("_ptg_progress_index") or 0), 0)
            except (TypeError, ValueError):
                job_index = 0
            try:
                job_total = max(int(job.get("_ptg_progress_total") or attempted_files or 1), 1)
            except (TypeError, ValueError):
                job_total = max(attempted_files, 1)
            return {
                **current_live_progress_context(),
                "overall_progress_start_pct": start_pct + (job_index / job_total) * (end_pct - start_pct),
                "overall_progress_end_pct": start_pct + ((job_index + 1) / job_total) * (end_pct - start_pct),
            }

        async def process_downloaded_job(downloaded) -> PTG2FileProcessResult | None:
            job = downloaded.job
            token = set_live_progress_context(**file_progress_context(job, start_pct=20.0, end_pct=90.0))
            try:
                if job.get("type") == "in_network":
                    return await _process_in_network_file(
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
                        rust_stage_tables=None,
                        ptg2_manifest_stage_table=ptg2_manifest_stage_table,
                        source_network_names=job.get("source_network_names"),
                        raw_artifact=downloaded.raw_artifact,
                        logical_artifact=downloaded.logical_artifact,
                    )
                if job.get("type") == "allowed_amounts":
                    return await _process_allowed_amounts_file(
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
                return None
            finally:
                reset_live_progress_context(token)

        try:
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
                if result is not None:
                    await record_file_result(result)
                    continue

                processing_tasks.add(asyncio.create_task(process_downloaded_job(downloaded)))
                await drain_processing_tasks()
            await drain_processing_tasks(force=True)
        finally:
            for task in processing_tasks:
                task.cancel()

        failure_report = {
            "jobs_discovered": jobs_discovered_before_dedupe,
            "jobs_unique": len(jobs),
            "duplicate_jobs_skipped": duplicate_jobs_skipped,
            "duplicate_raw_files_skipped": duplicate_raw_files_skipped,
            "files_attempted": attempted_files,
            "files_processed": processed_file_count_map["done"],
            "files_failed": len(failed_files),
            "files_skipped": len(skipped_files),
            "successful_files": successful_files,
            "skipped_files": skipped_files,
            "failed_files": failed_files,
            "toc_failures": toc_failures,
            "snapshot_id": snapshot_id,
            "legacy_table_suffix": import_id_val,
        }
        if jobs and processed_file_count_map["done"] == 0:
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
        write_live_progress(phase="publishing", pct=92, message="publishing PTG snapshot")
        manifest_merge_metrics: dict[str, Any] = {"enabled": False}
        has_serving_files = any(
            file_summary.get("source_type") == "in_network" and not file_summary.get("skipped")
            for file_summary in successful_files
        )
        if has_serving_files and _env_bool(PTG2_MANIFEST_PRECOPY_MERGE_ENV, True):
            manifest_merge_metrics = await _merge_and_copy_ptg2_manifest_files(
                successful_files=successful_files,
                manifest_stage_table=ptg2_manifest_stage_table,
            )
            for file_summary in successful_files:
                summary_payload = file_summary.get("summary") if isinstance(file_summary, dict) else None
                manifest_payload = summary_payload.get("manifest") if isinstance(summary_payload, dict) else None
                if isinstance(manifest_payload, dict):
                    manifest_payload.pop("copy_files", None)
        manifest_artifacts = _collect_manifest_artifacts(successful_files)
        assert source_key_val is not None
        if has_serving_files:
            if not ptg2_manifest_stage_table:
                raise RuntimeError("PTG import did not create a manifest-backed serving stage table")
            serving_index = await _publish_ptg2_manifest_serving_snapshot(
                ptg2_manifest_stage_table,
                snapshot_id=snapshot_id,
                source_key=source_key_val,
                artifacts=manifest_artifacts,
                # Pre-copy merge removes exact duplicate rows in the artifact stream,
                # but live payer files can still produce duplicate serving identities.
                # Keep the DB DISTINCT pass as the final publish-time guard.
                db_dedupe_fallback=True,
            )
            ptg2_manifest_stage_table = None
        else:
            if ptg2_manifest_stage_table:
                await _drop_ptg2_snapshot_table_names(
                    [
                        ptg2_manifest_stage_table,
                        _ptg2_manifest_support_stage_table(ptg2_manifest_stage_table, "price_atom"),
                        _ptg2_manifest_support_stage_table(ptg2_manifest_stage_table, "provider_group_member"),
                    ]
                )
                ptg2_manifest_stage_table = None
            serving_index = {
                "type": "allowed_amounts_only",
                "storage": "metadata_only",
                "source_key": source_key_val,
                "serving_rates": 0,
                "rate_count": 0,
            }
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
        report_payload = {
            **failure_report,
            "serving_index": serving_index,
            "timings": timing_payload,
            "manifest_precopy_merge": manifest_merge_metrics,
        }
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
        if has_serving_files and source_scoped_compact and source_key_val:
            await _publish_ptg2_source_pointers(
                source_key=source_key_val,
                snapshot_id=snapshot_id,
                previous_snapshot_id=previous_snapshot_id,
                import_month=import_month_value,
                updated_at=finished,
                serving_index=serving_index,
            )
        if has_serving_files:
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
        if has_serving_files and source_scoped_compact and source_key_val:
            keep_snapshot_ids = {snapshot_id}
            if previous_snapshot_id:
                keep_snapshot_ids.add(previous_snapshot_id)
            await _cleanup_old_ptg2_source_tables(source_key_val, keep_snapshot_ids)
        address_refresh_result = await _enqueue_ptg2_auto_address_refresh_after_import(
            source_key=source_key_val,
            snapshot_id=snapshot_id,
            import_run_id=import_run_id,
            has_serving_files=has_serving_files,
            source_scoped_compact=source_scoped_compact,
            test_mode=test_mode,
        )
        report_payload["address_refresh"] = address_refresh_result
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
            f"\tfiles_processed={processed_file_count_map['done']}"
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
        write_live_progress(
            status="succeeded",
            phase="succeeded",
            unit="files",
            done=processed_file_count_map["done"],
            total=attempted_files,
            pct=100,
            eta_seconds=0,
            message="PTG import succeeded",
        )
        return {
            "status": "succeeded",
            "import_run_id": import_run_id,
            "snapshot_id": snapshot_id,
            "source_key": source_key_val,
            "import_month": import_month_value.isoformat(),
            "jobs_discovered": jobs_discovered_before_dedupe,
            "jobs_unique": len(jobs),
            "duplicate_jobs_skipped": duplicate_jobs_skipped,
            "duplicate_raw_files_skipped": duplicate_raw_files_skipped,
            "files_attempted": attempted_files,
            "files_processed": processed_file_count_map["done"],
            "files_failed": len(failed_files),
            "files_skipped": len(skipped_files),
            "serving_rates": report_payload.get("serving_rates"),
            "rate_count": report_payload.get("rate_count"),
            "source_file_versions": _ptg2_source_file_versions_from_results(successful_files + skipped_files),
            "address_refresh": address_refresh_result,
        }
    except asyncio.CancelledError:
        await mark_import_failed(
            "worker task was cancelled",
            progress_message="PTG import interrupted: worker task was cancelled",
        )
        raise
    except Exception as exc:
        await mark_import_failed(exc)
        raise
    finally:
        reset_live_progress_context(live_token)


def _default_ptg2_import_id(
    import_month_value: datetime.date,
    source_key_val: str | None,
    *,
    toc_urls: list[str] | None = None,
    toc_list: str | None = None,
    in_network_url: str | None = None,
    allowed_url: str | None = None,
    provider_ref_url: str | None = None,
) -> str:
    month_id = import_month_value.strftime("%Y%m%d")
    if not source_key_val:
        return month_id
    source_inputs = {
        "source_key": source_key_val,
        "toc_urls": toc_urls or [],
        "toc_list": toc_list or "",
        "in_network_url": in_network_url or "",
        "allowed_url": allowed_url or "",
        "provider_ref_url": provider_ref_url or "",
    }
    if not any(source_inputs[key] for key in ("toc_urls", "toc_list", "in_network_url", "allowed_url", "provider_ref_url")):
        return month_id
    fingerprint = hash_prefix(
        semantic_hash(
            {"import_month": month_id, **source_inputs},
            domain="ptg2_import_identity",
        ),
        16,
    )
    return f"{month_id}_{fingerprint}"


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


def _manifest_sidecars_list(manifest_payload: dict[str, Any]) -> list[dict[str, Any]]:
    raw_sidecars = manifest_payload.get("sidecars") or {}
    if isinstance(raw_sidecars, dict):
        return [dict(sidecar) for sidecar in raw_sidecars.values() if isinstance(sidecar, dict)]
    if isinstance(raw_sidecars, list):
        return [dict(sidecar) for sidecar in raw_sidecars if isinstance(sidecar, dict)]
    return []


def _collect_manifest_artifacts(
    successful_files: list[dict[str, Any]],
) -> dict[str, Any]:
    sidecar_entries: list[dict[str, Any]] = []
    for file_summary in successful_files:
        summary_payload = file_summary.get("summary") if isinstance(file_summary, dict) else None
        if not isinstance(summary_payload, dict):
            continue
        manifest_payload = summary_payload.get("manifest")
        if not isinstance(manifest_payload, dict):
            continue
        existing_sidecars = _manifest_sidecars_list(manifest_payload)
        if existing_sidecars:
            sidecar_entries.extend(existing_sidecars)
            continue
        raw_sidecar_path_map = manifest_payload.get("sidecar_paths")
        if not isinstance(raw_sidecar_path_map, dict):
            continue
        sidecar_path_map: dict[str, Path | None] = {}
        for name, raw_path in raw_sidecar_path_map.items():
            path = Path(str(raw_path)) if raw_path else None
            sidecar_path_map[str(name)] = path
        fallback_sidecar_map = _collect_ptg2_manifest_sidecar_artifacts(sidecar_path_map)
        sidecar_entries.extend(dict(sidecar) for sidecar in fallback_sidecar_map.values())
    return {"sidecars": sidecar_entries} if sidecar_entries else {}
